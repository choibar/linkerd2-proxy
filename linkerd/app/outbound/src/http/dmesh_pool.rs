//! One lazy DMA H2 transport per (worker, compatible endpoint).
//! Route filters/identity checks/metrics stay outside this transport-only layer.
use linkerd_app_core::{
    proxy::http,
    svc::{self, NewService, Param, Service},
    tls,
    transport::{Remote, ServerAddr},
    Error,
};
use std::{
    collections::HashMap,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock, Weak,
    },
    task::{Context, Poll, Waker},
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::PollSender;

type Response = http::Response<http::BoxBody>;
type ResponseFuture = Pin<Box<dyn Future<Output = Result<Response, Error>> + Send>>;
struct Message {
    source_worker: usize,
    source_thread: std::thread::ThreadId,
    request: http::Request<http::BoxBody>,
    reply: oneshot::Sender<Result<Response, Error>>,
}
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Key {
    worker: usize,
    address: SocketAddr,
    params: http::h2::ClientParams,
    tls: tls::ConditionalClientTls,
}
struct Pool {
    worker: usize,
    address: SocketAddr,
    sender: mpsc::Sender<Message>,
    shared: Arc<parking_lot::Mutex<Shared>>,
}
/// The owner's transport, shared with its routing handles so that a caller on
/// the owner's thread dispatches on its own stack instead of through the owner.
struct Shared {
    service: Option<svc::BoxHttp>,
    /// Set while the owner is idle and the transport last reported ready. A
    /// caller's readiness probe carries no waker, so it must never replace one
    /// the owner is waiting on: the owner clears this before it polls.
    direct: bool,
    /// The owner's thread when it runs on a current-thread runtime, else 0.
    thread: usize,
    live: Arc<AtomicBool>,
    requests: u64,
}
impl Shared {
    fn new() -> Arc<parking_lot::Mutex<Self>> {
        Arc::new(parking_lot::Mutex::new(Self {
            service: None,
            direct: false,
            thread: 0,
            live: Arc::new(AtomicBool::new(false)),
            requests: 0,
        }))
    }
    fn close(&mut self) {
        self.service = None;
        self.direct = false;
    }
}
impl Pool {
    /// Dispatches on the caller's stack when the transport is ready and owned
    /// by this thread; otherwise hands the request back for the owner's queue.
    fn direct(
        &self,
        request: http::Request<http::BoxBody>,
    ) -> Result<ResponseFuture, http::Request<http::BoxBody>> {
        let mut shared = self.shared.lock();
        if !shared.direct
            || shared.thread != thread_token()
            || !shared.live.load(Ordering::Acquire)
            || !dmesh_doca::backend::endpoint_enabled(self.address)
        {
            return Err(request);
        }
        let Some(svc) = shared.service.as_mut() else {
            return Err(request);
        };
        match svc.poll_ready(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(())) => {
                let future = svc.call(request);
                shared.requests += 1;
                Ok(future)
            }
            Poll::Ready(Err(e)) => {
                shared.close();
                Ok(Box::pin(async { Err(e) }))
            }
            Poll::Pending => {
                shared.direct = false;
                Err(request)
            }
        }
    }
}
/// `DMESH_POOL_DIRECT=0` sends every request through the owner task.
fn direct_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("DMESH_POOL_DIRECT").map_or(true, |v| v != "0"))
}
/// Identifies the calling thread; never 0.
fn thread_token() -> usize {
    thread_local!(static TOKEN: u8 = const { 0 });
    TOKEN.with(|token| token as *const u8 as usize)
}
fn directory() -> &'static Mutex<HashMap<Key, Weak<Pool>>> {
    static POOLS: OnceLock<Mutex<HashMap<Key, Weak<Pool>>>> = OnceLock::new();
    POOLS.get_or_init(Default::default)
}

pub struct NewPool<T>(svc::ArcNewHttp<T>, usize);
impl<T> NewPool<T> {
    pub fn new(inner: svc::ArcNewHttp<T>) -> Self {
        Self(inner, dmesh_doca::backend::current_worker())
    }
}
impl<T> NewService<T> for NewPool<T>
where
    T: Param<Remote<ServerAddr>>
        + Param<http::client::Params>
        + Param<tls::ConditionalClientTls>
        + Clone
        + Send
        + Sync
        + 'static,
{
    type Service = svc::BoxHttp;
    fn new_service(&self, target: T) -> Self::Service {
        let Remote(ServerAddr(address)) = target.param();
        let params: http::client::Params = target.param();
        if !dmesh_doca::backend::is_dma(&address) || !matches!(params, http::client::Params::H2(_))
        {
            return self.0.new_service(target);
        }
        let tls: tls::ConditionalClientTls = target.param();
        let http::client::Params::H2(params) = params else {
            unreachable!()
        };
        let worker = self.1;
        let key = Key {
            worker,
            address,
            params,
            tls,
        };
        let mut pools = directory().lock().unwrap();
        // Weak entries do not retain transports after the last routing handle drains.
        pools.retain(|_, pool| pool.strong_count() != 0);
        // One active H2 connection per worker/replica, including across routing
        // stacks. Incompatible parameters cannot silently create a second one.
        if pools
            .keys()
            .any(|other| other.worker == worker && other.address == address && other != &key)
        {
            return svc::BoxService::new(svc::mk(|_| async { Err(unavailable()) }));
        }
        let pool = if let Some(pool) = pools.get(&key).and_then(Weak::upgrade) {
            pool
        } else {
            let (tx, rx) = mpsc::channel::<Message>(128);
            let shared = Shared::new();
            let owned = shared.clone();
            let inner = self.0.clone();
            let Some(runtime) = dmesh_doca::backend::worker_runtime(worker) else {
                return svc::BoxService::new(svc::mk(|_| async { Err(unavailable()) }));
            };
            runtime.spawn(async move {
                let mut rx = rx;
                // No flow exists until a request actually uses this endpoint.
                while let Some(mut first) = rx.recv().await {
                    // Many workers may request every replica at once. Host setup and
                    // DPA SDK admission are serialized in parts; allow a bounded
                    // cold-start budget while still honoring caller cancellation.
                    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
                    let route = loop {
                        if first.reply.is_closed() {
                            break None;
                        }
                        if !dmesh_doca::backend::endpoint_enabled(address) {
                            break None;
                        }
                        if let Some((route, _)) =
                            dmesh_doca::backend::endpoint_runtime(worker, address)
                        {
                            break Some(route);
                        }
                        if tokio::time::Instant::now() >= deadline {
                            tracing::warn!(worker, %address, "dmesh backend creation timed out");
                            break None;
                        }
                        dmesh_doca::backend::request_connection(worker, address);
                        tokio::select! {
                            _ = first.reply.closed() => break None,
                            _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
                        }
                    };
                    let Some(route) = route else {
                        let _ = first.reply.send(Err(unavailable()));
                        continue;
                    };
                    let lease = RouteLease(address, route);
                    tracing::info!(%address, ?route, "dmesh worker-local H2 owner started");
                    let inner = inner.clone();
                    let target = target.clone();
                    rx = run(
                        address,
                        route,
                        move || inner.new_service(target.clone()),
                        &owned,
                        rx,
                        first,
                    )
                    .await;
                    drop(lease);
                }
            });
            let pool = Arc::new(Pool {
                worker,
                address,
                sender: tx,
                shared,
            });
            pools.insert(key, Arc::downgrade(&pool));
            pool
        };
        svc::BoxService::new(Handle {
            sender: PollSender::new(pool.sender.clone()),
            pool,
        })
    }
}

struct Handle {
    sender: PollSender<Message>,
    pool: Arc<Pool>,
}
impl Service<http::Request<http::BoxBody>> for Handle {
    type Response = Response;
    type Error = Error;
    type Future = ResponseFuture;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        // A listening endpoint may have no DMA flow yet. Admission must not
        // wait on publication: the first admitted request triggers creation.
        self.sender.poll_reserve(cx).map_err(|_| unavailable())
    }
    fn call(&mut self, mut request: http::Request<http::BoxBody>) -> Self::Future {
        if direct_enabled() {
            match self.pool.direct(request) {
                Ok(future) => {
                    // The slot reserved by poll_ready goes back to the queue.
                    self.sender.abort_send();
                    return future;
                }
                Err(queued) => request = queued,
            }
        }
        let (reply, rx) = oneshot::channel();
        if self
            .sender
            .send_item(Message {
                source_worker: self.pool.worker,
                source_thread: std::thread::current().id(),
                request,
                reply,
            })
            .is_err()
        {
            return Box::pin(async { Err(unavailable()) });
        }
        Box::pin(async { rx.await.map_err(|_| unavailable())? })
    }
}
fn unavailable() -> Error {
    std::io::Error::new(
        std::io::ErrorKind::ConnectionRefused,
        "DMA endpoint unavailable",
    )
    .into()
}

// Per-transport counters: no atomics or per-request logging on the hot path.
// Thread mismatches are meaningful in sharded mode; logical ownership also
// applies when multiple workers share one runtime.
struct FlowAudit {
    address: SocketAddr,
    route: dmesh_doca::backend::Route,
    requests: u64,
    cross_worker: u64,
    cross_thread: u64,
    shared: Arc<parking_lot::Mutex<Shared>>,
}
impl FlowAudit {
    fn report(&self) {
        // Direct dispatches bypass the owner, which counts only what it queued.
        let direct = self.shared.lock().requests;
        tracing::info!(address = %self.address, route = ?self.route,
            requests = self.requests + direct, direct,
            cross_worker = self.cross_worker,
            cross_thread = self.cross_thread, "dmesh flow audit");
    }
}
impl Drop for FlowAudit {
    fn drop(&mut self) {
        self.report();
    }
}

async fn run(
    address: SocketAddr,
    route: dmesh_doca::backend::Route,
    make: impl Fn() -> svc::BoxHttp,
    shared: &Arc<parking_lot::Mutex<Shared>>,
    mut rx: mpsc::Receiver<Message>,
    first: Message,
) -> mpsc::Receiver<Message> {
    let mut audit = FlowAudit {
        address,
        route,
        requests: 0,
        cross_worker: 0,
        cross_thread: 0,
        shared: shared.clone(),
    };
    let mut first = Some(first);
    let live = dmesh_doca::backend::route_signal(address, route);
    {
        let mut shared = shared.lock();
        shared.live = live.clone();
        shared.requests = 0;
        // On a shared multi-thread runtime the owner has no thread of its own.
        shared.thread = match tokio::runtime::Handle::current().runtime_flavor() {
            tokio::runtime::RuntimeFlavor::CurrentThread => thread_token(),
            _ => 0,
        };
    }
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(100));
    let mut audit_tick = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        if !live.load(std::sync::atomic::Ordering::Acquire) {
            break;
        }
        let mut msg = if let Some(first) = first.take() {
            first
        } else {
            tokio::select! {
                _=audit_tick.tick()=> { audit.report(); continue; },
                _=tick.tick()=> {
                    if !dmesh_doca::backend::endpoint_enabled(address) { shared.lock().close(); }
                    continue;
                },
                m=rx.recv()=> match m {Some(m)=>m,None=>break},
            }
        };
        if !dmesh_doca::backend::endpoint_enabled(address)
            || !live.load(std::sync::atomic::Ordering::Acquire)
        {
            shared.lock().close();
            let _ = msg.reply.send(Err(unavailable()));
            continue;
        }
        {
            // The owner waits with its own waker from here: callers queue
            // behind this request until the transport reports ready.
            let mut shared = shared.lock();
            shared.direct = false;
            shared.service.get_or_insert_with(&make);
        }
        let poll_ready = std::future::poll_fn(|cx| match shared.lock().service.as_mut() {
            Some(svc) => svc.poll_ready(cx),
            None => Poll::Ready(Err(unavailable())),
        });
        // Original request timeouts drop the receiver even while waiting for H2.
        let ready = tokio::select! {
            _=msg.reply.closed()=>continue,
            ready=tokio::time::timeout(std::time::Duration::from_secs(3),poll_ready)=> {
                match ready { Ok(r)=>r, Err(_)=> { tracing::warn!(worker = route.worker, %address, "dmesh H2 readiness timed out"); Err(unavailable()) } }
            },
        };
        if let Err(e) = ready {
            let _ = msg.reply.send(Err(e));
            shared.lock().close();
            continue;
        }
        let future = {
            let mut shared = shared.lock();
            let future = shared.service.as_mut().map(|svc| svc.call(msg.request));
            shared.direct = future.is_some();
            future
        };
        match future {
            None => {
                let _ = msg.reply.send(Err(unavailable()));
            }
            Some(future) => {
                audit.requests += 1;
                audit.cross_worker += u64::from(msg.source_worker != route.worker);
                audit.cross_thread += u64::from(msg.source_thread != std::thread::current().id());
                if audit.requests % 100_000 == 0 {
                    audit.report();
                }
                tokio::spawn(async move {
                    tokio::select! { _=msg.reply.closed()=>{}, result=future=>{ let _=msg.reply.send(result); } }
                });
            }
        }
    }
    shared.lock().close();
    rx
}

struct RouteLease(SocketAddr, dmesh_doca::backend::Route);
impl Drop for RouteLease {
    fn drop(&mut self) {
        dmesh_doca::backend::release_route(self.0, self.1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use linkerd_app_core::svc::ServiceExt;
    use std::sync::atomic::AtomicUsize;
    #[derive(Clone)]
    struct Target(SocketAddr);
    impl Param<Remote<ServerAddr>> for Target {
        fn param(&self) -> Remote<ServerAddr> {
            Remote(ServerAddr(self.0))
        }
    }
    impl Param<http::client::Params> for Target {
        fn param(&self) -> http::client::Params {
            http::client::Params::H2(Default::default())
        }
    }
    impl Param<tls::ConditionalClientTls> for Target {
        fn param(&self) -> tls::ConditionalClientTls {
            tls::ConditionalClientTls::None(tls::NoClientTls::Disabled)
        }
    }

    #[tokio::test]
    async fn shared_owner_cancellation_and_bounded_admission() {
        let file =
            std::env::temp_dir().join(format!("dmesh-pool-test-{}.json", std::process::id()));
        std::fs::write(&file,r#"{"schema_version":2,"generation":1,"workers":1,"services":{"s":{"vip":"10.254.1.1","port":80,"discovery":"s.dmesh:80","endpoints":[{"id":"e","dma":"10.254.1.2:80","worker":0,"enabled":true}]}}}"#).unwrap();
        std::env::set_var("DMESH_ROUTES", &file);
        std::env::set_var("DMESH_SHARD_ROUTES", "1");
        dmesh_doca::backend::init_routes().unwrap();
        let (stop, stopped) = oneshot::channel::<()>();
        let (ready, started) = std::sync::mpsc::channel();
        let owner = std::thread::Builder::new()
            .name("pool-test-owner".into())
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async {
                        dmesh_doca::backend::register_worker(0);
                        ready.send(()).unwrap();
                        let _ = stopped.await;
                    });
            })
            .unwrap();
        started.recv().unwrap();
        let address = "10.254.1.2:80".parse().unwrap();
        let token = dmesh_doca::backend::new_owner();
        let (io, _handle) = dmesh_doca::dmesh_io_pair(address);
        dmesh_doca::backend::publish(token, 0, address, io);
        let creates = Arc::new(AtomicUsize::new(0));
        let created = creates.clone();
        let began = Arc::new(tokio::sync::Notify::new());
        let begin = began.clone();
        let cancelled = Arc::new(tokio::sync::Notify::new());
        let cancel = cancelled.clone();
        struct Guard(Arc<tokio::sync::Notify>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.notify_one();
            }
        }
        let inner = svc::ArcNewService::new(move |_: Target| {
            assert_eq!(std::thread::current().name(), Some("pool-test-owner"));
            created.fetch_add(1, Ordering::SeqCst);
            let begin = begin.clone();
            let cancel = cancel.clone();
            svc::BoxService::new(svc::mk(move |req: http::Request<http::BoxBody>| {
                assert_eq!(std::thread::current().name(), Some("pool-test-owner"));
                let begin = begin.clone();
                let cancel = cancel.clone();
                async move {
                    if req.uri().path() == "/wait" {
                        let _guard = Guard(cancel);
                        begin.notify_one();
                        std::future::pending::<()>().await;
                    }
                    Ok::<_, Error>(http::Response::new(http::BoxBody::default()))
                }
            }))
        });
        let a = NewPool::new(inner.clone());
        let b = NewPool::new(inner);
        let mut first = a.new_service(Target(address));
        let mut second = b.new_service(Target(address));
        first
            .ready()
            .await
            .unwrap()
            .call(http::Request::new(Default::default()))
            .await
            .unwrap();
        second
            .ready()
            .await
            .unwrap()
            .call(http::Request::new(Default::default()))
            .await
            .unwrap();
        assert_eq!(
            creates.load(Ordering::SeqCst),
            1,
            "two routing stacks share one transport"
        );
        // Another worker gets an independent pool for this same replica.
        // Publish only after the first request: poll_ready must admit it even
        // though no backend exists, otherwise lazy creation deadlocks.
        dmesh_doca::backend::register_worker(1);
        let worker1_tid = std::thread::current().id();
        let worker1_creates = Arc::new(AtomicUsize::new(0));
        let count = worker1_creates.clone();
        let inner1 = svc::ArcNewService::new(move |_: Target| {
            assert_eq!(std::thread::current().id(), worker1_tid);
            count.fetch_add(1, Ordering::SeqCst);
            svc::BoxService::new(svc::mk(|_: http::Request<http::BoxBody>| async {
                Ok::<_, Error>(http::Response::new(http::BoxBody::default()))
            }))
        });
        let c = dmesh_doca::backend::on_worker(1, async { NewPool::new(inner1) }).await;
        let mut third = c.new_service(Target(address));
        let mut fourth = c.new_service(Target(address));
        assert_eq!(worker1_creates.load(Ordering::SeqCst), 0);
        let token1 = dmesh_doca::backend::new_owner();
        let publish = tokio::spawn(async move {
            // Regression: a burst of worker/replica setup can exceed five seconds.
            tokio::time::sleep(std::time::Duration::from_secs(6)).await;
            let (io, handle) = dmesh_doca::dmesh_io_pair(address);
            dmesh_doca::backend::publish_on_worker(token1, 0, 1, address, io);
            handle
        });
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            third
                .ready()
                .await
                .unwrap()
                .call(http::Request::new(Default::default()))
                .await
                .unwrap();
            fourth
                .ready()
                .await
                .unwrap()
                .call(http::Request::new(Default::default()))
                .await
                .unwrap();
        })
        .await
        .unwrap();
        let _handle1 = publish.await.unwrap();
        assert_eq!(worker1_creates.load(Ordering::SeqCst), 1);
        // Worker 1's owner shares this thread, so once its transport is up a
        // caller dispatches directly; worker 0's owner has its own thread, and
        // its callers here always go through the queue.
        let direct = |worker| {
            let pools = directory().lock().unwrap();
            let (_, pool) = pools.iter().find(|(key, _)| key.worker == worker).unwrap();
            let pool = pool.upgrade().unwrap();
            let requests = pool.shared.lock().requests;
            requests
        };
        assert_eq!(direct(1), 1);
        assert_eq!(direct(0), 0);
        assert_eq!(
            creates.load(Ordering::SeqCst),
            1,
            "worker 0's pool is unchanged"
        );
        dmesh_doca::backend::unpublish(token1, 0, &address);
        drop(third);
        drop(fourth);
        let f = first.ready().await.unwrap().call(
            http::Request::builder()
                .uri("/wait")
                .body(Default::default())
                .unwrap(),
        );
        let call = tokio::spawn(f);
        began.notified().await;
        call.abort();
        tokio::time::timeout(std::time::Duration::from_secs(1), cancelled.notified())
            .await
            .unwrap();
        let (tx, _rx) = mpsc::channel(1);
        let pool = Arc::new(Pool {
            worker: 0,
            address,
            sender: tx,
            shared: Shared::new(),
        });
        let mut h1 = Handle {
            sender: PollSender::new(pool.sender.clone()),
            pool: pool.clone(),
        };
        let mut h2 = Handle {
            sender: PollSender::new(pool.sender.clone()),
            pool,
        };
        h1.ready().await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), h2.ready())
                .await
                .is_err()
        );
        drop(h1);
        h2.ready().await.unwrap();
        dmesh_doca::backend::unpublish(token, 0, &address);
        directory().lock().unwrap().clear();
        let _ = stop.send(());
        owner.join().unwrap();
        std::fs::remove_file(file).unwrap();
    }
}

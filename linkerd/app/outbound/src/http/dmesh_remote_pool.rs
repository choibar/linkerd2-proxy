//! Remote H2 pools are keyed by verified source registration, as well as the
//! worker and endpoint. Two Pods must never share an authenticated pair lane.
use super::dmesh_origin::Scoped;
use linkerd_app_core::{
    proxy::http,
    svc::{self, NewService, Param, Service, ServiceExt},
    tls,
    transport::{Remote, ServerAddr},
    Error,
};
use std::{
    collections::HashMap,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex, OnceLock, Weak},
    task::{Context, Poll},
};
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Hash, PartialEq, Eq)]
struct Key {
    worker: usize,
    address: SocketAddr,
    source: Option<Arc<dmesh_doca::PeerOrigin>>,
    params: http::h2::ClientParams,
    tls: tls::ConditionalClientTls,
}
struct Message {
    request: http::Request<http::BoxBody>,
    reply: oneshot::Sender<Result<http::Response<http::BoxBody>, Error>>,
}
struct Pool {
    sender: mpsc::Sender<Message>,
}
fn directory() -> &'static Mutex<HashMap<Key, Weak<Pool>>> {
    static D: OnceLock<Mutex<HashMap<Key, Weak<Pool>>>> = OnceLock::new();
    D.get_or_init(Default::default)
}
fn unavailable() -> Error {
    std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "remote RDMA H2 endpoint requires a live source registration",
    )
    .into()
}

pub(super) fn new<T>(inner: svc::ArcNewHttp<T>, target: T, worker: usize) -> svc::BoxHttp
where
    T: Param<Remote<ServerAddr>>
        + Param<http::client::Params>
        + Param<tls::ConditionalClientTls>
        + Clone
        + Send
        + Sync
        + 'static,
{
    dispatch(inner, target, worker, true)
}

/// An endpoint may be constructed before its signed Pod topology arrives.
/// Keep verified origins isolated even while the connector still sees TCP,
/// so its later reconnect can select RDMA with the original source identity.
pub(super) fn unclassified<T>(inner: svc::ArcNewHttp<T>, target: T, worker: usize) -> svc::BoxHttp
where
    T: Param<Remote<ServerAddr>>
        + Param<http::client::Params>
        + Param<tls::ConditionalClientTls>
        + Clone
        + Send
        + Sync
        + 'static,
{
    dispatch(inner, target, worker, false)
}

fn dispatch<T>(
    inner: svc::ArcNewHttp<T>,
    target: T,
    worker: usize,
    require_origin: bool,
) -> svc::BoxHttp
where
    T: Param<Remote<ServerAddr>>
        + Param<http::client::Params>
        + Param<tls::ConditionalClientTls>
        + Clone
        + Send
        + Sync
        + 'static,
{
    svc::BoxService::new(Dispatch {
        inner,
        target,
        worker,
        require_origin,
        held: HashMap::new(),
    })
}
struct Dispatch<T> {
    inner: svc::ArcNewHttp<T>,
    target: T,
    worker: usize,
    require_origin: bool,
    held: HashMap<Option<Arc<dmesh_doca::PeerOrigin>>, Arc<Pool>>,
}
impl<T> Dispatch<T>
where
    T: Param<Remote<ServerAddr>>
        + Param<http::client::Params>
        + Param<tls::ConditionalClientTls>
        + Clone
        + Send
        + Sync
        + 'static,
{
    fn pool(&mut self, origin: &Option<Arc<dmesh_doca::PeerOrigin>>) -> Result<Arc<Pool>, Error> {
        if let Some(pool) = self.held.get(origin) {
            return Ok(pool.clone());
        }
        let Remote(ServerAddr(address)) = self.target.param();
        let http::client::Params::H2(params) = self.target.param() else {
            return Err(unavailable());
        };
        let key = Key {
            worker: self.worker,
            address,
            source: origin.clone(),
            params,
            tls: self.target.param(),
        };
        let mut directory = directory().lock().unwrap();
        directory.retain(|_, pool| pool.strong_count() != 0);
        let pool = if let Some(pool) = directory.get(&key).and_then(Weak::upgrade) {
            pool
        } else {
            let runtime =
                dmesh_doca::backend::worker_runtime(self.worker).ok_or_else(unavailable)?;
            let (sender, mut receiver) = mpsc::channel::<Message>(128);
            let inner = self.inner.clone();
            let target = self.target.clone();
            // Copy identity strings only when a transport owner is created.
            // Requests reuse the connection's Arc, without new allocations.
            let origin = origin.as_deref().cloned();
            runtime.spawn(dmesh_doca::origin::scope(origin, async move {
                let mut service: Option<Scoped<svc::BoxHttp>> = None;
                while let Some(mut message) = receiver.recv().await {
                    if message.reply.is_closed() { continue; }
                    let svc = service.get_or_insert_with(|| Scoped::new(inner.new_service(target.clone())));
                    let ready = tokio::select! {
                        _ = message.reply.closed() => continue,
                        ready = tokio::time::timeout(std::time::Duration::from_secs(30), svc.ready()) => ready,
                    };
                    match ready {
                        Ok(Ok(svc)) => {
                            let future = svc.call(message.request);
                            tokio::spawn(async move {
                                tokio::select! {
                                    _ = message.reply.closed() => {},
                                    result = future => { let _ = message.reply.send(result); },
                                }
                            });
                        }
                        _ => { let _ = message.reply.send(Err(unavailable())); service = None; }
                    }
                }
            }));
            let pool = Arc::new(Pool { sender });
            directory.insert(key, Arc::downgrade(&pool));
            pool
        };
        // Bound old source generations retained by a long-lived routing stack.
        // In-flight request futures keep their own Arc when an entry is evicted.
        if self.held.len() >= 64 {
            if let Some(key) = self.held.keys().next().cloned() {
                self.held.remove(&key);
            }
        }
        self.held.insert(origin.clone(), pool.clone());
        Ok(pool)
    }
}
impl<T> Service<http::Request<http::BoxBody>> for Dispatch<T>
where
    T: Param<Remote<ServerAddr>>
        + Param<http::client::Params>
        + Param<tls::ConditionalClientTls>
        + Clone
        + Send
        + Sync
        + 'static,
{
    type Response = http::Response<http::BoxBody>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Error>> + Send>>;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<http::BoxBody>) -> Self::Future {
        let origin = request
            .extensions()
            .get::<Arc<dmesh_doca::PeerOrigin>>()
            .cloned();
        if self.require_origin && origin.is_none() {
            return Box::pin(async { Err(unavailable()) });
        }
        let pool = match self.pool(&origin) {
            Ok(pool) => pool,
            Err(e) => return Box::pin(async { Err(e) }),
        };
        Box::pin(async move {
            let (reply, response) = oneshot::channel();
            pool.sender
                .send(Message { request, reply })
                .await
                .map_err(|_| unavailable())?;
            response.await.map_err(|_| unavailable())?
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Clone)]
    struct Target;
    impl Param<Remote<ServerAddr>> for Target {
        fn param(&self) -> Remote<ServerAddr> {
            Remote(ServerAddr("10.20.30.40:50051".parse().unwrap()))
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
    fn origin(id: u64, generation: u64) -> dmesh_doca::PeerOrigin {
        dmesh_doca::PeerOrigin {
            session_id: id,
            session_epoch: 1,
            generation,
            channel_slot: id as u32,
            pod_uid: format!("pod-{id}"),
            namespace: "test".into(),
            service_account: "default".into(),
            service: "client".into(),
        }
    }
    struct RetryProbe(bool);
    impl Service<http::Request<http::BoxBody>> for RetryProbe {
        type Response = http::Response<http::BoxBody>;
        type Error = Error;
        type Future = std::future::Ready<Result<Self::Response, Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
            Poll::Ready(if self.0 { Err(unavailable()) } else { Ok(()) })
        }

        fn call(&mut self, _: http::Request<http::BoxBody>) -> Self::Future {
            std::future::ready(Ok(http::Response::new(Default::default())))
        }
    }
    #[tokio::test]
    async fn different_pods_and_recreated_registrations_never_share_a_transport() {
        const WORKER: usize = 81;
        dmesh_doca::backend::register_worker(WORKER);
        let created = Arc::new(AtomicUsize::new(0));
        let count = created.clone();
        let inner = svc::ArcNewService::new(move |_: Target| {
            let source = dmesh_doca::origin::current().expect("source reaches spawned H2 owner");
            count.fetch_add(1, Ordering::SeqCst);
            svc::BoxService::new(svc::mk(move |request: http::Request<http::BoxBody>| {
                let expected = source.clone();
                async move {
                    assert_eq!(
                        request
                            .extensions()
                            .get::<Arc<dmesh_doca::PeerOrigin>>()
                            .unwrap()
                            .as_ref(),
                        &expected
                    );
                    assert_eq!(dmesh_doca::origin::current().as_ref(), Some(&expected));
                    Ok::<_, Error>(http::Response::new(Default::default()))
                }
            }))
        });
        let mut dispatch = new(inner, Target, WORKER);
        assert!(dispatch
            .ready()
            .await
            .unwrap()
            .call(http::Request::new(Default::default()))
            .await
            .is_err());
        assert_eq!(created.load(Ordering::SeqCst), 0);
        for source in [
            origin(1, 1),
            origin(2, 1),
            origin(1, 1),
            origin(1, 2),
            origin(2, 1),
        ] {
            let mut request = http::Request::new(Default::default());
            request.extensions_mut().insert(Arc::new(source));
            dispatch.ready().await.unwrap().call(request).await.unwrap();
        }
        assert_eq!(created.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn topology_arriving_after_stack_creation_keeps_origin_on_retry() {
        const WORKER: usize = 82;
        dmesh_doca::backend::register_worker(WORKER);
        let attempts = Arc::new(AtomicUsize::new(0));
        let count = attempts.clone();
        let source = origin(3, 7);
        let expected = source.clone();
        let inner = svc::ArcNewService::new(move |_: Target| {
            assert_eq!(dmesh_doca::origin::current().as_ref(), Some(&expected));
            let attempt = count.fetch_add(1, Ordering::SeqCst);
            // Before topology publication, dialing the endpoint fails. On
            // retry it is an RDMA endpoint and requires the captured identity.
            svc::BoxService::new(RetryProbe(attempt == 0))
        });
        assert!(dmesh_doca::origin::current().is_none());
        let mut dispatch = unclassified(inner, Target, WORKER);
        for attempt in 0..2 {
            let mut request = http::Request::new(Default::default());
            request.extensions_mut().insert(Arc::new(source.clone()));
            let result = dispatch.ready().await.unwrap().call(request).await;
            assert_eq!(result.is_ok(), attempt == 1);
        }
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn unclassified_tcp_and_verified_pod_do_not_share_a_transport() {
        const WORKER: usize = 83;
        dmesh_doca::backend::register_worker(WORKER);
        let created = Arc::new(AtomicUsize::new(0));
        let count = created.clone();
        let inner = svc::ArcNewService::new(move |_: Target| {
            let captured = dmesh_doca::origin::current();
            count.fetch_add(1, Ordering::SeqCst);
            svc::BoxService::new(svc::mk(move |request: http::Request<http::BoxBody>| {
                let expected = captured.clone();
                async move {
                    let source = request.extensions().get::<Arc<dmesh_doca::PeerOrigin>>();
                    assert_eq!(source.map(Arc::as_ref), expected.as_ref());
                    assert_eq!(dmesh_doca::origin::current(), expected);
                    Ok::<_, Error>(http::Response::new(Default::default()))
                }
            }))
        });
        let mut dispatch = unclassified(inner, Target, WORKER);
        for source in [None, Some(origin(4, 1)), None, Some(origin(4, 1))] {
            let mut request = http::Request::new(Default::default());
            if let Some(source) = source {
                request.extensions_mut().insert(Arc::new(source));
            }
            dispatch.ready().await.unwrap().call(request).await.unwrap();
        }
        assert_eq!(created.load(Ordering::SeqCst), 2);
    }
}

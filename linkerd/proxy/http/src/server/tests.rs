use super::*;
use bytes::Bytes;
use futures::FutureExt;
use http_body_util::BodyExt;
use hyper_util::rt::tokio::TokioExecutor;
use linkerd_io as io;
use linkerd_stack::CloneParam;
use std::vec;
use tokio::time;
use tower::ServiceExt;
use tower_test::mock;
use tracing::info_span;

thread_local! {
    /// Per-thread override of the `DMESH_INLINE_STREAMS` gate for tests
    /// (the server is built on the test thread, where this is read).
    pub(super) static INLINE_STREAMS_OVERRIDE: std::cell::Cell<Option<bool>> =
        const { std::cell::Cell::new(None) };
}

fn set_inline_streams(on: bool) {
    INLINE_STREAMS_OVERRIDE.with(|c| c.set(Some(on)));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn h2_connection_window_exhaustion() {
    set_inline_streams(false);
    h2_connection_window_exhaustion_impl().await
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn h2_connection_window_exhaustion_inline() {
    set_inline_streams(true);
    h2_connection_window_exhaustion_impl().await
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn h2_stream_window_exhaustion() {
    set_inline_streams(false);
    h2_stream_window_exhaustion_impl().await
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn h2_stream_window_exhaustion_inline() {
    set_inline_streams(true);
    h2_stream_window_exhaustion_impl().await
}

/// Tests how the server behaves when the client connection window is exhausted.
async fn h2_connection_window_exhaustion_impl() {
    let _trace = linkerd_tracing::test::with_default_filter(LOG_LEVEL);

    // Setup a HTTP/2 server with consumers and producers that are mocked for
    // tests.
    const CONCURRENCY: u32 = 3;
    const CLIENT_STREAM_WINDOW: u32 = 65535;
    const CLIENT_CONN_WINDOW: u32 = CONCURRENCY * CLIENT_STREAM_WINDOW;

    tracing::info!("Connecting to server");
    let mut server = TestServer::connect_h2(
        // A basic HTTP/2 server configuration with no overrides.
        h2::ServerParams::default(),
        // An HTTP/2 client with constrained connection and stream windows to
        // force window exhaustion.
        hyper::client::conn::http2::Builder::new(TokioExecutor::new())
            .initial_connection_window_size(CLIENT_CONN_WINDOW)
            .initial_stream_window_size(CLIENT_STREAM_WINDOW)
            .timer(hyper_util::rt::TokioTimer::new()),
    )
    .await;

    // Mocked response data to fill up the stream and connection windows.
    let bytes = (0..CLIENT_STREAM_WINDOW).map(|_| b'a').collect::<Bytes>();

    // Response bodies held to exhaust connection window.
    let mut retain = vec![];

    tracing::info!(
        streams = CONCURRENCY - 1,
        data = bytes.len(),
        "Consuming connection window"
    );
    for _ in 0..CONCURRENCY - 1 {
        let rx = timeout(server.respond(bytes.clone()))
            .await
            .expect("timed out");
        retain.push(rx);
    }

    tracing::info!("Processing a stream with available connection window");
    let rx = timeout(server.respond(bytes.clone()))
        .await
        .expect("timed out");
    let body = timeout(rx.collect().instrument(info_span!("collect")))
        .await
        .expect("response timed out")
        .expect("response");
    assert_eq!(body.to_bytes(), bytes);

    tracing::info!("Consuming the remaining connection window");
    let rx = timeout(server.respond(bytes.clone()))
        .await
        .expect("timed out");
    retain.push(rx);

    tracing::info!("The connection window is exhausted");

    tracing::info!("Trying to process an additional stream. The response headers are received but no data is received.");
    let mut rx = timeout(server.respond(bytes.clone()))
        .await
        .expect("timed out");
    tokio::select! {
        _ = time::sleep(time::Duration::from_secs(2)) => {}
        _ = rx.frame() => panic!("unexpected data"),
    }

    tracing::info!("Dropping one of the retained response bodies frees capacity so that the data can be received");
    drop(retain.pop());
    let body = timeout(rx.collect().instrument(info_span!("collect")))
        .await
        .expect("response timed out")
        .expect("response");
    assert_eq!(body.to_bytes(), bytes);
}

/// Tests how the server behaves when the client stream window is exhausted.
async fn h2_stream_window_exhaustion_impl() {
    let _trace = linkerd_tracing::test::with_default_filter(LOG_LEVEL);

    // Setup a HTTP/2 server with consumers and producers that are mocked for
    // tests.
    const CLIENT_STREAM_WINDOW: u32 = 1024;

    let mut server = TestServer::connect_h2(
        // A basic HTTP/2 server configuration with no overrides.
        h2::ServerParams::default(),
        // An HTTP/2 client with stream windows to force window exhaustion.
        hyper::client::conn::http2::Builder::new(TokioExecutor::new())
            .initial_stream_window_size(CLIENT_STREAM_WINDOW)
            .timer(hyper_util::rt::TokioTimer::new()),
    )
    .await;

    let (mut tx, mut body) = timeout(server.get()).await.expect("timed out");

    let chunk = (0..CLIENT_STREAM_WINDOW).map(|_| b'a').collect::<Bytes>();
    tracing::info!(sz = chunk.len(), "Sending chunk");
    tx.send_data(chunk.clone()).await.expect("can send data");
    tokio::task::yield_now().await;

    tracing::info!(sz = chunk.len(), "Buffering chunk in channel");
    tx.send_data(chunk.clone()).await.expect("can send data");
    tokio::task::yield_now().await;

    tracing::info!(sz = chunk.len(), "Confirming stream window exhaustion");
    /*
     * XXX(kate): this can be reinstate when we have a `poll_ready(cx)` method on the new sender.
    assert!(
        timeout(futures::future::poll_fn(|cx| tx.poll_ready(cx)))
            .await
            .is_err(),
        "stream window should be exhausted"
    );
    */

    tracing::info!("Once the pending data is read, the stream window should be replenished");
    let data = body
        .frame()
        .await
        .expect("yields a result")
        .expect("yields a frame")
        .into_data()
        .expect("yields data");
    assert_eq!(data, chunk);
    let data = body
        .frame()
        .await
        .expect("yields a result")
        .expect("yields a frame")
        .into_data()
        .expect("yields data");
    assert_eq!(data, chunk);

    timeout(body.frame()).await.expect_err("no more chunks");

    tracing::info!(sz = chunk.len(), "Confirming stream window availability");
    /*
     * XXX(kate): this can be reinstated when we have a `poll_ready(cx)` method on the new sender.
    timeout(futures::future::poll_fn(|cx| tx.poll_ready(cx)))
        .await
        .expect("timed out")
        .expect("ready");
    */
}

// === Utilities ===

const LOG_LEVEL: &str = "h2::proto=trace,hyper=trace,linkerd=trace,info";

struct TestServer {
    client: hyper::client::conn::http2::SendRequest<BoxBody>,
    server: Handle,
}

type Mock = mock::Mock<http::Request<BoxBody>, http::Response<BoxBody>>;
type Handle = mock::Handle<http::Request<BoxBody>, http::Response<BoxBody>>;

/// Allows us to configure a server from the Params type.
#[derive(Clone, Debug)]
struct NewMock(mock::Mock<http::Request<BoxBody>, http::Response<BoxBody>>);

impl NewService<()> for NewMock {
    type Service = NewMock;
    fn new_service(&self, _: ()) -> Self::Service {
        self.clone()
    }
}

impl NewService<ClientHandle> for NewMock {
    type Service = Mock;
    fn new_service(&self, _: ClientHandle) -> Self::Service {
        self.0.clone()
    }
}

fn drain() -> drain::Watch {
    let (mut sig, drain) = drain::channel();
    tokio::spawn(async move {
        sig.closed().await;
    });
    drain
}

async fn timeout<F: Future>(inner: F) -> Result<F::Output, time::error::Elapsed> {
    time::timeout(time::Duration::from_secs(2), inner).await
}

/// Spawns an h2 server built from `new_svc` on one end of a duplex socket and
/// returns the client end.
fn spawn_h2_server<N>(h2: h2::ServerParams, new_svc: N) -> linkerd_io::DuplexStream
where
    N: NewService<()> + Clone,
    N::Service: NewService<ClientHandle> + Send + 'static,
    ServeHttp<N::Service>: Service<linkerd_io::DuplexStream, Response = (), Error = Error>,
    <ServeHttp<N::Service> as Service<linkerd_io::DuplexStream>>::Future: Send + 'static,
{
    let params = Params {
        drain: drain(),
        version: Variant::H2,
        http2: h2,
    };
    let (sio, cio) = io::duplex(20 * 1024 * 1024); // 20 MB
    let svc = NewServeHttp::new(CloneParam::from(params), new_svc).new_service(());
    let fut = svc.oneshot(sio).instrument(info_span!("server"));
    tokio::spawn(fut);
    cio
}

impl TestServer {
    #[tracing::instrument(skip_all)]
    async fn connect_h2(
        h2: h2::ServerParams,
        client: &mut hyper::client::conn::http2::Builder<TokioExecutor>,
    ) -> Self {
        // Build the HTTP server with a mocked inner service so that we can handle
        // requests.
        let (mock, server) = mock::pair::<http::Request<BoxBody>, http::Response<BoxBody>>();
        let cio = spawn_h2_server(h2, NewMock(mock));

        // Build a real HTTP/2 client using the mocked socket.
        let (client, task) = client
            .handshake::<_, BoxBody>(hyper_util::rt::tokio::TokioIo::new(cio))
            .await
            .expect("client connect");
        tokio::spawn(task.instrument(info_span!("client")));

        Self { client, server }
    }

    /// Issues a request through the client to the mocked server and processes the
    /// response. The mocked response body sender and the readable response body are
    /// returned.
    #[tracing::instrument(skip(self))]
    async fn get(
        &mut self,
    ) -> (
        http_body_util::channel::Sender<Bytes>,
        hyper::body::Incoming,
    ) {
        self.server.allow(1);
        let mut call0 = self
            .client
            .send_request(http::Request::new(BoxBody::default()))
            .boxed();
        let (_req, next) = tokio::select! {
            _ = (&mut call0) => unreachable!("client cannot receive a response"),
            next = self.server.next_request() => next.expect("server not dropped"),
        };
        let (tx, rx) = http_body_util::channel::Channel::new(512);
        next.send_response(http::Response::new(BoxBody::new(rx)));
        let rsp = call0.await.expect("response");
        (tx, rsp.into_body())
    }

    #[tracing::instrument(skip(self))]
    async fn respond(&mut self, body: Bytes) -> hyper::body::Incoming {
        let (mut tx, rx) = self.get().await;
        tx.send_data(body.clone()).await.expect("send data");
        rx
    }
}

/// `CallInPlace` dispatches from the hyper connection task's poll. That poll
/// has a tokio coop budget of 128; budget-aware readiness (bounded channels,
/// semaphores) returns a spurious `Pending` past it, which `LoadShed` would
/// turn into shed requests. The dispatch must therefore be unconstrained: 300
/// in-place calls from a single poll must all reach the inner service.
#[tokio::test(flavor = "current_thread")]
async fn call_in_place_is_not_coop_budget_limited() {
    use linkerd_stack::LoadShed;
    use std::sync::Arc;
    use tokio::sync::{OwnedSemaphorePermit, Semaphore};

    /// Readiness acquires (and keeps) a semaphore permit: tokio's `Acquire`
    /// future consumes coop budget on every poll, like `Buffer::poll_ready`.
    struct BudgetedReady {
        sem: Arc<Semaphore>,
        acquire: Option<Pin<Box<dyn Future<Output = OwnedSemaphorePermit> + Send>>>,
        held: Arc<std::sync::Mutex<Vec<OwnedSemaphorePermit>>>,
    }

    impl Clone for BudgetedReady {
        fn clone(&self) -> Self {
            Self {
                sem: self.sem.clone(),
                acquire: None,
                held: self.held.clone(),
            }
        }
    }

    impl Service<()> for BudgetedReady {
        type Response = ();
        type Error = Error;
        type Future = futures::future::Ready<Result<(), Error>>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
            let fut = self.acquire.get_or_insert_with(|| {
                let sem = self.sem.clone();
                Box::pin(async move { sem.acquire_owned().await.expect("open") })
            });
            let permit = futures::ready!(fut.as_mut().poll(cx));
            self.acquire = None;
            self.held.lock().unwrap().push(permit);
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, (): ()) -> Self::Future {
            futures::future::ok(())
        }
    }

    const N: usize = 300;
    let inner = BudgetedReady {
        sem: Arc::new(Semaphore::new(N * 2)),
        acquire: None,
        held: Default::default(),
    };
    let svc = CallInPlace::new(LoadShed::new(inner));

    // All N dispatches happen within ONE poll of this task (like hyper's
    // `poll_server` loop), so they share this task's coop budget.
    let futs = std::future::poll_fn(|_cx| {
        Poll::Ready(
            (0..N)
                .map(|_| hyper::service::Service::call(&svc, ()))
                .collect::<Vec<_>>(),
        )
    })
    .await;

    for (i, fut) in futs.into_iter().enumerate() {
        fut.await
            .unwrap_or_else(|e| panic!("request {i} was shed by the coop budget: {e}"));
    }
}

// === Inline-streams tests ===

/// Readiness that consumes tokio coop budget (a semaphore acquire, like
/// `Buffer::poll_ready`) before delegating to the inner service.
struct BudgetedReady<S> {
    inner: S,
    sem: std::sync::Arc<tokio::sync::Semaphore>,
    acquire: Option<Pin<Box<dyn Future<Output = tokio::sync::OwnedSemaphorePermit> + Send>>>,
}

impl<S: Clone> Clone for BudgetedReady<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            sem: self.sem.clone(),
            acquire: None,
        }
    }
}

impl<S, Req> Service<Req> for BudgetedReady<S>
where
    S: Service<Req>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        let fut = self.acquire.get_or_insert_with(|| {
            let sem = self.sem.clone();
            Box::pin(async move { sem.acquire_owned().await.expect("open") })
        });
        let permit = futures::ready!(fut.as_mut().poll(cx));
        self.acquire = None;
        permit.forget();
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        self.inner.call(req)
    }
}

/// Builds `LoadShed<BudgetedReady<Mock>>` services: the shape that turns a
/// coop-budget `Pending` into a shed request.
#[derive(Clone)]
struct NewBudgeted(Mock);

impl NewService<()> for NewBudgeted {
    type Service = Self;
    fn new_service(&self, _: ()) -> Self {
        self.clone()
    }
}

impl NewService<ClientHandle> for NewBudgeted {
    type Service = linkerd_stack::LoadShed<BudgetedReady<Mock>>;
    fn new_service(&self, _: ClientHandle) -> Self::Service {
        linkerd_stack::LoadShed::new(BudgetedReady {
            inner: self.0.clone(),
            sem: std::sync::Arc::new(tokio::sync::Semaphore::new(1 << 20)),
            acquire: None,
        })
    }
}

/// 300 concurrent streams on one connection (h2load `-m300`), all must
/// complete with 200 — no coop-budget shedding, in either dispatch mode.
async fn many_concurrent_streams(inline: bool) {
    set_inline_streams(inline);
    let _trace = linkerd_tracing::test::with_default_filter("info");
    const N: usize = 300;

    let (mock, mut handle) = mock::pair::<http::Request<BoxBody>, http::Response<BoxBody>>();
    let cio = spawn_h2_server(h2::ServerParams::default(), NewBudgeted(mock));
    let (mut client, task) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .timer(hyper_util::rt::TokioTimer::new())
        .handshake::<_, BoxBody>(hyper_util::rt::tokio::TokioIo::new(cio))
        .await
        .expect("client connect");
    tokio::spawn(task);

    handle.allow(N as u64);
    let responder = tokio::spawn(async move {
        for _ in 0..N {
            let (_req, rsp) = handle.next_request().await.expect("request");
            rsp.send_response(http::Response::new(BoxBody::default()));
        }
    });

    let futs = (0..N)
        .map(|_| client.send_request(http::Request::new(BoxBody::default())))
        .collect::<Vec<_>>();
    let rsps = time::timeout(time::Duration::from_secs(10), futures::future::join_all(futs))
        .await
        .expect("timed out");
    for (i, rsp) in rsps.into_iter().enumerate() {
        let rsp = rsp.unwrap_or_else(|e| panic!("stream {i} failed: {e}"));
        assert_eq!(rsp.status(), http::StatusCode::OK, "stream {i} not OK");
    }
    responder.await.expect("responder");
}

#[tokio::test(flavor = "current_thread")]
async fn many_concurrent_streams_spawned() {
    many_concurrent_streams(false).await
}

#[tokio::test(flavor = "current_thread")]
async fn many_concurrent_streams_inline() {
    many_concurrent_streams(true).await
}

/// A service whose response future never completes and reports its drop.
#[derive(Clone)]
struct NewPendingGuarded {
    called: std::sync::Arc<tokio::sync::Notify>,
    dropped: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl NewService<()> for NewPendingGuarded {
    type Service = Self;
    fn new_service(&self, _: ()) -> Self {
        self.clone()
    }
}

impl NewService<ClientHandle> for NewPendingGuarded {
    type Service = Self;
    fn new_service(&self, _: ClientHandle) -> Self {
        self.clone()
    }
}

struct DropGuard(std::sync::Arc<std::sync::atomic::AtomicUsize>);
impl Drop for DropGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Service<http::Request<BoxBody>> for NewPendingGuarded {
    type Response = http::Response<BoxBody>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: http::Request<BoxBody>) -> Self::Future {
        let guard = DropGuard(self.dropped.clone());
        self.called.notify_one();
        Box::pin(async move {
            let _guard = guard;
            futures::future::pending::<()>().await;
            unreachable!()
        })
    }
}

/// A client RST_STREAM mid-request must drop the stream's service future
/// (cancellation), in either dispatch mode.
async fn rst_mid_request_drops_stream(inline: bool) {
    set_inline_streams(inline);
    let _trace = linkerd_tracing::test::with_default_filter("info");
    let called = std::sync::Arc::new(tokio::sync::Notify::new());
    let dropped = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let cio = spawn_h2_server(
        h2::ServerParams::default(),
        NewPendingGuarded {
            called: called.clone(),
            dropped: dropped.clone(),
        },
    );

    // Raw h2 client so the RST_STREAM is under our control.
    let (mut client, conn) = ::h2::client::handshake(cio).await.expect("handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let req = http::Request::builder()
        .method("GET")
        .uri("http://test/")
        .body(())
        .unwrap();
    let (rsp, stream) = client.send_request(req, true).expect("send_request");

    // The service has been called and its future is pending.
    time::timeout(time::Duration::from_secs(2), called.notified())
        .await
        .expect("service not called");
    assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 0);

    // Dropping the client's stream handles sends RST_STREAM(CANCEL).
    drop(rsp);
    drop(stream);

    time::timeout(time::Duration::from_secs(2), async {
        while dropped.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            time::sleep(time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("service future was not dropped after RST_STREAM");
}

#[tokio::test(flavor = "current_thread")]
async fn rst_mid_request_drops_stream_spawned() {
    rst_mid_request_drops_stream(false).await
}

#[tokio::test(flavor = "current_thread")]
async fn rst_mid_request_drops_stream_inline() {
    rst_mid_request_drops_stream(true).await
}

//! Per-connection request statistics for the DPU's flow placement.
//!
//! An acceptor runs a connection inside [`scope`] with its counters and wraps
//! the connection task in [`Cpu`]. The HTTP server then counts each request
//! as its response ends, with its residence (request received to response
//! end); the HTTP/2 server's stream tasks add the time they are polled
//! ([`Executor`]), so a connection's CPU covers its task and its streams; and
//! the client that forwards a request adds its backend wait (request sent to
//! response headers). With an instruction reader set, the same polls also add
//! the thread's retired instructions. Shared work, such as a backend
//! connection serving many client connections, is not attributed.
//! `DMESH_FLOW_STATS=0` turns counting off.

use crate::BoxBody;
use linkerd_error::Error;
use linkerd_stack::Service;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, OnceLock,
    },
    task::{Context, Poll},
    time::Instant,
};

/// Cumulative: requests, CPU ns, residence ns, backend ns, instructions.
pub type Counters = [AtomicU64; 5];
pub const REQUESTS: usize = 0;
pub const CPU_NS: usize = 1;
pub const RESIDENCE_NS: usize = 2;
pub const BACKEND_NS: usize = 3;
pub const INSTRUCTIONS: usize = 4;

/// Reads the polling thread's retired instructions; set once (see
/// [`set_instruction_reader`]) to count instructions alongside poll time.
static INSTRUCTION_READER: OnceLock<fn() -> u64> = OnceLock::new();

/// Counts each connection's retired instructions as its tasks are polled,
/// with `read` returning the calling thread's instruction count.
pub fn set_instruction_reader(read: fn() -> u64) {
    let _ = INSTRUCTION_READER.set(read);
}

tokio::task_local! {
    static COUNTERS: Arc<Counters>;
}

/// Whether acceptors should count (`DMESH_FLOW_STATS`, on unless "0").
pub fn enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("DMESH_FLOW_STATS").map_or(true, |v| v != "0"))
}

/// Runs `fut`, a connection's future, with `counters` as its statistics.
pub fn scope<F: Future>(counters: Arc<Counters>, fut: F) -> impl Future<Output = F::Output> {
    COUNTERS.scope(counters, fut)
}

pub(crate) fn current() -> Option<Arc<Counters>> {
    COUNTERS.try_with(Clone::clone).ok()
}

fn add(counters: &Counters, i: usize, v: u64) {
    counters[i].fetch_add(v, Ordering::Relaxed);
}

fn since(t: Instant) -> u64 {
    t.elapsed().as_nanos() as u64
}

/// Request extension: the serving connection's counters, for the client that
/// forwards the request.
#[derive(Clone)]
pub(crate) struct FlowCounters(Arc<Counters>);

impl FlowCounters {
    /// Adds the backend wait that began at `start`.
    pub(crate) fn backend(&self, start: Instant) {
        add(&self.0, BACKEND_NS, since(start));
    }
}

/// A future whose poll time is added to its connection's CPU.
#[pin_project::pin_project]
pub struct Cpu<F> {
    #[pin]
    inner: F,
    counters: Arc<Counters>,
}

impl<F> Cpu<F> {
    pub fn new(inner: F, counters: Arc<Counters>) -> Self {
        Self { inner, counters }
    }
}

impl<F: Future> Future for Cpu<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.project();
        let read = INSTRUCTION_READER.get();
        let i = read.map_or(0, |r| r());
        let t = Instant::now();
        let out = this.inner.poll(cx);
        add(this.counters, CPU_NS, since(t));
        if let Some(r) = read {
            add(this.counters, INSTRUCTIONS, r().wrapping_sub(i));
        }
        out
    }
}

/// Spawns the HTTP/2 server's stream tasks; one spawned from a counted
/// connection adds its poll time to that connection's CPU.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Executor;

impl<F> hyper::rt::Executor<F> for Executor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        match current() {
            Some(counters) => {
                tokio::spawn(Cpu::new(fut, counters));
            }
            None => {
                tokio::spawn(fut);
            }
        }
    }
}

/// Counts each request of a connection once its response ends (or is
/// dropped), and hands the connection's counters to the client.
#[derive(Clone)]
pub(crate) struct CountRequests<S> {
    inner: S,
    counters: Option<Arc<Counters>>,
}

impl<S> CountRequests<S> {
    pub(crate) fn new(inner: S, counters: Option<Arc<Counters>>) -> Self {
        Self { inner, counters }
    }
}

type BoxRsp =
    Pin<Box<dyn Future<Output = Result<http::Response<BoxBody>, Error>> + Send + 'static>>;

impl<S> Service<http::Request<BoxBody>> for CountRequests<S>
where
    S: Service<http::Request<BoxBody>, Response = http::Response<BoxBody>, Error = Error>,
    S::Future: Send + 'static,
{
    type Response = http::Response<BoxBody>;
    type Error = Error;
    type Future = futures::future::Either<S::Future, BoxRsp>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<BoxBody>) -> Self::Future {
        let Some(counters) = self.counters.clone() else {
            return futures::future::Either::Left(self.inner.call(req));
        };
        let start = Instant::now();
        req.extensions_mut().insert(FlowCounters(counters.clone()));
        let rsp = self.inner.call(req);
        futures::future::Either::Right(Box::pin(async move {
            match rsp.await {
                Ok(rsp) => Ok(rsp.map(|inner| {
                    BoxBody::new(CountedBody {
                        inner,
                        done: Some((start, counters)),
                    })
                })),
                Err(error) => {
                    finish(&counters, start);
                    Err(error)
                }
            }
        }))
    }
}

fn finish(counters: &Counters, start: Instant) {
    add(counters, REQUESTS, 1);
    add(counters, RESIDENCE_NS, since(start));
}

/// A response body that counts its request when it ends.
struct CountedBody {
    inner: BoxBody,
    done: Option<(Instant, Arc<Counters>)>,
}

impl CountedBody {
    fn end(&mut self) {
        if let Some((start, counters)) = self.done.take() {
            finish(&counters, start);
        }
    }
}

impl http_body::Body for CountedBody {
    type Data = <BoxBody as http_body::Body>::Data;
    type Error = <BoxBody as http_body::Body>::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let out = Pin::new(&mut self.inner).poll_frame(cx);
        match &out {
            Poll::Ready(None) | Poll::Ready(Some(Err(_))) => self.end(),
            Poll::Ready(Some(Ok(_))) if self.inner.is_end_stream() => self.end(),
            _ => {}
        }
        out
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for CountedBody {
    fn drop(&mut self) {
        self.end();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use linkerd_stack::{service_fn, ServiceExt};
    use std::sync::atomic::Ordering::Relaxed;

    fn counters() -> Arc<Counters> {
        Arc::new(Default::default())
    }

    // A request counts once its response body ends, with its residence, and
    // carries the connection's counters to the client that forwards it.
    #[tokio::test(flavor = "current_thread")]
    async fn counts_a_request_when_its_response_ends() {
        let c = counters();
        let svc = service_fn(|req: http::Request<BoxBody>| async move {
            let forwarded = req.extensions().get::<FlowCounters>().cloned();
            forwarded
                .expect("the request carries its connection's counters")
                .backend(Instant::now());
            Ok::<_, Error>(http::Response::new(BoxBody::from_static("pong")))
        });
        let rsp = CountRequests::new(svc, Some(c.clone()))
            .oneshot(http::Request::new(BoxBody::empty()))
            .await
            .unwrap();
        assert_eq!(
            c[REQUESTS].load(Relaxed),
            0,
            "counted before the response ended"
        );
        let body = rsp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"pong");
        assert_eq!(c[REQUESTS].load(Relaxed), 1);
        assert!(c[RESIDENCE_NS].load(Relaxed) > 0);
        assert!(c[BACKEND_NS].load(Relaxed) > 0);
    }

    // A response dropped before its end still counts its request once.
    #[tokio::test(flavor = "current_thread")]
    async fn counts_a_dropped_response_once() {
        let c = counters();
        let svc = service_fn(|_: http::Request<BoxBody>| async {
            Ok::<_, Error>(http::Response::new(BoxBody::from_static("pong")))
        });
        let rsp = CountRequests::new(svc, Some(c.clone()))
            .oneshot(http::Request::new(BoxBody::empty()))
            .await
            .unwrap();
        drop(rsp);
        assert_eq!(c[REQUESTS].load(Relaxed), 1);
    }

    // Without counters the service passes requests through untouched.
    #[tokio::test(flavor = "current_thread")]
    async fn uncounted_connections_pass_through() {
        let svc = service_fn(|req: http::Request<BoxBody>| async move {
            assert!(req.extensions().get::<FlowCounters>().is_none());
            Ok::<_, Error>(http::Response::new(BoxBody::empty()))
        });
        CountRequests::new(svc, None)
            .oneshot(http::Request::new(BoxBody::empty()))
            .await
            .unwrap();
    }

    // A stream task spawned from a counted connection adds its poll time.
    #[tokio::test(flavor = "current_thread")]
    async fn stream_tasks_add_their_cpu() {
        use hyper::rt::Executor as _;
        let c = counters();
        let (tx, rx) = tokio::sync::oneshot::channel();
        scope(c.clone(), async move {
            Executor.execute(async move {
                let t = Instant::now();
                while t.elapsed() < std::time::Duration::from_millis(2) {}
                let _ = tx.send(());
            });
        })
        .await;
        rx.await.unwrap();
        tokio::task::yield_now().await;
        assert!(
            c[CPU_NS].load(Relaxed) >= 2_000_000,
            "cpu {}",
            c[CPU_NS].load(Relaxed)
        );
    }
}

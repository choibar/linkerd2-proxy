use crate::{
    failfast::FailFast,
    gate,
    layer::{self, Layer},
    BoxService, ExtractParam, NewService, Service,
};
use linkerd_error::Error;
use parking_lot::Mutex;
use pin_project::pin_project;
use std::{
    fmt,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll, Waker},
    time,
};
use tower::{buffer::Buffer, ServiceExt};

/// A param type that configures the capacity of a queue.
#[derive(Clone, Debug)]
pub struct Capacity(pub usize);

/// A param type that configures a queue's failfast timeout.
#[derive(Clone, Debug)]
pub struct Timeout(pub time::Duration);

/// A [`NewService`] that wraps inner services in a [`Buffer`] with a failfast
/// [`Timeout`]. When the inner service is unavailable for that timeout, the
/// queue stops processing new requests and all pending requests are failed.
pub struct NewQueue<X, Req, N> {
    inner: N,
    extract: X,
    _req: PhantomData<fn(Req)>,
}

/// A [`NewService`] that wraps inner services in a [`Buffer`] **without** a
/// failfast timeout. Requests remain queued until the caller gives up waiting
/// or the inner service becomes available.
pub struct NewQueueWithoutTimeout<X, Req, N> {
    inner: N,
    extract: X,
    _req: PhantomData<fn(Req)>,
}

pub type Queue<Req, Rsp, E = Error> =
    gate::Gate<Buffer<Req, <BoxService<Req, Rsp, E> as tower::Service<Req>>::Future>>;

pub type QueueWithoutTimeout<Req, Rsp, E = Error> =
    Buffer<Req, <BoxService<Req, Rsp, E> as tower::Service<Req>>::Future>;

/// A param type that selects direct dispatch for a [`NewDirectQueue`] target.
#[derive(Clone, Copy, Debug)]
pub struct Direct(pub bool);

/// A [`NewService`] like [`NewQueue`], except that a target with a set
/// [`Direct`] param gets a queue its callers skip while the inner service is
/// ready.
pub struct NewDirectQueue<X, Req, N> {
    inner: N,
    extract: X,
    _req: PhantomData<fn(Req)>,
}

/// A [`Queue`] that shares its inner service with its callers. While the
/// service is ready a caller polls and calls it on its own stack, so the
/// request never visits the queue's worker task; a request that finds the
/// service unready is queued as usual, with the failfast timeout.
pub struct DirectQueue<Req, Rsp> {
    queue: Queue<Req, Rsp>,
    /// Whether `queue` was polled to readiness for the next call.
    reserved: bool,
    /// Unset when the target did not select direct dispatch.
    shared: Option<Arc<Mutex<Shared<Req, Rsp>>>>,
}

#[pin_project(project = DirectFutureProj)]
pub enum DirectFuture<Rsp> {
    Direct(#[pin] BoxFuture<Rsp>),
    Queued(#[pin] tower::buffer::future::ResponseFuture<BoxFuture<Rsp>>),
}

type BoxFuture<Rsp> = Pin<Box<dyn Future<Output = Result<Rsp, Error>> + Send>>;

struct Shared<Req, Rsp> {
    service: BoxService<Req, Rsp, Error>,
    /// Set while the queue's worker waits on `service` with its own waker. A
    /// caller's readiness probe carries none, so it must not replace that one.
    waiting: bool,
    failed: Option<Failed>,
}

/// The inner service as the queue's worker drives it.
struct Locked<Req, Rsp>(Arc<Mutex<Shared<Req, Rsp>>>);

/// The readiness error that closed a [`DirectQueue`]'s inner service, shared
/// by every request that follows.
#[derive(Clone, Debug)]
pub struct Failed(Arc<Error>);

// === impl NewQueue ===

impl<X: Clone, Req, N> NewQueue<X, Req, N> {
    pub fn new(inner: N, extract: X) -> Self {
        Self {
            inner,
            extract,
            _req: PhantomData,
        }
    }

    /// Returns a [`Layer`] that constructs new [`gate::Gate`]d [`Buffer`]s
    /// using an `X`-typed [`ExtractParam`] implementation to extract
    /// [`Capacity`] and [`Timeout`] from a `T`-typed target.
    #[inline]
    #[must_use]
    pub fn layer_via(extract: X) -> impl Layer<N, Service = Self> + Clone {
        layer::mk(move |inner| Self::new(inner, extract.clone()))
    }
}

impl<Req, N> NewQueue<(), Req, N> {
    /// Returns a [`Layer`] that constructs new queued service configured by a
    /// target that implements [`crate::Param`]`<`[`Capacity`]`>` and .
    /// [`crate::Param`]`<`[`Timeout`]`>`.
    #[inline]
    #[must_use]
    pub fn layer() -> impl Layer<N, Service = Self> + Clone {
        Self::layer_via(())
    }
}

impl<T, X, Req, N, S> NewService<T> for NewQueue<X, Req, N>
where
    Req: Send + 'static,
    X: ExtractParam<Capacity, T>,
    X: ExtractParam<Timeout, T>,
    N: NewService<T, Service = S>,
    S: Service<Req> + Send + 'static,
    S::Response: 'static,
    S::Error: Into<Error>,
    S::Future: Send,
{
    type Service = Queue<Req, S::Response>;

    fn new_service(&self, target: T) -> Self::Service {
        let Capacity(capacity) = self.extract.extract_param(&target);
        let Timeout(failfast_timeout) = self.extract.extract_param(&target);
        let buf = layer::mk(move |inner| Buffer::new(BoxService::new(inner), capacity));
        let buf = FailFast::layer_gated(failfast_timeout, buf);
        buf.layer(self.inner.new_service(target))
    }
}

impl<X, Req, N> Clone for NewQueue<X, Req, N>
where
    X: Clone,
    N: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            extract: self.extract.clone(),
            _req: PhantomData,
        }
    }
}

impl<X, Req, N> fmt::Debug for NewQueue<X, Req, N>
where
    X: fmt::Debug,
    N: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NewQueue")
            .field("inner", &self.inner)
            .field("extract", &self.extract)
            .finish()
    }
}

// === impl NewQueueWithoutTimeout ===

impl<X: Clone, Req, N> NewQueueWithoutTimeout<X, Req, N> {
    pub fn new(inner: N, extract: X) -> Self {
        Self {
            inner,
            extract,
            _req: PhantomData,
        }
    }

    /// Returns a [`Layer`] that constructs new [`Buffer`]s using an `X`-typed
    /// [`ExtractParam`] implementation to extract [`Capacity`] from a
    /// `T`-typed target.
    #[inline]
    #[must_use]
    pub fn layer_via(extract: X) -> impl Layer<N, Service = Self> + Clone {
        layer::mk(move |inner| Self::new(inner, extract.clone()))
    }
}

impl<Req, N> NewQueueWithoutTimeout<(), Req, N> {
    /// Returns a [`Layer`] that constructs new [`Buffer`]s configured by a
    /// target that implements [`crate::Param`]`<`[`Capacity`]`>`.
    #[inline]
    #[must_use]
    pub fn layer() -> impl Layer<N, Service = Self> + Clone {
        Self::layer_via(())
    }
}

impl<T, X, Req, N, S> NewService<T> for NewQueueWithoutTimeout<X, Req, N>
where
    Req: Send + 'static,
    X: ExtractParam<Capacity, T>,
    N: NewService<T, Service = S>,
    S: Service<Req> + Send + 'static,
    S::Response: 'static,
    S::Error: Into<Error> + Send + Sync + 'static,
    S::Future: Send,
{
    type Service = QueueWithoutTimeout<Req, S::Response, S::Error>;

    fn new_service(&self, target: T) -> Self::Service {
        let Capacity(capacity) = self.extract.extract_param(&target);
        let inner = self.inner.new_service(target);
        Buffer::new(BoxService::new(inner), capacity)
    }
}

impl<X, Req, N> Clone for NewQueueWithoutTimeout<X, Req, N>
where
    X: Clone,
    N: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            extract: self.extract.clone(),
            _req: PhantomData,
        }
    }
}

impl<X, Req, N> fmt::Debug for NewQueueWithoutTimeout<X, Req, N>
where
    X: fmt::Debug,
    N: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NewQueueWithoutTimeout")
            .field("inner", &self.inner)
            .field("extract", &self.extract)
            .finish()
    }
}

// === impl NewDirectQueue ===

impl<X: Clone, Req, N> NewDirectQueue<X, Req, N> {
    pub fn new(inner: N, extract: X) -> Self {
        Self {
            inner,
            extract,
            _req: PhantomData,
        }
    }

    /// Returns a [`Layer`] that constructs new [`DirectQueue`]s using an
    /// `X`-typed [`ExtractParam`] implementation to extract [`Capacity`],
    /// [`Timeout`] and [`Direct`] from a `T`-typed target.
    #[inline]
    #[must_use]
    pub fn layer_via(extract: X) -> impl Layer<N, Service = Self> + Clone {
        layer::mk(move |inner| Self::new(inner, extract.clone()))
    }
}

impl<Req, N> NewDirectQueue<(), Req, N> {
    /// Returns a [`Layer`] that constructs new [`DirectQueue`]s configured by
    /// a target that implements [`crate::Param`] for [`Capacity`], [`Timeout`]
    /// and [`Direct`].
    #[inline]
    #[must_use]
    pub fn layer() -> impl Layer<N, Service = Self> + Clone {
        Self::layer_via(())
    }
}

impl<T, X, Req, N, S> NewService<T> for NewDirectQueue<X, Req, N>
where
    Req: Send + 'static,
    X: ExtractParam<Capacity, T>,
    X: ExtractParam<Timeout, T>,
    X: ExtractParam<Direct, T>,
    N: NewService<T, Service = S>,
    S: Service<Req> + Send + 'static,
    S::Response: 'static,
    S::Error: Into<Error>,
    S::Future: Send,
{
    type Service = DirectQueue<Req, S::Response>;

    fn new_service(&self, target: T) -> Self::Service {
        let Capacity(capacity) = self.extract.extract_param(&target);
        let Timeout(failfast_timeout) = self.extract.extract_param(&target);
        let Direct(direct) = self.extract.extract_param(&target);
        let inner = self.inner.new_service(target);
        if !direct {
            let buf = layer::mk(move |inner| Buffer::new(BoxService::new(inner), capacity));
            let buf = FailFast::layer_gated(failfast_timeout, buf);
            return DirectQueue {
                queue: buf.layer(inner),
                reserved: false,
                shared: None,
            };
        }
        let shared = Arc::new(Mutex::new(Shared {
            service: BoxService::new(inner.map_err(Into::<Error>::into)),
            waiting: false,
            failed: None,
        }));
        let buf = layer::mk(move |inner| Buffer::new(BoxService::new(inner), capacity));
        let buf = FailFast::layer_gated(failfast_timeout, buf);
        DirectQueue {
            queue: buf.layer(Locked(shared.clone())),
            reserved: false,
            shared: Some(shared),
        }
    }
}

impl<X, Req, N> Clone for NewDirectQueue<X, Req, N>
where
    X: Clone,
    N: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            extract: self.extract.clone(),
            _req: PhantomData,
        }
    }
}

impl<X, Req, N> fmt::Debug for NewDirectQueue<X, Req, N>
where
    X: fmt::Debug,
    N: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NewDirectQueue")
            .field("inner", &self.inner)
            .field("extract", &self.extract)
            .finish()
    }
}

// === impl DirectQueue ===

impl<Req, Rsp> Service<Req> for DirectQueue<Req, Rsp>
where
    Req: Send + 'static,
    Rsp: 'static,
{
    type Response = Rsp;
    type Error = Error;
    type Future = DirectFuture<Rsp>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if let Some(shared) = self.shared.as_ref() {
            if shared.lock().is_idle() {
                return Poll::Ready(Ok(()));
            }
        }
        ready!(self.queue.poll_ready(cx))?;
        self.reserved = true;
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut req: Req) -> Self::Future {
        if let Some(shared) = self.shared.as_ref() {
            let dispatched = shared.lock().dispatch(req);
            match dispatched {
                Ok(future) => return DirectFuture::Direct(future),
                Err(queued) => req = queued,
            }
        }
        if std::mem::take(&mut self.reserved) {
            return DirectFuture::Queued(self.queue.call(req));
        }
        // Readiness was advertised for a direct dispatch that is no longer
        // possible, so this request waits for the queue instead.
        DirectFuture::Direct(Box::pin(self.queue.clone().oneshot(req)))
    }
}

impl<Req, Rsp> Clone for DirectQueue<Req, Rsp>
where
    Req: Send + 'static,
    Rsp: 'static,
{
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
            reserved: false,
            shared: self.shared.clone(),
        }
    }
}

impl<Req, Rsp> fmt::Debug for DirectQueue<Req, Rsp> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DirectQueue")
            .field("direct", &self.shared.is_some())
            .finish()
    }
}

// === impl Shared ===

impl<Req, Rsp> Shared<Req, Rsp> {
    fn is_idle(&self) -> bool {
        !self.waiting && self.failed.is_none()
    }

    /// Calls the service on the caller's stack if it is ready now, and hands
    /// the request back otherwise.
    fn dispatch(&mut self, req: Req) -> Result<BoxFuture<Rsp>, Req> {
        if !self.is_idle() {
            return Err(req);
        }
        // The probe carries no waker: a request that finds the service unready
        // is queued, and the worker then polls with its own.
        match self
            .service
            .poll_ready(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(Ok(())) => Ok(self.service.call(req)),
            Poll::Pending => Err(req),
            Poll::Ready(Err(error)) => {
                let failed = Failed(Arc::new(error));
                self.failed = Some(failed.clone());
                Ok(Box::pin(async move { Err(failed.into()) }))
            }
        }
    }
}

// === impl Locked ===

impl<Req, Rsp> Service<Req> for Locked<Req, Rsp> {
    type Response = Rsp;
    type Error = Error;
    type Future = BoxFuture<Rsp>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let mut shared = self.0.lock();
        if let Some(failed) = shared.failed.clone() {
            return Poll::Ready(Err(failed.into()));
        }
        let poll = shared.service.poll_ready(cx);
        shared.waiting = poll.is_pending();
        poll.map_err(|error| {
            let failed = Failed(Arc::new(error));
            shared.failed = Some(failed.clone());
            failed.into()
        })
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let mut shared = self.0.lock();
        if let Some(failed) = shared.failed.clone() {
            return Box::pin(async move { Err(failed.into()) });
        }
        shared.service.call(req)
    }
}

// === impl Failed ===

impl fmt::Display for Failed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "queued service failed: {}", self.0)
    }
}

impl std::error::Error for Failed {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&**self.0)
    }
}

// === impl DirectFuture ===

impl<Rsp> Future for DirectFuture<Rsp> {
    type Output = Result<Rsp, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            DirectFutureProj::Direct(future) => future.poll(cx),
            DirectFutureProj::Queued(future) => future.poll(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Param;
    use tokio_test::{assert_pending, assert_ready, assert_ready_ok};
    use tower_test::mock;

    struct Target(bool);

    impl Param<Capacity> for Target {
        fn param(&self) -> Capacity {
            Capacity(10)
        }
    }

    impl Param<Timeout> for Target {
        fn param(&self) -> Timeout {
            Timeout(time::Duration::from_secs(10))
        }
    }

    impl Param<Direct> for Target {
        fn param(&self) -> Direct {
            Direct(self.0)
        }
    }

    type Mock = mock::Spawn<DirectQueue<(), ()>>;

    fn queue(direct: bool) -> (Mock, mock::Handle<(), ()>) {
        let (service, handle) = mock::pair::<(), ()>();
        let service = Mutex::new(Some(service));
        let new = NewDirectQueue::layer()
            .layer(move |_: Target| service.lock().take().expect("one service"));
        (mock::Spawn::new(new.new_service(Target(direct))), handle)
    }

    #[tokio::test]
    async fn dispatches_on_the_callers_stack() {
        let (mut queue, mut handle) = queue(true);
        assert_ready_ok!(queue.poll_ready());
        let rsp = queue.call(());
        // The worker task has not run since the call.
        let (_, send) = assert_ready!(handle.poll_request()).expect("request");
        send.send_response(());
        rsp.await.expect("response");
    }

    #[tokio::test]
    async fn queues_without_direct_param() {
        let (mut queue, mut handle) = queue(false);
        assert_ready_ok!(queue.poll_ready());
        let rsp = queue.call(());
        assert_pending!(handle.poll_request());
        let (_, send) = handle.next_request().await.expect("request");
        send.send_response(());
        rsp.await.expect("response");
    }

    #[tokio::test]
    async fn queues_while_unready() {
        let (mut queue, mut handle) = queue(true);
        handle.allow(0);
        // Nothing waits on the service yet, so the request itself finds out.
        assert_ready_ok!(queue.poll_ready());
        let first = tokio::spawn(queue.call(()));
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert_pending!(handle.poll_request());

        // The worker now waits on the service, and a later request must not
        // overtake the one it holds.
        handle.allow(1);
        assert_ready_ok!(queue.poll_ready());
        let second = tokio::spawn(queue.call(()));
        let (_, send) = handle.next_request().await.expect("first request");
        send.send_response(());
        first.await.unwrap().expect("first response");
        handle.allow(1);
        let (_, send) = handle.next_request().await.expect("second request");
        send.send_response(());
        second.await.unwrap().expect("second response");

        // With the queue drained, dispatch is direct again.
        handle.allow(1);
        assert_ready_ok!(queue.poll_ready());
        let rsp = queue.call(());
        let (_, send) = assert_ready!(handle.poll_request()).expect("request");
        send.send_response(());
        rsp.await.expect("response");
    }

    #[tokio::test]
    async fn fails_for_good_after_a_readiness_error() {
        let (mut queue, mut handle) = queue(true);
        handle.send_error("lost");
        assert_ready_ok!(queue.poll_ready());
        let error = queue.call(()).await.expect_err("must fail");
        assert!(error.is::<Failed>());

        // Like a buffer, the queue fails every request from then on.
        let error = match queue.get_mut().ready().await {
            Ok(queue) => queue.call(()).await.expect_err("must fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("lost"), "{error}");
        assert_pending!(handle.poll_request());
    }
}

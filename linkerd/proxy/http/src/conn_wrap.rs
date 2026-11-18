//! A per-connection wrapper around the HTTP service a server runs, set by the
//! connection's acceptor. An acceptor runs a connection's future inside
//! [`scope`]; the HTTP server picks the wrapper up when it starts serving and
//! passes every request of that connection through the wrapped service.
//!
//! The DMA acceptor uses it for the inbound role of a connection (the
//! destination's request authorization, rate limit and access log), which a
//! TCP sidecar runs in its inbound server.

use crate::BoxBody;
use linkerd_error::Error;
use linkerd_stack::Service;
use std::{
    future::Future,
    sync::Arc,
    task::{Context, Poll},
};
use tower::util::BoxCloneService;

/// The HTTP service of one connection, boxed.
pub type BoxHttp = BoxCloneService<http::Request<BoxBody>, http::Response<BoxBody>, Error>;

/// Wraps the HTTP service of one connection.
pub type Wrap = Arc<dyn Fn(BoxHttp) -> BoxHttp + Send + Sync + 'static>;

tokio::task_local! {
    static WRAP: Wrap;
}

/// Runs `fut`, a connection's future, with `wrap` around its HTTP service.
pub fn scope<F: Future>(wrap: Wrap, fut: F) -> impl Future<Output = F::Output> {
    WRAP.scope(wrap, fut)
}

pub(crate) fn current() -> Option<Wrap> {
    WRAP.try_with(Clone::clone).ok()
}

/// A connection's HTTP service: the wrapped one when the acceptor set a
/// wrapper, the inner one otherwise.
#[derive(Clone)]
pub(crate) struct Wrapped<S> {
    inner: S,
    wrapped: Option<BoxHttp>,
}

impl<S> Wrapped<S>
where
    S: Service<http::Request<BoxBody>, Response = http::Response<BoxBody>, Error = Error>,
    S: Clone + Send + 'static,
    S::Future: Send + 'static,
{
    pub(crate) fn new(inner: S, wrap: Option<Wrap>) -> Self {
        let wrapped = wrap.map(|wrap| wrap(BoxCloneService::new(inner.clone())));
        Self { inner, wrapped }
    }
}

impl<S> Service<http::Request<BoxBody>> for Wrapped<S>
where
    S: Service<http::Request<BoxBody>, Response = http::Response<BoxBody>, Error = Error>,
{
    type Response = http::Response<BoxBody>;
    type Error = Error;
    type Future = futures::future::Either<
        S::Future,
        <BoxHttp as Service<http::Request<BoxBody>>>::Future,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        match self.wrapped.as_mut() {
            Some(wrapped) => wrapped.poll_ready(cx),
            None => self.inner.poll_ready(cx),
        }
    }

    fn call(&mut self, req: http::Request<BoxBody>) -> Self::Future {
        match self.wrapped.as_mut() {
            Some(wrapped) => futures::future::Either::Right(wrapped.call(req)),
            None => futures::future::Either::Left(self.inner.call(req)),
        }
    }
}

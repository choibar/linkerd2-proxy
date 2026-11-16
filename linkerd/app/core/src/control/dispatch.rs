//! Keep shared control-plane objects under the subscriber that created them.
//!
//! In particular, registry parent cleanup uses the current default dispatcher.
//! Polling alone is insufficient: cancellation, response bodies, clones, and
//! service teardown must also restore the controller's owning dispatcher.
//! Restore its runtime too, so background controller tasks do not escape onto
//! a private datapath worker merely because that worker requested a policy.
use crate::svc;
use futures::{future, FutureExt};
use http::{Request, Response};
use http_body::{Body, Frame, SizeHint};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tracing::{dispatcher, Dispatch};

#[pin_project::pin_project(PinnedDrop)]
#[derive(Debug)]
pub struct Scoped<T> {
    #[pin]
    inner: Option<T>,
    dispatch: Dispatch,
    runtime: Option<tokio::runtime::Handle>,
}

impl<T> Scoped<T> {
    pub(super) fn new(inner: T) -> Self {
        Self {
            inner: Some(inner),
            dispatch: dispatcher::get_default(Clone::clone),
            runtime: tokio::runtime::Handle::try_current().ok(),
        }
    }
}

impl<T: Clone> Clone for Scoped<T> {
    fn clone(&self) -> Self {
        let _runtime = self.runtime.as_ref().map(tokio::runtime::Handle::enter);
        let _guard = dispatcher::set_default(&self.dispatch);
        Self {
            inner: self.inner.clone(),
            dispatch: self.dispatch.clone(),
            runtime: self.runtime.clone(),
        }
    }
}

#[pin_project::pinned_drop]
impl<T> PinnedDrop for Scoped<T> {
    fn drop(self: Pin<&mut Self>) {
        let mut this = self.project();
        let _runtime = this.runtime.as_ref().map(tokio::runtime::Handle::enter);
        let _guard = dispatcher::set_default(this.dispatch);
        this.inner.set(None);
    }
}

impl<T: Future> Future for Scoped<T> {
    type Output = T::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let _runtime = this.runtime.as_ref().map(tokio::runtime::Handle::enter);
        let _guard = dispatcher::set_default(this.dispatch);
        this.inner.as_pin_mut().expect("controller future").poll(cx)
    }
}

impl<T: Body> Body for Scoped<T> {
    type Data = T::Data;
    type Error = T::Error;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let _runtime = this.runtime.as_ref().map(tokio::runtime::Handle::enter);
        let _guard = dispatcher::set_default(this.dispatch);
        this.inner
            .as_pin_mut()
            .expect("controller body")
            .poll_frame(cx)
    }
    fn is_end_stream(&self) -> bool {
        let _runtime = self.runtime.as_ref().map(tokio::runtime::Handle::enter);
        let _guard = dispatcher::set_default(&self.dispatch);
        self.inner
            .as_ref()
            .expect("controller body")
            .is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        let _runtime = self.runtime.as_ref().map(tokio::runtime::Handle::enter);
        let _guard = dispatcher::set_default(&self.dispatch);
        self.inner.as_ref().expect("controller body").size_hint()
    }
}

impl<T, N: svc::NewService<T>> svc::NewService<T> for Scoped<N> {
    type Service = Scoped<N::Service>;
    fn new_service(&self, target: T) -> Self::Service {
        let _runtime = self.runtime.as_ref().map(tokio::runtime::Handle::enter);
        let _guard = dispatcher::set_default(&self.dispatch);
        Scoped::new(
            self.inner
                .as_ref()
                .expect("controller factory")
                .new_service(target),
        )
    }
}

type MapResponse<F, B, E> =
    future::Map<F, fn(Result<Response<B>, E>) -> Result<Response<Scoped<B>>, E>>;

impl<S, Req, B> svc::Service<Request<Req>> for Scoped<S>
where
    S: svc::Service<Request<Req>, Response = Response<B>>,
{
    type Response = Response<Scoped<B>>;
    type Error = S::Error;
    type Future = Scoped<MapResponse<S::Future, B, S::Error>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let _runtime = self.runtime.as_ref().map(tokio::runtime::Handle::enter);
        let _guard = dispatcher::set_default(&self.dispatch);
        self.inner
            .as_mut()
            .expect("controller service")
            .poll_ready(cx)
    }
    fn call(&mut self, req: Request<Req>) -> Self::Future {
        let _runtime = self.runtime.as_ref().map(tokio::runtime::Handle::enter);
        let _guard = dispatcher::set_default(&self.dispatch);
        let future = self.inner.as_mut().expect("controller service").call(req);
        let map: fn(Result<Response<B>, S::Error>) -> Result<Response<Scoped<B>>, S::Error> =
            |result| result.map(|response| response.map(Scoped::new));
        Scoped::new(future.map(map))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::svc::{NewService, Service};
    use std::convert::Infallible;
    use tracing::{info_span, Span};

    #[test]
    // Return the pending task so the owner runtime can be driven afterwards.
    #[allow(clippy::async_yields_async)]
    fn controller_background_tasks_stay_on_the_owning_runtime() {
        struct Spawn;
        impl NewService<()> for Spawn {
            type Service = tokio::task::JoinHandle<usize>;
            fn new_service(&self, _: ()) -> Self::Service {
                tokio::spawn(async { current_registry() })
            }
        }
        let owner_rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let worker_rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let (owner, _) = linkerd_tracing::test::trace_subscriber("info");
        let (worker, _) = linkerd_tracing::test::trace_subscriber("info");
        let (factory, expected) = dispatcher::with_default(&owner, || {
            let _runtime = owner_rt.enter();
            (Scoped::new(Spawn), current_registry())
        });
        let pending = dispatcher::with_default(&worker, || {
            worker_rt.block_on(async {
                let pending = factory.new_service(());
                for _ in 0..4 {
                    tokio::task::yield_now().await;
                }
                assert!(
                    !pending.inner.as_ref().unwrap().is_finished(),
                    "controller task escaped onto worker runtime"
                );
                pending
            })
        });
        let actual = dispatcher::with_default(&owner, || owner_rt.block_on(pending)).unwrap();
        assert_eq!(actual, expected);
    }

    fn current_registry() -> usize {
        dispatcher::get_default(|d| {
            d.downcast_ref::<linkerd_tracing::registry::Registry>()
                .unwrap() as *const _ as usize
        })
    }

    #[derive(Clone)]
    struct Factory(usize);
    impl NewService<()> for Factory {
        type Service = Client;
        fn new_service(&self, _: ()) -> Client {
            assert_eq!(current_registry(), self.0);
            Client {
                registry: self.0,
                parent: info_span!("controller"),
            }
        }
    }
    #[derive(Clone)]
    struct Client {
        registry: usize,
        parent: Span,
    }
    struct ResponseBody {
        registry: usize,
        _span: Span,
    }
    impl Body for ResponseBody {
        type Data = bytes::Bytes;
        type Error = Infallible;
        fn poll_frame(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            assert_eq!(current_registry(), self.registry);
            Poll::Ready(None)
        }
    }
    impl Service<Request<()>> for Client {
        type Response = Response<ResponseBody>;
        type Error = Infallible;
        type Future = future::Ready<Result<Self::Response, Infallible>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            assert_eq!(current_registry(), self.registry);
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, _: Request<()>) -> Self::Future {
            assert_eq!(current_registry(), self.registry);
            future::ready(Ok(Response::new(ResponseBody {
                registry: self.registry,
                _span: info_span!(parent: &self.parent, "response"),
            })))
        }
    }

    #[test]
    fn shared_controller_keeps_its_dispatch_through_calls_cancellation_and_body_drop() {
        let (owner, _) = linkerd_tracing::test::trace_subscriber("info");
        let (worker, _) = linkerd_tracing::test::trace_subscriber("info");
        let factory = dispatcher::with_default(&owner, || Scoped::new(Factory(current_registry())));
        dispatcher::with_default(&worker, || {
            let mut service = factory.new_service(()).clone();
            let mut cx = Context::from_waker(futures::task::noop_waker_ref());
            assert!(service.poll_ready(&mut cx).is_ready());
            // Cancel a future before its first poll; its global spans must be
            // dropped under the global dispatcher even on a worker thread.
            drop(service.call(Request::new(())));
            let mut future = Box::pin(service.call(Request::new(())));
            let response = match future.as_mut().poll(&mut cx) {
                Poll::Ready(Ok(response)) => response,
                _ => panic!("controller response not ready"),
            };
            drop(future);
            drop(service);
            let mut body = Box::pin(response.into_body());
            assert!(body.as_mut().poll_frame(&mut cx).is_ready());
            // The response child is now the only owner of its global parent.
            // Registry cleanup used to try to close that parent in the worker.
            drop(body);
        });
    }
}

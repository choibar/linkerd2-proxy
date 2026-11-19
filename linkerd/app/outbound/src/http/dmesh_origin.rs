//! Keep verified DMA identity across HTTP service and reconnect task boundaries.
//! No HTTP header is accepted as identity evidence.
use linkerd_app_core::{
    proxy::http,
    svc::{NewService, Service},
};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

#[derive(Clone)]
pub struct NewRequestOrigin<N>(N);
impl<N> NewRequestOrigin<N> {
    pub fn new(inner: N) -> Self {
        Self(inner)
    }
}
#[derive(Clone)]
pub struct RequestOrigin<S> {
    inner: S,
    origin: Option<Arc<dmesh_doca::PeerOrigin>>,
}
impl<T, N: NewService<T>> NewService<T> for NewRequestOrigin<N> {
    type Service = RequestOrigin<N::Service>;
    fn new_service(&self, target: T) -> Self::Service {
        RequestOrigin {
            inner: self.0.new_service(target),
            origin: dmesh_doca::origin::current().map(Arc::new),
        }
    }
}
impl<B, S: Service<http::Request<B>>> Service<http::Request<B>> for RequestOrigin<S> {
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, mut request: http::Request<B>) -> Self::Future {
        if let Some(origin) = &self.origin {
            request.extensions_mut().insert(origin.clone());
        }
        self.inner.call(request)
    }
}

#[derive(Clone)]
pub struct Scoped<S> {
    inner: S,
    origin: Option<dmesh_doca::PeerOrigin>,
}
impl<S> Scoped<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            origin: dmesh_doca::origin::current(),
        }
    }
}
impl<R, S> Service<R> for Scoped<S>
where
    S: Service<R>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        dmesh_doca::origin::sync_scope(self.origin.clone(), || self.inner.poll_ready(cx))
    }
    fn call(&mut self, request: R) -> Self::Future {
        let future =
            dmesh_doca::origin::sync_scope(self.origin.clone(), || self.inner.call(request));
        Box::pin(dmesh_doca::origin::scope(self.origin.clone(), future))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use linkerd_app_core::svc;

    #[tokio::test]
    async fn connection_origin_survives_spawn_and_overwrites_untrusted_extension() {
        let trusted = dmesh_doca::PeerOrigin {
            session_id: 7,
            session_epoch: 3,
            generation: 2,
            channel_slot: 1,
            pod_uid: "verified-pod".into(),
            namespace: "bench".into(),
            service_account: "caller".into(),
            service: "client".into(),
        };
        let inner = svc::ArcNewService::new(|_: ()| {
            svc::mk(|request: http::Request<()>| async move {
                Ok::<_, std::io::Error>(
                    request
                        .extensions()
                        .get::<Arc<dmesh_doca::PeerOrigin>>()
                        .cloned()
                        .unwrap(),
                )
            })
        });
        let mut service = dmesh_doca::origin::sync_scope(Some(trusted.clone()), || {
            NewRequestOrigin::new(inner).new_service(())
        });
        let mut untrusted = trusted.clone();
        untrusted.pod_uid = "wrong-pod".into();
        let mut request = http::Request::new(());
        request.extensions_mut().insert(Arc::new(untrusted));
        let got = tokio::spawn(async move {
            assert!(dmesh_doca::origin::current().is_none());
            service.call(request).await.unwrap()
        })
        .await
        .unwrap();
        assert_eq!(got.as_ref(), &trusted);
    }
}

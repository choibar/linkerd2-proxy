//! The inbound role of a connection that reaches the proxy with its source
//! already attested (the DMA path). Such a connection is served by the
//! outbound stack alone, so what a sidecar's inbound HTTP server does for the
//! destination runs around that connection's HTTP service instead: the access
//! log, then the destination's request authorization and local rate limit.

use crate::{metrics::authz::HttpAuthzMetrics, policy};
use linkerd_app_core::{
    proxy::http::conn_wrap::{BoxHttp, Wrap},
    svc::{self, NewService, Param},
    tls,
    transport::{ClientAddr, Remote},
};
use linkerd_http_access_log::NewAccessLog;
use std::sync::Arc;

/// Wraps the HTTP service of a connection from `target` to the destination
/// that `policy` belongs to.
pub fn http_wrap<T>(target: T, policy: policy::AllowPolicy, metrics: HttpAuthzMetrics) -> Wrap
where
    T: Param<tls::ConditionalServerTls> + Param<Remote<ClientAddr>>,
    T: Clone + Send + Sync + 'static,
{
    Arc::new(move |inner: BoxHttp| {
        let inner = policy::FusedHttpPolicy::new(
            inner,
            policy.clone(),
            target.param(),
            target.param(),
            metrics.clone(),
        );
        let logged = svc::Layer::layer(&NewAccessLog::layer(), move |_: T| inner.clone())
            .new_service(target.clone());
        BoxHttp::new(logged)
    })
}

mod api;
mod config;
pub mod defaults;
mod http;
mod store;
mod tcp;

use crate::metrics::authz::HTTPLocalRateLimitLabels;

pub(crate) use self::store::Store;
pub use self::{
    config::Config,
    http::{
        FusedHttpPolicy, HttpInvalidPolicy, HttpRouteInvalidRedirect, HttpRouteNotFound,
        HttpRouteRedirect, HttpRouteUnauthorized, NewHttpPolicy, PermitVariant, Permitted,
    },
    tcp::NewTcpPolicy,
};

pub use linkerd_app_core::metrics::ServerLabel;
use linkerd_app_core::{
    identity as id,
    metrics::{RouteAuthzLabels, ServerAuthzLabels},
    tls,
    transport::{ClientAddr, OrigDstAddr, Remote},
};
use linkerd_idle_cache::Cached;
pub use linkerd_proxy_server_policy::{
    authz::Suffix,
    grpc::Route as GrpcRoute,
    http::{filter::Redirection, Route as HttpRoute},
    route, Authentication, Authorization, Meta, Protocol, RateLimitError, RoutePolicy,
    ServerPolicy,
};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;

#[derive(Clone, Debug, Error)]
#[error("unauthorized connection on {}/{}", server.kind(), server.name())]
pub struct ServerUnauthorized {
    server: Arc<Meta>,
}

pub trait GetPolicy {
    /// Returns the traffic policy configured for the destination address.
    fn get_policy(&self, dst: OrigDstAddr) -> AllowPolicy;
}

/// A policy store may be held behind a pointer, so that a proxy serving many
/// destination workloads (the DMA path) can keep one store per workload.
impl<T: GetPolicy + ?Sized> GetPolicy for Arc<T> {
    fn get_policy(&self, dst: OrigDstAddr) -> AllowPolicy {
        (**self).get_policy(dst)
    }
}

#[derive(Clone, Debug)]
pub enum DefaultPolicy {
    Allow(ServerPolicy),
    Deny,
}

#[derive(Clone, Debug)]
pub struct AllowPolicy {
    dst: OrigDstAddr,
    server: Cached<watch::Receiver<ServerPolicy>>,
}

/// Describes an authorized non-HTTP connection.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ServerPermit {
    pub dst: OrigDstAddr,
    pub protocol: Protocol,
    pub labels: ServerAuthzLabels,
}

/// Describes an authorized HTTP request.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HttpRoutePermit {
    pub dst: OrigDstAddr,
    pub labels: RouteAuthzLabels,
}

pub enum Routes {
    Http(Arc<[HttpRoute]>),
    Grpc(Arc<[GrpcRoute]>),
}

// === impl DefaultPolicy ===

impl From<ServerPolicy> for DefaultPolicy {
    fn from(p: ServerPolicy) -> Self {
        DefaultPolicy::Allow(p)
    }
}

impl From<DefaultPolicy> for ServerPolicy {
    fn from(d: DefaultPolicy) -> Self {
        match d {
            DefaultPolicy::Allow(p) => p,
            DefaultPolicy::Deny => ServerPolicy {
                protocol: Protocol::Opaque(Arc::new([])),
                local_rate_limit: Default::default(),
                meta: Meta::new_default("deny"),
            },
        }
    }
}

// === impl AllowPolicy ===

impl AllowPolicy {
    #[cfg(any(test, fuzzing, feature = "test-util"))]
    pub fn for_test(dst: OrigDstAddr, server: ServerPolicy) -> (Self, watch::Sender<ServerPolicy>) {
        let (tx, server) = watch::channel(server);
        let server = Cached::uncached(server);
        let p = Self { dst, server };
        (p, tx)
    }

    #[inline]
    pub(crate) fn borrow(&self) -> tokio::sync::watch::Ref<'_, ServerPolicy> {
        self.server.borrow()
    }

    #[inline]
    pub(crate) fn protocol(&self) -> Protocol {
        self.server.borrow().protocol.clone()
    }

    #[inline]
    pub fn dst_addr(&self) -> OrigDstAddr {
        self.dst
    }

    #[inline]
    pub fn meta(&self) -> Arc<Meta> {
        self.server.borrow().meta.clone()
    }

    #[inline]
    pub fn server_label(&self) -> ServerLabel {
        ServerLabel(self.server.borrow().meta.clone(), self.dst.port())
    }

    pub fn ratelimit_label(&self, error: &RateLimitError) -> HTTPLocalRateLimitLabels {
        use RateLimitError::*;

        let scope = match error {
            Total(_) => "total",
            PerIdentity(_) | Override(_) => "identity",
        };
        HTTPLocalRateLimitLabels {
            server: self.server_label(),
            rate_limit: self.server.borrow().local_rate_limit.meta(),
            scope,
        }
    }

    pub async fn changed(&mut self) {
        if self.server.changed().await.is_err() {
            // If the sender was dropped, then there can be no further changes.
            futures::future::pending::<()>().await;
        }
    }

    fn routes(&self) -> Option<Routes> {
        let borrow = self.server.borrow();
        match &borrow.protocol {
            Protocol::Detect { http, .. } | Protocol::Http1(http) | Protocol::Http2(http) => {
                Some(Routes::Http(http.clone()))
            }
            Protocol::Grpc(grpc) => Some(Routes::Grpc(grpc.clone())),
            _ => None,
        }
    }
}

/// Connection-level authorization for the fused DMA (dmesh) path.
///
/// The dmesh fused stack applies the destination's inbound authorization once
/// per connection: there is no mTLS handshake, so the source workload identity
/// is carried in `tls` as a pre-attested peer (see `DmeshTarget`). This reuses
/// the exact per-authorization decision of the normal inbound path
/// ([`is_authorized`]). For HTTP/gRPC services, whose authorizations are
/// per-route, a connection is permitted if the client is authorized by ANY
/// route rule; each request is then authorized against its own route by
/// [`FusedHttpPolicy`]. A server whose protocol is detected (`Detect`, a
/// `Server` without `proxyProtocol`) carries both kinds: the connection is
/// permitted by its TCP authorizations or by any HTTP route rule, because a
/// route-scoped `AuthorizationPolicy` leaves the TCP authorizations empty
/// while HTTP requests on the connection may still be authorized. Returns
/// true if `client`/`tls` may reach the server behind `policy`.
pub fn dmesh_connection_authorized(
    policy: &AllowPolicy,
    client_addr: Remote<ClientAddr>,
    tls: &tls::ConditionalServerTls,
) -> bool {
    let server = policy.borrow();
    let any = |authzs: &[Authorization]| authzs.iter().any(|a| is_authorized(a, client_addr, tls));
    match &server.protocol {
        Protocol::Detect {
            http,
            tcp_authorizations,
            ..
        } => {
            any(tcp_authorizations)
                || http
                    .iter()
                    .flat_map(|r| r.rules.iter())
                    .any(|rule| any(&rule.policy.authorizations))
        }
        Protocol::Opaque(authzs) | Protocol::Tls(authzs) => any(authzs),
        Protocol::Http1(routes) | Protocol::Http2(routes) => routes
            .iter()
            .flat_map(|r| r.rules.iter())
            .any(|rule| any(&rule.policy.authorizations)),
        Protocol::Grpc(routes) => routes
            .iter()
            .flat_map(|r| r.rules.iter())
            .any(|rule| any(&rule.policy.authorizations)),
    }
}

fn is_tls_authorized(tls: &tls::ConditionalServerTls, authz: &Authorization) -> bool {
    match authz.authentication {
        Authentication::Unauthenticated => true,

        Authentication::TlsUnauthenticated => {
            matches!(
                tls,
                tls::ConditionalServerTls::Some(tls::ServerTls::Established { .. })
            )
        }

        Authentication::TlsAuthenticated {
            ref identities,
            ref suffixes,
        } => match tls {
            tls::ConditionalServerTls::Some(tls::ServerTls::Established {
                client_id: Some(tls::server::ClientId(ref id)),
                ..
            }) => match id {
                id::Id::Uri(_) => identities.contains(&*id.to_str()),
                id::Id::Dns(_) => {
                    identities.contains(&*id.to_str())
                        || suffixes.iter().any(|s| s.contains(&id.to_str()))
                }
            },
            _ => false,
        },
    }
}

fn is_authorized(
    authz: &Authorization,
    client_addr: Remote<ClientAddr>,
    tls: &tls::ConditionalServerTls,
) -> bool {
    if !authz.networks.iter().any(|n| n.contains(&client_addr.ip())) {
        return false;
    }

    is_tls_authorized(tls, authz)
}

// === impl Permit ===

impl ServerPermit {
    fn new(dst: OrigDstAddr, server: &ServerPolicy, authz: &Authorization) -> Self {
        Self {
            dst,
            protocol: server.protocol.clone(),
            labels: ServerAuthzLabels {
                authz: authz.meta.clone(),
                server: ServerLabel(server.meta.clone(), dst.port()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::is_tls_authorized;
    use super::Meta;
    use super::Suffix;
    use super::{Authentication, Authorization};
    use linkerd_app_core::tls;
    use std::collections::BTreeSet;
    use std::str::FromStr;
    use std::sync::Arc;

    fn authorization(identities: BTreeSet<String>, suffixes: Vec<Suffix>) -> Authorization {
        Authorization {
            networks: vec![],
            meta: Arc::new(Meta::Default {
                name: "name".into(),
            }),
            authentication: Authentication::TlsAuthenticated {
                identities,
                suffixes,
            },
        }
    }

    fn server_tls(identity: &str) -> tls::ConditionalServerTls {
        let client_id = tls::ClientId::from_str(identity).expect("should parse id");
        tls::ConditionalServerTls::Some(tls::ServerTls::Established {
            client_id: Some(client_id),
            negotiated_protocol: None,
        })
    }

    #[test]
    fn is_authorized_for_matching_spiffe_ids() {
        let tls = server_tls("spiffe://some-root/some-workload");
        let authz = authorization(
            BTreeSet::from(["spiffe://some-root/some-workload".into()]),
            vec![],
        );
        assert!(is_tls_authorized(&tls, &authz))
    }

    #[test]
    fn is_not_authorized_for_non_matching_spiffe_ids() {
        let tls = server_tls("spiffe://some-root/some-workload-1");
        let authz = authorization(
            BTreeSet::from(["spiffe://some-root/some-workload-2".into()]),
            vec![],
        );
        assert!(!is_tls_authorized(&tls, &authz))
    }

    #[test]
    fn is_authorized_for_matching_dns_ids() {
        let tls = server_tls("some.id.local");
        let authz = authorization(BTreeSet::from(["some.id.local".into()]), vec![]);
        assert!(is_tls_authorized(&tls, &authz))
    }

    #[test]
    fn is_not_authorized_for_non_matching_dns_ids() {
        let tls = server_tls("some.id.local.one");
        let authz = authorization(BTreeSet::from(["some.id.local.two".into()]), vec![]);
        assert!(!is_tls_authorized(&tls, &authz))
    }

    #[test]
    fn is_authorized_for_matching_dns_suffixes_ids() {
        let tls = server_tls("some.id.local");
        let authz = authorization(BTreeSet::new(), vec![Suffix::new("id.local")]);
        assert!(is_tls_authorized(&tls, &authz))
    }

    #[test]
    fn is_not_authorized_for_non_matching_suffixes_ids() {
        let tls = server_tls("some.id.local");
        let authz = authorization(BTreeSet::new(), vec![Suffix::new("another-id.local")]);
        assert!(!is_tls_authorized(&tls, &authz))
    }

    #[test]
    fn is_not_authorized_for_suffixes_and_spiffe_id() {
        let tls = server_tls("spiffe://some-root/some-workload-1");
        let authz = authorization(BTreeSet::new(), vec![Suffix::new("some-workload-1")]);
        assert!(!is_tls_authorized(&tls, &authz))
    }

    #[test]
    fn is_authorized_for_one_matching_spiffe_id() {
        let tls = server_tls("spiffe://some-root/some-workload-1");
        let authz = authorization(
            BTreeSet::from([
                "spiffe://some-root/some-workload-1".into(),
                "some.workload.one".into(),
                "some.workload.two".into(),
            ]),
            vec![],
        );
        assert!(is_tls_authorized(&tls, &authz))
    }

    #[test]
    fn is_authorized_for_one_matching_dns_id() {
        let tls = server_tls("some.workload.one");
        let authz = authorization(
            BTreeSet::from([
                "spiffe://some-root/some-workload-1".into(),
                "some.workload.one".into(),
                "some.workload.two".into(),
            ]),
            vec![],
        );
        assert!(is_tls_authorized(&tls, &authz))
    }
}

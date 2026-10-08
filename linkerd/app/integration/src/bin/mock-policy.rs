use futures::{stream, StreamExt as _};
use linkerd2_proxy_api::{
    destination,
    inbound::{self, inbound_server_policies_server},
    outbound::{self, outbound_policies_server},
};
use linkerd_app_integration::policy;
use std::{net::SocketAddr, pin::Pin};
use tokio_stream::Stream;
use tonic::{transport::Server, Request, Response, Status};

const DEFAULT_ADDR: &str = "127.0.0.1:8087";

type InboundStream =
    Pin<Box<dyn Stream<Item = Result<inbound::Server, Status>> + Send + Sync + 'static>>;
type OutboundStream =
    Pin<Box<dyn Stream<Item = Result<outbound::OutboundPolicy, Status>> + Send + Sync + 'static>>;

#[derive(Clone, Debug)]
struct Policy {
    inbound: inbound::Server,
    backend: SocketAddr,
    routes: Option<tokio::sync::watch::Receiver<std::sync::Arc<dmesh_routing::Manifest>>>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("MOCK_POLICY_ADDR")
        .unwrap_or_else(|_| DEFAULT_ADDR.to_string())
        .parse::<SocketAddr>()?;
    let backend = std::env::var("MOCK_POLICY_BACKEND")
        .unwrap_or_else(|_| "127.0.0.1:8086".to_string())
        .parse::<SocketAddr>()?;

    // MOCK_POLICY_REQUIRE_ID=<identity> serves an inbound policy that only
    // permits mesh-TLS clients with that identity (used to exercise the fused
    // DMA authz gate: allow when the flow's workload matches, deny otherwise).
    let (inbound, inbound_desc) = match std::env::var("MOCK_POLICY_REQUIRE_ID") {
        Ok(id) if !id.is_empty() => (
            policy::all_authenticated(id.clone()),
            format!("all-authenticated (require id={id})"),
        ),
        _ => (
            policy::all_unauthenticated(),
            "all-unauthenticated".to_string(),
        ),
    };

    let routes = dmesh_routing::from_env().map_err(std::io::Error::other)?;
    let svc = Policy {
        inbound,
        backend,
        routes,
    };

    eprintln!("mock policy serving on {addr}");
    eprintln!("inbound policy: {inbound_desc}");
    eprintln!("outbound policy: forward to {backend}");

    Server::builder()
        .add_service(inbound_server_policies_server::InboundServerPoliciesServer::new(svc.clone()))
        .add_service(outbound_policies_server::OutboundPoliciesServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}

#[tonic::async_trait]
impl inbound_server_policies_server::InboundServerPolicies for Policy {
    type WatchPortStream = InboundStream;

    async fn get_port(
        &self,
        req: Request<inbound::PortSpec>,
    ) -> Result<Response<inbound::Server>, Status> {
        let req = req.into_inner();
        eprintln!(
            "inbound get_port workload={} port={}",
            req.workload, req.port
        );
        Ok(Response::new(self.inbound.clone()))
    }

    async fn watch_port(
        &self,
        req: Request<inbound::PortSpec>,
    ) -> Result<Response<Self::WatchPortStream>, Status> {
        let req = req.into_inner();
        eprintln!(
            "inbound watch_port workload={} port={}",
            req.workload, req.port
        );
        let policy = self.inbound.clone();
        let stream = stream::once(async move { Ok(policy) }).chain(stream::pending());
        Ok(Response::new(Box::pin(stream)))
    }
}

#[tonic::async_trait]
impl outbound_policies_server::OutboundPolicies for Policy {
    type WatchStream = OutboundStream;

    async fn get(
        &self,
        req: Request<outbound::TrafficSpec>,
    ) -> Result<Response<outbound::OutboundPolicy>, Status> {
        let req = req.into_inner();
        if let Some(routes) = &self.routes {
            let manifest = routes.borrow();
            let addr = target_addr(&req)
                .ok_or_else(|| Status::invalid_argument("DMA routing requires an address"))?;
            let p = if let Some(s) = manifest.by_vip(addr) {
                policy::outbound_default(&s.discovery)
            } else if manifest.endpoint(addr).is_some() {
                forward_policy(addr)
            } else {
                return Err(Status::not_found("unknown DMA service/endpoint"));
            };
            return Ok(Response::new(p));
        }
        let backend = target_backend(&req).unwrap_or(self.backend);
        eprintln!("outbound get target={:?} backend={}", req.target, backend);
        Ok(Response::new(forward_policy(backend)))
    }

    async fn watch(
        &self,
        req: Request<outbound::TrafficSpec>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let req = req.into_inner();
        if let Some(routes) = &self.routes {
            let manifest = routes.borrow();
            let addr = target_addr(&req)
                .ok_or_else(|| Status::invalid_argument("DMA routing requires an address"))?;
            let p = if let Some(s) = manifest.by_vip(addr) {
                policy::outbound_default(&s.discovery)
            } else if manifest.endpoint(addr).is_some() {
                forward_policy(addr)
            } else {
                return Err(Status::not_found("unknown DMA service/endpoint"));
            };
            let stream = stream::once(async move { Ok(p) }).chain(stream::pending());
            return Ok(Response::new(Box::pin(stream)));
        }
        let backend = target_backend(&req).unwrap_or(self.backend);
        eprintln!("outbound watch target={:?} backend={}", req.target, backend);
        let policy = forward_policy(backend);
        let stream = stream::once(async move { Ok(policy) }).chain(stream::pending());
        Ok(Response::new(Box::pin(stream)))
    }
}

/// MOCK_POLICY_ECHO_TARGET=1: forward each flow to its own original
/// destination (per-dst sharding, used by the DMA N-driver benchmark where
/// each ingress/backend channel pair carries a distinct dst key).
fn target_backend(req: &outbound::TrafficSpec) -> Option<SocketAddr> {
    if std::env::var("MOCK_POLICY_ECHO_TARGET").is_err() {
        return None;
    }
    target_addr(req)
}

fn target_addr(req: &outbound::TrafficSpec) -> Option<SocketAddr> {
    use linkerd2_proxy_api::net::ip_address::Ip;
    match req.target.as_ref()? {
        outbound::traffic_spec::Target::Addr(a) => {
            let ip = match a.ip.as_ref()?.ip.as_ref()? {
                Ip::Ipv4(v4) => std::net::IpAddr::from(v4.to_be_bytes()),
                Ip::Ipv6(_) => return None,
            };
            Some(SocketAddr::new(ip, a.port as u16))
        }
        _ => None,
    }
}

fn forward_policy(addr: SocketAddr) -> outbound::OutboundPolicy {
    let mut policy = policy::outbound_default("local-backend.example:80");

    if let Some(outbound::ProxyProtocol {
        kind: Some(outbound::proxy_protocol::Kind::Detect(detect)),
    }) = policy.protocol.as_mut()
    {
        if let Some(http1) = detect.http1.as_mut() {
            for route in &mut http1.routes {
                replace_http_route_backends(route, addr);
            }
        }

        if let Some(http2) = detect.http2.as_mut() {
            for route in &mut http2.routes {
                replace_http_route_backends(route, addr);
            }
        }

        if let Some(opaque) = detect.opaque.as_mut() {
            for route in &mut opaque.routes {
                replace_opaque_route_backends(route, addr);
            }
        }
    }

    // MOCK_OUTBOUND_OPAQUE=1 replaces the Detect protocol with a BARE Opaque
    // protocol: the outbound stack then skips HTTP protocol detection and
    // L4-forwards the connection (no h2 termination), so we can measure the
    // proxy's cost with the h2 stack fully bypassed.
    if std::env::var("MOCK_OUTBOUND_OPAQUE").is_ok() {
        if let Some(outbound::ProxyProtocol {
            kind: Some(outbound::proxy_protocol::Kind::Detect(detect)),
        }) = policy.protocol.take()
        {
            let opaque = detect.opaque.unwrap_or_default();
            policy.protocol = Some(outbound::ProxyProtocol {
                kind: Some(outbound::proxy_protocol::Kind::Opaque(opaque)),
            });
        }
    }

    policy
}

fn replace_http_route_backends(route: &mut outbound::HttpRoute, addr: SocketAddr) {
    for rule in &mut route.rules {
        let Some(outbound::http_route::Distribution {
            kind: Some(outbound::http_route::distribution::Kind::FirstAvailable(first_available)),
        }) = rule.backends.as_mut()
        else {
            continue;
        };

        for backend in &mut first_available.backends {
            if let Some(backend) = backend.backend.as_mut() {
                replace_backend(backend, addr);
            }
        }
    }
}

fn replace_opaque_route_backends(route: &mut outbound::OpaqueRoute, addr: SocketAddr) {
    for rule in &mut route.rules {
        let Some(outbound::opaque_route::Distribution {
            kind: Some(outbound::opaque_route::distribution::Kind::FirstAvailable(first_available)),
        }) = rule.backends.as_mut()
        else {
            continue;
        };

        for backend in &mut first_available.backends {
            if let Some(backend) = backend.backend.as_mut() {
                replace_backend(backend, addr);
            }
        }
    }
}

fn replace_backend(backend: &mut outbound::Backend, addr: SocketAddr) {
    // MOCK_POLICY_TAGGED_ID=<identity> + MOCK_POLICY_TAGGED_PORT=<port> mark the
    // endpoint as a meshed peer: the outbound proxy then dials
    // (endpoint.ip, TAGGED_PORT) with mesh TLS and prepends a
    // TransportHeader{port: addr.port} — i.e. it sends traffic through a local
    // "server sidecar" proxy's inbound (direct stack) instead of straight to the
    // backend. This is what makes a 2-proxy (client+server sidecar) chain work
    // without iptables. Shape mirrors integration::controller::DestinationBuilder.
    let tagged = std::env::var("MOCK_POLICY_TAGGED_ID").ok().zip(
        std::env::var("MOCK_POLICY_TAGGED_PORT")
            .ok()
            .and_then(|p| p.parse::<u32>().ok()),
    );
    let (protocol_hint, tls_identity) = match tagged {
        Some((id, inbound_port)) => (
            Some(destination::ProtocolHint {
                protocol: None,
                opaque_transport: Some(destination::protocol_hint::OpaqueTransport {
                    inbound_port,
                }),
            }),
            Some(destination::TlsIdentity {
                strategy: Some(destination::tls_identity::Strategy::DnsLikeIdentity(
                    destination::tls_identity::DnsLikeIdentity { name: id.clone() },
                )),
                server_name: Some(destination::tls_identity::DnsLikeIdentity { name: id }),
            }),
        ),
        None => (None, None),
    };

    backend.kind = Some(outbound::backend::Kind::Forward(
        destination::WeightedAddr {
            addr: Some(
                addr.try_into()
                    .expect("socket addr must convert to protobuf"),
            ),
            weight: 1,
            metric_labels: Default::default(),
            protocol_hint,
            tls_identity,
            authority_override: None,
            http2: None,
            resource_ref: None,
        },
    ));
}

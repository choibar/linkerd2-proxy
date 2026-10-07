//! Feeds connections received over the DPU DMA path into the outbound stack.
//!
//! This is the DMA analogue of `serve::serve`: instead of a TCP listener it
//! consumes [`dmesh_doca::DmeshEvent`]s from the driver. On each `ConnReady`
//! it builds a [`DmeshTarget`] (carrying the flow's original destination, the
//! routing key normally recovered via `SO_ORIGINAL_DST`) and a
//! [`dmesh_doca::DmeshIo`], registers the IO handle with the driver, then
//! drives the outbound `NewService` on a per-connection task. The outbound
//! stack then applies protocol detection, discovery, load balancing and mTLS
//! exactly as for an intercepted TCP connection.

use std::future::Future;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use dmesh_doca::{dmesh_io_pair, DmeshEvent, DmeshIo, FlowId, Registrar};
use linkerd_app_core::{
    identity,
    svc::{self, NewService, Param, ServiceExt},
    tls,
    transport::addrs::{AddrPair, ClientAddr, OrigDstAddr, Remote, ServerAddr},
    Conditional,
};
use linkerd_app_inbound::policy::{dmesh_connection_authorized, AllowPolicy};
use tokio::sync::mpsc;
use tracing::{debug, debug_span, info, warn, Instrument};

/// Server-side target for a DMA-received connection. Mirrors the `Param` impls
/// of `linkerd_proxy_transport::orig_dst::Addrs` that the outbound stack reads,
/// plus the source workload identity (`client_id`) that the fused inbound-authz
/// gate consumes. Because the DMA path has no mTLS handshake, the source
/// identity is carried explicitly from the flow metadata and presented to
/// authorization as an already-established peer.
#[derive(Clone, Debug)]
pub struct DmeshTarget {
    orig_dst: OrigDstAddr,
    client: Remote<ClientAddr>,
    /// Source workload identity attested at DMA ingress, or `None` for an
    /// unauthenticated (plaintext-equivalent) flow.
    client_id: Option<tls::ClientId>,
}

impl DmeshTarget {
    /// Source workload identity, if the flow carried one.
    pub fn client_id(&self) -> Option<tls::ClientId> {
        self.client_id.clone()
    }
}

impl Param<OrigDstAddr> for DmeshTarget {
    fn param(&self) -> OrigDstAddr {
        self.orig_dst
    }
}

impl Param<Remote<ClientAddr>> for DmeshTarget {
    fn param(&self) -> Remote<ClientAddr> {
        self.client
    }
}

impl Param<AddrPair> for DmeshTarget {
    fn param(&self) -> AddrPair {
        let Remote(client) = self.client;
        AddrPair(client, ServerAddr(self.orig_dst.into()))
    }
}

/// The source identity presented to authorization. With a workload it reads as
/// an established mesh peer (no handshake — same-node in-process attestation);
/// without one it is `NoClientHello`, i.e. unauthenticated, so identity-gated
/// policies deny exactly as they would for a plaintext connection.
impl Param<tls::ConditionalServerTls> for DmeshTarget {
    fn param(&self) -> tls::ConditionalServerTls {
        match &self.client_id {
            Some(id) => Conditional::Some(tls::ServerTls::Established {
                client_id: Some(id.clone()),
                negotiated_protocol: None,
            }),
            None => Conditional::None(tls::NoServerTls::NoClientHello),
        }
    }
}

impl From<&FlowId> for DmeshTarget {
    fn from(flow: &FlowId) -> Self {
        // Parse the attested workload string into a mesh identity; an empty or
        // malformed workload yields no identity (unauthenticated).
        let client_id = if flow.workload.is_empty() {
            None
        } else {
            identity::Id::from_str(&flow.workload)
                .ok()
                .map(tls::ClientId)
        };
        Self {
            orig_dst: OrigDstAddr(SocketAddr::V4(flow.dst)),
            client: Remote(ClientAddr(SocketAddr::V4(flow.src))),
            client_id,
        }
    }
}

/// Looks up the destination's inbound authorization policy by original
/// destination address. A closure over the inbound policy client, so the fused
/// authz gate can enforce it without threading a concrete `GetPolicy` type.
pub type DmeshGetPolicy = Arc<dyn Fn(OrigDstAddr) -> AllowPolicy + Send + Sync>;

/// Serve DMA connections through the FUSED inbound-authz + outbound stack until
/// the event stream ends or shutdown is signalled. Each connection is gated by
/// the destination's inbound `AuthorizationPolicy` (the inbound role) using the
/// source identity carried in the flow, then served by the outbound stack
/// (routing/LB) — one L7 pass, no extra h2 termination. `outbound` is an
/// `ArcNewTcp<DmeshTarget, DmeshIo>`; `get_policy` resolves the inbound policy.
pub async fn serve<N>(
    mut events: mpsc::UnboundedReceiver<DmeshEvent>,
    registrar: Registrar,
    outbound: N,
    get_policy: DmeshGetPolicy,
    shutdown: impl Future,
) where
    N: NewService<DmeshTarget, Service = svc::BoxTcp<DmeshIo>> + Send + 'static,
{
    tokio::pin!(shutdown);
    let backend_owner = dmesh_doca::backend::new_owner();
    // slot -> published backend address, so ConnClosed can unpublish it.
    let mut backend_slots: std::collections::HashMap<usize, SocketAddr> =
        std::collections::HashMap::new();
    loop {
        let ev = tokio::select! {
            _ = &mut shutdown => return,
            ev = events.recv() => match ev {
                Some(ev) => ev,
                None => return,
            },
        };

        match ev {
            // A backend slot was torn down (its host process died or
            // disconnected): evict its not-yet-taken channel from the
            // registry so the connector refuses instead of handing out a
            // dead channel. Non-backend slots have nothing registered.
            DmeshEvent::ConnClosed(slot) => {
                if let Some(addr) = backend_slots.remove(&slot) {
                    dmesh_doca::backend::unpublish(backend_owner, slot, &addr);
                }
                continue;
            }
            DmeshEvent::ConnError(slot) | DmeshEvent::ConnDraining(slot) => {
                if let Some(addr) = backend_slots.get(&slot) {
                    dmesh_doca::backend::disable(backend_owner, slot, addr);
                }
                continue;
            }
            DmeshEvent::ConnReady(slot, flow) => {
                let peer = SocketAddr::V4(flow.src);
                let (io, handle) = dmesh_io_pair(peer);
                // Register the IO handle so the driver pumps recv segments into
                // it and picks up the stack's writes.
                if registrar
                    .send((slot, flow.key, flow.location, handle))
                    .is_err()
                {
                    warn!("dmesh driver gone; stopping acceptor");
                    return;
                }

                // BACKEND-mode connections are not inbound flows: the host end
                // provides the service at flow.dst, and the outbound connector
                // (DmeshOrTcp) picks this DmeshIo up from the registry instead
                // of dialing TCP. Nothing to serve here.
                if flow.is_backend {
                    let addr = SocketAddr::V4(flow.dst);
                    info!(slot, %addr, "dmesh backend channel ready");
                    backend_slots.insert(slot, addr);
                    dmesh_doca::backend::publish_on_worker(
                        backend_owner,
                        slot,
                        flow.location.worker as usize,
                        addr,
                        io,
                    );
                    continue;
                }

                let target = DmeshTarget::from(&flow);

                // INBOUND role: enforce the destination's authorization policy
                // at the connection level before applying outbound routing.
                // The source identity is carried in the target's server-tls
                // param (pre-attested; no mTLS handshake). Deny → close the
                // connection (h2 reset) without serving.
                //
                // get_policy returns a watch seeded with the startup default and
                // updated asynchronously once discovery responds; wait briefly
                // for the discovered policy so the decision isn't made against
                // the default. (Bounded so a connection is never blocked long.)
                let mut policy = get_policy(target.param());
                let _ =
                    tokio::time::timeout(std::time::Duration::from_millis(500), policy.changed())
                        .await;
                let tls: tls::ConditionalServerTls = target.param();
                let client: Remote<ClientAddr> = target.param();
                if !dmesh_connection_authorized(&policy, client, &tls) {
                    info!(
                        slot, src = %flow.src, dst = %flow.dst,
                        server = ?policy.server_label(),
                        "dmesh inbound authz DENIED; closing connection"
                    );
                    // Dropping `io` closes the connection; the driver tears the
                    // slot down. (`handle` was registered so teardown is clean.)
                    drop(io);
                    continue;
                }

                let span = debug_span!("dmesh", slot, src = %flow.src, orig_dst = %flow.dst);
                let svc = outbound.new_service(target);
                tokio::spawn(
                    async move {
                        match svc.oneshot(io).await {
                            Ok(()) => debug!("dmesh connection closed"),
                            Err(error) => debug!(%error, "dmesh connection failed"),
                        }
                    }
                    .instrument(span),
                );
            }
            DmeshEvent::InfraReady => info!("dmesh infrastructure ready"),
            DmeshEvent::Stats {
                elapsed_ms,
                recv_msgs,
                recv_bytes,
                sent_msgs,
                dma_pending,
                dma_dropped,
            } => {
                let secs = elapsed_ms as f64 / 1000.0;
                info!(
                    recv_msgs_per_s = (recv_msgs as f64 / secs) as i64,
                    recv_gbps = format_args!("{:.2}", (recv_bytes as f64 * 8.0) / secs / 1e9),
                    sent_msgs_per_s = (sent_msgs as f64 / secs) as i64,
                    dma_pending,
                    dma_dropped,
                    "dmesh datapath stats"
                )
            }
        }
    }
}

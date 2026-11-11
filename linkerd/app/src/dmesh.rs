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
    proxy::http,
    svc::{self, NewService, Param, ServiceExt},
    tls,
    transport::addrs::{AddrPair, ClientAddr, OrigDstAddr, Remote, ServerAddr},
    Conditional,
};
use linkerd_app_inbound::policy::{dmesh_connection_authorized, AllowPolicy};
use tokio::sync::{mpsc, watch};
use tracing::{debug, debug_span, info, warn, Instrument};

/// Counts each DMA client connection's retired instructions with its poll
/// time; `read` returns the calling thread's instruction count.
pub fn count_flow_instructions(read: fn() -> u64) {
    http::flow_stats::set_instruction_reader(read);
}

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

/// Builds the inbound role of one connection's HTTP service (access log,
/// request authorization, local rate limit) from its target and the
/// destination's inbound policy; see `linkerd_app_inbound::fused`.
pub type DmeshHttpWrap = Arc<dyn Fn(&DmeshTarget, AllowPolicy) -> http::conn_wrap::Wrap + Send + Sync>;

/// The destinations whose inbound policy belongs to another workload than the
/// proxy's own: `DMESH_INBOUND_WORKLOADS` names a file with one
/// `<destination IPv4> <policy workload>` pair per line, the workload as the
/// policy controller takes it (`{"ns":"<namespace>","pod":"<pod>"}`). A sidecar
/// asks for the policy of the one pod it runs in; this proxy is the inbound
/// enforcement point of every destination it serves, so it asks per destination.
pub fn inbound_workloads_from_env() -> std::collections::HashMap<std::net::IpAddr, Arc<str>> {
    let mut map = std::collections::HashMap::new();
    let Some(path) = std::env::var_os("DMESH_INBOUND_WORKLOADS") else {
        return map;
    };
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) => {
            warn!(?path, %error, "DMESH_INBOUND_WORKLOADS is unreadable; every destination uses the proxy's own workload");
            return map;
        }
    };
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        match line.split_once(char::is_whitespace) {
            Some((ip, workload)) => match ip.parse::<std::net::IpAddr>() {
                Ok(ip) => {
                    map.insert(ip, Arc::from(workload.trim()));
                }
                Err(error) => warn!(%line, %error, "DMESH_INBOUND_WORKLOADS: bad address"),
            },
            None => warn!(%line, "DMESH_INBOUND_WORKLOADS: expected '<ip> <workload>'"),
        }
    }
    info!(destinations = map.len(), "dmesh inbound policy workloads loaded");
    map
}

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
    http_wrap: DmeshHttpWrap,
    shutdown: impl Future,
) where
    N: NewService<DmeshTarget, Service = svc::BoxTcp<DmeshIo>> + Send + 'static,
{
    tokio::pin!(shutdown);
    let backend_owner = dmesh_doca::backend::new_owner();
    // slot -> published backend address, so ConnClosed can unpublish it.
    let mut backend_slots: std::collections::HashMap<usize, SocketAddr> =
        std::collections::HashMap::new();
    // slot -> the client connection's drain signal, fired to migrate it.
    let mut migrations: std::collections::HashMap<usize, watch::Sender<bool>> =
        std::collections::HashMap::new();
    // slot -> (worker, the client connection's flow statistics).
    let mut flow_stats: std::collections::HashMap<
        usize,
        (usize, Arc<dmesh_doca::flowstats::Counters>),
    > = std::collections::HashMap::new();
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
                migrations.remove(&slot);
                if let Some((worker, counters)) = flow_stats.remove(&slot) {
                    dmesh_doca::flowstats::unregister(worker, slot, &counters);
                }
                if let Some(addr) = backend_slots.remove(&slot) {
                    dmesh_doca::backend::unpublish(backend_owner, slot, &addr);
                }
                continue;
            }
            DmeshEvent::ConnMigrate(slot) => {
                if let Some(signal) = migrations.get(&slot) {
                    info!(slot, "dmesh connection migrating; sending GOAWAY");
                    let _ = signal.send(true);
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
                // The rest of the inbound role runs per request, around this
                // connection's HTTP service.
                let wrap = http_wrap(&target, policy);
                let svc = outbound.new_service(target);
                let (signal, drain) = watch::channel(false);
                migrations.insert(slot, signal);
                let serve = http::conn_drain::scope(
                    drain,
                    http::conn_wrap::scope(wrap, async move {
                        match svc.oneshot(io).await {
                            Ok(()) => debug!("dmesh connection closed"),
                            Err(error) => debug!(%error, "dmesh connection failed"),
                        }
                    }),
                );
                if http::flow_stats::enabled() {
                    // Count this connection's requests, CPU and backend wait
                    // for the dispatcher's flow placement.
                    let worker = flow.location.worker as usize;
                    let counters: Arc<http::flow_stats::Counters> = Arc::new(Default::default());
                    dmesh_doca::flowstats::register(worker, slot, counters.clone());
                    flow_stats.insert(slot, (worker, counters.clone()));
                    let serve = http::flow_stats::Cpu::new(
                        http::flow_stats::scope(counters.clone(), serve),
                        counters,
                    );
                    tokio::spawn(serve.instrument(span));
                } else {
                    tokio::spawn(serve.instrument(span));
                }
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

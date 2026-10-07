use crate::DmeshIo;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
};

/// A unique acceptor owner; slot numbers alone repeat across workers.
pub fn new_owner() -> usize {
    static NEXT: AtomicUsize = AtomicUsize::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

static ROUTES: OnceLock<
    Option<tokio::sync::watch::Receiver<std::sync::Arc<dmesh_routing::Manifest>>>,
> = OnceLock::new();

/// Validate configuration before opening any hardware resources.
pub fn init_routes() -> Result<(), String> {
    let routes = dmesh_routing::from_env()?;
    if shard_routes() && routes.is_none() {
        return Err("DMESH_SHARD_ROUTES requires DMESH_ROUTES".into());
    }
    if let Some(r) = &routes {
        let m = r.borrow();
        if let Ok(w) = std::env::var("DMESH_NUM_WORKERS") {
            if w.parse::<usize>().ok() != Some(m.workers) {
                return Err("manifest worker count mismatch".into());
            }
        }
        for s in m.services.values() {
            for e in &s.endpoints {
                seen().lock().unwrap().insert(e.dma);
            }
        }
    }
    ROUTES
        .set(routes)
        .map_err(|_| "routes already initialized".to_string())
}

extern "C" {
    fn dmesh_dispatch_listener_known(ip: u32, port: u16) -> i32;
}

/// DMA membership is declared before first publication. Keep tombstones so
/// removed endpoints cannot accidentally fall through to TCP on stale routes.
pub fn is_dma(addr: &SocketAddr) -> bool {
    if let SocketAddr::V4(a) = addr {
        let known =
            unsafe { dmesh_dispatch_listener_known(u32::from_ne_bytes(a.ip().octets()), a.port()) };
        if known != 0 {
            seen().lock().unwrap().insert(*addr);
            return true;
        }
    }
    if let Some(Some(routes)) = ROUTES.get() {
        let m = routes.borrow();
        let mut known = seen().lock().unwrap();
        for s in m.services.values() {
            for e in &s.endpoints {
                known.insert(e.dma);
            }
        }
        if m.by_vip(*addr).is_some() {
            return true;
        }
        if known.contains(addr) {
            return true;
        }
        if let Some(path) = std::env::var_os("DMESH_ROUTES") {
            if let Ok(latest) = dmesh_routing::Manifest::read(std::path::Path::new(&path)) {
                for s in latest.services.values() {
                    for e in &s.endpoints {
                        known.insert(e.dma);
                    }
                }
            }
        }
        return known.contains(addr);
    }
    was_published(addr)
}

tokio::task_local! { static WORKER: usize; }
/// Capture this when constructing a worker's stack; spawned connection tasks
/// need not inherit task-local state because pools/connectors retain the ID.
pub fn current_worker() -> usize {
    WORKER.try_with(|w| *w).unwrap_or(0)
}
pub async fn on_worker<F: std::future::Future>(worker: usize, future: F) -> F::Output {
    WORKER.scope(worker, future).await
}
fn requesters() -> &'static Mutex<HashMap<usize, tokio::sync::mpsc::Sender<SocketAddr>>> {
    static R: OnceLock<Mutex<HashMap<usize, tokio::sync::mpsc::Sender<SocketAddr>>>> =
        OnceLock::new();
    R.get_or_init(Default::default)
}
pub(crate) fn register_requester(worker: usize, sender: tokio::sync::mpsc::Sender<SocketAddr>) {
    requesters().lock().unwrap().insert(worker, sender);
}
/// Requests are control-only and idempotent at the dispatcher. Bounded queue;
/// a caller retries while awaiting publication, never from the data path.
pub fn request_connection(worker: usize, addr: SocketAddr) {
    if let Some(tx) = requesters().lock().unwrap().get(&worker) {
        let _ = tx.try_send(addr);
    }
}
pub fn worker_runtime(worker: usize) -> Option<tokio::runtime::Handle> {
    workers().lock().unwrap().get(&worker).cloned()
}

fn workers() -> &'static Mutex<HashMap<usize, tokio::runtime::Handle>> {
    static W: OnceLock<Mutex<HashMap<usize, tokio::runtime::Handle>>> = OnceLock::new();
    W.get_or_init(Default::default)
}
pub fn register_worker(worker: usize) {
    workers()
        .lock()
        .unwrap()
        .insert(worker, tokio::runtime::Handle::current());
}
pub fn shard_routes() -> bool {
    std::env::var("DMESH_SHARD_ROUTES").as_deref() == Ok("1")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Route {
    pub worker: usize,
    pub owner: usize,
    pub slot: usize,
    pub epoch: usize,
}
struct Candidate {
    route: Route,
    io: Option<DmeshIo>,
    draining: bool,
    retired: bool,
    live: Arc<AtomicBool>,
}
#[derive(Default)]
struct Replica {
    candidates: Vec<Candidate>,
    active: Option<Route>,
    leased: bool,
}
fn reg() -> &'static Mutex<HashMap<(usize, SocketAddr), Replica>> {
    static R: OnceLock<Mutex<HashMap<(usize, SocketAddr), Replica>>> = OnceLock::new();
    R.get_or_init(Default::default)
}
pub fn endpoint_runtime(
    worker: usize,
    addr: SocketAddr,
) -> Option<(Route, tokio::runtime::Handle)> {
    let mut registry = reg().lock().unwrap();
    let r = registry.get_mut(&(worker, addr))?;
    let route = select(r)?;
    if !r
        .candidates
        .iter()
        .any(|c| c.route == route && !c.draining && !c.retired)
    {
        return None;
    }
    let runtime = workers().lock().unwrap().get(&route.worker)?.clone();
    if r.leased {
        return None;
    }
    r.leased = true;
    Some((route, runtime))
}
pub fn endpoint_enabled(addr: SocketAddr) -> bool {
    match ROUTES.get().and_then(|r| r.as_ref()) {
        Some(r) => r.borrow().endpoint(addr).is_some_and(|e| e.enabled),
        None => true,
    }
}
fn select(r: &mut Replica) -> Option<Route> {
    if let Some(route) = r.active {
        return Some(route);
    }
    let route = r
        .candidates
        .iter()
        .find(|c| !c.draining && !c.retired && c.io.is_some())?
        .route;
    r.active = Some(route);
    Some(route)
}
pub fn route_live(addr: SocketAddr, route: Route) -> bool {
    reg()
        .lock()
        .unwrap()
        .get(&(route.worker, addr))
        .is_some_and(|r| {
            r.active == Some(route)
                && r.candidates
                    .iter()
                    .any(|c| c.route == route && !c.draining && !c.retired)
        })
}
/// Cached by the H2 owner: no registry mutex on each RPC. Epoch changes get a
/// different signal; old owners can never become live again through reuse.
pub fn route_signal(addr: SocketAddr, route: Route) -> Arc<AtomicBool> {
    reg()
        .lock()
        .unwrap()
        .get(&(route.worker, addr))
        .and_then(|r| r.candidates.iter().find(|c| c.route == route))
        .map(|c| c.live.clone())
        .unwrap_or_else(|| Arc::new(AtomicBool::new(false)))
}
/// Owner service must be dropped before releasing its lease. A replacement
/// requires both physical retirement and release of the old HTTP/2 owner.
pub fn release_route(addr: SocketAddr, route: Route) {
    let mut registry = reg().lock().unwrap();
    if let Some(r) = registry.get_mut(&(route.worker, addr)) {
        if r.active != Some(route) {
            return;
        }
        r.leased = false;
        if r.candidates.iter().any(|c| c.route == route && c.retired) {
            r.active = None;
            r.candidates.retain(|c| c.route != route);
        }
        refresh(route.worker, addr, r);
    }
}
pub struct Availability {
    count: AtomicUsize,
    changed: tokio::sync::watch::Sender<u64>,
}
impl Availability {
    pub fn is_live(&self) -> bool {
        self.count.load(Ordering::Acquire) != 0
    }
    pub async fn wait_live(&self) {
        let mut changes = self.changed.subscribe();
        while !self.is_live() {
            if changes.changed().await.is_err() {
                return;
            }
        }
    }
    fn set(&self, live: bool) {
        self.count.store(usize::from(live), Ordering::Release);
        self.changed.send_modify(|v| *v = v.wrapping_add(1));
    }
}
pub fn availability(addr: SocketAddr) -> std::sync::Arc<Availability> {
    availability_on_worker(0, addr)
}
pub fn availability_on_worker(worker: usize, addr: SocketAddr) -> std::sync::Arc<Availability> {
    static A: OnceLock<Mutex<HashMap<(usize, SocketAddr), std::sync::Arc<Availability>>>> =
        OnceLock::new();
    A.get_or_init(Default::default)
        .lock()
        .unwrap()
        .entry((worker, addr))
        .or_insert_with(|| {
            std::sync::Arc::new(Availability {
                count: AtomicUsize::new(0),
                changed: tokio::sync::watch::channel(0).0,
            })
        })
        .clone()
}
pub fn has_live(addr: SocketAddr) -> bool {
    availability(addr).is_live()
}
fn seen() -> &'static Mutex<HashSet<SocketAddr>> {
    static S: OnceLock<Mutex<HashSet<SocketAddr>>> = OnceLock::new();
    S.get_or_init(Default::default)
}
pub fn was_published(addr: &SocketAddr) -> bool {
    seen().lock().unwrap().contains(addr)
}

/// Compatibility helper for callers with a single runtime.
pub fn publish(owner: usize, slot: usize, addr: SocketAddr, io: DmeshIo) {
    publish_on_worker(owner, slot, 0, addr, io);
}
pub fn publish_on_worker(owner: usize, slot: usize, worker: usize, addr: SocketAddr, io: DmeshIo) {
    static EPOCH: AtomicUsize = AtomicUsize::new(1);
    let route = Route {
        owner,
        slot,
        worker,
        epoch: EPOCH.fetch_add(1, Ordering::Relaxed),
    };
    tracing::info!(?route, %addr, "dmesh backend candidate published");
    seen().lock().unwrap().insert(addr);
    let mut registry = reg().lock().unwrap();
    let r = registry.entry((worker, addr)).or_default();
    assert!(
        !r.candidates
            .iter()
            .any(|c| c.route.owner == owner && c.route.slot == slot && !c.retired),
        "backend slot reused before retirement"
    );
    r.candidates.push(Candidate {
        route,
        io: Some(io),
        draining: false,
        retired: false,
        live: Arc::new(AtomicBool::new(true)),
    });
    refresh(worker, addr, r);
}
fn refresh(worker: usize, addr: SocketAddr, r: &Replica) {
    let live = if let Some(active) = r.active {
        r.candidates
            .iter()
            .any(|c| c.route == active && !c.draining && !c.retired)
    } else {
        r.candidates.iter().any(|c| !c.draining && !c.retired)
    };
    availability_on_worker(worker, addr).set(live);
}
/// ERROR/CLOSING is not physical retirement. Keep the active reservation so a
/// spare cannot start while old DMA or exported memory is still outstanding.
pub fn disable(owner: usize, slot: usize, addr: &SocketAddr) {
    let mut registry = reg().lock().unwrap();
    for ((worker, address), r) in registry.iter_mut() {
        if address != addr {
            continue;
        }
        for c in &mut r.candidates {
            if c.route.owner == owner && c.route.slot == slot {
                c.live.store(false, Ordering::Release);
                c.draining = true;
                c.io = None;
            }
        }
        refresh(*worker, *addr, r);
    }
}
pub fn unpublish(owner: usize, slot: usize, addr: &SocketAddr) {
    let mut registry = reg().lock().unwrap();
    for ((worker, address), r) in registry.iter_mut() {
        if address != addr {
            continue;
        }
        for c in &mut r.candidates {
            if c.route.owner == owner && c.route.slot == slot {
                c.live.store(false, Ordering::Release);
                c.retired = true;
                c.draining = true;
                c.io = None;
                if !r.leased && r.active == Some(c.route) {
                    r.active = None;
                }
            }
        }
        r.candidates
            .retain(|c| !c.retired || r.active == Some(c.route));
        refresh(*worker, *addr, r);
    }
}
pub fn take(addr: &SocketAddr) -> Option<DmeshIo> {
    take_on_worker(0, addr)
}
pub fn take_on_worker(worker: usize, addr: &SocketAddr) -> Option<DmeshIo> {
    let mut registry = reg().lock().unwrap();
    let r = registry.get_mut(&(worker, *addr))?;
    let active = select(r)?;
    r.candidates
        .iter_mut()
        .find(|c| c.route == active && !c.draining && !c.retired)?
        .io
        .take()
}
pub fn contains(addr: &SocketAddr) -> bool {
    availability(*addr).is_live()
}

#[cfg(test)]
mod worker_tests {
    use super::*;
    #[tokio::test]
    async fn replica_routes_are_independent_per_worker() {
        let addr = "10.251.3.1:8080".parse().unwrap();
        let owners = [new_owner(), new_owner()];
        register_worker(21);
        register_worker(22);
        for (worker, owner) in [21, 22].into_iter().zip(owners) {
            let (io, _) = crate::dmesh_io_pair(addr);
            publish_on_worker(owner, 0, worker, addr, io);
        }
        let (a, _) = endpoint_runtime(21, addr).unwrap();
        assert_eq!(a.worker, 21);
        assert!(
            endpoint_runtime(21, addr).is_none(),
            "only one H2 owner per worker"
        );
        let (b, _) = endpoint_runtime(22, addr).unwrap();
        assert_eq!(b.worker, 22);
        assert!(
            take_on_worker(23, &addr).is_none(),
            "never borrow another worker's flow"
        );
        assert!(take_on_worker(21, &addr).is_some());
        assert!(take_on_worker(22, &addr).is_some());
        disable(owners[0], 0, &addr);
        assert!(!route_live(addr, a));
        assert!(
            route_live(addr, b),
            "closing one worker preserves the other"
        );
        unpublish(owners[0], 0, &addr);
        release_route(addr, a);
        unpublish(owners[1], 0, &addr);
        release_route(addr, b);
    }
}

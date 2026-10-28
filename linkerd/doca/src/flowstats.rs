//! Per-slot request counters for the dispatcher's flow placement. The acceptor
//! registers a client connection's counters (filled by the HTTP layer, see
//! `linkerd_proxy_http::flow_stats`) under its worker and slot; the driver
//! copies them to the slot's C connection, where the dispatcher reads them.

use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc, Mutex, OnceLock},
};

/// Cumulative: requests, CPU ns, residence ns, backend ns.
pub type Counters = [AtomicU64; 4];

type Registry = Mutex<HashMap<(usize, usize), Arc<Counters>>>;

fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(Default::default)
}

/// The connection on `slot` of `worker` counts into `counters`.
pub fn register(worker: usize, slot: usize, counters: Arc<Counters>) {
    registry().lock().unwrap().insert((worker, slot), counters);
}

/// The connection on `slot` closed; a later connection's counters stay.
pub fn unregister(worker: usize, slot: usize, counters: &Arc<Counters>) {
    let mut r = registry().lock().unwrap();
    if r.get(&(worker, slot))
        .is_some_and(|c| Arc::ptr_eq(c, counters))
    {
        r.remove(&(worker, slot));
    }
}

/// Each registered slot of `worker` with its counters' current values.
pub(crate) fn snapshot(worker: usize, out: &mut Vec<(usize, [u64; 4])>) {
    use std::sync::atomic::Ordering::Relaxed;
    out.clear();
    for ((w, slot), c) in registry().lock().unwrap().iter() {
        if *w == worker {
            out.push((
                *slot,
                [
                    c[0].load(Relaxed),
                    c[1].load(Relaxed),
                    c[2].load(Relaxed),
                    c[3].load(Relaxed),
                ],
            ));
        }
    }
}

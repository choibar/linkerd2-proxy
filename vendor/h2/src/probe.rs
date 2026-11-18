//! Request-path latency probe, `DMESH_LATPROBE=1` (a dmesh experiment).
//!
//! Each point keeps the number of requests that passed it and the sum of
//! their pass times on one monotonic clock. Between two points A and B the
//! requests' total time is, over a window [T0, T1] (Little's law),
//! `(S_B - S_A)|window + I(T1)*T1 - I(T0)*T0` with `I = n_A - n_B`, so the
//! mean time is that over the window's passes of B: no request is tracked
//! individually, which multiplexed HTTP/2 connections would make hard. A
//! thread prints every point's cumulative count and sum once a second
//! (`[dmesh-latprobe]`). Only passes on a dmesh shard thread count: the DMA
//! connections run there, and a pass that only some points see (a control
//! plane stream) would bias every section after it by a whole window.

use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Server: a request's HEADERS decoded (the client connection).
pub const SRV_RECV_REQ: usize = 0;
/// The proxy's service called with the request (residence start).
pub const RESIDENCE_START: usize = 1;
/// The request handed to the backend client (backend wait start).
pub const BACKEND_START: usize = 2;
/// Client: the request's HEADERS frame written to the codec.
pub const CLI_SEND_REQ: usize = 3;
/// Client: the response HEADERS decoded (the backend connection).
pub const CLI_RECV_RSP: usize = 4;
/// The backend client's response future ready (backend wait end).
pub const BACKEND_END: usize = 5;
/// The response body ended (residence end).
pub const RESIDENCE_END: usize = 6;
/// Server: the response's end-of-stream frame written to the codec.
pub const SRV_SEND_END: usize = 7;
const POINTS: usize = 8;

struct State {
    base: Instant,
    n: [u64; POINTS],
    sum_ns: [u64; POINTS],
}

fn state() -> Option<&'static Mutex<State>> {
    static STATE: OnceLock<Option<Mutex<State>>> = OnceLock::new();
    STATE
        .get_or_init(|| {
            std::env::var_os("DMESH_LATPROBE")?;
            std::thread::Builder::new()
                .name("dmesh-latprobe".into())
                .spawn(report)
                .ok()?;
            Some(Mutex::new(State {
                base: Instant::now(),
                n: [0; POINTS],
                sum_ns: [0; POINTS],
            }))
        })
        .as_ref()
}

/// Counts one request passing `point` now.
pub fn record(point: usize) {
    thread_local! {
        static SHARD: bool = std::thread::current()
            .name()
            .is_some_and(|n| n.starts_with("dmesh-shard"));
    }
    if !SHARD.with(|s| *s) {
        return;
    }
    if let Some(s) = state() {
        let mut s = s.lock().unwrap();
        let t = s.base.elapsed().as_nanos() as u64;
        s.n[point] += 1;
        s.sum_ns[point] += t;
    }
}

fn report() {
    loop {
        std::thread::sleep(Duration::from_secs(1));
        let Some(s) = state() else { return };
        let (mono, n, sum) = {
            let s = s.lock().unwrap();
            (s.base.elapsed().as_nanos() as u64, s.n, s.sum_ns)
        };
        let unix_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let join = |v: &[u64]| v.iter().map(u64::to_string).collect::<Vec<_>>().join(",");
        eprintln!(
            "[dmesh-latprobe] unix_ns={unix_ns} mono_ns={mono} n={} sum_ns={}",
            join(&n),
            join(&sum)
        );
    }
}

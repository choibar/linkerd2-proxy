//! Poll time and PMU counts of a dmesh shard's tasks by spawn site
//! (`DMESH_TASKSTAT=1`).
//!
//! Tokio's poll hooks time every task poll on the shard's runtime and add it
//! to the task's spawn site, the `file:line` of its `tokio::spawn`, together
//! with the shard thread's retired instructions, cycles and L1i refills over
//! the poll. The counters are per-thread perf events read from user space
//! (arm64 `rdpmc`, which needs `kernel.perf_user_access=1`), so a poll costs
//! a few register reads rather than syscalls. Without user access only time
//! is recorded.
//!
//! Once a second the shard prints, per site, the polls, poll time and counts
//! since its last line, and the thread's totals over the same interval, so
//! its CPU can be split by task kind: what no site covers (the runtime's I/O
//! and timer driver, the `block_on` future) is the total minus their sum.
//! Needs `--cfg tokio_unstable`; off, the runtime has no hooks.

use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    panic::Location,
    time::{Duration, Instant},
};

type Site = &'static Location<'static>;

/// Raw PMUv3 events counted: INST_RETIRED, CPU_CYCLES, L1I_CACHE_REFILL.
const EVENTS: [u64; 3] = [0x08, 0x11, 0x01];
type Counts = [u64; 3];

#[derive(Default)]
struct Stat {
    polls: u64,
    ns: u64,
    counts: Counts,
}

thread_local! {
    static PMU: Option<Vec<dmesh_doca::pmu::Counter>> = open_pmu();
    static POLL_START: Cell<Option<(Instant, Counts)>> = const { Cell::new(None) };
    // site address -> (site, stats)
    static SITES: RefCell<HashMap<usize, (Site, Stat)>> = RefCell::new(HashMap::new());
}

fn open_pmu() -> Option<Vec<dmesh_doca::pmu::Counter>> {
    let counters: Option<Vec<_>> = EVENTS
        .iter()
        .map(|&e| dmesh_doca::pmu::Counter::open(e))
        .collect();
    if counters.is_none() {
        eprintln!("taskstat: no user-space PMU access (kernel.perf_user_access=1?); timing only");
    }
    counters
}

fn read_counts() -> Counts {
    PMU.with(|pmu| {
        let mut c = [0; 3];
        if let Some(pmu) = pmu {
            for (c, counter) in c.iter_mut().zip(pmu) {
                *c = counter.read();
            }
        }
        c
    })
}

/// Whether shards should time their tasks (`DMESH_TASKSTAT=1`).
pub fn enabled() -> bool {
    std::env::var("DMESH_TASKSTAT").is_ok_and(|v| v == "1")
}

/// Times every task poll of the runtime `builder` builds.
pub fn install(builder: &mut tokio::runtime::Builder) {
    #[cfg(tokio_unstable)]
    builder
        .on_before_task_poll(|_| {
            let counts = read_counts();
            POLL_START.with(|s| s.set(Some((Instant::now(), counts))))
        })
        .on_after_task_poll(|meta| {
            let Some((start, before)) = POLL_START.with(Cell::take) else {
                return;
            };
            let ns = start.elapsed().as_nanos() as u64;
            let after = read_counts();
            let site = meta.spawned_at();
            SITES.with(|sites| {
                let mut sites = sites.borrow_mut();
                let (_, s) = sites
                    .entry(site as *const _ as usize)
                    .or_insert((site, Stat::default()));
                s.polls += 1;
                s.ns += ns;
                for i in 0..3 {
                    s.counts[i] += after[i].wrapping_sub(before[i]);
                }
            });
        });
    #[cfg(not(tokio_unstable))]
    let _ = builder;
}

/// Prints this shard's per-site stats every second, as `taskstat unix_ns=..
/// worker=.. site=file:line polls=.. poll_ns=.. instructions=.. cycles=..
/// l1i_refills=..`, then the thread's totals over the interval as a
/// `site=thread` line (`polls` 0, `poll_ns` the interval), then each client
/// connection's cumulative requests, poll time and instructions as
/// `taskflow unix_ns=.. worker=.. slot=.. requests=.. cpu_ns=.. instructions=..`.
pub async fn report(worker: usize) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut last = (Instant::now(), read_counts());
    loop {
        tick.tick().await;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let line = |site: &str, s: &Stat| {
            eprintln!(
                "taskstat unix_ns={now} worker={worker} site={site} polls={} poll_ns={} \
                 instructions={} cycles={} l1i_refills={}",
                s.polls, s.ns, s.counts[0], s.counts[1], s.counts[2]
            )
        };
        let sites: Vec<_> = SITES.with(|s| s.borrow_mut().drain().map(|(_, v)| v).collect());
        for (site, s) in &sites {
            line(&format!("{}:{}", site.file(), site.line()), s);
        }
        let counts = read_counts();
        let mut total = Stat {
            polls: 0,
            ns: last.0.elapsed().as_nanos() as u64,
            counts: [0; 3],
        };
        for i in 0..3 {
            total.counts[i] = counts[i].wrapping_sub(last.1[i]);
        }
        line("thread", &total);
        last = (Instant::now(), counts);
        // Each counted client connection's cumulative flow statistics.
        for (slot, c) in dmesh_doca::flowstats::snapshot_all(worker) {
            eprintln!(
                "taskflow unix_ns={now} worker={worker} slot={slot} requests={} cpu_ns={} \
                 instructions={}",
                c[0], c[1], c[4]
            );
        }
    }
}

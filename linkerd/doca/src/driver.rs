//! Async driver for the DMesh control and data paths.
//!
//! Mirrors the event-driven C worker (`run_dpu_worker_event_driven` in
//! DPUMesh/dpu_worker.c): both DOCA progress-engine notification fds are
//! registered with the tokio reactor via `AsyncFd`, and the shared
//! `dmesh_doca_ctrl_advance` state machine (comch_server.c) sequences the
//! per-connection setup. The driver additionally diffs per-slot connection
//! states after every advance and emits [`DmeshEvent`]s, which is the
//! foundation the future `DmeshBind` acceptor consumes.

use std::os::raw::{c_int, c_void};
use std::os::unix::io::{AsRawFd, RawFd};
use std::time::Instant;

use tokio::io::unix::AsyncFd;
use tokio::io::Interest;
use tokio::sync::mpsc;

use crate::{DmeshDoca, Error};

extern "C" {
    fn dmesh_doca_ctrl_get_fd(objs: *mut c_void, out_fd: *mut c_int) -> c_int;
    fn dmesh_doca_ctrl_arm(objs: *mut c_void) -> c_int;
    fn dmesh_doca_ctrl_drain(objs: *mut c_void) -> c_int;
    fn dmesh_doca_ctrl_clear_and_drain(objs: *mut c_void, fd: c_int) -> c_int;
    fn dmesh_doca_ctrl_advance(objs: *mut c_void, out_state: *mut c_int) -> c_int;

    fn dmesh_doca_data_get_fd(objs: *mut c_void, out_fd: *mut c_int) -> c_int;
    fn dmesh_doca_data_arm(objs: *mut c_void) -> c_int;
    fn dmesh_doca_data_clear_and_drain(
        objs: *mut c_void,
        fd: c_int,
        budget: c_int,
        out_drained: *mut c_int,
    ) -> c_int;

    fn dmesh_doca_max_conns() -> c_int;
    fn dmesh_doca_conn_state_get(objs: *mut c_void, slot: i32) -> i32;
    fn dmesh_doca_conn_readers_detached(objs: *mut c_void, slot: i32) -> c_int;
    fn dmesh_doca_conn_flow_get(
        objs: *mut c_void,
        slot: i32,
        src_ip: *mut u32,
        src_port: *mut u16,
        dst_ip: *mut u32,
        dst_port: *mut u16,
        workload: *mut std::os::raw::c_char,
        workload_len: i32,
    ) -> c_int;
    fn dmesh_doca_stats_get(
        objs: *mut c_void,
        sent: *mut i64,
        recv: *mut i64,
        recv_bytes: *mut i64,
        dma_pending: *mut i64,
        dma_dropped: *mut i64,
    );

    fn dmesh_doca_conn_staging_base(
        objs: *mut c_void,
        slot: i32,
        out_base: *mut *const u8,
        out_len: *mut usize,
    ) -> c_int;
    fn dmesh_doca_conn_recv_pop(
        objs: *mut c_void,
        slot: i32,
        out_pos: *mut u32,
        out_len: *mut u32,
    ) -> c_int;
    // Used once staging-buffer flow control lands (read watermark -> DPA).
    #[allow(dead_code)]
    fn dmesh_doca_conn_rx_watermark(objs: *mut c_void, slot: i32, pos: u32) -> i32;
    fn dmesh_doca_conn_recv_release(
        objs: *mut c_void,
        slot: i32,
        pos: u32,
        len: u32,
    ) -> c_int;
    // Flow mode of a slot: 0 = client, 1 = backend provider.
    fn dmesh_doca_conn_mode_get(objs: *mut c_void, slot: i32) -> i32;
    // Report the connection's mapped tx_staging region (usable base + len) so
    // the writer stages response bytes in place. Negative doca_error_t until
    // the reverse path is ready.
    // Free buffers parked by conn teardown; call only after the dead slots'
    // IO handles were marked via clear_tx_staging/clear_rx_staging.
    fn dmesh_doca_reap_graves(objs: *mut c_void);

    fn dmesh_doca_data_drain_only(objs: *mut c_void, budget: c_int, out_drained: *mut c_int) -> c_int;

    fn dmesh_doca_conn_tx_staging(
        objs: *mut c_void,
        slot: i32,
        out_base: *mut usize,
        out_len: *mut usize,
    ) -> i32;
    // Publish response bytes already staged at [pos, pos+len) (no memcpy).
    // Returns bytes accepted (>=0, may be < len) or a negative doca_error_t.
    fn dmesh_doca_conn_send_staged(objs: *mut c_void, slot: i32, pos: u32, len: u32) -> i32;
}

/// Global init state (mirrors `enum dmesh_doca_init_state` in comch_server.h)
const STATE_ERROR: c_int = -1;
const STATE_RUNNING: c_int = 1;

/// Per-connection state (mirrors `enum dmesh_conn_state` in object.h)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnState {
    Free,
    New,
    ConsumerStarting,
    AwaitMetadata,
    Running,
    Error,
    Closing,
}

impl ConnState {
    fn from_raw(v: i32) -> Self {
        match v {
            1 => Self::New,
            2 => Self::AwaitMetadata,
            3 => Self::Running,
            4 => Self::Error,
            5 => Self::ConsumerStarting,
            6 => Self::Closing,
            _ => Self::Free,
        }
    }
}

/// Identity of the flow carried by a dmesh connection, conveyed explicitly in
/// the host's metadata message because the DMA path has no TCP/IP headers.
/// `dst` is the ORIGINAL destination (what iptables interception used to
/// recover via SO_ORIGINAL_DST) and is the outbound routing key; `src` stands
/// in for the peer address a TCP accept would have provided.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlowId {
    pub src: std::net::SocketAddrV4,
    pub dst: std::net::SocketAddrV4,
    /// Source workload identity (pod / service-account) for policy & telemetry.
    pub workload: String,
    /// True for a BACKEND-mode connection: the host end provides the service
    /// at `dst`; the connector reaches it through this channel instead of TCP.
    pub is_backend: bool,
}

/// Events emitted by the driver as the shared state machine progresses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DmeshEvent {
    /// Shared infrastructure (DPA pool, consumer PE) is up; serving connections.
    InfraReady,
    /// The connection in this slot completed setup and its DPA thread is
    /// running; the flow identity came from the host's metadata message.
    ConnReady(usize, FlowId),
    /// The connection in this slot was unbound (host disconnected).
    ConnClosed(usize),
    /// Setup of the connection in this slot failed; the slot is parked.
    ConnError(usize),
    /// Periodic datapath report (deltas over `elapsed_ms`, emitted only when
    /// traffic flowed). Rates: msgs/s = recv_msgs * 1000 / elapsed_ms.
    Stats {
        elapsed_ms: u64,
        recv_msgs: i64,
        recv_bytes: i64,
        sent_msgs: i64,
        dma_pending: i64,
        dma_dropped: i64,
    },
}

/// Max connection slots per driver (mirrors DMESH_MAX_CONNECTIONS).
pub const MAX_CONNS: usize = 32;

/// Max consumer-PE events drained per loop iteration, matching the C worker's
/// DATA_DRAIN_BUDGET: bounds each wakeup so the control path cannot starve.
const DATA_DRAIN_BUDGET: c_int = 8192;

/// Wraps a DOCA PE notification fd for AsyncFd registration. The fd is owned
/// by the progress engine for the engine's lifetime, so this type must never
/// close it (it implements only AsRawFd; there is no Drop).
struct PeFd(RawFd);

impl AsRawFd for PeFd {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

/// Aggregate datapath counters (monotonic, see `dmesh_doca_stats_get`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub sent: i64,
    pub recv: i64,
    pub recv_bytes: i64,
    pub dma_pending: i64,
    pub dma_dropped: i64,
}

/// Owns the DOCA handle and drives the control/data paths from a single task.
///
/// All FFI calls go through `&mut self`, so the underlying `struct objects`
/// (and its non-thread-safe progress engines) is only ever touched by the one
/// task that owns the `Driver`.
/// Registration message from the acceptor: bind a per-connection IO handle to
/// a slot so the driver can pump recv segments into it and pick up its writes.
pub type Registration = (usize, crate::DmeshIoHandle);
/// Sender the acceptor uses to register connection handles with the driver.
pub type Registrar = mpsc::UnboundedSender<Registration>;

pub struct Driver {
    doca: DmeshDoca,
    events: mpsc::UnboundedSender<DmeshEvent>,
    reg_rx: mpsc::UnboundedReceiver<Registration>,
    conn_states: [ConnState; MAX_CONNS],
    handles: [Option<crate::DmeshIoHandle>; MAX_CONNS],
    // Set once any connection on this worker has been torn down: from then on
    // the consumer PE is driven by the 1ms tick (drain-only), never by
    // arm/clear_notification - teardown corrupts that path inside libdoca.
    saw_teardown: bool,

    // Whether a slot's tx_staging region has been reported to its handle yet.
    // The reverse path becomes ready a tick or two after registration, so this
    // is retried until `dmesh_doca_conn_tx_staging` succeeds.
    tx_set: [bool; MAX_CONNS],
    // DPU-internal latency probe: t_in set when a forward segment is delivered,
    // measured when the response leaves via pump_send. (linkerd + backend time.)
    t_in: Option<std::time::Instant>,
    dpu_sum_us: u64,
    dpu_cnt: u64,
}

// SAFETY: the raw DOCA handle is only dereferenced through &mut self, and a
// Driver is owned by exactly one task at a time; moving it between threads
// (e.g. across a tokio work-steal) is safe because accesses never overlap.
unsafe impl Send for Driver {}

fn check(code: c_int) -> Result<(), Error> {
    if code == 0 {
        Ok(())
    } else {
        Err(Error::from_doca(code))
    }
}

/// Clearing both pointers takes the IO mutex, waiting for any concurrent copy.
/// The C side may release staging only after the acknowledgement succeeds.
fn detach_readers(
    handle: &mut Option<crate::DmeshIoHandle>,
    acknowledge: impl FnOnce() -> Result<(), Error>,
) -> Result<(), Error> {
    if let Some(handle) = handle.take() {
        handle.clear_tx_staging();
        handle.clear_rx_staging();
    }
    acknowledge()
}

impl Driver {
    /// Build the driver and its registrar. The event receiver (paired with
    /// `events`) and the `Registrar` are given to the acceptor.
    pub fn new(doca: DmeshDoca, events: mpsc::UnboundedSender<DmeshEvent>) -> (Self, Registrar) {
        debug_assert_eq!(unsafe { dmesh_doca_max_conns() } as usize, MAX_CONNS);
        let (reg_tx, reg_rx) = mpsc::unbounded_channel();
        const NONE: Option<crate::DmeshIoHandle> = None;
        let driver = Self {
            doca,
            events,
            reg_rx,
            conn_states: [ConnState::Free; MAX_CONNS],
            handles: [NONE; MAX_CONNS],
            saw_teardown: false,
            tx_set: [false; MAX_CONNS],
            t_in: None,
            dpu_sum_us: 0,
            dpu_cnt: 0,
        };
        (driver, reg_tx)
    }

    /// Install any pending IO handles, wiring each to its slot's staging region.
    fn drain_registrations(&mut self) {
        while let Ok((slot, handle)) = self.reg_rx.try_recv() {
            if slot >= MAX_CONNS
                || ConnState::from_raw(unsafe {
                    dmesh_doca_conn_state_get(self.doca.raw(), slot as i32)
                }) != ConnState::Running
            {
                // A delayed registration must not resurrect a closing flow.
                handle.clear_tx_staging();
                handle.clear_rx_staging();
                continue;
            }
            // The replaced IO may still be used on another runtime thread.
            let _ = detach_readers(&mut self.handles[slot], || Ok(()));
            let mut base: *const u8 = std::ptr::null();
            let mut len: usize = 0;
            let rc = unsafe {
                dmesh_doca_conn_staging_base(self.doca.raw(), slot as i32, &mut base, &mut len)
            };
            if rc == 0 && !base.is_null() {
                handle.set_staging(base as usize, len);
            }
            self.tx_set[slot] = false;
            self.handles[slot] = Some(handle);
        }
    }

    /// Report each slot's tx_staging region to its handle once the reverse path
    /// is ready (retried every tick until it succeeds). Enables write-side
    /// zero-copy: the stack copies response bytes straight into staging.
    fn wire_tx_staging(&mut self) {
        for slot in 0..MAX_CONNS {
            if self.tx_set[slot] {
                continue;
            }
            let Some(handle) = self.handles[slot].as_ref() else {
                continue;
            };
            let mut base: usize = 0;
            let mut len: usize = 0;
            let rc =
                unsafe { dmesh_doca_conn_tx_staging(self.doca.raw(), slot as i32, &mut base, &mut len) };
            if rc == 0 && base != 0 && len > 0 {
                handle.set_tx_staging(base, len);
                self.tx_set[slot] = true;
            }
        }
    }

    /// Pump completed recv segments from every registered slot into its handle.
    fn pump_recv(&mut self) {
        for slot in 0..MAX_CONNS {
            let Some(handle) = self.handles[slot].as_ref() else {
                continue;
            };
            loop {
                let mut pos: u32 = 0;
                let mut len: u32 = 0;
                let rc = unsafe {
                    dmesh_doca_conn_recv_pop(self.doca.raw(), slot as i32, &mut pos, &mut len)
                };
                if rc != 0 {
                    break; // DOCA_ERROR_EMPTY or error
                }
                if self.t_in.is_none() {
                    self.t_in = Some(std::time::Instant::now());
                }
                handle.push_segment(pos, len);
            }
        }
    }

    /// Publish response bytes the stack staged (write-side zero-copy: the bytes
    /// are already in tx_staging) back to the host over the reverse DMA path.
    /// Each contiguous unpublished run is handed to the C side, which emits DMA
    /// descriptors / push batches without copying. Partial acceptance (ring or
    /// backend batch momentarily full) is retried on the next tick.
    fn pump_send(&mut self) {
        self.wire_tx_staging();
        for slot in 0..MAX_CONNS {
            let Some(handle) = self.handles[slot].as_ref() else {
                continue;
            };
            loop {
                let Some((pos, len)) = handle.take_staged() else {
                    break;
                };
                let rc =
                    unsafe { dmesh_doca_conn_send_staged(self.doca.raw(), slot as i32, pos, len) };
                if rc < 0 {
                    // reverse path not ready yet (BAD_STATE) or error: retry later
                    break;
                }
                let sent = rc as u32;
                if sent > 0 {
                    handle.advance_publish(sent);
                    if let Some(t) = self.t_in.take() {
                        self.dpu_sum_us += t.elapsed().as_micros() as u64;
                        self.dpu_cnt += 1;
                        if self.dpu_cnt % 200 == 0 {
                            eprintln!(
                                "[dmesh-lat] DPU-internal (fwd-arrive -> resp-send) mean {} us over {}",
                                self.dpu_sum_us / self.dpu_cnt,
                                self.dpu_cnt
                            );
                        }
                    }
                }
                if sent < len {
                    // ring / batch momentarily full: retry the rest next tick
                    break;
                }
                // fully accepted; loop to drain the wrapped remainder if any
            }
        }
    }

    pub fn stats(&self) -> Stats {
        let mut s = Stats::default();
        unsafe {
            dmesh_doca_stats_get(
                self.doca.raw(),
                &mut s.sent,
                &mut s.recv,
                &mut s.recv_bytes,
                &mut s.dma_pending,
                &mut s.dma_dropped,
            )
        };
        s
    }

    /// Emit a Stats event once per second while traffic is flowing. Runs on
    /// every loop iteration; when the driver is asleep (idle) nothing flows,
    /// so nothing needs reporting.
    fn maybe_report_stats(&mut self, last: &mut Instant, prev: &mut Stats) {
        let elapsed = last.elapsed();
        if elapsed.as_millis() < 1000 {
            return;
        }
        let cur = self.stats();
        if cur.recv != prev.recv || cur.sent != prev.sent {
            let _ = self.events.send(DmeshEvent::Stats {
                elapsed_ms: elapsed.as_millis() as u64,
                recv_msgs: cur.recv - prev.recv,
                recv_bytes: cur.recv_bytes - prev.recv_bytes,
                sent_msgs: cur.sent - prev.sent,
                dma_pending: cur.dma_pending,
                dma_dropped: cur.dma_dropped,
            });
        }
        *prev = cur;
        *last = Instant::now();
    }

    /// Read the flow identity the host reported for a slot's connection.
    fn conn_flow(&self, slot: usize) -> FlowId {
        let (mut src_ip, mut dst_ip) = (0u32, 0u32);
        let (mut src_port, mut dst_port) = (0u16, 0u16);
        let mut workload = [0i8 as std::os::raw::c_char; 64];
        unsafe {
            dmesh_doca_conn_flow_get(
                self.doca.raw(),
                slot as i32,
                &mut src_ip,
                &mut src_port,
                &mut dst_ip,
                &mut dst_port,
                workload.as_mut_ptr(),
                workload.len() as i32,
            );
        }
        let workload = unsafe { std::ffi::CStr::from_ptr(workload.as_ptr()) }
            .to_string_lossy()
            .into_owned();
        // inet_addr() stores the address bytes in network order in memory;
        // both PCIe endpoints are little-endian, so the raw u32's LE bytes
        // are exactly the network-order octets.
        let is_backend =
            unsafe { dmesh_doca_conn_mode_get(self.doca.raw(), slot as i32) } == 1;
        FlowId {
            src: std::net::SocketAddrV4::new(src_ip.to_le_bytes().into(), src_port),
            dst: std::net::SocketAddrV4::new(dst_ip.to_le_bytes().into(), dst_port),
            workload,
            is_backend,
        }
    }

    fn advance(&mut self) -> Result<c_int, Error> {
        // Backends can be taken by another shard through the shared registry.
        // Fence their Rust readers/writers before C may destroy any mappings.
        for slot in 0..MAX_CONNS {
            let raw = self.doca.raw();
            if ConnState::from_raw(unsafe { dmesh_doca_conn_state_get(raw, slot as i32) })
                != ConnState::Closing
            {
                continue;
            }
            self.tx_set[slot] = false;
            self.saw_teardown = true;
            detach_readers(&mut self.handles[slot], || {
                check(unsafe { dmesh_doca_conn_readers_detached(raw, slot as i32) })
            })?;
        }
        let mut state: c_int = 0;
        check(unsafe { dmesh_doca_ctrl_advance(self.doca.raw(), &mut state) })?;
        if state == STATE_ERROR {
            return Err(Error::new(-1, "dmesh control state machine failed"));
        }
        Ok(state)
    }

    /// Diff per-slot connection states against the last observed snapshot and
    /// emit an event for every transition an acceptor cares about.
    fn emit_conn_events(&mut self) {
        for slot in 0..MAX_CONNS {
            let cur = ConnState::from_raw(unsafe {
                dmesh_doca_conn_state_get(self.doca.raw(), slot as i32)
            });
            let prev = self.conn_states[slot];
            if cur == prev {
                continue;
            }
            self.conn_states[slot] = cur;

            let ev = match cur {
                ConnState::Running => Some(DmeshEvent::ConnReady(slot, self.conn_flow(slot))),
                ConnState::Error => Some(DmeshEvent::ConnError(slot)),
                ConnState::Free if prev != ConnState::Free => Some(DmeshEvent::ConnClosed(slot)),
                _ => None, // New / ConsumerStarting / AwaitMetadata are internal setup states
            };

            // A slot leaving Running tears down its IO: mark tx staging dead
            // (its mapping is about to be freed), signal EOF to the reader, and
            // drop the handle so its connection task finishes.
            if matches!(cur, ConnState::Free | ConnState::Error) {
                if let Some(handle) = self.handles[slot].take() {
                    handle.clear_tx_staging();
                    handle.clear_rx_staging();
                }
                self.tx_set[slot] = false;
                self.saw_teardown = true;
            }

            if let Some(ev) = ev {
                // A dropped receiver just means nobody is listening; keep serving.
                let _ = self.events.send(ev);
            }
        }
    }

    /// Run the driver until an unrecoverable error. Mirrors the C event loop:
    /// arm both PEs -> drain control -> bounded data drain -> advance ->
    /// emit events -> sleep on either fd unless the data budget was exhausted.
    pub async fn run(mut self) -> Result<(), Error> {
        // Build the shared infrastructure (DPA pool, consumer PE, ...) before
        // serving connections; this also makes the consumer PE fd available.
        let state = self.advance()?;
        if state != STATE_RUNNING {
            return Err(Error::new(-1, "dmesh infrastructure did not reach RUNNING"));
        }
        let _ = self.events.send(DmeshEvent::InfraReady);

        let mut ctrl_fd: c_int = -1;
        check(unsafe { dmesh_doca_ctrl_get_fd(self.doca.raw(), &mut ctrl_fd) })?;
        let mut data_fd: c_int = -1;
        check(unsafe { dmesh_doca_data_get_fd(self.doca.raw(), &mut data_fd) })?;

        let ctrl = AsyncFd::with_interest(PeFd(ctrl_fd), Interest::READABLE)
            .map_err(|e| Error::new(-1, format!("register ctrl fd: {e}")))?;
        let data = AsyncFd::with_interest(PeFd(data_fd), Interest::READABLE)
            .map_err(|e| Error::new(-1, format!("register data fd: {e}")))?;

        let mut stats_last = Instant::now();
        let mut stats_prev = self.stats();

        // DMESH_BUSY_POLL=1 polls both progress engines in a tight loop instead
        // of sleeping on their notification fds. Profiling the datapath showed
        // epoll_pwait taking >50% of the core at saturation: with a request/
        // response workload each wakeup carries only a message or two, so the
        // per-wakeup cost (epoll round-trip + PE re-arm) is paid per message
        // rather than amortized over a batch. Busy-polling trades an always-hot
        // core for removing that cost. (Mirrors DPUMESH_BUSY_POLL in the C
        // worker, dpu_worker.c.)
        let busy_poll = std::env::var("DMESH_BUSY_POLL")
            .map(|v| v != "0" && !v.is_empty())
            .unwrap_or(false);
        if busy_poll {
            tracing::info!("dmesh driver: busy-poll mode (progress engines polled, fds unused)");
        }

        loop {
            // Arm first so events pending now (or arriving during the drains
            // below) signal the fds; the eager drain+advance closes the race
            // where a setup step already consumed the awaited event. Both PEs
            // run in PROGRESS_ALL mode, so arming clears prior notifications.
            // Busy-poll never waits on the fds, so arming (and its per-iteration
            // syscall/doorbell cost) is skipped entirely.
            if !busy_poll {
                check(unsafe { dmesh_doca_ctrl_arm(self.doca.raw()) })?;
                // After the first teardown the data PE's notification state is
                // poisoned inside libdoca (clear_notification hits a NULL
                // internal pointer); stop arming it - the 1ms safety-net tick
                // below keeps the datapath live via drain-only polling.
                if !self.saw_teardown {
                    check(unsafe { dmesh_doca_data_arm(self.doca.raw()) })?;
                }
            }

            check(unsafe { dmesh_doca_ctrl_drain(self.doca.raw()) })?;
            let mut drained: c_int = 0;
            if self.saw_teardown {
                check(unsafe {
                    dmesh_doca_data_drain_only(self.doca.raw(), DATA_DRAIN_BUDGET, &mut drained)
                })?;
            } else {
                check(unsafe {
                    dmesh_doca_data_clear_and_drain(
                        self.doca.raw(),
                        data_fd,
                        DATA_DRAIN_BUDGET,
                        &mut drained,
                    )
                })?;
            }

            self.advance()?;
            self.emit_conn_events();
            // Legacy compatibility hook. Native flow mappings are fenced by
            // the explicit reader acknowledgement before advance().
            unsafe { dmesh_doca_reap_graves(self.doca.raw()) };
            // Install any handles the acceptor registered, then deliver the
            // recv segments the drain above produced to the reading stacks.
            self.drain_registrations();
            self.pump_recv();
            self.pump_send();
            // Staging flow control: tell each slot's DPA thread how far the
            // reader got, so it can reuse the staging ring behind it.
            for slot in 0..MAX_CONNS {
                if let Some(h) = self.handles[slot].as_ref() {
                    if let Some(p) = h.take_rx_watermark() {
                        unsafe { dmesh_doca_conn_rx_watermark(self.doca.raw(), slot as i32, p) };
                    }
                }
            }
            self.maybe_report_stats(&mut stats_last, &mut stats_prev);

            // Budget exhausted: more data-path work is pending, don't sleep
            // (but yield so other tasks on this runtime can make progress).
            if drained >= DATA_DRAIN_BUDGET {
                tokio::task::yield_now().await;
                continue;
            }

            // Busy-poll: never block on the notification fds. Yield so the
            // connection tasks sharing this current_thread runtime run (they
            // produce the tx bytes this loop publishes), then poll again.
            if busy_poll {
                tokio::task::yield_now().await;
                continue;
            }

            tokio::select! {
                guard = ctrl.readable() => {
                    let mut guard = guard.map_err(|e| Error::new(-1, format!("ctrl fd wait: {e}")))?;
                    guard.clear_ready();
                    check(unsafe { dmesh_doca_ctrl_clear_and_drain(self.doca.raw(), ctrl_fd) })?;
                }
                guard = data.readable() => {
                    let mut guard = guard.map_err(|e| Error::new(-1, format!("data fd wait: {e}")))?;
                    guard.clear_ready();
                    // Work is processed by the bounded drain at the top of the
                    // next iteration.
                }
                // Outbound (response) bytes: the stack writing into any
                // connection's tx (DmeshIo::poll_write -> wake_driver) wakes us
                // so pump_send runs immediately instead of on the next
                // safety-net tick. Measured without this arm: every response
                // sat in tx for a full timer period (~2ms epoll rounding of the
                // 1ms sleep) - 81% of single-stream request latency.
                _ = std::future::poll_fn(|cx| {
                    for h in self.handles.iter().flatten() {
                        if h.poll_tx_ready(cx).is_ready() {
                            return std::task::Poll::Ready(());
                        }
                    }
                    std::task::Poll::Pending
                }) => {}
                // Safety net: tokio registers fds edge-triggered, and the DOCA
                // notification fd does not re-signal while a notification is
                // already pending - a lost edge would otherwise stall the
                // datapath forever. A periodic tick bounds the stall to 1ms;
                // under load the loop never sleeps, so this arm is idle-only.
                // (Measured: reducing to 100us or busy-polling did NOT change
                // request latency - the driver poll is not the bottleneck.)
                _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {}
            }
        }
    }
}

#[cfg(test)]
mod reader_fence_tests {
    use super::*;
    use std::{
        pin::Pin,
        sync::Arc,
        task::{Context, Poll, Wake, Waker},
    };
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    struct Noop;
    impl Wake for Noop {
        fn wake(self: Arc<Self>) {}
    }

    #[test]
    fn closing_has_its_own_state() {
        assert_eq!(ConnState::from_raw(6), ConnState::Closing);
        assert_ne!(ConnState::from_raw(6), ConnState::Free);
    }

    #[test]
    fn acknowledge_only_after_io_pointers_are_cleared() {
        let (mut io, handle) = crate::dmesh_io_pair("127.0.0.1:42".parse().unwrap());
        let rx = vec![1u8; 16];
        let mut tx = vec![0u8; 16];
        handle.set_staging(rx.as_ptr() as usize, rx.len());
        handle.push_segment(0, 8);
        handle.set_tx_staging(tx.as_mut_ptr() as usize, tx.len());
        let mut slot = Some(handle);
        let mut acknowledged = false;
        detach_readers(&mut slot, || {
            let waker = Waker::from(Arc::new(Noop));
            let mut cx = Context::from_waker(&waker);
            let mut bytes = [0u8; 8];
            let mut read = ReadBuf::new(&mut bytes);
            assert!(matches!(
                Pin::new(&mut io).poll_read(&mut cx, &mut read),
                Poll::Ready(Ok(()))
            ));
            assert!(read.filled().is_empty());
            assert!(matches!(
                Pin::new(&mut io).poll_write(&mut cx, b"x"),
                Poll::Ready(Err(_))
            ));
            acknowledged = true;
            Ok(())
        })
        .unwrap();
        assert!(acknowledged && slot.is_none());
        assert!(tx.iter().all(|byte| *byte == 0));
        // This represents C releasing the mappings after the acknowledgement.
        drop(rx);
        drop(tx);
    }

    #[test]
    fn failed_acknowledgement_can_retry_without_reopening_io() {
        let (_, handle) = crate::dmesh_io_pair("127.0.0.1:42".parse().unwrap());
        let mut slot = Some(handle);
        assert!(
            detach_readers(&mut slot, || Err(Error::new(-1, "injected ack failure"))).is_err()
        );
        assert!(slot.is_none());
        let mut retried = false;
        detach_readers(&mut slot, || {
            retried = true;
            Ok(())
        })
        .unwrap();
        assert!(retried);
    }
}

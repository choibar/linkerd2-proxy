//! `DmeshIo`: an `AsyncRead + AsyncWrite` endpoint backed by DMesh DMA
//! buffers instead of a TCP socket.
//!
//! Received bytes are read **zero-copy** directly out of the per-connection DMA
//! staging buffer: the driver pushes `(pos, len)` segments (offsets into the
//! mapped staging region) via [`DmeshIoHandle::push_segment`], and
//! [`DmeshIo::poll_read`] copies straight from `staging_base + pos` into the
//! caller's `ReadBuf` (the single copy every `AsyncRead` performs; no extra
//! intermediate buffer on the DPU).
//!
//! Written bytes are likewise **staged in place**: `poll_write` copies the
//! stack's bytes once, directly into the connection's mapped `tx_staging`
//! region at a write cursor, and the driver publishes the accumulated
//! `[publish, write)` range as DMA descriptors ([`DmeshIoHandle::take_staged`]
//! / [`advance_publish`]). This replaces the old three-copy pipeline
//! (stack -> tx Vec -> take Vec -> memcpy into staging) with a single copy and
//! no per-tick heap allocation. Small writes (h2 frame headers etc.) coalesce
//! in staging and are published in batches, so descriptor efficiency matches
//! the old Vec-batching path.
//!
//! Because linkerd stacks are generic over `I: AsyncRead + AsyncWrite + Peek +
//! PeerAddr`, implementing those traits here is what lets a DMA-backed
//! connection flow through the real outbound stack (detect / discovery / LB /
//! mTLS) unchanged.

use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;

/// Receive-side cache warming + first-touch latency probe, both env-gated for
/// A/B measurement. DMA writes into the staging region are IO-coherent but do
/// NOT allocate into the A78 caches (no DDIO analogue on the DPU side), so the
/// reader's first touch of a segment is a DRAM-latency miss. `push_segment`
/// runs at completion time, well before the woken reader task actually copies
/// the bytes — a prefetch issued here has microseconds to land.
mod seg_cache {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    use std::sync::OnceLock;

    /// `DMESH_PREFETCH=1`: warm segments into L2/LLC at completion time.
    pub(super) fn prefetch_on() -> bool {
        static ON: OnceLock<bool> = OnceLock::new();
        *ON.get_or_init(|| std::env::var_os("DMESH_PREFETCH").is_some())
    }

    fn probe_on() -> bool {
        static ON: OnceLock<bool> = OnceLock::new();
        *ON.get_or_init(|| std::env::var_os("DMESH_TOUCH_PROBE").is_some())
    }

    /// PLDL2KEEP, not L1: the reading task may be scheduled on another core,
    /// so stop at the cluster-shared level instead of polluting this core's
    /// L1. Capped at 2KB — once the head lines hit, the hardware streamer
    /// follows the rest of a sequential copy.
    #[inline]
    pub(super) fn prefetch(base: *const u8, len: usize) {
        #[cfg(target_arch = "aarch64")]
        {
            let lines = (len.min(2048) + 63) / 64;
            for i in 0..lines {
                unsafe {
                    core::arch::asm!(
                        "prfm pldl2keep, [{0}]",
                        in(reg) base.add(i * 64),
                        options(nostack, preserves_flags, readonly)
                    );
                }
            }
        }
        #[cfg(not(target_arch = "aarch64"))]
        let _ = (base, len);
    }

    /// `DMESH_EVICT=1`: after the reader fully consumes a segment, clean+
    /// invalidate its lines (`dc civac`). As an optimization this is expected
    /// to be null-or-harmful; its purpose is to SIMULATE the many-connection
    /// regime where staging buffers exceed the LLC and lines are capacity-
    /// evicted between ring wraps — so the next DMA write's cache behavior
    /// (and the value of `DMESH_PREFETCH` under cold data) can be measured
    /// with a single connection. Calibrated on this SoC: post-civac first
    /// touch ~200ns vs ~21ns warm (harness floor).
    pub(super) fn evict_on() -> bool {
        static ON: OnceLock<bool> = OnceLock::new();
        *ON.get_or_init(|| std::env::var_os("DMESH_EVICT").is_some())
    }

    #[inline]
    pub(super) fn evict(base: *const u8, len: usize) {
        #[cfg(target_arch = "aarch64")]
        unsafe {
            let lines = (len + 63) / 64;
            for i in 0..lines {
                core::arch::asm!(
                    "dc civac, {0}",
                    in(reg) base.add(i * 64),
                    options(nostack, preserves_flags)
                );
            }
            core::arch::asm!("dsb ish", options(nostack, preserves_flags));
        }
        #[cfg(not(target_arch = "aarch64"))]
        let _ = (base, len);
    }

    /// `DMESH_TOUCH_PROBE=1`: sample the latency of the very first load of a
    /// freshly completed segment (every 64th segment), to establish whether
    /// DMA'd data is actually cache-cold. Reported as a running mean so cold
    /// (~DRAM, >100ns) vs warm (~cache, <40ns) is unambiguous. NOTE: the
    /// isb/dsb/mrs bracket itself costs ~21ns on this SoC — a warm line reads
    /// as ~21-26ns (overhead-limited), a cold one as ~200ns+.
    #[inline]
    pub(super) fn probe(base: *const u8) {
        if !probe_on() {
            return;
        }
        static N: AtomicU64 = AtomicU64::new(0);
        static TICKS: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Relaxed);
        if n % 64 != 0 {
            return;
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            let t0: u64;
            let t1: u64;
            let freq: u64;
            // isb orders the counter read against surrounding instructions;
            // dsb sy after the load waits for the memory access itself, so
            // t1-t0 brackets the load's completion.
            core::arch::asm!("isb", "mrs {0}, cntvct_el0", out(reg) t0, options(nostack));
            core::ptr::read_volatile(base as *const u64);
            core::arch::asm!("dsb sy", "mrs {0}, cntvct_el0", out(reg) t1, options(nostack));
            core::arch::asm!("mrs {0}, cntfrq_el0", out(reg) freq, options(nostack));
            let total = TICKS.fetch_add(t1.wrapping_sub(t0), Relaxed) + t1.wrapping_sub(t0);
            let samples = n / 64 + 1;
            if samples % 2048 == 0 {
                let ns = total as f64 / samples as f64 * 1e9 / freq as f64;
                eprintln!("[dmesh-touch] first-touch mean {ns:.0} ns over {samples} samples");
            }
        }
        #[cfg(not(target_arch = "aarch64"))]
        let _ = base;
    }
}
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use linkerd_io::{Peek, PeerAddr};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Default)]
struct Inner {
    /// Owned received bytes (copy path; used by tests and any non-DMA push).
    rx: Vec<u8>,

    /// Zero-copy staging: base address (as usize for Send) + length of the
    /// mapped DMA staging region, and a FIFO of completed `(pos, len)`
    /// segments within it. Read directly by `poll_read`.
    staging_base: usize,
    staging_len: usize,
    segs: VecDeque<(u32, u32)>,
    /// Staging offset up to which every segment has been consumed (published
    /// to the DPA as its reuse gate), and whether it moved since last publish.
    rx_watermark: u32,
    rx_watermark_dirty: bool,
    seg_read_off: usize, // partial-consume cursor into segs.front()

    /// Peer half closed: reads drain rx/segs then return EOF.
    rx_closed: bool,
    rx_waker: Option<Waker>,

    /// TX staging (write-side zero-copy): the connection's mapped tx_staging
    /// region. The stack writes at `tx_write`, the driver publishes
    /// `[tx_publish, tx_write)` as DMA work. Cursors are cumulative (u64), the
    /// physical offset is `cursor % tx_len`; unpublished bytes are never
    /// overwritten (`room = tx_len - (tx_write - tx_publish)`).
    tx_base: usize,
    tx_len: usize,
    tx_write: u64,
    tx_publish: u64,
    /// Connection torn down: the staging pointer is no longer valid.
    tx_dead: bool,

    /// Local half shut down: further writes fail.
    tx_closed: bool,
    /// The driver took the end of stream (see `take_fin`).
    fin_taken: bool,
    tx_waker: Option<Waker>,

    /// Driver-side waker: TX, shutdown, and consumed RX credit all need progress.
    driver_waker: Option<Waker>,
}

impl Inner {
    fn wake_reader(&mut self) {
        if let Some(w) = self.rx_waker.take() {
            w.wake();
        }
    }
    fn wake_writer(&mut self) {
        if let Some(w) = self.tx_waker.take() {
            w.wake();
        }
    }
    fn wake_driver(&mut self) {
        if let Some(w) = self.driver_waker.take() {
            w.wake();
        }
    }
    fn rx_has_data(&self) -> bool {
        !self.rx.is_empty() || !self.segs.is_empty()
    }
    fn tx_unpublished(&self) -> u64 {
        self.tx_write - self.tx_publish
    }
}

/// Stack-facing endpoint (the `TcpStream` analogue).
pub struct DmeshIo {
    inner: Arc<Mutex<Inner>>,
    peer: SocketAddr,
}

/// Driver-facing endpoint bridging the DMA staging buffers to the stack.
pub struct DmeshIoHandle {
    inner: Arc<Mutex<Inner>>,
}

// SAFETY: `staging_base` / `tx_base` are raw addresses into DMA regions that
// outlive the connection (`clear_tx_staging` marks tx dead before the C side
// frees it); access is serialized by the `Mutex`, with a single logical reader
// and a single logical writer.
unsafe impl Send for DmeshIo {}
unsafe impl Sync for DmeshIo {}
unsafe impl Send for DmeshIoHandle {}
unsafe impl Sync for DmeshIoHandle {}

/// Create a connected pair. The driver keeps the handle, the stack gets the IO.
/// `peer` is the flow's source address, reported via `PeerAddr`.
pub fn dmesh_io_pair(peer: SocketAddr) -> (DmeshIo, DmeshIoHandle) {
    let inner = Arc::new(Mutex::new(Inner::default()));
    (
        DmeshIo {
            inner: inner.clone(),
            peer,
        },
        DmeshIoHandle { inner },
    )
}

impl fmt::Debug for DmeshIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DmeshIo").field("peer", &self.peer).finish()
    }
}

impl AsyncRead for DmeshIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let mut inner = self.inner.lock().unwrap();

        // Owned bytes first (copy path).
        if !inner.rx.is_empty() {
            let n = buf.remaining().min(inner.rx.len());
            buf.put_slice(&inner.rx[..n]);
            inner.rx.drain(..n);
            return Poll::Ready(Ok(()));
        }

        // Zero-copy staging segment.
        if let Some(&(pos, len)) = inner.segs.front() {
            let seg_len = len as usize;
            let off = inner.seg_read_off;
            let avail = seg_len - off;
            let n = buf.remaining().min(avail);
            let base = inner.staging_base as *const u8;
            debug_assert!(pos as usize + off + n <= inner.staging_len);
            // SAFETY: [pos, pos+len) is a completed DMA segment inside the
            // staging region [base, base+staging_len) reported by the C side;
            // the region lives for the connection's lifetime and is only read.
            let src = unsafe { std::slice::from_raw_parts(base.add(pos as usize + off), n) };
            buf.put_slice(src);
            inner.seg_read_off += n;
            if inner.seg_read_off >= seg_len {
                inner.segs.pop_front();
                inner.seg_read_off = 0;
                // Segment fully consumed: the DPA may reuse staging up to here.
                inner.rx_watermark = pos + len;
                inner.rx_watermark_dirty = true;
                inner.wake_driver();
                if seg_cache::evict_on() {
                    // SAFETY: same completed-segment invariant as the copy above.
                    seg_cache::evict(unsafe { base.add(pos as usize) }, seg_len);
                }
            }
            return Poll::Ready(Ok(()));
        }

        if inner.rx_closed {
            return Poll::Ready(Ok(())); // EOF
        }
        inner.rx_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl AsyncWrite for DmeshIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut inner = self.inner.lock().unwrap();
        if inner.tx_closed || inner.tx_dead {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "dmesh connection is shut down",
            )));
        }
        // The tx staging region appears once the reverse path is set up; until
        // then writers wait (for client channels that is the same driver tick
        // that made the connection ready).
        if inner.tx_len == 0 {
            inner.tx_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        let room = inner.tx_len as u64 - inner.tx_unpublished();
        if room == 0 {
            inner.tx_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        let n = (data.len() as u64).min(room) as usize;
        let off = (inner.tx_write % inner.tx_len as u64) as usize;
        let contiguous = inner.tx_len - off;
        let first = n.min(contiguous);
        let base = inner.tx_base as *mut u8;
        // SAFETY: [off, off+first) and (on wrap) [0, n-first) lie inside the
        // mapped tx_staging region of `tx_len` bytes; the room check above
        // guarantees these bytes are not part of the unpublished window, and
        // the mutex serializes all access. The region stays alive until
        // `clear_tx_staging` sets `tx_dead` (checked above under this lock).
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), base.add(off), first);
            if first < n {
                std::ptr::copy_nonoverlapping(data.as_ptr().add(first), base, n - first);
            }
        }
        inner.tx_write += n as u64;
        inner.wake_driver();
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut inner = self.inner.lock().unwrap();
        inner.tx_closed = true;
        inner.wake_driver();
        Poll::Ready(Ok(()))
    }
}

#[async_trait::async_trait]
impl Peek for DmeshIo {
    /// The DMA endpoint is not peekable; return 0 so protocol detection falls
    /// back to `read_buf` + `PrefixedIo` replay (see linkerd-tls / http-detect).
    async fn peek(&self, _buf: &mut [u8]) -> io::Result<usize> {
        Ok(0)
    }
}

impl PeerAddr for DmeshIo {
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.peer)
    }
}

impl Drop for DmeshIo {
    fn drop(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        inner.tx_closed = true;
        inner.wake_driver();
    }
}

impl DmeshIoHandle {
    /// Point the reader at the connection's mapped DMA staging region. Must be
    /// set before any `push_segment`.
    pub fn set_staging(&self, base_addr: usize, len: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.staging_base = base_addr;
        inner.staging_len = len;
    }

    /// Point the writer at the connection's mapped tx_staging region (usable
    /// length, i.e. minus any C-side reserved tail). Wakes writers that were
    /// waiting for the reverse path to come up.
    pub fn set_tx_staging(&self, base_addr: usize, len: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.tx_base = base_addr;
        inner.tx_len = len;
        inner.wake_writer();
    }

    /// The connection is being torn down: the tx staging pointer is about to
    /// be freed. Further writes fail; the driver stops publishing.
    pub fn clear_tx_staging(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.tx_dead = true;
        inner.tx_len = 0;
        inner.wake_writer();
        inner.wake_driver();
    }

    /// Deliver a completed recv segment `[pos, pos+len)` in the staging region
    /// to the reading stack (zero-copy: no bytes are moved here).
    /// Take the consumed-staging watermark if it moved since the last take.
    pub fn take_rx_watermark(&self) -> Option<u32> {
        let mut inner = self.inner.lock().unwrap();
        if inner.rx_watermark_dirty {
            inner.rx_watermark_dirty = false;
            Some(inner.rx_watermark)
        } else {
            None
        }
    }

    pub fn push_segment(&self, pos: u32, len: u32) {
        let mut inner = self.inner.lock().unwrap();
        if inner.staging_base != 0 {
            // SAFETY: [pos, pos+len) is a completed DMA segment inside the
            // mapped staging region (same invariant as poll_read's copy).
            let src = unsafe { (inner.staging_base as *const u8).add(pos as usize) };
            seg_cache::probe(src);
            if seg_cache::prefetch_on() {
                seg_cache::prefetch(src, len as usize);
            }
        }
        inner.segs.push_back((pos, len));
        inner.wake_reader();
    }

    /// Deliver owned bytes to the reading stack (copy path; tests / non-DMA).
    pub fn push_rx(&self, bytes: &[u8]) {
        let mut inner = self.inner.lock().unwrap();
        inner.rx.extend_from_slice(bytes);
        inner.wake_reader();
    }

    /// The connection is being torn down: the forward staging region is about
    /// to be freed. Drop undelivered zero-copy segments (the peer is gone) and
    /// return EOF to the reader; no further copies from `staging_base` happen
    /// once this returns.
    pub fn clear_rx_staging(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.staging_base = 0;
        inner.segs.clear();
        inner.seg_read_off = 0;
        inner.rx_closed = true;
        inner.wake_reader();
    }

    /// Signal peer half-close: pending data drains, then reads return EOF.
    pub fn close_rx(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.rx_closed = true;
        inner.wake_reader();
    }

    /// Next contiguous unpublished run `[pos, pos+len)` in tx staging, if any.
    /// Runs never cross the buffer wrap; call again after `advance_publish` to
    /// pick up the wrapped remainder. Does not consume - the driver reports
    /// what the DMA layer actually accepted via [`advance_publish`].
    pub fn take_staged(&self) -> Option<(u32, u32)> {
        let inner = self.inner.lock().unwrap();
        if inner.tx_dead || inner.tx_len == 0 {
            return None;
        }
        let unpublished = inner.tx_unpublished();
        if unpublished == 0 {
            return None;
        }
        let off = (inner.tx_publish % inner.tx_len as u64) as usize;
        let run = (unpublished as usize).min(inner.tx_len - off);
        Some((off as u32, run as u32))
    }

    /// Mark `n` staged bytes as published (queued for / covered by DMA); frees
    /// writer room.
    pub fn advance_publish(&self, n: u32) {
        if n == 0 {
            return;
        }
        let mut inner = self.inner.lock().unwrap();
        debug_assert!(n as u64 <= inner.tx_unpublished());
        inner.tx_publish += n as u64;
        inner.wake_writer();
    }

    /// True exactly once, when the stack shut down its write half and staging
    /// fully drained: the driver then ends the stream toward the host.
    pub fn take_fin(&self) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if inner.fin_taken || !inner.tx_closed || inner.tx_unpublished() != 0 {
            return false;
        }
        inner.fin_taken = true;
        true
    }

    /// True once the stack shut down its write half and staging fully drained.
    pub fn tx_finished(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.tx_closed && inner.tx_unpublished() == 0
    }

    /// True while the reader still has undelivered/undrained data.
    pub fn has_rx(&self) -> bool {
        self.inner.lock().unwrap().rx_has_data()
    }

    /// Wait for TX/FIN or consumed RX credit that the driver must publish.
    pub fn poll_driver_ready(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut inner = self.inner.lock().unwrap();
        if inner.rx_watermark_dirty
            || inner.tx_unpublished() > 0
            || (inner.tx_closed && !inner.fin_taken)
        {
            return Poll::Ready(());
        }
        inner.driver_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::poll_fn;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn pair() -> (DmeshIo, DmeshIoHandle) {
        dmesh_io_pair("127.0.0.1:40000".parse().unwrap())
    }

    /// A fake tx staging region (stands in for the mapped DMA buffer).
    fn tx_staging(handle: &DmeshIoHandle, len: usize) -> &'static mut [u8] {
        let buf: &'static mut [u8] = Box::leak(vec![0u8; len].into_boxed_slice());
        handle.set_tx_staging(buf.as_ptr() as usize, len);
        buf
    }

    #[tokio::test]
    async fn read_waits_then_delivers() {
        let (mut io, handle) = pair();

        let reader = tokio::spawn(async move {
            let mut buf = [0u8; 16];
            let n = io.read(&mut buf).await.unwrap();
            buf[..n].to_vec()
        });

        tokio::task::yield_now().await;
        handle.push_rx(b"hello dma");

        assert_eq!(reader.await.unwrap(), b"hello dma");
    }

    #[tokio::test]
    async fn eof_after_close() {
        let (mut io, handle) = pair();
        handle.push_rx(b"tail");
        handle.close_rx();

        let mut out = Vec::new();
        io.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"tail");
    }

    #[tokio::test]
    async fn zero_copy_staging_read() {
        // Simulate a staging region: a leaked buffer the driver "DMA'd" into.
        let staging: &'static [u8] = Box::leak(
            b"....GET / HTTP/1.1\r\n\r\nXXXX"
                .to_vec()
                .into_boxed_slice(),
        );
        let (mut io, handle) = pair();
        handle.set_staging(staging.as_ptr() as usize, staging.len());
        // Two segments referencing offsets within the staging region.
        handle.push_segment(4, 14); // "GET / HTTP/1.1"
        handle.push_segment(18, 4); // "\r\n\r\n"
        handle.close_rx();

        let mut out = Vec::new();
        io.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"GET / HTTP/1.1\r\n\r\n");
    }

    #[tokio::test]
    async fn consumed_rx_credit_wakes_idle_driver() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        use std::task::{Wake, Waker};
        struct Count(AtomicUsize);
        impl Wake for Count {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let staging = b"data";
        let (mut io, handle) = pair();
        handle.set_staging(staging.as_ptr() as usize, staging.len());
        handle.push_segment(0, 4);
        let count = Arc::new(Count(AtomicUsize::new(0)));
        let waker = Waker::from(count.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(handle.poll_driver_ready(&mut cx).is_pending());
        let mut buf = [0; 2];
        io.read_exact(&mut buf).await.unwrap();
        assert_eq!(count.0.load(Ordering::Relaxed), 0);
        io.read_exact(&mut buf).await.unwrap();
        assert_eq!(count.0.load(Ordering::Relaxed), 1);
        // A consumed segment remains ready even if it preceded registration.
        assert!(handle.poll_driver_ready(&mut cx).is_ready());
        assert_eq!(handle.take_rx_watermark(), Some(4));
        assert!(handle.poll_driver_ready(&mut cx).is_pending());
    }

    #[tokio::test]
    async fn peer_addr_reports_flow_src() {
        let (io, _h) = pair();
        assert_eq!(io.peer_addr().unwrap(), "127.0.0.1:40000".parse().unwrap());
    }

    #[tokio::test]
    async fn write_pending_until_tx_staging_set() {
        let (mut io, handle) = pair();

        let mut w = tokio::spawn(async move {
            io.write_all(b"early").await.unwrap();
            io
        });
        tokio::task::yield_now().await;
        assert!(!w.is_finished(), "write must wait for tx staging");

        let buf = tx_staging(&handle, 64);
        let _io = (&mut w).await.unwrap();
        let (pos, len) = handle.take_staged().unwrap();
        assert_eq!((pos, len), (0, 5));
        assert_eq!(&buf[..5], b"early");
    }

    #[tokio::test]
    async fn staged_write_take_advance() {
        let (mut io, handle) = pair();
        let buf = tx_staging(&handle, 64);

        io.write_all(b"ping").await.unwrap();
        poll_fn(|cx| handle.poll_driver_ready(cx)).await;

        let (pos, len) = handle.take_staged().unwrap();
        assert_eq!((pos, len), (0, 4));
        assert_eq!(&buf[..4], b"ping");
        handle.advance_publish(4);
        assert!(handle.take_staged().is_none());

        // Small writes coalesce into one contiguous run.
        io.write_all(b"ab").await.unwrap();
        io.write_all(b"cd").await.unwrap();
        let (pos, len) = handle.take_staged().unwrap();
        assert_eq!((pos, len), (4, 4));
        assert_eq!(&buf[4..8], b"abcd");
        handle.advance_publish(4);
    }

    #[tokio::test]
    async fn wrap_splits_published_runs() {
        let (mut io, handle) = pair();
        let buf = tx_staging(&handle, 8);

        io.write_all(b"abcdef").await.unwrap(); // [0..6)
        let (pos, len) = handle.take_staged().unwrap();
        assert_eq!((pos, len), (0, 6));
        handle.advance_publish(6);

        // 6 more bytes: 2 fit before the end, 4 wrap to the front.
        io.write_all(b"ghijkl").await.unwrap();
        assert_eq!(&buf[6..8], b"gh");
        assert_eq!(&buf[..4], b"ijkl");

        let (pos, len) = handle.take_staged().unwrap();
        assert_eq!((pos, len), (6, 2)); // run stops at the wrap
        handle.advance_publish(2);
        let (pos, len) = handle.take_staged().unwrap();
        assert_eq!((pos, len), (0, 4)); // wrapped remainder
        handle.advance_publish(4);
        assert!(handle.take_staged().is_none());
    }

    #[tokio::test]
    async fn backpressure_until_publish_frees_room() {
        let (mut io, handle) = pair();
        let _buf = tx_staging(&handle, 8);

        io.write_all(b"12345678").await.unwrap(); // fills the region

        let mut blocked = tokio::spawn(async move {
            io.write_all(b"x").await.unwrap();
            io
        });
        tokio::task::yield_now().await;
        assert!(!blocked.is_finished(), "full staging must backpressure");

        let (_, len) = handle.take_staged().unwrap();
        handle.advance_publish(len); // frees the whole region

        let mut io = (&mut blocked).await.unwrap();
        let (pos, len) = handle.take_staged().unwrap();
        assert_eq!((pos, len), (0, 1)); // wrapped to the front

        handle.advance_publish(1);
        io.shutdown().await.unwrap();
        assert!(handle.tx_finished());
    }

    #[tokio::test]
    async fn fin_follows_the_last_published_byte_once() {
        let (mut io, handle) = pair();
        let _buf = tx_staging(&handle, 16);
        io.write_all(b"tail").await.unwrap();
        io.shutdown().await.unwrap();
        assert!(!handle.take_fin(), "staged bytes precede the end of stream");

        let (_, len) = handle.take_staged().unwrap();
        handle.advance_publish(len);
        assert!(handle.take_fin());
        assert!(!handle.take_fin());

        // A taken end of stream no longer wakes the driver.
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(handle.poll_driver_ready(&mut cx).is_pending());
    }

    #[tokio::test]
    async fn cleared_staging_fails_writes() {
        let (mut io, handle) = pair();
        let _buf = tx_staging(&handle, 16);
        io.write_all(b"ok").await.unwrap();
        handle.advance_publish(2);

        handle.clear_tx_staging();
        assert!(io.write_all(b"nope").await.is_err());
        assert!(handle.take_staged().is_none());
    }
}

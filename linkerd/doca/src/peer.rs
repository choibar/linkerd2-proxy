//! Worker-owned RDMA stream adapter. Only control requests cross the channel;
//! L7 reads RX staging and writes registered TX arena chunks through DmeshIo.
use crate::{
    origin::WireOrigin, DmeshIo, DmeshIoHandle, FlowId, FlowKey, FlowLocation, PeerOrigin,
};
use std::{
    collections::{HashMap, HashSet},
    io,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    os::{
        raw::c_void,
        unix::io::{AsRawFd, RawFd},
    },
    sync::{Arc, Mutex, OnceLock},
    task::{Context, Poll},
};
use tokio::{
    io::unix::AsyncFd,
    sync::{mpsc, oneshot, Notify},
};

#[repr(C)]
struct Info {
    origin: WireOrigin,
    token: u32,
    src_ip: u32,
    dst_ip: u32,
    port: u16,
    incoming: u8,
    state: u8,
}
extern "C" {
    fn dmesh_doca_peer_worker(objects: *mut c_void) -> *mut c_void;
    fn peer_node_streams(worker: *mut c_void) -> *mut c_void;
    fn peer_node_worker_progress(worker: *mut c_void) -> i32;
    fn peer_node_worker_fds(worker: *mut c_void, carrier: *mut i32, msgq: *mut i32) -> i32;
    fn peer_node_worker_arm(worker: *mut c_void) -> i32;
    fn peer_node_worker_disarm(worker: *mut c_void) -> i32;
    fn peer_node_remote(ip: u32, port: u16) -> i32;
    fn peer_stream_open(
        w: *mut c_void,
        origin: *const WireOrigin,
        ip: u32,
        port: u16,
        token: *mut u32,
    ) -> i32;
    fn peer_stream_next(w: *mut c_void, cursor: *mut u32, info: *mut Info) -> i32;
    fn peer_stream_buffers(
        w: *mut c_void,
        token: u32,
        tx: *mut *mut c_void,
        tx_bytes: *mut usize,
        rx: *mut *mut c_void,
        rx_bytes: *mut usize,
    ) -> i32;
    fn peer_stream_tx_acquire(
        w: *mut c_void,
        token: u32,
        chunk: *mut u32,
        data: *mut *mut c_void,
        capacity: *mut usize,
    ) -> i32;
    fn peer_stream_receive(w: *mut c_void, token: u32, pos: *mut u32, bytes: *mut u32) -> i32;
    fn peer_stream_consumed(w: *mut c_void, token: u32, pos: u32) -> i32;
    fn peer_stream_send(w: *mut c_void, token: u32, chunk: u32, bytes: u32) -> i32;
    fn peer_stream_completed(w: *mut c_void, token: u32, bytes: *mut u64) -> i32;
    fn peer_stream_fin(w: *mut c_void, token: u32) -> i32;
    fn peer_stream_detach(w: *mut c_void, token: u32) -> i32;
}
fn unavailable() -> io::Error {
    io::Error::new(
        io::ErrorKind::ConnectionRefused,
        "authenticated RDMA stream unavailable",
    )
}
struct Request {
    origin: PeerOrigin,
    address: SocketAddrV4,
    reply: oneshot::Sender<io::Result<DmeshIo>>,
}
struct Requester {
    tx: mpsc::Sender<Request>,
    wake: Arc<Notify>,
}
fn requesters() -> &'static Mutex<HashMap<usize, Requester>> {
    static R: OnceLock<Mutex<HashMap<usize, Requester>>> = OnceLock::new();
    R.get_or_init(Default::default)
}
/// An incoming authenticated stream still goes through receiving Linkerd L7
/// authorization. The Host backend is not exposed directly to an RDMA peer.
pub struct Incoming {
    pub flow: FlowId,
    pub io: DmeshIo,
}
fn acceptors() -> &'static Mutex<HashMap<usize, mpsc::Sender<Incoming>>> {
    static A: OnceLock<Mutex<HashMap<usize, mpsc::Sender<Incoming>>>> = OnceLock::new();
    A.get_or_init(Default::default)
}
pub fn accept(worker: usize) -> mpsc::Receiver<Incoming> {
    let (tx, rx) = mpsc::channel(64);
    acceptors().lock().unwrap().insert(worker, tx);
    rx
}
/// Remember withdrawn RDMA endpoints so a stale route cannot fall back to TCP.
pub fn is_remote(address: SocketAddr) -> bool {
    static SEEN: OnceLock<Mutex<HashSet<SocketAddr>>> = OnceLock::new();
    let SocketAddr::V4(addr) = address else {
        return false;
    };
    let status = unsafe { peer_node_remote(u32::from_ne_bytes(addr.ip().octets()), addr.port()) };
    let mut seen = SEEN.get_or_init(Default::default).lock().unwrap();
    if status != 0 {
        seen.insert(address);
    }
    status != 0 || seen.contains(&address)
}
pub async fn connect(
    worker: usize,
    address: SocketAddr,
    origin: PeerOrigin,
) -> io::Result<DmeshIo> {
    let SocketAddr::V4(address) = address else {
        return Err(unavailable());
    };
    let (tx, wake) = {
        let map = requesters().lock().unwrap();
        let r = map.get(&worker).ok_or_else(unavailable)?;
        (r.tx.clone(), r.wake.clone())
    };
    let (reply, rx) = oneshot::channel();
    tx.send(Request {
        origin,
        address,
        reply,
    })
    .await
    .map_err(|_| unavailable())?;
    wake.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(30), rx)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "RDMA stream OPEN timed out"))?
        .map_err(|_| unavailable())?
}

struct Entry {
    io: Option<DmeshIo>,
    handle: DmeshIoHandle,
    reply: Option<oneshot::Sender<io::Result<DmeshIo>>>,
    fin_pending: bool,
}
pub(crate) struct PeerDriver {
    worker: *mut c_void,
    streams: *mut c_void,
    id: usize,
    requests: mpsc::Receiver<Request>,
    pending: HashMap<u32, oneshot::Sender<io::Result<DmeshIo>>>,
    entries: HashMap<u32, Entry>,
    pub wake: Arc<Notify>,
}
// The parent Driver is the sole caller of every FFI operation on this owner.
unsafe impl Send for PeerDriver {}
impl PeerDriver {
    pub(crate) unsafe fn new(objects: *mut c_void, id: usize) -> Option<Self> {
        let worker = dmesh_doca_peer_worker(objects);
        if worker.is_null() {
            return None;
        }
        let streams = peer_node_streams(worker);
        let (tx, requests) = mpsc::channel(64);
        let wake = Arc::new(Notify::new());
        requesters().lock().unwrap().insert(
            id,
            Requester {
                tx,
                wake: wake.clone(),
            },
        );
        Some(Self {
            worker,
            streams,
            id,
            requests,
            pending: HashMap::new(),
            entries: HashMap::new(),
            wake,
        })
    }
    pub(crate) fn fds(&self) -> io::Result<PeerFds> {
        let (mut carrier, mut msgq) = (-1, -1);
        if unsafe { peer_node_worker_fds(self.worker, &mut carrier, &mut msgq) } != 0 {
            return Err(unavailable());
        }
        Ok(PeerFds {
            carrier: if carrier >= 0 {
                Some(AsyncFd::new(Fd(carrier))?)
            } else {
                None
            },
            msgq: if msgq >= 0 {
                Some(AsyncFd::new(Fd(msgq))?)
            } else {
                None
            },
        })
    }
    pub(crate) fn arm(&self) -> io::Result<()> {
        if unsafe { peer_node_worker_arm(self.worker) } < 0 {
            return Err(unavailable());
        }
        Ok(())
    }
    fn make_entry(&mut self, info: &Info) -> io::Result<()> {
        let peer = SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::from(info.src_ip.to_ne_bytes()),
            info.port,
        ));
        let (io, handle) = crate::dmesh_io_pair(peer);
        let (mut tx, mut rx) = (std::ptr::null_mut(), std::ptr::null_mut());
        let (mut tx_bytes, mut rx_bytes) = (0, 0);
        if unsafe {
            peer_stream_buffers(
                self.streams,
                info.token,
                &mut tx,
                &mut tx_bytes,
                &mut rx,
                &mut rx_bytes,
            )
        } != 0
        {
            return Err(unavailable());
        }
        handle.set_staging(rx as usize, rx_bytes);
        handle.enable_tx_chunks();
        self.entries.insert(
            info.token,
            Entry {
                io: Some(io),
                handle,
                reply: self.pending.remove(&info.token),
                fin_pending: false,
            },
        );
        Ok(())
    }
    fn detach(&mut self, token: u32) -> bool {
        if let Some(entry) = self.entries.remove(&token) {
            entry.handle.clear_tx_staging();
            entry.handle.clear_rx_staging();
            if let Some(reply) = entry.reply {
                let _ = reply.send(Err(unavailable()));
            }
        }
        if let Some(reply) = self.pending.remove(&token) {
            let _ = reply.send(Err(unavailable()));
        }
        unsafe { peer_stream_detach(self.streams, token) == 1 }
    }
    pub(crate) fn progress(&mut self) -> io::Result<()> {
        while let Ok(request) = self.requests.try_recv() {
            if request.reply.is_closed() {
                continue;
            }
            let Ok(origin) = WireOrigin::encode(&request.origin) else {
                let _ = request.reply.send(Err(unavailable()));
                continue;
            };
            let mut token = 0;
            let rc = unsafe {
                peer_stream_open(
                    self.streams,
                    &origin,
                    u32::from_ne_bytes(request.address.ip().octets()),
                    request.address.port(),
                    &mut token,
                )
            };
            if rc != 0 {
                let _ = request.reply.send(Err(unavailable()));
            } else {
                self.pending.insert(token, request.reply);
            }
        }
        unsafe {
            peer_node_worker_progress(self.worker);
        }
        let mut cursor = 0;
        loop {
            let mut info: Info = unsafe { std::mem::zeroed() };
            if unsafe { peer_stream_next(self.streams, &mut cursor, &mut info) } != 1 {
                break;
            }
            if info.state == 3 || self.pending.get(&info.token).is_some_and(|r| r.is_closed()) {
                // Retirement can wait for DMA. Revisit this position only if
                // the dense-list entry was actually removed.
                if self.detach(info.token) {
                    cursor = cursor.saturating_sub(1);
                }
                continue;
            }
            if info.state != 2 {
                continue;
            }
            if !self.entries.contains_key(&info.token) {
                self.make_entry(&info)?;
            }
            let entry = self
                .entries
                .get_mut(&info.token)
                .expect("entry initialized");
            if let Some(io) = entry.io.take() {
                if info.incoming != 0 {
                    let origin = info.origin.decode().map_err(|_| unavailable())?;
                    let incoming = Incoming {
                        io,
                        flow: FlowId {
                            key: FlowKey::default(),
                            location: FlowLocation {
                                worker: self.id as u32,
                                ..Default::default()
                            },
                            src: SocketAddrV4::new(Ipv4Addr::from(info.src_ip.to_ne_bytes()), 0),
                            dst: SocketAddrV4::new(
                                Ipv4Addr::from(info.dst_ip.to_ne_bytes()),
                                info.port,
                            ),
                            workload: String::new(),
                            origin: Some(origin),
                            is_backend: false,
                        },
                    };
                    let tx = acceptors().lock().unwrap().get(&self.id).cloned();
                    match tx {
                        Some(tx) => {
                            if let Err(error) = tx.try_send(incoming) {
                                entry.io = Some(error.into_inner().io);
                            }
                        }
                        None => entry.io = Some(incoming.io),
                    }
                } else if let Some(reply) = entry.reply.take() {
                    if reply.send(Ok(io)).is_err() {
                        if self.detach(info.token) {
                            cursor = cursor.saturating_sub(1);
                        }
                        continue;
                    }
                } else {
                    if self.detach(info.token) {
                        cursor = cursor.saturating_sub(1);
                    }
                    continue;
                }
            }
            let mut completed = 0;
            if unsafe { peer_stream_completed(self.streams, info.token, &mut completed) } == 0 {
                entry.handle.advance_completed(completed)?;
            }
            if let Some(pos) = entry.handle.get_dpu_rx_consumed_pos() {
                if unsafe { peer_stream_consumed(self.streams, info.token, pos) } != 0 {
                    return Err(unavailable());
                }
            }
            while entry.handle.wants_tx_chunk() {
                let (mut chunk, mut data, mut capacity) = (0, std::ptr::null_mut(), 0);
                let rc = unsafe {
                    peer_stream_tx_acquire(
                        self.streams,
                        info.token,
                        &mut chunk,
                        &mut data,
                        &mut capacity,
                    )
                };
                if rc < 0 {
                    return Err(unavailable());
                }
                if rc == 0 {
                    break;
                }
                entry.handle.add_tx_chunk(chunk, data as usize, capacity);
            }
            for _ in 0..32 {
                let (mut pos, mut bytes) = (0, 0);
                let rc =
                    unsafe { peer_stream_receive(self.streams, info.token, &mut pos, &mut bytes) };
                if rc == -libc::ENODATA {
                    entry.handle.close_rx();
                    break;
                }
                if rc <= 0 {
                    break;
                }
                entry.handle.push_segment(pos, bytes);
            }
            for _ in 0..32 {
                let Some((chunk, bytes)) = entry.handle.take_staged() else {
                    break;
                };
                let sent = unsafe { peer_stream_send(self.streams, info.token, chunk, bytes) };
                if sent <= 0 {
                    break;
                }
                entry.handle.advance_publish(sent as u32);
            }
            entry.fin_pending |= entry.handle.take_fin();
            if entry.fin_pending && unsafe { peer_stream_fin(self.streams, info.token) } == 0 {
                entry.fin_pending = false;
            }
        }
        Ok(())
    }
    pub(crate) fn disarm(&self) {
        unsafe {
            peer_node_worker_disarm(self.worker);
        }
    }
    pub(crate) fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        for entry in self.entries.values() {
            if entry.handle.poll_driver_ready(cx).is_ready() {
                return Poll::Ready(());
            }
        }
        Poll::Pending
    }
}
impl Drop for PeerDriver {
    fn drop(&mut self) {
        requesters().lock().unwrap().remove(&self.id);
        let tokens: Vec<_> = self
            .entries
            .keys()
            .chain(self.pending.keys())
            .copied()
            .collect();
        for token in tokens {
            self.detach(token);
        }
    }
}
struct Fd(RawFd);
impl AsRawFd for Fd {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}
pub(crate) struct PeerFds {
    carrier: Option<AsyncFd<Fd>>,
    msgq: Option<AsyncFd<Fd>>,
}
impl PeerFds {
    pub(crate) async fn wait(&self) -> io::Result<()> {
        async fn wait(fd: &Option<AsyncFd<Fd>>) -> io::Result<()> {
            let Some(fd) = fd else {
                return std::future::pending().await;
            };
            fd.readable().await?.clear_ready();
            Ok(())
        }
        tokio::select! { r = wait(&self.carrier) => r, r = wait(&self.msgq) => r }
    }
}

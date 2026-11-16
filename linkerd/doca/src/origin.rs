//! Verified source Pod identity. Native Host flow metadata is not its authority.
use std::os::raw::{c_char, c_void};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PeerOrigin {
    pub session_id: u64,
    pub session_epoch: u64,
    pub generation: u64,
    pub channel_slot: u32,
    pub pod_uid: String,
    pub namespace: String,
    pub service_account: String,
    pub service: String,
}

#[repr(C)]
#[derive(Clone)]
pub(crate) struct WireOrigin {
    session_id: u64,
    session_epoch: u64,
    generation: u64,
    channel_slot: u32,
    pod_uid: [c_char; 64],
    namespace: [c_char; 64],
    service_account: [c_char; 254],
    service: [c_char; 64],
}

impl WireOrigin {
    pub(crate) fn decode(&self) -> Result<PeerOrigin, ()> {
        let text = |value: &[c_char]| -> Result<String, ()> {
            let end = value.iter().position(|byte| *byte == 0).ok_or(())?;
            String::from_utf8(value[..end].iter().map(|byte| *byte as u8).collect()).map_err(|_| ())
        };
        Ok(PeerOrigin {
            session_id: self.session_id,
            session_epoch: self.session_epoch,
            generation: self.generation,
            channel_slot: self.channel_slot,
            pod_uid: text(&self.pod_uid)?,
            namespace: text(&self.namespace)?,
            service_account: text(&self.service_account)?,
            service: text(&self.service)?,
        })
    }
    pub(crate) fn encode(origin: &PeerOrigin) -> Result<Self, ()> {
        fn copy(dst: &mut [c_char], src: &str) -> Result<(), ()> {
            if src.len() >= dst.len() || src.as_bytes().contains(&0) {
                return Err(());
            }
            for (out, byte) in dst.iter_mut().zip(src.bytes()) {
                *out = byte as c_char;
            }
            Ok(())
        }
        let mut raw: Self = unsafe { std::mem::zeroed() };
        raw.session_id = origin.session_id;
        raw.session_epoch = origin.session_epoch;
        raw.generation = origin.generation;
        raw.channel_slot = origin.channel_slot;
        copy(&mut raw.pod_uid, &origin.pod_uid)?;
        copy(&mut raw.namespace, &origin.namespace)?;
        copy(&mut raw.service_account, &origin.service_account)?;
        copy(&mut raw.service, &origin.service)?;
        Ok(raw)
    }
}

tokio::task_local! { static CURRENT: Option<PeerOrigin>; }
pub fn current() -> Option<PeerOrigin> {
    CURRENT.try_with(Clone::clone).ok().flatten()
}
pub async fn scope<F: std::future::Future>(origin: Option<PeerOrigin>, future: F) -> F::Output {
    CURRENT.scope(origin, future).await
}
pub fn sync_scope<T>(origin: Option<PeerOrigin>, f: impl FnOnce() -> T) -> T {
    CURRENT.sync_scope(origin, f)
}

extern "C" {
    fn dmesh_dispatch_flow_origin(objects: *mut c_void, slot: u32, out: *mut WireOrigin) -> i32;
}

pub(crate) unsafe fn read(objects: *mut c_void, slot: usize) -> Result<Option<PeerOrigin>, ()> {
    let mut raw: WireOrigin = std::mem::zeroed();
    match dmesh_dispatch_flow_origin(objects, slot as u32, &mut raw) {
        0 => Ok(None),
        1 => {
            let text = |value: &[c_char]| -> Result<String, ()> {
                let end = value.iter().position(|byte| *byte == 0).ok_or(())?;
                let bytes: Vec<u8> = value[..end].iter().map(|byte| *byte as u8).collect();
                String::from_utf8(bytes).map_err(|_| ())
            };
            Ok(Some(PeerOrigin {
                session_id: raw.session_id,
                session_epoch: raw.session_epoch,
                generation: raw.generation,
                channel_slot: raw.channel_slot,
                pod_uid: text(&raw.pod_uid)?,
                namespace: text(&raw.namespace)?,
                service_account: text(&raw.service_account)?,
                service: text(&raw.service)?,
            }))
        }
        _ => Err(()),
    }
}

//! Per-thread PMU counters read from user space (arm64 `rdpmc`).
//!
//! A [`Counter`] is a perf event counting on the thread that opened it, with
//! its control page mapped so the thread reads the count with a few register
//! reads instead of a syscall. Needs `kernel.perf_user_access=1` (and the
//! privilege to open the event); otherwise, and off arm64, `open` fails.

#[cfg(target_arch = "aarch64")]
pub use self::arm64::Counter;

/// INST_RETIRED.
const INSTRUCTIONS: u64 = 0x08;

thread_local! {
    static THREAD_INSTRUCTIONS: Option<Counter> = Counter::open(INSTRUCTIONS);
}

/// The calling thread's retired instructions, from a counter opened on its
/// first call; 0 when user-space PMU reads are unavailable.
pub fn thread_instructions() -> u64 {
    THREAD_INSTRUCTIONS.with(|c| c.as_ref().map_or(0, Counter::read))
}

#[cfg(not(target_arch = "aarch64"))]
pub struct Counter;

#[cfg(not(target_arch = "aarch64"))]
impl Counter {
    pub fn open(_config: u64) -> Option<Self> {
        None
    }

    pub fn read(&self) -> u64 {
        0
    }
}

#[cfg(target_arch = "aarch64")]
mod arm64 {
    use std::{ptr, sync::atomic};

    /// `struct perf_event_attr` (PERF_ATTR_SIZE_VER8).
    #[repr(C)]
    #[derive(Default)]
    struct Attr {
        type_: u32,
        size: u32,
        config: u64,
        sample_period: u64,
        sample_type: u64,
        read_format: u64,
        flags: u64,
        wakeup_events: u32,
        bp_type: u32,
        config1: u64,
        config2: u64,
        branch_sample_type: u64,
        sample_regs_user: u64,
        sample_stack_user: u32,
        clockid: i32,
        sample_regs_intr: u64,
        aux_watermark: u32,
        sample_max_stack: u16,
        reserved_2: u16,
        aux_sample_size: u32,
        reserved_3: u32,
        sig_data: u64,
        config3: u64,
    }

    const PERF_TYPE_RAW: u32 = 4;
    const EXCLUDE_HV: u64 = 1 << 6;
    /// armv8_pmuv3 format `rdpmc` (config1:1): map the counter for EL0 reads.
    const CONFIG1_RDPMC: u64 = 1 << 1;
    /// `struct perf_event_mmap_page` offsets.
    const LOCK: usize = 8;
    const INDEX: usize = 12;
    const OFFSET: usize = 16;
    const CAPABILITIES: usize = 40;
    const PMC_WIDTH: usize = 48;
    const CAP_USER_RDPMC: u64 = 1 << 2;

    /// One counting event on the calling thread with its mmap'd control page.
    pub struct Counter {
        page: *const u8,
    }

    impl Counter {
        /// Opens raw PMUv3 event `config` (e.g. 0x08 INST_RETIRED) on the
        /// calling thread; `None` unless its counter is user-readable.
        pub fn open(config: u64) -> Option<Self> {
            let attr = Attr {
                type_: PERF_TYPE_RAW,
                size: std::mem::size_of::<Attr>() as u32,
                config,
                flags: EXCLUDE_HV,
                config1: CONFIG1_RDPMC,
                ..Default::default()
            };
            // SAFETY: perf_event_open(attr, this thread, any cpu, no group, 0)
            // and a read-only shared map of its control page, which stays
            // mapped (and the fd open) for the life of the process.
            unsafe {
                let fd = libc::syscall(libc::SYS_perf_event_open, &attr, 0, -1, -1, 0) as i32;
                if fd < 0 {
                    return None;
                }
                let len = libc::sysconf(libc::_SC_PAGESIZE) as usize;
                let page = libc::mmap(
                    ptr::null_mut(),
                    len,
                    libc::PROT_READ,
                    libc::MAP_SHARED,
                    fd,
                    0,
                );
                if page == libc::MAP_FAILED {
                    libc::close(fd);
                    return None;
                }
                let c = Self {
                    page: page as *const u8,
                };
                let caps = ptr::read_volatile(c.page.add(CAPABILITIES) as *const u64);
                (caps & CAP_USER_RDPMC != 0).then_some(c)
            }
        }

        /// The event's count, per the perf_event_mmap_page self-monitoring
        /// protocol (seqlock over offset + the live hardware counter). Only
        /// meaningful on the thread that opened the counter.
        #[inline]
        pub fn read(&self) -> u64 {
            // SAFETY: the page stays mapped; the counter registers are
            // EL0-readable while the event is scheduled on this thread.
            unsafe {
                let at = |off: usize| self.page.add(off);
                loop {
                    let seq = ptr::read_volatile(at(LOCK) as *const u32);
                    atomic::compiler_fence(atomic::Ordering::SeqCst);
                    let idx = ptr::read_volatile(at(INDEX) as *const u32);
                    let mut count = ptr::read_volatile(at(OFFSET) as *const i64);
                    let caps = ptr::read_volatile(at(CAPABILITIES) as *const u64);
                    if caps & CAP_USER_RDPMC != 0 && idx != 0 {
                        let width = ptr::read_volatile(at(PMC_WIDTH) as *const u16) as u32;
                        let mut pmc = read_counter(idx - 1) as i64;
                        pmc <<= 64 - width;
                        pmc >>= 64 - width;
                        count = count.wrapping_add(pmc);
                    }
                    atomic::compiler_fence(atomic::Ordering::SeqCst);
                    if ptr::read_volatile(at(LOCK) as *const u32) == seq {
                        return count as u64;
                    }
                }
            }
        }
    }

    /// Hardware counter `n`: 31 is the cycle counter, others event counters.
    #[inline(always)]
    unsafe fn read_counter(n: u32) -> u64 {
        let v: u64;
        if n == 31 {
            std::arch::asm!("mrs {v}, pmccntr_el0", v = out(reg) v, options(nostack));
        } else {
            std::arch::asm!(
                "msr pmselr_el0, {n}",
                "isb",
                "mrs {v}, pmxevcntr_el0",
                n = in(reg) n as u64,
                v = out(reg) v,
                options(nostack),
            );
        }
        v
    }
}

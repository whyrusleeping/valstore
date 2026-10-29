use std::cell::{Cell, RefCell};
use std::ffi::{CStr, CString};
use std::future::Future;
use std::os::raw::{c_char, c_int, c_void};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::{
    check_range, AsyncRead, BlockDevice, BlockError, MaintenanceCacheKey, Result, WriteHandle,
    WriteState,
};

#[derive(Debug, Clone)]
pub struct SpdkConfig {
    pub pci_addr: String,
    pub nsid: u32,
}

mod ffi {
    use super::{c_char, c_int, c_void};

    #[repr(C)]
    pub struct vals_spdk_dev {
        _opaque: [u8; 0],
    }

    #[repr(C)]
    pub struct vals_spdk_ctrlr {
        _opaque: [u8; 0],
    }

    extern "C" {
        pub fn vals_spdk_env_init(
            name: *const c_char,
            shm_id: c_int,
            main_core: c_int,
            mem_size_mb: c_int,
        ) -> c_int;
        pub fn vals_spdk_open(
            pci_addr: *const c_char,
            nsid: u32,
            out_sector_size: *mut u32,
            out_num_sectors: *mut u64,
        ) -> *mut vals_spdk_dev;
        pub fn vals_spdk_attach(
            pci_addr: *const c_char,
            nsid: u32,
            out_sector_size: *mut u32,
            out_num_sectors: *mut u64,
        ) -> *mut vals_spdk_ctrlr;
        pub fn vals_spdk_open_qpair(ctrlr: *mut vals_spdk_ctrlr) -> *mut vals_spdk_dev;
        pub fn vals_spdk_ctrlr_serial(
            ctrlr: *const vals_spdk_ctrlr,
            out: *mut c_char,
            out_len: usize,
        ) -> c_int;
        pub fn vals_spdk_detach(ctrlr: *mut vals_spdk_ctrlr);
        pub fn vals_spdk_read(
            dev: *mut vals_spdk_dev,
            lba: u64,
            out: *mut c_void,
            nbytes: u64,
        ) -> c_int;
        pub fn vals_spdk_write(
            dev: *mut vals_spdk_dev,
            lba: u64,
            input: *const c_void,
            nbytes: u64,
        ) -> c_int;
        pub fn vals_spdk_read_batch(
            dev: *mut vals_spdk_dev,
            lbas: *const u64,
            nbytes: *const u64,
            out_ptrs: *const *mut c_void,
            n: u32,
        ) -> c_int;
        pub fn vals_spdk_aio_read(
            dev: *mut vals_spdk_dev,
            lba: u64,
            out: *mut c_void,
            nbytes: u64,
            complete: extern "C" fn(*mut c_void, bool),
            ctx: *mut c_void,
        ) -> c_int;
        pub fn vals_spdk_aio_write(
            dev: *mut vals_spdk_dev,
            lba: u64,
            input: *const c_void,
            nbytes: u64,
            complete: extern "C" fn(*mut c_void, bool),
            ctx: *mut c_void,
        ) -> c_int;
        pub fn vals_spdk_poll(dev: *mut vals_spdk_dev) -> c_int;
        pub fn vals_spdk_aio_inflight(dev: *const vals_spdk_dev) -> u64;
        pub fn vals_spdk_flush(dev: *mut vals_spdk_dev) -> c_int;
        pub fn vals_spdk_deallocate(dev: *mut vals_spdk_dev, lbas: *const u64, counts: *const u32, n: u32) -> c_int;
        pub fn vals_spdk_aio_flush(
            dev: *mut vals_spdk_dev,
            complete: extern "C" fn(*mut c_void, bool),
            ctx: *mut c_void,
        ) -> c_int;
        pub fn vals_spdk_close(dev: *mut vals_spdk_dev);
        pub fn vals_spdk_dma_alloc(nbytes: u64) -> *mut c_void;
        pub fn vals_spdk_dma_free(p: *mut c_void);
    }
}

#[derive(Debug, Clone)]
pub struct SpdkEnv {
    pub name: Option<String>,
    pub shm_id: i32,
    pub core: i32,
    pub mem_mb: i32,
}

impl Default for SpdkEnv {
    fn default() -> Self {
        SpdkEnv {
            name: None,
            shm_id: -1,
            core: -1,
            mem_mb: 0,
        }
    }
}

pub fn spdk_env_init(env: &SpdkEnv) -> Result<()> {
    let c_name = match &env.name {
        Some(n) => {
            Some(CString::new(n.as_str()).map_err(|_| io_err("spdk env name has a NUL byte"))?)
        }
        None => None,
    };
    let name_ptr = c_name.as_ref().map_or(std::ptr::null(), |s| s.as_ptr());
    unsafe {
        if ffi::vals_spdk_env_init(name_ptr, env.shm_id, env.core, env.mem_mb) != 0 {
            return Err(io_err("spdk_env_init failed (hugepages? permissions?)"));
        }
    }
    Ok(())
}

struct IoState {
    done: Cell<bool>,
    err: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

fn complete_aio(ctx: *mut c_void, success: bool) {
    // SAFETY: `ctx` is the `Rc<IoState>` reference the submitting poll leaked with
    // `Rc::into_raw`; the shim fires this exactly once per accepted submit, so
    // reconstituting it here (and dropping it at end of scope) balances that leak.
    // The shim completes on the thread that polls the qpair, so the `Rc` stays
    // thread-confined.
    let state = unsafe { Rc::from_raw(ctx as *const IoState) };
    crate::note_async_io_completed();
    state.err.set(!success);
    state.done.set(true);
    let waker = state.waker.borrow_mut().take();
    if let Some(w) = waker {
        w.wake();
    }
}

extern "C" fn aio_complete(ctx: *mut c_void, success: bool) {
    complete_aio(ctx, success);
}

extern "C" fn aio_write_complete(ctx: *mut c_void, success: bool) {
    crate::note_spdk_api_write_complete(success);
    complete_aio(ctx, success);
}

extern "C" fn flush_complete(ctx: *mut c_void, success: bool) {
    // SAFETY: `ctx` is the `Arc<WriteState>` leaked by `submit_flush`; the
    // shim invokes this exactly once.
    let state = unsafe { Arc::from_raw(ctx as *const WriteState) };
    state.complete(success);
}

extern "C" fn write_complete(ctx: *mut c_void, success: bool) {
    // SAFETY: as `aio_complete`, for the `Arc<WriteState>` leaked at submit.
    crate::note_spdk_api_write_complete(success);
    let state = unsafe { Arc::from_raw(ctx as *const WriteState) };
    state.complete(success);
}

struct SubmittedIo {
    dev: *mut c_void,
    state: Rc<IoState>,
    poll: fn(*mut c_void) -> c_int,
}

impl SubmittedIo {
    fn done(&self) -> bool {
        self.state.done.get()
    }
}

struct AbortOnUnwind;

impl Drop for AbortOnUnwind {
    fn drop(&mut self) {
        std::process::abort();
    }
}

impl Drop for SubmittedIo {
    fn drop(&mut self) {
        let unwinding = AbortOnUnwind;
        drop(self.state.waker.borrow_mut().take());
        while !self.state.done.get() {
            if (self.poll)(self.dev) < 0 {
                std::process::abort();
            }
            std::hint::spin_loop();
        }
        std::mem::forget(unwinding);
    }
}

fn poll_submitted_spdk(dev: *mut c_void) -> c_int {
    // SAFETY: `dev` is the SpdkBlockDevice the submitting ReadAt or WriteAt
    // borrows, and its Drop drains before that borrow ends.
    let dev = unsafe { &*dev.cast::<SpdkBlockDevice>() };
    dev.poll()
}

pub struct ReadAt<'a> {
    dev: &'a SpdkBlockDevice,
    lba: u64,
    buf: &'a mut [u8],
    submitted: Option<SubmittedIo>,
}

impl Future for ReadAt<'_> {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let this = self.get_mut();
        if let Some(submitted) = &this.submitted {
            if submitted.done() {
                return Poll::Ready(if submitted.state.err.get() {
                    Err(io_err("nvme async read failed"))
                } else {
                    Ok(())
                });
            }
            *submitted.state.waker.borrow_mut() = Some(cx.waker().clone());
            return Poll::Pending;
        }

        let dev = this.dev;
        if let Err(e) = check_range(dev.block_size, dev.num_blocks, this.lba, this.buf.len()) {
            return Poll::Ready(Err(e));
        }
        let state = Rc::new(IoState {
            done: Cell::new(false),
            err: Cell::new(false),
            waker: RefCell::new(Some(cx.waker().clone())),
        });
        let qpair = dev.qpair();
        let ctx = Rc::into_raw(state.clone()) as *mut c_void;
        // SAFETY: the qpair guard makes this thread the qpair's only user;
        // `read_at_async`'s contract keeps `buf` valid until completion.
        let rc = unsafe {
            ffi::vals_spdk_aio_read(
                qpair.dev,
                dev.dev_sector(this.lba),
                this.buf.as_mut_ptr() as *mut c_void,
                this.buf.len() as u64,
                aio_complete,
                ctx,
            )
        };
        drop(qpair);
        if rc != 0 {
            // SAFETY: submit failed, so no completion will fire — reclaim the
            // reference leaked into `ctx` above.
            unsafe { drop(Rc::from_raw(ctx as *const IoState)) };
            return Poll::Ready(Err(BlockError::Io(std::io::Error::from_raw_os_error(-rc))));
        }
        crate::note_async_io_submitted();
        this.submitted = Some(SubmittedIo {
            dev: (dev as *const SpdkBlockDevice).cast_mut().cast(),
            state,
            poll: poll_submitted_spdk,
        });
        Poll::Pending
    }
}

pub struct WriteAt<'a> {
    dev: &'a SpdkBlockDevice,
    lba: u64,
    buf: &'a [u8],
    submitted: Option<SubmittedIo>,
}

impl Future for WriteAt<'_> {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let this = self.get_mut();
        if let Some(submitted) = &this.submitted {
            if submitted.done() {
                return Poll::Ready(if submitted.state.err.get() {
                    Err(io_err("nvme async write failed"))
                } else {
                    Ok(())
                });
            }
            *submitted.state.waker.borrow_mut() = Some(cx.waker().clone());
            return Poll::Pending;
        }

        let dev = this.dev;
        if let Err(e) = check_range(dev.block_size, dev.num_blocks, this.lba, this.buf.len()) {
            return Poll::Ready(Err(e));
        }
        let state = Rc::new(IoState {
            done: Cell::new(false),
            err: Cell::new(false),
            waker: RefCell::new(Some(cx.waker().clone())),
        });
        let qpair = dev.qpair();
        let ctx = Rc::into_raw(state.clone()) as *mut c_void;
        // SAFETY: the qpair guard makes this thread the qpair's only user; the
        // shim copies `buf` into its own DMA buffer before returning, so the
        // write never reads `buf` after this call.
        let rc = unsafe {
            ffi::vals_spdk_aio_write(
                qpair.dev,
                dev.dev_sector(this.lba),
                this.buf.as_ptr() as *const c_void,
                this.buf.len() as u64,
                aio_write_complete,
                ctx,
            )
        };
        drop(qpair);
        if rc != 0 {
            // SAFETY: submit failed, so no completion will fire — reclaim the
            // reference leaked into `ctx` above.
            unsafe { drop(Rc::from_raw(ctx as *const IoState)) };
            return Poll::Ready(Err(BlockError::Io(std::io::Error::from_raw_os_error(-rc))));
        }
        crate::note_spdk_api_write_submit(this.buf.len() as u64);
        crate::note_async_io_submitted();
        this.submitted = Some(SubmittedIo {
            dev: (dev as *const SpdkBlockDevice).cast_mut().cast(),
            state,
            poll: poll_submitted_spdk,
        });
        Poll::Pending
    }
}

struct OwnerBinding {
    owner: Option<std::thread::ThreadId>,
    sealed: bool,
}

impl OwnerBinding {
    fn assert_owner(&mut self, current: std::thread::ThreadId) {
        if let Some(owner) = self.owner {
            assert_eq!(
                owner, current,
                "SPDK qpair used outside its bound owner thread"
            );
        } else {
            self.owner = Some(current);
        }
    }

    fn bind(&mut self, current: std::thread::ThreadId, aio_inflight: impl FnOnce() -> u64) {
        assert!(
            !self.sealed || self.owner == Some(current),
            "SPDK qpair ownership steal after seal"
        );
        if self.owner.is_some_and(|owner| owner != current) {
            assert_eq!(
                aio_inflight(),
                0,
                "SPDK qpair ownership move with async I/O in flight"
            );
        }
        self.owner = Some(current);
        self.sealed = true;
    }
}

struct QpairGuard<'a> {
    _binding: MutexGuard<'a, OwnerBinding>,
    dev: *mut ffi::vals_spdk_dev,
}

pub struct SpdkBlockDevice {
    dev: *mut ffi::vals_spdk_dev,
    block_size: usize,
    num_blocks: u64,
    sectors_per_block: u64,
    base_block: u64,
    qpair_owner_thread: Mutex<OwnerBinding>,
    _ctrlr: Option<Arc<SpdkController>>,
    maintenance_qpair: Option<Arc<MaintenanceQpair>>,
}

fn geometry(sector_size: u32) -> (usize, u64) {
    debug_assert!(sector_size > 0, "sector_size validated at open/attach");
    let sector = sector_size as usize;
    let logical = if sector == 0 || 4096 % sector != 0 {
        sector
    } else {
        4096
    };
    (logical, (logical / sector.max(1)) as u64)
}

// SAFETY: `dev` is one SPDK I/O qpair, which only one thread may use at a time.
// Every call that touches it goes through `qpair()`, which holds
// `qpair_owner_thread` for the whole call and asserts that the caller is the
// bound owner (and, for a shared maintenance qpair, the thread that opened it),
// so no two threads ever use the qpair at once. First use binds the owner;
// `bind_io_owner` moves ownership only while no async I/O is in flight, so
// completions, whose `IoState` is a thread-confined `Rc`, always run on the
// thread that submitted them. A maintenance qpair never changes thread.
unsafe impl Send for SpdkBlockDevice {}
unsafe impl Sync for SpdkBlockDevice {}

impl SpdkBlockDevice {
    pub fn open(cfg: &SpdkConfig) -> Result<Self> {
        let c_pci = CString::new(cfg.pci_addr.as_str())
            .map_err(|_| io_err("pci address contains a NUL byte"))?;
        unsafe {
            if ffi::vals_spdk_env_init(std::ptr::null(), -1, -1, 0) != 0 {
                return Err(io_err("spdk_env_init failed (hugepages? permissions?)"));
            }
            let mut sector_size: u32 = 0;
            let mut num_sectors: u64 = 0;
            let dev =
                ffi::vals_spdk_open(c_pci.as_ptr(), cfg.nsid, &mut sector_size, &mut num_sectors);
            if dev.is_null() {
                return Err(io_err("failed to attach NVMe controller/namespace"));
            }
            if sector_size == 0 {
                ffi::vals_spdk_close(dev);
                return Err(io_err("namespace reports sector_size 0"));
            }
            let (logical, spb) = geometry(sector_size);
            Ok(SpdkBlockDevice {
                dev,
                block_size: logical,
                num_blocks: num_sectors / spb,
                sectors_per_block: spb,
                base_block: 0,
                qpair_owner_thread: Mutex::new(OwnerBinding {
                    owner: None,
                    sealed: false,
                }),
                _ctrlr: None,
                maintenance_qpair: None,
            })
        }
    }

    #[inline]
    fn dev_sector(&self, lba: u64) -> u64 {
        (self.base_block + lba) * self.sectors_per_block
    }

    #[inline]
    fn qpair(&self) -> QpairGuard<'_> {
        let current = std::thread::current().id();
        let mut binding = self
            .qpair_owner_thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        binding.assert_owner(current);
        if let Some(qpair) = self.maintenance_qpair.as_ref() {
            assert_eq!(
                qpair.owner_thread, current,
                "maintenance qpair used outside its owning worker thread"
            );
        }
        QpairGuard {
            _binding: binding,
            dev: self.dev,
        }
    }
}

impl SpdkBlockDevice {
    /// Reads into `buf`; the device DMAs straight into it when it is DMA
    /// memory (a [`crate::Scratch`] from this device).
    ///
    /// # Safety
    ///
    /// Dropping the returned future waits for the device, but leaking it
    /// after its first poll (`mem::forget`, an `Rc` cycle) does not. The
    /// caller must not free or reuse `buf` unless the future has completed or
    /// been dropped. Awaiting it inside an async fn or block that owns `buf`
    /// meets this: leaking that outer future leaks `buf` with it.
    pub unsafe fn read_at_async<'a>(&'a self, lba: u64, buf: &'a mut [u8]) -> ReadAt<'a> {
        ReadAt {
            dev: self,
            lba,
            buf,
            submitted: None,
        }
    }

    pub fn write_at_async<'a>(&'a self, lba: u64, buf: &'a [u8]) -> WriteAt<'a> {
        WriteAt {
            dev: self,
            lba,
            buf,
            submitted: None,
        }
    }

    pub fn poll(&self) -> i32 {
        let qpair = self.qpair();
        // SAFETY: the guard makes this thread the qpair's only user.
        let completions = unsafe { ffi::vals_spdk_poll(qpair.dev) };
        drop(qpair);
        crate::note_spdk_async_poll(completions);
        completions
    }
}

struct MaintenanceQpair {
    dev: *mut ffi::vals_spdk_dev,
    controller: Arc<SpdkController>,
    owner_thread: std::thread::ThreadId,
}

// SAFETY: every use of `dev` goes through SpdkBlockDevice::qpair, which asserts
// `owner_thread`; Arc provides lifetime sharing, not permission for concurrent I/O.
unsafe impl Send for MaintenanceQpair {}
unsafe impl Sync for MaintenanceQpair {}

impl Drop for MaintenanceQpair {
    fn drop(&mut self) {
        let current = std::thread::current().id();
        if self.owner_thread != current {
            eprintln!(
                "WARN SPDK: qpair closed on non-owner thread role=maintenance \
                 owner={:?} current={current:?}",
                self.owner_thread
            );
        }
        unsafe { ffi::vals_spdk_close(self.dev) };
        let live = self
            .controller
            .open_qpairs
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed)
            - 1;
        self.controller
            .maintenance_qpairs
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        eprintln!(
            "INFO SPDK: released NVMe qpair role=maintenance controller={} ({live} live qpairs)",
            self.controller.id
        );
    }
}

pub struct SpdkController {
    ctrlr: *mut ffi::vals_spdk_ctrlr,
    sector_size: u32,
    num_sectors: u64,
    id: usize,
    open_qpairs: std::sync::atomic::AtomicUsize,
    serving_qpairs: std::sync::atomic::AtomicUsize,
    maintenance_qpairs: std::sync::atomic::AtomicUsize,
}

// SAFETY: `ctrlr` is shared read-only; the only cross-thread operation is
// `open_shard`, whose `spdk_nvme_ctrlr_alloc_io_qpair` is thread-safe (the
// controller takes its own lock). Each qpair it returns is then owned by one
// thread. Detach happens in `Drop`, i.e. after the last `Arc` clone is gone.
unsafe impl Send for SpdkController {}
unsafe impl Sync for SpdkController {}

static NEXT_CONTROLLER_ID: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(1);

impl SpdkController {
    pub fn attach(cfg: &SpdkConfig) -> Result<Arc<Self>> {
        let c_pci = CString::new(cfg.pci_addr.as_str())
            .map_err(|_| io_err("pci address contains a NUL byte"))?;
        unsafe {
            if ffi::vals_spdk_env_init(std::ptr::null(), -1, -1, 0) != 0 {
                return Err(io_err("spdk_env_init failed (hugepages? permissions?)"));
            }
            let mut sector_size: u32 = 0;
            let mut num_sectors: u64 = 0;
            let ctrlr =
                ffi::vals_spdk_attach(c_pci.as_ptr(), cfg.nsid, &mut sector_size, &mut num_sectors);
            if ctrlr.is_null() {
                return Err(io_err("failed to attach NVMe controller/namespace"));
            }
            if sector_size == 0 {
                ffi::vals_spdk_detach(ctrlr);
                return Err(io_err("namespace reports sector_size 0"));
            }
            Ok(Arc::new(SpdkController {
                ctrlr,
                sector_size,
                num_sectors,
                id: NEXT_CONTROLLER_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                open_qpairs: std::sync::atomic::AtomicUsize::new(0),
                serving_qpairs: std::sync::atomic::AtomicUsize::new(0),
                maintenance_qpairs: std::sync::atomic::AtomicUsize::new(0),
            }))
        }
    }

    pub fn num_blocks(&self) -> u64 {
        let (_, spb) = geometry(self.sector_size);
        self.num_sectors / spb
    }

    pub fn serial_number(&self) -> Result<String> {
        let mut buf = [0 as c_char; 21];
        // SAFETY: the controller is live for self's lifetime and buf is writable for buf.len().
        let rc = unsafe {
            ffi::vals_spdk_ctrlr_serial(self.ctrlr, buf.as_mut_ptr(), buf.len())
        };
        if rc != 0 {
            return Err(io_err("failed to read NVMe controller serial number"));
        }
        // SAFETY: a successful shim call always NUL-terminates within buf.
        let serial = unsafe { CStr::from_ptr(buf.as_ptr()) }
            .to_str()
            .map_err(|_| io_err("NVMe controller serial number is not valid UTF-8"))?;
        Ok(serial.to_string())
    }

    pub fn open_shard(
        self: &Arc<Self>,
        base_block: u64,
        num_blocks: u64,
    ) -> Result<SpdkBlockDevice> {
        let (logical, spb) = geometry(self.sector_size);
        if base_block
            .checked_add(num_blocks)
            .is_none_or(|end| end > self.num_sectors / spb)
        {
            return Err(io_err("shard range exceeds namespace"));
        }
        let dev = unsafe { ffi::vals_spdk_open_qpair(self.ctrlr) };
        if dev.is_null() {
            let reached = self
                .open_qpairs
                .load(std::sync::atomic::Ordering::Relaxed);
            eprintln!(
                "WARN SPDK: failed to allocate NVMe qpair role=serving controller={} \
                 (controller I/O-qpair ceiling reached at {reached} live qpairs)",
                self.id
            );
            return Err(io_err("failed to allocate NVMe qpair for shard"));
        }
        let live = self
            .open_qpairs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        self.serving_qpairs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        eprintln!(
            "INFO SPDK: allocated NVMe qpair role=serving controller={} ({live} live qpairs)",
            self.id
        );
        Ok(SpdkBlockDevice {
            dev,
            block_size: logical,
            num_blocks,
            sectors_per_block: spb,
            base_block,
            qpair_owner_thread: Mutex::new(OwnerBinding {
                owner: None,
                sealed: false,
            }),
            _ctrlr: Some(self.clone()),
            maintenance_qpair: None,
        })
    }

    fn open_maintenance_qpair(self: &Arc<Self>) -> Result<Arc<MaintenanceQpair>> {
        let dev = unsafe { ffi::vals_spdk_open_qpair(self.ctrlr) };
        if dev.is_null() {
            let reached = self
                .open_qpairs
                .load(std::sync::atomic::Ordering::Relaxed);
            eprintln!(
                "WARN SPDK: failed to allocate NVMe qpair role=maintenance controller={} \
                 (controller I/O-qpair ceiling reached at {reached} live qpairs)",
                self.id
            );
            return Err(io_err("failed to allocate maintenance NVMe qpair"));
        }
        let live = self
            .open_qpairs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        self.maintenance_qpairs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let current = std::thread::current();
        let owner_thread = current.id();
        let owner_name = current.name().unwrap_or("unnamed");
        eprintln!(
            "INFO SPDK: allocated NVMe qpair role=maintenance controller={} \
             owner={owner_name} owner_thread={owner_thread:?} ({live} live qpairs)",
            self.id
        );
        Ok(Arc::new(MaintenanceQpair {
            dev,
            controller: self.clone(),
            owner_thread,
        }))
    }
}

impl Drop for SpdkController {
    fn drop(&mut self) {
        unsafe { ffi::vals_spdk_detach(self.ctrlr) };
    }
}

thread_local! {
    static MAINTENANCE_QPAIRS: RefCell<Vec<(
        std::sync::Weak<SpdkController>,
        std::sync::Weak<MaintenanceQpair>,
    )>> = const { RefCell::new(Vec::new()) };
    static DMA_SCRATCH_POOL: RefCell<Vec<(*mut u8, usize)>> = const { RefCell::new(Vec::new()) };
}

fn dma_alloc(len: usize) -> *mut u8 {
    let p = unsafe { ffi::vals_spdk_dma_alloc(len as u64) } as *mut u8;
    assert!(!p.is_null(), "spdk_dma_zmalloc({len}) returned null");
    p
}

pub(crate) fn release_dma_scratch(ptr: *mut u8, cap: usize) {
    DMA_SCRATCH_POOL.with(|p| {
        let mut pool = p.borrow_mut();
        if pool.len() < crate::SCRATCH_POOL_MAX {
            pool.push((ptr, cap));
        } else {
            unsafe { ffi::vals_spdk_dma_free(ptr.cast()) };
        }
    });
}

impl BlockDevice for SpdkBlockDevice {
    fn block_size(&self) -> usize {
        self.block_size
    }

    fn num_blocks(&self) -> u64 {
        self.num_blocks
    }

    fn take_scratch(&self, len: usize) -> crate::Scratch {
        let reused = DMA_SCRATCH_POOL.with(|p| p.borrow_mut().pop());
        let (ptr, cap) = match reused {
            Some((ptr, cap)) if cap >= len => (ptr, cap),
            Some((ptr, _)) => {
                unsafe { ffi::vals_spdk_dma_free(ptr as *mut c_void) };
                (dma_alloc(len), len)
            }
            None => (dma_alloc(len), len),
        };
        crate::Scratch::dma(ptr, len, cap)
    }

    fn release_scratch(&self, buf: crate::Scratch) {
        drop(buf);
    }

    fn read_at(&self, lba: u64, buf: &mut [u8]) -> Result<()> {
        check_range(self.block_size, self.num_blocks, lba, buf.len())?;
        let qpair = self.qpair();
        let rc = unsafe {
            ffi::vals_spdk_read(
                qpair.dev,
                self.dev_sector(lba),
                buf.as_mut_ptr() as *mut c_void,
                buf.len() as u64,
            )
        };
        rc_to_result(rc, "nvme read")
    }

    fn write_at(&self, lba: u64, buf: &[u8]) -> Result<()> {
        check_range(self.block_size, self.num_blocks, lba, buf.len())?;
        let qpair = self.qpair();
        let rc = unsafe {
            ffi::vals_spdk_write(
                qpair.dev,
                self.dev_sector(lba),
                buf.as_ptr() as *const c_void,
                buf.len() as u64,
            )
        };
        drop(qpair);
        if rc == 0 {
            crate::note_spdk_api_write_submit(buf.len() as u64);
            crate::note_spdk_api_write_complete(true);
        }
        rc_to_result(rc, "nvme write")
    }

    fn flush(&self) -> Result<()> {
        let qpair = self.qpair();
        let rc = unsafe { ffi::vals_spdk_flush(qpair.dev) };
        drop(qpair);
        rc_to_result(rc, "nvme flush")
    }

    fn deallocate(&self, ranges: &[(u64, u64)]) -> Result<bool> {
        crate::check_dealloc_ranges(self.num_blocks, ranges)?;
        let commands = crate::dealloc_commands(ranges, self.base_block, self.sectors_per_block, crate::DSM_MAX_SECTORS)?;
        for command in commands {
            let lbas: Vec<_> = command.iter().map(|r| r.0).collect();
            let counts: Vec<_> = command.iter().map(|r| r.1).collect();
            let start = std::time::Instant::now();
            let qpair = self.qpair();
            let rc = unsafe {
                ffi::vals_spdk_deallocate(qpair.dev, lbas.as_ptr(), counts.as_ptr(), command.len() as u32)
            };
            drop(qpair);
            let result = if rc == -95 { Ok(false) } else { rc_to_result(rc, "nvme deallocate").map(|()| true) };
            let bytes = counts.iter().map(|&n| n as u64).sum::<u64>() * (self.block_size as u64 / self.sectors_per_block);
            crate::dealloc::note_dealloc(start, command.len(), bytes, &result);
            if !result? { return Ok(false); }
        }
        Ok(true)
    }

    fn read_batch(&self, ops: &mut [crate::ReadOp<'_>]) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        for op in ops.iter() {
            check_range(self.block_size, self.num_blocks, op.lba, op.buf.len())?;
        }
        let lbas: Vec<u64> = ops.iter().map(|o| self.dev_sector(o.lba)).collect();
        let nbytes: Vec<u64> = ops.iter().map(|o| o.buf.len() as u64).collect();
        let out_ptrs: Vec<*mut c_void> = ops
            .iter_mut()
            .map(|o| o.buf.as_mut_ptr() as *mut c_void)
            .collect();
        let qpair = self.qpair();
        let rc = unsafe {
            ffi::vals_spdk_read_batch(
                qpair.dev,
                lbas.as_ptr(),
                nbytes.as_ptr(),
                out_ptrs.as_ptr(),
                ops.len() as u32,
            )
        };
        rc_to_result(rc, "nvme read_batch")
    }

    fn submit_write(&self, lba: u64, buf: &[u8]) -> WriteHandle {
        if let Err(e) = check_range(self.block_size, self.num_blocks, lba, buf.len()) {
            return WriteHandle::ready(Err(e));
        }
        let state = WriteState::pending();
        let qpair = self.qpair();
        let ctx = Arc::into_raw(state.clone()) as *mut c_void;
        // SAFETY: the guard makes this thread the qpair's only user; the shim
        // copies `buf` into its own DMA buffer before returning, so the write
        // never reads `buf` after this call.
        let rc = unsafe {
            ffi::vals_spdk_aio_write(
                qpair.dev,
                self.dev_sector(lba),
                buf.as_ptr() as *const c_void,
                buf.len() as u64,
                write_complete,
                ctx,
            )
        };
        drop(qpair);
        if rc != 0 {
            // SAFETY: submit failed, so no completion will fire — reclaim the
            // reference leaked into `ctx` above.
            unsafe { drop(Arc::from_raw(ctx as *const WriteState)) };
            return WriteHandle::ready(Err(BlockError::Io(std::io::Error::from_raw_os_error(-rc))));
        }
        crate::note_spdk_api_write_submit(buf.len() as u64);
        WriteHandle::pending(state)
    }

    fn submit_flush(&self) -> Option<WriteHandle> {
        let state = WriteState::timed_pending();
        let ctx = Arc::into_raw(state.clone()) as *mut c_void;
        let qpair = self.qpair();
        let rc = unsafe { ffi::vals_spdk_aio_flush(qpair.dev, flush_complete, ctx) };
        drop(qpair);
        if rc != 0 {
            // SAFETY: submit failed, so no completion will fire — reclaim the
            // reference leaked into `ctx` above.
            unsafe { drop(Arc::from_raw(ctx as *const WriteState)) };
            return Some(WriteHandle::ready(Err(BlockError::Io(
                std::io::Error::from_raw_os_error(-rc),
            ))));
        }
        Some(WriteHandle::pending(state))
    }

    fn flush_domain(&self) -> Option<usize> {
        if self.maintenance_qpair.is_some() {
            return None;
        }
        self._ctrlr.as_ref().map(|ctrlr| ctrlr.id)
    }

    fn poll_writes(&self) -> i32 {
        self.poll()
    }
    fn poll_device(&self) -> i32 {
        self.poll()
    }

    fn bind_io_owner(&self) {
        let maintenance = self.maintenance_qpair.is_some();
        self.qpair_owner_thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .bind(std::thread::current().id(), || {
                if maintenance {
                    return 0;
                }
                // SAFETY: every other use of this serving qpair holds the
                // binding lock this call holds, so nothing touches it meanwhile.
                unsafe { ffi::vals_spdk_aio_inflight(self.dev) }
            });
    }

    fn maintenance_sibling_cache_key(&self) -> Option<MaintenanceCacheKey> {
        self._ctrlr.as_ref().map(|ctrlr| {
            MaintenanceCacheKey::new(ctrlr.id, self.base_block, self.num_blocks)
        })
    }

    fn sibling(&self) -> Result<Self> {
        match self._ctrlr.as_ref() {
            Some(ctrlr) => ctrlr.open_shard(self.base_block, self.num_blocks),
            None => Err(BlockError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "SpdkBlockDevice has no controller to open a sibling qpair on",
            ))),
        }
    }

    fn maintenance_sibling(&self) -> Result<Self> {
        let ctrlr = self._ctrlr.as_ref().ok_or_else(|| {
            BlockError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "SpdkBlockDevice has no controller to open a maintenance qpair on",
            ))
        })?;
        let qpair = MAINTENANCE_QPAIRS.with(|cell| -> Result<Arc<MaintenanceQpair>> {
            let mut qpairs = cell.borrow_mut();
            qpairs.retain(|(controller, qpair)| {
                controller.upgrade().is_some() && qpair.upgrade().is_some()
            });
            if let Some(qpair) = qpairs.iter().find_map(|(controller, qpair)| {
                std::sync::Weak::ptr_eq(controller, &Arc::downgrade(ctrlr))
                    .then(|| qpair.upgrade())
                    .flatten()
            }) {
                return Ok(qpair);
            }
            let qpair = ctrlr.open_maintenance_qpair()?;
            qpairs.push((Arc::downgrade(ctrlr), Arc::downgrade(&qpair)));
            Ok(qpair)
        })?;
        Ok(SpdkBlockDevice {
            dev: qpair.dev,
            block_size: self.block_size,
            num_blocks: self.num_blocks,
            sectors_per_block: self.sectors_per_block,
            base_block: self.base_block,
            qpair_owner_thread: Mutex::new(OwnerBinding {
                owner: None,
                sealed: false,
            }),
            _ctrlr: Some(ctrlr.clone()),
            maintenance_qpair: Some(qpair),
        })
    }
}

impl AsyncRead for SpdkBlockDevice {
    async fn read_async(&self, lba: u64, buf: &mut [u8]) -> Result<()> {
        check_range(self.block_size, self.num_blocks, lba, buf.len())?;
        let mut scratch = self.take_scratch(buf.len());
        // SAFETY: `scratch` belongs to this future, so leaking the future leaks
        // it too and its memory is never reused while the device writes it.
        let read = unsafe { self.read_at_async(lba, scratch.as_mut_slice()) }.await;
        if read.is_ok() {
            buf.copy_from_slice(&scratch);
        }
        self.release_scratch(scratch);
        read
    }

    async unsafe fn read_async_in_place(&self, lba: u64, buf: &mut [u8]) -> Result<()> {
        // SAFETY: this method's contract is `read_at_async`'s, and the caller upholds it.
        unsafe { self.read_at_async(lba, buf) }.await
    }
}

impl Drop for SpdkBlockDevice {
    fn drop(&mut self) {
        if self.maintenance_qpair.is_some() {
            return;
        }
        let current = std::thread::current().id();
        let owner = self
            .qpair_owner_thread
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .owner;
        if let Some(owner) = owner.filter(|owner| *owner != current) {
            eprintln!(
                "WARN SPDK: qpair closed on non-owner thread role=serving \
                 owner={owner:?} current={current:?}"
            );
        }
        if let Some(ctrlr) = self._ctrlr.as_ref() {
            let live = ctrlr
                .open_qpairs
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed)
                - 1;
            ctrlr
                .serving_qpairs
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            eprintln!(
                "INFO SPDK: released NVMe qpair role=serving controller={} ({live} live qpairs)",
                ctrlr.id
            );
        }
        unsafe { ffi::vals_spdk_close(self.dev) }
    }
}

fn io_err(msg: &str) -> BlockError {
    BlockError::Io(std::io::Error::other(msg))
}

fn rc_to_result(rc: c_int, what: &str) -> Result<()> {
    if rc == 0 {
        Ok(())
    } else {
        Err(BlockError::Io(std::io::Error::from_raw_os_error(-rc)).map_ctx(what))
    }
}

trait MapCtx {
    fn map_ctx(self, what: &str) -> BlockError;
}
impl MapCtx for BlockError {
    fn map_ctx(self, what: &str) -> BlockError {
        match self {
            BlockError::Io(e) => {
                BlockError::Io(std::io::Error::new(e.kind(), format!("{what}: {e}")))
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_panicking_waker_drop_aborts_instead_of_skipping_the_drain() {
        const CHILD: &str = "VALS_SPDK_TEST_PANICKING_WAKER_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "spdk::tests::a_panicking_waker_drop_aborts_instead_of_skipping_the_drain", "--nocapture"])
                .env(CHILD, "1")
                .status()
                .unwrap();
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(6), "child status {status:?}");
            return;
        }

        struct PanicOnDrop;
        #[allow(clippy::manual_noop_waker)]
        impl std::task::Wake for PanicOnDrop {
            fn wake(self: Arc<Self>) {}
        }
        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                panic!("waker drop");
            }
        }
        fn never_completes(_: *mut c_void) -> c_int {
            0
        }
        let state = Rc::new(IoState {
            done: Cell::new(false),
            err: Cell::new(false),
            waker: RefCell::new(Some(Waker::from(Arc::new(PanicOnDrop)))),
        });
        let submitted = SubmittedIo {
            dev: std::ptr::null_mut(),
            state,
            poll: never_completes,
        };
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(submitted)));
        std::process::exit(0);
    }

    #[test]
    fn submitted_io_drop_drains_before_buffer_can_be_reused() {
        struct ModelCompletion {
            state: Rc<IoState>,
            out: *mut u8,
            polls: usize,
        }

        fn poll_model(dev: *mut c_void) -> c_int {
            // SAFETY: the test keeps `ModelCompletion` and its output byte live
            // until `SubmittedIo::drop` finishes polling this function.
            let model = unsafe { &mut *dev.cast::<ModelCompletion>() };
            model.polls += 1;
            if model.polls == 2 {
                // SAFETY: `out` points to the live test byte and completion owns
                // the simulated device write until it marks the operation done.
                unsafe { *model.out = 9 };
                model.state.done.set(true);
                1
            } else {
                0
            }
        }

        let state = Rc::new(IoState {
            done: Cell::new(false),
            err: Cell::new(false),
            waker: RefCell::new(None),
        });
        let mut byte = 1u8;
        let mut model = ModelCompletion {
            state: state.clone(),
            out: &mut byte,
            polls: 0,
        };
        let submitted = SubmittedIo {
            dev: (&mut model as *mut ModelCompletion).cast(),
            state,
            poll: poll_model,
        };
        drop(submitted);
        assert_eq!(model.polls, 2);
        assert_eq!(byte, 9);
        byte = 4;
        assert_eq!(byte, 4);
    }

    #[test]
    fn sealed_owner_binding_rejects_cross_thread_use_and_rebind() {
        let owner = std::thread::current().id();
        let binding = Arc::new(Mutex::new(OwnerBinding {
            owner: Some(owner),
            sealed: false,
        }));
        binding.lock().unwrap().bind(owner, || 0);
        binding.lock().unwrap().assert_owner(owner);
        let cross_thread = binding.clone();
        assert!(std::thread::spawn(move || {
            cross_thread
                .lock()
                .unwrap()
                .assert_owner(std::thread::current().id())
        })
        .join()
        .is_err());

        let binding = Arc::new(Mutex::new(OwnerBinding {
            owner: Some(owner),
            sealed: false,
        }));
        binding.lock().unwrap().bind(owner, || 0);
        assert!(std::thread::spawn(move || {
            binding
                .lock()
                .unwrap()
                .bind(std::thread::current().id(), || 0)
        })
        .join()
        .is_err());
    }

    #[test]
    fn unsealed_ownership_moves_only_with_no_async_io_in_flight() {
        let owner = std::thread::current().id();
        let busy = Arc::new(Mutex::new(OwnerBinding {
            owner: Some(owner),
            sealed: false,
        }));
        let moved = busy.clone();
        assert!(std::thread::spawn(move || {
            moved.lock().unwrap().bind(std::thread::current().id(), || 1)
        })
        .join()
        .is_err());
        assert_eq!(busy.lock().unwrap_or_else(PoisonError::into_inner).owner, Some(owner));

        let idle = Arc::new(Mutex::new(OwnerBinding {
            owner: Some(owner),
            sealed: false,
        }));
        let moved = idle.clone();
        let new_owner = std::thread::spawn(move || {
            moved.lock().unwrap().bind(std::thread::current().id(), || 0);
            std::thread::current().id()
        })
        .join()
        .unwrap();
        let binding = idle.lock().unwrap();
        assert_eq!(binding.owner, Some(new_owner));
        assert!(binding.sealed);
    }

    #[test]
    fn config_builds() {
        let cfg = SpdkConfig {
            pci_addr: "0000:4a:00.0".into(),
            nsid: 1,
        };
        assert_eq!(cfg.nsid, 1);
        assert_eq!(cfg.pci_addr, "0000:4a:00.0");
    }
}

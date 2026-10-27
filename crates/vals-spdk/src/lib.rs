
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;

mod dealloc;
pub use dealloc::{check_dealloc_ranges, dealloc_commands, device_dealloc_totals, DeviceDeallocTotals, DSM_MAX_SECTORS};

pub const DEFAULT_BLOCK_SIZE: usize = 4096;

pub struct WriteState {
    done: AtomicBool,
    err: AtomicBool,
    timing: Option<Box<WriteTiming>>,
}

struct WriteTiming {
    started: std::time::Instant,
    completion_ns: std::sync::atomic::AtomicU64,
}

impl WriteState {
    pub fn pending() -> Arc<Self> {
        Arc::new(WriteState {
            done: AtomicBool::new(false),
            err: AtomicBool::new(false),
            timing: None,
        })
    }
    pub fn timed_pending() -> Arc<Self> {
        Arc::new(WriteState {
            done: AtomicBool::new(false),
            err: AtomicBool::new(false),
            timing: Some(Box::new(WriteTiming {
                started: std::time::Instant::now(),
                completion_ns: std::sync::atomic::AtomicU64::new(0),
            })),
        })
    }
    pub fn complete(&self, success: bool) {
        if let Some(timing) = &self.timing {
            let ns = timing.started.elapsed().as_nanos().min(u64::MAX as u128 - 1) as u64;
            timing.completion_ns.store(ns + 1, AtomicOrdering::Release);
        }
        self.err.store(!success, AtomicOrdering::Relaxed);
        self.done.store(true, AtomicOrdering::Relaxed);
    }
}

pub struct WriteHandle {
    state: Arc<WriteState>,
}

impl WriteHandle {
    pub fn ready(res: Result<()>) -> Self {
        let state = WriteState::pending();
        state.complete(res.is_ok());
        WriteHandle { state }
    }
    pub fn pending(state: Arc<WriteState>) -> Self {
        WriteHandle { state }
    }
    pub fn is_done(&self) -> bool {
        self.state.done.load(AtomicOrdering::Relaxed)
    }
    pub fn completed_at(&self) -> Option<std::time::Instant> {
        let timing = self.state.timing.as_ref()?;
        let ns = timing.completion_ns.load(AtomicOrdering::Acquire);
        (ns != 0).then(|| timing.started + std::time::Duration::from_nanos(ns - 1))
    }
    pub fn into_result(self) -> Result<()> {
        if self.state.err.load(AtomicOrdering::Relaxed) {
            Err(BlockError::Io(std::io::Error::other(
                "async nvme write failed",
            )))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
pub enum BlockError {
    Io(std::io::Error),
    Misaligned {
        len: usize,
        block_size: usize,
    },
    OutOfRange {
        lba: u64,
        blocks: u64,
        num_blocks: u64,
    },
}

impl fmt::Display for BlockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlockError::Io(e) => write!(f, "block io error: {e}"),
            BlockError::Misaligned { len, block_size } => write!(
                f,
                "buffer length {len} is not a multiple of block size {block_size}"
            ),
            BlockError::OutOfRange {
                lba,
                blocks,
                num_blocks,
            } => write!(
                f,
                "lba range [{lba}, {}) exceeds device size {num_blocks} blocks",
                lba + blocks
            ),
        }
    }
}

impl std::error::Error for BlockError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BlockError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for BlockError {
    fn from(e: std::io::Error) -> Self {
        BlockError::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, BlockError>;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SpdkIoStats {
    pub api_write_ops: u64,
    pub api_write_bytes: u64,
    pub api_write_completions: u64,
    pub api_write_completion_errors: u64,
    pub api_writes_outstanding: u64,
    pub max_api_writes_outstanding: u64,
    pub async_poll_calls: u64,
    pub async_poll_completions: u64,
    pub async_io_pending_underflows: u64,
}

static SPDK_API_WRITE_OPS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SPDK_API_WRITE_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SPDK_API_WRITE_COMPLETIONS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SPDK_API_WRITE_COMPLETION_ERRORS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SPDK_API_WRITES_OUTSTANDING: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SPDK_MAX_API_WRITES_OUTSTANDING: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SPDK_ASYNC_POLL_CALLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SPDK_ASYNC_POLL_COMPLETIONS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SPDK_ASYNC_IO_PENDING_UNDERFLOWS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

thread_local! {
    static ASYNC_IO_PENDING: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub fn async_io_pending() -> usize {
    ASYNC_IO_PENDING.with(std::cell::Cell::get)
}

pub fn note_async_io_submitted() {
    ASYNC_IO_PENDING.with(|pending| pending.set(pending.get() + 1));
}

pub fn note_async_io_completed() {
    ASYNC_IO_PENDING.with(|pending| match pending.get().checked_sub(1) {
        Some(left) => pending.set(left),
        None => {
            SPDK_ASYNC_IO_PENDING_UNDERFLOWS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            debug_assert!(
                false,
                "async I/O completion on a thread with no pending submission \
                 (cross-thread completion?)"
            );
        }
    });
}

pub fn spdk_io_stats() -> SpdkIoStats {
    let ordering = std::sync::atomic::Ordering::Relaxed;
    SpdkIoStats {
        api_write_ops: SPDK_API_WRITE_OPS.load(ordering),
        api_write_bytes: SPDK_API_WRITE_BYTES.load(ordering),
        api_write_completions: SPDK_API_WRITE_COMPLETIONS.load(ordering),
        api_write_completion_errors: SPDK_API_WRITE_COMPLETION_ERRORS.load(ordering),
        api_writes_outstanding: SPDK_API_WRITES_OUTSTANDING.load(ordering),
        max_api_writes_outstanding: SPDK_MAX_API_WRITES_OUTSTANDING.load(ordering),
        async_poll_calls: SPDK_ASYNC_POLL_CALLS.load(ordering),
        async_poll_completions: SPDK_ASYNC_POLL_COMPLETIONS.load(ordering),
        async_io_pending_underflows: SPDK_ASYNC_IO_PENDING_UNDERFLOWS.load(ordering),
    }
}

#[cfg(feature = "spdk")]
pub(crate) fn note_spdk_api_write_submit(bytes: u64) {
    let ordering = std::sync::atomic::Ordering::Relaxed;
    SPDK_API_WRITE_OPS.fetch_add(1, ordering);
    SPDK_API_WRITE_BYTES.fetch_add(bytes, ordering);
    let outstanding = SPDK_API_WRITES_OUTSTANDING.fetch_add(1, ordering) + 1;
    SPDK_MAX_API_WRITES_OUTSTANDING.fetch_max(outstanding, ordering);
}

#[cfg(feature = "spdk")]
pub(crate) fn note_spdk_api_write_complete(success: bool) {
    let ordering = std::sync::atomic::Ordering::Relaxed;
    SPDK_API_WRITE_COMPLETIONS.fetch_add(1, ordering);
    if !success {
        SPDK_API_WRITE_COMPLETION_ERRORS.fetch_add(1, ordering);
    }
    SPDK_API_WRITES_OUTSTANDING.fetch_sub(1, ordering);
}

#[cfg(feature = "spdk")]
pub(crate) fn note_spdk_async_poll(completions: i32) {
    let ordering = std::sync::atomic::Ordering::Relaxed;
    SPDK_ASYNC_POLL_CALLS.fetch_add(1, ordering);
    if completions > 0 {
        SPDK_ASYNC_POLL_COMPLETIONS.fetch_add(completions as u64, ordering);
    }
}

pub struct ReadOp<'a> {
    pub lba: u64,
    pub buf: &'a mut [u8],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MaintenanceCacheKey {
    scope: usize,
    base: u64,
    len: u64,
}

impl MaintenanceCacheKey {
    pub fn new(scope: usize, base: u64, len: u64) -> Self {
        MaintenanceCacheKey { scope, base, len }
    }
}

pub trait BlockDevice: Send + Sync + 'static {
    fn block_size(&self) -> usize;

    fn num_blocks(&self) -> u64;

    fn capacity_bytes(&self) -> u64 {
        self.block_size() as u64 * self.num_blocks()
    }

    fn read_at(&self, lba: u64, buf: &mut [u8]) -> Result<()>;

    fn write_at(&self, lba: u64, buf: &[u8]) -> Result<()>;

    fn flush(&self) -> Result<()>;

    fn deallocate(&self, ranges: &[(u64, u64)]) -> Result<bool> {
        let result = Ok(false);
        dealloc::note_dealloc(std::time::Instant::now(), ranges.len(), 0, &result);
        result
    }

    fn read_batch(&self, ops: &mut [ReadOp<'_>]) -> Result<()> {
        for op in ops.iter_mut() {
            self.read_at(op.lba, op.buf)?;
        }
        Ok(())
    }

    fn submit_write(&self, lba: u64, buf: &[u8]) -> WriteHandle {
        WriteHandle::ready(self.write_at(lba, buf))
    }

    /// Submit a device cache flush without waiting for it. `None` means the
    /// backend has no asynchronous flush; callers use the blocking `flush`.
    fn submit_flush(&self) -> Option<WriteHandle> {
        None
    }

    /// Identity of the durability domain one device cache flush covers: every
    /// handle returning the same key shares one namespace, so a flush submitted
    /// through any of them persists writes completed through all of them.
    fn flush_domain(&self) -> Option<usize> {
        None
    }

    fn poll_writes(&self) -> i32 {
        0
    }

    fn poll_device(&self) -> i32 {
        0
    }

    fn bind_io_owner(&self) {}

    fn maintenance_sibling_cache_key(&self) -> Option<MaintenanceCacheKey> {
        None
    }

    fn take_scratch(&self, len: usize) -> Scratch {
        Scratch::heap_take(len)
    }

    fn release_scratch(&self, buf: Scratch) {
        Scratch::heap_release(buf)
    }

    fn sibling(&self) -> Result<Self>
    where
        Self: Sized,
    {
        Err(BlockError::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "this backend has no maintenance sibling handle",
        )))
    }

    fn maintenance_sibling(&self) -> Result<Self>
    where
        Self: Sized,
    {
        self.sibling()
    }
}

pub trait AsyncRead {
    fn read_async(&self, lba: u64, buf: &mut [u8])
        -> impl std::future::Future<Output = Result<()>>;
}

impl AsyncRead for MemBlockDevice {
    async fn read_async(&self, lba: u64, buf: &mut [u8]) -> Result<()> {
        self.read_at(lba, buf)
    }
}

impl AsyncRead for FileBlockDevice {
    async fn read_async(&self, lba: u64, buf: &mut [u8]) -> Result<()> {
        self.read_at(lba, buf)
    }
}

macro_rules! forward_block_device {
    ($wrapper:ty) => {
        impl<T: BlockDevice + ?Sized> BlockDevice for $wrapper {
            fn block_size(&self) -> usize {
                (**self).block_size()
            }
            fn num_blocks(&self) -> u64 {
                (**self).num_blocks()
            }
            fn read_at(&self, lba: u64, buf: &mut [u8]) -> Result<()> {
                (**self).read_at(lba, buf)
            }
            fn write_at(&self, lba: u64, buf: &[u8]) -> Result<()> {
                (**self).write_at(lba, buf)
            }
            fn flush(&self) -> Result<()> {
                (**self).flush()
            }
            fn deallocate(&self, ranges: &[(u64, u64)]) -> Result<bool> {
                (**self).deallocate(ranges)
            }
            fn read_batch(&self, ops: &mut [ReadOp<'_>]) -> Result<()> {
                (**self).read_batch(ops)
            }
            fn submit_write(&self, lba: u64, buf: &[u8]) -> WriteHandle {
                (**self).submit_write(lba, buf)
            }
            fn submit_flush(&self) -> Option<WriteHandle> {
                (**self).submit_flush()
            }
            fn flush_domain(&self) -> Option<usize> {
                (**self).flush_domain()
            }
            fn poll_writes(&self) -> i32 {
                (**self).poll_writes()
            }
            fn poll_device(&self) -> i32 {
                (**self).poll_device()
            }
            fn bind_io_owner(&self) {
                (**self).bind_io_owner()
            }
            fn maintenance_sibling_cache_key(&self) -> Option<MaintenanceCacheKey> {
                (**self).maintenance_sibling_cache_key()
            }
            fn take_scratch(&self, len: usize) -> Scratch {
                (**self).take_scratch(len)
            }
            fn release_scratch(&self, buf: Scratch) {
                (**self).release_scratch(buf)
            }
        }
    };
}

forward_block_device!(Box<T>);
forward_block_device!(std::sync::Arc<T>);

impl<T: AsyncRead + ?Sized> AsyncRead for std::sync::Arc<T> {
    fn read_async(
        &self,
        lba: u64,
        buf: &mut [u8],
    ) -> impl std::future::Future<Output = Result<()>> {
        (**self).read_async(lba, buf)
    }
}

pub struct Scratch(pub(crate) ScratchKind);

pub(crate) enum ScratchKind {
    Heap(Vec<u8>),
    #[cfg(feature = "spdk")]
    Dma {
        ptr: *mut u8,
        len: usize,
        cap: usize,
    },
}

pub(crate) const SCRATCH_POOL_MAX: usize = 8192;

thread_local! {
    static HEAP_SCRATCH_POOL: std::cell::RefCell<Vec<Vec<u8>>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

impl Scratch {
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        match &mut self.0 {
            ScratchKind::Heap(v) => v.as_mut_slice(),
            #[cfg(feature = "spdk")]
            // SAFETY: `ptr` is a live `len`-byte DMA allocation owned by this
            // Scratch until released; `u8` has no invalid bit patterns.
            ScratchKind::Dma { ptr, len, .. } => unsafe {
                std::slice::from_raw_parts_mut(*ptr, *len)
            },
        }
    }

    #[allow(clippy::uninit_vec)]
    pub(crate) fn heap_take(len: usize) -> Scratch {
        let mut v = HEAP_SCRATCH_POOL
            .with(|p| p.borrow_mut().pop())
            .unwrap_or_default();
        v.clear();
        v.reserve(len);
        // SAFETY: `len` bytes reserved; `u8` has no invalid patterns; the read
        // overwrites all `len` bytes before decode.
        unsafe { v.set_len(len) };
        Scratch(ScratchKind::Heap(v))
    }

    pub(crate) fn heap_release(buf: Scratch) {
        drop(buf);
    }
}

#[cfg(feature = "spdk")]
impl Scratch {
    pub(crate) fn dma(ptr: *mut u8, len: usize, cap: usize) -> Scratch {
        Scratch(ScratchKind::Dma { ptr, len, cap })
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        match &mut self.0 {
            ScratchKind::Heap(v) => {
                let v = std::mem::take(v);
                HEAP_SCRATCH_POOL.with(|p| {
                    let mut pool = p.borrow_mut();
                    if pool.len() < SCRATCH_POOL_MAX {
                        pool.push(v);
                    }
                });
            }
            #[cfg(feature = "spdk")]
            ScratchKind::Dma { ptr, cap, .. } => crate::spdk::release_dma_scratch(*ptr, *cap),
        }
    }
}

impl std::ops::Deref for Scratch {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match &self.0 {
            ScratchKind::Heap(v) => v,
            #[cfg(feature = "spdk")]
            // SAFETY: see `as_mut_slice`.
            ScratchKind::Dma { ptr, len, .. } => unsafe { std::slice::from_raw_parts(*ptr, *len) },
        }
    }
}

pub(crate) fn check_range(block_size: usize, num_blocks: u64, lba: u64, len: usize) -> Result<u64> {
    if !len.is_multiple_of(block_size) {
        return Err(BlockError::Misaligned { len, block_size });
    }
    let blocks = (len / block_size) as u64;
    if lba.checked_add(blocks).is_none_or(|end| end > num_blocks) {
        return Err(BlockError::OutOfRange {
            lba,
            blocks,
            num_blocks,
        });
    }
    Ok(blocks)
}

use std::sync::Mutex;

pub struct MemBlockDevice {
    block_size: usize,
    num_blocks: u64,
    data: Arc<Mutex<Box<[u8]>>>,
}

impl MemBlockDevice {
    pub fn new(block_size: usize, num_blocks: u64) -> Self {
        let bytes = block_size
            .checked_mul(num_blocks as usize)
            .expect("device size overflows usize");
        MemBlockDevice {
            block_size,
            num_blocks,
            data: Arc::new(Mutex::new(vec![0u8; bytes].into_boxed_slice())),
        }
    }

    pub fn share(&self) -> MemBlockDevice {
        MemBlockDevice {
            block_size: self.block_size,
            num_blocks: self.num_blocks,
            data: Arc::clone(&self.data),
        }
    }
}

impl BlockDevice for MemBlockDevice {
    fn block_size(&self) -> usize {
        self.block_size
    }

    fn num_blocks(&self) -> u64 {
        self.num_blocks
    }

    fn maintenance_sibling_cache_key(&self) -> Option<MaintenanceCacheKey> {
        Some(MaintenanceCacheKey::new(
            Arc::as_ptr(&self.data) as usize,
            0,
            self.num_blocks,
        ))
    }

    fn read_at(&self, lba: u64, buf: &mut [u8]) -> Result<()> {
        check_range(self.block_size, self.num_blocks, lba, buf.len())?;
        let off = lba as usize * self.block_size;
        let data = self.data.lock().unwrap();
        buf.copy_from_slice(&data[off..off + buf.len()]);
        Ok(())
    }

    fn write_at(&self, lba: u64, buf: &[u8]) -> Result<()> {
        check_range(self.block_size, self.num_blocks, lba, buf.len())?;
        let off = lba as usize * self.block_size;
        let mut data = self.data.lock().unwrap();
        data[off..off + buf.len()].copy_from_slice(buf);
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        Ok(())
    }

    fn deallocate(&self, ranges: &[(u64, u64)]) -> Result<bool> {
        let start = std::time::Instant::now();
        let result = (|| {
            check_dealloc_ranges(self.num_blocks, ranges)?;
            let mut data = self.data.lock().unwrap();
            for &(lba, blocks) in ranges {
                let off = lba as usize * self.block_size;
                let len = blocks as usize * self.block_size;
                data[off..off + len].fill(0);
            }
            Ok(true)
        })();
        let bytes = ranges.iter().fold(0u64, |sum, r| sum.saturating_add(r.1)).saturating_mul(self.block_size as u64);
        dealloc::note_dealloc(start, ranges.len(), bytes, &result);
        result
    }

    fn sibling(&self) -> Result<Self> {
        Ok(self.share())
    }
}

use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::Path;

pub struct FileBlockDevice {
    block_size: usize,
    num_blocks: u64,
    file: File,
    identity: Arc<u8>,
}

impl FileBlockDevice {
    pub fn create<P: AsRef<Path>>(path: P, block_size: usize, num_blocks: u64) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        let len = block_size as u64 * num_blocks;
        file.set_len(len)?;
        Ok(FileBlockDevice {
            block_size,
            num_blocks,
            file,
            identity: Arc::new(0),
        })
    }

    pub fn open<P: AsRef<Path>>(path: P, block_size: usize) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let len = file.metadata()?.len();
        Ok(FileBlockDevice {
            block_size,
            num_blocks: len / block_size as u64,
            file,
            identity: Arc::new(0),
        })
    }
}

impl BlockDevice for FileBlockDevice {
    fn block_size(&self) -> usize {
        self.block_size
    }

    fn num_blocks(&self) -> u64 {
        self.num_blocks
    }

    fn maintenance_sibling_cache_key(&self) -> Option<MaintenanceCacheKey> {
        Some(MaintenanceCacheKey::new(
            Arc::as_ptr(&self.identity) as usize,
            0,
            self.num_blocks,
        ))
    }

    fn read_at(&self, lba: u64, buf: &mut [u8]) -> Result<()> {
        check_range(self.block_size, self.num_blocks, lba, buf.len())?;
        let off = lba * self.block_size as u64;
        self.file.read_exact_at(buf, off)?;
        Ok(())
    }

    fn write_at(&self, lba: u64, buf: &[u8]) -> Result<()> {
        check_range(self.block_size, self.num_blocks, lba, buf.len())?;
        let off = lba * self.block_size as u64;
        self.file.write_all_at(buf, off)?;
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    fn sibling(&self) -> Result<Self> {
        Ok(FileBlockDevice {
            block_size: self.block_size,
            num_blocks: self.num_blocks,
            file: self.file.try_clone()?,
            identity: self.identity.clone(),
        })
    }
}

#[cfg(feature = "spdk")]
pub mod spdk;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    const BS: usize = 4096;

    fn temp_path(tag: &str) -> std::path::PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("vals-test-{}-{}-{}", std::process::id(), tag, n))
    }

    fn roundtrip<D: BlockDevice>(dev: &D) {
        let mut w = vec![0u8; BS * 2];
        for (i, b) in w.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        dev.write_at(1, &w).unwrap();
        dev.flush().unwrap();
        let mut r = vec![0u8; BS * 2];
        dev.read_at(1, &mut r).unwrap();
        assert_eq!(w, r);
        let mut z = vec![0xffu8; BS];
        dev.read_at(0, &mut z).unwrap();
        assert!(z.iter().all(|&b| b == 0));
    }

    #[test]
    fn unmatched_async_io_completion_is_counted_and_never_wraps() {
        let before = spdk_io_stats().async_io_pending_underflows;
        let (submitter_left, completer) = std::thread::spawn(|| {
            note_async_io_submitted();
            let completer = std::thread::spawn(|| {
                let tripped = std::panic::catch_unwind(note_async_io_completed).is_err();
                (tripped, async_io_pending())
            })
            .join()
            .unwrap();
            (async_io_pending(), completer)
        })
        .join()
        .unwrap();
        assert_eq!(submitter_left, 1, "the submitting thread keeps its leaked count");
        assert_eq!(completer, (cfg!(debug_assertions), 0));
        assert!(spdk_io_stats().async_io_pending_underflows > before);
    }

    #[test]
    fn timed_write_handle_records_completion_on_the_callback_thread() {
        for success in [true, false] {
            let before_submit = std::time::Instant::now();
            let state = WriteState::timed_pending();
            let handle = WriteHandle::pending(state.clone());
            assert!(!handle.is_done());
            assert!(handle.completed_at().is_none());
            let (before_callback, after_callback) = std::thread::spawn(move || {
                let before = std::time::Instant::now();
                state.complete(success);
                (before, std::time::Instant::now())
            }).join().unwrap();
            assert!(handle.is_done());
            let completed = handle.completed_at().unwrap();
            assert!(completed >= before_submit && completed >= before_callback);
            assert!(completed <= after_callback);
            assert_eq!(handle.into_result().is_ok(), success);
        }
        let handle = WriteHandle::ready(Ok(()));
        assert!(handle.is_done());
        assert!(handle.completed_at().is_none());
    }

    #[test]
    fn mem_roundtrip() {
        let dev = MemBlockDevice::new(BS, 16);
        assert_eq!(dev.capacity_bytes(), BS as u64 * 16);
        roundtrip(&dev);
    }

    #[test]
    fn file_roundtrip_and_persist() {
        let path = temp_path("rt");
        {
            let dev = FileBlockDevice::create(&path, BS, 16).unwrap();
            roundtrip(&dev);
        }
        let dev = FileBlockDevice::open(&path, BS).unwrap();
        assert_eq!(dev.num_blocks(), 16);
        let mut r = vec![0u8; BS * 2];
        dev.read_at(1, &mut r).unwrap();
        assert_eq!(r[0], 0);
        assert_eq!(r[1], 1);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn deallocation_zero_fills_through_wrappers_and_file_is_unsupported() {
        let dev: Arc<Box<dyn BlockDevice>> = Arc::new(Box::new(MemBlockDevice::new(BS, 4)));
        dev.write_at(0, &vec![7; BS * 4]).unwrap();
        assert!(dev.deallocate(&[(1, 2)]).unwrap());
        let mut read = vec![0; BS * 4];
        dev.read_at(0, &mut read).unwrap();
        assert_eq!(&read[..BS], &vec![7; BS]);
        assert!(read[BS..BS * 3].iter().all(|&b| b == 0));
        assert_eq!(&read[BS * 3..], &vec![7; BS]);
        assert!(dev.deallocate(&[(u64::MAX, 1)]).is_err());
        let path = temp_path("dealloc-unsupported");
        let file = FileBlockDevice::create(&path, BS, 4).unwrap();
        assert!(!file.deallocate(&[(0, 4)]).unwrap());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn boxed_device_is_a_block_device() {
        let dev: Box<dyn BlockDevice> = Box::new(MemBlockDevice::new(BS, 4));
        roundtrip(&dev);
        assert_eq!(dev.num_blocks(), 4);
    }

    #[test]
    fn mem_maintenance_cache_key_tracks_backing_identity_through_wrappers() {
        let dev = MemBlockDevice::new(BS, 4);
        let sibling = dev.sibling().unwrap();
        let fresh = MemBlockDevice::new(BS, 4);
        assert_eq!(
            dev.maintenance_sibling_cache_key(),
            sibling.maintenance_sibling_cache_key()
        );
        assert_ne!(
            dev.maintenance_sibling_cache_key(),
            fresh.maintenance_sibling_cache_key()
        );

        let shared = Arc::new(dev);
        assert_eq!(
            shared.maintenance_sibling_cache_key(),
            shared.as_ref().maintenance_sibling_cache_key()
        );
        let boxed: Box<dyn BlockDevice> = Box::new(fresh);
        assert!(boxed.maintenance_sibling_cache_key().is_some());
    }

    #[test]
    fn file_maintenance_cache_key_changes_when_path_is_recreated() {
        let path = temp_path("maintenance-key");
        let old = FileBlockDevice::create(&path, BS, 4).unwrap();
        let old_sibling = old.sibling().unwrap();
        assert_eq!(
            old.maintenance_sibling_cache_key(),
            old_sibling.maintenance_sibling_cache_key()
        );

        std::fs::remove_file(&path).unwrap();
        let new = FileBlockDevice::create(&path, BS, 4).unwrap();
        assert_ne!(
            old.maintenance_sibling_cache_key(),
            new.maintenance_sibling_cache_key()
        );
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn dropped_heap_scratch_returns_to_pool() {
        HEAP_SCRATCH_POOL.with(|pool| pool.borrow_mut().clear());
        let scratch = Scratch::heap_take(BS);
        HEAP_SCRATCH_POOL.with(|pool| assert!(pool.borrow().is_empty()));
        drop(scratch);
        HEAP_SCRATCH_POOL.with(|pool| assert_eq!(pool.borrow().len(), 1));
    }

    #[test]
    fn read_batch_matches_individual_reads() {
        let dev = MemBlockDevice::new(BS, 8);
        for lba in 0..8u64 {
            dev.write_at(lba, &vec![lba as u8; BS]).unwrap();
        }
        let (mut b0, mut b1, mut b2) = (vec![0u8; BS], vec![0u8; BS], vec![0u8; BS]);
        let mut ops = vec![
            ReadOp {
                lba: 5,
                buf: &mut b0,
            },
            ReadOp {
                lba: 1,
                buf: &mut b1,
            },
            ReadOp {
                lba: 7,
                buf: &mut b2,
            },
        ];
        dev.read_batch(&mut ops).unwrap();
        assert!(b0.iter().all(|&x| x == 5));
        assert!(b1.iter().all(|&x| x == 1));
        assert!(b2.iter().all(|&x| x == 7));
    }

    #[test]
    fn rejects_misaligned() {
        let dev = MemBlockDevice::new(BS, 4);
        let mut buf = vec![0u8; BS + 1];
        assert!(matches!(
            dev.read_at(0, &mut buf),
            Err(BlockError::Misaligned { .. })
        ));
    }

    #[test]
    fn rejects_out_of_range() {
        let dev = MemBlockDevice::new(BS, 4);
        let mut buf = vec![0u8; BS * 2];
        assert!(matches!(
            dev.read_at(3, &mut buf),
            Err(BlockError::OutOfRange { .. })
        ));
        assert!(dev.read_at(2, &mut buf).is_ok());
    }
}

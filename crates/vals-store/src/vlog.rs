use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::crc::Crc32;
use vals_spdk::{AsyncRead, BlockDevice, BlockError, ReadOp, Scratch, WriteHandle};

use crate::alloc::{Allocator, SegmentId};

const MAX_INFLIGHT_WRITES: usize = 64;

static VLOG_STALLS: AtomicU64 = AtomicU64::new(0);
static VLOG_STALL_US_TOTAL: AtomicU64 = AtomicU64::new(0);
static VLOG_SYNC_DRAIN_COUNT: AtomicU64 = AtomicU64::new(0);
static VLOG_SYNC_DRAIN_US: AtomicU64 = AtomicU64::new(0);
static VLOG_SYNC_TAIL_COUNT: AtomicU64 = AtomicU64::new(0);
static VLOG_SYNC_TAIL_US: AtomicU64 = AtomicU64::new(0);
static VLOG_TAIL_WRITE_BYTES: AtomicU64 = AtomicU64::new(0);
static VLOG_REPLAY_SKIPS: AtomicU64 = AtomicU64::new(0);
static VLOG_REPLAY_SKIPPED_BYTES: AtomicU64 = AtomicU64::new(0);

pub fn vlog_replay_skip_totals() -> (u64, u64) {
    (
        VLOG_REPLAY_SKIPS.load(Ordering::Relaxed),
        VLOG_REPLAY_SKIPPED_BYTES.load(Ordering::Relaxed),
    )
}

pub fn vlog_write_bytes() -> u64 {
    crate::attribution::normal_vlog_bytes() + VLOG_TAIL_WRITE_BYTES.load(Ordering::Relaxed)
}

pub fn vlog_sync_totals() -> (u64, u64, u64, u64) {
    (
        VLOG_SYNC_DRAIN_COUNT.load(Ordering::Relaxed),
        VLOG_SYNC_DRAIN_US.load(Ordering::Relaxed),
        VLOG_SYNC_TAIL_COUNT.load(Ordering::Relaxed),
        VLOG_SYNC_TAIL_US.load(Ordering::Relaxed),
    )
}

pub fn vlog_stall_totals() -> (u64, u64) {
    (
        VLOG_STALLS.load(Ordering::Relaxed),
        VLOG_STALL_US_TOTAL.load(Ordering::Relaxed),
    )
}

#[inline]
#[allow(clippy::uninit_vec)] // deliberate: see SAFETY — the device read fills all
pub(crate) fn io_buf(len: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(len);
    // SAFETY: `with_capacity(len)` reserved exactly `len` bytes. `u8` has no
    // invalid bit patterns, and the contract above guarantees the device read
    // overwrites all `len` bytes before any read of `buf` (on read error the
    // caller returns before decoding) — so no uninitialized byte is ever read.
    unsafe { buf.set_len(len) };
    buf
}

const REC_MAGIC: u32 = 0x324C_4156;
const SEG_MAGIC: u32 = 0x3147_5356;

pub const FORMAT_VERSION: u16 = 1;

pub const HEADER_LEN: usize = 50;
const VERSION_OFF: usize = 30;
const CRC_OFF: usize = 46;
const KEY_OFF: usize = 50;

pub const SEG_HEADER_LEN: usize = 22;

pub const FLAG_TOMBSTONE: u8 = 0x01;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordKind {
    Put = 0,
    Delete = 1,
    RangeDelete = 2,
}

impl RecordKind {
    fn from_u8(b: u8) -> Option<Self> {
        match b {
            0 => Some(RecordKind::Put),
            1 => Some(RecordKind::Delete),
            2 => Some(RecordKind::RangeDelete),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub seq: u64,
    pub epoch: u64,
    pub kind: RecordKind,
    pub tombstone: bool,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub version: u128,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValuePtr {
    pub segment: SegmentId,
    pub offset: u32,
    pub len: u32,
}

#[derive(Debug)]
pub enum LogError {
    Io(BlockError),
    RecordTooLarge {
        size: usize,
        max: usize,
    },
    DeviceFull,
    Backpressure,
    Corrupt(&'static str),
    Unsupported(&'static str),
}

impl std::fmt::Display for LogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogError::Io(e) => write!(f, "value-log io error: {e}"),
            LogError::RecordTooLarge { size, max } => {
                write!(f, "record of {size} bytes exceeds segment capacity {max}")
            }
            LogError::DeviceFull => write!(f, "no free segments"),
            LogError::Backpressure => write!(
                f,
                "write backpressure: free segments held for maintenance reclaim; retry"
            ),
            LogError::Corrupt(why) => write!(f, "corrupt record: {why}"),
            LogError::Unsupported(what) => write!(
                f,
                "value log holds an intact {what} this release does not know \
                 (written by a newer release?); refusing to open"
            ),
        }
    }
}

impl std::error::Error for LogError {}

impl From<BlockError> for LogError {
    fn from(e: BlockError) -> Self {
        LogError::Io(e)
    }
}

fn encode_header(
    kind: RecordKind,
    tombstone: bool,
    seq: u64,
    epoch: u64,
    version: u128,
    key_len: usize,
    val_len: usize,
) -> [u8; HEADER_LEN] {
    let mut h = [0u8; HEADER_LEN];
    h[0..4].copy_from_slice(&REC_MAGIC.to_le_bytes());
    h[4] = kind as u8;
    h[5] = if tombstone { FLAG_TOMBSTONE } else { 0 };
    h[6..10].copy_from_slice(&(key_len as u32).to_le_bytes());
    h[10..14].copy_from_slice(&(val_len as u32).to_le_bytes());
    h[14..22].copy_from_slice(&seq.to_le_bytes());
    h[22..30].copy_from_slice(&epoch.to_le_bytes());
    h[VERSION_OFF..VERSION_OFF + 16].copy_from_slice(&version.to_le_bytes());
    h
}

fn validate_record(b: &[u8]) -> Result<(RecordKind, bool, usize, u64, u64, u128), LogError> {
    if b.len() < HEADER_LEN {
        return Err(LogError::Corrupt("short header"));
    }
    let magic = u32::from_le_bytes(b[0..4].try_into().unwrap());
    if magic != REC_MAGIC {
        return Err(LogError::Corrupt("bad magic"));
    }
    let kind = RecordKind::from_u8(b[4]).ok_or(LogError::Corrupt(BAD_KIND))?;
    let tombstone = b[5] & FLAG_TOMBSTONE != 0;
    let key_len = u32::from_le_bytes(b[6..10].try_into().unwrap()) as usize;
    let val_len = u32::from_le_bytes(b[10..14].try_into().unwrap()) as usize;
    let seq = u64::from_le_bytes(b[14..22].try_into().unwrap());
    let epoch = u64::from_le_bytes(b[22..30].try_into().unwrap());
    let version = u128::from_le_bytes(b[VERSION_OFF..VERSION_OFF + 16].try_into().unwrap());
    let stored_crc = u32::from_le_bytes(b[CRC_OFF..KEY_OFF].try_into().unwrap());
    if b.len() != HEADER_LEN + key_len + val_len {
        return Err(LogError::Corrupt("length mismatch"));
    }
    let mut c = Crc32::new();
    c.update(&b[0..CRC_OFF]);
    c.update(&b[KEY_OFF..]);
    if c.finalize() != stored_crc {
        return Err(LogError::Corrupt("crc mismatch"));
    }
    Ok((kind, tombstone, key_len, seq, epoch, version))
}

pub fn decode_record(b: &[u8]) -> Result<Record, LogError> {
    let (kind, tombstone, key_len, seq, epoch, version) = validate_record(b)?;
    let key = b[KEY_OFF..KEY_OFF + key_len].to_vec();
    let value = b[KEY_OFF + key_len..].to_vec();
    Ok(Record {
        seq,
        epoch,
        kind,
        tombstone,
        key,
        value,
        version,
    })
}

const BAD_KIND: &str = "bad kind";
const KEY_MISMATCH: &str = "record key mismatch";
const VERSION_MISMATCH: &str = "record version mismatch";

pub const CORRUPT_READ_KINDS: [&str; 7] = ["magic", "kind", "length", "crc", "key", "version", "other"];

pub fn corrupt_read_kind(reason: &str) -> &'static str {
    match reason {
        "bad magic" => "magic",
        BAD_KIND => "kind",
        "short header" | "length mismatch" => "length",
        "crc mismatch" => "crc",
        KEY_MISMATCH => "key",
        VERSION_MISMATCH => "version",
        _ => "other",
    }
}

fn check_identity(
    b: &[u8],
    key_len: usize,
    version: u128,
    expected_key: Option<&[u8]>,
    expected_version: Option<u128>,
) -> Result<(), LogError> {
    if expected_key.is_some_and(|k| &b[KEY_OFF..KEY_OFF + key_len] != k) {
        return Err(LogError::Corrupt(KEY_MISMATCH));
    }
    if expected_version.is_some_and(|v| v != version) {
        return Err(LogError::Corrupt(VERSION_MISMATCH));
    }
    Ok(())
}

pub fn decode_record_value(b: &[u8]) -> Result<Record, LogError> {
    decode_record_value_inner(b, None, None)
}

fn decode_record_value_checked(
    b: &[u8],
    expected_key: &[u8],
    expected_version: u128,
) -> Result<Record, LogError> {
    decode_record_value_inner(b, Some(expected_key), Some(expected_version))
}

fn decode_record_value_inner(
    b: &[u8],
    expected_key: Option<&[u8]>,
    expected_version: Option<u128>,
) -> Result<Record, LogError> {
    let (kind, tombstone, key_len, seq, epoch, version) = validate_record(b)?;
    check_identity(b, key_len, version, expected_key, expected_version)?;
    Ok(Record {
        seq,
        epoch,
        kind,
        tombstone,
        key: Vec::new(),
        value: b[KEY_OFF + key_len..].to_vec(),
        version,
    })
}

pub struct ReadPlan {
    lba: u64,
    nblocks: usize,
    block_size: usize,
    within: usize,
    ptr: ValuePtr,
    tail_overlay: Option<(usize, Vec<u8>)>,
}

impl ReadPlan {
    pub async fn execute<D: AsyncRead + BlockDevice>(&self, dev: &D) -> Result<Record, LogError> {
        self.execute_inner(dev, None).await
    }

    pub async fn execute_checked<D: AsyncRead + BlockDevice>(
        &self,
        dev: &D,
        expected_key: &[u8],
    ) -> Result<Record, LogError> {
        self.execute_inner(dev, Some(expected_key)).await
    }

    async fn execute_inner<D: AsyncRead + BlockDevice>(
        &self,
        dev: &D,
        expected_key: Option<&[u8]>,
    ) -> Result<Record, LogError> {
        let buf = self.read_buf(dev).await?;
        let rec = decode_record_value_inner(self.record(&buf), expected_key, None);
        dev.release_scratch(buf);
        rec
    }

    pub fn read_len(&self) -> usize {
        self.nblocks * self.block_size
    }

    pub fn ptr(&self) -> ValuePtr {
        self.ptr
    }

    #[doc(hidden)]
    pub fn read_range(&self) -> (u64, usize) {
        (self.lba, self.nblocks)
    }

    async fn read_buf<D: AsyncRead + BlockDevice>(&self, dev: &D) -> Result<Scratch, LogError> {
        let mut buf: Scratch = dev.take_scratch(self.nblocks * self.block_size);
        if let Err(e) = dev.read_async(self.lba, buf.as_mut_slice()).await {
            dev.release_scratch(buf);
            return Err(e.into());
        }
        if let Some((off, tail)) = &self.tail_overlay {
            buf.as_mut_slice()[*off..*off + self.block_size].copy_from_slice(tail);
        }
        Ok(buf)
    }

    fn record<'a>(&self, buf: &'a [u8]) -> &'a [u8] {
        &buf[self.within..self.within + self.ptr.len as usize]
    }

    pub async fn read_value<D, R>(
        &self,
        dev: &D,
        f: impl FnOnce(u128, &[u8]) -> R,
    ) -> Result<R, LogError>
    where
        D: AsyncRead + BlockDevice,
    {
        self.read_value_inner(dev, None, None, f).await
    }

    pub async fn read_value_checked<D, R>(
        &self,
        dev: &D,
        expected_key: &[u8],
        expected_version: u128,
        f: impl FnOnce(u128, &[u8]) -> R,
    ) -> Result<R, LogError>
    where
        D: AsyncRead + BlockDevice,
    {
        self.read_value_inner(dev, Some(expected_key), Some(expected_version), f)
            .await
    }

    async fn read_value_inner<D, R>(
        &self,
        dev: &D,
        expected_key: Option<&[u8]>,
        expected_version: Option<u128>,
        f: impl FnOnce(u128, &[u8]) -> R,
    ) -> Result<R, LogError>
    where
        D: AsyncRead + BlockDevice,
    {
        let buf = self.read_buf(dev).await?;
        let out = (|| {
            let rec = self.record(&buf);
            let (_, _, key_len, _, _, version) = validate_record(rec)?;
            check_identity(rec, key_len, version, expected_key, expected_version)?;
            Ok(f(version, &rec[KEY_OFF + key_len..]))
        })();
        dev.release_scratch(buf);
        out
    }
}

const SPAN_GAP_BLOCKS: u64 = 4;
const SPAN_MAX_BYTES: usize = 128 * 1024;

struct Span {
    lba: u64,
    nblocks: usize,
    tail_overlay: Option<(usize, Vec<u8>)>,
    slots: Vec<(u32, u32, ValuePtr)>,
}

pub struct SpanReadPlan {
    block_size: usize,
    spans: Vec<Span>,
    num_records: usize,
}

#[derive(Debug)]
pub struct SpanReadError {
    pub failed: Option<(usize, ValuePtr)>,
    pub error: LogError,
}

impl std::fmt::Display for SpanReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for SpanReadError {}

impl From<SpanReadError> for LogError {
    fn from(e: SpanReadError) -> Self {
        e.error
    }
}

type UncheckedSpan = fn(usize) -> (&'static [u8], u128);

impl SpanReadPlan {
    pub fn num_spans(&self) -> usize {
        self.spans.len()
    }

    pub fn num_records(&self) -> usize {
        self.num_records
    }

    pub async fn execute_span<D: AsyncRead + BlockDevice>(
        &self,
        i: usize,
        dev: &D,
    ) -> Result<Vec<(usize, Record)>, LogError> {
        Ok(self
            .execute_span_inner(i, dev, None::<&UncheckedSpan>)
            .await?)
    }

    pub async fn execute_span_checked<'k, D, F>(
        &self,
        i: usize,
        dev: &D,
        expected: &F,
    ) -> Result<Vec<(usize, Record)>, SpanReadError>
    where
        D: AsyncRead + BlockDevice,
        F: Fn(usize) -> (&'k [u8], u128),
    {
        self.execute_span_inner(i, dev, Some(expected)).await
    }

    async fn execute_span_inner<'k, D, F>(
        &self,
        i: usize,
        dev: &D,
        expected: Option<&F>,
    ) -> Result<Vec<(usize, Record)>, SpanReadError>
    where
        D: AsyncRead + BlockDevice,
        F: Fn(usize) -> (&'k [u8], u128),
    {
        let span = &self.spans[i];
        let mut buf: Scratch = dev.take_scratch(span.nblocks * self.block_size);
        let read = dev
            .read_async(
                span.lba,
                &mut buf.as_mut_slice()[..span.nblocks * self.block_size],
            )
            .await;
        if let Err(e) = read {
            dev.release_scratch(buf);
            return Err(SpanReadError {
                failed: None,
                error: e.into(),
            });
        }
        if let Some((off, tail)) = &span.tail_overlay {
            buf.as_mut_slice()[*off..*off + self.block_size].copy_from_slice(tail);
        }
        let out = Self::decode_span(span, &buf[..span.nblocks * self.block_size], expected);
        dev.release_scratch(buf);
        out
    }

    pub fn execute_sync<D: BlockDevice>(&self, dev: &D) -> Result<Vec<Record>, LogError> {
        Ok(self.execute_sync_inner(dev, None::<&UncheckedSpan>)?)
    }

    pub fn execute_sync_checked<'k, D, F>(
        &self,
        dev: &D,
        expected: &F,
    ) -> Result<Vec<Record>, SpanReadError>
    where
        D: BlockDevice,
        F: Fn(usize) -> (&'k [u8], u128),
    {
        self.execute_sync_inner(dev, Some(expected))
    }

    fn execute_sync_inner<'k, D, F>(
        &self,
        dev: &D,
        expected: Option<&F>,
    ) -> Result<Vec<Record>, SpanReadError>
    where
        D: BlockDevice,
        F: Fn(usize) -> (&'k [u8], u128),
    {
        let mut bufs: Vec<Vec<u8>> = self
            .spans
            .iter()
            .map(|s| io_buf(s.nblocks * self.block_size))
            .collect();
        {
            let mut ops: Vec<ReadOp<'_>> = bufs
                .iter_mut()
                .zip(&self.spans)
                .map(|(buf, s)| ReadOp { lba: s.lba, buf })
                .collect();
            dev.read_batch(&mut ops).map_err(|e| SpanReadError {
                failed: None,
                error: e.into(),
            })?;
        }
        let mut out: Vec<Option<Record>> = Vec::with_capacity(self.num_records);
        out.resize_with(self.num_records, || None);
        for (span, buf) in self.spans.iter().zip(&mut bufs) {
            if let Some((off, tail)) = &span.tail_overlay {
                buf[*off..*off + self.block_size].copy_from_slice(tail);
            }
            for (slot, rec) in Self::decode_span(span, buf, expected)? {
                out[slot] = Some(rec);
            }
        }
        Ok(out
            .into_iter()
            .map(|r| r.expect("every slot decoded by exactly one span"))
            .collect())
    }

    fn decode_span<'k, F: Fn(usize) -> (&'k [u8], u128)>(
        span: &Span,
        buf: &[u8],
        expected: Option<&F>,
    ) -> Result<Vec<(usize, Record)>, SpanReadError> {
        let mut out = Vec::with_capacity(span.slots.len());
        for &(slot, off, ptr) in &span.slots {
            let slot = slot as usize;
            let (key, version) = match expected {
                Some(f) => {
                    let (key, version) = f(slot);
                    (Some(key), Some(version))
                }
                None => (None, None),
            };
            let bytes = &buf[off as usize..off as usize + ptr.len as usize];
            match decode_record_value_inner(bytes, key, version) {
                Ok(rec) => out.push((slot, rec)),
                Err(error) => {
                    return Err(SpanReadError {
                        failed: Some((slot, ptr)),
                        error,
                    })
                }
            }
        }
        Ok(out)
    }
}

pub fn plan_read_spans_detached(block_size: usize, items: &[(u64, ValuePtr)]) -> SpanReadPlan {
    let bs = block_size;
    let span_blocks = |ptr: &ValuePtr| -> (u64, usize) {
        let b = bs as u64;
        let first = ptr.offset as u64 / b;
        let last = (ptr.offset as u64 + ptr.len as u64 - 1) / b;
        (first, (last - first + 1) as usize)
    };
    let mut recs: Vec<(u64, usize, u32)> = items
        .iter()
        .enumerate()
        .map(|(slot, (seg_start, ptr))| {
            let (first, nblocks) = span_blocks(ptr);
            (seg_start + first, nblocks, slot as u32)
        })
        .collect();
    recs.sort_unstable_by_key(|&(lba, _, _)| lba);

    let max_blocks = (SPAN_MAX_BYTES / bs).max(1) as u64;
    let mut spans: Vec<Span> = Vec::new();
    for &(lba, nblocks, slot) in &recs {
        let ptr = &items[slot as usize].1;
        let end = lba + nblocks as u64;
        let merged = match spans.last_mut() {
            Some(cur) => {
                let cur_end = cur.lba + cur.nblocks as u64;
                if lba <= cur_end + SPAN_GAP_BLOCKS && end - cur.lba <= max_blocks {
                    cur.nblocks = cur.nblocks.max((end - cur.lba) as usize);
                    cur.slots.push((
                        slot,
                        ((lba - cur.lba) as usize * bs + ptr.offset as usize % bs) as u32,
                        *ptr,
                    ));
                    true
                } else {
                    false
                }
            }
            None => false,
        };
        if !merged {
            spans.push(Span {
                lba,
                nblocks,
                tail_overlay: None,
                slots: vec![(slot, (ptr.offset as usize % bs) as u32, *ptr)],
            });
        }
    }

    SpanReadPlan {
        block_size: bs,
        spans,
        num_records: items.len(),
    }
}

pub struct VlogWriter<'a, D: BlockDevice> {
    dev: &'a D,
    segs: &'a [SegmentId],
    seg_lbas: &'a [u64],
    epochs: &'a [u64],
    seg_capacity: usize,
    block_size: usize,
    seg_idx: usize,
    open: bool,
    buf: Vec<u8>,
    err: Option<LogError>,
}

impl<'a, D: BlockDevice> VlogWriter<'a, D> {
    pub fn new(
        dev: &'a D,
        segs: &'a [SegmentId],
        seg_lbas: &'a [u64],
        epochs: &'a [u64],
        seg_capacity: usize,
        block_size: usize,
    ) -> Self {
        debug_assert_eq!(segs.len(), seg_lbas.len());
        debug_assert_eq!(segs.len(), epochs.len());
        VlogWriter {
            dev,
            segs,
            seg_lbas,
            epochs,
            seg_capacity,
            block_size,
            seg_idx: usize::MAX,
            open: false,
            buf: Vec::with_capacity(seg_capacity),
            err: None,
        }
    }

    pub fn usable_per_segment(&self) -> usize {
        self.seg_capacity - SEG_HEADER_LEN
    }

    #[allow(clippy::too_many_arguments)]
    pub fn append(
        &mut self,
        kind: RecordKind,
        tombstone: bool,
        seq: u64,
        version: u128,
        key: &[u8],
        value: &[u8],
    ) -> Result<ValuePtr, LogError> {
        if self.err.is_some() {
            return Err(LogError::Corrupt(
                "vlog co-compaction writer already errored",
            ));
        }
        let total = HEADER_LEN + key.len() + value.len();
        if total > self.usable_per_segment() {
            return Err(LogError::RecordTooLarge {
                size: total,
                max: self.usable_per_segment(),
            });
        }
        let need_roll = !self.open || self.buf.len() + total > self.seg_capacity;
        if need_roll {
            self.roll()?;
        }
        let seg = self.segs[self.seg_idx];
        let epoch = self.epochs[self.seg_idx];
        let offset = self.buf.len() as u32;
        let mut header =
            encode_header(kind, tombstone, seq, epoch, version, key.len(), value.len());
        let mut c = Crc32::new();
        c.update(&header[0..CRC_OFF]);
        c.update(key);
        c.update(value);
        header[CRC_OFF..KEY_OFF].copy_from_slice(&c.finalize().to_le_bytes());
        self.buf.extend_from_slice(&header);
        self.buf.extend_from_slice(key);
        self.buf.extend_from_slice(value);
        Ok(ValuePtr {
            segment: seg,
            offset,
            len: total as u32,
        })
    }

    fn roll(&mut self) -> Result<(), LogError> {
        if self.open {
            self.flush_current()?;
        }
        let next = if self.seg_idx == usize::MAX {
            0
        } else {
            self.seg_idx + 1
        };
        if next >= self.segs.len() {
            let e = LogError::Corrupt("vlog co-compaction exceeded its reserved segments");
            self.err = Some(LogError::Corrupt(
                "vlog co-compaction exceeded its reserved segments",
            ));
            return Err(e);
        }
        self.seg_idx = next;
        self.buf.clear();
        let hdr = encode_seg_header_flagged(
            self.epochs[next],
            self.block_size as u32,
            /* not_tail */ true,
        );
        self.buf.extend_from_slice(&hdr);
        self.open = true;
        Ok(())
    }

    fn flush_current(&mut self) -> Result<(), LogError> {
        let lba = self.seg_lbas[self.seg_idx];
        let nblocks = self.buf.len().div_ceil(self.block_size).max(1);
        let mut out = vec![0u8; nblocks * self.block_size];
        out[..self.buf.len()].copy_from_slice(&self.buf);
        crate::attribution::write_bytes(2, out.len());
        if let Err(e) = self.dev.write_at(lba, &out) {
            let le = LogError::from(e);
            self.err = Some(LogError::Corrupt("vlog co-compaction write failed"));
            return Err(le);
        }
        Ok(())
    }

    pub async fn finish(mut self) -> Result<Vec<SegmentId>, LogError> {
        if let Some(e) = self.err.take() {
            return Err(e);
        }
        let used = if self.open {
            self.flush_current()?;
            self.seg_idx + 1
        } else {
            0
        };
        self.dev.flush()?;
        vals_rt::yield_now().await;
        Ok(self.segs[..used].to_vec())
    }
}

fn encode_seg_header(epoch: u64, block_size: u32) -> Vec<u8> {
    encode_seg_header_flagged(epoch, block_size, false)
}

pub const SEG_FLAG_NOT_TAIL: u16 = 0x8000;

fn encode_seg_header_flagged(epoch: u64, block_size: u32, not_tail: bool) -> Vec<u8> {
    let mut b = vec![0u8; SEG_HEADER_LEN];
    b[0..4].copy_from_slice(&SEG_MAGIC.to_le_bytes());
    let version = FORMAT_VERSION | if not_tail { SEG_FLAG_NOT_TAIL } else { 0 };
    b[4..6].copy_from_slice(&version.to_le_bytes());
    b[6..14].copy_from_slice(&epoch.to_le_bytes());
    b[14..18].copy_from_slice(&block_size.to_le_bytes());
    let crc = {
        let mut c = Crc32::new();
        c.update(&b[0..18]);
        c.finalize()
    };
    b[18..22].copy_from_slice(&crc.to_le_bytes());
    b
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegHeader {
    pub epoch: u64,
    pub block_size: u32,
    pub not_tail: bool,
}

pub fn decode_seg_header(b: &[u8]) -> Result<SegHeader, LogError> {
    if b.len() < SEG_HEADER_LEN {
        return Err(LogError::Corrupt("short seg header"));
    }
    let magic = u32::from_le_bytes(b[0..4].try_into().unwrap());
    if magic != SEG_MAGIC {
        return Err(LogError::Corrupt("bad seg magic"));
    }
    let stored_crc = u32::from_le_bytes(b[18..22].try_into().unwrap());
    let mut c = Crc32::new();
    c.update(&b[0..18]);
    if c.finalize() != stored_crc {
        return Err(LogError::Corrupt("seg crc mismatch"));
    }
    let raw_version = u16::from_le_bytes(b[4..6].try_into().unwrap());
    let not_tail = raw_version & SEG_FLAG_NOT_TAIL != 0;
    let version = raw_version & !SEG_FLAG_NOT_TAIL;
    if version != FORMAT_VERSION {
        return Err(LogError::Unsupported("segment format version"));
    }
    Ok(SegHeader {
        epoch: u64::from_le_bytes(b[6..14].try_into().unwrap()),
        block_size: u32::from_le_bytes(b[14..18].try_into().unwrap()),
        not_tail,
    })
}

pub struct ValueLog {
    block_size: usize,
    seg_blocks: u64,
    cur_seg: Option<SegmentId>,
    cur_seg_start: u64,
    write_off: u32,
    tail: Box<[u8]>,
    tail_dirty: bool,
    next_seq: u64,
    next_epoch: u64,
    cur_epoch: u64,
    inflight: RefCell<Vec<WriteHandle>>,
    tail_segs: Vec<(u64, SegmentId)>,
}

impl ValueLog {
    pub fn new(block_size: usize, seg_blocks: u64, start_seq: u64, start_epoch: u64) -> Self {
        assert!(block_size > 0 && seg_blocks > 0);
        assert!(
            seg_blocks
                .checked_mul(block_size as u64)
                .is_some_and(|bytes| bytes <= u32::MAX as u64),
            "segment size ({seg_blocks} blocks x {block_size} B) exceeds the u32 segment-offset space"
        );
        ValueLog {
            block_size,
            seg_blocks,
            cur_seg: None,
            cur_seg_start: 0,
            write_off: 0,
            tail: vec![0u8; block_size].into_boxed_slice(),
            tail_dirty: false,
            next_seq: start_seq,
            next_epoch: start_epoch,
            cur_epoch: 0,
            inflight: RefCell::new(Vec::new()),
            tail_segs: Vec::new(),
        }
    }

    fn reap(&self) -> Result<(), LogError> {
        let mut inflight = self.inflight.borrow_mut();
        let mut i = 0;
        let mut result = Ok(());
        while i < inflight.len() {
            if inflight[i].is_done() {
                let h = inflight.swap_remove(i);
                if let Err(e) = h.into_result() {
                    result = Err(e.into());
                }
            } else {
                i += 1;
            }
        }
        result
    }

    fn head_done(&self) -> bool {
        self.inflight.borrow().first().is_some_and(|h| h.is_done())
    }

    fn submit_block<D: BlockDevice>(&self, dev: &D, lba: u64, buf: &[u8]) -> Result<(), LogError> {
        if self.inflight.borrow().len() >= MAX_INFLIGHT_WRITES {
            let stall_start = std::time::Instant::now();
            while self.inflight.borrow().len() >= MAX_INFLIGHT_WRITES {
                if dev.poll_writes() > 0 || self.head_done() {
                    self.reap()?;
                }
            }
            let us = stall_start.elapsed().as_micros() as u64;
            VLOG_STALLS.fetch_add(1, Ordering::Relaxed);
            VLOG_STALL_US_TOTAL.fetch_add(us, Ordering::Relaxed);
        }
        let h = dev.submit_write(lba, buf);
        crate::attribution::write_bytes(0, buf.len());
        self.inflight.borrow_mut().push(h);
        Ok(())
    }

    fn drain_inflight<D: BlockDevice>(&self, dev: &D) -> Result<(), LogError> {
        while !self.inflight.borrow().is_empty() {
            if dev.poll_writes() > 0 || self.head_done() {
                self.reap()?;
            }
        }
        Ok(())
    }

    pub(crate) fn quiesce<D: BlockDevice>(&self, dev: &D) {
        while !self.inflight.borrow().is_empty() {
            if dev.poll_writes() > 0 || self.head_done() {
                let _ = self.reap();
            }
        }
    }

    pub fn inflight_writes(&self) -> usize {
        self.inflight.borrow().iter().filter(|h| !h.is_done()).count()
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    pub fn skip_seqs_below(&mut self, seq: u64) {
        self.next_seq = self.next_seq.max(seq);
    }

    pub fn next_epoch(&self) -> u64 {
        self.next_epoch
    }

    pub fn reserve_epochs(&mut self, n: usize) -> Vec<u64> {
        let start = self.next_epoch;
        self.next_epoch += n as u64;
        (start..start + n as u64).collect()
    }

    pub fn open_segment(&self) -> Option<SegmentId> {
        self.cur_seg
    }

    pub fn seed_tail_segs(&mut self, entries: impl IntoIterator<Item = (u64, SegmentId)>) {
        self.tail_segs.extend(entries);
    }

    pub fn tail_segs_through(&self, epoch: u64) -> Vec<SegmentId> {
        self.tail_segs
            .iter()
            .filter(|(e, _)| *e <= epoch)
            .map(|(_, s)| *s)
            .collect()
    }

    pub fn prune_tail_segs_through(&mut self, epoch: u64) {
        self.tail_segs.retain(|(e, _)| *e > epoch);
    }

    pub fn forget_tail_seg(&mut self, seg: SegmentId) {
        self.tail_segs.retain(|(_, s)| *s != seg);
    }

    pub fn take_tail_segs(&mut self) -> Vec<(u64, SegmentId)> {
        std::mem::take(&mut self.tail_segs)
    }

    fn seg_capacity(&self) -> usize {
        self.seg_blocks as usize * self.block_size
    }

    pub fn append<D: BlockDevice>(
        &mut self,
        dev: &D,
        alloc: &mut Allocator,
        kind: RecordKind,
        version: u128,
        key: &[u8],
        value: &[u8],
    ) -> Result<(u64, ValuePtr), LogError> {
        self.append_tombstone(dev, alloc, kind, false, version, key, value)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn append_tombstone<D: BlockDevice>(
        &mut self,
        dev: &D,
        alloc: &mut Allocator,
        kind: RecordKind,
        tombstone: bool,
        version: u128,
        key: &[u8],
        value: &[u8],
    ) -> Result<(u64, ValuePtr), LogError> {
        let total = HEADER_LEN + key.len() + value.len();
        let max = self.seg_capacity() - SEG_HEADER_LEN;
        if total > max {
            return Err(LogError::RecordTooLarge { size: total, max });
        }
        let need_roll = match self.cur_seg {
            None => true,
            Some(_) => self.write_off as usize + total > self.seg_capacity(),
        };
        if need_roll {
            self.roll(dev, alloc)?;
        }

        let seq = self.next_seq;
        self.next_seq += 1;
        let mut header = encode_header(
            kind,
            tombstone,
            seq,
            self.cur_epoch,
            version,
            key.len(),
            value.len(),
        );
        let mut c = Crc32::new();
        c.update(&header[0..CRC_OFF]);
        c.update(key);
        c.update(value);
        header[CRC_OFF..KEY_OFF].copy_from_slice(&c.finalize().to_le_bytes());
        let start_off = self.write_off;
        let seg = self.cur_seg.expect("segment open after roll");
        self.append_bytes(dev, &header)?;
        self.append_bytes(dev, key)?;
        self.append_bytes(dev, value)?;
        Ok((
            seq,
            ValuePtr {
                segment: seg,
                offset: start_off,
                len: total as u32,
            },
        ))
    }

    fn span_blocks(&self, ptr: &ValuePtr) -> (u64, usize) {
        let bs = self.block_size as u64;
        let first = ptr.offset as u64 / bs;
        let last = (ptr.offset as u64 + ptr.len as u64 - 1) / bs;
        (first, (last - first + 1) as usize)
    }

    fn tail_overlay_off(&self, ptr: &ValuePtr, first: u64, nblocks: usize) -> Option<usize> {
        if Some(ptr.segment) != self.cur_seg || !self.tail_dirty {
            return None;
        }
        let bs = self.block_size as u64;
        let tail_block = self.write_off as u64 / bs;
        let last = first + nblocks as u64 - 1;
        (tail_block >= first && tail_block <= last).then(|| ((tail_block - first) * bs) as usize)
    }

    fn read_bufs(&self, items: &[(u64, ValuePtr)]) -> Vec<Vec<u8>> {
        items
            .iter()
            .map(|(_, ptr)| io_buf(self.span_blocks(ptr).1 * self.block_size))
            .collect()
    }

    fn record_bytes<'a>(&self, buf: &'a mut [u8], ptr: &ValuePtr) -> &'a [u8] {
        let (first, nblocks) = self.span_blocks(ptr);
        if let Some(off) = self.tail_overlay_off(ptr, first, nblocks) {
            buf[off..off + self.block_size].copy_from_slice(&self.tail);
        }
        let within = ptr.offset as usize % self.block_size;
        &buf[within..within + ptr.len as usize]
    }

    fn decode_records_checked(
        &self,
        bufs: &mut [Vec<u8>],
        items: &[(u64, ValuePtr)],
        expected: &[(&[u8], u128)],
    ) -> Result<Vec<Record>, LogError> {
        if expected.len() != items.len() {
            return Err(LogError::Corrupt("checked batch length mismatch"));
        }
        let mut out = Vec::with_capacity(items.len());
        for ((buf, (_, ptr)), &(key, version)) in bufs.iter_mut().zip(items).zip(expected) {
            out.push(decode_record_value_checked(
                self.record_bytes(buf, ptr),
                key,
                version,
            )?);
        }
        Ok(out)
    }

    pub fn read<D: BlockDevice>(
        &self,
        dev: &D,
        seg_start_lba: u64,
        ptr: &ValuePtr,
    ) -> Result<Record, LogError> {
        self.drain_inflight(dev)?;
        let (first_block, nblocks) = self.span_blocks(ptr);
        if self.tail_overlay_off(ptr, first_block, nblocks).is_some() {
            let mut buf = io_buf(nblocks * self.block_size);
            dev.read_at(seg_start_lba + first_block, &mut buf)?;
            return decode_record_value(self.record_bytes(&mut buf, ptr));
        }
        read_record(dev, self.block_size, seg_start_lba, ptr)
    }

    pub fn read_checked<D: BlockDevice>(
        &self,
        dev: &D,
        seg_start_lba: u64,
        ptr: &ValuePtr,
        expected_key: &[u8],
        expected_version: u128,
    ) -> Result<Record, LogError> {
        self.drain_inflight(dev)?;
        let (first_block, nblocks) = self.span_blocks(ptr);
        let mut buf = io_buf(nblocks * self.block_size);
        dev.read_at(seg_start_lba + first_block, &mut buf)?;
        decode_record_value_checked(
            self.record_bytes(&mut buf, ptr),
            expected_key,
            expected_version,
        )
    }

    pub fn read_many_checked<D: BlockDevice>(
        &self,
        dev: &D,
        items: &[(u64, ValuePtr)],
        expected: &[(&[u8], u128)],
    ) -> Result<Vec<Record>, LogError> {
        self.drain_inflight(dev)?;
        let mut bufs = self.read_bufs(items);
        {
            let mut ops: Vec<ReadOp<'_>> = bufs
                .iter_mut()
                .zip(items)
                .map(|(buf, (seg_start, ptr))| ReadOp {
                    lba: seg_start + self.span_blocks(ptr).0,
                    buf,
                })
                .collect();
            dev.read_batch(&mut ops)?;
        }
        self.decode_records_checked(&mut bufs, items, expected)
    }

    pub async fn read_many_async_checked<D: AsyncRead + BlockDevice>(
        &self,
        dev: &D,
        items: &[(u64, ValuePtr)],
        expected: &[(&[u8], u128)],
    ) -> Result<Vec<Record>, LogError> {
        self.drain_inflight(dev)?;
        let mut bufs = self.read_bufs(items);

        let acks = {
            let futs: Vec<_> = bufs
                .iter_mut()
                .zip(items)
                .map(|(buf, (seg_start, ptr))| {
                    let lba = seg_start + self.span_blocks(ptr).0;
                    dev.read_async(lba, buf.as_mut_slice())
                })
                .collect();
            vals_rt::join_all(futs).await
        };
        for ack in acks {
            ack?;
        }
        self.decode_records_checked(&mut bufs, items, expected)
    }

    pub fn plan_read<D: BlockDevice>(
        &self,
        dev: &D,
        seg_start_lba: u64,
        ptr: &ValuePtr,
    ) -> Result<ReadPlan, LogError> {
        self.drain_inflight(dev)?;
        let (first_block, nblocks) = self.span_blocks(ptr);
        let tail_overlay = self
            .tail_overlay_off(ptr, first_block, nblocks)
            .map(|off| (off, self.tail.to_vec()));
        Ok(ReadPlan {
            lba: seg_start_lba + first_block,
            nblocks,
            block_size: self.block_size,
            within: ptr.offset as usize % self.block_size,
            ptr: *ptr,
            tail_overlay,
        })
    }

    pub fn plan_read_spans<D: BlockDevice>(
        &self,
        dev: &D,
        items: &[(u64, ValuePtr)],
    ) -> Result<SpanReadPlan, LogError> {
        self.drain_inflight(dev)?;
        let bs = self.block_size;
        let mut plan = plan_read_spans_detached(bs, items);

        if self.tail_dirty && self.cur_seg.is_some() {
            let tail_lba = self.cur_seg_start + self.write_off as u64 / bs as u64;
            for span in &mut plan.spans {
                if tail_lba >= span.lba && tail_lba < span.lba + span.nblocks as u64 {
                    let off = (tail_lba - span.lba) as usize * bs;
                    span.tail_overlay = Some((off, self.tail.to_vec()));
                }
            }
        }

        Ok(plan)
    }

    pub fn sync<D: BlockDevice>(&mut self, dev: &D) -> Result<(), LogError> {
        self.drain_inflight(dev)?;
        if self.tail_dirty {
            self.flush_tail(dev)?;
        }
        dev.flush()?;
        Ok(())
    }

    pub fn drain_and_flush_tail<D: BlockDevice>(&mut self, dev: &D) -> Result<(), LogError> {
        let started = std::time::Instant::now();
        let drained = self.drain_inflight(dev);
        VLOG_SYNC_DRAIN_US.fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        VLOG_SYNC_DRAIN_COUNT.fetch_add(1, Ordering::Relaxed);
        drained?;
        if self.tail_dirty {
            let started = std::time::Instant::now();
            let flushed = self.flush_tail(dev);
            VLOG_SYNC_TAIL_US.fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
            VLOG_SYNC_TAIL_COUNT.fetch_add(1, Ordering::Relaxed);
            flushed?;
        }
        Ok(())
    }

    fn roll<D: BlockDevice>(&mut self, dev: &D, alloc: &mut Allocator) -> Result<(), LogError> {
        if self.tail_dirty {
            self.flush_tail(dev)?;
        }
        let seg = alloc.allocate_data().ok_or_else(|| {
            if alloc.maint_reserve() > 0 {
                LogError::Backpressure
            } else {
                LogError::DeviceFull
            }
        })?;
        let (start, _) = alloc.segment_range(seg);
        self.cur_seg = Some(seg);
        self.cur_seg_start = start;
        self.write_off = 0;
        self.tail.fill(0);
        self.tail_dirty = false;

        let epoch = self.next_epoch;
        self.next_epoch += 1;
        self.cur_epoch = epoch;
        self.tail_segs.push((epoch, seg));
        let hdr = encode_seg_header(epoch, self.block_size as u32);
        self.append_bytes(dev, &hdr)?;
        Ok(())
    }

    fn append_bytes<D: BlockDevice>(&mut self, dev: &D, data: &[u8]) -> Result<(), LogError> {
        let bs = self.block_size;
        let mut rest = data;

        let off = self.write_off as usize % bs;
        if off != 0 && !rest.is_empty() {
            let space = bs - off;
            let n = space.min(rest.len());
            self.tail[off..off + n].copy_from_slice(&rest[..n]);
            self.write_off += n as u32;
            self.tail_dirty = true;
            rest = &rest[n..];
            if off + n == bs {
                let block_idx = self.write_off as u64 / bs as u64 - 1;
                self.submit_block(dev, self.cur_seg_start + block_idx, &self.tail)?;
                self.tail.fill(0);
                self.tail_dirty = false;
            }
        }

        let full = rest.len() / bs;
        if full > 0 {
            debug_assert_eq!(self.write_off as usize % bs, 0);
            let nbytes = full * bs;
            let block_idx = self.write_off as u64 / bs as u64;
            self.submit_block(dev, self.cur_seg_start + block_idx, &rest[..nbytes])?;
            self.write_off += nbytes as u32;
            rest = &rest[nbytes..];
        }

        if !rest.is_empty() {
            debug_assert_eq!(self.write_off as usize % bs, 0);
            self.tail.fill(0);
            self.tail[..rest.len()].copy_from_slice(rest);
            self.write_off += rest.len() as u32;
            self.tail_dirty = true;
        }
        Ok(())
    }

    fn flush_tail<D: BlockDevice>(&mut self, dev: &D) -> Result<(), LogError> {
        let block_idx = self.write_off as u64 / self.block_size as u64;
        crate::attribution::write_bytes(1, self.tail.len());
        dev.write_at(self.cur_seg_start + block_idx, &self.tail)?;
        VLOG_TAIL_WRITE_BYTES.fetch_add(self.tail.len() as u64, Ordering::Relaxed);
        self.tail_dirty = false;
        Ok(())
    }
}

pub fn replay_segment(buf: &[u8]) -> Result<(SegHeader, Vec<(u32, Record)>), LogError> {
    let hdr = decode_seg_header(buf)?;
    let mut out = Vec::new();
    let mut off = SEG_HEADER_LEN;
    while off < buf.len() {
        if let Some(total) = record_extent(buf, off) {
            match decode_record(&buf[off..off + total]) {
                Ok(rec) if rec.epoch == hdr.epoch => {
                    out.push((off as u32, rec));
                    off += total;
                    continue;
                }
                Err(LogError::Corrupt(BAD_KIND)) if intact_record_of_epoch(&buf[off..off + total], hdr.epoch) => {
                    return Err(LogError::Unsupported("record kind"));
                }
                _ => {}
            }
        }
        let Some(next) = next_intact_record(buf, off + 1, hdr.epoch) else {
            break;
        };
        note_replay_skip(hdr.epoch, off, next);
        off = next;
    }
    Ok((hdr, out))
}

fn record_extent(buf: &[u8], off: usize) -> Option<usize> {
    if off + HEADER_LEN > buf.len() || buf[off..off + 4] != REC_MAGIC.to_le_bytes() {
        return None;
    }
    let key_len = u32::from_le_bytes(buf[off + 6..off + 10].try_into().unwrap()) as usize;
    let val_len = u32::from_le_bytes(buf[off + 10..off + 14].try_into().unwrap()) as usize;
    let total = HEADER_LEN + key_len + val_len;
    (off + total <= buf.len()).then_some(total)
}

fn next_intact_record(buf: &[u8], from: usize, epoch: u64) -> Option<usize> {
    let first = REC_MAGIC.to_le_bytes()[0];
    (from..buf.len().saturating_sub(HEADER_LEN - 1)).find(|&p| {
        buf[p] == first
            && record_extent(buf, p).is_some_and(|total| intact_record_of_epoch(&buf[p..p + total], epoch))
    })
}

fn note_replay_skip(epoch: u64, from: usize, to: usize) {
    VLOG_REPLAY_SKIPS.fetch_add(1, Ordering::Relaxed);
    VLOG_REPLAY_SKIPPED_BYTES.fetch_add((to - from) as u64, Ordering::Relaxed);
    log::warn!(
        "value-log segment epoch {epoch}: bytes {from}..{to} hold no valid record but intact records of the \
         same segment follow; skipping them (mid-segment corruption, or blocks a crash left unwritten after \
         the last sync)"
    );
}

fn intact_record_of_epoch(b: &[u8], epoch: u64) -> bool {
    let stored_crc = u32::from_le_bytes(b[CRC_OFF..KEY_OFF].try_into().unwrap());
    let mut c = Crc32::new();
    c.update(&b[0..CRC_OFF]);
    c.update(&b[KEY_OFF..]);
    c.finalize() == stored_crc && u64::from_le_bytes(b[22..30].try_into().unwrap()) == epoch
}

pub fn read_record<D: BlockDevice>(
    dev: &D,
    block_size: usize,
    seg_start_lba: u64,
    ptr: &ValuePtr,
) -> Result<Record, LogError> {
    let bs = block_size as u64;
    let total = ptr.len as u64;
    let first_block = ptr.offset as u64 / bs;
    let last_block = (ptr.offset as u64 + total - 1) / bs;
    let nblocks = (last_block - first_block + 1) as usize;
    let mut buf = vec![0u8; nblocks * block_size];
    dev.read_at(seg_start_lba + first_block, &mut buf)?;
    let within = ptr.offset as usize % block_size;
    decode_record_value(&buf[within..within + total as usize])
}

#[cfg(test)]
mod tests {
    use super::*;
    use vals_spdk::MemBlockDevice;

    const BS: usize = 256;
    const SEG_BLOCKS: u64 = 4;

    fn setup() -> (MemBlockDevice, Allocator, ValueLog) {
        let dev = MemBlockDevice::new(BS, 64);
        let alloc = Allocator::new(64, SEG_BLOCKS, 0);
        let log = ValueLog::new(BS, SEG_BLOCKS, 0, 0);
        (dev, alloc, log)
    }

    fn read_back(dev: &MemBlockDevice, alloc: &Allocator, ptr: &ValuePtr) -> Record {
        let (start, _) = alloc.segment_range(ptr.segment);
        read_record(dev, BS, start, ptr).unwrap()
    }

    #[test]
    fn inflight_write_measurement_excludes_completed_unreaped_handles() {
        let (_, _, log) = setup();
        let first = vals_spdk::WriteState::pending();
        let second = vals_spdk::WriteState::pending();
        log.inflight.borrow_mut().extend([
            WriteHandle::pending(first.clone()), WriteHandle::pending(second.clone()),
        ]);
        assert_eq!(log.inflight_writes(), 2);
        first.complete(true);
        assert_eq!(log.inflight_writes(), 1);
        assert_eq!(log.inflight.borrow().len(), 2);
        second.complete(false);
        assert_eq!(log.inflight_writes(), 0);
        assert!(log.reap().is_err());
        assert!(log.inflight.borrow().is_empty());
    }

    #[test]
    fn append_and_read_back() {
        let (dev, mut alloc, mut log) = setup();
        let (seq, ptr) = log
            .append(&dev, &mut alloc, RecordKind::Put, 0, b"alpha", b"one")
            .unwrap();
        log.sync(&dev).unwrap();
        assert_eq!(seq, 0);
        let r = read_back(&dev, &alloc, &ptr);
        assert_eq!(r.kind, RecordKind::Put);
        assert!(r.key.is_empty());
        assert_eq!(r.value, b"one");
        assert_eq!(r.seq, 0);
        assert_eq!(ptr.offset as usize, SEG_HEADER_LEN);
    }

    #[test]
    fn many_records_distinct_seqs() {
        let (dev, mut alloc, mut log) = setup();
        let mut ptrs = Vec::new();
        for i in 0..20u32 {
            let key = format!("k{i}");
            let val = format!("value-number-{i}");
            let (seq, ptr) = log
                .append(
                    &dev,
                    &mut alloc,
                    RecordKind::Put,
                    0,
                    key.as_bytes(),
                    val.as_bytes(),
                )
                .unwrap();
            assert_eq!(seq, i as u64);
            ptrs.push((key, val, ptr));
        }
        log.sync(&dev).unwrap();
        for (_key, val, ptr) in &ptrs {
            let r = read_back(&dev, &alloc, ptr);
            assert_eq!(r.value, val.as_bytes());
        }
    }

    #[test]
    fn record_spanning_blocks() {
        let (dev, mut alloc, mut log) = setup();
        let big = vec![0xABu8; BS + 50];
        let (_, ptr) = log
            .append(&dev, &mut alloc, RecordKind::Put, 0, b"big", &big)
            .unwrap();
        log.sync(&dev).unwrap();
        let r = read_back(&dev, &alloc, &ptr);
        assert_eq!(r.value, big);
    }

    #[test]
    fn tail_seg_tracker_roll_filter_prune_forget_seed() {
        let (dev, mut alloc, mut log) = setup();
        let val = vec![7u8; 600];
        let mut rolled: Vec<SegmentId> = Vec::new();
        for _ in 0..10 {
            let (_, ptr) = log
                .append(&dev, &mut alloc, RecordKind::Put, 0, b"k", &val)
                .unwrap();
            if rolled.last() != Some(&ptr.segment) {
                rolled.push(ptr.segment);
            }
        }
        assert!(rolled.len() > 1, "test premise: rolled multiple segments");

        let all = log.tail_segs_through(u64::MAX);
        assert_eq!(all, rolled, "tracker holds every rolled segment in order");

        let through_2 = log.tail_segs_through(2);
        assert_eq!(through_2, rolled[..3].to_vec());

        log.prune_tail_segs_through(2);
        assert_eq!(log.tail_segs_through(u64::MAX), rolled[3..].to_vec());

        let victim = rolled[5];
        log.forget_tail_seg(victim);
        assert!(!log.tail_segs_through(u64::MAX).contains(&victim));

        log.seed_tail_segs([(100u64, SegmentId(42))]);
        assert!(log
            .tail_segs_through(u64::MAX)
            .contains(&SegmentId(42)));
        assert!(log.tail_segs_through(50).iter().all(|s| *s != SegmentId(42)));

        let taken = log.take_tail_segs();
        assert!(!taken.is_empty());
        assert!(log.tail_segs_through(u64::MAX).is_empty());
    }

    #[test]
    fn rolls_to_new_segment_when_full() {
        let (dev, mut alloc, mut log) = setup();
        let val = vec![7u8; 200];
        let mut segs = std::collections::HashSet::new();
        for _ in 0..12 {
            let (_, ptr) = log
                .append(&dev, &mut alloc, RecordKind::Put, 0, b"key", &val)
                .unwrap();
            segs.insert(ptr.segment);
        }
        log.sync(&dev).unwrap();
        assert!(
            segs.len() > 1,
            "should have rolled across multiple segments"
        );
        assert_eq!(alloc.used_segments() as usize, segs.len());
    }

    #[test]
    fn detects_corruption() {
        let (dev, mut alloc, mut log) = setup();
        let (_, ptr) = log
            .append(&dev, &mut alloc, RecordKind::Put, 0, b"k", b"vvvv")
            .unwrap();
        log.sync(&dev).unwrap();
        let (start, _) = alloc.segment_range(ptr.segment);
        let mut blk = vec![0u8; BS];
        dev.read_at(start, &mut blk).unwrap();
        let corrupt_at = ptr.offset as usize + KEY_OFF + 1;
        blk[corrupt_at] ^= 0xFF;
        dev.write_at(start, &blk).unwrap();
        assert!(matches!(
            read_record(&dev, BS, start, &ptr),
            Err(LogError::Corrupt(_))
        ));
    }

    #[test]
    fn rejects_oversized_record() {
        let (dev, mut alloc, mut log) = setup();
        let huge = vec![0u8; 1024];
        assert!(matches!(
            log.append(&dev, &mut alloc, RecordKind::Put, 0, b"k", &huge),
            Err(LogError::RecordTooLarge { .. })
        ));
    }

    fn read_segment(dev: &MemBlockDevice, alloc: &Allocator, seg: SegmentId) -> Vec<u8> {
        let (start, blocks) = alloc.segment_range(seg);
        let mut buf = vec![0u8; blocks as usize * BS];
        dev.read_at(start, &mut buf).unwrap();
        buf
    }

    #[test]
    fn replay_segment_recovers_records() {
        let (dev, mut alloc, mut log) = setup();
        let inputs: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"bb", b"22"), (b"ccc", b"333")];
        for (k, v) in inputs {
            log.append(&dev, &mut alloc, RecordKind::Put, 0, k, v)
                .unwrap();
        }
        log.sync(&dev).unwrap();

        let buf = read_segment(&dev, &alloc, SegmentId(0));
        let (hdr, recs) = replay_segment(&buf).unwrap();
        assert_eq!(hdr.epoch, 0);
        assert_eq!(recs.len(), 3);
        for (i, (off, rec)) in recs.iter().enumerate() {
            assert_eq!(rec.key, inputs[i].0);
            assert_eq!(rec.value, inputs[i].1);
            assert_eq!(rec.seq, i as u64);
            if i == 0 {
                assert_eq!(*off as usize, SEG_HEADER_LEN);
            }
        }
    }

    #[test]
    fn replay_stops_at_torn_tail() {
        let (dev, mut alloc, mut log) = setup();
        for i in 0..3u32 {
            log.append(
                &dev,
                &mut alloc,
                RecordKind::Put,
                0,
                format!("k{i}").as_bytes(),
                b"v",
            )
            .unwrap();
        }
        log.sync(&dev).unwrap();

        let mut buf = read_segment(&dev, &alloc, SegmentId(0));
        let (_, recs) = replay_segment(&buf).unwrap();
        let last_off = recs[2].0 as usize;
        buf[last_off + KEY_OFF] ^= 0xFF;
        let (_, torn) = replay_segment(&buf).unwrap();
        assert_eq!(torn.iter().map(|(_, r)| r.key.clone()).collect::<Vec<_>>(), [b"k0".to_vec(), b"k1".to_vec()]);
        buf[last_off..].fill(0);
        let (_, unwritten) = replay_segment(&buf).unwrap();
        assert_eq!(unwritten.len(), 2);
    }

    #[test]
    fn replay_skips_mid_segment_corruption_and_recovers_later_records() {
        let (dev, mut alloc, mut log) = setup();
        for i in 0..4u32 {
            log.append(&dev, &mut alloc, RecordKind::Put, 0, format!("k{i}").as_bytes(), b"v")
                .unwrap();
        }
        log.sync(&dev).unwrap();

        let mut buf = read_segment(&dev, &alloc, SegmentId(0));
        let (_, recs) = replay_segment(&buf).unwrap();
        let keys = |rs: &[(u32, Record)]| rs.iter().map(|(o, r)| (*o, r.key.clone())).collect::<Vec<_>>();
        buf[recs[0].0 as usize + KEY_OFF] ^= 0x01;
        let (_, after_bit_flip) = replay_segment(&buf).unwrap();
        assert_eq!(keys(&after_bit_flip), keys(&recs[1..]));

        buf[recs[1].0 as usize + 6] ^= 0x40;
        let (_, after_bad_length) = replay_segment(&buf).unwrap();
        assert_eq!(keys(&after_bad_length), keys(&recs[2..]));
    }

    #[test]
    fn replay_rejects_prior_incarnation_records_on_reused_segment() {
        let dev = MemBlockDevice::new(BS, 64);
        let boundary_fill = vec![1u8; BS - SEG_HEADER_LEN - HEADER_LEN - 4];
        {
            let mut alloc = Allocator::new(64, SEG_BLOCKS, 0);
            let mut log1 = ValueLog::new(BS, SEG_BLOCKS, 0, 0);
            log1.append(
                &dev,
                &mut alloc,
                RecordKind::Put,
                0,
                b"gen1",
                &boundary_fill,
            )
            .unwrap();
            log1.append(&dev, &mut alloc, RecordKind::Put, 0, b"stale", b"old-data")
                .unwrap();
            log1.sync(&dev).unwrap();
        }
        let mut alloc = Allocator::new(64, SEG_BLOCKS, 0);
        let mut log2 = ValueLog::new(BS, SEG_BLOCKS, 100, 1);
        let (_, ptr) = log2
            .append(
                &dev,
                &mut alloc,
                RecordKind::Put,
                0,
                b"gen2",
                &boundary_fill,
            )
            .unwrap();
        assert_eq!(ptr.segment, SegmentId(0), "test premise: segment reused");
        assert_eq!(
            (ptr.offset + ptr.len) as usize % BS,
            0,
            "test premise: new data ends exactly at a block boundary"
        );
        log2.sync(&dev).unwrap();

        let buf = read_segment(&dev, &alloc, SegmentId(0));
        let stale = decode_record(&buf[256..256 + HEADER_LEN + 5 + 8]).unwrap();
        assert_eq!(stale.key, b"stale");
        assert_eq!(stale.epoch, 0);
        let (hdr, recs) = replay_segment(&buf).unwrap();
        assert_eq!(hdr.epoch, 1);
        assert_eq!(
            recs.len(),
            1,
            "stale prior-generation record must not replay"
        );
        assert_eq!(recs[0].1.key, b"gen2");
        assert_eq!(recs[0].1.epoch, 1);
    }

    #[test]
    #[should_panic(expected = "u32 segment-offset space")]
    fn rejects_segment_exceeding_u32_offset_space() {
        let _ = ValueLog::new(4096, u32::MAX as u64 / 4096 + 1, 0, 0);
    }

    #[test]
    fn seg_header_roundtrips() {
        let (dev, mut alloc, mut log) = setup();
        log.append(&dev, &mut alloc, RecordKind::Put, 0, b"k", b"v")
            .unwrap();
        log.sync(&dev).unwrap();
        let (start, _) = alloc.segment_range(SegmentId(0));
        let mut blk = vec![0u8; BS];
        dev.read_at(start, &mut blk).unwrap();
        let hdr = decode_seg_header(&blk).unwrap();
        assert_eq!(hdr.epoch, 0);
        assert_eq!(hdr.block_size, BS as u32);
    }

    #[test]
    fn seg_header_unknown_version_is_rejected_loud() {
        let mut b = encode_seg_header(7, BS as u32);
        b[4..6].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
        let crc = {
            let mut c = Crc32::new();
            c.update(&b[0..18]);
            c.finalize()
        };
        b[18..22].copy_from_slice(&crc.to_le_bytes());
        assert!(
            matches!(
                decode_seg_header(&b),
                Err(LogError::Unsupported("segment format version"))
            ),
            "an intact header with an unknown version must be rejected"
        );
    }
}

use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::crc::Crc32;
use vals_spdk::{AsyncRead, BlockDevice, BlockError, ReadOp, Scratch, WriteHandle};

use crate::alloc::{Allocator, SegmentId};

const MAX_INFLIGHT_WRITES: usize = 64;

/// Full value-log blocks are coalesced into contiguous writes of up to this
/// many bytes (one SPDK pooled DMA slot) instead of one 4 KiB write per block:
/// a drive in steady state serves 16 KiB writes ~3x faster than 4 KiB ones at
/// the same queue depth (task 0544).
const COALESCE_BYTES: usize = 16 * 1024;

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
pub(crate) fn io_buf(len: usize) -> Vec<u8> {
    vec![0u8; len]
}

const REC_MAGIC: u32 = 0x324C_4156;
const SEG_MAGIC: u32 = 0x3147_5356;

/// The original value-log format: a 22-byte segment header and 50-byte
/// record headers that carry the full epoch, seq and version.
pub const SEG_FORMAT_V1: u16 = 1;
/// The compact value-log format (0565): the segment header carries the seq
/// and version of the segment's first record, and records encode theirs
/// relative to it (formats.md §2.1).
pub const SEG_FORMAT_V2: u16 = 2;
pub const FORMAT_VERSION: u16 = SEG_FORMAT_V2;

/// The format-1 record header length.
pub const HEADER_LEN: usize = 50;
const VERSION_OFF: usize = 30;
const CRC_OFF: usize = 46;
const KEY_OFF: usize = 50;

/// The shortest record either format can hold.
pub const MIN_RECORD_LEN: usize = 13;

const SEG_HEADER_LEN_V1: usize = 22;
/// The header length of the segments this build writes by default.
pub const SEG_HEADER_LEN: usize = 50;

const REC_V2: u8 = 0x80;
const REC_V2_WRITER_SAME: u8 = 0x40;
const REC_V2_TOMBSTONE: u8 = 0x20;
const REC_V2_KIND: u8 = 0x1F;

pub const FLAG_TOMBSTONE: u8 = 0x01;

pub fn parse_vlog_write_version(raw: Option<&str>) -> u16 {
    match raw.map(str::trim) {
        None | Some("2") => SEG_FORMAT_V2,
        Some("1") => SEG_FORMAT_V1,
        Some(other) => {
            log::error!("VALS_VLOG_WRITE_VERSION={other:?} is not 1 or 2; writing value-log format 1");
            SEG_FORMAT_V1
        }
    }
}

#[cfg(any(test, feature = "test-fixtures"))]
thread_local! {
    static VLOG_WRITE_VERSION_OVERRIDE: std::cell::Cell<Option<u16>> = const { std::cell::Cell::new(None) };
}

#[cfg(any(test, feature = "test-fixtures"))]
pub fn with_vlog_write_version<R>(version: u16, f: impl FnOnce() -> R) -> R {
    struct Restore(Option<u16>);
    impl Drop for Restore {
        fn drop(&mut self) {
            VLOG_WRITE_VERSION_OVERRIDE.with(|c| c.set(self.0));
        }
    }
    let _restore = Restore(VLOG_WRITE_VERSION_OVERRIDE.with(|c| c.replace(Some(version))));
    f()
}

/// The value-log format new segments are written in (`VALS_VLOG_WRITE_VERSION`,
/// read once).
pub fn vlog_write_version() -> u16 {
    #[cfg(any(test, feature = "test-fixtures"))]
    if let Some(v) = VLOG_WRITE_VERSION_OVERRIDE.with(|c| c.get()) {
        return v;
    }
    static VALUE: std::sync::OnceLock<u16> = std::sync::OnceLock::new();
    *VALUE.get_or_init(|| {
        parse_vlog_write_version(std::env::var("VALS_VLOG_WRITE_VERSION").ok().as_deref())
    })
}

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

/// A decoded value-log record. Records from replay (`replay_segment`,
/// `decode_record`) carry every field. Value reads leave `key` empty, and
/// for format-2 records they also leave `seq` and `epoch` 0: those are
/// encoded relative to the segment header, which a value read does not
/// fetch.
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

pub fn check_value_ptr(ptr: &ValuePtr, seg_bytes: u64) -> Result<(), LogError> {
    if (ptr.len as usize) < MIN_RECORD_LEN || ptr.offset as u64 + ptr.len as u64 > seg_bytes {
        return Err(LogError::Corrupt("value pointer outside its segment"));
    }
    Ok(())
}

fn record_span(ptr: &ValuePtr, block_size: usize, seg_bytes: u64) -> Result<(u64, usize), LogError> {
    check_value_ptr(ptr, seg_bytes)?;
    let bs = block_size as u64;
    let first = ptr.offset as u64 / bs;
    let last = (ptr.offset as u64 + ptr.len as u64 - 1) / bs;
    Ok((first, (last - first + 1) as usize))
}

pub(crate) fn value_ptr_start(alloc: &Allocator, block_size: usize, ptr: &ValuePtr) -> Result<u64, LogError> {
    check_value_ptr(ptr, alloc.blocks_per_segment() * block_size as u64)?;
    value_segment_start(alloc, ptr)
}

pub(crate) fn value_segment_start(alloc: &Allocator, ptr: &ValuePtr) -> Result<u64, LogError> {
    if ptr.segment.0 >= alloc.num_segments() {
        return Err(LogError::Corrupt("value pointer outside its segment"));
    }
    Ok(alloc.segment_range(ptr.segment).0)
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

fn zigzag(delta: u64) -> u64 {
    (delta << 1) ^ (((delta as i64) >> 63) as u64)
}

fn unzigzag(z: u64) -> u64 {
    (z >> 1) ^ (z & 1).wrapping_neg()
}

fn varint_len(mut v: u64) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

fn put_varint(out: &mut [u8], at: &mut usize, mut v: u64) {
    while v >= 0x80 {
        out[*at] = v as u8 | 0x80;
        *at += 1;
        v >>= 7;
    }
    out[*at] = v as u8;
    *at += 1;
}

fn get_varint(b: &[u8], at: &mut usize) -> Option<u64> {
    let mut v: u64 = 0;
    for i in 0..10 {
        let byte = *b.get(*at)?;
        *at += 1;
        if i == 9 && byte > 1 {
            return None;
        }
        v |= u64::from(byte & 0x7F) << (7 * i);
        if byte & 0x80 == 0 {
            return Some(v);
        }
    }
    None
}

fn epoch_tag(epoch: u64) -> u32 {
    let mut z = epoch.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    (z ^ (z >> 31)) as u32
}

fn version_hi(version: u128) -> u64 {
    (version >> 64) as u64
}

fn v2_crc(head: &[u8], version: u128, key: &[u8], value: &[u8]) -> u32 {
    let mut c = Crc32::new();
    c.update(head);
    c.update(&version.to_le_bytes());
    c.update(key);
    c.update(value);
    c.finalize()
}

/// What a segment's records are encoded against: its format, epoch, and the
/// seq and version of its first record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordBase {
    format: u16,
    epoch: u64,
    seq: u64,
    version: u128,
}

struct RecHeader {
    buf: [u8; HEADER_LEN],
    len: usize,
}

impl RecHeader {
    fn bytes(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

impl RecordBase {
    fn header_len(&self, seq: u64, version: u128, key_len: usize, val_len: usize) -> usize {
        if self.format == SEG_FORMAT_V1 {
            return HEADER_LEN;
        }
        let writer = if version as u64 == self.version as u64 { 0 } else { 8 };
        1 + varint_len(key_len as u64)
            + varint_len(val_len as u64)
            + varint_len(zigzag(seq.wrapping_sub(self.seq)))
            + varint_len(zigzag(version_hi(version).wrapping_sub(version_hi(self.version))))
            + writer
            + 8
    }

    fn record_len(&self, seq: u64, version: u128, key: &[u8], value: &[u8]) -> usize {
        self.header_len(seq, version, key.len(), value.len()) + key.len() + value.len()
    }

    fn encode(
        &self,
        kind: RecordKind,
        tombstone: bool,
        seq: u64,
        version: u128,
        key: &[u8],
        value: &[u8],
    ) -> RecHeader {
        if self.format == SEG_FORMAT_V1 {
            let mut buf = encode_header(kind, tombstone, seq, self.epoch, version, key.len(), value.len());
            let mut c = Crc32::new();
            c.update(&buf[0..CRC_OFF]);
            c.update(key);
            c.update(value);
            buf[CRC_OFF..KEY_OFF].copy_from_slice(&c.finalize().to_le_bytes());
            return RecHeader { buf, len: HEADER_LEN };
        }
        let writer_same = version as u64 == self.version as u64;
        let mut buf = [0u8; HEADER_LEN];
        buf[0] = REC_V2
            | kind as u8
            | if writer_same { REC_V2_WRITER_SAME } else { 0 }
            | if tombstone { REC_V2_TOMBSTONE } else { 0 };
        let mut at = 1;
        put_varint(&mut buf, &mut at, key.len() as u64);
        put_varint(&mut buf, &mut at, value.len() as u64);
        put_varint(&mut buf, &mut at, zigzag(seq.wrapping_sub(self.seq)));
        put_varint(
            &mut buf,
            &mut at,
            zigzag(version_hi(version).wrapping_sub(version_hi(self.version))),
        );
        if !writer_same {
            buf[at..at + 8].copy_from_slice(&(version as u64).to_le_bytes());
            at += 8;
        }
        buf[at..at + 4].copy_from_slice(&epoch_tag(self.epoch).to_le_bytes());
        at += 4;
        let crc = v2_crc(&buf[..at], version, key, value);
        buf[at..at + 4].copy_from_slice(&crc.to_le_bytes());
        RecHeader { buf, len: at + 4 }
    }
}

struct V2Head {
    tag: u8,
    key_len: usize,
    val_len: usize,
    seq_delta: u64,
    hi_delta: u64,
    writer: Option<u64>,
    epoch_tag: u32,
    crc: u32,
    len: usize,
}

impl V2Head {
    fn parse(b: &[u8]) -> Option<V2Head> {
        let tag = *b.first()?;
        if tag & REC_V2 == 0 {
            return None;
        }
        let mut at = 1;
        let key_len = u32::try_from(get_varint(b, &mut at)?).ok()? as usize;
        let val_len = u32::try_from(get_varint(b, &mut at)?).ok()? as usize;
        let seq_delta = unzigzag(get_varint(b, &mut at)?);
        let hi_delta = unzigzag(get_varint(b, &mut at)?);
        let writer = if tag & REC_V2_WRITER_SAME != 0 {
            None
        } else {
            let w = u64::from_le_bytes(b.get(at..at + 8)?.try_into().unwrap());
            at += 8;
            Some(w)
        };
        let epoch_tag = u32::from_le_bytes(b.get(at..at + 4)?.try_into().unwrap());
        let crc = u32::from_le_bytes(b.get(at + 4..at + 8)?.try_into().unwrap());
        Some(V2Head {
            tag,
            key_len,
            val_len,
            seq_delta,
            hi_delta,
            writer,
            epoch_tag,
            crc,
            len: at + 8,
        })
    }

    fn total(&self) -> usize {
        self.len + self.key_len + self.val_len
    }

    fn version(&self, base: u128) -> u128 {
        let hi = version_hi(base).wrapping_add(self.hi_delta);
        let writer = self.writer.unwrap_or(base as u64);
        (hi as u128) << 64 | writer as u128
    }

    fn crc_matches(&self, b: &[u8], version: u128) -> bool {
        v2_crc(&b[..self.len - 4], version, &b[self.len..self.total()], &[]) == self.crc
    }

    fn tombstone(&self) -> bool {
        self.tag & REC_V2_TOMBSTONE != 0
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

fn is_v2_record(b: &[u8]) -> bool {
    b.first().is_some_and(|t| t & REC_V2 != 0)
}

/// Fully decodes the record `b`, read from a segment with header `seg`.
pub fn decode_record(b: &[u8], seg: &SegHeader) -> Result<Record, LogError> {
    if is_v2_record(b) {
        let h = V2Head::parse(b).ok_or(LogError::Corrupt("short header"))?;
        if b.len() != h.total() {
            return Err(LogError::Corrupt("length mismatch"));
        }
        let version = h.version(seg.base_version);
        if !h.crc_matches(b, version) {
            return Err(LogError::Corrupt("crc mismatch"));
        }
        if h.epoch_tag != epoch_tag(seg.epoch) {
            return Err(LogError::Corrupt(EPOCH_MISMATCH));
        }
        let kind = RecordKind::from_u8(h.tag & REC_V2_KIND).ok_or(LogError::Corrupt(BAD_KIND))?;
        return Ok(Record {
            seq: seg.base_seq.wrapping_add(h.seq_delta),
            epoch: seg.epoch,
            kind,
            tombstone: h.tombstone(),
            key: b[h.len..h.len + h.key_len].to_vec(),
            value: b[h.len + h.key_len..].to_vec(),
            version,
        });
    }
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
const EPOCH_MISMATCH: &str = "record epoch mismatch";
const KEY_MISMATCH: &str = "record key mismatch";
const VERSION_MISMATCH: &str = "record version mismatch";
const CRC_OR_VERSION_MISMATCH: &str = "record crc or version mismatch";

pub const CORRUPT_READ_KINDS: [&str; 7] = ["magic", "kind", "length", "crc", "key", "version", "other"];

pub fn corrupt_read_kind(reason: &str) -> &'static str {
    match reason {
        "bad magic" => "magic",
        BAD_KIND => "kind",
        "short header" | "length mismatch" => "length",
        "crc mismatch" | CRC_OR_VERSION_MISMATCH => "crc",
        KEY_MISMATCH => "key",
        VERSION_MISMATCH => "version",
        _ => "other",
    }
}

struct ValueRecord {
    kind: RecordKind,
    tombstone: bool,
    value_off: usize,
    seq: u64,
    epoch: u64,
}

/// Validates a record read through a value pointer. A format-2 record's
/// checksum covers its full version, so it validates only against the
/// version the pointer's index entry expects.
fn validate_value(
    b: &[u8],
    expected_key: Option<&[u8]>,
    expected_version: u128,
) -> Result<ValueRecord, LogError> {
    if is_v2_record(b) {
        let h = V2Head::parse(b).ok_or(LogError::Corrupt("short header"))?;
        if b.len() != h.total() {
            return Err(LogError::Corrupt("length mismatch"));
        }
        let key_differs = expected_key.is_some_and(|k| &b[h.len..h.len + h.key_len] != k);
        if !h.crc_matches(b, expected_version) {
            return Err(LogError::Corrupt(if key_differs { KEY_MISMATCH } else { CRC_OR_VERSION_MISMATCH }));
        }
        let kind = RecordKind::from_u8(h.tag & REC_V2_KIND).ok_or(LogError::Corrupt(BAD_KIND))?;
        if key_differs {
            return Err(LogError::Corrupt(KEY_MISMATCH));
        }
        return Ok(ValueRecord {
            kind,
            tombstone: h.tombstone(),
            value_off: h.len + h.key_len,
            seq: 0,
            epoch: 0,
        });
    }
    let (kind, tombstone, key_len, seq, epoch, version) = validate_record(b)?;
    if expected_key.is_some_and(|k| &b[KEY_OFF..KEY_OFF + key_len] != k) {
        return Err(LogError::Corrupt(KEY_MISMATCH));
    }
    if version != expected_version {
        return Err(LogError::Corrupt(VERSION_MISMATCH));
    }
    Ok(ValueRecord {
        kind,
        tombstone,
        value_off: KEY_OFF + key_len,
        seq,
        epoch,
    })
}

/// Decodes a record read through a value pointer whose index entry has
/// version `expected_version` (and key `expected_key`, if given).
pub fn decode_record_value(
    b: &[u8],
    expected_key: Option<&[u8]>,
    expected_version: u128,
) -> Result<Record, LogError> {
    let rec = validate_value(b, expected_key, expected_version)?;
    Ok(Record {
        seq: rec.seq,
        epoch: rec.epoch,
        kind: rec.kind,
        tombstone: rec.tombstone,
        key: Vec::new(),
        value: b[rec.value_off..].to_vec(),
        version: expected_version,
    })
}

pub struct ReadPlan {
    lba: u64,
    nblocks: usize,
    block_size: usize,
    within: usize,
    ptr: ValuePtr,
    version: u128,
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
        let rec = decode_record_value(self.record(&buf), expected_key, self.version);
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
        // SAFETY: `buf` is owned by this future, so leaking the read leaks it too.
        let read = unsafe { dev.read_async_in_place(self.lba, buf.as_mut_slice()) }.await;
        if let Err(e) = read {
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
        self.read_value_inner(dev, None, self.version, f).await
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
        self.read_value_inner(dev, Some(expected_key), expected_version, f)
            .await
    }

    async fn read_value_inner<D, R>(
        &self,
        dev: &D,
        expected_key: Option<&[u8]>,
        expected_version: u128,
        f: impl FnOnce(u128, &[u8]) -> R,
    ) -> Result<R, LogError>
    where
        D: AsyncRead + BlockDevice,
    {
        let buf = self.read_buf(dev).await?;
        let out = (|| {
            let rec = self.record(&buf);
            let value = validate_value(rec, expected_key, expected_version)?;
            Ok(f(expected_version, &rec[value.value_off..]))
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

impl SpanReadPlan {
    pub fn num_spans(&self) -> usize {
        self.spans.len()
    }

    pub fn num_records(&self) -> usize {
        self.num_records
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
        let span = &self.spans[i];
        let mut buf: Scratch = dev.take_scratch(span.nblocks * self.block_size);
        // SAFETY: `buf` is owned by this future, so leaking the read leaks it too.
        let read = unsafe {
            dev.read_async_in_place(
                span.lba,
                &mut buf.as_mut_slice()[..span.nblocks * self.block_size],
            )
        }
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

    pub fn execute_sync_checked<'k, D, F>(
        &self,
        dev: &D,
        expected: &F,
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
        expected: &F,
    ) -> Result<Vec<(usize, Record)>, SpanReadError> {
        let mut out = Vec::with_capacity(span.slots.len());
        for &(slot, off, ptr) in &span.slots {
            let slot = slot as usize;
            let (key, version) = expected(slot);
            let bytes = &buf[off as usize..off as usize + ptr.len as usize];
            match decode_record_value(bytes, Some(key), version) {
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

pub fn plan_read_spans_detached(
    block_size: usize,
    seg_bytes: u64,
    items: &[(u64, ValuePtr)],
) -> Result<SpanReadPlan, LogError> {
    let bs = block_size;
    let mut recs: Vec<(u64, usize, u32)> = Vec::with_capacity(items.len());
    for (slot, (seg_start, ptr)) in items.iter().enumerate() {
        let (first, nblocks) = record_span(ptr, bs, seg_bytes)?;
        recs.push((seg_start + first, nblocks, slot as u32));
    }
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

    Ok(SpanReadPlan {
        block_size: bs,
        spans,
        num_records: items.len(),
    })
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
    fence_first_header: bool,
    format: u16,
    base: RecordBase,
    full: bool,
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
            fence_first_header: false,
            format: vlog_write_version(),
            base: RecordBase {
                format: SEG_FORMAT_V1,
                epoch: 0,
                seq: 0,
                version: 0,
            },
            full: false,
            buf: Vec::with_capacity(seg_capacity),
            err: None,
        }
    }

    pub fn fence_first_header(mut self, fence: bool) -> Self {
        self.fence_first_header = fence;
        self
    }

    pub fn usable_per_segment(&self) -> usize {
        self.seg_capacity - seg_header_len(self.format)
    }

    /// Whether the reserved segments ran out. A rewritten record can be
    /// longer than its source (a format-2 header depends on the segment it
    /// lands in), so a rewrite can need more than it reserved.
    pub fn is_full(&self) -> bool {
        self.full
    }

    pub fn reserved_segments(&self) -> usize {
        self.segs.len()
    }

    /// Appends a record, or returns `None` once the reserved segments are
    /// full (the caller keeps the record where it is).
    #[allow(clippy::too_many_arguments)]
    pub fn append(
        &mut self,
        kind: RecordKind,
        tombstone: bool,
        seq: u64,
        version: u128,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<ValuePtr>, LogError> {
        if self.err.is_some() {
            return Err(LogError::Corrupt(
                "vlog co-compaction writer already errored",
            ));
        }
        if self.full {
            return Ok(None);
        }
        let first = RecordBase { format: self.format, epoch: 0, seq, version }.record_len(seq, version, key, value);
        if first > self.usable_per_segment() {
            return Err(LogError::RecordTooLarge {
                size: first,
                max: self.usable_per_segment(),
            });
        }
        let need_roll =
            !self.open || self.buf.len() + self.base.record_len(seq, version, key, value) > self.seg_capacity;
        if need_roll && !self.roll(seq, version)? {
            self.full = true;
            return Ok(None);
        }
        let seg = self.segs[self.seg_idx];
        let offset = self.buf.len() as u32;
        let header = self.base.encode(kind, tombstone, seq, version, key, value);
        let total = header.len + key.len() + value.len();
        self.buf.extend_from_slice(header.bytes());
        self.buf.extend_from_slice(key);
        self.buf.extend_from_slice(value);
        Ok(Some(ValuePtr {
            segment: seg,
            offset,
            len: total as u32,
        }))
    }

    fn roll(&mut self, seq: u64, version: u128) -> Result<bool, LogError> {
        let next = if self.seg_idx == usize::MAX {
            0
        } else {
            self.seg_idx + 1
        };
        if next >= self.segs.len() {
            return Ok(false);
        }
        if self.open {
            self.flush_current()?;
        }
        self.seg_idx = next;
        self.buf.clear();
        self.base = RecordBase {
            format: self.format,
            epoch: self.epochs[next],
            seq,
            version,
        };
        let hdr = encode_seg_header(&self.base, self.block_size as u32, /* not_tail */ true);
        self.buf.extend_from_slice(&hdr);
        self.open = true;
        if next == 0 && self.fence_first_header {
            let mut block = vec![0u8; self.block_size];
            block[..hdr.len()].copy_from_slice(&hdr);
            crate::attribution::write_bytes(2, block.len());
            if let Err(e) = self.dev.write_at(self.seg_lbas[0], &block).and_then(|()| self.dev.flush()) {
                self.err = Some(LogError::Corrupt("vlog co-compaction write failed"));
                return Err(e.into());
            }
        }
        Ok(true)
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

pub const SEG_FLAG_NOT_TAIL: u16 = 0x8000;

fn seg_header_len(format: u16) -> usize {
    if format == SEG_FORMAT_V1 {
        SEG_HEADER_LEN_V1
    } else {
        SEG_HEADER_LEN
    }
}

fn encode_seg_header(base: &RecordBase, block_size: u32, not_tail: bool) -> Vec<u8> {
    let mut b = vec![0u8; seg_header_len(base.format)];
    b[0..4].copy_from_slice(&SEG_MAGIC.to_le_bytes());
    let version = base.format | if not_tail { SEG_FLAG_NOT_TAIL } else { 0 };
    b[4..6].copy_from_slice(&version.to_le_bytes());
    b[6..14].copy_from_slice(&base.epoch.to_le_bytes());
    b[14..18].copy_from_slice(&block_size.to_le_bytes());
    let mut c = Crc32::new();
    c.update(&b[0..18]);
    b[18..22].copy_from_slice(&c.finalize().to_le_bytes());
    if base.format != SEG_FORMAT_V1 {
        b[22..30].copy_from_slice(&base.seq.to_le_bytes());
        b[30..46].copy_from_slice(&base.version.to_le_bytes());
        let mut c = Crc32::new();
        c.update(&b[0..46]);
        b[46..50].copy_from_slice(&c.finalize().to_le_bytes());
    }
    b
}

/// A decoded segment header. `base_seq` and `base_version` are the seq and
/// version of the segment's first record, which format-2 records are
/// encoded against (0 for format 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegHeader {
    pub epoch: u64,
    pub block_size: u32,
    pub not_tail: bool,
    pub format: u16,
    pub base_seq: u64,
    pub base_version: u128,
}

impl SegHeader {
    pub fn header_len(&self) -> usize {
        seg_header_len(self.format)
    }
}

pub fn decode_seg_header(b: &[u8]) -> Result<SegHeader, LogError> {
    if b.len() < SEG_HEADER_LEN_V1 {
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
    let format = raw_version & !SEG_FLAG_NOT_TAIL;
    let (base_seq, base_version) = match format {
        SEG_FORMAT_V1 => (0, 0),
        SEG_FORMAT_V2 => {
            if b.len() < SEG_HEADER_LEN {
                return Err(LogError::Corrupt("short seg header"));
            }
            let stored_crc = u32::from_le_bytes(b[46..50].try_into().unwrap());
            let mut c = Crc32::new();
            c.update(&b[0..46]);
            if c.finalize() != stored_crc {
                return Err(LogError::Corrupt("seg crc mismatch"));
            }
            (
                u64::from_le_bytes(b[22..30].try_into().unwrap()),
                u128::from_le_bytes(b[30..46].try_into().unwrap()),
            )
        }
        _ => return Err(LogError::Unsupported("segment format version")),
    };
    Ok(SegHeader {
        epoch: u64::from_le_bytes(b[6..14].try_into().unwrap()),
        block_size: u32::from_le_bytes(b[14..18].try_into().unwrap()),
        not_tail,
        format,
        base_seq,
        base_version,
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
    cur_base: RecordBase,
    inflight: RefCell<Vec<WriteHandle>>,
    /// Full blocks appended but not yet submitted, contiguous from `staged_lba`.
    /// Submitted before any wait for in-flight writes, so readers and syncs
    /// (which drain first) never miss them.
    staged: RefCell<Vec<u8>>,
    staged_lba: std::cell::Cell<u64>,
    tail_segs: Vec<(u64, SegmentId)>,
    epoch_fence_pending: bool,
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
            cur_base: RecordBase {
                format: SEG_FORMAT_V1,
                epoch: 0,
                seq: 0,
                version: 0,
            },
            inflight: RefCell::new(Vec::new()),
            staged: RefCell::new(Vec::new()),
            staged_lba: std::cell::Cell::new(0),
            tail_segs: Vec::new(),
            epoch_fence_pending: false,
        }
    }

    pub fn require_epoch_fence(&mut self) {
        self.epoch_fence_pending = true;
    }

    pub fn carry_epoch_fence(&mut self, from: &ValueLog) {
        self.epoch_fence_pending |= from.epoch_fence_pending;
    }

    pub fn epoch_fence_pending(&self) -> bool {
        self.epoch_fence_pending
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

    /// Adds full blocks starting at `lba` to the coalescing buffer, submitting
    /// it whenever it fills or `lba` does not continue it.
    fn stage_blocks<D: BlockDevice>(
        &self,
        dev: &D,
        mut lba: u64,
        mut buf: &[u8],
    ) -> Result<(), LogError> {
        let bs = self.block_size as u64;
        let pending = self.staged.borrow().len() as u64;
        if pending > 0 && self.staged_lba.get() + pending / bs != lba {
            self.submit_staged(dev)?;
        }
        while !buf.is_empty() {
            let n = {
                let mut staged = self.staged.borrow_mut();
                if staged.is_empty() {
                    self.staged_lba.set(lba);
                }
                let n = (COALESCE_BYTES.max(self.block_size) - staged.len()).min(buf.len());
                staged.extend_from_slice(&buf[..n]);
                n
            };
            buf = &buf[n..];
            lba += n as u64 / bs;
            if self.staged.borrow().len() >= COALESCE_BYTES {
                self.submit_staged(dev)?;
            }
        }
        Ok(())
    }

    /// Submits the coalescing buffer as one write. On failure the buffer is
    /// kept, so a later drain submits it again.
    fn submit_staged<D: BlockDevice>(&self, dev: &D) -> Result<(), LogError> {
        let mut buf = std::mem::take(&mut *self.staged.borrow_mut());
        if buf.is_empty() {
            return Ok(());
        }
        let submitted = self.submit_block(dev, self.staged_lba.get(), &buf);
        if submitted.is_ok() {
            buf.clear();
        }
        *self.staged.borrow_mut() = buf;
        submitted
    }

    /// Waits for every in-flight write, then writes the coalescing buffer
    /// synchronously: on failure it is kept, so a retried drain writes it again.
    fn drain_inflight<D: BlockDevice>(&self, dev: &D) -> Result<(), LogError> {
        self.wait_inflight(dev)?;
        let mut staged = self.staged.borrow_mut();
        if !staged.is_empty() {
            crate::attribution::write_bytes(0, staged.len());
            dev.write_at(self.staged_lba.get(), &staged)?;
            staged.clear();
        }
        Ok(())
    }

    fn wait_inflight<D: BlockDevice>(&self, dev: &D) -> Result<(), LogError> {
        while !self.inflight.borrow().is_empty() {
            if dev.poll_writes() > 0 || self.head_done() {
                self.reap()?;
            }
        }
        Ok(())
    }

    pub(crate) fn quiesce<D: BlockDevice>(&self, dev: &D) {
        let _ = self.submit_staged(dev);
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

    /// The epoch a flush started now records as its recovery watermark: the
    /// open segment's, since records appended after the flush still land in
    /// it. Not `next_epoch - 1`, which may be an epoch reserved for a value
    /// rewrite above the open segment. With no open segment, every later tail
    /// segment gets an epoch of at least `next_epoch`.
    pub fn flush_watermark(&self) -> u64 {
        match self.cur_seg {
            Some(_) => self.cur_epoch,
            None => self.next_epoch.saturating_sub(1),
        }
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

    pub fn tail_segs_after(&self, epoch: u64) -> u64 {
        self.tail_segs.iter().filter(|(e, _)| *e > epoch).count() as u64
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
        let seq = self.next_seq;
        let format = vlog_write_version();
        let first = RecordBase { format, epoch: 0, seq, version }.record_len(seq, version, key, value);
        let max = self.seg_capacity() - seg_header_len(format);
        if first > max {
            return Err(LogError::RecordTooLarge { size: first, max });
        }
        let need_roll = match self.cur_seg {
            None => true,
            Some(_) => {
                self.write_off as usize + self.cur_base.record_len(seq, version, key, value) > self.seg_capacity()
            }
        };
        if need_roll {
            self.roll(dev, alloc, seq, version)?;
        }

        self.next_seq += 1;
        let header = self.cur_base.encode(kind, tombstone, seq, version, key, value);
        let total = header.len + key.len() + value.len();
        let start_off = self.write_off;
        let seg = self.cur_seg.expect("segment open after roll");
        self.append_bytes(dev, header.bytes())?;
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

    fn span_blocks(&self, ptr: &ValuePtr) -> Result<(u64, usize), LogError> {
        record_span(ptr, self.block_size, self.seg_blocks * self.block_size as u64)
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

    fn read_bufs(&self, items: &[(u64, ValuePtr)]) -> Result<(Vec<(u64, usize)>, Vec<Vec<u8>>), LogError> {
        let mut spans = Vec::with_capacity(items.len());
        for (_, ptr) in items {
            spans.push(self.span_blocks(ptr)?);
        }
        let bufs = spans.iter().map(|&(_, n)| io_buf(n * self.block_size)).collect();
        Ok((spans, bufs))
    }

    fn record_bytes<'a>(&self, buf: &'a mut [u8], ptr: &ValuePtr, (first, nblocks): (u64, usize)) -> &'a [u8] {
        if let Some(off) = self.tail_overlay_off(ptr, first, nblocks) {
            buf[off..off + self.block_size].copy_from_slice(&self.tail);
        }
        let within = ptr.offset as usize % self.block_size;
        &buf[within..within + ptr.len as usize]
    }

    fn decode_records_checked(
        &self,
        bufs: &mut [Vec<u8>],
        spans: &[(u64, usize)],
        items: &[(u64, ValuePtr)],
        expected: &[(&[u8], u128)],
    ) -> Result<Vec<Record>, LogError> {
        if expected.len() != items.len() {
            return Err(LogError::Corrupt("checked batch length mismatch"));
        }
        let mut out = Vec::with_capacity(items.len());
        for (((buf, &span), (_, ptr)), &(key, version)) in bufs.iter_mut().zip(spans).zip(items).zip(expected) {
            out.push(decode_record_value(
                self.record_bytes(buf, ptr, span),
                Some(key),
                version,
            )?);
        }
        Ok(out)
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
        let span = self.span_blocks(ptr)?;
        let mut buf = io_buf(span.1 * self.block_size);
        dev.read_at(seg_start_lba + span.0, &mut buf)?;
        decode_record_value(
            self.record_bytes(&mut buf, ptr, span),
            Some(expected_key),
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
        let (spans, mut bufs) = self.read_bufs(items)?;
        {
            let mut ops: Vec<ReadOp<'_>> = bufs
                .iter_mut()
                .zip(&spans)
                .zip(items)
                .map(|((buf, &(first, _)), (seg_start, _))| ReadOp {
                    lba: seg_start + first,
                    buf,
                })
                .collect();
            dev.read_batch(&mut ops)?;
        }
        self.decode_records_checked(&mut bufs, &spans, items, expected)
    }

    pub async fn read_many_async_checked<D: AsyncRead + BlockDevice>(
        &self,
        dev: &D,
        items: &[(u64, ValuePtr)],
        expected: &[(&[u8], u128)],
    ) -> Result<Vec<Record>, LogError> {
        self.drain_inflight(dev)?;
        let (spans, mut bufs) = self.read_bufs(items)?;

        let acks = {
            let futs: Vec<_> = bufs
                .iter_mut()
                .zip(&spans)
                .zip(items)
                .map(|((buf, &(first, _)), (seg_start, _))| {
                    // SAFETY: `bufs` is owned by this future, so leaking the reads leaks it too.
                    unsafe { dev.read_async_in_place(seg_start + first, buf.as_mut_slice()) }
                })
                .collect();
            vals_rt::join_all(futs).await
        };
        for ack in acks {
            ack?;
        }
        self.decode_records_checked(&mut bufs, &spans, items, expected)
    }

    pub fn plan_read<D: BlockDevice>(
        &self,
        dev: &D,
        seg_start_lba: u64,
        ptr: &ValuePtr,
        version: u128,
    ) -> Result<ReadPlan, LogError> {
        self.drain_inflight(dev)?;
        let (first_block, nblocks) = self.span_blocks(ptr)?;
        let tail_overlay = self
            .tail_overlay_off(ptr, first_block, nblocks)
            .map(|off| (off, self.tail.to_vec()));
        Ok(ReadPlan {
            lba: seg_start_lba + first_block,
            nblocks,
            block_size: self.block_size,
            within: ptr.offset as usize % self.block_size,
            ptr: *ptr,
            version,
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
        let mut plan = plan_read_spans_detached(bs, self.seg_blocks * bs as u64, items)?;

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
        let drained = self.wait_inflight(dev);
        VLOG_SYNC_DRAIN_US.fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        VLOG_SYNC_DRAIN_COUNT.fetch_add(1, Ordering::Relaxed);
        drained?;
        if !self.tail_dirty {
            return self.drain_inflight(dev);
        }
        let started = std::time::Instant::now();
        let flushed = self.flush_staged_and_tail(dev);
        VLOG_SYNC_TAIL_US.fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        VLOG_SYNC_TAIL_COUNT.fetch_add(1, Ordering::Relaxed);
        flushed
    }

    fn roll<D: BlockDevice>(
        &mut self,
        dev: &D,
        alloc: &mut Allocator,
        seq: u64,
        version: u128,
    ) -> Result<(), LogError> {
        if self.tail_dirty {
            self.flush_tail(dev)?;
        }
        // After the synchronous tail write, so its error still surfaces here.
        self.submit_staged(dev)?;
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
        self.cur_base = RecordBase {
            format: vlog_write_version(),
            epoch,
            seq,
            version,
        };
        self.tail_segs.push((epoch, seg));
        let hdr = encode_seg_header(&self.cur_base, self.block_size as u32, false);
        self.append_bytes(dev, &hdr)?;
        if self.epoch_fence_pending {
            self.flush_tail(dev)?;
            dev.flush()?;
            self.epoch_fence_pending = false;
        }
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
                self.stage_blocks(dev, self.cur_seg_start + block_idx, &self.tail)?;
                self.tail.fill(0);
                self.tail_dirty = false;
            }
        }

        let full = rest.len() / bs;
        if full > 0 {
            debug_assert_eq!(self.write_off as usize % bs, 0);
            let nbytes = full * bs;
            let block_idx = self.write_off as u64 / bs as u64;
            self.stage_blocks(dev, self.cur_seg_start + block_idx, &rest[..nbytes])?;
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

    /// Writes the coalescing buffer and the dirty tail block that directly
    /// follows it as one synchronous write. Both stay pending on failure.
    fn flush_staged_and_tail<D: BlockDevice>(&mut self, dev: &D) -> Result<(), LogError> {
        let bs = self.block_size;
        let tail_lba = self.cur_seg_start + self.write_off as u64 / bs as u64;
        let staged_len = self.staged.borrow().len();
        if staged_len == 0 || self.staged_lba.get() + (staged_len / bs) as u64 != tail_lba {
            self.drain_inflight(dev)?;
            return self.flush_tail(dev);
        }
        crate::attribution::write_bytes(0, staged_len);
        crate::attribution::write_bytes(1, bs);
        let written = {
            let mut staged = self.staged.borrow_mut();
            staged.extend_from_slice(&self.tail);
            let written = dev.write_at(self.staged_lba.get(), &staged);
            staged.truncate(staged_len);
            written
        };
        written?;
        self.staged.borrow_mut().clear();
        VLOG_TAIL_WRITE_BYTES.fetch_add(bs as u64, Ordering::Relaxed);
        self.tail_dirty = false;
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

/// Replays a whole segment: every intact record of the segment's
/// incarnation, as `(offset, record)`.
pub fn replay_segment(buf: &[u8]) -> Result<(SegHeader, Vec<(u32, Record)>), LogError> {
    let (hdr, recs) = replay_segment_extents(buf)?;
    Ok((hdr, recs.into_iter().map(|(off, _, rec)| (off, rec)).collect()))
}

/// Like [`replay_segment`], as `(offset, length, record)`.
pub fn replay_segment_extents(buf: &[u8]) -> Result<(SegHeader, Vec<(u32, u32, Record)>), LogError> {
    let hdr = decode_seg_header(buf)?;
    let mut out = Vec::new();
    let mut off = hdr.header_len();
    while off < buf.len() {
        if let Some(total) = record_extent(buf, off, &hdr) {
            match decode_record(&buf[off..off + total], &hdr) {
                Ok(rec) if rec.epoch == hdr.epoch => {
                    out.push((off as u32, total as u32, rec));
                    off += total;
                    continue;
                }
                Err(LogError::Corrupt(BAD_KIND)) if intact_record(&buf[off..off + total], &hdr) => {
                    return Err(LogError::Unsupported("record kind"));
                }
                _ => {}
            }
        }
        let Some(next) = next_intact_record(buf, off + 1, &hdr) else {
            break;
        };
        note_replay_skip(hdr.epoch, off, next);
        off = next;
    }
    Ok((hdr, out))
}

fn record_extent(buf: &[u8], off: usize, hdr: &SegHeader) -> Option<usize> {
    let total = if hdr.format == SEG_FORMAT_V1 {
        if off + HEADER_LEN > buf.len() || buf[off..off + 4] != REC_MAGIC.to_le_bytes() {
            return None;
        }
        let key_len = u32::from_le_bytes(buf[off + 6..off + 10].try_into().unwrap()) as usize;
        let val_len = u32::from_le_bytes(buf[off + 10..off + 14].try_into().unwrap()) as usize;
        HEADER_LEN + key_len + val_len
    } else {
        V2Head::parse(&buf[off..])?.total()
    };
    (off + total <= buf.len()).then_some(total)
}

fn next_intact_record(buf: &[u8], from: usize, hdr: &SegHeader) -> Option<usize> {
    let (min_len, starts) = if hdr.format == SEG_FORMAT_V1 {
        let first = REC_MAGIC.to_le_bytes()[0];
        (HEADER_LEN, Box::new(move |b: u8| b == first) as Box<dyn Fn(u8) -> bool>)
    } else {
        (MIN_RECORD_LEN, Box::new(|b: u8| b & REC_V2 != 0) as Box<dyn Fn(u8) -> bool>)
    };
    (from..buf.len().saturating_sub(min_len - 1)).find(|&p| {
        starts(buf[p])
            && record_extent(buf, p, hdr).is_some_and(|total| intact_record(&buf[p..p + total], hdr))
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

/// Whether `b` (one record extent) is an intact record of the segment
/// `hdr`'s incarnation, whatever its kind.
fn intact_record(b: &[u8], hdr: &SegHeader) -> bool {
    if hdr.format == SEG_FORMAT_V1 {
        let stored_crc = u32::from_le_bytes(b[CRC_OFF..KEY_OFF].try_into().unwrap());
        let mut c = Crc32::new();
        c.update(&b[0..CRC_OFF]);
        c.update(&b[KEY_OFF..]);
        return c.finalize() == stored_crc && u64::from_le_bytes(b[22..30].try_into().unwrap()) == hdr.epoch;
    }
    V2Head::parse(b).is_some_and(|h| {
        h.total() == b.len()
            && h.epoch_tag == epoch_tag(hdr.epoch)
            && h.crc_matches(b, h.version(hdr.base_version))
    })
}

pub fn read_record<D: BlockDevice>(
    dev: &D,
    block_size: usize,
    seg_bytes: u64,
    seg_start_lba: u64,
    ptr: &ValuePtr,
    version: u128,
) -> Result<Record, LogError> {
    let (first_block, nblocks) = record_span(ptr, block_size, seg_bytes)?;
    let mut buf = vec![0u8; nblocks * block_size];
    dev.read_at(seg_start_lba + first_block, &mut buf)?;
    let within = ptr.offset as usize % block_size;
    decode_record_value(&buf[within..within + ptr.len as usize], None, version)
}

/// Raw record edits for tests that forge on-disk records.
#[cfg(test)]
pub(crate) mod forge {
    use super::*;

    pub(crate) fn set_kind(rec: &mut [u8], kind: u8) {
        if is_v2_record(rec) {
            rec[0] = (rec[0] & !REC_V2_KIND) | kind;
        } else {
            rec[4] = kind;
        }
    }

    pub(crate) fn set_epoch(rec: &mut [u8], epoch: u64) {
        if is_v2_record(rec) {
            let h = V2Head::parse(rec).unwrap();
            rec[h.len - 8..h.len - 4].copy_from_slice(&epoch_tag(epoch).to_le_bytes());
        } else {
            rec[22..30].copy_from_slice(&epoch.to_le_bytes());
        }
    }

    pub(crate) fn epoch(rec: &[u8], seg: &SegHeader) -> u64 {
        if is_v2_record(rec) {
            seg.epoch
        } else {
            u64::from_le_bytes(rec[22..30].try_into().unwrap())
        }
    }

    pub(crate) fn reseal(rec: &mut [u8], seg: &SegHeader) {
        let crc = if is_v2_record(rec) {
            let h = V2Head::parse(rec).unwrap();
            let version = h.version(seg.base_version);
            v2_crc(&rec[..h.len - 4], version, &rec[h.len..], &[])
        } else {
            let mut c = Crc32::new();
            c.update(&rec[..CRC_OFF]);
            c.update(&rec[KEY_OFF..]);
            c.finalize()
        };
        let at = if is_v2_record(rec) { V2Head::parse(rec).unwrap().len - 4 } else { CRC_OFF };
        rec[at..at + 4].copy_from_slice(&crc.to_le_bytes());
    }
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
        read_record(dev, BS, SEG_BLOCKS * BS as u64, start, ptr, 0).unwrap()
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
    fn value_pointers_outside_their_segment_are_refused_before_reading() {
        let (dev, mut alloc, mut log) = setup();
        let (_, good) = log
            .append(&dev, &mut alloc, RecordKind::Put, 7, b"k", b"v")
            .unwrap();
        log.sync(&dev).unwrap();
        let start = alloc.segment_range(good.segment).0;
        let seg_bytes = SEG_BLOCKS * BS as u64;
        let at = |offset: u32, len: u32| ValuePtr { segment: good.segment, offset, len };
        let refused = |r: Result<(), LogError>| {
            matches!(r, Err(LogError::Corrupt("value pointer outside its segment")))
        };
        for ptr in [
            at(0, 0),
            at(good.offset, 0),
            at(good.offset, MIN_RECORD_LEN as u32 - 1),
            at(0, u32::MAX),
            at(u32::MAX, good.len),
            at(seg_bytes as u32 - good.len + 1, good.len),
        ] {
            assert!(refused(check_value_ptr(&ptr, seg_bytes)), "{ptr:?}");
            assert!(refused(log.read_checked(&dev, start, &ptr, b"k", 7).map(drop)), "{ptr:?}");
            assert!(refused(log.read_many_checked(&dev, &[(start, good), (start, ptr)], &[(b"k", 7), (b"k", 7)]).map(drop)), "{ptr:?}");
            let (items, expected) = ([(start, good), (start, ptr)], [(&b"k"[..], 7), (&b"k"[..], 7)]);
            let many = vals_rt::block_on(log.read_many_async_checked(&dev, &items, &expected), || {});
            assert!(refused(many.map(drop)), "{ptr:?}");
            assert!(refused(log.plan_read(&dev, start, &ptr, 7).map(drop)), "{ptr:?}");
            assert!(refused(log.plan_read_spans(&dev, &[(start, good), (start, ptr)]).map(drop)), "{ptr:?}");
            assert!(refused(plan_read_spans_detached(BS, seg_bytes, &[(start, ptr)]).map(drop)), "{ptr:?}");
            assert!(refused(read_record(&dev, BS, seg_bytes, start, &ptr, 7).map(drop)), "{ptr:?}");
            assert!(refused(value_ptr_start(&alloc, BS, &ptr).map(drop)), "{ptr:?}");
        }
        for segment in [SegmentId(alloc.num_segments()), SegmentId(u64::MAX)] {
            assert!(refused(value_ptr_start(&alloc, BS, &ValuePtr { segment, ..good }).map(drop)));
        }
        assert!(check_value_ptr(&at(seg_bytes as u32 - good.len, good.len), seg_bytes).is_ok());
        assert_eq!(value_ptr_start(&alloc, BS, &good).unwrap(), start);
        assert_eq!(log.read_checked(&dev, start, &good, b"k", 7).unwrap().value, b"v");
        assert_eq!(log.read_many_checked(&dev, &[(start, good)], &[(b"k", 7)]).unwrap()[0].value, b"v");
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
        let corrupt_at = (ptr.offset + ptr.len) as usize - 1;
        blk[corrupt_at] ^= 0xFF;
        dev.write_at(start, &blk).unwrap();
        assert!(matches!(
            read_record(&dev, BS, SEG_BLOCKS * BS as u64, start, &ptr, 0),
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

    struct SizeLogDevice {
        inner: MemBlockDevice,
        writes: std::sync::Mutex<Vec<usize>>,
    }

    impl BlockDevice for SizeLogDevice {
        fn block_size(&self) -> usize {
            self.inner.block_size()
        }
        fn num_blocks(&self) -> u64 {
            self.inner.num_blocks()
        }
        fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.inner.read_at(lba, buf)
        }
        fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
            self.writes.lock().unwrap().push(buf.len());
            self.inner.write_at(lba, buf)
        }
        fn flush(&self) -> vals_spdk::Result<()> {
            self.inner.flush()
        }
    }

    #[test]
    fn full_blocks_are_coalesced_and_reads_and_syncs_see_staged_blocks() {
        with_vlog_write_version(SEG_FORMAT_V1, coalesced_v1_records);
    }

    fn coalesced_v1_records() {
        const SEG: u64 = 128; // 32 KiB segments of 256 B blocks: one 16 KiB coalesced write fits twice
        let dev = SizeLogDevice {
            inner: MemBlockDevice::new(BS, 1024),
            writes: std::sync::Mutex::new(Vec::new()),
        };
        let mut alloc = Allocator::new(1024, SEG, 0);
        let mut log = ValueLog::new(BS, SEG, 0, 0);
        let value = [7u8; 46]; // 50 B header + 4 B key + 46 B value = 100 B records
        let mut ptrs = Vec::new();
        for i in 0..200u32 {
            let key = format!("k{i:03}");
            ptrs.push(
                log.append(&dev, &mut alloc, RecordKind::Put, 0, key.as_bytes(), &value)
                    .unwrap()
                    .1,
            );
        }
        // 200 records fill 78 full blocks: one 64-block (16 KiB) write so far, the rest staged.
        assert_eq!(*dev.writes.lock().unwrap(), [COALESCE_BYTES]);
        assert_eq!(log.staged.borrow().len(), 14 * BS);
        // A read drains first, so it sees a staged block's record.
        let (start, _) = alloc.segment_range(ptrs[190].segment);
        assert_eq!(log.read_checked(&dev, start, &ptrs[190], b"k190", 0).unwrap().value, value);
        assert_eq!(*dev.writes.lock().unwrap(), [COALESCE_BYTES, 14 * BS]);
        assert!(log.staged.borrow().is_empty());

        for i in 200..260u32 {
            log.append(
                &dev,
                &mut alloc,
                RecordKind::Put,
                0,
                format!("k{i:03}").as_bytes(),
                &value,
            )
            .unwrap();
        }
        log.sync(&dev).unwrap();
        let mut buf = vec![0u8; SEG as usize * BS];
        dev.read_at(start, &mut buf).unwrap();
        let (_, recs) = replay_segment(&buf).unwrap();
        assert_eq!(recs.len(), 260);
        assert!(recs
            .iter()
            .enumerate()
            .all(|(i, (_, r))| r.key == format!("k{i:03}").into_bytes()));
        // Coalesced writes, then one partial-tail write: far fewer than one write per block.
        assert!(
            dev.writes.lock().unwrap().len() <= 4,
            "{:?}",
            dev.writes.lock().unwrap()
        );
    }

    struct FailWritesDevice {
        inner: MemBlockDevice,
        fail: std::sync::atomic::AtomicBool,
        writes: std::sync::Mutex<Vec<usize>>,
    }

    impl BlockDevice for FailWritesDevice {
        fn block_size(&self) -> usize {
            self.inner.block_size()
        }
        fn num_blocks(&self) -> u64 {
            self.inner.num_blocks()
        }
        fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.inner.read_at(lba, buf)
        }
        fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
            if self.fail.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(BlockError::Io(std::io::Error::other(
                    "injected write failure",
                )));
            }
            self.writes.lock().unwrap().push(buf.len());
            self.inner.write_at(lba, buf)
        }
        fn flush(&self) -> vals_spdk::Result<()> {
            self.inner.flush()
        }
    }

    #[test]
    fn a_commit_writes_staged_blocks_and_tail_once_and_a_failed_commit_keeps_both() {
        with_vlog_write_version(SEG_FORMAT_V1, commit_v1_records);
    }

    fn commit_v1_records() {
        const SEG: u64 = 128;
        let dev = FailWritesDevice {
            inner: MemBlockDevice::new(BS, 1024),
            fail: std::sync::atomic::AtomicBool::new(false),
            writes: std::sync::Mutex::new(Vec::new()),
        };
        let mut alloc = Allocator::new(1024, SEG, 0);
        let mut log = ValueLog::new(BS, SEG, 0, 0);
        for i in 0..30u32 {
            log.append(
                &dev,
                &mut alloc,
                RecordKind::Put,
                0,
                format!("k{i:03}").as_bytes(),
                &[7u8; 46],
            )
            .unwrap();
        }
        // 3,022 B: 11 full staged blocks plus a partial tail, no device write yet.
        assert!(dev.writes.lock().unwrap().is_empty());
        dev.fail.store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(log.drain_and_flush_tail(&dev).is_err());
        assert_eq!(log.staged.borrow().len(), 11 * BS);
        assert!(log.tail_dirty);
        dev.fail.store(false, std::sync::atomic::Ordering::Relaxed);
        log.drain_and_flush_tail(&dev).unwrap();
        assert_eq!(
            *dev.writes.lock().unwrap(),
            [12 * BS],
            "staged blocks and tail in one write"
        );
        let (start, _) = alloc.segment_range(SegmentId(0));
        let mut buf = vec![0u8; SEG as usize * BS];
        dev.read_at(start, &mut buf).unwrap();
        assert_eq!(replay_segment(&buf).unwrap().1.len(), 30);
    }

    const FORMATS: [u16; 2] = [SEG_FORMAT_V1, SEG_FORMAT_V2];

    #[test]
    fn replay_stops_at_torn_tail() {
        for format in FORMATS {
            with_vlog_write_version(format, replay_stops_at_torn_tail_in);
        }
    }

    fn replay_stops_at_torn_tail_in() {
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
        let (_, recs) = replay_segment_extents(&buf).unwrap();
        let (last_off, last_len, _) = recs[2];
        buf[(last_off + last_len) as usize - 2] ^= 0xFF;
        let (_, torn) = replay_segment(&buf).unwrap();
        assert_eq!(torn.iter().map(|(_, r)| r.key.clone()).collect::<Vec<_>>(), [b"k0".to_vec(), b"k1".to_vec()]);
        buf[last_off as usize..].fill(0);
        let (_, unwritten) = replay_segment(&buf).unwrap();
        assert_eq!(unwritten.len(), 2);
    }

    #[test]
    fn replay_skips_mid_segment_corruption_and_recovers_later_records() {
        for format in FORMATS {
            with_vlog_write_version(format, || replay_skips_mid_segment_corruption_in(format));
        }
    }

    fn replay_skips_mid_segment_corruption_in(format: u16) {
        let (dev, mut alloc, mut log) = setup();
        for i in 0..4u32 {
            log.append(&dev, &mut alloc, RecordKind::Put, 0, format!("k{i}").as_bytes(), b"v")
                .unwrap();
        }
        log.sync(&dev).unwrap();

        let mut buf = read_segment(&dev, &alloc, SegmentId(0));
        let (_, recs) = replay_segment(&buf).unwrap();
        let (_, extents) = replay_segment_extents(&buf).unwrap();
        let keys = |rs: &[(u32, Record)]| rs.iter().map(|(o, r)| (*o, r.key.clone())).collect::<Vec<_>>();
        let (off0, len0, _) = extents[0];
        buf[(off0 + len0) as usize - 2] ^= 0x01;
        let (_, after_bit_flip) = replay_segment(&buf).unwrap();
        assert_eq!(keys(&after_bit_flip), keys(&recs[1..]));

        let key_len_at = if format == SEG_FORMAT_V1 { 6 } else { 1 };
        buf[recs[1].0 as usize + key_len_at] ^= 0x40;
        let (_, after_bad_length) = replay_segment(&buf).unwrap();
        assert_eq!(keys(&after_bad_length), keys(&recs[2..]));
    }

    #[test]
    fn replay_rejects_prior_incarnation_records_on_reused_segment() {
        for format in FORMATS {
            with_vlog_write_version(format, || prior_incarnation_in(format));
        }
    }

    fn prior_incarnation_in(format: u16) {
        let dev = MemBlockDevice::new(BS, 64);
        let (seg_header, first_header) = match format {
            SEG_FORMAT_V1 => (SEG_HEADER_LEN_V1, HEADER_LEN),
            _ => (SEG_HEADER_LEN, MIN_RECORD_LEN + 1),
        };
        let boundary_fill = vec![1u8; BS - seg_header - first_header - 4];
        let gen1 = {
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
            read_segment(&dev, &alloc, SegmentId(0))
        };
        let (_, old) = replay_segment(&gen1).unwrap();
        assert_eq!(old[1].0 as usize, BS, "test premise: the stale record starts at a block boundary");
        assert_eq!(old[1].1.key, b"stale");
        assert_eq!(old[1].1.epoch, 0);

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
        assert_eq!(buf[BS..], gen1[BS..], "test premise: the stale record is still on the device");
        let (hdr, recs) = replay_segment(&buf).unwrap();
        assert_eq!(hdr.epoch, 1);
        assert_eq!(
            recs.len(),
            1,
            "stale prior-generation record must not replay"
        );
        assert_eq!(recs[0].1.key, b"gen2");
        assert_eq!(recs[0].1.epoch, 1);
        assert_eq!(recs[0].1.seq, 100);
    }

    #[test]
    fn compact_records_round_trip_seq_and_version_deltas() {
        let (dev, mut alloc, mut log) = setup();
        let base = (1_700_000_000_000u128 << 16) << 64 | 0xABCD_EF01_2345_6789;
        let versions = [
            base,
            base + (5u128 << 64),
            base - (3u128 << 64),
            (base & !(u64::MAX as u128)) | 42,
            u128::MAX,
            0,
            base,
        ];
        let mut ptrs = Vec::new();
        for (i, &version) in versions.iter().enumerate() {
            let key = format!("key-{i}");
            let tombstone = i % 3 == 1;
            let (seq, ptr) = log
                .append_tombstone(&dev, &mut alloc, RecordKind::Put, tombstone, version, key.as_bytes(), b"value")
                .unwrap();
            ptrs.push((seq, ptr, key, version, tombstone));
        }
        log.append(&dev, &mut alloc, RecordKind::Delete, base, b"gone", &[]).unwrap();
        log.sync(&dev).unwrap();
        assert_eq!(ptrs[0].1.len as usize, MIN_RECORD_LEN + 5 + 5, "the first record encodes zero deltas");
        assert_eq!(ptrs[1].1.len as usize, MIN_RECORD_LEN + 5 + 5, "a small hlc step and the same writer");
        assert_eq!(ptrs[3].1.len as usize, MIN_RECORD_LEN + 8 + 5 + 5, "another writer is stored in full");

        let buf = read_segment(&dev, &alloc, SegmentId(0));
        let (hdr, recs) = replay_segment_extents(&buf).unwrap();
        assert_eq!(hdr.format, SEG_FORMAT_V2);
        assert_eq!((hdr.base_seq, hdr.base_version), (0, base));
        assert_eq!(recs.len(), versions.len() + 1);
        for ((off, len, rec), (seq, ptr, key, version, tombstone)) in recs.iter().zip(&ptrs) {
            assert_eq!((*off, *len), (ptr.offset, ptr.len));
            assert_eq!((rec.seq, rec.version, rec.tombstone), (*seq, *version, *tombstone));
            assert_eq!((rec.key.as_slice(), rec.value.as_slice()), (key.as_bytes(), &b"value"[..]));
            let start = alloc.segment_range(ptr.segment).0;
            let read = log.read_checked(&dev, start, ptr, key.as_bytes(), *version).unwrap();
            assert_eq!((read.value.as_slice(), read.version, read.tombstone), (&b"value"[..], *version, *tombstone));
            let wrong = log.read_checked(&dev, start, ptr, key.as_bytes(), version ^ 1).unwrap_err();
            assert!(matches!(wrong, LogError::Corrupt(CRC_OR_VERSION_MISMATCH)), "{wrong}");
            assert_eq!(corrupt_read_kind(CRC_OR_VERSION_MISMATCH), "crc");
            let wrong_key = log.read_checked(&dev, start, ptr, b"other", *version).unwrap_err();
            assert!(matches!(wrong_key, LogError::Corrupt(KEY_MISMATCH)), "{wrong_key}");
        }
        let (_, _, gone) = recs.last().unwrap();
        assert_eq!((gone.kind, gone.key.as_slice(), gone.version), (RecordKind::Delete, &b"gone"[..], base));
    }

    #[test]
    fn segments_keep_the_format_they_were_opened_with() {
        let (dev, mut alloc, mut log) = setup();
        let value = vec![7u8; 600];
        let (_, first) = with_vlog_write_version(SEG_FORMAT_V1, || {
            log.append(&dev, &mut alloc, RecordKind::Put, 1, b"old", &value).unwrap()
        });
        let (_, same_seg) = log.append(&dev, &mut alloc, RecordKind::Put, 2, b"old2", b"v").unwrap();
        assert_eq!(same_seg.segment, first.segment);
        assert_eq!(same_seg.len as usize, HEADER_LEN + 5, "the open format-1 segment keeps format 1");
        let (_, rolled) = log.append(&dev, &mut alloc, RecordKind::Put, 3, b"new", &value).unwrap();
        assert_ne!(rolled.segment, first.segment);
        log.sync(&dev).unwrap();
        let formats: Vec<u16> = [first.segment, rolled.segment]
            .iter()
            .map(|s| replay_segment(&read_segment(&dev, &alloc, *s)).unwrap().0.format)
            .collect();
        assert_eq!(formats, [SEG_FORMAT_V1, SEG_FORMAT_V2]);
        for (ptr, key, version) in [(first, &b"old"[..], 1), (same_seg, b"old2", 2), (rolled, b"new", 3)] {
            let start = alloc.segment_range(ptr.segment).0;
            assert!(log.read_checked(&dev, start, &ptr, key, version).is_ok(), "{key:?}");
        }
    }

    #[test]
    fn rewrite_writer_reports_full_instead_of_failing() {
        let dev = MemBlockDevice::new(BS, 64);
        let mut alloc = Allocator::new(64, SEG_BLOCKS, 0);
        let segs = [alloc.allocate().unwrap()];
        let lbas = [alloc.segment_range(segs[0]).0];
        let epochs = [9];
        let mut w = VlogWriter::new(&dev, &segs, &lbas, &epochs, SEG_BLOCKS as usize * BS, BS);
        let mut written = Vec::new();
        for i in 0..100u64 {
            match w.append(RecordKind::Put, false, i, i as u128, format!("k{i}").as_bytes(), &[1u8; 40]).unwrap() {
                Some(ptr) => written.push((i, ptr)),
                None => break,
            }
        }
        assert!(w.is_full(), "test premise: one segment does not hold 100 records");
        assert_eq!(w.append(RecordKind::Put, false, 0, 0, b"k", b"v").unwrap(), None);
        let used = vals_rt::block_on(w.finish(), || {}).unwrap();
        assert_eq!(used, segs);
        let buf = read_segment(&dev, &alloc, segs[0]);
        let (hdr, recs) = replay_segment(&buf).unwrap();
        assert!(hdr.not_tail);
        assert_eq!(recs.iter().map(|(_, r)| r.seq).collect::<Vec<_>>(), written.iter().map(|(i, _)| *i).collect::<Vec<_>>());
    }

    #[test]
    fn varints_and_zigzag_round_trip_at_their_limits() {
        for v in [0u64, 1, 127, 128, 16_383, 16_384, u32::MAX as u64, u64::MAX - 1, u64::MAX] {
            let mut buf = [0u8; 10];
            let mut at = 0;
            put_varint(&mut buf, &mut at, v);
            assert_eq!(at, varint_len(v));
            let mut read = 0;
            assert_eq!(get_varint(&buf, &mut read), Some(v));
            assert_eq!(read, at);
            assert_eq!(unzigzag(zigzag(v)), v);
        }
        assert_eq!(zigzag(u64::MAX), 1, "-1 encodes in one byte");
        let mut at = 0;
        assert_eq!(get_varint(&[0xFF; 10], &mut at), None, "an over-long varint is refused");
        let mut at = 0;
        assert_eq!(get_varint(&[0x80, 0x80], &mut at), None, "a truncated varint is refused");
    }

    #[test]
    fn vlog_write_version_parses_like_the_run_knob() {
        assert_eq!(parse_vlog_write_version(None), SEG_FORMAT_V2);
        assert_eq!(parse_vlog_write_version(Some("2")), SEG_FORMAT_V2);
        assert_eq!(parse_vlog_write_version(Some(" 1 ")), SEG_FORMAT_V1);
        assert_eq!(parse_vlog_write_version(Some("")), SEG_FORMAT_V1);
        assert_eq!(parse_vlog_write_version(Some("3")), SEG_FORMAT_V1);
        assert_eq!(vlog_write_version(), SEG_FORMAT_V2);
        assert_eq!(with_vlog_write_version(SEG_FORMAT_V1, vlog_write_version), SEG_FORMAT_V1);
        assert_eq!(vlog_write_version(), SEG_FORMAT_V2);
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
        let base = RecordBase { format: SEG_FORMAT_V2, epoch: 7, seq: 0, version: 0 };
        let mut b = encode_seg_header(&base, BS as u32, false);
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

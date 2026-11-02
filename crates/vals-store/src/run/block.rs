use super::*;
use std::borrow::Cow;
use std::cell::RefCell;

const SUFFIX_TOMB_BIT: u32 = 1 << 31;
const BLOCK_CODEC_ZSTD: u32 = 1;
const CODED_HEADER_LEN: usize = 8;
pub(super) const MAX_BLOCK_BODY: usize = 64 << 20;
const ZSTD_LEVEL: i32 = 1;

const ZSTD_CONTEXT_POOL_MAX: usize = 64;

#[derive(Default)]
struct ZstdContexts {
    compressor: Option<zstd::bulk::Compressor<'static>>,
    decompressor: Option<zstd::bulk::Decompressor<'static>>,
}

static ZSTD_CONTEXT_POOL: std::sync::Mutex<Vec<ZstdContexts>> = std::sync::Mutex::new(Vec::new());

struct ThreadZstdContexts(ZstdContexts);

impl Drop for ThreadZstdContexts {
    fn drop(&mut self) {
        if let Ok(mut pool) = ZSTD_CONTEXT_POOL.lock() {
            if pool.len() < ZSTD_CONTEXT_POOL_MAX {
                pool.push(std::mem::take(&mut self.0));
            }
        }
    }
}

thread_local! {
    static ZSTD_CONTEXTS: RefCell<Option<ThreadZstdContexts>> = const { RefCell::new(None) };
}

fn with_zstd<R>(f: impl FnOnce(&mut ZstdContexts) -> R) -> R {
    ZSTD_CONTEXTS.with(|z| {
        let mut z = z.borrow_mut();
        let ctx = z.get_or_insert_with(|| {
            ThreadZstdContexts(ZSTD_CONTEXT_POOL.lock().ok().and_then(|mut pool| pool.pop()).unwrap_or_default())
        });
        f(&mut ctx.0)
    })
}

fn new_compressor() -> std::io::Result<zstd::bulk::Compressor<'static>> {
    let mut c = zstd::bulk::Compressor::new(ZSTD_LEVEL)?;
    c.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(true))?;
    Ok(c)
}

/// The compressed v7 form of an encoded block body, `[u32 body_len][u32 codec][zstd frame]`
/// without the CRC, or `None` when compression does not make it shorter.
pub(super) fn compress_block_body(body: &[u8]) -> Option<Vec<u8>> {
    if body.len() > MAX_BLOCK_BODY {
        return None;
    }
    let bound = zstd::zstd_safe::compress_bound(body.len());
    let mut out = vec![0u8; CODED_HEADER_LEN + bound];
    out[..4].copy_from_slice(&(body.len() as u32).to_le_bytes());
    out[4..8].copy_from_slice(&BLOCK_CODEC_ZSTD.to_le_bytes());
    let n = with_zstd(|z| {
        if z.compressor.is_none() {
            z.compressor = new_compressor().ok();
        }
        z.compressor.as_mut()?.compress_to_buffer(body, &mut out[CODED_HEADER_LEN..]).ok()
    })?;
    if CODED_HEADER_LEN + n >= body.len() {
        return None;
    }
    out.truncate(CODED_HEADER_LEN + n);
    Some(out)
}

/// Decodes the payload of a block (its bytes before the CRC) to the encoded entry body:
/// a payload whose bytes 4..8 are zero, or that is shorter than a coded header, is already
/// the body (the first entry of a block always has a zero shared-prefix length there).
pub(super) fn decode_block_payload(payload: &[u8]) -> Result<Cow<'_, [u8]>, FrameError> {
    if payload.len() < CODED_HEADER_LEN || payload[4..8] == [0; 4] {
        return Ok(Cow::Borrowed(payload));
    }
    let body_len = u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize;
    if u32::from_le_bytes(payload[4..8].try_into().unwrap()) != BLOCK_CODEC_ZSTD {
        return Err(FrameError::Codec);
    }
    if body_len > MAX_BLOCK_BODY {
        return Err(FrameError::Decompress);
    }
    let mut body = vec![0u8; body_len];
    let n = with_zstd(|z| {
        if z.decompressor.is_none() {
            z.decompressor = zstd::bulk::Decompressor::new().ok();
        }
        z.decompressor.as_mut()?.decompress_to_buffer(&payload[CODED_HEADER_LEN..], &mut body[..]).ok()
    });
    if n != Some(body_len) {
        return Err(FrameError::Decompress);
    }
    Ok(Cow::Owned(body))
}

/// The decoded body length a stored v7 block declares (0 for a plain block), capped at
/// `MAX_BLOCK_BODY`, so a reader can reserve the memory before decompressing.
pub(super) fn coded_body_len(block: &[u8]) -> usize {
    if block.len() < CODED_HEADER_LEN + 4 || block[4..8] == [0; 4] {
        return 0;
    }
    (u32::from_le_bytes(block[..4].try_into().unwrap()) as usize).min(MAX_BLOCK_BODY)
}

/// CRC-checks an on-disk block and returns its encoded entry body, decompressing a v7 block.
#[inline]
pub(super) fn block_body(block: &[u8]) -> Result<Cow<'_, [u8]>, FrameError> {
    if block.len() < 8 {
        return Err(FrameError::Truncated);
    }
    if !block_crc_ok(block) {
        return Err(FrameError::Crc);
    }
    decode_block_payload(&block[..block.len() - 4])
}

#[inline]
pub(super) fn body_count(body: &[u8]) -> Result<u32, &'static str> {
    Ok(u32::from_le_bytes(body.get(..4).ok_or("truncated")?.try_into().unwrap()))
}

pub fn encode_block(entries: &[SnapshotEntry]) -> Vec<u8> {
    debug_assert!(
        entries.windows(2).all(|w| w[0].key <= w[1].key),
        "run block entries must be ascending by key"
    );
    let mut b = encode_block_body(entries);
    let crc = crc::crc32(&b);
    b.extend_from_slice(&crc.to_le_bytes());
    b
}

pub(super) fn encode_block_body(entries: &[SnapshotEntry]) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    let mut prev: &[u8] = &[];
    for e in entries {
        let shared = common_prefix_len(prev, &e.key);
        let suffix = &e.key[shared..];
        debug_assert!(
            (suffix.len() as u64) < SUFFIX_TOMB_BIT as u64,
            "key suffix too long to pack the tombstone flag"
        );
        let mut suffix_field = suffix.len() as u32;
        if e.tombstone {
            suffix_field |= SUFFIX_TOMB_BIT;
        }
        b.extend_from_slice(&(shared as u32).to_le_bytes());
        b.extend_from_slice(&suffix_field.to_le_bytes());
        b.extend_from_slice(suffix);
        b.extend_from_slice(&e.ptr.segment.0.to_le_bytes());
        b.extend_from_slice(&e.ptr.offset.to_le_bytes());
        b.extend_from_slice(&e.ptr.len.to_le_bytes());
        b.extend_from_slice(&e.seq.to_le_bytes());
        b.extend_from_slice(&e.version.to_le_bytes());
        prev = &e.key;
    }
    b
}

pub(super) struct Cursor<'a> {
    pub(super) b: &'a [u8],
    pub(super) off: usize,
}

impl<'a> Cursor<'a> {
    pub(super) fn take(&mut self, n: usize) -> Result<&'a [u8], &'static str> {
        let end = self.off.checked_add(n).ok_or("length overflow")?;
        let s = self.b.get(self.off..end).ok_or("truncated")?;
        self.off = end;
        Ok(s)
    }
    pub(super) fn u32(&mut self) -> Result<u32, &'static str> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub(super) fn u64(&mut self) -> Result<u64, &'static str> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub(super) fn u128(&mut self) -> Result<u128, &'static str> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }
}

pub(super) const ENTRY_FIELDS_LEN: usize = 8 + 4 + 4 + 8 + 16;
const ENTRY_VERSION_AT: usize = 8 + 4 + 4 + 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FrameError {
    Truncated,
    Crc,
    Codec,
    Decompress,
}

impl FrameError {
    pub(super) fn why(self) -> &'static str {
        match self {
            FrameError::Truncated => "truncated",
            FrameError::Crc => "crc mismatch",
            FrameError::Codec => "unknown block codec",
            FrameError::Decompress => "block decompression failed",
        }
    }

    pub(super) fn run_block_why(self) -> &'static str {
        match self {
            FrameError::Truncated => "run block truncated",
            FrameError::Crc => "run block crc mismatch",
            FrameError::Codec => "run block has an unknown codec",
            FrameError::Decompress => "run block decompression failed",
        }
    }
}

pub(super) struct EntryKey<'a> {
    pub(super) shared: usize,
    pub(super) suffix: &'a [u8],
    pub(super) tombstone: bool,
}

pub(super) struct EntryFields {
    pub(super) ptr: ValuePtr,
    pub(super) seq: u64,
    pub(super) version: u128,
}

impl EntryFields {
    #[inline]
    fn into_entry(self, key: Vec<u8>, tombstone: bool) -> SnapshotEntry {
        SnapshotEntry { key, ptr: self.ptr, seq: self.seq, version: self.version, tombstone }
    }
}

impl<'a> Cursor<'a> {
    #[inline]
    pub(super) fn entry_key(&mut self, prev_len: usize) -> Result<EntryKey<'a>, &'static str> {
        let shared = self.u32()? as usize;
        let suffix_field = self.u32()?;
        if shared > prev_len {
            return Err("shared prefix longer than previous key");
        }
        let suffix = self.take((suffix_field & !SUFFIX_TOMB_BIT) as usize)?;
        Ok(EntryKey { shared, suffix, tombstone: suffix_field & SUFFIX_TOMB_BIT != 0 })
    }

    #[inline]
    pub(super) fn entry_fields(&mut self) -> Result<EntryFields, &'static str> {
        let segment = SegmentId(self.u64()?);
        let offset = self.u32()?;
        let len = self.u32()?;
        let seq = self.u64()?;
        let version = self.u128()?;
        Ok(EntryFields { ptr: ValuePtr { segment, offset, len }, seq, version })
    }

    #[inline]
    fn skip_entry_fields(&mut self) -> Result<(), &'static str> {
        self.take(ENTRY_FIELDS_LEN).map(drop)
    }

    #[inline]
    pub(super) fn entry_version(&mut self) -> Result<u128, &'static str> {
        self.take(ENTRY_VERSION_AT)?;
        self.u128()
    }
}

pub fn decode_block(bytes: &[u8]) -> Result<Vec<SnapshotEntry>, &'static str> {
    decode_block_body(&block_body(bytes).map_err(FrameError::why)?)
}

pub(super) fn decode_block_body(body: &[u8]) -> Result<Vec<SnapshotEntry>, &'static str> {
    let mut c = Cursor { b: body, off: 0 };
    let count = c.u32()?;
    decode_entries(c, count)
}

fn decode_entries(mut c: Cursor<'_>, count: u32) -> Result<Vec<SnapshotEntry>, &'static str> {
    let mut out: Vec<SnapshotEntry> = Vec::with_capacity((count as usize).min(1 << 16));
    for _ in 0..count {
        let prev: &[u8] = out.last().map_or(&[], |e| e.key.as_slice());
        let k = c.entry_key(prev.len())?;
        let mut key = Vec::with_capacity(k.shared + k.suffix.len());
        key.extend_from_slice(&prev[..k.shared]);
        key.extend_from_slice(k.suffix);
        let entry = c.entry_fields()?.into_entry(key, k.tombstone);
        out.push(entry);
    }
    if c.off != c.b.len() {
        return Err("trailing bytes");
    }
    Ok(out)
}

pub fn block_lookup(bytes: &[u8], key: &[u8]) -> Result<Option<SnapshotEntry>, &'static str> {
    let body = block_body(bytes).map_err(FrameError::why)?;
    let count = body_count(&body)?;
    let mut c = Cursor { b: &body, off: 4 };
    let mut prev_len = 0usize;
    let mut matched = 0usize;
    for _ in 0..count {
        let k = c.entry_key(prev_len)?;
        prev_len = k.shared + k.suffix.len();
        if k.shared > matched {
            c.skip_entry_fields()?;
            continue;
        }
        let rest = &key[k.shared..];
        let lcp = common_prefix_len(k.suffix, rest);
        match k.suffix[lcp..].cmp(&rest[lcp..]) {
            std::cmp::Ordering::Less => {
                matched = k.shared + lcp;
                c.skip_entry_fields()?;
            }
            std::cmp::Ordering::Equal => {
                return Ok(Some(c.entry_fields()?.into_entry(key.to_vec(), k.tombstone)));
            }
            std::cmp::Ordering::Greater => return Ok(None),
        }
    }
    Ok(None)
}

#[derive(Debug, Default)]
pub struct BlockCursor {
    bytes: Vec<u8>,
    pos: usize,
    body_end: usize,
    left: u32,
    key: Vec<u8>,
    prev_len: usize,
}

impl BlockCursor {
    pub fn new(bytes: Vec<u8>, lo: Option<&[u8]>, exclusive: bool) -> Result<Self, &'static str> {
        let decoded = match block_body(&bytes).map_err(FrameError::why)? {
            Cow::Owned(body) => Some(body),
            Cow::Borrowed(_) => None,
        };
        let bytes = decoded.unwrap_or_else(|| {
            let mut bytes = bytes;
            bytes.truncate(bytes.len() - 4);
            bytes
        });
        let left = body_count(&bytes)?;
        let mut cursor = BlockCursor {
            body_end: bytes.len(),
            bytes,
            pos: 4,
            left,
            key: Vec::new(),
            prev_len: 0,
        };
        if let Some(lo) = lo {
            cursor.seek(lo, exclusive)?;
        }
        Ok(cursor)
    }

    fn seek(&mut self, lo: &[u8], exclusive: bool) -> Result<(), &'static str> {
        let mut matched = 0usize;
        while self.left > 0 {
            let mut c = Cursor { b: &self.bytes[..self.body_end], off: self.pos };
            let k = c.entry_key(self.prev_len)?;
            c.skip_entry_fields()?;
            let skip = if k.shared > matched {
                true
            } else {
                let rest = &lo[k.shared..];
                let lcp = common_prefix_len(k.suffix, rest);
                let ord = k.suffix[lcp..].cmp(&rest[lcp..]);
                let skip = ord.is_lt() || (exclusive && ord.is_eq());
                if skip {
                    matched = k.shared + lcp;
                }
                skip
            };
            if !skip {
                self.key.clear();
                self.key.extend_from_slice(&lo[..matched]);
                return Ok(());
            }
            self.prev_len = k.shared + k.suffix.len();
            self.pos = c.off;
            self.left -= 1;
        }
        Ok(())
    }

    pub fn next_entry(&mut self) -> Result<Option<SnapshotEntry>, &'static str> {
        if self.left == 0 {
            if self.pos != self.body_end {
                return Err("trailing bytes");
            }
            return Ok(None);
        }
        let mut c = Cursor { b: &self.bytes[..self.body_end], off: self.pos };
        let k = c.entry_key(self.prev_len)?;
        if k.shared > self.key.len() {
            return Err("shared prefix longer than previous key");
        }
        self.key.truncate(k.shared);
        self.key.extend_from_slice(k.suffix);
        let fields = c.entry_fields()?;
        self.pos = c.off;
        self.prev_len = self.key.len();
        self.left -= 1;
        Ok(Some(fields.into_entry(self.key.clone(), k.tombstone)))
    }
}

pub fn block_first_key(bytes: &[u8]) -> Result<Option<Vec<u8>>, &'static str> {
    let body = block_body(bytes).map_err(FrameError::why)?;
    if body_count(&body)? == 0 {
        return Ok(None);
    }
    Ok(Some(Cursor { b: &body, off: 4 }.entry_key(0)?.suffix.to_vec()))
}

pub(super) fn block_crc_ok(block: &[u8]) -> bool {
    block.len() >= 4
        && crc::crc32(&block[..block.len() - 4])
            == u32::from_le_bytes(block[block.len() - 4..].try_into().unwrap())
}

pub(super) fn crc_checked_body(bytes: &[u8]) -> Result<&[u8], &'static str> {
    if bytes.len() < 4 {
        return Err("truncated");
    }
    if !block_crc_ok(bytes) {
        return Err("crc mismatch");
    }
    Ok(&bytes[..bytes.len() - 4])
}

use crate::alloc::SegmentId;
use crate::crc;
use crate::snapshot::SnapshotEntry;
use crate::range_tombstone::{decode_rt_block, RtRecord};
use crate::vlog::ValuePtr;

#[cfg(any(test, feature = "test-fixtures"))]
pub mod legacy_fixture;

mod encode_check;
mod fence;
use fence::{FenceBuilder, FenceIndex};
#[cfg(any(test, feature = "test-fixtures"))]
pub use encode_check::test_seams as encode_test_seams;
pub use encode_check::{encode_integrity_failures, EncodeIntegrityError, EncodePath, RunBuilt, RunSource};
use encode_check::{EncodeCheck, Expect};
use crate::index::snapshot_entry_hash;

use crate::index::common_prefix_len;

const SUFFIX_TOMB_BIT: u32 = 1 << 31;

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

fn encode_block_body(entries: &[SnapshotEntry]) -> Vec<u8> {
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

struct Cursor<'a> {
    b: &'a [u8],
    off: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], &'static str> {
        let end = self.off.checked_add(n).ok_or("length overflow")?;
        let s = self.b.get(self.off..end).ok_or("truncated")?;
        self.off = end;
        Ok(s)
    }
    fn u32(&mut self) -> Result<u32, &'static str> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, &'static str> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn u128(&mut self) -> Result<u128, &'static str> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }
}

pub fn decode_block(bytes: &[u8]) -> Result<Vec<SnapshotEntry>, &'static str> {
    if bytes.len() < 4 {
        return Err("truncated");
    }
    let body = &bytes[..bytes.len() - 4];
    let stored = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap());
    if crc::crc32(body) != stored {
        return Err("crc mismatch");
    }
    decode_block_body(body)
}

fn decode_block_body(body: &[u8]) -> Result<Vec<SnapshotEntry>, &'static str> {
    let mut c = Cursor { b: body, off: 0 };
    let count = c.u32()? as usize;
    let mut out = Vec::with_capacity(count.min(1 << 16));
    let mut prev: Vec<u8> = Vec::new();
    for _ in 0..count {
        let shared = c.u32()? as usize;
        let suffix_field = c.u32()?;
        let tombstone = suffix_field & SUFFIX_TOMB_BIT != 0;
        let suffix_len = (suffix_field & !SUFFIX_TOMB_BIT) as usize;
        if shared > prev.len() {
            return Err("shared prefix longer than previous key");
        }
        let suffix = c.take(suffix_len)?;
        let mut key = Vec::with_capacity(shared + suffix_len);
        key.extend_from_slice(&prev[..shared]);
        key.extend_from_slice(suffix);
        let segment = SegmentId(c.u64()?);
        let offset = c.u32()?;
        let len = c.u32()?;
        let seq = c.u64()?;
        let version = c.u128()?;
        prev = key.clone();
        out.push(SnapshotEntry {
            key,
            ptr: ValuePtr {
                segment,
                offset,
                len,
            },
            seq,
            version,
            tombstone,
        });
    }
    if c.off != body.len() {
        return Err("trailing bytes");
    }
    Ok(out)
}

pub fn block_lookup(bytes: &[u8], key: &[u8]) -> Result<Option<SnapshotEntry>, &'static str> {
    if bytes.len() < 4 {
        return Err("truncated");
    }
    let body = &bytes[..bytes.len() - 4];
    let stored = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap());
    if crc::crc32(body) != stored {
        return Err("crc mismatch");
    }
    let mut c = Cursor { b: body, off: 0 };
    let count = c.u32()? as usize;
    let mut prev_len = 0usize;
    let mut matched = 0usize;
    for _ in 0..count {
        let shared = c.u32()? as usize;
        let suffix_field = c.u32()?;
        let tombstone = suffix_field & SUFFIX_TOMB_BIT != 0;
        let suffix_len = (suffix_field & !SUFFIX_TOMB_BIT) as usize;
        if shared > prev_len {
            return Err("shared prefix longer than previous key");
        }
        let suffix = c.take(suffix_len)?;
        prev_len = shared + suffix_len;
        if shared > matched {
            c.take(40)?;
            continue;
        }
        let rest = &key[shared..];
        let lcp = common_prefix_len(suffix, rest);
        let ord = suffix[lcp..].cmp(&rest[lcp..]);
        match ord {
            std::cmp::Ordering::Less => {
                matched = shared + lcp;
                c.take(40)?;
            }
            std::cmp::Ordering::Equal => {
                let segment = SegmentId(c.u64()?);
                let offset = c.u32()?;
                let len = c.u32()?;
                let seq = c.u64()?;
                let version = c.u128()?;
                return Ok(Some(SnapshotEntry {
                    key: key.to_vec(),
                    ptr: ValuePtr {
                        segment,
                        offset,
                        len,
                    },
                    seq,
                    version,
                    tombstone,
                }));
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
        if bytes.len() < 8 {
            return Err("truncated");
        }
        if !block_crc_ok(&bytes) {
            return Err("crc mismatch");
        }
        let left = u32::from_le_bytes(bytes[..4].try_into().unwrap());
        let mut cursor = BlockCursor {
            body_end: bytes.len() - 4,
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
            let start = self.pos;
            let mut c = Cursor { b: &self.bytes[..self.body_end], off: start };
            let shared = c.u32()? as usize;
            let suffix_len = (c.u32()? & !SUFFIX_TOMB_BIT) as usize;
            if shared > self.prev_len {
                return Err("shared prefix longer than previous key");
            }
            let suffix = c.take(suffix_len)?;
            c.take(40)?;
            let skip = if shared > matched {
                true
            } else {
                let rest = &lo[shared..];
                let lcp = common_prefix_len(suffix, rest);
                let ord = suffix[lcp..].cmp(&rest[lcp..]);
                let skip = ord.is_lt() || (exclusive && ord.is_eq());
                if skip {
                    matched = shared + lcp;
                }
                skip
            };
            if !skip {
                self.key.clear();
                self.key.extend_from_slice(&lo[..matched]);
                return Ok(());
            }
            self.prev_len = shared + suffix_len;
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
        let shared = c.u32()? as usize;
        let suffix_field = c.u32()?;
        let tombstone = suffix_field & SUFFIX_TOMB_BIT != 0;
        let suffix_len = (suffix_field & !SUFFIX_TOMB_BIT) as usize;
        if shared > self.prev_len || shared > self.key.len() {
            return Err("shared prefix longer than previous key");
        }
        let suffix = c.take(suffix_len)?;
        self.key.truncate(shared);
        self.key.extend_from_slice(suffix);
        let segment = SegmentId(c.u64()?);
        let offset = c.u32()?;
        let len = c.u32()?;
        let seq = c.u64()?;
        let version = c.u128()?;
        self.pos = c.off;
        self.prev_len = self.key.len();
        self.left -= 1;
        Ok(Some(SnapshotEntry {
            key: self.key.clone(),
            ptr: ValuePtr { segment, offset, len },
            seq,
            version,
            tombstone,
        }))
    }
}

pub fn block_first_key(bytes: &[u8]) -> Result<Option<Vec<u8>>, &'static str> {
    if bytes.len() < 8 {
        return Err("truncated");
    }
    let mut c = Cursor {
        b: &bytes[..bytes.len() - 4],
        off: 0,
    };
    let count = c.u32()?;
    if count == 0 {
        return Ok(None);
    }
    let shared = c.u32()? as usize;
    debug_assert_eq!(shared, 0, "first entry must store its full key");
    let suffix_len = (c.u32()? & !SUFFIX_TOMB_BIT) as usize;
    Ok(Some(c.take(suffix_len)?.to_vec()))
}

fn bloom_hash(key: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[derive(Clone, Copy, Debug)]
pub struct BloomKey {
    h1: u64,
    z: u64,
}

impl BloomKey {
    #[inline]
    pub fn new(key: &[u8]) -> BloomKey {
        let h1 = bloom_hash(key);
        BloomKey { h1, z: splitmix64(h1) }
    }

    #[inline]
    pub(crate) fn z(&self) -> u64 {
        self.z
    }
}

#[inline]
pub(crate) fn prefetch_read<T>(ptr: *const T) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: prefetch is only a non-faulting cache hint; callers derive pointers from live vectors.
    unsafe {
        std::arch::x86_64::_mm_prefetch(ptr.cast(), std::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = ptr;
}

pub(crate) fn splitmix64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bloom {
    num_bits: u64,
    k: u32,
    bits: Vec<u64>,
}

impl Bloom {
    fn estimated_fpr(&self) -> f64 {
        let ones: u64 = self.bits.iter().map(|w| w.count_ones() as u64).sum();
        (ones as f64 / self.num_bits.max(1) as f64).powi(self.k as i32)
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn with_capacity(n_upper: usize, bits_per_key: usize) -> Bloom {
        let num_bits = ((n_upper.max(1) * bits_per_key) as u64)
            .next_multiple_of(64)
            .max(64);
        let k = ((bits_per_key as f64 * std::f64::consts::LN_2).round() as u32).clamp(1, 30);
        Bloom {
            num_bits,
            k,
            bits: vec![0u64; (num_bits / 64) as usize],
        }
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn build<'a>(keys: impl Iterator<Item = &'a [u8]>, n: usize, bits_per_key: usize) -> Bloom {
        let mut bloom = Bloom::with_capacity(n, bits_per_key);
        for key in keys {
            bloom.insert(key);
        }
        bloom
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    fn positions(&self, key: &[u8]) -> impl Iterator<Item = u64> + '_ {
        self.positions_hashed(&BloomKey::new(key))
    }

    fn positions_hashed(&self, hk: &BloomKey) -> impl Iterator<Item = u64> + '_ {
        let h1 = hk.h1;
        let h2 = hk.z | 1;
        let num_bits = self.num_bits;
        (0..self.k).map(move |i| h1.wrapping_add((i as u64).wrapping_mul(h2)) % num_bits)
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    fn insert(&mut self, key: &[u8]) {
        let positions: Vec<u64> = self.positions(key).collect();
        for p in positions {
            self.bits[(p / 64) as usize] |= 1u64 << (p % 64);
        }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        self.contains_hashed(&BloomKey::new(key))
    }

    pub fn contains_hashed(&self, hk: &BloomKey) -> bool {
        self.positions_hashed(hk)
            .all(|p| self.bits[(p / 64) as usize] & (1u64 << (p % 64)) != 0)
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(12 + self.bits.len() * 8 + 4);
        b.extend_from_slice(&self.num_bits.to_le_bytes());
        b.extend_from_slice(&self.k.to_le_bytes());
        for w in &self.bits {
            b.extend_from_slice(&w.to_le_bytes());
        }
        let crc = crc::crc32(&b);
        b.extend_from_slice(&crc.to_le_bytes());
        b
    }

    fn decode(bytes: &[u8]) -> Result<Bloom, &'static str> {
        if bytes.len() < 4 {
            return Err("truncated");
        }
        let body = &bytes[..bytes.len() - 4];
        let stored = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap());
        if crc::crc32(body) != stored {
            return Err("crc mismatch");
        }
        let mut c = Cursor { b: body, off: 0 };
        let num_bits = c.u64()?;
        let k = c.u32()?;
        if num_bits == 0 || num_bits % 64 != 0 {
            return Err("bad bloom size");
        }
        let words = num_bits / 64;
        let available = body.len() - c.off;
        if words > (available / 8) as u64 {
            return Err("truncated");
        }
        if words < (available / 8) as u64 || available % 8 != 0 {
            return Err("trailing bytes");
        }
        let mut bits = Vec::with_capacity(words as usize);
        for _ in 0..words {
            bits.push(c.u64()?);
        }
        Ok(Bloom { num_bits, k, bits })
    }
}

const BLOCKED_BLOOM_MAGIC: u32 = 0x3146_4242;

const BLOOM_BLOCK_BITS: u64 = 512;
const BLOOM_BLOCK_WORDS: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockedBloom {
    num_bits: u64,
    k: u32,
    bits: Vec<u64>,
}

impl BlockedBloom {
    fn estimated_fpr(&self) -> f64 {
        let blocks = self.bits.chunks(BLOOM_BLOCK_WORDS);
        let n = blocks.len().max(1);
        blocks
            .map(|b| {
                let ones: u32 = b.iter().map(|w| w.count_ones()).sum();
                (ones as f64 / BLOOM_BLOCK_BITS as f64).powi(self.k as i32)
            })
            .sum::<f64>()
            / n as f64
    }

    pub fn with_capacity(n_upper: usize, bits_per_key: usize) -> BlockedBloom {
        let num_bits = ((n_upper.max(1) * bits_per_key) as u64)
            .next_multiple_of(BLOOM_BLOCK_BITS)
            .max(BLOOM_BLOCK_BITS);
        let k = ((bits_per_key as f64 * std::f64::consts::LN_2).round() as u32).clamp(1, 30);
        BlockedBloom {
            num_bits,
            k,
            bits: vec![0u64; (num_bits / 64) as usize],
        }
    }

    pub fn build<'a>(
        keys: impl Iterator<Item = &'a [u8]>,
        n: usize,
        bits_per_key: usize,
    ) -> BlockedBloom {
        let mut bloom = BlockedBloom::with_capacity(n, bits_per_key);
        for key in keys {
            bloom.insert(key);
        }
        bloom
    }

    #[inline]
    fn block_base_and_hashes(&self, key: &[u8]) -> (usize, u64, u64) {
        self.block_base_and_hashes_z(BloomKey::new(key).z)
    }

    #[inline]
    fn block_base_and_hashes_z(&self, z: u64) -> (usize, u64, u64) {
        let num_blocks = self.num_bits / BLOOM_BLOCK_BITS;
        let block = ((z as u128 * num_blocks as u128) >> 64) as usize;
        let z2 = splitmix64(z);
        (block * BLOOM_BLOCK_WORDS, z2, (z2 >> 32) | 1)
    }

    fn insert(&mut self, key: &[u8]) {
        let (base, h1, h2) = self.block_base_and_hashes(key);
        for i in 0..self.k as u64 {
            let pos = h1.wrapping_add(i.wrapping_mul(h2)) % BLOOM_BLOCK_BITS;
            self.bits[base + (pos / 64) as usize] |= 1u64 << (pos % 64);
        }
    }

    #[inline]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.contains_hashed(&BloomKey::new(key))
    }

    #[inline]
    fn prefetch(&self, hk: &BloomKey) {
        let num_blocks = self.num_bits / BLOOM_BLOCK_BITS;
        let block = ((hk.z as u128 * num_blocks as u128) >> 64) as usize;
        prefetch_read(self.bits.as_ptr().wrapping_add(block * BLOOM_BLOCK_WORDS));
    }

    #[inline]
    pub fn contains_hashed(&self, hk: &BloomKey) -> bool {
        let (base, h1, h2) = self.block_base_and_hashes_z(hk.z);
        (0..self.k as u64).all(|i| {
            let pos = h1.wrapping_add(i.wrapping_mul(h2)) % BLOOM_BLOCK_BITS;
            self.bits[base + (pos / 64) as usize] & (1u64 << (pos % 64)) != 0
        })
    }

    fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(16 + self.bits.len() * 8 + 4);
        b.extend_from_slice(&BLOCKED_BLOOM_MAGIC.to_le_bytes());
        b.extend_from_slice(&self.num_bits.to_le_bytes());
        b.extend_from_slice(&self.k.to_le_bytes());
        for w in &self.bits {
            b.extend_from_slice(&w.to_le_bytes());
        }
        let crc = crc::crc32(&b);
        b.extend_from_slice(&crc.to_le_bytes());
        b
    }

    fn decode(bytes: &[u8]) -> Result<BlockedBloom, &'static str> {
        if bytes.len() < 4 {
            return Err("truncated");
        }
        let body = &bytes[..bytes.len() - 4];
        let stored = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap());
        if crc::crc32(body) != stored {
            return Err("crc mismatch");
        }
        let mut c = Cursor { b: body, off: 0 };
        if c.u32()? != BLOCKED_BLOOM_MAGIC {
            return Err("bad blocked-bloom magic");
        }
        let num_bits = c.u64()?;
        if num_bits == 0 || num_bits % BLOOM_BLOCK_BITS != 0 {
            return Err("bad blocked-bloom size");
        }
        let k = c.u32()?;
        let words = num_bits / 64;
        let available = body.len() - c.off;
        if words > (available / 8) as u64 {
            return Err("truncated");
        }
        if words < (available / 8) as u64 || available % 8 != 0 {
            return Err("trailing bytes");
        }
        let mut bits = Vec::with_capacity(words as usize);
        for _ in 0..words {
            bits.push(c.u64()?);
        }
        Ok(BlockedBloom { num_bits, k, bits })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunFilter {
    Classic(Bloom),
    Blocked(BlockedBloom),
    Unfiltered,
}

impl RunFilter {
    pub(crate) fn estimated_fpr(&self) -> f64 {
        match self {
            RunFilter::Classic(b) => b.estimated_fpr(),
            RunFilter::Blocked(b) => b.estimated_fpr(),
            RunFilter::Unfiltered => 1.0,
        }
    }

    fn decode(version: u16, bytes: &[u8]) -> Result<RunFilter, &'static str> {
        if version >= 4 {
            Ok(RunFilter::Blocked(BlockedBloom::decode(bytes)?))
        } else {
            Ok(RunFilter::Classic(Bloom::decode(bytes)?))
        }
    }

    #[inline]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.contains_hashed(&BloomKey::new(key))
    }

    #[inline]
    pub fn contains_hashed(&self, hk: &BloomKey) -> bool {
        match self {
            RunFilter::Classic(b) => b.contains_hashed(hk),
            RunFilter::Blocked(b) => b.contains_hashed(hk),
            RunFilter::Unfiltered => true,
        }
    }

    #[inline]
    pub fn prefetch(&self, hk: &BloomKey) {
        if let RunFilter::Blocked(b) = self {
            b.prefetch(hk);
        }
    }
}

pub type Digest = (Vec<u8>, Vec<u8>);

pub(crate) const RUN_MAGIC: u32 = 0x4e55_5256;
pub const RUN_VERSION: u16 = 6;
pub const RUN_BLOCK_VERSION: u16 = 5;
pub const RUN_SIDECAR_VERSION: u16 = 4;
pub(crate) const FOOTER_LEN: usize = 4 + 2 + 2 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 4 + 4;
const INDEX_RT_TRAILER_LEN: usize = 8 + 8 + 8;

fn parse_run_write_version(raw: Option<&str>) -> u16 {
    match raw.map(str::trim) {
        None | Some("6") => RUN_VERSION,
        Some("5") => RUN_BLOCK_VERSION,
        Some("4") => RUN_SIDECAR_VERSION,
        Some(other) => {
            log::error!("VALS_RUN_WRITE_VERSION={other:?} is not 4, 5 or 6; writing sidecar runs (version 4)");
            RUN_SIDECAR_VERSION
        }
    }
}

pub fn version_supports_range_tombstones(version: u16) -> bool {
    version >= RUN_VERSION
}

#[cfg(any(test, feature = "test-fixtures"))]
thread_local! {
    static WRITE_VERSION_OVERRIDE: std::cell::Cell<Option<u16>> = const { std::cell::Cell::new(None) };
}

#[cfg(any(test, feature = "test-fixtures"))]
pub fn with_run_write_version<R>(version: u16, f: impl FnOnce() -> R) -> R {
    struct Restore(Option<u16>);
    impl Drop for Restore {
        fn drop(&mut self) {
            WRITE_VERSION_OVERRIDE.with(|c| c.set(self.0));
        }
    }
    let _restore = Restore(WRITE_VERSION_OVERRIDE.with(|c| c.replace(Some(version))));
    f()
}

pub fn run_write_version() -> u16 {
    #[cfg(any(test, feature = "test-fixtures"))]
    if let Some(v) = WRITE_VERSION_OVERRIDE.with(|c| c.get()) {
        return v;
    }
    static VALUE: std::sync::OnceLock<u16> = std::sync::OnceLock::new();
    *VALUE.get_or_init(|| {
        parse_run_write_version(std::env::var("VALS_RUN_WRITE_VERSION").ok().as_deref())
    })
}

pub fn merge_write_version(input_versions: impl IntoIterator<Item = u16>) -> u16 {
    let configured = run_write_version();
    match input_versions.into_iter().max() {
        Some(newest) if newest > RUN_SIDECAR_VERSION => configured.max(newest),
        _ => configured,
    }
}

pub fn rewrite_growth_bytes<D: BlockDevice>(input: &DeviceRun<D>, out_version: u16) -> usize {
    if out_version >= RUN_VERSION && input.version < RUN_VERSION {
        input.index.len() * std::mem::size_of::<u64>() + INDEX_RT_TRAILER_LEN
    } else {
        0
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RtLocator {
    pub seq_floor: u64,
    pub off: u64,
    pub len: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct RunRts<'a> {
    pub seq_floor: u64,
    pub records: &'a [RtRecord],
}

impl RunRts<'_> {
    pub const NONE: RunRts<'static> = RunRts { seq_floor: 0, records: &[] };
}

fn run_build_verify() -> bool {
    static VALUE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *VALUE.get_or_init(|| {
        !matches!(std::env::var("VALS_RUN_BUILD_VERIFY").ok().as_deref().map(str::trim), Some("0"))
    })
}

static BUILD_VERIFY_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static BUILD_VERIFY_FAILURES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn run_build_verify_totals() -> (u64, u64) {
    (
        BUILD_VERIFY_BYTES.load(std::sync::atomic::Ordering::Relaxed),
        BUILD_VERIFY_FAILURES.load(std::sync::atomic::Ordering::Relaxed),
    )
}

fn block_crc_ok(block: &[u8]) -> bool {
    block.len() >= 4
        && crc::crc32(&block[..block.len() - 4])
            == u32::from_le_bytes(block[block.len() - 4..].try_into().unwrap())
}

pub const DEFAULT_RUN_BLOCK_TARGET_BYTES: usize = 32 * 1024;
pub const RUN_BLOCK_TARGET_BYTES_RANGE: std::ops::RangeInclusive<usize> = 4096..=256 * 1024;
pub const DEFAULT_RUN_BLOOM_BITS_PER_KEY: usize = 10;
pub const RUN_BLOOM_BITS_PER_KEY_RANGE: std::ops::RangeInclusive<usize> = 4..=24;

fn parse_bounded_knob(
    name: &str,
    raw: Option<&str>,
    default: usize,
    range: std::ops::RangeInclusive<usize>,
) -> usize {
    match raw.map(str::trim) {
        None | Some("") => default,
        Some(s) => match s.parse::<usize>() {
            Ok(v) if range.contains(&v) => v,
            Ok(v) => {
                let clamped = v.clamp(*range.start(), *range.end());
                log::error!("{name}={v} is outside {}..={}; using {clamped}", range.start(), range.end());
                clamped
            }
            Err(_) => {
                log::error!("{name}={s:?} is not an unsigned integer; using {default}");
                default
            }
        },
    }
}

pub fn parse_run_block_target_bytes(raw: Option<&str>) -> usize {
    parse_bounded_knob(
        "VALS_RUN_BLOCK_TARGET_BYTES",
        raw,
        DEFAULT_RUN_BLOCK_TARGET_BYTES,
        RUN_BLOCK_TARGET_BYTES_RANGE,
    )
}

pub fn parse_run_bloom_bits_per_key(raw: Option<&str>) -> usize {
    parse_bounded_knob(
        "VALS_RUN_BLOOM_BITS_PER_KEY",
        raw,
        DEFAULT_RUN_BLOOM_BITS_PER_KEY,
        RUN_BLOOM_BITS_PER_KEY_RANGE,
    )
}

#[cfg(any(test, feature = "test-fixtures"))]
thread_local! {
    static BLOCK_TARGET_OVERRIDE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

#[cfg(any(test, feature = "test-fixtures"))]
pub fn with_run_block_target<R>(bytes: usize, f: impl FnOnce() -> R) -> R {
    struct Restore(Option<usize>);
    impl Drop for Restore {
        fn drop(&mut self) {
            BLOCK_TARGET_OVERRIDE.with(|c| c.set(self.0));
        }
    }
    let _restore = Restore(BLOCK_TARGET_OVERRIDE.with(|c| c.replace(Some(bytes))));
    f()
}

#[cfg(any(test, feature = "test-fixtures"))]
static PROCESS_BLOCK_TARGET_OVERRIDE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[cfg(any(test, feature = "test-fixtures"))]
pub fn set_process_run_block_target(bytes: Option<usize>) {
    PROCESS_BLOCK_TARGET_OVERRIDE.store(bytes.unwrap_or(0), std::sync::atomic::Ordering::Relaxed);
}

pub fn run_block_target_bytes() -> usize {
    #[cfg(any(test, feature = "test-fixtures"))]
    if let Some(v) = BLOCK_TARGET_OVERRIDE.with(|c| c.get()) {
        return v;
    }
    #[cfg(any(test, feature = "test-fixtures"))]
    match PROCESS_BLOCK_TARGET_OVERRIDE.load(std::sync::atomic::Ordering::Relaxed) {
        0 => {}
        v => return v,
    }
    static VALUE: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *VALUE.get_or_init(|| {
        parse_run_block_target_bytes(std::env::var("VALS_RUN_BLOCK_TARGET_BYTES").ok().as_deref())
    })
}

pub fn run_bloom_bits_per_key() -> usize {
    static VALUE: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *VALUE.get_or_init(|| {
        parse_run_bloom_bits_per_key(std::env::var("VALS_RUN_BLOOM_BITS_PER_KEY").ok().as_deref())
    })
}

#[derive(Debug, Clone, Copy)]
pub struct RunBuildOpts {
    pub block_target_bytes: usize,
    pub bloom_bits_per_key: usize,
    pub num_buckets: u32,
}

impl Default for RunBuildOpts {
    fn default() -> Self {
        RunBuildOpts {
            block_target_bytes: run_block_target_bytes(),
            bloom_bits_per_key: run_bloom_bits_per_key(),
            num_buckets: 4096,
        }
    }
}

pub trait RunSink {
    fn write(&mut self, bytes: &[u8]);
    fn position(&self) -> u64;
}

pub struct VecSink {
    out: Vec<u8>,
}

impl VecSink {
    fn new() -> Self {
        VecSink { out: Vec::new() }
    }
    fn into_inner(self) -> Vec<u8> {
        self.out
    }
}

impl RunSink for VecSink {
    fn write(&mut self, bytes: &[u8]) {
        self.out.extend_from_slice(bytes);
    }
    fn position(&self) -> u64 {
        self.out.len() as u64
    }
}

pub fn build_run(entries: &[SnapshotEntry], opts: RunBuildOpts) -> Vec<u8> {
    build_run_versioned(entries, opts, run_write_version())
}

pub fn build_run_versioned(entries: &[SnapshotEntry], opts: RunBuildOpts, version: u16) -> Vec<u8> {
    let mut sink = VecSink::new();
    build_run_into_versioned(entries, opts, version, &mut sink);
    sink.into_inner()
}

pub fn build_run_checked(
    entries: &[SnapshotEntry],
    opts: RunBuildOpts,
    version: u16,
    path: EncodePath,
    source: RunSource<'_>,
) -> Result<(Vec<u8>, RunBuilt), Box<EncodeIntegrityError>> {
    build_run_checked_rts(entries, opts, version, path, source, RunRts::NONE)
}

pub fn build_run_checked_rts(
    entries: &[SnapshotEntry],
    opts: RunBuildOpts,
    version: u16,
    path: EncodePath,
    source: RunSource<'_>,
    rts: RunRts<'_>,
) -> Result<(Vec<u8>, RunBuilt), Box<EncodeIntegrityError>> {
    let mut sink = VecSink::new();
    let built = build_run_into_checked_rts(entries, opts, version, path, source, rts, &mut sink)?;
    Ok((sink.into_inner(), built))
}

pub fn build_run_into<S: RunSink>(entries: &[SnapshotEntry], opts: RunBuildOpts, sink: &mut S) {
    build_run_into_versioned(entries, opts, run_write_version(), sink)
}

pub fn build_run_into_versioned<S: RunSink>(
    entries: &[SnapshotEntry],
    opts: RunBuildOpts,
    version: u16,
    sink: &mut S,
) {
    if let Err(e) = build_run_into_checked(entries, opts, version, EncodePath::Merge, RunSource::Inline, sink) {
        panic!("{e}");
    }
}

pub fn build_run_into_checked<S: RunSink>(
    entries: &[SnapshotEntry],
    opts: RunBuildOpts,
    version: u16,
    path: EncodePath,
    source: RunSource<'_>,
    sink: &mut S,
) -> Result<RunBuilt, Box<EncodeIntegrityError>> {
    build_run_into_checked_rts(entries, opts, version, path, source, RunRts::NONE, sink)
}

#[allow(clippy::too_many_arguments)]
pub fn build_run_into_checked_rts<S: RunSink>(
    entries: &[SnapshotEntry],
    opts: RunBuildOpts,
    version: u16,
    path: EncodePath,
    source: RunSource<'_>,
    rts: RunRts<'_>,
    sink: &mut S,
) -> Result<RunBuilt, Box<EncodeIntegrityError>> {
    debug_assert!(
        (RUN_SIDECAR_VERSION..=RUN_VERSION).contains(&version),
        "runs are written as version 4, 5 or 6"
    );
    let v6 = version_supports_range_tombstones(version);
    let mut index = FenceBuilder::default();
    let mut check = EncodeCheck::new(path);
    let mut bloom = BlockedBloom::with_capacity(entries.len(), opts.bloom_bits_per_key);
    let data_start = sink.position();
    let mut hashes: Vec<u64> = Vec::new();

    let mut i = 0;
    while i < entries.len() {
        let start = i;
        let mut est = 0usize;
        while i < entries.len() && (i == start || est < opts.block_target_bytes) {
            est += entries[i].key.len() + 24 + 16;
            i += 1;
        }
        let src = &entries[start..i];
        let expect = match source {
            RunSource::Inline => {
                hashes.clear();
                hashes.extend(src.iter().map(snapshot_entry_hash));
                Expect::Entries(&hashes)
            }
            RunSource::Attested(groups) => Expect::Groups { groups, all: entries },
        };
        let (block, first_key) = check.block(src, expect, &mut bloom)?;
        let off = sink.position();
        if v6 {
            index.push_with_max_hlc(&first_key, off, block.len() as u32, check.last_block_max_hlc())
        } else {
            index.push(&first_key, off, block.len() as u32)
        }
        .map_err(|why| check.index_error(why))?;
        sink.write(&block);
        check.retire(block);
    }
    check.finish_source(source)?;
    let index = index.finish();

    let bloom_off = sink.position();
    let bloom_bytes = bloom.encode();
    sink.write(&bloom_bytes);

    let rt = if v6 {
        let rt_bytes = check.rt_block(rts.records)?;
        let rt = RtLocator { seq_floor: rts.seq_floor, off: sink.position(), len: rt_bytes.len() as u64 };
        sink.write(&rt_bytes);
        Some(rt)
    } else {
        if !rts.records.is_empty() {
            return Err(check.rt_error("range tombstone records need run format v6"));
        }
        None
    };

    let index_off = sink.position();
    let idx = encode_index_bytes(&index, rt);
    check.verify_index(&idx, data_start, bloom_off, rt)?;
    sink.write(&idx);

    let sidecar_off = sink.position();
    let mut side_len = 0u64;
    if version <= RUN_SIDECAR_VERSION {
        let mut side_crc = crc::Crc32::new();
        let head = opts.num_buckets.to_le_bytes();
        side_crc.update(&head);
        sink.write(&head);
        side_len += head.len() as u64;
        let mut rec = Vec::new();
        for e in entries {
            encode_sidecar_record(&mut rec, &e.key, e.version);
            side_crc.update(&rec);
            sink.write(&rec);
            side_len += rec.len() as u64;
        }
        let crc_bytes = side_crc.finalize().to_le_bytes();
        sink.write(&crc_bytes);
        side_len += crc_bytes.len() as u64;
    }

    let f = encode_footer(&FooterFields {
        entry_count: entries.len() as u64,
        num_blocks: index.len() as u64,
        index_off,
        index_len: idx.len() as u64,
        bloom_off,
        bloom_len: bloom_bytes.len() as u64,
        sidecar_off,
        sidecar_len: side_len,
        num_buckets: opts.num_buckets,
        version,
    });
    sink.write(&f);
    crate::attribution::run_parts(if matches!(path, EncodePath::Flush) { 0 } else { 1 },
        bloom_off - data_start, idx.len() as u64, bloom_bytes.len() as u64, f.len() as u64, side_len);
    Ok(check.bounds())
}

fn encode_sidecar_record(buf: &mut Vec<u8>, key: &[u8], version: u128) {
    buf.clear();
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&version.to_le_bytes());
}

fn encode_index_bytes(index: &FenceIndex, rt: Option<RtLocator>) -> Vec<u8> {
    debug_assert!(rt.is_none() || index.has_max_hlc(), "a v6 index carries max_hlc for every block");
    let mut idx = Vec::new();
    idx.extend_from_slice(&(index.len() as u64).to_le_bytes());
    for (i, (k, off, len)) in index.iter().enumerate() {
        idx.extend_from_slice(&(k.len() as u32).to_le_bytes());
        idx.extend_from_slice(k);
        idx.extend_from_slice(&off.to_le_bytes());
        idx.extend_from_slice(&len.to_le_bytes());
        if rt.is_some() {
            idx.extend_from_slice(&index.max_hlc(i).unwrap_or(u64::MAX).to_le_bytes());
        }
    }
    if let Some(rt) = rt {
        idx.extend_from_slice(&rt.seq_floor.to_le_bytes());
        idx.extend_from_slice(&rt.off.to_le_bytes());
        idx.extend_from_slice(&rt.len.to_le_bytes());
    }
    #[cfg(any(test, feature = "test-fixtures"))]
    encode_test_seams::flip_index(&mut idx);
    let idx_crc = crc::crc32(&idx);
    idx.extend_from_slice(&idx_crc.to_le_bytes());
    idx
}

struct FooterFields {
    entry_count: u64,
    num_blocks: u64,
    index_off: u64,
    index_len: u64,
    bloom_off: u64,
    bloom_len: u64,
    sidecar_off: u64,
    sidecar_len: u64,
    num_buckets: u32,
    version: u16,
}

fn encode_footer(ff: &FooterFields) -> Vec<u8> {
    let mut f = Vec::with_capacity(FOOTER_LEN);
    f.extend_from_slice(&RUN_MAGIC.to_le_bytes());
    f.extend_from_slice(&ff.version.to_le_bytes());
    f.extend_from_slice(&0u16.to_le_bytes());
    f.extend_from_slice(&ff.entry_count.to_le_bytes());
    f.extend_from_slice(&ff.num_blocks.to_le_bytes());
    f.extend_from_slice(&ff.index_off.to_le_bytes());
    f.extend_from_slice(&ff.index_len.to_le_bytes());
    f.extend_from_slice(&ff.bloom_off.to_le_bytes());
    f.extend_from_slice(&ff.bloom_len.to_le_bytes());
    f.extend_from_slice(&ff.sidecar_off.to_le_bytes());
    f.extend_from_slice(&ff.sidecar_len.to_le_bytes());
    f.extend_from_slice(&ff.num_buckets.to_le_bytes());
    let f_crc = crc::crc32(&f);
    f.extend_from_slice(&f_crc.to_le_bytes());
    debug_assert_eq!(f.len(), FOOTER_LEN);
    f
}

pub fn run_len_upper_bound(entries: &[SnapshotEntry], opts: RunBuildOpts) -> usize {
    run_len_upper_bound_for(entries, opts, run_write_version())
}

pub fn run_len_upper_bound_for(entries: &[SnapshotEntry], opts: RunBuildOpts, version: u16) -> usize {
    let n = entries.len();
    let key_bytes: usize = entries.iter().map(|e| e.key.len()).sum();
    let max_key = entries.iter().map(|e| e.key.len()).max().unwrap_or(0);
    run_len_upper_bound_agg(n, key_bytes, max_key, opts, version)
}

pub fn run_len_upper_bound_agg(
    n: usize,
    key_bytes: usize,
    max_key: usize,
    opts: RunBuildOpts,
    version: u16,
) -> usize {
    let mut bound = 0usize;
    bound += n * 48 + key_bytes;
    let n_blocks = n;
    bound += n_blocks * (4 + 4);
    bound += 8 + n_blocks * (4 + max_key + 8 + 4) + 4;
    bound += n * opts.bloom_bits_per_key / 8 + 1024;
    if version <= RUN_SIDECAR_VERSION {
        bound += 4 + n * 20 + key_bytes + 4;
    }
    if version_supports_range_tombstones(version) {
        bound += n_blocks * std::mem::size_of::<u64>() + INDEX_RT_TRAILER_LEN;
    }
    bound += FOOTER_LEN;
    bound
}

#[cfg(test)]
pub struct Run {
    bytes: Vec<u8>,
    index: FenceIndex,
    bloom: RunFilter,
    entry_count: u64,
    num_buckets: u32,
    sidecar_off: u64,
    sidecar_len: u64,
    version: u16,
}

#[cfg(test)]
impl Run {
    pub fn parse(bytes: Vec<u8>) -> Result<Run, &'static str> {
        if bytes.len() < FOOTER_LEN {
            return Err("truncated");
        }
        let ft = decode_footer(&bytes[bytes.len() - FOOTER_LEN..])?;
        let bloom = RunFilter::decode(ft.version, section(&bytes, ft.bloom_off, ft.bloom_len)?)?;
        let (index, _) = decode_index(section(&bytes, ft.index_off, ft.index_len)?, ft.version)?;
        Ok(Run {
            bytes,
            index,
            bloom,
            entry_count: ft.entry_count,
            num_buckets: ft.num_buckets,
            sidecar_off: ft.sidecar_off,
            sidecar_len: ft.sidecar_len,
            version: ft.version,
        })
    }

    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    pub fn lookup(&self, key: &[u8]) -> Result<Option<SnapshotEntry>, &'static str> {
        if !self.bloom.contains(key) {
            return Ok(None);
        }
        let blk = match self
            .index
            .partition_point(|fk| fk <= key)
        {
            0 => return Ok(None),
            p => self.index.get(p - 1).unwrap(),
        };
        let (_, off, len) = blk;
        let block = section(&self.bytes, *off, *len as u64)?;
        block_lookup(block, key)
    }

    pub fn bucket_digests(
        &self,
        bucket: u32,
        bucket_of: impl Fn(&[u8]) -> u32,
    ) -> Result<Vec<Digest>, &'static str> {
        if self.version > RUN_SIDECAR_VERSION {
            if bucket >= self.num_buckets {
                return Err("bucket out of range");
            }
            let mut out = Vec::new();
            for (_, off, len) in self.index.iter() {
                for e in decode_block(section(&self.bytes, *off, *len as u64)?)? {
                    if bucket_of(&e.key) % self.num_buckets.max(1) == bucket {
                        out.push((e.key, e.version.to_le_bytes().to_vec()));
                    }
                }
            }
            return Ok(out);
        }
        let side = section(&self.bytes, self.sidecar_off, self.sidecar_len)?;
        let body = sidecar_body(side)?;
        if self.version >= 3 {
            return sidecar_v3_bucket_digests(body, bucket, self.num_buckets, bucket_of);
        }
        let mut c = Cursor { b: body, off: 0 };
        let n = c.u32()?;
        if bucket >= n {
            return Err("bucket out of range");
        }
        for b in 0..n {
            let count = c.u32()? as usize;
            if b == bucket {
                let mut out = Vec::with_capacity(count);
                for _ in 0..count {
                    let kl = c.u32()? as usize;
                    let k = c.take(kl)?.to_vec();
                    let v = c.take(16)?.to_vec();
                    out.push((k, v));
                }
                return Ok(out);
            }
            for _ in 0..count {
                let kl = c.u32()? as usize;
                c.take(kl)?;
                c.take(16)?;
            }
        }
        Ok(Vec::new())
    }

    pub fn num_buckets(&self) -> u32 {
        self.num_buckets
    }
}

fn sidecar_body(side: &[u8]) -> Result<&[u8], &'static str> {
    if side.len() < 4 {
        return Err("sidecar truncated");
    }
    let body = &side[..side.len() - 4];
    let stored = u32::from_le_bytes(side[side.len() - 4..].try_into().unwrap());
    if crc::crc32(body) != stored {
        return Err("sidecar crc mismatch");
    }
    Ok(body)
}

pub struct SidecarVersions {
    body: Vec<u8>,
    version: u16,
    off: usize,
    groups_left: u32,
    records_left: u32,
}

impl SidecarVersions {
    fn from_body(version: u16, body: Vec<u8>) -> Result<Self, SnapshotError> {
        let mut c = Cursor { b: &body, off: 0 };
        let groups_left = c.u32().map_err(SnapshotError::Corrupt)?;
        let off = c.off;
        Ok(SidecarVersions {
            body,
            version,
            off,
            groups_left,
            records_left: 0,
        })
    }

    fn from_sidecar(version: u16, mut side: Vec<u8>) -> Result<Self, SnapshotError> {
        sidecar_body(&side).map_err(SnapshotError::Corrupt)?;
        side.truncate(side.len() - 4);
        Self::from_body(version, side)
    }

    pub fn body_len(&self) -> usize {
        self.body.len()
    }

    pub fn fold_page(
        &mut self,
        max_keys: usize,
        mut f: impl FnMut(&[u8], u128),
    ) -> Result<bool, SnapshotError> {
        let mut c = Cursor {
            b: &self.body,
            off: self.off,
        };
        let mut folded = 0usize;
        if self.version >= 3 {
            while c.off < self.body.len() && folded < max_keys {
                let kl = c.u32().map_err(SnapshotError::Corrupt)? as usize;
                let key = c.take(kl).map_err(SnapshotError::Corrupt)?;
                let version = c.u128().map_err(SnapshotError::Corrupt)?;
                f(key, version);
                folded += 1;
            }
            self.off = c.off;
            return Ok(self.off == self.body.len());
        }

        while folded < max_keys {
            if self.records_left == 0 {
                if self.groups_left == 0 {
                    self.off = c.off;
                    return Ok(true);
                }
                self.records_left = c.u32().map_err(SnapshotError::Corrupt)?;
                self.groups_left -= 1;
                continue;
            }
            let kl = c.u32().map_err(SnapshotError::Corrupt)? as usize;
            let key = c.take(kl).map_err(SnapshotError::Corrupt)?;
            let version = c.u128().map_err(SnapshotError::Corrupt)?;
            f(key, version);
            self.records_left -= 1;
            folded += 1;
        }
        self.off = c.off;
        Ok(self.groups_left == 0 && self.records_left == 0)
    }
}

fn sidecar_v3_bucket_digests(
    body: &[u8],
    bucket: u32,
    num_buckets: u32,
    bucket_of: impl Fn(&[u8]) -> u32,
) -> Result<Vec<Digest>, &'static str> {
    let mut c = Cursor { b: body, off: 0 };
    let n = c.u32()?;
    debug_assert_eq!(n, num_buckets, "sidecar head disagrees with footer");
    if bucket >= n {
        return Err("bucket out of range");
    }
    let mut out = Vec::new();
    while c.off < body.len() {
        let kl = c.u32()? as usize;
        let k = c.take(kl)?;
        let v = c.take(16)?;
        if bucket_of(k) % n.max(1) == bucket {
            out.push((k.to_vec(), v.to_vec()));
        }
    }
    Ok(out)
}

#[cfg(test)]
fn section(bytes: &[u8], off: u64, len: u64) -> Result<&[u8], &'static str> {
    let end = off.checked_add(len).ok_or("section overflow")?;
    bytes
        .get(off as usize..end as usize)
        .ok_or("section out of range")
}

fn decode_index(bytes: &[u8], version: u16) -> Result<(FenceIndex, Option<RtLocator>), &'static str> {
    if bytes.len() < 4 {
        return Err("truncated");
    }
    let body = &bytes[..bytes.len() - 4];
    let stored = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap());
    if crc::crc32(body) != stored {
        return Err("index crc mismatch");
    }
    let v6 = version_supports_range_tombstones(version);
    let (entry_fixed, trailer) = if v6 { (24, INDEX_RT_TRAILER_LEN) } else { (16, 0) };
    let mut c = Cursor { b: body, off: 0 };
    let n = c.u64()?;
    if body.len() - c.off < trailer || n > ((body.len() - c.off - trailer) / entry_fixed) as u64 {
        return Err("truncated");
    }
    let entries_end = body.len() - trailer;
    let start = c.off;
    let mut key_bytes = 0;
    for _ in 0..n {
        let kl = c.u32()? as usize;
        c.take(kl)?;
        c.take(entry_fixed - 4)?;
        key_bytes = fence::key_end(key_bytes, kl)? as usize;
    }
    if c.off != entries_end {
        return Err("trailing bytes");
    }
    c.off = start;
    let mut out = FenceBuilder::with_capacity(n as usize, key_bytes);
    for _ in 0..n {
        let kl = c.u32()? as usize;
        let k = c.take(kl)?;
        let off = c.u64()?;
        let len = c.u32()?;
        if v6 {
            out.push_with_max_hlc(k, off, len, c.u64()?)?;
        } else {
            out.push(k, off, len)?;
        }
    }
    let rt = if v6 {
        let rt = RtLocator { seq_floor: c.u64()?, off: c.u64()?, len: c.u64()? };
        Some(rt)
    } else {
        None
    };
    if c.off != body.len() {
        return Err("trailing bytes");
    }
    Ok((out.finish(), rt))
}

struct Footer {
    entry_count: u64,
    index_off: u64,
    index_len: u64,
    bloom_off: u64,
    bloom_len: u64,
    sidecar_off: u64,
    sidecar_len: u64,
    num_buckets: u32,
    version: u16,
}

fn decode_footer(f: &[u8]) -> Result<Footer, &'static str> {
    decode_footer_capped(f, RUN_VERSION)
}

fn decode_footer_capped(f: &[u8], max_version: u16) -> Result<Footer, &'static str> {
    if f.len() != FOOTER_LEN {
        return Err("bad footer length");
    }
    let body = &f[..FOOTER_LEN - 4];
    let stored = u32::from_le_bytes(f[FOOTER_LEN - 4..].try_into().unwrap());
    if crc::crc32(body) != stored {
        return Err("footer crc mismatch");
    }
    let mut c = Cursor { b: body, off: 0 };
    if c.u32()? != RUN_MAGIC {
        return Err("bad run magic");
    }
    let version = u16::from_le_bytes(c.take(2)?.try_into().unwrap());
    if !(1..=max_version).contains(&version) {
        return Err("unsupported run version");
    }
    let _reserved = c.take(2)?;
    let entry_count = c.u64()?;
    let _num_blocks = c.u64()?;
    let index_off = c.u64()?;
    let index_len = c.u64()?;
    let bloom_off = c.u64()?;
    let bloom_len = c.u64()?;
    let sidecar_off = c.u64()?;
    let sidecar_len = c.u64()?;
    let num_buckets = c.u32()?;
    Ok(Footer {
        entry_count,
        index_off,
        index_len,
        bloom_off,
        bloom_len,
        sidecar_off,
        sidecar_len,
        num_buckets,
        version,
    })
}

use std::sync::Arc;
use vals_spdk::{AsyncRead, BlockDevice};

use crate::alloc::Allocator;
use crate::snapshot_io::SnapshotError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunHandle {
    pub segs: Vec<SegmentId>,
    pub len: u64,
}

pub fn allocate_run_segs(
    alloc: &mut Allocator,
    block_size: usize,
    run_len: usize,
) -> Result<Vec<SegmentId>, SnapshotError> {
    let seg_bytes = alloc.blocks_per_segment() as usize * block_size;
    let n = run_len.div_ceil(seg_bytes).max(1);
    let mut segs = Vec::with_capacity(n);
    for _ in 0..n {
        match alloc.allocate() {
            Some(s) => segs.push(s),
            None => {
                for s in &segs {
                    let _ = alloc.free(*s);
                }
                return Err(SnapshotError::DeviceFull);
            }
        }
    }
    Ok(segs)
}

fn run_seg_write_buf(run_bytes: &[u8], i: usize, seg_bytes: usize, block_size: usize) -> Vec<u8> {
    let start = i * seg_bytes;
    let end = ((i + 1) * seg_bytes).min(run_bytes.len());
    let chunk = &run_bytes[start..end.max(start)];
    let nblocks = chunk.len().div_ceil(block_size).max(1);
    let mut buf = vec![0u8; nblocks * block_size];
    buf[..chunk.len()].copy_from_slice(chunk);
    buf
}

#[cfg(test)]
pub async fn write_run_segs<D: BlockDevice>(
    dev: &D,
    segs: &[SegmentId],
    seg_lbas: &[u64],
    seg_bytes: usize,
    block_size: usize,
    run_bytes: &[u8],
) -> Result<RunHandle, SnapshotError> {
    debug_assert_eq!(segs.len(), seg_lbas.len());
    for (i, &lba) in seg_lbas.iter().enumerate() {
        let buf = run_seg_write_buf(run_bytes, i, seg_bytes, block_size);
        crate::attribution::write_bytes(3, buf.len());
        dev.write_at(lba, &buf)?;
        if (i + 1) % FLUSH_WRITE_BATCH_SEGS == 0 {
            vals_rt::yield_now().await;
        }
    }
    dev.flush()?;
    Ok(RunHandle {
        segs: segs.to_vec(),
        len: run_bytes.len() as u64,
    })
}

#[cfg(test)]
const FLUSH_WRITE_BATCH_SEGS: usize = 4;

pub struct SegmentStreamSink<'a, D: BlockDevice> {
    dev: &'a D,
    segs: &'a [SegmentId],
    seg_lbas: &'a [u64],
    seg_bytes: usize,
    block_size: usize,
    buf: Vec<u8>,
    seg_idx: usize,
    position: u64,
    paced: bool,
    write_source: usize,
    flush_every_bytes: u64,
    since_flush_bytes: u64,
    err: Option<SnapshotError>,
}

impl<'a, D: BlockDevice> SegmentStreamSink<'a, D> {
    pub fn new(
        dev: &'a D,
        segs: &'a [SegmentId],
        seg_lbas: &'a [u64],
        seg_bytes: usize,
        block_size: usize,
    ) -> Self {
        debug_assert_eq!(segs.len(), seg_lbas.len());
        SegmentStreamSink {
            dev,
            segs,
            seg_lbas,
            seg_bytes,
            block_size,
            buf: Vec::with_capacity(seg_bytes),
            seg_idx: 0,
            position: 0,
            paced: false,
            write_source: 4,
            flush_every_bytes: 0,
            since_flush_bytes: 0,
            err: None,
        }
    }

    pub fn write_source(mut self, source: usize) -> Self {
        self.write_source = source;
        self
    }

    pub fn paced(mut self, paced: bool) -> Self {
        self.paced = paced;
        self
    }

    pub fn flush_every(mut self, bytes: u64) -> Self {
        self.flush_every_bytes = bytes;
        self
    }

    fn flush_full_segments(&mut self) {
        if self.err.is_some() {
            return;
        }
        while self.buf.len() >= self.seg_bytes {
            let Some(&lba) = self.seg_lbas.get(self.seg_idx) else {
                self.err = Some(SnapshotError::Corrupt(
                    "streamed run exceeded its reserved segments",
                ));
                return;
            };
            let write_t = std::time::Instant::now();
            crate::attribution::write_bytes(self.write_source, self.seg_bytes);
            if let Err(e) = self.dev.write_at(lba, &self.buf[..self.seg_bytes]) {
                self.err = Some(SnapshotError::from(e));
                return;
            }
            self.since_flush_bytes += self.seg_bytes as u64;
            if self.flush_every_bytes > 0 && self.since_flush_bytes >= self.flush_every_bytes {
                if let Err(e) = self.dev.flush() {
                    self.err = Some(SnapshotError::from(e));
                    return;
                }
                self.since_flush_bytes = 0;
            }
            if self.paced {
                crate::pace::maint_pace_charge(write_t.elapsed());
            }
            self.buf.drain(..self.seg_bytes);
            self.seg_idx += 1;
        }
    }

    fn read_logical(&self, off: u64, len: usize) -> Result<Vec<u8>, SnapshotError> {
        if self.err.is_some() {
            return Err(SnapshotError::Corrupt("streamed run write already failed"));
        }
        let off = off as usize;
        let end = off
            .checked_add(len)
            .ok_or(SnapshotError::Corrupt("stream read range overflow"))?;
        let flushed = self.seg_idx * self.seg_bytes;
        if end > flushed + self.buf.len() {
            return Err(SnapshotError::Corrupt("stream read past written bytes"));
        }
        let mut out = Vec::with_capacity(len);
        if off < flushed {
            let dev_len = len.min(flushed - off);
            let bytes = read_run_range_batched(
                self.dev,
                self.seg_lbas,
                self.seg_bytes,
                off as u64,
                dev_len,
                READBACK_QD,
            )?;
            out.extend_from_slice(&bytes);
        }
        if end > flushed {
            let start = off.max(flushed) - flushed;
            out.extend_from_slice(&self.buf[start..end - flushed]);
        }
        Ok(out)
    }

    pub async fn finish(mut self) -> Result<RunHandle, SnapshotError> {
        if let Some(e) = self.err.take() {
            return Err(e);
        }
        let mut used = self.seg_idx;
        if !self.buf.is_empty() {
            let Some(&lba) = self.seg_lbas.get(self.seg_idx) else {
                return Err(SnapshotError::Corrupt(
                    "streamed run exceeded its reserved segments",
                ));
            };
            let nblocks = self.buf.len().div_ceil(self.block_size).max(1);
            self.buf.resize(nblocks * self.block_size, 0);
            crate::attribution::write_bytes(self.write_source, self.buf.len());
            self.dev.write_at(lba, &self.buf)?;
            used += 1;
        } else if used == 0 {
            let Some(&lba) = self.seg_lbas.first() else {
                return Err(SnapshotError::Corrupt(
                    "streamed run exceeded its reserved segments",
                ));
            };
            let buf = vec![0u8; self.block_size];
            crate::attribution::write_bytes(self.write_source, buf.len());
            self.dev.write_at(lba, &buf)?;
            used = 1;
        }
        self.dev.flush()?;
        vals_rt::yield_now().await;
        Ok(RunHandle {
            segs: self.segs[..used].to_vec(),
            len: self.position,
        })
    }
}

impl<'a, D: BlockDevice> RunSink for SegmentStreamSink<'a, D> {
    fn write(&mut self, bytes: &[u8]) {
        self.position += bytes.len() as u64;
        if self.err.is_some() {
            return;
        }
        self.buf.extend_from_slice(bytes);
        if self.buf.len() >= self.seg_bytes {
            self.flush_full_segments();
        }
    }
    fn position(&self) -> u64 {
        self.position
    }
}

pub struct RunStreamBuilder<'a, D: BlockDevice> {
    sink: SegmentStreamSink<'a, D>,
    opts: RunBuildOpts,
    index: FenceBuilder,
    bloom: BlockedBloom,
    cur: Vec<SnapshotEntry>,
    cur_hashes: Vec<u64>,
    cur_est: usize,
    entry_count: u64,
    version: u16,
    verify: bool,
    data_start: u64,
    check: EncodeCheck,
    failed: Option<Box<EncodeIntegrityError>>,
    rt_seq_floor: u64,
    rt_records: Vec<RtRecord>,
}

impl<'a, D: BlockDevice> RunStreamBuilder<'a, D> {
    pub(crate) fn profile_bytes(&self) -> u64 {
        (self.index.allocated_bytes()
            + self.bloom.bits.capacity() * std::mem::size_of::<u64>()) as u64
    }

    pub fn new(sink: SegmentStreamSink<'a, D>, opts: RunBuildOpts, n_upper: usize) -> Self {
        let data_start = sink.position();
        RunStreamBuilder {
            sink,
            opts,
            index: FenceBuilder::default(),
            bloom: BlockedBloom::with_capacity(n_upper, opts.bloom_bits_per_key),
            cur: Vec::new(),
            cur_hashes: Vec::new(),
            cur_est: 0,
            entry_count: 0,
            version: run_write_version(),
            verify: run_build_verify(),
            data_start,
            check: EncodeCheck::new(EncodePath::Merge),
            failed: None,
            rt_seq_floor: 0,
            rt_records: Vec::new(),
        }
    }

    pub fn version(mut self, version: u16) -> Self {
        debug_assert!((RUN_SIDECAR_VERSION..=RUN_VERSION).contains(&version));
        self.version = version;
        self
    }

    pub fn range_tombstones(mut self, seq_floor: u64, records: Vec<RtRecord>) -> Self {
        self.rt_seq_floor = seq_floor;
        self.rt_records = records;
        self
    }

    pub fn verify(mut self, verify: bool) -> Self {
        self.verify = verify;
        self
    }

    pub fn take_failure(&mut self) -> Option<Box<EncodeIntegrityError>> {
        self.failed.take()
    }

    pub fn push(&mut self, e: &SnapshotEntry) {
        if self.failed.is_none() {
            self.push_owned(e.clone());
        }
    }

    pub fn push_owned(&mut self, e: SnapshotEntry) {
        if self.failed.is_some() {
            return;
        }
        if !self.cur.is_empty() && self.cur_est >= self.opts.block_target_bytes {
            self.close_block();
        }
        self.cur_hashes.push(snapshot_entry_hash(&e));
        self.cur_est += e.key.len() + 24 + 16;
        self.cur.push(e);
        #[cfg(any(test, feature = "test-fixtures"))]
        if let Some((byte, mask, before_hash)) = encode_test_seams::take_stream_flip(self.entry_count) {
            let last = self.cur.last_mut().unwrap();
            last.key[byte] ^= mask;
            if before_hash {
                *self.cur_hashes.last_mut().unwrap() = snapshot_entry_hash(last);
            }
        }
        self.entry_count += 1;
    }

    fn close_block(&mut self) {
        if self.failed.is_none() {
            match self.check.block(&self.cur, Expect::Entries(&self.cur_hashes), &mut self.bloom) {
                Ok((block, first_key)) => {
                    let off = self.sink.position();
                    let pushed = if version_supports_range_tombstones(self.version) {
                        let max_hlc = self.check.last_block_max_hlc();
                        self.index.push_with_max_hlc(&first_key, off, block.len() as u32, max_hlc)
                    } else {
                        self.index.push(&first_key, off, block.len() as u32)
                    };
                    if let Err(why) = pushed {
                        self.failed = Some(self.check.index_error(why));
                        return;
                    }
                    self.sink.write(&block);
                    self.check.retire(block);
                }
                Err(e) => self.failed = Some(e),
            }
        }
        self.cur.clear();
        self.cur_hashes.clear();
        self.cur_est = 0;
    }

    #[cfg(test)]
    pub async fn finish(self) -> Result<RunHandle, SnapshotError> {
        Ok(self.finish_with_parts().await?.0)
    }

    pub async fn finish_with_parts(self) -> Result<(RunHandle, RunParts), SnapshotError> {
        let (handle, parts, _) = self.finish_with_bounds().await?;
        Ok((handle, parts))
    }

    pub async fn finish_with_bounds(mut self) -> Result<(RunHandle, RunParts, RunBuilt), SnapshotError> {
        if !self.cur.is_empty() {
            self.close_block();
        }
        if let Some(e) = self.failed.take() {
            return Err(SnapshotError::EncodeIntegrity(e));
        }
        let index = std::mem::take(&mut self.index).finish();
        let built = self.check.bounds();
        let bloom_off = self.sink.position();
        let bloom_bytes = self.bloom.encode();
        self.sink.write(&bloom_bytes);
        let (rt, rt_bytes) = if version_supports_range_tombstones(self.version) {
            let rt_bytes = self.check.rt_block(&self.rt_records).map_err(SnapshotError::EncodeIntegrity)?;
            let rt = RtLocator { seq_floor: self.rt_seq_floor, off: self.sink.position(), len: rt_bytes.len() as u64 };
            self.sink.write(&rt_bytes);
            (Some(rt), rt_bytes)
        } else if !self.rt_records.is_empty() {
            return Err(SnapshotError::EncodeIntegrity(
                self.check.rt_error("range tombstone records need run format v6"),
            ));
        } else {
            (None, Vec::new())
        };
        let index_off = self.sink.position();
        let idx = encode_index_bytes(&index, rt);
        self.check
            .verify_index(&idx, self.data_start, bloom_off, rt)
            .map_err(SnapshotError::EncodeIntegrity)?;
        self.sink.write(&idx);
        let sidecar_off = self.sink.position();
        let mut side_len = 0u64;
        if self.version <= RUN_SIDECAR_VERSION {
            let mut side_crc = crc::Crc32::new();
            let head = self.opts.num_buckets.to_le_bytes();
            side_crc.update(&head);
            self.sink.write(&head);
            side_len += head.len() as u64;
            if self.sink.err.is_none() {
                let mut rec = Vec::new();
                let mut i = 0usize;
                while i < index.len() {
                    let (j, w_off, w_end) = readback_window(&index, i);
                    let win = self.sink.read_logical(w_off, (w_end - w_off) as usize)?;
                    for k in i..j {
                        let (off, len) = index.block_ref(k);
                        let s = (off - w_off) as usize;
                        let entries = decode_block(&win[s..s + len as usize])
                            .map_err(SnapshotError::Corrupt)?;
                        for e in &entries {
                            encode_sidecar_record(&mut rec, &e.key, e.version);
                            side_crc.update(&rec);
                            self.sink.write(&rec);
                            side_len += rec.len() as u64;
                        }
                    }
                    i = j;
                }
            }
            let crc_bytes = side_crc.finalize().to_le_bytes();
            self.sink.write(&crc_bytes);
            side_len += crc_bytes.len() as u64;
        }
        let f = encode_footer(&FooterFields {
            entry_count: self.entry_count,
            num_blocks: index.len() as u64,
            index_off,
            index_len: idx.len() as u64,
            bloom_off,
            bloom_len: bloom_bytes.len() as u64,
            sidecar_off,
            sidecar_len: side_len,
            num_buckets: self.opts.num_buckets,
            version: self.version,
        });
        self.sink.write(&f);
        crate::attribution::run_parts(1, bloom_off - self.data_start,
            idx.len() as u64, bloom_bytes.len() as u64, f.len() as u64, side_len);
        drop(index);
        drop(self.bloom);
        drop(self.cur);
        let dev = self.sink.dev;
        let seg_lbas = self.sink.seg_lbas;
        let seg_bytes = self.sink.seg_bytes;
        let handle = self.sink.finish().await?;
        let seg_lbas = &seg_lbas[..handle.segs.len()];
        if !self.verify || self.version <= RUN_SIDECAR_VERSION {
            drop((f, bloom_bytes, idx, rt_bytes));
            let parts = read_run_parts(dev, &handle, seg_lbas, seg_bytes)?;
            return Ok((handle, parts, built));
        }
        let _verify_timer = crate::attribution::Timer::new(&crate::attribution::MERGE_VERIFY, None);
        let parts = verify_run_tail(dev, &handle, seg_lbas, seg_bytes, [f, bloom_bytes, idx, rt_bytes])?;
        verify_blocks(dev, seg_lbas, seg_bytes, &parts.index)?;
        Ok((handle, parts, built))
    }
}

fn verify_blocks<D: BlockDevice>(
    dev: &D,
    seg_lbas: &[u64],
    seg_bytes: usize,
    index: &FenceIndex,
) -> Result<(), SnapshotError> {
    let mut i = 0usize;
    while i < index.len() {
        let (j, w_off, w_end) = readback_window(index, i);
        let win = read_run_range_batched(dev, seg_lbas, seg_bytes, w_off, (w_end - w_off) as usize, READBACK_QD)?;
        for k in i..j {
            let (off, len) = index.block_ref(k);
            let s = (off - w_off) as usize;
            if !block_crc_ok(&win[s..s + len as usize]) {
                BUILD_VERIFY_FAILURES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err(SnapshotError::Corrupt("run block crc mismatch on build verify"));
            }
        }
        BUILD_VERIFY_BYTES.fetch_add(win.len() as u64, std::sync::atomic::Ordering::Relaxed);
        i = j;
    }
    Ok(())
}

fn verify_run_tail<D: BlockDevice>(
    dev: &D,
    handle: &RunHandle,
    seg_lbas: &[u64],
    seg_bytes: usize,
    expect: [Vec<u8>; 4],
) -> Result<RunParts, SnapshotError> {
    let parts = read_run_parts_inner(dev, handle, seg_lbas, seg_bytes, Some(expect));
    if matches!(parts, Err(SnapshotError::Corrupt(_))) {
        BUILD_VERIFY_FAILURES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    parts
}

fn readback_window(index: &FenceIndex, i: usize) -> (usize, u64, u64) {
    let w_off = index.block_ref(i).0;
    let mut j = i;
    let mut w_end = w_off;
    while j < index.len() {
        let (off, len) = index.block_ref(j);
        debug_assert_eq!(off, w_end, "streamed data blocks are contiguous");
        w_end = off + len as u64;
        j += 1;
        if (w_end - w_off) as usize >= SIDECAR_READBACK_WINDOW {
            break;
        }
    }
    (j, w_off, w_end)
}

pub fn write_run(
    dev: &impl BlockDevice,
    alloc: &mut Allocator,
    run_bytes: &[u8],
) -> Result<RunHandle, SnapshotError> {
    write_run_source(dev, alloc, run_bytes, 6)
}

pub(crate) fn write_run_source(
    dev: &impl BlockDevice,
    alloc: &mut Allocator,
    run_bytes: &[u8],
    source: usize,
) -> Result<RunHandle, SnapshotError> {
    let block_size = dev.block_size();
    let seg_bytes = alloc.blocks_per_segment() as usize * block_size;
    let segs = allocate_run_segs(alloc, block_size, run_bytes.len())?;
    for (i, seg) in segs.iter().enumerate() {
        let buf = run_seg_write_buf(run_bytes, i, seg_bytes, block_size);
        let (lba, _) = alloc.segment_range(*seg);
        crate::attribution::write_bytes(source, buf.len());
        dev.write_at(lba, &buf)?;
    }
    dev.flush()?;
    Ok(RunHandle {
        segs,
        len: run_bytes.len() as u64,
    })
}

fn read_run_range_core<D: BlockDevice>(
    dev: &D,
    seg_bytes: usize,
    off: u64,
    len: usize,
    resolve_seg_lba: impl Fn(usize) -> Option<u64>,
) -> Result<Vec<u8>, SnapshotError> {
    let block_size = dev.block_size();
    let mut out = Vec::with_capacity(len);
    let mut pos = off as usize;
    let mut remaining = len;
    while remaining > 0 {
        let seg_idx = pos / seg_bytes;
        let within = pos % seg_bytes;
        let seg_lba = resolve_seg_lba(seg_idx)
            .ok_or(SnapshotError::Corrupt("run offset past its segments"))?;
        let take = remaining.min(seg_bytes - within);
        let blk_start = within - (within % block_size);
        let blk_end = (within + take).div_ceil(block_size) * block_size;
        let mut buf = vec![0u8; blk_end - blk_start];
        let lba = seg_lba + (blk_start / block_size) as u64;
        dev.read_at(lba, &mut buf)?;
        let s = within - blk_start;
        out.extend_from_slice(&buf[s..s + take]);
        pos += take;
        remaining -= take;
    }
    Ok(out)
}

fn read_run_range(
    dev: &impl BlockDevice,
    alloc: &Allocator,
    segs: &[SegmentId],
    seg_bytes: usize,
    off: u64,
    len: usize,
) -> Result<Vec<u8>, SnapshotError> {
    read_run_range_core(dev, seg_bytes, off, len, |seg_idx| {
        segs.get(seg_idx).map(|s| alloc.segment_range(*s).0)
    })
}

fn read_run_range_lbas<D: BlockDevice>(
    dev: &D,
    seg_lbas: &[u64],
    seg_bytes: usize,
    off: u64,
    len: usize,
) -> Result<Vec<u8>, SnapshotError> {
    read_run_range_core(dev, seg_bytes, off, len, |seg_idx| {
        seg_lbas.get(seg_idx).copied()
    })
}

const RUN_READ_OP_MAX_BYTES: usize = 128 * 1024;

const SIDECAR_READBACK_WINDOW: usize = 1024 * 1024;

const READBACK_QD: usize = 8;

fn read_run_range_batched<D: BlockDevice>(
    dev: &D,
    seg_lbas: &[u64],
    seg_bytes: usize,
    off: u64,
    len: usize,
    qd: usize,
) -> Result<Vec<u8>, SnapshotError> {
    use vals_spdk::ReadOp;
    let block_size = dev.block_size();
    struct SpanBuf {
        lba: u64,
        buf: Vec<u8>,
        skip: usize,
        take: usize,
    }
    let mut spans: Vec<SpanBuf> = Vec::new();
    let mut pos = off as usize;
    let mut remaining = len;
    while remaining > 0 {
        let seg_idx = pos / seg_bytes;
        let within = pos % seg_bytes;
        let seg_lba = seg_lbas
            .get(seg_idx)
            .copied()
            .ok_or(SnapshotError::Corrupt("run offset past its segments"))?;
        let take = remaining.min(seg_bytes - within);
        let blk_start = within - (within % block_size);
        let blk_end = (within + take).div_ceil(block_size) * block_size;
        spans.push(SpanBuf {
            lba: seg_lba + (blk_start / block_size) as u64,
            buf: crate::vlog::io_buf(blk_end - blk_start),
            skip: within - blk_start,
            take,
        });
        pos += take;
        remaining -= take;
    }
    let total: usize = spans.iter().map(|s| s.buf.len()).sum();
    let chunk = (total / qd.max(1))
        .clamp(block_size, RUN_READ_OP_MAX_BYTES)
        .div_ceil(block_size)
        * block_size;
    let mut ops: Vec<ReadOp<'_>> = Vec::new();
    for s in &mut spans {
        let lba0 = s.lba;
        let mut done = 0usize;
        let mut rest: &mut [u8] = &mut s.buf;
        while !rest.is_empty() {
            let n = rest.len().min(chunk);
            let (head, tail) = rest.split_at_mut(n);
            ops.push(ReadOp {
                lba: lba0 + (done / block_size) as u64,
                buf: head,
            });
            done += n;
            rest = tail;
        }
    }
    dev.read_batch(&mut ops)?;
    drop(ops);
    let mut out = Vec::with_capacity(len);
    for s in &spans {
        out.extend_from_slice(&s.buf[s.skip..s.skip + s.take]);
    }
    Ok(out)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeLocator {
    seg_lbas: smallvec::SmallVec<[u64; 4]>,
    seg_bytes: usize,
    block_size: usize,
    off: u64,
    len: usize,
}

impl RangeLocator {
    pub fn read_bytes(&self) -> usize {
        let start = self.off as usize;
        (start + self.len).div_ceil(self.block_size) * self.block_size
            - start / self.block_size * self.block_size
    }

    pub async fn read<D: AsyncRead + BlockDevice>(
        &self,
        dev: &D,
    ) -> Result<Vec<u8>, SnapshotError> {
        self.read_range(dev, 0, self.len).await
    }

    pub(crate) async fn read_range<D: AsyncRead + BlockDevice>(
        &self,
        dev: &D,
        off: usize,
        len: usize,
    ) -> Result<Vec<u8>, SnapshotError> {
        let end = off
            .checked_add(len)
            .filter(|end| *end <= self.len)
            .ok_or(SnapshotError::Corrupt("run range past locator"))?;
        let block_size = self.block_size;
        let mut out = Vec::with_capacity(len);
        let mut pos = self.off as usize + off;
        let mut remaining = end - off;
        while remaining > 0 {
            let seg_idx = pos / self.seg_bytes;
            let within = pos % self.seg_bytes;
            let seg_lba = *self
                .seg_lbas
                .get(seg_idx)
                .ok_or(SnapshotError::Corrupt("run offset past its segments"))?;
            let take = remaining.min(self.seg_bytes - within);
            let blk_start = within - (within % block_size);
            let blk_end = (within + take).div_ceil(block_size) * block_size;
            let mut buf = vec![0u8; blk_end - blk_start];
            let lba = seg_lba + (blk_start / block_size) as u64;
            dev.read_async(lba, &mut buf)
                .await
                .map_err(SnapshotError::Io)?;
            let s = within - blk_start;
            out.extend_from_slice(&buf[s..s + take]);
            pos += take;
            remaining -= take;
        }
        Ok(out)
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

pub const SIDECAR_CURSOR_WINDOW: usize = 256 * 1024;

pub struct SidecarStream<D: BlockDevice> {
    dev: Arc<D>,
    loc: RangeLocator,
    body_len: usize,
    read_off: usize,
    buf: Vec<u8>,
    consumed: usize,
    crc: crc::Crc32,
    header_read: bool,
    previous: Option<Vec<u8>>,
}

impl<D: BlockDevice> SidecarStream<D> {
    fn new(dev: Arc<D>, loc: RangeLocator) -> Result<Self, SnapshotError> {
        let body_len = loc.len().checked_sub(4)
            .ok_or(SnapshotError::Corrupt("sidecar truncated"))?;
        Ok(Self {
            dev, loc, body_len, read_off: 0, buf: Vec::new(), consumed: 0,
            crc: crc::Crc32::new(), header_read: false, previous: None,
        })
    }

    pub fn resident_bound_bytes(&self) -> u64 {
        (SIDECAR_CURSOR_WINDOW.max(self.buf.capacity()) + self.previous.as_ref().map_or(0, Vec::len)) as u64
    }

    fn fill(&mut self, need: usize, reserve: &mut impl FnMut(u64) -> bool)
        -> Result<Option<bool>, SnapshotError> {
        let remaining = self.buf.len() - self.consumed;
        if remaining >= need { return Ok(Some(true)); }
        if self.read_off == self.body_len {
            return Err(SnapshotError::Corrupt("sidecar record truncated"));
        }
        let take = SIDECAR_CURSOR_WINDOW.max(need).saturating_sub(remaining)
            .min(self.body_len - self.read_off);
        if !reserve((take as u64).saturating_mul(2)) { return Ok(None); }
        if self.consumed != 0 {
            self.buf.drain(..self.consumed);
            self.consumed = 0;
        }
        self.buf.reserve_exact(take);
        let chunk = read_run_range_lbas(&*self.dev, &self.loc.seg_lbas,
            self.loc.seg_bytes, self.loc.off + self.read_off as u64, take)?;
        self.crc.update(&chunk);
        self.read_off += chunk.len();
        self.buf.extend_from_slice(&chunk);
        if self.read_off == self.body_len {
            let trailer = read_run_range_lbas(&*self.dev, &self.loc.seg_lbas,
                self.loc.seg_bytes, self.loc.off + self.body_len as u64, 4)?;
            if self.crc.clone().finalize() != u32::from_le_bytes(trailer.as_slice().try_into().unwrap()) {
                return Err(SnapshotError::Corrupt("sidecar crc mismatch"));
            }
        }
        Ok(Some(false))
    }

    pub fn next_step(&mut self) -> Result<SidecarCursorStep, SnapshotError> {
        Ok(self.next_step_reserving(|_| true)?.unwrap())
    }

    pub fn next_step_reserving(&mut self, mut reserve: impl FnMut(u64) -> bool)
        -> Result<Option<SidecarCursorStep>, SnapshotError> {
        if !self.header_read {
            let Some(ready) = self.fill(4, &mut reserve)? else { return Ok(None); };
            if !ready { return Ok(Some(SidecarCursorStep::Checkpoint)); }
            let buckets = u32::from_le_bytes(self.buf[self.consumed..self.consumed + 4].try_into().unwrap());
            if buckets == 0 { return Err(SnapshotError::Corrupt("sidecar has zero buckets")); }
            self.consumed += 4;
            self.header_read = true;
        }
        if self.buf.len() == self.consumed && self.read_off == self.body_len {
            return Ok(Some(SidecarCursorStep::Done));
        }
        let Some(ready) = self.fill(4, &mut reserve)? else { return Ok(None); };
        if !ready { return Ok(Some(SidecarCursorStep::Checkpoint)); }
        let kl = u32::from_le_bytes(self.buf[self.consumed..self.consumed + 4].try_into().unwrap()) as usize;
        let len = 4usize.checked_add(kl).and_then(|n| n.checked_add(16))
            .ok_or(SnapshotError::Corrupt("sidecar record length overflow"))?;
        if len > self.body_len - (self.read_off - (self.buf.len() - self.consumed)) {
            return Err(SnapshotError::Corrupt("sidecar record truncated"));
        }
        let Some(ready) = self.fill(len, &mut reserve)? else { return Ok(None); };
        if !ready { return Ok(Some(SidecarCursorStep::Checkpoint)); }
        if !reserve((kl as u64).saturating_mul(2)) { return Ok(None); }
        let start = self.consumed + 4;
        let end = start + kl;
        let key = self.buf[start..end].to_vec();
        if self.previous.as_ref().is_some_and(|p| *p >= key) {
            return Err(SnapshotError::Corrupt("sidecar keys not strictly increasing"));
        }
        let version = u128::from_le_bytes(self.buf[end..end + 16].try_into().unwrap());
        self.consumed += len;
        self.previous = Some(key.clone());
        Ok(Some(SidecarCursorStep::Entry((key, version))))
    }
}

pub enum SidecarCursorStep {
    Entry((Vec<u8>, u128)),
    Done,
    Checkpoint,
}

pub struct SidecarCursor {
    loc: RangeLocator,
    body_len: usize,
    read_off: usize,
    buf: Vec<u8>,
    consumed: usize,
    crc: crc::Crc32,
    header_read: bool,
    crc_verified: bool,
    last_key: Option<(usize, usize)>,
    scanned: Option<Vec<u8>>,
}

impl SidecarCursor {
    pub fn new(loc: RangeLocator) -> Result<Self, SnapshotError> {
        let body_len = loc
            .len
            .checked_sub(4)
            .ok_or(SnapshotError::Corrupt("sidecar truncated"))?;
        Ok(SidecarCursor {
            loc,
            body_len,
            read_off: 0,
            buf: Vec::with_capacity(SIDECAR_CURSOR_WINDOW),
            consumed: 0,
            crc: crc::Crc32::new(),
            header_read: false,
            crc_verified: false,
            last_key: None,
            scanned: None,
        })
    }

    pub fn scanned_through(&self) -> Option<&[u8]> {
        self.scanned.as_deref()
    }

    async fn fill<D: AsyncRead + BlockDevice>(
        &mut self,
        need: usize,
        dev: &D,
    ) -> Result<(bool, bool), SnapshotError> {
        let mut read = false;
        while self.buf.len().saturating_sub(self.consumed) < need {
            if self.read_off == self.body_len {
                return Ok((false, read));
            }
            if let Some((start, len)) = self.last_key.take() {
                let scanned = self.scanned.get_or_insert_with(Vec::new);
                scanned.clear();
                scanned.extend_from_slice(&self.buf[start..start + len]);
            }
            if self.consumed != 0 {
                self.buf.drain(..self.consumed);
                self.consumed = 0;
            }
            let available = self.buf.len();
            let target = SIDECAR_CURSOR_WINDOW.max(need);
            let take = target
                .saturating_sub(available)
                .min(self.body_len - self.read_off);
            let chunk = self.loc.read_range(dev, self.read_off, take).await?;
            self.crc.update(&chunk);
            self.read_off += chunk.len();
            self.buf.extend_from_slice(&chunk);
            read = true;
            vals_rt::yield_now().await;
            if self.read_off == self.body_len && !self.crc_verified {
                let trailer = self.loc.read_range(dev, self.body_len, 4).await?;
                let stored = u32::from_le_bytes(trailer.as_slice().try_into().unwrap());
                if self.crc.clone().finalize() != stored {
                    return Err(SnapshotError::Corrupt("sidecar crc mismatch"));
                }
                self.crc_verified = true;
            }
        }
        Ok((true, read))
    }

    pub async fn next_matching_step<D: AsyncRead + BlockDevice>(
        &mut self,
        dev: &D,
        want: &std::collections::HashSet<u32>,
        bucket_of: &impl Fn(&[u8]) -> u32,
        after: Option<&[u8]>,
    ) -> Result<SidecarCursorStep, SnapshotError> {
        if !self.header_read {
            let (ready, read) = self.fill(4, dev).await?;
            if !ready {
                return Err(SnapshotError::Corrupt("sidecar truncated"));
            }
            if read {
                return Ok(SidecarCursorStep::Checkpoint);
            }
            let n = u32::from_le_bytes(
                self.buf[self.consumed..self.consumed + 4]
                    .try_into()
                    .unwrap(),
            );
            if n == 0 {
                return Err(SnapshotError::Corrupt("sidecar has zero buckets"));
            }
            self.consumed += 4;
            self.header_read = true;
        }
        loop {
            let available = self.buf.len() - self.consumed;
            if available == 0 && self.read_off == self.body_len {
                return Ok(SidecarCursorStep::Done);
            }
            let (ready, read) = self.fill(4, dev).await?;
            if !ready {
                return Err(SnapshotError::Corrupt("sidecar record truncated"));
            }
            if read {
                return Ok(SidecarCursorStep::Checkpoint);
            }
            let kl = u32::from_le_bytes(
                self.buf[self.consumed..self.consumed + 4]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let record_len = 4usize
                .checked_add(kl)
                .and_then(|n| n.checked_add(16))
                .ok_or(SnapshotError::Corrupt("sidecar record length overflow"))?;
            let record_off = self.read_off - (self.buf.len() - self.consumed);
            if record_len > self.body_len - record_off {
                return Err(SnapshotError::Corrupt("sidecar record truncated"));
            }
            let (ready, read) = self.fill(record_len, dev).await?;
            if !ready {
                return Err(SnapshotError::Corrupt("sidecar record truncated"));
            }
            if read {
                return Ok(SidecarCursorStep::Checkpoint);
            }
            let key_start = self.consumed + 4;
            let key_end = key_start + kl;
            let key = &self.buf[key_start..key_end];
            let take = after.is_none_or(|a| key > a) && want.contains(&bucket_of(key));
            let version = u128::from_le_bytes(
                self.buf[key_end..key_end + 16].try_into().unwrap(),
            );
            self.consumed += record_len;
            self.last_key = Some((key_start, kl));
            if take {
                return Ok(SidecarCursorStep::Entry((key.to_vec(), version)));
            }
        }
    }
}

enum Walk {
    Entry(u128),
    Window,
    Done,
    Blocked,
}

struct BlockWalker {
    blocks: Arc<FenceIndex>,
    next_block: usize,
    win: Vec<u8>,
    win_off: u64,
    win_end_block: usize,
    cur_block: usize,
    pos: usize,
    body_end: usize,
    left: u32,
    in_block: bool,
    first_in_block: bool,
    key: Vec<u8>,
    has_key: bool,
}

impl BlockWalker {
    fn new(blocks: Arc<FenceIndex>, start_block: usize) -> Self {
        let start = start_block.min(blocks.len());
        BlockWalker {
            blocks,
            next_block: start,
            win: Vec::new(),
            win_off: 0,
            win_end_block: start,
            cur_block: start,
            pos: 0,
            body_end: 0,
            left: 0,
            in_block: false,
            first_in_block: false,
            key: Vec::new(),
            has_key: false,
        }
    }

    fn window(&self) -> Option<(u64, usize, usize)> {
        let first = self.next_block;
        let start = *self.blocks.get(first)?.1;
        let mut end = start + self.blocks.block_ref(first).1 as u64;
        let mut last = first + 1;
        while let Some((_, off, len)) = self.blocks.get(last) {
            if *off != end || (end - start) as usize + *len as usize > SIDECAR_CURSOR_WINDOW {
                break;
            }
            end = off + *len as u64;
            last += 1;
        }
        Some((start, (end - start) as usize, last))
    }

    fn install(&mut self, bytes: Vec<u8>, off: u64, end_block: usize) {
        self.win = bytes;
        self.win_off = off;
        self.cur_block = self.next_block;
        self.win_end_block = end_block;
        self.next_block = end_block;
        self.in_block = false;
    }

    fn advance(&mut self, reserve: &mut impl FnMut(u64) -> bool) -> Result<Walk, SnapshotError> {
        loop {
            if !self.in_block {
                if self.cur_block >= self.win_end_block {
                    return Ok(if self.next_block >= self.blocks.len() {
                        Walk::Done
                    } else {
                        Walk::Window
                    });
                }
                let (_, off, len) = self.blocks.get(self.cur_block).unwrap();
                let s = off
                    .checked_sub(self.win_off)
                    .map(|s| s as usize)
                    .filter(|s| s + *len as usize <= self.win.len())
                    .ok_or(SnapshotError::Corrupt("run block outside its read window"))?;
                let block = &self.win[s..s + *len as usize];
                if block.len() < 8 {
                    return Err(SnapshotError::Corrupt("run block truncated"));
                }
                if !block_crc_ok(block) {
                    return Err(SnapshotError::Corrupt("run block crc mismatch"));
                }
                self.left = u32::from_le_bytes(block[..4].try_into().unwrap());
                self.pos = s + 4;
                self.body_end = s + block.len() - 4;
                self.in_block = true;
                self.first_in_block = true;
            }
            if self.left == 0 {
                if self.pos != self.body_end {
                    return Err(SnapshotError::Corrupt("run block has trailing bytes"));
                }
                self.in_block = false;
                self.cur_block += 1;
                continue;
            }
            let mut c = Cursor { b: &self.win[..self.body_end], off: self.pos };
            let shared = c.u32().map_err(SnapshotError::Corrupt)? as usize;
            let suffix_len = (c.u32().map_err(SnapshotError::Corrupt)? & !SUFFIX_TOMB_BIT) as usize;
            let base = if self.first_in_block { 0 } else { self.key.len() };
            if shared > base {
                return Err(SnapshotError::Corrupt("shared prefix longer than previous key"));
            }
            let suffix_start = c.off;
            c.take(suffix_len).map_err(SnapshotError::Corrupt)?;
            c.take(24).map_err(SnapshotError::Corrupt)?;
            let version = c.u128().map_err(SnapshotError::Corrupt)?;
            let next = c.off;
            if !reserve(((shared + suffix_len) as u64).saturating_mul(2)) {
                return Ok(Walk::Blocked);
            }
            let suffix = &self.win[suffix_start..suffix_start + suffix_len];
            if self.has_key && suffix <= &self.key[shared..] {
                return Err(SnapshotError::Corrupt("run keys not strictly increasing"));
            }
            self.key.truncate(shared);
            self.key.extend_from_slice(suffix);
            self.has_key = true;
            self.first_in_block = false;
            self.pos = next;
            self.left -= 1;
            return Ok(Walk::Entry(version));
        }
    }
}

pub struct BlockVersionStream<D: BlockDevice> {
    dev: Arc<D>,
    seg_lbas: Vec<u64>,
    seg_bytes: usize,
    num_buckets: u32,
    walker: BlockWalker,
}

impl<D: BlockDevice> BlockVersionStream<D> {
    pub fn resident_bound_bytes(&self) -> u64 {
        (SIDECAR_CURSOR_WINDOW.max(self.walker.win.capacity()) + self.walker.key.capacity()) as u64
    }

    pub fn next_step_reserving(&mut self, mut reserve: impl FnMut(u64) -> bool)
        -> Result<Option<SidecarCursorStep>, SnapshotError> {
        if self.num_buckets == 0 {
            return Err(SnapshotError::Corrupt("run has zero buckets"));
        }
        match self.walker.advance(&mut reserve)? {
            Walk::Entry(version) => Ok(Some(SidecarCursorStep::Entry((self.walker.key.clone(), version)))),
            Walk::Blocked => Ok(None),
            Walk::Done => Ok(Some(SidecarCursorStep::Done)),
            Walk::Window => {
                let (off, len, end) = self.walker.window().expect("a pending window has a block");
                if !reserve((len as u64).saturating_mul(2)) {
                    return Ok(None);
                }
                self.walker.win = Vec::new();
                let bytes = read_run_range_lbas(&*self.dev, &self.seg_lbas, self.seg_bytes, off, len)?;
                self.walker.install(bytes, off, end);
                Ok(Some(SidecarCursorStep::Checkpoint))
            }
        }
    }
}

pub struct RunStreamSource<D: BlockDevice> {
    dev: Arc<D>,
    version: u16,
    seg_lbas: Vec<u64>,
    seg_bytes: usize,
    num_buckets: u32,
    index: Option<Arc<FenceIndex>>,
    sidecar: Option<RangeLocator>,
}

impl<D: BlockDevice> RunStreamSource<D> {
    pub fn version_stream(&self) -> Result<Option<RunVersionStream<D>>, SnapshotError> {
        self.version_stream_on(self.dev.clone())
    }

    pub(crate) fn streamed_bytes(&self) -> u64 {
        match (&self.sidecar, &self.index) {
            (Some(loc), _) => loc.len() as u64,
            (None, Some(index)) => index.last().map_or(0, |(_, off, len)| off + *len as u64),
            (None, None) => 0,
        }
    }

    pub(crate) fn version_stream_on(&self, dev: Arc<D>) -> Result<Option<RunVersionStream<D>>, SnapshotError> {
        if self.version < 3 {
            return Ok(None);
        }
        match (&self.sidecar, &self.index) {
            (Some(loc), _) => Ok(Some(RunVersionStream::Sidecar(SidecarStream::new(
                dev,
                loc.clone(),
            )?))),
            (None, Some(index)) => Ok(Some(RunVersionStream::Blocks(BlockVersionStream {
                dev,
                seg_lbas: self.seg_lbas.clone(),
                seg_bytes: self.seg_bytes,
                num_buckets: self.num_buckets,
                walker: BlockWalker::new(index.clone(), 0),
            }))),
            (None, None) => Err(SnapshotError::Corrupt("run stream source has no block index")),
        }
    }

    pub fn sidecar_versions(&self) -> Result<SidecarVersions, SnapshotError> {
        let Some(loc) = &self.sidecar else {
            return Err(SnapshotError::Corrupt("run has no sidecar"));
        };
        let side = read_run_range_lbas(&*self.dev, &loc.seg_lbas, loc.seg_bytes, loc.off, loc.len)?;
        SidecarVersions::from_sidecar(self.version, side)
    }
}

pub enum RunVersionStream<D: BlockDevice> {
    Sidecar(SidecarStream<D>),
    Blocks(BlockVersionStream<D>),
}

impl<D: BlockDevice> RunVersionStream<D> {
    pub fn resident_bound_bytes(&self) -> u64 {
        match self {
            RunVersionStream::Sidecar(s) => s.resident_bound_bytes(),
            RunVersionStream::Blocks(s) => s.resident_bound_bytes(),
        }
    }

    pub fn next_step(&mut self) -> Result<SidecarCursorStep, SnapshotError> {
        Ok(self.next_step_reserving(|_| true)?.unwrap())
    }

    pub fn next_step_reserving(&mut self, reserve: impl FnMut(u64) -> bool)
        -> Result<Option<SidecarCursorStep>, SnapshotError> {
        match self {
            RunVersionStream::Sidecar(s) => s.next_step_reserving(reserve),
            RunVersionStream::Blocks(s) => s.next_step_reserving(reserve),
        }
    }
}

pub struct BlockVersionCursor {
    loc: RangeLocator,
    base: u64,
    num_buckets: u32,
    max_block: usize,
    walker: BlockWalker,
}

impl BlockVersionCursor {
    pub fn scanned_through(&self) -> Option<&[u8]> {
        self.walker.has_key.then_some(self.walker.key.as_slice())
    }

    pub fn resident_bound_bytes(&self) -> usize {
        SIDECAR_CURSOR_WINDOW.max(self.max_block).saturating_mul(2).saturating_add(self.max_block)
    }

    pub async fn next_matching_step<D: AsyncRead + BlockDevice>(
        &mut self,
        dev: &D,
        want: &std::collections::HashSet<u32>,
        bucket_of: &impl Fn(&[u8]) -> u32,
        after: Option<&[u8]>,
    ) -> Result<SidecarCursorStep, SnapshotError> {
        if self.num_buckets == 0 {
            return Err(SnapshotError::Corrupt("run has zero buckets"));
        }
        loop {
            match self.walker.advance(&mut |_| true)? {
                Walk::Entry(version) => {
                    let key = self.walker.key.as_slice();
                    if after.is_none_or(|a| key > a) && want.contains(&bucket_of(key)) {
                        return Ok(SidecarCursorStep::Entry((key.to_vec(), version)));
                    }
                }
                Walk::Done => return Ok(SidecarCursorStep::Done),
                Walk::Blocked => unreachable!("an unbounded reserve never blocks"),
                Walk::Window => {
                    let (off, len, end) = self.walker.window().expect("a pending window has a block");
                    self.walker.win = Vec::new();
                    let bytes = self.loc.read_range(dev, (off - self.base) as usize, len).await?;
                    self.walker.install(bytes, off, end);
                    vals_rt::yield_now().await;
                    return Ok(SidecarCursorStep::Checkpoint);
                }
            }
        }
    }
}

pub enum VersionSource {
    Sidecar { loc: RangeLocator, version: u16 },
    Blocks(BlockVersionCursor),
}

impl VersionSource {
    pub fn resident_bound_bytes(&self) -> usize {
        match self {
            VersionSource::Sidecar { loc, version } => {
                if *version >= 3 {
                    SIDECAR_CURSOR_WINDOW.saturating_mul(2)
                } else {
                    loc.len()
                }
            }
            VersionSource::Blocks(cursor) => cursor.resident_bound_bytes(),
        }
    }

    pub fn stored_len(&self) -> usize {
        match self {
            VersionSource::Sidecar { loc, .. } => loc.len(),
            VersionSource::Blocks(cursor) => cursor.loc.len(),
        }
    }
}

pub fn parse_want_bucket_digests(
    side: &[u8],
    version: u16,
    want: &std::collections::HashSet<u32>,
    bucket_of: impl Fn(&[u8]) -> u32,
) -> Result<Vec<Digest>, SnapshotError> {
    if want.is_empty() {
        return Ok(Vec::new());
    }
    let body = sidecar_body(side).map_err(SnapshotError::Corrupt)?;
    let mut c = Cursor { b: body, off: 0 };
    let n = c.u32().map_err(SnapshotError::Corrupt)?;
    let mut out = Vec::new();
    if version >= 3 {
        while c.off < body.len() {
            let kl = c.u32().map_err(SnapshotError::Corrupt)? as usize;
            let k = c.take(kl).map_err(SnapshotError::Corrupt)?;
            if want.contains(&(bucket_of(k) % n.max(1))) {
                let key = k.to_vec();
                let v = c.take(16).map_err(SnapshotError::Corrupt)?.to_vec();
                out.push((key, v));
            } else {
                c.take(16).map_err(SnapshotError::Corrupt)?;
            }
        }
        return Ok(out);
    }
    for b in 0..n {
        let count = c.u32().map_err(SnapshotError::Corrupt)? as usize;
        let take = want.contains(&b);
        for _ in 0..count {
            let kl = c.u32().map_err(SnapshotError::Corrupt)? as usize;
            if take {
                let k = c.take(kl).map_err(SnapshotError::Corrupt)?.to_vec();
                let v = c.take(16).map_err(SnapshotError::Corrupt)?.to_vec();
                out.push((k, v));
            } else {
                c.take(kl).map_err(SnapshotError::Corrupt)?;
                c.take(16).map_err(SnapshotError::Corrupt)?;
            }
        }
    }
    out.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

pub fn parse_want_bucket_digests_page(
    side: &[u8],
    version: u16,
    want: &std::collections::HashSet<u32>,
    bucket_of: impl Fn(&[u8]) -> u32,
    after: Option<&[u8]>,
    max_bytes: usize,
    per_entry_overhead: usize,
) -> Result<(Vec<Digest>, Option<Vec<u8>>), SnapshotError> {
    if version < 3 {
        let mut all = parse_want_bucket_digests(side, version, want, &bucket_of)?;
        all.retain(|(key, _)| {
            want.contains(&bucket_of(key)) && after.is_none_or(|a| key.as_slice() > a)
        });
        let mut bytes = 0usize;
        let mut seen = false;
        let mut stop = None;
        let keep = all
            .iter()
            .position(|(key, _)| {
                let size = key.len().saturating_add(per_entry_overhead);
                if seen && bytes.saturating_add(size) > max_bytes {
                    stop = Some(key.clone());
                    true
                } else {
                    seen = true;
                    bytes = bytes.saturating_add(size);
                    false
                }
            })
            .unwrap_or(all.len());
        all.truncate(keep);
        return Ok((all, stop));
    }
    let body = sidecar_body(side).map_err(SnapshotError::Corrupt)?;
    let mut c = Cursor { b: body, off: 0 };
    let n = c.u32().map_err(SnapshotError::Corrupt)?;
    let mut out = Vec::new();
    let mut bytes = 0usize;
    while c.off < body.len() {
        let kl = c.u32().map_err(SnapshotError::Corrupt)? as usize;
        let key = c.take(kl).map_err(SnapshotError::Corrupt)?;
        let version = c.take(16).map_err(SnapshotError::Corrupt)?;
        if !want.contains(&(bucket_of(key) % n.max(1)))
            || after.is_some_and(|a| key <= a)
        {
            continue;
        }
        let size = key.len().saturating_add(per_entry_overhead);
        if !out.is_empty() && bytes.saturating_add(size) > max_bytes {
            return Ok((out, Some(key.to_vec())));
        }
        bytes = bytes.saturating_add(size);
        out.push((key.to_vec(), version.to_vec()));
    }
    Ok((out, None))
}

pub struct RunParts {
    index: FenceIndex,
    bloom: RunFilter,
    entry_count: u64,
    num_buckets: u32,
    sidecar_off: u64,
    sidecar_len: u64,
    version: u16,
    rt: Option<RtLocator>,
}

fn check_rt_locator(rt: Option<RtLocator>, ft: &Footer) -> Result<(), SnapshotError> {
    let filter_end = ft.bloom_off.saturating_add(ft.bloom_len);
    match rt {
        Some(rt) if rt.off < filter_end || rt.off.checked_add(rt.len).is_none_or(|end| end > ft.index_off) => {
            Err(SnapshotError::Corrupt("range tombstone block outside the run"))
        }
        _ => Ok(()),
    }
}

pub(crate) fn read_run_range_tombstones(
    dev: &impl BlockDevice,
    alloc: &Allocator,
    handle: &RunHandle,
    mut pace: impl FnMut(usize),
) -> Result<Vec<RtRecord>, SnapshotError> {
    const CHUNK: usize = 1 << 20;
    let seg_bytes = alloc.blocks_per_segment() as usize * dev.block_size();
    if handle.len < FOOTER_LEN as u64 {
        return Err(SnapshotError::Corrupt("run shorter than its footer"));
    }
    pace(FOOTER_LEN);
    let footer = read_run_range(dev, alloc, &handle.segs, seg_bytes, handle.len - FOOTER_LEN as u64, FOOTER_LEN)?;
    let ft = decode_footer(&footer).map_err(SnapshotError::Corrupt)?;
    if !version_supports_range_tombstones(ft.version) {
        return Ok(Vec::new());
    }
    let index_len = usize::try_from(ft.index_len).map_err(|_| SnapshotError::Corrupt("truncated"))?;
    if index_len < INDEX_RT_TRAILER_LEN + 4 {
        return Err(SnapshotError::Corrupt("truncated"));
    }
    let body_len = index_len - 4;
    let trailer_at = body_len - INDEX_RT_TRAILER_LEN;
    let mut crc = crc::Crc32::new();
    let mut trailer = Vec::with_capacity(INDEX_RT_TRAILER_LEN + 4);
    let mut off = 0usize;
    while off < index_len {
        let n = (index_len - off).min(CHUNK);
        pace(n);
        let chunk = read_run_range(dev, alloc, &handle.segs, seg_bytes, ft.index_off + off as u64, n)?;
        crc.update(&chunk[..body_len.saturating_sub(off).min(n)]);
        if off + n > trailer_at {
            trailer.extend_from_slice(&chunk[trailer_at.saturating_sub(off)..]);
        }
        off += n;
    }
    let field = |i: usize| u64::from_le_bytes(trailer[i * 8..i * 8 + 8].try_into().unwrap());
    if crc.finalize() != u32::from_le_bytes(trailer[INDEX_RT_TRAILER_LEN..].try_into().unwrap()) {
        return Err(SnapshotError::Corrupt("index crc mismatch"));
    }
    let rt = RtLocator { seq_floor: field(0), off: field(1), len: field(2) };
    check_rt_locator(Some(rt), &ft)?;
    if rt.len == 0 {
        return Ok(Vec::new());
    }
    let len = usize::try_from(rt.len).map_err(|_| SnapshotError::Corrupt("range tombstone block outside the run"))?;
    pace(len);
    let bytes = read_run_range(dev, alloc, &handle.segs, seg_bytes, rt.off, len)?;
    decode_rt_block(&bytes).map_err(SnapshotError::Corrupt)
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RunResidentStats {
    pub total_bytes: u64,
    pub fence_bytes: u64,
    pub fence_payload_bytes: u64,
    pub filter_bytes: u64,
    pub filter_payload_bytes: u64,
}

fn resident_stats(index: &FenceIndex, bloom: &RunFilter, base: usize) -> RunResidentStats {
    let fence_bytes = index.allocated_bytes();
    let fence_payload_bytes = fence_bytes;
    let (capacity, len) = match bloom {
        RunFilter::Classic(bloom) => (bloom.bits.capacity(), bloom.bits.len()),
        RunFilter::Blocked(bloom) => (bloom.bits.capacity(), bloom.bits.len()),
        RunFilter::Unfiltered => (0, 0),
    };
    let filter_bytes = capacity * std::mem::size_of::<u64>();
    RunResidentStats {
        total_bytes: (base + fence_bytes + filter_bytes) as u64,
        fence_bytes: fence_bytes as u64,
        fence_payload_bytes: fence_payload_bytes as u64,
        filter_bytes: filter_bytes as u64,
        filter_payload_bytes: (len * std::mem::size_of::<u64>()) as u64,
    }
}

impl RunParts {
    pub(crate) fn estimated_resident_bytes(&self) -> usize {
        resident_stats(&self.index, &self.bloom, std::mem::size_of::<Self>())
            .total_bytes as usize
    }
}

#[derive(Debug, PartialEq, Eq)]
struct IndexCrc {
    stored: u32,
    computed: u32,
}

impl IndexCrc {
    fn of(bytes: &[u8]) -> Self {
        let end = bytes.len() - 4;
        IndexCrc {
            stored: u32::from_le_bytes(bytes[end..].try_into().unwrap()),
            computed: crc::crc32(&bytes[..end]),
        }
    }

    fn passes(&self) -> bool {
        self.stored == self.computed
    }
}

#[derive(Debug, PartialEq, Eq)]
struct IndexDiffLocation {
    run_off: u64,
    seg_index: usize,
    lba: u64,
    byte_in_block: usize,
}

#[derive(Debug, PartialEq, Eq)]
struct IndexDiff {
    bytes: usize,
    first: Option<IndexDiffLocation>,
    last: Option<IndexDiffLocation>,
    first_read_all_zero: bool,
    span_4k_aligned: bool,
    span_128k_aligned: bool,
}

#[derive(Debug)]
struct IndexCrcEvidence {
    first_crc: IndexCrc,
    reread_crc: [Result<IndexCrc, String>; 2],
    verdict: &'static str,
    diff: Option<IndexDiff>,
}

fn inspect_index_crc<D: BlockDevice>(
    dev: &D,
    seg_lbas: &[u64],
    seg_bytes: usize,
    ft: &Footer,
    first: &[u8],
) -> IndexCrcEvidence {
    let rereads = [
        read_run_range_lbas(dev, seg_lbas, seg_bytes, ft.index_off, ft.index_len as usize),
        read_run_range_lbas(dev, seg_lbas, seg_bytes, ft.index_off, ft.index_len as usize),
    ];
    let reread_crc = rereads.each_ref().map(|r| {
        r.as_ref().map(|b| IndexCrc::of(b)).map_err(|e| e.to_string())
    });
    let verdict = if reread_crc.iter().any(|r| r.as_ref().is_ok_and(IndexCrc::passes)) {
        "transient"
    } else {
        match (&rereads[0], &rereads[1]) {
            (Ok(a), Ok(b)) if a == b => "persistent",
            (Ok(_), Ok(_)) => "unstable",
            _ => "unavailable",
        }
    };
    let diff = rereads[0].as_ref().ok().map(|reread| {
        let mut bytes = 0;
        let mut first_off = None;
        let mut last_off = None;
        let mut all_zero = true;
        for (i, (&a, &b)) in first.iter().zip(reread).enumerate() {
            if a != b {
                bytes += 1;
                first_off.get_or_insert(ft.index_off + i as u64);
                last_off = Some(ft.index_off + i as u64);
                all_zero &= a == 0;
            }
        }
        let locate = |run_off: u64| {
            let seg_index = run_off as usize / seg_bytes;
            let within = run_off as usize % seg_bytes;
            IndexDiffLocation {
                run_off,
                seg_index,
                lba: seg_lbas[seg_index] + (within / dev.block_size()) as u64,
                byte_in_block: within % dev.block_size(),
            }
        };
        let aligned = |size| match (first_off, last_off) {
            (Some(a), Some(b)) => a % size == 0 && (b + 1) % size == 0,
            _ => false,
        };
        IndexDiff {
            bytes,
            first: first_off.map(locate),
            last: last_off.map(locate),
            first_read_all_zero: bytes > 0 && all_zero,
            span_4k_aligned: aligned(4096),
            span_128k_aligned: aligned(128 * 1024),
        }
    });
    IndexCrcEvidence { first_crc: IndexCrc::of(first), reread_crc, verdict, diff }
}

struct IndexCrcWarning<'a> {
    site: &'static str,
    handle: &'a RunHandle,
    seg_lbas: &'a [u64],
    seg_bytes: usize,
    block_size: usize,
    device_key: Option<vals_spdk::MaintenanceCacheKey>,
    ft: &'a Footer,
    evidence: &'a IndexCrcEvidence,
}

impl std::fmt::Display for IndexCrcWarning<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WARN run index crc mismatch site={} len={} segments(id,lba)=[",
            self.site, self.handle.len)?;
        for (i, (seg, lba)) in self.handle.segs.iter().zip(self.seg_lbas).enumerate() {
            if i > 0 { write!(f, ",")?; }
            write!(f, "({},{})", seg.0, lba)?;
        }
        write!(f, "] lba_space=block_device device_key={:?} seg_bytes={} block_size={} index_off={} index_len={} bloom_off={} bloom_len={} first_crc={:?} reread_crc={:?} verdict={} diff={:?}",
            self.device_key, self.seg_bytes, self.block_size, self.ft.index_off, self.ft.index_len,
            self.ft.bloom_off, self.ft.bloom_len, self.evidence.first_crc,
            self.evidence.reread_crc, self.evidence.verdict, self.evidence.diff)
    }
}

fn warn_index_crc<D: BlockDevice>(
    site: &'static str,
    dev: &D,
    handle: &RunHandle,
    seg_lbas: &[u64],
    seg_bytes: usize,
    ft: &Footer,
    first: &[u8],
) {
    static WARNINGS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    if WARNINGS.fetch_add(1, std::sync::atomic::Ordering::Relaxed) >= 32 {
        return;
    }
    let evidence = inspect_index_crc(dev, seg_lbas, seg_bytes, ft, first);
    eprintln!("{}", IndexCrcWarning {
        site, handle, seg_lbas, seg_bytes, block_size: dev.block_size(),
        device_key: dev.maintenance_sibling_cache_key(), ft, evidence: &evidence,
    });
}

pub fn read_run_parts<D: BlockDevice>(
    dev: &D,
    handle: &RunHandle,
    seg_lbas: &[u64],
    seg_bytes: usize,
) -> Result<RunParts, SnapshotError> {
    read_run_parts_inner(dev, handle, seg_lbas, seg_bytes, None)
}

fn read_run_parts_inner<D: BlockDevice>(
    dev: &D,
    handle: &RunHandle,
    seg_lbas: &[u64],
    seg_bytes: usize,
    mut expect: Option<[Vec<u8>; 4]>,
) -> Result<RunParts, SnapshotError> {
    let verifying = expect.is_some();
    let mut same = |i: usize, read: &[u8]| {
        let Some(exp) = expect.as_mut() else {
            return Ok(());
        };
        if std::mem::take(&mut exp[i]).as_slice() != read {
            return Err(SnapshotError::Corrupt("run tail differs from its encoded bytes on build verify"));
        }
        BUILD_VERIFY_BYTES.fetch_add(read.len() as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    };
    if handle.len < FOOTER_LEN as u64 {
        return Err(SnapshotError::Corrupt("run shorter than its footer"));
    }
    let footer_bytes = read_run_range_lbas(
        dev,
        seg_lbas,
        seg_bytes,
        handle.len - FOOTER_LEN as u64,
        FOOTER_LEN,
    )?;
    let footer_same = same(0, &footer_bytes);
    let ft = decode_footer(&footer_bytes).map_err(SnapshotError::Corrupt)?;
    footer_same?;
    let bloom_bytes = read_run_range_lbas(
        dev,
        seg_lbas,
        seg_bytes,
        ft.bloom_off,
        ft.bloom_len as usize,
    )?;
    let bloom_same = same(1, &bloom_bytes);
    let bloom = RunFilter::decode(ft.version, &bloom_bytes).map_err(SnapshotError::Corrupt)?;
    bloom_same?;
    drop(bloom_bytes);
    let index_bytes = read_run_range_lbas(
        dev,
        seg_lbas,
        seg_bytes,
        ft.index_off,
        ft.index_len as usize,
    )?;
    let index_same = same(2, &index_bytes);
    let (index, rt) = decode_index(&index_bytes, ft.version).map_err(|e| {
        if e == "index crc mismatch" {
            warn_index_crc("readback", dev, handle, seg_lbas, seg_bytes, &ft, &index_bytes);
        }
        SnapshotError::Corrupt(e)
    })?;
    index_same?;
    check_rt_locator(rt, &ft)?;
    if verifying {
        let rt_bytes = match rt {
            Some(rt) if rt.len > 0 => read_run_range_lbas(dev, seg_lbas, seg_bytes, rt.off, rt.len as usize)?,
            _ => Vec::new(),
        };
        same(3, &rt_bytes)?;
    }
    Ok(RunParts {
        index,
        bloom,
        entry_count: ft.entry_count,
        num_buckets: ft.num_buckets,
        sidecar_off: ft.sidecar_off,
        sidecar_len: ft.sidecar_len,
        version: ft.version,
        rt,
    })
}

enum KeySourceKind {
    Sidecar {
        off: u64,
        body_len: u64,
        pos: u64,
        carry: Vec<u8>,
        crc: crc::Crc32,
        header_read: bool,
        max_key: usize,
    },
    Blocks {
        blocks: Vec<(u64, u32)>,
        next: usize,
    },
}

pub struct RunKeySource {
    seg_lbas: Vec<u64>,
    seg_bytes: usize,
    kind: KeySourceKind,
    bytes_read: u64,
}

impl RunKeySource {
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    #[cfg(test)]
    pub(crate) fn first_sidecar_record_location(&self, block_size: usize) -> Option<(u64, usize)> {
        let KeySourceKind::Sidecar { off, .. } = &self.kind else {
            return None;
        };
        let logical = *off + 4;
        let seg = (logical / self.seg_bytes as u64) as usize;
        let within = (logical % self.seg_bytes as u64) as usize;
        Some((self.seg_lbas[seg] + (within / block_size) as u64, within % block_size))
    }

    pub fn step<D: BlockDevice>(
        &mut self,
        dev: &D,
        max_bytes: usize,
        mut f: impl FnMut(&[u8]),
    ) -> Result<bool, SnapshotError> {
        let max_bytes = max_bytes.max(1);
        match &mut self.kind {
            KeySourceKind::Sidecar { off, body_len, pos, carry, crc, header_read, max_key } => {
                if *pos < *body_len {
                    let take = (max_bytes as u64).min(*body_len - *pos) as usize;
                    let chunk = read_run_range_lbas(dev, &self.seg_lbas, self.seg_bytes, *off + *pos, take)?;
                    crc.update(&chunk);
                    *pos += chunk.len() as u64;
                    self.bytes_read += chunk.len() as u64;
                    carry.extend_from_slice(&chunk);
                }
                let mut consumed = 0usize;
                if !*header_read && carry.len() >= 4 {
                    if u32::from_le_bytes(carry[..4].try_into().unwrap()) == 0 {
                        return Err(SnapshotError::Corrupt("sidecar has zero buckets"));
                    }
                    consumed = 4;
                    *header_read = true;
                }
                if *header_read {
                    while carry.len() - consumed >= 4 {
                        let kl = u32::from_le_bytes(carry[consumed..consumed + 4].try_into().unwrap()) as usize;
                        if kl > *max_key {
                            return Err(SnapshotError::Corrupt("sidecar record key longer than any run block"));
                        }
                        let len = 4 + kl + 16;
                        if (len as u64) > (carry.len() - consumed) as u64 + (*body_len - *pos) {
                            return Err(SnapshotError::Corrupt("sidecar record truncated"));
                        }
                        if carry.len() - consumed < len {
                            break;
                        }
                        f(&carry[consumed + 4..consumed + 4 + kl]);
                        consumed += len;
                    }
                }
                carry.drain(..consumed);
                if *pos < *body_len {
                    return Ok(false);
                }
                if !carry.is_empty() || !*header_read {
                    return Err(SnapshotError::Corrupt("sidecar record truncated"));
                }
                let trailer = read_run_range_lbas(dev, &self.seg_lbas, self.seg_bytes, *off + *body_len, 4)?;
                self.bytes_read += 4;
                if crc.clone().finalize() != u32::from_le_bytes(trailer.as_slice().try_into().unwrap()) {
                    return Err(SnapshotError::Corrupt("sidecar crc mismatch"));
                }
                Ok(true)
            }
            KeySourceKind::Blocks { blocks, next } => {
                if *next >= blocks.len() {
                    return Ok(true);
                }
                let first = *next;
                let start = blocks[first].0;
                let mut end = start;
                let mut last = first;
                while last < blocks.len() && blocks[last].0 == end && (last == first || ((end - start) as usize) < max_bytes) {
                    end = blocks[last].0 + blocks[last].1 as u64;
                    last += 1;
                }
                let win = read_run_range_lbas(dev, &self.seg_lbas, self.seg_bytes, start, (end - start) as usize)?;
                self.bytes_read += win.len() as u64;
                for &(off, len) in &blocks[first..last] {
                    let s = (off - start) as usize;
                    let entries = decode_block(&win[s..s + len as usize]).map_err(SnapshotError::Corrupt)?;
                    for e in &entries {
                        f(&e.key);
                    }
                }
                *next = last;
                Ok(*next >= blocks.len())
            }
        }
    }
}

pub struct DeviceRun<D: BlockDevice> {
    dev: Arc<D>,
    handle: RunHandle,
    seg_bytes: usize,
    index: Arc<FenceIndex>,
    max_block: usize,
    bloom: RunFilter,
    entry_count: u64,
    num_buckets: u32,
    sidecar_off: u64,
    sidecar_len: u64,
    version: u16,
    rt: Option<RtLocator>,
}

fn max_block_len(index: &FenceIndex) -> usize {
    index.max_block_len()
}

impl<D: BlockDevice> DeviceRun<D> {
    pub fn open(dev: Arc<D>, alloc: &Allocator, handle: RunHandle) -> Result<Self, SnapshotError> {
        Self::open_inner(dev, alloc, handle, true)
    }

    pub(crate) fn open_scan(dev: Arc<D>, alloc: &Allocator, handle: RunHandle) -> Result<Self, SnapshotError> {
        Self::open_inner(dev, alloc, handle, false)
    }

    fn open_inner(dev: Arc<D>, alloc: &Allocator, handle: RunHandle, filter: bool) -> Result<Self, SnapshotError> {
        let seg_bytes = alloc.blocks_per_segment() as usize * dev.block_size();
        if handle.len < FOOTER_LEN as u64 {
            return Err(SnapshotError::Corrupt("run shorter than its footer"));
        }
        let footer_bytes = read_run_range(
            &*dev,
            alloc,
            &handle.segs,
            seg_bytes,
            handle.len - FOOTER_LEN as u64,
            FOOTER_LEN,
        )?;
        let ft = decode_footer(&footer_bytes).map_err(SnapshotError::Corrupt)?;
        let bloom = if filter {
            let bytes = read_run_range(
                &*dev, alloc, &handle.segs, seg_bytes, ft.bloom_off, ft.bloom_len as usize,
            )?;
            RunFilter::decode(ft.version, &bytes).map_err(SnapshotError::Corrupt)?
        } else {
            RunFilter::Unfiltered
        };
        let index_bytes = read_run_range(
            &*dev,
            alloc,
            &handle.segs,
            seg_bytes,
            ft.index_off,
            ft.index_len as usize,
        )?;
        let (index, rt) = decode_index(&index_bytes, ft.version).map_err(|e| {
            if e == "index crc mismatch" {
                let seg_lbas: Vec<u64> = handle.segs.iter()
                    .map(|s| alloc.segment_range(*s).0).collect();
                warn_index_crc("open", &*dev, &handle, &seg_lbas, seg_bytes, &ft, &index_bytes);
            }
            SnapshotError::Corrupt(e)
        })?;
        drop(index_bytes);
        check_rt_locator(rt, &ft)?;
        Ok(DeviceRun {
            dev,
            handle,
            seg_bytes,
            max_block: max_block_len(&index),
            index: Arc::new(index),
            bloom,
            entry_count: ft.entry_count,
            num_buckets: ft.num_buckets,
            sidecar_off: ft.sidecar_off,
            sidecar_len: ft.sidecar_len,
            version: ft.version,
            rt,
        })
    }

    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    pub fn from_parts(dev: Arc<D>, seg_bytes: usize, handle: RunHandle, parts: RunParts) -> Self {
        DeviceRun {
            dev,
            handle,
            seg_bytes,
            max_block: max_block_len(&parts.index),
            index: Arc::new(parts.index),
            bloom: parts.bloom,
            entry_count: parts.entry_count,
            num_buckets: parts.num_buckets,
            sidecar_off: parts.sidecar_off,
            sidecar_len: parts.sidecar_len,
            version: parts.version,
            rt: parts.rt,
        }
    }

    pub fn num_buckets(&self) -> u32 {
        self.num_buckets
    }

    pub fn format_version(&self) -> u16 {
        self.version
    }

    pub fn resident_stats(&self) -> RunResidentStats {
        let mut stats = resident_stats(
            &self.index,
            &self.bloom,
            std::mem::size_of::<Self>(),
        );
        stats.total_bytes +=
            (self.handle.segs.capacity() * std::mem::size_of::<SegmentId>()) as u64;
        stats
    }

    pub fn sidecar_len(&self) -> u64 {
        self.sidecar_len
    }

    pub fn has_sidecar(&self) -> bool {
        self.version <= RUN_SIDECAR_VERSION
    }

    pub fn supports_range_tombstones(&self) -> bool {
        self.rt.is_some()
    }

    pub fn rt_seq_floor(&self) -> u64 {
        self.rt.map_or(0, |rt| rt.seq_floor)
    }

    pub fn has_range_tombstones(&self) -> bool {
        self.rt.is_some_and(|rt| rt.len > 0)
    }

    pub fn block_max_hlc(&self, block: usize) -> Option<u64> {
        if self.rt.is_some() { self.index.max_hlc(block) } else { None }
    }

    pub fn read_range_tombstones(&self, alloc: &Allocator) -> Result<Vec<RtRecord>, SnapshotError> {
        match self.rt {
            Some(rt) if rt.len > 0 => {
                let bytes = self.range(alloc, rt.off, rt.len as usize)?;
                decode_rt_block(&bytes).map_err(SnapshotError::Corrupt)
            }
            _ => Ok(Vec::new()),
        }
    }

    fn walk_block_versions(
        &self,
        alloc: &Allocator,
        start_block: usize,
        mut f: impl FnMut(&[u8], u128) -> bool,
    ) -> Result<(), SnapshotError> {
        let mut walker = BlockWalker::new(self.index.clone(), start_block);
        loop {
            match walker.advance(&mut |_| true)? {
                Walk::Entry(version) => {
                    if !f(&walker.key, version) {
                        return Ok(());
                    }
                }
                Walk::Done => return Ok(()),
                Walk::Blocked => unreachable!("an unbounded reserve never blocks"),
                Walk::Window => {
                    let (off, len, end) = walker.window().expect("a pending window has a block");
                    walker.win = Vec::new();
                    let bytes = self.range(alloc, off, len)?;
                    walker.install(bytes, off, end);
                }
            }
        }
    }

    fn range(&self, alloc: &Allocator, off: u64, len: usize) -> Result<Vec<u8>, SnapshotError> {
        read_run_range(
            &*self.dev,
            alloc,
            &self.handle.segs,
            self.seg_bytes,
            off,
            len,
        )
    }

    fn seg_lbas(&self, alloc: &Allocator) -> Vec<u64> {
        self.handle
            .segs
            .iter()
            .map(|s| alloc.segment_range(*s).0)
            .collect()
    }

    fn locate_range(&self, alloc: &Allocator, off: u64, len: usize) -> RangeLocator {
        let seg_bytes = self.seg_bytes as u64;
        let first = (off / seg_bytes) as usize;
        let last = ((off + (len as u64).max(1) - 1) / seg_bytes) as usize;
        let seg_lbas = self
            .handle
            .segs
            .iter()
            .skip(first)
            .take(last - first + 1)
            .map(|s| alloc.segment_range(*s).0)
            .collect();
        RangeLocator {
            seg_lbas,
            seg_bytes: self.seg_bytes,
            block_size: self.dev.block_size(),
            off: off - first as u64 * seg_bytes,
            len,
        }
    }

    pub fn locate_block(&self, alloc: &Allocator, key: &[u8]) -> Option<RangeLocator> {
        self.locate_block_hashed(alloc, key, &BloomKey::new(key))
    }

    #[inline]
    pub fn prefetch_filter(&self, hk: &BloomKey) {
        self.bloom.prefetch(hk);
    }

    pub fn locate_block_hashed(
        &self,
        alloc: &Allocator,
        key: &[u8],
        hk: &BloomKey,
    ) -> Option<RangeLocator> {
        self.locate_block_hashed_counted(alloc, key, hk).1
    }

    pub(crate) fn locate_block_hashed_counted(
        &self,
        alloc: &Allocator,
        key: &[u8],
        hk: &BloomKey,
    ) -> (bool, Option<RangeLocator>) {
        if !self.bloom.contains_hashed(hk) {
            return (false, None);
        }
        let blk = match self
            .index
            .partition_point(|fk| fk <= key)
        {
            0 => return (true, None),
            p => self.index.get(p - 1).unwrap(),
        };
        let (_, off, len) = blk;
        (true, Some(self.locate_range(alloc, *off, *len as usize)))
    }

    pub fn locate_blocks_window(
        &self,
        alloc: &Allocator,
        start_block: usize,
        hi: Option<&[u8]>,
        max: usize,
    ) -> (Vec<RangeLocator>, usize, bool) {
        self.locate_blocks_window_picking(alloc, start_block, hi, max, |i| i)
    }

    pub fn locate_blocks_window_picking(
        &self,
        alloc: &Allocator,
        start_block: usize,
        hi: Option<&[u8]>,
        max: usize,
        mut pick: impl FnMut(usize) -> usize,
    ) -> (Vec<RangeLocator>, usize, bool) {
        let mut out = Vec::new();
        let mut i = start_block.min(self.index.len());
        while i < self.index.len() && out.len() < max {
            i = pick(i).max(i);
            let Some((fk, off, len)) = self.index.get(i) else { break };
            if hi.is_some_and(|h| fk >= h) {
                break;
            }
            out.push(self.locate_range(alloc, *off, *len as usize));
            i += 1;
        }
        let i = i.min(self.index.len());
        let has_more = i < self.index.len() && hi.is_none_or(|h| self.index.key(i) < h);
        (out, i, has_more)
    }

    pub fn locate_blocks_window_rev_picking(
        &self,
        alloc: &Allocator,
        end_block: usize,
        lo: Option<&[u8]>,
        max: usize,
        mut pick: impl FnMut(usize) -> usize,
    ) -> (Vec<RangeLocator>, usize, bool) {
        let start = self.straddling_start_block(lo);
        let mut end = end_block.min(self.index.len());
        let mut out = Vec::new();
        while end > start && out.len() < max {
            end = pick(end).min(end);
            if end <= start { break; }
            end -= 1;
            let (_, off, len) = self.index.get(end).unwrap();
            out.push(self.locate_range(alloc, *off, *len as usize));
        }
        (out, end, end > start)
    }

    pub fn straddling_start_block(&self, lo: Option<&[u8]>) -> usize {
        match lo {
            Some(l) => self
                .index
                .partition_point(|fk| fk <= l)
                .saturating_sub(1),
            None => 0,
        }
    }

    pub fn index_len(&self) -> usize {
        self.index.len()
    }

    pub fn blocks_starting_at_or_below(&self, key: &[u8]) -> usize {
        self.index.partition_point(|fk| fk <= key)
    }

    pub fn blocks_starting_below(&self, key: &[u8]) -> usize {
        self.index.partition_point(|fk| fk < key)
    }

    pub fn max_block_bytes(&self) -> usize {
        self.max_block
    }

    pub fn data_bytes(&self) -> u64 {
        self.index.last().map_or(0, |(_, off, len)| off + *len as u64)
    }

    pub fn blocks_for_bytes(&self, bytes: usize) -> usize {
        let n = self.index.len().max(1);
        let avg = (self.data_bytes() as usize).div_ceil(n).max(1);
        bytes.div_ceil(avg).max(1)
    }

    pub fn might_contain(&self, key: &[u8]) -> bool {
        self.bloom.contains(key)
    }

    pub(crate) fn filter_fpr(&self) -> f64 {
        self.bloom.estimated_fpr()
    }

    pub(crate) fn might_contain_hashed(&self, key: &BloomKey) -> bool {
        self.bloom.contains_hashed(key)
    }

    pub fn block_read_bytes(&self, alloc: &Allocator, block: usize) -> usize {
        self.index.get(block).map_or(0, |(_, off, len)| {
            let loc = self.locate_range(alloc, *off, *len as usize);
            let start = loc.off as usize % loc.block_size;
            (start + loc.len).div_ceil(loc.block_size) * loc.block_size
        })
    }

    pub fn read_block_entries(
        &self,
        alloc: &Allocator,
        block: usize,
    ) -> Result<Vec<SnapshotEntry>, SnapshotError> {
        let Some((_, off, len)) = self.index.get(block) else {
            return Ok(Vec::new());
        };
        let bytes = self.range(alloc, *off, *len as usize)?;
        decode_block(&bytes).map_err(SnapshotError::Corrupt)
    }

    pub fn block_first_key(&self, i: usize) -> &[u8] {
        self.index.key(i)
    }

    pub fn range_block_window(&self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> (usize, usize) {
        let start = self.straddling_start_block(lo);
        let end = match hi {
            Some(h) => self.index.partition_point(|fk| fk < h),
            None => self.index.len(),
        };
        (start, end.max(start))
    }

    pub fn locate_sidecar(&self, alloc: &Allocator) -> RangeLocator {
        self.locate_range(alloc, self.sidecar_off, self.sidecar_len as usize)
    }

    pub fn version_source(&self, alloc: &Allocator, after: Option<&[u8]>) -> VersionSource {
        if self.has_sidecar() && (after.is_none() || self.version < RUN_SIDECAR_VERSION) {
            return VersionSource::Sidecar {
                loc: self.locate_sidecar(alloc),
                version: self.version,
            };
        }
        let start = self.straddling_start_block(after);
        let data_end = self.index.last().map_or(0, |(_, off, len)| off + *len as u64);
        let base = self.index.get(start).map_or(data_end, |(_, off, _)| *off);
        VersionSource::Blocks(BlockVersionCursor {
            loc: self.locate_range(alloc, base, (data_end - base) as usize),
            base,
            num_buckets: self.num_buckets,
            max_block: self.max_block,
            walker: BlockWalker::new(self.index.clone(), start),
        })
    }

    pub fn block_lookup_bytes(
        block: &[u8],
        key: &[u8],
    ) -> Result<Option<SnapshotEntry>, SnapshotError> {
        block_lookup(block, key).map_err(SnapshotError::Corrupt)
    }

    pub fn lookup(
        &self,
        alloc: &Allocator,
        key: &[u8],
    ) -> Result<Option<SnapshotEntry>, SnapshotError> {
        self.lookup_hashed(alloc, key, &BloomKey::new(key))
    }

    pub fn lookup_hashed(
        &self,
        alloc: &Allocator,
        key: &[u8],
        hk: &BloomKey,
    ) -> Result<Option<SnapshotEntry>, SnapshotError> {
        self.lookup_hashed_counted(alloc, key, hk).1
    }

    pub(crate) fn lookup_hashed_counted(
        &self,
        alloc: &Allocator,
        key: &[u8],
        hk: &BloomKey,
    ) -> (bool, Result<Option<SnapshotEntry>, SnapshotError>) {
        if !self.bloom.contains_hashed(hk) {
            return (false, Ok(None));
        }
        let blk = match self
            .index
            .partition_point(|fk| fk <= key)
        {
            0 => return (true, Ok(None)),
            p => self.index.get(p - 1).unwrap(),
        };
        let (_, off, len) = blk;
        let result = self.range(alloc, *off, *len as usize)
            .and_then(|block| block_lookup(&block, key).map_err(SnapshotError::Corrupt));
        (true, result)
    }

    pub fn block_scan<'a>(&'a self, lo: Option<&'a [u8]>, hi: Option<&'a [u8]>) -> RunScan<'a, D> {
        let start = match lo {
            Some(l) => self
                .index
                .partition_point(|fk| fk <= l)
                .saturating_sub(1),
            None => 0,
        };
        RunScan {
            run: self,
            lo,
            hi,
            next_block: start,
            cur: BlockCursor::default(),
        }
    }

    pub fn block_scan_rev<'a>(&'a self, lo: Option<&'a [u8]>, hi: Option<&'a [u8]>) -> RunScanRev<'a, D> {
        RunScanRev {
            run: self,
            lo,
            hi,
            end_block: if matches!((lo, hi), (Some(l), Some(h)) if l >= h) {
                0
            } else {
                hi.map_or(self.index.len(), |h| self.blocks_starting_below(h))
            },
            cur: Vec::new(),
        }
    }

    pub fn prefetch_scan(
        &self,
        alloc: &Allocator,
        window_bytes: usize,
        qd: usize,
    ) -> RunPrefetchScan<'_, D> {
        RunPrefetchScan {
            run: self,
            seg_lbas: self.seg_lbas(alloc),
            window_bytes: window_bytes.max(1),
            qd: qd.max(1),
            next_block: 0,
            win: Vec::new(),
            win_off: 0,
            win_end_block: 0,
            cur: Vec::new(),
            pos: 0,
        }
    }

    pub fn want_bucket_digests(
        &self,
        alloc: &Allocator,
        want: &std::collections::HashSet<u32>,
        bucket_of: impl Fn(&[u8]) -> u32,
    ) -> Result<Vec<Digest>, SnapshotError> {
        if want.is_empty() {
            return Ok(Vec::new());
        }
        if !self.has_sidecar() {
            let n = self.num_buckets.max(1);
            let mut out = Vec::new();
            self.walk_block_versions(alloc, 0, |key, version| {
                if want.contains(&(bucket_of(key) % n)) {
                    out.push((key.to_vec(), version.to_le_bytes().to_vec()));
                }
                true
            })?;
            return Ok(out);
        }
        let side = self.range(alloc, self.sidecar_off, self.sidecar_len as usize)?;
        parse_want_bucket_digests(&side, self.version, want, bucket_of)
    }

    pub fn want_bucket_digests_page(
        &self,
        alloc: &Allocator,
        want: &std::collections::HashSet<u32>,
        bucket_of: impl Fn(&[u8]) -> u32,
        after: Option<&[u8]>,
        max_bytes: usize,
        per_entry_overhead: usize,
    ) -> Result<(Vec<Digest>, Option<Vec<u8>>), SnapshotError> {
        if want.is_empty() {
            return Ok((Vec::new(), None));
        }
        if !self.has_sidecar() {
            let n = self.num_buckets.max(1);
            let mut out = Vec::new();
            let mut bytes = 0usize;
            let mut stop = None;
            self.walk_block_versions(alloc, self.straddling_start_block(after), |key, version| {
                if !want.contains(&(bucket_of(key) % n)) || after.is_some_and(|a| key <= a) {
                    return true;
                }
                let size = key.len().saturating_add(per_entry_overhead);
                if !out.is_empty() && bytes.saturating_add(size) > max_bytes {
                    stop = Some(key.to_vec());
                    return false;
                }
                bytes = bytes.saturating_add(size);
                out.push((key.to_vec(), version.to_le_bytes().to_vec()));
                true
            })?;
            return Ok((out, stop));
        }
        let side = self.range(alloc, self.sidecar_off, self.sidecar_len as usize)?;
        parse_want_bucket_digests_page(
            &side,
            self.version,
            want,
            bucket_of,
            after,
            max_bytes,
            per_entry_overhead,
        )
    }

    pub fn version_stream(
        &self,
        alloc: &Allocator,
    ) -> Result<Option<RunVersionStream<D>>, SnapshotError> {
        if self.version < 3 { return Ok(None); }
        if !self.has_sidecar() {
            return Ok(Some(RunVersionStream::Blocks(BlockVersionStream {
                dev: self.dev.clone(),
                seg_lbas: self.seg_lbas(alloc),
                seg_bytes: self.seg_bytes,
                num_buckets: self.num_buckets,
                walker: BlockWalker::new(self.index.clone(), 0),
            })));
        }
        Ok(Some(RunVersionStream::Sidecar(SidecarStream::new(self.dev.clone(), self.locate_sidecar(alloc))?)))
    }

    pub fn key_source(&self, alloc: &Allocator) -> RunKeySource {
        let kind = if self.version >= 3 && self.has_sidecar() && self.sidecar_len >= 4 {
            KeySourceKind::Sidecar {
                off: self.sidecar_off,
                body_len: self.sidecar_len - 4,
                pos: 0,
                carry: Vec::new(),
                crc: crc::Crc32::new(),
                header_read: false,
                max_key: self.index.max_block_len(),
            }
        } else {
            self.block_key_source_kind()
        };
        RunKeySource {
            seg_lbas: self.seg_lbas(alloc),
            seg_bytes: self.seg_bytes,
            kind,
            bytes_read: 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn block_key_source(&self, alloc: &Allocator) -> RunKeySource {
        RunKeySource {
            seg_lbas: self.seg_lbas(alloc),
            seg_bytes: self.seg_bytes,
            kind: self.block_key_source_kind(),
            bytes_read: 0,
        }
    }

    fn block_key_source_kind(&self) -> KeySourceKind {
        KeySourceKind::Blocks {
            blocks: self.index.iter().map(|(_, off, len)| (*off, *len)).collect(),
            next: 0,
        }
    }

    pub fn sidecar_versions(
        &self,
        alloc: &Allocator,
    ) -> Result<SidecarVersions, SnapshotError> {
        if !self.has_sidecar() {
            return Err(SnapshotError::Corrupt("run has no sidecar"));
        }
        let side = self.range(alloc, self.sidecar_off, self.sidecar_len as usize)?;
        SidecarVersions::from_sidecar(self.version, side)
    }

    pub fn stream_source(&self, alloc: &Allocator) -> RunStreamSource<D> {
        let sidecar = self.has_sidecar();
        RunStreamSource {
            dev: self.dev.clone(),
            version: self.version,
            seg_lbas: if sidecar { Vec::new() } else { self.seg_lbas(alloc) },
            seg_bytes: self.seg_bytes,
            num_buckets: self.num_buckets,
            index: (!sidecar).then(|| self.index.clone()),
            sidecar: sidecar.then(|| self.locate_sidecar(alloc)),
        }
    }

    pub fn for_each_bucket_digest(
        &self,
        alloc: &Allocator,
        mut f: impl FnMut(&[u8], u128),
    ) -> Result<(), SnapshotError> {
        if !self.has_sidecar() {
            return self.walk_block_versions(alloc, 0, |key, version| {
                f(key, version);
                true
            });
        }
        let side = self.range(alloc, self.sidecar_off, self.sidecar_len as usize)?;
        let body = sidecar_body(&side).map_err(SnapshotError::Corrupt)?;
        let mut c = Cursor { b: body, off: 0 };
        let n = c.u32().map_err(SnapshotError::Corrupt)?;
        if self.version >= 3 {
            while c.off < body.len() {
                let kl = c.u32().map_err(SnapshotError::Corrupt)? as usize;
                let k = c.take(kl).map_err(SnapshotError::Corrupt)?;
                let v = c.take(16).map_err(SnapshotError::Corrupt)?;
                f(k, u128::from_le_bytes(v.try_into().unwrap()));
            }
            return Ok(());
        }
        for _ in 0..n {
            let count = c.u32().map_err(SnapshotError::Corrupt)? as usize;
            for _ in 0..count {
                let kl = c.u32().map_err(SnapshotError::Corrupt)? as usize;
                let k = c.take(kl).map_err(SnapshotError::Corrupt)?;
                let v = c.take(16).map_err(SnapshotError::Corrupt)?;
                f(k, u128::from_le_bytes(v.try_into().unwrap()));
            }
        }
        Ok(())
    }

    pub fn bucket_digests(
        &self,
        alloc: &Allocator,
        bucket: u32,
        bucket_of: impl Fn(&[u8]) -> u32,
    ) -> Result<Vec<Digest>, SnapshotError> {
        if !self.has_sidecar() {
            if bucket >= self.num_buckets {
                return Err(SnapshotError::Corrupt("bucket out of range"));
            }
            let n = self.num_buckets.max(1);
            let mut out = Vec::new();
            self.walk_block_versions(alloc, 0, |key, version| {
                if bucket_of(key) % n == bucket {
                    out.push((key.to_vec(), version.to_le_bytes().to_vec()));
                }
                true
            })?;
            return Ok(out);
        }
        let side = self.range(alloc, self.sidecar_off, self.sidecar_len as usize)?;
        let body = sidecar_body(&side).map_err(SnapshotError::Corrupt)?;
        if self.version >= 3 {
            return sidecar_v3_bucket_digests(body, bucket, self.num_buckets, bucket_of)
                .map_err(SnapshotError::Corrupt);
        }
        let mut c = Cursor { b: body, off: 0 };
        let n = c.u32().map_err(SnapshotError::Corrupt)?;
        if bucket >= n {
            return Err(SnapshotError::Corrupt("bucket out of range"));
        }
        for b in 0..n {
            let count = c.u32().map_err(SnapshotError::Corrupt)? as usize;
            if b == bucket {
                let mut out = Vec::with_capacity(count);
                for _ in 0..count {
                    let kl = c.u32().map_err(SnapshotError::Corrupt)? as usize;
                    let k = c.take(kl).map_err(SnapshotError::Corrupt)?.to_vec();
                    let v = c.take(16).map_err(SnapshotError::Corrupt)?.to_vec();
                    out.push((k, v));
                }
                return Ok(out);
            }
            for _ in 0..count {
                let kl = c.u32().map_err(SnapshotError::Corrupt)? as usize;
                c.take(kl).map_err(SnapshotError::Corrupt)?;
                c.take(16).map_err(SnapshotError::Corrupt)?;
            }
        }
        Ok(Vec::new())
    }
}

pub struct RunScan<'a, D: BlockDevice> {
    run: &'a DeviceRun<D>,
    lo: Option<&'a [u8]>,
    hi: Option<&'a [u8]>,
    next_block: usize,
    cur: BlockCursor,
}

impl<'a, D: BlockDevice> RunScan<'a, D> {
    #[inline(never)]
    fn refill(&mut self, alloc: &Allocator, pick: &mut impl FnMut(usize) -> usize) -> Result<bool, SnapshotError> {
        if self.next_block >= self.run.index.len() {
            return Ok(false);
        }
        self.next_block = pick(self.next_block).max(self.next_block);
        if self.next_block >= self.run.index.len() {
            return Ok(false);
        }
        let (off, len) = self.run.index.block_ref(self.next_block);
        self.next_block += 1;
        let block = self.run.range(alloc, off, len as usize)?;
        self.cur = BlockCursor::new(block, self.lo, false).map_err(SnapshotError::Corrupt)?;
        Ok(true)
    }

    pub fn next(&mut self, alloc: &Allocator) -> Result<Option<SnapshotEntry>, SnapshotError> {
        self.next_picking(alloc, |b| b)
    }

    pub fn next_picking(
        &mut self,
        alloc: &Allocator,
        mut pick: impl FnMut(usize) -> usize,
    ) -> Result<Option<SnapshotEntry>, SnapshotError> {
        loop {
            let Some(e) = self.cur.next_entry().map_err(SnapshotError::Corrupt)? else {
                if !self.refill(alloc, &mut pick)? {
                    return Ok(None);
                }
                continue;
            };
            if self.hi.is_some_and(|h| e.key.as_slice() >= h) {
                return Ok(None);
            }
            if self.lo.is_some_and(|l| e.key.as_slice() < l) {
                continue;
            }
            return Ok(Some(e));
        }
    }
}

pub struct RunScanRev<'a, D: BlockDevice> {
    run: &'a DeviceRun<D>,
    lo: Option<&'a [u8]>,
    hi: Option<&'a [u8]>,
    end_block: usize,
    cur: Vec<SnapshotEntry>,
}

impl<D: BlockDevice> RunScanRev<'_, D> {
    pub fn next(&mut self, alloc: &Allocator) -> Result<Option<SnapshotEntry>, SnapshotError> {
        self.next_picking(alloc, |end| end)
    }

    pub fn next_picking(
        &mut self,
        alloc: &Allocator,
        mut pick: impl FnMut(usize) -> usize,
    ) -> Result<Option<SnapshotEntry>, SnapshotError> {
        loop {
            if let Some(e) = self.cur.pop() {
                if self.hi.is_some_and(|h| e.key.as_slice() >= h) {
                    continue;
                }
                if self.lo.is_some_and(|l| e.key.as_slice() < l) {
                    self.end_block = 0;
                    self.cur.clear();
                    return Ok(None);
                }
                return Ok(Some(e));
            }
            if self.end_block == 0 {
                return Ok(None);
            }
            self.end_block = pick(self.end_block).min(self.end_block);
            if self.end_block == 0 {
                return Ok(None);
            }
            self.end_block -= 1;
            let (off, len) = self.run.index.block_ref(self.end_block);
            let block = self.run.range(alloc, off, len as usize)?;
            self.cur = decode_block(&block).map_err(SnapshotError::Corrupt)?;
        }
    }
}

pub struct RunPrefetchScan<'a, D: BlockDevice> {
    run: &'a DeviceRun<D>,
    seg_lbas: Vec<u64>,
    window_bytes: usize,
    qd: usize,
    next_block: usize,
    win: Vec<u8>,
    win_off: u64,
    win_end_block: usize,
    cur: Vec<SnapshotEntry>,
    pos: usize,
}

impl<'a, D: BlockDevice> RunPrefetchScan<'a, D> {
    fn refill(&mut self) -> Result<(), SnapshotError> {
        let idx = &self.run.index;
        let first = self.next_block;
        let (first_off, _) = idx.block_ref(first);
        let mut end = first;
        let mut end_off = first_off;
        while end < idx.len() {
            let (off, len) = idx.block_ref(end);
            if off != end_off {
                break;
            }
            end_off = off + len as u64;
            end += 1;
            if (end_off - first_off) as usize >= self.window_bytes {
                break;
            }
        }
        self.win = read_run_range_batched(
            &*self.run.dev,
            &self.seg_lbas,
            self.run.seg_bytes,
            first_off,
            (end_off - first_off) as usize,
            self.qd,
        )?;
        self.win_off = first_off;
        self.win_end_block = end;
        Ok(())
    }

    pub fn next_entry(&mut self) -> Result<Option<SnapshotEntry>, SnapshotError> {
        while self.pos >= self.cur.len() {
            if self.next_block >= self.run.index.len() {
                return Ok(None);
            }
            if self.next_block >= self.win_end_block {
                self.refill()?;
            }
            let (off, len) = self.run.index.block_ref(self.next_block);
            let s = (off - self.win_off) as usize;
            let block = &self.win[s..s + len as usize];
            self.cur = decode_block(block).map_err(SnapshotError::Corrupt)?;
            self.pos = 0;
            self.next_block += 1;
        }
        let out = self.cur[self.pos].clone();
        self.pos += 1;
        Ok(Some(out))
    }
}

#[cfg(test)]
mod tests {
    mod v6;
    use super::*;

    fn entry(key: &[u8], seq: u64, version: u128) -> SnapshotEntry {
        SnapshotEntry {
            key: key.to_vec(),
            ptr: ValuePtr {
                segment: SegmentId(seq),
                offset: seq as u32 * 3,
                len: 17,
            },
            seq,
            version,
            tombstone: false,
        }
    }

    fn firehose(n: usize) -> Vec<SnapshotEntry> {
        let did = "did:plc:abcdefghijklmnopqrstuvwx";
        (0..n)
            .map(|i| {
                entry(
                    format!("app.bsky.feed.like/{did}/3k{i:013}").as_bytes(),
                    i as u64,
                    0xabab_0000 | i as u128,
                )
            })
            .collect()
    }

    #[test]
    fn sidecar_versions_pages_v2_and_v3() {
        fn record(body: &mut Vec<u8>, key: &[u8], version: u128) {
            body.extend_from_slice(&(key.len() as u32).to_le_bytes());
            body.extend_from_slice(key);
            body.extend_from_slice(&version.to_le_bytes());
        }

        let expected = vec![
            (b"a".to_vec(), 1u128),
            (b"bb".to_vec(), 2u128),
            (b"ccc".to_vec(), 3u128),
            (b"dddd".to_vec(), 4u128),
            (b"eeeee".to_vec(), 5u128),
        ];

        let mut v3 = 8u32.to_le_bytes().to_vec();
        for (key, version) in &expected {
            record(&mut v3, key, *version);
        }
        let mut pager = SidecarVersions::from_body(3, v3).unwrap();
        let mut got = Vec::new();
        assert!(!pager.fold_page(2, |k, v| got.push((k.to_vec(), v))).unwrap());
        assert!(!pager.fold_page(2, |k, v| got.push((k.to_vec(), v))).unwrap());
        assert!(pager.fold_page(2, |k, v| got.push((k.to_vec(), v))).unwrap());
        assert_eq!(got, expected);

        let mut v2 = 3u32.to_le_bytes().to_vec();
        v2.extend_from_slice(&2u32.to_le_bytes());
        for (key, version) in &expected[..2] {
            record(&mut v2, key, *version);
        }
        v2.extend_from_slice(&0u32.to_le_bytes());
        v2.extend_from_slice(&3u32.to_le_bytes());
        for (key, version) in &expected[2..] {
            record(&mut v2, key, *version);
        }
        let mut pager = SidecarVersions::from_body(2, v2).unwrap();
        let mut got = Vec::new();
        assert!(!pager.fold_page(2, |k, v| got.push((k.to_vec(), v))).unwrap());
        assert!(!pager.fold_page(2, |k, v| got.push((k.to_vec(), v))).unwrap());
        assert!(pager.fold_page(2, |k, v| got.push((k.to_vec(), v))).unwrap());
        assert_eq!(got, expected);
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }
        fn range(&mut self, lo: u64, hi: u64) -> u64 {
            lo + self.next() % (hi - lo + 1)
        }
    }

    #[test]
    fn run_len_upper_bound_dominates_build_run_over_randomized_shapes() {
        let mut rng = Rng(0x5331_5333_C0FF_EE01);
        for _ in 0..3000 {
            let n = rng.range(0, 300) as usize;
            let bloom_bits_per_key = rng.range(1, 24) as usize;
            let num_buckets = rng.range(1, 16) as u32;
            let block_target_bytes = if rng.next() & 1 == 0 {
                1
            } else {
                rng.range(1, 8192) as usize
            };
            let opts = RunBuildOpts {
                block_target_bytes,
                bloom_bits_per_key,
                num_buckets,
            };

            let uniform_body = (rng.next() & 1 == 0).then(|| rng.range(0, 200) as usize);
            let mut entries: Vec<SnapshotEntry> = Vec::with_capacity(n);
            for i in 0..n {
                let body_len = match uniform_body {
                    Some(len) => len,
                    None => match rng.range(0, 2) {
                        0 => 0,
                        1 => rng.range(1, 8) as usize,
                        _ => rng.range(8, 200) as usize,
                    },
                };
                let mut key = (i as u64).to_be_bytes().to_vec();
                for _ in 0..body_len {
                    key.push((rng.next() & 0xff) as u8);
                }
                entries.push(entry(&key, i as u64, rng.next() as u128));
            }

            for version in [RUN_SIDECAR_VERSION, RUN_VERSION] {
                let bound = run_len_upper_bound_for(&entries, opts, version);
                let actual = build_run_versioned(&entries, opts, version).len();
                assert!(
                    bound >= actual,
                    "upper bound {bound} < actual {actual} for v{version} n={n}, bloom_bits={bloom_bits_per_key}, \
                     num_buckets={num_buckets}, block_target={block_target_bytes}"
                );
            }
        }
    }

    #[test]
    fn round_trip_preserves_entries() {
        let entries = firehose(50);
        let block = encode_block(&entries);
        assert_eq!(decode_block(&block).unwrap(), entries);
    }

    #[test]
    fn tombstone_flag_round_trips_through_block_and_run() {
        let mut entries: Vec<SnapshotEntry> = firehose(40);
        for (i, e) in entries.iter_mut().enumerate() {
            e.tombstone = i % 3 == 0;
        }
        let block = encode_block(&entries);
        assert_eq!(decode_block(&block).unwrap(), entries);
        for e in &entries {
            let got = block_lookup(&block, &e.key).unwrap().unwrap();
            assert_eq!(got.tombstone, e.tombstone, "flag for {:?}", e.key);
            assert_eq!(&got, e);
        }
        let bytes = build_run(
            &entries,
            RunBuildOpts {
                block_target_bytes: 256,
                bloom_bits_per_key: 10,
                num_buckets: 16,
            },
        );
        let (dev, mut alloc) = dev_alloc(2048, 4);
        let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
        let run = DeviceRun::open(dev, &alloc, handle).unwrap();
        for e in &entries {
            assert_eq!(run.lookup(&alloc, &e.key).unwrap().as_ref(), Some(e));
        }
        let mut scan = run.block_scan(None, None);
        let mut scanned = Vec::new();
        while let Some(e) = scan.next(&alloc).unwrap() {
            scanned.push(e);
        }
        assert_eq!(scanned, entries);
    }

    #[test]
    fn v1_run_footer_decodes_as_all_live() {
        let entries = firehose(30);
        let mut bytes = build_run_v3(&entries, RunBuildOpts::default());
        let flen = bytes.len();
        let foff = flen - FOOTER_LEN;
        stamp_footer_version(&mut bytes[foff..], 1);
        let run = Run::parse(bytes).unwrap();
        assert!(matches!(run.bloom, RunFilter::Classic(_)));
        for e in &entries {
            let got = run.lookup(&e.key).unwrap().unwrap();
            assert!(!got.tombstone, "v1 entry must decode as live");
            assert_eq!(&got, e);
        }
    }

    #[test]
    fn round_trip_empty_and_single() {
        assert_eq!(decode_block(&encode_block(&[])).unwrap(), vec![]);
        let one = vec![entry(b"solo", 1, 0x010203)];
        assert_eq!(decode_block(&encode_block(&one)).unwrap(), one);
    }

    #[test]
    fn round_trip_empty_key_and_version() {
        let entries = vec![entry(b"", 0, 0), entry(b"a", 1, u128::MAX)];
        assert_eq!(decode_block(&encode_block(&entries)).unwrap(), entries);
    }

    #[test]
    fn front_coding_actually_shrinks_shared_prefixes() {
        let entries = firehose(100);
        let raw_key_bytes: usize = entries.iter().map(|e| e.key.len()).sum();
        let block = encode_block(&entries);
        assert!(
            block.len() < raw_key_bytes,
            "front-coded block ({}) should beat raw key bytes ({raw_key_bytes})",
            block.len()
        );
    }

    #[test]
    fn block_lookup_finds_present_and_absent() {
        let entries = firehose(64);
        let block = encode_block(&entries);
        for e in &entries {
            assert_eq!(block_lookup(&block, &e.key).unwrap().as_ref(), Some(e));
        }
        assert_eq!(block_lookup(&block, b"app.aaa").unwrap(), None);
        assert_eq!(block_lookup(&block, b"zzz").unwrap(), None);
    }

    #[test]
    fn block_first_key_matches() {
        let entries = firehose(10);
        let block = encode_block(&entries);
        assert_eq!(
            block_first_key(&block).unwrap().as_deref(),
            Some(entries[0].key.as_slice())
        );
        assert_eq!(block_first_key(&encode_block(&[])).unwrap(), None);
    }

    #[test]
    fn crc_mismatch_and_truncation_are_errors() {
        let mut block = encode_block(&firehose(8));
        let n = block.len();
        block[n / 2] ^= 0xff;
        assert_eq!(decode_block(&block), Err("crc mismatch"));
        let good = encode_block(&firehose(8));
        for cut in [0usize, 3, 7, good.len() - 1] {
            assert!(decode_block(&good[..cut]).is_err(), "cut {cut}");
        }
    }

    fn test_bucket(k: &[u8]) -> u32 {
        bloom_hash(k) as u32
    }

    #[test]
    fn bloom_no_false_negatives() {
        let entries = firehose(500);
        let bloom = Bloom::build(entries.iter().map(|e| e.key.as_slice()), entries.len(), 10);
        for e in &entries {
            assert!(bloom.contains(&e.key), "false negative for {:?}", e.key);
        }
        assert_eq!(Bloom::decode(&bloom.encode()).unwrap(), bloom);
        let fp = (0..2000)
            .filter(|i| bloom.contains(format!("absent-{i}").as_bytes()))
            .count();
        assert!(fp < 200, "false-positive rate too high: {fp}/2000");
    }

    #[test]
    fn run_round_trip_and_lookup() {
        let entries = firehose(300);
        let opts = RunBuildOpts {
            block_target_bytes: 512,
            bloom_bits_per_key: 10,
            num_buckets: 64,
        };
        let bytes = build_run(&entries, opts);
        let run = Run::parse(bytes).unwrap();
        assert_eq!(run.entry_count(), 300);
        assert!(run.index.len() > 1, "expected a multi-block run");
        for e in &entries {
            assert_eq!(run.lookup(&e.key).unwrap().as_ref(), Some(e));
        }
        assert_eq!(run.lookup(b"aaa").unwrap(), None);
        assert_eq!(run.lookup(b"zzz").unwrap(), None);
        assert_eq!(run.lookup(b"app.bsky.feed.like/did:plc:zzz").unwrap(), None);
    }

    #[test]
    fn run_bucket_digests_cover_all_entries_in_key_order() {
        let entries = firehose(400);
        let opts = RunBuildOpts {
            block_target_bytes: 1024,
            bloom_bits_per_key: 10,
            num_buckets: 32,
        };
        let bytes = build_run(&entries, opts);
        let run = Run::parse(bytes).unwrap();

        let mut seen = 0usize;
        for b in 0..run.num_buckets() {
            let digests = run.bucket_digests(b, test_bucket).unwrap();
            assert!(digests.windows(2).all(|w| w[0].0 <= w[1].0));
            for (k, v) in &digests {
                assert_eq!(test_bucket(k) % run.num_buckets(), b);
                let src = entries.iter().find(|e| &e.key == k).unwrap();
                assert_eq!(&src.version.to_le_bytes()[..], &v[..]);
                seen += 1;
            }
        }
        assert_eq!(
            seen,
            entries.len(),
            "every entry must appear in exactly one bucket"
        );
    }

    #[test]
    fn run_footer_corruption_is_an_error() {
        let bytes = build_run(&firehose(20), RunBuildOpts::default());
        let mut bad = bytes.clone();
        let n = bad.len();
        bad[n - 8] ^= 0xff;
        assert!(Run::parse(bad).is_err());
    }

    #[test]
    fn empty_run_round_trips() {
        let bytes = build_run(&[], RunBuildOpts::default());
        let run = Run::parse(bytes).unwrap();
        assert_eq!(run.entry_count(), 0);
        assert_eq!(run.lookup(b"anything").unwrap(), None);
    }

    use vals_spdk::MemBlockDevice;

    const BS: usize = 4096;

    fn dev_alloc(num_blocks: u64, seg_blocks: u64) -> (Arc<MemBlockDevice>, Allocator) {
        (
            Arc::new(MemBlockDevice::new(BS, num_blocks)),
            Allocator::new(num_blocks, seg_blocks, 0),
        )
    }

    #[test]
    fn device_run_round_trip_lookup() {
        let entries = firehose(900);
        let bytes = build_run(
            &entries,
            RunBuildOpts {
                block_target_bytes: 512,
                bloom_bits_per_key: 10,
                num_buckets: 64,
            },
        );
        let (dev, mut alloc) = dev_alloc(4096, 8);
        let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
        assert!(handle.segs.len() > 1, "expected a multi-segment run");

        let run = DeviceRun::open(dev, &alloc, handle).unwrap();
        assert_eq!(run.entry_count(), 900);
        for e in &entries {
            assert_eq!(run.lookup(&alloc, &e.key).unwrap().as_ref(), Some(e));
        }
        assert_eq!(run.lookup(&alloc, b"aaa").unwrap(), None);
        assert_eq!(run.lookup(&alloc, b"zzz").unwrap(), None);
    }

    #[test]
    fn from_parts_matches_open() {
        let entries = firehose(900);
        let opts = RunBuildOpts {
            block_target_bytes: 512,
            bloom_bits_per_key: 10,
            num_buckets: 64,
        };
        let bytes = build_run(&entries, opts);
        let (dev, mut alloc) = dev_alloc(4096, 8);
        let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
        assert!(handle.segs.len() > 1, "expected a multi-segment run");

        let seg_bytes = alloc.blocks_per_segment() as usize * dev.block_size();
        let seg_lbas: Vec<u64> = handle
            .segs
            .iter()
            .map(|s| alloc.segment_range(*s).0)
            .collect();
        let parts = read_run_parts(&*dev, &handle, &seg_lbas, seg_bytes).unwrap();
        let from_parts = DeviceRun::from_parts(dev.clone(), seg_bytes, handle.clone(), parts);

        let opened = DeviceRun::open(dev.clone(), &alloc, handle).unwrap();
        assert_eq!(from_parts.entry_count(), opened.entry_count());
        assert_eq!(from_parts.num_buckets(), opened.num_buckets());
        for e in &entries {
            assert_eq!(
                from_parts.lookup(&alloc, &e.key).unwrap(),
                opened.lookup(&alloc, &e.key).unwrap(),
                "lookup differs from_parts vs open for {:?}",
                e.key
            );
            assert_eq!(from_parts.lookup(&alloc, &e.key).unwrap().as_ref(), Some(e));
        }
        assert_eq!(from_parts.lookup(&alloc, b"aaa").unwrap(), None);
        assert_eq!(from_parts.lookup(&alloc, b"zzz").unwrap(), None);
        for b in 0..opts.num_buckets {
            assert_eq!(
                from_parts.bucket_digests(&alloc, b, test_bucket).unwrap(),
                opened.bucket_digests(&alloc, b, test_bucket).unwrap(),
                "bucket {b} digests differ from_parts vs open"
            );
        }
    }

    #[test]
    fn device_run_bucket_digests_match_in_memory() {
        let entries = firehose(250);
        let opts = RunBuildOpts {
            block_target_bytes: 1024,
            bloom_bits_per_key: 10,
            num_buckets: 32,
        };
        let bytes = build_run(&entries, opts);
        let mem = Run::parse(bytes.clone()).unwrap();

        let (dev, mut alloc) = dev_alloc(2048, 4);
        let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
        let run = DeviceRun::open(dev, &alloc, handle).unwrap();

        for b in 0..opts.num_buckets {
            assert_eq!(
                run.bucket_digests(&alloc, b, test_bucket).unwrap(),
                mem.bucket_digests(b, test_bucket).unwrap(),
                "bucket {b} digests differ device vs in-memory"
            );
        }
    }

    #[test]
    fn device_run_want_bucket_digests_match_per_bucket_union() {
        let entries = firehose(250);
        let opts = RunBuildOpts {
            block_target_bytes: 1024,
            bloom_bits_per_key: 10,
            num_buckets: 32,
        };
        let bytes = build_run(&entries, opts);
        let (dev, mut alloc) = dev_alloc(2048, 4);
        let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
        let run = DeviceRun::open(dev, &alloc, handle).unwrap();

        let want: std::collections::HashSet<u32> = [1u32, 4, 7, 30].into_iter().collect();
        let got = run.want_bucket_digests(&alloc, &want, test_bucket).unwrap();
        assert!(got.windows(2).all(|w| w[0].0 <= w[1].0));
        let mut expect: Vec<Digest> = Vec::new();
        for b in &want {
            expect.extend(run.bucket_digests(&alloc, *b, test_bucket).unwrap());
        }
        expect.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(got, expect);
        assert!(run
            .want_bucket_digests(&alloc, &std::collections::HashSet::new(), test_bucket)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn device_run_for_each_bucket_digest_streams_every_bucket_once() {
        let entries = firehose(250);
        let opts = RunBuildOpts {
            block_target_bytes: 1024,
            bloom_bits_per_key: 10,
            num_buckets: 32,
        };
        let bytes = build_run(&entries, opts);
        let (dev, mut alloc) = dev_alloc(2048, 4);
        let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
        let run = DeviceRun::open(dev, &alloc, handle).unwrap();

        let mut got: Vec<(Vec<u8>, u128)> = Vec::new();
        run.for_each_bucket_digest(&alloc, |k, v| got.push((k.to_vec(), v)))
            .unwrap();
        let mut expect: Vec<(Vec<u8>, u128)> = Vec::new();
        for b in 0..opts.num_buckets {
            for (k, vbytes) in run.bucket_digests(&alloc, b, test_bucket).unwrap() {
                expect.push((
                    k,
                    u128::from_le_bytes(vbytes.as_slice().try_into().unwrap()),
                ));
            }
        }
        got.sort();
        expect.sort();
        assert_eq!(got.len(), entries.len());
        assert_eq!(got, expect);
    }

    #[test]
    fn write_run_device_full_rolls_back() {
        let bytes = build_run(&firehose(2000), RunBuildOpts::default());
        let (dev, mut alloc) = dev_alloc(16, 2);
        let before = alloc.free_segments();
        let err = write_run(&*dev, &mut alloc, &bytes).unwrap_err();
        assert!(matches!(err, SnapshotError::DeviceFull));
        assert_eq!(alloc.free_segments(), before, "reservation rolled back");
    }

    #[test]
    fn device_run_single_segment() {
        let entries = firehose(5);
        let bytes = build_run(&entries, RunBuildOpts::default());
        let (dev, mut alloc) = dev_alloc(256, 64);
        let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
        assert_eq!(handle.segs.len(), 1);
        let run = DeviceRun::open(dev, &alloc, handle).unwrap();
        for e in &entries {
            assert_eq!(run.lookup(&alloc, &e.key).unwrap().as_ref(), Some(e));
        }
    }

    fn block_on<F: std::future::Future>(fut: F) -> F::Output {
        vals_rt::block_on(fut, || {})
    }

    fn reserve(
        alloc: &mut Allocator,
        block_size: usize,
        entries: &[SnapshotEntry],
        opts: RunBuildOpts,
    ) -> (Vec<SegmentId>, Vec<u64>) {
        let bound = run_len_upper_bound(entries, opts);
        let segs = allocate_run_segs(alloc, block_size, bound).unwrap();
        let lbas = segs.iter().map(|s| alloc.segment_range(*s).0).collect();
        (segs, lbas)
    }

    #[test]
    fn stream_sink_bytes_identical_to_write_run_segs() {
        let entries = firehose(4000);
        let opts = RunBuildOpts {
            block_target_bytes: 512,
            bloom_bits_per_key: 10,
            num_buckets: 64,
        };
        let seg_blocks = 8u64;

        let (dev_a, mut alloc_a) = dev_alloc(4096, seg_blocks);
        let run_bytes = build_run(&entries, opts);
        let seg_bytes_a = alloc_a.blocks_per_segment() as usize * BS;
        let (segs_a, lbas_a) = reserve(&mut alloc_a, BS, &entries, opts);
        let n_used_a = run_bytes.len().div_ceil(seg_bytes_a).max(1);
        let handle_a = block_on(write_run_segs(
            &*dev_a,
            &segs_a[..n_used_a],
            &lbas_a[..n_used_a],
            seg_bytes_a,
            BS,
            &run_bytes,
        ))
        .unwrap();

        let (dev_b, mut alloc_b) = dev_alloc(4096, seg_blocks);
        let seg_bytes_b = alloc_b.blocks_per_segment() as usize * BS;
        let (segs_b, lbas_b) = reserve(&mut alloc_b, BS, &entries, opts);
        assert_eq!(segs_a, segs_b);
        assert_eq!(lbas_a, lbas_b);
        let handle_b = {
            let mut sink = SegmentStreamSink::new(&*dev_b, &segs_b, &lbas_b, seg_bytes_b, BS);
            build_run_into(&entries, opts, &mut sink);
            block_on(sink.finish()).unwrap()
        };

        assert_eq!(handle_a, handle_b, "run handles differ");
        assert!(handle_a.segs.len() > 1, "expected a multi-segment run");
        assert_ne!(
            run_bytes.len() % seg_bytes_a,
            0,
            "test should produce a partial final segment"
        );

        for &lba in &lbas_a[..n_used_a] {
            let mut a = vec![0u8; seg_bytes_a];
            let mut b = vec![0u8; seg_bytes_b];
            dev_a.read_at(lba, &mut a).unwrap();
            dev_b.read_at(lba, &mut b).unwrap();
            assert_eq!(
                a, b,
                "segment at lba {lba} differs between Vec and stream paths"
            );
        }
    }

    #[test]
    fn stream_sink_round_trips_through_device_run() {
        let entries = firehose(1500);
        let opts = RunBuildOpts {
            block_target_bytes: 512,
            bloom_bits_per_key: 10,
            num_buckets: 64,
        };
        let (dev, mut alloc) = dev_alloc(4096, 8);
        let seg_bytes = alloc.blocks_per_segment() as usize * BS;
        let (segs, lbas) = reserve(&mut alloc, BS, &entries, opts);
        let handle = {
            let mut sink = SegmentStreamSink::new(&*dev, &segs, &lbas, seg_bytes, BS);
            build_run_into(&entries, opts, &mut sink);
            block_on(sink.finish()).unwrap()
        };
        assert!(handle.segs.len() > 1, "expected a multi-segment run");

        let run = DeviceRun::open(dev, &alloc, handle).unwrap();
        assert_eq!(run.entry_count(), entries.len() as u64);
        for e in &entries {
            assert_eq!(run.lookup(&alloc, &e.key).unwrap().as_ref(), Some(e));
        }
        assert_eq!(run.lookup(&alloc, b"aaa").unwrap(), None);
        assert_eq!(run.lookup(&alloc, b"zzz~~~~").unwrap(), None);
    }

    #[test]
    fn range_locator_trims_to_covered_segments_and_spans_boundaries() {
        let entries = firehose(4000);
        let opts = RunBuildOpts {
            block_target_bytes: 512,
            bloom_bits_per_key: 10,
            num_buckets: 64,
        };
        let (dev, mut alloc) = dev_alloc(4096, 8);
        let run_bytes = build_run(&entries, opts);
        let seg_bytes = alloc.blocks_per_segment() as usize * BS;
        let (segs, lbas) = reserve(&mut alloc, BS, &entries, opts);
        let n_used = run_bytes.len().div_ceil(seg_bytes).max(1);
        let handle = block_on(write_run_segs(
            &*dev,
            &segs[..n_used],
            &lbas[..n_used],
            seg_bytes,
            BS,
            &run_bytes,
        ))
        .unwrap();
        let run = DeviceRun::open(dev.clone(), &alloc, handle).unwrap();
        assert!(
            run.handle.segs.len() > 4,
            "need a many-segment run so an untrimmed locator would spill"
        );

        let total = run.handle.len;
        let sb = seg_bytes as u64;
        let cases: [(u64, usize); 4] = [
            (2 * sb + 5, 100),
            (sb - 7, 20),
            (sb / 2, 3 * seg_bytes),
            (total - 33, 33),
        ];
        for (off, len) in cases {
            assert!(off + len as u64 <= total, "case out of range");
            let loc = run.locate_range(&alloc, off, len);
            let covered = ((off + len as u64 - 1) / sb - off / sb + 1) as usize;
            assert_eq!(
                loc.seg_lbas.len(),
                covered,
                "locator must carry exactly the covered segments (off={off} len={len})"
            );
            let sync = run.range(&alloc, off, len).unwrap();
            let async_bytes = block_on(loc.read(&*dev)).unwrap();
            assert_eq!(
                async_bytes, sync,
                "trimmed locator read diverged from the sync path (off={off} len={len})"
            );
        }

        let loc = run
            .locate_block(&alloc, &entries[entries.len() / 2].key)
            .expect("present key");
        assert!(loc.seg_lbas.len() <= 2, "a block spans at most 2 segments");
        assert!(!loc.seg_lbas.spilled(), "point probes must not heap-allocate");
    }

    #[test]
    fn stream_sink_single_segment_matches() {
        let entries = firehose(5);
        let opts = RunBuildOpts::default();
        let run_bytes = build_run(&entries, opts);

        let (dev_a, mut alloc_a) = dev_alloc(256, 64);
        let seg_bytes = alloc_a.blocks_per_segment() as usize * BS;
        let (segs_a, lbas_a) = reserve(&mut alloc_a, BS, &entries, opts);
        let n_used_a = run_bytes.len().div_ceil(seg_bytes).max(1);
        let handle_a = block_on(write_run_segs(
            &*dev_a,
            &segs_a[..n_used_a],
            &lbas_a[..n_used_a],
            seg_bytes,
            BS,
            &run_bytes,
        ))
        .unwrap();

        let (dev_b, mut alloc_b) = dev_alloc(256, 64);
        let (segs_b, lbas_b) = reserve(&mut alloc_b, BS, &entries, opts);
        let handle_b = {
            let mut sink = SegmentStreamSink::new(&*dev_b, &segs_b, &lbas_b, seg_bytes, BS);
            build_run_into(&entries, opts, &mut sink);
            block_on(sink.finish()).unwrap()
        };
        assert_eq!(handle_a.segs.len(), 1);
        assert_eq!(handle_a, handle_b);
        let mut a = vec![0u8; seg_bytes];
        let mut b = vec![0u8; seg_bytes];
        dev_a.read_at(lbas_a[0], &mut a).unwrap();
        dev_b.read_at(lbas_b[0], &mut b).unwrap();
        assert_eq!(a, b);
    }

    fn build_streamed(
        entries: &[SnapshotEntry],
        opts: RunBuildOpts,
        n_upper: usize,
        num_blocks: u64,
        seg_blocks: u64,
    ) -> (Arc<MemBlockDevice>, Allocator, DeviceRun<MemBlockDevice>) {
        let (dev, mut alloc) = dev_alloc(num_blocks, seg_blocks);
        let seg_bytes = alloc.blocks_per_segment() as usize * BS;
        let (segs, lbas) = reserve(&mut alloc, BS, entries, opts);
        let handle = {
            let sink = SegmentStreamSink::new(&*dev, &segs, &lbas, seg_bytes, BS);
            let mut builder = RunStreamBuilder::new(sink, opts, n_upper);
            for e in entries {
                builder.push(e);
            }
            block_on(builder.finish()).unwrap()
        };
        let run = DeviceRun::open(dev.clone(), &alloc, handle).unwrap();
        (dev, alloc, run)
    }

    #[test]
    fn run_stream_builder_read_equivalent_to_build_run() {
        let entries = firehose(1500);
        let opts = RunBuildOpts {
            block_target_bytes: 512,
            bloom_bits_per_key: 10,
            num_buckets: 32,
        };

        let (ref_dev, mut ref_alloc) = dev_alloc(4096, 8);
        let ref_bytes = build_run(&entries, opts);
        let ref_handle = write_run(&*ref_dev, &mut ref_alloc, &ref_bytes).unwrap();
        let ref_run = DeviceRun::open(ref_dev, &ref_alloc, ref_handle).unwrap();

        let (_dev, alloc, run) = build_streamed(&entries, opts, entries.len() + 500, 4096, 8);
        assert!(run.handle.segs.len() > 1, "expected a multi-segment run");

        assert_eq!(run.entry_count(), ref_run.entry_count());
        assert_eq!(run.num_buckets(), ref_run.num_buckets());
        let mut scan = run.block_scan(None, None);
        let mut got = Vec::new();
        while let Some(e) = scan.next(&alloc).unwrap() {
            got.push(e);
        }
        assert_eq!(got, entries);
        for e in &entries {
            assert_eq!(run.lookup(&alloc, &e.key).unwrap().as_ref(), Some(e));
        }
        assert_eq!(run.lookup(&alloc, b"zzz").unwrap(), None);
        for b in 0..opts.num_buckets {
            assert_eq!(
                run.bucket_digests(&alloc, b, test_bucket).unwrap(),
                ref_run.bucket_digests(&ref_alloc, b, test_bucket).unwrap(),
                "bucket {b} digests differ streamed vs slice build"
            );
        }
    }

    #[test]
    fn run_stream_builder_empty_run() {
        let (_dev, alloc, run) = build_streamed(&[], RunBuildOpts::default(), 0, 256, 64);
        assert_eq!(run.entry_count(), 0);
        assert_eq!(run.lookup(&alloc, b"anything").unwrap(), None);
        for b in 0..RunBuildOpts::default().num_buckets {
            assert!(run.bucket_digests(&alloc, b, test_bucket).unwrap().is_empty());
        }
    }

    fn stamp_footer_version(f: &mut [u8], v: u16) {
        f[4..6].copy_from_slice(&v.to_le_bytes());
        let n = f.len();
        let crc = crc::crc32(&f[..n - 4]);
        f[n - 4..].copy_from_slice(&crc.to_le_bytes());
    }

    use super::legacy_fixture::build_run_v2;

    fn build_run_v3(entries: &[SnapshotEntry], opts: RunBuildOpts) -> Vec<u8> {
        let mut out = Vec::new();
        let mut index = FenceBuilder::default();
        let mut i = 0;
        while i < entries.len() {
            let start = i;
            let mut est = 0usize;
            while i < entries.len() && (i == start || est < opts.block_target_bytes) {
                est += entries[i].key.len() + 24 + 16;
                i += 1;
            }
            let block = encode_block(&entries[start..i]);
            index.push(&entries[start].key, out.len() as u64, block.len() as u32).unwrap();
            out.extend_from_slice(&block);
        }
        let bloom_off = out.len() as u64;
        let bloom = Bloom::build(
            entries.iter().map(|e| e.key.as_slice()),
            entries.len(),
            opts.bloom_bits_per_key,
        );
        let bloom_bytes = bloom.encode();
        out.extend_from_slice(&bloom_bytes);
        let index_off = out.len() as u64;
        let index = index.finish();
        let idx = encode_index_bytes(&index, None);
        out.extend_from_slice(&idx);
        let sidecar_off = out.len() as u64;
        let mut side = Vec::new();
        side.extend_from_slice(&opts.num_buckets.to_le_bytes());
        let mut rec = Vec::new();
        for e in entries {
            encode_sidecar_record(&mut rec, &e.key, e.version);
            side.extend_from_slice(&rec);
        }
        let side_crc = crc::crc32(&side);
        side.extend_from_slice(&side_crc.to_le_bytes());
        out.extend_from_slice(&side);
        let f = encode_footer(&FooterFields {
            entry_count: entries.len() as u64,
            num_blocks: index.len() as u64,
            index_off,
            index_len: idx.len() as u64,
            bloom_off,
            bloom_len: bloom_bytes.len() as u64,
            sidecar_off,
            sidecar_len: side.len() as u64,
            num_buckets: opts.num_buckets,
            version: 3,
        });
        out.extend_from_slice(&f);
        out
    }

    #[test]
    fn v2_and_v3_runs_serve_identical_digests() {
        let entries = firehose(300);
        let opts = RunBuildOpts {
            block_target_bytes: 512,
            bloom_bits_per_key: 10,
            num_buckets: 32,
        };
        let v2_bytes = build_run_v2(&entries, opts, test_bucket);
        let v3_bytes = build_run_v3(&entries, opts);

        let v2_mem = Run::parse(v2_bytes.clone()).unwrap();
        let v3_mem = Run::parse(v3_bytes.clone()).unwrap();
        assert_eq!(v2_mem.version, 2);
        assert_eq!(v3_mem.version, 3);
        assert!(matches!(v3_mem.bloom, RunFilter::Classic(_)));
        for b in 0..opts.num_buckets {
            assert_eq!(
                v2_mem.bucket_digests(b, test_bucket).unwrap(),
                v3_mem.bucket_digests(b, test_bucket).unwrap(),
                "bucket {b} digests differ v2 vs v3 (in-memory)"
            );
        }

        let (dev2, mut alloc2) = dev_alloc(2048, 4);
        let h2 = write_run(&*dev2, &mut alloc2, &v2_bytes).unwrap();
        let v2 = DeviceRun::open(dev2, &alloc2, h2).unwrap();
        let (dev3, mut alloc3) = dev_alloc(2048, 4);
        let h3 = write_run(&*dev3, &mut alloc3, &v3_bytes).unwrap();
        let v3 = DeviceRun::open(dev3, &alloc3, h3).unwrap();
        assert_eq!(v2.format_version(), 2);
        assert_eq!(v3.format_version(), 3);
        for e in &entries {
            assert_eq!(v2.lookup(&alloc2, &e.key).unwrap().as_ref(), Some(e));
            assert_eq!(v3.lookup(&alloc3, &e.key).unwrap().as_ref(), Some(e));
        }
        for b in 0..opts.num_buckets {
            assert_eq!(
                v2.bucket_digests(&alloc2, b, test_bucket).unwrap(),
                v3.bucket_digests(&alloc3, b, test_bucket).unwrap(),
                "bucket {b} digests differ v2 vs v3 (device)"
            );
        }
        let want: std::collections::HashSet<u32> = [0u32, 3, 9, 17, 31].into_iter().collect();
        let got2 = v2.want_bucket_digests(&alloc2, &want, test_bucket).unwrap();
        let got3 = v3.want_bucket_digests(&alloc3, &want, test_bucket).unwrap();
        assert!(got2.windows(2).all(|w| w[0].0 <= w[1].0));
        assert!(got3.windows(2).all(|w| w[0].0 <= w[1].0));
        assert_eq!(got2, got3);
        assert!(!got2.is_empty(), "want set should hit some buckets");
        let mut all2 = Vec::new();
        v2.for_each_bucket_digest(&alloc2, |k, v| all2.push((k.to_vec(), v)))
            .unwrap();
        let mut all3 = Vec::new();
        v3.for_each_bucket_digest(&alloc3, |k, v| all3.push((k.to_vec(), v)))
            .unwrap();
        all2.sort();
        all3.sort();
        assert_eq!(all2.len(), entries.len());
        assert_eq!(all2, all3);
    }

    #[test]
    fn blocked_bloom_no_false_negatives_and_round_trip() {
        let entries = firehose(4000);
        let bloom =
            BlockedBloom::build(entries.iter().map(|e| e.key.as_slice()), entries.len(), 12);
        for e in &entries {
            assert!(bloom.contains(&e.key), "false negative for {:?}", e.key);
        }
        assert_eq!(BlockedBloom::decode(&bloom.encode()).unwrap(), bloom);
        let empty = BlockedBloom::with_capacity(0, 12);
        assert_eq!(empty.num_bits, BLOOM_BLOCK_BITS);
        assert!(!empty.contains(b"anything"));
        let mut one = BlockedBloom::with_capacity(1, 12);
        one.insert(b"solo");
        assert!(one.contains(b"solo"));
    }

    #[test]
    fn bloom_decode_has_exact_capacity() {
        for words in [8, 1 << 20, (1 << 20) + 8] {
            let bits: Vec<u64> = (0..words).map(|i| splitmix64(i as u64)).collect();
            let blocked = BlockedBloom { num_bits: words as u64 * 64, k: 7, bits };
            let decoded = BlockedBloom::decode(&blocked.encode()).unwrap();
            assert_eq!(decoded.bits.capacity(), words);
            assert_eq!(decoded, blocked);
            let classic = Bloom { num_bits: blocked.num_bits, k: blocked.k, bits: blocked.bits };
            let decoded = Bloom::decode(&classic.encode()).unwrap();
            assert_eq!(decoded.bits.capacity(), words);
            assert_eq!(decoded, classic);
        }
    }

    fn replace_crc(bytes: &mut [u8]) {
        let end = bytes.len() - 4;
        let crc = crc::crc32(&bytes[..end]);
        bytes[end..].copy_from_slice(&crc.to_le_bytes());
    }

    #[test]
    fn bloom_decode_rejects_invalid_lengths() {
        for blocked in [false, true] {
            let (encoded, size_off, header_len) = if blocked {
                (BlockedBloom::with_capacity(1, 10).encode(), 4, 16)
            } else {
                (Bloom::with_capacity(1, 10).encode(), 0, 12)
            };
            for num_bits in [0u64, 1, 512 * 2, u64::MAX - 511] {
                let mut bad = encoded.clone();
                bad[size_off..size_off + 8].copy_from_slice(&num_bits.to_le_bytes());
                replace_crc(&mut bad);
                assert!(RunFilter::decode(if blocked { 4 } else { 3 }, &bad).is_err());
            }
            for len in [0, 3, header_len + 4, encoded.len() - 1] {
                let mut bad = encoded[..len].to_vec();
                if len >= 4 {
                    replace_crc(&mut bad);
                }
                assert!(RunFilter::decode(if blocked { 4 } else { 3 }, &bad).is_err());
            }
            let mut extra = encoded.clone();
            extra.push(0);
            replace_crc(&mut extra);
            assert!(RunFilter::decode(if blocked { 4 } else { 3 }, &extra).is_err());
        }
    }

    #[test]
    fn index_decode_has_exact_capacity_and_rejects_invalid_lengths() {
        for n in [0, 3, (1 << 20) + 1] {
            let mut builder = FenceBuilder::default();
            for i in 0..n {
                builder.push(&[], i as u64, 32).unwrap();
            }
            let index = builder.finish();
            let encoded = encode_index_bytes(&index, None);
            let (decoded, rt) = decode_index(&encoded, RUN_BLOCK_VERSION).unwrap();
            assert_eq!((decoded.allocated_bytes(), rt), (n * 16, None));
            assert_eq!(decoded, index);
            assert_eq!(encode_index_bytes(&decoded, None), encoded);
        }
        let decode_index = |b: &[u8]| decode_index(b, RUN_BLOCK_VERSION).map(|(i, _)| i);
        let mut builder = FenceBuilder::default();
        builder.push(b"key", 0, 32).unwrap();
        let encoded = encode_index_bytes(&builder.finish(), None);
        for (n, why) in [(0u64, "trailing bytes"), (2, "truncated"), (u64::MAX, "truncated")] {
            let mut bad = encoded.clone();
            bad[..8].copy_from_slice(&n.to_le_bytes());
            replace_crc(&mut bad);
            assert_eq!(decode_index(&bad).unwrap_err(), why);
        }
        let mut bad = encoded.clone();
        bad[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
        replace_crc(&mut bad);
        assert_eq!(decode_index(&bad).unwrap_err(), "truncated");
        let mut bad = encoded[..encoded.len() - 1].to_vec();
        replace_crc(&mut bad);
        assert_eq!(decode_index(&bad).unwrap_err(), "truncated");
        let mut bad = encoded.clone();
        bad[12] ^= 1;
        assert_eq!(decode_index(&bad).unwrap_err(), "index crc mismatch");
        let mut bad = encoded.clone();
        bad.insert(bad.len() - 4, 0);
        replace_crc(&mut bad);
        assert_eq!(decode_index(&bad).unwrap_err(), "trailing bytes");
        for cut in 0..12 {
            let mut bad = encoded[..cut].to_vec();
            if cut >= 4 { replace_crc(&mut bad); }
            assert_eq!(decode_index(&bad).unwrap_err(), "truncated");
        }
        assert_eq!(fence::key_end(u32::MAX as usize, 0), Ok(u32::MAX));
        assert_eq!(fence::key_end(u32::MAX as usize, 1), Err("index key bytes overflow"));
        assert_eq!(fence::key_end(usize::MAX, 1), Err("index key bytes overflow"));
    }

    #[test]
    fn compact_fences_preserve_decode_lookup_and_scan_boundaries() {
        for empty_first in [false, true] {
            let mut entries: Vec<_> = (0..200).map(|i| {
                let key = format!("key/{i:04}/{}", "x".repeat(i % 79));
                let mut e = entry(key.as_bytes(), i as u64, i as u128);
                e.tombstone = i % 7 == 0;
                e
            }).collect();
            if empty_first { entries.insert(0, entry(b"", 0, 0)); }
            let opts = RunBuildOpts { block_target_bytes: 128, ..RunBuildOpts::default() };
            let bytes = build_run_versioned(&entries, opts, RUN_BLOCK_VERSION);
            let ft = decode_footer(&bytes[bytes.len() - FOOTER_LEN..]).unwrap();
            let encoded = section(&bytes, ft.index_off, ft.index_len).unwrap();
            let (decoded, rt) = decode_index(encoded, ft.version).unwrap();
            assert_eq!(encode_index_bytes(&decoded, rt), encoded);
            let mut c = Cursor { b: &encoded[..encoded.len() - 4], off: 0 };
            let n = c.u64().unwrap();
            let reference: Vec<_> = (0..n).map(|_| {
                let len = c.u32().unwrap() as usize;
                (c.take(len).unwrap().to_vec(), c.u64().unwrap(), c.u32().unwrap())
            }).collect();
            assert!(reference.len() > 1);
            for (i, (key, off, len)) in reference.iter().enumerate() {
                assert_eq!(decoded.key(i), key);
                assert_eq!(decoded.block_ref(i), (*off, *len));
            }
            let in_memory = Run::parse(bytes.clone()).unwrap();
            let (dev, mut alloc) = dev_alloc(4096, 4);
            let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
            let run = DeviceRun::open(dev, &alloc, handle).unwrap();
            let mut probes = vec![Vec::new(), b"!".to_vec(), b"zzz".to_vec()];
            for (key, _, _) in &reference {
                probes.push(key.clone());
                let mut between = key.clone();
                between.push(0);
                probes.push(between);
            }
            probes.extend(entries.iter().map(|e| e.key.clone()));
            for key in &probes {
                let upper = reference.partition_point(|(fk, _, _)| fk.as_slice() <= key.as_slice());
                let lower = reference.partition_point(|(fk, _, _)| fk.as_slice() < key.as_slice());
                assert_eq!(decoded.partition_point(|fk| fk <= key.as_slice()), upper);
                assert_eq!(decoded.partition_point(|fk| fk < key.as_slice()), lower);
                assert_eq!(run.straddling_start_block(Some(key)), upper.saturating_sub(1));
                let expected = entries.iter().find(|e| e.key == *key);
                assert_eq!(in_memory.lookup(key).unwrap().as_ref(), expected);
                assert_eq!(run.lookup(&alloc, key).unwrap().as_ref(), expected);
            }
            for lo in [None].into_iter().chain(probes.iter().step_by(17).map(|k| Some(k.as_slice()))) {
                for hi in [None, Some(b"key/0100".as_slice()), Some(b"zzz".as_slice())] {
                    let mut scan = run.block_scan(lo, hi);
                    let mut got = Vec::new();
                    while let Some(e) = scan.next(&alloc).unwrap() { got.push(e); }
                    let expected: Vec<_> = entries.iter().filter(|e| {
                        lo.is_none_or(|l| e.key.as_slice() >= l) && hi.is_none_or(|h| e.key.as_slice() < h)
                    }).cloned().collect();
                    assert_eq!(got, expected);
                }
            }
            let mut scan = run.prefetch_scan(&alloc, 4096, 3);
            let mut got = Vec::new();
            while let Some(e) = scan.next_entry().unwrap() { got.push(e); }
            assert_eq!(got, entries);
        }
    }

    #[test]
    fn resident_stats_distinguish_payload_from_capacity() {
        let mut builder = FenceBuilder::with_capacity(4, 32);
        builder.push(b"key", 0, 32).unwrap();
        let index = builder.finish();
        let mut bloom = BlockedBloom::with_capacity(1, 10);
        bloom.bits.reserve_exact(8);
        let stats = resident_stats(&index, &RunFilter::Blocked(bloom), 0);
        assert_eq!(stats.fence_payload_bytes, 19);
        assert_eq!(stats.fence_bytes, 19);
        assert_eq!(stats.filter_payload_bytes, 64);
        assert_eq!(stats.filter_bytes, 128);
        assert_eq!(stats.total_bytes, stats.fence_bytes + stats.filter_bytes);
    }

    #[test]
    #[ignore]
    fn bloom_decode_allocation_measurement() {
        for entries in [10_000_000, 20_000_000, 37_036_223] {
            let bloom = BlockedBloom::with_capacity(entries, 10);
            let encoded = bloom.encode();
            let decoded = BlockedBloom::decode(&encoded).unwrap();
            let mut old = Vec::with_capacity(bloom.bits.len().min(1 << 20));
            for word in encoded[16..encoded.len() - 4].chunks_exact(8) {
                old.push(u64::from_le_bytes(word.try_into().unwrap()));
            }
            assert_eq!(decoded.bits, old);
            let payload = decoded.bits.len() * 8;
            let before = old.capacity() * 8;
            let after = decoded.bits.capacity() * 8;
            assert_eq!(after, payload);
            println!("entries={entries} payload={payload} before={before} after={after} before_B_per_entry={:.6} after_B_per_entry={:.6} saved_B_per_entry={:.6}",
                before as f64 / entries as f64, after as f64 / entries as f64,
                (before - after) as f64 / entries as f64);
        }
    }

    #[test]
    fn hashed_filter_probes_match_keyed_probes() {
        let entries = firehose(3000);
        let keys = || entries.iter().take(1500).map(|e| e.key.as_slice());
        let filters = [
            RunFilter::Blocked(BlockedBloom::build(keys(), 1500, 12)),
            RunFilter::Classic(Bloom::build(keys(), 1500, 12)),
        ];
        for filter in &filters {
            let mut hits = 0;
            for e in &entries {
                let hk = BloomKey::new(&e.key);
                filter.prefetch(&hk);
                assert_eq!(filter.contains_hashed(&hk), filter.contains(&e.key));
                hits += filter.contains(&e.key) as usize;
            }
            assert!(hits >= 1500 && hits < 3000);
        }
    }

    #[test]
    fn blocked_bloom_fp_rate_within_budget() {
        let n = 50_000usize;
        let keys: Vec<Vec<u8>> = (0..n)
            .map(|i| format!("present-{i:08}").into_bytes())
            .collect();
        let bloom = BlockedBloom::build(keys.iter().map(|k| k.as_slice()), n, 12);
        let probes = 100_000usize;
        let fp = (0..probes)
            .filter(|i| bloom.contains(format!("absent-{i:08}").as_bytes()))
            .count();
        assert!(
            (fp as f64) / (probes as f64) <= 0.025,
            "blocked-bloom FP rate too high: {fp}/{probes}"
        );
    }

    #[test]
    fn blocked_bloom_rejects_corruption_and_wrong_magic() {
        let bloom = BlockedBloom::build([b"a".as_slice(), b"b"].into_iter(), 2, 12);
        let enc = bloom.encode();
        let mut bad = enc.clone();
        bad[enc.len() / 2] ^= 0xff;
        assert_eq!(BlockedBloom::decode(&bad), Err("crc mismatch"));
        let mut wrong = enc.clone();
        wrong[0] ^= 0xff;
        let n = wrong.len();
        let crc = crc::crc32(&wrong[..n - 4]);
        wrong[n - 4..].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(BlockedBloom::decode(&wrong), Err("bad blocked-bloom magic"));
        let classic = Bloom::build([b"a".as_slice(), b"b"].into_iter(), 2, 12).encode();
        assert_eq!(BlockedBloom::decode(&classic), Err("bad blocked-bloom magic"));
    }

    #[test]
    fn v3_classic_bloom_run_reads_back() {
        let entries = firehose(300);
        let opts = RunBuildOpts {
            block_target_bytes: 512,
            bloom_bits_per_key: 10,
            num_buckets: 32,
        };
        let bytes = build_run_v3(&entries, opts);
        let mem = Run::parse(bytes.clone()).unwrap();
        assert_eq!(mem.version, 3);
        assert!(matches!(mem.bloom, RunFilter::Classic(_)));
        for e in &entries {
            assert_eq!(mem.lookup(&e.key).unwrap().as_ref(), Some(e));
        }
        assert_eq!(mem.lookup(b"zzz").unwrap(), None);
        let (dev, mut alloc) = dev_alloc(2048, 4);
        let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
        let run = DeviceRun::open(dev, &alloc, handle).unwrap();
        assert_eq!(run.format_version(), 3);
        assert!(matches!(run.bloom, RunFilter::Classic(_)));
        for e in &entries {
            assert_eq!(run.lookup(&alloc, &e.key).unwrap().as_ref(), Some(e));
        }
        assert_eq!(run.lookup(&alloc, b"zzz").unwrap(), None);
        assert_eq!(run.lookup(&alloc, b"app.aaa").unwrap(), None);
    }

    #[test]
    fn v4_run_round_trips_with_blocked_bloom() {
        let entries = firehose(400);
        let opts = RunBuildOpts {
            block_target_bytes: 512,
            ..Default::default()
        };
        let bytes = build_run(&entries, opts);
        let mem = Run::parse(bytes.clone()).unwrap();
        assert_eq!(mem.version, RUN_VERSION);
        assert!(matches!(mem.bloom, RunFilter::Blocked(_)));
        for e in &entries {
            assert_eq!(mem.lookup(&e.key).unwrap().as_ref(), Some(e));
        }
        assert_eq!(mem.lookup(b"zzz").unwrap(), None);

        let (dev, mut alloc) = dev_alloc(2048, 4);
        let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
        let run = DeviceRun::open(dev.clone(), &alloc, handle).unwrap();
        assert_eq!(run.format_version(), RUN_VERSION);
        assert!(matches!(run.bloom, RunFilter::Blocked(_)));
        for e in &entries {
            assert_eq!(run.lookup(&alloc, &e.key).unwrap().as_ref(), Some(e));
            let loc = run
                .locate_block(&alloc, &e.key)
                .expect("present key must bloom-hit");
            let block = block_on(loc.read(&*dev)).unwrap();
            assert_eq!(
                DeviceRun::<MemBlockDevice>::block_lookup_bytes(&block, &e.key)
                    .unwrap()
                    .as_ref(),
                Some(e)
            );
        }
        assert_eq!(run.lookup(&alloc, b"zzz").unwrap(), None);

        let (_sdev, salloc, srun) = build_streamed(&entries, opts, entries.len() + 777, 4096, 8);
        assert_eq!(srun.format_version(), RUN_VERSION);
        assert!(matches!(srun.bloom, RunFilter::Blocked(_)));
        for e in &entries {
            assert_eq!(srun.lookup(&salloc, &e.key).unwrap().as_ref(), Some(e));
        }
        assert_eq!(srun.lookup(&salloc, b"zzz").unwrap(), None);
    }

    fn rand_entries(rng: &mut Rng, n: usize, max_tail: u64) -> Vec<SnapshotEntry> {
        let prefixes: [&[u8]; 5] = [
            b"",
            b"a",
            b"app.bsky.graph.follow/did:plc:abcdefghijklmnop/",
            b"app.bsky.feed.like/did:plc:abcdefghijklmnop/3k",
            b"app.bsky.feed.like/did:plc:abcdefghijklmnoq/",
        ];
        let mut keys: Vec<Vec<u8>> = (0..n)
            .map(|_| {
                let mut key = prefixes[rng.range(0, prefixes.len() as u64 - 1) as usize].to_vec();
                for _ in 0..rng.range(0, max_tail) {
                    key.push(b"abz~"[rng.range(0, 3) as usize]);
                }
                key
            })
            .collect();
        keys.sort();
        keys.dedup();
        keys.into_iter()
            .enumerate()
            .map(|(i, key)| {
                let mut e = entry(&key, i as u64, ((rng.next() as u128) << 64) | rng.next() as u128);
                e.tombstone = rng.range(0, 3) == 0;
                e
            })
            .collect()
    }

    fn reserve_for(
        alloc: &mut Allocator,
        entries: &[SnapshotEntry],
        opts: RunBuildOpts,
        version: u16,
    ) -> (Vec<SegmentId>, Vec<u64>) {
        let bound = run_len_upper_bound_for(entries, opts, version);
        let segs = allocate_run_segs(alloc, BS, bound).unwrap();
        let lbas = segs.iter().map(|s| alloc.segment_range(*s).0).collect();
        (segs, lbas)
    }

    fn open_versioned(
        entries: &[SnapshotEntry],
        opts: RunBuildOpts,
        version: u16,
        streamed: bool,
    ) -> (Arc<MemBlockDevice>, Allocator, DeviceRun<MemBlockDevice>) {
        let (dev, mut alloc) = dev_alloc(8192, 8);
        let handle = if streamed {
            let seg_bytes = alloc.blocks_per_segment() as usize * BS;
            let (segs, lbas) = reserve_for(&mut alloc, entries, opts, version);
            let sink = SegmentStreamSink::new(&*dev, &segs, &lbas, seg_bytes, BS);
            let mut builder = RunStreamBuilder::new(sink, opts, entries.len()).version(version);
            for e in entries {
                builder.push(e);
            }
            block_on(builder.finish()).unwrap()
        } else {
            let bytes = build_run_versioned(entries, opts, version);
            assert!(bytes.len() <= run_len_upper_bound_for(entries, opts, version));
            write_run(&*dev, &mut alloc, &bytes).unwrap()
        };
        let run = DeviceRun::open(dev.clone(), &alloc, handle).unwrap();
        assert_eq!(run.format_version(), version);
        assert_eq!(run.has_sidecar(), version <= RUN_SIDECAR_VERSION);
        assert_eq!(run.sidecar_len() == 0, version > RUN_SIDECAR_VERSION);
        (dev, alloc, run)
    }

    fn seed_stream(run: &DeviceRun<MemBlockDevice>, alloc: &Allocator, rng: &mut Rng)
        -> Result<Vec<(Vec<u8>, u128)>, SnapshotError> {
        let mut stream = run.version_stream(alloc)?.expect("v3+ runs stream");
        let mut out = Vec::new();
        loop {
            let fail = rng.range(0, 4) == 0;
            match stream.next_step_reserving(|_| !fail)? {
                None => assert!(fail),
                Some(SidecarCursorStep::Entry(e)) => out.push(e),
                Some(SidecarCursorStep::Done) => return Ok(out),
                Some(SidecarCursorStep::Checkpoint) => {}
            }
        }
    }

    fn drain_version_source(
        dev: &MemBlockDevice,
        source: VersionSource,
        want: &std::collections::HashSet<u32>,
        bucket_of: &impl Fn(&[u8]) -> u32,
        after: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, u128)>, SnapshotError> {
        block_on(async {
            let mut out = Vec::new();
            match source {
                VersionSource::Sidecar { loc, version } => {
                    assert!(version >= 3);
                    let mut cursor = SidecarCursor::new(loc)?;
                    loop {
                        match cursor.next_matching_step(dev, want, bucket_of, after).await? {
                            SidecarCursorStep::Entry(e) => out.push(e),
                            SidecarCursorStep::Done => return Ok(out),
                            SidecarCursorStep::Checkpoint => {}
                        }
                    }
                }
                VersionSource::Blocks(mut cursor) => loop {
                    match cursor.next_matching_step(dev, want, bucket_of, after).await? {
                        SidecarCursorStep::Entry(e) => out.push(e),
                        SidecarCursorStep::Done => return Ok(out),
                        SidecarCursorStep::Checkpoint => {}
                    }
                },
            }
        })
    }

    fn digest_pages(
        run: &DeviceRun<MemBlockDevice>,
        alloc: &Allocator,
        want: &std::collections::HashSet<u32>,
        after: Option<&[u8]>,
        max_bytes: usize,
        overhead: usize,
    ) -> Vec<(Vec<Digest>, Option<Vec<u8>>)> {
        let mut pages = Vec::new();
        let mut cursor = after.map(<[u8]>::to_vec);
        loop {
            let (page, stop) = run
                .want_bucket_digests_page(alloc, want, test_bucket, cursor.as_deref(), max_bytes, overhead)
                .unwrap();
            let next = page.last().map(|(k, _)| k.clone());
            let done = stop.is_none();
            pages.push((page, stop));
            if done {
                return pages;
            }
            cursor = next;
        }
    }

    fn assert_block_stream_matches_sidecar(entries: &[SnapshotEntry], opts: RunBuildOpts, rng: &mut Rng) {
        let model: Vec<(Vec<u8>, u128)> = entries.iter().map(|e| (e.key.clone(), e.version)).collect();
        let streamed = rng.range(0, 1) == 1;
        let (dev4, alloc4, v4) = open_versioned(entries, opts, RUN_SIDECAR_VERSION, streamed);
        let (dev5, alloc5, v5) = open_versioned(entries, opts, RUN_VERSION, streamed);
        let ctx = format!("n={} opts={opts:?} streamed={streamed}", entries.len());
        assert_eq!(v4.entry_count(), v5.entry_count());
        assert_eq!(v4.num_buckets(), v5.num_buckets());

        let seed4 = seed_stream(&v4, &alloc4, rng).unwrap();
        let seed5 = seed_stream(&v5, &alloc5, rng).unwrap();
        assert_eq!(seed4, model, "{ctx}");
        assert_eq!(seed5, seed4, "seed stream {ctx}");

        let mut all4 = Vec::new();
        v4.for_each_bucket_digest(&alloc4, |k, v| all4.push((k.to_vec(), v))).unwrap();
        let mut all5 = Vec::new();
        v5.for_each_bucket_digest(&alloc5, |k, v| all5.push((k.to_vec(), v))).unwrap();
        assert_eq!(all5, all4, "for_each_bucket_digest {ctx}");
        assert_eq!(all4, model);

        for b in 0..opts.num_buckets {
            assert_eq!(
                v5.bucket_digests(&alloc5, b, test_bucket).unwrap(),
                v4.bucket_digests(&alloc4, b, test_bucket).unwrap(),
                "bucket {b} {ctx}"
            );
        }
        assert!(v5.bucket_digests(&alloc5, opts.num_buckets, test_bucket).is_err());
        assert!(v4.bucket_digests(&alloc4, opts.num_buckets, test_bucket).is_err());

        let mut source4 = v4.key_source(&alloc4);
        let mut source5 = v5.key_source(&alloc5);
        assert!(source4.first_sidecar_record_location(BS).is_some());
        assert!(source5.first_sidecar_record_location(BS).is_none());
        let chunk = rng.range(1, 70_000) as usize;
        let mut keys4 = Vec::new();
        while !source4.step(&*dev4, chunk, |k| keys4.push(k.to_vec())).unwrap() {}
        let mut keys5 = Vec::new();
        while !source5.step(&*dev5, chunk, |k| keys5.push(k.to_vec())).unwrap() {}
        assert_eq!(keys5, keys4, "key source {ctx}");
        assert_eq!(keys5.len() as u64, v5.entry_count());

        let afters: Vec<Option<Vec<u8>>> = vec![
            None,
            entries.get(rng.range(0, entries.len() as u64) as usize).map(|e| e.key.clone()),
            entries.first().map(|e| e.key.clone()),
            entries.last().map(|e| e.key.clone()),
            Some(b"app.bsky.feed.like/".to_vec()),
            Some(Vec::new()),
        ];
        for after in &afters {
            let after = after.as_deref();
            let want: std::collections::HashSet<u32> =
                (0..opts.num_buckets).filter(|_| rng.range(0, 2) > 0).collect();
            assert_eq!(
                v5.want_bucket_digests(&alloc5, &want, test_bucket).unwrap(),
                v4.want_bucket_digests(&alloc4, &want, test_bucket).unwrap(),
                "want {want:?} {ctx}"
            );
            let max_bytes = if entries.len() > 500 {
                [16_384usize, usize::MAX][rng.range(0, 1) as usize]
            } else {
                [0usize, 1, 64, 4096, usize::MAX][rng.range(0, 4) as usize]
            };
            let overhead = rng.range(0, 24) as usize;
            assert_eq!(
                digest_pages(&v5, &alloc5, &want, after, max_bytes, overhead),
                digest_pages(&v4, &alloc4, &want, after, max_bytes, overhead),
                "digest pages want {want:?} after {after:?} max {max_bytes} {ctx}"
            );
            let raw_want: std::collections::HashSet<u32> = (0..16).filter(|_| rng.range(0, 1) == 1).collect();
            let bucket_of = |k: &[u8]| test_bucket(k) % 16;
            let src4 = v4.version_source(&alloc4, after);
            let src5 = v5.version_source(&alloc5, after);
            assert_eq!(matches!(src4, VersionSource::Sidecar { .. }), after.is_none());
            assert!(matches!(src5, VersionSource::Blocks(_)));
            let side4 = VersionSource::Sidecar { loc: v4.locate_sidecar(&alloc4), version: RUN_SIDECAR_VERSION };
            let got4 = drain_version_source(&dev4, side4, &raw_want, &bucket_of, after).unwrap();
            let seek4 = drain_version_source(&dev4, src4, &raw_want, &bucket_of, after).unwrap();
            assert_eq!(seek4, got4, "v4 continuation source after {after:?} {ctx}");
            let got5 = drain_version_source(&dev5, src5, &raw_want, &bucket_of, after).unwrap();
            let expect: Vec<(Vec<u8>, u128)> = model
                .iter()
                .filter(|(k, _)| after.is_none_or(|a| k.as_slice() > a) && raw_want.contains(&bucket_of(k)))
                .cloned()
                .collect();
            assert_eq!(got4, expect, "async sidecar cursor {ctx}");
            assert_eq!(got5, got4, "async block cursor after {after:?} {ctx}");
        }
    }

    #[test]
    fn block_version_streams_match_sidecar_streams_for_every_consumer() {
        let mut rng = Rng(0x0365_5eed_0000_0001);
        let mut shapes: Vec<(usize, u64)> = vec![(0, 8), (1, 8), (1, 0), (2, 3), (5_000, 200)];
        for _ in 0..24 {
            shapes.push((rng.range(2, 400) as usize, rng.range(0, 40)));
        }
        for (n, max_tail) in shapes {
            let entries = rand_entries(&mut rng, n, max_tail);
            let opts = RunBuildOpts {
                block_target_bytes: [1usize, 64, 512, 4096][rng.range(0, 3) as usize],
                bloom_bits_per_key: 10,
                num_buckets: rng.range(1, 64) as u32,
            };
            if n == 5_000 {
                let data: usize = entries.iter().map(|e| e.key.len() + 48).sum();
                assert!(data > 2 * SIDECAR_CURSOR_WINDOW, "blocks must span several stream windows");
                assert!(entries.iter().any(|e| e.tombstone));
            }
            assert_block_stream_matches_sidecar(&entries, opts, &mut rng);
        }
    }

    #[test]
    fn v5_streams_keep_tombstones_for_every_consumer() {
        let mut rng = Rng(0x0371_7063_0000_0001);
        let mut entries = rand_entries(&mut rng, 600, 30);
        for (i, e) in entries.iter_mut().enumerate() {
            e.tombstone = i % 4 != 1;
        }
        let model: Vec<(Vec<u8>, u128)> = entries.iter().map(|e| (e.key.clone(), e.version)).collect();
        let tombstones = entries.iter().filter(|e| e.tombstone).count();
        assert!(tombstones > model.len() / 2 && tombstones < model.len());
        let opts = RunBuildOpts { block_target_bytes: 512, bloom_bits_per_key: 10, num_buckets: 8 };
        let all: std::collections::HashSet<u32> = (0..opts.num_buckets).collect();
        let bucket_of = |k: &[u8]| test_bucket(k) % opts.num_buckets;
        let mid = entries.len() / 2;
        for streamed in [false, true] {
            let (dev, alloc, run) = open_versioned(&entries, opts, RUN_VERSION, streamed);
            let ctx = format!("streamed={streamed}");
            assert_eq!(seed_stream(&run, &alloc, &mut rng).unwrap(), model, "seed stream {ctx}");
            let mut folded = Vec::new();
            run.for_each_bucket_digest(&alloc, |k, v| folded.push((k.to_vec(), v))).unwrap();
            assert_eq!(folded, model, "for_each_bucket_digest {ctx}");
            let per_bucket: usize = (0..opts.num_buckets)
                .map(|b| run.bucket_digests(&alloc, b, test_bucket).unwrap().len())
                .sum();
            assert_eq!(per_bucket, model.len(), "bucket_digests {ctx}");
            let (page, stop) = run.want_bucket_digests_page(&alloc, &all, test_bucket, None, usize::MAX, 0).unwrap();
            assert!(stop.is_none());
            let page: Vec<(Vec<u8>, u128)> = page
                .into_iter()
                .map(|(k, v)| (k, u128::from_le_bytes(v.as_slice().try_into().unwrap())))
                .collect();
            assert_eq!(page, model, "digest page {ctx}");
            let full = drain_version_source(&dev, run.version_source(&alloc, None), &all, &bucket_of, None).unwrap();
            assert_eq!(full, model, "async block cursor {ctx}");
            let after = entries[mid].key.as_slice();
            let tail = drain_version_source(&dev, run.version_source(&alloc, Some(after)), &all, &bucket_of, Some(after)).unwrap();
            assert_eq!(tail, model[mid + 1..].to_vec(), "async block cursor after {ctx}");
            let mut keys = Vec::new();
            let mut source = run.key_source(&alloc);
            while !source.step(&*dev, 4096, |k| keys.push(k.to_vec())).unwrap() {}
            assert_eq!(keys, model.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(), "key source {ctx}");
        }
    }

    #[test]
    fn block_cursor_after_starts_in_the_straddling_block_and_skips_nothing() {
        let entries = backlink_entries(3_000, 0x0371_0000_0000_0002);
        let opts = RunBuildOpts { block_target_bytes: 4096, bloom_bits_per_key: 10, num_buckets: 16 };
        let bytes = build_run_versioned(&entries, opts, RUN_VERSION);
        let (dev, mut alloc) = dev_alloc((bytes.len() / BS) as u64 * 2 + 1024, 256);
        let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
        let run = DeviceRun::open(dev.clone(), &alloc, handle).unwrap();
        let n_blocks = run.index_len();
        assert!(n_blocks >= 8, "{n_blocks}");
        let all: std::collections::HashSet<u32> = (0..16).collect();
        let bucket_of = |k: &[u8]| test_bucket(k) % 16;
        let firsts: Vec<Vec<u8>> = (0..n_blocks).map(|b| run.block_first_key(b).to_vec()).collect();
        let mut afters: Vec<Vec<u8>> = vec![Vec::new(), entries.last().unwrap().key.clone()];
        for b in [1, 2, n_blocks / 2, n_blocks - 1] {
            let i = entries.iter().position(|e| e.key == firsts[b]).unwrap();
            afters.push(entries[i - 1].key.clone());
            afters.push(entries[i].key.clone());
            afters.push(entries[(i + 1).min(entries.len() - 1)].key.clone());
            let mut between = firsts[b].clone();
            between.push(0);
            afters.push(between);
        }
        for after in &afters {
            let expect: Vec<(Vec<u8>, u128)> = entries
                .iter()
                .filter(|e| e.key.as_slice() > after.as_slice())
                .map(|e| (e.key.clone(), e.version))
                .collect();
            let straddling = firsts.partition_point(|fk| fk.as_slice() <= after.as_slice()).saturating_sub(1);
            let VersionSource::Blocks(cursor) = run.version_source(&alloc, Some(after)) else {
                panic!("v5 runs stream blocks");
            };
            assert_eq!(cursor.walker.next_block, straddling, "start block for after {after:?}");
            assert_eq!(cursor.base, run.index.block_ref(straddling).0, "cursor base for after {after:?}");
            let got = drain_version_source(&dev, VersionSource::Blocks(cursor), &all, &bucket_of, Some(after)).unwrap();
            assert_eq!(got, expect, "keys after {after:?}");
            let (page, stop) = run.want_bucket_digests_page(&alloc, &all, test_bucket, Some(after), usize::MAX, 0).unwrap();
            assert!(stop.is_none());
            assert_eq!(page.len(), expect.len(), "digest page after {after:?}");
        }
    }

    #[test]
    fn v5_runs_drop_the_sidecar_and_shrink() {
        let mut entries: Vec<SnapshotEntry> = (0..2_000u64)
            .map(|i| {
                let mut key = format!("at://did:plc:{:024}/app.bsky.feed.like/3k{i:011}", i % 37).into_bytes();
                key.resize(148, b'x');
                entry(&key, i, i as u128)
            })
            .collect();
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        let opts = RunBuildOpts::default();
        let v4 = build_run_versioned(&entries, opts, RUN_SIDECAR_VERSION);
        let v5 = build_run_versioned(&entries, opts, RUN_BLOCK_VERSION);
        let f4 = decode_footer(&v4[v4.len() - FOOTER_LEN..]).unwrap();
        let f5 = decode_footer(&v5[v5.len() - FOOTER_LEN..]).unwrap();
        assert_eq!((f4.version, f5.version), (RUN_SIDECAR_VERSION, RUN_BLOCK_VERSION));
        assert_eq!(f5.sidecar_len, 0);
        assert_eq!(f5.sidecar_off, f5.index_off + f5.index_len);
        assert_eq!(v4.len() - v5.len(), f4.sidecar_len as usize);
        assert_eq!(f4.sidecar_len as usize, 8 + entries.iter().map(|e| 20 + e.key.len()).sum::<usize>());
        assert_eq!(&v4[..f4.sidecar_off as usize], &v5[..f5.sidecar_off as usize]);
        assert!(v5.len() <= run_len_upper_bound_for(&entries, opts, RUN_BLOCK_VERSION));
        assert!(run_len_upper_bound_for(&entries, opts, RUN_BLOCK_VERSION)
            < run_len_upper_bound_for(&entries, opts, RUN_SIDECAR_VERSION));
        let mem = Run::parse(v5).unwrap();
        for b in 0..opts.num_buckets.min(64) {
            assert_eq!(
                mem.bucket_digests(b, test_bucket).unwrap(),
                Run::parse(v4.clone()).unwrap().bucket_digests(b, test_bucket).unwrap()
            );
        }
    }

    #[test]
    fn run_len_upper_bound_ignores_num_buckets_and_counts_the_v4_sidecar_exactly() {
        let opts = |num_buckets| RunBuildOpts { num_buckets, ..RunBuildOpts::default() };
        for n in [0usize, 1, 37, 900] {
            let entries = firehose(n);
            for version in [RUN_SIDECAR_VERSION, RUN_BLOCK_VERSION, RUN_VERSION] {
                let bounds = [1, 4096, 65535].map(|nb| run_len_upper_bound_for(&entries, opts(nb), version));
                assert!(bounds.iter().all(|&b| b == bounds[0]), "v{version} n={n}: {bounds:?}");
                assert!(build_run_versioned(&entries, opts(65535), version).len() <= bounds[0]);
            }
            let v4 = build_run_versioned(&entries, opts(65535), RUN_SIDECAR_VERSION);
            let f4 = decode_footer(&v4[v4.len() - FOOTER_LEN..]).unwrap();
            assert_eq!(
                run_len_upper_bound_for(&entries, opts(65535), RUN_SIDECAR_VERSION)
                    - run_len_upper_bound_for(&entries, opts(65535), RUN_BLOCK_VERSION),
                f4.sidecar_len as usize,
                "n={n}"
            );
        }
    }

    #[test]
    fn max_bucket_run_streams_into_a_device_sized_to_its_reservation() {
        let entries = firehose(900);
        let opts = RunBuildOpts { block_target_bytes: 512, num_buckets: 65535, ..RunBuildOpts::default() };
        let seg_blocks = 8u64;
        let seg_bytes = seg_blocks as usize * BS;
        let legacy_header_slack = 4 + 65535 * 4 + 4;
        for version in [RUN_SIDECAR_VERSION, RUN_VERSION] {
            let bound = run_len_upper_bound_for(&entries, opts, version);
            let segs = bound.div_ceil(seg_bytes) as u64;
            let (_, mut tight) = dev_alloc(segs * seg_blocks, seg_blocks);
            assert!(matches!(
                allocate_run_segs(&mut tight, BS, bound + legacy_header_slack),
                Err(SnapshotError::DeviceFull)
            ));
            assert_eq!(tight.free_segments(), segs);

            let (dev, mut alloc) = dev_alloc(segs * seg_blocks, seg_blocks);
            let (rsegs, lbas) = reserve_for(&mut alloc, &entries, opts, version);
            assert_eq!(alloc.free_segments(), 0);
            let sink = SegmentStreamSink::new(&*dev, &rsegs, &lbas, seg_bytes, BS);
            let mut builder = RunStreamBuilder::new(sink, opts, entries.len()).version(version);
            for e in &entries {
                builder.push(e);
            }
            let handle = block_on(builder.finish()).unwrap();
            let run = DeviceRun::open(dev, &alloc, handle).unwrap();
            assert_eq!((run.format_version(), run.num_buckets()), (version, 65535));
            for e in &entries {
                assert_eq!(run.lookup(&alloc, &e.key).unwrap().as_ref(), Some(e), "v{version}");
            }
        }
    }

    #[test]
    fn readers_capped_at_older_versions_reject_newer_runs_and_write_version_parses() {
        let entries = firehose(50);
        for version in [RUN_SIDECAR_VERSION, RUN_BLOCK_VERSION, RUN_VERSION] {
            let bytes = build_run_versioned(&entries, RunBuildOpts::default(), version);
            let footer = &bytes[bytes.len() - FOOTER_LEN..];
            assert_eq!(decode_footer(footer).unwrap().version, version);
            for reader in [RUN_SIDECAR_VERSION, RUN_BLOCK_VERSION, RUN_VERSION] {
                let old = decode_footer_capped(footer, reader);
                assert_eq!(old.is_ok(), version <= reader, "v{version} run, v{reader} reader");
                if version > reader {
                    assert_eq!(old.err(), Some("unsupported run version"));
                }
            }
        }
        let mut f = build_run(&entries, RunBuildOpts::default());
        let n = f.len();
        stamp_footer_version(&mut f[n - FOOTER_LEN..], RUN_VERSION + 1);
        assert!(Run::parse(f).is_err());
        assert_eq!(parse_run_write_version(None), RUN_VERSION);
        assert_eq!(parse_run_write_version(Some("6")), RUN_VERSION);
        assert_eq!(parse_run_write_version(Some("5")), RUN_BLOCK_VERSION);
        assert_eq!(parse_run_write_version(Some(" 4 ")), RUN_SIDECAR_VERSION);
        assert_eq!(parse_run_write_version(Some("v4")), RUN_SIDECAR_VERSION);
        assert_eq!(parse_run_write_version(Some("")), RUN_SIDECAR_VERSION);
        assert_eq!(parse_run_write_version(Some(" ")), RUN_SIDECAR_VERSION);
        assert_eq!(run_write_version(), RUN_VERSION);
        assert_eq!(with_run_write_version(RUN_SIDECAR_VERSION, run_write_version), RUN_SIDECAR_VERSION);
        assert_eq!(run_write_version(), RUN_VERSION);
        let merge_as = |configured, inputs: &[u16]| with_run_write_version(configured, || merge_write_version(inputs.iter().copied()));
        assert_eq!(merge_as(RUN_SIDECAR_VERSION, &[3, RUN_SIDECAR_VERSION]), RUN_SIDECAR_VERSION);
        assert_eq!(merge_as(RUN_SIDECAR_VERSION, &[RUN_SIDECAR_VERSION, RUN_BLOCK_VERSION]), RUN_BLOCK_VERSION);
        assert_eq!(merge_as(RUN_SIDECAR_VERSION, &[RUN_BLOCK_VERSION, RUN_VERSION]), RUN_VERSION);
        assert_eq!(merge_as(RUN_BLOCK_VERSION, &[RUN_SIDECAR_VERSION]), RUN_BLOCK_VERSION);
        assert_eq!(merge_as(RUN_BLOCK_VERSION, &[RUN_BLOCK_VERSION]), RUN_BLOCK_VERSION);
        assert_eq!(merge_as(RUN_BLOCK_VERSION, &[RUN_VERSION, RUN_SIDECAR_VERSION]), RUN_VERSION);
        assert_eq!(merge_as(RUN_VERSION, &[RUN_SIDECAR_VERSION]), RUN_VERSION);
        assert_eq!(merge_write_version([]), RUN_VERSION);
    }

    struct WriteCorruptDev {
        inner: MemBlockDevice,
        corrupt_first_write: std::sync::atomic::AtomicBool,
    }

    impl BlockDevice for WriteCorruptDev {
        fn block_size(&self) -> usize { self.inner.block_size() }
        fn num_blocks(&self) -> u64 { self.inner.num_blocks() }
        fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.inner.read_at(lba, buf)
        }
        fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
            if self.corrupt_first_write.swap(false, std::sync::atomic::Ordering::Relaxed) {
                let mut bad = buf.to_vec();
                bad[100] ^= 0x10;
                return self.inner.write_at(lba, &bad);
            }
            self.inner.write_at(lba, buf)
        }
        fn flush(&self) -> vals_spdk::Result<()> { self.inner.flush() }
    }

    #[test]
    fn v5_streamed_build_verifies_written_blocks() {
        let entries = firehose(3_000);
        let opts = RunBuildOpts { block_target_bytes: 512, ..Default::default() };
        for (corrupt, verify) in [(false, true), (true, true), (true, false)] {
            let dev = WriteCorruptDev {
                inner: MemBlockDevice::new(BS, 8192),
                corrupt_first_write: std::sync::atomic::AtomicBool::new(corrupt),
            };
            let mut alloc = Allocator::new(8192, 8, 0);
            let seg_bytes = alloc.blocks_per_segment() as usize * BS;
            let (segs, lbas) = reserve_for(&mut alloc, &entries, opts, RUN_VERSION);
            let (bytes_before, failures_before) = run_build_verify_totals();
            let sink = SegmentStreamSink::new(&dev, &segs, &lbas, seg_bytes, BS);
            let mut builder = RunStreamBuilder::new(sink, opts, entries.len())
                .version(RUN_VERSION)
                .verify(verify);
            for e in &entries {
                builder.push(e);
            }
            let result = block_on(builder.finish());
            let (bytes_after, failures_after) = run_build_verify_totals();
            match (corrupt, verify) {
                (false, true) => {
                    let handle = result.unwrap();
                    assert!(handle.segs.len() > 2);
                    assert!(bytes_after - bytes_before >= seg_bytes as u64);
                }
                (true, true) => {
                    assert!(matches!(result, Err(SnapshotError::Corrupt("run block crc mismatch on build verify"))));
                    assert!(failures_after > failures_before);
                }
                _ => {
                    let handle = result.expect("an unverified build publishes the corrupt block");
                    let run = DeviceRun::open(Arc::new(dev.inner.share()), &alloc, handle).unwrap();
                    let mut stream = run.version_stream(&alloc).unwrap().unwrap();
                    let err = loop {
                        match stream.next_step() {
                            Ok(SidecarCursorStep::Done) => panic!("corrupt block streamed clean"),
                            Ok(_) => {}
                            Err(e) => break e,
                        }
                    };
                    assert!(matches!(err, SnapshotError::Corrupt("run block crc mismatch")));
                }
            }
        }
    }

    type WriteMutation = Box<dyn FnMut(u64, &mut [u8]) -> bool + Send>;

    struct MutateOnWriteDev {
        inner: MemBlockDevice,
        mutate: std::sync::Mutex<Option<WriteMutation>>,
    }

    impl BlockDevice for MutateOnWriteDev {
        fn block_size(&self) -> usize { self.inner.block_size() }
        fn num_blocks(&self) -> u64 { self.inner.num_blocks() }
        fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.inner.read_at(lba, buf)
        }
        fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
            let mut slot = self.mutate.lock().unwrap();
            if let Some(f) = slot.as_mut() {
                let mut bad = buf.to_vec();
                if f(lba * self.inner.block_size() as u64, &mut bad) {
                    *slot = None;
                    return self.inner.write_at(lba, &bad);
                }
            }
            self.inner.write_at(lba, buf)
        }
        fn flush(&self) -> vals_spdk::Result<()> { self.inner.flush() }
    }

    #[derive(Clone, Copy)]
    enum TailFault {
        None,
        Flip(u64),
        ResealFooter(u64),
    }

    fn write_mutation(fault: TailFault, lbas: &[u64], seg_bytes: usize) -> Option<WriteMutation> {
        let dev_byte = |run_off: u64| {
            lbas[run_off as usize / seg_bytes] * BS as u64 + run_off % seg_bytes as u64
        };
        match fault {
            TailFault::None => None,
            TailFault::Flip(run_off) => {
                let at = dev_byte(run_off);
                Some(Box::new(move |start, buf| {
                    if !(start..start + buf.len() as u64).contains(&at) {
                        return false;
                    }
                    buf[(at - start) as usize] ^= 0x80;
                    true
                }))
            }
            TailFault::ResealFooter(run_off) => {
                let at = dev_byte(run_off);
                Some(Box::new(move |start, buf| {
                    if at < start || at + FOOTER_LEN as u64 > start + buf.len() as u64 {
                        return false;
                    }
                    let f = &mut buf[(at - start) as usize..][..FOOTER_LEN];
                    f[8] ^= 0x01;
                    let crc = crc::crc32(&f[..FOOTER_LEN - 4]);
                    f[FOOTER_LEN - 4..].copy_from_slice(&crc.to_le_bytes());
                    true
                }))
            }
        }
    }

    #[test]
    fn v5_streamed_build_verifies_index_filter_and_footer_from_the_device() {
        let opts = RunBuildOpts::default();
        let seg_bytes = 8 * BS;
        let (entries, reference) = (2_000..4_000)
            .step_by(100)
            .map(|n| {
                let entries = firehose(n);
                let reference = build_run_versioned(&entries, opts, RUN_VERSION);
                (entries, reference)
            })
            .find(|(_, r)| {
                let ft = decode_footer(&r[r.len() - FOOTER_LEN..]).unwrap();
                let (index, _) = decode_index(section(r, ft.index_off, ft.index_len).unwrap(), ft.version).unwrap();
                let final_seg = (r.len() - 1) / seg_bytes;
                *index.last().unwrap().1 as usize / seg_bytes == final_seg
                    && (r.len() - FOOTER_LEN) / seg_bytes == final_seg
            })
            .expect("a run whose last data block shares the final, sink-buffered segment");
        let ft = decode_footer(&reference[reference.len() - FOOTER_LEN..]).unwrap();
        let (index, _) = decode_index(section(&reference, ft.index_off, ft.index_len).unwrap(), ft.version).unwrap();
        let last_block = index.last().unwrap().1;
        let footer_off = (reference.len() - FOOTER_LEN) as u64;
        let index_crc = "index crc mismatch";
        let cases = [
            (TailFault::None, true, None),
            (TailFault::Flip(ft.index_off + 12 + 10), true, Some(index_crc)),
            (TailFault::Flip(ft.index_off + ft.index_len - 2), true, Some(index_crc)),
            (TailFault::Flip(ft.bloom_off + ft.bloom_len / 2), true, Some("crc mismatch")),
            (TailFault::Flip(footer_off + 30), true, Some("footer crc mismatch")),
            (TailFault::ResealFooter(footer_off), true,
                Some("run tail differs from its encoded bytes on build verify")),
            (TailFault::Flip(last_block + 5), true, Some("run block crc mismatch on build verify")),
            (TailFault::Flip(ft.index_off + 12 + 10), false, Some(index_crc)),
            (TailFault::Flip(last_block + 5), false, None),
        ];
        for (fault, verify, expect) in cases {
            let mut alloc = Allocator::new(8192, 8, 0);
            assert_eq!(alloc.blocks_per_segment() as usize * BS, seg_bytes);
            let (segs, lbas) = reserve_for(&mut alloc, &entries, opts, RUN_VERSION);
            let dev = MutateOnWriteDev {
                inner: MemBlockDevice::new(BS, 8192),
                mutate: std::sync::Mutex::new(write_mutation(fault, &lbas, seg_bytes)),
            };
            let (bytes_before, failures_before) = run_build_verify_totals();
            let sink = SegmentStreamSink::new(&dev, &segs, &lbas, seg_bytes, BS);
            let mut builder = RunStreamBuilder::new(sink, opts, entries.len())
                .version(RUN_VERSION)
                .verify(verify);
            for e in &entries {
                builder.push(e);
            }
            let result = block_on(builder.finish_with_parts());
            let (bytes_after, failures_after) = run_build_verify_totals();
            assert!(dev.mutate.lock().unwrap().is_none(), "fault was not injected");
            match (expect, result) {
                (None, Ok((handle, parts))) => {
                    assert_eq!(handle.len, reference.len() as u64);
                    assert_eq!(parts.index.len(), index.len());
                    if matches!(fault, TailFault::None) {
                        let on_dev = read_run_range_lbas(&dev, &lbas, seg_bytes, 0, reference.len()).unwrap();
                        assert!(on_dev == reference);
                        assert!(bytes_after - bytes_before >= reference.len() as u64);
                    }
                }
                (Some(msg), Err(SnapshotError::Corrupt(got))) => {
                    assert_eq!(got, msg);
                    if verify {
                        assert!(failures_after > failures_before, "{msg}: failure not counted");
                    }
                }
                (expect, got) => panic!("verify={verify} expected {expect:?}, got {:?}", got.map(|(h, _)| h)),
            }
        }
    }

    #[test]
    #[ignore]
    fn build_verify_cost_microbench() {
        let n = 400_000usize;
        let entries: Vec<SnapshotEntry> = (0..n)
            .map(|i| {
                let key = format!(
                    "at://did:plc:{:024x}/app.bsky.feed.post/3l{:011}\0at://did:plc:{:024x}/app.bsky.feed.like/3l{:011}\0subject.uri",
                    i / 7, i % 7, i.wrapping_mul(2654435761) % (1 << 40), i
                );
                entry(key.as_bytes(), i as u64, i as u128)
            })
            .collect();
        let opts = RunBuildOpts::default();
        let mut alloc = Allocator::new(1 << 17, 256, 0);
        let seg_bytes = alloc.blocks_per_segment() as usize * BS;
        let dev = MemBlockDevice::new(BS, 1 << 17);
        let (segs, lbas) = reserve_for(&mut alloc, &entries, opts, RUN_VERSION);
        for round in 0..3 {
            for verify in [false, true] {
                let (bytes_before, _) = run_build_verify_totals();
                let sink = SegmentStreamSink::new(&dev, &segs, &lbas, seg_bytes, BS);
                let mut builder = RunStreamBuilder::new(sink, opts, n).version(RUN_VERSION).verify(verify);
                let t = std::time::Instant::now();
                for e in &entries {
                    builder.push(e);
                }
                let push_s = t.elapsed().as_secs_f64();
                let (handle, parts) = block_on(builder.finish_with_parts()).unwrap();
                let finish_s = t.elapsed().as_secs_f64() - push_s;
                let (bytes_after, _) = run_build_verify_totals();
                let ft_bytes = read_run_range_lbas(&dev, &lbas, seg_bytes, handle.len - FOOTER_LEN as u64, FOOTER_LEN).unwrap();
                let ft = decode_footer(&ft_bytes).unwrap();
                let tail = ft.bloom_len + ft.index_len + FOOTER_LEN as u64;
                println!(
                    "round {round} verify={verify}: run {:.1} MiB, {} blocks, data {:.1} MiB, index {:.2} MiB, \
                     filter {:.2} MiB, tail {:.2}% of run; push {push_s:.3}s finish {finish_s:.3}s; \
                     verify bytes {:.1} MiB",
                    handle.len as f64 / 1048576.0, parts.index.len(), ft.bloom_off as f64 / 1048576.0,
                    ft.index_len as f64 / 1048576.0, ft.bloom_len as f64 / 1048576.0,
                    100.0 * tail as f64 / handle.len as f64, (bytes_after - bytes_before) as f64 / 1048576.0,
                );
                if verify {
                    let lbas = &lbas[..handle.segs.len()];
                    let read = |off: u64, len: u64| read_run_range_lbas(&dev, lbas, seg_bytes, off, len as usize).unwrap();
                    let (bloom, idx) = (read(ft.bloom_off, ft.bloom_len), read(ft.index_off, ft.index_len));
                    let t = std::time::Instant::now();
                    for _ in 0..10 {
                        read_run_parts(&dev, &handle, lbas, seg_bytes).unwrap();
                    }
                    let plain = t.elapsed().as_secs_f64() / 10.0;
                    let expects: Vec<[Vec<u8>; 4]> =
                        (0..10).map(|_| [ft_bytes.clone(), bloom.clone(), idx.clone(), Vec::new()]).collect();
                    let t = std::time::Instant::now();
                    for expect in expects {
                        verify_run_tail(&dev, &handle, lbas, seg_bytes, expect).unwrap();
                    }
                    let checked = t.elapsed().as_secs_f64() / 10.0;
                    let t = std::time::Instant::now();
                    for _ in 0..10 {
                        verify_blocks(&dev, lbas, seg_bytes, &parts.index).unwrap();
                    }
                    let blocks = t.elapsed().as_secs_f64() / 10.0;
                    println!(
                        "  tail readback+decode {:.2} ms, with compare {:.2} ms (+{:.2} ms); data-block verify {:.2} ms",
                        plain * 1e3, checked * 1e3, (checked - plain) * 1e3, blocks * 1e3,
                    );
                }
            }
        }
    }

    #[test]
    #[ignore]
    fn bloom_probe_microbench() {
        let n = 32_000_000usize;
        let keys: Vec<Vec<u8>> = (0..n)
            .map(|i| format!("app.bsky.feed.like/did:plc:bench/{i:012}").into_bytes())
            .collect();
        let classic = Bloom::build(keys.iter().map(|k| k.as_slice()), n, 12);
        let blocked = BlockedBloom::build(keys.iter().map(|k| k.as_slice()), n, 12);
        let probes: Vec<Vec<u8>> = (0..2_000_000usize)
            .map(|i| format!("app.bsky.feed.like/did:plc:absent/{i:012}").into_bytes())
            .collect();
        for (name, f) in [
            (
                "classic",
                &(|k: &[u8]| classic.contains(k) as u64) as &dyn Fn(&[u8]) -> u64,
            ),
            (
                "blocked",
                &(|k: &[u8]| blocked.contains(k) as u64) as &dyn Fn(&[u8]) -> u64,
            ),
        ] {
            let t = std::time::Instant::now();
            let mut hits = 0u64;
            for p in &probes {
                hits += f(p);
            }
            let el = t.elapsed();
            println!(
                "{name}: {:.1} ns/probe ({hits} false positives / {} probes)",
                el.as_nanos() as f64 / probes.len() as f64,
                probes.len()
            );
        }
    }

    struct IndexCorruptDev {
        inner: MemBlockDevice,
        target: std::sync::Mutex<Option<u64>>,
        corrupt_next: std::sync::atomic::AtomicBool,
        vary_reads: std::sync::atomic::AtomicBool,
        target_reads: std::sync::atomic::AtomicUsize,
    }

    impl BlockDevice for IndexCorruptDev {
        fn block_size(&self) -> usize { self.inner.block_size() }
        fn num_blocks(&self) -> u64 { self.inner.num_blocks() }
        fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
            self.inner.write_at(lba, buf)
        }
        fn flush(&self) -> vals_spdk::Result<()> { self.inner.flush() }
        fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            use std::sync::atomic::Ordering::Relaxed;
            self.inner.read_at(lba, buf)?;
            let start = lba * self.block_size() as u64;
            if let Some(target) = *self.target.lock().unwrap() {
                if target >= start && target < start + buf.len() as u64 {
                    let n = self.target_reads.fetch_add(1, Relaxed);
                    if self.vary_reads.load(Relaxed) {
                        buf[(target - start) as usize] ^= (n + 1) as u8;
                    }
                    if self.corrupt_next.swap(false, Relaxed) {
                        buf[(target - start) as usize] ^= 0x80;
                    }
                }
            }
            Ok(())
        }
    }

    #[test]
    fn index_crc_attribution_transient_and_persistent_at_both_sites() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
        for site in ["open", "readback"] {
            for persistent in [false, true] {
                let bytes = build_run(&firehose(600), RunBuildOpts {
                    block_target_bytes: 128, bloom_bits_per_key: 10, num_buckets: 8,
                });
                let ft = decode_footer(&bytes[bytes.len() - FOOTER_LEN..]).unwrap();
                let dev = Arc::new(IndexCorruptDev {
                    inner: MemBlockDevice::new(512, 8192),
                    target: std::sync::Mutex::new(None),
                    corrupt_next: AtomicBool::new(false),
                    vary_reads: AtomicBool::new(false),
                    target_reads: AtomicUsize::new(0),
                });
                let mut alloc = Allocator::new(8192, 8, 3);
                for i in (0..alloc.num_segments()).step_by(2) {
                    alloc.mark_allocated(SegmentId(i)).unwrap();
                }
                let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
                let seg_bytes = 4096;
                assert!(ft.index_len > 2 * seg_bytes as u64);
                let seg_lbas: Vec<u64> = handle.segs.iter()
                    .map(|s| alloc.segment_range(*s).0).collect();
                let run_off = ft.index_off + seg_bytes as u64 + 17;
                let seg_index = run_off as usize / seg_bytes;
                let within = run_off as usize % seg_bytes;
                let lba = seg_lbas[seg_index] + (within / 512) as u64;
                *dev.target.lock().unwrap() = Some(lba * 512 + (within % 512) as u64);
                if persistent {
                    let mut block = vec![0; 512];
                    dev.inner.read_at(lba, &mut block).unwrap();
                    block[within % 512] ^= 0x80;
                    dev.inner.write_at(lba, &block).unwrap();
                }
                dev.corrupt_next.store(!persistent, Relaxed);
                let first = read_run_range_lbas(&*dev, &seg_lbas, seg_bytes,
                    ft.index_off, ft.index_len as usize).unwrap();
                assert_eq!(decode_index(&first, ft.version).unwrap_err(), "index crc mismatch");
                let evidence = inspect_index_crc(&*dev, &seg_lbas, seg_bytes, &ft, &first);
                assert_eq!(evidence.verdict, if persistent { "persistent" } else { "transient" });
                assert!(!evidence.first_crc.passes());
                assert_eq!(evidence.reread_crc[0].as_ref().unwrap().passes(), !persistent);
                assert_eq!(evidence.reread_crc[0], evidence.reread_crc[1]);
                let diff = evidence.diff.as_ref().unwrap();
                assert_eq!(diff.bytes, usize::from(!persistent));
                if !persistent {
                    let location = IndexDiffLocation {
                        run_off, seg_index, lba, byte_in_block: within % 512,
                    };
                    assert_eq!(diff.first.as_ref(), Some(&location));
                    assert_eq!(diff.last.as_ref(), Some(&location));
                    assert!(!diff.first_read_all_zero);
                    assert!(!diff.span_4k_aligned);
                    assert!(!diff.span_128k_aligned);
                } else {
                    assert_eq!(diff.first, None);
                    assert_eq!(diff.last, None);
                }
                let warning = IndexCrcWarning {
                    site, handle: &handle, seg_lbas: &seg_lbas, seg_bytes,
                    block_size: 512, device_key: dev.maintenance_sibling_cache_key(),
                    ft: &ft, evidence: &evidence,
                }.to_string();
                assert!(warning.starts_with(&format!("WARN run index crc mismatch site={site}")));
                assert!(warning.contains(&format!("verdict={}", evidence.verdict)));
                dev.corrupt_next.store(!persistent, Relaxed);
                dev.target_reads.store(0, Relaxed);
                let result = if site == "open" {
                    DeviceRun::open(dev.clone(), &alloc, handle.clone()).map(|_| ())
                } else {
                    read_run_parts(&*dev, &handle, &seg_lbas, seg_bytes).map(|_| ())
                };
                assert!(matches!(result, Err(SnapshotError::Corrupt("index crc mismatch"))));
                assert_eq!(dev.target_reads.load(Relaxed), 3);
                let retry = read_run_parts(&*dev, &handle, &seg_lbas, seg_bytes);
                assert_eq!(retry.is_ok(), !persistent);
                if !persistent {
                    dev.target_reads.store(0, Relaxed);
                    dev.vary_reads.store(true, Relaxed);
                    let first = read_run_range_lbas(&*dev, &seg_lbas, seg_bytes,
                        ft.index_off, ft.index_len as usize).unwrap();
                    let evidence = inspect_index_crc(&*dev, &seg_lbas, seg_bytes, &ft, &first);
                    assert_eq!(evidence.verdict, "unstable");
                    assert!(evidence.reread_crc.iter().all(|r| !r.as_ref().unwrap().passes()));
                    assert_eq!(evidence.diff.unwrap().bytes, 1);
                }
            }
        }
    }

    #[test]
    fn index_crc_diff_zero_span_crosses_noncontiguous_segments() {
        let dev = MemBlockDevice::new(512, 1024);
        let seg_bytes = 4096;
        let seg_lbas = [3, 35, 19, 67];
        let mut builder = FenceBuilder::default();
        builder.push(&vec![b'x'; 12300], 0, 32).unwrap();
        let index = encode_index_bytes(&builder.finish(), None);
        let ft = Footer {
            entry_count: 1, index_off: 0, index_len: index.len() as u64,
            bloom_off: 0, bloom_len: 0, sidecar_off: 0, sidecar_len: 0,
            num_buckets: 1, version: RUN_VERSION,
        };
        for (i, &lba) in seg_lbas.iter().enumerate() {
            dev.write_at(lba, &run_seg_write_buf(&index, i, seg_bytes, 512)).unwrap();
        }
        let mut first = index.clone();
        first[4096..12288].fill(0);
        let evidence = inspect_index_crc(&dev, &seg_lbas, seg_bytes, &ft, &first);
        assert_eq!(evidence.verdict, "transient");
        let diff = evidence.diff.unwrap();
        assert_eq!(diff.bytes, 8192);
        assert_eq!(diff.first, Some(IndexDiffLocation {
            run_off: 4096, seg_index: 1, lba: 35, byte_in_block: 0,
        }));
        assert_eq!(diff.last, Some(IndexDiffLocation {
            run_off: 12287, seg_index: 2, lba: 26, byte_in_block: 511,
        }));
        assert!(diff.first_read_all_zero);
        assert!(diff.span_4k_aligned);
        assert!(!diff.span_128k_aligned);
    }

    struct BatchCountingDev {
        inner: MemBlockDevice,
        batches: std::sync::Mutex<Vec<usize>>,
    }

    impl BatchCountingDev {
        fn new(block_size: usize, num_blocks: u64) -> Self {
            BatchCountingDev {
                inner: MemBlockDevice::new(block_size, num_blocks),
                batches: std::sync::Mutex::new(Vec::new()),
            }
        }
        fn take_batches(&self) -> Vec<usize> {
            std::mem::take(&mut self.batches.lock().unwrap())
        }
    }

    impl BlockDevice for BatchCountingDev {
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
            self.inner.write_at(lba, buf)
        }
        fn flush(&self) -> vals_spdk::Result<()> {
            self.inner.flush()
        }
        fn read_batch(&self, ops: &mut [vals_spdk::ReadOp<'_>]) -> vals_spdk::Result<()> {
            self.batches.lock().unwrap().push(ops.len());
            self.inner.read_batch(ops)
        }
    }

    fn collect_prefetch<D: BlockDevice>(mut s: RunPrefetchScan<'_, D>) -> Vec<SnapshotEntry> {
        let mut out = Vec::new();
        while let Some(e) = s.next_entry().unwrap() {
            out.push(e);
        }
        out
    }

    #[test]
    fn prefetch_scan_matches_block_scan_and_issues_qd_batches() {
        let entries = firehose(600);
        let opts = RunBuildOpts {
            block_target_bytes: 256,
            bloom_bits_per_key: 10,
            num_buckets: 8,
        };
        let bytes = build_run(&entries, opts);
        let dev = Arc::new(BatchCountingDev::new(512, 8192));
        let mut alloc = Allocator::new(8192, 8, 0);
        let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
        assert!(handle.segs.len() > 2, "expected a multi-segment run");
        let run = DeviceRun::open(dev.clone(), &alloc, handle).unwrap();

        let mut want = Vec::new();
        let mut s = run.block_scan(None, None);
        while let Some(e) = s.next(&alloc).unwrap() {
            want.push(e);
        }
        assert_eq!(want, entries, "sanity: full scan yields every entry");

        dev.take_batches();
        let got = collect_prefetch(run.prefetch_scan(&alloc, 4096, 8));
        assert_eq!(got, want);
        let batches = dev.take_batches();
        assert!(!batches.is_empty(), "read-ahead must go through read_batch");
        assert!(
            batches.iter().any(|&n| n > 1),
            "expected a read_batch with ops > 1 (QD > 1), got {batches:?}"
        );
    }

    #[test]
    fn prefetch_scan_edge_cases() {
        let opts = RunBuildOpts {
            block_target_bytes: 256,
            bloom_bits_per_key: 10,
            num_buckets: 4,
        };

        let one = vec![entry(b"only-key", 1, 42)];
        let (dev, mut alloc) = dev_alloc(256, 4);
        let handle = write_run(&*dev, &mut alloc, &build_run(&one, opts)).unwrap();
        let run = DeviceRun::open(dev, &alloc, handle).unwrap();
        assert_eq!(collect_prefetch(run.prefetch_scan(&alloc, 1024, 4)), one);

        let (dev, mut alloc) = dev_alloc(256, 4);
        let handle = write_run(&*dev, &mut alloc, &build_run(&[], opts)).unwrap();
        let run = DeviceRun::open(dev, &alloc, handle).unwrap();
        assert_eq!(collect_prefetch(run.prefetch_scan(&alloc, 1024, 4)), vec![]);

        let entries = firehose(200);
        let (dev, mut alloc) = dev_alloc(2048, 4);
        let handle = write_run(&*dev, &mut alloc, &build_run(&entries, opts)).unwrap();
        let run = DeviceRun::open(dev, &alloc, handle).unwrap();
        assert_eq!(collect_prefetch(run.prefetch_scan(&alloc, 1, 1)), entries);

        assert_eq!(
            collect_prefetch(run.prefetch_scan(&alloc, usize::MAX, 8)),
            entries
        );
    }

    #[test]
    fn read_run_range_batched_matches_sequential() {
        let entries = firehose(400);
        let opts = RunBuildOpts {
            block_target_bytes: 512,
            bloom_bits_per_key: 10,
            num_buckets: 8,
        };
        let bytes = build_run(&entries, opts);
        let dev = Arc::new(BatchCountingDev::new(512, 8192));
        let mut alloc = Allocator::new(8192, 8, 0);
        let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
        let seg_bytes = alloc.blocks_per_segment() as usize * dev.block_size();
        let seg_lbas: Vec<u64> = handle
            .segs
            .iter()
            .map(|s| alloc.segment_range(*s).0)
            .collect();
        let len = handle.len as usize;
        let cases: [(u64, usize); 7] = [
            (0, len),
            (0, 1),
            (37, 1000),
            (seg_bytes as u64 - 100, 200),
            (0, seg_bytes * 2 + 513),
            (len as u64 - 1, 1),
            (513, seg_bytes),
        ];
        for (off, l) in cases {
            let l = l.min(len - off as usize);
            let want =
                read_run_range_lbas(&*dev, &seg_lbas, seg_bytes, off, l).unwrap();
            for qd in [1usize, 3, 8, 64] {
                let got =
                    read_run_range_batched(&*dev, &seg_lbas, seg_bytes, off, l, qd).unwrap();
                assert_eq!(got, want, "range ({off}, {l}) at qd {qd}");
            }
        }
        assert!(read_run_range_batched(&*dev, &seg_lbas, seg_bytes, handle.len, len, 8).is_err());
    }

    #[test]
    #[ignore]
    fn bench_v4_v5_run_bytes_and_version_stream_cpu() {
        let n = 400_000usize;
        let mut rng = Rng(0x0365_bac0_0000_0001);
        let mut keys: Vec<Vec<u8>> = (0..n)
            .map(|i| {
                let subject = (i / 4) as u64;
                let mut key = format!(
                    "at://did:plc:{:024x}/app.bsky.feed.post/3k{:011}|app.bsky.feed.like|did:plc:{:024x}/3l",
                    splitmix64(subject) as u128 * 7, subject, rng.next() as u128
                ).into_bytes();
                while key.len() < 148 {
                    key.push(b"234567abcdefghijklmnopqrstuvwxyz"[rng.range(0, 31) as usize]);
                }
                key
            })
            .collect();
        keys.sort();
        keys.dedup();
        let entries: Vec<SnapshotEntry> = keys
            .into_iter()
            .enumerate()
            .map(|(i, key)| {
                let mut e = entry(&key, i as u64, ((rng.next() as u128) << 64) | i as u128);
                e.ptr.len = 9 + 40;
                e
            })
            .collect();
        let n = entries.len();
        let avg_key = entries.iter().map(|e| e.key.len()).sum::<usize>() as f64 / n as f64;
        println!("entries={n} avg_key={avg_key:.1}B");
        let opts = RunBuildOpts::default();
        for version in [RUN_SIDECAR_VERSION, RUN_VERSION] {
            let bytes = build_run_versioned(&entries, opts, version);
            let dev = Arc::new(MemBlockDevice::new(BS, (bytes.len() as u64 * 3) / BS as u64 + 4096));
            let mut alloc = Allocator::new(dev.num_blocks(), 256, 0);
            let seg_bytes = alloc.blocks_per_segment() as usize * BS;
            let (segs, lbas) = reserve_for(&mut alloc, &entries, opts, version);
            struct ReadBytes(MemBlockDevice, std::sync::atomic::AtomicU64);
            let reads = ReadBytes(dev.share(), std::sync::atomic::AtomicU64::new(0));
            impl BlockDevice for ReadBytes {
                fn block_size(&self) -> usize { self.0.block_size() }
                fn num_blocks(&self) -> u64 { self.0.num_blocks() }
                fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
                    self.1.fetch_add(buf.len() as u64, std::sync::atomic::Ordering::Relaxed);
                    self.0.read_at(lba, buf)
                }
                fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> { self.0.write_at(lba, buf) }
                fn flush(&self) -> vals_spdk::Result<()> { self.0.flush() }
            }
            let t = std::time::Instant::now();
            let sink = SegmentStreamSink::new(&reads, &segs, &lbas, seg_bytes, BS);
            let mut builder = RunStreamBuilder::new(sink, opts, n).version(version);
            for e in &entries {
                builder.push(e);
            }
            let handle = block_on(builder.finish()).unwrap();
            let build = t.elapsed();
            let readback = reads.1.load(std::sync::atomic::Ordering::Relaxed);
            assert_eq!(handle.len as usize, bytes.len());
            let run = DeviceRun::open(dev.clone(), &alloc, handle).unwrap();
            let mut best_seed = f64::MAX;
            let mut best_async = f64::MAX;
            let mut best_fold = f64::MAX;
            for _ in 0..3 {
                let t = std::time::Instant::now();
                let mut stream = run.version_stream(&alloc).unwrap().unwrap();
                let mut count = 0usize;
                loop {
                    match stream.next_step().unwrap() {
                        SidecarCursorStep::Entry(_) => count += 1,
                        SidecarCursorStep::Done => break,
                        SidecarCursorStep::Checkpoint => {}
                    }
                }
                assert_eq!(count, n);
                best_seed = best_seed.min(t.elapsed().as_nanos() as f64 / n as f64);
                let want: std::collections::HashSet<u32> = (0..8).collect();
                let bucket_of = |k: &[u8]| test_bucket(k) % 16;
                let t = std::time::Instant::now();
                let got = drain_version_source(&dev, run.version_source(&alloc, None), &want, &bucket_of, None).unwrap();
                assert!(got.len() > n / 3);
                best_async = best_async.min(t.elapsed().as_nanos() as f64 / n as f64);
                let t = std::time::Instant::now();
                let mut folded = 0usize;
                run.for_each_bucket_digest(&alloc, |_, _| folded += 1).unwrap();
                assert_eq!(folded, n);
                best_fold = best_fold.min(t.elapsed().as_nanos() as f64 / n as f64);
            }
            println!(
                "v{version}: run {:.1} B/entry (sidecar {:.1} B/entry), streamed build {:.0} ns/entry with {:.1} B/entry build readback; seed stream {best_seed:.0} ns/entry, async digest cursor {best_async:.0} ns/entry, for_each_bucket_digest {best_fold:.0} ns/entry",
                bytes.len() as f64 / n as f64,
                run.sidecar_len() as f64 / n as f64,
                build.as_nanos() as f64 / n as f64,
                readback as f64 / n as f64,
            );
        }
    }

    #[test]
    #[ignore]
    fn bench_prefetch_scan_file_device() {
        use vals_spdk::FileBlockDevice;
        let n = 1_000_000usize;
        let entries = firehose(n);
        let opts = RunBuildOpts::default();
        let bytes = build_run(&entries, opts);
        let path = std::env::temp_dir().join(format!(
            "vals-bench-prefetch-{}.img",
            std::process::id()
        ));
        let seg_blocks = 512u64;
        let blocks = (bytes.len() as u64 / BS as u64 + 2 * seg_blocks) / seg_blocks * seg_blocks
            + seg_blocks;
        let dev = Arc::new(FileBlockDevice::create(&path, BS, blocks).unwrap());
        let mut alloc = Allocator::new(blocks, seg_blocks, 0);
        let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
        let run = DeviceRun::open(dev.clone(), &alloc, handle.clone()).unwrap();
        let seg_bytes = alloc.blocks_per_segment() as usize * BS;
        let seg_lbas: Vec<u64> = handle
            .segs
            .iter()
            .map(|s| alloc.segment_range(*s).0)
            .collect();
        let (_, last_off, last_len) = run.index.last().unwrap();
        let data_len = (*last_off + *last_len as u64) as usize;
        let mibs = data_len as f64 / (1024.0 * 1024.0);
        println!(
            "run: {n} entries, {:.1} MiB data blocks, {} segments",
            mibs,
            handle.segs.len()
        );
        let rate = |d: std::time::Duration| data_len as f64 / d.as_secs_f64() / 1e6;

        for round in 0..3 {
            let t = std::time::Instant::now();
            let mut sink = 0usize;
            for (_, off, len) in run.index.iter() {
                sink += read_run_range_lbas(&*dev, &seg_lbas, seg_bytes, *off, *len as usize)
                    .unwrap()
                    .len();
            }
            let per_block = t.elapsed();
            let t = std::time::Instant::now();
            let mut off = 0usize;
            while off < data_len {
                let l = (data_len - off).min(2 * 1024 * 1024);
                sink += read_run_range_batched(&*dev, &seg_lbas, seg_bytes, off as u64, l, 8)
                    .unwrap()
                    .len();
                off += l;
            }
            let windowed = t.elapsed();
            let t = std::time::Instant::now();
            let mut scan = run.block_scan(None, None);
            let mut count = 0usize;
            while let Some(e) = scan.next(&alloc).unwrap() {
                count += e.key.len();
            }
            let scan_drain = t.elapsed();
            let t = std::time::Instant::now();
            let mut pre = run.prefetch_scan(&alloc, 2 * 1024 * 1024, 8);
            let mut count2 = 0usize;
            while let Some(e) = pre.next_entry().unwrap() {
                count2 += e.key.len();
            }
            let pre_drain = t.elapsed();
            assert_eq!(count, count2);
            println!(
                "round {round}: raw per-block {:.0} MB/s | raw windowed {:.0} MB/s ({:.2}x) | \
                 RunScan drain {:.0} MB/s | prefetch drain {:.0} MB/s ({:.2}x) [sink {sink}]",
                rate(per_block),
                rate(windowed),
                rate(windowed) / rate(per_block),
                rate(scan_drain),
                rate(pre_drain),
                rate(pre_drain) / rate(scan_drain),
            );
        }
        drop(run);
        let _ = std::fs::remove_file(&path);
    }

    fn block_lookup_alloc_reference(bytes: &[u8], key: &[u8]) -> Result<Option<SnapshotEntry>, &'static str> {
        let entries = decode_block(bytes)?;
        for e in entries {
            match e.key.as_slice().cmp(key) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => return Ok(Some(e)),
                std::cmp::Ordering::Greater => return Ok(None),
            }
        }
        Ok(None)
    }

    fn rand_key(rng: &mut Rng) -> Vec<u8> {
        let len = rng.range(0, 6) as usize;
        (0..len).map(|_| b"abc\x00\xff"[rng.range(0, 4) as usize]).collect()
    }

    #[test]
    fn in_place_block_lookup_matches_decoding_lookup_over_random_blocks() {
        let mut rng = Rng(0x0378_b10c_0000_0001);
        for _ in 0..3000 {
            let n = rng.range(0, 40) as usize;
            let mut keys: Vec<Vec<u8>> = (0..n).map(|_| rand_key(&mut rng)).collect();
            keys.sort();
            if rng.next() & 1 == 0 {
                keys.dedup();
            }
            let entries: Vec<SnapshotEntry> = keys
                .iter()
                .enumerate()
                .map(|(i, k)| {
                    let mut e = entry(k, i as u64, rng.next() as u128);
                    e.tombstone = rng.next() & 3 == 0;
                    e
                })
                .collect();
            let block = encode_block(&entries);
            for _ in 0..20 {
                let probe = if !keys.is_empty() && rng.next() & 1 == 0 {
                    keys[rng.range(0, keys.len() as u64 - 1) as usize].clone()
                } else {
                    rand_key(&mut rng)
                };
                assert_eq!(block_lookup(&block, &probe), block_lookup_alloc_reference(&block, &probe), "keys {keys:?} probe {probe:?}");
            }
        }
        let mut bad = encode_block(&[entry(b"ab", 1, 1), entry(b"ac", 2, 2)]);
        bad[4..8].copy_from_slice(&0u32.to_le_bytes());
        let body = bad.len() - 4;
        let at = 4 + 8 + 2 + 40;
        bad[at..at + 4].copy_from_slice(&9u32.to_le_bytes());
        let crc = crc::crc32(&bad[..body]).to_le_bytes();
        bad[body..].copy_from_slice(&crc);
        assert_eq!(block_lookup(&bad, b"zz"), Err("shared prefix longer than previous key"));
        assert_eq!(block_lookup(&bad, b"zz"), block_lookup_alloc_reference(&bad, b"zz"));
    }

    #[test]
    fn block_cursor_seeks_in_place_and_decodes_lazily_like_decode_block() {
        let mut rng = Rng(0x0378_b10c_0000_0002);
        for _ in 0..3000 {
            let n = rng.range(0, 40) as usize;
            let mut keys: Vec<Vec<u8>> = (0..n).map(|_| rand_key(&mut rng)).collect();
            keys.sort();
            keys.dedup();
            let entries: Vec<SnapshotEntry> = keys
                .iter()
                .enumerate()
                .map(|(i, k)| {
                    let mut e = entry(k, i as u64, rng.next() as u128);
                    e.tombstone = rng.next() & 3 == 0;
                    e
                })
                .collect();
            let block = encode_block(&entries);
            for _ in 0..12 {
                let lo = if rng.next() % 5 == 0 {
                    None
                } else if !keys.is_empty() && rng.next() & 1 == 0 {
                    Some(keys[rng.range(0, keys.len() as u64 - 1) as usize].clone())
                } else {
                    Some(rand_key(&mut rng))
                };
                let exclusive = rng.next() & 1 == 0;
                let want: Vec<SnapshotEntry> = entries
                    .iter()
                    .filter(|e| lo.as_ref().is_none_or(|l| if exclusive { e.key > *l } else { e.key >= *l }))
                    .cloned()
                    .collect();
                let mut cursor = BlockCursor::new(block.clone(), lo.as_deref(), exclusive).unwrap();
                let mut got = Vec::new();
                while let Some(e) = cursor.next_entry().unwrap() {
                    got.push(e);
                }
                assert_eq!(got, want, "keys {keys:?} lo {lo:?} exclusive {exclusive}");
            }
        }
        let mut bad = encode_block(&firehose(8));
        let n = bad.len();
        bad[n / 2] ^= 0xff;
        assert_eq!(BlockCursor::new(bad, None, false).err(), Some("crc mismatch"));
        assert!(BlockCursor::new(vec![0; 4], None, false).is_err());
    }

    #[test]
    fn block_target_and_bloom_knobs_parse_within_bounds() {
        assert_eq!(parse_run_block_target_bytes(None), 32 * 1024);
        assert_eq!(parse_run_block_target_bytes(Some(" ")), 32 * 1024);
        assert_eq!(parse_run_block_target_bytes(Some("4096")), 4096);
        assert_eq!(parse_run_block_target_bytes(Some("262144")), 262144);
        assert_eq!(parse_run_block_target_bytes(Some("65536 ")), 65536);
        assert_eq!(parse_run_block_target_bytes(Some("100")), 4096);
        assert_eq!(parse_run_block_target_bytes(Some("262145")), 262144);
        assert_eq!(parse_run_block_target_bytes(Some("32k")), 32 * 1024);
        assert_eq!(parse_run_block_target_bytes(Some("-1")), 32 * 1024);
        assert_eq!(parse_run_bloom_bits_per_key(None), 10);
        assert_eq!(parse_run_bloom_bits_per_key(Some("12")), 12);
        assert_eq!(parse_run_bloom_bits_per_key(Some("1")), 4);
        assert_eq!(parse_run_bloom_bits_per_key(Some("99")), 24);
        assert_eq!(parse_run_bloom_bits_per_key(Some("x")), 10);
        assert_eq!(with_run_block_target(4096, || RunBuildOpts::default().block_target_bytes), 4096);
        assert_eq!(RunBuildOpts::default().block_target_bytes, run_block_target_bytes());
    }

    fn backlink_entries(n: usize, seed: u64) -> Vec<SnapshotEntry> {
        let mut rng = Rng(seed);
        let mut keys: Vec<Vec<u8>> = (0..n)
            .map(|i| {
                let subject = (i / 4) as u64;
                let mut key = format!(
                    "at://did:plc:{:024x}/app.bsky.feed.post/3k{:011}|app.bsky.feed.like|did:plc:{:024x}/3l",
                    splitmix64(subject) as u128 * 7, subject, rng.next() as u128
                ).into_bytes();
                while key.len() < 148 {
                    key.push(b"234567abcdefghijklmnopqrstuvwxyz"[rng.range(0, 31) as usize]);
                }
                key
            })
            .collect();
        keys.sort();
        keys.dedup();
        keys.iter().enumerate().map(|(i, k)| entry(k, i as u64, i as u128 + 1)).collect()
    }

    fn record_entries(n: usize, seed: u64) -> Vec<SnapshotEntry> {
        let mut rng = Rng(seed);
        let mut keys: Vec<Vec<u8>> = (0..n)
            .map(|i| {
                let did = splitmix64((i / 16) as u64);
                format!("at://did:plc:{did:024x}/app.bsky.feed.post/3k{:011}", rng.next() % 100_000_000_000)
                    .into_bytes()
            })
            .collect();
        keys.sort();
        keys.dedup();
        keys.iter().enumerate().map(|(i, k)| entry(k, i as u64, i as u128 + 1)).collect()
    }

    fn open_built(entries: &[SnapshotEntry], opts: RunBuildOpts) -> (Arc<MemBlockDevice>, Allocator, DeviceRun<MemBlockDevice>) {
        let bytes = build_run_versioned(entries, opts, RUN_VERSION);
        let (dev, mut alloc) = dev_alloc((bytes.len() / BS) as u64 * 2 + 64, 256);
        let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
        let run = DeviceRun::open(dev.clone(), &alloc, handle).unwrap();
        (dev, alloc, run)
    }

    #[test]
    fn scan_open_matches_serving_scans_with_tombstones() {
        let mut entries = backlink_entries(2_000, 0x0397);
        for e in entries.iter_mut().step_by(7) { e.tombstone = true; }
        for version in RUN_SIDECAR_VERSION..=RUN_VERSION {
            let bytes = build_run_versioned(&entries, RunBuildOpts::default(), version);
            let (dev, mut alloc) = dev_alloc((bytes.len() / BS) as u64 * 2 + 1024, 256);
            let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
            let scan_run = DeviceRun::open_scan(dev.clone(), &alloc, handle.clone()).unwrap();
            let serving = DeviceRun::open(dev, &alloc, handle).unwrap();
            assert_eq!(scan_run.resident_stats().filter_bytes, 0);
            assert!(serving.resident_stats().filter_bytes > 0);
            let mut scan = scan_run.block_scan(None, None);
            let mut prefetched = scan_run.prefetch_scan(&alloc, 64 << 10, 2);
            for expected in &entries {
                assert_eq!(scan.next(&alloc).unwrap().as_ref(), Some(expected));
                assert_eq!(prefetched.next_entry().unwrap().as_ref(), Some(expected));
                assert_eq!(serving.lookup(&alloc, &expected.key).unwrap().as_ref(), Some(expected));
            }
            assert!(scan.next(&alloc).unwrap().is_none());
            assert!(prefetched.next_entry().unwrap().is_none());
        }
    }

    #[test]
    fn default_runs_build_32k_fences_and_10_bit_blooms() {
        let entries = backlink_entries(20_000, 0x0378_0000_0000_0002);
        let n = entries.len();
        assert_eq!(n, 20_000);
        let (_dev, alloc, run) = open_built(&entries, RunBuildOpts::default());
        let per_block = n / run.index_len();
        assert!((150..=200).contains(&per_block), "{per_block} entries per default block");
        assert!((12 * 1024..=34 * 1024).contains(&run.max_block_bytes()), "max block {}", run.max_block_bytes());
        let stats = run.resident_stats();
        let fence_per_entry = stats.fence_bytes as f64 / n as f64;
        assert!(fence_per_entry < 1.2, "{fence_per_entry} fence B/entry");
        let RunFilter::Blocked(bloom) = &run.bloom else { panic!("v5 runs use the blocked bloom") };
        assert_eq!(bloom.k, 7);
        assert_eq!(bloom.num_bits, (n as u64 * 10).next_multiple_of(BLOOM_BLOCK_BITS));
        for e in entries.iter().step_by(37) {
            assert_eq!(run.lookup(&alloc, &e.key).unwrap().as_ref(), Some(e));
        }
    }

    #[test]
    fn old_12_bit_4k_runs_still_read_and_10_bits_hold_fpr() {
        let entries = record_entries(30_000, 0x0378_0000_0000_0003);
        let n = entries.len();
        let old = RunBuildOpts { block_target_bytes: 4096, bloom_bits_per_key: 12, num_buckets: 4096 };
        let (_d12, a12, r12) = open_built(&entries, old);
        let (_d10, a10, r10) = open_built(&entries, RunBuildOpts { block_target_bytes: 32 * 1024, bloom_bits_per_key: 10, num_buckets: 4096 });
        let (RunFilter::Blocked(b12), RunFilter::Blocked(b10)) = (&r12.bloom, &r10.bloom) else { panic!() };
        assert_eq!((b12.k, b10.k), (8, 7));
        assert!(r12.index_len() > 6 * r10.index_len());
        for e in entries.iter().step_by(11) {
            assert_eq!(r12.lookup(&a12, &e.key).unwrap().as_ref(), Some(e));
            assert_eq!(r10.lookup(&a10, &e.key).unwrap().as_ref(), Some(e));
        }
        let probes = 200_000usize;
        let fpr = |r: &DeviceRun<MemBlockDevice>| {
            (0..probes).filter(|i| r.might_contain(format!("absent-{i:09}").as_bytes())).count() as f64 / probes as f64
        };
        let (f12, f10) = (fpr(&r12), fpr(&r10));
        eprintln!("run bloom fpr: 12 bits/key {f12:.5}, 10 bits/key {f10:.5} over {n} keys");
        assert!(f12 < 0.006, "12-bit fpr {f12}");
        assert!(f10 < 0.013, "10-bit fpr {f10}");
    }

    #[test]
    fn block_larger_than_the_stream_window_streams_seeds_digests_and_keys() {
        let entries = backlink_entries(8_000, 0x0378_0000_0000_0004);
        let opts = RunBuildOpts { block_target_bytes: SIDECAR_CURSOR_WINDOW * 3, bloom_bits_per_key: 10, num_buckets: 16 };
        let (dev, alloc, run) = build_streamed(&entries, opts, entries.len(), 8192, 16);
        assert!(run.max_block_bytes() > SIDECAR_CURSOR_WINDOW, "max block {}", run.max_block_bytes());
        assert!(run.index_len() >= 2);
        let want_pairs: Vec<(Vec<u8>, u128)> = entries.iter().map(|e| (e.key.clone(), e.version)).collect();
        let mut stream = run.version_stream(&alloc).unwrap().unwrap();
        let mut got = Vec::new();
        let mut reserved_max = 0u64;
        let mut bound_max = 0u64;
        loop {
            match stream.next_step_reserving(|b| { reserved_max = reserved_max.max(b); true }).unwrap().unwrap() {
                SidecarCursorStep::Entry(pair) => got.push(pair),
                SidecarCursorStep::Checkpoint => bound_max = bound_max.max(stream.resident_bound_bytes()),
                SidecarCursorStep::Done => break,
            }
        }
        assert_eq!(got, want_pairs);
        assert!(bound_max >= run.max_block_bytes() as u64, "resident bound {bound_max} covers the oversized window");
        assert!(reserved_max >= 2 * SIDECAR_CURSOR_WINDOW as u64, "an oversized window reserves its full length");
        let source = run.version_source(&alloc, None);
        assert!(source.resident_bound_bytes() >= 3 * run.max_block_bytes());
        let all: std::collections::HashSet<u32> = (0..16).collect();
        let bucket_of = |k: &[u8]| test_bucket(k) % 16;
        let drained = drain_version_source(&dev, source, &all, &bucket_of, None).unwrap();
        assert_eq!(drained, want_pairs);
        let mid = &entries[entries.len() / 2].key;
        let after = drain_version_source(&dev, run.version_source(&alloc, Some(mid)), &all, &bucket_of, Some(mid)).unwrap();
        assert_eq!(after.len(), entries.len() - entries.len() / 2 - 1);
        let mut folded = Vec::new();
        run.for_each_bucket_digest(&alloc, |k, v| folded.push((k.to_vec(), v))).unwrap();
        assert_eq!(folded, want_pairs);
        let mut keys = Vec::new();
        let mut source = run.key_source(&alloc);
        while !source.step(&*dev, 4096, |k| keys.push(k.to_vec())).unwrap() {}
        assert_eq!(keys.len(), entries.len());
        let (page, stop) = run.want_bucket_digests_page(&alloc, &all, bucket_of, Some(mid), 4096, 0).unwrap();
        assert!(!page.is_empty() && stop.is_some());
        assert!(page[0].0.as_slice() > mid.as_slice());
        for e in entries.iter().step_by(97) {
            assert_eq!(run.lookup(&alloc, &e.key).unwrap().as_ref(), Some(e));
        }
    }

    #[test]
    #[ignore]
    fn bench_block_target_fence_filter_and_point_lookup() {
        let n = 400_000usize;
        let shapes: [(&str, Vec<SnapshotEntry>); 2] = [
            ("backlinks", backlink_entries(n, 0x0378_bbbb_0000_0001)),
            ("records", record_entries(n, 0x0378_cccc_0000_0001)),
        ];
        let device_bs = 4096usize;
        for (name, entries) in &shapes {
            let n = entries.len();
            let avg_key = entries.iter().map(|e| e.key.len()).sum::<usize>() as f64 / n as f64;
            for (block, bits) in [(4096usize, 12usize), (32 * 1024, 10)] {
                let opts = RunBuildOpts { block_target_bytes: block, bloom_bits_per_key: bits, num_buckets: 4096 };
                let bytes = build_run_versioned(entries, opts, RUN_VERSION);
                let dev = Arc::new(MemBlockDevice::new(device_bs, (bytes.len() / device_bs) as u64 * 2 + 64));
                let mut alloc = Allocator::new(dev.num_blocks(), 256, 0);
                let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
                let run = DeviceRun::open(dev.clone(), &alloc, handle).unwrap();
                let stats = run.resident_stats();
                let malloc_fence = [run.index.iter().map(|(k, _, _)| k.len()).sum::<usize>(), run.index.len() * 16]
                    .into_iter().filter(|n| *n != 0).map(|n| (n + 8).next_multiple_of(16).max(32)).sum::<usize>();
                let mut rng = Rng(0x0378_1000 ^ block as u64);
                let probes: Vec<&[u8]> = (0..200_000).map(|_| entries[rng.range(0, n as u64 - 1) as usize].key.as_slice()).collect();
                let mut stored = 0u64;
                let mut aligned = 0u64;
                for k in &probes {
                    let loc = run.locate_block(&alloc, k).unwrap();
                    stored += loc.len() as u64;
                    let start = loc.off as usize / device_bs;
                    let end = (loc.off as usize + loc.len()).div_ceil(device_bs);
                    aligned += ((end - start) * device_bs) as u64;
                }
                let blocks: Vec<Vec<u8>> = probes.iter().map(|k| run.locate_block(&alloc, k).map(|l| block_on(l.read(&*dev)).unwrap()).unwrap()).collect();
                let mut best = [f64::MAX; 4];
                for _ in 0..3 {
                    let t = std::time::Instant::now();
                    for b in &blocks {
                        assert!(block_crc_ok(b));
                    }
                    best[3] = best[3].min(t.elapsed().as_nanos() as f64 / probes.len() as f64);
                    let t = std::time::Instant::now();
                    for (k, b) in probes.iter().zip(&blocks) {
                        assert!(block_lookup(b, k).unwrap().is_some());
                    }
                    best[0] = best[0].min(t.elapsed().as_nanos() as f64 / probes.len() as f64);
                    let t = std::time::Instant::now();
                    for (k, b) in probes.iter().zip(&blocks) {
                        assert!(block_lookup_alloc_reference(b, k).unwrap().is_some());
                    }
                    best[1] = best[1].min(t.elapsed().as_nanos() as f64 / probes.len() as f64);
                    let t = std::time::Instant::now();
                    for k in &probes {
                        assert!(run.lookup(&alloc, k).unwrap().is_some());
                    }
                    best[2] = best[2].min(t.elapsed().as_nanos() as f64 / probes.len() as f64);
                }
                println!(
                    "BLOCKBENCH {name} avg_key={avg_key:.1}B entries={n} block_target={block} bloom_bits={bits}: entries/block={:.1} fence={:.3} B/entry (gauge) {:.3} B/entry (malloc est) filter={:.3} B/entry; get bytes/lookup stored={:.0} aligned={:.0}; block_lookup in-place={:.0} ns (crc alone {:.0} ns) alloc-ref={:.0} ns; DeviceRun::lookup (mem device read + crc + search)={:.0} ns",
                    n as f64 / run.index_len() as f64,
                    stats.fence_bytes as f64 / n as f64,
                    malloc_fence as f64 / n as f64,
                    stats.filter_bytes as f64 / n as f64,
                    stored as f64 / probes.len() as f64,
                    aligned as f64 / probes.len() as f64,
                    best[0], best[3], best[1], best[2],
                );
            }
        }
    }
}

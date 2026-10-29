use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::hash::{BuildHasherDefault, Hasher};
use std::ops::Bound;

#[derive(Default)]
pub struct FxHasher {
    hash: u64,
}

const FX_SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;

impl FxHasher {
    #[inline]
    fn add(&mut self, i: u64) {
        self.hash = (self.hash.rotate_left(5) ^ i).wrapping_mul(FX_SEED);
    }
}

impl Hasher for FxHasher {
    #[inline]
    fn write(&mut self, mut bytes: &[u8]) {
        while bytes.len() >= 8 {
            self.add(u64::from_le_bytes(bytes[..8].try_into().unwrap()));
            bytes = &bytes[8..];
        }
        if !bytes.is_empty() {
            let mut buf = [0u8; 8];
            buf[..bytes.len()].copy_from_slice(bytes);
            self.add(u64::from_le_bytes(buf));
        }
    }
    #[inline]
    fn finish(&self) -> u64 {
        let mut z = self.hash;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

pub(crate) type FxBuildHasher = BuildHasherDefault<FxHasher>;

use crate::alloc::SegmentId;
use crate::snapshot::SnapshotEntry;
use crate::vlog::ValuePtr;
use crate::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub ptr: ValuePtr,
    pub seq: u64,
    pub version: u128,
    pub tombstone: bool,
}

#[derive(Clone, Copy)]
pub(crate) struct PackedEntry([u8; 36]);

const TOMBSTONE_BIT: u64 = 1 << 63;

const _: () = {
    assert!(std::mem::size_of::<PackedEntry>() == 36);
    assert!(std::mem::align_of::<PackedEntry>() == 1);
};

impl PackedEntry {
    #[inline]
    pub(crate) fn pack(e: &IndexEntry) -> PackedEntry {
        assert!(
            e.ptr.segment.0 <= u32::MAX as u64,
            "segment id {} exceeds the u32 packed-index-entry space",
            e.ptr.segment.0
        );
        assert!(
            e.seq < TOMBSTONE_BIT,
            "seq {} exceeds the 63-bit packed-index-entry space (top bit reserved for the tombstone flag)",
            e.seq
        );
        let mut b = [0u8; 36];
        b[0..4].copy_from_slice(&(e.ptr.segment.0 as u32).to_le_bytes());
        b[4..8].copy_from_slice(&e.ptr.offset.to_le_bytes());
        b[8..12].copy_from_slice(&e.ptr.len.to_le_bytes());
        let seq_word = e.seq | if e.tombstone { TOMBSTONE_BIT } else { 0 };
        b[12..20].copy_from_slice(&seq_word.to_le_bytes());
        b[20..36].copy_from_slice(&e.version.to_le_bytes());
        PackedEntry(b)
    }

    #[inline]
    pub(crate) fn unpack(&self) -> IndexEntry {
        let b = &self.0;
        let segment = SegmentId(u32::from_le_bytes(b[0..4].try_into().unwrap()) as u64);
        let offset = u32::from_le_bytes(b[4..8].try_into().unwrap());
        let len = u32::from_le_bytes(b[8..12].try_into().unwrap());
        let seq_word = u64::from_le_bytes(b[12..20].try_into().unwrap());
        let version = u128::from_le_bytes(b[20..36].try_into().unwrap());
        IndexEntry {
            ptr: ValuePtr {
                segment,
                offset,
                len,
            },
            seq: seq_word & !TOMBSTONE_BIT,
            version,
            tombstone: seq_word & TOMBSTONE_BIT != 0,
        }
    }

    #[inline]
    fn ptr_len(&self) -> u32 {
        u32::from_le_bytes(self.0[8..12].try_into().unwrap())
    }

    #[inline]
    fn version(&self) -> u128 {
        u128::from_le_bytes(self.0[20..36].try_into().unwrap())
    }
}

pub type IndexScanPage = (Vec<(Vec<u8>, IndexEntry)>, bool);

static INTEGRITY_FAILURES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn integrity_failures() -> u64 {
    INTEGRITY_FAILURES.load(std::sync::atomic::Ordering::Relaxed)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexIntegrityError {
    pub first_key: Box<[u8]>,
    pub entries: usize,
    pub packed_bytes: usize,
    pub finding: IntegrityFinding,
}

impl std::fmt::Display for IndexIntegrityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "front-coded index integrity mismatch block_first_key={:02x?} first_key_bytes={} entries={} packed_bytes={} {}",
            &self.first_key[..self.first_key.len().min(64)], self.first_key.len(), self.entries, self.packed_bytes, self.finding
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntegrityFinding {
    BitFlip {
        region: &'static str,
        key_index: usize,
        offset: usize,
        actual: u8,
        expected: u8,
        candidates: usize,
        partial: bool,
        window_start: usize,
        window: Box<[u8]>,
    },
    Unexplained {
        reason: &'static str,
        stored: u64,
        computed: Option<u64>,
        searched: bool,
    },
    AtSplit(Box<IntegrityFinding>),
}

impl IntegrityFinding {
    pub fn bit_flip_xor(&self) -> Option<u8> {
        match self {
            IntegrityFinding::BitFlip { actual, expected, candidates: 1, .. } => Some(actual ^ expected),
            IntegrityFinding::BitFlip { .. } => None,
            IntegrityFinding::Unexplained { .. } => None,
            IntegrityFinding::AtSplit(inner) => inner.bit_flip_xor(),
        }
    }

    pub fn search_partial(&self) -> bool {
        match self {
            IntegrityFinding::BitFlip { partial, .. } => *partial,
            IntegrityFinding::Unexplained { .. } => false,
            IntegrityFinding::AtSplit(inner) => inner.search_partial(),
        }
    }
}

impl std::fmt::Display for IntegrityFinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IntegrityFinding::BitFlip {
                region, key_index, offset, actual, expected, candidates, partial, window_start, window,
            } => {
                if *partial { write!(f, "search=partial reason=budget candidates_lower_bound=true ")?; }
                if *candidates > 1 { write!(f, "ambiguous=true candidate=first ")?; }
                write!(
                    f,
                    "diagnosis=single-bit region={region} key_index={key_index} offset={offset} \
                     actual=0x{actual:02x} expected=0x{expected:02x} xor=0x{:02x} candidates={candidates} \
                     window@{window_start}={window:02x?}",
                    actual ^ expected
                )
            },
            IntegrityFinding::Unexplained { reason, stored, computed, searched } => {
                write!(f, "diagnosis=unexplained reason={reason} stored=0x{stored:016x} computed=")?;
                match computed {
                    Some(c) => write!(f, "0x{c:016x}")?,
                    None => write!(f, "none")?,
                }
                write!(f, " search={}", if *searched { "done" } else { "skipped" })
            }
            IntegrityFinding::AtSplit(inner) => write!(f, "detected_at=split {inner}"),
        }
    }
}

pub trait Index: Send {
    fn get(&self, key: &[u8]) -> Option<IndexEntry>;

    fn put(&mut self, key: &[u8], entry: IndexEntry);

    fn remove(&mut self, key: &[u8]) -> Option<IndexEntry>;

    fn remove_if_eq(&mut self, key: &[u8], expected: &IndexEntry) -> bool {
        if self.get(key) == Some(*expected) {
            self.remove(key);
            true
        } else {
            false
        }
    }

    fn remove_if_eq_rev(&mut self, entries: &[SnapshotEntry]) -> usize {
        let mut removed = 0usize;
        for e in entries.iter().rev() {
            removed += self.remove_if_eq(&e.key, &snapshot_entry(e)) as usize;
        }
        removed
    }

    fn len(&self) -> usize;

    fn key_bytes(&self) -> u64;

    fn allocated_bytes(&self) -> u64;

    #[cfg(test)]
    fn hash_bucket_count(&self) -> usize { 0 }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn ordering(&self) -> Ordering;

    fn scan(&self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Option<Vec<(Vec<u8>, IndexEntry)>>;

    fn range_count_median(&self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Option<(usize, Option<Vec<u8>>)> {
        let mut keys = self.scan(lo, hi)?;
        let n = keys.len();
        Some((n, (n >= 2).then(|| keys.swap_remove(n / 2).0)))
    }

    fn scan_page(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Option<IndexScanPage>;

    fn scan_page_rev(
        &self,
        _lo: Option<&[u8]>,
        _hi: Option<&[u8]>,
        _max_entries: usize,
        _max_bytes: usize,
    ) -> Option<IndexScanPage> {
        None
    }

    fn maintenance_page(
        &self,
        after: Option<&[u8]>,
        max_entries: usize,
    ) -> (Vec<(Vec<u8>, IndexEntry)>, bool);

    fn flush_page(
        &self,
        after: Option<&[u8]>,
        max_entries: usize,
    ) -> Result<IndexScanPage, IndexIntegrityError> {
        self.flush_page_attested(after, max_entries, u64::MAX, &mut Vec::new())
    }

    fn flush_page_attested(
        &self,
        after: Option<&[u8]>,
        max_entries: usize,
        seq_ceiling: u64,
        groups: &mut Vec<SourceGroup>,
    ) -> Result<IndexScanPage, IndexIntegrityError> {
        let mut page: IndexScanPage = (Vec::new(), false);
        let mut cursor = after.map(<[u8]>::to_vec);
        loop {
            let (scanned, more) = self.maintenance_page(cursor.as_deref(), max_entries - page.0.len());
            page.1 = more;
            cursor = scanned.last().map(|(k, _)| k.clone());
            page.0.extend(scanned.into_iter().filter(|(_, e)| e.seq <= seq_ceiling));
            if !more || page.0.len() >= max_entries || cursor.is_none() {
                break;
            }
        }
        for chunk in page.0.chunks(HASH_PAGE_GROUP) {
            let mut g = SourceGroup::default();
            for (key, entry) in chunk {
                g.add(index_entry_hash(key, entry));
            }
            groups.push(g);
        }
        Ok(page)
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    fn flip_packed_bit(&mut self, _block: usize, _offset: usize, _mask: u8) {
        panic!("index is not front-coded");
    }

    fn keys(&self) -> Vec<Vec<u8>>;

    fn for_each_value_segment(&self, f: &mut dyn FnMut(SegmentId));

    fn versions(&self) -> Vec<(Box<[u8]>, u128)>;

    fn live_bytes(&self) -> u64;

    fn max_key_len(&self) -> u64;
}

#[inline]
fn snapshot_entry(e: &SnapshotEntry) -> IndexEntry {
    IndexEntry {
        ptr: e.ptr,
        seq: e.seq,
        version: e.version,
        tombstone: e.tombstone,
    }
}

pub fn new_index(ordering: Ordering) -> Box<dyn Index> {
    match ordering {
        Ordering::Sorted => Box::new(FrontCodedIndex::new()),
        Ordering::Unsorted => Box::new(HashIndex::new()),
    }
}

#[derive(Default)]
pub struct HashIndex {
    map: HashMap<Arc<[u8]>, PackedEntry, FxBuildHasher>,
    ordered: BTreeSet<Arc<[u8]>>,
    map_capacity: usize,
    key_bytes: u64,
    max_key_len: u64,
    live_ptr_bytes: u64,
}

impl HashIndex {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Index for HashIndex {
    fn get(&self, key: &[u8]) -> Option<IndexEntry> {
        self.map.get(key).map(|p| p.unpack())
    }

    fn put(&mut self, key: &[u8], entry: IndexEntry) {
        let packed = PackedEntry::pack(&entry);
        if let Some(slot) = self.map.get_mut(key) {
            let old_ptr_len = slot.ptr_len() as u64;
            *slot = packed;
            self.live_ptr_bytes = self.live_ptr_bytes + entry.ptr.len as u64 - old_ptr_len;
            return;
        }
        self.key_bytes += key.len() as u64;
        self.max_key_len = self.max_key_len.max(key.len() as u64);
        self.live_ptr_bytes += entry.ptr.len as u64;
        let owned: Arc<[u8]> = Arc::from(key);
        self.ordered.insert(owned.clone());
        self.map.insert(owned, packed);
        self.map_capacity = self.map_capacity.max(self.map.capacity());
    }

    fn remove(&mut self, key: &[u8]) -> Option<IndexEntry> {
        let removed = if self.map.len() == 1 {
            let p = self.map.get(key).copied();
            if p.is_some() { self.map.clear(); }
            p
        } else {
            self.map.remove(key)
        };
        if let Some(p) = &removed {
            self.ordered.remove(key);
            self.key_bytes -= key.len() as u64;
            self.live_ptr_bytes -= p.ptr_len() as u64;
            if self.map.is_empty() {
                self.max_key_len = 0;
            }
        }
        removed.map(|p| p.unpack())
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn key_bytes(&self) -> u64 {
        self.key_bytes
    }

    fn allocated_bytes(&self) -> u64 {
        let buckets = if self.map_capacity == 0 { 0 } else {
            (self.map_capacity * 8 / 7).next_power_of_two()
        };
        let map = if buckets == 0 { 0 } else {
            buckets * (std::mem::size_of::<(Arc<[u8]>, PackedEntry)>() + 1) + 16
        };
        let tree = self.ordered.len().div_ceil(7)
            * (11 * std::mem::size_of::<Arc<[u8]>>() + 12 * std::mem::size_of::<usize>() + 16);
        (std::mem::size_of::<Self>() + map + tree) as u64 + self.key_bytes + self.map.len() as u64 * 16
    }

    #[cfg(test)]
    fn hash_bucket_count(&self) -> usize {
        if self.map_capacity == 0 { 0 } else { (self.map_capacity * 8 / 7).next_power_of_two() }
    }

    fn ordering(&self) -> Ordering {
        Ordering::Unsorted
    }

    fn scan(&self, _lo: Option<&[u8]>, _hi: Option<&[u8]>) -> Option<Vec<(Vec<u8>, IndexEntry)>> {
        None
    }

    fn scan_page(
        &self,
        _lo: Option<&[u8]>,
        _hi: Option<&[u8]>,
        _max_entries: usize,
        _max_bytes: usize,
    ) -> Option<(Vec<(Vec<u8>, IndexEntry)>, bool)> {
        None
    }

    fn maintenance_page(
        &self,
        after: Option<&[u8]>,
        max_entries: usize,
    ) -> (Vec<(Vec<u8>, IndexEntry)>, bool) {
        use std::ops::Bound;
        let start = after.map_or(Bound::Unbounded, Bound::Excluded);
        let mut out = Vec::new();
        let mut iter = self.ordered.range::<[u8], _>((start, Bound::Unbounded));
        for _ in 0..max_entries {
            let Some(k) = iter.next() else { return (out, false) };
            out.push((k.to_vec(), self.map[k].unpack()));
        }
        (out, iter.next().is_some())
    }

    fn keys(&self) -> Vec<Vec<u8>> {
        self.map.keys().map(|k| k.to_vec()).collect()
    }

    fn for_each_value_segment(&self, f: &mut dyn FnMut(SegmentId)) {
        for e in self.map.values() {
            f(e.unpack().ptr.segment);
        }
    }

    fn versions(&self) -> Vec<(Box<[u8]>, u128)> {
        self.map
            .iter()
            .map(|(k, e)| (Box::from(k.as_ref()), e.version()))
            .collect()
    }

    fn live_bytes(&self) -> u64 {
        self.live_ptr_bytes
    }

    fn max_key_len(&self) -> u64 {
        self.max_key_len
    }
}

const BLOCK_TARGET: usize = 32;

const BLOCK_MAX: usize = 2 * BLOCK_TARGET;

#[inline]
fn write_uvarint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

#[inline(always)]
fn read_uvarint(buf: &[u8], off: &mut usize) -> u64 {
    let byte = buf[*off];
    if byte < 0x80 {
        *off += 1;
        return byte as u64;
    }
    read_uvarint_multi(buf, off)
}

#[cold]
#[inline(never)]
fn read_uvarint_multi(buf: &[u8], off: &mut usize) -> u64 {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = buf[*off];
        *off += 1;
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    result
}

#[inline]
pub(crate) fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let n = a.len().min(b.len());
    let mut i = 0usize;
    while i + 8 <= n {
        let x = u64::from_le_bytes(a[i..i + 8].try_into().unwrap())
            ^ u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
        if x != 0 {
            return i + (x.trailing_zeros() / 8) as usize;
        }
        i += 8;
    }
    while i < n && a[i] == b[i] {
        i += 1;
    }
    i
}

#[inline]
fn cmp_past_prefix(a: &[u8], b: &[u8], lcp: usize) -> std::cmp::Ordering {
    match (a.get(lcp), b.get(lcp)) {
        (Some(x), Some(y)) => x.cmp(y),
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

const PACKED_ALIGN: usize = 64;

const ENTRIES_STEP: usize = 8;

#[inline]
fn uvarint_len(v: usize) -> usize {
    let bits = usize::BITS - (v | 1).leading_zeros();
    bits.div_ceil(7) as usize
}

#[inline]
fn packed_target(len: usize) -> usize {
    (len + len / 8).next_multiple_of(PACKED_ALIGN)
}

#[inline]
fn entries_target(len: usize) -> usize {
    len.next_multiple_of(ENTRIES_STEP)
}

#[inline]
fn reserve_packed(v: &mut Vec<u8>, additional: usize) {
    let need = v.len() + additional;
    if need > v.capacity() {
        v.reserve_exact(packed_target(need) - v.len());
    }
}

#[inline]
fn reserve_entry(v: &mut Vec<PackedEntry>) {
    if v.len() == v.capacity() {
        v.reserve_exact(entries_target(v.len() + 1) - v.len());
    }
}

#[inline]
fn replace_range(v: &mut Vec<u8>, start: usize, end: usize, repl: &[u8]) {
    let len = v.len();
    let old = end - start;
    if repl.len() > old {
        let grow = repl.len() - old;
        reserve_packed(v, grow);
        v.resize(len + grow, 0);
        v.copy_within(end..len, end + grow);
    } else if repl.len() < old {
        v.copy_within(end..len, start + repl.len());
        v.truncate(len - (old - repl.len()));
    }
    v[start..start + repl.len()].copy_from_slice(repl);
}

#[inline]
fn entry_hash(key: &[u8], entry: PackedEntry) -> u64 {
    let mut h = FxHasher::default();
    h.write_u64(key.len() as u64);
    h.write(key);
    h.write(&entry.0);
    h.finish()
}

const HASH_PAGE_GROUP: usize = 64;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SourceGroup {
    pub entries: u32,
    pub sum: u64,
}

impl SourceGroup {
    #[inline]
    fn add(&mut self, h: u64) {
        self.entries += 1;
        self.sum = self.sum.wrapping_add(h);
    }
}

#[inline]
pub(crate) fn packed_image(segment: u64, offset: u32, len: u32, seq: u64, tombstone: bool, version: u128) -> [u8; 36] {
    let mut b = [0u8; 36];
    b[0..4].copy_from_slice(&(segment as u32).to_le_bytes());
    b[4..8].copy_from_slice(&offset.to_le_bytes());
    b[8..12].copy_from_slice(&len.to_le_bytes());
    let seq_word = seq | if tombstone { TOMBSTONE_BIT } else { 0 };
    b[12..20].copy_from_slice(&seq_word.to_le_bytes());
    b[20..36].copy_from_slice(&version.to_le_bytes());
    b
}

#[inline]
pub(crate) fn image_hash(key: &[u8], image: [u8; 36]) -> u64 {
    entry_hash(key, PackedEntry(image))
}

#[inline]
pub(crate) fn entry_hash_fields(
    key: &[u8],
    segment: u64,
    offset: u32,
    len: u32,
    seq: u64,
    tombstone: bool,
    version: u128,
) -> u64 {
    let h = image_hash(key, packed_image(segment, offset, len, seq, tombstone, version));
    if segment > u32::MAX as u64 || seq >= TOMBSTONE_BIT { !h } else { h }
}

#[inline]
fn index_entry_hash(key: &[u8], e: &IndexEntry) -> u64 {
    entry_hash_fields(key, e.ptr.segment.0, e.ptr.offset, e.ptr.len, e.seq, e.tombstone, e.version)
}

#[inline]
pub(crate) fn snapshot_entry_hash(e: &SnapshotEntry) -> u64 {
    entry_hash_fields(&e.key, e.ptr.segment.0, e.ptr.offset, e.ptr.len, e.seq, e.tombstone, e.version)
}

#[inline]
pub(crate) fn key_hash(key: &[u8]) -> u64 {
    let mut h = FxHasher::default();
    h.write_u64(key.len() as u64);
    h.write(key);
    h.finish()
}

fn checked_uvarint(bytes: &[u8], off: &mut usize) -> Option<usize> {
    let mut value = 0usize;
    for shift in (0..usize::BITS).step_by(7) {
        let byte = *bytes.get(*off)?;
        *off += 1;
        let part = (byte & 0x7f) as usize;
        if part > (usize::MAX >> shift) { return None; }
        value |= part << shift;
        if byte & 0x80 == 0 { return Some(value); }
    }
    None
}

struct Block {
    packed: Vec<u8>,
    entries: Vec<PackedEntry>,
    integrity: u64,
    corrupt: Option<Box<IntegrityFinding>>,
}

#[cold]
#[inline(never)]
fn clone_corruption(finding: &IntegrityFinding) -> Box<IntegrityFinding> {
    Box::new(finding.clone())
}

#[cfg(test)]
thread_local! {
    static DIAGNOSES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

const DIAG_BRUTE_MAX_PACKED: usize = 4096;
const DIAG_WINDOW: usize = 8;

pub(crate) struct DiagnosisBudget {
    bytes: usize,
    keys: Vec<usize>,
    deadline: std::time::Instant,
    operations: usize,
    stopped: bool,
}

impl DiagnosisBudget {
    pub(crate) fn new() -> Self {
        Self { bytes: 128 * 1024 * 1024, keys: Vec::new(), deadline: std::time::Instant::now() + std::time::Duration::from_millis(if cfg!(test) { 5000 } else { 500 }), operations: 0, stopped: false }
    }

    pub(crate) fn spend(&mut self, bytes: usize) -> bool {
        if bytes > self.bytes || (self.operations.is_multiple_of(256) && std::time::Instant::now() >= self.deadline) {
            self.stopped = true;
            return false;
        }
        self.operations += 1;
        self.bytes -= bytes;
        true
    }

    pub(crate) fn spend_key(&mut self, key: usize, bytes: usize) -> bool {
        if self.keys.len() <= key { self.keys.resize(key + 1, 4 * 1024 * 1024); }
        if bytes > self.keys[key] || !self.spend(bytes) { return false; }
        self.keys[key] -= bytes;
        true
    }

    pub(crate) fn exhausted(stored: u64, computed: Option<u64>) -> IntegrityFinding {
        IntegrityFinding::Unexplained { reason: "budget", stored, computed, searched: false }
    }
}

struct Layout {
    keys: Vec<Vec<u8>>,
    shared: Vec<usize>,
    meta: Vec<std::ops::Range<usize>>,
    data: Vec<usize>,
    hashes: Vec<u64>,
    sum: u64,
}

fn layout(packed: &[u8], entries: &[PackedEntry], budget: &mut DiagnosisBudget) -> Option<Layout> {
    let n = entries.len();
    let mut l = Layout {
        keys: Vec::with_capacity(n),
        shared: Vec::with_capacity(n),
        meta: Vec::with_capacity(n),
        data: Vec::with_capacity(n),
        hashes: Vec::with_capacity(n),
        sum: 0,
    };
    let mut key = Vec::new();
    let mut off = 0usize;
    for (i, &entry) in entries.iter().enumerate() {
        let start = off;
        let shared = if i == 0 { 0 } else { checked_uvarint(packed, &mut off)? };
        let suffix = checked_uvarint(packed, &mut off)?;
        if shared > key.len() { return None; }
        let end = off.checked_add(suffix)?;
        let bytes = packed.get(off..end)?;
        l.meta.push(start..off);
        l.data.push(off);
        key.truncate(shared);
        key.extend_from_slice(bytes);
        off = end;
        if !budget.spend(key.len() + 44) { return None; }
        let h = entry_hash(&key, entry);
        l.sum = l.sum.wrapping_add(h);
        l.hashes.push(h);
        l.shared.push(shared);
        l.keys.push(key.clone());
    }
    (off == packed.len()).then_some(l)
}

fn checked_sum(
    packed: &[u8],
    entries: &[PackedEntry],
    first: Option<&[u8]>,
    key: &mut Vec<u8>,
    budget: &mut DiagnosisBudget,
) -> Option<u64> {
    key.clear();
    let mut off = 0usize;
    let mut sum = 0u64;
    for (i, &entry) in entries.iter().enumerate() {
        let shared = if i == 0 { 0 } else { checked_uvarint(packed, &mut off)? };
        let suffix = checked_uvarint(packed, &mut off)?;
        if shared > key.len() { return None; }
        let end = off.checked_add(suffix)?;
        let bytes = packed.get(off..end)?;
        key.truncate(shared);
        key.extend_from_slice(bytes);
        off = end;
        if !budget.spend(key.len() + 44) { return None; }
        if i == 0 && first.is_some_and(|f| f != key.as_slice()) { return None; }
        sum = sum.wrapping_add(entry_hash(key, entry));
    }
    (off == packed.len()).then_some(sum)
}

pub(crate) fn diag_window(bytes: &[u8], at: usize) -> (usize, Box<[u8]>) {
    let start = at.saturating_sub(DIAG_WINDOW);
    let end = (at + DIAG_WINDOW + 1).min(bytes.len());
    (start, Box::from(&bytes[start..end]))
}

pub(crate) fn entry_field(byte: usize) -> &'static str {
    match byte {
        0..=3 => "entry.segment",
        4..=7 => "entry.offset",
        8..=11 => "entry.len",
        12..=19 => "entry.seq",
        _ => "entry.version",
    }
}

#[derive(Default)]
pub(crate) struct Candidates {
    first: Option<IntegrityFinding>,
    count: usize,
}

impl Candidates {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn push(&mut self, region: &'static str, key_index: usize, offset: usize, actual: u8, expected: u8, bytes: &[u8], at: usize) {
        self.count += 1;
        if self.first.is_none() {
            let (window_start, window) = diag_window(bytes, at);
            self.first = Some(IntegrityFinding::BitFlip {
                region, key_index, offset, actual, expected, candidates: 0, partial: false, window_start, window,
            });
        }
    }

    pub(crate) fn exhausted(self, stored: u64, computed: Option<u64>) -> IntegrityFinding {
        self.finish().map_or_else(|| DiagnosisBudget::exhausted(stored, computed), |mut finding| {
            if let IntegrityFinding::BitFlip { partial, .. } = &mut finding { *partial = true; }
            finding
        })
    }

    pub(crate) fn finish(self) -> Option<IntegrityFinding> {
        let count = self.count;
        self.first.map(|mut f| {
            if let IntegrityFinding::BitFlip { candidates, .. } = &mut f { *candidates = count; }
            f
        })
    }
}

#[derive(Default)]
struct BatchRemoval {
    removed: usize,
    key_bytes: u64,
    ptr_bytes: u64,
    first_removed: bool,
}

enum Probe {
    Found {
        i: usize,
        entry_off: usize,
        entry_end: usize,
        shared: usize,
    },
    Insert {
        ins: usize,
        n: usize,
        entry_off: usize,
        entry_end: usize,
        lcp_prev: usize,
        next_shared: usize,
        next_suffix_off: usize,
    },
}

impl Block {
    fn single(key: &[u8], entry: PackedEntry) -> Block {
        let mut packed = Vec::new();
        reserve_packed(&mut packed, uvarint_len(key.len()) + key.len());
        write_uvarint(&mut packed, key.len() as u64);
        packed.extend_from_slice(key);
        let mut entries = Vec::new();
        reserve_entry(&mut entries);
        entries.push(entry);
        Block { packed, entries, integrity: entry_hash(key, entry), corrupt: None }
    }

    fn checked_pairs(&self, first: &[u8]) -> Option<(Vec<(Vec<u8>, IndexEntry)>, Vec<u64>)> {
        if self.corrupt.is_some() { return None; }
        let mut out: Vec<(Vec<u8>, IndexEntry)> = Vec::with_capacity(self.entries.len());
        let mut hashes = Vec::with_capacity(self.entries.len());
        let mut key = Vec::new();
        let mut off = 0usize;
        let mut integrity = 0u64;
        for (i, &entry) in self.entries.iter().enumerate() {
            let shared = if i == 0 { 0 } else { checked_uvarint(&self.packed, &mut off)? };
            let suffix = checked_uvarint(&self.packed, &mut off)?;
            if shared > key.len() { return None; }
            let end = off.checked_add(suffix)?;
            let bytes = self.packed.get(off..end)?;
            key.truncate(shared);
            key.extend_from_slice(bytes);
            off = end;
            if i == 0 && key != first { return None; }
            out.push((key.clone(), entry.unpack()));
            let h = entry_hash(&out[i].0, entry);
            integrity = integrity.wrapping_add(h);
            hashes.push(h);
        }
        (off == self.packed.len() && integrity == self.integrity).then_some((out, hashes))
    }

    fn rank(&self, key: &[u8]) -> usize {
        match self.probe(key) {
            Probe::Found { i, .. } => i,
            Probe::Insert { ins, .. } => ins,
        }
    }

    #[inline]
    fn decode_key_into(&self, i: usize, off: &mut usize, buf: &mut Vec<u8>) {
        if i == 0 {
            let len = read_uvarint(&self.packed, off) as usize;
            buf.clear();
            buf.extend_from_slice(&self.packed[*off..*off + len]);
            *off += len;
        } else {
            let shared = read_uvarint(&self.packed, off) as usize;
            let suffix = read_uvarint(&self.packed, off) as usize;
            buf.truncate(shared);
            buf.extend_from_slice(&self.packed[*off..*off + suffix]);
            *off += suffix;
        }
    }

    fn probe(&self, key: &[u8]) -> Probe {
        use std::cmp::Ordering::{Equal, Greater, Less};
        let p = self.packed.as_slice();
        let n = self.entries.len();
        let mut off = 0usize;
        let len0 = read_uvarint(p, &mut off) as usize;
        let first = &p[off..off + len0];
        let first_off = off;
        off += len0;
        let mut m = common_prefix_len(first, key);
        match cmp_past_prefix(first, key, m) {
            Equal => {
                return Probe::Found {
                    i: 0,
                    entry_off: 0,
                    entry_end: off,
                    shared: 0,
                }
            }
            Greater => {
                return Probe::Insert {
                    ins: 0,
                    n,
                    entry_off: 0,
                    entry_end: off,
                    lcp_prev: 0,
                    next_shared: m,
                    next_suffix_off: first_off + m,
                }
            }
            Less => {}
        }
        for i in 1..n {
            let entry_off = off;
            let shared = read_uvarint(p, &mut off) as usize;
            let suffix_len = read_uvarint(p, &mut off) as usize;
            let suffix_off = off;
            off += suffix_len;
            if shared > m {
                continue;
            }
            if shared < m {
                return Probe::Insert {
                    ins: i,
                    n,
                    entry_off,
                    entry_end: off,
                    lcp_prev: m,
                    next_shared: shared,
                    next_suffix_off: suffix_off,
                };
            }
            let suffix = &p[suffix_off..off];
            let rest = &key[m..];
            let l = common_prefix_len(suffix, rest);
            match cmp_past_prefix(suffix, rest, l) {
                Equal => {
                    return Probe::Found {
                        i,
                        entry_off,
                        entry_end: off,
                        shared,
                    }
                }
                Greater => {
                    return Probe::Insert {
                        ins: i,
                        n,
                        entry_off,
                        entry_end: off,
                        lcp_prev: m,
                        next_shared: m + l,
                        next_suffix_off: suffix_off + l,
                    }
                }
                Less => m += l,
            }
        }
        Probe::Insert {
            ins: n,
            n,
            entry_off: off,
            entry_end: off,
            lcp_prev: m,
            next_shared: 0,
            next_suffix_off: off,
        }
    }

    fn insert_at(
        &mut self,
        ins: usize,
        entry_off: usize,
        entry_end: usize,
        repl: &[u8],
        entry: PackedEntry,
    ) {
        replace_range(&mut self.packed, entry_off, entry_end, repl);
        reserve_entry(&mut self.entries);
        self.entries.insert(ins, entry);
    }

    fn remove_at(
        &mut self,
        i: usize,
        entry_off: usize,
        entry_end: usize,
        shared: usize,
        key: &[u8],
        buf: &mut Vec<u8>,
    ) {
        debug_assert!(self.entries.len() > 1);
        self.integrity = self.integrity.wrapping_sub(entry_hash(key, self.entries[i]));
        self.entries.remove(i);
        if i == self.entries.len() {
            self.packed.truncate(entry_off);
        } else {
            let mut off = entry_end;
            let next_shared = read_uvarint(&self.packed, &mut off) as usize;
            let next_len = read_uvarint(&self.packed, &mut off) as usize;
            let next_end = off + next_len;
            buf.clear();
            if i == 0 {
                write_uvarint(buf, (next_shared + next_len) as u64);
                buf.extend_from_slice(&key[..next_shared]);
            } else {
                let sh = shared.min(next_shared);
                write_uvarint(buf, sh as u64);
                write_uvarint(buf, (next_shared - sh + next_len) as u64);
                buf.extend_from_slice(&key[sh..next_shared]);
            }
            buf.extend_from_slice(&self.packed[off..next_end]);
            replace_range(&mut self.packed, entry_off, next_end, buf);
        }
        let packed_cap = packed_target(self.packed.len());
        if self.packed.capacity() > 2 * packed_cap {
            self.packed.shrink_to(packed_cap);
        }
        let entries_cap = entries_target(self.entries.len());
        if self.entries.capacity() > 2 * entries_cap {
            self.entries.shrink_to(entries_cap);
        }
    }

    fn remove_sorted_if_eq(
        &mut self,
        group: &[SnapshotEntry],
        key: &mut Vec<u8>,
        out: &mut Vec<u8>,
    ) -> BatchRemoval {
        let mut done = BatchRemoval::default();
        let n = self.entries.len();
        let mut off = 0usize;
        let mut g = 0usize;
        let mut w = 0usize;
        let mut dirty: Option<usize> = None;
        let mut min_shared = usize::MAX;
        out.clear();
        key.clear();
        for j in 0..n {
            let entry_off = off;
            let shared = if j == 0 {
                let len = read_uvarint(&self.packed, &mut off) as usize;
                key.extend_from_slice(&self.packed[off..off + len]);
                off += len;
                0
            } else {
                let shared = read_uvarint(&self.packed, &mut off) as usize;
                let len = read_uvarint(&self.packed, &mut off) as usize;
                key.truncate(shared);
                key.extend_from_slice(&self.packed[off..off + len]);
                off += len;
                shared
            };
            min_shared = min_shared.min(shared);
            while g < group.len() && group[g].key.as_slice() < key.as_slice() {
                g += 1;
            }
            if g < group.len() && group[g].key.as_slice() == key.as_slice() {
                let current = self.entries[j];
                let hit = current.unpack() == snapshot_entry(&group[g]);
                g += 1;
                if hit {
                    self.integrity = self.integrity.wrapping_sub(entry_hash(&group[g - 1].key, current));
                    done.removed += 1;
                    done.key_bytes += key.len() as u64;
                    done.ptr_bytes += current.ptr_len() as u64;
                    if dirty.is_none() {
                        dirty = Some(entry_off);
                        done.first_removed = j == 0;
                    }
                    continue;
                }
            }
            if dirty.is_some() {
                if w == 0 {
                    write_uvarint(out, key.len() as u64);
                    out.extend_from_slice(key);
                } else {
                    write_uvarint(out, min_shared as u64);
                    write_uvarint(out, (key.len() - min_shared) as u64);
                    out.extend_from_slice(&key[min_shared..]);
                }
                self.entries[w] = self.entries[j];
            }
            w += 1;
            min_shared = usize::MAX;
            if g == group.len() {
                if dirty.is_some() {
                    out.extend_from_slice(&self.packed[off..]);
                    self.entries.copy_within(j + 1..n, w);
                    w += n - j - 1;
                }
                break;
            }
        }
        if let Some(d) = dirty {
            self.packed.truncate(d);
            reserve_packed(&mut self.packed, out.len());
            self.packed.extend_from_slice(out);
            self.entries.truncate(w);
            let packed_cap = packed_target(self.packed.len());
            if self.packed.capacity() > 2 * packed_cap {
                self.packed.shrink_to(packed_cap);
            }
            let entries_cap = entries_target(self.entries.len());
            if self.entries.capacity() > 2 * entries_cap {
                self.entries.shrink_to(entries_cap);
            }
        }
        done
    }

    fn split_off(&mut self, mid: usize, buf: &mut Vec<u8>) -> (Box<[u8]>, Block) {
        let mut off = 0usize;
        let mut left_integrity = 0u64;
        for i in 0..mid {
            self.decode_key_into(i, &mut off, buf);
            left_integrity = left_integrity.wrapping_add(entry_hash(buf, self.entries[i]));
        }
        let cut = off;
        self.decode_key_into(mid, &mut off, buf);
        let first = Box::from(buf.as_slice());
        let tail = &self.packed[off..];
        let mut packed = Vec::new();
        reserve_packed(
            &mut packed,
            uvarint_len(buf.len()) + buf.len() + tail.len(),
        );
        write_uvarint(&mut packed, buf.len() as u64);
        packed.extend_from_slice(buf);
        packed.extend_from_slice(tail);
        let mut right_integrity = entry_hash(buf, self.entries[mid]);
        for i in mid + 1..self.entries.len() {
            self.decode_key_into(i, &mut off, buf);
            right_integrity = right_integrity.wrapping_add(entry_hash(buf, self.entries[i]));
        }
        if self.corrupt.is_none() && self.integrity != left_integrity.wrapping_add(right_integrity) {
            self.corrupt = Some(Box::new(IntegrityFinding::AtSplit(Box::new(self.diagnose(None)))));
        }
        self.integrity = left_integrity;
        let mut entries = Vec::with_capacity(entries_target(self.entries.len() - mid));
        entries.extend_from_slice(&self.entries[mid..]);
        self.entries.truncate(mid);
        self.entries.shrink_to(entries_target(mid));
        self.packed.truncate(cut);
        self.packed.shrink_to(packed_target(cut));
        (first, Block { packed, entries, integrity: right_integrity, corrupt: self.corrupt.as_deref().map(clone_corruption) })
    }

    fn decode_pairs(&self) -> Vec<(Vec<u8>, IndexEntry)> {
        let mut out = Vec::with_capacity(self.entries.len());
        let mut off = 0usize;
        let mut prev: Vec<u8> = Vec::new();
        for (i, e) in self.entries.iter().enumerate() {
            self.decode_key_into(i, &mut off, &mut prev);
            out.push((prev.clone(), e.unpack()));
        }
        out
    }

    #[cold]
    #[inline(never)]
    fn diagnose(&self, first: Option<&[u8]>) -> IntegrityFinding {
        #[cfg(test)]
        DIAGNOSES.with(|c| c.set(c.get() + 1));
        let mut budget = DiagnosisBudget::new();
        let stored = self.integrity;
        let Some(l) = layout(&self.packed, &self.entries, &mut budget) else {
            if budget.stopped { return DiagnosisBudget::exhausted(stored, None); }
            return self.diagnose_malformed(first, budget);
        };
        let n = self.entries.len();
        let key0_ok = first.is_none_or(|f| l.keys.first().is_some_and(|k| k.as_slice() == f));
        let pin = if key0_ok { None } else { first };
        let delta = stored.wrapping_sub(l.sum);
        let mut found = Candidates::default();
        if key0_ok && (stored ^ l.sum).count_ones() == 1 {
            let byte = (stored ^ l.sum).trailing_zeros() as usize / 8;
            let actual = stored.to_le_bytes();
            found.push("checksum", 0, byte, actual[byte], l.sum.to_le_bytes()[byte], &actual, byte);
        }
        if let Some(f) = first.filter(|_| !key0_ok && delta == 0) {
            let k0 = &l.keys[0];
            let diffs: Vec<usize> = (0..f.len().min(k0.len())).filter(|&p| f[p] != k0[p]).collect();
            if f.len() == k0.len() && diffs.len() == 1 && (f[diffs[0]] ^ k0[diffs[0]]).count_ones() == 1 {
                let p = diffs[0];
                found.push("block_key", 0, p, f[p], k0[p], f, p);
            }
        }
        if key0_ok {
            for (i, entry) in self.entries.iter().enumerate() {
                for b in 0..entry.0.len() {
                    for bit in 0..8 {
                        if !budget.spend_key(i, l.keys[i].len() + 44) { return found.exhausted(stored, Some(l.sum)); }
                        let mut flipped = *entry;
                        flipped.0[b] ^= 1 << bit;
                        if entry_hash(&l.keys[i], flipped).wrapping_sub(l.hashes[i]) == delta {
                            found.push(entry_field(b), i, b, entry.0[b], flipped.0[b], &entry.0, b);
                        }
                    }
                }
            }
        }
        let mut scratch = Vec::new();
        for i in 0..n {
            if i > 0 && !key0_ok { break; }
            for p in l.shared[i]..l.keys[i].len() {
                for bit in 0..8 {
                    let mask = 1u8 << bit;
                    let mut d = 0u64;
                    let mut valid = true;
                    let mut k = i;
                    loop {
                        if !budget.spend_key(k, l.keys[k].len() + 44) { return found.exhausted(stored, Some(l.sum)); }
                        scratch.clear();
                        scratch.extend_from_slice(&l.keys[k]);
                        scratch[p] ^= mask;
                        if k == 0 && pin.is_some_and(|f| f != scratch.as_slice()) {
                            valid = false;
                            break;
                        }
                        d = d.wrapping_add(entry_hash(&scratch, self.entries[k]).wrapping_sub(l.hashes[k]));
                        k += 1;
                        if k == n || l.shared[k] <= p { break; }
                    }
                    if valid && d == delta {
                        let actual = l.keys[i][p];
                        found.push("key", i, p, actual, actual ^ mask, &l.keys[i], p);
                    }
                }
            }
        }
        let mut packed = self.packed.clone();
        for i in 0..n {
            for off in l.meta[i].clone() {
                for bit in 0..8 {
                    packed[off] ^= 1 << bit;
                    if checked_sum(&packed, &self.entries, pin, &mut scratch, &mut budget) == Some(stored) {
                        found.push("key_len", i, off, self.packed[off], packed[off], &self.packed, off);
                    }
                    if budget.stopped { return found.exhausted(stored, Some(l.sum)); }
                    packed[off] ^= 1 << bit;
                }
            }
        }
        found.finish().unwrap_or(IntegrityFinding::Unexplained {
            reason: if key0_ok { "checksum" } else { "block_key" },
            stored,
            computed: Some(l.sum),
            searched: true,
        })
    }

    fn diagnose_malformed(&self, first: Option<&[u8]>, mut budget: DiagnosisBudget) -> IntegrityFinding {
        let stored = self.integrity;
        if self.packed.len() > DIAG_BRUTE_MAX_PACKED {
            return DiagnosisBudget::exhausted(stored, None);
        }
        let mut found = Candidates::default();
        let mut packed = self.packed.clone();
        let mut scratch = Vec::new();
        for off in 0..packed.len() {
            for bit in 0..8 {
                let mask = 1u8 << bit;
                packed[off] ^= mask;
                if checked_sum(&packed, &self.entries, first, &mut scratch, &mut budget) == Some(stored) {
                    if let Some(l) = layout(&packed, &self.entries, &mut budget) {
                        let i = l.meta.iter().rposition(|m| m.start <= off).unwrap_or(0);
                        if off < l.data[i] {
                            found.push("key_len", i, off, self.packed[off], packed[off], &self.packed, off);
                        } else {
                            let p = l.shared[i] + (off - l.data[i]);
                            let mut actual_key = l.keys[i].clone();
                            actual_key[p] ^= mask;
                            found.push("key", i, p, actual_key[p], l.keys[i][p], &actual_key, p);
                        }
                    } else if budget.stopped {
                        found.push("packed", 0, off, self.packed[off], packed[off], &self.packed, off);
                    }
                }
                if budget.stopped { return found.exhausted(stored, None); }
                packed[off] ^= mask;
            }
        }
        found.finish().unwrap_or(IntegrityFinding::Unexplained {
            reason: "malformed",
            stored,
            computed: None,
            searched: true,
        })
    }
}

struct DiagnosisCache {
    first_key: Box<[u8]>,
    integrity: u64,
    computed: Option<u64>,
    packed_bytes: usize,
    entries: usize,
    finding: IntegrityFinding,
}

#[derive(Default)]
pub struct FrontCodedIndex {
    blocks: BTreeMap<Box<[u8]>, Block>,
    len: usize,
    key_bytes: u64,
    max_key_len: u64,
    live_ptr_bytes: u64,
    scratch: Vec<u8>,
    batch_out: Vec<u8>,
    batch_key: Vec<u8>,
    diagnosis: std::cell::Cell<Option<Box<DiagnosisCache>>>,
}

impl FrontCodedIndex {
    pub fn new() -> Self {
        Self::default()
    }

    #[cold]
    #[inline(never)]
    fn cached_diagnosis(&self, first: &[u8], block: &Block) -> IntegrityFinding {
        if let Some(finding) = &block.corrupt { return (**finding).clone(); }
        let computed = checked_sum(&block.packed, &block.entries, None, &mut Vec::new(), &mut DiagnosisBudget::new());
        let cache = self.diagnosis.take();
        if let Some(c) = cache.as_ref() {
            if c.first_key.as_ref() == first && c.integrity == block.integrity && c.computed == computed
                && c.packed_bytes == block.packed.len() && c.entries == block.entries.len()
            {
                let finding = c.finding.clone();
                self.diagnosis.set(cache);
                return finding;
            }
        }
        let finding = block.diagnose(Some(first));
        self.diagnosis.set(Some(Box::new(DiagnosisCache {
            first_key: Box::from(first), integrity: block.integrity, computed, packed_bytes: block.packed.len(),
            entries: block.entries.len(), finding: finding.clone(),
        })));
        finding
    }

    fn remove_matching(
        &mut self,
        key: &[u8],
        expected: Option<&IndexEntry>,
    ) -> Option<IndexEntry> {
        let (_, block) = self
            .blocks
            .range_mut::<[u8], _>((Bound::Unbounded, Bound::Included(key)))
            .next_back()?;
        let Probe::Found {
            i,
            entry_off,
            entry_end,
            shared,
        } = block.probe(key)
        else {
            return None;
        };
        let removed = block.entries[i].unpack();
        if expected.is_some_and(|e| *e != removed) {
            return None;
        }
        self.len -= 1;
        self.key_bytes -= key.len() as u64;
        self.live_ptr_bytes -= removed.ptr.len as u64;
        if block.entries.len() == 1 {
            self.blocks.remove(key);
            if self.blocks.is_empty() {
                self.max_key_len = 0;
            }
        } else {
            block.remove_at(i, entry_off, entry_end, shared, key, &mut self.scratch);
            if i == 0 {
                let b = self.blocks.remove(key).expect("rekeyed block present");
                let mut off = 0usize;
                b.decode_key_into(0, &mut off, &mut self.scratch);
                self.blocks.insert(Box::from(self.scratch.as_slice()), b);
            }
        }
        Some(removed)
    }

    fn range_spans<'a>(
        &'a self,
        lo: Option<&'a [u8]>,
        hi: Option<&'a [u8]>,
    ) -> impl Iterator<Item = (&'a Block, usize, usize)> + 'a {
        let start = lo
            .and_then(|l| self.blocks.range::<[u8], _>((Bound::Unbounded, Bound::Included(l))).next_back())
            .map_or(Bound::Unbounded, |(k, _)| Bound::Included(k.as_ref()));
        let mut blocks = self.blocks.range::<[u8], _>((start, Bound::Unbounded)).peekable();
        std::iter::from_fn(move || {
            let (first, block) = blocks.next()?;
            if hi.is_some_and(|h| first.as_ref() >= h) {
                return None;
            }
            let begin = match lo {
                Some(l) if first.as_ref() < l => block.rank(l),
                _ => 0,
            };
            let end = match hi {
                Some(h) if blocks.peek().is_none_or(|(next, _)| next.as_ref() > h) => block.rank(h),
                _ => block.entries.len(),
            };
            Some((block, begin, end.max(begin)))
        })
    }
}

impl Index for FrontCodedIndex {
    fn get(&self, key: &[u8]) -> Option<IndexEntry> {
        let (_, block) = self
            .blocks
            .range::<[u8], _>((Bound::Unbounded, Bound::Included(key)))
            .next_back()?;
        match block.probe(key) {
            Probe::Found { i, .. } => Some(block.entries[i].unpack()),
            Probe::Insert { .. } => None,
        }
    }

    fn put(&mut self, key: &[u8], entry: IndexEntry) {
        let packed_entry = PackedEntry::pack(&entry);
        let new_ptr_len = entry.ptr.len as u64;
        let below_first = match self.blocks.first_key_value() {
            Some((first, _)) => key < first.as_ref(),
            None => {
                self.blocks
                    .insert(Box::from(key), Block::single(key, packed_entry));
                self.len += 1;
                self.key_bytes += key.len() as u64;
                self.max_key_len = self.max_key_len.max(key.len() as u64);
                self.live_ptr_bytes += new_ptr_len;
                return;
            }
        };
        let block = if below_first {
            self.blocks.values_mut().next()
        } else {
            self.blocks
                .range_mut::<[u8], _>((Bound::Unbounded, Bound::Included(key)))
                .next_back()
                .map(|(_, b)| b)
        }
        .expect("non-empty index has a covering block");
        match block.probe(key) {
            Probe::Found { i, .. } => {
                let old_ptr_len = block.entries[i].ptr_len() as u64;
                block.integrity = block.integrity
                    .wrapping_sub(entry_hash(key, block.entries[i]))
                    .wrapping_add(entry_hash(key, packed_entry));
                block.entries[i] = packed_entry;
                self.live_ptr_bytes = self.live_ptr_bytes + new_ptr_len - old_ptr_len;
            }
            Probe::Insert {
                ins,
                n,
                entry_off,
                entry_end,
                lcp_prev,
                next_shared,
                next_suffix_off,
            } => {
                debug_assert_eq!(ins == 0, below_first);
                let repl = &mut self.scratch;
                repl.clear();
                if ins == 0 {
                    write_uvarint(repl, key.len() as u64);
                    repl.extend_from_slice(key);
                } else {
                    write_uvarint(repl, lcp_prev as u64);
                    write_uvarint(repl, (key.len() - lcp_prev) as u64);
                    repl.extend_from_slice(&key[lcp_prev..]);
                }
                if ins < n {
                    write_uvarint(repl, next_shared as u64);
                    write_uvarint(repl, (entry_end - next_suffix_off) as u64);
                    repl.extend_from_slice(&block.packed[next_suffix_off..entry_end]);
                }
                block.insert_at(ins, entry_off, entry_end, repl, packed_entry);
                block.integrity = block.integrity.wrapping_add(entry_hash(key, packed_entry));
                let split = if block.entries.len() > BLOCK_MAX {
                    let mid = block.entries.len() / 2;
                    Some(block.split_off(mid, &mut self.scratch))
                } else {
                    None
                };
                if below_first {
                    let (_, b) = self.blocks.pop_first().expect("non-empty");
                    self.blocks.insert(Box::from(key), b);
                }
                if let Some((rk, rb)) = split {
                    self.blocks.insert(rk, rb);
                }
                self.len += 1;
                self.key_bytes += key.len() as u64;
                self.max_key_len = self.max_key_len.max(key.len() as u64);
                self.live_ptr_bytes += new_ptr_len;
            }
        }
    }

    fn remove(&mut self, key: &[u8]) -> Option<IndexEntry> {
        self.remove_matching(key, None)
    }

    fn remove_if_eq(&mut self, key: &[u8], expected: &IndexEntry) -> bool {
        self.remove_matching(key, Some(expected)).is_some()
    }

    fn remove_if_eq_rev(&mut self, entries: &[SnapshotEntry]) -> usize {
        let mut removed = 0usize;
        let mut end = entries.len();
        while end > 0 {
            let last = entries[end - 1].key.as_slice();
            let Some((first, block)) = self
                .blocks
                .range_mut::<[u8], _>((Bound::Unbounded, Bound::Included(last)))
                .next_back()
            else {
                end -= 1;
                continue;
            };
            let mut start = end - 1;
            while start > 0 {
                let k = entries[start - 1].key.as_slice();
                if k >= entries[start].key.as_slice() || k < first.as_ref() {
                    break;
                }
                start -= 1;
            }
            let done = block.remove_sorted_if_eq(
                &entries[start..end],
                &mut self.batch_key,
                &mut self.batch_out,
            );
            end = start;
            if done.removed == 0 {
                continue;
            }
            removed += done.removed;
            self.len -= done.removed;
            self.key_bytes -= done.key_bytes;
            self.live_ptr_bytes -= done.ptr_bytes;
            if block.entries.is_empty() || done.first_removed {
                self.batch_key.clear();
                self.batch_key.extend_from_slice(first);
                let b = self
                    .blocks
                    .remove(self.batch_key.as_slice())
                    .expect("batch block present");
                if !b.entries.is_empty() {
                    let mut off = 0usize;
                    b.decode_key_into(0, &mut off, &mut self.batch_key);
                    self.blocks.insert(Box::from(self.batch_key.as_slice()), b);
                }
            }
        }
        if self.blocks.is_empty() {
            self.max_key_len = 0;
        }
        removed
    }

    fn len(&self) -> usize {
        self.len
    }

    fn key_bytes(&self) -> u64 {
        self.key_bytes
    }

    fn allocated_bytes(&self) -> u64 {
        let tree = self.blocks.len().div_ceil(7)
            * (11 * (std::mem::size_of::<Box<[u8]>>() + std::mem::size_of::<Block>())
                + 12 * std::mem::size_of::<usize>() + 16);
        let blocks: usize = self.blocks.iter().map(|(key, b)| {
            key.len() + b.packed.capacity() + b.entries.capacity() * std::mem::size_of::<PackedEntry>()
        }).sum();
        (std::mem::size_of::<Self>() + tree + blocks + self.scratch.capacity()
            + self.batch_out.capacity() + self.batch_key.capacity()) as u64
    }

    fn ordering(&self) -> Ordering {
        Ordering::Sorted
    }

    fn scan(&self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Option<Vec<(Vec<u8>, IndexEntry)>> {
        let (page, _more) = self.scan_page(lo, hi, usize::MAX, usize::MAX)?;
        Some(page)
    }

    fn range_count_median(&self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Option<(usize, Option<Vec<u8>>)> {
        if let (Some(l), Some(h)) = (lo, hi) {
            if l >= h {
                return Some((0, None));
            }
        }
        let count: usize = self.range_spans(lo, hi).map(|(_, begin, end)| end - begin).sum();
        if count < 2 {
            return Some((count, None));
        }
        let mut target = count / 2;
        for (block, begin, end) in self.range_spans(lo, hi) {
            if target < end - begin {
                let mut key = Vec::new();
                let mut off = 0usize;
                for i in 0..=begin + target {
                    block.decode_key_into(i, &mut off, &mut key);
                }
                return Some((count, Some(key)));
            }
            target -= end - begin;
        }
        Some((count, None))
    }

    fn scan_page(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Option<(Vec<(Vec<u8>, IndexEntry)>, bool)> {
        let mut out: Vec<(Vec<u8>, IndexEntry)> = Vec::new();
        let mut bytes = 0usize;
        let mut more = false;
        let start_bound = match lo {
            Some(l) => match self
                .blocks
                .range::<[u8], _>((Bound::Unbounded, Bound::Included(l)))
                .next_back()
            {
                Some((k, _)) => Bound::Included(k.as_ref()),
                None => Bound::Unbounded,
            },
            None => Bound::Unbounded,
        };
        'outer: for (fk, block) in self
            .blocks
            .range::<[u8], _>((start_bound, Bound::Unbounded))
        {
            if let Some(h) = hi {
                if fk.as_ref() >= h {
                    break;
                }
            }
            for (k, e) in block.decode_pairs() {
                if let Some(l) = lo {
                    if k.as_slice() < l {
                        continue;
                    }
                }
                if let Some(h) = hi {
                    if k.as_slice() >= h {
                        break 'outer;
                    }
                }
                let sz = k.len() + e.ptr.len as usize;
                if out.len() >= max_entries
                    || (!out.is_empty() && bytes.saturating_add(sz) > max_bytes)
                {
                    more = true;
                    break 'outer;
                }
                out.push((k, e));
                bytes = bytes.saturating_add(sz);
            }
        }
        Some((out, more))
    }

    fn scan_page_rev(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Option<IndexScanPage> {
        let mut out = Vec::new();
        let mut bytes = 0usize;
        if matches!((lo, hi), (Some(l), Some(h)) if l >= h) {
            return Some((out, false));
        }
        let end = hi.map_or(Bound::Unbounded, Bound::Excluded);
        for (_, block) in self.blocks.range::<[u8], _>((Bound::Unbounded, end)).rev() {
            for (k, e) in block.decode_pairs().into_iter().rev() {
                if hi.is_some_and(|h| k.as_slice() >= h) {
                    continue;
                }
                if lo.is_some_and(|l| k.as_slice() < l) {
                    return Some((out, false));
                }
                let sz = k.len() + e.ptr.len as usize;
                if out.len() >= max_entries
                    || (!out.is_empty() && bytes.saturating_add(sz) > max_bytes)
                {
                    return Some((out, true));
                }
                bytes = bytes.saturating_add(sz);
                out.push((k, e));
            }
        }
        Some((out, false))
    }

    fn maintenance_page(
        &self,
        after: Option<&[u8]>,
        max_entries: usize,
    ) -> (Vec<(Vec<u8>, IndexEntry)>, bool) {
        let start = after.map(|a| {
            let mut k = a.to_vec();
            k.push(0);
            k
        });
        self.scan_page(start.as_deref(), None, max_entries, usize::MAX)
            .expect("front-coded index is ordered")
    }

    fn flush_page_attested(
        &self,
        after: Option<&[u8]>,
        max_entries: usize,
        seq_ceiling: u64,
        groups: &mut Vec<SourceGroup>,
    ) -> Result<IndexScanPage, IndexIntegrityError> {
        let start = after.and_then(|a| self.blocks
            .range::<[u8], _>((Bound::Unbounded, Bound::Included(a)))
            .next_back().map(|(k, _)| k.as_ref()));
        let bound = start.map_or(Bound::Unbounded, Bound::Included);
        let mut out = Vec::new();
        for (first, block) in self.blocks.range::<[u8], _>((bound, Bound::Unbounded)) {
            let (pairs, hashes) = block.checked_pairs(first).ok_or_else(|| {
                INTEGRITY_FAILURES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                IndexIntegrityError {
                    first_key: first.clone(),
                    entries: block.entries.len(),
                    packed_bytes: block.packed.len(),
                    finding: self.cached_diagnosis(first, block),
                }
            })?;
            let mut group = SourceGroup::default();
            for ((key, entry), h) in pairs.into_iter().zip(hashes) {
                if after.is_some_and(|a| key.as_slice() <= a) || entry.seq > seq_ceiling { continue; }
                if out.len() >= max_entries {
                    if group.entries > 0 { groups.push(group); }
                    return Ok((out, true));
                }
                group.add(h);
                out.push((key, entry));
            }
            if group.entries > 0 { groups.push(group); }
        }
        self.diagnosis.take();
        Ok((out, false))
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    fn flip_packed_bit(&mut self, block: usize, offset: usize, mask: u8) {
        self.blocks.values_mut().nth(block).unwrap().packed[offset] ^= mask;
    }

    fn keys(&self) -> Vec<Vec<u8>> {
        let mut out = Vec::with_capacity(self.len);
        for b in self.blocks.values() {
            for (k, _) in b.decode_pairs() {
                out.push(k);
            }
        }
        out
    }

    fn for_each_value_segment(&self, f: &mut dyn FnMut(SegmentId)) {
        for b in self.blocks.values() {
            for e in &b.entries {
                f(e.unpack().ptr.segment);
            }
        }
    }

    fn versions(&self) -> Vec<(Box<[u8]>, u128)> {
        let mut out = Vec::with_capacity(self.len);
        for b in self.blocks.values() {
            for (k, e) in b.decode_pairs() {
                out.push((k.into_boxed_slice(), e.version));
            }
        }
        out
    }

    fn live_bytes(&self) -> u64 {
        self.live_ptr_bytes
    }

    fn max_key_len(&self) -> u64 {
        self.max_key_len
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alloc::SegmentId;
    use std::collections::BTreeMap;

    fn entry(seq: u64) -> IndexEntry {
        IndexEntry {
            ptr: ValuePtr {
                segment: SegmentId(seq),
                offset: 0,
                len: 1,
            },
            seq,
            version: seq as u128,
            tombstone: false,
        }
    }

    #[test]
    fn hash_point_ops() {
        let mut idx = HashIndex::new();
        assert!(idx.is_empty());
        idx.put(b"a", entry(1));
        idx.put(b"b", entry(2));
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.get(b"a"), Some(entry(1)));
        idx.put(b"a", entry(5));
        assert_eq!(idx.get(b"a"), Some(entry(5)));
        assert_eq!(idx.remove(b"a"), Some(entry(5)));
        assert_eq!(idx.get(b"a"), None);
        assert_eq!(idx.ordering(), Ordering::Unsorted);
        assert!(idx.scan(None, None).is_none());
        assert!(idx.scan_page(None, None, 10, 1024).is_none());
    }

    #[test]
    fn ordered_scan_page_respects_budget() {
        let mut idx = FrontCodedIndex::new();
        for k in [b"a", b"b", b"c", b"d", b"e"] {
            idx.put(
                k,
                IndexEntry {
                    ptr: ValuePtr {
                        segment: SegmentId(0),
                        offset: 0,
                        len: 100,
                    },
                    seq: 0,
                    version: 0,
                    tombstone: false,
                },
            );
        }

        let (page, more) = idx.scan_page(None, None, 2, usize::MAX).unwrap();
        let keys: Vec<_> = page.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
        assert!(more);

        let (page, more) = idx.scan_page(None, None, 5, usize::MAX).unwrap();
        assert_eq!(page.len(), 5);
        assert!(!more);

        let (page, more) = idx.scan_page(None, None, usize::MAX, 202).unwrap();
        assert_eq!(page.len(), 2);
        assert!(more);

        let (page, more) = idx.scan_page(None, None, usize::MAX, 1).unwrap();
        assert_eq!(page.len(), 1);
        assert!(more);

        let (page, more) = idx
            .scan_page(Some(b"b"), Some(b"e"), 2, usize::MAX)
            .unwrap();
        let keys: Vec<_> = page.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec()]);
        assert!(more);
        let (page, more) = idx
            .scan_page(Some(b"d"), Some(b"e"), 2, usize::MAX)
            .unwrap();
        let keys: Vec<_> = page.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![b"d".to_vec()]);
        assert!(!more);
    }

    #[test]
    fn ordered_point_ops_and_scan() {
        let mut idx = FrontCodedIndex::new();
        for (i, k) in [b"d", b"a", b"c", b"b", b"e"].iter().enumerate() {
            idx.put(*k, entry(i as u64));
        }
        assert_eq!(idx.ordering(), Ordering::Sorted);

        let all = idx.scan(None, None).unwrap();
        let keys: Vec<_> = all.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![b"a", b"b", b"c", b"d", b"e"]);

        let mid = idx.scan(Some(b"b"), Some(b"e")).unwrap();
        let mid_keys: Vec<_> = mid.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(mid_keys, vec![b"b", b"c", b"d"]);

        let from_c = idx.scan(Some(b"c"), None).unwrap();
        assert_eq!(from_c.len(), 3);
    }

    #[test]
    fn front_coded_point_ops_last_write_wins() {
        let mut idx = FrontCodedIndex::new();
        assert!(idx.is_empty());
        idx.put(b"shared/key/aaa", entry(1));
        idx.put(b"shared/key/bbb", entry(2));
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.get(b"shared/key/aaa"), Some(entry(1)));
        assert_eq!(idx.get(b"shared/key/bbb"), Some(entry(2)));
        assert_eq!(idx.get(b"absent"), None);
        let kb = idx.key_bytes();
        idx.put(b"shared/key/aaa", entry(9));
        assert_eq!(idx.get(b"shared/key/aaa"), Some(entry(9)));
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.key_bytes(), kb);
        assert_eq!(idx.remove(b"shared/key/aaa"), Some(entry(9)));
        assert_eq!(idx.get(b"shared/key/aaa"), None);
        assert_eq!(idx.remove(b"shared/key/aaa"), None);
    }

    #[test]
    fn front_coded_splits_and_stays_sorted() {
        let mut idx = FrontCodedIndex::new();
        let n = 500u32;
        let key = |i: u32| format!("app.bsky.feed.like/did:plc:abcdef/{i:08}").into_bytes();
        let mut order: Vec<u32> = (0..n).collect();
        for i in 0..n {
            order.swap(i as usize, ((i.wrapping_mul(2654435761)) % n) as usize);
        }
        for &i in &order {
            idx.put(&key(i), entry(i as u64));
        }
        assert_eq!(idx.len(), n as usize);
        assert!(idx.blocks.len() > 1, "should have split into many blocks");
        for i in 0..n {
            assert_eq!(idx.get(&key(i)), Some(entry(i as u64)));
        }
        let scan = idx.scan(None, None).unwrap();
        assert_eq!(scan.len(), n as usize);
        assert!(scan.windows(2).all(|w| w[0].0 < w[1].0));
    }

    #[test]
    fn front_coded_rekeys_on_split_and_first_key_ops() {
        let mut idx = FrontCodedIndex::new();
        let key = |i: u32| format!("k{i:05}").into_bytes();
        for i in 0..400u32 {
            idx.put(&key(i), entry(i as u64));
        }
        let check_invariant = |idx: &FrontCodedIndex| {
            for (mk, block) in &idx.blocks {
                let first = block.decode_pairs()[0].0.clone();
                assert_eq!(
                    mk.as_ref(),
                    first.as_slice(),
                    "map key must equal the block's decoded first key"
                );
            }
        };
        assert!(idx.blocks.len() > 2);
        check_invariant(&idx);

        idx.put(b"a-smallest", entry(1000));
        assert_eq!(idx.get(b"a-smallest"), Some(entry(1000)));
        assert!(idx.blocks.contains_key(b"a-smallest".as_slice()));
        check_invariant(&idx);

        for _ in 0..5 {
            let victim = idx.blocks.keys().next().unwrap().clone();
            assert!(idx.remove(&victim).is_some());
            assert_eq!(idx.get(&victim), None);
            check_invariant(&idx);
        }

        let scan = idx.scan(None, None).unwrap();
        assert!(scan.windows(2).all(|w| w[0].0 < w[1].0));
        for (k, e) in &scan {
            assert_eq!(idx.get(k), Some(*e));
        }
    }

    #[test]
    fn key_bytes_tracks_insert_overwrite_remove() {
        for mut idx in [
            Box::new(HashIndex::new()) as Box<dyn Index>,
            Box::new(FrontCodedIndex::new()),
        ] {
            assert_eq!(idx.key_bytes(), 0);

            idx.put(b"ab", entry(1));
            idx.put(b"cde", entry(2));
            assert_eq!(idx.key_bytes(), 5);

            idx.put(b"ab", entry(3));
            assert_eq!(idx.key_bytes(), 5);

            assert!(idx.remove(b"ab").is_some());
            assert_eq!(idx.key_bytes(), 3);

            assert!(idx.remove(b"zzz").is_none());
            assert_eq!(idx.key_bytes(), 3);

            assert!(idx.remove(b"cde").is_some());
            assert_eq!(idx.key_bytes(), 0);
        }
    }

    #[test]
    fn hash_index_empty_reset_preserves_overlap_and_order() {
        let mut idx = HashIndex::new();
        for cycle in 0..20u64 {
            for i in 0..10_000u64 {
                idx.put(&(i * 20 + cycle).to_be_bytes(), entry(i));
            }
            let (page, more) = idx.maintenance_page(None, 20_000);
            assert!(!more);
            assert!(page.windows(2).all(|w| w[0].0 < w[1].0));
            idx.put(&page[0].0, entry(99_999));
            for (key, e) in &page { idx.remove_if_eq(key, e); }
            assert_eq!(idx.len(), 1);
            assert_eq!(idx.get(&page[0].0), Some(entry(99_999)));
            idx.remove(&page[0].0);
            assert_eq!(idx.len(), 0);
            assert_eq!(idx.map.capacity(), idx.map_capacity);
            assert_eq!(idx.map_capacity, 14_336);
            assert_eq!(idx.allocated_bytes(), std::mem::size_of::<HashIndex>() as u64 + 16_384 * 57 + 16);
        }
    }

    #[test]
    #[ignore]
    fn sorted_index_retained_capacity_measurement() {
        for key_len in [70, 148] {
            let mut idx = FrontCodedIndex::new();
            for i in 0..1_000_000u64 {
                let mut key = vec![b'x'; key_len];
                key[key_len - 8..].copy_from_slice(&i.to_be_bytes());
                idx.put(&key, entry(i));
            }
            eprintln!("SORTED397 key_len={key_len} stage=full entries={} allocated={}", idx.len(), idx.allocated_bytes());
            loop {
                let (page, more) = idx.maintenance_page(None, 4096);
                let entries: Vec<_> = page.into_iter().map(|(key, e)| SnapshotEntry {
                    key, ptr: e.ptr, seq: e.seq, version: e.version, tombstone: e.tombstone,
                }).collect();
                idx.remove_if_eq_rev(&entries);
                if idx.len() <= 200_000 && idx.len() + entries.len() > 200_000 {
                    eprintln!("SORTED397 key_len={key_len} stage=partial entries={} allocated={}", idx.len(), idx.allocated_bytes());
                }
                if !more { break; }
            }
            assert!(idx.is_empty());
            eprintln!("SORTED397 key_len={key_len} stage=empty entries={} allocated={}", idx.len(), idx.allocated_bytes());
        }
    }

    #[test]
    #[ignore]
    fn hash_index_deleted_slot_measurement() {
        for clear in [false, true] {
            let mut map: HashMap<Arc<[u8]>, PackedEntry, FxBuildHasher> = HashMap::default();
            for cycle in 0..100u64 {
                for i in 0..1_010_000u64 {
                    map.insert(Arc::from((i * 100 + cycle).to_be_bytes().as_slice()), PackedEntry::pack(&entry(i)));
                }
                let full = map.capacity();
                for i in (u64::from(clear)..1_010_000u64).rev() {
                    map.remove((i * 100 + cycle).to_be_bytes().as_slice());
                }
                let empty = map.capacity();
                if clear { map.clear(); }
                if cycle % 10 == 0 || cycle == 99 {
                    eprintln!("HASH397 clear={clear} cycle={cycle} full={full} removed={empty} reset={}", map.capacity());
                }
            }
        }
    }

    #[test]
    fn factory_picks_impl() {
        assert_eq!(new_index(Ordering::Sorted).ordering(), Ordering::Sorted);
        assert_eq!(new_index(Ordering::Unsorted).ordering(), Ordering::Unsorted);
        assert!(new_index(Ordering::Unsorted).scan(None, None).is_none());
        assert!(new_index(Ordering::Sorted).scan(None, None).is_some());
    }

    fn mk_entry(
        segment: u64,
        offset: u32,
        len: u32,
        seq: u64,
        version: u128,
        tombstone: bool,
    ) -> IndexEntry {
        IndexEntry {
            ptr: ValuePtr {
                segment: SegmentId(segment),
                offset,
                len,
            },
            seq,
            version,
            tombstone,
        }
    }

    #[test]
    fn packed_entry_size_is_36_bytes() {
        assert_eq!(std::mem::size_of::<PackedEntry>(), 36);
        assert_eq!(std::mem::align_of::<PackedEntry>(), 1);
        assert_eq!(std::mem::size_of::<IndexEntry>(), 48);
    }

    #[test]
    fn packed_entry_round_trips() {
        let segments = [0u64, 1, 12345, u32::MAX as u64];
        let offsets = [0u32, 7, u32::MAX];
        let lens = [0u32, 100, u32::MAX];
        let seqs = [0u64, 42, TOMBSTONE_BIT - 1];
        let versions = [0u128, 1, u128::MAX];
        for &segment in &segments {
            for &offset in &offsets {
                for &len in &lens {
                    for &seq in &seqs {
                        for &version in &versions {
                            for &tombstone in &[false, true] {
                                let e = mk_entry(segment, offset, len, seq, version, tombstone);
                                let round = PackedEntry::pack(&e).unpack();
                                assert_eq!(round, e, "round-trip mismatch for {e:?}");
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn packed_entry_direct_accessors() {
        let e = mk_entry(3, 9, 777, 55, 123456789, true);
        let p = PackedEntry::pack(&e);
        assert_eq!(p.ptr_len(), 777);
        assert_eq!(p.version(), 123456789);
    }

    #[test]
    #[should_panic(expected = "exceeds the u32 packed-index-entry space")]
    fn packed_entry_rejects_oversized_segment() {
        let e = mk_entry(u32::MAX as u64 + 1, 0, 0, 0, 0, false);
        let _ = PackedEntry::pack(&e);
    }

    #[test]
    #[should_panic(expected = "exceeds the 63-bit packed-index-entry space")]
    fn packed_entry_rejects_oversized_seq() {
        let e = mk_entry(0, 0, 0, TOMBSTONE_BIT, 0, false);
        let _ = PackedEntry::pack(&e);
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    fn gen_key(rng: &mut Rng) -> Vec<u8> {
        const PREFIXES: &[&[u8]] = &[
            b"app.bsky.feed.like/did:plc:aaaaaaaa/",
            b"app.bsky.feed.like/did:plc:aaaaaaab/",
            b"app.bsky.feed.post/did:plc:zzzz/",
            b"at://did:plc:qqqq/\0",
            b"",
            b"\0",
        ];
        let mut k = PREFIXES[rng.below(PREFIXES.len())].to_vec();
        let suffix_len = rng.below(6);
        for _ in 0..suffix_len {
            k.push((rng.below(4)) as u8);
        }
        k
    }

    #[test]
    fn range_count_median_matches_materialized_scan() {
        let mut rng = Rng(0x0415_0415_0415_0415);
        let mut idx = FrontCodedIndex::new();
        let mut checked = 0usize;
        for step in 0..30_000u64 {
            let key = gen_key(&mut rng);
            if rng.below(4) == 0 {
                idx.remove(&key);
            } else {
                idx.put(&key, entry(step));
            }
            if step % 500 != 499 {
                continue;
            }
            let firsts: Vec<Vec<u8>> = idx.blocks.keys().map(|k| k.to_vec()).collect();
            for _ in 0..200 {
                let bound = |rng: &mut Rng| match rng.below(4) {
                    0 => None,
                    1 => Some(firsts[rng.below(firsts.len())].clone()),
                    _ => Some(gen_key(rng)),
                };
                let lo = bound(&mut rng);
                let hi = bound(&mut rng);
                let scanned = idx.scan(lo.as_deref(), hi.as_deref()).unwrap();
                let expected = if lo.is_some() && hi.is_some() && lo >= hi {
                    (0, None)
                } else {
                    (scanned.len(), (scanned.len() >= 2).then(|| scanned[scanned.len() / 2].0.clone()))
                };
                assert_eq!(idx.range_count_median(lo.as_deref(), hi.as_deref()), Some(expected), "lo={lo:?} hi={hi:?}");
                checked += 1;
            }
        }
        assert_eq!(checked, 12_000);
        assert!(idx.blocks.len() > 20, "bounds cross many blocks");
        assert!(HashIndex::new().range_count_median(None, None).is_none());
    }

    #[test]
    fn front_coded_matches_btreemap_reference() {
        let mut rng = Rng(0x1234_5678_9abc_def0);
        let mut idx = FrontCodedIndex::new();
        let mut reference: BTreeMap<Vec<u8>, IndexEntry> = BTreeMap::new();

        for step in 0..20_000u64 {
            let key = gen_key(&mut rng);
            match rng.below(10) {
                0..=6 => {
                    let e = entry(step);
                    idx.put(&key, e);
                    reference.insert(key.clone(), e);
                }
                7 | 8 => {
                    let a = idx.remove(&key);
                    let b = reference.remove(&key);
                    assert_eq!(a, b, "remove disagreed on {key:?}");
                }
                _ => {
                    assert_eq!(idx.get(&key), reference.get(&key).copied(), "get {key:?}");
                }
            }

            check_after_step(&idx, &key);
            if step % 97 == 0 {
                check_front_coded(&idx, &reference);
                assert_eq!(idx.len(), reference.len());
                assert_eq!(
                    idx.key_bytes(),
                    reference.keys().map(|k| k.len() as u64).sum::<u64>()
                );

                let full: Vec<_> = idx.scan(None, None).unwrap();
                let refv: Vec<_> = reference.iter().map(|(k, e)| (k.clone(), *e)).collect();
                assert_eq!(full, refv);

                let mut ik = idx.keys();
                ik.sort();
                let rk: Vec<Vec<u8>> = reference.keys().cloned().collect();
                assert_eq!(ik, rk);
                assert_eq!(idx.versions().len(), reference.len());

                let mut a = gen_key(&mut rng);
                let mut b = gen_key(&mut rng);
                if a > b {
                    std::mem::swap(&mut a, &mut b);
                }
                let lo = if rng.below(2) == 0 {
                    Some(a.as_slice())
                } else {
                    None
                };
                let hi = if rng.below(2) == 0 {
                    Some(b.as_slice())
                } else {
                    None
                };
                let got = idx.scan(lo, hi).unwrap();
                let want: Vec<_> = reference
                    .iter()
                    .filter(|(k, _)| lo.is_none_or(|l| k.as_slice() >= l))
                    .filter(|(k, _)| hi.is_none_or(|h| k.as_slice() < h))
                    .map(|(k, e)| (k.clone(), *e))
                    .collect();
                assert_eq!(got, want, "bounded scan lo={lo:?} hi={hi:?}");

                let max_entries = 1 + rng.below(5);
                let max_bytes = 1 + rng.below(400);
                let (page, more) = idx.scan_page(lo, hi, max_entries, max_bytes).unwrap();
                assert_eq!(page.as_slice(), &want[..page.len().min(want.len())]);
                if more {
                    assert!(page.len() < want.len(), "more implies a remainder exists");
                } else {
                    assert_eq!(
                        page.len(),
                        want.len(),
                        "no-more implies the page is complete"
                    );
                }
            }
        }
    }

    #[test]
    fn front_coded_scan_page_edges_at_block_boundaries() {
        let mut idx = FrontCodedIndex::new();
        let key = |i: u32| format!("k{i:05}").into_bytes();
        for i in 0..300u32 {
            idx.put(&key(i), entry(i as u64));
        }
        assert!(idx.blocks.len() > 1);
        let first_of_block1 = idx.blocks.keys().nth(1).unwrap().clone();
        assert!(idx.remove(&first_of_block1).is_some());
        assert_eq!(idx.get(&first_of_block1), None);
        assert!(!idx.blocks.contains_key(&first_of_block1));

        let full = idx.scan(None, None).unwrap();
        let mut reassembled: Vec<(Vec<u8>, IndexEntry)> = Vec::new();
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let lo = cursor.as_deref();
            let (page, more) = idx.scan_page(lo, None, 7, usize::MAX).unwrap();
            if page.is_empty() {
                break;
            }
            let mut next = page.last().unwrap().0.clone();
            next.push(0);
            reassembled.extend(page);
            cursor = Some(next);
            if !more {
                break;
            }
        }
        assert_eq!(reassembled, full);
    }

    #[test]
    fn front_coded_splice_directed_cases() {
        let mut idx = FrontCodedIndex::new();
        let mut reference: BTreeMap<Vec<u8>, IndexEntry> = BTreeMap::new();
        let mut ins_seq = 0u64;
        let mut put =
            |idx: &mut FrontCodedIndex, reference: &mut BTreeMap<Vec<u8>, IndexEntry>, k: &[u8]| {
                let e = entry(ins_seq);
                ins_seq += 1;
                idx.put(k, e);
                reference.insert(k.to_vec(), e);
                assert_eq!(idx.blocks.len(), 1, "should not split in this test");
                let scan = idx.scan(None, None).unwrap();
                let want: Vec<_> = reference.iter().map(|(k, e)| (k.clone(), *e)).collect();
                assert_eq!(scan, want, "scan mismatch after inserting {k:?}");
                for (rk, re) in reference.iter() {
                    assert_eq!(idx.get(rk), Some(*re), "get {rk:?}");
                }
            };
        put(&mut idx, &mut reference, b"m_base");
        put(&mut idx, &mut reference, b"prefix/aaa");
        put(&mut idx, &mut reference, b"prefix/zzz");

        put(&mut idx, &mut reference, b"prefix/mmm");
        put(&mut idx, &mut reference, b"prefix/zz");
        put(&mut idx, &mut reference, b"m_base\0");

        idx.put(b"prefix/mmm", entry(999));
        reference.insert(b"prefix/mmm".to_vec(), entry(999));
        assert_eq!(idx.get(b"prefix/mmm"), Some(entry(999)));
        assert_eq!(
            idx.remove(b"prefix/zz"),
            reference.remove(b"prefix/zz".as_slice())
        );
        assert_eq!(
            idx.remove(b"prefix/aaa"),
            reference.remove(b"prefix/aaa".as_slice())
        );
        let scan = idx.scan(None, None).unwrap();
        let want: Vec<_> = reference.iter().map(|(k, e)| (k.clone(), *e)).collect();
        assert_eq!(scan, want);
    }

    #[test]
    fn front_coded_splice_fills_block_then_splits() {
        let mut idx = FrontCodedIndex::new();
        let mut reference: BTreeMap<Vec<u8>, IndexEntry> = BTreeMap::new();
        let key = |i: usize| format!("shared/prefix/{i:04}").into_bytes();
        for i in 0..BLOCK_MAX {
            idx.put(&key(i), entry(i as u64));
            reference.insert(key(i), entry(i as u64));
        }
        assert_eq!(idx.blocks.len(), 1, "BLOCK_MAX entries fit one block");
        let block = idx.blocks.values().next().unwrap();
        assert!(
            block.entries.capacity() <= entries_target(block.entries.len()),
            "entries growth stays within one step of the live count"
        );
        assert!(block.packed.capacity() <= packed_target(block.packed.len()));
        idx.put(&key(BLOCK_MAX), entry(BLOCK_MAX as u64));
        reference.insert(key(BLOCK_MAX), entry(BLOCK_MAX as u64));
        assert_eq!(idx.blocks.len(), 2, "the overflow insert splits the block");
        let scan = idx.scan(None, None).unwrap();
        let want: Vec<_> = reference.iter().map(|(k, e)| (k.clone(), *e)).collect();
        assert_eq!(scan, want);
        for i in 0..=BLOCK_MAX {
            assert_eq!(idx.get(&key(i)), Some(entry(i as u64)));
        }
        for (mk, block) in &idx.blocks {
            assert_eq!(mk.as_ref(), block.decode_pairs()[0].0.as_slice());
        }
    }

    #[test]
    fn front_coded_splice_heavy_random_single_block() {
        let mut idx = FrontCodedIndex::new();
        let mut reference: BTreeMap<Vec<u8>, IndexEntry> = BTreeMap::new();
        let mut rng = Rng(0x5eed_1234);
        idx.put(b"aaaaaa/base", entry(0));
        reference.insert(b"aaaaaa/base".to_vec(), entry(0));
        for step in 1..4_000u64 {
            let mut k = b"aaaaaa/k".to_vec();
            k.push(b'a' + (rng.below(30) as u8));
            match rng.below(6) {
                0 => {
                    let a = idx.remove(&k);
                    let b = reference.remove(&k);
                    assert_eq!(a, b, "remove {k:?}");
                }
                _ => {
                    let e = entry(step);
                    idx.put(&k, e);
                    reference.insert(k.clone(), e);
                }
            }
            if step % 101 == 0 {
                assert_eq!(idx.len(), reference.len());
                let scan = idx.scan(None, None).unwrap();
                let want: Vec<_> = reference.iter().map(|(k, e)| (k.clone(), *e)).collect();
                assert_eq!(scan, want);
                assert_eq!(idx.blocks.len(), 1, "small live set stays one block");
            }
        }
    }

    fn check_front_coded(idx: &FrontCodedIndex, reference: &BTreeMap<Vec<u8>, IndexEntry>) {
        idx.flush_page(None, usize::MAX).unwrap();
        let mut all: Vec<(Vec<u8>, IndexEntry)> = Vec::new();
        for (mk, block) in &idx.blocks {
            let n = block.entries.len();
            assert!((1..=BLOCK_MAX).contains(&n), "block size {n}");
            let p = block.packed.as_slice();
            let mut off = 0usize;
            let mut prev: Vec<u8> = Vec::new();
            for i in 0..n {
                let key = if i == 0 {
                    let len = read_uvarint(p, &mut off) as usize;
                    let k = p[off..off + len].to_vec();
                    off += len;
                    assert_eq!(mk.as_ref(), k.as_slice(), "map key is the first key");
                    k
                } else {
                    let shared = read_uvarint(p, &mut off) as usize;
                    let len = read_uvarint(p, &mut off) as usize;
                    let mut k = prev[..shared].to_vec();
                    k.extend_from_slice(&p[off..off + len]);
                    off += len;
                    let exact = prev.iter().zip(&k).take_while(|(a, b)| a == b).count();
                    assert_eq!(shared, exact, "shared prefix must be the exact lcp");
                    k
                };
                if let Some((last, _)) = all.last() {
                    assert!(*last < key, "keys strictly increase across blocks");
                }
                all.push((key.clone(), block.entries[i].unpack()));
                prev = key;
            }
            assert_eq!(off, p.len(), "packed fully consumed");
            assert!(block.packed.capacity() <= 2 * packed_target(p.len()));
            assert!(block.entries.capacity() <= 2 * entries_target(n));
        }
        let want: Vec<(Vec<u8>, IndexEntry)> =
            reference.iter().map(|(k, e)| (k.clone(), *e)).collect();
        assert_eq!(all, want);
        assert_eq!(idx.len(), reference.len());
        assert_eq!(
            idx.key_bytes(),
            reference.keys().map(|k| k.len() as u64).sum::<u64>()
        );
        assert_eq!(
            idx.live_bytes(),
            reference.values().map(|e| e.ptr.len as u64).sum::<u64>()
        );
        if reference.is_empty() {
            assert_eq!(idx.max_key_len(), 0);
            assert!(idx.blocks.is_empty());
        } else {
            assert!(
                idx.max_key_len() >= reference.keys().map(|k| k.len() as u64).max().unwrap()
            );
        }
        let versions: Vec<(Box<[u8]>, u128)> = reference
            .iter()
            .map(|(k, e)| (k.clone().into_boxed_slice(), e.version))
            .collect();
        assert_eq!(idx.versions(), versions);
        let mut segs = Vec::new();
        idx.for_each_value_segment(&mut |s| segs.push(s.0));
        let mut want_segs: Vec<u64> = reference.values().map(|e| e.ptr.segment.0).collect();
        segs.sort_unstable();
        want_segs.sort_unstable();
        assert_eq!(segs, want_segs);
    }

    fn rich_entry(seq: u64) -> IndexEntry {
        mk_entry(
            seq % 1013,
            seq as u32,
            (seq % 977) as u32,
            seq,
            (seq as u128) << 70 | 5,
            seq.is_multiple_of(7),
        )
    }

    fn gen_long_key(rng: &mut Rng) -> Vec<u8> {
        const LENS: &[usize] = &[0, 1, 2, 126, 127, 128, 129, 130, 200, 255, 256, 257, 290];
        let base: Vec<u8> = (0..300u32).map(|i| b'a' + (i % 26) as u8).collect();
        let mut k = base[..LENS[rng.below(LENS.len())]].to_vec();
        match rng.below(8) {
            0 => {
                let n = 120 + rng.below(100);
                for _ in 0..n {
                    k.push(b'0' + rng.below(3) as u8);
                }
            }
            1 => {}
            _ => {
                let alphabet = [0u8, 0x7f, 0xff, b'a' + (k.len() % 26) as u8];
                for _ in 0..rng.below(4) {
                    k.push(alphabet[rng.below(alphabet.len())]);
                }
            }
        }
        k
    }

    fn model_step(
        idx: &mut FrontCodedIndex,
        reference: &mut BTreeMap<Vec<u8>, IndexEntry>,
        rng: &mut Rng,
        key: Vec<u8>,
        step: u64,
    ) {
        match rng.below(20) {
            0..=9 => {
                let e = rich_entry(step);
                idx.put(&key, e);
                reference.insert(key.clone(), e);
            }
            10..=12 => {
                assert_eq!(idx.remove(&key), reference.remove(&key), "remove {key:?}");
            }
            13..=14 => {
                let current = reference.get(&key).copied();
                let expected = current.unwrap_or_else(|| rich_entry(step));
                let removed = idx.remove_if_eq(&key, &expected);
                assert_eq!(removed, current.is_some(), "remove_if_eq current {key:?}");
                if removed {
                    reference.remove(&key);
                }
            }
            15..=16 => {
                let stale = rich_entry(step + 1_000_000);
                assert!(!idx.remove_if_eq(&key, &stale), "stale remove_if_eq {key:?}");
                if let Some(cur) = reference.get(&key).copied() {
                    let mut near = cur;
                    near.tombstone = !near.tombstone;
                    assert!(!idx.remove_if_eq(&key, &near));
                    near = cur;
                    near.version ^= 1 << 100;
                    assert!(!idx.remove_if_eq(&key, &near));
                }
            }
            _ => {
                assert_eq!(idx.get(&key), reference.get(&key).copied(), "get {key:?}");
            }
        }
        check_after_step(idx, &key);
    }

    fn full_lane() -> bool {
        std::env::var_os("VALS_TEST_FULL").is_some_and(|v| v != "0")
    }

    fn check_after_step(idx: &FrontCodedIndex, key: &[u8]) {
        if full_lane() {
            idx.flush_page(None, usize::MAX).unwrap();
            return;
        }
        let below = idx.blocks.range::<[u8], _>((Bound::Unbounded, Bound::Included(key))).rev().take(2);
        let above = idx.blocks.range::<[u8], _>((Bound::Excluded(key), Bound::Unbounded)).take(1);
        for (first, block) in below.chain(above) {
            assert!(block.checked_pairs(first).is_some(), "block {first:?} fails its integrity check after a step on {key:?}");
        }
    }

    #[test]
    fn front_coded_integrity_detects_shared_suffix_metadata_and_malformed_lengths() {
        let keys: Vec<Vec<u8>> = (0..4).map(|i| {
            format!("{}\0at://did:plc:{i:024x}/app.bsky.feed.like/3mee7ygcz4k2b\0subject.uri",
                if i == 0 {
                    "at://did:plc:11niu5w3iw4cghdpdqomndhd/app.bsky.feed.post/3mee7ygcz4k2b"
                } else {
                    "at://did:plc:22niu5w3iw4cghdpdqomndhd/app.bsky.feed.post/3mee7ygcz4k2b"
                }).into_bytes()
        }).collect();
        let make = || {
            let mut idx = FrontCodedIndex::new();
            for (i, key) in keys.iter().enumerate() { idx.put(key, rich_entry(i as u64)); }
            idx
        };
        let mut idx = make();
        let block = idx.blocks.values().next().unwrap();
        let mut off = 0;
        let mut key = Vec::new();
        block.decode_key_into(0, &mut off, &mut key);
        let shared = read_uvarint(&block.packed, &mut off) as usize;
        read_uvarint(&block.packed, &mut off);
        idx.flip_packed_bit(0, off + 73 - shared, 0x10);
        let decoded = idx.scan(None, None).unwrap();
        assert_eq!(decoded[0].0, keys[0]);
        assert!(decoded[1..].iter().all(|(key, _)| key[73] == b'*'));
        assert!(idx.flush_page(None, usize::MAX).is_err());
        for i in 4..=BLOCK_MAX {
            idx.put(format!("zzzz/{i:04}").as_bytes(), rich_entry(i as u64));
        }
        assert!(idx.blocks.len() > 1);
        assert!(idx.flush_page(None, usize::MAX).is_err());
        let mut idx = make();
        idx.blocks.values_mut().next().unwrap().entries[2].0[25] ^= 0x10;
        assert!(idx.flush_page(None, usize::MAX).is_err());
        let mut idx = make();
        idx.flip_packed_bit(0, 1, 0x10);
        assert!(idx.flush_page(None, usize::MAX).is_err());
    }

    #[test]
    fn front_coded_integrity_diagnosis_locates_single_bit_flips() {
        let keys: Vec<Vec<u8>> = (0..4).map(|i| {
            format!("{}\0at://did:plc:{i:024x}/app.bsky.feed.like/3mee7ygcz4k2b\0subject.uri",
                if i == 0 {
                    "at://did:plc:11niu5w3iw4cghdpdqomndhd/app.bsky.feed.post/3mee7ygcz4k2b"
                } else {
                    "at://did:plc:22niu5w3iw4cghdpdqomndhd/app.bsky.feed.post/3mee7ygcz4k2b"
                }).into_bytes()
        }).collect();
        let make = || {
            let mut idx = FrontCodedIndex::new();
            for (i, key) in keys.iter().enumerate() { idx.put(key, rich_entry(i as u64)); }
            idx
        };
        let finding = |idx: &FrontCodedIndex| {
            let error = idx.flush_page(None, usize::MAX).unwrap_err();
            assert_eq!(error.first_key.as_ref(), keys[0].as_slice());
            assert!(error.to_string().starts_with("front-coded index integrity mismatch block_first_key=["));
            error.finding
        };
        let flip = |f: &IntegrityFinding| match f {
            IntegrityFinding::BitFlip { region, key_index, offset, actual, expected, candidates, .. } =>
                (*region, *key_index, *offset, *actual, *expected, *candidates),
            other => panic!("not a single-bit finding: {other}"),
        };
        let suffix_off = |idx: &FrontCodedIndex| {
            let block = idx.blocks.values().next().unwrap();
            let mut off = 0;
            let mut key = Vec::new();
            block.decode_key_into(0, &mut off, &mut key);
            let shared = read_uvarint(&block.packed, &mut off) as usize;
            let meta = off - 1;
            read_uvarint(&block.packed, &mut off);
            (meta, off, shared)
        };

        let mut idx = make();
        let (_, off, shared) = suffix_off(&idx);
        idx.flip_packed_bit(0, off + 73 - shared, 0x10);
        let f = finding(&idx);
        assert_eq!(flip(&f), ("key", 1, 73, b'*', b':', 1));
        assert_eq!(f.bit_flip_xor(), Some(0x10));
        assert!(f.to_string().contains("xor=0x10"), "{f}");
        for i in 4..=BLOCK_MAX {
            idx.put(format!("zzzz/{i:04}").as_bytes(), rich_entry(i as u64));
        }
        assert!(idx.blocks.len() > 1);
        match finding(&idx) {
            IntegrityFinding::AtSplit(inner) => assert_eq!(flip(&inner), ("key", 1, 73, b'*', b':', 1)),
            other => panic!("expected a split-time finding, got {other}"),
        }

        let mut idx = make();
        let before = idx.blocks.values().next().unwrap().entries[2].0[25];
        idx.blocks.values_mut().next().unwrap().entries[2].0[25] ^= 0x10;
        assert_eq!(flip(&finding(&idx)), ("entry.version", 2, 25, before ^ 0x10, before, 1));

        let mut idx = make();
        idx.flip_packed_bit(0, 2, 0x10);
        assert_eq!(flip(&finding(&idx)), ("key", 0, 0, b'a' ^ 0x10, b'a', 1));

        let mut idx = make();
        let len_byte = idx.blocks.values().next().unwrap().packed[1];
        idx.flip_packed_bit(0, 1, 0x10);
        assert_eq!(flip(&finding(&idx)), ("key_len", 0, 1, len_byte ^ 0x10, len_byte, 1));

        let mut idx = make();
        idx.blocks.values_mut().next().unwrap().integrity ^= 1 << 13;
        assert_eq!(flip(&finding(&idx)).0, "checksum");
        assert_eq!(flip(&finding(&idx)).2, 1);

        let mut idx = make();
        let (first, block) = idx.blocks.pop_first().unwrap();
        let mut first = first.to_vec();
        first[5] ^= 0x10;
        idx.blocks.insert(first.into_boxed_slice(), block);
        let error = idx.flush_page(None, usize::MAX).unwrap_err();
        assert_eq!(flip(&error.finding), ("block_key", 0, 5, b'd' ^ 0x10, b'd', 1));

        let mut idx = make();
        let (meta, _, _) = suffix_off(&idx);
        let shared_byte = idx.blocks.values().next().unwrap().packed[meta];
        idx.flip_packed_bit(0, meta, 0x01);
        let f = finding(&idx);
        let (region, key_index, offset, actual, expected, _) = flip(&f);
        assert_eq!((region, key_index, offset, actual, expected), ("key_len", 1, meta, shared_byte ^ 0x01, shared_byte));

        let mut idx = make();
        let block = idx.blocks.values_mut().next().unwrap();
        block.entries[1].0[3] ^= 0x01;
        block.entries[2].0[3] ^= 0x01;
        match finding(&idx) {
            IntegrityFinding::Unexplained { reason, computed, searched, .. } => {
                assert_eq!(reason, "checksum");
                assert!(computed.is_some() && searched);
            }
            other => panic!("a two-bit error must not look like a single flip: {other}"),
        }
    }

    #[test]
    fn front_coded_matches_reference_with_remove_if_eq() {
        let mut rng = Rng(0x0313_0313_0313_0313);
        let mut idx = FrontCodedIndex::new();
        let mut reference: BTreeMap<Vec<u8>, IndexEntry> = BTreeMap::new();
        for step in 0..30_000u64 {
            let key = gen_key(&mut rng);
            model_step(&mut idx, &mut reference, &mut rng, key, step);
            if step % 211 == 0 {
                check_front_coded(&idx, &reference);
            }
        }
        check_front_coded(&idx, &reference);
    }

    #[test]
    fn front_coded_long_shared_prefixes_and_prefix_keys() {
        let mut rng = Rng(0xfeed_beef_0313);
        let mut idx = FrontCodedIndex::new();
        let mut reference: BTreeMap<Vec<u8>, IndexEntry> = BTreeMap::new();
        for step in 0..12_000u64 {
            let key = gen_long_key(&mut rng);
            model_step(&mut idx, &mut reference, &mut rng, key, step);
            if step % 101 == 0 {
                check_front_coded(&idx, &reference);
                let after = gen_long_key(&mut rng);
                let (page, more) = idx.maintenance_page(Some(&after), 1 + rng.below(70));
                let want: Vec<_> = reference
                    .range::<Vec<u8>, _>((Bound::Excluded(after.clone()), Bound::Unbounded))
                    .map(|(k, e)| (k.clone(), *e))
                    .collect();
                assert_eq!(page.as_slice(), &want[..page.len()]);
                assert_eq!(more, page.len() < want.len());
            }
        }
        assert!(idx.blocks.len() > 1);
        check_front_coded(&idx, &reference);
        let keys: Vec<Vec<u8>> = reference.keys().cloned().collect();
        for k in keys {
            assert_eq!(idx.remove(&k), reference.remove(&k));
        }
        check_front_coded(&idx, &reference);
    }

    #[test]
    fn front_coded_flush_commit_pattern_descending_remove_if_eq() {
        for (seed, long, batch) in [
            (1u64, false, false),
            (2, true, false),
            (3, false, false),
            (1, false, true),
            (2, true, true),
            (4, false, true),
            (5, true, true),
        ] {
            let mut rng = Rng(0xabc0_0313 ^ seed);
            let mut idx = FrontCodedIndex::new();
            let mut reference: BTreeMap<Vec<u8>, IndexEntry> = BTreeMap::new();
            let mut step = 0u64;
            let next_key = |rng: &mut Rng| {
                let mut k = if long { gen_long_key(rng) } else { gen_key(rng) };
                k.extend_from_slice(&rng.next().to_be_bytes()[..3]);
                k
            };
            while reference.len() < 4000 {
                let k = next_key(&mut rng);
                step += 1;
                idx.put(&k, rich_entry(step));
                reference.insert(k, rich_entry(step));
            }
            check_front_coded(&idx, &reference);
            let mut snapshot = idx.scan(None, None).unwrap();
            let mut survivors: BTreeMap<Vec<u8>, IndexEntry> = BTreeMap::new();
            for (k, _) in snapshot.iter() {
                if rng.below(10) == 0 {
                    step += 1;
                    idx.put(k, rich_entry(step));
                    reference.insert(k.clone(), rich_entry(step));
                    survivors.insert(k.clone(), rich_entry(step));
                }
            }
            for _ in 0..300 {
                let k = next_key(&mut rng);
                step += 1;
                if !reference.contains_key(&k) {
                    idx.put(&k, rich_entry(step));
                    reference.insert(k.clone(), rich_entry(step));
                    survivors.insert(k, rich_entry(step));
                }
            }
            check_front_coded(&idx, &reference);
            while batch && !snapshot.is_empty() {
                let start = snapshot.len().saturating_sub(1 + rng.below(600));
                let slice: Vec<SnapshotEntry> = snapshot
                    .drain(start..)
                    .map(|(key, e)| to_snapshot(key, e))
                    .collect();
                let mut want = 0usize;
                for e in slice.iter().rev() {
                    if reference.get(&e.key) == Some(&snapshot_entry(e)) {
                        reference.remove(&e.key);
                        want += 1;
                    }
                }
                assert_eq!(idx.remove_if_eq_rev(&slice), want);
                check_front_coded(&idx, &reference);
            }
            while !snapshot.is_empty() {
                for _ in 0..512 {
                    let Some((k, captured)) = snapshot.pop() else { break };
                    let current = reference.get(&k).copied();
                    let removed = idx.remove_if_eq(&k, &captured);
                    assert_eq!(removed, current == Some(captured));
                    if removed {
                        reference.remove(&k);
                    }
                }
                check_front_coded(&idx, &reference);
            }
            assert_eq!(reference, survivors);
        }
    }

    fn diagnosis_block(len: usize) -> FrontCodedIndex {
        let mut idx = FrontCodedIndex::new();
        for i in 0..BLOCK_MAX {
            let key = match len {
                36 => format!("did:plc:sim36rxr2peojm7mthz5us7s/{i:03}").into_bytes(),
                66 => format!("at://did:plc:22niu5w3iw4cghdpdqomndhd/app.bsky.feed.post/{i:09x}").into_bytes(),
                153 => format!("at://did:plc:22niu5w3iw4cghdpdqomndhd/app.bsky.feed.post/3mee7ygcz4k2b\0at://did:plc:{i:024x}/app.bsky.feed.like/{i:013x}\0subject.uri").into_bytes(),
                _ => {
                    let mut key = vec![b'p'; len - 4];
                    key.extend_from_slice(format!("{i:04}").as_bytes());
                    key
                }
            };
            assert_eq!(key.len(), len);
            idx.put(&key, rich_entry(i as u64));
        }
        assert_eq!(idx.blocks.len(), 1);
        idx
    }

    #[test]
    fn checked_sum_charges_actual_decoded_key_bytes() {
        for len in [36usize, 66, 153] {
            let idx = diagnosis_block(len);
            let block = idx.blocks.values().next().unwrap();
            let mut scratch = Vec::new();
            let mut budget = DiagnosisBudget::new();
            let before = budget.bytes;
            assert_eq!(checked_sum(&block.packed, &block.entries, None, &mut scratch, &mut budget), Some(block.integrity));
            assert_eq!(before - budget.bytes, BLOCK_MAX * (len + 44));
            let before = budget.bytes;
            assert_eq!(checked_sum(&block.packed, &block.entries, Some(b"wrong"), &mut scratch, &mut budget), None);
            assert_eq!(before - budget.bytes, len + 44);
        }
    }

    #[test]
    fn diagnosis_is_complete_for_realistic_full_blocks() {
        for len in [36usize, 66, 153] {
            for region in ["key", "entry"] {
                let mut idx = diagnosis_block(len);
                if region == "key" {
                    idx.flip_packed_bit(0, uvarint_len(len) + 19, 0x10);
                } else {
                    idx.blocks.values_mut().next().unwrap().entries[63].0[3] ^= 0x10;
                }
                let finding = idx.flush_page(None, usize::MAX).unwrap_err().finding;
                assert!(matches!(finding, IntegrityFinding::BitFlip { candidates: 1, partial: false, .. }), "len={len} {finding}");
                assert_eq!(finding.bit_flip_xor(), Some(0x10));
            }
        }
    }

    #[test]
    fn diagnosis_retains_candidates_when_the_budget_expires() {
        let mut idx = diagnosis_block(4096);
        idx.blocks.values_mut().next().unwrap().entries[0].0[30] ^= 0x10;
        let finding = idx.flush_page(None, usize::MAX).unwrap_err().finding;
        assert!(matches!(finding, IntegrityFinding::BitFlip { partial: true, .. }), "{finding}");
        assert_eq!(finding.bit_flip_xor(), Some(0x10));
        assert!(finding.to_string().contains("search=partial reason=budget"));
    }

    #[test]
    fn diagnosis_budget_limits_bytes_and_time() {
        let mut budget = DiagnosisBudget::new();
        assert!(!budget.spend(128 * 1024 * 1024 + 1));
        assert!(!budget.spend_key(0, 4 * 1024 * 1024 + 1));
        budget.deadline = std::time::Instant::now();
        assert!(!budget.spend(0));
    }

    #[test]
    fn diagnosis_clock_is_sampled_every_256_operations() {
        let mut budget = DiagnosisBudget::new();
        assert!(budget.spend(0));
        budget.deadline = std::time::Instant::now();
        for _ in 0..255 { assert!(budget.spend(0)); }
        assert!(!budget.spend(0));
    }

    #[test]
    fn malformed_candidate_survives_budget_exhaustion_during_layout() {
        let mut block = Block::single(b"bbb", PackedEntry::pack(&rich_entry(1)));
        block.packed[0] ^= 1;
        let mut budget = DiagnosisBudget::new();
        budget.bytes = 3 + 44;
        let finding = block.diagnose_malformed(Some(b"bbb"), budget);
        assert!(matches!(finding, IntegrityFinding::BitFlip { partial: true, region: "packed", .. }), "{finding}");
        assert_eq!(finding.bit_flip_xor(), Some(1));
    }

    #[test]
    fn diagnosis_budget_exhaustion_is_reported_for_long_and_malformed_keys() {
        let key = vec![b'p'; 4096];
        let mut idx = FrontCodedIndex::new();
        idx.put(&key, rich_entry(1));
        idx.flip_packed_bit(0, 100, 0x10);
        let finding = idx.flush_page(None, usize::MAX).unwrap_err().finding;
        assert!(matches!(finding, IntegrityFinding::BitFlip { partial: true, .. }), "{finding}");
        assert_eq!(finding.bit_flip_xor(), Some(0x10));
        let mut idx = FrontCodedIndex::new();
        idx.put(&key, rich_entry(1));
        idx.flip_packed_bit(0, 1, 0x40);
        let finding = idx.flush_page(None, usize::MAX).unwrap_err().finding;
        assert!(matches!(finding, IntegrityFinding::Unexplained { reason: "budget", .. }), "{finding}");
    }

    #[test]
    fn diagnosis_cache_reuses_only_an_unchanged_block() {
        assert_eq!(std::mem::size_of::<Block>(), 64);
        let mut idx = FrontCodedIndex::new();
        idx.put(b"bbb", rich_entry(1));
        idx.put(b"bbc", rich_entry(2));
        let at = idx.blocks.values().next().unwrap().packed.len() - 1;
        idx.flip_packed_bit(0, at, 0x10);
        let before = DIAGNOSES.with(|c| c.get());
        let finding = idx.flush_page(None, usize::MAX).unwrap_err().finding;
        for _ in 0..20 {
            assert_eq!(idx.flush_page(None, usize::MAX).unwrap_err().finding, finding);
        }
        assert_eq!(DIAGNOSES.with(|c| c.get()), before + 1);
        idx.put(b"aaa", rich_entry(3));
        let finding = idx.flush_page(None, usize::MAX).unwrap_err().finding;
        assert!(matches!(finding, IntegrityFinding::BitFlip { key_index: 2, .. }), "{finding}");
        assert_eq!(DIAGNOSES.with(|c| c.get()), before + 2);
        idx.put(b"aaa", rich_entry(100));
        assert_eq!(idx.flush_page(None, usize::MAX).unwrap_err().finding, finding);
        assert_eq!(DIAGNOSES.with(|c| c.get()), before + 3);
        idx.remove(b"aaa");
        let finding = idx.flush_page(None, usize::MAX).unwrap_err().finding;
        assert!(matches!(finding, IntegrityFinding::BitFlip { key_index: 1, .. }), "{finding}");
        assert_eq!(DIAGNOSES.with(|c| c.get()), before + 4);
        let at = idx.blocks.values().next().unwrap().packed.len() - 1;
        idx.flip_packed_bit(0, at, 0x10);
        assert!(idx.flush_page(None, usize::MAX).is_ok());
        assert!(idx.diagnosis.take().is_none());
        idx.flip_packed_bit(0, at, 0x10);
        idx.flush_page(None, usize::MAX).unwrap_err();
        assert_eq!(DIAGNOSES.with(|c| c.get()), before + 5);
        idx.flip_packed_bit(0, at, 0x01);
        let second = idx.flush_page(None, usize::MAX).unwrap_err().finding;
        assert_eq!(DIAGNOSES.with(|c| c.get()), before + 6);
        assert_eq!(second.bit_flip_xor(), None, "{second}");
    }

    #[test]
    fn split_diagnosis_does_not_reuse_a_stale_key_index() {
        let mut idx = FrontCodedIndex::new();
        for i in 0..8 { idx.put(format!("m{i:03}").as_bytes(), rich_entry(i)); }
        idx.blocks.values_mut().next().unwrap().entries[7].0[30] ^= 0x10;
        let finding = idx.flush_page(None, usize::MAX).unwrap_err().finding;
        assert!(matches!(finding, IntegrityFinding::BitFlip { key_index: 7, .. }));
        idx.put(b"a", rich_entry(10));
        idx.put(b"b", rich_entry(11));
        for i in 10..=BLOCK_MAX { idx.put(format!("z{i:03}").as_bytes(), rich_entry(i as u64)); }
        let finding = idx.flush_page(None, usize::MAX).unwrap_err().finding;
        let IntegrityFinding::AtSplit(inner) = finding else { panic!("{finding}"); };
        assert!(matches!(*inner, IntegrityFinding::BitFlip { key_index: 9, .. }), "{inner}");
    }

    #[test]
    fn ambiguous_xor_is_not_unique() {
        let mut found = Candidates::default();
        found.push("key", 0, 0, b'p', b'q', b"p", 0);
        found.push("key", 1, 0, b'r', b's', b"r", 0);
        let finding = found.finish().unwrap();
        assert_eq!(finding.bit_flip_xor(), None);
        assert!(finding.to_string().contains("ambiguous=true candidate=first"));
    }

    #[test]
    fn integrity_evidence_first_keys_are_bounded() {
        let finding = DiagnosisBudget::exhausted(1, None);
        let error = IndexIntegrityError { first_key: vec![b'p'; 4096].into_boxed_slice(), entries: 1, packed_bytes: 4096, finding };
        assert!(error.to_string().len() < 700);
        let error = crate::run::EncodeIntegrityError {
            path: crate::run::EncodePath::Flush, check: "order", block: 0, entry: 0,
            source_first_key: vec![b'p'; 4096].into_boxed_slice(), finding: None,
            detail: format!("reason=order prev={:02x?} key={:02x?}", vec![b'p'; 4096], vec![b'q'; 4096]),
        };
        assert!(error.to_string().len() < 1400);
    }

    #[test]
    #[ignore]
    fn diagnosis_shared_prefix_microbench() {
        for len in [36usize, 66, 153, 1024, 4096] {
            let mut idx = diagnosis_block(len);
            if len <= 153 {
                idx.blocks.values_mut().next().unwrap().entries[63].0[3] ^= 0x10;
            } else {
                let at = idx.blocks.values().next().unwrap().packed.len() - 1;
                idx.flip_packed_bit(0, at, 0x10);
            }
            let start = std::time::Instant::now();
            let error = idx.flush_page(None, usize::MAX).unwrap_err();
            let first = start.elapsed();
            if len <= 153 {
                assert!(matches!(error.finding, IntegrityFinding::BitFlip { candidates: 1, partial: false, .. }), "{}", error.finding);
                assert_eq!(error.finding.bit_flip_xor(), Some(0x10));
            }
            let start = std::time::Instant::now();
            assert_eq!(idx.flush_page(None, usize::MAX).unwrap_err(), error);
            println!("0372 key_bytes={len} entries={BLOCK_MAX} first_us={} retry_us={} {}",
                first.as_micros(), start.elapsed().as_micros(), error.finding);
        }
    }

    #[test]
    #[ignore]
    fn front_coded_integrity_diagnosis_cost() {
        use std::time::Instant;
        for (prefix, distinct) in [(16usize, false), (256, false), (16, true), (256, true)] {
            let mut block_keys: Vec<Vec<u8>> = (0..BLOCK_MAX as u64).map(|i| {
                let mut k = if distinct { format!("{i:02x}").into_bytes() } else { Vec::new() };
                k.extend(std::iter::repeat_n(b'p', prefix));
                k.extend_from_slice(format!("{i:034x}").as_bytes());
                k
            }).collect();
            block_keys.sort();
            let mut idx = FrontCodedIndex::new();
            for (i, key) in block_keys.iter().enumerate() { idx.put(key, rich_entry(i as u64)); }
            assert_eq!(idx.blocks.len(), 1);
            let at = idx.blocks.values().next().unwrap().packed.len() - 3;
            idx.flip_packed_bit(0, at, 0x10);
            let t = Instant::now();
            let error = idx.flush_page(None, usize::MAX).unwrap_err();
            let elapsed = t.elapsed().as_secs_f64();
            assert_eq!(error.finding.bit_flip_xor(), Some(0x10));
            println!("0370 diagnosis prefix={prefix} distinct={distinct} entries={} packed_bytes={} s={elapsed:.6}", error.entries, error.packed_bytes);
            idx.diagnosis.take();
            idx.blocks.values_mut().next().unwrap().entries[BLOCK_MAX / 2].0[30] ^= 0x01;
            let t = Instant::now();
            let error = idx.flush_page(None, usize::MAX).unwrap_err();
            println!("0370 unexplained prefix={prefix} distinct={distinct} s={:.6} {}", t.elapsed().as_secs_f64(), error.finding);
        }
    }

    #[test]
    #[ignore]
    fn front_coded_integrity_bench() {
        use std::hint::black_box;
        use std::time::Instant;
        let keys: Vec<Vec<u8>> = (0..500_000u64).map(|i| {
            format!("at://did:plc:22niu5w3iw4cghdpdqomndhd/app.bsky.feed.post/3mee7ygcz4k2b\0at://did:plc:{:024x}/app.bsky.feed.like/{:013x}\0subject.uri", i % 1009, i).into_bytes()
        }).collect();
        for order in ["sequential", "shuffled"] {
            let mut positions: Vec<usize> = (0..keys.len()).collect();
            if order == "shuffled" {
                let mut rng = Rng(0x0361);
                for i in (1..positions.len()).rev() {
                    positions.swap(i, rng.below(i + 1));
                }
            }
            for trial in 0..5 {
                let mut idx = FrontCodedIndex::new();
                let t = Instant::now();
                for &i in &positions {
                    idx.put(black_box(&keys[i]), rich_entry(i as u64));
                }
                let insert = t.elapsed().as_secs_f64();
                let t = Instant::now();
                let mut after: Option<Vec<u8>> = None;
                let mut count = 0;
                loop {
                    let (page, more) = idx.flush_page(after.as_deref(), 4096).unwrap();
                    count += page.len();
                    after = page.last().map(|(k, _)| k.clone());
                    black_box(&page);
                    if !more { break; }
                }
                assert_eq!(count, keys.len());
                println!("0361 {order} trial={trial} key_len={} insert_s={insert:.6} snapshot_s={:.6}", keys[0].len(), t.elapsed().as_secs_f64());
            }
        }
    }

    fn to_snapshot(key: Vec<u8>, e: IndexEntry) -> SnapshotEntry {
        SnapshotEntry {
            key,
            ptr: e.ptr,
            seq: e.seq,
            version: e.version,
            tombstone: e.tombstone,
        }
    }

    #[test]
    fn remove_if_eq_rev_matches_per_key_model_for_arbitrary_batches() {
        for (seed, long) in [(11u64, false), (12, true), (13, false)] {
            let mut rng = Rng(0x0313_ba7c ^ seed);
            let mut idx = FrontCodedIndex::new();
            let mut hash = HashIndex::new();
            let mut reference: BTreeMap<Vec<u8>, IndexEntry> = BTreeMap::new();
            let gen = |rng: &mut Rng| if long { gen_long_key(rng) } else { gen_key(rng) };
            for step in 0..6_000u64 {
                let k = gen(&mut rng);
                idx.put(&k, rich_entry(step));
                hash.put(&k, rich_entry(step));
                reference.insert(k, rich_entry(step));
            }
            check_front_coded(&idx, &reference);
            let mut step = 1_000_000u64;
            for round in 0..300 {
                let live: Vec<(Vec<u8>, IndexEntry)> =
                    reference.iter().map(|(k, e)| (k.clone(), *e)).collect();
                let mut batch: Vec<SnapshotEntry> = Vec::new();
                let size = rng.below(80);
                if round % 3 == 0 && !live.is_empty() {
                    let a = rng.below(live.len());
                    let b = (a + size).min(live.len());
                    for (k, e) in &live[a..b] {
                        let e = match rng.below(5) {
                            0 => rich_entry(step),
                            _ => *e,
                        };
                        step += 1;
                        batch.push(to_snapshot(k.clone(), e));
                    }
                    if rng.below(2) == 0 {
                        batch.insert(rng.below(batch.len() + 1), to_snapshot(gen(&mut rng), rich_entry(step)));
                    }
                } else {
                    for _ in 0..size {
                        let (k, e) = match (rng.below(3), live.is_empty()) {
                            (0, _) | (_, true) => (gen(&mut rng), rich_entry(step)),
                            (1, false) => live[rng.below(live.len())].clone(),
                            _ => (live[rng.below(live.len())].0.clone(), rich_entry(step)),
                        };
                        step += 1;
                        batch.push(to_snapshot(k, e));
                    }
                    if rng.below(2) == 0 {
                        batch.sort_by(|a, b| a.key.cmp(&b.key));
                    }
                }
                let mut want = 0usize;
                for e in batch.iter().rev() {
                    if reference.get(&e.key) == Some(&snapshot_entry(e)) {
                        reference.remove(&e.key);
                        want += 1;
                    }
                }
                assert_eq!(idx.remove_if_eq_rev(&batch), want, "round {round}");
                assert_eq!(hash.remove_if_eq_rev(&batch), want, "hash round {round}");
                check_front_coded(&idx, &reference);
                if round % 50 == 0 {
                    for _ in 0..200 {
                        let k = gen(&mut rng);
                        step += 1;
                        idx.put(&k, rich_entry(step));
                        hash.put(&k, rich_entry(step));
                        reference.insert(k, rich_entry(step));
                    }
                }
            }
            let all: Vec<SnapshotEntry> = reference
                .iter()
                .map(|(k, e)| to_snapshot(k.clone(), *e))
                .collect();
            assert_eq!(idx.remove_if_eq_rev(&all), all.len());
            assert_eq!(hash.remove_if_eq_rev(&all), all.len());
            reference.clear();
            check_front_coded(&idx, &reference);
            assert_eq!(hash.len(), 0);
            assert_eq!(idx.remove_if_eq_rev(&all), 0);
        }
    }

    #[test]
    fn remove_if_eq_rev_partial_block_tail_and_head() {
        let key = |i: u32| format!("shared/{i:04}").into_bytes();
        let mut idx = FrontCodedIndex::new();
        let mut reference: BTreeMap<Vec<u8>, IndexEntry> = BTreeMap::new();
        for i in 0..200u32 {
            idx.put(&key(i), rich_entry(i as u64));
            reference.insert(key(i), rich_entry(i as u64));
        }
        let firsts: Vec<Vec<u8>> = idx.blocks.keys().map(|k| k.to_vec()).collect();
        assert!(firsts.len() > 2);
        let head: Vec<SnapshotEntry> = [&firsts[1], &firsts[2]]
            .iter()
            .map(|k| to_snapshot(k.to_vec(), reference[*k]))
            .collect();
        assert_eq!(idx.remove_if_eq_rev(&head), 2);
        for e in &head {
            reference.remove(&e.key);
        }
        check_front_coded(&idx, &reference);
        let mid: Vec<SnapshotEntry> = (40..60u32)
            .filter(|i| i % 3 != 0)
            .filter_map(|i| reference.get(&key(i)).map(|e| to_snapshot(key(i), *e)))
            .collect();
        let n = mid.len();
        assert_eq!(idx.remove_if_eq_rev(&mid), n);
        for e in &mid {
            reference.remove(&e.key);
        }
        check_front_coded(&idx, &reference);
        let stale: Vec<SnapshotEntry> = reference
            .keys()
            .map(|k| to_snapshot(k.clone(), rich_entry(99_999)))
            .collect();
        assert_eq!(idx.remove_if_eq_rev(&stale), 0);
        check_front_coded(&idx, &reference);
    }

    #[test]
    fn front_coded_in_place_removal_positions() {
        let key = |i: u32| format!("prefix/{:03}/{i:05}", i % 7).into_bytes();
        let mut idx = FrontCodedIndex::new();
        let mut reference: BTreeMap<Vec<u8>, IndexEntry> = BTreeMap::new();
        for i in 0..1000u32 {
            idx.put(&key(i), rich_entry(i as u64));
            reference.insert(key(i), rich_entry(i as u64));
        }
        check_front_coded(&idx, &reference);
        let ascending: Vec<Vec<u8>> = reference.keys().cloned().collect();
        for (n, k) in ascending.iter().enumerate().take(300) {
            let first_of_block = idx.blocks.contains_key(k.as_slice());
            assert!(first_of_block, "ascending removal always hits a block's first key");
            assert_eq!(idx.remove(k), reference.remove(k));
            if n % 25 == 0 {
                check_front_coded(&idx, &reference);
            }
        }
        check_front_coded(&idx, &reference);
        let mut rng = Rng(77);
        while !reference.is_empty() {
            let keys: Vec<Vec<u8>> = reference.keys().cloned().collect();
            let k = &keys[rng.below(keys.len())];
            let e = reference.remove(k).unwrap();
            assert!(idx.remove_if_eq(k, &e));
            assert_eq!(idx.get(k), None);
            if reference.len().is_multiple_of(37) {
                check_front_coded(&idx, &reference);
            }
        }
        check_front_coded(&idx, &reference);
        assert_eq!(idx.max_key_len(), 0);
    }

    #[test]
    fn front_coded_insert_below_first_key_splits_and_rekeys() {
        let mut idx = FrontCodedIndex::new();
        let mut reference: BTreeMap<Vec<u8>, IndexEntry> = BTreeMap::new();
        for i in (0..(3 * BLOCK_MAX) as u32).rev() {
            let k = format!("zz/{i:05}").into_bytes();
            idx.put(&k, rich_entry(i as u64));
            reference.insert(k, rich_entry(i as u64));
            check_front_coded(&idx, &reference);
        }
        assert!(idx.blocks.len() > 2);
        let e = rich_entry(9999);
        idx.put(b"", e);
        reference.insert(Vec::new(), e);
        check_front_coded(&idx, &reference);
        assert_eq!(idx.get(b""), Some(e));
        assert!(!idx.remove_if_eq(b"", &rich_entry(1)));
        assert!(idx.remove_if_eq(b"", &e));
        reference.remove(&Vec::new());
        check_front_coded(&idx, &reference);
        assert_eq!(idx.remove(b"a"), None);
        assert!(!idx.remove_if_eq(b"a", &e));
    }

    #[test]
    fn front_coded_removal_shrinks_capacity() {
        let mut idx = FrontCodedIndex::new();
        let key = |i: usize| format!("shared/prefix/{i:04}").into_bytes();
        for i in 0..BLOCK_MAX {
            idx.put(&key(i), entry(i as u64));
        }
        for i in (4..BLOCK_MAX).rev() {
            assert!(idx.remove(&key(i)).is_some());
        }
        let block = idx.blocks.values().next().unwrap();
        assert_eq!(block.entries.len(), 4);
        assert!(block.entries.capacity() <= 2 * entries_target(4));
        assert!(block.packed.capacity() <= 2 * packed_target(block.packed.len()));
    }

    #[test]
    fn remove_if_eq_only_removes_matching_entry() {
        for mut idx in [
            Box::new(HashIndex::new()) as Box<dyn Index>,
            Box::new(FrontCodedIndex::new()),
        ] {
            idx.put(b"a", entry(1));
            idx.put(b"b", entry(2));
            assert!(!idx.remove_if_eq(b"a", &entry(2)));
            assert!(!idx.remove_if_eq(b"zz", &entry(1)));
            assert_eq!(idx.len(), 2);
            assert!(idx.remove_if_eq(b"a", &entry(1)));
            assert_eq!(idx.get(b"a"), None);
            assert_eq!(idx.len(), 1);
            assert_eq!(idx.key_bytes(), 1);
            assert_eq!(idx.live_bytes(), 1);
            assert!(!idx.remove_if_eq(b"a", &entry(1)));
        }
    }

    #[test]
    fn paged_flush_attestation_covers_every_emitted_entry_in_order() {
        for (sorted, ceiling) in [true, false].into_iter().flat_map(|s| [(s, u64::MAX), (s, 400), (s, 5)]) {
            let mut idx: Box<dyn Index> = if sorted { Box::new(FrontCodedIndex::new()) } else { Box::new(HashIndex::new()) };
            for i in 0..1000u64 {
                idx.put(format!("did:plc:sim36rxr2peojm7mthz5us7s/{:05}", (i * 7919) % 1000).as_bytes(), rich_entry(i));
            }
            let mut after: Option<Vec<u8>> = None;
            let mut pairs = Vec::new();
            let mut groups = Vec::new();
            loop {
                let (page, more) = idx.flush_page_attested(after.as_deref(), 37, ceiling, &mut groups).unwrap();
                assert!(!page.is_empty() || !more, "a page that asks for more names its resume key");
                after = page.last().map(|(k, _)| k.clone());
                pairs.extend(page);
                if !more { break; }
            }
            let want = 1000.min(ceiling.saturating_add(1)) as usize;
            assert!(pairs.iter().all(|(_, e)| e.seq <= ceiling));
            assert_eq!(pairs.len(), want);
            assert!(pairs.windows(2).all(|w| w[0].0 < w[1].0));
            assert!(groups.iter().all(|g| g.entries > 0 && g.entries as usize <= if sorted { BLOCK_MAX } else { HASH_PAGE_GROUP }));
            let mut at = 0usize;
            for g in &groups {
                let sum = pairs[at..at + g.entries as usize]
                    .iter()
                    .fold(0u64, |a, (k, e)| a.wrapping_add(index_entry_hash(k, e)));
                assert_eq!(sum, g.sum);
                at += g.entries as usize;
            }
            assert_eq!(at, want);
        }
    }
}

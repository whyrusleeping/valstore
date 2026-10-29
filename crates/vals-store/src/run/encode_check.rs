use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use super::{decode_block_body, encode_block_body, BlockedBloom, Cursor, RtLocator, SUFFIX_TOMB_BIT};
use crate::range_tombstone::{decode_rt_block, encode_rt_block, RtRecord};
use crate::crc;
use crate::index::{
    diag_window, entry_field, entry_hash_fields, image_hash, key_hash, packed_image, snapshot_entry_hash,
    IntegrityFinding, SourceGroup,
};
use crate::snapshot::SnapshotEntry;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodePath {
    Flush,
    Merge,
}

impl EncodePath {
    pub fn label(self) -> &'static str {
        match self {
            EncodePath::Flush => "flush",
            EncodePath::Merge => "merge",
        }
    }

    fn slot(self) -> usize {
        match self {
            EncodePath::Flush => 0,
            EncodePath::Merge => 1,
        }
    }
}

static FAILURES: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];

pub fn encode_integrity_failures() -> [(&'static str, u64); 2] {
    [EncodePath::Flush, EncodePath::Merge].map(|p| (p.label(), FAILURES[p.slot()].load(AtomicOrdering::Relaxed)))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodeIntegrityError {
    pub path: EncodePath,
    pub check: &'static str,
    pub block: usize,
    pub entry: u64,
    pub source_first_key: Box<[u8]>,
    pub finding: Option<IntegrityFinding>,
    pub detail: String,
}

impl EncodeIntegrityError {
    pub fn single_bit_xor(&self) -> Option<u8> {
        self.finding.as_ref().and_then(IntegrityFinding::bit_flip_xor)
    }
}

impl std::fmt::Display for EncodeIntegrityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "run encode integrity mismatch path={} check={} block={} entry={} source_first_key={:02x?} first_key_bytes={} detail_bytes={} {:.768}",
            self.path.label(), self.check, self.block, self.entry,
            &self.source_first_key[..self.source_first_key.len().min(64)], self.source_first_key.len(), self.detail.len(), self.detail
        )?;
        if let Some(finding) = &self.finding {
            write!(f, " {finding}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum RunSource<'a> {
    Inline,
    Attested(&'a [SourceGroup]),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunBuilt {
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
}

const KNOCK_ORDER: u8 = 1;
const KNOCK_SOURCE_HASH: u8 = 2;
const KNOCK_INDEX: u8 = 4;

#[cfg(any(test, feature = "test-fixtures"))]
pub mod test_seams {
    use std::cell::Cell;

    pub const ORDER: u8 = super::KNOCK_ORDER;
    pub const SOURCE_HASH: u8 = super::KNOCK_SOURCE_HASH;
    pub const INDEX: u8 = super::KNOCK_INDEX;

    thread_local! {
        static KNOCKOUTS: Cell<u8> = const { Cell::new(0) };
        static BUFFER_FLIP: Cell<Option<(usize, usize, u8)>> = const { Cell::new(None) };
        static STREAM_FLIP: Cell<Option<(u64, usize, u8, bool)>> = const { Cell::new(None) };
        static INDEX_FLIP: Cell<Option<(usize, u8)>> = const { Cell::new(None) };
        static RT_FLIP: Cell<Option<(usize, u8)>> = const { Cell::new(None) };
    }

    pub fn arm_rt_block_flip(offset: usize, mask: u8) {
        RT_FLIP.with(|c| c.set(Some((offset, mask))));
    }

    pub(crate) fn flip_rt_block(bytes: &mut [u8]) {
        RT_FLIP.with(|c| {
            if let Some((offset, mask)) = c.take() {
                let body = bytes.len() - 4;
                bytes[offset] ^= mask;
                let crc = crate::crc::crc32(&bytes[..body]);
                bytes[body..].copy_from_slice(&crc.to_le_bytes());
            }
        });
    }

    pub fn with_knockouts<R>(mask: u8, f: impl FnOnce() -> R) -> R {
        struct Restore(u8);
        impl Drop for Restore {
            fn drop(&mut self) {
                KNOCKOUTS.with(|c| c.set(self.0));
            }
        }
        let _restore = Restore(KNOCKOUTS.with(|c| c.replace(mask)));
        f()
    }

    pub fn arm_buffer_flip(block: usize, offset: usize, mask: u8) {
        BUFFER_FLIP.with(|c| c.set(Some((block, offset, mask))));
    }

    pub fn buffer_flip_armed() -> bool {
        BUFFER_FLIP.with(|c| c.get().is_some())
    }

    pub fn arm_stream_entry_flip(entry: u64, byte: usize, mask: u8, before_hash: bool) {
        STREAM_FLIP.with(|c| c.set(Some((entry, byte, mask, before_hash))));
    }

    pub fn stream_entry_flip_armed() -> bool {
        STREAM_FLIP.with(|c| c.get().is_some())
    }

    pub fn arm_index_flip(offset: usize, mask: u8) {
        INDEX_FLIP.with(|c| c.set(Some((offset, mask))));
    }

    pub fn index_flip_armed() -> bool {
        INDEX_FLIP.with(|c| c.get().is_some())
    }

    pub(crate) fn flip_index(idx: &mut [u8]) {
        INDEX_FLIP.with(|c| {
            if let Some((offset, mask)) = c.take() {
                idx[offset] ^= mask;
            }
        });
    }

    pub(crate) fn knockouts() -> u8 {
        KNOCKOUTS.with(|c| c.get())
    }

    pub(crate) fn flip_buffer(block: usize, body: &mut [u8]) {
        BUFFER_FLIP.with(|c| {
            if let Some((b, offset, mask)) = c.get() {
                if b == block {
                    body[offset] ^= mask;
                    c.set(None);
                }
            }
        });
    }

    pub(crate) fn take_stream_flip(entry: u64) -> Option<(usize, u8, bool)> {
        STREAM_FLIP.with(|c| match c.get() {
            Some((e, byte, mask, before_hash)) if e == entry => {
                c.set(None);
                Some((byte, mask, before_hash))
            }
            _ => None,
        })
    }
}

#[inline]
fn knockouts() -> u8 {
    #[cfg(any(test, feature = "test-fixtures"))]
    {
        test_seams::knockouts()
    }
    #[cfg(not(any(test, feature = "test-fixtures")))]
    {
        0
    }
}

pub(super) enum Expect<'a> {
    Entries(&'a [u64]),
    Groups { groups: &'a [SourceGroup], all: &'a [SnapshotEntry] },
}

struct Raw<'a> {
    shared: usize,
    tombstone: bool,
    suffix: &'a [u8],
    segment: u64,
    offset: u32,
    len: u32,
    seq: u64,
    version: u128,
}

fn read_raw<'a>(c: &mut Cursor<'a>) -> Result<Raw<'a>, &'static str> {
    let shared = c.u32()? as usize;
    let suffix_field = c.u32()?;
    let suffix = c.take((suffix_field & !SUFFIX_TOMB_BIT) as usize)?;
    Ok(Raw {
        shared,
        tombstone: suffix_field & SUFFIX_TOMB_BIT != 0,
        suffix,
        segment: c.u64()?,
        offset: c.u32()?,
        len: c.u32()?,
        seq: c.u64()?,
        version: c.u128()?,
    })
}

pub(super) struct EncodeCheck {
    path: EncodePath,
    key: Vec<u8>,
    entries: u64,
    blocks: usize,
    block_base: u64,
    first_key_hashes: Vec<u64>,
    max_hlcs: Vec<u64>,
    min_key: Option<Vec<u8>>,
    group: usize,
    group_left: u32,
    group_sum: u64,
    group_start: u64,
    group_hashes: Vec<u64>,
    retired: Option<(u64, Vec<u8>)>,
}

impl EncodeCheck {
    pub(super) fn new(path: EncodePath) -> Self {
        EncodeCheck {
            path,
            key: Vec::new(),
            entries: 0,
            blocks: 0,
            block_base: 0,
            first_key_hashes: Vec::new(),
            max_hlcs: Vec::new(),
            min_key: None,
            group: 0,
            group_left: 0,
            group_sum: 0,
            group_start: 0,
            group_hashes: Vec::new(),
            retired: None,
        }
    }

    pub(super) fn retire(&mut self, block: Vec<u8>) {
        self.retired = Some((self.block_base, block));
    }

    pub(super) fn bounds(&self) -> RunBuilt {
        RunBuilt {
            min_key: self.min_key.clone().unwrap_or_default(),
            max_key: if self.entries > 0 { self.key.clone() } else { Vec::new() },
        }
    }

    fn fail(
        &self,
        check: &'static str,
        block: usize,
        entry: u64,
        source_first_key: &[u8],
        finding: Option<IntegrityFinding>,
        detail: String,
    ) -> Box<EncodeIntegrityError> {
        FAILURES[self.path.slot()].fetch_add(1, AtomicOrdering::Relaxed);
        let e = Box::new(EncodeIntegrityError {
            path: self.path,
            check,
            block,
            entry,
            source_first_key: Box::from(source_first_key),
            finding,
            detail,
        });
        log::error!("{e}; aborting the run build without publishing it");
        e
    }

    fn decode_fail(&self, entry: u64, source_first_key: &[u8], why: &str) -> Box<EncodeIntegrityError> {
        self.fail("decode", self.blocks, entry, source_first_key, None, format!("reason={why}"))
    }

    pub(super) fn block(
        &mut self,
        src: &[SnapshotEntry],
        expect: Expect<'_>,
        bloom: &mut BlockedBloom,
    ) -> Result<(Vec<u8>, Vec<u8>), Box<EncodeIntegrityError>> {
        #[allow(unused_mut)]
        let mut body = encode_block_body(src);
        #[cfg(any(test, feature = "test-fixtures"))]
        test_seams::flip_buffer(self.blocks, &mut body);
        let crc = crc::crc32(&body);
        let first = self.verify_body(&body, src, &expect, bloom)?;
        body.extend_from_slice(&crc.to_le_bytes());
        self.blocks += 1;
        Ok((body, first))
    }

    fn verify_body(
        &mut self,
        body: &[u8],
        src: &[SnapshotEntry],
        expect: &Expect<'_>,
        bloom: &mut BlockedBloom,
    ) -> Result<Vec<u8>, Box<EncodeIntegrityError>> {
        let knock = knockouts();
        self.block_base = self.entries;
        let src_first: &[u8] = src.first().map_or(&[], |e| e.key.as_slice());
        let mut c = Cursor { b: body, off: 0 };
        let count = c.u32().map_err(|why| self.decode_fail(self.entries, src_first, why))? as usize;
        if count != src.len() || count == 0 {
            return Err(self.fail(
                "decode",
                self.blocks,
                self.entries,
                src_first,
                None,
                format!("reason=entry count encoded={count} source={}", src.len()),
            ));
        }
        let mut first = Vec::new();
        let mut max_hlc = 0u64;
        for j in 0..count {
            let at = self.entries;
            let r = read_raw(&mut c).map_err(|why| self.decode_fail(at, src_first, why))?;
            max_hlc = max_hlc.max(crate::range_tombstone::version_hlc(r.version));
            if (j == 0 && r.shared != 0) || r.shared > self.key.len() {
                return Err(self.decode_fail(at, src_first, "shared prefix longer than the previous key"));
            }
            if knock & KNOCK_ORDER == 0 && at > 0 && &self.key[r.shared..] >= r.suffix {
                let mut next = self.key[..r.shared].to_vec();
                next.extend_from_slice(r.suffix);
                return Err(self.fail(
                    "order",
                    self.blocks,
                    at,
                    src_first,
                    None,
                    format!("reason=key not strictly above the previous encoded key prev={:02x?} prev_bytes={} key={:02x?} key_bytes={}",
                        &self.key[..self.key.len().min(64)], self.key.len(), &next[..next.len().min(64)], next.len()),
                ));
            }
            self.key.truncate(r.shared);
            self.key.extend_from_slice(r.suffix);
            if knock & KNOCK_SOURCE_HASH == 0 {
                let h = entry_hash_fields(&self.key, r.segment, r.offset, r.len, r.seq, r.tombstone, r.version);
                match expect {
                    Expect::Entries(want) => {
                        if want[j] != h {
                            return Err(self.entry_mismatch(body, src, j, want[j]));
                        }
                    }
                    Expect::Groups { groups, all } => self.group_step(h, groups, all, body, src)?,
                }
            }
            bloom.insert(&self.key);
            if j == 0 {
                self.first_key_hashes.push(key_hash(&self.key));
                first = self.key.clone();
                if self.min_key.is_none() {
                    self.min_key = Some(first.clone());
                }
            }
            self.entries += 1;
        }
        if c.off != body.len() {
            return Err(self.decode_fail(self.entries, src_first, "trailing bytes"));
        }
        self.max_hlcs.push(max_hlc);
        Ok(first)
    }

    pub(super) fn last_block_max_hlc(&self) -> u64 {
        self.max_hlcs.last().copied().unwrap_or(0)
    }

    pub(super) fn rt_error(&self, why: &str) -> Box<EncodeIntegrityError> {
        self.fail("range-tombstones", self.blocks, self.entries, &[], None, format!("reason={why}"))
    }

    pub(super) fn rt_block(&self, records: &[RtRecord]) -> Result<Vec<u8>, Box<EncodeIntegrityError>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(r) = records.iter().find(|r| r.span_is_empty()) {
            return Err(self.rt_error(&format!("record seq={} has an empty span", r.seq)));
        }
        #[allow(unused_mut)]
        let mut bytes = encode_rt_block(records);
        #[cfg(any(test, feature = "test-fixtures"))]
        test_seams::flip_rt_block(&mut bytes);
        match decode_rt_block(&bytes) {
            Ok(back) if back == records => Ok(bytes),
            Ok(back) => Err(self.rt_error(&format!(
                "encoded block decodes to {} records that differ from the {} source records",
                back.len(),
                records.len()
            ))),
            Err(why) => Err(self.rt_error(why)),
        }
    }

    fn group_step(
        &mut self,
        h: u64,
        groups: &[SourceGroup],
        all: &[SnapshotEntry],
        body: &[u8],
        src: &[SnapshotEntry],
    ) -> Result<(), Box<EncodeIntegrityError>> {
        if self.group_left == 0 {
            while groups.get(self.group).is_some_and(|g| g.entries == 0) {
                self.group += 1;
            }
            let Some(g) = groups.get(self.group) else {
                let first = all.get(self.entries as usize).map_or(&[][..], |e| e.key.as_slice());
                return Err(self.fail(
                    "source-hash",
                    self.blocks,
                    self.entries,
                    first,
                    None,
                    format!("reason=encoded more entries than the source attested ({} groups)", groups.len()),
                ));
            };
            self.group_left = g.entries;
            self.group_sum = 0;
            self.group_start = self.entries;
            self.group_hashes.clear();
        }
        self.group_sum = self.group_sum.wrapping_add(h);
        self.group_hashes.push(h);
        self.group_left -= 1;
        if self.group_left == 0 {
            let g = groups[self.group];
            if self.group_sum != g.sum {
                return Err(self.group_mismatch(g, all, body, src));
            }
            self.group += 1;
        }
        Ok(())
    }

    pub(super) fn finish_source(&self, source: RunSource<'_>) -> Result<(), Box<EncodeIntegrityError>> {
        let RunSource::Attested(groups) = source else { return Ok(()) };
        if knockouts() & KNOCK_SOURCE_HASH != 0 {
            return Ok(());
        }
        let tail = |from: usize| groups.get(from..).map_or(0u64, |g| g.iter().map(|g| g.entries as u64).sum());
        let rest = if self.group_left > 0 {
            self.group_left as u64 + tail(self.group + 1)
        } else {
            tail(self.group)
        };
        if rest > 0 {
            return Err(self.fail(
                "source-hash",
                self.blocks,
                self.entries,
                &self.key,
                None,
                format!("reason=the source attested {rest} more entries than were encoded"),
            ));
        }
        if let Some(g) = groups.iter().find(|g| g.entries == 0 && g.sum != 0) {
            return Err(self.fail(
                "source-hash",
                self.blocks,
                self.entries,
                &self.key,
                None,
                format!("reason=an emptied source group keeps a residual sum 0x{:016x}", g.sum),
            ));
        }
        Ok(())
    }

    fn group_mismatch(
        &self,
        g: SourceGroup,
        all: &[SnapshotEntry],
        body: &[u8],
        src: &[SnapshotEntry],
    ) -> Box<EncodeIntegrityError> {
        let start = self.group_start as usize;
        let members = &all[start..start + g.entries as usize];
        let first = members[0].key.as_slice();
        let hashes: Vec<u64> = members.iter().map(snapshot_entry_hash).collect();
        match (0..members.len()).find(|&i| hashes[i] != self.group_hashes[i]) {
            Some(i) => {
                let at = (start + i) as u64;
                let (block, finding, detail) = self.diff_encoded(body, src, Some(all), at);
                self.fail(
                    "source-hash",
                    block,
                    at,
                    first,
                    finding,
                    format!("stage=encode group_entries={} {detail}", g.entries),
                )
            }
            None => {
                let finding = single_bit_repair(members, &hashes, g.sum, start);
                let at = match &finding {
                    Some(IntegrityFinding::BitFlip { key_index, .. }) => *key_index as u64,
                    _ => start as u64,
                };
                self.fail(
                    "source-hash",
                    self.blocks,
                    at,
                    first,
                    finding,
                    format!(
                        "stage=source group_entries={} attested=0x{:016x} encoded=0x{:016x}",
                        g.entries, g.sum, self.group_sum
                    ),
                )
            }
        }
    }

    fn entry_mismatch(&self, body: &[u8], src: &[SnapshotEntry], j: usize, want: u64) -> Box<EncodeIntegrityError> {
        let at = self.entries;
        let e = &src[j];
        let first = src[0].key.as_slice();
        let have = snapshot_entry_hash(e);
        if have != want {
            let finding = single_bit_repair(std::slice::from_ref(e), &[have], want, at as usize);
            self.fail("source-hash", self.blocks, at, first, finding, "stage=source-copy".to_string())
        } else {
            let (block, finding, detail) = self.diff_encoded(body, src, None, at);
            self.fail("source-hash", block, at, first, finding, format!("stage=encode {detail}"))
        }
    }

    fn diff_encoded(
        &self,
        body: &[u8],
        src: &[SnapshotEntry],
        all: Option<&[SnapshotEntry]>,
        at: u64,
    ) -> (usize, Option<IntegrityFinding>, String) {
        let (block, body, base, s) = match at.checked_sub(self.block_base) {
            Some(j) if (j as usize) < src.len() => (self.blocks, body, self.block_base, &src[j as usize]),
            _ => match (&self.retired, all.and_then(|a| a.get(at as usize))) {
                (Some((base, retired)), Some(s)) if at >= *base && retired.len() >= 4 => {
                    (self.blocks.saturating_sub(1), &retired[..retired.len() - 4], *base, s)
                }
                _ => return (self.blocks, None, "detail=entry encoded in an earlier block".to_string()),
            },
        };
        let decoded = match decode_block_body(body) {
            Ok(d) => d,
            Err(why) => return (block, None, format!("detail=block body unreadable ({why})")),
        };
        let Some(enc) = decoded.get((at - base) as usize) else {
            return (block, None, "detail=entry missing from the decoded block".to_string());
        };
        if enc.key.len() != s.key.len() {
            return (block, None, format!("detail=key length encoded={} source={}", enc.key.len(), s.key.len()));
        }
        let (a, b) = (run_image(enc), run_image(s));
        let bits: u32 = a.iter().zip(&b).map(|(x, y)| (x ^ y).count_ones()).sum();
        let Some(off) = a.iter().zip(&b).position(|(x, y)| x != y) else {
            return (block, None, "detail=encoded entry equals the source".to_string());
        };
        let klen = s.key.len();
        let (region, offset, window_src) = if off < klen {
            ("key", off, &a[..klen])
        } else {
            (run_field(off - klen), off - klen, &a[klen..])
        };
        if bits != 1 {
            return (block, None, format!("detail=bits_differ={bits} first_region={region} offset={offset}"));
        }
        let (window_start, window) = diag_window(window_src, offset);
        (
            block,
            Some(IntegrityFinding::BitFlip {
                region,
                key_index: at as usize,
                offset,
                actual: a[off],
                expected: b[off],
                candidates: 1,
                partial: false,
                window_start,
                window,
            }),
            String::new(),
        )
    }

    pub(super) fn index_error(&self, why: &str) -> Box<EncodeIntegrityError> {
        self.fail("index", self.blocks, 0, &[], None, format!("reason={why}"))
    }

    pub(super) fn verify_index(
        &self,
        idx: &[u8],
        data_start: u64,
        data_end: u64,
        rt: Option<RtLocator>,
    ) -> Result<(), Box<EncodeIntegrityError>> {
        if knockouts() & KNOCK_INDEX != 0 {
            return Ok(());
        }
        let bad = |k: usize, why: String| self.fail("index", k, 0, &[], None, why);
        let Some(body_len) = idx.len().checked_sub(4) else {
            return Err(bad(0, "reason=index shorter than its crc".to_string()));
        };
        let mut c = Cursor { b: &idx[..body_len], off: 0 };
        let n = c.u64().map_err(|why| bad(0, format!("reason={why}")))?;
        if n != self.first_key_hashes.len() as u64 {
            return Err(bad(0, format!("reason=index entries={n} encoded blocks={}", self.first_key_hashes.len())));
        }
        let mut expect_off = data_start;
        let mut prev: &[u8] = &[];
        for (k, &want) in self.first_key_hashes.iter().enumerate() {
            let (key, off, len) = read_index_entry(&mut c).map_err(|why| bad(k, format!("reason={why}")))?;
            if rt.is_some() {
                let max_hlc = c.u64().map_err(|why| bad(k, format!("reason={why}")))?;
                if self.max_hlcs.get(k) != Some(&max_hlc) {
                    return Err(bad(k, format!("reason=index max_hlc={max_hlc} differs from the encoded block's {:?}", self.max_hlcs.get(k))));
                }
            }
            if key_hash(key) != want {
                return Err(bad(k, format!("reason=index first key differs from the encoded block's first key key={:02x?} key_bytes={}", &key[..key.len().min(64)], key.len())));
            }
            if k > 0 && prev >= key {
                return Err(bad(k, format!("reason=index first keys not strictly ascending prev={:02x?} prev_bytes={} key={:02x?} key_bytes={}",
                    &prev[..prev.len().min(64)], prev.len(), &key[..key.len().min(64)], key.len())));
            }
            if off != expect_off || len < 8 {
                return Err(bad(k, format!("reason=index block range off={off} len={len} expected_off={expect_off}")));
            }
            expect_off = off + len as u64;
            prev = key;
        }
        if expect_off != data_end {
            return Err(bad(
                self.first_key_hashes.len(),
                format!("reason=index covers data through {expect_off} but blocks end at {data_end}"),
            ));
        }
        if let Some(rt) = rt {
            let n = self.first_key_hashes.len();
            let got = (|| Ok::<_, &'static str>(RtLocator { seq_floor: c.u64()?, off: c.u64()?, len: c.u64()? }))()
                .map_err(|why| bad(n, format!("reason={why}")))?;
            if got != rt {
                return Err(bad(n, format!("reason=index range tombstone trailer {got:?} differs from {rt:?}")));
            }
        }
        if c.off != body_len {
            return Err(bad(self.first_key_hashes.len(), "reason=trailing bytes after the index".to_string()));
        }
        Ok(())
    }
}

fn read_index_entry<'a>(c: &mut Cursor<'a>) -> Result<(&'a [u8], u64, u32), &'static str> {
    let klen = c.u32()? as usize;
    let key = c.take(klen)?;
    Ok((key, c.u64()?, c.u32()?))
}

fn run_image(e: &SnapshotEntry) -> Vec<u8> {
    let mut b = Vec::with_capacity(e.key.len() + 41);
    b.extend_from_slice(&e.key);
    b.extend_from_slice(&e.ptr.segment.0.to_le_bytes());
    b.extend_from_slice(&e.ptr.offset.to_le_bytes());
    b.extend_from_slice(&e.ptr.len.to_le_bytes());
    b.extend_from_slice(&e.seq.to_le_bytes());
    b.extend_from_slice(&e.version.to_le_bytes());
    b.push(e.tombstone as u8);
    b
}

fn run_field(byte: usize) -> &'static str {
    match byte {
        0..=7 => "entry.segment",
        8..=11 => "entry.offset",
        12..=15 => "entry.len",
        16..=23 => "entry.seq",
        24..=39 => "entry.version",
        _ => "entry.tombstone",
    }
}

fn single_bit_repair(members: &[SnapshotEntry], hashes: &[u64], want: u64, start: usize) -> Option<IntegrityFinding> {
    let mut budget = crate::index::DiagnosisBudget::new();
    let total = hashes.iter().fold(0u64, |a, h| a.wrapping_add(*h));
    let mut found = crate::index::Candidates::default();
    for (i, e) in members.iter().enumerate() {
        let rest = total.wrapping_sub(hashes[i]);
        let image = packed_image(e.ptr.segment.0, e.ptr.offset, e.ptr.len, e.seq, e.tombstone, e.version);
        let mut repaired = image;
        for off in 0..repaired.len() {
            for bit in 0..8 {
                if !budget.spend_key(i, e.key.len() + 44) { return Some(found.exhausted(want, Some(total))); }
                repaired[off] ^= 1 << bit;
                if rest.wrapping_add(image_hash(&e.key, repaired)) == want {
                    found.push(entry_field(off), start + i, off, image[off], repaired[off], &image, off);
                }
                repaired[off] ^= 1 << bit;
            }
        }
    }
    for (i, e) in members.iter().enumerate() {
        let rest = total.wrapping_sub(hashes[i]);
        let image = packed_image(e.ptr.segment.0, e.ptr.offset, e.ptr.len, e.seq, e.tombstone, e.version);
        if !budget.spend_key(i, e.key.len().saturating_add(44)) { return Some(found.exhausted(want, Some(total))); }
        let mut key = e.key.clone();
        for off in 0..key.len() {
            for bit in 0..8 {
                if !budget.spend_key(i, key.len() + 44) { return Some(found.exhausted(want, Some(total))); }
                key[off] ^= 1 << bit;
                if rest.wrapping_add(image_hash(&key, image)) == want {
                    found.push("key", start + i, off, e.key[off], key[off], &e.key, off);
                }
                key[off] ^= 1 << bit;
            }
        }
    }
    found.finish()
}

#[cfg(test)]
mod tests {
    use super::test_seams::{self, INDEX, ORDER, SOURCE_HASH};
    use super::*;
    use crate::alloc::{Allocator, SegmentId};
    use crate::run::{
        allocate_run_segs, build_run_checked, build_run_versioned, run_len_upper_bound_for, Run, RunBuildOpts,
        RunStreamBuilder, SegmentStreamSink, RUN_VERSION,
    };
    use crate::snapshot_io::SnapshotError;
    use crate::vlog::ValuePtr;
    use vals_spdk::MemBlockDevice;

    const BS: usize = 4096;

    fn entries(n: usize) -> Vec<SnapshotEntry> {
        (0..n)
            .map(|i| SnapshotEntry {
                key: format!("at://did:plc:tpghvdx6/app.bsky.feed.post/{:06}", i * 10).into_bytes(),
                ptr: ValuePtr { segment: SegmentId(46716 + i as u64 / 50), offset: i as u32 * 64, len: 40 },
                seq: 56_000_000 + i as u64,
                version: 0x1234_0000_0000 + i as u128,
                tombstone: i % 17 == 3,
            })
            .collect()
    }

    #[test]
    fn single_bit_repair_has_a_budget_for_each_long_key() {
        for len in [1024usize, 4096] {
            let mut src = entries(1);
            src[0].key = vec![b'p'; len];
            let want = snapshot_entry_hash(&src[0]);
            src[0].key[len - 1] ^= 0x10;
            let hashes = [snapshot_entry_hash(&src[0])];
            let finding = single_bit_repair(&src, &hashes, want, 0).unwrap();
            assert!(matches!(finding, IntegrityFinding::Unexplained { reason: "budget", searched: false, .. }), "{finding}");
        }
    }

    #[test]
    fn single_bit_repair_preserves_entry_candidates_before_long_key_search() {
        for len in [1024usize, 4096] {
            let mut src = entries(2);
            for e in &mut src { e.key.resize(len, b'p'); }
            let want = src.iter().fold(0u64, |sum, e| sum.wrapping_add(snapshot_entry_hash(e)));
            src[1].version ^= 0x10;
            let hashes: Vec<_> = src.iter().map(snapshot_entry_hash).collect();
            let finding = single_bit_repair(&src, &hashes, want, 0).unwrap();
            assert!(matches!(finding, IntegrityFinding::BitFlip { partial: true, region: "entry.version", key_index: 1, .. }), "{finding}");
            assert_eq!(finding.bit_flip_xor(), Some(0x10));
        }
    }

    #[test]
    fn order_and_index_details_bound_long_keys() {
        let key = vec![b'p'; 4096];
        let mut src = entries(2);
        for e in &mut src { e.key = key.clone(); }
        let error = build(&src, RunSource::Inline).unwrap_err();
        assert_eq!(error.check, "order");
        assert!(error.detail.contains("prev_bytes=4096"));
        assert!(error.detail.len() < 800, "{}", error.detail.len());
        assert!(error.to_string().len() < 1400);
        let mut check = EncodeCheck::new(EncodePath::Flush);
        check.first_key_hashes = vec![key_hash(&key); 2];
        let mut index = 2u64.to_le_bytes().to_vec();
        for off in [0u64, 8] {
            index.extend_from_slice(&(key.len() as u32).to_le_bytes());
            index.extend_from_slice(&key);
            index.extend_from_slice(&off.to_le_bytes());
            index.extend_from_slice(&8u32.to_le_bytes());
        }
        index.extend_from_slice(&0u32.to_le_bytes());
        let error = check.verify_index(&index, 0, 16, None).unwrap_err();
        assert!(error.detail.contains("prev_bytes=4096"));
        assert!(error.detail.len() < 800);
        check.first_key_hashes[0] ^= 1;
        let error = check.verify_index(&index, 0, 16, None).unwrap_err();
        assert!(error.detail.contains("key_bytes=4096"));
        assert!(error.detail.len() < 800);
    }

    fn opts() -> RunBuildOpts {
        RunBuildOpts { block_target_bytes: 1024, ..Default::default() }
    }

    fn groups(entries: &[SnapshotEntry], size: usize) -> Vec<SourceGroup> {
        entries
            .chunks(size)
            .map(|c| SourceGroup {
                entries: c.len() as u32,
                sum: c.iter().fold(0u64, |a, e| a.wrapping_add(snapshot_entry_hash(e))),
            })
            .collect()
    }

    fn build(entries: &[SnapshotEntry], source: RunSource<'_>) -> Result<(Vec<u8>, RunBuilt), Box<EncodeIntegrityError>> {
        build_run_checked(entries, opts(), RUN_VERSION, EncodePath::Flush, source)
    }

    fn flip(f: &IntegrityFinding) -> (&'static str, usize, usize, u8) {
        match f {
            IntegrityFinding::BitFlip { region, key_index, offset, actual, expected, .. } => {
                (region, *key_index, *offset, actual ^ expected)
            }
            other => panic!("not a single-bit finding: {other:?}"),
        }
    }

    fn first_key_len_offset(entries: &[SnapshotEntry], block: usize) -> (usize, usize) {
        let mut est = 0usize;
        let mut start = 0usize;
        let mut b = 0usize;
        for (i, e) in entries.iter().enumerate() {
            if i > start && est >= opts().block_target_bytes {
                b += 1;
                start = i;
                est = 0;
            }
            if b == block && i == start {
                return (i, e.key.len());
            }
            est += e.key.len() + 40;
        }
        panic!("no block {block}");
    }

    #[test]
    fn checked_builds_match_the_unchecked_encoding_and_report_decoded_bounds() {
        let src = entries(300);
        let reference = build_run_versioned(&src, opts(), RUN_VERSION);
        let g = groups(&src, 37);
        for source in [RunSource::Inline, RunSource::Attested(&g)] {
            let (bytes, built) = build(&src, source).unwrap();
            assert!(bytes == reference);
            assert_eq!(built.min_key, src[0].key);
            assert_eq!(built.max_key, src[299].key);
        }
        let (bytes, built) = build(&[], RunSource::Attested(&[])).unwrap();
        assert_eq!(Run::parse(bytes).unwrap().entry_count(), 0);
        assert_eq!(built, RunBuilt::default());
    }

    #[test]
    fn an_encode_buffer_flip_before_the_crc_fails_the_build_and_is_located() {
        let src = entries(300);
        let g = groups(&src, 37);
        let (first, klen) = first_key_len_offset(&src, 2);
        let field = 12 + klen;
        for attested in [false, true] {
            let source = if attested { RunSource::Attested(&g) } else { RunSource::Inline };
            let before = encode_integrity_failures()[0].1;
            test_seams::arm_buffer_flip(2, field + 1, 0x04);
            let err = build(&src, source).unwrap_err();
            assert!(!test_seams::buffer_flip_armed());
            assert_eq!((err.path, err.check, err.block), (EncodePath::Flush, "source-hash", 2));
            assert!(err.detail.contains("stage=encode"), "{err}");
            assert_eq!(flip(err.finding.as_ref().unwrap()), ("entry.segment", first, 1, 0x04));
            assert_eq!(err.single_bit_xor(), Some(0x04));
            assert!(encode_integrity_failures()[0].1 > before);
            assert!(err.to_string().starts_with("run encode integrity mismatch path=flush check=source-hash block=2"));

            test_seams::arm_buffer_flip(2, field + 1, 0x04);
            let (bytes, _) = test_seams::with_knockouts(SOURCE_HASH, || build(&src, source)).unwrap();
            let run = Run::parse(bytes).unwrap();
            let got = run.lookup(&src[first].key).unwrap().unwrap();
            assert_eq!(got.ptr.segment.0, src[first].ptr.segment.0 ^ (0x04 << 8));
        }
    }

    #[test]
    fn an_attested_source_flip_after_the_memtable_check_is_located_by_single_bit_search() {
        let clean = entries(300);
        let g = groups(&clean, 32);
        let mut src = clean.clone();
        let last = src[100].key.len() - 1;
        src[100].key[last] ^= 0x01;
        assert!(src[99].key < src[100].key && src[100].key < src[101].key);
        let err = build(&src, RunSource::Attested(&g)).unwrap_err();
        assert_eq!(err.check, "source-hash");
        assert!(err.detail.contains("stage=source"), "{err}");
        assert_eq!(flip(err.finding.as_ref().unwrap()), ("key", 100, last, 0x01));
        assert_eq!(&*err.source_first_key, clean[96].key.as_slice());
        let (bytes, _) = test_seams::with_knockouts(SOURCE_HASH, || build(&src, RunSource::Attested(&g))).unwrap();
        let run = Run::parse(bytes).unwrap();
        assert!(run.lookup(&clean[100].key).unwrap().is_none());
        assert!(run.lookup(&src[100].key).unwrap().is_some());

        let mut src = clean.clone();
        src[7].ptr.offset ^= 0x10;
        let err = build(&src, RunSource::Attested(&g)).unwrap_err();
        assert_eq!(flip(err.finding.as_ref().unwrap()), ("entry.offset", 7, 4, 0x10));

        let err = build(&clean[..299], RunSource::Attested(&g)).unwrap_err();
        assert!(err.detail.contains("attested 1 more entries"), "{err}");
        let err = build(&clean, RunSource::Attested(&g[..9])).unwrap_err();
        assert!(err.detail.contains("encoded more entries than the source attested"), "{err}");
    }

    #[test]
    fn a_key_order_violation_fails_the_build_unless_knocked_out() {
        let clean = entries(300);
        let mut src = clean.clone();
        src[150].key[5] ^= 0x04;
        assert_eq!(&src[150].key[..6], b"at://`");
        let g = groups(&src, 32);
        for source in [RunSource::Inline, RunSource::Attested(&g)] {
            let err = build(&src, source).unwrap_err();
            assert_eq!((err.check, err.entry), ("order", 150));
            assert!(err.detail.contains("not strictly above the previous encoded key"), "{err}");
            let (bytes, _) = test_seams::with_knockouts(ORDER, || build(&src, source)).unwrap();
            assert_eq!(Run::parse(bytes).unwrap().entry_count(), 300);
        }
        let mut dup = clean.clone();
        dup[11].key = dup[10].key.clone();
        assert_eq!(build(&dup, RunSource::Inline).unwrap_err().check, "order");
    }

    #[test]
    fn an_index_flip_before_its_crc_fails_the_build_unless_knocked_out() {
        let src = entries(300);
        for (offset, mask) in [(8 + 4 + 3, 0x01u8), (8 + 4 + src[0].key.len() + 1, 0x01)] {
            test_seams::arm_index_flip(offset, mask);
            let err = build(&src, RunSource::Inline).unwrap_err();
            assert!(!test_seams::index_flip_armed());
            assert_eq!((err.check, err.block), ("index", 0));
            test_seams::arm_index_flip(offset, mask);
            assert!(test_seams::with_knockouts(INDEX, || build(&src, RunSource::Inline)).is_ok());
        }
    }

    fn stream_build(src: &[SnapshotEntry]) -> (Result<RunBuilt, SnapshotError>, MemBlockDevice, Vec<u64>) {
        let dev = MemBlockDevice::new(BS, 4096);
        let mut alloc = Allocator::new(4096, 8, 0);
        let seg_bytes = alloc.blocks_per_segment() as usize * BS;
        let segs = allocate_run_segs(&mut alloc, BS, run_len_upper_bound_for(src, opts(), RUN_VERSION)).unwrap();
        let lbas: Vec<u64> = segs.iter().map(|s| alloc.segment_range(*s).0).collect();
        let result = {
            let sink = SegmentStreamSink::new(&dev, &segs, &lbas, seg_bytes, BS);
            let mut builder = RunStreamBuilder::new(sink, opts(), src.len()).version(RUN_VERSION);
            for e in src {
                builder.push(e);
            }
            vals_rt::block_on(builder.finish_with_bounds(), || {}).map(|(_, _, b)| b)
        };
        (result, dev, lbas)
    }

    #[test]
    fn merge_output_entry_flips_fail_the_streamed_build() {
        let src = entries(300);
        let (ok, _, _) = stream_build(&src);
        assert_eq!(ok.unwrap(), RunBuilt { min_key: src[0].key.clone(), max_key: src[299].key.clone() });

        let last = src[120].key.len() - 1;
        let before = encode_integrity_failures()[1].1;
        test_seams::arm_stream_entry_flip(120, last, 0x01, false);
        let err = match stream_build(&src).0 {
            Err(SnapshotError::EncodeIntegrity(e)) => e,
            other => panic!("{other:?}"),
        };
        assert!(!test_seams::stream_entry_flip_armed());
        assert_eq!((err.path, err.check, err.entry), (EncodePath::Merge, "source-hash", 120));
        assert!(err.detail.contains("stage=source-copy"), "{err}");
        assert_eq!(flip(err.finding.as_ref().unwrap()), ("key", 120, last, 0x01));
        assert!(encode_integrity_failures()[1].1 > before);
        test_seams::arm_stream_entry_flip(120, last, 0x01, false);
        assert!(test_seams::with_knockouts(SOURCE_HASH, || stream_build(&src).0).is_ok());

        test_seams::arm_stream_entry_flip(150, 5, 0x04, true);
        let err = match stream_build(&src).0 {
            Err(SnapshotError::EncodeIntegrity(e)) => e,
            other => panic!("{other:?}"),
        };
        assert_eq!((err.check, err.entry), ("order", 150));
        test_seams::arm_stream_entry_flip(150, 5, 0x04, true);
        assert!(test_seams::with_knockouts(ORDER, || stream_build(&src).0).is_ok());

        let (first, klen) = first_key_len_offset(&src, 1);
        test_seams::arm_buffer_flip(1, 12 + klen + 16, 0x80);
        let err = match stream_build(&src).0 {
            Err(SnapshotError::EncodeIntegrity(e)) => e,
            other => panic!("{other:?}"),
        };
        assert!(err.detail.contains("stage=encode"), "{err}");
        assert_eq!(flip(err.finding.as_ref().unwrap()), ("entry.seq", first, 16, 0x80));

        test_seams::arm_index_flip(8 + 4 + 2, 0x02);
        let err = match stream_build(&src).0 {
            Err(SnapshotError::EncodeIntegrity(e)) => e,
            other => panic!("{other:?}"),
        };
        assert_eq!(err.check, "index");
    }

    #[test]
    #[ignore]
    fn encode_check_cost_microbench() {
        use crate::run::{encode_block, encode_index_bytes, RunSink, VecSink};
        let n = 400_000usize;
        let src: Vec<SnapshotEntry> = (0..n)
            .map(|i| SnapshotEntry {
                key: format!(
                    "at://did:plc:{:024x}/app.bsky.feed.post/3l{:011}\0at://did:plc:{:024x}/app.bsky.feed.like/3l{:011}\0subject.uri",
                    i / 7, i % 7, i.wrapping_mul(2654435761) % (1 << 40), i
                )
                .into_bytes(),
                ptr: ValuePtr { segment: SegmentId(i as u64 / 1000), offset: i as u32 * 64, len: 300 },
                seq: i as u64,
                version: i as u128,
                tombstone: false,
            })
            .collect();
        let opts = RunBuildOpts::default();
        let g = groups(&src, 32);
        let plain_flush = || {
            let mut sink = VecSink::new();
            let mut index = super::super::FenceBuilder::default();
            let mut i = 0;
            while i < src.len() {
                let start = i;
                let mut est = 0usize;
                while i < src.len() && (i == start || est < opts.block_target_bytes) {
                    est += src[i].key.len() + 40;
                    i += 1;
                }
                let block = encode_block(&src[start..i]);
                index.push(&src[start].key, sink.position(), block.len() as u32).unwrap();
                sink.write(&block);
            }
            let bloom = BlockedBloom::build(src.iter().map(|e| e.key.as_slice()), src.len(), opts.bloom_bits_per_key);
            sink.write(&bloom.encode());
            sink.write(&encode_index_bytes(&index.finish(), None));
            sink.into_inner().len()
        };
        let checked_flush = || {
            let mut sink = VecSink::new();
            crate::run::build_run_into_checked(&src, opts, RUN_VERSION, EncodePath::Flush, RunSource::Attested(&g), &mut sink)
                .unwrap();
            sink.into_inner().len()
        };
        let stream = |checked: bool, owned: Option<Vec<SnapshotEntry>>| {
            let mut sink = VecSink::new();
            let mut index = Vec::new();
            let mut bloom = BlockedBloom::with_capacity(src.len(), opts.bloom_bits_per_key);
            let mut check = EncodeCheck::new(EncodePath::Merge);
            let mut cur: Vec<SnapshotEntry> = Vec::new();
            let mut hashes: Vec<u64> = Vec::new();
            let mut est = 0usize;
            let mut close = |cur: &mut Vec<SnapshotEntry>, hashes: &mut Vec<u64>, sink: &mut VecSink, bloom: &mut BlockedBloom| {
                if checked {
                    let (block, first) = check.block(cur, Expect::Entries(hashes), bloom).unwrap();
                    index.push((first, sink.position(), block.len() as u32));
                    sink.write(&block);
                    check.retire(block);
                } else {
                    let block = encode_block(cur);
                    index.push((cur[0].key.clone(), sink.position(), block.len() as u32));
                    sink.write(&block);
                }
                cur.clear();
                hashes.clear();
            };
            let mut push = |e: SnapshotEntry, cur: &mut Vec<SnapshotEntry>, bloom: &mut BlockedBloom| {
                if !cur.is_empty() && est >= opts.block_target_bytes {
                    close(cur, &mut hashes, &mut sink, bloom);
                    est = 0;
                }
                if checked {
                    hashes.push(snapshot_entry_hash(&e));
                } else {
                    bloom.insert(&e.key);
                }
                est += e.key.len() + 40;
                cur.push(e);
            };
            match owned {
                Some(v) => v.into_iter().for_each(|e| push(e, &mut cur, &mut bloom)),
                None => src.iter().for_each(|e| push(e.clone(), &mut cur, &mut bloom)),
            }
            close(&mut cur, &mut hashes, &mut sink, &mut bloom);
            std::hint::black_box(bloom.encode().len()) + sink.position() as usize
        };
        let time = |f: &mut dyn FnMut() -> usize| {
            let t = std::time::Instant::now();
            std::hint::black_box(f());
            t.elapsed().as_secs_f64() * 1e9 / n as f64
        };
        for round in 0..5 {
            let (pf, cf) = (time(&mut || plain_flush()), time(&mut || checked_flush()));
            let (pm, cm) = (time(&mut || stream(false, None)), time(&mut || stream(true, None)));
            let mut input = Some(src.clone());
            let co = time(&mut || stream(true, input.take()));
            println!(
                "round {round}: flush encode {pf:.1} ns/entry, checked {cf:.1} (+{:.1} ns, +{:.1}%); \
                 merge-output encode {pm:.1} ns/entry, checked {cm:.1} (+{:.1} ns, +{:.1}%), \
                 checked push_owned {co:.1} ({:+.1} ns, {:+.1}%)",
                cf - pf, 100.0 * (cf - pf) / pf, cm - pm, 100.0 * (cm - pm) / pm,
                co - pm, 100.0 * (co - pm) / pm,
            );
        }
    }
}

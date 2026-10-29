use std::sync::Arc;
use vals_spdk::{AsyncRead, BlockDevice};

use crate::alloc::{Allocator, SegmentId};
use crate::crc;
use crate::index::{common_prefix_len, snapshot_entry_hash};
use crate::range_tombstone::{decode_rt_block, RtRecord};
use crate::snapshot::SnapshotEntry;
use crate::snapshot_io::SnapshotError;
use crate::vlog::ValuePtr;

#[cfg(any(test, feature = "test-fixtures"))]
pub mod legacy_fixture;

mod block;
mod build;
mod encode_check;
mod fence;
mod filter;
mod footer;
mod io;
mod sidecar;
mod stream;

pub use block::*;
pub use build::*;
#[cfg(any(test, feature = "test-fixtures"))]
pub use encode_check::test_seams as encode_test_seams;
pub use encode_check::{encode_integrity_failures, EncodeIntegrityError, EncodePath, RunBuilt, RunSource};
pub use filter::*;
pub use footer::*;
pub use io::*;
pub use sidecar::*;
pub use stream::*;

use encode_check::{EncodeCheck, Expect};
use fence::{FenceBuilder, FenceIndex};

pub type Digest = (Vec<u8>, Vec<u8>);

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
        let ft = decode_run_footer(&bytes[bytes.len() - FOOTER_LEN..], bytes.len() as u64)?;
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
        let ft = decode_run_footer(&footer_bytes, handle.len).map_err(SnapshotError::Corrupt)?;
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
mod tests;

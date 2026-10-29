use super::*;

pub(crate) const RUN_MAGIC: u32 = 0x4e55_5256;
pub const RUN_VERSION: u16 = 6;
pub const RUN_BLOCK_VERSION: u16 = 5;
pub const RUN_SIDECAR_VERSION: u16 = 4;
pub(crate) const FOOTER_LEN: usize = 4 + 2 + 2 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 4 + 4;
pub(super) const INDEX_RT_TRAILER_LEN: usize = 8 + 8 + 8;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RtLocator {
    pub seq_floor: u64,
    pub off: u64,
    pub len: u64,
}

pub(super) fn encode_index_bytes(index: &FenceIndex, rt: Option<RtLocator>) -> Vec<u8> {
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

pub(super) struct FooterFields {
    pub(super) entry_count: u64,
    pub(super) num_blocks: u64,
    pub(super) index_off: u64,
    pub(super) index_len: u64,
    pub(super) bloom_off: u64,
    pub(super) bloom_len: u64,
    pub(super) sidecar_off: u64,
    pub(super) sidecar_len: u64,
    pub(super) num_buckets: u32,
    pub(super) version: u16,
}

pub(super) fn encode_footer(ff: &FooterFields) -> Vec<u8> {
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

#[cfg(test)]
pub(super) fn section(bytes: &[u8], off: u64, len: u64) -> Result<&[u8], &'static str> {
    let end = off.checked_add(len).ok_or("section overflow")?;
    bytes
        .get(off as usize..end as usize)
        .ok_or("section out of range")
}

pub(super) fn decode_index(bytes: &[u8], version: u16) -> Result<(FenceIndex, Option<RtLocator>), &'static str> {
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
        Some(RtLocator { seq_floor: c.u64()?, off: c.u64()?, len: c.u64()? })
    } else {
        None
    };
    if c.off != body.len() {
        return Err("trailing bytes");
    }
    Ok((out.finish(), rt))
}

pub(super) struct Footer {
    pub(super) entry_count: u64,
    pub(super) index_off: u64,
    pub(super) index_len: u64,
    pub(super) bloom_off: u64,
    pub(super) bloom_len: u64,
    pub(super) sidecar_off: u64,
    pub(super) sidecar_len: u64,
    pub(super) num_buckets: u32,
    pub(super) version: u16,
}

pub(super) fn decode_footer(f: &[u8]) -> Result<Footer, &'static str> {
    decode_footer_capped(f, RUN_VERSION)
}

pub(super) fn decode_run_footer(f: &[u8], run_len: u64) -> Result<Footer, &'static str> {
    let ft = decode_footer(f)?;
    let body_len = run_len.checked_sub(FOOTER_LEN as u64).ok_or("run shorter than its footer")?;
    let sections = [(ft.bloom_off, ft.bloom_len), (ft.index_off, ft.index_len), (ft.sidecar_off, ft.sidecar_len)];
    if sections.iter().any(|&(off, len)| off.checked_add(len).is_none_or(|end| end > body_len)) {
        return Err("run footer section outside the run");
    }
    Ok(ft)
}

pub(super) fn decode_footer_capped(f: &[u8], max_version: u16) -> Result<Footer, &'static str> {
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

pub struct RunParts {
    pub(super) index: FenceIndex,
    pub(super) bloom: RunFilter,
    pub(super) entry_count: u64,
    pub(super) num_buckets: u32,
    pub(super) sidecar_off: u64,
    pub(super) sidecar_len: u64,
    pub(super) version: u16,
    pub(super) rt: Option<RtLocator>,
}

pub(super) fn check_rt_locator(rt: Option<RtLocator>, ft: &Footer) -> Result<(), SnapshotError> {
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
    let ft = decode_run_footer(&footer, handle.len).map_err(SnapshotError::Corrupt)?;
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

pub(super) fn resident_stats(index: &FenceIndex, bloom: &RunFilter, base: usize) -> RunResidentStats {
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
pub(super) struct IndexCrc {
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

    pub(super) fn passes(&self) -> bool {
        self.stored == self.computed
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct IndexDiffLocation {
    pub(super) run_off: u64,
    pub(super) seg_index: usize,
    pub(super) lba: u64,
    pub(super) byte_in_block: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct IndexDiff {
    pub(super) bytes: usize,
    pub(super) first: Option<IndexDiffLocation>,
    pub(super) last: Option<IndexDiffLocation>,
    pub(super) first_read_all_zero: bool,
    pub(super) span_4k_aligned: bool,
    pub(super) span_128k_aligned: bool,
}

#[derive(Debug)]
pub(super) struct IndexCrcEvidence {
    pub(super) first_crc: IndexCrc,
    pub(super) reread_crc: [Result<IndexCrc, String>; 2],
    pub(super) verdict: &'static str,
    pub(super) diff: Option<IndexDiff>,
}

pub(super) fn inspect_index_crc<D: BlockDevice>(
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

pub(super) struct IndexCrcWarning<'a> {
    pub(super) site: &'static str,
    pub(super) handle: &'a RunHandle,
    pub(super) seg_lbas: &'a [u64],
    pub(super) seg_bytes: usize,
    pub(super) block_size: usize,
    pub(super) device_key: Option<vals_spdk::MaintenanceCacheKey>,
    pub(super) ft: &'a Footer,
    pub(super) evidence: &'a IndexCrcEvidence,
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

pub(super) fn warn_index_crc<D: BlockDevice>(
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

pub(super) fn read_run_parts_inner<D: BlockDevice>(
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
    let ft = decode_run_footer(&footer_bytes, handle.len).map_err(SnapshotError::Corrupt)?;
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

use super::*;

pub(super) fn parse_run_write_version(raw: Option<&str>) -> u16 {
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

pub(super) static BUILD_VERIFY_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static BUILD_VERIFY_FAILURES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn run_build_verify_totals() -> (u64, u64) {
    (
        BUILD_VERIFY_BYTES.load(std::sync::atomic::Ordering::Relaxed),
        BUILD_VERIFY_FAILURES.load(std::sync::atomic::Ordering::Relaxed),
    )
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
    pub(super) fn new() -> Self {
        VecSink { out: Vec::new() }
    }
    pub(super) fn into_inner(self) -> Vec<u8> {
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
            est += entries[i].key.len() + ENTRY_FIELDS_LEN;
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
        self.cur_est += e.key.len() + ENTRY_FIELDS_LEN;
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

pub(super) fn verify_blocks<D: BlockDevice>(
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

pub(super) fn verify_run_tail<D: BlockDevice>(
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

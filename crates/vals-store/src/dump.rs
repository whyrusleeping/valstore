use std::sync::Mutex;
use std::time::{Duration, Instant};

use vals_spdk::{BlockDevice, ReadOp};

use crate::alloc::{Allocator, SegmentId};
use crate::engine::{Config, EngineError};
use crate::range_tombstone::{decode_rt_value, FragmentSet, RtRecord};
use crate::snapshot_io::SnapshotError;
use crate::vlog::{decode_seg_header, replay_segment, LogError, Record, RecordKind};

pub struct ReadPacer {
    rate_mib: u64,
    next: Mutex<Instant>,
}

impl ReadPacer {
    pub fn new(rate_mib: u64) -> Option<Self> {
        (rate_mib > 0).then(|| ReadPacer {
            rate_mib,
            next: Mutex::new(Instant::now()),
        })
    }

    fn wait_for_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let bytes_per_sec = (self.rate_mib as u128) << 20;
        let nanos = (bytes as u128 * 1_000_000_000).div_ceil(bytes_per_sec);
        let delay = Duration::new(
            (nanos / 1_000_000_000) as u64,
            (nanos % 1_000_000_000) as u32,
        );
        let deadline = {
            let mut next = self.next.lock().unwrap();
            let now = Instant::now();
            let base = (*next).max(now);
            let deadline = base + delay;
            *next = deadline;
            deadline
        };
        std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
    }
}

pub(crate) fn scan_log_segments<D: BlockDevice>(
    dev: &D,
    alloc: &Allocator,
    block_size: usize,
) -> Result<Vec<(SegmentId, u64, bool)>, EngineError> {
    scan_log_segments_inner(dev, alloc, block_size, None)
}

fn scan_log_segments_inner<D: BlockDevice>(
    dev: &D,
    alloc: &Allocator,
    block_size: usize,
    pacer: Option<&ReadPacer>,
) -> Result<Vec<(SegmentId, u64, bool)>, EngineError> {
    let num_segs = alloc.num_segments();
    let mut log_segs: Vec<(SegmentId, u64, bool)> = Vec::new();
    const BATCH: usize = 4096;
    let mut bufs: Vec<Vec<u8>> = (0..BATCH.min(num_segs.max(1) as usize))
        .map(|_| vec![0u8; block_size])
        .collect();
    let mut s: u64 = 0;
    while s < num_segs {
        let n = ((num_segs - s) as usize).min(BATCH);
        {
            let mut ops: Vec<ReadOp> = bufs[..n]
                .iter_mut()
                .enumerate()
                .map(|(i, b)| ReadOp {
                    lba: alloc.segment_range(SegmentId(s + i as u64)).0,
                    buf: b.as_mut_slice(),
                })
                .collect();
            if let Some(pacer) = pacer {
                pacer.wait_for_bytes(n * block_size);
            }
            dev.read_batch(&mut ops)?;
        }
        for (i, b) in bufs[..n].iter().enumerate() {
            match decode_seg_header(b) {
                Ok(hdr) => {
                    if hdr.block_size as usize != block_size {
                        return Err(EngineError::GeometryMismatch {
                            recorded_block_size: hdr.block_size,
                            open_block_size: block_size,
                        });
                    }
                    log_segs.push((SegmentId(s + i as u64), hdr.epoch, hdr.not_tail));
                }
                Err(e @ LogError::Unsupported(_)) => return Err(e.into()),
                Err(_) => {}
            }
        }
        s += n as u64;
    }
    Ok(log_segs)
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct VlogScanStats {
    pub segments: u64,
    pub records: u64,
    pub bytes: u64,
}

fn load_manifest_settled<D: BlockDevice>(
    dev: &D,
    alloc: &Allocator,
) -> Result<Option<(crate::manifest::Manifest, crate::snapshot_io::LiveSnapshot)>, SnapshotError> {
    const ATTEMPTS: u32 = 5;
    for _ in 1..ATTEMPTS {
        match crate::snapshot_io::load_manifest(dev, alloc) {
            Err(SnapshotError::ManifestLost { .. }) => std::thread::sleep(std::time::Duration::from_millis(20)),
            result => return result,
        }
    }
    crate::snapshot_io::load_manifest(dev, alloc)
}

pub fn read_range_tombstones<D: BlockDevice>(
    dev: &D,
    cfg: &Config,
    pacer: Option<&ReadPacer>,
) -> Result<FragmentSet, EngineError> {
    let block_size = dev.block_size();
    let alloc = Allocator::new(dev.num_blocks(), cfg.seg_blocks, cfg.reserved_blocks);
    let pace = |bytes: usize| {
        if let Some(pacer) = pacer {
            pacer.wait_for_bytes(bytes);
        }
    };
    let manifest = match load_manifest_settled(dev, &alloc) {
        Ok(found) => found.map(|(man, _)| man),
        Err(SnapshotError::Io(e)) => return Err(EngineError::Io(e)),
        Err(e @ SnapshotError::ManifestLost { .. }) if crate::snapshot_io::rebuild_lost_manifest_requested() => {
            log::warn!("read range tombstones: {e}; continuing without the runs' range tombstones as requested");
            None
        }
        Err(e) => return Err(e.into()),
    };
    let mut records: Vec<RtRecord> = Vec::new();
    if let Some(man) = &manifest {
        for meta in &man.runs {
            records.extend(crate::run::read_run_range_tombstones(dev, &alloc, &meta.handle, pace)?);
        }
    }
    let covered = manifest.as_ref().map(|man| (man.covered_epoch, man.covered_seq));
    let mut log_segs = scan_log_segments_inner(dev, &alloc, block_size, pacer)?;
    log_segs.sort_by_key(|(_, ep, _)| *ep);
    let mut buf = vec![0u8; cfg.seg_blocks as usize * block_size];
    for (seg, epoch, not_tail) in &log_segs {
        if *not_tail || covered.is_some_and(|(covered_epoch, _)| *epoch < covered_epoch) {
            continue;
        }
        pace(buf.len());
        dev.read_at(alloc.segment_range(*seg).0, &mut buf)
            .map_err(|e| EngineError::from(LogError::Io(e)))?;
        let (_hdr, recs) = replay_segment(&buf)?;
        for (_off, rec) in recs {
            if rec.kind != RecordKind::RangeDelete || covered.is_some_and(|(_, seq)| rec.seq <= seq) {
                continue;
            }
            if !rec.key.is_empty() {
                return Err(LogError::Corrupt("range tombstone record carries a key").into());
            }
            let (lo, hi) = decode_rt_value(&rec.value).map_err(LogError::Corrupt)?;
            records.push(RtRecord { lo, hi, version: rec.version, seq: rec.seq });
        }
    }
    Ok(FragmentSet::from_records(&records))
}

pub fn scan_vlog_records<D, E>(
    dev: &D,
    cfg: &Config,
    emit: impl FnMut(&Record) -> Result<(), E>,
) -> Result<VlogScanStats, ScanError<E>>
where
    D: BlockDevice,
{
    scan_vlog_records_inner(dev, cfg, None, emit)
}

pub fn scan_vlog_records_paced<D, E>(
    dev: &D,
    cfg: &Config,
    pacer: &ReadPacer,
    emit: impl FnMut(&Record) -> Result<(), E>,
) -> Result<VlogScanStats, ScanError<E>>
where
    D: BlockDevice,
{
    scan_vlog_records_inner(dev, cfg, Some(pacer), emit)
}

fn scan_vlog_records_inner<D, E>(
    dev: &D,
    cfg: &Config,
    pacer: Option<&ReadPacer>,
    mut emit: impl FnMut(&Record) -> Result<(), E>,
) -> Result<VlogScanStats, ScanError<E>>
where
    D: BlockDevice,
{
    let block_size = dev.block_size();
    let alloc = Allocator::new(dev.num_blocks(), cfg.seg_blocks, cfg.reserved_blocks);
    let seg_bytes = cfg.seg_blocks as usize * block_size;

    let mut log_segs =
        scan_log_segments_inner(dev, &alloc, block_size, pacer).map_err(ScanError::Engine)?;
    log_segs.sort_by_key(|(_, ep, _)| *ep);

    let mut buf = vec![0u8; seg_bytes];
    let mut stats = VlogScanStats::default();
    for (seg, _epoch, _not_tail) in &log_segs {
        let (start, _) = alloc.segment_range(*seg);
        if let Some(pacer) = pacer {
            pacer.wait_for_bytes(seg_bytes);
        }
        dev.read_at(start, &mut buf)
            .map_err(|e| ScanError::Engine(EngineError::from(crate::vlog::LogError::Io(e))))?;
        let (_hdr, recs) = replay_segment(&buf).map_err(|e| ScanError::Engine(e.into()))?;
        stats.segments += 1;
        stats.bytes += seg_bytes as u64;
        for (_off, rec) in recs {
            emit(&rec).map_err(ScanError::Emit)?;
            stats.records += 1;
        }
    }
    Ok(stats)
}

pub fn scan_vlog_segments_parallel<D, T, E>(
    dev: &D,
    cfg: &Config,
    workers: usize,
    map: impl Fn(&[(u32, Record)]) -> Result<T, E> + Sync,
    emit: impl FnMut(T) -> Result<(), E>,
) -> Result<VlogScanStats, ScanError<E>>
where
    D: BlockDevice,
    T: Send,
    E: Send,
{
    scan_vlog_segments_parallel_inner(dev, cfg, workers, None, map, emit)
}

pub fn scan_vlog_segments_parallel_paced<D, T, E>(
    dev: &D,
    cfg: &Config,
    workers: usize,
    pacer: &ReadPacer,
    map: impl Fn(&[(u32, Record)]) -> Result<T, E> + Sync,
    emit: impl FnMut(T) -> Result<(), E>,
) -> Result<VlogScanStats, ScanError<E>>
where
    D: BlockDevice,
    T: Send,
    E: Send,
{
    scan_vlog_segments_parallel_inner(dev, cfg, workers, Some(pacer), map, emit)
}

fn scan_vlog_segments_parallel_inner<D, T, E>(
    dev: &D,
    cfg: &Config,
    workers: usize,
    pacer: Option<&ReadPacer>,
    map: impl Fn(&[(u32, Record)]) -> Result<T, E> + Sync,
    mut emit: impl FnMut(T) -> Result<(), E>,
) -> Result<VlogScanStats, ScanError<E>>
where
    D: BlockDevice,
    T: Send,
    E: Send,
{
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::mpsc;

    let block_size = dev.block_size();
    let alloc = Allocator::new(dev.num_blocks(), cfg.seg_blocks, cfg.reserved_blocks);
    let seg_bytes = cfg.seg_blocks as usize * block_size;

    let mut log_segs =
        scan_log_segments_inner(dev, &alloc, block_size, pacer).map_err(ScanError::Engine)?;
    log_segs.sort_by_key(|(_, ep, _)| *ep);
    let n = log_segs.len();

    let mut stats = VlogScanStats {
        segments: n as u64,
        records: 0,
        bytes: n as u64 * seg_bytes as u64,
    };

    let read_map = |dev: &D, buf: &mut [u8], idx: usize| -> Result<(T, u64), ScanError<E>> {
        let (start, _) = alloc.segment_range(log_segs[idx].0);
        if let Some(pacer) = pacer {
            pacer.wait_for_bytes(seg_bytes);
        }
        dev.read_at(start, buf)
            .map_err(|e| ScanError::Engine(EngineError::from(crate::vlog::LogError::Io(e))))?;
        let (_hdr, recs) = replay_segment(buf).map_err(|e| ScanError::Engine(e.into()))?;
        let count = recs.len() as u64;
        let item = map(&recs).map_err(ScanError::Emit)?;
        Ok((item, count))
    };

    if workers <= 1 || n <= 1 {
        let mut buf = vec![0u8; seg_bytes];
        for idx in 0..n {
            let (item, count) = read_map(dev, &mut buf, idx)?;
            emit(item).map_err(ScanError::Emit)?;
            stats.records += count;
        }
        return Ok(stats);
    }

    let workers = workers.min(n);
    let window = workers.saturating_mul(2).max(1);
    let sem = Semaphore::new(window);
    let next = AtomicUsize::new(0);
    let (tx, rx) = mpsc::channel::<Result<(usize, u64, T), ScanError<E>>>();

    let result = std::thread::scope(|s| {
        for _ in 0..workers {
            let tx = tx.clone();
            let sem = &sem;
            let next = &next;
            let read_map = &read_map;
            s.spawn(move || {
                let mut buf = vec![0u8; seg_bytes];
                let sib = match dev.sibling() {
                    Ok(s) => s,
                    Err(e) => {
                        let _ = tx.send(Err(ScanError::Engine(EngineError::from(
                            crate::vlog::LogError::Io(e),
                        ))));
                        return;
                    }
                };
                loop {
                    if !sem.acquire() {
                        break;
                    }
                    let idx = next.fetch_add(1, AtomicOrdering::Relaxed);
                    if idx >= n {
                        sem.release();
                        break;
                    }
                    match read_map(&sib, &mut buf, idx) {
                        Ok((item, count)) => {
                            if tx.send(Ok((idx, count, item))).is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e));
                            break;
                        }
                    }
                }
            });
        }
        drop(tx);

        let mut buffered: BTreeMap<usize, (u64, T)> = BTreeMap::new();
        let mut next_emit = 0usize;
        let mut outcome: Result<(), ScanError<E>> = Ok(());
        'recv: while next_emit < n {
            let msg = match rx.recv() {
                Ok(m) => m,
                Err(_) => break,
            };
            match msg {
                Ok((idx, count, item)) => {
                    buffered.insert(idx, (count, item));
                    while let Some((count, item)) = buffered.remove(&next_emit) {
                        if let Err(e) = emit(item) {
                            outcome = Err(ScanError::Emit(e));
                            break 'recv;
                        }
                        stats.records += count;
                        next_emit += 1;
                        sem.release();
                    }
                }
                Err(e) => {
                    outcome = Err(e);
                    break;
                }
            }
        }
        sem.abort();
        outcome
    });

    result.map(|()| stats)
}

struct Semaphore {
    inner: std::sync::Mutex<SemState>,
    cv: std::sync::Condvar,
}

struct SemState {
    permits: usize,
    aborted: bool,
}

impl Semaphore {
    fn new(permits: usize) -> Self {
        Semaphore {
            inner: std::sync::Mutex::new(SemState {
                permits,
                aborted: false,
            }),
            cv: std::sync::Condvar::new(),
        }
    }

    fn acquire(&self) -> bool {
        let mut st = self.inner.lock().unwrap();
        loop {
            if st.aborted {
                return false;
            }
            if st.permits > 0 {
                st.permits -= 1;
                return true;
            }
            st = self.cv.wait(st).unwrap();
        }
    }

    fn release(&self) {
        let mut st = self.inner.lock().unwrap();
        st.permits += 1;
        self.cv.notify_one();
    }

    fn abort(&self) {
        let mut st = self.inner.lock().unwrap();
        st.aborted = true;
        self.cv.notify_all();
    }
}

#[derive(Debug)]
pub enum ScanError<E> {
    Engine(EngineError),
    Emit(E),
}

impl<E: std::fmt::Display> std::fmt::Display for ScanError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScanError::Engine(e) => write!(f, "{e}"),
            ScanError::Emit(e) => write!(f, "{e}"),
        }
    }
}

impl<E: std::fmt::Debug + std::fmt::Display> std::error::Error for ScanError<E> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Engine;
    use crate::Ordering;
    use std::collections::HashMap;
    use vals_spdk::{MemBlockDevice, DEFAULT_BLOCK_SIZE};

    fn cfg() -> Config {
        Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        }
    }

    fn replay_to_map(
        dev: &MemBlockDevice,
        cfg: &Config,
    ) -> (HashMap<Vec<u8>, Vec<u8>>, VlogScanStats) {
        let mut map: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let stats = scan_vlog_records(dev, cfg, |rec: &Record| {
            match rec.kind {
                RecordKind::Put => {
                    map.insert(rec.key.clone(), rec.value.clone());
                }
                RecordKind::Delete => {
                    map.remove(&rec.key);
                }
                RecordKind::RangeDelete => {}
            }
            Ok::<(), std::convert::Infallible>(())
        })
        .unwrap();
        (map, stats)
    }

    fn engine_map(engine: &Engine<MemBlockDevice>) -> HashMap<Vec<u8>, Vec<u8>> {
        let mut m = HashMap::new();
        for k in engine.keys() {
            if let Some(v) = engine.get(&k).unwrap() {
                m.insert(k, v);
            }
        }
        m
    }

    type RecStream = Vec<(RecordKind, Vec<u8>, Vec<u8>)>;

    fn collect_serial(dev: &MemBlockDevice, cfg: &Config) -> RecStream {
        let mut out = Vec::new();
        scan_vlog_records(dev, cfg, |rec: &Record| {
            out.push((rec.kind, rec.key.clone(), rec.value.clone()));
            Ok::<(), std::convert::Infallible>(())
        })
        .unwrap();
        out
    }

    fn collect_parallel(
        dev: &MemBlockDevice,
        cfg: &Config,
        workers: usize,
    ) -> (RecStream, VlogScanStats) {
        let mut out: RecStream = Vec::new();
        let stats = scan_vlog_segments_parallel(
            dev,
            cfg,
            workers,
            |recs: &[(u32, Record)]| {
                let frag: Vec<(RecordKind, Vec<u8>, Vec<u8>)> = recs
                    .iter()
                    .map(|(_off, r)| (r.kind, r.key.clone(), r.value.clone()))
                    .collect();
                Ok::<_, std::convert::Infallible>(frag)
            },
            |frag| {
                out.extend(frag);
                Ok::<(), std::convert::Infallible>(())
            },
        )
        .unwrap();
        (out, stats)
    }

    fn build_corpus(seg_blocks: u64, num_blocks: u64) -> MemBlockDevice {
        let cfg = Config {
            seg_blocks,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        };
        let dev = MemBlockDevice::new(DEFAULT_BLOCK_SIZE, num_blocks);
        let mut engine = Engine::create_prewiped(dev.share(), cfg);
        let big = |tag: &str, i: u32| format!("{tag}-{i:05}-{}", "x".repeat(240));
        for i in 0..600u32 {
            let k = format!("key-{:04}", i % 400);
            engine
                .put(k.as_bytes(), big("val", i).as_bytes(), i as u128 + 1)
                .unwrap();
        }
        for i in 0..50u32 {
            let k = format!("key-{:04}", i);
            engine.delete(k.as_bytes(), 1_000_000 + i as u128).unwrap();
        }
        engine
            .put(b"key-0003", big("revived", 0).as_bytes(), 2_000_000)
            .unwrap();
        engine.sync().unwrap();
        engine
            .flush(crate::run::RunBuildOpts::default())
            .unwrap();
        for i in 600..900u32 {
            let k = format!("key-{:04}", i % 400);
            engine
                .put(k.as_bytes(), big("post", i).as_bytes(), i as u128 + 1)
                .unwrap();
        }
        engine.compact().unwrap();
        engine.sync().unwrap();
        dev
    }

    #[test]
    fn parallel_stream_byte_identical_to_serial() {
        let dev = build_corpus(8, 65_536);
        let cfg = cfg();
        let serial = collect_serial(&dev, &cfg);
        assert!(!serial.is_empty(), "corpus should produce records");
        for workers in [2usize, 4, 8, 16] {
            let (parallel, stats) = collect_parallel(&dev, &cfg, workers);
            assert!(
                stats.segments > 1,
                "corpus must span multiple segments to exercise parallelism (got {})",
                stats.segments
            );
            assert_eq!(
                serial, parallel,
                "parallel stream (workers={workers}) must match serial exactly"
            );
            assert_eq!(stats.records as usize, serial.len());
        }
    }

    #[test]
    fn parallel_pacing_is_aggregate_and_preserves_stream() {
        let dev = build_corpus(8, 65_536);
        let cfg = cfg();
        let serial = collect_serial(&dev, &cfg);
        let rate_mib = 128u64;
        let pacer = ReadPacer::new(rate_mib).unwrap();
        let start = Instant::now();
        let mut paced = Vec::new();
        let stats = scan_vlog_segments_parallel_paced(
            &dev,
            &cfg,
            4,
            &pacer,
            |recs: &[(u32, Record)]| {
                Ok::<_, std::convert::Infallible>(
                    recs.iter()
                        .map(|(_off, r)| (r.kind, r.key.clone(), r.value.clone()))
                        .collect::<RecStream>(),
                )
            },
            |frag| {
                paced.extend(frag);
                Ok::<(), std::convert::Infallible>(())
            },
        )
        .unwrap();
        let elapsed = start.elapsed();

        assert_eq!(paced, serial);
        let alloc = Allocator::new(dev.num_blocks(), cfg.seg_blocks, cfg.reserved_blocks);
        let header_bytes = alloc.num_segments() * dev.block_size() as u64;
        let read_bytes = header_bytes + stats.bytes;
        let minimum = Duration::from_secs_f64(
            read_bytes as f64 / ((rate_mib << 20) as f64),
        );
        assert!(
            elapsed + Duration::from_millis(10) >= minimum,
            "paced {read_bytes} bytes at {rate_mib} MiB/s in {elapsed:?}, faster than {minimum:?}"
        );
    }

    #[test]
    fn parallel_matches_live_state() {
        let dev = build_corpus(8, 65_536);
        let cfg = cfg();
        let (parallel, _) = collect_parallel(&dev, &cfg, 4);
        let mut pmap: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        for (kind, k, v) in parallel {
            match kind {
                RecordKind::Put => {
                    pmap.insert(k, v);
                }
                RecordKind::Delete => {
                    pmap.remove(&k);
                }
                RecordKind::RangeDelete => {}
            }
        }
        let (smap, _) = replay_to_map(&dev, &cfg);
        assert_eq!(pmap, smap);
    }

    #[test]
    fn parallel_degenerate_cases() {
        let dev = MemBlockDevice::new(DEFAULT_BLOCK_SIZE, 64);
        let (recs, stats) = collect_parallel(&dev, &cfg(), 4);
        assert!(recs.is_empty());
        assert_eq!(stats.segments, 0);
        assert_eq!(stats.records, 0);

        let dev = build_corpus(8, 65_536);
        let (one, _) = collect_parallel(&dev, &cfg(), 1);
        let (four, _) = collect_parallel(&dev, &cfg(), 4);
        assert_eq!(one, four);
        assert_eq!(one, collect_serial(&dev, &cfg()));

        let (many, _) = collect_parallel(&dev, &cfg(), 128);
        assert_eq!(many, four);
    }

    #[test]
    fn parallel_map_error_propagates_without_hanging() {
        let dev = build_corpus(8, 65_536);
        let err: Result<VlogScanStats, ScanError<&'static str>> = scan_vlog_segments_parallel(
            &dev,
            &cfg(),
            4,
            |_recs: &[(u32, Record)]| Err::<(), _>("boom"),
            |()| Ok(()),
        );
        assert!(matches!(err, Err(ScanError::Emit("boom"))), "{err:?}");
    }

    #[test]
    fn parallel_emit_error_propagates_without_hanging() {
        let dev = build_corpus(8, 65_536);
        let mut seen = 0u32;
        let err: Result<VlogScanStats, ScanError<&'static str>> = scan_vlog_segments_parallel(
            &dev,
            &cfg(),
            4,
            |_recs: &[(u32, Record)]| Ok::<(), &'static str>(()),
            |()| {
                seen += 1;
                if seen >= 2 {
                    Err("emit-boom")
                } else {
                    Ok(())
                }
            },
        );
        assert!(matches!(err, Err(ScanError::Emit("emit-boom"))), "{err:?}");
    }

    #[test]
    fn empty_device_yields_no_records() {
        let dev = MemBlockDevice::new(DEFAULT_BLOCK_SIZE, 64);
        let (map, stats) = replay_to_map(&dev, &cfg());
        assert!(map.is_empty());
        assert_eq!(stats.records, 0);
        assert_eq!(stats.segments, 0);
    }

    #[test]
    fn stream_matches_live_state_with_overwrites_and_deletes() {
        let dev = MemBlockDevice::new(DEFAULT_BLOCK_SIZE, 256);
        let mut engine = Engine::create_prewiped(dev.share(), cfg());
        engine.put(b"a", b"a1", 1).unwrap();
        engine.put(b"b", b"b1", 2).unwrap();
        engine.put(b"a", b"a2", 3).unwrap();
        engine.put(b"c", b"c1", 4).unwrap();
        engine.delete(b"b", 5).unwrap();
        engine.put(b"a", b"a3", 6).unwrap();
        engine.delete(b"c", 7).unwrap();
        engine.put(b"c", b"c2", 8).unwrap();
        engine.sync().unwrap();

        let live = engine_map(&engine);
        let (replayed, stats) = replay_to_map(&dev, &cfg());
        assert_eq!(replayed, live, "vlog-order replay must equal live state");
        assert!(stats.records as usize > live.len(), "{stats:?}");
    }

    #[test]
    fn stream_matches_after_compaction() {
        let dev = MemBlockDevice::new(DEFAULT_BLOCK_SIZE, 256);
        let mut engine = Engine::create_prewiped(dev.share(), cfg());
        for i in 0..50u32 {
            let k = format!("k{}", i % 10);
            engine
                .put(k.as_bytes(), format!("v{i}").as_bytes(), i as u128 + 1)
                .unwrap();
        }
        engine.delete(b"k3", 1000).unwrap();
        engine.sync().unwrap();
        engine.compact().unwrap();
        engine.sync().unwrap();

        let live = engine_map(&engine);
        let (replayed, _) = replay_to_map(&dev, &cfg());
        assert_eq!(replayed, live);
    }

    #[test]
    fn stream_matches_after_flush_runs_present() {
        let dev = MemBlockDevice::new(DEFAULT_BLOCK_SIZE, 512);
        let mut engine = Engine::create_prewiped(dev.share(), cfg());
        let mut expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        for i in 0..100u32 {
            let k = format!("key-{:04}", i % 40);
            let v = format!("val-{i}");
            engine
                .put(k.as_bytes(), v.as_bytes(), i as u128 + 1)
                .unwrap();
            expected.insert(k.into_bytes(), v.into_bytes());
        }
        engine.delete(b"key-0005", 10_000).unwrap();
        expected.remove(&b"key-0005"[..]);
        engine.sync().unwrap();
        engine
            .flush(crate::run::RunBuildOpts::default())
            .unwrap();
        engine.sync().unwrap();

        let (replayed, stats) = replay_to_map(&dev, &cfg());
        assert_eq!(
            replayed, expected,
            "records still all live in the vlog after a flush"
        );
        assert!(stats.records as usize > expected.len());
    }

    #[test]
    fn geometry_mismatch_fails_loud() {
        use vals_spdk::FileBlockDevice;
        let path = std::env::temp_dir().join(format!(
            "vals-dump-geom-{}-{}.vlog",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        {
            let dev = FileBlockDevice::create(&path, DEFAULT_BLOCK_SIZE, 128).unwrap();
            let mut engine = Engine::create_prewiped(dev, cfg());
            engine.put(b"x", b"y", 1).unwrap();
            engine.sync().unwrap();
        }
        let dev2 = FileBlockDevice::open(&path, DEFAULT_BLOCK_SIZE * 2).unwrap();
        let err = scan_vlog_records(&dev2, &cfg(), |_r: &Record| {
            Ok::<(), std::convert::Infallible>(())
        });
        std::fs::remove_file(&path).ok();
        assert!(
            matches!(
                err,
                Err(ScanError::Engine(EngineError::GeometryMismatch { .. }))
            ),
            "{err:?}"
        );
    }
}

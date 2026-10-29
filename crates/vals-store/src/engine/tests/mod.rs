use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
    use vals_spdk::{FileBlockDevice, MemBlockDevice};

    const BS: usize = 256;

    fn isolated_env_test(settings: &[(&str, Option<&str>)]) -> bool {
        let thread = std::thread::current();
        let name = thread.name().expect("named test thread");
        if let Ok(child) = std::env::var("VALS_STORE_ENV_TEST_CHILD") {
            assert_eq!(child, name, "child must run only its selected test");
            return false;
        }
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command.args(["--exact", name, "--include-ignored", "--nocapture"])
            .env("VALS_STORE_ENV_TEST_CHILD", name);
        for &(key, value) in settings {
            match value {
                Some(value) => { command.env(key, value); }
                None => { command.env_remove(key); }
            }
        }
        let output = command.output().expect("spawn isolated test");
        let stdout = String::from_utf8_lossy(&output.stdout);
        print!("{stdout}");
        assert!(output.status.success() && stdout.contains("test result: ok. 1 passed;"),
            "isolated test {name} failed: {}\n{stdout}\n{}",
            output.status, String::from_utf8_lossy(&output.stderr));
        true
    }

    fn co_compact_env(on: bool) -> bool {
        compact_env(on, false)
    }

    fn compact_env(on: bool, bottommost: bool) -> bool {
        isolated_env_test(&[
            ("VALS_VLOG_CO_COMPACT", Some(if on { "1" } else { "0" })),
            ("VALS_MERGE_TIER_K", bottommost.then_some("64")),
            ("VALS_MERGE_TIER_MIN_K", bottommost.then_some("2")),
            ("VALS_MERGE_FORCE_RUNS", None),
            ("VALS_MERGE_READAHEAD", None),
            ("VALS_MERGE_INPUT_CAP_BYTES", None),
            ("VALS_VICTIM_DEAD_FRACTION", None),
            ("VALS_VLOG_GC", None),
            ("VALS_SHADOW_EST", None),
            ("VALS_SHADOW_FOLD", None),
            ("VALS_SHADOW_SAMPLE_BLOCKS", None),
            ("VALS_SHADOW_FOLD_MIN_GAIN_SEGS", None),
        ])
    }

    fn bottommost_co_compact_env() -> bool {
        compact_env(true, true)
    }

    fn temp_path(tag: &str) -> std::path::PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, AtomicOrdering::Relaxed);
        std::env::temp_dir().join(format!("vals-engine-{}-{}-{}", std::process::id(), tag, n))
    }

    fn measurement_rss() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|status| {
                status
                    .lines()
                    .find(|line| line.starts_with("VmRSS:"))
                    .and_then(|line| line.split_whitespace().nth(1))
                    .and_then(|value| value.parse::<u64>().ok())
            })
            .unwrap_or(0)
            * 1024
    }

    fn engine(ordering: Ordering) -> Engine<MemBlockDevice> {
        let dev = MemBlockDevice::new(BS, 256);
        Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 4,
                reserved_blocks: 0,
                ordering,
            },
        )
    }

    struct DelayDevice {
        inner: MemBlockDevice,
        #[allow(clippy::type_complexity)]
        pending: std::sync::Mutex<
            std::collections::VecDeque<(u64, Vec<u8>, std::sync::Arc<vals_spdk::WriteState>)>,
        >,
    }

    impl DelayDevice {
        fn new(blocks: u64) -> Self {
            DelayDevice {
                inner: MemBlockDevice::new(BS, blocks),
                pending: std::sync::Mutex::new(std::collections::VecDeque::new()),
            }
        }
    }

    impl BlockDevice for DelayDevice {
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
        fn submit_write(&self, lba: u64, buf: &[u8]) -> vals_spdk::WriteHandle {
            let state = vals_spdk::WriteState::pending();
            self.pending
                .lock()
                .unwrap()
                .push_back((lba, buf.to_vec(), state.clone()));
            vals_spdk::WriteHandle::pending(state)
        }
        fn poll_writes(&self) -> i32 {
            let next = self.pending.lock().unwrap().pop_front();
            if let Some((lba, data, state)) = next {
                self.inner.write_at(lba, &data).unwrap();
                state.complete(true);
                1
            } else {
                0
            }
        }
    }

    fn ver_of(key: &[u8]) -> u128 {
        0xabcd_0000u128.wrapping_add(crate::crc::crc32(key) as u128)
    }

    fn run_flush<D: BlockDevice + 'static>(e: &mut Engine<D>, opts: RunBuildOpts) -> Option<RunMeta> {
        let plan = e.flush_plan(opts).unwrap()?;
        let wdev = e.device().sibling().expect("mem/file sibling");
        let written = vals_rt::block_on(build_and_write_flush(plan, &wdev), || {}).unwrap();
        Some(e.flush_commit(written).unwrap())
    }

    struct CountingDevice {
        inner: MemBlockDevice,
        reads: AtomicU64,
    }
    impl BlockDevice for CountingDevice {
        fn block_size(&self) -> usize {
            self.inner.block_size()
        }
        fn num_blocks(&self) -> u64 {
            self.inner.num_blocks()
        }
        fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.reads.fetch_add(1, AtomicOrdering::Relaxed);
            self.inner.read_at(lba, buf)
        }
        fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
            self.inner.write_at(lba, buf)
        }
        fn flush(&self) -> vals_spdk::Result<()> {
            self.inner.flush()
        }
    }

    struct ScriptSource {
        items: std::collections::VecDeque<SnapshotEntry>,
    }
    impl ScriptSource {
        fn new(items: Vec<SnapshotEntry>) -> Self {
            ScriptSource {
                items: items.into(),
            }
        }
    }
    impl MergeSource for ScriptSource {
        fn next_entry(&mut self) -> Result<Option<SnapshotEntry>, EngineError> {
            Ok(self.items.pop_front())
        }
    }

    fn snap_entry(key: &[u8], version: u128) -> SnapshotEntry {
        SnapshotEntry {
            key: key.to_vec(),
            ptr: ValuePtr {
                segment: SegmentId(0),
                offset: 0,
                len: 1,
            },
            seq: version as u64,
            version,
            tombstone: false,
        }
    }

    #[cfg(test)]
    fn hammer_hot_key<D: BlockDevice + 'static>(
        e: &mut Engine<D>,
        hot: &[u8],
        rounds: u32,
        base_ver: u128,
        val_len: usize,
    ) {
        for r in 0..rounds {
            let v = vec![(base_ver as u8).wrapping_add(r as u8); val_len];
            e.put(hot, &v, base_ver + r as u128).unwrap();
        }
    }

    fn manifest_watermark(path: &std::path::Path, cfg: Config) -> (u64, u64) {
        let dev = FileBlockDevice::open(path, BS).unwrap();
        let alloc = Allocator::new(dev.num_blocks(), cfg.seg_blocks, cfg.reserved_blocks);
        match snapshot_io::load_manifest(&dev, &alloc).unwrap() {
            Some((man, _live)) => (man.covered_seq, man.covered_epoch),
            None => (0, 0),
        }
    }

    struct ManifestCrashDevice {
        inner: MemBlockDevice,
        armed: AtomicBool,
    }
    impl BlockDevice for ManifestCrashDevice {
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
            const MAN_MAGIC_LE: [u8; 4] = [0x56, 0x4d, 0x4e, 0x43];
            if self.armed.load(AtomicOrdering::Relaxed)
                && buf.len() >= 4
                && buf[..4] == MAN_MAGIC_LE
            {
                return Ok(());
            }
            self.inner.write_at(lba, buf)
        }
        fn flush(&self) -> vals_spdk::Result<()> {
            self.inner.flush()
        }
    }

    struct ManifestWriteFailDevice {
        inner: MemBlockDevice,
        armed: AtomicBool,
    }
    impl BlockDevice for ManifestWriteFailDevice {
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
            const MAN_MAGIC_LE: [u8; 4] = [0x56, 0x4d, 0x4e, 0x43];
            if self.armed.load(AtomicOrdering::Relaxed)
                && buf.len() >= 4
                && buf[..4] == MAN_MAGIC_LE
            {
                return Err(BlockError::Io(std::io::Error::other(
                    "injected manifest write failure (device full)",
                )));
            }
            self.inner.write_at(lba, buf)
        }
        fn flush(&self) -> vals_spdk::Result<()> {
            self.inner.flush()
        }
        fn deallocate(&self, ranges: &[(u64, u64)]) -> vals_spdk::Result<bool> {
            self.inner.deallocate(ranges)
        }
        fn sibling(&self) -> vals_spdk::Result<Self> {
            Ok(ManifestWriteFailDevice {
                inner: self.inner.share(),
                armed: AtomicBool::new(false),
            })
        }
    }

    struct TailWriteFailDevice {
        inner: MemBlockDevice,
        armed: Arc<AtomicBool>,
    }
    impl BlockDevice for TailWriteFailDevice {
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
            if self.armed.load(AtomicOrdering::Relaxed) {
                return Err(BlockError::Io(std::io::Error::other(
                    "injected tail write failure",
                )));
            }
            self.inner.write_at(lba, buf)
        }
        fn flush(&self) -> vals_spdk::Result<()> {
            self.inner.flush()
        }
        fn sibling(&self) -> vals_spdk::Result<Self> {
            Ok(TailWriteFailDevice {
                inner: self.inner.share(),
                armed: self.armed.clone(),
            })
        }
    }

    struct ReadFailDevice {
        inner: MemBlockDevice,
        armed: AtomicBool,
    }
    impl BlockDevice for ReadFailDevice {
        fn block_size(&self) -> usize {
            self.inner.block_size()
        }
        fn num_blocks(&self) -> u64 {
            self.inner.num_blocks()
        }
        fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            if self.armed.load(AtomicOrdering::Relaxed) {
                return Err(BlockError::Io(std::io::Error::other(
                    "injected run-read failure",
                )));
            }
            self.inner.read_at(lba, buf)
        }
        fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
            self.inner.write_at(lba, buf)
        }
        fn flush(&self) -> vals_spdk::Result<()> {
            self.inner.flush()
        }
    }

    #[derive(Default)]
    struct PostManifestReadFault {
        armed: AtomicBool,
        failing: AtomicBool,
        failed_reads: AtomicU64,
    }

    impl PostManifestReadFault {
        fn arm(&self) {
            self.armed.store(true, AtomicOrdering::Relaxed);
        }
        fn disarm(&self) -> u64 {
            self.armed.store(false, AtomicOrdering::Relaxed);
            self.failing.store(false, AtomicOrdering::Relaxed);
            self.failed_reads.swap(0, AtomicOrdering::Relaxed)
        }
    }

    struct PostManifestReadFailDevice {
        inner: MemBlockDevice,
        fault: Arc<PostManifestReadFault>,
    }
    impl BlockDevice for PostManifestReadFailDevice {
        fn block_size(&self) -> usize {
            self.inner.block_size()
        }
        fn num_blocks(&self) -> u64 {
            self.inner.num_blocks()
        }
        fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            if self.fault.failing.load(AtomicOrdering::Relaxed) {
                self.fault.failed_reads.fetch_add(1, AtomicOrdering::Relaxed);
                return Err(BlockError::Io(std::io::Error::other(
                    "injected post-commit read failure",
                )));
            }
            self.inner.read_at(lba, buf)
        }
        fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
            const MAN_MAGIC_LE: [u8; 4] = [0x56, 0x4d, 0x4e, 0x43];
            self.inner.write_at(lba, buf)?;
            if buf.starts_with(&MAN_MAGIC_LE) && self.fault.armed.swap(false, AtomicOrdering::Relaxed) {
                self.fault.failing.store(true, AtomicOrdering::Relaxed);
            }
            Ok(())
        }
        fn flush(&self) -> vals_spdk::Result<()> {
            self.inner.flush()
        }
        fn deallocate(&self, ranges: &[(u64, u64)]) -> vals_spdk::Result<bool> {
            self.inner.deallocate(ranges)
        }
        fn sibling(&self) -> vals_spdk::Result<Self> {
            Ok(PostManifestReadFailDevice {
                inner: self.inner.share(),
                fault: self.fault.clone(),
            })
        }
    }

    #[derive(Default)]
    struct TruncateQueueTrace {
        pending: std::collections::VecDeque<(
            u64,
            Vec<u8>,
            std::sync::Arc<vals_spdk::WriteState>,
        )>,
        used_headers: std::collections::HashSet<u64>,
        synchronous_writes: usize,
        submissions: usize,
        max_inflight: usize,
        submissions_at_flush: Vec<usize>,
        pending_at_flush: Vec<usize>,
        phase_b_before_phase_a_flush: bool,
        fail_next_completion: bool,
    }

    struct TruncateQueueDevice {
        inner: MemBlockDevice,
        armed: AtomicBool,
        trace: std::sync::Mutex<TruncateQueueTrace>,
    }

    impl TruncateQueueDevice {
        fn new(blocks: u64) -> Self {
            TruncateQueueDevice {
                inner: MemBlockDevice::new(BS, blocks),
                armed: AtomicBool::new(false),
                trace: std::sync::Mutex::new(TruncateQueueTrace::default()),
            }
        }

        fn arm(&self, used_headers: std::collections::HashSet<u64>, fail: bool) {
            let mut trace = self.trace.lock().unwrap();
            *trace = TruncateQueueTrace {
                used_headers,
                fail_next_completion: fail,
                ..Default::default()
            };
            self.armed.store(true, AtomicOrdering::Relaxed);
        }
    }

    impl BlockDevice for TruncateQueueDevice {
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
            if self.armed.load(AtomicOrdering::Relaxed) {
                self.trace.lock().unwrap().synchronous_writes += 1;
            }
            self.inner.write_at(lba, buf)
        }

        fn flush(&self) -> vals_spdk::Result<()> {
            if self.armed.load(AtomicOrdering::Relaxed) {
                let mut trace = self.trace.lock().unwrap();
                let pending = trace.pending.len();
                let submissions = trace.submissions;
                trace.pending_at_flush.push(pending);
                trace.submissions_at_flush.push(submissions);
            }
            self.inner.flush()
        }

        fn submit_write(&self, lba: u64, buf: &[u8]) -> WriteHandle {
            if !self.armed.load(AtomicOrdering::Relaxed) {
                return WriteHandle::ready(self.inner.write_at(lba, buf));
            }

            let state = vals_spdk::WriteState::pending();
            let mut trace = self.trace.lock().unwrap();
            if trace.used_headers.contains(&lba) && trace.submissions_at_flush.is_empty() {
                trace.phase_b_before_phase_a_flush = true;
            }
            trace
                .pending
                .push_back((lba, buf.to_vec(), state.clone()));
            trace.submissions += 1;
            trace.max_inflight = trace.max_inflight.max(trace.pending.len());
            WriteHandle::pending(state)
        }

        fn poll_writes(&self) -> i32 {
            let (next, fail) = {
                let mut trace = self.trace.lock().unwrap();
                let next = trace.pending.pop_front();
                let fail = next.is_some() && std::mem::take(&mut trace.fail_next_completion);
                (next, fail)
            };
            let Some((lba, data, state)) = next else {
                return 0;
            };
            let success = !fail && self.inner.write_at(lba, &data).is_ok();
            state.complete(success);
            1
        }
    }

    fn queued_truncate_engine() -> Engine<TruncateQueueDevice> {
        let blocks = 4 * (MAX_INFLIGHT_HEADER_WIPES as u64 + 44);
        let cfg = Config {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        };
        let mut e = Engine::create_prewiped(TruncateQueueDevice::new(blocks), cfg);
        e.put(b"used", b"value", 0).unwrap();
        e.sync().unwrap();
        e
    }

    fn arm_queued_truncate(e: &Engine<TruncateQueueDevice>, fail: bool) -> (usize, usize) {
        let old_used = e.alloc.used_list();
        let used_headers = old_used
            .iter()
            .map(|&seg| e.alloc.segment_range(seg).0)
            .collect();
        let total = e.alloc.num_segments() as usize;
        e.device().arm(used_headers, fail);
        (total, old_used.len())
    }

    struct CrashDevice {
        inner: MemBlockDevice,
        armed: AtomicBool,
    }

    impl BlockDevice for CrashDevice {
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
            if self.armed.load(AtomicOrdering::Relaxed) && buf.iter().all(|&b| b == 0) {
                return Ok(());
            }
            self.inner.write_at(lba, buf)
        }
        fn flush(&self) -> vals_spdk::Result<()> {
            self.inner.flush()
        }
    }

    fn fill_dirty_multi_segment(e: &mut Engine<MemBlockDevice>) {
        for round in 0..20u32 {
            for k in ["hot0", "hot1", "hot2", "hot3"] {
                e.put(k.as_bytes(), format!("val-{round}").as_bytes(), 0)
                    .unwrap();
            }
        }
        for k in ["keepa", "keepb", "keepc", "keepd"] {
            e.put(k.as_bytes(), b"durable", 0).unwrap();
        }
        e.put(b"gone", b"bye", 0).unwrap();
        e.delete(b"gone", 0).unwrap();
        e.sync().unwrap();
    }

    const K_ONE: usize = 1;

    fn e_used_after(path: &std::path::Path, cfg: Config) -> u64 {
        let dev = FileBlockDevice::open(path, BS).unwrap();
        Engine::open(dev, cfg).unwrap().used_segments()
    }

    struct FailingDevice {
        inner: MemBlockDevice,
        armed: AtomicBool,
        fail_zero_too: bool,
    }

    impl BlockDevice for FailingDevice {
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
            if self.armed.load(AtomicOrdering::Relaxed)
                && (self.fail_zero_too || buf.iter().any(|&b| b != 0))
            {
                return Err(vals_spdk::BlockError::Io(std::io::Error::other(
                    "injected write failure",
                )));
            }
            self.inner.write_at(lba, buf)
        }
        fn flush(&self) -> vals_spdk::Result<()> {
            self.inner.flush()
        }
    }

    fn stalled_mostly_live_floor_engine() -> Engine<MemBlockDevice> {
        let dev = MemBlockDevice::new(BS, 4096);
        let mut e = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 16,
                reserved_blocks: 0,
                ordering: Ordering::Unsorted,
            },
        );
        e.set_maint_reserve_enabled(true);
        for i in 0..100u32 {
            e.put(format!("full-{i:04}").as_bytes(), &[4u8; 3900], i as u128 + 1)
                .unwrap();
        }
        assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
        let mut blocked = false;
        for i in 0..400u32 {
            match e.put(
                format!("resident-{i:04}").as_bytes(),
                &[5u8; 3900],
                i as u128 + 1000,
            ) {
                Ok(()) => {}
                Err(EngineError::Log(LogError::Backpressure)) => {
                    blocked = true;
                    break;
                }
                Err(other) => panic!("unexpected fill error: {other:?}"),
            }
        }
        assert!(blocked);
        assert!(!e.index.is_empty());
        while e.free_segments() > e.maint_reserve_for_test() {
            e.alloc.allocate().unwrap();
        }
        assert!(e.at_maintenance_floor());
        assert_eq!(e.run_count(), 1);
        assert!(
            e.lsm_dead_fraction()
                .is_some_and(|dead| dead < MAINT_RESERVE_DEAD_MIN_FRACTION),
            "mostly-live fill must stay below the dead gate: {:?}",
            e.lsm_dead_fraction()
        );
        let _ = e.lsm_dead_fraction_gate();
        e
    }

    fn fail_pressure_plans<D: BlockDevice + 'static>(e: &mut Engine<D>, n: usize) {
        for _ in 0..n {
            match e.compact_plan_pressure(RunBuildOpts::default(), None) {
                Err(EngineError::InsufficientHeadroom { .. }) => {}
                Ok(Some(plan)) => panic!(
                    "stalled floor engine must not produce a plan: {} inputs",
                    plan.merged_inputs.len()
                ),
                Ok(None) => panic!("floor refusal must surface as InsufficientHeadroom"),
                Err(other) => panic!("unexpected pressure error: {other:?}"),
            }
        }
    }

    const VDP_BUCKETS: u32 = 4;
    fn vdp_bucket(k: &[u8]) -> u32 {
        (k[0] as u32) % VDP_BUCKETS
    }

    fn all_buckets() -> std::collections::HashSet<u32> {
        (0..VDP_BUCKETS).collect()
    }

    fn page(
        e: &Engine<MemBlockDevice>,
        want: &std::collections::HashSet<u32>,
        after: Option<&[u8]>,
        max_bytes: usize,
    ) -> (Vec<(Vec<u8>, u128)>, bool) {
        e.version_digest_page(want, vdp_bucket, after, max_bytes, 0)
            .unwrap()
    }

    fn async_get(e: &Engine<MemBlockDevice>, key: &[u8]) -> Option<Vec<u8>> {
        vals_rt::block_on(
            async {
                let plan = match e.probe_get(key) {
                    StagedGet::Memtable(p) => Some(*p),
                    StagedGet::WinnerDirectory { plan, .. } => Some(*plan),
                    StagedGet::Absent | StagedGet::Covered(_) => None,
                    StagedGet::Err(err) => panic!("probe_get err: {err}"),
                    StagedGet::Runs {
                        key,
                        probes,
                        mutable,
                        cover,
                        ..
                    } => {
                        let run = resolve_run_probe(&key, &probes, e.device()).await.unwrap();
                        let (ptr, version) = match (mutable, run) {
                            (Some(mem), Some(cold)) if cold.version > mem.version => (cold.ptr, cold.version),
                            (Some(mem), _) => (mem.ptr, mem.version),
                            (None, Some(cold)) => (cold.ptr, cold.version),
                            (None, None) => return None,
                        };
                        if cover.is_some_and(|c| version <= c) {
                            return None;
                        }
                        Some(e.plan_get_ptr(&ptr).unwrap())
                    }
                };
                match plan {
                    None => None,
                    Some(plan) => {
                        let rec = plan.execute(e.device()).await.unwrap();
                        match rec.kind {
                            crate::vlog::RecordKind::Put => Some(rec.value),
                            crate::vlog::RecordKind::Delete | crate::vlog::RecordKind::RangeDelete => None,
                        }
                    }
                }
            },
            || {},
        )
    }

    fn async_version(e: &Engine<MemBlockDevice>, key: &[u8]) -> Option<u128> {
        let probe = e.probe_version(key);
        vals_rt::block_on(Engine::version_from_probe(probe, e.device()), || {}).unwrap()
    }

    fn async_digest_page<D: BlockDevice + AsyncRead>(
        e: &Engine<D>,
        want: &std::collections::HashSet<u32>,
        after: Option<&[u8]>,
        max_bytes: usize,
    ) -> (Vec<(Vec<u8>, u128)>, bool) {
        let plan = e
            .plan_version_digest_page(want, vdp_bucket, after, max_bytes, 0)
            .unwrap();
        vals_rt::block_on(
            Engine::version_digest_page_from_plan(
                plan,
                want,
                vdp_bucket,
                after,
                max_bytes,
                0,
                e.device(),
            ),
            || {},
        )
        .unwrap()
    }

    fn mixed_engine() -> (Engine<MemBlockDevice>, RunBuildOpts) {
        let dev = MemBlockDevice::new(BS, 4096);
        let cfg = Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        let mut e = Engine::create_prewiped(dev, cfg);
        let opts = RunBuildOpts {
            num_buckets: VDP_BUCKETS,
            ..Default::default()
        };
        for (k, v) in [(b"a", 1u128), (b"b", 1), (b"c", 1), (b"d", 1)] {
            e.put(k, format!("val-{}", k[0]).as_bytes(), v).unwrap();
        }
        e.flush(opts).unwrap();
        e.put(b"a", b"val-a-new", 5).unwrap();
        e.put(b"e", b"val-e", 2).unwrap();
        e.flush(opts).unwrap();
        e.put(b"f", b"val-f", 3).unwrap();
        e.put(b"c", b"val-c-new", 9).unwrap();
        e.delete(b"b", 8).unwrap();
        (e, opts)
    }

    struct AsyncCountingDevice {
        inner: MemBlockDevice,
        reads: std::sync::Arc<AtomicU64>,
        read_bytes: std::sync::Arc<AtomicU64>,
    }
    impl BlockDevice for AsyncCountingDevice {
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
    }
    impl AsyncRead for AsyncCountingDevice {
        async fn read_async(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.reads.fetch_add(1, AtomicOrdering::Relaxed);
            self.read_bytes
                .fetch_add(buf.len() as u64, AtomicOrdering::Relaxed);
            self.inner.read_at(lba, buf)
        }
    }

    fn two_run_counting_engine() -> (Engine<AsyncCountingDevice>, std::sync::Arc<AtomicU64>) {
        let reads = std::sync::Arc::new(AtomicU64::new(0));
        let dev = AsyncCountingDevice {
            inner: MemBlockDevice::new(BS, 4096),
            reads: reads.clone(),
            read_bytes: std::sync::Arc::new(AtomicU64::new(0)),
        };
        let cfg = Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        let mut e = Engine::create_prewiped(dev, cfg);
        e.put(b"k", b"old", 3).unwrap();
        e.flush(RunBuildOpts::default()).unwrap();
        e.put(b"k", b"new", 7).unwrap();
        e.flush(RunBuildOpts::default()).unwrap();
        assert_eq!(e.len(), 0, "k lives only in the two runs");
        (e, reads)
    }

    fn compact_off<D: BlockDevice + 'static>(
        e: &mut Engine<D>,
        opts: RunBuildOpts,
        gc: Option<u128>,
    ) -> CompactRunsStats {
        let plan = e.compact_plan(opts, gc).unwrap().unwrap();
        let wdev = Arc::new(e.device().sibling().unwrap());
        let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
        e.compact_commit(written).unwrap()
    }

    fn shuffled(n: u32, seed: u64) -> Vec<u32> {
        let mut order: Vec<u32> = (0..n).collect();
        let mut s = seed;
        for i in (1..order.len()).rev() {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let j = (s >> 33) as usize % (i + 1);
            order.swap(i, j);
        }
        order
    }

    fn paged_scan_span_count<D: BlockDevice>(e: &Engine<D>, page: usize) -> (usize, usize) {
        use std::collections::BTreeMap;
        let mut best: BTreeMap<Vec<u8>, (u128, ValuePtr)> = BTreeMap::new();
        for or in &e.runs {
            let mut sc = or.run.block_scan(None, None);
            while let Some(ent) = sc.next(&e.alloc).unwrap() {
                best.entry(ent.key.clone())
                    .and_modify(|slot| {
                        if ent.version > slot.0 {
                            *slot = (ent.version, ent.ptr);
                        }
                    })
                    .or_insert((ent.version, ent.ptr));
            }
        }
        let ordered: Vec<ValuePtr> = best.values().map(|(_, p)| *p).collect();
        let mut total_spans = 0;
        for chunk in ordered.chunks(page) {
            let items: Vec<(u64, ValuePtr)> = chunk
                .iter()
                .map(|p| (e.alloc.segment_range(p.segment).0, *p))
                .collect();
            total_spans += crate::vlog::plan_read_spans_detached(e.block_size, &items).num_spans();
        }
        (total_spans, ordered.len())
    }

    fn overwrite_rounds_then_index_only_merge(
        e: &mut Engine<MemBlockDevice>,
    ) -> CompactRunsStats {
        let opts = RunBuildOpts::default();
        for r in 0..6u128 {
            for k in 0..40u32 {
                let key = format!("k-{k:03}");
                e.put(
                    key.as_bytes(),
                    format!("v-{r}-{k:03}").as_bytes(),
                    1 + r * 1000 + k as u128,
                )
                .unwrap();
            }
            run_flush(e, opts);
        }
        assert_eq!(e.run_count(), 6);
        let plan = e.compact_plan(opts, None).unwrap().unwrap();
        assert!(!plan.co_compacts(), "recurring merge is index-only");
        let wdev = Arc::new(e.device().sibling().unwrap());
        let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
        e.compact_commit(written).unwrap()
    }

    fn staged_winners<D: BlockDevice + AsyncRead + 'static>(
        e: &Engine<D>,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<WinnersPage, EngineError> {
        match e.stage_scan_page(lo, hi, max_entries, max_bytes)? {
            StagedScanPage::Done(done) => Ok((done.winners, done.more)),
            StagedScanPage::Runs(mut merge) => {
                let dev = e.device();
                vals_rt::block_on(
                    async {
                        loop {
                            match merge.step(dev).await? {
                                StagedScanStep::Done => break,
                                StagedScanStep::NeedMem { from } => {
                                    e.scan_supply_mem(&mut merge, &from)?;
                                }
                                StagedScanStep::NeedBlocks { cursor } => {
                                    e.scan_supply_blocks(&mut merge, cursor)?;
                                }
                            }
                        }
                        Ok::<(), EngineError>(())
                    },
                    || {},
                )?;
                let done = merge.finish();
                Ok((done.winners, done.more))
            }
        }
    }

    fn assert_staged_eq_sync<D: BlockDevice + AsyncRead + 'static>(
        e: &Engine<D>,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) {
        let (sync, sync_more) = e.scan_winners_page(lo, hi, max_entries, max_bytes).unwrap();
        let (staged, staged_more) = staged_winners(e, lo, hi, max_entries, max_bytes).unwrap();
        assert_eq!(
            sync_more, staged_more,
            "`more` differs (lo={lo:?} hi={hi:?})"
        );
        assert_eq!(
            sync.len(),
            staged.len(),
            "page length differs (lo={lo:?} hi={hi:?})"
        );
        for (a, b) in sync.iter().zip(&staged) {
            assert_eq!(a.0, b.0, "key differs");
            assert_eq!(a.1.version, b.1.version, "version differs for {:?}", a.0);
            assert_eq!(a.1.ptr, b.1.ptr, "ptr differs for {:?}", a.0);
            assert_eq!(a.1.tombstone, b.1.tombstone, "tombstone flag differs");
        }
    }

    fn engine_with_runs_and_memtable() -> Engine<MemBlockDevice> {
        let dev = MemBlockDevice::new(BS, 8192);
        let cfg = Config {
            seg_blocks: 16,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        let mut e = Engine::create_prewiped(dev, cfg);
        let opts = RunBuildOpts {
            block_target_bytes: 256,
            bloom_bits_per_key: 10,
            num_buckets: 16,
        };
        for i in 0..80u32 {
            let k = format!("k{i:04}");
            e.put(
                k.as_bytes(),
                format!("r1-{i:04}").as_bytes(),
                100 + i as u128,
            )
            .unwrap();
        }
        run_flush(&mut e, opts);
        for i in 40..120u32 {
            let k = format!("k{i:04}");
            let v = if i < 80 && i % 2 == 0 {
                100 + i as u128
            } else {
                200 + i as u128
            };
            e.put(k.as_bytes(), format!("r2-{i:04}").as_bytes(), v)
                .unwrap();
        }
        run_flush(&mut e, opts);
        for i in 100..160u32 {
            let k = format!("k{i:04}");
            e.put(
                k.as_bytes(),
                format!("r3-{i:04}").as_bytes(),
                300 + i as u128,
            )
            .unwrap();
        }
        e.delete(b"k0005", 900).unwrap();
        e.delete(b"k0150", 901).unwrap();
        run_flush(&mut e, opts);
        e.put(b"k0200", b"mt-new", 1000).unwrap();
        e.put(b"k0060", b"mt-over", 5000).unwrap();
        e.put(b"k0100", b"mt-tie", 300 + 100).unwrap();
        e
    }

    fn engine_with_many_block_run(n: u32) -> Engine<MemBlockDevice> {
        let dev = MemBlockDevice::new(BS, 1 << 17);
        let cfg = Config {
            seg_blocks: 64,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        let mut e = Engine::create_prewiped(dev, cfg);
        let opts = RunBuildOpts {
            block_target_bytes: 32,
            bloom_bits_per_key: 10,
            num_buckets: 64,
        };
        for i in 0..n {
            e.put(
                format!("k{i:05}").as_bytes(),
                format!("v{i:05}").as_bytes(),
                i as u128,
            )
            .unwrap();
        }
        run_flush(&mut e, opts);
        assert_eq!(e.run_count(), 1);
        e
    }

    fn staged_winners_counting_block_refills(
        e: &Engine<MemBlockDevice>,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) -> (Vec<(Vec<u8>, IndexEntry)>, bool, usize) {
        let refills = std::cell::Cell::new(0usize);
        let (winners, more) = match e.stage_scan_page(lo, hi, max_entries, max_bytes).unwrap() {
            StagedScanPage::Done(done) => (done.winners, done.more),
            StagedScanPage::Runs(mut merge) => {
                let dev = e.device();
                vals_rt::block_on(
                    async {
                        loop {
                            match merge.step(dev).await? {
                                StagedScanStep::Done => break,
                                StagedScanStep::NeedMem { from } => {
                                    e.scan_supply_mem(&mut merge, &from)?;
                                }
                                StagedScanStep::NeedBlocks { cursor } => {
                                    refills.set(refills.get() + 1);
                                    e.scan_supply_blocks(&mut merge, cursor)?;
                                }
                            }
                        }
                        Ok::<(), EngineError>(())
                    },
                    || {},
                )
                .unwrap();
                let done = merge.finish();
                (done.winners, done.more)
            }
        };
        (winners, more, refills.get())
    }

    #[derive(Clone, Copy, Debug)]
    struct PlanRecord {
        kind: MergeKind,
        input_bytes: u64,
        frozen_inputs: usize,
        paced: bool,
    }

    thread_local! {
        static PLAN_LOG: std::cell::RefCell<Vec<PlanRecord>> = const { std::cell::RefCell::new(Vec::new()) };
    }

    fn take_plan_log() -> Vec<PlanRecord> {
        PLAN_LOG.with(|log| std::mem::take(&mut *log.borrow_mut()))
    }

    fn run_compact_once<D: BlockDevice + 'static>(
        e: &mut Engine<D>,
        pressure: bool,
    ) -> Result<Option<CompactRunsStats>, EngineError> {
        let plan = if pressure {
            e.compact_plan_pressure(RunBuildOpts::default(), None)?
        } else {
            e.compact_plan(RunBuildOpts::default(), None)?
        };
        let Some(plan) = plan else {
            return Ok(None);
        };
        let policy = e.merge_policy;
        let record = PlanRecord {
            kind: plan.kind(),
            input_bytes: plan.input_bytes(),
            frozen_inputs: plan.merged_inputs.iter().filter(|m| policy.frozen(m.handle.len)).count(),
            paced: plan.paced,
        };
        PLAN_LOG.with(|log| log.borrow_mut().push(record));
        let desc = format!(
            "kind={} inputs={} levels={:?} rewrite={} vlog_segs={}",
            if pressure { "pressure" } else { "recurring" },
            plan.merged_inputs.len(),
            plan.merged_inputs.iter().map(|m| m.level).collect::<Vec<_>>(),
            match &plan.vlog_rewrite {
                VlogRewrite::None => "none".to_string(),
                VlogRewrite::All => "all".to_string(),
                VlogRewrite::Victims(v) => format!("victims({})", v.len()),
            },
            plan.vlog_segs.len(),
        );
        let wdev = Arc::new(e.device().sibling().unwrap());
        let written = match vals_rt::block_on(compact_and_write(plan, wdev), || {}) {
            Ok(w) => w,
            Err((segs, err)) => {
                println!("compact_and_write FAILED [{desc}]: {err:?}");
                for seg in &segs {
                    let _ = e.alloc.free(*seg);
                }
                e.compact_in_flight = false;
                e.shadow.pending = None;
                return Err(err);
            }
        };
        let stats = e.compact_commit(written)?;
        assert_refs_allocated(e, &desc);
        Ok(Some(stats))
    }

    fn assert_refs_allocated<D: BlockDevice>(e: &Engine<D>, ctx: &str) {
        for (i, or) in e.runs.iter().enumerate() {
            for seg in &or.meta.handle.segs {
                assert!(
                    !e.alloc.is_free(*seg),
                    "{ctx}: run[{i}] index seg {seg:?} is free while referenced"
                );
            }
            if let Some(vs) = &or.meta.value_segs {
                for seg in vs {
                    assert!(
                        !e.alloc.is_free(*seg),
                        "{ctx}: run[{i}] value seg {seg:?} is free while referenced"
                    );
                }
            }
        }
        for key in e.index.keys() {
            if let Some(entry) = e.index.get(&key) {
                assert!(
                    !e.alloc.is_free(entry.ptr.segment),
                    "{ctx}: memtable key {:?} value seg {:?} is free while referenced",
                    String::from_utf8_lossy(&key),
                    entry.ptr.segment,
                );
            }
        }
    }

    struct ShadowRepro<D: BlockDevice> {
        e: Engine<D>,
        ver: u128,
        rng: u64,
        nrepos: usize,
        keys_per_repo: usize,
        val: Vec<u8>,
        puts: u64,
        flushes: u64,
        merges: u64,
        bottom_folds: u64,
        pressure_plans: u64,
        pressure_freed: u64,
        churn_bytes: u64,
        flush_index_bytes: u64,
    }

    impl<D: BlockDevice + 'static> ShadowRepro<D> {
        fn next_rand(&mut self) -> u64 {
            self.rng = self
                .rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.rng >> 11
        }

        fn key(&mut self, overwrite_pass: bool) -> Vec<u8> {
            let r = self.next_rand();
            let hot_repos = (self.nrepos / 5).max(1);
            let repo = if overwrite_pass && r % 10 < 8 {
                (self.next_rand() as usize) % hot_repos
            } else {
                (self.next_rand() as usize) % self.nrepos
            };
            let idx = (self.next_rand() as usize) % self.keys_per_repo;
            format!("r{repo:04}/k{idx:05}").into_bytes()
        }

        fn true_live_value_bytes(&self) -> u64 {
            (self.nrepos * self.keys_per_repo) as u64 * self.val.len() as u64
        }

        fn visible_dead(&self) -> f64 {
            self.e.lsm_dead_fraction().unwrap_or(-1.0)
        }

        fn true_dead(&self) -> f64 {
            let payload = ((self.e.alloc.blocks_per_segment() as usize * self.e.block_size)
                .saturating_sub(SEG_HEADER_LEN)) as u64;
            let capacity = self.e.alloc.num_segments().saturating_mul(payload);
            let used = self.e.alloc.used_segments().saturating_mul(payload);
            let mut live = self.true_live_value_bytes() + self.e.memtable_bytes();
            for or in &self.e.runs {
                live = live.saturating_add(or.meta.handle.len);
            }
            used.saturating_sub(live) as f64 / capacity as f64
        }

        fn maintain(&mut self, pressure_allowed: bool) -> Result<(), EngineError> {
            if self.e.memtable_bytes() >= 256 * 1024 {
                if let Some(meta) = run_flush(&mut self.e, RunBuildOpts::default()) {
                    self.flushes += 1;
                    self.flush_index_bytes += meta.handle.len;
                    assert_refs_allocated(&self.e, "post-flush");
                }
            }
            while self.e.run_count() >= 8 {
                let bottom = self.e.run_count();
                match run_compact_once(&mut self.e, false)? {
                    Some(stats) => {
                        self.merges += 1;
                        self.churn_bytes += stats.output_bytes;
                        if stats.runs_merged == bottom {
                            self.bottom_folds += 1;
                        }
                    }
                    None => break,
                }
            }
            let pressure_due = self.e.at_maintenance_floor()
                || (self.e.free_fraction() <= 0.50
                    && self
                        .e
                        .lsm_dead_fraction_gate()
                        .is_some_and(|d| d >= 0.10));
            if pressure_allowed && pressure_due {
                let free_before = self.e.free_segments();
                if let Some(stats) = run_compact_once(&mut self.e, true)? {
                    self.pressure_plans += 1;
                    self.churn_bytes += stats.output_bytes;
                    self.pressure_freed += self
                        .e
                        .free_segments()
                        .saturating_sub(free_before);
                }
            }
            Ok(())
        }

        fn report(&self, tag: &str) {
            println!(
                "{tag}: puts={} flushes={} merges={} bottom_folds={} pressure_plans={} \
                 pressure_freed={} free={}/{} free_frac={:.3} runs={} visible_dead={:.3} \
                 true_dead={:.3} at_floor={}",
                self.puts,
                self.flushes,
                self.merges,
                self.bottom_folds,
                self.pressure_plans,
                self.pressure_freed,
                self.e.free_segments(),
                self.e.alloc.num_segments(),
                self.e.free_fraction(),
                self.e.run_count(),
                self.visible_dead(),
                self.true_dead(),
                self.e.at_maintenance_floor(),
            );
        }

        fn floor_autopsy(&self) {
            let seg_bytes = self.e.alloc.blocks_per_segment() as usize * self.e.block_size;
            let usable = seg_bytes.saturating_sub(SEG_HEADER_LEN).max(1);
            for (i, or) in self.e.runs.iter().enumerate() {
                let m = &or.meta;
                let (segs, live) = match (&m.value_segs, m.value_bytes) {
                    (Some(s), Some(l)) => (s.len() as u64, l),
                    _ => continue,
                };
                let dead = segs * usable as u64 - live.min(segs * usable as u64);
                let victims = self
                    .e
                    .select_victims(std::slice::from_ref(m), usable as u64);
                let (nvict, gainful) = match victims {
                    Some((v, _)) => {
                        let g = Engine::<D>::victim_lives_ascending(
                            std::slice::from_ref(m),
                            &v,
                        )
                        .and_then(|lives| {
                            Engine::<D>::gainful_victim_prefix_len(&lives, usable)
                        });
                        (v.len(), g)
                    }
                    None => (0, None),
                };
                println!(
                    "run[{i}]: level={} index={}B entries={} vsegs={} vlive={}B \
                     visible_dead={}B victims={} gainful_prefix={:?}",
                    m.level, m.handle.len, m.entry_count, segs, live, dead, nvict, gainful,
                );
            }
        }
    }

    fn shadow_repro(num_blocks: u64) -> ShadowRepro<MemBlockDevice> {
        let dev = MemBlockDevice::new(BS, num_blocks);
        let mut e = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 32,
                reserved_blocks: 0,
                ordering: Ordering::Unsorted,
            },
        );
        e.set_maint_reserve_enabled(true);
        ShadowRepro {
            e,
            ver: 0,
            rng: 0x243f6a8885a308d3,
            nrepos: 100,
            keys_per_repo: 300,
            val: vec![0xabu8; 256],
            puts: 0,
            flushes: 0,
            merges: 0,
            bottom_folds: 0,
            pressure_plans: 0,
            pressure_freed: 0,
            churn_bytes: 0,
            flush_index_bytes: 0,
        }
    }

    struct OverwriteSummary {
        rejections: u32,
        max_true_dead: f64,
        write_amp: f64,
    }

    fn drive_bootes_shaped_overwrites(r: &mut ShadowRepro<MemBlockDevice>) -> OverwriteSummary {
        for repo in 0..r.nrepos {
            for idx in 0..r.keys_per_repo {
                r.ver += 1;
                let key = format!("r{repo:04}/k{idx:05}").into_bytes();
                let val = r.val.clone();
                r.e.put(&key, &val, r.ver).unwrap();
                r.puts += 1;
                if r.puts % 64 == 0 {
                    r.maintain(true).unwrap();
                }
            }
        }
        let force_runs = r.e.merge_policy.force_runs;
        r.e.merge_policy.force_runs = 2;
        while r.e.run_count() >= 2 {
            match run_compact_once(&mut r.e, false) {
                Ok(Some(_)) => {}
                _ => break,
            }
        }
        r.e.merge_policy.force_runs = force_runs;
        r.report("post-fill-folded");

        r.e.merge_policy.size_cap = 1048576;
        let overwrite_start = r.puts;
        let churn_start = r.churn_bytes + r.flush_index_bytes;
        let mut rejections = 0u32;
        let mut max_true_dead: f64 = 0.0;
        let mut stalled = 0u32;
        loop {
            r.ver += 1;
            let key = r.key(true);
            let val = r.val.clone();
            match r.e.put(&key, &val, r.ver) {
                Ok(()) => {
                    r.puts += 1;
                    stalled = 0;
                }
                Err(err) => {
                    rejections += 1;
                    max_true_dead = max_true_dead.max(r.true_dead());
                    println!(
                        "put rejected at puts={} true_dead={:.3}: {err:?}",
                        r.puts,
                        r.true_dead()
                    );
                    stalled += 1;
                    if stalled >= 25 {
                        break;
                    }
                }
            }
            if r.puts % 64 == 0 || stalled > 0 {
                if let Err(err) = r.maintain(true) {
                    println!("maintenance error at puts={}: {err:?}", r.puts);
                }
            }
            if r.puts % 5_000 == 0 {
                max_true_dead = max_true_dead.max(r.true_dead());
            }
            if r.puts % 100_000 == 0 && stalled == 0 {
                r.report("progress");
            }
            if r.puts - overwrite_start >= 500_000 {
                break;
            }
        }
        r.report("terminal");
        let logical = (r.puts - overwrite_start) * (r.val.len() as u64 + 12);
        let churn = (r.churn_bytes + r.flush_index_bytes) - churn_start;
        let write_amp = if logical == 0 {
            0.0
        } else {
            (logical + churn) as f64 / logical as f64
        };
        std::env::remove_var("VALS_MERGE_INPUT_CAP_BYTES");
        OverwriteSummary {
            rejections,
            max_true_dead,
            write_amp,
        }
    }


mod attribution;
mod basic;
mod crash_points;
mod flush;
mod frozen_garbage;
mod dealloc;
mod keyfilter;
mod keyfilter_bench;
#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub(crate) mod rss_bench;
mod merge;
mod newer_format;
mod pins;
mod policy;
mod range_delete;
mod read;
mod reclaim;
mod runformat;
mod run_count;
mod staged_scan;
mod scan_rev;
mod scan_rev_staged;
mod stress;
mod winner;

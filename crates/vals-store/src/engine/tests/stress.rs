use super::*;
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::AtomicU64;
use std::sync::{mpsc, Mutex};
use vals_spdk::{WriteHandle, WriteState};

const SBS: usize = 256;

struct StressDisk {
    data: Mutex<Vec<u8>>,
}

type PendingWrite = (u64, Vec<u8>, Arc<WriteState>);

pub(super) struct StressDev {
    disk: Arc<StressDisk>,
    base: u64,
    blocks: u64,
    deferred: bool,
    pending: Mutex<Vec<PendingWrite>>,
    rng: AtomicU64,
}

impl StressDev {
    fn region(disk: &Arc<StressDisk>, base: u64, blocks: u64, deferred: bool, seed: u64) -> Self {
        StressDev {
            disk: disk.clone(),
            base,
            blocks,
            deferred,
            pending: Mutex::new(Vec::new()),
            rng: AtomicU64::new(seed | 1),
        }
    }

    fn range(&self, lba: u64, len: usize) -> std::ops::Range<usize> {
        assert!(len % SBS == 0 && lba + (len / SBS) as u64 <= self.blocks, "out of region: lba {lba} len {len}");
        let off = ((self.base + lba) as usize) * SBS;
        off..off + len
    }

    fn next(&self) -> u64 {
        let mut s = self.rng.load(AtomicOrdering::Relaxed);
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        self.rng.store(s, AtomicOrdering::Relaxed);
        s
    }

    fn complete_some(&self) -> i32 {
        let mut pending = self.pending.lock().unwrap();
        let mut done = 0;
        let mut i = 0;
        while i < pending.len() {
            if self.next() % 3 == 0 {
                let j = (self.next() as usize) % pending.len();
                let (lba, buf, state) = pending.swap_remove(j);
                let range = self.range(lba, buf.len());
                self.disk.data.lock().unwrap()[range].copy_from_slice(&buf);
                state.complete(true);
                done += 1;
            } else {
                i += 1;
            }
        }
        done
    }

    fn pending_writes(&self) -> usize {
        self.pending.lock().unwrap().len()
    }
}

impl BlockDevice for StressDev {
    fn block_size(&self) -> usize {
        SBS
    }

    fn num_blocks(&self) -> u64 {
        self.blocks
    }

    fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
        let range = self.range(lba, buf.len());
        buf.copy_from_slice(&self.disk.data.lock().unwrap()[range]);
        Ok(())
    }

    fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
        let range = self.range(lba, buf.len());
        self.disk.data.lock().unwrap()[range].copy_from_slice(buf);
        Ok(())
    }

    fn flush(&self) -> vals_spdk::Result<()> {
        Ok(())
    }

    fn deallocate(&self, ranges: &[(u64, u64)]) -> vals_spdk::Result<bool> {
        let mut data = self.disk.data.lock().unwrap();
        for &(lba, blocks) in ranges {
            let range = self.range(lba, blocks as usize * SBS);
            data[range].fill(0xdd);
        }
        Ok(true)
    }

    fn submit_write(&self, lba: u64, buf: &[u8]) -> WriteHandle {
        if !self.deferred {
            return WriteHandle::ready(self.write_at(lba, buf));
        }
        self.range(lba, buf.len());
        let state = WriteState::pending();
        self.pending.lock().unwrap().push((lba, buf.to_vec(), state.clone()));
        WriteHandle::pending(state)
    }

    fn poll_writes(&self) -> i32 {
        self.complete_some()
    }

    fn poll_device(&self) -> i32 {
        self.complete_some()
    }

    fn sibling(&self) -> vals_spdk::Result<Self> {
        Ok(StressDev::region(&self.disk, self.base, self.blocks, false, 1))
    }
}

impl vals_spdk::AsyncRead for StressDev {
    async fn read_async(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
        self.read_at(lba, buf)
    }
}

enum Job {
    Flush(usize, FlushPlan<StressDev>),
    Compact(usize, CompactPlan<StressDev>),
}

enum Done {
    Flushed(usize, Result<WrittenFlush<StressDev>, (Vec<SegmentId>, EngineError)>),
    Compacted(usize, Result<WrittenCompact<StressDev>, (Vec<SegmentId>, EngineError)>),
}

fn spawn_lanes(compact_lanes: usize) -> (mpsc::Sender<Job>, mpsc::Sender<Job>, mpsc::Receiver<Done>, Vec<std::thread::JoinHandle<()>>) {
    let (done_tx, done_rx) = mpsc::channel::<Done>();
    let (flush_tx, flush_rx) = mpsc::channel::<Job>();
    let (compact_tx, compact_rx) = mpsc::channel::<Job>();
    let compact_rx = Arc::new(Mutex::new(compact_rx));
    let flush_rx = Arc::new(Mutex::new(flush_rx));
    let mut threads = Vec::new();
    for lane in 0..=compact_lanes {
        let rx = if lane == 0 { flush_rx.clone() } else { compact_rx.clone() };
        let done = done_tx.clone();
        threads.push(std::thread::spawn(move || loop {
            let job = rx.lock().unwrap().recv();
            let Ok(job) = job else { return };
            let out = match job {
                Job::Flush(e, plan) => {
                    let wdev = plan.serving_dev().sibling().unwrap();
                    Done::Flushed(e, vals_rt::block_on(build_and_write_flush(plan, &wdev), || {}))
                }
                Job::Compact(e, plan) => {
                    let wdev = Arc::new(plan.serving_dev().sibling().unwrap());
                    Done::Compacted(e, vals_rt::block_on(compact_and_write(plan, wdev), || {}))
                }
            };
            if done.send(out).is_err() {
                return;
            }
        }));
    }
    (flush_tx, compact_tx, done_rx, threads)
}

#[derive(Default, Debug)]
pub(super) struct StressStats {
    pub(super) ops: u64,
    pub(super) flushes: u64,
    pub(super) merges: BTreeMap<&'static str, u64>,
    pub(super) middle_windows: u64,
    pub(super) concurrent_merges: u64,
    pub(super) pins: u64,
    pub(super) pin_reaps: u64,
    pub(super) dealloc_segments: u64,
    pub(super) late_writes_max: usize,
    pub(super) crc_scans: u64,
    pub(super) failed_puts: u64,
}

fn check_storage(e: &Engine<StressDev>, inflight: &[Vec<SegmentId>], ctx: &str) {
    let mut owner: std::collections::HashMap<SegmentId, String> = std::collections::HashMap::new();
    let mut claim = |seg: SegmentId, who: String| {
        assert!(!e.alloc.is_free(seg), "{ctx}: {who} references free segment {seg:?}");
        if let Some(prev) = owner.insert(seg, who.clone()) {
            panic!("{ctx}: segment {seg:?} owned by both {prev} and {who}");
        }
    };
    for (i, or) in e.runs.iter().enumerate() {
        for seg in &or.meta.handle.segs {
            claim(*seg, format!("run[{i}] index"));
        }
    }
    let mut pinned: HashSet<SegmentId> = HashSet::new();
    e.extend_pinned_segments(&mut pinned);
    for seg in pinned {
        claim(seg, "pinned retired run".to_string());
    }
    if let Some(live) = &e.live_manifest {
        for seg in &live.segs {
            claim(*seg, "manifest".to_string());
        }
    }
    for (j, segs) in inflight.iter().enumerate() {
        for seg in segs {
            claim(*seg, format!("in-flight plan {j}"));
        }
    }
    let mut values: HashSet<SegmentId> = HashSet::new();
    for or in &e.runs {
        values.extend(or.meta.value_segs.iter().flatten().copied());
    }
    values.extend(e.flush_value_segs.iter().copied());
    values.extend(e.log.tail_segs_through(u64::MAX));
    values.extend(e.log.open_segment());
    e.index.for_each_value_segment(&mut |seg| {
        values.insert(seg);
    });
    for seg in values {
        assert!(!e.alloc.is_free(seg), "{ctx}: value segment {seg:?} is free while referenced");
        if let Some(prev) = owner.get(&seg) {
            panic!("{ctx}: value segment {seg:?} is also {prev}");
        }
    }
}

fn crc_scan_runs(e: &Engine<StressDev>, ctx: &str) {
    for (i, or) in e.runs.iter().enumerate() {
        let reopened = DeviceRun::open(e.dev.clone(), &e.alloc, or.meta.handle.clone())
            .unwrap_or_else(|err| panic!("{ctx}: run[{i}] reopen failed: {err:?}"));
        let mut scan = reopened.block_scan(None, None);
        let mut n = 0u64;
        while let Some(_entry) = scan
            .next(&e.alloc)
            .unwrap_or_else(|err| panic!("{ctx}: run[{i}] block scan failed after {n} entries: {err:?}"))
        {
            n += 1;
        }
        assert_eq!(n, or.meta.entry_count, "{ctx}: run[{i}] entry count");
    }
}

fn value_for(engine: usize, key: u32, version: u128, len: usize) -> Vec<u8> {
    let tag = format!("e{engine}/k{key}/v{version}/");
    tag.bytes().cycle().take(len.max(tag.len())).collect()
}

pub(super) struct StressConfig {
    pub(super) engines: usize,
    pub(super) ops: u64,
    pub(super) keys: u32,
    pub(super) region_blocks: u64,
    pub(super) seg_blocks: u64,
    pub(super) deferred: bool,
    pub(super) compact_lanes: usize,
    pub(super) rogue: Rogue,
    pub(super) merge_gate: u64,
    pub(super) bulky: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Rogue {
    None,
    FreeOutput,
    ScribbleOutput,
}

pub(super) fn run_stress(seed: u64, cfg: &StressConfig) -> StressStats {
    let disk = Arc::new(StressDisk {
        data: Mutex::new(vec![0u8; cfg.engines * cfg.region_blocks as usize * SBS]),
    });
    let mut engines: Vec<Engine<StressDev>> = (0..cfg.engines)
        .map(|i| {
            let dev = StressDev::region(&disk, i as u64 * cfg.region_blocks, cfg.region_blocks, cfg.deferred, seed ^ (i as u64 + 1) * 0x9e37);
            let mut e = Engine::create_prewiped(
                dev,
                Config { seg_blocks: cfg.seg_blocks, reserved_blocks: 0, ordering: if i % 2 == 0 { Ordering::Sorted } else { Ordering::Unsorted } },
            );
            e.set_maint_reserve_enabled(true);
            e.set_device_dealloc(true);
            e
        })
        .collect();
    let mut models: Vec<BTreeMap<u32, (u128, Option<Vec<u8>>)>> = vec![BTreeMap::new(); cfg.engines];
    let mut ver = vec![0u128; cfg.engines];
    let mut inflight: Vec<Vec<Option<Vec<SegmentId>>>> = vec![vec![None, None]; cfg.engines];
    let mut pins: Vec<(usize, RunPin, u64)> = Vec::new();
    let mut next_flush = vec![4 * 1024u64; cfg.engines];
    let (flush_tx, compact_tx, done_rx, threads) = spawn_lanes(cfg.compact_lanes);
    let mut rng = Rng(seed.wrapping_mul(0x2545_F491_4F6C_DD1D) | 1);
    let mut stats = StressStats::default();
    let (verify_bytes_before, verify_failures_before) = run::run_build_verify_totals();
    let opts = RunBuildOpts::default();
    let rogue = cfg.rogue;
    let inflight_segs = |slots: &[Option<Vec<SegmentId>>]| -> Vec<Vec<SegmentId>> { slots.iter().flatten().cloned().collect() };

    let handle_done = |done: Done, engines: &mut Vec<Engine<StressDev>>, inflight: &mut Vec<Vec<Option<Vec<SegmentId>>>>, stats: &mut StressStats| {
        match done {
            Done::Flushed(i, res) => {
                let written = res.unwrap_or_else(|(_, err)| panic!("seed {seed}: engine {i} flush failed: {err:?}"));
                engines[i].flush_commit(written).unwrap_or_else(|err| panic!("seed {seed}: engine {i} flush commit: {err:?}"));
                inflight[i][0] = None;
                stats.flushes += 1;
                check_storage(&engines[i], &inflight_segs(&inflight[i]), &format!("seed {seed} engine {i} after flush"));
            }
            Done::Compacted(i, res) => {
                let written = res.unwrap_or_else(|(_, err)| panic!("seed {seed}: engine {i} off-core merge failed: {err:?}"));
                if rogue == Rogue::ScribbleOutput && written.kind() == MergeKind::Forced {
                    let seg = written.handle.segs[0];
                    let (lba, blocks) = engines[i].alloc.segment_range(seg);
                    engines[i].dev.write_at(lba + blocks / 2, &[0x5a; SBS]).unwrap();
                }
                engines[i].compact_commit(written).unwrap_or_else(|err| panic!("seed {seed}: engine {i} merge commit: {err:?}"));
                inflight[i][1] = None;
                check_storage(&engines[i], &inflight_segs(&inflight[i]), &format!("seed {seed} engine {i} after merge"));
            }
        }
    };

    let mut sched = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let mut due: Vec<[Option<u64>; 2]> = vec![[None; 2]; cfg.engines];
    let mut arrived: Vec<[Option<Done>; 2]> = (0..cfg.engines).map(|_| [None, None]).collect();
    let await_done = |i: usize, slot: usize, arrived: &mut Vec<[Option<Done>; 2]>| -> Done {
        loop {
            if let Some(done) = arrived[i][slot].take() {
                return done;
            }
            let done = done_rx.recv_timeout(std::time::Duration::from_secs(120)).expect("lanes stalled");
            let (j, s) = match &done {
                Done::Flushed(j, _) => (*j, 0),
                Done::Compacted(j, _) => (*j, 1),
            };
            arrived[j][s] = Some(done);
        }
    };

    let mut op = 0u64;
    while op < cfg.ops {
        op += 1;
        let i = rng.below(cfg.engines as u64) as usize;
        let roll = rng.below(1000);
        let e = &mut engines[i];
        if roll < 760 {
            ver[i] += 1;
            let key = rng.below(cfg.keys as u64) as u32;
            let tomb = roll < 90 && models[i].contains_key(&key);
            let value = if tomb { Vec::new() } else { value_for(i, key, ver[i], 24 + rng.below(360) as usize) };
            match e.put_kind(format!("key{key:06}").as_bytes(), &value, ver[i], tomb) {
                Ok(()) => {
                    models[i].insert(key, (ver[i], (!tomb).then_some(value)));
                }
                Err(_) => stats.failed_puts += 1,
            }
        } else if roll < 830 {
            if inflight[i][0].is_none() && e.memtable_bytes() >= next_flush[i] {
                next_flush[i] = if cfg.bulky && rng.below(4) == 0 { 40 + rng.below(72) } else { 2 + rng.below(10) } * 1024;
                if let Some(plan) = e.flush_plan(opts).unwrap() {
                    inflight[i][0] = Some(plan.segments().to_vec());
                    let lag = sched.below(200);
                    due[i][0] = Some(op + 5 + sched.below(1 + lag));
                    flush_tx.send(Job::Flush(i, plan)).unwrap();
                }
            }
        } else if roll < 900 {
            if inflight[i][1].is_none() && rng.below(cfg.merge_gate) == 0 {
                let pressure_due = e.at_maintenance_floor()
                    || (e.free_fraction() <= 0.5 && e.lsm_dead_fraction_gate().is_some_and(|d| d >= 0.10));
                let planned = if pressure_due { e.compact_plan_pressure(opts, None) } else { e.compact_plan(opts, None) };
                if let Ok(Some(plan)) = planned {
                    let middle = plan.merged_inputs.last().map(|m| &m.handle) != e.runs.last().map(|or| &or.meta.handle);
                    let windowed = matches!(plan.kind(), MergeKind::Recurring | MergeKind::Forced);
                    stats.middle_windows += u64::from(windowed && middle);
                    *stats.merges.entry(plan.kind().label()).or_default() += 1;
                    if inflight.iter().filter(|slots| slots[1].is_some()).count() > 0 {
                        stats.concurrent_merges += 1;
                    }
                    let mut segs = plan.segments();
                    check_storage(e, &[inflight_segs(&inflight[i]), vec![segs.clone()]].concat(), &format!("seed {seed} engine {i} at merge plan"));
                    let rogue = cfg.rogue == Rogue::FreeOutput && plan.kind() == MergeKind::Forced;
                    let victim = plan.segs.first().copied();
                    compact_tx.send(Job::Compact(i, plan)).unwrap();
                    if let (true, Some(seg)) = (rogue, victim) {
                        e.alloc.free(seg).unwrap();
                        segs.retain(|s| *s != seg);
                    }
                    inflight[i][1] = Some(segs);
                    let lag = sched.below(800);
                    due[i][1] = Some(op + 20 + sched.below(1 + lag));
                }
            }
        } else if roll < 920 {
            if let Some(pin) = e.pin_runs(true).pin {
                pins.push((i, pin, op + 50 + rng.below(2000)));
                stats.pins += 1;
            }
        } else if roll < 940 {
            let before = pins.len();
            pins.retain(|(_, _, until)| *until > op);
            if pins.len() != before {
                for (j, e) in engines.iter_mut().enumerate() {
                    stats.pin_reaps += e.reap_run_pins() as u64;
                    check_storage(e, &inflight_segs(&inflight[j]), &format!("seed {seed} engine {j} after reap"));
                }
            }
        } else if roll < 960 {
            stats.dealloc_segments += e.dealloc_step(1 + rng.below(32) as usize).segments as u64;
        } else if roll < 990 {
            stats.late_writes_max = stats.late_writes_max.max(e.dev.pending_writes());
            e.dev.poll_writes();
        } else {
            let key = rng.below(cfg.keys as u64) as u32;
            let got = e.get(format!("key{key:06}").as_bytes()).unwrap();
            match models[i].get(&key) {
                Some((_, Some(v))) => assert_eq!(got.as_deref(), Some(v.as_slice()), "seed {seed} engine {i} key {key}"),
                Some((_, None)) => assert_eq!(got.as_deref(), Some(&[][..]), "seed {seed} engine {i} key {key} tombstone"),
                None => assert_eq!(got, None, "seed {seed} engine {i} key {key} never written"),
            }
        }
        for (j, slots) in due.iter_mut().enumerate() {
            for (slot, due_at) in slots.iter_mut().enumerate() {
                if due_at.is_some_and(|at| at <= op) {
                    *due_at = None;
                    handle_done(await_done(j, slot, &mut arrived), &mut engines, &mut inflight, &mut stats);
                }
            }
        }
        if op % 2_000 == 0 {
            for (j, e) in engines.iter().enumerate() {
                crc_scan_runs(e, &format!("seed {seed} engine {j} op {op}"));
                stats.crc_scans += e.run_count() as u64;
            }
        }
    }
    loop {
        let next = (0..cfg.engines)
            .flat_map(|j| [(j, 0), (j, 1)])
            .filter_map(|(j, slot)| due[j][slot].map(|at| (at, j, slot)))
            .min();
        let Some((_, j, slot)) = next else { break };
        due[j][slot] = None;
        handle_done(await_done(j, slot, &mut arrived), &mut engines, &mut inflight, &mut stats);
    }
    assert!(inflight.iter().all(|slots| slots.iter().all(Option::is_none)), "seed {seed}: undelivered lane results");
    drop((flush_tx, compact_tx));
    for t in threads {
        t.join().unwrap();
    }
    pins.clear();
    for (i, e) in engines.iter_mut().enumerate() {
        e.reap_run_pins();
        while e.dev.pending_writes() > 0 {
            e.dev.poll_writes();
        }
        check_storage(e, &[], &format!("seed {seed} engine {i} final"));
        crc_scan_runs(e, &format!("seed {seed} engine {i} final"));
        for (&key, (version, value)) in &models[i] {
            let k = format!("key{key:06}");
            assert_eq!(e.version(k.as_bytes()).unwrap(), Some(*version), "seed {seed} engine {i} key {key}");
            let want: &[u8] = value.as_deref().unwrap_or(&[]);
            assert_eq!(e.get(k.as_bytes()).unwrap().as_deref(), Some(want), "seed {seed} engine {i} key {key}");
        }
        if i % 2 == 1 {
            continue;
        }
        let scanned = e.scan(None, None).unwrap();
        assert_eq!(scanned.len(), models[i].len(), "seed {seed} engine {i} scan length");
        for ((k, v), (key, (_, value))) in scanned.iter().zip(&models[i]) {
            assert_eq!(k, format!("key{key:06}").as_bytes(), "seed {seed} engine {i} scan key order");
            assert_eq!(v.as_slice(), value.as_deref().unwrap_or(&[]), "seed {seed} engine {i} scan value {key}");
        }
    }
    let (verify_bytes_after, verify_failures_after) = run::run_build_verify_totals();
    assert_eq!(verify_failures_after, verify_failures_before, "seed {seed}: build verify failures");
    assert!(verify_bytes_after > verify_bytes_before, "seed {seed}: build verify never ran");
    stats.ops = op;
    stats
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 11) % n.max(1)
    }
}

fn stress_env() -> bool {
    isolated_env_test(&[
        ("VALS_VLOG_CO_COMPACT", Some("0")),
        ("VALS_MERGE_TIER_K", Some("6")),
        ("VALS_MERGE_TIER_MIN_K", Some("4")),
        ("VALS_MERGE_TIER_SIZE_RATIO", None),
        ("VALS_MERGE_FORCE_RUNS", Some("3")),
        ("VALS_MERGE_INPUT_CAP_BYTES", Some("98304")),
        ("VALS_MERGE_FREEZE_BYTES", None),
        ("VALS_RUN_BUILD_VERIFY", Some("1")),
        ("VALS_RUN_WRITE_VERSION", None),
        ("VALS_MAINT_DUTY_PCT", Some("0")),
        ("VALS_VLOG_GC", None),
        ("VALS_SHADOW_EST", None),
        ("VALS_SHADOW_FOLD", None),
    ])
}

#[test]
fn concurrent_in_place_forced_windows_never_corrupt_merge_outputs() {
    if stress_env() { return; }
    let inner_before = inner_window_merges_total();
    let mut total = StressStats::default();
    let runs = [
        (1u64, 4, true, 1, false),
        (7, 4, true, 6, false),
        (42, 8, false, 3, false),
        (1_000_003, 8, true, 6, false),
        (2, 4, true, 1, true),
        (8, 4, false, 2, true),
        (43, 8, true, 1, true),
        (1_000_004, 6, true, 3, true),
    ];
    for (seed, engines, deferred, merge_gate, bulky) in runs {
        let cfg = StressConfig {
            engines,
            ops: 20_000,
            keys: if bulky { 4_000 } else { 1_500 },
            region_blocks: if bulky { 12_288 } else { 6_144 },
            seg_blocks: 16,
            deferred,
            compact_lanes: 2,
            rogue: Rogue::None,
            merge_gate,
            bulky,
        };
        let s = run_stress(seed, &cfg);
        println!("seed {seed} engines={engines} deferred={deferred} bulky={bulky}: {s:?}");
        total.ops += s.ops;
        total.flushes += s.flushes;
        for (k, v) in s.merges {
            *total.merges.entry(k).or_default() += v;
        }
        total.middle_windows += s.middle_windows;
        total.concurrent_merges += s.concurrent_merges;
        total.pins += s.pins;
        total.pin_reaps += s.pin_reaps;
        total.dealloc_segments += s.dealloc_segments;
        total.late_writes_max = total.late_writes_max.max(s.late_writes_max);
        total.crc_scans += s.crc_scans;
        total.failed_puts += s.failed_puts;
    }
    let (verify_bytes, verify_failures) = run::run_build_verify_totals();
    let inner = inner_window_merges_total() - inner_before;
    println!("total: {total:?} inner_windows={inner} verify_bytes={verify_bytes} verify_failures={verify_failures}");
    assert!(inner >= total.middle_windows);
    assert_eq!(verify_failures, 0);
    assert!(total.middle_windows > 0, "no in-place middle window was exercised: {total:?}");
    assert!(total.merges.get("forced").copied().unwrap_or(0) > 0, "{total:?}");
    assert!(total.concurrent_merges > 0, "merges of different engines never overlapped: {total:?}");
    assert!(total.pin_reaps > 0 && total.dealloc_segments > 0 && total.late_writes_max > 0, "{total:?}");
    assert!(total.merges.get("pressure").copied().unwrap_or(0) > 0, "{total:?}");
}

#[test]
fn stress_harness_detects_freed_or_overwritten_in_flight_merge_outputs() {
    if stress_env() { return; }
    for rogue in [Rogue::FreeOutput, Rogue::ScribbleOutput] {
        let cfg = StressConfig {
            engines: 2,
            ops: 20_000,
            keys: 1_500,
            region_blocks: 6_144,
            seg_blocks: 16,
            deferred: true,
            compact_lanes: 2,
            rogue,
            merge_gate: 1,
            bulky: false,
        };
        let caught = std::panic::catch_unwind(|| run_stress(5, &cfg));
        let message = caught
            .err()
            .and_then(|p| p.downcast_ref::<String>().cloned().or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string())))
            .expect("a freed or overwritten merge output must be detected");
        println!("detected: {message}");
    }
}

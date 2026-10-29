use super::*;
use std::collections::{BTreeMap, BTreeSet};

const SECTOR: usize = 512;
const CRASH_KEYS: u32 = 32;

#[derive(Clone, Copy, Debug)]
struct Geometry {
    bs: usize,
    blocks: u64,
    seg_blocks: u64,
}

const GEOMETRIES: [Geometry; 2] = [
    Geometry { bs: 4096, blocks: 512, seg_blocks: 4 },
    Geometry { bs: 512, blocks: 4096, seg_blocks: 32 },
];

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    fn coin(&mut self) -> bool {
        self.next() & 1 == 1
    }
}

struct WriteBackCache {
    bs: usize,
    durable: Vec<u8>,
    live: Vec<u8>,
    pending: Vec<(u64, Vec<u8>)>,
}

impl WriteBackCache {
    fn new(bs: usize, blocks: u64) -> Self {
        let len = bs * blocks as usize;
        WriteBackCache { bs, durable: vec![0; len], live: vec![0; len], pending: Vec::new() }
    }

    fn write(&mut self, lba: u64, data: &[u8]) {
        let off = lba as usize * self.bs;
        self.live[off..off + data.len()].copy_from_slice(data);
        self.pending.push((lba, data.to_vec()));
    }

    fn flush(&mut self) {
        for (lba, data) in self.pending.drain(..) {
            let off = lba as usize * self.bs;
            self.durable[off..off + data.len()].copy_from_slice(&data);
        }
    }

    fn crash_keeping(&self, keep: impl Fn(u64) -> bool) -> Vec<u8> {
        let mut image = self.durable.clone();
        for (lba, data) in &self.pending {
            for (i, block) in data.chunks(self.bs).enumerate() {
                let at = *lba + i as u64;
                if keep(at) {
                    let off = at as usize * self.bs;
                    image[off..off + block.len()].copy_from_slice(block);
                }
            }
        }
        image
    }

    fn crash(&self, seed: u64, tear: bool) -> Vec<u8> {
        let mut rng = Rng(seed);
        let mut image = self.durable.clone();
        let pending_blocks: usize = self.pending.iter().map(|(_, d)| d.len().div_ceil(self.bs)).sum();
        let torn = (tear && self.bs > SECTOR && pending_blocks > 0)
            .then(|| rng.below(pending_blocks as u64) as usize);
        let mut n = 0usize;
        for (lba, data) in &self.pending {
            for (i, block) in data.chunks(self.bs).enumerate() {
                let off = (*lba as usize + i) * self.bs;
                if Some(n) == torn {
                    for (s, sector) in block.chunks(SECTOR).enumerate() {
                        if rng.coin() {
                            let at = off + s * SECTOR;
                            image[at..at + sector.len()].copy_from_slice(sector);
                        }
                    }
                } else if rng.coin() {
                    image[off..off + block.len()].copy_from_slice(block);
                }
                n += 1;
            }
        }
        image
    }
}

enum CrashEvent {
    Write(u64, Vec<u8>),
    Flush,
}

struct CrashSimState {
    cache: WriteBackCache,
    events: Vec<CrashEvent>,
}

#[derive(Clone)]
struct CrashSimDevice {
    bs: usize,
    blocks: u64,
    state: Arc<std::sync::Mutex<CrashSimState>>,
}

impl CrashSimDevice {
    fn new(bs: usize, blocks: u64) -> Self {
        CrashSimDevice {
            bs,
            blocks,
            state: Arc::new(std::sync::Mutex::new(CrashSimState {
                cache: WriteBackCache::new(bs, blocks),
                events: Vec::new(),
            })),
        }
    }

    fn events(&self) -> usize {
        self.state.lock().unwrap().events.len()
    }

    fn take_events(&self) -> Vec<CrashEvent> {
        std::mem::take(&mut self.state.lock().unwrap().events)
    }
}

impl BlockDevice for CrashSimDevice {
    fn block_size(&self) -> usize {
        self.bs
    }
    fn num_blocks(&self) -> u64 {
        self.blocks
    }
    fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
        check_sim_range(self.bs, self.blocks, lba, buf.len())?;
        let state = self.state.lock().unwrap();
        let off = lba as usize * self.bs;
        buf.copy_from_slice(&state.cache.live[off..off + buf.len()]);
        Ok(())
    }
    fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
        check_sim_range(self.bs, self.blocks, lba, buf.len())?;
        let mut state = self.state.lock().unwrap();
        state.cache.write(lba, buf);
        state.events.push(CrashEvent::Write(lba, buf.to_vec()));
        Ok(())
    }
    fn flush(&self) -> vals_spdk::Result<()> {
        let mut state = self.state.lock().unwrap();
        state.cache.flush();
        state.events.push(CrashEvent::Flush);
        Ok(())
    }
    fn sibling(&self) -> vals_spdk::Result<Self> {
        Ok(self.clone())
    }
}

fn check_sim_range(bs: usize, blocks: u64, lba: u64, len: usize) -> vals_spdk::Result<()> {
    if !len.is_multiple_of(bs) || lba.saturating_add((len / bs) as u64) > blocks {
        return Err(BlockError::Io(std::io::Error::other(format!(
            "crash sim: bad I/O at lba {lba} len {len}"
        ))));
    }
    Ok(())
}

#[derive(Clone, Debug)]
enum Effect {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    RangeDelete(Vec<u8>, Vec<u8>),
}

impl Effect {
    fn on(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        match self {
            Effect::Put(k, v) if k == key => Some(Some(v.clone())),
            Effect::Delete(k) if k == key => Some(None),
            Effect::RangeDelete(lo, hi) if lo.as_slice() <= key && key < hi.as_slice() => Some(None),
            _ => None,
        }
    }
}

struct Op {
    effect: Effect,
    start: usize,
}

struct Ack {
    ops: usize,
    end: usize,
}

struct Workload {
    e: Engine<CrashSimDevice>,
    dev: CrashSimDevice,
    rng: Rng,
    ver: u128,
    ops: Vec<Op>,
    acks: Vec<Ack>,
    maint: Vec<(&'static str, usize)>,
    rewrite_values: bool,
    geo: Geometry,
    cfg: Config,
}

fn crash_key(i: u32) -> Vec<u8> {
    format!("key-{i:02}").into_bytes()
}

fn crash_value(ver: u128, len: usize) -> Vec<u8> {
    ver.to_le_bytes().iter().copied().cycle().take(len).collect()
}

impl Workload {
    fn new(geo: Geometry, cfg: Config, seed: u64, rewrite_values: bool) -> Self {
        let dev = CrashSimDevice::new(geo.bs, geo.blocks);
        let e = Engine::create_prewiped(dev.clone(), cfg);
        Workload { e, dev, rng: Rng(seed), ver: 1, ops: Vec::new(), acks: Vec::new(), maint: Vec::new(), rewrite_values, geo, cfg }
    }

    fn record(&mut self, effect: Effect, start: usize) {
        self.ops.push(Op { effect, start });
    }

    fn sync(&mut self) {
        self.e.sync().unwrap();
        self.acks.push(Ack { ops: self.ops.len(), end: self.dev.events() });
    }

    fn maintain(&mut self, what: &'static str) {
        let start = self.dev.events();
        let opts = RunBuildOpts::default();
        match what {
            "flush" => {
                self.e.flush(opts).unwrap().expect("flush builds a run");
            }
            "merge" => self.merge(None),
            "gc-merge" => self.merge(Some(self.ver)),
            "log-gc" => {
                let stats = self.e.compact().unwrap();
                assert!(stats.segments_freed > 0, "log GC frees segments: {stats:?}");
            }
            "reopen" => {
                self.sync();
                let idle = CrashSimDevice::new(self.geo.bs, 64);
                drop(std::mem::replace(&mut self.e, Engine::create_prewiped(idle, self.cfg)));
                self.e = Engine::open(self.dev.clone(), self.cfg).unwrap();
            }
            _ => unreachable!(),
        }
        self.maint.push((what, start));
        self.check_live();
    }

    fn merge(&mut self, gc_collect_before: Option<u128>) {
        let runs = self.e.run_count();
        let plan = self.e.compact_plan(RunBuildOpts::default(), gc_collect_before).unwrap().expect("a merge plan");
        assert_eq!(plan.merged_inputs.len(), runs, "the merge folds every run");
        assert_eq!(plan.co_compacts(), self.rewrite_values, "the merge rewrites values only under co-compaction");
        let wdev = Arc::new(self.e.device().sibling().unwrap());
        let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).map_err(|(_, err)| err).unwrap();
        self.e.compact_commit(written).unwrap();
        assert_eq!(self.e.run_count(), 1);
    }

    fn step(&mut self, range_deletes: bool) {
        let start = self.dev.events();
        let ver = self.ver;
        self.ver += 1;
        let roll = self.rng.below(100);
        if roll < 12 {
            self.sync();
        } else if roll < 72 {
            let key = crash_key(self.rng.below(CRASH_KEYS as u64) as u32);
            let len = [9usize, 120, 900, 3000, 7000][self.rng.below(5) as usize];
            let value = crash_value(ver, len);
            self.e.put(&key, &value, ver).unwrap();
            self.record(Effect::Put(key, value), start);
        } else if roll < 94 || !range_deletes {
            let key = crash_key(self.rng.below(CRASH_KEYS as u64) as u32);
            self.e.put_kind(&key, &[], ver, true).unwrap();
            self.record(Effect::Delete(key), start);
        } else {
            let a = self.rng.below(CRASH_KEYS as u64) as u32;
            let b = (a + 1 + self.rng.below(6) as u32).min(CRASH_KEYS);
            let (lo, hi) = (crash_key(a), crash_key(b));
            self.e.apply_range_delete(Some(&lo), Some(&hi), ver).unwrap();
            self.record(Effect::RangeDelete(lo, hi), start);
        }
    }

    fn steps(&mut self, n: usize, range_deletes: bool) {
        for _ in 0..n {
            self.step(range_deletes);
        }
    }

    fn model_state(&self, ops: usize) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        let mut state = BTreeMap::new();
        for key in crash_universe() {
            let value = self.ops[..ops].iter().rev().find_map(|op| op.effect.on(&key)).flatten();
            state.insert(key, value);
        }
        state
    }

    fn check_live(&self) {
        for (key, want) in self.model_state(self.ops.len()) {
            let got = self.e.get(&key).unwrap().filter(|v| !v.is_empty());
            assert!(got == want, "live read of {}: {} instead of {}", String::from_utf8_lossy(&key), show(&got), show(&want));
        }
    }
}

fn crash_universe() -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = (0..CRASH_KEYS).map(crash_key).collect();
    keys.push(b"never-written".to_vec());
    keys
}

fn run_crash_workload(geo: Geometry, cfg: Config, seed: u64, rewrite_values: bool) -> Workload {
    let sorted = cfg.ordering == Ordering::Sorted;
    let mut w = Workload::new(geo, cfg, seed, rewrite_values);
    w.steps(40, false);
    for _ in 0..24 {
        let key = crash_key(0);
        let ver = w.ver;
        w.ver += 1;
        let start = w.dev.events();
        let value = crash_value(ver, 3000);
        w.e.put(&key, &value, ver).unwrap();
        w.record(Effect::Put(key, value), start);
    }
    w.sync();
    w.maintain("log-gc");
    w.steps(30, false);
    w.maintain("flush");
    for _ in 0..3 {
        w.steps(25, sorted);
        w.maintain("flush");
    }
    w.maintain("merge");
    w.maintain("reopen");
    for _ in 0..3 {
        w.steps(25, sorted);
        w.maintain("flush");
    }
    w.maintain("gc-merge");
    w.steps(8, sorted);
    w.maintain("reopen");
    w.steps(22, sorted);
    w.sync();
    w.steps(10, sorted);
    w.check_live();
    w
}

struct CrashCase {
    point: usize,
    seed: u64,
    tear: bool,
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn crash_cases(geo: Geometry, total: usize, seed: u64) -> Vec<CrashCase> {
    let mut rng = Rng(seed ^ 0x5eed_c0de);
    let tears: &[bool] = if geo.bs > SECTOR { &[false, true] } else { &[false] };
    let mut cases = Vec::new();
    for point in 0..=total {
        for &tear in tears {
            cases.push(CrashCase { point, seed: rng.next(), tear });
        }
    }
    cases
}

fn image_device(geo: Geometry, image: &[u8]) -> MemBlockDevice {
    let dev = MemBlockDevice::new(geo.bs, geo.blocks);
    dev.write_at(0, image).unwrap();
    dev
}

fn allowed_after_crash(w: &Workload, point: usize) -> BTreeMap<Vec<u8>, Vec<Option<Vec<u8>>>> {
    let synced = w.acks.iter().filter(|a| a.end <= point).map(|a| a.ops).max().unwrap_or(0);
    let mut allowed = BTreeMap::new();
    for (key, value) in w.model_state(synced) {
        let mut set = vec![value];
        for op in &w.ops[synced..] {
            if op.start >= point {
                break;
            }
            if let Some(v) = op.effect.on(&key) {
                if !set.contains(&v) {
                    set.push(v);
                }
            }
        }
        allowed.insert(key, set);
    }
    allowed
}

fn show(v: &Option<Vec<u8>>) -> String {
    match v {
        None => "absent".to_string(),
        Some(v) => {
            let mut ver = [0u8; 16];
            let n = v.len().min(16);
            ver[..n].copy_from_slice(&v[..n]);
            format!("v{}x{}", u128::from_le_bytes(ver), v.len())
        }
    }
}

fn read_all(e: &Engine<MemBlockDevice>) -> Result<BTreeMap<Vec<u8>, Option<Vec<u8>>>, String> {
    let mut out = BTreeMap::new();
    for key in crash_universe() {
        let got = e
            .get(&key)
            .map_err(|err| format!("get {:?}: {err:?}", String::from_utf8_lossy(&key)))?
            .filter(|v| !v.is_empty());
        out.insert(key, got);
    }
    Ok(out)
}

fn check_crash_image(w: &Workload, geo: Geometry, cfg: Config, case: &CrashCase, image: Vec<u8>) -> Result<(), String> {
    let dev = image_device(geo, &image);
    let shared = dev.share();
    let opened = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| Engine::open(dev, cfg)));
    let mut e = match opened {
        Ok(Ok(e)) => e,
        Ok(Err(err)) => return Err(format!("open failed: {err:?}")),
        Err(_) => return Err("open panicked".to_string()),
    };
    e.storage_audit_for_test().map_err(|err| format!("storage audit after open: {err}"))?;
    let got = read_all(&e)?;
    let allowed = allowed_after_crash(w, case.point);
    for (key, value) in &got {
        let ok = &allowed[key];
        if !ok.contains(value) {
            return Err(format!(
                "{} reads {} but only {} are allowed (synced value first)",
                String::from_utf8_lossy(key),
                show(value),
                ok.iter().map(show).collect::<Vec<_>>().join("/"),
            ));
        }
    }
    let probe_ver = w.ver + 1_000;
    let after = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<(), String> {
        e.put(b"post-crash", b"probe", probe_ver).map_err(|err| format!("post-crash put: {err:?}"))?;
        e.flush(RunBuildOpts::default()).map_err(|err| format!("post-crash flush: {err:?}"))?;
        e.sync().map_err(|err| format!("post-crash sync: {err:?}"))?;
        Ok(())
    }));
    match after {
        Ok(r) => r?,
        Err(_) => return Err("post-crash write panicked".to_string()),
    }
    drop(e);
    let reopened = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| Engine::open(shared, cfg)));
    let e = match reopened {
        Ok(Ok(e)) => e,
        Ok(Err(err)) => return Err(format!("reopen after recovery failed: {err:?}")),
        Err(_) => return Err("reopen after recovery panicked".to_string()),
    };
    e.storage_audit_for_test().map_err(|err| format!("storage audit after reopen: {err}"))?;
    if e.get(b"post-crash").map_err(|err| format!("{err:?}"))?.as_deref() != Some(&b"probe"[..]) {
        return Err("post-crash write lost on reopen".to_string());
    }
    let again = read_all(&e)?;
    if let Some((key, value)) = got.iter().find(|(k, v)| again[*k] != **v) {
        return Err(format!(
            "{} read {} after recovery but {} after a write, flush and reopen",
            String::from_utf8_lossy(key),
            show(value),
            show(&again[key]),
        ));
    }
    Ok(())
}

fn crash_point_harness(ordering: Ordering, rewrite_values: bool) {
    let first_seed = env_u64("VALS_CRASH_SEED", 0x0532);
    let seeds = env_u64("VALS_CRASH_SEEDS", 1);
    let mut failures = Vec::new();
    let mut checked = 0usize;
    for seed in first_seed..first_seed + seeds {
        for geo in GEOMETRIES {
            let cfg = Config { seg_blocks: geo.seg_blocks, reserved_blocks: 0, ordering };
            let w = run_crash_workload(geo, cfg, seed, rewrite_values);
            let events = w.dev.take_events();
            let flushes = events.iter().filter(|e| matches!(e, CrashEvent::Flush)).count();
            let cases = crash_cases(geo, events.len(), seed);
            println!(
                "crash harness: {ordering:?} {}-byte blocks, seed {seed}: {} ops, {} acked syncs, {} device writes, \
                 {flushes} flushes, maintenance at {:?}; {} crash cases",
                geo.bs,
                w.ops.len(),
                w.acks.len(),
                events.len() - flushes,
                w.maint,
                cases.len(),
            );
            checked += cases.len();
            let mut cache = WriteBackCache::new(geo.bs, geo.blocks);
            let mut applied = 0usize;
            for case in &cases {
                while applied < case.point {
                    match &events[applied] {
                        CrashEvent::Write(lba, data) => cache.write(*lba, data),
                        CrashEvent::Flush => cache.flush(),
                    }
                    applied += 1;
                }
                let image = cache.crash(case.seed, case.tear);
                if let Err(why) = check_crash_image(&w, geo, cfg, case, image) {
                    let phase = w
                        .maint
                        .iter()
                        .rev()
                        .find(|(_, at)| *at <= case.point)
                        .map_or("before any maintenance", |(m, _)| m);
                    failures.push(format!(
                        "{}-byte blocks, seed {seed}: crash before event {} of {} (after {phase}, tear {}, \
                         image seed {:#x}): {why}",
                        geo.bs,
                        case.point,
                        events.len(),
                        case.tear,
                        case.seed,
                    ));
                }
            }
        }
    }
    for f in failures.iter().take(12) {
        println!("{f}");
    }
    assert!(failures.is_empty(), "{} of {checked} crash cases failed; first: {}", failures.len(), failures[0]);
}

#[test]
fn write_back_cache_crash_keeps_flushed_and_a_subset_of_unflushed_blocks() {
    const BS: usize = 4096;
    let mut cache = WriteBackCache::new(BS, 8);
    cache.write(0, &[1u8; BS * 2]);
    cache.flush();
    cache.write(2, &[2u8; BS * 4]);
    cache.write(0, &[3u8; BS]);
    let mut seen: BTreeSet<(usize, u8)> = BTreeSet::new();
    let mut torn = 0;
    for seed in 0..64 {
        let tear = seed % 2 == 1;
        let image = cache.crash(seed, tear);
        assert!(image[BS..2 * BS].iter().all(|&b| b == 1), "flushed block survives");
        assert!(image[6 * BS..].iter().all(|&b| b == 0), "unwritten blocks stay zero");
        let mut torn_here = 0;
        for (i, block) in image[..6 * BS].chunks(BS).enumerate() {
            let allowed: &[u8] = match i {
                0 => &[1, 3],
                1 => &[1],
                _ => &[0, 2],
            };
            for sector in block.chunks(SECTOR) {
                assert!(sector.iter().all(|&b| b == sector[0]), "sectors are atomic");
                assert!(allowed.contains(&sector[0]), "block {i} holds {}", sector[0]);
            }
            if block.iter().all(|&b| b == block[0]) {
                seen.insert((i, block[0]));
            } else {
                torn_here += 1;
            }
        }
        assert!(torn_here <= usize::from(tear), "at most one torn block, and only when tearing");
        torn += torn_here;
    }
    for i in 2..6 {
        assert!(seen.contains(&(i, 0)) && seen.contains(&(i, 2)), "unflushed block {i} is sometimes kept, sometimes lost");
    }
    assert!(seen.contains(&(0, 1)) && seen.contains(&(0, 3)));
    assert!(torn > 0);
}

#[test]
fn crash_at_every_write_recovers_synced_state_sorted_with_value_rewrite() {
    if bottommost_co_compact_env() {
        return;
    }
    crash_point_harness(Ordering::Sorted, true);
}

#[test]
fn crash_at_every_write_recovers_synced_state_unsorted() {
    if compact_env(false, true) {
        return;
    }
    crash_point_harness(Ordering::Unsorted, false);
}

#[test]
fn reopen_skips_the_recovery_epoch_gap_without_counting_it_as_tail() {
    let cfg = Config { seg_blocks: 32, reserved_blocks: 0, ordering: Ordering::Sorted };
    let dev = MemBlockDevice::new(512, 4096);
    let mut e = Engine::create_prewiped(dev.share(), cfg);
    e.put(b"flushed", b"v", 1).unwrap();
    e.flush(RunBuildOpts::default()).unwrap().unwrap();
    for i in 0..12u32 {
        e.put(format!("tail-{i:02}").as_bytes(), &[7u8; 7000], 10 + i as u128).unwrap();
    }
    e.sync().unwrap();
    let tail = e.tail_epochs();
    assert!(tail >= 3, "the unflushed tail spans several segments: {tail}");
    let next = e.log.next_epoch();
    drop(e);
    let mut e = Engine::open(dev.share(), cfg).unwrap();
    assert_eq!(e.log.next_epoch(), next + super::super::recovery::RECOVERY_EPOCH_GAP);
    assert_eq!(e.tail_epochs(), tail);
    e.put(b"after", &[8u8; 7000], 100).unwrap();
    e.put(b"after-2", &[8u8; 7000], 101).unwrap();
    e.sync().unwrap();
    let grown = e.tail_epochs();
    assert!(grown > tail && grown < tail + 4, "{tail} -> {grown}");
    drop(e);
    let mut e = Engine::open(dev.share(), cfg).unwrap();
    assert_eq!(e.tail_epochs(), grown, "an earlier reopen's gap is not tail either");
    e.flush(RunBuildOpts::default()).unwrap().unwrap();
    assert_eq!(e.tail_epochs(), 0);
    assert_eq!(e.get(b"tail-11").unwrap(), Some(vec![7u8; 7000]));
}

#[test]
fn a_crash_that_loses_a_reopened_logs_first_header_does_not_reuse_its_epoch() {
    let cfg = Config { seg_blocks: 16, reserved_blocks: 0, ordering: Ordering::Sorted };
    let dev = CrashSimDevice::new(512, 256);
    let mut e = Engine::create_prewiped(dev.clone(), cfg);
    e.put(b"anchor", b"durable", 1).unwrap();
    e.sync().unwrap();
    drop(e);
    let mut e = Engine::open(dev.clone(), cfg).unwrap();
    for i in 0..40u32 {
        e.put(format!("old-{i:02}").as_bytes(), &[7; 100], 100 + i as u128).unwrap();
    }
    e.sync_prepare().unwrap();
    let (header, _) = e.alloc.segment_range(e.log.open_segment().unwrap());
    drop(e);
    let image = dev.state.lock().unwrap().cache.crash_keeping(|lba| lba != header);
    let geo = Geometry { bs: 512, blocks: 256, seg_blocks: 16 };
    let crashed = image_device(geo, &image);
    let mut e = Engine::open(crashed.share(), cfg).unwrap();
    assert_eq!(e.get(b"anchor").unwrap().as_deref(), Some(&b"durable"[..]));
    let seen = e.get(b"old-20").unwrap();
    e.put(b"new", b"v", 500).unwrap();
    e.sync().unwrap();
    drop(e);
    let e = Engine::open(crashed.share(), cfg).unwrap();
    assert_eq!(e.get(b"old-20").unwrap(), seen, "recovery writes must not expose records recovery did not see");
    assert_eq!(e.get(b"new").unwrap().as_deref(), Some(&b"v"[..]));
}

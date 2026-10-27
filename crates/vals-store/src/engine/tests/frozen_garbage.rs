use super::*;

#[derive(Clone, Copy, Debug)]
struct GateCfg {
    keys: u64,
    value_len: usize,
    cap: u64,
    flush_bytes: u64,
    ops: u64,
    delete_pct: u64,
    hot_pct: u64,
    gc_grace_ops: Option<u128>,
    blocks: u64,
    seg_blocks: u64,
    sample_every: u64,
}

impl GateCfg {
    fn env_or(name: &str, default: u64) -> u64 {
        std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
    }

    fn from_env(d: GateCfg) -> GateCfg {
        GateCfg {
            keys: Self::env_or("GATE_KEYS", d.keys),
            value_len: Self::env_or("GATE_VALUE_LEN", d.value_len as u64) as usize,
            cap: Self::env_or("GATE_CAP", d.cap),
            flush_bytes: Self::env_or("GATE_FLUSH_BYTES", d.flush_bytes),
            ops: Self::env_or("GATE_OPS", d.ops),
            delete_pct: Self::env_or("GATE_DELETE_PCT", d.delete_pct),
            hot_pct: Self::env_or("GATE_HOT_PCT", d.hot_pct),
            gc_grace_ops: match std::env::var("GATE_GC_GRACE_OPS").ok().and_then(|v| v.parse::<u128>().ok()) {
                Some(0) => None,
                Some(g) => Some(g),
                None => d.gc_grace_ops,
            },
            blocks: Self::env_or("GATE_BLOCKS", d.blocks),
            seg_blocks: Self::env_or("GATE_SEG_BLOCKS", d.seg_blocks),
            sample_every: Self::env_or("GATE_SAMPLE_EVERY", d.sample_every),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum KeyState {
    Absent,
    Live(u128),
    Deleted(u128),
}

#[derive(Clone, Copy, Debug)]
struct GatePlan {
    kind: MergeKind,
    input_bytes: u64,
    read_bytes: u64,
    output_bytes: u64,
    freed_bytes: u64,
    frozen_inputs: usize,
    inputs: usize,
    rewrite: u8,
    paced: bool,
}

#[derive(Default, Debug)]
struct GateAudit {
    run_entries: u64,
    run_tombstones: u64,
    collectible_tombstones: u64,
    shadowed_entries: u64,
    distinct_keys: u64,
    stale: u64,
    resurrected: u64,
    lost: u64,
}

struct Gate {
    e: Engine<MemBlockDevice>,
    cfg: GateCfg,
    ver: u128,
    rng: u64,
    state: Vec<KeyState>,
    live: u64,
    plans: Vec<GatePlan>,
    sheds: u64,
    rejections: u64,
    headroom_skips: u64,
    conflicts: u64,
    flush_bytes_written: u64,
    max_amp: f64,
    max_amp_frozen: f64,
    min_frozen: usize,
    frozen_at_load: usize,
    max_runs: usize,
}

impl Gate {
    fn new(cfg: GateCfg) -> Gate {
        let dev = MemBlockDevice::new(4096, cfg.blocks);
        let mut e = Engine::create_prewiped(
            dev,
            Config { seg_blocks: cfg.seg_blocks, reserved_blocks: 0, ordering: Ordering::Unsorted },
        );
        e.set_maint_reserve_enabled(true);
        assert_eq!(e.merge_policy.size_cap, cfg.cap, "VALS_MERGE_INPUT_CAP_BYTES must match the gate cap");
        Gate {
            e,
            cfg,
            ver: 0,
            rng: 0x9e37_79b9_7f4a_7c15,
            state: vec![KeyState::Absent; cfg.keys as usize],
            live: 0,
            plans: Vec::new(),
            sheds: 0,
            rejections: 0,
            headroom_skips: 0,
            conflicts: 0,
            flush_bytes_written: 0,
            max_amp: 0.0,
            max_amp_frozen: 0.0,
            min_frozen: usize::MAX,
            frozen_at_load: 0,
            max_runs: 0,
        }
    }

    fn key(id: u64) -> Vec<u8> {
        format!("{:016x}/app.rec/{id:010}", crate::run::splitmix64(id)).into_bytes()
    }

    fn key_len() -> usize {
        Self::key(0).len()
    }

    fn value(&self, ver: u128) -> Vec<u8> {
        let mut v = vec![0x5au8; self.cfg.value_len.max(16)];
        v[..16].copy_from_slice(&ver.to_le_bytes());
        v
    }

    fn next_rand(&mut self) -> u64 {
        self.rng = self.rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.rng >> 11
    }

    fn pick(&mut self) -> u64 {
        let hot = (self.cfg.keys / 10).max(1);
        if self.next_rand() % 100 < self.cfg.hot_pct {
            crate::run::splitmix64(self.next_rand() % hot) % self.cfg.keys
        } else {
            self.next_rand() % self.cfg.keys
        }
    }

    fn gc_cutoff(&self) -> Option<u128> {
        self.cfg.gc_grace_ops.map(|g| self.ver.saturating_sub(g).max(1))
    }

    fn apply(&mut self, id: u64, delete: bool) {
        for _ in 0..16 {
            if !self.e.admission_shed() {
                break;
            }
            self.sheds += 1;
            self.maintain();
        }
        self.ver += 1;
        let key = Self::key(id);
        let applied = if delete {
            self.e.put_kind(&key, b"", self.ver, true)
        } else {
            let value = self.value(self.ver);
            self.e.put(&key, &value, self.ver)
        };
        if applied.is_err() {
            self.rejections += 1;
            return;
        }
        let before = self.state[id as usize];
        self.state[id as usize] = if delete { KeyState::Deleted(self.ver) } else { KeyState::Live(self.ver) };
        match (matches!(before, KeyState::Live(_)), delete) {
            (true, true) => self.live -= 1,
            (false, false) => self.live += 1,
            _ => {}
        }
    }

    fn exec(&mut self, plan: CompactPlan<MemBlockDevice>) {
        let policy = self.e.merge_policy;
        let mut record = GatePlan {
            kind: plan.kind(),
            input_bytes: plan.input_bytes(),
            read_bytes: plan.read_bytes(),
            output_bytes: 0,
            freed_bytes: 0,
            frozen_inputs: plan.merged_inputs.iter().filter(|m| policy.frozen(m.handle.len)).count(),
            inputs: plan.merged_inputs.len(),
            rewrite: match &plan.vlog_rewrite {
                VlogRewrite::None => b'n',
                VlogRewrite::All => b'a',
                VlogRewrite::Victims(_) => b'v',
            },
            paced: plan.paced,
        };
        let wdev = Arc::new(self.e.device().sibling().unwrap());
        let written = match vals_rt::block_on(compact_and_write(plan, wdev), || {}) {
            Ok(w) => w,
            Err((segs, err)) => panic!("gate merge write failed: {err:?} ({} segs)", segs.len()),
        };
        match self.e.compact_commit(written) {
            Ok(stats) => {
                record.output_bytes = stats.output_bytes;
                record.freed_bytes = stats.segments_freed as u64 * self.seg_bytes();
                self.plans.push(record);
            }
            Err(EngineError::CompactionConflict) => self.conflicts += 1,
            Err(err) => panic!("gate merge commit failed: {err:?}"),
        }
    }

    fn maintain(&mut self) {
        for _ in 0..16 {
            let e = &self.e;
            let pressure = e.at_maintenance_floor()
                || (e.free_fraction() <= 0.50 && e.lsm_dead_fraction_gate().is_some_and(|d| d >= 0.10));
            let flush = e.memtable_bytes() >= self.cfg.flush_bytes || (pressure && !e.index.is_empty());
            if flush {
                match self.e.flush_plan(RunBuildOpts::default()) {
                    Ok(Some(plan)) => {
                        let wdev = self.e.device().sibling().unwrap();
                        let written = vals_rt::block_on(build_and_write_flush(plan, &wdev), || {}).unwrap();
                        let meta = self.e.flush_commit(written).unwrap();
                        self.flush_bytes_written += meta.handle.len;
                    }
                    Ok(None) => {}
                    Err(EngineError::InsufficientHeadroom { .. }) => self.headroom_skips += 1,
                    Err(err) => panic!("gate flush failed: {err:?}"),
                }
            }
            let gc = self.gc_cutoff();
            let e = &self.e;
            let compact = e.recurring_merge_selectable() || (pressure && e.run_count() >= 1);
            let lone = !compact
                && e.run_count() == 1
                && gc.is_some_and(|c| e.runs_tombstone_min_version().is_some_and(|m| m < c));
            if !compact && !lone {
                return;
            }
            let opts = RunBuildOpts::default();
            let planned = if lone {
                self.e.compact_plan_lone_gc(opts, gc)
            } else if pressure {
                self.e.compact_plan_pressure(opts, gc)
            } else {
                self.e.compact_plan(opts, gc)
            };
            match planned {
                Ok(Some(plan)) => self.exec(plan),
                Ok(None) => return,
                Err(EngineError::InsufficientHeadroom { .. }) => {
                    self.headroom_skips += 1;
                    return;
                }
                Err(err) => panic!("gate plan failed: {err:?}"),
            }
        }
    }

    fn seg_bytes(&self) -> u64 {
        self.e.alloc.blocks_per_segment() * self.e.block_size as u64
    }

    fn sample(&mut self, op: u64, tag: &str) {
        let used = self.e.used_segments() * self.seg_bytes();
        let logical = self.live * (Self::key_len() + self.cfg.value_len) as u64;
        let amp = used as f64 / logical.max(1) as f64;
        let index: u64 = self.e.runs.iter().map(|or| or.meta.handle.len).sum();
        let frozen = self.e.frozen_run_count();
        self.max_amp = self.max_amp.max(amp);
        if tag == "ops" {
            self.max_amp_frozen = self.max_amp_frozen.max(amp);
            self.min_frozen = self.min_frozen.min(frozen);
        }
        if tag == "load" {
            self.frozen_at_load = frozen;
        }
        self.max_runs = self.max_runs.max(self.e.run_count());
        println!(
            "gate {tag} op={op} used={:.1}MiB live={:.1}MiB amp={amp:.3} runs={} frozen={frozen} index={:.1}MiB \
             free={:.3} dead_gate={:.3} shadow_est={:.1}MiB plans={}",
            used as f64 / 1048576.0,
            logical as f64 / 1048576.0,
            self.e.run_count(),
            index as f64 / 1048576.0,
            self.e.free_fraction(),
            self.e.lsm_dead_fraction_gate().unwrap_or(-1.0),
            self.e.shadowed_bytes_estimate_cached() as f64 / 1048576.0,
            self.plans.len(),
        );
        println!(
            "gate folds={} disarms={} backoff={} cooldown={}",
            self.e.shadow_folds(),
            self.e.shadow_fold_disarms(),
            self.e.shadow.backoff_shift,
            self.e.log.next_epoch() < self.e.shadow.cooldown_until_epoch,
        );
    }

    fn audit(&self) -> GateAudit {
        let mut a = GateAudit::default();
        let cutoff = self.gc_cutoff();
        let mut versions: std::collections::HashMap<Vec<u8>, u32> = std::collections::HashMap::new();
        for or in &self.e.runs {
            let mut scan = or.run.block_scan(None, None);
            while let Some(entry) = scan.next(&self.e.alloc).unwrap() {
                a.run_entries += 1;
                if entry.tombstone {
                    a.run_tombstones += 1;
                    if cutoff.is_some_and(|c| entry.version < c) {
                        a.collectible_tombstones += 1;
                    }
                }
                *versions.entry(entry.key).or_insert(0) += 1;
            }
        }
        a.distinct_keys = versions.len() as u64;
        a.shadowed_entries = a.run_entries - a.distinct_keys;
        for id in 0..self.cfg.keys {
            let key = Self::key(id);
            let got = self.e.version(&key).unwrap();
            match self.state[id as usize] {
                KeyState::Absent => {
                    if got.is_some() {
                        a.resurrected += 1;
                    }
                }
                KeyState::Deleted(v) => match got {
                    None => {}
                    Some(g) if g == v => {}
                    Some(_) => a.resurrected += 1,
                },
                KeyState::Live(v) => match got {
                    None => a.lost += 1,
                    Some(g) if g != v => a.stale += 1,
                    Some(_) => {
                        let value = self.e.get(&key).unwrap().unwrap_or_default();
                        if value.len() < 16 || value[..16] != v.to_le_bytes() {
                            a.stale += 1;
                        }
                    }
                },
            }
        }
        a
    }

    fn report(&self, audit: &GateAudit) {
        println!("gate cfg {:?}", self.cfg);
        println!(
            "gate end: max_amp={:.3} max_amp_after_load={:.3} frozen_at_load={} min_frozen={} max_runs={} sheds={} \
             rejections={} headroom_skips={} conflicts={} flushed={:.1}MiB fold_dropped={} fold_collected={}",
            self.max_amp,
            self.max_amp_frozen,
            self.frozen_at_load,
            self.min_frozen,
            self.max_runs,
            self.sheds,
            self.rejections,
            self.headroom_skips,
            self.conflicts,
            self.flush_bytes_written as f64 / 1048576.0,
            crate::attribution::SHADOW_FOLD_DROPPED.get(),
            crate::attribution::SHADOW_FOLD_COLLECTED.get(),
        );
        println!("gate audit: {audit:?}");
        for kind in MergeKind::ALL {
            let of: Vec<&GatePlan> = self.plans.iter().filter(|p| p.kind == kind).collect();
            if of.is_empty() {
                continue;
            }
            let sum = |f: fn(&GatePlan) -> u64| of.iter().map(|p| f(p)).sum::<u64>();
            println!(
                "gate merges {}: n={} input={:.1}MiB max_input={:.2}MiB read={:.1}MiB max_read={:.2}MiB \
                 output={:.1}MiB freed={:.1}MiB frozen_inputs={} paced={}",
                kind.label(),
                of.len(),
                sum(|p| p.input_bytes) as f64 / 1048576.0,
                of.iter().map(|p| p.input_bytes).max().unwrap_or(0) as f64 / 1048576.0,
                sum(|p| p.read_bytes) as f64 / 1048576.0,
                of.iter().map(|p| p.read_bytes).max().unwrap_or(0) as f64 / 1048576.0,
                sum(|p| p.output_bytes) as f64 / 1048576.0,
                sum(|p| p.freed_bytes) as f64 / 1048576.0,
                of.iter().map(|p| p.frozen_inputs).sum::<usize>(),
                of.iter().filter(|p| p.paced).count(),
            );
            for mode in [b'n', b'a', b'v'] {
                let m: Vec<&&GatePlan> = of.iter().filter(|p| p.rewrite == mode).collect();
                if !m.is_empty() {
                    println!(
                        "gate merges {} rewrite={}: n={} inputs={} output={:.1}MiB freed={:.1}MiB",
                        kind.label(), mode as char, m.len(),
                        m.iter().map(|p| p.inputs).sum::<usize>(),
                        m.iter().map(|p| p.output_bytes).sum::<u64>() as f64 / 1048576.0,
                        m.iter().map(|p| p.freed_bytes).sum::<u64>() as f64 / 1048576.0,
                    );
                }
            }
        }
    }

    fn run(cfg: GateCfg) -> (Gate, GateAudit) {
        let mut g = Gate::new(cfg);
        for id in 0..cfg.keys {
            g.apply(id, false);
            if id % 64 == 63 {
                g.maintain();
            }
        }
        g.maintain();
        g.sample(0, "load");
        for op in 0..cfg.ops {
            if g.rejections > 1_000 {
                println!("gate WEDGED at op={op}: {} rejected writes", g.rejections);
                break;
            }
            let id = g.pick();
            let delete = g.next_rand() % 100 < cfg.delete_pct
                && matches!(g.state[id as usize], KeyState::Live(_));
            g.apply(id, delete);
            if op % 64 == 63 {
                g.maintain();
            }
            if (op + 1) % cfg.sample_every == 0 {
                g.sample(op + 1, "ops");
            }
        }
        let audit = g.audit();
        g.report(&audit);
        (g, audit)
    }
}

const GATE_L1: GateCfg = GateCfg {
    keys: 400_000,
    value_len: 160,
    cap: 512 * 1024,
    flush_bytes: 96 * 1024,
    ops: 2_000_000,
    delete_pct: 20,
    hot_pct: 0,
    gc_grace_ops: Some(100_000),
    blocks: 65_536,
    seg_blocks: 8,
    sample_every: 100_000,
};

fn gate_env(cap: u64) -> bool {
    let cap = cap.to_string();
    isolated_env_test(&[
        ("VALS_MERGE_INPUT_CAP_BYTES", Some(cap.as_str())),
        ("VALS_MERGE_FREEZE_BYTES", None),
        ("VALS_VLOG_CO_COMPACT", Some("0")),
        ("VALS_MERGE_TIER_K", None),
        ("VALS_MERGE_TIER_MIN_K", None),
        ("VALS_MERGE_FORCE_RUNS", None),
        ("VALS_VLOG_GC", None),
        ("VALS_SHADOW_EST", None),
        ("VALS_SHADOW_FOLD", None),
        ("VALS_SHADOW_FOLD_MIN_GAIN_SEGS", Some("1")),
    ])
}

#[test]
#[ignore]
fn frozen_garbage_gate_l1() {
    let cfg = GateCfg::from_env(GATE_L1);
    if gate_env(cfg.cap) {
        return;
    }
    let (g, audit) = Gate::run(cfg);
    assert_eq!(audit.resurrected + audit.stale + audit.lost, 0, "{audit:?}");
    assert!(g.frozen_at_load >= 50, "the gate needs >= 50 frozen runs ({})", g.frozen_at_load);
}

#[test]
fn capped_shadow_fold_drops_shadowed_entries_and_never_resurrects() {
    if co_compact_env(false) {
        return;
    }
    let dev = MemBlockDevice::new(BS, 10240);
    let mut e = Engine::create_prewiped(
        dev,
        Config { seg_blocks: 32, reserved_blocks: 0, ordering: Ordering::Unsorted },
    );
    let key = |i: usize| format!("k{i:04}").into_bytes();
    for i in 0..2000 {
        e.put(&key(i), &[1u8; 256], 1_000 + i as u128).unwrap();
    }
    run_flush(&mut e, RunBuildOpts::default()).unwrap();
    for i in 0..2000 {
        if i < 200 {
            e.put_kind(&key(i), b"", 3_000 + i as u128, true).unwrap();
        } else {
            e.put(&key(i), &[2u8; 256], 3_000 + i as u128).unwrap();
        }
    }
    run_flush(&mut e, RunBuildOpts::default()).unwrap();
    for i in 0..50 {
        e.put(&key(i), &[9u8; 256], 2_000 + i as u128).unwrap();
    }
    run_flush(&mut e, RunBuildOpts::default()).unwrap();
    for i in 200..2000 {
        e.put(&key(i), &[4u8; 256], 5_000 + i as u128).unwrap();
    }
    run_flush(&mut e, RunBuildOpts::default()).unwrap();
    assert_eq!(e.run_count(), 4);
    assert!(e.free_fraction() <= SHADOW_EST_FREE_GATE);
    e.merge_policy.size_cap = 1;

    let check = |e: &Engine<MemBlockDevice>, ctx: &str| {
        for i in 0..2000 {
            let got = e.version(&key(i)).unwrap();
            match i {
                0..50 => assert_eq!(got, Some(3_000 + i as u128), "{ctx}: k{i} tombstone kept over an older copy"),
                50..200 => assert!(
                    got.is_none() || got == Some(3_000 + i as u128),
                    "{ctx}: k{i} resurrected at {got:?}"
                ),
                _ => {
                    assert_eq!(got, Some(5_000 + i as u128), "{ctx}: k{i}");
                    assert_eq!(e.get(&key(i)).unwrap().as_deref(), Some(&[4u8; 256][..]), "{ctx}: k{i}");
                }
            }
        }
    };
    check(&e, "before");
    let run_entries = |e: &Engine<MemBlockDevice>| e.runs.iter().map(|or| or.meta.entry_count).sum::<u64>();
    let entries_before = run_entries(&e);
    let mut folds = 0;
    for round in 0..8 {
        e.shadow.cooldown_until_epoch = 0;
        let plan = match e.compact_plan_pressure(RunBuildOpts::default(), Some(u128::MAX)).unwrap() {
            Some(plan) if plan.kind() == MergeKind::ShadowFold => plan,
            Some(plan) => {
                let reserved = plan.segments();
                e.free_compact_segs(&reserved);
                break;
            }
            None => break,
        };
        assert_eq!(plan.merged_inputs.len(), 1, "a 1-byte cap folds one run at a time");
        assert!(plan.read_bytes() > plan.input_bytes(), "newer runs are streamed as shadow sources");
        let wdev = Arc::new(e.device().sibling().unwrap());
        let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
        e.compact_commit(written).unwrap();
        folds += 1;
        check(&e, &format!("after fold {round}"));
        e.storage_audit_for_test().unwrap();
    }
    assert!(folds >= 2, "both shadowed runs fold ({folds})");
    assert!(
        run_entries(&e) + 2000 + 1800 <= entries_before,
        "shadowed entries are dropped: {} -> {}",
        entries_before,
        run_entries(&e)
    );
    let collected = (50..200).filter(|&i| e.version(&key(i)).unwrap().is_none()).count();
    assert_eq!(collected, 150, "tombstones with no other copy are collected once the older value is gone");
}

struct AffineDevice {
    inner: MemBlockDevice,
    owner: std::sync::Mutex<Option<std::thread::ThreadId>>,
}

impl AffineDevice {
    fn check(&self) {
        let me = std::thread::current().id();
        let mut owner = self.owner.lock().unwrap();
        match *owner {
            None => *owner = Some(me),
            Some(bound) => assert_eq!(bound, me, "device used outside its owner thread"),
        }
    }
}

impl BlockDevice for AffineDevice {
    fn block_size(&self) -> usize {
        self.inner.block_size()
    }
    fn num_blocks(&self) -> u64 {
        self.inner.num_blocks()
    }
    fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
        self.check();
        self.inner.read_at(lba, buf)
    }
    fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
        self.check();
        self.inner.write_at(lba, buf)
    }
    fn flush(&self) -> vals_spdk::Result<()> {
        self.check();
        self.inner.flush()
    }
    fn sibling(&self) -> vals_spdk::Result<Self> {
        Ok(AffineDevice { inner: self.inner.sibling()?, owner: std::sync::Mutex::new(None) })
    }
}

#[test]
fn shadow_fold_sources_read_through_the_maintenance_device() {
    if co_compact_env(false) {
        return;
    }
    let dev = AffineDevice { inner: MemBlockDevice::new(BS, 10240), owner: std::sync::Mutex::new(None) };
    let mut e = Engine::create_prewiped(
        dev,
        Config { seg_blocks: 32, reserved_blocks: 0, ordering: Ordering::Unsorted },
    );
    let key = |i: usize| format!("k{i:04}").into_bytes();
    for round in 0..2u128 {
        for i in 0..2000 {
            e.put(&key(i), &[round as u8; 256], round * 10_000 + i as u128).unwrap();
        }
        run_flush(&mut e, RunBuildOpts::default()).unwrap();
    }
    e.merge_policy.size_cap = 1;
    let plan = e.compact_plan_pressure(RunBuildOpts::default(), None).unwrap().expect("a shadow fold");
    assert_eq!(plan.kind(), MergeKind::ShadowFold);
    assert!(plan.read_bytes() > plan.input_bytes());
    assert!(plan.paced);
    let charged_before = crate::pace::maint_pace_totals().0;
    let wdev = Arc::new(e.device().sibling().unwrap());
    let written = std::thread::spawn(move || {
        vals_rt::block_on(compact_and_write(plan, wdev), || {}).map_err(|(_, err)| format!("{err:?}"))
    })
    .join()
    .expect("the fold must not touch the serving device off its thread")
    .unwrap();
    assert!(
        crate::pace::maint_pace_totals().0 > charged_before,
        "a fold that keeps nothing still charges its source and input work to the pacer"
    );
    e.compact_commit(written).unwrap();
    for i in 0..2000 {
        assert_eq!(e.get(&key(i)).unwrap().as_deref(), Some(&[1u8; 256][..]));
    }
    assert_eq!(e.runs[0].meta.entry_count, 0, "every base entry was shadowed");
}

#[test]
fn shadow_fold_collects_a_tombstone_only_run_once_older_copies_are_gone() {
    if co_compact_env(false) {
        return;
    }
    let dev = MemBlockDevice::new(BS, 6144);
    let mut e = Engine::create_prewiped(
        dev,
        Config { seg_blocks: 32, reserved_blocks: 0, ordering: Ordering::Unsorted },
    );
    let key = |i: usize| format!("k{i:04}").into_bytes();
    for i in 0..2000 {
        e.put(&key(i), &[1u8; 256], 1_000 + i as u128).unwrap();
    }
    run_flush(&mut e, RunBuildOpts::default()).unwrap();
    for i in 0..2000 {
        e.put_kind(&key(i), b"", 3_000 + i as u128, true).unwrap();
    }
    run_flush(&mut e, RunBuildOpts::default()).unwrap();
    assert!(e.free_fraction() <= SHADOW_EST_FREE_GATE, "free {}", e.free_fraction());
    e.merge_policy.size_cap = 1;
    let entries = |e: &Engine<MemBlockDevice>| e.runs.iter().map(|or| or.meta.entry_count).sum::<u64>();
    for round in 0..6 {
        if entries(&e) == 0 {
            break;
        }
        e.shadow.cooldown_until_epoch = 0;
        let Some(plan) = e.compact_plan_pressure(RunBuildOpts::default(), Some(u128::MAX)).unwrap() else {
            break;
        };
        assert_eq!(plan.kind(), MergeKind::ShadowFold, "round {round}");
        let wdev = Arc::new(e.device().sibling().unwrap());
        let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
        e.compact_commit(written).unwrap();
        for i in 0..2000 {
            let got = e.version(&key(i)).unwrap();
            assert!(got.is_none() || got == Some(3_000 + i as u128), "round {round}: k{i} resurrected at {got:?}");
        }
    }
    assert_eq!(entries(&e), 0, "the deleted values and then the tombstone-only run are both reclaimed");
    e.storage_audit_for_test().unwrap();
}

#[test]
fn shadow_probe_bounds_each_turn_over_many_overlapping_sources() {
    let dev = MemBlockDevice::new(BS, 16384);
    let mut e = Engine::create_prewiped(
        dev,
        Config { seg_blocks: 32, reserved_blocks: 0, ordering: Ordering::Unsorted },
    );
    let key = |i: usize| format!("k{i:04}").into_bytes();
    for round in 0..60u128 {
        for i in 0..20 {
            e.put(&key(i), &[round as u8; 8], round * 100 + i as u128 + 1).unwrap();
        }
        run_flush(&mut e, RunBuildOpts::default()).unwrap();
    }
    let sources: Vec<_> = e.runs[1..].iter().map(|or| (or.run.stream_source(&e.alloc), true)).collect();
    let mut probe = ShadowProbe::new(sources, &e.dev).unwrap();
    let mut turns = 0;
    for i in 0..20 {
        let entry = e.runs[0].run.lookup(&e.alloc, &key(i)).unwrap().unwrap();
        let verdict = loop {
            let mut budget = 8;
            let step = probe.step(&entry, true, None, &mut budget).unwrap();
            turns += 1;
            if let Some(verdict) = step {
                break verdict;
            }
            assert_eq!(budget, 0, "a pending turn spends its whole budget");
        };
        assert_eq!(verdict, ShadowVerdict::Shadowed, "k{i}");
    }
    assert!(turns >= 59 * 20 / 8, "59 overlapping sources per key are consumed in bounded turns ({turns})");
}

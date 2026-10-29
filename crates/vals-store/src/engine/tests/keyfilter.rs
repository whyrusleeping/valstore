use super::*;
use super::super::keyfilter::KeyFilterKnockouts;
use crate::keyfilter::{KeyFilter, KeyFilterParams, SubFilter};
use crate::run::BloomKey;

fn small_params(min_keys: u64) -> KeyFilterParams {
    KeyFilterParams {
        enabled: true,
        min_keys,
        bits_per_key: crate::keyfilter::DEFAULT_BITS_PER_KEY,
        max_bits_per_key: crate::keyfilter::DEFAULT_MAX_BITS_PER_KEY,
        growth_pct: crate::keyfilter::DEFAULT_GROWTH_PCT,
        rebuild: false,
        step_pct: 20,
        rebuild_subs: 4,
        rebuild_concurrency: 1,
        rebuild_urgent_fpr_ppm: 25_000,
        rebuild_urgent_extra: 1,
    }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        crate::run::splitmix64(self.0)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

fn full_lane() -> bool {
    std::env::var_os("VALS_TEST_FULL").is_some_and(|v| v != "0")
}

fn kf_key(i: u64) -> Vec<u8> {
    format!("kf-{i:08}").into_bytes()
}

fn absent_key(i: u64) -> Vec<u8> {
    format!("kf-{i:08}-absent").into_bytes()
}

fn kf_engine() -> Engine<MemBlockDevice> {
    let mut e = Engine::create_prewiped(
        MemBlockDevice::new(512, 16384),
        Config {
            seg_blocks: 16,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    e.key_filter.set_params(small_params(1_000));
    e
}

fn run_keys<D: BlockDevice>(e: &Engine<D>, idx: usize) -> Vec<Vec<u8>> {
    let mut scan = e.runs[idx].run.block_scan(None, None);
    let mut out = Vec::new();
    while let Some(entry) = scan.next(&e.alloc).unwrap() {
        out.push(entry.key);
    }
    out
}

fn coverage_violations<D: BlockDevice>(e: &Engine<D>) -> usize {
    let mut missing = 0;
    for idx in 0..e.runs.len() {
        if !e.runs[idx].covered && !e.runs[idx].rebuild_covered {
            continue;
        }
        for key in run_keys(e, idx) {
            if e.runs[idx].covered && !e.key_filter.may_contain(&key) {
                missing += 1;
            }
            if e.runs[idx].rebuild_covered && !e.key_filter.rebuild_filter().unwrap().may_contain(&key) {
                missing += 1;
            }
        }
    }
    missing
}

fn reference_version<D: BlockDevice>(e: &Engine<D>, key: &[u8]) -> Option<u128> {
    let mut best = e.index.get(key).map(|entry| entry.version);
    for or in &e.runs {
        if let Some(entry) = or.run.lookup(&e.alloc, key).unwrap() {
            best = best.max(Some(entry.version));
        }
    }
    best
}

fn filtered_version<D: BlockDevice + AsyncRead>(e: &Engine<D>, key: &[u8]) -> Option<u128> {
    let probe = e.probe_version(key);
    vals_rt::block_on(Engine::version_from_probe(probe, e.device()), || {}).unwrap()
}

fn assert_probes_match<D: BlockDevice + AsyncRead>(e: &Engine<D>, keys: &[Vec<u8>], ctx: &str) {
    for key in keys {
        let want = reference_version(e, key);
        assert_eq!(filtered_version(e, key), want, "{ctx}: probe_version mismatch for {key:?}");
        assert_eq!(e.version(key).unwrap(), want, "{ctx}: version mismatch for {key:?}");
    }
}

fn offcore_compact<D: BlockDevice + 'static>(e: &mut Engine<D>, plan: CompactPlan<D>) {
    let wdev = Arc::new(e.device().sibling().unwrap());
    match vals_rt::block_on(compact_and_write(plan, wdev), || {}) {
        Ok(written) => {
            let _ = e.compact_commit(written);
        }
        Err((segs, err)) => {
            e.free_compact_segs(&segs);
            panic!("compact_and_write failed: {err}");
        }
    }
}

fn cover_all<D: BlockDevice + 'static>(e: &mut Engine<D>, chunk: usize) -> usize {
    let mut committed = 0;
    while let Some(mut plan) = e.cover_scan_plan() {
        let dev = e.device().sibling().unwrap();
        while !plan.step(&dev, chunk).unwrap() {}
        assert!(e.cover_scan_commit(plan));
        committed += 1;
    }
    committed
}

fn file_engine(path: &std::path::Path, create: bool, params: KeyFilterParams) -> Engine<FileBlockDevice> {
    let cfg = Config {
        seg_blocks: 16,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    };
    let dev = if create {
        FileBlockDevice::create(path, 512, 16384).unwrap()
    } else {
        FileBlockDevice::open(path, 512).unwrap()
    };
    let mut e = Engine::open(dev, cfg).unwrap();
    e.key_filter.set_params(params);
    for or in &mut e.runs {
        or.covered = false;
    }
    e
}

#[test]
fn sub_filter_has_no_false_negatives_and_bounded_fpr() {
    let sub = SubFilter::new(20_000, 14, 7);
    for i in 0..20_000 {
        sub.insert(&BloomKey::new(&kf_key(i)));
    }
    for i in 0..20_000 {
        assert!(sub.contains(&BloomKey::new(&kf_key(i))), "false negative for key {i}");
    }
    let fp = (0..100_000).filter(|&i| sub.contains(&BloomKey::new(&absent_key(i)))).count();
    let fpr = fp as f64 / 100_000.0;
    assert!(fpr < 0.004, "14 bits/key sub-filter fpr {fpr}");
}

#[test]
fn default_bits_and_cap_parse_and_clamp() {
    let lookup = |pairs: &'static [(&'static str, &'static str)]| {
        move |name: &str| pairs.iter().find(|(k, _)| *k == name).map(|(_, v)| v.to_string())
    };
    let p = KeyFilterParams::from_lookup(lookup(&[]));
    assert_eq!((p.bits_per_key, p.max_bits_per_key, p.growth_pct), (12, 12, 200));
    assert_eq!((p.rebuild, p.step_pct, p.rebuild_subs, p.rebuild_concurrency), (true, 20, 4, 1));
    assert_eq!((p.rebuild_urgent_fpr_ppm, p.rebuild_urgent_extra), (25_000, 1));
    let urgent = KeyFilterParams::from_lookup(lookup(&[
        ("VALS_KEY_FILTER_REBUILD_URGENT_FPR_PPM", "1"),
        ("VALS_KEY_FILTER_REBUILD_URGENT_EXTRA", "99"),
    ]));
    assert_eq!((urgent.rebuild_urgent_fpr_ppm, urgent.rebuild_urgent_extra), (1_000, 8));
    let disabled = KeyFilterParams::from_lookup(lookup(&[
        ("VALS_KEY_FILTER_REBUILD_URGENT_FPR_PPM", "0"), ("VALS_KEY_FILTER_REBUILD_URGENT_EXTRA", "0"),
    ]));
    assert_eq!((disabled.rebuild_urgent_fpr_ppm, disabled.rebuild_urgent_extra), (0, 0));
    let capped = KeyFilterParams::from_lookup(lookup(&[("VALS_KEY_FILTER_REBUILD_URGENT_FPR_PPM", "999999")]));
    assert_eq!(capped.rebuild_urgent_fpr_ppm, 500_000);
    for value in ["1000", "25000", "500000"] {
        let p = KeyFilterParams::from_lookup(|name| {
            (name == "VALS_KEY_FILTER_REBUILD_URGENT_FPR_PPM").then(|| value.to_string())
        });
        assert_eq!(p.rebuild_urgent_fpr_ppm, value.parse::<u64>().unwrap());
    }
    let legacy = KeyFilterParams::from_lookup(lookup(&[
        ("VALS_KEY_FILTER_REBUILD", "0"), ("VALS_KEY_FILTER_STEP_PCT", "1"),
        ("VALS_KEY_FILTER_REBUILD_SUBS", "99"), ("VALS_KEY_FILTER_REBUILD_CONCURRENCY", "0"),
    ]));
    assert_eq!((legacy.rebuild, legacy.step_pct, legacy.rebuild_subs, legacy.rebuild_concurrency), (false, 5, 16, 1));
    let p = KeyFilterParams::from_lookup(lookup(&[("VALS_KEY_FILTER_BITS_PER_KEY", "13")]));
    assert_eq!((p.bits_per_key, p.max_bits_per_key), (13, 13));
    let p = KeyFilterParams::from_lookup(lookup(&[
        ("VALS_KEY_FILTER_BITS_PER_KEY", "13"),
        ("VALS_KEY_FILTER_MAX_BITS_PER_KEY", "18"),
        ("VALS_KEY_FILTER_GROWTH_PCT", "150"),
    ]));
    assert_eq!((p.bits_per_key, p.max_bits_per_key, p.growth_pct), (13, 18, 150));
    let p = KeyFilterParams::from_lookup(lookup(&[
        ("VALS_KEY_FILTER_BITS_PER_KEY", "2"),
        ("VALS_KEY_FILTER_MAX_BITS_PER_KEY", "40"),
    ]));
    assert_eq!((p.bits_per_key, p.max_bits_per_key), (4, 18));
    let p = KeyFilterParams::from_lookup(lookup(&[("VALS_KEY_FILTER_MAX_BITS_PER_KEY", "7")]));
    assert_eq!((p.bits_per_key, p.max_bits_per_key), (12, 12));
    let p = KeyFilterParams::from_lookup(lookup(&[("VALS_KEY_FILTER_MAX_BITS_PER_KEY", "x")]));
    assert_eq!((p.bits_per_key, p.max_bits_per_key), (12, 12));
}

#[test]
fn sub_filters_hold_their_modelled_fpr() {
    for (bits, max_fpr) in [(10u64, 0.011), (11, 0.0075), (12, 0.005)] {
        let n = 40_000u64;
        let mut kf = KeyFilter::new(KeyFilterParams { bits_per_key: bits, max_bits_per_key: bits, ..small_params(n) });
        let mut sink = kf.sink(n, 0).unwrap();
        for i in 0..n {
            sink.insert(&kf_key(i));
        }
        drop(sink);
        assert_eq!((kf.sub_filters().len(), kf.sub_filters()[0].capacity()), (1, n));
        let probes = 200_000u64;
        let fp = (0..probes).filter(|&i| kf.may_contain(&absent_key(i))).count();
        let fpr = fp as f64 / probes as f64;
        let est = kf.estimated_fpr();
        eprintln!("key filter sub-filter {bits} bits/key full: measured fpr {fpr:.5}, modelled {est:.5}");
        assert!(fpr < max_fpr, "{bits} bits/key fpr {fpr}");
        assert!((est - fpr).abs() < fpr * 0.2, "{bits} bits/key model {est} vs measured {fpr}");
    }
}

fn grow_key_filter(min_keys: u64, total: u64, stage_fpr: f64) -> (KeyFilter, f64) {
    let mut kf = KeyFilter::new(small_params(min_keys));
    let batch = (min_keys / 10).max(1);
    let mut inserted = 0u64;
    let mut subs_seen = 0usize;
    while inserted < total {
        let n = batch.min(total - inserted);
        let mut sink = kf.sink(n, inserted).unwrap();
        for i in inserted..inserted + n {
            sink.insert(&kf_key(i));
        }
        drop(sink);
        inserted += n;
        if subs_seen != kf.sub_filters().len() {
            subs_seen = kf.sub_filters().len();
            let fp = (0..50_000u64).filter(|&i| kf.may_contain(&absent_key(i))).count();
            let fpr = fp as f64 / 50_000.0;
            assert!(fpr < stage_fpr, "compound fpr {fpr} at {inserted} keys, {subs_seen} sub-filters");
        }
    }
    for i in (0..total).step_by(7) {
        assert!(kf.may_contain(&kf_key(i)), "false negative for key {i}");
    }
    let fp = (0..200_000u64).filter(|&i| kf.may_contain(&absent_key(i))).count();
    let fpr = fp as f64 / 200_000.0;
    let caps: Vec<u64> = kf.sub_filters().iter().map(|s| s.capacity()).collect();
    let bits: Vec<u64> = kf.sub_filters().iter().map(|s| s.bits_per_key()).collect();
    eprintln!(
        "scalable growth: {total} keys from a {min_keys} floor, sub-filters {caps:?} at {bits:?} bits/key, measured fpr {fpr:.5}, estimated {:.5}, {:.2} B/key allocated, {:.2} B/key resident",
        kf.estimated_fpr(),
        kf.filter_bytes() as f64 / total as f64,
        kf.resident_bytes() as f64 / total as f64,
    );
    assert!(bits.iter().all(|&b| b == 12), "default schedule is a constant 12 bits/key: {bits:?}");
    assert!((kf.estimated_fpr() - fpr).abs() < fpr * 0.25 + 0.0005, "estimate {} vs measured {fpr}", kf.estimated_fpr());
    (kf, fpr)
}

#[test]
fn scalable_growth_keeps_compound_fpr_bounded() {
    let (kf, fpr) = grow_key_filter(20_000, 550_000, 0.02);
    assert_eq!(kf.sub_filters().len(), 5, "440M keys over a 16M floor, scaled down 800x");
    assert!(fpr < 0.02, "compound fpr {fpr} at the scaled 440M-key point");
    let bytes_per_key = kf.filter_bytes() as f64 / 550_000.0;
    assert!(bytes_per_key < 1.8, "{bytes_per_key} B/key");

    let (kf, fpr) = grow_key_filter(5_000, 400_000, 0.03);
    assert_eq!(kf.sub_filters().len(), 7, "long growth: seven doublings past the floor");
    assert!(fpr < 0.03, "compound fpr {fpr} with seven sub-filters");
}

#[test]
fn sidecar_and_block_key_sources_yield_every_run_key() {
    for version in [run::RUN_SIDECAR_VERSION, run::RUN_VERSION] {
        let mut e = kf_engine();
        for i in 0..400u64 {
            e.put(&kf_key(i), b"v", ver_of(&kf_key(i))).unwrap();
        }
        run::with_run_write_version(version, || run_flush(&mut e, RunBuildOpts::default())).unwrap();
        assert_eq!(e.runs[0].run.format_version(), version);
        assert_eq!(e.runs[0].run.has_sidecar(), version == run::RUN_SIDECAR_VERSION);
        let want = run_keys(&e, 0);
        assert_eq!(want.len(), 400);
        for chunk in [1usize, 7, 64, 1 << 20] {
            for blocks in [false, true] {
                let mut source = if blocks {
                    e.runs[0].run.block_key_source(&e.alloc)
                } else {
                    e.runs[0].run.key_source(&e.alloc)
                };
                assert_eq!(
                    source.first_sidecar_record_location(512).is_some(),
                    !blocks && version == run::RUN_SIDECAR_VERSION
                );
                let mut got = Vec::new();
                while !source.step(e.device(), chunk, |k| got.push(k.to_vec())).unwrap() {}
                assert_eq!(got, want, "version {version} chunk {chunk} blocks {blocks}");
                assert!(source.bytes_read() > 0);
            }
        }
    }
}

#[test]
fn flush_and_merge_publish_covered_runs_and_skip_them_on_negatives() {
    let mut e = kf_engine();
    for round in 0..4u64 {
        for i in 0..200u64 {
            let k = kf_key(round * 1000 + i);
            e.put(&k, b"v", ver_of(&k)).unwrap();
        }
        if round % 2 == 0 {
            run_flush(&mut e, RunBuildOpts::default()).unwrap();
        } else {
            e.flush(RunBuildOpts::default()).unwrap().unwrap();
        }
    }
    assert_eq!(e.key_filter_stats().uncovered_runs, 0);
    assert_eq!(e.key_filter_stats().covered_runs, 4);
    assert_eq!(coverage_violations(&e), 0);
    let before = e.key_filter_stats();
    let absent: Vec<Vec<u8>> = (0..500).map(absent_key).collect();
    for k in &absent {
        assert_eq!(filtered_version(&e, k), None);
    }
    let after = e.key_filter_stats();
    assert_eq!(after.probes - before.probes, 500);
    assert!(after.negatives - before.negatives >= 490, "{after:?}");
    assert!(after.run_probes - before.run_probes <= 4 * 10, "{after:?}");
    let plan = e.compact_plan(RunBuildOpts::default(), None).unwrap().unwrap();
    assert!(plan.key_sink.is_none(), "covered inputs need no re-insertion");
    offcore_compact(&mut e, plan);
    assert!(e.runs.iter().all(|or| or.covered));
    assert_eq!(coverage_violations(&e), 0);
    let present: Vec<Vec<u8>> = (0..4).flat_map(|r| (0..200).map(move |i| kf_key(r * 1000 + i))).collect();
    assert_probes_match(&e, &present, "after merge");
}

#[test]
fn knockout_skipped_flush_insertion_is_detected() {
    let mut e = kf_engine();
    for i in 0..100u64 {
        e.put(&kf_key(i), b"v", ver_of(&kf_key(i))).unwrap();
    }
    run_flush(&mut e, RunBuildOpts::default()).unwrap();
    for i in 100..200u64 {
        e.put(&kf_key(i), b"v", ver_of(&kf_key(i))).unwrap();
    }
    e.test_key_filter_knockout = true;
    run_flush(&mut e, RunBuildOpts::default()).unwrap();
    assert!(e.runs.iter().all(|or| or.covered), "the knocked-out run is still marked covered");
    assert!(coverage_violations(&e) > 0, "coverage checker must detect the missing run");
    let wrong = (100..200u64)
        .filter(|&i| filtered_version(&e, &kf_key(i)) != reference_version(&e, &kf_key(i)))
        .count();
    assert!(wrong > 0, "the knockout must produce false-absent probes");
}

#[test]
fn restart_probes_uncovered_runs_until_cover_scan_completes() {
    let path = temp_path("kf-restart");
    let params = small_params(1_000);
    {
        let mut e = file_engine(&path, true, params);
        for round in 0..3u64 {
            for i in 0..1_500u64 {
                let k = kf_key(i * 3 + round);
                e.put(&k, b"v", ver_of(&k)).unwrap();
            }
            run_flush(&mut e, RunBuildOpts::default()).unwrap();
            assert!(e.runs.last().unwrap().run.index_len() >= 2, "a partial cover scan needs multi-block runs");
        }
        assert_eq!(e.key_filter_stats().covered_runs, 3);
        e.sync().unwrap();
    }
    let mut e = file_engine(&path, false, params);
    let stats = e.key_filter_stats();
    assert_eq!((stats.covered_runs, stats.uncovered_runs, stats.sub_filters), (0, 3, 0));
    let absent: Vec<Vec<u8>> = (0..300).map(|i| absent_key(i * 3)).collect();
    let before = e.key_filter_stats();
    for k in &absent {
        assert_eq!(filtered_version(&e, k), None);
    }
    let after = e.key_filter_stats();
    assert_eq!(after.probes, before.probes, "no filter yet");
    assert!(after.run_probes - before.run_probes >= 300 * 2, "uncovered runs must be probed: {after:?}");

    let mut plan = e.cover_scan_plan().unwrap();
    assert!(e.cover_scan_plan().is_none(), "one cover scan at a time");
    let dev = e.device().sibling().unwrap();
    assert!(!plan.step(&dev, 64).unwrap());
    let mid = e.key_filter_stats();
    assert_eq!(mid.covered_runs, 0, "a partial scan covers nothing");
    let present: Vec<Vec<u8>> = (0..4_500).map(kf_key).collect();
    assert_probes_match(&e, &present, "mid-scan");
    while !plan.step(&dev, 64).unwrap() {}
    assert!(e.cover_scan_commit(plan));
    assert_eq!(cover_all(&mut e, 4096), 2);
    assert_eq!(e.key_filter_stats().uncovered_runs, 0);
    assert_eq!(coverage_violations(&e), 0);
    let before = e.key_filter_stats();
    for k in &absent {
        assert_eq!(filtered_version(&e, k), None);
    }
    let after = e.key_filter_stats();
    assert!(after.negatives - before.negatives >= 295, "{after:?}");
    assert!(after.run_probes - before.run_probes <= 3 * 5, "{after:?}");
    assert_probes_match(&e, &present, "after cover scan");
    drop(e);
    std::fs::remove_file(&path).ok();
}

#[test]
fn cover_scan_of_a_merged_away_run_is_discarded() {
    let mut e = kf_engine();
    for round in 0..4u64 {
        for i in 0..100u64 {
            let k = kf_key(round * 1000 + i);
            e.put(&k, b"v", ver_of(&k)).unwrap();
        }
        run_flush(&mut e, RunBuildOpts::default()).unwrap();
    }
    e.key_filter.reset();
    for or in &mut e.runs {
        or.covered = false;
    }
    let mut plan = e.cover_scan_plan().unwrap();
    let merge = e.compact_plan(RunBuildOpts::default(), None).unwrap().unwrap();
    assert!(merge.key_sink.is_some(), "uncovered inputs are re-inserted by the merge");
    offcore_compact(&mut e, merge);
    assert!(e.runs.iter().all(|or| or.covered));
    let dev = e.device().sibling().unwrap();
    while !plan.step(&dev, 1 << 20).unwrap_or(true) {}
    assert!(!e.cover_scan_commit(plan));
    assert!(e.cover_scan_plan().is_none());
    assert_eq!(coverage_violations(&e), 0);
}

type Fe = FileBlockDevice;

enum FlushStage {
    Planned(FlushPlan<Fe>),
    Written(WrittenFlush<Fe>),
}

enum CompactStage {
    Planned(CompactPlan<Fe>),
    Written(WrittenCompact<Fe>),
}

#[derive(Debug, Default, Clone, Copy)]
struct PropTally {
    cover_commits: u64,
    cover_discards: u64,
    explicit_resets: u64,
    natural_resets: u64,
    reopens: u64,
    covered_checks: u64,
    flush_commits: u64,
    merge_commits: u64,
    stale_flush_commits: u64,
    stale_merge_commits: u64,
    grown_flush_commits: u64,
    grown_merge_commits: u64,
    sinkless_partial_merge_commits: u64,
    sinkless_uncovered_merge_commits: u64,
    interleaved_commits: u64,
    rebuilds_seen: u64,
    rebuilds_completed: u64,
}

impl PropTally {
    fn add(&mut self, o: &PropTally) {
        self.cover_commits += o.cover_commits;
        self.cover_discards += o.cover_discards;
        self.explicit_resets += o.explicit_resets;
        self.natural_resets += o.natural_resets;
        self.reopens += o.reopens;
        self.covered_checks += o.covered_checks;
        self.flush_commits += o.flush_commits;
        self.merge_commits += o.merge_commits;
        self.stale_flush_commits += o.stale_flush_commits;
        self.stale_merge_commits += o.stale_merge_commits;
        self.grown_flush_commits += o.grown_flush_commits;
        self.grown_merge_commits += o.grown_merge_commits;
        self.sinkless_partial_merge_commits += o.sinkless_partial_merge_commits;
        self.sinkless_uncovered_merge_commits += o.sinkless_uncovered_merge_commits;
        self.interleaved_commits += o.interleaved_commits;
        self.rebuilds_seen += o.rebuilds_seen;
        self.rebuilds_completed += o.rebuilds_completed;
    }
}

macro_rules! ensure {
    ($cond:expr, $($fmt:tt)+) => {
        if !$cond {
            return Err(format!($($fmt)+));
        }
    };
}

fn check_probes<D: BlockDevice + AsyncRead>(e: &Engine<D>, keys: &[Vec<u8>], ctx: &str) -> Result<(), String> {
    for key in keys {
        let want = reference_version(e, key);
        let probed = filtered_version(e, key);
        ensure!(probed == want, "{ctx}: probe_version {probed:?} != reference {want:?} for {key:?}");
        let version = e.version(key).map_err(|err| format!("{ctx}: version failed: {err}"))?;
        ensure!(version == want, "{ctx}: version {version:?} != reference {want:?} for {key:?}");
    }
    Ok(())
}

fn sink_state<D: BlockDevice>(e: &Engine<D>, sink: Option<&crate::keyfilter::KeyFilterSink>) -> (bool, bool) {
    let Some(sink) = sink else {
        return (false, false);
    };
    if !e.key_filter.owns(sink) {
        return (true, false);
    }
    let newest = e.key_filter.sub_filters().last().expect("an owned sink's generation has a sub-filter");
    (false, !Arc::ptr_eq(sink.sub_filter(), newest))
}

fn covered_inputs<D: BlockDevice>(e: &Engine<D>, inputs: &[RunMeta]) -> usize {
    inputs
        .iter()
        .filter(|m| e.runs.iter().any(|or| or.meta.handle == m.handle && or.covered))
        .count()
}

#[derive(Debug, Clone, Copy)]
struct PropCfg {
    min_keys: u64,
    churn: bool,
    offcore_only: bool,
    rebuild: bool,
    probes: usize,
}

fn property_body(
    path: &std::path::Path,
    seed: u64,
    ops: usize,
    cfg: PropCfg,
    knock: KeyFilterKnockouts,
) -> Result<PropTally, String> {
    let params = KeyFilterParams { rebuild: cfg.rebuild, rebuild_subs: 2, ..small_params(cfg.min_keys) };
    let churn = cfg.churn;
    let open = |create: bool| {
        let mut e = file_engine(path, create, params);
        e.test_kf_knockouts = knock;
        e
    };
    let mut e = open(true);
    let mut rng = Rng(seed);
    let mut next_version = 1u128 << 64;
    let span = if churn { 300u64 } else { 1_200 };
    let window = |step: usize| if churn { step as u64 * 4 } else { 0 };
    let mut scan: Option<CoverScanPlan<Fe>> = None;
    let mut flush: Option<FlushStage> = None;
    let mut compact: Option<CompactStage> = None;
    let mut t = PropTally::default();
    for step in 0..ops {
        let op = rng.below(1000);
        let ctx = format!("seed {seed} step {step} op {op}");
        let lo = window(step);
        let keyspace = lo + span;
        match op {
            0..=249 | 940..=999 => {
                let n = 1 + rng.below(120);
                for _ in 0..n {
                    let k = kf_key(lo + rng.below(span));
                    next_version += 1 + rng.below(3) as u128;
                    if rng.below(10) == 0 {
                        e.put_kind(&k, b"", next_version, true).unwrap();
                    } else {
                        e.put(&k, b"value", next_version).unwrap();
                    }
                }
            }
            250..=379 => match flush.take() {
                None => {
                    flush = e.flush_plan(RunBuildOpts::default()).unwrap().map(FlushStage::Planned);
                }
                Some(FlushStage::Planned(plan)) => {
                    let wdev = e.device().sibling().unwrap();
                    match vals_rt::block_on(build_and_write_flush(plan, &wdev), || {}) {
                        Ok(written) => flush = Some(FlushStage::Written(written)),
                        Err((segs, err)) => {
                            e.free_flush_segs(&segs);
                            return Err(format!("{ctx}: flush build failed: {err}"));
                        }
                    }
                }
                Some(FlushStage::Written(written)) if rng.below(2) == 0 => flush = Some(FlushStage::Written(written)),
                Some(FlushStage::Written(written)) => {
                    let (stale, grown) = sink_state(&e, written.key_sink_for_test());
                    t.stale_flush_commits += stale as u64;
                    t.grown_flush_commits += grown as u64;
                    t.interleaved_commits += compact.is_some() as u64;
                    e.flush_commit(written).map_err(|err| format!("{ctx}: flush commit failed: {err}"))?;
                    t.flush_commits += 1;
                }
            },
            380..=389 => match flush.take() {
                Some(FlushStage::Planned(plan)) => e.free_flush_segs(plan.segments()),
                other => flush = other,
            },
            390..=409 => {
                if flush.is_none() && compact.is_none() && !e.flush_in_flight() {
                    e.flush(RunBuildOpts::default()).unwrap();
                }
            }
            410..=539 => match compact.take() {
                None => {
                    let planned = if rng.below(3) == 0 {
                        let exact: std::collections::HashMap<Vec<u8>, u128> = (0..1 + rng.below(20))
                            .map(|_| kf_key(rng.below(keyspace)))
                            .filter_map(|k| reference_version(&e, &k).map(|v| (k, v)))
                            .collect();
                        e.forget_plan(RunBuildOpts::default(), &exact).unwrap()
                    } else {
                        e.compact_plan(RunBuildOpts::default(), None).unwrap()
                    };
                    compact = planned.map(CompactStage::Planned);
                }
                Some(CompactStage::Planned(plan)) => {
                    let wdev = Arc::new(e.device().sibling().unwrap());
                    match vals_rt::block_on(compact_and_write(plan, wdev), || {}) {
                        Ok(written) => compact = Some(CompactStage::Written(written)),
                        Err((segs, err)) => {
                            e.free_compact_segs(&segs);
                            return Err(format!("{ctx}: compaction build failed: {err}"));
                        }
                    }
                }
                Some(CompactStage::Written(written)) if rng.below(4) != 0 => compact = Some(CompactStage::Written(written)),
                Some(CompactStage::Written(written)) => {
                    let (stale, grown) = sink_state(&e, written.key_sink_for_test());
                    t.stale_merge_commits += stale as u64;
                    t.grown_merge_commits += grown as u64;
                    t.interleaved_commits += flush.is_some() as u64;
                    if written.key_sink_for_test().is_none() {
                        let inputs = written.merged_inputs_for_test();
                        let covered = covered_inputs(&e, inputs);
                        if covered < inputs.len() {
                            t.sinkless_uncovered_merge_commits += 1;
                            t.sinkless_partial_merge_commits += (covered > 0) as u64;
                        }
                    }
                    if e.compact_commit(written).is_ok() {
                        t.merge_commits += 1;
                    }
                }
            },
            540..=549 => match compact.take() {
                Some(CompactStage::Planned(plan)) => e.free_compact_segs(&plan.segments()),
                other => compact = other,
            },
            550..=609 if cfg.offcore_only => {}
            550..=569 => {
                if flush.is_none() && compact.is_none() && !e.flush_in_flight() && !e.compact_in_flight() {
                    e.compact_runs(RunBuildOpts::default(), None).unwrap();
                }
            }
            570..=609 => {
                let keys: std::collections::HashSet<Vec<u8>> = if churn {
                    match flush.take() {
                        Some(FlushStage::Planned(plan)) => e.free_flush_segs(plan.segments()),
                        Some(FlushStage::Written(written)) => {
                            e.flush_commit(written).map_err(|err| format!("{ctx}: flush commit failed: {err}"))?;
                        }
                        None => {}
                    }
                    match compact.take() {
                        Some(CompactStage::Planned(plan)) => e.free_compact_segs(&plan.segments()),
                        Some(CompactStage::Written(written)) => {
                            let _ = e.compact_commit(written);
                        }
                        None => {}
                    }
                    (lo.saturating_sub(800)..lo).map(kf_key).collect()
                } else {
                    (0..1 + rng.below(20)).map(|_| kf_key(rng.below(keyspace))).collect()
                };
                e.forget_keys(&keys, RunBuildOpts::default()).unwrap();
            }
            610..=624 if !churn => {
                if let Some(plan) = scan.take() {
                    e.cover_scan_abort(&plan);
                }
                flush = None;
                compact = None;
                e.sync().unwrap();
                t.natural_resets += e.key_filter.resets;
                drop(e);
                e = open(false);
                t.reopens += 1;
            }
            625..=899 => match scan.take() {
                Some(plan) if !e.cover_scan_current(&plan) => {
                    e.cover_scan_abort(&plan);
                    t.cover_discards += 1;
                }
                Some(mut plan) => {
                    let dev = e.device().sibling().unwrap();
                    let chunk = 1 + rng.below(16 * 1024) as usize;
                    match plan.step(&dev, chunk) {
                        Ok(true) => {
                            if e.cover_scan_commit(plan) {
                                t.cover_commits += 1;
                            } else {
                                t.cover_discards += 1;
                            }
                        }
                        Ok(false) if e.cover_scan_current(&plan) => scan = Some(plan),
                        Ok(false) => e.cover_scan_abort(&plan),
                        Err(err) => {
                            e.cover_scan_fail(&plan);
                            return Err(format!("{ctx}: cover scan failed: {err}"));
                        }
                    }
                }
                None => scan = e.cover_scan_plan(),
            },
            900..=929 if !churn => {
                t.explicit_resets += 1;
                e.reset_key_filter_for_test();
            }
            _ => {
                if let Some(plan) = scan.take() {
                    e.cover_scan_abort(&plan);
                }
            }
        }
        if cfg.rebuild { e.key_filter_maintain(); }
        t.rebuilds_seen += e.key_filter.rebuilding() as u64;
        t.rebuilds_completed = t.rebuilds_completed.max(e.key_filter_stats().rebuilds_completed);
        let missing = coverage_violations(&e);
        ensure!(missing == 0, "{ctx}: {missing} covered-run keys missing from the key filter");
        t.covered_checks += e.runs.iter().filter(|or| or.covered).count() as u64;
        let sample: Vec<Vec<u8>> = (0..40)
            .map(|i| if i % 4 == 0 { absent_key(rng.below(1 << 20)) } else { kf_key(rng.below(keyspace)) })
            .collect();
        check_probes(&e, &sample[..cfg.probes], &ctx)?;
    }
    let all: Vec<Vec<u8>> = (0..window(ops) + span).map(kf_key).chain((0..400).map(absent_key)).collect();
    check_probes(&e, &all, &format!("seed {seed} final"))?;
    t.natural_resets += e.key_filter.resets;
    Ok(t)
}

fn property_run(seed: u64, ops: usize, cfg: PropCfg, knock: KeyFilterKnockouts) -> Result<PropTally, String> {
    let path = temp_path(&format!("kf-prop-{seed}"));
    let result = property_body(&path, seed, ops, cfg, knock);
    std::fs::remove_file(&path).ok();
    result
}

const PROPERTY_OPS: usize = 600;

fn property_seeds() -> Vec<(u64, PropCfg)> {
    let cfg = |min_keys, churn| PropCfg { min_keys, churn, offcore_only: false, rebuild: false, probes: 40 };
    (1..=12u64).map(|seed| (seed, cfg(64 + seed % 512, false))).chain((13..=16u64).map(|seed| (seed, cfg(8, true)))).collect()
}

fn lane_seeds(default_lane: &[u64]) -> Vec<(u64, PropCfg)> {
    if full_lane() {
        return property_seeds();
    }
    property_seeds().into_iter()
        .filter(|(seed, _)| default_lane.contains(seed))
        .map(|(seed, cfg)| (seed, PropCfg { probes: 10, ..cfg }))
        .collect()
}

#[test]
fn property_global_filter_covers_every_covered_run_and_matches_per_run_probes() {
    if isolated_env_test(&[("VALS_MERGE_TIER_MIN_K", Some("2")), ("VALS_MERGE_TIER_K", None)]) {
        return;
    }
    let mut total = PropTally::default();
    for (seed, cfg) in lane_seeds(&[3, 5, 14]) {
        let t = property_run(seed, PROPERTY_OPS, cfg, KeyFilterKnockouts::default()).unwrap_or_else(|err| panic!("{err}"));
        eprintln!("seed {seed} {cfg:?}: {t:?}");
        if cfg.churn {
            assert!(t.natural_resets > 0, "seed {seed}: tiny min_keys must fire the stale reset: {t:?}");
        }
        assert!(t.covered_checks > 0, "seed {seed}: {t:?}");
        total.add(&t);
    }
    eprintln!("property total: {total:?}");
    assert!(total.stale_flush_commits > 0, "{total:?}");
    assert!(total.stale_merge_commits > 0, "{total:?}");
    assert!(total.grown_flush_commits > 0, "{total:?}");
    assert!(total.grown_merge_commits > 0, "{total:?}");
    assert!(total.sinkless_partial_merge_commits > 0, "{total:?}");
    assert!(total.interleaved_commits > 0, "{total:?}");
    assert!(total.cover_commits > 0 && total.cover_discards > 0, "{total:?}");
}

fn assert_knockout_detected(knock: KeyFilterKnockouts) {
    for offcore_only in [false, true] {
        let detected = property_seeds().into_iter().find_map(|(seed, cfg)| {
            property_run(seed, PROPERTY_OPS, PropCfg { offcore_only, ..cfg }, knock).err()
        });
        match detected {
            Some(err) => eprintln!("{knock:?} offcore_only={offcore_only} detected: {err}"),
            None => panic!("knockout {knock:?} offcore_only={offcore_only} was not detected by any property seed"),
        }
    }
}

#[test]
fn property_detects_a_knocked_out_merge_sink() {
    assert_knockout_detected(KeyFilterKnockouts { merge: true, ..Default::default() });
}

#[test]
fn property_detects_a_knocked_out_cover_scan_sink() {
    assert_knockout_detected(KeyFilterKnockouts { cover_scan: true, ..Default::default() });
}

#[test]
fn property_detects_a_broken_all_inputs_covered_rule() {
    assert_knockout_detected(KeyFilterKnockouts { inputs_covered: true, ..Default::default() });
}

#[test]
fn property_detects_a_knocked_out_flush_sink() {
    assert_knockout_detected(KeyFilterKnockouts { flush: true, ..Default::default() });
}

fn sinkless_merge_across_reset_and_partial_recover(knock: KeyFilterKnockouts) -> usize {
    let mut e = kf_engine();
    for round in 0..4u64 {
        for i in 0..(100 + round * 20) {
            let k = kf_key(round * 1000 + i);
            e.put(&k, b"v", ver_of(&k)).unwrap();
        }
        run_flush(&mut e, RunBuildOpts::default()).unwrap();
    }
    e.test_kf_knockouts = knock;
    let plan = e.compact_plan(RunBuildOpts::default(), None).unwrap().unwrap();
    assert!(plan.key_sink.is_none(), "all inputs covered at plan time");
    let n_inputs = plan.merged_inputs.len();
    assert!(n_inputs >= 2);
    let wdev = Arc::new(e.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    e.reset_key_filter_for_test();
    let mut scan = e.cover_scan_plan().unwrap();
    let dev = e.device().sibling().unwrap();
    while !scan.step(&dev, 1 << 20).unwrap() {}
    assert!(e.cover_scan_commit(scan));
    let inputs = written.merged_inputs_for_test().to_vec();
    assert_eq!(covered_inputs(&e, &inputs), 1, "exactly one input re-covered before the merge commits");
    e.compact_commit(written).unwrap();
    let missing = coverage_violations(&e);
    if missing == 0 {
        assert!(e.runs.iter().any(|or| !or.covered), "the merge output stays uncovered");
        let present: Vec<Vec<u8>> = (0..4u64).flat_map(|r| (0..100).map(move |i| kf_key(r * 1000 + i))).collect();
        assert_probes_match(&e, &present, "after a sinkless merge across a reset");
        cover_all(&mut e, 4096);
        assert_eq!(e.key_filter_stats().uncovered_runs, 0);
        assert_eq!(coverage_violations(&e), 0);
    }
    missing
}

#[test]
fn sinkless_merge_planned_covered_then_reset_and_partially_recovered_commits_uncovered() {
    assert_eq!(sinkless_merge_across_reset_and_partial_recover(KeyFilterKnockouts::default()), 0);
}

#[test]
fn knockout_broken_inputs_covered_rule_marks_a_sinkless_merge_covered() {
    let knock = KeyFilterKnockouts { inputs_covered: true, ..Default::default() };
    assert!(sinkless_merge_across_reset_and_partial_recover(knock) > 0);
}

#[test]
fn concurrent_sinks_reserve_sub_filter_capacity() {
    let mut kf = KeyFilter::new(small_params(1_000));
    let mut a = kf.sink(600, 0).unwrap();
    assert_eq!(kf.sub_filters().len(), 1);
    assert_eq!(kf.sub_filters()[0].reserved(), 600);
    let b = kf.sink(600, 0).unwrap();
    assert_eq!(kf.sub_filters().len(), 2, "a second in-flight sink must not overfill the first sub-filter");
    assert!(!Arc::ptr_eq(a.sub_filter(), b.sub_filter()));
    for i in 0..600 {
        a.insert(&kf_key(i));
    }
    drop(a);
    let first = &kf.sub_filters()[0];
    assert_eq!(first.reserved(), 0);
    assert!(first.inserted() <= 600 && first.inserted() > 550, "{}", first.inserted());
    let c = kf.sink(300, 0).unwrap();
    assert!(Arc::ptr_eq(c.sub_filter(), &kf.sub_filters()[1]));
    drop((b, c));
    assert!(kf.sub_filters().iter().all(|s| s.reserved() == 0));
}

#[test]
fn reset_keeps_charging_sub_filters_held_by_in_flight_sinks() {
    let mut kf = KeyFilter::new(small_params(10_000));
    let mut held = kf.sink(3_000, 0).unwrap();
    for i in 0..3_000 {
        held.insert(&kf_key(i));
    }
    let idle = kf.sink(10, 0).unwrap();
    drop(idle);
    let bytes = kf.filter_bytes();
    assert!(bytes > 0);
    kf.reset();
    let stats = kf.stats();
    assert_eq!((stats.sub_filters, stats.retired_sub_filters), (0, 1));
    assert_eq!(stats.retired_bytes, bytes);
    assert_eq!(stats.retired_resident_bytes, stats.resident_bytes);
    assert!(stats.retired_resident_bytes > 0 && stats.retired_resident_bytes <= stats.retired_bytes);
    assert_eq!(kf.filter_bytes(), bytes, "an orphaned sub-filter still referenced by a sink stays charged");
    assert!(kf.resident_bytes() > 0);
    assert!(!kf.owns(&held));
    drop(held);
    let fresh = kf.sink(10, 0).unwrap();
    let stats = kf.stats();
    assert_eq!(stats.retired_sub_filters, 0, "a released orphan is pruned at the next sink");
    assert_eq!((stats.retired_bytes, stats.retired_resident_bytes), (0, 0));
    assert_eq!(kf.filter_bytes(), kf.sub_filters()[0].bytes());
    drop(fresh);
    kf.reset();
    assert_eq!((kf.filter_bytes(), kf.resident_bytes(), kf.stats().retired_sub_filters), (0, 0, 0));
}

#[test]
fn merge_sink_reserves_all_input_entries_when_partially_covered() {
    let mut e = kf_engine();
    for round in 0..4u64 {
        for i in 0..(100 + round * 10) {
            let k = kf_key(round * 1000 + i);
            e.put(&k, b"v", ver_of(&k)).unwrap();
        }
        run_flush(&mut e, RunBuildOpts::default()).unwrap();
    }
    let original = e.key_filter.sub_filters()[0].clone();
    let held = e.key_filter_sink(SinkKind::Flush, 2000).unwrap();
    assert!(!Arc::ptr_eq(&original, held.sub_filter()));
    let inputs: Vec<_> = e.runs.iter().map(|or| or.meta.clone()).collect();
    assert!(e.key_filter_merge_sink(&inputs).is_none(), "covered inputs need no new sink");
    e.runs[1].covered = false;
    let plan = e.compact_plan(RunBuildOpts::default(), None).unwrap().unwrap();
    let expected: u64 = plan.merged_inputs.iter().map(|m| m.entry_count).sum();
    let sink = plan.key_sink.as_ref().expect("an uncovered input needs a merge sink");
    assert_eq!(sink.sub_filter().reserved(), expected);
    assert!(!Arc::ptr_eq(held.sub_filter(), sink.sub_filter()));
    assert!(!Arc::ptr_eq(&original, sink.sub_filter()));
    offcore_compact(&mut e, plan);
    drop(held);
    assert!(e.runs.iter().all(|or| or.covered));
    assert!(e.key_filter.sub_filters().iter().all(|s| s.reserved() == 0));
    assert_eq!(coverage_violations(&e), 0);
}

#[test]
fn partially_shadow_covered_merge_with_a_flush_reservation_keeps_fpr_low() {
    if isolated_env_test(&[]) { return; }
    let n = 100_000u64;
    for offcore in [false, true] {
        let mut e = Engine::create_prewiped(
            MemBlockDevice::new(512, 262144),
            Config { seg_blocks: 16, reserved_blocks: 0, ordering: Ordering::Unsorted },
        );
        e.key_filter.set_params(KeyFilterParams { rebuild: true, rebuild_subs: 16, ..small_params(n) });
        for (start, end) in [(0, n * 9 / 10), (n * 9 / 10, n)] {
            for i in start..end {
                e.put(&i.to_le_bytes(), b"v", 1).unwrap();
            }
            run_flush(&mut e, RunBuildOpts::default()).unwrap();
        }
        assert!(e.key_filter.start_rebuild(n));
        let mut scan = e.cover_scan_plan().unwrap();
        let dev = e.device().sibling().unwrap();
        while !scan.step(&dev, 4096).unwrap() {}
        assert_eq!(scan.keys(), n * 9 / 10);
        assert!(e.cover_scan_commit(scan));
        assert_eq!(e.key_filter_stats().rebuild_covered_runs, 1);
        let held = e.key_filter_sink(SinkKind::Flush, n * 15 / 100).unwrap();
        let inputs: Vec<RunMeta> = e.runs.iter().map(|or| or.meta.clone()).collect();
        if offcore {
            let forget = std::collections::HashMap::from([(b"not-present".to_vec(), 1u128)]);
            let plan = e.forget_plan(RunBuildOpts::default(), &forget).unwrap().unwrap();
            let sink = plan.key_sink.as_ref().unwrap();
            assert!(!e.key_filter.owns(sink) && e.key_filter.owns_rebuild(sink));
            assert_eq!(sink.sub_filter().reserved(), n);
            assert!(sink.sub_filter().capacity() >= n);
            offcore_compact(&mut e, plan);
        } else {
            let sink = e.key_filter_merge_sink(&inputs).unwrap();
            assert_eq!(sink.sub_filter().reserved(), n);
            assert!(sink.sub_filter().capacity() >= n);
            drop(sink);
            e.compact_runs(RunBuildOpts::default(), None).unwrap();
        }
        assert!(!e.key_filter.rebuilding(), "offcore={offcore}: merge must complete the swap");
        let tail = e.key_filter.sub_filters().last().unwrap();
        let tail_capacity = tail.capacity();
        assert!(tail_capacity >= n);
        assert!(tail.inserted() <= tail_capacity);
        drop(held);
        e.key_filter_maintain();
        assert!(!e.key_filter.stale(n));
        for i in 0..n { assert!(e.key_filter.may_contain(&i.to_le_bytes())); }
        let probes = 100_000u64;
        let fp = (n..n + probes).filter(|i| e.key_filter.may_contain(&i.to_le_bytes())).count();
        let fpr = fp as f64 / probes as f64;
        eprintln!("partial-shadow merge offcore={offcore}: fpr={fpr:.5} tail_capacity={tail_capacity}");
        assert!(fpr < 0.02, "offcore={offcore}: fpr={fpr}");
        assert!(e.key_filter.estimated_fpr() < 0.02);
        assert!(e.runs.iter().all(|or| or.covered && !or.rebuild_covered));
    }
}

#[test]
fn empty_runs_can_be_covered_merged_and_shadow_swapped() {
    if isolated_env_test(&[]) { return; }
    let mut e = kf_engine();
    e.key_filter.set_params(KeyFilterParams { rebuild: true, rebuild_subs: 16, ..small_params(1000) });
    for i in 0..2u64 {
        e.put_kind(&i.to_le_bytes(), b"", 1, true).unwrap();
        run_flush(&mut e, RunBuildOpts::default()).unwrap();
    }
    e.compact_runs(RunBuildOpts::default(), Some(u128::MAX)).unwrap();
    assert_eq!(e.runs.len(), 1);
    assert_eq!(e.runs[0].meta.entry_count, 0);
    e.reset_key_filter_for_test();
    assert!(!e.runs[0].covered);
    let inputs = vec![e.runs[0].meta.clone()];
    let sink = e.key_filter_merge_sink(&inputs).expect("an empty active-uncovered merge lane is still requested");
    assert!(e.key_filter.owns(&sink));
    assert_eq!(sink.sub_filter().reserved(), 1);
    drop(sink);
    assert_eq!(cover_all(&mut e, 4096), 1);
    assert!(e.runs[0].covered);
    assert!(e.key_filter.start_rebuild(0));
    let inputs = vec![e.runs[0].meta.clone()];
    let sink = e.key_filter_merge_sink(&inputs).expect("an empty uncovered merge lane is still requested");
    assert!(e.key_filter.owns_rebuild(&sink));
    assert_eq!(sink.sub_filter().reserved(), 1);
    drop(sink);
    assert_eq!(cover_all(&mut e, 4096), 1);
    assert!(!e.key_filter.rebuilding());
    assert_eq!(e.key_filter_stats().rebuilds_completed, 1);
    assert!(e.runs[0].covered && !e.runs[0].rebuild_covered);
    assert!(KeyFilter::new(KeyFilterParams { rebuild: true, ..small_params(1000) }).start_rebuild(0),
        "the empty-run swap releases the process-wide permit");
}

#[test]
fn winner_directory_disables_the_key_filter() {
    let mut e = kf_engine();
    for i in 0..200u64 {
        e.put(&kf_key(i), b"v", ver_of(&kf_key(i))).unwrap();
    }
    run_flush(&mut e, RunBuildOpts::default()).unwrap();
    assert!(e.key_filter_stats().filter_bytes > 0);
    e.rebuild_winner_directory().unwrap();
    let stats = e.key_filter_stats();
    assert_eq!((stats.sub_filters, stats.filter_bytes, stats.resident_bytes, stats.covered_runs), (0, 0, 0, 0));
    for i in 200..400u64 {
        e.put(&kf_key(i), b"v", ver_of(&kf_key(i))).unwrap();
    }
    run_flush(&mut e, RunBuildOpts::default()).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    assert!(e.cover_scan_plan().is_none());
    let plan = e.compact_plan(RunBuildOpts::default(), None).unwrap();
    if let Some(plan) = plan {
        assert!(plan.key_sink.is_none());
        offcore_compact(&mut e, plan);
    }
    let stats = e.key_filter_stats();
    assert_eq!((stats.sub_filters, stats.filter_bytes, stats.covered_runs), (0, 0, 0));
    assert_eq!(e.key_filter.resident_bytes(), 0);
    for i in 0..400u64 {
        assert_eq!(e.version(&kf_key(i)).unwrap(), Some(ver_of(&kf_key(i))));
    }
}

fn corrupt_first_sidecar_record_length(e: &Engine<MemBlockDevice>, idx: usize) {
    let source = e.runs[idx].run.key_source(&e.alloc);
    let bs = e.device().block_size();
    let (lba, within) = source.first_sidecar_record_location(bs).expect("v3 run has a sidecar");
    let mut block = vec![0u8; bs];
    e.device().read_at(lba, &mut block).unwrap();
    let span = within..(within + 4).min(bs);
    let n = span.len();
    block[span].copy_from_slice(&0x7fff_fff0u32.to_le_bytes()[..n]);
    e.device().write_at(lba, &block).unwrap();
}

#[test]
fn failing_cover_scan_backs_off_and_covers_the_other_runs() {
    let mut e = kf_engine();
    for (round, n) in [(0u64, 400u64), (1, 150), (2, 120)] {
        for i in 0..n {
            let k = kf_key(round * 1000 + i);
            e.put(&k, b"v", ver_of(&k)).unwrap();
        }
        run::with_run_write_version(run::RUN_SIDECAR_VERSION, || run_flush(&mut e, RunBuildOpts::default())).unwrap();
    }
    e.reset_key_filter_for_test();
    assert!(e.runs.iter().all(|or| or.run.has_sidecar()));
    let largest = (0..e.runs.len()).max_by_key(|&i| e.runs[i].meta.handle.len).unwrap();
    let bad_id = e.runs[largest].id;
    corrupt_first_sidecar_record_length(&e, largest);

    let mut plan = e.cover_scan_plan().unwrap();
    assert_eq!(plan.run_id, bad_id);
    let dev = e.device().sibling().unwrap();
    let err = plan.step(&dev, 64);
    assert!(err.is_err(), "a corrupt key length must fail fast: {:?}", err.map(|_| ()));
    assert!(plan.bytes_read() <= 64, "the corrupt record must not be buffered: {}", plan.bytes_read());
    assert!(e.cover_scan_fail(&plan));
    let stats = e.key_filter_stats();
    assert_eq!((stats.cover_scan_failures, stats.cover_scan_backoff_runs), (1, 1));

    assert_eq!(cover_all(&mut e, 4096), 2, "the other runs are covered while the failing run backs off");
    assert!(e.runs.iter().all(|or| or.covered == (or.id != bad_id)));
    assert!(e.cover_scan_plan().is_none());
    let present: Vec<Vec<u8>> = (0..3u64).flat_map(|r| (0..120).map(move |i| kf_key(r * 1000 + i))).collect();
    assert_probes_match(&e, &present, "with a failing run");

    let now = std::time::Instant::now();
    let base = crate::engine::keyfilter::COVER_SCAN_RETRY_BASE;
    let retry = e.cover_scan_plan_at(now + base + std::time::Duration::from_secs(1)).unwrap();
    assert_eq!(retry.run_id, bad_id);
    assert!(e.cover_scan_fail(&retry));
    assert!(e.cover_scan_plan_at(now + base + base / 2).is_none(), "the backoff doubles");
    let again = e.cover_scan_plan_at(now + base * 4).unwrap();
    e.cover_scan_abort(&again);
    assert_eq!(e.key_filter_stats().cover_scan_failures, 2);

    e.compact_runs(RunBuildOpts::default(), None).unwrap();
    let stats = e.key_filter_stats();
    assert_eq!((stats.uncovered_runs, stats.cover_scan_backoff_runs), (0, 0));
    assert!(e.cover_scan_backoff.is_empty());
}

#[test]
fn cover_scan_key_count_mismatch_is_discarded() {
    let mut e = kf_engine();
    for i in 0..300u64 {
        e.put(&kf_key(i), b"v", ver_of(&kf_key(i))).unwrap();
    }
    run_flush(&mut e, RunBuildOpts::default()).unwrap();
    e.reset_key_filter_for_test();
    let mut plan = e.cover_scan_plan().unwrap();
    let dev = e.device().sibling().unwrap();
    while !plan.step(&dev, 1 << 20).unwrap() {}
    assert_eq!(plan.keys(), 300);
    plan.keys -= 1;
    assert!(!e.cover_scan_commit(plan));
    let stats = e.key_filter_stats();
    assert_eq!((stats.covered_runs, stats.cover_scan_mismatches, stats.cover_scan_backoff_runs), (0, 1, 1));
    assert!(e.cover_scan_plan().is_none());
}

#[test]
fn cover_scan_prefers_frozen_runs_outside_the_next_merge_suffix() {
    if isolated_env_test(&[
        ("VALS_MERGE_TIER_K", None),
        ("VALS_MERGE_TIER_SIZE_RATIO", None),
        ("VALS_MERGE_INPUT_CAP_BYTES", None),
    ]) {
        return;
    }
    let mut e = kf_engine();
    for (round, n) in [(0u64, 40u64), (1, 50), (2, 300), (3, 300), (4, 300), (5, 300), (6, 300), (7, 300)] {
        for i in 0..n {
            let k = kf_key(round * 1000 + i);
            e.put(&k, b"v", ver_of(&k)).unwrap();
        }
        run_flush(&mut e, RunBuildOpts::default()).unwrap();
    }
    e.reset_key_filter_for_test();
    let frozen = [e.runs[0].id, e.runs[1].id];
    let first = e.cover_scan_plan().unwrap();
    assert_eq!(first.run_id, frozen[1], "the larger frozen run is scanned before any soon-merged run");
    let dev = e.device().sibling().unwrap();
    let mut first = first;
    while !first.step(&dev, 1 << 20).unwrap() {}
    assert!(e.cover_scan_commit(first));
    let second = e.cover_scan_plan().unwrap();
    assert_eq!(second.run_id, frozen[0]);
    e.cover_scan_abort(&second);
}

#[test]
fn property_shadow_rebuild_preserves_coverage_and_probes() {
    if isolated_env_test(&[("VALS_MERGE_TIER_MIN_K", Some("2")), ("VALS_MERGE_TIER_K", None)]) {
        return;
    }
    let mut total = PropTally::default();
    for (seed, cfg) in lane_seeds(&[3, 13]).into_iter().filter(|(seed, _)| *seed <= 4 || *seed >= 13) {
        let t = property_run(seed, PROPERTY_OPS, PropCfg { rebuild: true, ..cfg }, KeyFilterKnockouts::default())
            .unwrap_or_else(|err| panic!("{err}"));
        eprintln!("rebuild seed {seed}: {t:?}");
        total.add(&t);
    }
    assert!(total.rebuilds_seen > 0 && total.rebuilds_completed > 0, "{total:?}");
    assert!(total.stale_flush_commits > 0 && total.interleaved_commits > 0, "{total:?}");
}

fn assert_rebuild_knockout_detected(knock: KeyFilterKnockouts) {
    for offcore_only in [false, true] {
        let detected = (1..=4).find_map(|seed| {
            let cfg = PropCfg { min_keys: 64, churn: false, offcore_only, rebuild: true, probes: 40 };
            property_run(seed, PROPERTY_OPS, cfg, knock).err()
        });
        assert!(detected.is_some(), "{knock:?} offcore_only={offcore_only} not detected");
        eprintln!("{knock:?}: {detected:?}");
    }
}

#[test]
fn property_detects_a_knocked_out_rebuild_lane() {
    if isolated_env_test(&[]) { return; }
    assert_rebuild_knockout_detected(KeyFilterKnockouts { rebuild: true, ..Default::default() });
}

#[test]
fn property_detects_a_knocked_out_rebuild_scan() {
    if isolated_env_test(&[]) { return; }
    assert_rebuild_knockout_detected(KeyFilterKnockouts { rebuild_scan: true, ..Default::default() });
}

#[test]
fn rebuild_permit_limits_and_releases_on_swap_reset_and_drop() {
    if isolated_env_test(&[]) { return; }
    let params = KeyFilterParams { rebuild: true, ..small_params(1000) };
    let mut a = KeyFilter::new(params);
    let mut b = KeyFilter::new(params);
    assert!(a.start_rebuild(1000));
    assert!(!b.start_rebuild(1000));
    a.complete_rebuild();
    assert!(b.start_rebuild(1000));
    b.reset();
    assert_eq!(b.stats().rebuilds_abandoned, 1);
    assert!(a.start_rebuild(1000));
    drop(a);
    assert!(b.start_rebuild(1000));
    let mut c = KeyFilter::new(KeyFilterParams { rebuild_concurrency: 2, ..params });
    assert!(c.start_rebuild(1000));
    assert!(!KeyFilter::new(params).start_rebuild(1000));
}

#[test]
fn rebuild_waiter_deregisters_on_reset_and_drop() {
    if isolated_env_test(&[]) { return; }
    let params = KeyFilterParams { rebuild: true, ..small_params(1000) };
    let mut holder = KeyFilter::new(params);
    assert!(holder.start_rebuild(1000));
    let mut low = KeyFilter::new(params);
    let mut high = KeyFilter::new(params);
    {
        let mut sink = high.sink(1000, 0).unwrap();
        for i in 0..1000u64 { sink.insert(&i.to_le_bytes()); }
    }
    assert!(!low.start_rebuild(1000));
    assert!(!high.start_rebuild(1000));
    holder.complete_rebuild();
    assert!(!low.start_rebuild(1000), "higher FPR waiter has priority");
    high.reset();
    assert!(low.start_rebuild(1000), "reset removes the higher-priority waiter");
    assert!(!high.start_rebuild(1000));
    drop(high);
    low.complete_rebuild();
    assert!(holder.start_rebuild(1000), "drop removes even an earlier equal-FPR waiter");
}

#[test]
fn urgent_rebuild_requires_fpr_threshold() {
    if isolated_env_test(&[]) { return; }
    let params = KeyFilterParams { rebuild: true, rebuild_urgent_fpr_ppm: 1_000, ..small_params(1000) };
    let before = crate::keyfilter::rebuild_urgent_grants();
    let mut normal = KeyFilter::new(params);
    assert!(normal.start_rebuild(1000));
    assert_eq!(crate::keyfilter::rebuild_urgent_grants(), before);
    let mut urgent = KeyFilter::new(params);
    let mut held = urgent.sink(1000, 0).unwrap();
    drop(urgent.sink(1000, 0));
    assert_eq!(urgent.sub_filters().len(), 2);
    assert!(!urgent.start_rebuild(1000), "empty subfilters have harmless FPR");
    assert_eq!(crate::keyfilter::rebuild_urgent_grants(), before);
    for i in 0..1000u64 { held.insert(&i.to_le_bytes()); }
    drop(held);
    assert!(urgent.estimated_fpr() * 1_000_000.0 >= 1_000.0);
    assert!(urgent.start_rebuild(1000));
    assert_eq!(crate::keyfilter::rebuild_urgent_grants(), before + 1);
    let mut blocked = KeyFilter::new(params);
    let mut held2 = blocked.sink(1000, 0).unwrap();
    for i in 0..1000u64 { held2.insert(&i.to_le_bytes()); }
    drop(held2);
    assert!(!blocked.start_rebuild(1000), "the extra permit is occupied");
    assert_eq!(crate::keyfilter::rebuild_urgent_grants(), before + 1);
    urgent.reset();
    assert!(blocked.start_rebuild(1000));
    assert_eq!(crate::keyfilter::rebuild_urgent_grants(), before + 2);
    drop((normal, blocked));
    assert_eq!(crate::keyfilter::rebuild_urgent_grants(), before + 2);
}

#[test]
fn reset_charges_both_lanes_until_the_sink_releases_them() {
    if isolated_env_test(&[]) { return; }
    let params = KeyFilterParams { rebuild: true, ..small_params(10_000) };
    let mut kf = KeyFilter::new(params);
    drop(kf.sink(1000, 0));
    assert!(kf.start_rebuild(1000));
    let mut held = kf.sink(1000, 0).unwrap();
    for i in 0..1000u64 { held.insert(&i.to_le_bytes()); }
    let bytes = kf.filter_bytes();
    kf.reset();
    let stats = kf.stats();
    assert_eq!(stats.filter_bytes, bytes);
    assert_eq!(stats.retired_bytes, bytes);
    assert_eq!(stats.retired_sub_filters, 2);
    assert_eq!((stats.rebuilding, stats.rebuilds_abandoned), (0, 1));
    assert!(!kf.owns(&held) && !kf.owns_rebuild(&held));
    assert!(KeyFilter::new(params).start_rebuild(1000), "reset releases the permit even while a sink holds both lanes");
    drop(held);
    kf.prune_retired();
    assert_eq!(kf.stats().filter_bytes, 0);
}

#[test]
fn old_and_dual_lane_plans_commit_conservatively_across_shadow_swap() {
    if isolated_env_test(&[]) { return; }
    let mut e = kf_engine();
    e.key_filter.set_params(KeyFilterParams { rebuild: true, rebuild_subs: 16, ..small_params(10_000) });
    for round in 0..3u64 {
        for i in 0..100u64 {
            let k = kf_key(round * 100 + i);
            e.put(&k, b"v", ver_of(&k)).unwrap();
        }
        run_flush(&mut e, RunBuildOpts::default()).unwrap();
    }
    for i in 300..400u64 {
        let k = kf_key(i);
        e.put(&k, b"v", ver_of(&k)).unwrap();
    }
    let plan = e.flush_plan(RunBuildOpts::default()).unwrap().unwrap();
    let dev = e.device().sibling().unwrap();
    let written = vals_rt::block_on(build_and_write_flush(plan, &dev), || {}).unwrap_or_else(|(_, err)| panic!("{err}"));
    assert!(e.key_filter.start_rebuild(400));
    cover_all(&mut e, 4096);
    assert!(!e.key_filter.rebuilding());
    assert!(e.key_filter_stats().retired_bytes > 0, "the old-only sink is still charged");
    e.flush_commit(written).unwrap();
    assert!(!e.runs.last().unwrap().covered, "a pre-rebuild flush cannot prove new-generation coverage");
    cover_all(&mut e, 4096);
    assert_eq!(e.key_filter_stats().retired_bytes, 0);
    assert!(e.key_filter.start_rebuild(400));
    let merge = e.compact_plan(RunBuildOpts::default(), None).unwrap().unwrap();
    let sink = merge.key_sink.as_ref().unwrap();
    assert!(!e.key_filter.owns(sink) && e.key_filter.owns_rebuild(sink), "already-active-covered inputs need only the shadow lane");
    let dev = Arc::new(e.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(merge, dev), || {}).unwrap_or_else(|(_, err)| panic!("{err}"));
    cover_all(&mut e, 4096);
    assert!(e.key_filter.owns(written.key_sink_for_test().unwrap()));
    e.compact_commit(written).unwrap();
    assert!(e.runs.iter().all(|or| or.covered));
    assert_eq!(coverage_violations(&e), 0);
    let all: Vec<Vec<u8>> = (0..400).map(kf_key).collect();
    assert_probes_match(&e, &all, "after held plans commit across swap");
    assert!(e.key_filter.start_rebuild(400));
    e.rebuild_winner_directory().unwrap();
    let stats = e.key_filter_stats();
    assert_eq!((stats.rebuilding, stats.rebuilds_abandoned, stats.filter_bytes), (0, 1, 0));
    assert!(e.runs.iter().all(|or| !or.covered && !or.rebuild_covered));
    assert_probes_match(&e, &all, "winner directory abandons shadow rebuild");
}

#[test]
fn restart_and_reset_during_shadow_rebuild_preserve_present_keys() {
    if isolated_env_test(&[]) { return; }
    let path = temp_path("kf-shadow-restart");
    let params = KeyFilterParams { rebuild: true, rebuild_subs: 2, ..small_params(100) };
    let mut e = file_engine(&path, true, params);
    for round in 0..3u64 {
        for i in 0..1500u64 {
            let k = kf_key(round * 1500 + i);
            e.put(&k, b"v", ver_of(&k)).unwrap();
        }
        run_flush(&mut e, RunBuildOpts::default()).unwrap();
    }
    assert!(e.key_filter.rebuilding());
    let mut scan = e.cover_scan_plan().unwrap();
    assert!(scan.is_rebuild_scan(), "covered runs are scanned only into the shadow lane");
    let dev = e.device().sibling().unwrap();
    assert!(!scan.step(&dev, 1).unwrap());
    let all: Vec<Vec<u8>> = (0..4500).map(kf_key).collect();
    assert_probes_match(&e, &all, "mid rebuild");
    e.sync().unwrap();
    drop(scan);
    drop(e);
    let mut e = file_engine(&path, false, params);
    assert!(!e.key_filter.rebuilding());
    assert!(e.runs.iter().all(|or| !or.covered && !or.rebuild_covered));
    assert_probes_match(&e, &all, "restart during rebuild");
    let reopened = e.cover_scan_plan().unwrap();
    assert!(!reopened.is_rebuild_scan(), "reopened runs are active-uncovered");
    e.cover_scan_abort(&reopened);
    cover_all(&mut e, 4096);
    if !e.key_filter.rebuilding() { assert!(e.key_filter.start_rebuild(4500)); }
    let mut scan = e.cover_scan_plan().unwrap();
    let dev = e.device().sibling().unwrap();
    while !scan.step(&dev, 4096).unwrap() {}
    e.reset_key_filter_for_test();
    assert!(!e.cover_scan_commit(scan));
    assert_eq!(e.key_filter_stats().rebuilds_abandoned, 1);
    assert_probes_match(&e, &all, "reset during rebuild");
    cover_all(&mut e, 4096);
    assert_eq!(coverage_violations(&e), 0);
    drop(e);
    std::fs::remove_file(path).unwrap();
}

pub(super) fn process_rss() -> u64 {
    process_status_bytes("VmRSS:")
}

#[cfg(target_os = "linux")]
fn process_anon_rss() -> u64 {
    process_status_bytes("RssAnon:")
}

fn process_status_bytes(field: &str) -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    status.lines().find(|l| l.starts_with(field)).unwrap()
        .split_whitespace().nth(1).unwrap().parse::<u64>().unwrap() * 1024
}

#[test]
#[cfg(target_os = "linux")]
fn rebuilt_filter_has_scaled_resident_and_compound_fpr_bounds() {
    if isolated_env_test(&[]) { return; }
    let n = if full_lane() { 50_000_000u64 } else { 4_000_000 };
    let batch_keys = n / 20;
    let before = process_anon_rss();
    let mut kf = KeyFilter::new(KeyFilterParams { rebuild: true, ..small_params(batch_keys) });
    for batch in 0..20 {
        let mut sink = kf.sink(batch_keys, batch * batch_keys).unwrap();
        for i in batch * batch_keys..(batch + 1) * batch_keys {
            sink.insert(&i.to_le_bytes());
        }
    }
    assert!(kf.start_rebuild(n));
    for batch in 0..20 {
        let mut sink = kf.sink_lanes(0, batch_keys, n).unwrap();
        for i in batch * batch_keys..(batch + 1) * batch_keys {
            sink.insert(&i.to_le_bytes());
        }
    }
    let peak = kf.stats().filter_bytes;
    kf.complete_rebuild();
    let stats = kf.stats();
    assert!(stats.filter_bytes as f64 <= 1.8 * n as f64, "{stats:?}");
    assert!(stats.resident_bytes <= stats.filter_bytes);
    assert_eq!(stats.rebuild_peak_bytes, peak);
    let rss_delta = process_anon_rss().saturating_sub(before);
    eprintln!("KF396 scaled bound keys={n} bytes={} resident={} rss_delta={rss_delta} peak={peak}", stats.filter_bytes, stats.resident_bytes);
    assert!(rss_delta <= (1.8 * n as f64) as u64 + 512 * 1024, "RSS delta {rss_delta}: {stats:?}");
    for batch in 20..28 {
        let mut sink = kf.sink(batch_keys, batch * batch_keys).unwrap();
        for i in batch * batch_keys..(batch + 1) * batch_keys {
            sink.insert(&i.to_le_bytes());
        }
    }
    let total = 28 * batch_keys;
    assert_eq!(kf.sub_filters().len(), 3);
    assert!(kf.filter_bytes() as f64 <= 1.8 * total as f64);
    for i in 0..total { assert!(kf.may_contain(&i.to_le_bytes())); }
    let probes = 200_000u64;
    let fp = (total..total + probes).filter(|i| kf.may_contain(&i.to_le_bytes())).count();
    assert!(fp as f64 / (probes as f64) < 0.02, "{fp} false positives");
    assert!(kf.estimated_fpr() < 0.02);
}

#[test]
#[ignore]
fn keyfilter_microbench_probe_version_vs_run_count() {
    let keys_per_run: u64 = std::env::var("VALS_KF_BENCH_KEYS_PER_RUN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000);
    let probes: u64 = 400_000;
    for runs in [3u64, 10, 20] {
        let blocks = (runs * keys_per_run * 160 / 4096 + 4096) * 2;
        let dev = MemBlockDevice::new(4096, blocks);
        let mut e = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 256,
                reserved_blocks: 0,
                ordering: Ordering::Unsorted,
            },
        );
        e.key_filter.set_params(KeyFilterParams {
            min_keys: (runs * keys_per_run / 8).max(1),
            ..KeyFilterParams::from_env()
        });
        for r in 0..runs {
            for i in 0..keys_per_run {
                let k = kf_key(i * runs + r);
                e.put(&k, b"v", (i * runs + r) as u128 + 1).unwrap();
            }
            run_flush(&mut e, RunBuildOpts::default()).unwrap();
        }
        let absent: Vec<Vec<u8>> = (0..probes).map(|i| absent_key(i * (keys_per_run * runs / probes).max(1))).collect();
        let time = |e: &Engine<MemBlockDevice>| {
            let started = std::time::Instant::now();
            let mut hits = 0u64;
            for k in &absent {
                if !matches!(e.probe_version(k), StagedVersion::Absent) {
                    hits += 1;
                }
            }
            (started.elapsed().as_nanos() as f64 / probes as f64, hits)
        };
        let before_stats = e.key_filter_stats();
        let (covered_ns, covered_hits) = time(&e);
        let after_stats = e.key_filter_stats();
        let negatives = (after_stats.negatives - before_stats.negatives) as f64 / probes as f64;
        let run_probes = (after_stats.run_probes - before_stats.run_probes) as f64 / probes as f64;
        let saved_subs = std::mem::take(e.key_filter.subs_for_bench());
        for or in &mut e.runs {
            or.covered = false;
        }
        let (baseline_ns, baseline_hits) = time(&e);
        *e.key_filter.subs_for_bench() = saved_subs;
        let half = e.runs.len() / 2;
        for (i, or) in e.runs.iter_mut().enumerate() {
            or.covered = i < half;
        }
        let (half_ns, _) = time(&e);
        eprintln!(
            "KFBENCH runs={runs} keys/run={keys_per_run} baseline(no filter)={baseline_ns:.1}ns hits={baseline_hits} \
             covered={covered_ns:.1}ns hits={covered_hits} negatives={negatives:.4} run_probes/probe={run_probes:.3} \
             half_uncovered={half_ns:.1}ns sub_filters={} filter_bytes={}",
            after_stats.sub_filters, after_stats.filter_bytes
        );
    }
}

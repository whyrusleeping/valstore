use super::*;
use crate::range_tombstone::{CoverDelta, Fragment};

const RD_BLOCKS: u64 = 1 << 14;

fn rd_cfg() -> Config {
    Config { seg_blocks: 8, reserved_blocks: 0, ordering: Ordering::Sorted }
}

fn rd_engine() -> Engine<MemBlockDevice> {
    Engine::create_prewiped(MemBlockDevice::new(BS, RD_BLOCKS), rd_cfg())
}

fn v(n: u64) -> u128 {
    (n as u128) << 64 | 7
}

fn reopen(e: Engine<MemBlockDevice>) -> Engine<MemBlockDevice> {
    let dev = e.device().share();
    drop(e);
    Engine::open(dev, rd_cfg()).unwrap()
}

fn fragments(e: &Engine<MemBlockDevice>) -> Vec<Fragment> {
    e.range_tombstones().fragments().collect()
}

fn frag(lo: &[u8], hi: Option<&[u8]>, n: u64) -> Fragment {
    Fragment { lo: lo.to_vec(), hi: hi.map(<[u8]>::to_vec), version: v(n) }
}

fn persisted_seqs(e: &Engine<MemBlockDevice>) -> Vec<u64> {
    let mut seqs: Vec<u64> = e.runs.iter().flat_map(|or| or.rts.iter().map(|r| r.seq)).collect();
    seqs.sort_unstable();
    seqs
}

fn assert_each_record_once(e: &Engine<MemBlockDevice>) {
    let mut holder: std::collections::HashMap<u64, Option<usize>> = std::collections::HashMap::new();
    let held = e.runs.iter().enumerate().flat_map(|(i, or)| or.rts.iter().map(move |r| (r.seq, Some(i))));
    for (seq, at) in held.chain(e.rts.tail.iter().map(|(r, _)| (r.seq, None))) {
        assert!(holder.insert(seq, at).is_none_or(|was| was == at), "a range tombstone record is held twice");
    }
    for or in &e.runs {
        assert_eq!(or.rts, or.run.read_range_tombstones(&e.alloc).unwrap(), "cached records match the run's RT block");
    }
    assert_eq!(*e.range_tombstones(), FragmentSet::from_records(&e.range_tombstone_records()));
}

fn assert_manifest_fenced(e: &Engine<MemBlockDevice>, what: &str) {
    let (man, _) = snapshot_io::load_manifest(e.device(), &e.alloc).unwrap().expect("a committed manifest");
    let versions: Vec<u16> = man
        .runs
        .iter()
        .map(|m| DeviceRun::open(e.dev.clone(), &e.alloc, m.handle.clone()).unwrap().format_version())
        .collect();
    assert!(versions.contains(&run::RUN_VERSION), "{what}: committed manifest has no v6 run: {versions:?}");
}

fn merge_env() -> bool {
    isolated_env_test(&[
        ("VALS_MERGE_TIER_MIN_K", Some("2")),
        ("VALS_VLOG_CO_COMPACT", Some("1")),
        ("VALS_MERGE_FORCE_RUNS", None),
        ("VALS_MERGE_INPUT_CAP_BYTES", None),
        ("VALS_RUN_WRITE_VERSION", None),
    ])
}

fn put_range(e: &mut Engine<MemBlockDevice>, prefix: &str, n: u64, ver: u64) {
    for i in 0..n {
        e.put(format!("{prefix}{i:04}").as_bytes(), b"value", v(ver)).unwrap();
    }
}

#[test]
fn apply_overlays_appends_and_reports_no_ops_and_refusals() {
    let mut e = rd_engine();
    put_range(&mut e, "k", 10, 1);
    let applied = e.apply_range_delete(Some(b"k0002"), Some(b"k0005"), v(5)).unwrap();
    let seq = applied.seq.expect("an overlay that changes F appends a record");
    assert_eq!(
        applied.delta,
        vec![CoverDelta { lo: b"k0002".to_vec(), hi: Some(b"k0005".to_vec()), old: None, new: v(5) }]
    );
    assert_eq!(e.range_tombstone_cover(b"k0003"), Some(v(5)));
    assert_eq!(e.range_tombstone_cover(b"k0005"), None);
    let next_seq = e.log.next_seq();
    assert_eq!(next_seq, seq + 1);
    let noop = e.apply_range_delete(Some(b"k0003"), Some(b"k0004"), v(4)).unwrap();
    assert_eq!(noop, RangeDeleteApplied { seq: None, delta: Vec::new() });
    assert_eq!(e.log.next_seq(), next_seq, "an implied range delete appends nothing");
    assert!(matches!(e.apply_range_delete(Some(b"b"), Some(b"a"), v(9)), Err(EngineError::InvalidRange)));
    assert!(matches!(e.apply_range_delete(Some(b"b"), Some(b"b"), v(9)), Err(EngineError::InvalidRange)));
    e.set_range_tombstone_max_fragments(1);
    assert!(matches!(e.apply_range_delete(Some(b"x"), Some(b"y"), v(9)), Err(EngineError::RangeTombstoneCap(_))));
    assert_eq!(e.log.next_seq(), next_seq, "a refused range delete appends nothing");
    assert_eq!(fragments(&e), vec![frag(b"k0002", Some(b"k0005"), 5)]);
    run::with_run_write_version(run::RUN_BLOCK_VERSION, || {
        assert!(matches!(e.apply_range_delete(None, None, v(9)), Err(EngineError::RangeTombstonesUnsupported)));
    });
    let mut unsorted = Engine::create_prewiped(
        MemBlockDevice::new(BS, RD_BLOCKS),
        Config { ordering: Ordering::Unsorted, ..rd_cfg() },
    );
    assert!(matches!(unsorted.apply_range_delete(None, None, v(1)), Err(EngineError::NotSorted)));
}

#[test]
fn first_range_delete_commits_a_v6_fence_before_appending() {
    let mut e = rd_engine();
    assert!(snapshot_io::load_manifest(e.device(), &e.alloc).unwrap().is_none());
    e.apply_range_delete(None, Some(b"m"), v(3)).unwrap();
    assert_manifest_fenced(&e, "first RT on an engine without runs");
    assert_eq!(e.runs.len(), 1);
    assert_eq!((e.runs[0].meta.entry_count, e.runs[0].rts.len()), (0, 0), "an empty marker run");

    let mut legacy = rd_engine();
    run::with_run_write_version(run::RUN_BLOCK_VERSION, || {
        put_range(&mut legacy, "a", 20, 1);
        legacy.flush(RunBuildOpts::default()).unwrap();
    });
    assert!(!legacy.has_range_tombstone_fence());
    legacy.apply_range_delete(Some(b"a0003"), Some(b"a0009"), v(2)).unwrap();
    assert_manifest_fenced(&legacy, "first RT on an engine with only v5 runs");
    assert_eq!(legacy.runs.len(), 2);
    let before = legacy.manifest_generation();
    legacy.apply_range_delete(Some(b"a0010"), None, v(3)).unwrap();
    assert_eq!(legacy.manifest_generation(), before, "the fence is committed once");
}

#[test]
fn reopen_recovers_fragments_from_the_synced_tail_runs_and_merges() {
    if merge_env() {
        return;
    }
    let mut e = rd_engine();
    put_range(&mut e, "k", 40, 1);
    e.apply_range_delete(Some(b"k0005"), Some(b"k0015"), v(4)).unwrap();
    e.apply_range_delete(Some(b"k0010"), Some(b"k0020"), v(6)).unwrap();
    e.apply_range_delete(None, Some(b"k0001"), v(2)).unwrap();
    let want = fragments(&e);
    assert_eq!(want.len(), 3);
    e.sync().unwrap();
    let mut e = reopen(e);
    assert_eq!(fragments(&e), want, "synced tail");
    assert_eq!(persisted_seqs(&e), Vec::<u64>::new());
    assert_each_record_once(&e);

    e.flush(RunBuildOpts::default()).unwrap();
    assert_eq!(persisted_seqs(&e).len(), 3);
    assert!(!e.has_unflushed_range_tombstones());
    let mut e = reopen(e);
    assert_eq!(fragments(&e), want, "records persisted in a run");
    assert_each_record_once(&e);

    e.flush(RunBuildOpts::default()).unwrap();
    assert!(e.index.is_empty(), "test premise: the replayed tail is flushed");
    e.apply_range_delete(Some(b"k0030"), None, v(8)).unwrap();
    let want = fragments(&e);
    e.flush(RunBuildOpts::default()).unwrap();
    let rt_only = e.runs.last().unwrap();
    assert_eq!((rt_only.meta.entry_count, rt_only.rts.len()), (0, 1), "an RT-only flush");
    assert_eq!((rt_only.meta.min_key.as_slice(), rt_only.meta.max_key.as_slice()), (&[][..], &[][..]));
    put_range(&mut e, "z", 5, 9);
    e.flush(RunBuildOpts::default()).unwrap();
    let mut e = reopen(e);
    assert_eq!(fragments(&e), want);
    assert_each_record_once(&e);

    let runs_before = e.runs.len();
    let plan = e.compact_plan(RunBuildOpts::default(), None).unwrap().expect("a merge");
    let inputs = plan.merged_inputs.len();
    let written = vals_rt::block_on(compact_and_write(plan, e.dev.clone()), || {}).unwrap();
    e.compact_commit(written).unwrap();
    assert_eq!(e.runs.len(), runs_before - inputs + 1);
    assert_eq!(persisted_seqs(&e).len(), 4, "a streamed merge moves its inputs' records into its output");
    assert_each_record_once(&e);
    let mut e = reopen(e);
    assert_eq!(fragments(&e), want, "after a streamed merge");
    e.compact_runs(RunBuildOpts::default(), None).unwrap();
    assert_eq!(e.runs.len(), 1);
    assert_eq!(e.runs[0].rts.len(), 4);
    let e = reopen(e);
    assert_eq!(fragments(&e), want, "after a full merge");
    assert_each_record_once(&e);
    for i in 0..40 {
        let covered = i == 0 || (5..20).contains(&i) || i >= 30;
        let key = format!("k{i:04}");
        assert_eq!(e.get(key.as_bytes()).unwrap().is_none(), covered, "{key}");
        assert_eq!(e.version(key.as_bytes()).unwrap(), e.range_tombstone_cover(key.as_bytes()).or(Some(v(1))), "{key}");
    }
}

#[test]
fn losing_an_unsynced_range_delete_loses_nothing_it_covered() {
    let mut e = rd_engine();
    put_range(&mut e, "k", 20, 1);
    e.flush(RunBuildOpts::default()).unwrap();
    put_range(&mut e, "m", 20, 1);
    e.sync().unwrap();
    e.apply_range_delete(Some(b"k0005"), Some(b"m0010"), v(5)).unwrap();
    assert!(e.range_tombstone_cover(b"m0001").is_some());
    let e = reopen(e);
    assert!(e.range_tombstones().is_empty(), "the unsynced record is gone");
    assert!(e.range_tombstone_records().is_empty());
    for prefix in ["k", "m"] {
        for i in 0..20 {
            let key = format!("{prefix}{i:04}");
            assert_eq!(e.version(key.as_bytes()).unwrap(), Some(v(1)), "{key}");
        }
    }
    assert_manifest_fenced(&e, "the fence run committed before the lost append");
}

#[test]
fn async_flush_persists_only_records_it_covers_and_keeps_later_tail_segments_protected() {
    let mut e = rd_engine();
    put_range(&mut e, "k", 50, 1);
    e.apply_range_delete(Some(b"k0001"), Some(b"k0003"), v(2)).unwrap();
    let plan = e.flush_plan(RunBuildOpts::default()).unwrap().unwrap();
    let late = e.apply_range_delete(Some(b"k0010"), Some(b"k0020"), v(3)).unwrap().seq.unwrap();
    let written = vals_rt::block_on(build_and_write_flush(plan, e.device()), || {}).unwrap();
    e.flush_commit(written).unwrap();
    assert_eq!(persisted_seqs(&e).len(), 1);
    assert_eq!(e.rts.tail.iter().map(|(r, _)| r.seq).collect::<Vec<_>>(), vec![late]);
    let late_seg = e.rts.tail[0].1.segment;
    assert!(e.protected_segments().contains(&late_seg), "an unflushed range delete's segment stays protected");
    assert_eq!(e.reclaim_orphan_segments(), 0);
    e.sync().unwrap();
    let want = fragments(&e);
    let e = reopen(e);
    assert_eq!(fragments(&e), want);
    assert_eq!(e.rts.tail.len(), 1, "the late record replays from the tail");
    assert_each_record_once(&e);
}

#[test]
fn every_committed_manifest_keeps_a_v6_run_once_a_range_delete_was_accepted() {
    if merge_env() {
        return;
    }
    let mut e = rd_engine();
    run::with_run_write_version(run::RUN_BLOCK_VERSION, || {
        for round in 0..3 {
            put_range(&mut e, &format!("r{round}/"), 30, round + 1);
            e.flush(RunBuildOpts::default()).unwrap();
        }
    });
    e.apply_range_delete(Some(b"r1/"), Some(b"r2/"), v(10)).unwrap();
    assert_manifest_fenced(&e, "fence");
    run::with_run_write_version(run::RUN_BLOCK_VERSION, || {
        put_range(&mut e, "r3/", 30, 11);
        e.flush(RunBuildOpts::default()).unwrap();
        assert_manifest_fenced(&e, "v5 flush carrying the tail record");
        assert!(e.runs.last().unwrap().run.supports_range_tombstones(), "a flush with records writes v6");
        let mut merges = 0;
        while let Some(plan) = e.compact_plan(RunBuildOpts::default(), None).unwrap() {
            let written = vals_rt::block_on(compact_and_write(plan, e.dev.clone()), || {}).unwrap();
            e.compact_commit(written).unwrap();
            assert_manifest_fenced(&e, "streamed merge under the v5 rollback knob");
            merges += 1;
        }
        assert!(merges > 0, "test premise: the knob still merges");
        let keys: std::collections::HashMap<Vec<u8>, u128> =
            (0..30).map(|i| (format!("r0/{i:04}").into_bytes(), v(1))).collect();
        let plan = e.forget_plan(RunBuildOpts::default(), &keys).unwrap().expect("test premise: a forget plan");
        let written = vals_rt::block_on(compact_and_write(plan, e.dev.clone()), || {}).unwrap();
        e.compact_commit(written).unwrap();
        assert_manifest_fenced(&e, "forget plan");
        e.compact_runs(RunBuildOpts::default(), Some(u128::MAX)).unwrap();
        assert_manifest_fenced(&e, "full merge with tombstone GC");
    });
    let e = reopen(e);
    assert_manifest_fenced(&e, "reopen");
    assert_eq!(e.range_tombstone_cover(b"r1/0001"), Some(v(10)));
}

#[test]
fn rt_only_runs_are_served_merged_digested_and_covered_like_any_run() {
    let mut e = rd_engine();
    put_range(&mut e, "a", 30, 1);
    e.flush(RunBuildOpts::default()).unwrap();
    e.apply_range_delete(Some(b"a0010"), Some(b"a0012"), v(4)).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    put_range(&mut e, "b", 30, 2);
    e.flush(RunBuildOpts::default()).unwrap();
    assert_eq!(e.runs.iter().map(|or| or.meta.entry_count).collect::<Vec<_>>(), vec![30, 0, 30]);

    assert_eq!(e.get(b"a0011").unwrap(), None);
    assert_eq!(e.get(b"a0012").unwrap().as_deref(), Some(&b"value"[..]));
    assert_eq!(e.get(b"").unwrap(), None);
    let (page, more) = e.scan_bounded(None, None, usize::MAX, usize::MAX).unwrap();
    assert!(!more);
    assert_eq!(page.len(), 58);
    let digests = e.version_digest_page(&all_buckets(), vdp_bucket, None, usize::MAX, 0).unwrap();
    assert_eq!(digests.0.len(), 60);
    let stats = e.capacity_stats();
    assert_eq!((stats.run_count, stats.run_entries), (3, 60));
    let _ = (e.reclaimable_fraction(), e.shadowed_bytes_estimate_cached());
    assert_eq!(super::runformat::cover_every_run(&mut e), 3);

    let plan = e.compact_plan(RunBuildOpts::default(), None).unwrap();
    if let Some(plan) = plan {
        let written = vals_rt::block_on(compact_and_write(plan, e.dev.clone()), || {}).unwrap();
        e.compact_commit(written).unwrap();
    }
    e.compact_runs(RunBuildOpts::default(), None).unwrap();
    assert_eq!(e.runs.len(), 1);
    assert_eq!((e.runs[0].meta.entry_count, e.runs[0].rts.len()), (60, 1));
    let e = reopen(e);
    assert_eq!(e.scan_bounded(None, None, usize::MAX, usize::MAX).unwrap().0.len(), 58);
    assert_eq!(e.range_tombstone_cover(b"a0011"), Some(v(4)));
}

#[test]
fn truncate_forgets_range_tombstones() {
    let mut e = rd_engine();
    e.apply_range_delete(None, None, v(1)).unwrap();
    e.truncate().unwrap();
    assert!(e.range_tombstones().is_empty() && e.range_tombstone_records().is_empty());
    let e = reopen(e);
    assert!(e.range_tombstones().is_empty());
}

#[test]
fn co_compaction_keeps_the_segment_of_an_unflushed_range_delete() {
    if merge_env() {
        return;
    }
    let mut e = rd_engine();
    for i in 0..20u32 {
        e.put(format!("a{i:04}").as_bytes(), &[1u8; 300], v(1)).unwrap();
    }
    e.flush(RunBuildOpts::default()).unwrap();
    for i in 0..20u32 {
        e.put(format!("b{i:04}").as_bytes(), &[2u8; 300], v(2)).unwrap();
    }
    let plan = e.flush_plan(RunBuildOpts::default()).unwrap().unwrap();
    let late = e.apply_range_delete(Some(b"a0003"), Some(b"a0007"), v(5)).unwrap().seq.unwrap();
    let late_seg = e.rts.tail.iter().find(|(r, _)| r.seq == late).unwrap().1.segment;
    for i in 0..20u32 {
        e.put(format!("c{i:04}").as_bytes(), &[3u8; 300], v(3)).unwrap();
    }
    assert_ne!(e.log.open_segment(), Some(late_seg), "test premise: the log rolled past the record");
    let written = vals_rt::block_on(build_and_write_flush(plan, e.device()), || {}).unwrap();
    e.flush_commit(written).unwrap();
    assert!(e.has_unflushed_range_tombstones());

    let plan = e.compact_plan(RunBuildOpts::default(), None).unwrap().expect("a merge of both runs");
    assert!(plan.co_compacts(), "test premise: the merge rewrites and frees its inputs' value segments");
    let written = vals_rt::block_on(compact_and_write(plan, e.dev.clone()), || {}).unwrap();
    e.compact_commit(written).unwrap();
    assert!(!e.alloc.is_free(late_seg), "the unflushed range delete's segment must survive co-compaction");
    e.sync().unwrap();
    let want = fragments(&e);
    let e = reopen(e);
    assert_eq!(fragments(&e), want);
    assert_eq!(e.range_tombstone_cover(b"a0005"), Some(v(5)));
}

#[test]
fn a_range_delete_that_is_the_first_record_survives_reopen() {
    let mut e = rd_engine();
    let applied = e.apply_range_delete(Some(b"a"), Some(b"b"), v(1)).unwrap();
    assert!(applied.seq.is_some_and(|s| s > e.last_flush_seq), "the record sorts above the fence manifest's covered_seq");
    e.sync().unwrap();
    let want = fragments(&e);
    let e = reopen(e);
    assert_eq!(fragments(&e), want);
    assert_eq!(e.range_tombstone_records().len(), 1);
}

#[test]
fn an_unflushed_first_put_survives_the_fence_manifest_and_reopen() {
    let mut e = rd_engine();
    e.put(b"z", b"value", v(1)).unwrap();
    assert_eq!(e.mutable_entry(b"z").map(|m| m.seq), Some(0), "premise: the put is seq 0");
    e.apply_range_delete(Some(b"a"), Some(b"b"), v(2)).unwrap();
    assert_eq!(e.manifest_generation(), 1, "premise: a fence manifest with covered_seq 0 and no flush");
    e.sync().unwrap();
    let e = reopen(e);
    assert_eq!(e.get(b"z").unwrap().as_deref(), Some(&b"value"[..]));
}

#[test]
fn maintenance_reserve_keeps_room_to_flush_an_rt_only_tail() {
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 512), rd_cfg());
    e.set_maint_reserve_enabled(true);
    let mut accepted = 0u64;
    let refused = loop {
        match e.apply_range_delete(None, None, v(accepted + 1)) {
            Ok(_) => accepted += 1,
            Err(err) => break err,
        }
        assert!(accepted < 100_000, "test premise: the device fills");
    };
    assert!(refused.is_backpressure() || refused.is_device_full(), "{refused}");
    assert!(accepted > 100, "test premise: many records accumulated ({accepted})");
    let plan = e.flush_plan(RunBuildOpts::default()).unwrap().expect("the reserve leaves room to flush the tail");
    let written = vals_rt::block_on(build_and_write_flush(plan, e.device()), || {}).unwrap();
    e.flush_commit(written).unwrap();
    assert!(!e.has_unflushed_range_tombstones());
    assert_eq!(e.range_tombstone_records().len() as u64, accepted);
    let want = fragments(&e);
    let dev = e.device().share();
    drop(e);
    let e = Engine::open(dev, rd_cfg()).unwrap();
    assert_eq!(fragments(&e), want);
}

fn small_blocks() -> RunBuildOpts {
    RunBuildOpts { block_target_bytes: 128, ..RunBuildOpts::default() }
}

fn after(k: &[u8]) -> Vec<u8> {
    let mut k = k.to_vec();
    k.push(0);
    k
}

fn scan_all(e: &Engine<MemBlockDevice>, lo: Option<&[u8]>, hi: Option<&[u8]>, page: usize) -> (Vec<(Vec<u8>, u128)>, RtScanStats) {
    let mut out = Vec::new();
    let mut total = RtScanStats::default();
    let mut cursor = lo.map(<[u8]>::to_vec);
    loop {
        let ((entries, more), stats) = e.scan_winners_page_counted(cursor.as_deref(), hi, page, usize::MAX).unwrap();
        add_stats(&mut total, stats);
        let Some((last, _)) = entries.last() else {
            assert!(!more, "a truncated page carries entries");
            break;
        };
        cursor = Some(after(last));
        out.extend(entries.into_iter().map(|(k, ie)| (k, ie.version)));
        if !more {
            break;
        }
    }
    let staged = scan_all_staged(e, lo, hi, page, SCAN_COVERED_BUDGET);
    assert_eq!(staged.entries, out, "the staged scan matches the synchronous scan for {lo:?}..{hi:?} page {page}");
    (out, total)
}

fn add_stats(total: &mut RtScanStats, s: RtScanStats) {
    total.filtered_entries += s.filtered_entries;
    total.skipped_blocks += s.skipped_blocks;
    total.block_reads += s.block_reads;
    total.index_probes += s.index_probes;
}

struct StagedScanAll {
    entries: Vec<(Vec<u8>, u128)>,
    stats: RtScanStats,
    pages: Vec<(usize, Option<Vec<u8>>, RtScanStats)>,
}

fn staged_page(
    e: &Engine<MemBlockDevice>,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
    page: usize,
    budget: ScanCoveredBudget,
) -> StagedScanDone {
    match e.stage_scan_page_covered(lo, hi, page, usize::MAX, budget).unwrap() {
        StagedScanPage::Done(done) => done,
        StagedScanPage::Runs(mut merge) => {
            let dev = e.device();
            vals_rt::block_on(
                async {
                    loop {
                        match merge.step(dev).await? {
                            StagedScanStep::Done => break,
                            StagedScanStep::NeedMem { from } => e.scan_supply_mem(&mut merge, &from)?,
                            StagedScanStep::NeedBlocks { cursor } => e.scan_supply_blocks(&mut merge, cursor)?,
                        }
                    }
                    Ok::<(), EngineError>(())
                },
                || {},
            )
            .unwrap();
            merge.finish()
        }
    }
}

fn scan_all_staged(
    e: &Engine<MemBlockDevice>,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
    page: usize,
    budget: ScanCoveredBudget,
) -> StagedScanAll {
    let mut all = StagedScanAll { entries: Vec::new(), stats: RtScanStats::default(), pages: Vec::new() };
    let mut cursor = lo.map(<[u8]>::to_vec);
    loop {
        let done = staged_page(e, cursor.as_deref(), hi, page, budget);
        let (resume, stats) = (done.resume().map(<[u8]>::to_vec), done.rt_stats());
        add_stats(&mut all.stats, stats);
        let (entries, more) = (done.winners, done.more);
        all.pages.push((entries.len(), resume.clone(), stats));
        if let Some(r) = &resume {
            assert!(more, "a resume key comes with more");
            assert!(cursor.as_deref().is_none_or(|c| r.as_slice() > c), "the resume key is strictly above lo");
            assert!(entries.last().is_none_or(|(k, _)| r.as_slice() > k.as_slice()));
        }
        let next = resume.or_else(|| entries.last().map(|(k, _)| after(k)));
        all.entries.extend(entries.into_iter().map(|(k, ie)| (k, ie.version)));
        if !more {
            break;
        }
        cursor = Some(next.expect("a truncated page carries entries or a resume key"));
        assert!(all.pages.len() < 100_000, "the staged scan makes progress");
    }
    all
}

type Model = std::collections::BTreeMap<Vec<u8>, u128>;

fn model_put(e: &mut Engine<MemBlockDevice>, model: &mut Model, key: &[u8], version: u128) {
    e.put(key, b"value", version).unwrap();
    let w = model.entry(key.to_vec()).or_insert(version);
    *w = (*w).max(version);
}

fn model_scan(e: &Engine<MemBlockDevice>, model: &Model, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Vec<(Vec<u8>, u128)> {
    model
        .iter()
        .filter(|(k, _)| lo.is_none_or(|l| k.as_slice() >= l) && hi.is_none_or(|h| k.as_slice() < h))
        .filter(|(k, v)| e.range_tombstones().cover(k).is_none_or(|c| **v > c))
        .map(|(k, v)| (k.clone(), *v))
        .collect()
}

fn block_firsts(e: &Engine<MemBlockDevice>, run: usize) -> Vec<Vec<u8>> {
    let r = &e.runs[run].run;
    (0..r.index_len()).map(|i| r.block_first_key(i).to_vec()).collect()
}

#[test]
fn skip_scan_returns_every_live_key_around_range_tombstone_bounds_inside_blocks() {
    let mut e = rd_engine();
    let mut model = Model::new();
    for i in 0..200 {
        model_put(&mut e, &mut model, format!("k{i:05}").as_bytes(), v(1));
    }
    e.flush(small_blocks()).unwrap();
    for i in 0..60 {
        model_put(&mut e, &mut model, format!("m{i:05}").as_bytes(), v(1));
    }
    e.flush(small_blocks()).unwrap();
    for i in 0..40 {
        model_put(&mut e, &mut model, format!("p{i:05}").as_bytes(), v(1));
    }
    e.flush(small_blocks()).unwrap();
    let (k_firsts, m_firsts) = (block_firsts(&e, 0), block_firsts(&e, 1));
    assert!(k_firsts.len() > 20 && m_firsts.len() > 5, "test premise: multi-block runs");
    let lo_block = k_firsts.iter().position(|f| f.as_slice() > b"k00040".as_slice()).unwrap();
    let hi_block = k_firsts.iter().position(|f| f.as_slice() > b"k00150".as_slice()).unwrap();
    let lo = after(&k_firsts[lo_block]);
    let hi = after(&k_firsts[hi_block]);
    assert!(lo.as_slice() < k_firsts[lo_block + 1].as_slice() && hi.as_slice() < k_firsts[hi_block + 1].as_slice());
    let m_last = m_firsts.last().unwrap().clone();
    let m_hi = after(&m_last);
    assert!(m_hi.as_slice() < b"m00059".as_slice(), "test premise: the last block holds keys at and above hi");

    model_put(&mut e, &mut model, b"k00100", v(9));
    e.flush(small_blocks()).unwrap();
    model_put(&mut e, &mut model, b"k00120", v(9));
    e.apply_range_delete(Some(&lo), Some(&hi), v(5)).unwrap();
    e.apply_range_delete(Some(b"m00010"), Some(&m_hi), v(5)).unwrap();
    e.apply_range_delete(Some(b"p00005"), None, v(5)).unwrap();
    model_put(&mut e, &mut model, b"k00130", v(8));
    model_put(&mut e, &mut model, b"k00131", v(3));
    assert_eq!(e.range_tombstone_max_tree_blocks(), 0, "trees wait for the explicit first-RT call");
    let (pre_fold, pre_stats) = scan_all(&e, None, None, usize::MAX);
    assert_eq!(pre_fold, model_scan(&e, &model, None, None));
    assert_eq!(pre_stats.skipped_blocks, 0, "without a tree runs are filtered entry by entry");
    let staged = scan_all_staged(&e, None, None, usize::MAX, SCAN_COVERED_BUDGET);
    let pre_staged_reads = staged.stats.block_reads;
    assert_eq!(staged.stats.skipped_blocks, 0, "staged: without a tree runs are filtered entry by entry");
    assert!(staged.stats.filtered_entries > 100, "staged: the covered memtable and run entries are filtered: {:?}", staged.stats);
    assert!(e.build_range_tombstone_max_trees() > 0);

    let want = model_scan(&e, &model, None, None);
    for (k, w) in [(&k_firsts[lo_block], true), (&k_firsts[hi_block], false), (&k_firsts[hi_block + 1], true)] {
        assert_eq!(want.iter().any(|(wk, _)| wk == k), w, "test premise: {k:?}");
    }
    assert!(want.iter().any(|(k, _)| k.as_slice() == b"m00059") && want.iter().any(|(k, _)| k.as_slice() == b"p00004"));
    for page in [usize::MAX, 3] {
        let (got, stats) = scan_all(&e, None, None, page);
        assert_eq!(got, want, "page {page}");
        assert!(stats.skipped_blocks > 10, "the covered interiors are skipped: {stats:?}");
        assert!(stats.filtered_entries > 0, "boundary blocks are filtered: {stats:?}");
        let staged = scan_all_staged(&e, None, None, page, SCAN_COVERED_BUDGET);
        assert!(staged.stats.skipped_blocks > 10, "staged: the covered interiors are skipped: {:?}", staged.stats);
        assert!(staged.stats.filtered_entries > 0, "staged: boundary blocks are filtered: {:?}", staged.stats);
        if page == usize::MAX {
            assert!(staged.stats.block_reads < pre_staged_reads / 2, "staged: skipped blocks are never read: {:?}", staged.stats);
        }
        assert!(staged.pages.iter().all(|(_, resume, _)| resume.is_none()), "no page reaches the covered-work budget");
    }
    for (lo, hi) in [
        (Some(b"k00080".to_vec()), None),
        (Some(after(b"k00080")), Some(b"k00170".to_vec())),
        (Some(lo.clone()), Some(hi.clone())),
        (Some(b"m00020".to_vec()), Some(b"p00010".to_vec())),
        (Some(b"p00020".to_vec()), None),
    ] {
        let (got, _) = scan_all(&e, lo.as_deref(), hi.as_deref(), 2);
        assert_eq!(got, model_scan(&e, &model, lo.as_deref(), hi.as_deref()), "window {lo:?}..{hi:?}");
    }
    let (got, stats) = scan_all(&e, Some(b"p00020"), None, usize::MAX);
    assert!(got.is_empty());
    assert_eq!(stats.block_reads, 0, "a cursor inside an unbounded fragment skips the straddling and last blocks: {stats:?}");
    let staged = scan_all_staged(&e, Some(b"p00020"), None, usize::MAX, SCAN_COVERED_BUDGET);
    assert_eq!(staged.stats.block_reads, 0, "staged: {:?}", staged.stats);
    e.sync().unwrap();
    let e = reopen(e);
    assert!(e.range_tombstone_max_tree_blocks() > 0, "open builds trees");
    let built = e.range_tombstone_max_tree_blocks();
    let (got, stats) = scan_all(&e, None, None, 4);
    assert_eq!(got, want, "after reopen");
    assert!(stats.skipped_blocks > 10);
    assert_eq!(e.range_tombstone_max_tree_blocks(), built);
}

#[test]
fn staged_scan_caps_covered_work_per_page_with_resume_keys_before_the_fold() {
    let mut e = rd_engine();
    let mut model = Model::new();
    for i in 0..100 {
        model_put(&mut e, &mut model, format!("b{i:04}").as_bytes(), v(1));
    }
    e.flush(small_blocks()).unwrap();
    for i in 0..5 {
        model_put(&mut e, &mut model, format!("a{i:04}").as_bytes(), v(1));
        model_put(&mut e, &mut model, format!("e{i:04}").as_bytes(), v(1));
    }
    for i in 0..300 {
        model_put(&mut e, &mut model, format!("c{i:04}").as_bytes(), v(1));
    }
    model_put(&mut e, &mut model, b"c0150", v(9));
    e.apply_range_delete(Some(b"b"), Some(b"d"), v(5)).unwrap();
    e.put_kind(b"c0299", b"", v(5), true).unwrap();
    model.insert(b"c0299".to_vec(), v(5));
    assert_eq!(e.range_tombstone_max_tree_blocks(), 0, "test premise: before the first fold no run has a tree");
    let want = model_scan(&e, &model, None, None);
    assert_eq!(want.len(), 11);

    let whole = scan_all_staged(&e, None, None, 4, SCAN_COVERED_BUDGET);
    assert_eq!(whole.entries, want);
    assert!(whole.pages.iter().all(|(_, resume, _)| resume.is_none()), "the default budget is not reached: {:?}", whole.pages);
    assert_eq!(whole.stats.filtered_entries, 399, "covered memtable and run entries are filtered one by one");

    let entries_budget = ScanCoveredBudget { filtered_entries: 50, block_reads: u64::MAX };
    let capped = scan_all_staged(&e, None, None, 4, entries_budget);
    assert_eq!(capped.entries, want, "resume pages return every live key");
    let resumed = capped.pages.iter().filter(|(_, resume, _)| resume.is_some()).count();
    assert!(resumed >= 399 / 50, "the covered span takes many resume pages: {:?}", capped.pages);
    assert!(capped.pages.iter().any(|(n, resume, _)| *n == 0 && resume.is_some()), "a page may carry no entries");
    assert!(capped.pages.iter().all(|(_, _, s)| s.filtered_entries <= 50), "{:?}", capped.pages);
    assert_eq!(capped.stats.filtered_entries, 399);

    let reads_budget = ScanCoveredBudget { filtered_entries: u64::MAX, block_reads: 4 };
    let capped = scan_all_staged(&e, Some(b"b"), Some(b"c"), 100, reads_budget);
    assert!(capped.entries.is_empty());
    assert!(capped.pages.len() > 10, "dry block reads cut the pages: {:?}", capped.pages.len());
    for (n, resume, s) in &capped.pages[..capped.pages.len() - 1] {
        assert!(*n == 0 && resume.is_some());
        assert!(s.block_reads <= 4 + 1, "a page reads at most the budget plus its straddling block: {s:?}");
    }

    e.build_range_tombstone_max_trees();
    let cover = e.range_tombstones().clone();
    let removed = e.remove_covered_mutable(None, None, &cover);
    assert_eq!(removed, 299, "the fold removes the covered memtable entries");
    let folded = scan_all_staged(&e, None, None, 4, entries_budget);
    assert_eq!(folded.entries, want);
    assert!(folded.pages.iter().all(|(_, resume, _)| resume.is_none()), "after the fold the skip leaves nothing to cap: {:?}", folded.pages);
    assert!(folded.stats.skipped_blocks > 10 && folded.stats.filtered_entries < 50, "{:?}", folded.stats);
}

#[test]
fn covered_block_budget_cuts_pages_that_interleave_covered_and_live_keys() {
    let mut e = rd_engine();
    let mut model = Model::new();
    for i in 0..1000u64 {
        model_put(&mut e, &mut model, format!("k{i:05}").as_bytes(), v(if i % 2 == 0 { 1 } else { 3 }));
    }
    e.flush(small_blocks()).unwrap();
    e.apply_range_delete(Some(b"k"), Some(b"l"), v(2)).unwrap();
    assert_eq!(e.range_tombstone_max_tree_blocks(), 0, "test premise: no tree before the first fold");
    let blocks = e.runs.iter().map(|r| r.run.index_len()).sum::<usize>();
    assert!(blocks > 3 * 64, "test premise: the span holds many budgets of blocks ({blocks})");
    let want = model_scan(&e, &model, None, None);
    assert_eq!(want.len(), 500);
    let all = scan_all_staged(&e, None, None, usize::MAX, SCAN_COVERED_BUDGET);
    assert_eq!(all.entries, want);
    let (last, cut) = all.pages.split_last().unwrap();
    assert!(cut.len() >= 3, "every page meets covered blocks: {:?}", all.pages);
    for (n, resume, s) in cut {
        assert!(resume.is_some() && *n > 0, "live keys between covered ones do not reset the budget");
        assert!(s.block_reads <= 64 + 1, "a page reads at most the budget plus one lookahead block: {s:?}");
    }
    assert!(last.1.is_none());
}

#[test]
fn skip_scan_matches_a_brute_force_model_over_random_runs_and_range_tombstones() {
    struct Rng(u64);
    impl Rng {
        fn below(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }
    }
    let key = |i: u64| format!("k{i:04}").into_bytes();
    let mut skipped = 0;
    for seed in 1..=10u64 {
        let mut r = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let mut e = rd_engine();
        let mut model = Model::new();
        let mut uniq = 0u128;
        let mut ver = |hlc: u64| {
            uniq += 1;
            (hlc as u128) << 64 | uniq
        };
        for round in 0..5 {
            for _ in 0..120 {
                let k = key(r.below(300));
                let hlc = 1 + r.below(12) + 8 * round;
                model_put(&mut e, &mut model, &k, ver(hlc));
            }
            for _ in 0..r.below(3) {
                let lo = (r.below(6) != 0).then(|| key(r.below(300)));
                let hi = (r.below(6) != 0).then(|| key(r.below(300)));
                if matches!((&lo, &hi), (Some(l), Some(h)) if l >= h) {
                    continue;
                }
                let hlc = 4 + r.below(12) + 8 * round;
                e.apply_range_delete(lo.as_deref(), hi.as_deref(), ver(hlc)).unwrap();
            }
            if r.below(4) != 0 {
                e.flush(small_blocks()).unwrap();
            }
            if round == 2 {
                e.build_range_tombstone_max_trees();
            }
        }
        if r.below(2) == 0 {
            e.compact_runs(small_blocks(), None).unwrap();
        }
        let mut e = e;
        if r.below(2) == 0 {
            e.sync().unwrap();
            e = reopen(e);
        }
        e.build_range_tombstone_max_trees();
        for _ in 0..12 {
            let lo = (r.below(4) != 0).then(|| key(r.below(310)));
            let hi = (r.below(4) != 0).then(|| key(r.below(310)));
            let page = [1, 5, usize::MAX][r.below(3) as usize];
            let (got, stats) = scan_all(&e, lo.as_deref(), hi.as_deref(), page);
            let want = model_scan(&e, &model, lo.as_deref(), hi.as_deref());
            assert!(got == want, "seed {seed} {lo:?}..{hi:?} page {page}: extra {:?} missing {:?}",
                got.iter().filter(|g| !want.contains(g)).take(5).collect::<Vec<_>>(),
                want.iter().filter(|w| !got.contains(w)).take(5).collect::<Vec<_>>());
            skipped += stats.skipped_blocks;
        }
        for i in 0..310 {
            let k = key(i);
            let want = model.get(&k).copied().max(e.range_tombstone_cover(&k));
            assert_eq!(e.version(&k).unwrap(), want, "seed {seed} version {k:?}");
            let live = model.get(&k).is_some_and(|w| e.range_tombstone_cover(&k).is_none_or(|c| *w > c));
            assert_eq!(e.get(&k).unwrap().is_some(), live, "seed {seed} get {k:?}");
            assert_eq!(super::async_get(&e, &k).is_some(), live, "seed {seed} staged get {k:?}");
        }
        e.set_range_tombstone_max_fragments(usize::MAX);
    }
    assert!(skipped > 0, "the random layouts exercise block skips");
}

#[test]
fn covered_point_reads_return_a_synthetic_tombstone_and_newer_writes_win() {
    for directory in [false, true] {
        let mut e = rd_engine();
        put_range(&mut e, "k", 20, 1);
        e.flush(RunBuildOpts::default()).unwrap();
        put_range(&mut e, "m", 5, 1);
        e.put(b"k0012", b"newer-before", v(9)).unwrap();
        if directory {
            e.rebuild_winner_directory().unwrap();
        }
        e.apply_range_delete(Some(b"k0010"), Some(b"m0003"), v(5)).unwrap();
        e.put(b"k0015", b"newer-after", v(8)).unwrap();
        e.put(b"k0016", b"straggler", v(3)).unwrap();
        e.put_kind(b"k0017", b"", v(5), true).unwrap();
        for round in 0..3 {
            let what = format!("directory {directory} round {round}");
            for (key, want) in [
                (&b"k0009"[..], Some((v(1), Some(&b"value"[..])))),
                (b"k0010", Some((v(5), None))),
                (b"k0012", Some((v(9), Some(b"newer-before")))),
                (b"k0015", Some((v(8), Some(b"newer-after")))),
                (b"k0016", Some((v(5), None))),
                (b"k0017", Some((v(5), None))),
                (b"k00175", Some((v(5), None))),
                (b"m0002", Some((v(5), None))),
                (b"m0003", Some((v(1), Some(b"value")))),
                (b"z", None),
            ] {
                assert_eq!(e.version(key).unwrap(), want.map(|w| w.0), "{what}: version {key:?}");
                assert_eq!(e.get(key).unwrap().as_deref(), want.and_then(|w| w.1), "{what}: get {key:?}");
                assert_eq!(super::async_get(&e, key).as_deref(), want.and_then(|w| w.1), "{what}: staged get {key:?}");
                let probed = vals_rt::block_on(Engine::version_from_probe(e.probe_version(key), e.device()), || {}).unwrap();
                assert_eq!(probed, want.map(|w| w.0), "{what}: staged version {key:?}");
                let covered = want.is_some_and(|w| w.1.is_none());
                assert_eq!(e.plan_get(key).unwrap().is_none(), want.is_none() || covered, "{what}: plan_get {key:?}");
                if covered && directory && round < 2 {
                    assert!(matches!(e.probe_get(key), StagedGet::Covered(c) if c == v(5)), "{what}: probe_get {key:?}");
                    assert_eq!(e.winner_directory_version(key), Some(Some(v(5))));
                }
                if let StagedGet::Runs { cover, .. } = e.probe_get(key) {
                    assert_eq!(cover, e.range_tombstone_cover(key), "{what}: staged probe carries the cover");
                }
            }
            let got = e.multi_get(&[b"k0009", b"k0011", b"k0012", b"k0016"]).unwrap();
            assert_eq!(got, vec![Some(b"value".to_vec()), None, Some(b"newer-before".to_vec()), None], "{what}");
            if directory {
                assert!(e.winner_read_tokens(&[b"k0009"]).is_none(), "{what}: batch tokens defer to cover-aware probes");
            }
            match round {
                0 => {
                    e.flush(RunBuildOpts::default()).unwrap();
                }
                _ => {
                    e.sync().unwrap();
                    e = reopen(e);
                    if directory {
                        e.rebuild_winner_directory().unwrap();
                    }
                }
            }
        }
    }
}

#[test]
fn max_trees_exist_only_while_range_tombstones_do() {
    if merge_env() {
        return;
    }
    let mut e = rd_engine();
    for round in 0..3 {
        put_range(&mut e, &format!("r{round}/"), 60, round + 1);
        e.flush(small_blocks()).unwrap();
    }
    e.compact_runs(small_blocks(), None).unwrap();
    put_range(&mut e, "s/", 60, 4);
    e.flush(small_blocks()).unwrap();
    assert_eq!(e.build_range_tombstone_max_trees(), 0, "zero range tombstones: no tree");
    let e2 = reopen(e);
    assert_eq!(e2.range_tombstone_max_tree_blocks(), 0, "zero range tombstones: no tree at open");
    let mut e = e2;
    let mut legacy = rd_engine();
    run::with_run_write_version(run::RUN_BLOCK_VERSION, || {
        put_range(&mut legacy, "v5/", 60, 1);
        legacy.flush(small_blocks()).unwrap();
    });
    legacy.apply_range_delete(None, Some(b"v5/0030"), v(2)).unwrap();
    assert_eq!(legacy.build_range_tombstone_max_trees(), 0, "v5 runs and the empty fence get no tree");
    assert_eq!(legacy.scan(None, None).unwrap().len(), 30, "v5 runs are filtered entry by entry");

    e.apply_range_delete(Some(b"r1/"), Some(b"r2/"), v(10)).unwrap();
    assert_eq!(e.range_tombstone_max_tree_blocks(), 0, "the apply path builds nothing");
    put_range(&mut e, "t/", 60, 11);
    e.flush(small_blocks()).unwrap();
    let installed = e.runs.last().unwrap().run.index_len() as u64;
    assert_eq!(e.range_tombstone_max_tree_blocks(), installed, "a run installed under a range tombstone gets its tree");
    let total: u64 = e.runs.iter().map(|or| or.run.index_len() as u64).sum();
    assert_eq!(e.build_range_tombstone_max_trees(), total - installed);
    assert_eq!(e.build_range_tombstone_max_trees(), 0, "built once");
    let plan = e.compact_plan(small_blocks(), None).unwrap().expect("a merge");
    let written = vals_rt::block_on(compact_and_write(plan, e.dev.clone()), || {}).unwrap();
    e.compact_commit(written).unwrap();
    assert!(e.runs.iter().all(|or| or.run.index_len() == 0 || or.max_tree.is_some()), "a merge output gets its tree");
    e.compact_runs(small_blocks(), None).unwrap();
    assert!(e.runs[0].max_tree.is_some());
    let e = reopen(e);
    let total: u64 = e.runs.iter().map(|or| or.run.index_len() as u64).sum();
    assert_eq!(e.range_tombstone_max_tree_blocks(), total, "open builds every v6 run's tree");
    let mut e = e;
    e.truncate().unwrap();
    assert_eq!(e.range_tombstone_max_tree_blocks(), 0);
}

fn async_flush(e: &mut Engine<MemBlockDevice>) {
    let plan = e.flush_plan(RunBuildOpts::default()).unwrap().expect("something to flush");
    let written = vals_rt::block_on(build_and_write_flush(plan, e.device()), || {}).unwrap();
    e.flush_commit(written).unwrap();
}

fn merge_window(e: &mut Engine<MemBlockDevice>, start: usize, end: usize, gc: Option<u128>) -> Result<CompactRunsStats, EngineError> {
    let plan = e.compact_plan_window_for_test(RunBuildOpts::default(), start, end, gc).unwrap().expect("a merge plan");
    let written = vals_rt::block_on(compact_and_write(plan, e.dev.clone()), || {}).unwrap();
    e.compact_commit(written)
}

fn run_holds(e: &Engine<MemBlockDevice>, key: &[u8]) -> bool {
    e.runs.iter().any(|or| or.run.lookup(&e.alloc, key).unwrap().is_some())
}

#[test]
fn flushes_drop_entries_covered_by_accounted_range_tombstones() {
    for asynchronous in [false, true] {
        let flush = |e: &mut Engine<MemBlockDevice>| {
            if asynchronous {
                async_flush(e);
            } else {
                e.flush(RunBuildOpts::default()).unwrap();
            }
        };
        let what = format!("asynchronous {asynchronous}");
        let mut e = rd_engine();
        put_range(&mut e, "k", 40, 1);
        let r1 = e.apply_range_delete(Some(b"k0010"), Some(b"k0020"), v(5)).unwrap().seq.unwrap();
        flush(&mut e);
        let run = e.runs.last().unwrap();
        assert_eq!((run.meta.entry_count, run.run.rt_seq_floor()), (40, 0), "{what}: nothing is accounted yet");

        for i in (0..40).filter(|&i| i != 15) {
            e.put(format!("k{i:04}").as_bytes(), b"second", v(2)).unwrap();
        }
        e.put(b"k0015", b"newer", v(9)).unwrap();
        e.advance_range_tombstone_accounted_seq(r1);
        let r2 = e.apply_range_delete(Some(b"k0030"), None, v(6)).unwrap().seq.unwrap();
        assert!(r2 > e.range_tombstone_accounted_seq());
        let dropped = rt_covered_dropped_total();
        flush(&mut e);
        assert!(rt_covered_dropped_total() >= dropped + 9, "{what}");
        let run = e.runs.last().unwrap();
        assert_eq!(run.meta.entry_count, 31, "{what}: the nine entries covered by the accounted record are dropped");
        assert_eq!(run.run.rt_seq_floor(), r1, "{what}: floor = min(S_A, highest persisted record seq)");
        assert!(e.index.is_empty(), "{what}: covered memtable entries leave with the flush");
        assert!(run.run.lookup(&e.alloc, b"k0031").unwrap().is_some(), "{what}: an unaccounted record filters nothing");
        assert!(run.run.lookup(&e.alloc, b"k0012").unwrap().is_none(), "{what}");
        for _ in 0..2 {
            assert_eq!(e.get(b"k0012").unwrap(), None, "{what}");
            assert_eq!(e.version(b"k0012").unwrap(), Some(v(5)), "{what}");
            assert_eq!(e.get(b"k0015").unwrap().as_deref(), Some(&b"newer"[..]), "{what}");
            assert_eq!(e.get(b"k0031").unwrap(), None, "{what}");
            assert_eq!(e.get(b"k0020").unwrap().as_deref(), Some(&b"second"[..]), "{what}");
            assert_eq!(e.scan_bounded(None, None, usize::MAX, usize::MAX).unwrap().0.len(), 21, "{what}");
            e = reopen(e);
        }
    }
}

#[test]
fn compaction_reclaims_the_run_and_value_log_space_of_covered_entries() {
    if merge_env() {
        return;
    }
    let build = |accounted: bool| {
        let mut e = rd_engine();
        for round in 0..2u8 {
            for i in 0..300 {
                e.put(format!("a{round}{i:04}").as_bytes(), &[round; 200], v(1)).unwrap();
            }
            e.flush(RunBuildOpts::default()).unwrap();
        }
        put_range(&mut e, "b", 20, 1);
        e.flush(RunBuildOpts::default()).unwrap();
        let r = e.apply_range_delete(Some(b"a"), Some(b"b"), v(5)).unwrap().seq.unwrap();
        e.flush(RunBuildOpts::default()).unwrap();
        if accounted {
            e.advance_range_tombstone_accounted_seq(r);
        }
        let n = e.runs.len();
        let stats = merge_window(&mut e, 0, n, None).unwrap();
        (e, stats)
    };
    let (plain, plain_stats) = build(false);
    let (mut e, stats) = build(true);
    assert_eq!(plain_stats.covered_dropped, 0);
    assert_eq!(stats.covered_dropped, 600);
    assert!(stats.current_dropped_tombstones.is_empty(), "covered drops are not reported as current drops");
    assert_eq!(plain.runs.iter().map(|or| or.meta.entry_count).sum::<u64>(), 620);
    assert_eq!(e.runs.iter().map(|or| or.meta.entry_count).sum::<u64>(), 20);
    let (plain_bytes, bytes) = (plain.capacity_stats().run_disk_bytes, e.capacity_stats().run_disk_bytes);
    assert!(bytes * 4 < plain_bytes, "run index bytes of covered entries are reclaimed: {bytes} vs {plain_bytes}");
    let value_segs = (600 * 200) / (8 * BS);
    assert!(
        e.alloc.free_segments() >= plain.alloc.free_segments() + value_segs as u64,
        "value segments of covered entries are freed: {} vs {} (+{value_segs})",
        e.alloc.free_segments(),
        plain.alloc.free_segments()
    );
    e.storage_audit_for_test().unwrap();
    for _ in 0..2 {
        assert_eq!(e.get(b"a00007").unwrap(), None);
        assert_eq!(e.version(b"a10007").unwrap(), Some(v(5)));
        assert_eq!(e.get(b"b0003").unwrap().as_deref(), Some(&b"value"[..]));
        assert_eq!(e.scan_bounded(None, None, usize::MAX, usize::MAX).unwrap().0.len(), 20);
        e = reopen(e);
    }
}

#[test]
fn shadow_estimator_counts_accounted_covered_entries_as_garbage() {
    if co_compact_env(false) {
        return;
    }
    let mut e = Engine::create_prewiped(
        MemBlockDevice::new(BS, 6144),
        Config { seg_blocks: 32, reserved_blocks: 0, ordering: Ordering::Sorted },
    );
    for prefix in ["a", "b"] {
        for i in 0..1000usize {
            e.put(format!("{prefix}{i:04}").as_bytes(), &[7u8; 256], v(1)).unwrap();
        }
        e.flush(RunBuildOpts::default()).unwrap();
    }
    let covered_values = e.runs.iter().find(|or| or.meta.min_key.starts_with(b"a")).unwrap().meta.value_bytes.unwrap();
    let r = e.apply_range_delete(Some(b"a"), Some(b"b"), v(5)).unwrap().seq.unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    assert!(e.free_fraction() <= SHADOW_EST_FREE_GATE, "state must clear the estimation gate");
    e.lsm_dead_fraction_gate();
    let before = e.shadowed_bytes_estimate_cached();
    assert!(before <= covered_values / 10, "an unaccounted range tombstone is not garbage yet: {before}");
    e.advance_range_tombstone_accounted_seq(r);
    e.lsm_dead_fraction_gate();
    let after = e.shadowed_bytes_estimate_cached();
    assert!(after >= covered_values / 2, "accounted covered values are garbage: {after} of {covered_values}");
}

fn collection_fixture(a_in_old_run: bool) -> (Engine<MemBlockDevice>, u64, u64) {
    let mut e = rd_engine();
    put_range(&mut e, "m", 50, 1);
    put_range(&mut e, "b", 20, 1);
    if a_in_old_run {
        e.put(b"a0050", b"old", v(1)).unwrap();
    }
    let r2 = e.apply_range_delete(Some(b"b"), Some(b"c"), v(6)).unwrap().seq.unwrap();
    e.advance_range_tombstone_accounted_seq(r2);
    e.flush(RunBuildOpts::default()).unwrap();
    put_range(&mut e, "a", 20, 2);
    e.flush(RunBuildOpts::default()).unwrap();
    let r1 = e.apply_range_delete(Some(b"a"), Some(b"b"), v(7)).unwrap().seq.unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    assert_eq!(e.runs.len(), 4, "fence, old run with r2, a-run, RT-only run with r1");
    assert_eq!((e.runs[1].rts.len(), e.runs[3].rts.len()), (1, 1));
    (e, r1, r2)
}

fn record_seqs(e: &Engine<MemBlockDevice>) -> Vec<u64> {
    e.range_tombstone_records().iter().map(|r| r.seq).collect()
}

#[test]
fn range_tombstone_records_are_collected_only_under_all_three_conditions() {
    if merge_env() {
        return;
    }
    let gc = Some(v(100));

    let (mut e, r1, r2) = collection_fixture(false);
    e.advance_range_tombstone_accounted_seq(r1);
    let stats = merge_window(&mut e, 2, 4, None).unwrap();
    assert!(stats.collected_range_tombstones.is_empty(), "no gc_grace cutoff: nothing is collected");
    assert_eq!(stats.covered_dropped, 20);
    let stats = merge_window(&mut e, 2, 3, Some(v(7))).unwrap();
    assert!(stats.collected_range_tombstones.is_empty(), "the cutoff must be strictly above the record version");
    assert_eq!(record_seqs(&e), vec![r2, r1]);

    let (mut e, r1, r2) = collection_fixture(false);
    let stats = merge_window(&mut e, 2, 4, gc).unwrap();
    assert!(stats.collected_range_tombstones.is_empty(), "a record above S_A is not collected");
    assert_eq!(stats.covered_dropped, 0, "nor does it filter");
    e.advance_range_tombstone_accounted_seq(r1);
    let stats = merge_window(&mut e, 2, 3, gc).unwrap();
    assert_eq!(stats.collected_range_tombstones.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![r1]);
    assert_eq!(record_seqs(&e), vec![r2]);

    let (mut e, r1, r2) = collection_fixture(true);
    e.advance_range_tombstone_accounted_seq(r1);
    assert!(e.runs[1].meta.min_key.as_slice() < b"b" && e.runs[1].run.rt_seq_floor() < r1);
    let stats = merge_window(&mut e, 2, 4, gc).unwrap();
    assert!(stats.collected_range_tombstones.is_empty(), "an outside run under the span with a low floor blocks collection");
    assert_eq!(e.version(b"a0050").unwrap(), Some(v(7)));
    let stats = merge_window(&mut e, 1, 3, gc).unwrap();
    assert_eq!(stats.collected_range_tombstones.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![r2, r1]);
    assert!(e.range_tombstones().is_empty() && e.range_tombstone_max_tree_blocks() == 0);
    let e = reopen(e);
    assert!(e.range_tombstone_records().is_empty());
    assert_eq!(e.get(b"a0050").unwrap(), None);
    assert_eq!(e.scan_bounded(None, None, usize::MAX, usize::MAX).unwrap().0.len(), 50);
}

#[test]
fn a_partial_merge_collects_a_record_while_other_runs_keep_theirs_and_reopen_resurrects_nothing() {
    if merge_env() {
        return;
    }
    let (mut e, r1, r2) = collection_fixture(false);
    e.advance_range_tombstone_accounted_seq(r1);
    let collected = rt_collected_total();
    let stats = merge_window(&mut e, 2, 4, Some(v(100))).unwrap();
    assert_eq!(stats.collected_range_tombstones.iter().map(|r| (r.seq, r.version)).collect::<Vec<_>>(), vec![(r1, v(7))]);
    assert_eq!(stats.covered_dropped, 20);
    assert!(stats.current_dropped_tombstones.is_empty());
    assert!(rt_collected_total() > collected);
    assert_eq!(e.runs.len(), 3);
    assert_eq!(record_seqs(&e), vec![r2], "the old run keeps its record");
    assert_eq!(fragments(&e), vec![frag(b"b", Some(b"c"), 6)], "F is recomputed without the collected record");
    assert_each_record_once(&e);
    for _ in 0..2 {
        for i in 0..20 {
            let key = format!("a{i:04}");
            assert_eq!(e.version(key.as_bytes()).unwrap(), None, "{key} stays gone");
            assert!(!run_holds(&e, key.as_bytes()));
        }
        assert_eq!(e.get(b"b0001").unwrap(), None);
        assert_eq!(e.version(b"b0001").unwrap(), Some(v(6)));
        assert_eq!(e.scan_bounded(None, None, usize::MAX, usize::MAX).unwrap().0.len(), 50);
        e.sync().unwrap();
        e = reopen(e);
        assert_eq!(record_seqs(&e), vec![r2], "the collected record stays gone after reopen");
        assert!(e.index.is_empty(), "replay resurrects no covered entry into the memtable");
        e.advance_range_tombstone_accounted_seq(r1);
    }
}

#[test]
fn collection_waits_for_the_flush_that_persisted_the_record_to_finish_its_memtable_removal() {
    if merge_env() {
        return;
    }
    let mut e = rd_engine();
    put_range(&mut e, "m", 20, 1);
    e.flush(RunBuildOpts::default()).unwrap();
    put_range(&mut e, "a", 20, 1);
    let r = e.apply_range_delete(Some(b"a"), Some(b"b"), v(5)).unwrap().seq.unwrap();
    e.advance_range_tombstone_accounted_seq(r);
    let plan = e.flush_plan(RunBuildOpts::default()).unwrap().unwrap();
    let written = vals_rt::block_on(build_and_write_flush(plan, e.device()), || {}).unwrap();
    let mut commit = e.flush_commit_start(written).unwrap();
    assert_eq!(e.runs.last().unwrap().meta.entry_count, 0, "the flush dropped every covered entry");
    assert!(e.index.get(b"a0003").is_some(), "test premise: the memtable still holds the snapshotted entries");
    let n = e.runs.len();
    let stats = merge_window(&mut e, n - 1, n, Some(v(100))).unwrap();
    assert!(stats.collected_range_tombstones.is_empty(), "the persisting flush has not finished");
    assert_eq!(e.get(b"a0003").unwrap(), None);
    while let FlushCommitStep::Pending = e.flush_commit_step(&mut commit, 4) {}
    let n = e.runs.len();
    let stats = merge_window(&mut e, n - 1, n, Some(v(100))).unwrap();
    assert_eq!(stats.collected_range_tombstones.len(), 1);
    assert_eq!(e.get(b"a0003").unwrap(), None);
}

#[test]
fn a_collecting_merge_conflicts_when_an_unfiltered_run_under_the_span_commits_before_it() {
    if merge_env() {
        return;
    }
    let (mut e, r1, _) = collection_fixture(false);
    e.put(b"a0005", b"straggler", v(3)).unwrap();
    let flush = e.flush_plan(RunBuildOpts::default()).unwrap().unwrap();
    e.advance_range_tombstone_accounted_seq(r1);
    let plan = e.compact_plan_window_for_test(RunBuildOpts::default(), 2, 4, Some(v(100))).unwrap().unwrap();
    let written = vals_rt::block_on(compact_and_write(plan, e.dev.clone()), || {}).unwrap();
    let flushed = vals_rt::block_on(build_and_write_flush(flush, e.device()), || {}).unwrap();
    e.flush_commit(flushed).unwrap();
    let run = e.runs.last().unwrap();
    assert!(run.meta.entry_count == 1 && run.run.rt_seq_floor() < r1, "test premise: a flush planned before S_A covered r1");
    assert!(matches!(e.compact_commit(written), Err(EngineError::CompactionConflict)));
    assert_eq!(e.version(b"a0005").unwrap(), Some(v(7)), "the record stays and still covers the straggler");
    assert_eq!(e.range_tombstone_records().len(), 2);
    let n = e.runs.len();
    let stats = merge_window(&mut e, 2, n, Some(v(100))).unwrap();
    assert_eq!(stats.collected_range_tombstones.len(), 1, "a merge that rewrites the unfiltered run collects");
    assert_eq!(e.version(b"a0005").unwrap(), None);
}

#[test]
fn a_flush_rejects_a_corrupted_snapshot_entry_that_filtering_would_drop() {
    for asynchronous in [false, true] {
        let what = format!("asynchronous {asynchronous}");
        let mut e = rd_engine();
        let value = vec![1u8; 8 * BS - crate::vlog::SEG_HEADER_LEN - crate::vlog::HEADER_LEN - 1];
        e.put(b"b", &value, v(1)).unwrap();
        let r = e.apply_range_delete(Some(b"c"), Some(b"d"), v(5)).unwrap().seq.unwrap();
        e.advance_range_tombstone_accounted_seq(r);
        e.set_flush_source_flip_for_test(Some((0, 0, 1)));
        let err = if asynchronous {
            let plan = e.flush_plan(RunBuildOpts::default()).unwrap().unwrap();
            let (segs, err) = vals_rt::block_on(build_and_write_flush(plan, e.device()), || {}).err().expect("the build must fail");
            e.free_flush_segs(&segs);
            err
        } else {
            e.flush(RunBuildOpts::default()).expect_err("the flush must fail")
        };
        assert!(err.encode_integrity().is_some_and(|i| i.check == "source-hash"), "{what}: {err}");
        e.set_flush_source_flip_for_test(None);
        assert_eq!(e.get(b"b").unwrap(), Some(value.clone()), "{what}");
        let e = reopen(e);
        assert_eq!(e.get(b"b").unwrap(), Some(value), "{what}: the uncovered key survives reopen");
    }
}

fn released(pieces: &[Fragment]) -> FragmentSet {
    let mut set = FragmentSet::default();
    for p in pieces {
        set.overlay(Some(&p.lo), p.hi.as_deref(), p.version).unwrap();
    }
    set
}

fn forget_releasing(e: &mut Engine<MemBlockDevice>, spans: &FragmentSet) -> CompactRunsStats {
    let plan = e
        .forget_plan_releasing(RunBuildOpts::default(), &std::collections::HashMap::new(), spans)
        .unwrap()
        .expect("a forget plan");
    let written = vals_rt::block_on(compact_and_write(plan, e.dev.clone()), || {}).unwrap();
    e.compact_commit(written).unwrap()
}

#[test]
fn a_forget_plan_drops_entries_under_released_spans_and_clips_their_records() {
    let mut e = rd_engine();
    put_range(&mut e, "k", 50, 1);
    e.apply_range_delete(Some(b"k0005"), Some(b"k0035"), v(5)).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    e.put(b"k0016", b"newer", v(9)).unwrap();
    for i in 38..48 {
        e.put(format!("k{i:04}").as_bytes(), b"second", v(2)).unwrap();
    }
    e.apply_range_delete(Some(b"k0038"), Some(b"k0048"), v(7)).unwrap();
    let spans = released(&[frag(b"k0010", Some(b"k0020"), 5), frag(b"k0040", Some(b"k0042"), 7), frag(b"k0044", Some(b"k0046"), 6)]);
    assert!(e.index.get(b"k0041").is_some(), "premise: covered memtable entries under a released span");

    let plan = e
        .forget_plan_releasing(RunBuildOpts::default(), &std::collections::HashMap::new(), &spans)
        .unwrap()
        .expect("a forget plan");
    assert!(e.index.get(b"k0041").is_none() && e.index.get(b"k0039").is_some(), "the plan removes covered memtable entries under the released spans only");
    let flush = e.flush_plan(RunBuildOpts::default()).unwrap().expect("a flush during the forget rewrite");
    let written = vals_rt::block_on(compact_and_write(plan, e.dev.clone()), || {}).unwrap();
    let stats = e.compact_commit(written).unwrap();
    assert_eq!(stats.released_range_tombstones, 2, "a span over a newer cover is not released");
    let written = vals_rt::block_on(build_and_write_flush(flush, e.device()), || {}).unwrap();
    e.flush_commit(written).unwrap();
    assert_each_record_once(&e);
    assert_eq!(
        fragments(&e),
        vec![frag(b"k0005", Some(b"k0010"), 5), frag(b"k0020", Some(b"k0035"), 5), frag(b"k0038", Some(b"k0048"), 7)],
        "run records are clipped; records a flush in flight captured are not"
    );
    for (key, want) in [(&b"k0012"[..], None), (b"k0016", Some(v(9))), (b"k0007", Some(v(5))), (b"k0041", Some(v(7))), (b"k0049", Some(v(1)))] {
        assert_eq!(e.version(key).unwrap(), want, "{key:?}");
    }
    assert!(!run_holds(&e, b"k0012") && run_holds(&e, b"k0016") && run_holds(&e, b"k0007"), "the rewrite drops only entries under a released span, accounted or not");

    let stats = forget_releasing(&mut e, &released(&[frag(b"k0040", Some(b"k0042"), 7)]));
    assert_eq!(stats.released_range_tombstones, 1);
    assert_each_record_once(&e);
    let want = vec![
        frag(b"k0005", Some(b"k0010"), 5),
        frag(b"k0020", Some(b"k0035"), 5),
        frag(b"k0038", Some(b"k0040"), 7),
        frag(b"k0042", Some(b"k0048"), 7),
    ];
    assert_eq!(fragments(&e), want, "the next forget clips the flushed records");
    assert!(!run_holds(&e, b"k0041"));
    for _ in 0..2 {
        assert_eq!(fragments(&e), want);
        assert_eq!(e.version(b"k0012").unwrap(), None);
        assert_eq!(e.version(b"k0041").unwrap(), None);
        assert_eq!(e.version(b"k0016").unwrap(), Some(v(9)));
        assert_eq!(e.version(b"k0039").unwrap(), Some(v(7)));
        e = reopen(e);
    }
}

#[test]
fn a_forget_commit_keeps_newer_range_deletes_and_never_rereleases_a_clipped_span() {
    let spans = released(&[frag(b"a", Some(b"z"), 5)]);
    let mut e = rd_engine();
    e.put(b"k", b"newer", v(9)).unwrap();
    e.apply_range_delete(Some(b"a"), Some(b"z"), v(5)).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    let plan = e.forget_plan_releasing(RunBuildOpts::default(), &std::collections::HashMap::new(), &spans).unwrap().expect("a forget plan");
    e.apply_range_delete(Some(b"a"), Some(b"z"), v(10)).unwrap();
    let written = vals_rt::block_on(compact_and_write(plan, e.dev.clone()), || {}).unwrap();
    e.compact_commit(written).unwrap();
    assert_each_record_once(&e);
    assert_eq!(fragments(&e), vec![frag(b"a", Some(b"z"), 10)], "a delete newer than the released piece survives the clip");
    assert_eq!(e.get(b"k").unwrap(), None);
    assert!(
        e.forget_plan_releasing(RunBuildOpts::default(), &std::collections::HashMap::new(), &spans).unwrap().is_none(),
        "a piece F now covers at another version is not released"
    );

    let mut e = rd_engine();
    e.put(b"a1", b"old", v(1)).unwrap();
    e.apply_range_delete(Some(b"a"), Some(b"z"), v(5)).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    assert_eq!(forget_releasing(&mut e, &spans).released_range_tombstones, 1);
    assert_eq!(fragments(&e), Vec::new());
    e.put(b"a2", b"after", v(3)).unwrap();
    assert!(
        e.forget_plan_releasing(RunBuildOpts::default(), &std::collections::HashMap::new(), &spans).unwrap().is_none(),
        "a span F no longer covers is not released again"
    );
    assert_eq!(e.get(b"a2").unwrap().as_deref(), Some(&b"after"[..]));
}

fn queue_deletes(e: &mut Engine<MemBlockDevice>, from: u64, through: u64) -> Vec<u64> {
    (from..=through)
        .map(|j| {
            let hi = format!("a{j:04}");
            e.apply_range_delete(Some(b"a"), Some(hi.as_bytes()), v(100 + j)).unwrap().seq.unwrap();
            e.apply_range_delete(Some(b"b"), Some(b"c"), v(200 + j)).unwrap().seq.unwrap()
        })
        .collect()
}

fn run_record_versions(e: &Engine<MemBlockDevice>, run: usize) -> Vec<u128> {
    e.runs[run].rts.iter().map(|r| r.version).collect()
}

#[test]
fn flushes_and_rewrites_drop_dominated_accounted_records_without_changing_f() {
    if merge_env() {
        return;
    }
    for asynchronous in [false, true] {
        let what = format!("asynchronous {asynchronous}");
        let flush = |e: &mut Engine<MemBlockDevice>| {
            if asynchronous {
                async_flush(e);
            } else {
                e.flush(RunBuildOpts::default()).unwrap();
            }
        };
        let mut e = rd_engine();
        put_range(&mut e, "a", 40, 1);
        put_range(&mut e, "b", 10, 1);
        e.flush(RunBuildOpts::default()).unwrap();
        e.put(b"a0003", b"newer", v(500)).unwrap();

        let seqs = queue_deletes(&mut e, 1, 10);
        e.advance_range_tombstone_accounted_seq(*seqs.last().unwrap());
        let want = fragments(&e);
        let dominated = rt_dominated_dropped_total();
        flush(&mut e);
        assert_eq!(run_record_versions(&e, e.runs.len() - 1), vec![v(110), v(210)], "{what}: one record per delete series");
        assert!(rt_dominated_dropped_total() >= dominated + 18, "{what}");
        assert_eq!(fragments(&e), want, "{what}: F is unchanged");
        assert_each_record_once(&e);

        let seqs = queue_deletes(&mut e, 11, 20);
        e.advance_range_tombstone_accounted_seq(seqs[4]);
        flush(&mut e);
        let mut held = run_record_versions(&e, e.runs.len() - 1);
        held.sort_unstable();
        let mut unaccounted: Vec<u128> = (16..=20).flat_map(|j| [v(100 + j), v(200 + j)]).collect();
        unaccounted.extend([v(115), v(215)]);
        unaccounted.sort_unstable();
        assert_eq!(held, unaccounted, "{what}: records above S_A are kept and never dominate");
        assert_each_record_once(&e);

        let seqs = queue_deletes(&mut e, 21, 25);
        e.advance_range_tombstone_accounted_seq(*seqs.last().unwrap());
        let want = fragments(&e);
        e.sync().unwrap();
        let plan = e.flush_plan(RunBuildOpts::default()).unwrap().expect("a flush");
        let written = vals_rt::block_on(build_and_write_flush(plan, e.device()), || {}).unwrap();
        drop(written);
        let mut e = reopen(e);
        assert_eq!(fragments(&e), want, "{what}: a crash before the flush commits keeps every record");
        assert_eq!(e.range_tombstone_record_count(), 2 + 12 + 10, "{what}: the tail replays all of its records");
        e.advance_range_tombstone_accounted_seq(e.range_tombstone_max_seq());
        flush(&mut e);
        assert_eq!(e.range_tombstone_record_count(), 2 + 12 + 2, "{what}");

        let n = e.runs.len();
        let stats = merge_window(&mut e, 0, n, None).unwrap();
        assert_eq!(stats.dominated_range_tombstones, 14, "{what}: a rewrite drops records dominated within its output");
        assert!(stats.collected_range_tombstones.is_empty(), "{what}");
        assert_eq!(e.range_tombstone_records().iter().map(|r| r.version).collect::<Vec<_>>(), vec![v(125), v(225)], "{what}");
        assert_eq!(fragments(&e), want, "{what}");
        assert_each_record_once(&e);
        for _ in 0..2 {
            assert_eq!(fragments(&e), want, "{what}");
            assert_eq!(e.range_tombstone_record_count(), 2, "{what}");
            for i in 0..40 {
                let key = format!("a{i:04}");
                let want = match i {
                    3 => Some(v(500)),
                    0..25 => Some(v(125)),
                    _ => Some(v(1)),
                };
                assert_eq!(e.version(key.as_bytes()).unwrap(), want, "{what}: {key}");
            }
            assert_eq!(e.version(b"b0004").unwrap(), Some(v(225)), "{what}");
            assert_eq!(e.scan_bounded(None, None, usize::MAX, usize::MAX).unwrap().0.len(), 16, "{what}");
            e.sync().unwrap();
            e = reopen(e);
        }
    }
}

#[test]
fn collecting_a_dominating_record_after_its_dominated_record_was_dropped_resurrects_nothing() {
    if merge_env() {
        return;
    }
    let mut e = rd_engine();
    put_range(&mut e, "a", 20, 1);
    put_range(&mut e, "m", 10, 1);
    e.flush(RunBuildOpts::default()).unwrap();
    e.apply_range_delete(Some(b"a"), Some(b"a0010"), v(5)).unwrap();
    let r2 = e.apply_range_delete(Some(b"a"), Some(b"a0015"), v(7)).unwrap().seq.unwrap();
    e.advance_range_tombstone_accounted_seq(r2);
    e.flush(RunBuildOpts::default()).unwrap();
    assert_eq!(record_seqs(&e), vec![r2], "the flush drops the dominated record");
    let n = e.runs.len();
    let stats = merge_window(&mut e, 0, n, Some(v(100))).unwrap();
    assert_eq!(stats.collected_range_tombstones.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![r2]);
    assert_eq!(stats.covered_dropped, 15);
    assert!(e.range_tombstones().is_empty());
    for _ in 0..2 {
        assert!(e.range_tombstone_records().is_empty(), "neither record comes back");
        for i in 0..20 {
            let key = format!("a{i:04}");
            assert_eq!(e.version(key.as_bytes()).unwrap(), (i >= 15).then_some(v(1)), "{key}");
            assert!(i >= 15 || e.index.get(key.as_bytes()).is_none(), "replay resurrects no covered entry into the memtable: {key}");
        }
        assert_eq!(e.scan_bounded(None, None, usize::MAX, usize::MAX).unwrap().0.len(), 15);
        e.sync().unwrap();
        e = reopen(e);
    }
}

#[test]
fn a_released_span_clips_the_same_f_whether_or_not_its_dominated_records_were_dropped() {
    let build = |drop_in_flush: bool| {
        let mut e = rd_engine();
        put_range(&mut e, "a", 20, 1);
        e.flush(RunBuildOpts::default()).unwrap();
        e.apply_range_delete(Some(b"a"), Some(b"a0010"), v(5)).unwrap();
        let r2 = e.apply_range_delete(Some(b"a"), Some(b"a0015"), v(7)).unwrap().seq.unwrap();
        if drop_in_flush {
            e.advance_range_tombstone_accounted_seq(r2);
        }
        e.flush(RunBuildOpts::default()).unwrap();
        assert_eq!(e.range_tombstone_record_count(), if drop_in_flush { 1 } else { 2 });
        let stale = released(&[frag(b"a", Some(b"a0003"), 5)]);
        assert!(
            e.forget_plan_releasing(RunBuildOpts::default(), &std::collections::HashMap::new(), &stale).unwrap().is_none(),
            "a piece at the dominated record's version is not released: F is above it"
        );
        let stats = forget_releasing(&mut e, &released(&[frag(b"a0005", Some(b"a0012"), 7)]));
        assert_eq!(stats.released_range_tombstones, 1);
        assert_each_record_once(&e);
        (e, r2)
    };
    let ((mut dropped, _), (mut kept, r2)) = (build(true), build(false));
    let want = vec![frag(b"a", Some(b"a0005"), 7), frag(b"a0012", Some(b"a0015"), 7)];
    assert_eq!((fragments(&dropped), fragments(&kept)), (want.clone(), want.clone()));
    assert_eq!((dropped.range_tombstone_record_count(), kept.range_tombstone_record_count()), (2, 3));
    kept.advance_range_tombstone_accounted_seq(r2);
    let n = kept.runs.len();
    assert_eq!(merge_window(&mut kept, 0, n, None).unwrap().dominated_range_tombstones, 1, "the clipped dominated piece goes with the next rewrite");
    for _ in 0..2 {
        for e in [&dropped, &kept] {
            assert_eq!(fragments(e), want);
            assert_eq!(e.range_tombstone_record_count(), 2);
            for i in 0..20 {
                let key = format!("a{i:04}");
                let want = match i {
                    0..5 | 12..15 => Some(v(7)),
                    5..12 => None,
                    _ => Some(v(1)),
                };
                assert_eq!(e.version(key.as_bytes()).unwrap(), want, "{key}");
            }
        }
        dropped.sync().unwrap();
        kept.sync().unwrap();
        (dropped, kept) = (reopen(dropped), reopen(kept));
    }
}

fn held_records<D: BlockDevice>(e: &Engine<D>) -> impl Iterator<Item = (&RtRecord, bool)> + '_ {
    e.runs.iter().flat_map(|or| or.rts.iter().map(|r| (r, false))).chain(e.rts.tail.iter().map(|(r, _)| (r, true)))
}

fn scanned_records_between<D: BlockDevice>(e: &Engine<D>, after: u64, through: u64) -> Vec<RtRecord> {
    let mut out: Vec<RtRecord> = held_records(e).filter(|(r, _)| r.seq > after && r.seq <= through).map(|(r, _)| r.clone()).collect();
    out.sort_by_key(|r| r.seq);
    out
}

fn scanned_filter_set<D: BlockDevice>(e: &Engine<D>, limit: u64, tail_through: u64) -> Option<FragmentSet> {
    if e.rts.fragments.is_empty() || e.rts.accounted_seq.get() == 0 {
        return None;
    }
    let take = move |&(r, in_tail): &(&RtRecord, bool)| r.seq <= limit && (!in_tail || r.seq <= tail_through);
    held_records(e).any(|x| take(&x)).then(|| FragmentSet::from_records(held_records(e).filter(take).map(|(r, _)| r)))
}

fn assert_run_rt_cache_holds(e: &Engine<MemBlockDevice>, what: &str) {
    if let Some((at, set)) = e.rts.runs_through.borrow().as_ref() {
        let want = FragmentSet::from_records(e.runs.iter().flat_map(|or| or.rts.iter()).filter(|r| r.seq <= *at));
        assert_eq!(*set, want, "{what}: the cached run RT set through {at} matches the run records");
    }
}

fn assert_rt_index_matches_scan(e: &Engine<MemBlockDevice>, seed: &mut u64, windows: u64, what: &str) {
    assert_run_rt_cache_holds(e, what);
    let max = held_records(e).map(|(r, _)| r.seq).max().unwrap_or(0);
    assert_eq!(e.range_tombstone_max_seq(), max, "{what}");
    assert_eq!(e.range_tombstone_records(), scanned_records_between(e, 0, u64::MAX), "{what}");
    assert_eq!(*e.range_tombstones(), FragmentSet::from_records(&e.range_tombstone_records()), "{what}");
    let accounted = e.range_tombstone_accounted_seq();
    let step = (max / 16).max(1);
    for limit in (0..=max + 1).step_by(step as usize) {
        assert_eq!(e.rt_filter_set(limit, u64::MAX).map(std::borrow::Cow::into_owned), scanned_filter_set(e, limit, u64::MAX), "{what}: ascending filter {limit}");
    }
    let mut pick = || {
        *seed += 1;
        crate::run::splitmix64(*seed) % (max + 3)
    };
    for i in 0..windows {
        let (a, b) = (pick(), pick());
        let (after, through) = if i % 4 == 0 { (a.max(b), a.min(b)) } else { (a.min(b), a.max(b)) };
        assert_eq!(e.range_tombstone_records_between(after, through), scanned_records_between(e, after, through), "{what}: ({after}, {through}]");
        let through = pick();
        let want = FragmentSet::from_records(held_records(e).map(|(r, _)| r).filter(|r| r.seq <= through));
        assert_eq!(e.range_tombstones_through(through), want, "{what}: through {through}");
        let (limit, tail_through) = (pick(), if i % 3 == 0 { u64::MAX } else { pick() });
        let got = e.rt_filter_set(limit, tail_through).map(std::borrow::Cow::into_owned);
        assert_eq!(got, scanned_filter_set(e, limit, tail_through), "{what}: filter {limit} {tail_through}");
    }
    assert_eq!(e.rt_accounted_set().map(std::borrow::Cow::into_owned), scanned_filter_set(e, accounted, u64::MAX), "{what}: accounted set");
    let high = held_records(e).filter(|(_, in_tail)| !in_tail).map(|(r, _)| r.seq).max().unwrap_or(0);
    if high > 0 {
        let want = FragmentSet::from_records(held_records(e).map(|(r, _)| r).filter(|r| r.seq < high));
        assert_eq!(e.range_tombstones_through(high - 1), want, "{what}: through {}", high - 1);
    }
    assert_run_rt_cache_holds(e, what);
}

struct CumulativeQueue {
    cursors: Vec<u64>,
    latest: Vec<u64>,
    ver: u64,
    active: usize,
}

impl CumulativeQueue {
    fn new(partitions: usize) -> Self {
        CumulativeQueue { cursors: vec![0; partitions], latest: vec![0; partitions], ver: 0, active: partitions }
    }

    fn consume<D: BlockDevice>(&mut self, e: &mut Engine<D>, n: u64) -> u64 {
        let mut seq = 0;
        for _ in 0..n {
            let p = self.ver as usize % self.active;
            self.ver += 1;
            self.cursors[p] += 1;
            self.latest[p] = self.ver;
            if self.ver.is_multiple_of(7) {
                e.put(format!("p{p:02}/{:08}", self.cursors[p] + 3).as_bytes(), b"msg", v(self.ver)).unwrap();
            }
            let (lo, hi) = (format!("p{p:02}/"), format!("p{p:02}/{:08}", self.cursors[p]));
            seq = e.apply_range_delete(Some(lo.as_bytes()), Some(hi.as_bytes()), v(self.ver)).unwrap().seq.expect("a rising cursor appends");
        }
        seq
    }
}

fn rt_index_cfg() -> Config {
    Config { seg_blocks: 64, reserved_blocks: 0, ordering: Ordering::Sorted }
}

#[test]
fn rt_record_index_matches_a_full_scan_through_flushes_merges_collection_forget_and_reopen() {
    if merge_env() {
        return;
    }
    for n in [1_000u64, 10_000] {
        let what = |phase: &str| format!("n {n}: {phase}");
        let mut seed = n;
        let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 1 << 17), rt_index_cfg());
        let mut q = CumulativeQueue::new(5);
        for round in 0..8 {
            let last = q.consume(&mut e, n / 8);
            q.active = 4;
            e.advance_range_tombstone_accounted_seq(last.saturating_sub(n / 20));
            assert_rt_index_matches_scan(&e, &mut seed, 8, &what(&format!("round {round} tail")));
            if round % 2 == 0 {
                e.flush(RunBuildOpts::default()).unwrap();
            } else {
                async_flush(&mut e);
            }
            assert_rt_index_matches_scan(&e, &mut seed, 4, &what(&format!("round {round} flushed")));
            q.consume(&mut e, n / 40);
            assert_rt_index_matches_scan(&e, &mut seed, 4, &what(&format!("round {round} tail after flush")));
        }
        assert!(e.runs.iter().filter(|or| !or.rts.is_empty()).count() >= 8, "{}", what("records spread over runs"));
        assert!(e.has_unflushed_range_tombstones());

        let runs = e.runs.len();
        merge_window(&mut e, runs - 3, runs, None).unwrap();
        assert_rt_index_matches_scan(&e, &mut seed, 8, &what("partial merge"));
        let runs = e.runs.len();
        merge_window(&mut e, 0, runs / 2, None).unwrap();
        assert_rt_index_matches_scan(&e, &mut seed, 8, &what("second merge"));

        let last = q.consume(&mut e, n / 40);
        e.advance_range_tombstone_accounted_seq(last);
        e.flush(RunBuildOpts::default()).unwrap();
        let held = e.range_tombstone_records().len();
        let runs = e.runs.len();
        let stats = merge_window(&mut e, 0, runs, Some(v(q.ver / 2))).unwrap();
        let collected = stats.collected_range_tombstones.len();
        assert!(collected > 0 && collected < held, "{}: collected {collected} of {held}", what("collection"));
        assert_eq!(e.range_tombstone_records().len(), held - collected - stats.dominated_range_tombstones);
        assert_eq!(e.range_tombstone_cover(b"p04/00000000"), None, "{}: the retired partition's records are all collected", what("collection"));
        assert_rt_index_matches_scan(&e, &mut seed, 8, &what("collection"));

        let last = q.consume(&mut e, n / 16);
        e.advance_range_tombstone_accounted_seq(last - n / 40);
        e.flush(RunBuildOpts::default()).unwrap();
        q.consume(&mut e, n / 40);
        assert_rt_index_matches_scan(&e, &mut seed, 8, &what("before forget"));
        let hi = format!("p00/{:08}", q.cursors[0] / 2);
        let spans = released(&[Fragment { lo: b"p00/".to_vec(), hi: Some(hi.into_bytes()), version: v(q.latest[0]) }]);
        let held = e.range_tombstone_records();
        assert_eq!(forget_releasing(&mut e, &spans).released_range_tombstones, 1, "{}", what("forget"));
        assert_ne!(e.range_tombstone_records(), held, "{}: the forget clips records", what("forget"));
        assert_each_record_once(&e);
        assert_rt_index_matches_scan(&e, &mut seed, 8, &what("forget"));

        q.consume(&mut e, n / 40);
        e.sync().unwrap();
        let dev = e.device().share();
        drop(e);
        let mut e = Engine::open(dev, rt_index_cfg()).unwrap();
        assert_rt_index_matches_scan(&e, &mut seed, 4, &what("reopen, nothing accounted"));
        let last = e.range_tombstone_max_seq();
        e.advance_range_tombstone_accounted_seq(last - n / 40);
        assert_rt_index_matches_scan(&e, &mut seed, 8, &what("reopen"));
        q.consume(&mut e, n / 8);
        async_flush(&mut e);
        q.consume(&mut e, n / 40);
        assert_rt_index_matches_scan(&e, &mut seed, 8, &what("flush after reopen"));
        let runs = e.runs.len();
        merge_window(&mut e, 0, runs, None).unwrap();
        assert_each_record_once(&e);
        assert_rt_index_matches_scan(&e, &mut seed, 8, &what("full merge after reopen"));
    }
}

fn ns_per_call<T>(iters: u32, mut f: impl FnMut() -> T) -> f64 {
    let started = std::time::Instant::now();
    for _ in 0..iters {
        std::hint::black_box(f());
    }
    started.elapsed().as_nanos() as f64 / iters as f64
}

fn fold_target_of(accounted: &FragmentSet, records: Vec<RtRecord>) -> FragmentSet {
    let mut target = accounted.clone();
    target.set_max_fragments(usize::MAX);
    for r in records {
        target.overlay(r.lo.as_deref(), r.hi.as_deref(), r.version).unwrap();
    }
    target
}

#[test]
#[ignore = "measurement: prints RT fold-target and filter cost at 1k vs 100k held records, seq index vs full scan"]
fn rt_record_index_cost_measurement() {
    for n in [1_000u64, 100_000] {
        let mut e = Engine::create_prewiped(MemBlockDevice::new(4096, 1 << 15), rt_index_cfg());
        let mut q = CumulativeQueue::new(64);
        for _ in 0..10 {
            q.consume(&mut e, n / 10 - 10);
            e.flush(RunBuildOpts::default()).unwrap();
        }
        let last = q.consume(&mut e, 100);
        e.advance_range_tombstone_accounted_seq(last - 1);
        assert_eq!(held_records(&e).count() as u64, n);
        let accounted = e.range_tombstones_through(last - 1);
        let iters = if n > 10_000 { 200 } else { 20_000 };
        let fold_index = ns_per_call(20_000, || fold_target_of(&accounted, e.range_tombstone_records_between(last - 1, last)));
        let fold_scan = ns_per_call(iters, || fold_target_of(&accounted, scanned_records_between(&e, last - 1, last)));
        let max_index = ns_per_call(20_000, || e.range_tombstone_max_seq());
        let max_scan = ns_per_call(iters, || held_records(&e).map(|(r, _)| r.seq).max());
        let filter_index = ns_per_call(20_000, || e.rt_accounted_set().map(std::borrow::Cow::into_owned));
        let filter_scan = ns_per_call(iters, || scanned_filter_set(&e, last - 1, u64::MAX));
        assert_eq!(e.rt_accounted_set().map(std::borrow::Cow::into_owned), scanned_filter_set(&e, last - 1, u64::MAX));
        eprintln!(
            "RT473 held={n} runs={} tail={} fragments={} fold_target_ns index={fold_index:.0} scan={fold_scan:.0} max_seq_ns index={max_index:.0} scan={max_scan:.0} accounted_filter_ns index={filter_index:.0} scan={filter_scan:.0}",
            e.runs.len(),
            e.rts.tail.len(),
            e.range_tombstones().len()
        );
    }
}

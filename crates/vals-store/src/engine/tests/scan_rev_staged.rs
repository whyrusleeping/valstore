use super::*;

pub(super) const UNLIMITED: ScanCoveredBudget = ScanCoveredBudget { filtered_entries: u64::MAX, block_reads: u64::MAX };

fn drive<D: BlockDevice + AsyncRead>(e: &Engine<D>, staged: StagedScanRevPage) -> (StagedScanDone, usize, usize) {
    let StagedScanRevPage::Runs(mut merge) = staged else {
        let StagedScanRevPage::Done(done) = staged else { unreachable!() };
        return (done, 0, 0);
    };
    let (mut mem, mut blocks) = (0, 0);
    vals_rt::block_on(async {
        loop {
            assert_eq!(merge.aggregate_generation(), (e.storage_generation(), e.mutable_generation()));
            match merge.step(e.device()).await.unwrap() {
                StagedScanStep::Done => break,
                StagedScanStep::NeedMem { from } => {
                    mem += 1;
                    e.scan_supply_mem_rev(&mut merge, &from).unwrap();
                }
                StagedScanStep::NeedBlocks { cursor } => {
                    blocks += 1;
                    e.scan_supply_blocks_rev(&mut merge, cursor).unwrap();
                }
            }
        }
    }, || {});
    (merge.finish(), mem, blocks)
}

pub(super) fn page<D: BlockDevice + AsyncRead>(
    e: &Engine<D>, lo: Option<&[u8]>, hi: Option<&[u8]>, entries: usize, bytes: usize, budget: ScanCoveredBudget,
) -> StagedScanDone {
    drive(e, e.stage_scan_page_rev(lo, hi, entries, bytes, budget).unwrap()).0
}

fn walk(e: &Engine<MemBlockDevice>, budget: ScanCoveredBudget, cap: usize) -> Vec<StagedScanDone> {
    let (mut want, _) = e.scan_winners_page(None, None, usize::MAX, usize::MAX).unwrap();
    want.reverse();
    let mut got = Vec::new();
    let mut hi = None;
    let mut pages = Vec::new();
    loop {
        let done = page(e, None, hi.as_deref(), cap, usize::MAX, budget);
        got.extend(done.winners.clone());
        let next = done.resume.clone().or_else(|| done.winners.last().map(|(k, _)| k.clone()));
        if let Some(resume) = &done.resume {
            assert!(done.more);
            assert!(done.winners.last().is_none_or(|(k, _)| resume < k));
        }
        let more = done.more;
        pages.push(done);
        if !more { break; }
        let next = next.expect("truncated page must advance even with no visible winners");
        assert!(hi.as_ref().is_none_or(|h| next < *h), "exclusive upper bound must strictly decrease: {hi:?} -> {next:?}");
        hi = Some(next);
        assert!(pages.len() < 1000, "paged scan must terminate");
    }
    assert_eq!(got, want);
    pages
}

#[test]
fn staged_reverse_refills_both_sources_and_materializes_values_in_reverse_order() {
    let mut e = engine_with_many_block_run(4000);
    for i in 0..9000 {
        let key = format!("m{i:05}");
        e.put(key.as_bytes(), key.as_bytes(), 10000 + i as u128).unwrap();
    }
    let staged = e.stage_scan_page_rev(Some(b"k00017"), Some(b"m08999"), usize::MAX, usize::MAX, UNLIMITED).unwrap();
    let (done, mem, blocks) = drive(&e, staged);
    assert!(mem >= 2 && blocks >= 2, "mem refills {mem}, block refills {blocks}");
    let want = e.scan_winners_page_rev(Some(b"k00017"), Some(b"m08999"), usize::MAX, usize::MAX).unwrap();
    assert_eq!((done.winners.clone(), done.more), want);
    let (keys, plan, more) = e.plan_staged_scan(done).unwrap();
    assert!(!more);
    let expected = |i: usize| (keys[i].0.as_slice(), keys[i].1);
    let mut seen = vec![false; keys.len()];
    for span in 0..plan.num_spans() {
        let records = vals_rt::block_on(plan.execute_span_checked(span, e.device(), &expected), || {}).unwrap();
        for (i, record) in records {
            assert!(!seen[i]);
            seen[i] = true;
            assert_eq!(record.version, keys[i].1);
            assert_eq!(record.value, e.get(&keys[i].0).unwrap().unwrap());
        }
    }
    assert!(seen.into_iter().all(|seen| seen));
}

#[test]
fn staged_reverse_covered_work_pages_make_exclusive_progress_including_empty_key() {
    for runs in [false, true] {
        let mut e = if runs { engine_with_many_block_run(200) } else { engine(Ordering::Sorted) };
        if !runs {
            for i in 0..200 { e.put(format!("k{i:05}").as_bytes(), b"old", 1).unwrap(); }
        }
        for key in [b"".as_slice(), b"\0", b"\0\0", b"\xff", b"\xff\0"] { e.put(key, b"old", 1).unwrap(); }
        let cover = 5u128 << 64;
        e.apply_range_delete(None, None, cover).unwrap();
        e.put(b"k00020", b"new", cover + 1).unwrap();
        e.put_kind(b"k00080", b"", cover + 2, true).unwrap();
        for cap in [1, 17, usize::MAX] {
            let pages = walk(&e, ScanCoveredBudget { filtered_entries: 1, block_reads: u64::MAX }, cap);
            assert!(pages.iter().any(|p| p.winners.is_empty() && p.more));
            assert!(pages.iter().any(|p| p.resume.as_deref() == Some(b"".as_slice())));
            assert!(pages.iter().all(|p| p.rt.filtered_entries <= 1));
            assert!(pages.iter().flat_map(|p| &p.winners).any(|(k, ie)| k == b"k00080" && ie.tombstone));
        }
        if runs {
            let pages = walk(&e, ScanCoveredBudget { filtered_entries: u64::MAX, block_reads: 1 }, usize::MAX);
            assert!(pages.len() > 100, "covered block budget must interrupt a long covered run");
            assert!(pages.iter().all(|p| p.rt.block_reads <= 3), "one filtered run block plus at most one survivor and one lookahead");
        }
    }
}

#[test]
fn staged_reverse_skips_covered_blocks_but_retains_same_hlc_winners() {
    let mut e = engine_with_many_block_run(4000);
    let cover = 5u128 << 64;
    e.put(b"k02000", b"survivor", cover + 1).unwrap();
    e.flush(RunBuildOpts { block_target_bytes: 32, ..RunBuildOpts::default() }).unwrap();
    e.apply_range_delete(Some(b"k00005\0"), Some(b"k03995\0"), cover).unwrap();
    let before = page(&e, None, None, usize::MAX, usize::MAX, UNLIMITED);
    assert_eq!(before.rt.skipped_blocks, 0);
    assert!(e.build_range_tombstone_max_trees() > 0);
    let after = page(&e, None, None, usize::MAX, usize::MAX, UNLIMITED);
    assert_eq!(before.winners, after.winners);
    assert!(after.winners.iter().any(|(k, ie)| k == b"k02000" && ie.version == cover + 1));
    assert!(after.rt.skipped_blocks > 3900 && after.rt.block_reads < 20, "{:?}", after.rt);
    walk(&e, ScanCoveredBudget { filtered_entries: 1, block_reads: 1 }, 1);
    e.apply_range_delete(None, None, cover + (1u128 << 64)).unwrap();
    let done = page(&e, None, None, 1, 0, UNLIMITED);
    assert!(done.winners.is_empty() && !done.more);
    assert_eq!(done.rt.block_reads, 0);
    assert!(done.rt.index_probes < 100);
}

#[test]
fn staged_reverse_generations_capture_runset_and_mutable_changes_at_refill() {
    let mut e = engine_with_many_block_run(200);
    let StagedScanRevPage::Runs(mut merge) = e.stage_scan_page_rev(None, None, usize::MAX, usize::MAX, UNLIMITED).unwrap() else { panic!() };
    assert_eq!(merge.generation(), e.storage_generation());
    assert_eq!(merge.mutable_generation(), e.mutable_generation());
    assert!(matches!(vals_rt::block_on(merge.step(e.device()), || {}).unwrap(), StagedScanStep::NeedBlocks { .. }));
    e.put(b"k99999", b"new", 99999).unwrap();
    assert_ne!(merge.mutable_generation(), e.mutable_generation());
    assert_eq!(merge.generation(), e.storage_generation());
    e.flush(RunBuildOpts::default()).unwrap();
    assert_ne!(merge.generation(), e.storage_generation());
    let done = page(&e, None, None, usize::MAX, usize::MAX, UNLIMITED);
    assert_eq!(done.winners[0].0, b"k99999");
}

#[test]
fn staged_reverse_reports_corrupt_run_block() {
    let e = engine_with_many_block_run(1);
    let staged = e.stage_scan_page_rev(None, None, 1, usize::MAX, UNLIMITED).unwrap();
    let StagedScanRevPage::Runs(mut merge) = staged else { panic!() };
    let lba = e.alloc.segment_range(e.runs[0].meta.handle.segs[0]).0;
    let mut block = vec![0u8; BS];
    e.device().read_at(lba, &mut block).unwrap();
    block[40] ^= 0x04;
    e.device().write_at(lba, &block).unwrap();
    assert!(matches!(vals_rt::block_on(merge.step(e.device()), || {}), Err(EngineError::Snapshot(SnapshotError::Corrupt(_)))));
}

#[test]
fn staged_reverse_reads_async_and_meters_prime_and_refills_only_when_enabled() {
    if isolated_env_test(&[]) { return; }
    use crate::attribution::{SCAN_BLOCKS, SCAN_BYTES, SCAN_CURSORS};
    let (e, reads) = two_run_counting_engine();
    let staged = e.stage_scan_page_rev(None, None, 1, usize::MAX, UNLIMITED).unwrap();
    assert_eq!(reads.load(AtomicOrdering::Relaxed), 0);
    let done = drive(&e, staged).0;
    assert_eq!(done.winners[0].1.version, 7);
    assert_eq!(reads.load(AtomicOrdering::Relaxed), 2);
    assert_eq!((SCAN_CURSORS.get(), SCAN_BLOCKS.get(), SCAN_BYTES.get()), (0, 0, 0));
    let e = engine_with_many_block_run(200);
    for cap in [1, usize::MAX] {
        let StagedScanRevPage::Runs(mut merge) = e.stage_scan_page_rev(None, None, cap, usize::MAX, UNLIMITED).unwrap() else { panic!() };
        let before = (SCAN_CURSORS.get(), SCAN_BLOCKS.get(), SCAN_BYTES.get());
        merge.meter_server_scan();
        merge.meter_server_scan();
        let (done, _, refills) = drive(&e, StagedScanRevPage::Runs(merge));
        assert_eq!(SCAN_CURSORS.get() - before.0, 1);
        assert_eq!(SCAN_BLOCKS.get() - before.1, if cap == 1 { 3 } else { 200 });
        let bytes = SCAN_BYTES.get() - before.2;
        assert_eq!(bytes % BS as u64, 0);
        assert!(bytes >= (SCAN_BLOCKS.get() - before.1) * BS as u64);
        assert_eq!(done.winners[0].0, b"k00199");
        if cap == 1 { assert_eq!(refills, 0); } else { assert!(refills >= 2); }
    }
}

#[test]
fn staged_reverse_empty_unsorted_and_memtable_fast_paths() {
    let mut e = engine(Ordering::Sorted);
    assert!(matches!(e.stage_scan_page_rev(None, None, 1, 0, UNLIMITED).unwrap(), StagedScanRevPage::Done(_)));
    e.put(b"k", b"v", 1).unwrap();
    assert!(matches!(e.stage_scan_page_rev(None, None, 1, 0, UNLIMITED).unwrap(), StagedScanRevPage::Done(_)));
    let done = page(&e, Some(b"z"), Some(b"a"), 1, 0, UNLIMITED);
    assert!(done.winners.is_empty() && !done.more);
    let e = engine(Ordering::Unsorted);
    assert!(matches!(e.stage_scan_page_rev(None, None, 1, 0, UNLIMITED), Err(EngineError::NotSorted)));
}

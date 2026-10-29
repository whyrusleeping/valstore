use super::*;
use crate::keyfilter::KeyFilterParams;
use crate::run::legacy_fixture::build_run_v2;
use crate::vlog::ValuePtr;

fn fmt_key(i: u64) -> Vec<u8> {
    let mut key = format!("{}at://did:plc:{:06}/app.bsky.graph.follow/{i:08}", (b'a' + (i % 4) as u8) as char, i / 50)
        .into_bytes();
    key.resize(64 + (i % 90) as usize, b'k');
    key
}

fn fmt_cfg() -> Config {
    Config {
        seg_blocks: 16,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    }
}

fn fmt_opts() -> RunBuildOpts {
    RunBuildOpts {
        block_target_bytes: 512,
        bloom_bits_per_key: 10,
        num_buckets: VDP_BUCKETS,
    }
}

type Model = std::collections::BTreeMap<Vec<u8>, (u128, Option<Vec<u8>>)>;

fn mixed_format_engine() -> (Engine<MemBlockDevice>, Model) {
    let dev = MemBlockDevice::new(BS, 1 << 17);
    let cfg = fmt_cfg();
    let opts = fmt_opts();
    let mut model = Model::new();
    let mut alloc = crate::alloc::Allocator::new(dev.num_blocks(), cfg.seg_blocks, cfg.reserved_blocks);
    let legacy: Vec<SnapshotEntry> = (0..300u64)
        .map(|i| SnapshotEntry {
            key: fmt_key(i),
            ptr: ValuePtr { segment: crate::alloc::SegmentId(0), offset: 0, len: 0 },
            seq: i,
            version: 10 + i as u128,
            tombstone: true,
        })
        .collect();
    let mut legacy = legacy;
    legacy.sort_by(|a, b| a.key.cmp(&b.key));
    for e in &legacy {
        model.insert(e.key.clone(), (e.version, None));
    }
    let bytes = build_run_v2(&legacy, opts, vdp_bucket);
    let handle = run::write_run(&dev, &mut alloc, &bytes).unwrap();
    let meta = RunMeta {
        handle,
        min_key: legacy.first().unwrap().key.clone(),
        max_key: legacy.last().unwrap().key.clone(),
        entry_count: legacy.len() as u64,
        level: 0,
        value_segs: Some(Vec::new()),
        value_bytes: Some(0),
        tombstone_min_version: Some(10),
        value_seg_bytes: Some(Vec::new()),
    };
    let manifest = Manifest { generation: 1, covered_seq: 300, covered_epoch: 1, runs: vec![meta] };
    snapshot_io::write_manifest(&dev, &mut alloc, &manifest, 1, None).unwrap();
    let mut e = Engine::open(dev, cfg).unwrap();
    for (lo, hi, base, version) in [(200u64, 500u64, 1_000u128, run::RUN_SIDECAR_VERSION), (400, 700, 2_000, run::RUN_VERSION)] {
        for i in lo..hi {
            let key = fmt_key(i);
            if i % 7 == 0 {
                e.put_kind(&key, &[], base + i as u128, true).unwrap();
                model.insert(key, (base + i as u128, Some(Vec::new())));
            } else {
                let value = format!("v{version}-{i}").into_bytes();
                e.put(&key, &value, base + i as u128).unwrap();
                model.insert(key, (base + i as u128, Some(value)));
            }
        }
        run::with_run_write_version(version, || e.flush(opts)).unwrap().unwrap();
    }
    (e, model)
}

pub(super) fn cover_every_run(e: &mut Engine<MemBlockDevice>) -> usize {
    e.key_filter.set_params(KeyFilterParams {
        enabled: true,
        min_keys: 1_000,
        bits_per_key: crate::keyfilter::DEFAULT_BITS_PER_KEY,
        max_bits_per_key: crate::keyfilter::DEFAULT_MAX_BITS_PER_KEY,
        growth_pct: crate::keyfilter::DEFAULT_GROWTH_PCT,
        ..KeyFilterParams::from_lookup(|_| None)
    });
    e.reset_key_filter_for_test();
    let mut committed = 0;
    while let Some(mut plan) = e.cover_scan_plan() {
        let dev = e.device().sibling().unwrap();
        while !plan.step(&dev, 1_000).unwrap() {}
        assert!(e.cover_scan_commit(plan), "cover scan key count must equal entry_count");
        committed += 1;
    }
    committed
}

fn assert_serves_model(e: &Engine<MemBlockDevice>, model: &Model, ctx: &str) {
    for (key, (version, value)) in model {
        assert_eq!(e.version(key).unwrap(), Some(*version), "{ctx} version {key:?}");
        if *version >= 1_000 {
            assert_eq!(&e.get(key).unwrap(), value, "{ctx} get {key:?}");
        }
    }
    let expect: Vec<(Vec<u8>, u128)> = model.iter().map(|(k, (v, _))| (k.clone(), *v)).collect();
    let want = all_buckets();
    let (sync_full, more) = e.version_digest_page(&want, vdp_bucket, None, usize::MAX, 0).unwrap();
    assert!(!more);
    assert_eq!(sync_full, expect, "{ctx} sync digest page");
    assert_eq!(async_digest_page(e, &want, None, usize::MAX), (expect.clone(), false), "{ctx} async digest page");
    let mut paged = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let sync_pg = e.version_digest_page(&want, vdp_bucket, after.as_deref(), 2_000, 0).unwrap();
        assert_eq!(async_digest_page(e, &want, after.as_deref(), 2_000), sync_pg, "{ctx} page after {after:?}");
        after = sync_pg.0.last().map(|(k, _)| k.clone());
        paged.extend(sync_pg.0);
        if !sync_pg.1 {
            break;
        }
    }
    assert_eq!(paged, expect, "{ctx} paged digests");
    let odd: std::collections::HashSet<u32> = [1u32, 3].into_iter().collect();
    let odd_expect: Vec<(Vec<u8>, u128)> = expect.iter().filter(|(k, _)| odd.contains(&vdp_bucket(k))).cloned().collect();
    assert_eq!(async_digest_page(e, &odd, None, usize::MAX).0, odd_expect, "{ctx} async want subset");
    assert_eq!(e.version_digest_page(&odd, vdp_bucket, None, usize::MAX, 0).unwrap().0, odd_expect);
    let mut recovered = e.recovered_versions().unwrap();
    recovered.sort();
    let recovered: Vec<(Vec<u8>, u128)> = recovered.into_iter().map(|(k, v)| (k.to_vec(), v)).collect();
    assert_eq!(recovered, expect, "{ctx} recovered_versions");
}

#[test]
fn mixed_v2_v4_v5_runs_serve_reads_digests_cover_scans_and_merge_to_v5() {
    let (mut e, model) = mixed_format_engine();
    let versions: Vec<u16> = e.runs.iter().map(|or| or.run.format_version()).collect();
    assert_eq!(versions, vec![2, run::RUN_SIDECAR_VERSION, run::RUN_VERSION]);
    assert!(e.runs[2].run.sidecar_len() == 0 && e.runs[1].run.sidecar_len() > 0);
    let stats = e.capacity_stats();
    assert_eq!(stats.sidecar_disk_bytes, e.runs[0].run.sidecar_len() + e.runs[1].run.sidecar_len());
    assert_serves_model(&e, &model, "mixed");
    assert_eq!(cover_every_run(&mut e), 3);
    assert_eq!(e.key_filter_stats().covered_runs, 3);

    let handles = e.run_handles();
    assert!(e.run_version_stream(&handles[0]).unwrap().is_none());
    assert!(matches!(e.run_version_stream(&handles[1]).unwrap(), Some(run::RunVersionStream::Sidecar(_))));
    assert!(matches!(e.run_version_stream(&handles[2]).unwrap(), Some(run::RunVersionStream::Blocks(_))));

    let before = e.version_digest_page(&all_buckets(), vdp_bucket, None, usize::MAX, 0).unwrap();
    e.compact_runs(fmt_opts(), None).unwrap();
    assert_eq!(e.runs.len(), 1);
    assert_eq!(e.runs[0].run.format_version(), run::RUN_VERSION);
    assert_eq!(e.runs[0].run.sidecar_len(), 0);
    assert_eq!(e.version_digest_page(&all_buckets(), vdp_bucket, None, usize::MAX, 0).unwrap(), before);
    assert_serves_model(&e, &model, "merged");
    assert_eq!(e.capacity_stats().sidecar_disk_bytes, 0);

    let dev = e.device().share();
    drop(e);
    let mut reopened = Engine::open(dev, fmt_cfg()).unwrap();
    assert_eq!(reopened.runs[0].run.format_version(), run::RUN_VERSION);
    assert_serves_model(&reopened, &model, "reopened");
    assert_eq!(cover_every_run(&mut reopened), 1);
}

#[test]
fn rollback_write_version_keeps_sidecars_until_a_v5_input_exists() {
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 1 << 16), fmt_cfg());
    let round = |e: &mut Engine<MemBlockDevice>, base: u64| {
        for i in base..base + 100 {
            e.put(&fmt_key(i), b"v", i as u128 + 1).unwrap();
        }
    };
    run::with_run_write_version(run::RUN_SIDECAR_VERSION, || {
        round(&mut e, 0);
        e.flush(fmt_opts()).unwrap();
        round(&mut e, 50);
        run_flush(&mut e, fmt_opts()).unwrap();
        assert!(e.runs.iter().all(|or| or.run.format_version() == run::RUN_SIDECAR_VERSION));
        e.compact_runs(fmt_opts(), None).unwrap();
        assert_eq!(e.runs[0].run.format_version(), run::RUN_SIDECAR_VERSION);
    });
    round(&mut e, 120);
    e.flush(fmt_opts()).unwrap();
    assert_eq!(e.runs[1].run.format_version(), run::RUN_VERSION);
    run::with_run_write_version(run::RUN_SIDECAR_VERSION, || {
        e.compact_runs(fmt_opts(), None).unwrap();
    });
    assert_eq!(e.runs.len(), 1);
    assert_eq!(e.runs[0].run.format_version(), run::RUN_VERSION);
    assert_eq!(e.runs[0].meta.entry_count, 220);
}

#[test]
fn rollback_write_version_5_keeps_v5_until_a_v6_input_exists() {
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 1 << 16), fmt_cfg());
    let round = |e: &mut Engine<MemBlockDevice>, base: u64| {
        for i in base..base + 100 {
            e.put(&fmt_key(i), b"v", ((i as u128 + 1) << 64) | 1).unwrap();
        }
    };
    run::with_run_write_version(run::RUN_BLOCK_VERSION, || {
        round(&mut e, 0);
        e.flush(fmt_opts()).unwrap();
        round(&mut e, 50);
        run_flush(&mut e, fmt_opts()).unwrap();
        assert!(e.runs.iter().all(|or| or.run.format_version() == run::RUN_BLOCK_VERSION));
        e.compact_runs(fmt_opts(), None).unwrap();
        assert_eq!(e.runs[0].run.format_version(), run::RUN_BLOCK_VERSION);
        assert!(!e.runs[0].run.supports_range_tombstones());
        assert_eq!(e.runs[0].run.block_max_hlc(0), None);
    });
    round(&mut e, 120);
    e.flush(fmt_opts()).unwrap();
    assert_eq!(e.runs[1].run.format_version(), run::RUN_VERSION);
    assert!(e.runs[1].run.block_max_hlc(0).is_some());
    run::with_run_write_version(run::RUN_BLOCK_VERSION, || {
        e.compact_runs(fmt_opts(), None).unwrap();
    });
    assert_eq!(e.runs.len(), 1);
    assert_eq!(e.runs[0].run.format_version(), run::RUN_VERSION);
    assert_eq!(e.runs[0].meta.entry_count, 220);
    let max = (0..e.runs[0].run.index_len()).map(|b| e.runs[0].run.block_max_hlc(b).unwrap()).max();
    assert_eq!(max, Some(220));
    let dev = e.device().share();
    drop(e);
    let e = Engine::open(dev, fmt_cfg()).unwrap();
    assert_eq!(e.runs[0].run.format_version(), run::RUN_VERSION);
    assert_eq!(e.get(&fmt_key(219)).unwrap().as_deref(), Some(&b"v"[..]));
}

const TIGHT_SEG_BYTES: usize = 8 * 512;

fn tight_merge_env() -> bool {
    isolated_env_test(&[
        ("VALS_MERGE_TIER_MIN_K", Some("2")),
        ("VALS_VLOG_CO_COMPACT", Some("0")),
        ("VALS_MERGE_FORCE_RUNS", None),
        ("VALS_MERGE_INPUT_CAP_BYTES", None),
        ("VALS_RUN_WRITE_VERSION", None),
    ])
}

fn tight_opts() -> RunBuildOpts {
    RunBuildOpts { block_target_bytes: 1024, ..RunBuildOpts::default() }
}

fn engine_with_v5_runs_fitting_a_v5_merge_exactly(nruns: usize) -> (Engine<MemBlockDevice>, Config) {
    let cfg = Config { seg_blocks: 8, reserved_blocks: 0, ordering: Ordering::Sorted };
    let images: Vec<(Vec<SnapshotEntry>, Vec<u8>)> = (0..nruns)
        .map(|j| {
            let entries: Vec<SnapshotEntry> = (0..10_000)
                .map(|i| SnapshotEntry {
                    key: format!("{j}/{i:08}").into_bytes(),
                    ptr: ValuePtr { segment: SegmentId(0), offset: 0, len: 0 },
                    seq: 1,
                    version: 1,
                    tombstone: true,
                })
                .collect();
            let bytes = run::build_run_versioned(&entries, tight_opts(), run::RUN_BLOCK_VERSION);
            (entries, bytes)
        })
        .collect();
    let input_segs: usize = images.iter().map(|(_, b)| b.len().div_ceil(TIGHT_SEG_BYTES)).sum();
    let v5_out_segs = images.iter().map(|(_, b)| b.len()).sum::<usize>().div_ceil(TIGHT_SEG_BYTES);
    let manifest_segs = 1;
    let total_segs = input_segs + manifest_segs + v5_out_segs + manifest_segs + 1;
    let blocks = total_segs as u64 * cfg.seg_blocks;
    let dev = MemBlockDevice::new(512, blocks);
    let mut alloc = Allocator::new(blocks, cfg.seg_blocks, 0);
    let metas: Vec<RunMeta> = images
        .iter()
        .map(|(entries, bytes)| RunMeta {
            handle: run::write_run(&dev, &mut alloc, bytes).unwrap(),
            min_key: entries[0].key.clone(),
            max_key: entries.last().unwrap().key.clone(),
            entry_count: entries.len() as u64,
            level: 0,
            value_segs: Some(Vec::new()),
            value_bytes: Some(0),
            tombstone_min_version: Some(1),
            value_seg_bytes: Some(Vec::new()),
        })
        .collect();
    let manifest = Manifest { generation: 1, covered_seq: 1, covered_epoch: 1, runs: metas };
    let live = snapshot_io::write_manifest(&dev, &mut alloc, &manifest, 1, None).unwrap();
    assert_eq!(live.segs.len(), manifest_segs);
    assert_eq!(alloc.free_segments() as usize, v5_out_segs + manifest_segs + 1, "test premise: a v5-size merge fits exactly");
    let e = Engine::open(dev, cfg).unwrap();
    let growth: usize = e.runs.iter().map(|or| run::rewrite_growth_bytes(&or.run, run::RUN_VERSION)).sum();
    assert!(growth > TIGHT_SEG_BYTES, "test premise: v6 growth needs more segments than are free");
    (e, cfg)
}

fn run_plan(e: &mut Engine<MemBlockDevice>, plan: CompactPlan<MemBlockDevice>) {
    let written = vals_rt::block_on(compact_and_write(plan, e.dev.clone()), || {}).unwrap();
    e.compact_commit(written).unwrap();
}

#[test]
fn v5_to_v6_rewrite_growth_shrinks_a_merge_that_only_fits_at_v5_size() {
    if tight_merge_env() {
        return;
    }
    let (mut e, _) = engine_with_v5_runs_fitting_a_v5_merge_exactly(3);
    let plan = e.compact_plan(tight_opts(), None).unwrap().expect("a smaller merge that fits");
    assert_eq!(plan.merged_inputs.len(), 2, "the window shrinks instead of failing allocation");
    run_plan(&mut e, plan);
    assert_eq!(e.runs.len(), 2);
    assert!(e.runs.iter().any(|or| or.run.format_version() == run::RUN_VERSION));
    for j in 0..3 {
        assert_eq!(e.version(format!("{j}/00009999").as_bytes()).unwrap(), Some(1));
    }
}

#[test]
fn v5_rollback_merge_is_not_charged_v6_growth() {
    if tight_merge_env() {
        return;
    }
    let (mut e, _) = engine_with_v5_runs_fitting_a_v5_merge_exactly(2);
    run::with_run_write_version(run::RUN_BLOCK_VERSION, || {
        let plan = e.compact_plan(tight_opts(), None).unwrap().expect("a v5 merge fits at v5 size");
        assert_eq!(plan.merged_inputs.len(), 2);
        run_plan(&mut e, plan);
    });
    assert_eq!(e.runs.len(), 1);
    assert_eq!(e.runs[0].run.format_version(), run::RUN_BLOCK_VERSION);
    assert_eq!(e.version(b"1/00009999").unwrap(), Some(1));
}

#[test]
fn corrupt_v5_block_fails_seed_stream_digests_and_cover_scan() {
    let (mut e, _) = mixed_format_engine();
    let idx = 2;
    assert_eq!(e.runs[idx].run.format_version(), run::RUN_VERSION);
    let lba = e.alloc.segment_range(e.runs[idx].meta.handle.segs[0]).0;
    let mut block = vec![0u8; BS];
    e.device().read_at(lba, &mut block).unwrap();
    block[40] ^= 0x04;
    e.device().write_at(lba, &block).unwrap();

    let handle = e.run_handles()[idx].clone();
    let mut stream = e.run_version_stream(&handle).unwrap().unwrap();
    let err = loop {
        match stream.next_step() {
            Ok(run::SidecarCursorStep::Done) => panic!("a corrupt v5 block must not stream clean"),
            Ok(_) => {}
            Err(err) => break err,
        }
    };
    assert!(matches!(err, SnapshotError::Corrupt(_)), "{err:?}");
    let want = all_buckets();
    assert!(e.version_digest_page(&want, vdp_bucket, None, usize::MAX, 0).is_err());
    let plan = e.plan_version_digest_page(&want, vdp_bucket, None, usize::MAX, 0).unwrap();
    let page = vals_rt::block_on(
        Engine::version_digest_page_from_plan(plan, &want, vdp_bucket, None, usize::MAX, 0, e.device()),
        || {},
    );
    let run_id = e.runs[idx].id;
    assert!(
        matches!(page, Err(EngineError::CorruptRunBlock { run, block: Some(0), reason: "run block crc mismatch" }) if run == run_id),
        "{page:?}"
    );
    assert!(e.recovered_versions().is_err());

    e.key_filter.set_params(KeyFilterParams {
        enabled: true,
        min_keys: 1_000,
        bits_per_key: crate::keyfilter::DEFAULT_BITS_PER_KEY,
        max_bits_per_key: crate::keyfilter::DEFAULT_MAX_BITS_PER_KEY,
        growth_pct: crate::keyfilter::DEFAULT_GROWTH_PCT,
        ..KeyFilterParams::from_lookup(|_| None)
    });
    e.reset_key_filter_for_test();
    let bad_id = e.runs[idx].id;
    let mut failed = false;
    while let Some(mut plan) = e.cover_scan_plan() {
        let dev = e.device().sibling().unwrap();
        let mut result = Ok(false);
        while matches!(result, Ok(false)) {
            result = plan.step(&dev, 1_000);
        }
        if plan.run_id == bad_id {
            assert!(result.is_err());
            assert!(e.cover_scan_fail(&plan));
            failed = true;
        } else {
            assert!(result.unwrap());
            assert!(e.cover_scan_commit(plan));
        }
    }
    assert!(failed);
    assert_eq!(e.key_filter_stats().cover_scan_failures, 1);
}

fn bs_key(i: u64) -> Vec<u8> {
    let mut key = format!(
        "{}at://did:plc:{:024}/app.bsky.feed.like/3k{i:013}#",
        (b'a' + (i % 4) as u8) as char,
        i / 40
    )
    .into_bytes();
    key.resize(148, b'b');
    key
}

fn bs_opts(block_target_bytes: usize, bloom_bits_per_key: usize) -> RunBuildOpts {
    RunBuildOpts { block_target_bytes, bloom_bits_per_key, num_buckets: VDP_BUCKETS }
}

const BIG_BLOCK: usize = run::SIDECAR_CURSOR_WINDOW + 128 * 1024;

fn mixed_block_engine() -> (Engine<MemBlockDevice>, Model) {
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 1 << 18), fmt_cfg());
    let mut model = Model::new();
    let rounds = [
        (0u64, 1_500u64, 1_000u128, 4096usize, 12usize, false),
        (1_000, 2_500, 10_000, 32 * 1024, 10, true),
        (2_000, 3_000, 20_000, 4096, 12, true),
        (2_800, 4_000, 30_000, 32 * 1024, 10, false),
        (3_500, 6_000, 40_000, BIG_BLOCK, 10, true),
    ];
    for (lo, hi, base, block, bits, streamed) in rounds {
        for i in lo..hi {
            let key = bs_key(i);
            if i % 7 == 0 {
                e.put_kind(&key, &[], base + i as u128, true).unwrap();
                model.insert(key, (base + i as u128, Some(Vec::new())));
            } else {
                let value = format!("b{block}-{i}").into_bytes();
                e.put(&key, &value, base + i as u128).unwrap();
                model.insert(key, (base + i as u128, Some(value)));
            }
        }
        if streamed {
            run_flush(&mut e, bs_opts(block, bits)).unwrap();
        } else {
            e.flush(bs_opts(block, bits)).unwrap().unwrap();
        }
    }
    (e, model)
}

fn drain_version_stream(e: &Engine<MemBlockDevice>, idx: usize) -> Vec<(Vec<u8>, u128)> {
    let handle = e.run_handles()[idx].clone();
    let mut stream = e.run_version_stream(&handle).unwrap().unwrap();
    assert!(matches!(stream, run::RunVersionStream::Blocks(_)));
    let mut out = Vec::new();
    loop {
        match stream.next_step().unwrap() {
            run::SidecarCursorStep::Entry(entry) => out.push(entry),
            run::SidecarCursorStep::Checkpoint => {}
            run::SidecarCursorStep::Done => break,
        }
    }
    out
}

fn run_entries(e: &Engine<MemBlockDevice>, idx: usize) -> Vec<SnapshotEntry> {
    let mut scan = e.runs[idx].run.block_scan(None, None);
    let mut out = Vec::new();
    while let Some(entry) = scan.next(&e.alloc).unwrap() {
        out.push(entry);
    }
    out
}

fn assert_block_paths(e: &Engine<MemBlockDevice>, model: &Model, ctx: &str) {
    assert_serves_model(e, model, ctx);
    for (n, (key, (version, _))) in model.iter().enumerate() {
        if n % 3 != 0 {
            continue;
        }
        let probed = vals_rt::block_on(Engine::version_from_probe(e.probe_version(key), e.device()), || {}).unwrap();
        assert_eq!(probed, Some(*version), "{ctx} probe_version {key:?}");
        assert_eq!(e.lookup_runs(key).unwrap().map(|entry| entry.version), Some(*version), "{ctx} lookup_runs");
        match e.probe_get(key) {
            StagedGet::Runs { key: k, probes, mutable, .. } => {
                let cold = vals_rt::block_on(resolve_run_probe(&k, &probes, e.device()), || {}).unwrap();
                let best = cold.map(|entry| entry.version).into_iter().chain(mutable.map(|m| m.version)).max();
                assert_eq!(best, Some(*version), "{ctx} probe_get {key:?}");
            }
            _ => panic!("{ctx} probe_get of a flushed key must probe runs"),
        }
    }
    for i in (0..6_000u64).step_by(97) {
        let mut absent = bs_key(i);
        absent.push(b'~');
        assert_eq!(e.version(&absent).unwrap(), None);
        let probed = vals_rt::block_on(Engine::version_from_probe(e.probe_version(&absent), e.device()), || {}).unwrap();
        assert_eq!(probed, None, "{ctx} absent probe");
    }
    let expect_scan: Vec<(Vec<u8>, Vec<u8>)> = model
        .keys()
        .filter_map(|k| e.get(k).unwrap().map(|v| (k.clone(), v)))
        .collect();
    assert_eq!(e.scan(None, None).unwrap(), expect_scan, "{ctx} full scan");
    let mut bounds: Vec<Vec<u8>> = Vec::new();
    for or in &e.runs {
        let n = or.run.index_len();
        for b in [1, n / 3, n / 2, n.saturating_sub(1)] {
            if b > 0 && b < n {
                let first = or.run.block_first_key(b).to_vec();
                let mut after = first.clone();
                after.push(0);
                bounds.push(first);
                bounds.push(after);
            }
        }
    }
    bounds.sort();
    bounds.dedup();
    for w in bounds.windows(2).step_by(2) {
        let (lo, hi) = (Some(w[0].as_slice()), Some(w[1].as_slice()));
        assert_staged_eq_sync(e, lo, hi, usize::MAX, usize::MAX);
        assert_staged_eq_sync(e, lo, None, 37, usize::MAX);
        let want: Vec<&Vec<u8>> = expect_scan.iter().map(|(k, _)| k).filter(|k| k.as_slice() >= w[0].as_slice() && k.as_slice() < w[1].as_slice()).collect();
        let got = e.scan(lo, hi).unwrap();
        assert_eq!(got.iter().map(|(k, _)| k).collect::<Vec<_>>(), want, "{ctx} bounded scan");
        let (sync_n, _, _) = e.winner_count_page(lo, None, usize::MAX, None).unwrap();
        let mut from = w[0].clone();
        let mut total = 0usize;
        loop {
            let plan = e.winner_count_prefetch_plan_bytes(Some(&from), None, 8 * 1024).unwrap();
            let blocks = vals_rt::block_on(plan.read(e.device()), || {});
            let (n, last, more) = e.winner_count_page_prefetched(Some(&from), None, usize::MAX, None, blocks).unwrap().unwrap();
            total += n;
            if !more {
                break;
            }
            from = last.unwrap();
            from.push(0);
        }
        assert_eq!(total, sync_n, "{ctx} prefetched winner count from {:?}", &w[0][..20]);
    }
    for idx in 0..e.runs.len() {
        let entries = run_entries(e, idx);
        assert_eq!(entries.len() as u64, e.runs[idx].meta.entry_count);
        let stream = drain_version_stream(e, idx);
        assert_eq!(stream, entries.iter().map(|x| (x.key.clone(), x.version)).collect::<Vec<_>>(), "{ctx} run {idx} version stream");
        let mut keys = Vec::new();
        let mut source = e.runs[idx].run.key_source(&e.alloc);
        while !source.step(e.device(), 4096, |k| keys.push(k.to_vec())).unwrap() {}
        assert_eq!(keys, entries.iter().map(|x| x.key.clone()).collect::<Vec<_>>(), "{ctx} run {idx} key source");
        let mut pre = e.runs[idx].run.prefetch_scan(&e.alloc, 64 * 1024, 4);
        let mut prefetched = Vec::new();
        while let Some(entry) = pre.next_entry().unwrap() {
            prefetched.push(entry);
        }
        assert_eq!(prefetched, entries, "{ctx} run {idx} prefetch scan");
    }
}

fn block_shape(e: &Engine<MemBlockDevice>) -> Vec<(u64, usize)> {
    e.runs
        .iter()
        .map(|or| (or.meta.entry_count / or.run.index_len() as u64, or.run.max_block_bytes()))
        .collect()
}

#[test]
fn mixed_4k_32k_and_oversized_block_runs_serve_scan_stream_cover_merge_and_reopen() {
    let (mut e, mut model) = mixed_block_engine();
    let shape = block_shape(&e);
    for (i, &(per_block, max_block)) in shape.iter().enumerate() {
        match i {
            0 | 2 => assert!((15..=30).contains(&per_block) && max_block <= 8 * 1024, "4 KiB run {i}: {shape:?}"),
            1 | 3 => assert!((120..=260).contains(&per_block) && (16 * 1024..=40 * 1024).contains(&max_block), "32 KiB run {i}: {shape:?}"),
            _ => assert!(max_block > run::SIDECAR_CURSOR_WINDOW, "oversized-block run {i}: {shape:?}"),
        }
    }
    assert!(e.runs.iter().all(|or| or.run.format_version() == run::RUN_VERSION));
    assert_block_paths(&e, &model, "mixed");
    assert_eq!(cover_every_run(&mut e), 5);
    assert_block_paths(&e, &model, "covered");
    let capacity = e.capacity_stats();
    assert!(capacity.run_filter_resident_bytes > 0);
    assert!(capacity.run_filter_payload_bytes > 0);
    assert!(capacity.run_filter_payload_bytes <= capacity.run_filter_resident_bytes);
    assert!(capacity.fence_payload_bytes > 0);
    assert!(capacity.fence_payload_bytes <= capacity.fence_resident_bytes);
    assert_eq!(capacity.filter_resident_bytes, capacity.run_filter_resident_bytes + e.key_filter.resident_bytes());

    let (verified_before, failures_before) = run::run_build_verify_totals();
    let plan = e.compact_plan(bs_opts(32 * 1024, 10), None).unwrap().expect("five runs plan a merge");
    let inputs = plan.merged_inputs.len();
    assert!(inputs >= 2);
    let wdev = Arc::new(e.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).map_err(|(_, err)| err).unwrap();
    e.compact_commit(written).unwrap();
    let (verified_after, failures_after) = run::run_build_verify_totals();
    assert!(verified_after > verified_before, "merge output must be device-verified");
    assert_eq!(failures_after, failures_before);
    assert_block_paths(&e, &model, "merged to 32 KiB");

    for i in 5_900..6_300u64 {
        let key = bs_key(i);
        let value = format!("late-{i}").into_bytes();
        e.put(&key, &value, 50_000 + i as u128).unwrap();
        model.insert(key, (50_000 + i as u128, Some(value)));
    }
    e.flush(bs_opts(32 * 1024, 10)).unwrap().unwrap();
    assert_eq!(e.runs.len(), 2);
    e.compact_runs(bs_opts(4096, 10), None).unwrap();
    assert_eq!(e.runs.len(), 1);
    assert!((15..=30).contains(&block_shape(&e)[0].0), "{:?}", block_shape(&e));
    assert_block_paths(&e, &model, "merged to 4 KiB");

    let dev = e.device().share();
    drop(e);
    let mut reopened = Engine::open(dev, fmt_cfg()).unwrap();
    assert_block_paths(&reopened, &model, "reopened");
    assert_eq!(cover_every_run(&mut reopened), 1);
    assert_block_paths(&reopened, &model, "reopened covered");
}

#[test]
fn oversized_blocks_survive_reopen_and_merge_with_32k_runs() {
    let (mut e, model) = mixed_block_engine();
    let dev = e.device().share();
    e.sync().unwrap();
    drop(e);
    let mut e = Engine::open(dev, fmt_cfg()).unwrap();
    assert_eq!(e.runs.len(), 5);
    assert!(block_shape(&e)[4].1 > run::SIDECAR_CURSOR_WINDOW);
    assert_block_paths(&e, &model, "reopened mixed");
    e.compact_runs(bs_opts(BIG_BLOCK, 10), None).unwrap();
    assert!(block_shape(&e)[0].1 > run::SIDECAR_CURSOR_WINDOW);
    assert_block_paths(&e, &model, "oversized merge output");
}

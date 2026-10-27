use super::*;
use crate::range_tombstone::version_hlc;

const SEG_BLOCKS: u64 = 8;
const SEG_BYTES: usize = SEG_BLOCKS as usize * BS;

fn small_blocks() -> RunBuildOpts {
    RunBuildOpts { block_target_bytes: 512, ..RunBuildOpts::default() }
}

fn hlc_entries(n: usize) -> Vec<SnapshotEntry> {
    firehose(n)
        .into_iter()
        .enumerate()
        .map(|(i, mut e)| {
            e.version = ((1_000 + (i * 7_919) % 500) as u128) << 64 | 3;
            e
        })
        .collect()
}

fn records() -> Vec<RtRecord> {
    vec![
        RtRecord { lo: Some(b"app.bsky.feed.like/a".to_vec()), hi: Some(b"app.bsky.feed.like/b".to_vec()), version: 900 << 64, seq: 12 },
        RtRecord { lo: None, hi: Some(b"a".to_vec()), version: (901 << 64) | 9, seq: 30 },
        RtRecord { lo: Some(b"z".to_vec()), hi: None, version: 5, seq: 41 },
    ]
}

fn stream_build(
    entries: &[SnapshotEntry],
    opts: RunBuildOpts,
    version: u16,
    rts: Option<(u64, Vec<RtRecord>)>,
    bound: usize,
) -> (Arc<MemBlockDevice>, Allocator, Result<(RunHandle, RunParts), SnapshotError>) {
    let (dev, mut alloc) = dev_alloc(16_384, SEG_BLOCKS);
    let segs = allocate_run_segs(&mut alloc, BS, bound).unwrap();
    let lbas: Vec<u64> = segs.iter().map(|s| alloc.segment_range(*s).0).collect();
    let sink = SegmentStreamSink::new(&*dev, &segs, &lbas, SEG_BYTES, BS);
    let mut builder = RunStreamBuilder::new(sink, opts, entries.len()).version(version);
    if let Some((floor, records)) = rts {
        builder = builder.range_tombstones(floor, records);
    }
    for e in entries {
        builder.push(e);
    }
    let result = block_on(builder.finish_with_parts());
    (dev, alloc, result)
}

fn assert_block_bounds(run: &DeviceRun<MemBlockDevice>, alloc: &Allocator) {
    assert!(run.index_len() > 4, "test premise: several blocks");
    for i in 0..run.index_len() {
        let want = run.read_block_entries(alloc, i).unwrap().iter().map(|e| version_hlc(e.version)).max();
        assert_eq!(run.block_max_hlc(i), want, "block {i}");
    }
    assert_eq!(run.block_max_hlc(run.index_len()), None);
}

#[test]
fn v6_round_trips_block_max_hlc_rt_records_and_floor() {
    let entries = hlc_entries(600);
    let recs = records();
    let (bytes, built) = build_run_checked_rts(
        &entries,
        small_blocks(),
        RUN_VERSION,
        EncodePath::Merge,
        RunSource::Inline,
        RunRts { seq_floor: 41, records: &recs },
    )
    .unwrap();
    assert_eq!((built.min_key.as_slice(), built.max_key.as_slice()), (&entries[0].key[..], &entries[599].key[..]));
    let bound = run_len_upper_bound_for(&entries, small_blocks(), RUN_VERSION) + crate::range_tombstone::rt_block_len(&recs);
    assert!(bytes.len() <= bound);

    let (dev, alloc, result) = stream_build(&entries, small_blocks(), RUN_VERSION, Some((41, recs.clone())), bound);
    let (handle, parts) = result.unwrap();
    assert_eq!(handle.len, bytes.len() as u64, "streamed and in-memory builds agree");
    let lbas: Vec<u64> = handle.segs.iter().map(|s| alloc.segment_range(*s).0).collect();
    assert!(read_run_range_lbas(&*dev, &lbas, SEG_BYTES, 0, bytes.len()).unwrap() == bytes);

    let from_parts = DeviceRun::from_parts(dev.clone(), SEG_BYTES, handle.clone(), parts);
    let opened = DeviceRun::open(dev, &alloc, handle).unwrap();
    for run in [&from_parts, &opened] {
        assert_eq!(run.format_version(), RUN_VERSION);
        assert!(run.supports_range_tombstones() && run.has_range_tombstones());
        assert_eq!(run.rt_seq_floor(), 41);
        assert_eq!(run.read_range_tombstones(&alloc).unwrap(), recs);
        assert_block_bounds(run, &alloc);
        for e in &entries {
            assert_eq!(run.lookup(&alloc, &e.key).unwrap().as_ref(), Some(e));
        }
        let mut scan = run.block_scan(None, None);
        let mut scanned = Vec::new();
        while let Some(e) = scan.next(&alloc).unwrap() {
            scanned.push(e);
        }
        assert_eq!(scanned, entries);
    }
}

#[test]
fn v6_without_records_reads_no_rt_block_and_keeps_its_floor() {
    let entries = hlc_entries(300);
    let (dev, alloc, result) = stream_build(
        &entries,
        small_blocks(),
        RUN_VERSION,
        Some((7, Vec::new())),
        run_len_upper_bound_for(&entries, small_blocks(), RUN_VERSION),
    );
    let (handle, _) = result.unwrap();
    let run = DeviceRun::open(dev, &alloc, handle).unwrap();
    assert!(run.supports_range_tombstones() && !run.has_range_tombstones());
    assert_eq!(run.rt_seq_floor(), 7);
    assert_eq!(run.read_range_tombstones(&alloc).unwrap(), Vec::new());
    assert_block_bounds(&run, &alloc);
}

#[test]
fn v6_fence_costs_eight_bytes_per_block_over_v5() {
    let entries = hlc_entries(2_000);
    let open = |version| {
        let bytes = build_run_versioned(&entries, RunBuildOpts::default(), version);
        let (dev, mut alloc) = dev_alloc(16_384, SEG_BLOCKS);
        let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
        (DeviceRun::open(dev, &alloc, handle).unwrap(), bytes.len())
    };
    let ((v5, v5_len), (v6, v6_len)) = (open(RUN_BLOCK_VERSION), open(RUN_VERSION));
    assert_eq!(v5.index_len(), v6.index_len());
    let blocks = v6.index_len() as u64;
    assert_eq!(v6.resident_stats().fence_bytes - v5.resident_stats().fence_bytes, 8 * blocks);
    assert_eq!(v6_len - v5_len, 8 * blocks as usize + INDEX_RT_TRAILER_LEN);
    assert!(!v5.supports_range_tombstones());
    assert_eq!((v5.block_max_hlc(0), v5.rt_seq_floor()), (None, 0));
}

#[test]
fn rt_only_runs_build_open_and_hold_no_point_key() {
    let recs = records();
    for records in [recs.clone(), Vec::new()] {
        let (bytes, built) = build_run_checked_rts(
            &[],
            small_blocks(),
            RUN_VERSION,
            EncodePath::Flush,
            RunSource::Inline,
            RunRts { seq_floor: 3, records: &records },
        )
        .unwrap();
        assert_eq!(built, RunBuilt::default(), "an RT-only run reports empty point-key bounds");
        let (dev, alloc, result) = stream_build(&[], small_blocks(), RUN_VERSION, Some((3, records.clone())), bytes.len());
        let (handle, _) = result.unwrap();
        assert_eq!(handle.len, bytes.len() as u64);
        let run = DeviceRun::open(dev, &alloc, handle).unwrap();
        assert_eq!((run.entry_count(), run.index_len()), (0, 0));
        assert_eq!(run.read_range_tombstones(&alloc).unwrap(), records);
        assert_eq!(run.has_range_tombstones(), !records.is_empty());
        assert_eq!(run.lookup(&alloc, b"app.bsky.feed.like/a").unwrap(), None);
        assert_eq!(run.block_scan(None, None).next(&alloc).unwrap(), None);
        assert!(!run.might_contain(b"z"));
    }
}

#[test]
fn pre_v6_builds_refuse_rt_records() {
    let recs = records();
    for version in [RUN_SIDECAR_VERSION, RUN_BLOCK_VERSION] {
        let err = build_run_checked_rts(
            &hlc_entries(10),
            small_blocks(),
            version,
            EncodePath::Flush,
            RunSource::Inline,
            RunRts { seq_floor: 0, records: &recs },
        )
        .unwrap_err();
        assert_eq!(err.check, "range-tombstones", "v{version}");
        let (_, _, result) = stream_build(&hlc_entries(10), small_blocks(), version, Some((0, recs.clone())), 1 << 20);
        assert!(matches!(result, Err(SnapshotError::EncodeIntegrity(e)) if e.check == "range-tombstones"), "v{version}");
    }
}

#[test]
fn rt_block_encode_check_catches_a_resealed_flip() {
    let recs = records();
    for byte in [5usize, 30, 60] {
        encode_test_seams::arm_rt_block_flip(byte, 0x01);
        let err = build_run_checked_rts(
            &hlc_entries(10),
            small_blocks(),
            RUN_VERSION,
            EncodePath::Merge,
            RunSource::Inline,
            RunRts { seq_floor: 0, records: &recs },
        )
        .unwrap_err();
        assert_eq!(err.check, "range-tombstones", "flip at {byte}");
    }
}

#[test]
fn index_encode_check_catches_a_flipped_max_hlc_and_trailer() {
    let entries = hlc_entries(40);
    let (bytes, _) = build_run_checked(&entries, small_blocks(), RUN_VERSION, EncodePath::Merge, RunSource::Inline).unwrap();
    let ft = decode_footer(&bytes[bytes.len() - FOOTER_LEN..]).unwrap();
    let first_entry_hlc = 8 + 4 + entries[0].key.len() + 12;
    for at in [first_entry_hlc, ft.index_len as usize - 4 - 8] {
        encode_test_seams::arm_index_flip(at, 0x01);
        let err = build_run_checked(&entries, small_blocks(), RUN_VERSION, EncodePath::Merge, RunSource::Inline)
            .unwrap_err();
        assert_eq!(err.check, "index", "flip at index byte {at}: {err}");
    }
}

#[test]
fn v6_decoders_reject_malformed_index_trailers_and_rt_locators() {
    let entries = hlc_entries(40);
    let bytes = build_run_versioned(&entries, small_blocks(), RUN_VERSION);
    let ft = decode_footer(&bytes[bytes.len() - FOOTER_LEN..]).unwrap();
    let encoded = section(&bytes, ft.index_off, ft.index_len).unwrap().to_vec();
    let (_, rt) = decode_index(&encoded, RUN_VERSION).unwrap();
    assert_eq!(rt, Some(RtLocator { seq_floor: 0, off: ft.index_off, len: 0 }));
    assert!(decode_index(&encoded, RUN_BLOCK_VERSION).is_err(), "a v6 index is not a v5 index");
    let mut cut = encoded[..encoded.len() - 4 - 8].to_vec();
    replace_crc(&mut cut);
    assert!(decode_index(&cut, RUN_VERSION).is_err());
    let filter_end = ft.bloom_off + ft.bloom_len;
    assert_eq!(filter_end, ft.index_off, "no RT block: the index follows the filter");
    let at = |off, len| check_rt_locator(Some(RtLocator { seq_floor: 0, off, len }), &ft);
    assert!(at(filter_end, 0).is_ok());
    assert_eq!(
        at(ft.index_off - 2, 4).unwrap_err().to_string(),
        SnapshotError::Corrupt("range tombstone block outside the run").to_string()
    );
    assert!(at(filter_end - 1, 1).is_err(), "an RT block may not overlap the filter");
    assert!(at(u64::MAX, 2).is_err());
    assert!(check_rt_locator(None, &ft).is_ok());
}

#[test]
fn streamed_v6_build_verify_reads_back_the_rt_block() {
    let entries = hlc_entries(400);
    let recs = records();
    let (reference, _) = build_run_checked_rts(
        &entries,
        small_blocks(),
        RUN_VERSION,
        EncodePath::Merge,
        RunSource::Inline,
        RunRts { seq_floor: 2, records: &recs },
    )
    .unwrap();
    let ft = decode_footer(&reference[reference.len() - FOOTER_LEN..]).unwrap();
    let (_, rt) = decode_index(section(&reference, ft.index_off, ft.index_len).unwrap(), ft.version).unwrap();
    let rt = rt.unwrap();
    assert!(rt.len > 0);
    for verify in [true, false] {
        let mut alloc = Allocator::new(16_384, SEG_BLOCKS, 0);
        let segs = allocate_run_segs(&mut alloc, BS, reference.len()).unwrap();
        let lbas: Vec<u64> = segs.iter().map(|s| alloc.segment_range(*s).0).collect();
        let dev = MutateOnWriteDev {
            inner: MemBlockDevice::new(BS, 16_384),
            mutate: std::sync::Mutex::new(write_mutation(TailFault::Flip(rt.off + 9), &lbas, SEG_BYTES)),
        };
        let sink = SegmentStreamSink::new(&dev, &segs, &lbas, SEG_BYTES, BS);
        let mut builder = RunStreamBuilder::new(sink, small_blocks(), entries.len())
            .version(RUN_VERSION)
            .verify(verify)
            .range_tombstones(2, recs.clone());
        for e in &entries {
            builder.push(e);
        }
        let result = block_on(builder.finish_with_parts());
        assert!(dev.mutate.lock().unwrap().is_none(), "fault was not injected");
        match result {
            Err(SnapshotError::Corrupt(why)) if verify => {
                assert_eq!(why, "run tail differs from its encoded bytes on build verify");
            }
            Ok((handle, parts)) if !verify => {
                let run = DeviceRun::from_parts(Arc::new(dev.inner.share()), SEG_BYTES, handle, parts);
                assert!(matches!(
                    run.read_range_tombstones(&alloc),
                    Err(SnapshotError::Corrupt("range tombstone block crc mismatch"))
                ));
            }
            other => panic!("verify={verify}: {:?}", other.map(|(h, _)| h)),
        }
    }
}

#[test]
fn rewrite_growth_covers_a_v5_input_rewritten_as_v6() {
    for (n, opts) in [(1usize, RunBuildOpts::default()), (2_000, RunBuildOpts::default()), (2_000, small_blocks())] {
        let entries = hlc_entries(n);
        let v5 = build_run_versioned(&entries, opts, RUN_BLOCK_VERSION);
        let v6 = build_run_versioned(&entries, opts, RUN_VERSION);
        let (dev, mut alloc) = dev_alloc(16_384, SEG_BLOCKS);
        let handle = write_run(&*dev, &mut alloc, &v5).unwrap();
        let input = DeviceRun::open(dev, &alloc, handle).unwrap();
        assert_eq!(v6.len(), v5.len() + rewrite_growth_bytes(&input, RUN_VERSION), "n={n}");
        assert_eq!(rewrite_growth_bytes(&input, RUN_BLOCK_VERSION), 0);
    }
}

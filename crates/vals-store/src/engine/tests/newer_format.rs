use super::*;

const FENCE_BLOCKS: u64 = 512;

fn fence_cfg() -> Config {
    Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    }
}

fn fence_alloc() -> Allocator {
    let cfg = fence_cfg();
    Allocator::new(FENCE_BLOCKS, cfg.seg_blocks, cfg.reserved_blocks)
}

fn seg_bytes() -> usize {
    fence_cfg().seg_blocks as usize * BS
}

fn read_seg(dev: &MemBlockDevice, seg: SegmentId) -> Vec<u8> {
    let mut buf = vec![0u8; seg_bytes()];
    dev.read_at(fence_alloc().segment_range(seg).0, &mut buf).unwrap();
    buf
}

fn write_seg(dev: &MemBlockDevice, seg: SegmentId, buf: &[u8]) {
    dev.write_at(fence_alloc().segment_range(seg).0, buf).unwrap();
}

fn tail_engine(flush: bool) -> MemBlockDevice {
    tail_engine_in(crate::vlog::FORMAT_VERSION, flush)
}

fn tail_engine_in(format: u16, flush: bool) -> MemBlockDevice {
    crate::vlog::with_vlog_write_version(format, || tail_engine_writes(flush))
}

fn tail_engine_writes(flush: bool) -> MemBlockDevice {
    let dev = MemBlockDevice::new(BS, FENCE_BLOCKS);
    let mut e = Engine::create(dev.share(), fence_cfg()).unwrap();
    for i in 0..4u32 {
        e.put(format!("k{i}").as_bytes(), b"v", 10 + i as u128).unwrap();
    }
    if flush {
        e.flush(RunBuildOpts::default()).unwrap();
        e.put(b"late", b"v", 100).unwrap();
    }
    e.sync().unwrap();
    dev
}

fn first_tail_segment(dev: &MemBlockDevice) -> SegmentId {
    let mut segs = crate::dump::scan_log_segments(dev, &fence_alloc(), BS).unwrap();
    segs.sort_by_key(|(_, ep, _)| *ep);
    segs.iter().find(|(_, _, not_tail)| !*not_tail).expect("a tail segment").0
}

fn second_record(buf: &[u8]) -> (crate::vlog::SegHeader, std::ops::Range<usize>) {
    let (hdr, recs) = crate::vlog::replay_segment_extents(buf).unwrap();
    assert!(recs.len() >= 2, "test premise: the tail segment holds at least two records");
    let (off, len, _) = recs[1];
    (hdr, off as usize..(off + len) as usize)
}

fn open_err(dev: &MemBlockDevice) -> EngineError {
    match Engine::open(dev.share(), fence_cfg()) {
        Ok(_) => panic!("open must refuse a device holding a newer on-disk format"),
        Err(e) => e,
    }
}

#[test]
fn open_refuses_intact_segment_header_of_unknown_format_version() {
    let dev = tail_engine(false);
    let seg = first_tail_segment(&dev);
    let mut buf = read_seg(&dev, seg);
    buf[4..6].copy_from_slice(&(crate::vlog::FORMAT_VERSION + 1).to_le_bytes());
    let crc = crate::crc::crc32(&buf[0..18]);
    buf[18..22].copy_from_slice(&crc.to_le_bytes());
    write_seg(&dev, seg, &buf);

    let err = open_err(&dev);
    assert!(matches!(err, EngineError::Log(LogError::Unsupported("segment format version"))), "{err}");
    let scan = crate::scan_vlog_records(&dev, &fence_cfg(), |_| Ok::<(), ()>(()));
    assert!(scan.is_err(), "value-log dumps must report the newer segment, not skip it");
}

const FORMATS: [u16; 2] = [crate::vlog::SEG_FORMAT_V1, crate::vlog::SEG_FORMAT_V2];

#[test]
fn open_refuses_intact_tail_record_of_unknown_kind() {
    for format in FORMATS {
        let dev = tail_engine_in(format, false);
        let seg = first_tail_segment(&dev);
        let mut buf = read_seg(&dev, seg);
        let (hdr, rec) = second_record(&buf);
        assert_eq!(hdr.format, format);
        crate::vlog::forge::set_kind(&mut buf[rec.clone()], 3);
        crate::vlog::forge::reseal(&mut buf[rec], &hdr);
        write_seg(&dev, seg, &buf);

        let err = open_err(&dev);
        assert!(matches!(err, EngineError::Log(LogError::Unsupported("record kind"))), "format {format}: {err}");
        let scan = crate::scan_vlog_records(&dev, &fence_cfg(), |_| Ok::<(), ()>(()));
        assert!(scan.is_err(), "format {format}: value-log dumps must report the unknown record kind, not stop silently");
    }
}

#[test]
fn torn_or_stale_unknown_kind_record_still_ends_replay_silently() {
    for format in FORMATS {
        for stale in [false, true] {
            let dev = tail_engine_in(format, false);
            let seg = first_tail_segment(&dev);
            let mut buf = read_seg(&dev, seg);
            let (hdr, rec) = second_record(&buf);
            crate::vlog::forge::set_kind(&mut buf[rec.clone()], 3);
            if stale {
                let epoch = crate::vlog::forge::epoch(&buf[rec.clone()], &hdr);
                crate::vlog::forge::set_epoch(&mut buf[rec.clone()], epoch + 1);
                crate::vlog::forge::reseal(&mut buf[rec], &hdr);
            }
            write_seg(&dev, seg, &buf);

            let e = Engine::open(dev.share(), fence_cfg()).unwrap();
            assert_eq!(e.get(b"k0").unwrap().as_deref(), Some(&b"v"[..]), "format {format} stale={stale}");
            assert_eq!(
                e.get(b"k1").unwrap(),
                None,
                "format {format} stale={stale}: replay ends at the unreadable record"
            );
        }
    }
}

fn manifest_head(dev: &MemBlockDevice) -> SegmentId {
    let (_, live) = snapshot_io::load_manifest(dev, &fence_alloc()).unwrap().expect("a manifest");
    assert_eq!(live.segs.len(), 1, "test premise: a one-segment manifest chain");
    live.segs[0]
}

#[test]
fn open_refuses_intact_manifest_of_unknown_version() {
    const CHAIN_HDR_LEN: usize = 46;
    let dev = tail_engine(true);
    let seg = manifest_head(&dev);
    let mut buf = read_seg(&dev, seg);
    let total_len = u64::from_le_bytes(buf[22..30].try_into().unwrap()) as usize;
    let payload = &mut buf[CHAIN_HDR_LEN..CHAIN_HDR_LEN + total_len];
    let version = u16::from_le_bytes(payload[4..6].try_into().unwrap());
    payload[4..6].copy_from_slice(&(version + 1).to_le_bytes());
    let body = payload.len() - 4;
    let crc = crate::crc::crc32(&payload[..body]);
    payload[body..].copy_from_slice(&crc.to_le_bytes());
    write_seg(&dev, seg, &buf);

    let err = open_err(&dev);
    assert!(matches!(err, EngineError::Snapshot(SnapshotError::Unsupported("manifest version"))), "{err}");
}

#[test]
fn open_refuses_intact_manifest_chain_header_of_unknown_version() {
    let dev = tail_engine(true);
    let seg = manifest_head(&dev);
    let mut buf = read_seg(&dev, seg);
    let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
    buf[4..6].copy_from_slice(&(version + 1).to_le_bytes());
    let crc = crate::crc::crc32(&buf[0..42]);
    buf[42..46].copy_from_slice(&crc.to_le_bytes());
    write_seg(&dev, seg, &buf);

    let err = open_err(&dev);
    assert!(matches!(err, EngineError::Snapshot(SnapshotError::Unsupported("chain header version"))), "{err}");
}

fn flip_manifest_payload(dev: &MemBlockDevice, alloc: &Allocator) -> u64 {
    let (_, live) = snapshot_io::load_manifest(dev, alloc).unwrap().expect("a manifest");
    let (lba, _) = alloc.segment_range(live.segs[0]);
    let mut block = vec![0u8; BS];
    dev.read_at(lba, &mut block).unwrap();
    block[46 + 4] ^= 0xff;
    dev.write_at(lba, &block).unwrap();
    live.generation
}

fn assert_manifest_lost(result: Result<Engine<MemBlockDevice>, EngineError>, generation: Option<u64>) {
    match result {
        Ok(_) => panic!("open must refuse a store whose manifest is lost"),
        Err(EngineError::Snapshot(SnapshotError::ManifestLost { generation: g, .. })) if g == generation => {}
        Err(e) => panic!("expected a lost manifest (generation {generation:?}), got: {e}"),
    }
}

#[test]
fn corrupt_manifest_fails_open_until_an_explicit_rebuild_and_a_new_manifest() {
    let dev = tail_engine(true);
    let lost = flip_manifest_payload(&dev, &fence_alloc());
    assert_manifest_lost(Engine::open(dev.share(), fence_cfg()), Some(lost));

    let before = crate::manifest_rebuild_total();
    let e = Engine::open_rebuilding_lost_manifest(dev.share(), fence_cfg()).unwrap();
    assert!(crate::manifest_rebuild_total() > before, "the rebuild is counted");
    assert_eq!(e.run_count(), 0);
    assert_eq!(e.manifest_generation(), lost, "the next manifest supersedes the lost generation");
    for key in ["k0", "k1", "k2", "k3", "late"] {
        assert_eq!(e.get(key.as_bytes()).unwrap().as_deref(), Some(&b"v"[..]), "{key}");
    }
    drop(e);
    assert_manifest_lost(Engine::open(dev.share(), fence_cfg()), Some(lost));

    let mut e = Engine::open_rebuilding_lost_manifest(dev.share(), fence_cfg()).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    e.sync().unwrap();
    drop(e);
    let mut e = Engine::open(dev.share(), fence_cfg()).expect("a new manifest generation supersedes the lost one");
    assert_eq!(e.manifest_generation(), lost + 1);
    for key in ["k0", "k1", "k2", "k3", "late"] {
        assert_eq!(e.get(key.as_bytes()).unwrap().as_deref(), Some(&b"v"[..]), "{key}");
    }
    e.put(b"after", b"v", 1000).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    e.sync().unwrap();
    drop(e);
    let load = snapshot_io::load_manifest_chain(&dev, &fence_alloc()).unwrap();
    assert!(load.stale_heads.is_empty(), "the lost generation's head is retired: {:?}", load.stale_heads);
}

#[test]
fn rebuild_env_lets_plain_open_rebuild_a_lost_manifest() {
    if isolated_env_test(&[(snapshot_io::REBUILD_LOST_MANIFEST_ENV, Some("1"))]) {
        return;
    }
    let dev = tail_engine(true);
    flip_manifest_payload(&dev, &fence_alloc());
    let e = Engine::open(dev.share(), fence_cfg()).expect("the environment requests the rebuild");
    assert_eq!(e.get(b"late").unwrap().as_deref(), Some(&b"v"[..]));
}

#[test]
fn open_wipes_superseded_manifest_heads_a_crash_left_behind() {
    let dev = tail_engine(true);
    let mut alloc = fence_alloc();
    let (man, live) = snapshot_io::load_manifest(&dev, &alloc).unwrap().expect("a manifest");
    let stale_gen = live.generation - 1;
    alloc.mark_allocated(live.segs[0]).unwrap();
    for seg in crate::dump::scan_log_segments(&dev, &alloc, BS).unwrap() {
        alloc.mark_allocated(seg.0).unwrap();
    }
    for meta in &man.runs {
        for seg in meta.handle.segs.iter().chain(meta.value_segs.iter().flatten()) {
            let _ = alloc.mark_allocated(*seg);
        }
    }
    let stale = snapshot_io::write_manifest(&dev, &mut alloc, &man, stale_gen, None).unwrap();
    let load = snapshot_io::load_manifest_chain(&dev, &fence_alloc()).unwrap();
    assert_eq!(load.latest.unwrap().1.generation, live.generation);
    assert_eq!(load.stale_heads, stale.segs[..1]);

    let mut e = Engine::open(dev.share(), fence_cfg()).unwrap();
    assert!(!e.alloc.is_free(stale.segs[0]), "open holds the stale head until the next manifest retires it");
    let load = snapshot_io::load_manifest_chain(&dev, &fence_alloc()).unwrap();
    assert_eq!(load.stale_heads, stale.segs[..1], "open itself writes nothing");
    e.flush(RunBuildOpts::default()).unwrap();
    e.sync().unwrap();
    drop(e);
    let load = snapshot_io::load_manifest_chain(&dev, &fence_alloc()).unwrap();
    assert_eq!(load.latest.unwrap().1.generation, live.generation + 1);
    assert!(load.stale_heads.is_empty(), "the next manifest write wipes the stale head: {:?}", load.stale_heads);
}

#[test]
fn lost_manifest_with_rewrite_segments_fails_open_and_rebuild_replays_them() {
    if co_compact_env(false) {
        return;
    }
    let cfg = Config { seg_blocks: 4, reserved_blocks: 0, ordering: Ordering::Sorted };
    let opts = RunBuildOpts { block_target_bytes: 4096, bloom_bits_per_key: 10, num_buckets: 16 };
    let dev = MemBlockDevice::new(BS, 16384);
    let alloc = Allocator::new(16384, cfg.seg_blocks, 0);
    let value = |r: u32, k: u32| format!("round-{r:02}-{k:03}-{}", "x".repeat(20));
    let mut e = Engine::create_prewiped(dev.share(), cfg);
    for r in 0..10u32 {
        for k in 0..40u32 {
            e.put(format!("key-{k:03}").as_bytes(), value(r, k).as_bytes(), 1 + r as u128 * 1000 + k as u128).unwrap();
            if k % 8 == 0 {
                e.put(format!("cold-{r:02}-{k:03}").as_bytes(), value(r, k).as_bytes(), 1).unwrap();
            }
        }
    }
    run_flush(&mut e, opts);
    let plan = e.compact_plan_pressure(opts, None).unwrap().unwrap();
    assert!(plan.co_compacts(), "test premise: the merge rewrites value-log segments");
    let wdev = Arc::new(e.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    e.compact_commit(written).unwrap();
    e.sync().unwrap();
    let freed: Vec<SegmentId> = crate::dump::scan_log_segments(&dev, &alloc, BS)
        .unwrap()
        .into_iter()
        .filter(|(seg, _, not_tail)| !*not_tail && e.alloc.is_free(*seg))
        .map(|(seg, _, _)| seg)
        .collect();
    assert!(!freed.is_empty(), "test premise: the merge freed rewritten value-log segments");
    for seg in freed {
        dev.write_at(alloc.segment_range(seg).0, &vec![0u8; BS]).unwrap();
    }
    drop(e);
    let segs = crate::dump::scan_log_segments(&dev, &alloc, BS).unwrap();
    assert!(segs.iter().any(|(_, _, not_tail)| *not_tail), "test premise: rewrite segments exist");

    let (_, live) = snapshot_io::load_manifest(&dev, &alloc).unwrap().expect("a manifest");
    let head = alloc.segment_range(live.segs[0]).0;
    let lost = flip_manifest_payload(&dev, &alloc);
    assert_manifest_lost(Engine::open(dev.share(), cfg), Some(lost));
    dev.write_at(head, &vec![0u8; BS]).unwrap();
    assert_manifest_lost(Engine::open(dev.share(), cfg), None);

    let e = Engine::open_rebuilding_lost_manifest(dev.share(), cfg).unwrap();
    for k in 0..40u32 {
        assert_eq!(
            e.get(format!("key-{k:03}").as_bytes()).unwrap().as_deref(),
            Some(value(9, k).as_bytes()),
            "key-{k:03}"
        );
    }
    for r in 0..10u32 {
        for k in (0..40u32).step_by(8) {
            let cold = format!("cold-{r:02}-{k:03}");
            assert_eq!(e.get(cold.as_bytes()).unwrap().as_deref(), Some(value(r, k).as_bytes()), "{cold}");
        }
    }
}

fn rewrite_manifest(dev: &MemBlockDevice, edit: impl FnOnce(&mut crate::manifest::Manifest, SegmentId)) {
    let mut alloc = fence_alloc();
    let (mut man, live) = snapshot_io::load_manifest(dev, &alloc).unwrap().expect("a manifest");
    let log = crate::dump::scan_log_segments(dev, &alloc, BS).unwrap();
    let runs = man.runs.iter().flat_map(|m| m.handle.segs.iter().chain(m.value_segs.iter().flatten()));
    for seg in live.segs.iter().chain(log.iter().map(|(s, _, _)| s)).chain(runs) {
        let _ = alloc.mark_allocated(*seg);
    }
    let next_chain = alloc.allocate().unwrap();
    alloc.free(next_chain).unwrap();
    edit(&mut man, next_chain);
    let new = snapshot_io::write_manifest(dev, &mut alloc, &man, live.generation + 1, Some(&live)).unwrap();
    assert_eq!(new.segs, [next_chain], "test premise: the rewritten manifest's chain segment");
}

fn assert_corrupt_manifest(dev: &MemBlockDevice, case: &str) {
    match Engine::open(dev.share(), fence_cfg()) {
        Ok(e) => panic!("{case}: open accepted the manifest with {} runs", e.run_count()),
        Err(EngineError::Snapshot(SnapshotError::Corrupt(_))) => {}
        Err(e) => panic!("{case}: expected a corrupt-manifest error, got: {e}"),
    }
}

#[test]
fn manifest_segments_out_of_range_fail_open_as_corrupt() {
    for (case, run_seg) in [("value segment", false), ("run segment", true)] {
        let dev = tail_engine(true);
        rewrite_manifest(&dev, |man, _| {
            let far = SegmentId(1 << 40);
            if run_seg {
                man.runs[0].handle.segs.push(far);
            } else {
                man.runs[0].value_segs.as_mut().unwrap().push(far);
            }
        });
        assert_corrupt_manifest(&dev, case);
    }
}

#[test]
fn manifest_segments_claimed_twice_fail_open_as_corrupt() {
    type Edit = fn(&mut crate::manifest::Manifest, SegmentId);
    let cases: [(&str, Edit); 4] = [
        ("two runs share segments", |man, _| man.runs.push(man.runs[0].clone())),
        ("a value segment is a run segment", |man, _| {
            let seg = man.runs[0].handle.segs[0];
            man.runs[0].value_segs.as_mut().unwrap().push(seg);
        }),
        ("a value segment is the chain segment", |man, chain| {
            man.runs[0].value_segs.as_mut().unwrap().push(chain);
        }),
        ("a run segment is the chain segment", |man, chain| man.runs[0].handle.segs.push(chain)),
    ];
    for (case, edit) in cases {
        let dev = tail_engine(true);
        rewrite_manifest(&dev, edit);
        assert_corrupt_manifest(&dev, case);
    }
}

#[test]
fn legacy_manifest_without_value_segments_opens_with_log_segments() {
    let dev = tail_engine(true);
    rewrite_manifest(&dev, |man, _| {
        for run in &mut man.runs {
            run.value_segs = None;
            run.value_bytes = None;
            run.tombstone_min_version = None;
            run.value_seg_bytes = None;
        }
    });
    let mut e = Engine::open(dev.share(), fence_cfg()).expect("a v1-style manifest plus log segments opens");
    assert_eq!(e.run_count(), 1);
    for key in ["k0", "k1", "k2", "k3", "late"] {
        assert_eq!(e.get(key.as_bytes()).unwrap().as_deref(), Some(&b"v"[..]), "{key}");
    }
    e.put(b"after", b"v", 1000).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    e.sync().unwrap();
    drop(e);
    let e = Engine::open(dev.share(), fence_cfg()).unwrap();
    for key in ["k0", "k1", "k2", "k3", "late", "after"] {
        assert_eq!(e.get(key.as_bytes()).unwrap().as_deref(), Some(&b"v"[..]), "{key}");
    }
}

#[test]
fn a_format_1_store_opens_under_format_2_and_keeps_reading_both() {
    let dev = MemBlockDevice::new(BS, FENCE_BLOCKS);
    let old = |i: u32| (format!("old-{i:02}"), format!("v1-{i}"));
    crate::vlog::with_vlog_write_version(crate::vlog::SEG_FORMAT_V1, || {
        let mut e = Engine::create(dev.share(), fence_cfg()).unwrap();
        for i in 0..40u32 {
            let (k, v) = old(i);
            e.put(k.as_bytes(), v.as_bytes(), 10 + i as u128).unwrap();
            if i == 29 {
                e.flush(RunBuildOpts::default()).unwrap();
            }
        }
        e.sync().unwrap();
    });
    let check = |e: &Engine<MemBlockDevice>, overwritten: bool| {
        for i in 0..40u32 {
            let (k, v) = old(i);
            let want = if overwritten && i == 35 { "v2-35".to_string() } else { v };
            assert_eq!(e.get(k.as_bytes()).unwrap().as_deref(), Some(want.as_bytes()), "{k}");
        }
    };
    let mut e = Engine::open(dev.share(), fence_cfg()).unwrap();
    check(&e, false);
    for i in 0..20u32 {
        e.put(format!("new-{i:02}").as_bytes(), b"v2", 100 + i as u128).unwrap();
    }
    e.put(b"old-35", b"v2-35", 1000).unwrap();
    e.sync().unwrap();
    drop(e);

    let mut formats: Vec<u16> = crate::dump::scan_log_segments(&dev, &fence_alloc(), BS)
        .unwrap()
        .iter()
        .map(|(seg, _, _)| crate::vlog::decode_seg_header(&read_seg(&dev, *seg)).unwrap().format)
        .collect();
    formats.dedup();
    assert_eq!(formats.first(), Some(&crate::vlog::SEG_FORMAT_V1), "{formats:?}");
    assert_eq!(formats.last(), Some(&crate::vlog::SEG_FORMAT_V2), "{formats:?}");

    let mut e = Engine::open(dev.share(), fence_cfg()).unwrap();
    check(&e, true);
    for i in 0..20u32 {
        assert_eq!(e.get(format!("new-{i:02}").as_bytes()).unwrap().as_deref(), Some(&b"v2"[..]));
    }
    e.flush(RunBuildOpts::default()).unwrap();
    e.sync().unwrap();
    drop(e);
    let e = Engine::open(dev.share(), fence_cfg()).unwrap();
    check(&e, true);
    assert_eq!(e.get(b"new-19").unwrap().as_deref(), Some(&b"v2"[..]));
}

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

fn reseal_record(buf: &mut [u8], off: usize, edit: impl FnOnce(&mut [u8])) {
    let key_len = u32::from_le_bytes(buf[off + 6..off + 10].try_into().unwrap()) as usize;
    let val_len = u32::from_le_bytes(buf[off + 10..off + 14].try_into().unwrap()) as usize;
    let rec = &mut buf[off..off + HEADER_LEN + key_len + val_len];
    edit(rec);
    let mut c = crate::crc::Crc32::new();
    c.update(&rec[..HEADER_LEN - 4]);
    c.update(&rec[HEADER_LEN..]);
    let crc = c.finalize();
    rec[HEADER_LEN - 4..HEADER_LEN].copy_from_slice(&crc.to_le_bytes());
}

fn second_record_offset(buf: &[u8]) -> usize {
    let (_, recs) = replay_segment(buf).unwrap();
    assert!(recs.len() >= 2, "test premise: the tail segment holds at least two records");
    recs[1].0 as usize
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

#[test]
fn open_refuses_intact_tail_record_of_unknown_kind() {
    let dev = tail_engine(false);
    let seg = first_tail_segment(&dev);
    let mut buf = read_seg(&dev, seg);
    let off = second_record_offset(&buf);
    reseal_record(&mut buf, off, |rec| rec[4] = 3);
    write_seg(&dev, seg, &buf);

    let err = open_err(&dev);
    assert!(matches!(err, EngineError::Log(LogError::Unsupported("record kind"))), "{err}");
    let scan = crate::scan_vlog_records(&dev, &fence_cfg(), |_| Ok::<(), ()>(()));
    assert!(scan.is_err(), "value-log dumps must report the unknown record kind, not stop silently");
}

#[test]
fn torn_or_stale_unknown_kind_record_still_ends_replay_silently() {
    for stale in [false, true] {
        let dev = tail_engine(false);
        let seg = first_tail_segment(&dev);
        let mut buf = read_seg(&dev, seg);
        let off = second_record_offset(&buf);
        if stale {
            reseal_record(&mut buf, off, |rec| {
                rec[4] = 3;
                let epoch = u64::from_le_bytes(rec[22..30].try_into().unwrap());
                rec[22..30].copy_from_slice(&(epoch + 1).to_le_bytes());
            });
        } else {
            buf[off + 4] = 3;
        }
        write_seg(&dev, seg, &buf);

        let e = Engine::open(dev.share(), fence_cfg()).unwrap();
        assert_eq!(e.get(b"k0").unwrap().as_deref(), Some(&b"v"[..]), "stale={stale}");
        assert_eq!(e.get(b"k1").unwrap(), None, "stale={stale}: replay ends at the unreadable record");
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
}

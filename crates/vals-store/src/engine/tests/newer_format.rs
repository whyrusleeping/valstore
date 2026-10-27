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

#[test]
fn corrupt_manifest_still_falls_back_to_log_replay() {
    let dev = tail_engine(true);
    let seg = manifest_head(&dev);
    let mut buf = read_seg(&dev, seg);
    buf[46 + 4] ^= 0xff;
    write_seg(&dev, seg, &buf);
    Engine::open(dev.share(), fence_cfg()).expect("a CRC-invalid manifest is not a newer format");
}

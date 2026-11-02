use super::*;
use crate::run::encode_test_seams as seams;

fn backlinks_with_tombstones(n: usize) -> Vec<SnapshotEntry> {
    let mut entries = backlink_entries(n, 0x0564_0000_0000_0001);
    for e in entries.iter_mut().step_by(7) {
        e.tombstone = true;
    }
    entries
}

fn reads(dev: &MemBlockDevice, alloc: &Allocator, run: &DeviceRun<MemBlockDevice>, entries: &[SnapshotEntry]) -> Vec<String> {
    let mut out = Vec::new();
    for e in entries.iter().step_by(13) {
        out.push(format!("{:?}", run.lookup(alloc, &e.key).unwrap()));
        let mut absent = e.key.clone();
        absent.push(0);
        out.push(format!("{:?}", run.lookup(alloc, &absent).unwrap()));
    }
    let mut scan = run.block_scan(None, None);
    while let Some(e) = scan.next(alloc).unwrap() {
        out.push(format!("{e:?}"));
    }
    let (lo, hi) = (&entries[entries.len() / 3].key, &entries[entries.len() / 2].key);
    let mut scan = run.block_scan_rev(Some(lo), Some(hi));
    while let Some(e) = scan.next(alloc).unwrap() {
        out.push(format!("{e:?}"));
    }
    let mut prefetch = run.prefetch_scan(alloc, 64 << 10, 2);
    while let Some(e) = prefetch.next_entry().unwrap() {
        out.push(format!("{e:?}"));
    }
    for b in 0..run.index_len() {
        out.push(format!("{:?}", run.read_block_entries(alloc, b).unwrap()));
    }
    let mut stream = run.version_stream(alloc).unwrap().unwrap();
    loop {
        match stream.next_step().unwrap() {
            SidecarCursorStep::Entry(pair) => out.push(format!("{pair:?}")),
            SidecarCursorStep::Checkpoint => {}
            SidecarCursorStep::Done => break,
        }
    }
    let mut keys = run.key_source(alloc);
    while !keys.step(dev, 4096, |k| out.push(format!("{k:?}"))).unwrap() {}
    run.for_each_bucket_digest(alloc, |k, v| out.push(format!("{k:?}{v}"))).unwrap();
    let all: std::collections::HashSet<u32> = (0..16).collect();
    let bucket_of = |k: &[u8]| test_bucket(k) % 16;
    out.push(format!("{:?}", drain_version_source(dev, run.version_source(alloc, Some(lo)), &all, &bucket_of, Some(lo)).unwrap()));
    out.push(format!("{:?}", run.want_bucket_digests_page(alloc, &all, bucket_of, Some(lo), 4096, 0).unwrap()));
    out
}

#[test]
fn v7_compresses_blocks_and_every_read_path_matches_v6() {
    let entries = backlinks_with_tombstones(20_000);
    let (dev6, alloc6, v6) = open_built_versioned(&entries, RunBuildOpts::default(), RUN_RT_VERSION);
    let (dev7, alloc7, v7) = open_built_versioned(&entries, RunBuildOpts::default(), RUN_VERSION);
    assert_eq!(v7.format_version(), RUN_VERSION);
    assert!(v7.handle.len * 2 < v6.handle.len, "v7 {} vs v6 {} bytes", v7.handle.len, v6.handle.len);
    assert_eq!(v7.index_len(), v6.index_len());
    assert_eq!(v7.uncompressed_len(), v6.handle.len + INDEX_RAW_TRAILER_LEN as u64);
    assert_eq!(v7.compression_savings(), v6.data_bytes() - v7.data_bytes());
    assert_eq!(v7.max_uncompressed_block_bytes(), v6.max_block_bytes());
    assert!(v7.max_block_bytes() < v6.max_block_bytes());
    assert_eq!((v6.compression_savings(), v6.uncompressed_len()), (0, v6.handle.len));
    assert_eq!(reads(&dev7, &alloc7, &v7, &entries), reads(&dev6, &alloc6, &v6, &entries));

    let (dev, alloc, streamed) =
        with_run_write_version(RUN_VERSION, || build_streamed(&entries, RunBuildOpts::default(), entries.len(), 16_384, 64));
    assert_eq!(streamed.format_version(), RUN_VERSION);
    assert_eq!((streamed.handle.len, streamed.uncompressed_len()), (v7.handle.len, v7.uncompressed_len()));
    assert_eq!(reads(&dev, &alloc, &streamed, &entries), reads(&dev6, &alloc6, &v6, &entries));
}

#[test]
fn v7_blocks_compression_cannot_shrink_keep_the_v6_layout() {
    let mut rng = Rng(0x0564_0000_0000_0002);
    let mut keys: Vec<Vec<u8>> = (0..300).map(|_| (0..64).map(|_| rng.next() as u8).collect()).collect();
    keys.sort();
    keys.dedup();
    let entries: Vec<SnapshotEntry> = keys
        .iter()
        .map(|k| SnapshotEntry {
            key: k.clone(),
            ptr: ValuePtr { segment: SegmentId(rng.next()), offset: rng.next() as u32, len: rng.next() as u32 },
            seq: rng.next(),
            version: (rng.next() as u128) << 64 | rng.next() as u128,
            tombstone: false,
        })
        .collect();
    let opts = RunBuildOpts { block_target_bytes: 1, ..RunBuildOpts::default() };
    let v6 = build_run_versioned(&entries, opts, RUN_RT_VERSION);
    let v7 = build_run_versioned(&entries, opts, RUN_VERSION);
    assert_eq!(v7.len(), v6.len() + INDEX_RAW_TRAILER_LEN);
    let ft = decode_footer(&v7[v7.len() - FOOTER_LEN..]).unwrap();
    assert_eq!(v7[..ft.bloom_off as usize], v6[..ft.bloom_off as usize], "stored blocks are byte-identical to v6");
    let run = Run::parse(v7).unwrap();
    for e in entries.iter().step_by(17) {
        assert_eq!(run.lookup(&e.key).unwrap().as_ref(), Some(e));
    }
}

#[test]
fn v7_build_fails_before_publication_on_a_flipped_compressed_frame() {
    let entries = backlinks_with_tombstones(4_000);
    for at in [9usize, 40, 200] {
        seams::arm_coded_flip(1, at, 0x10);
        let err = build_run_checked(&entries, RunBuildOpts::default(), RUN_VERSION, EncodePath::Flush, RunSource::Inline)
            .unwrap_err();
        assert!(!seams::coded_flip_armed());
        assert!(matches!(err.check, "decode" | "source-hash"), "{err}");
        assert_eq!(err.block, 1, "{err}");
    }
    seams::arm_coded_flip(2, 64, 0x01);
    let (dev, mut alloc) = dev_alloc(16_384, 64);
    let (segs, lbas) = with_run_write_version(RUN_VERSION, || reserve(&mut alloc, BS, &entries, RunBuildOpts::default()));
    let sink = SegmentStreamSink::new(&*dev, &segs, &lbas, 64 * BS, BS);
    let mut builder = RunStreamBuilder::new(sink, RunBuildOpts::default(), entries.len()).version(RUN_VERSION);
    for e in &entries {
        builder.push(e);
    }
    assert!(matches!(block_on(builder.finish()), Err(SnapshotError::EncodeIntegrity(_))));
    assert!(!seams::coded_flip_armed());
}

#[test]
fn resealed_bad_compressed_blocks_are_corrupt_not_panics() {
    let entries = backlinks_with_tombstones(2_000);
    let bytes = build_run_versioned(&entries, RunBuildOpts::default(), RUN_VERSION);
    let first = Run::parse(bytes.clone()).unwrap().index.block_ref(0);
    let (off, len) = (first.0 as usize, first.1 as usize);
    let reseal = |mutate: &dyn Fn(&mut [u8])| {
        let mut b = bytes.clone();
        mutate(&mut b[off..off + len - 4]);
        let crc = crc::crc32(&b[off..off + len - 4]);
        b[off + len - 4..off + len].copy_from_slice(&crc.to_le_bytes());
        Run::parse(b).unwrap().lookup(&entries[1].key)
    };
    assert_eq!(reseal(&|_| {}).unwrap().as_ref(), Some(&entries[1]));
    assert_eq!(reseal(&|b| b[4] = 2), Err("unknown block codec"));
    assert_eq!(reseal(&|b| b[..4].copy_from_slice(&u32::MAX.to_le_bytes())), Err("block decompression failed"));
    assert_eq!(reseal(&|b| b[..4].copy_from_slice(&7u32.to_le_bytes())), Err("block decompression failed"));
    assert_eq!(reseal(&|b| b[len / 2] ^= 0x40), Err("block decompression failed"));
}

#[test]
fn codec_thread_knob_parses_and_falls_back() {
    use crate::run::encode_check::{parse_run_codec_threads, DEFAULT_RUN_CODEC_THREADS};
    assert_eq!(parse_run_codec_threads(None), DEFAULT_RUN_CODEC_THREADS);
    assert_eq!(parse_run_codec_threads(Some(" 8 ")), 8);
    assert_eq!(parse_run_codec_threads(Some("1")), 1);
    for bad in ["0", "65", "x", "-1"] {
        assert_eq!(parse_run_codec_threads(Some(bad)), DEFAULT_RUN_CODEC_THREADS, "{bad}");
    }
}

#[test]
fn version_stream_reserves_a_compressed_block_before_decompressing_it() {
    let entries = backlinks_with_tombstones(6_000);
    let opts = RunBuildOpts { block_target_bytes: 256 * 1024, ..RunBuildOpts::default() };
    let bytes = build_run_versioned(&entries, opts, RUN_VERSION);
    let (dev, mut alloc) = dev_alloc(4096, 64);
    let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
    let run = DeviceRun::open(dev, &alloc, handle).unwrap();
    assert!(run.max_uncompressed_block_bytes() > 2 * run.max_block_bytes());
    let mut stream = run.version_stream(&alloc).unwrap().unwrap();
    assert!(matches!(stream.next_step_reserving(|_| true).unwrap(), Some(SidecarCursorStep::Checkpoint)));
    let before = stream.resident_bound_bytes();
    let mut asked = Vec::new();
    assert!(stream.next_step_reserving(|b| { asked.push(b); false }).unwrap().is_none());
    assert_eq!(stream.resident_bound_bytes(), before, "a denied reservation must not decompress");
    assert!(asked[0] as usize > run.max_block_bytes(), "the first reservation covers the decoded block: {asked:?}");
    let first_block = run.read_block_entries(&alloc, 0).unwrap().len();
    assert!(run.index_len() > 2);
    let mut got = Vec::new();
    while got.len() < first_block {
        if let SidecarCursorStep::Entry(pair) = stream.next_step().unwrap() {
            got.push(pair);
        }
    }
    let before = stream.resident_bound_bytes();
    let mut asked = Vec::new();
    assert!(stream.next_step_reserving(|b| { asked.push(b); false }).unwrap().is_none());
    assert_eq!(stream.resident_bound_bytes(), before, "the next block is not decompressed either");
    loop {
        match stream.next_step().unwrap() {
            SidecarCursorStep::Entry(pair) => got.push(pair),
            SidecarCursorStep::Checkpoint => {}
            SidecarCursorStep::Done => break,
        }
    }
    assert_eq!(got, entries.iter().map(|e| (e.key.clone(), e.version)).collect::<Vec<_>>());
}

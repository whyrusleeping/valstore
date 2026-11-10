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
    let (dev7, alloc7, v7) =
        with_run_block_frame_bytes(0, || open_built_versioned(&entries, RunBuildOpts::default(), RUN_VERSION));
    assert_eq!(v7.format_version(), RUN_VERSION);
    assert!(v7.handle.len * 2 < v6.handle.len, "v7 {} vs v6 {} bytes", v7.handle.len, v6.handle.len);
    assert_eq!(v7.index_len(), v6.index_len());
    assert_eq!(v7.uncompressed_len(), v6.handle.len + INDEX_RAW_TRAILER_LEN as u64);
    assert_eq!(v7.compression_savings(), v6.data_bytes() - v7.data_bytes());
    assert_eq!(v7.max_uncompressed_block_bytes(), v6.max_block_bytes());
    assert!(v7.max_block_bytes() < v6.max_block_bytes());
    assert_eq!((v6.compression_savings(), v6.uncompressed_len()), (0, v6.handle.len));
    assert_eq!(reads(&dev7, &alloc7, &v7, &entries), reads(&dev6, &alloc6, &v6, &entries));

    let (dev, alloc, streamed) = with_run_block_frame_bytes(0, || {
        with_run_write_version(RUN_VERSION, || build_streamed(&entries, RunBuildOpts::default(), entries.len(), 16_384, 64))
    });
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
    let bytes = with_run_block_frame_bytes(0, || build_run_versioned(&entries, RunBuildOpts::default(), RUN_VERSION));
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

fn frame_counts(bytes: &[u8]) -> Vec<usize> {
    let run = Run::parse(bytes.to_vec()).unwrap();
    (0..run.index.len())
        .map(|i| {
            let (off, len) = run.index.block_ref(i);
            block_frame_count(&bytes[off as usize..off as usize + len as usize])
        })
        .collect()
}

#[test]
fn framed_v7_blocks_read_like_v6_on_every_path() {
    let entries = backlinks_with_tombstones(20_000);
    let (dev6, alloc6, v6) = open_built_versioned(&entries, RunBuildOpts::default(), RUN_RT_VERSION);
    let (_, _, single) =
        with_run_block_frame_bytes(0, || open_built_versioned(&entries, RunBuildOpts::default(), RUN_VERSION));
    for frame_bytes in [DEFAULT_RUN_BLOCK_FRAME_BYTES, 4096] {
        let bytes = with_run_block_frame_bytes(frame_bytes, || build_run_versioned(&entries, RunBuildOpts::default(), RUN_VERSION));
        let frames = frame_counts(&bytes);
        let want = 32_768 / frame_bytes;
        assert!(frames[..frames.len() - 1].iter().all(|&n| n >= want), "frame_bytes={frame_bytes}: {frames:?}");
        let run = Run::parse(bytes.clone()).unwrap();
        for e in &entries {
            assert_eq!(run.lookup(&e.key).unwrap().as_ref(), Some(e));
            let mut absent = e.key.clone();
            absent.push(0);
            assert_eq!(run.lookup(&absent).unwrap(), None);
            absent.truncate(e.key.len() - 1);
            assert_eq!(run.lookup(&absent).unwrap(), None);
        }
        let (dev7, alloc7, v7) =
            with_run_block_frame_bytes(frame_bytes, || open_built_versioned(&entries, RunBuildOpts::default(), RUN_VERSION));
        assert_eq!(v7.handle.len as usize, bytes.len());
        assert!(v7.uncompressed_len() > single.uncompressed_len(), "frame restarts lengthen the decoded bodies");
        assert!(v7.max_uncompressed_block_bytes() >= v7.max_block_bytes());
        assert_eq!(reads(&dev7, &alloc7, &v7, &entries), reads(&dev6, &alloc6, &v6, &entries), "frame_bytes={frame_bytes}");
    }
}

#[test]
fn a_framed_lookup_decompresses_only_the_frame_its_key_routes_to() {
    let entries = backlinks_with_tombstones(4_000);
    let bytes = with_run_block_frame_bytes(8192, || build_run_versioned(&entries, RunBuildOpts::default(), RUN_VERSION));
    let (off, len) = Run::parse(bytes.clone()).unwrap().index.block_ref(0);
    let (off, len) = (off as usize, len as usize);
    assert!(block_frame_count(&bytes[off..off + len]) >= 3);
    let block = decode_block(&bytes[off..off + len]).unwrap();
    let (first, last) = (block.first().unwrap(), block.last().unwrap());
    let mut bad = bytes.clone();
    bad[off + len - 6] ^= 0x40;
    let crc = crc::crc32(&bad[off..off + len - 4]);
    bad[off + len - 4..off + len].copy_from_slice(&crc.to_le_bytes());
    let run = Run::parse(bad.clone()).unwrap();
    assert_eq!(run.lookup(&first.key).unwrap().as_ref(), Some(first), "the first frame is intact");
    assert_eq!(run.lookup(&last.key), Err("block decompression failed"));
    assert_eq!(decode_block(&bad[off..off + len]), Err("block decompression failed"));

    let skippable = u32::from_le_bytes(bytes[off + 12..off + 16].try_into().unwrap()) as usize;
    let mut bad = bytes[off..off + len].to_vec();
    bad[16 + skippable + 12] ^= 0x40;
    let crc = crc::crc32(&bad[..len - 4]);
    bad[len - 4..].copy_from_slice(&crc.to_le_bytes());
    assert_eq!(block_lookup(&bad, &first.key), Err("block decompression failed"));
    let mut seek = BlockCursor::new(bad.clone(), Some(&last.key), false).unwrap();
    assert_eq!(seek.next_entry().unwrap().as_ref(), Some(last), "a seek past the first frame skips it");
    assert_eq!(seek.next_entry().unwrap(), None);
    assert!(BlockCursor::new(bad, Some(&first.key), false).is_err());
}

#[test]
fn a_framed_block_index_that_misroutes_lookups_fails_the_build() {
    let entries = backlinks_with_tombstones(4_000);
    let opts = RunBuildOpts::default();
    let bytes = with_run_block_frame_bytes(8192, || build_run_versioned(&entries, opts, RUN_VERSION));
    let (off, _) = Run::parse(bytes.clone()).unwrap().index.block_ref(1);
    let off = off as usize;
    let n = u32::from_le_bytes(bytes[off + 20..off + 24].try_into().unwrap()) as usize;
    assert!(n >= 3);
    for at in [4, 4 + 12 * n + 4, 4 + 12 * n + 5] {
        seams::arm_frame_index_flip(1, at, 0x01);
        let err = with_run_block_frame_bytes(8192, || {
            build_run_checked(&entries, opts, RUN_VERSION, EncodePath::Flush, RunSource::Inline).unwrap_err()
        });
        assert!(!seams::frame_index_flip_armed());
        assert_eq!((err.check, err.block), ("decode", 1), "{err}");
    }
}

#[test]
fn framed_block_bodies_restart_each_frame_and_otherwise_encode_like_plain_bodies() {
    let entries = backlinks_with_tombstones(400);
    let plain = encode_block_body(&entries);
    let (body, frames, extra) = encode_block_body_framed(&entries, 8192);
    assert!(frames.len() >= 6, "{} frames", frames.len());
    assert_eq!(body.len() - extra, plain.len());
    assert_eq!(decode_block_body(&body).unwrap(), entries);
    let coded = compress_block_body_framed(&body, &frames, body.len() - extra).unwrap();
    assert_eq!(check_frame_index(&coded, &body), Ok(()));
    assert_eq!(decode_block_payload(&coded).unwrap().as_ref(), &body[..]);
    let mut block = coded.clone();
    block.extend_from_slice(&crc::crc32(&coded).to_le_bytes());
    assert_eq!(block_frame_count(&block), frames.len());
    for e in &entries {
        assert_eq!(block_lookup(&block, &e.key), Ok(Some(e.clone())));
        let mut absent = e.key.clone();
        absent.push(0);
        assert_eq!(block_lookup(&block, &absent), Ok(None));
    }
    assert_eq!(block_lookup(&block, b""), Ok(None));
    assert_eq!(block_lookup(&block, &[0xff; 200]), Ok(None));
    assert!(compress_block_body_framed(&body, &frames, coded.len()).is_none(), "no smaller than the limit");
    for (n, frame_bytes) in [(10, 8192), (400, 0)] {
        let (b, f, x) = encode_block_body_framed(&entries[..n], frame_bytes);
        assert_eq!((b, f.len(), x), (encode_block_body(&entries[..n]), 0, 0));
    }
}

#[test]
fn block_frame_knob_parses_and_falls_back() {
    assert_eq!(parse_run_block_frame_bytes(None), DEFAULT_RUN_BLOCK_FRAME_BYTES);
    assert_eq!(parse_run_block_frame_bytes(Some(" 8192 ")), 8192);
    assert_eq!(parse_run_block_frame_bytes(Some("0")), 0);
    assert_eq!(parse_run_block_frame_bytes(Some("x")), DEFAULT_RUN_BLOCK_FRAME_BYTES);
    assert_eq!(parse_run_block_frame_bytes(Some("9999999")), 256 * 1024);
}

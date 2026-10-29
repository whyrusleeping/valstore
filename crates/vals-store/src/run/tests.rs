mod bounds;
mod decode_fuzz;
mod v6;
use super::*;

fn entry(key: &[u8], seq: u64, version: u128) -> SnapshotEntry {
    SnapshotEntry {
        key: key.to_vec(),
        ptr: ValuePtr {
            segment: SegmentId(seq),
            offset: seq as u32 * 3,
            len: 17,
        },
        seq,
        version,
        tombstone: false,
    }
}

fn firehose(n: usize) -> Vec<SnapshotEntry> {
    let did = "did:plc:abcdefghijklmnopqrstuvwx";
    (0..n)
        .map(|i| {
            entry(
                format!("app.bsky.feed.like/{did}/3k{i:013}").as_bytes(),
                i as u64,
                0xabab_0000 | i as u128,
            )
        })
        .collect()
}

#[test]
fn sidecar_versions_pages_v2_and_v3() {
    fn record(body: &mut Vec<u8>, key: &[u8], version: u128) {
        body.extend_from_slice(&(key.len() as u32).to_le_bytes());
        body.extend_from_slice(key);
        body.extend_from_slice(&version.to_le_bytes());
    }

    let expected = vec![
        (b"a".to_vec(), 1u128),
        (b"bb".to_vec(), 2u128),
        (b"ccc".to_vec(), 3u128),
        (b"dddd".to_vec(), 4u128),
        (b"eeeee".to_vec(), 5u128),
    ];

    let mut v3 = 8u32.to_le_bytes().to_vec();
    for (key, version) in &expected {
        record(&mut v3, key, *version);
    }
    let mut pager = SidecarVersions::from_body(3, v3).unwrap();
    let mut got = Vec::new();
    assert!(!pager.fold_page(2, |k, v| got.push((k.to_vec(), v))).unwrap());
    assert!(!pager.fold_page(2, |k, v| got.push((k.to_vec(), v))).unwrap());
    assert!(pager.fold_page(2, |k, v| got.push((k.to_vec(), v))).unwrap());
    assert_eq!(got, expected);

    let mut v2 = 3u32.to_le_bytes().to_vec();
    v2.extend_from_slice(&2u32.to_le_bytes());
    for (key, version) in &expected[..2] {
        record(&mut v2, key, *version);
    }
    v2.extend_from_slice(&0u32.to_le_bytes());
    v2.extend_from_slice(&3u32.to_le_bytes());
    for (key, version) in &expected[2..] {
        record(&mut v2, key, *version);
    }
    let mut pager = SidecarVersions::from_body(2, v2).unwrap();
    let mut got = Vec::new();
    assert!(!pager.fold_page(2, |k, v| got.push((k.to_vec(), v))).unwrap());
    assert!(!pager.fold_page(2, |k, v| got.push((k.to_vec(), v))).unwrap());
    assert!(pager.fold_page(2, |k, v| got.push((k.to_vec(), v))).unwrap());
    assert_eq!(got, expected);
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo + 1)
    }
}

#[test]
fn run_len_upper_bound_dominates_build_run_over_randomized_shapes() {
    let mut rng = Rng(0x5331_5333_C0FF_EE01);
    for _ in 0..3000 {
        let n = rng.range(0, 300) as usize;
        let bloom_bits_per_key = rng.range(1, 24) as usize;
        let num_buckets = rng.range(1, 16) as u32;
        let block_target_bytes = if rng.next() & 1 == 0 {
            1
        } else {
            rng.range(1, 8192) as usize
        };
        let opts = RunBuildOpts {
            block_target_bytes,
            bloom_bits_per_key,
            num_buckets,
        };

        let uniform_body = (rng.next() & 1 == 0).then(|| rng.range(0, 200) as usize);
        let mut entries: Vec<SnapshotEntry> = Vec::with_capacity(n);
        for i in 0..n {
            let body_len = match uniform_body {
                Some(len) => len,
                None => match rng.range(0, 2) {
                    0 => 0,
                    1 => rng.range(1, 8) as usize,
                    _ => rng.range(8, 200) as usize,
                },
            };
            let mut key = (i as u64).to_be_bytes().to_vec();
            for _ in 0..body_len {
                key.push((rng.next() & 0xff) as u8);
            }
            entries.push(entry(&key, i as u64, rng.next() as u128));
        }

        for version in [RUN_SIDECAR_VERSION, RUN_VERSION] {
            let bound = run_len_upper_bound_for(&entries, opts, version);
            let actual = build_run_versioned(&entries, opts, version).len();
            assert!(
                bound >= actual,
                "upper bound {bound} < actual {actual} for v{version} n={n}, bloom_bits={bloom_bits_per_key}, \
                 num_buckets={num_buckets}, block_target={block_target_bytes}"
            );
        }
    }
}

#[test]
fn round_trip_preserves_entries() {
    let entries = firehose(50);
    let block = encode_block(&entries);
    assert_eq!(decode_block(&block).unwrap(), entries);
}

#[test]
fn tombstone_flag_round_trips_through_block_and_run() {
    let mut entries: Vec<SnapshotEntry> = firehose(40);
    for (i, e) in entries.iter_mut().enumerate() {
        e.tombstone = i % 3 == 0;
    }
    let block = encode_block(&entries);
    assert_eq!(decode_block(&block).unwrap(), entries);
    for e in &entries {
        let got = block_lookup(&block, &e.key).unwrap().unwrap();
        assert_eq!(got.tombstone, e.tombstone, "flag for {:?}", e.key);
        assert_eq!(&got, e);
    }
    let bytes = build_run(
        &entries,
        RunBuildOpts {
            block_target_bytes: 256,
            bloom_bits_per_key: 10,
            num_buckets: 16,
        },
    );
    let (dev, mut alloc) = dev_alloc(2048, 4);
    let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
    let run = DeviceRun::open(dev, &alloc, handle).unwrap();
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

#[test]
fn v1_run_footer_decodes_as_all_live() {
    let entries = firehose(30);
    let mut bytes = build_run_v3(&entries, RunBuildOpts::default());
    let flen = bytes.len();
    let foff = flen - FOOTER_LEN;
    stamp_footer_version(&mut bytes[foff..], 1);
    let run = Run::parse(bytes).unwrap();
    assert!(matches!(run.bloom, RunFilter::Classic(_)));
    for e in &entries {
        let got = run.lookup(&e.key).unwrap().unwrap();
        assert!(!got.tombstone, "v1 entry must decode as live");
        assert_eq!(&got, e);
    }
}

#[test]
fn round_trip_empty_and_single() {
    assert_eq!(decode_block(&encode_block(&[])).unwrap(), vec![]);
    let one = vec![entry(b"solo", 1, 0x010203)];
    assert_eq!(decode_block(&encode_block(&one)).unwrap(), one);
}

#[test]
fn round_trip_empty_key_and_version() {
    let entries = vec![entry(b"", 0, 0), entry(b"a", 1, u128::MAX)];
    assert_eq!(decode_block(&encode_block(&entries)).unwrap(), entries);
}

#[test]
fn front_coding_actually_shrinks_shared_prefixes() {
    let entries = firehose(100);
    let raw_key_bytes: usize = entries.iter().map(|e| e.key.len()).sum();
    let block = encode_block(&entries);
    assert!(
        block.len() < raw_key_bytes,
        "front-coded block ({}) should beat raw key bytes ({raw_key_bytes})",
        block.len()
    );
}

#[test]
fn block_lookup_finds_present_and_absent() {
    let entries = firehose(64);
    let block = encode_block(&entries);
    for e in &entries {
        assert_eq!(block_lookup(&block, &e.key).unwrap().as_ref(), Some(e));
    }
    assert_eq!(block_lookup(&block, b"app.aaa").unwrap(), None);
    assert_eq!(block_lookup(&block, b"zzz").unwrap(), None);
}

#[test]
fn block_first_key_matches() {
    let entries = firehose(10);
    let block = encode_block(&entries);
    assert_eq!(
        block_first_key(&block).unwrap().as_deref(),
        Some(entries[0].key.as_slice())
    );
    assert_eq!(block_first_key(&encode_block(&[])).unwrap(), None);
}

#[test]
fn crc_mismatch_and_truncation_are_errors() {
    let mut block = encode_block(&firehose(8));
    let n = block.len();
    block[n / 2] ^= 0xff;
    assert_eq!(decode_block(&block), Err("crc mismatch"));
    let good = encode_block(&firehose(8));
    for cut in [0usize, 3, 7, good.len() - 1] {
        assert!(decode_block(&good[..cut]).is_err(), "cut {cut}");
    }
}

fn test_bucket(k: &[u8]) -> u32 {
    bloom_hash(k) as u32
}

#[test]
fn bloom_no_false_negatives() {
    let entries = firehose(500);
    let bloom = Bloom::build(entries.iter().map(|e| e.key.as_slice()), entries.len(), 10);
    for e in &entries {
        assert!(bloom.contains(&e.key), "false negative for {:?}", e.key);
    }
    assert_eq!(Bloom::decode(&bloom.encode()).unwrap(), bloom);
    let fp = (0..2000)
        .filter(|i| bloom.contains(format!("absent-{i}").as_bytes()))
        .count();
    assert!(fp < 200, "false-positive rate too high: {fp}/2000");
}

#[test]
fn run_round_trip_and_lookup() {
    let entries = firehose(300);
    let opts = RunBuildOpts {
        block_target_bytes: 512,
        bloom_bits_per_key: 10,
        num_buckets: 64,
    };
    let bytes = build_run(&entries, opts);
    let run = Run::parse(bytes).unwrap();
    assert_eq!(run.entry_count(), 300);
    assert!(run.index.len() > 1, "expected a multi-block run");
    for e in &entries {
        assert_eq!(run.lookup(&e.key).unwrap().as_ref(), Some(e));
    }
    assert_eq!(run.lookup(b"aaa").unwrap(), None);
    assert_eq!(run.lookup(b"zzz").unwrap(), None);
    assert_eq!(run.lookup(b"app.bsky.feed.like/did:plc:zzz").unwrap(), None);
}

#[test]
fn run_bucket_digests_cover_all_entries_in_key_order() {
    let entries = firehose(400);
    let opts = RunBuildOpts {
        block_target_bytes: 1024,
        bloom_bits_per_key: 10,
        num_buckets: 32,
    };
    let bytes = build_run(&entries, opts);
    let run = Run::parse(bytes).unwrap();

    let mut seen = 0usize;
    for b in 0..run.num_buckets() {
        let digests = run.bucket_digests(b, test_bucket).unwrap();
        assert!(digests.windows(2).all(|w| w[0].0 <= w[1].0));
        for (k, v) in &digests {
            assert_eq!(test_bucket(k) % run.num_buckets(), b);
            let src = entries.iter().find(|e| &e.key == k).unwrap();
            assert_eq!(&src.version.to_le_bytes()[..], &v[..]);
            seen += 1;
        }
    }
    assert_eq!(
        seen,
        entries.len(),
        "every entry must appear in exactly one bucket"
    );
}

#[test]
fn run_footer_corruption_is_an_error() {
    let bytes = build_run(&firehose(20), RunBuildOpts::default());
    let mut bad = bytes.clone();
    let n = bad.len();
    bad[n - 8] ^= 0xff;
    assert!(Run::parse(bad).is_err());
}

#[test]
fn empty_run_round_trips() {
    let bytes = build_run(&[], RunBuildOpts::default());
    let run = Run::parse(bytes).unwrap();
    assert_eq!(run.entry_count(), 0);
    assert_eq!(run.lookup(b"anything").unwrap(), None);
}

use vals_spdk::MemBlockDevice;

const BS: usize = 4096;

fn dev_alloc(num_blocks: u64, seg_blocks: u64) -> (Arc<MemBlockDevice>, Allocator) {
    (
        Arc::new(MemBlockDevice::new(BS, num_blocks)),
        Allocator::new(num_blocks, seg_blocks, 0),
    )
}

#[test]
fn device_run_round_trip_lookup() {
    let entries = firehose(900);
    let bytes = build_run(
        &entries,
        RunBuildOpts {
            block_target_bytes: 512,
            bloom_bits_per_key: 10,
            num_buckets: 64,
        },
    );
    let (dev, mut alloc) = dev_alloc(4096, 8);
    let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
    assert!(handle.segs.len() > 1, "expected a multi-segment run");

    let run = DeviceRun::open(dev, &alloc, handle).unwrap();
    assert_eq!(run.entry_count(), 900);
    for e in &entries {
        assert_eq!(run.lookup(&alloc, &e.key).unwrap().as_ref(), Some(e));
    }
    assert_eq!(run.lookup(&alloc, b"aaa").unwrap(), None);
    assert_eq!(run.lookup(&alloc, b"zzz").unwrap(), None);
}

#[test]
fn from_parts_matches_open() {
    let entries = firehose(900);
    let opts = RunBuildOpts {
        block_target_bytes: 512,
        bloom_bits_per_key: 10,
        num_buckets: 64,
    };
    let bytes = build_run(&entries, opts);
    let (dev, mut alloc) = dev_alloc(4096, 8);
    let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
    assert!(handle.segs.len() > 1, "expected a multi-segment run");

    let seg_bytes = alloc.blocks_per_segment() as usize * dev.block_size();
    let seg_lbas: Vec<u64> = handle
        .segs
        .iter()
        .map(|s| alloc.segment_range(*s).0)
        .collect();
    let parts = read_run_parts(&*dev, &handle, &seg_lbas, seg_bytes).unwrap();
    let from_parts = DeviceRun::from_parts(dev.clone(), seg_bytes, handle.clone(), parts);

    let opened = DeviceRun::open(dev.clone(), &alloc, handle).unwrap();
    assert_eq!(from_parts.entry_count(), opened.entry_count());
    assert_eq!(from_parts.num_buckets(), opened.num_buckets());
    for e in &entries {
        assert_eq!(
            from_parts.lookup(&alloc, &e.key).unwrap(),
            opened.lookup(&alloc, &e.key).unwrap(),
            "lookup differs from_parts vs open for {:?}",
            e.key
        );
        assert_eq!(from_parts.lookup(&alloc, &e.key).unwrap().as_ref(), Some(e));
    }
    assert_eq!(from_parts.lookup(&alloc, b"aaa").unwrap(), None);
    assert_eq!(from_parts.lookup(&alloc, b"zzz").unwrap(), None);
    for b in 0..opts.num_buckets {
        assert_eq!(
            from_parts.bucket_digests(&alloc, b, test_bucket).unwrap(),
            opened.bucket_digests(&alloc, b, test_bucket).unwrap(),
            "bucket {b} digests differ from_parts vs open"
        );
    }
}

#[test]
fn device_run_bucket_digests_match_in_memory() {
    let entries = firehose(250);
    let opts = RunBuildOpts {
        block_target_bytes: 1024,
        bloom_bits_per_key: 10,
        num_buckets: 32,
    };
    let bytes = build_run(&entries, opts);
    let mem = Run::parse(bytes.clone()).unwrap();

    let (dev, mut alloc) = dev_alloc(2048, 4);
    let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
    let run = DeviceRun::open(dev, &alloc, handle).unwrap();

    for b in 0..opts.num_buckets {
        assert_eq!(
            run.bucket_digests(&alloc, b, test_bucket).unwrap(),
            mem.bucket_digests(b, test_bucket).unwrap(),
            "bucket {b} digests differ device vs in-memory"
        );
    }
}

#[test]
fn device_run_want_bucket_digests_match_per_bucket_union() {
    let entries = firehose(250);
    let opts = RunBuildOpts {
        block_target_bytes: 1024,
        bloom_bits_per_key: 10,
        num_buckets: 32,
    };
    let bytes = build_run(&entries, opts);
    let (dev, mut alloc) = dev_alloc(2048, 4);
    let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
    let run = DeviceRun::open(dev, &alloc, handle).unwrap();

    let want: std::collections::HashSet<u32> = [1u32, 4, 7, 30].into_iter().collect();
    let got = run.want_bucket_digests(&alloc, &want, test_bucket).unwrap();
    assert!(got.windows(2).all(|w| w[0].0 <= w[1].0));
    let mut expect: Vec<Digest> = Vec::new();
    for b in &want {
        expect.extend(run.bucket_digests(&alloc, *b, test_bucket).unwrap());
    }
    expect.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(got, expect);
    assert!(run
        .want_bucket_digests(&alloc, &std::collections::HashSet::new(), test_bucket)
        .unwrap()
        .is_empty());
}

#[test]
fn device_run_for_each_bucket_digest_streams_every_bucket_once() {
    let entries = firehose(250);
    let opts = RunBuildOpts {
        block_target_bytes: 1024,
        bloom_bits_per_key: 10,
        num_buckets: 32,
    };
    let bytes = build_run(&entries, opts);
    let (dev, mut alloc) = dev_alloc(2048, 4);
    let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
    let run = DeviceRun::open(dev, &alloc, handle).unwrap();

    let mut got: Vec<(Vec<u8>, u128)> = Vec::new();
    run.for_each_bucket_digest(&alloc, |k, v| got.push((k.to_vec(), v)))
        .unwrap();
    let mut expect: Vec<(Vec<u8>, u128)> = Vec::new();
    for b in 0..opts.num_buckets {
        for (k, vbytes) in run.bucket_digests(&alloc, b, test_bucket).unwrap() {
            expect.push((
                k,
                u128::from_le_bytes(vbytes.as_slice().try_into().unwrap()),
            ));
        }
    }
    got.sort();
    expect.sort();
    assert_eq!(got.len(), entries.len());
    assert_eq!(got, expect);
}

#[test]
fn write_run_device_full_rolls_back() {
    let bytes = build_run(&firehose(2000), RunBuildOpts::default());
    let (dev, mut alloc) = dev_alloc(16, 2);
    let before = alloc.free_segments();
    let err = write_run(&*dev, &mut alloc, &bytes).unwrap_err();
    assert!(matches!(err, SnapshotError::DeviceFull));
    assert_eq!(alloc.free_segments(), before, "reservation rolled back");
}

#[test]
fn device_run_single_segment() {
    let entries = firehose(5);
    let bytes = build_run(&entries, RunBuildOpts::default());
    let (dev, mut alloc) = dev_alloc(256, 64);
    let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
    assert_eq!(handle.segs.len(), 1);
    let run = DeviceRun::open(dev, &alloc, handle).unwrap();
    for e in &entries {
        assert_eq!(run.lookup(&alloc, &e.key).unwrap().as_ref(), Some(e));
    }
}

fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    vals_rt::block_on(fut, || {})
}

fn reserve(
    alloc: &mut Allocator,
    block_size: usize,
    entries: &[SnapshotEntry],
    opts: RunBuildOpts,
) -> (Vec<SegmentId>, Vec<u64>) {
    let bound = run_len_upper_bound(entries, opts);
    let segs = allocate_run_segs(alloc, block_size, bound).unwrap();
    let lbas = segs.iter().map(|s| alloc.segment_range(*s).0).collect();
    (segs, lbas)
}

#[test]
fn stream_sink_bytes_identical_to_write_run_segs() {
    let entries = firehose(4000);
    let opts = RunBuildOpts {
        block_target_bytes: 512,
        bloom_bits_per_key: 10,
        num_buckets: 64,
    };
    let seg_blocks = 8u64;

    let (dev_a, mut alloc_a) = dev_alloc(4096, seg_blocks);
    let run_bytes = build_run(&entries, opts);
    let seg_bytes_a = alloc_a.blocks_per_segment() as usize * BS;
    let (segs_a, lbas_a) = reserve(&mut alloc_a, BS, &entries, opts);
    let n_used_a = run_bytes.len().div_ceil(seg_bytes_a).max(1);
    let handle_a = block_on(write_run_segs(
        &*dev_a,
        &segs_a[..n_used_a],
        &lbas_a[..n_used_a],
        seg_bytes_a,
        BS,
        &run_bytes,
    ))
    .unwrap();

    let (dev_b, mut alloc_b) = dev_alloc(4096, seg_blocks);
    let seg_bytes_b = alloc_b.blocks_per_segment() as usize * BS;
    let (segs_b, lbas_b) = reserve(&mut alloc_b, BS, &entries, opts);
    assert_eq!(segs_a, segs_b);
    assert_eq!(lbas_a, lbas_b);
    let handle_b = {
        let mut sink = SegmentStreamSink::new(&*dev_b, &segs_b, &lbas_b, seg_bytes_b, BS);
        build_run_into(&entries, opts, &mut sink);
        block_on(sink.finish()).unwrap()
    };

    assert_eq!(handle_a, handle_b, "run handles differ");
    assert!(handle_a.segs.len() > 1, "expected a multi-segment run");
    assert_ne!(
        run_bytes.len() % seg_bytes_a,
        0,
        "test should produce a partial final segment"
    );

    for &lba in &lbas_a[..n_used_a] {
        let mut a = vec![0u8; seg_bytes_a];
        let mut b = vec![0u8; seg_bytes_b];
        dev_a.read_at(lba, &mut a).unwrap();
        dev_b.read_at(lba, &mut b).unwrap();
        assert_eq!(
            a, b,
            "segment at lba {lba} differs between Vec and stream paths"
        );
    }
}

#[test]
fn stream_sink_round_trips_through_device_run() {
    let entries = firehose(1500);
    let opts = RunBuildOpts {
        block_target_bytes: 512,
        bloom_bits_per_key: 10,
        num_buckets: 64,
    };
    let (dev, mut alloc) = dev_alloc(4096, 8);
    let seg_bytes = alloc.blocks_per_segment() as usize * BS;
    let (segs, lbas) = reserve(&mut alloc, BS, &entries, opts);
    let handle = {
        let mut sink = SegmentStreamSink::new(&*dev, &segs, &lbas, seg_bytes, BS);
        build_run_into(&entries, opts, &mut sink);
        block_on(sink.finish()).unwrap()
    };
    assert!(handle.segs.len() > 1, "expected a multi-segment run");

    let run = DeviceRun::open(dev, &alloc, handle).unwrap();
    assert_eq!(run.entry_count(), entries.len() as u64);
    for e in &entries {
        assert_eq!(run.lookup(&alloc, &e.key).unwrap().as_ref(), Some(e));
    }
    assert_eq!(run.lookup(&alloc, b"aaa").unwrap(), None);
    assert_eq!(run.lookup(&alloc, b"zzz~~~~").unwrap(), None);
}

#[test]
fn range_locator_trims_to_covered_segments_and_spans_boundaries() {
    let entries = firehose(4000);
    let opts = RunBuildOpts {
        block_target_bytes: 512,
        bloom_bits_per_key: 10,
        num_buckets: 64,
    };
    let (dev, mut alloc) = dev_alloc(4096, 8);
    let run_bytes = build_run(&entries, opts);
    let seg_bytes = alloc.blocks_per_segment() as usize * BS;
    let (segs, lbas) = reserve(&mut alloc, BS, &entries, opts);
    let n_used = run_bytes.len().div_ceil(seg_bytes).max(1);
    let handle = block_on(write_run_segs(
        &*dev,
        &segs[..n_used],
        &lbas[..n_used],
        seg_bytes,
        BS,
        &run_bytes,
    ))
    .unwrap();
    let run = DeviceRun::open(dev.clone(), &alloc, handle).unwrap();
    assert!(
        run.handle.segs.len() > 4,
        "need a many-segment run so an untrimmed locator would spill"
    );

    let total = run.handle.len;
    let sb = seg_bytes as u64;
    let cases: [(u64, usize); 4] = [
        (2 * sb + 5, 100),
        (sb - 7, 20),
        (sb / 2, 3 * seg_bytes),
        (total - 33, 33),
    ];
    for (off, len) in cases {
        assert!(off + len as u64 <= total, "case out of range");
        let loc = run.locate_range(&alloc, off, len);
        let covered = ((off + len as u64 - 1) / sb - off / sb + 1) as usize;
        assert_eq!(
            loc.seg_lbas.len(),
            covered,
            "locator must carry exactly the covered segments (off={off} len={len})"
        );
        let sync = run.range(&alloc, off, len).unwrap();
        let async_bytes = block_on(loc.read(&*dev)).unwrap();
        assert_eq!(
            async_bytes, sync,
            "trimmed locator read diverged from the sync path (off={off} len={len})"
        );
    }

    let loc = run
        .locate_block(&alloc, &entries[entries.len() / 2].key)
        .expect("present key");
    assert!(loc.seg_lbas.len() <= 2, "a block spans at most 2 segments");
    assert!(!loc.seg_lbas.spilled(), "point probes must not heap-allocate");
}

#[test]
fn stream_sink_single_segment_matches() {
    let entries = firehose(5);
    let opts = RunBuildOpts::default();
    let run_bytes = build_run(&entries, opts);

    let (dev_a, mut alloc_a) = dev_alloc(256, 64);
    let seg_bytes = alloc_a.blocks_per_segment() as usize * BS;
    let (segs_a, lbas_a) = reserve(&mut alloc_a, BS, &entries, opts);
    let n_used_a = run_bytes.len().div_ceil(seg_bytes).max(1);
    let handle_a = block_on(write_run_segs(
        &*dev_a,
        &segs_a[..n_used_a],
        &lbas_a[..n_used_a],
        seg_bytes,
        BS,
        &run_bytes,
    ))
    .unwrap();

    let (dev_b, mut alloc_b) = dev_alloc(256, 64);
    let (segs_b, lbas_b) = reserve(&mut alloc_b, BS, &entries, opts);
    let handle_b = {
        let mut sink = SegmentStreamSink::new(&*dev_b, &segs_b, &lbas_b, seg_bytes, BS);
        build_run_into(&entries, opts, &mut sink);
        block_on(sink.finish()).unwrap()
    };
    assert_eq!(handle_a.segs.len(), 1);
    assert_eq!(handle_a, handle_b);
    let mut a = vec![0u8; seg_bytes];
    let mut b = vec![0u8; seg_bytes];
    dev_a.read_at(lbas_a[0], &mut a).unwrap();
    dev_b.read_at(lbas_b[0], &mut b).unwrap();
    assert_eq!(a, b);
}

fn build_streamed(
    entries: &[SnapshotEntry],
    opts: RunBuildOpts,
    n_upper: usize,
    num_blocks: u64,
    seg_blocks: u64,
) -> (Arc<MemBlockDevice>, Allocator, DeviceRun<MemBlockDevice>) {
    let (dev, mut alloc) = dev_alloc(num_blocks, seg_blocks);
    let seg_bytes = alloc.blocks_per_segment() as usize * BS;
    let (segs, lbas) = reserve(&mut alloc, BS, entries, opts);
    let handle = {
        let sink = SegmentStreamSink::new(&*dev, &segs, &lbas, seg_bytes, BS);
        let mut builder = RunStreamBuilder::new(sink, opts, n_upper);
        for e in entries {
            builder.push(e);
        }
        block_on(builder.finish()).unwrap()
    };
    let run = DeviceRun::open(dev.clone(), &alloc, handle).unwrap();
    (dev, alloc, run)
}

#[test]
fn run_stream_builder_read_equivalent_to_build_run() {
    let entries = firehose(1500);
    let opts = RunBuildOpts {
        block_target_bytes: 512,
        bloom_bits_per_key: 10,
        num_buckets: 32,
    };

    let (ref_dev, mut ref_alloc) = dev_alloc(4096, 8);
    let ref_bytes = build_run(&entries, opts);
    let ref_handle = write_run(&*ref_dev, &mut ref_alloc, &ref_bytes).unwrap();
    let ref_run = DeviceRun::open(ref_dev, &ref_alloc, ref_handle).unwrap();

    let (_dev, alloc, run) = build_streamed(&entries, opts, entries.len() + 500, 4096, 8);
    assert!(run.handle.segs.len() > 1, "expected a multi-segment run");

    assert_eq!(run.entry_count(), ref_run.entry_count());
    assert_eq!(run.num_buckets(), ref_run.num_buckets());
    let mut scan = run.block_scan(None, None);
    let mut got = Vec::new();
    while let Some(e) = scan.next(&alloc).unwrap() {
        got.push(e);
    }
    assert_eq!(got, entries);
    for e in &entries {
        assert_eq!(run.lookup(&alloc, &e.key).unwrap().as_ref(), Some(e));
    }
    assert_eq!(run.lookup(&alloc, b"zzz").unwrap(), None);
    for b in 0..opts.num_buckets {
        assert_eq!(
            run.bucket_digests(&alloc, b, test_bucket).unwrap(),
            ref_run.bucket_digests(&ref_alloc, b, test_bucket).unwrap(),
            "bucket {b} digests differ streamed vs slice build"
        );
    }
}

#[test]
fn run_stream_builder_empty_run() {
    let (_dev, alloc, run) = build_streamed(&[], RunBuildOpts::default(), 0, 256, 64);
    assert_eq!(run.entry_count(), 0);
    assert_eq!(run.lookup(&alloc, b"anything").unwrap(), None);
    for b in 0..RunBuildOpts::default().num_buckets {
        assert!(run.bucket_digests(&alloc, b, test_bucket).unwrap().is_empty());
    }
}

fn stamp_footer_version(f: &mut [u8], v: u16) {
    f[4..6].copy_from_slice(&v.to_le_bytes());
    let n = f.len();
    let crc = crc::crc32(&f[..n - 4]);
    f[n - 4..].copy_from_slice(&crc.to_le_bytes());
}

use super::legacy_fixture::build_run_v2;

fn build_run_v3(entries: &[SnapshotEntry], opts: RunBuildOpts) -> Vec<u8> {
    let mut out = Vec::new();
    let mut index = FenceBuilder::default();
    let mut i = 0;
    while i < entries.len() {
        let start = i;
        let mut est = 0usize;
        while i < entries.len() && (i == start || est < opts.block_target_bytes) {
            est += entries[i].key.len() + 24 + 16;
            i += 1;
        }
        let block = encode_block(&entries[start..i]);
        index.push(&entries[start].key, out.len() as u64, block.len() as u32).unwrap();
        out.extend_from_slice(&block);
    }
    let bloom_off = out.len() as u64;
    let bloom = Bloom::build(
        entries.iter().map(|e| e.key.as_slice()),
        entries.len(),
        opts.bloom_bits_per_key,
    );
    let bloom_bytes = bloom.encode();
    out.extend_from_slice(&bloom_bytes);
    let index_off = out.len() as u64;
    let index = index.finish();
    let idx = encode_index_bytes(&index, None);
    out.extend_from_slice(&idx);
    let sidecar_off = out.len() as u64;
    let mut side = Vec::new();
    side.extend_from_slice(&opts.num_buckets.to_le_bytes());
    let mut rec = Vec::new();
    for e in entries {
        encode_sidecar_record(&mut rec, &e.key, e.version);
        side.extend_from_slice(&rec);
    }
    let side_crc = crc::crc32(&side);
    side.extend_from_slice(&side_crc.to_le_bytes());
    out.extend_from_slice(&side);
    let f = encode_footer(&FooterFields {
        entry_count: entries.len() as u64,
        num_blocks: index.len() as u64,
        index_off,
        index_len: idx.len() as u64,
        bloom_off,
        bloom_len: bloom_bytes.len() as u64,
        sidecar_off,
        sidecar_len: side.len() as u64,
        num_buckets: opts.num_buckets,
        version: 3,
    });
    out.extend_from_slice(&f);
    out
}

#[test]
fn v2_and_v3_runs_serve_identical_digests() {
    let entries = firehose(300);
    let opts = RunBuildOpts {
        block_target_bytes: 512,
        bloom_bits_per_key: 10,
        num_buckets: 32,
    };
    let v2_bytes = build_run_v2(&entries, opts, test_bucket);
    let v3_bytes = build_run_v3(&entries, opts);

    let v2_mem = Run::parse(v2_bytes.clone()).unwrap();
    let v3_mem = Run::parse(v3_bytes.clone()).unwrap();
    assert_eq!(v2_mem.version, 2);
    assert_eq!(v3_mem.version, 3);
    assert!(matches!(v3_mem.bloom, RunFilter::Classic(_)));
    for b in 0..opts.num_buckets {
        assert_eq!(
            v2_mem.bucket_digests(b, test_bucket).unwrap(),
            v3_mem.bucket_digests(b, test_bucket).unwrap(),
            "bucket {b} digests differ v2 vs v3 (in-memory)"
        );
    }

    let (dev2, mut alloc2) = dev_alloc(2048, 4);
    let h2 = write_run(&*dev2, &mut alloc2, &v2_bytes).unwrap();
    let v2 = DeviceRun::open(dev2, &alloc2, h2).unwrap();
    let (dev3, mut alloc3) = dev_alloc(2048, 4);
    let h3 = write_run(&*dev3, &mut alloc3, &v3_bytes).unwrap();
    let v3 = DeviceRun::open(dev3, &alloc3, h3).unwrap();
    assert_eq!(v2.format_version(), 2);
    assert_eq!(v3.format_version(), 3);
    for e in &entries {
        assert_eq!(v2.lookup(&alloc2, &e.key).unwrap().as_ref(), Some(e));
        assert_eq!(v3.lookup(&alloc3, &e.key).unwrap().as_ref(), Some(e));
    }
    for b in 0..opts.num_buckets {
        assert_eq!(
            v2.bucket_digests(&alloc2, b, test_bucket).unwrap(),
            v3.bucket_digests(&alloc3, b, test_bucket).unwrap(),
            "bucket {b} digests differ v2 vs v3 (device)"
        );
    }
    let want: std::collections::HashSet<u32> = [0u32, 3, 9, 17, 31].into_iter().collect();
    let got2 = v2.want_bucket_digests(&alloc2, &want, test_bucket).unwrap();
    let got3 = v3.want_bucket_digests(&alloc3, &want, test_bucket).unwrap();
    assert!(got2.windows(2).all(|w| w[0].0 <= w[1].0));
    assert!(got3.windows(2).all(|w| w[0].0 <= w[1].0));
    assert_eq!(got2, got3);
    assert!(!got2.is_empty(), "want set should hit some buckets");
    let mut all2 = Vec::new();
    v2.for_each_bucket_digest(&alloc2, |k, v| all2.push((k.to_vec(), v)))
        .unwrap();
    let mut all3 = Vec::new();
    v3.for_each_bucket_digest(&alloc3, |k, v| all3.push((k.to_vec(), v)))
        .unwrap();
    all2.sort();
    all3.sort();
    assert_eq!(all2.len(), entries.len());
    assert_eq!(all2, all3);
}

#[test]
fn blocked_bloom_no_false_negatives_and_round_trip() {
    let entries = firehose(4000);
    let bloom =
        BlockedBloom::build(entries.iter().map(|e| e.key.as_slice()), entries.len(), 12);
    for e in &entries {
        assert!(bloom.contains(&e.key), "false negative for {:?}", e.key);
    }
    assert_eq!(BlockedBloom::decode(&bloom.encode()).unwrap(), bloom);
    let empty = BlockedBloom::with_capacity(0, 12);
    assert_eq!(empty.num_bits, BLOOM_BLOCK_BITS);
    assert!(!empty.contains(b"anything"));
    let mut one = BlockedBloom::with_capacity(1, 12);
    one.insert(b"solo");
    assert!(one.contains(b"solo"));
}

#[test]
fn bloom_decode_has_exact_capacity() {
    for words in [8, 1 << 20, (1 << 20) + 8] {
        let bits: Vec<u64> = (0..words).map(|i| splitmix64(i as u64)).collect();
        let blocked = BlockedBloom { num_bits: words as u64 * 64, k: 7, bits };
        let decoded = BlockedBloom::decode(&blocked.encode()).unwrap();
        assert_eq!(decoded.bits.capacity(), words);
        assert_eq!(decoded, blocked);
        let classic = Bloom { num_bits: blocked.num_bits, k: blocked.k, bits: blocked.bits };
        let decoded = Bloom::decode(&classic.encode()).unwrap();
        assert_eq!(decoded.bits.capacity(), words);
        assert_eq!(decoded, classic);
    }
}

fn replace_crc(bytes: &mut [u8]) {
    let end = bytes.len() - 4;
    let crc = crc::crc32(&bytes[..end]);
    bytes[end..].copy_from_slice(&crc.to_le_bytes());
}

#[test]
fn bloom_decode_rejects_invalid_lengths() {
    for blocked in [false, true] {
        let (encoded, size_off, header_len) = if blocked {
            (BlockedBloom::with_capacity(1, 10).encode(), 4, 16)
        } else {
            (Bloom::with_capacity(1, 10).encode(), 0, 12)
        };
        for num_bits in [0u64, 1, 512 * 2, u64::MAX - 511] {
            let mut bad = encoded.clone();
            bad[size_off..size_off + 8].copy_from_slice(&num_bits.to_le_bytes());
            replace_crc(&mut bad);
            assert!(RunFilter::decode(if blocked { 4 } else { 3 }, &bad).is_err());
        }
        for len in [0, 3, header_len + 4, encoded.len() - 1] {
            let mut bad = encoded[..len].to_vec();
            if len >= 4 {
                replace_crc(&mut bad);
            }
            assert!(RunFilter::decode(if blocked { 4 } else { 3 }, &bad).is_err());
        }
        let mut extra = encoded.clone();
        extra.push(0);
        replace_crc(&mut extra);
        assert!(RunFilter::decode(if blocked { 4 } else { 3 }, &extra).is_err());
    }
}

#[test]
fn index_decode_has_exact_capacity_and_rejects_invalid_lengths() {
    for n in [0, 3, (1 << 20) + 1] {
        let mut builder = FenceBuilder::default();
        for i in 0..n {
            builder.push(&[], i as u64, 32).unwrap();
        }
        let index = builder.finish();
        let encoded = encode_index_bytes(&index, None);
        let (decoded, rt) = decode_index(&encoded, RUN_BLOCK_VERSION).unwrap();
        assert_eq!((decoded.allocated_bytes(), rt), (n * 16, None));
        assert_eq!(decoded, index);
        assert_eq!(encode_index_bytes(&decoded, None), encoded);
    }
    let decode_index = |b: &[u8]| decode_index(b, RUN_BLOCK_VERSION).map(|(i, _)| i);
    let mut builder = FenceBuilder::default();
    builder.push(b"key", 0, 32).unwrap();
    let encoded = encode_index_bytes(&builder.finish(), None);
    for (n, why) in [(0u64, "trailing bytes"), (2, "truncated"), (u64::MAX, "truncated")] {
        let mut bad = encoded.clone();
        bad[..8].copy_from_slice(&n.to_le_bytes());
        replace_crc(&mut bad);
        assert_eq!(decode_index(&bad).unwrap_err(), why);
    }
    let mut bad = encoded.clone();
    bad[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    replace_crc(&mut bad);
    assert_eq!(decode_index(&bad).unwrap_err(), "truncated");
    let mut bad = encoded[..encoded.len() - 1].to_vec();
    replace_crc(&mut bad);
    assert_eq!(decode_index(&bad).unwrap_err(), "truncated");
    let mut bad = encoded.clone();
    bad[12] ^= 1;
    assert_eq!(decode_index(&bad).unwrap_err(), "index crc mismatch");
    let mut bad = encoded.clone();
    bad.insert(bad.len() - 4, 0);
    replace_crc(&mut bad);
    assert_eq!(decode_index(&bad).unwrap_err(), "trailing bytes");
    for cut in 0..12 {
        let mut bad = encoded[..cut].to_vec();
        if cut >= 4 { replace_crc(&mut bad); }
        assert_eq!(decode_index(&bad).unwrap_err(), "truncated");
    }
    assert_eq!(fence::key_end(u32::MAX as usize, 0), Ok(u32::MAX));
    assert_eq!(fence::key_end(u32::MAX as usize, 1), Err("index key bytes overflow"));
    assert_eq!(fence::key_end(usize::MAX, 1), Err("index key bytes overflow"));
}

#[test]
fn compact_fences_preserve_decode_lookup_and_scan_boundaries() {
    for empty_first in [false, true] {
        let mut entries: Vec<_> = (0..200).map(|i| {
            let key = format!("key/{i:04}/{}", "x".repeat(i % 79));
            let mut e = entry(key.as_bytes(), i as u64, i as u128);
            e.tombstone = i % 7 == 0;
            e
        }).collect();
        if empty_first { entries.insert(0, entry(b"", 0, 0)); }
        let opts = RunBuildOpts { block_target_bytes: 128, ..RunBuildOpts::default() };
        let bytes = build_run_versioned(&entries, opts, RUN_BLOCK_VERSION);
        let ft = decode_footer(&bytes[bytes.len() - FOOTER_LEN..]).unwrap();
        let encoded = section(&bytes, ft.index_off, ft.index_len).unwrap();
        let (decoded, rt) = decode_index(encoded, ft.version).unwrap();
        assert_eq!(encode_index_bytes(&decoded, rt), encoded);
        let mut c = Cursor { b: &encoded[..encoded.len() - 4], off: 0 };
        let n = c.u64().unwrap();
        let reference: Vec<_> = (0..n).map(|_| {
            let len = c.u32().unwrap() as usize;
            (c.take(len).unwrap().to_vec(), c.u64().unwrap(), c.u32().unwrap())
        }).collect();
        assert!(reference.len() > 1);
        for (i, (key, off, len)) in reference.iter().enumerate() {
            assert_eq!(decoded.key(i), key);
            assert_eq!(decoded.block_ref(i), (*off, *len));
        }
        let in_memory = Run::parse(bytes.clone()).unwrap();
        let (dev, mut alloc) = dev_alloc(4096, 4);
        let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
        let run = DeviceRun::open(dev, &alloc, handle).unwrap();
        let mut probes = vec![Vec::new(), b"!".to_vec(), b"zzz".to_vec()];
        for (key, _, _) in &reference {
            probes.push(key.clone());
            let mut between = key.clone();
            between.push(0);
            probes.push(between);
        }
        probes.extend(entries.iter().map(|e| e.key.clone()));
        for key in &probes {
            let upper = reference.partition_point(|(fk, _, _)| fk.as_slice() <= key.as_slice());
            let lower = reference.partition_point(|(fk, _, _)| fk.as_slice() < key.as_slice());
            assert_eq!(decoded.partition_point(|fk| fk <= key.as_slice()), upper);
            assert_eq!(decoded.partition_point(|fk| fk < key.as_slice()), lower);
            assert_eq!(run.straddling_start_block(Some(key)), upper.saturating_sub(1));
            let expected = entries.iter().find(|e| e.key == *key);
            assert_eq!(in_memory.lookup(key).unwrap().as_ref(), expected);
            assert_eq!(run.lookup(&alloc, key).unwrap().as_ref(), expected);
        }
        for lo in [None].into_iter().chain(probes.iter().step_by(17).map(|k| Some(k.as_slice()))) {
            for hi in [None, Some(b"key/0100".as_slice()), Some(b"zzz".as_slice())] {
                let mut scan = run.block_scan(lo, hi);
                let mut got = Vec::new();
                while let Some(e) = scan.next(&alloc).unwrap() { got.push(e); }
                let expected: Vec<_> = entries.iter().filter(|e| {
                    lo.is_none_or(|l| e.key.as_slice() >= l) && hi.is_none_or(|h| e.key.as_slice() < h)
                }).cloned().collect();
                assert_eq!(got, expected);
            }
        }
        let mut scan = run.prefetch_scan(&alloc, 4096, 3);
        let mut got = Vec::new();
        while let Some(e) = scan.next_entry().unwrap() { got.push(e); }
        assert_eq!(got, entries);
    }
}

#[test]
fn resident_stats_distinguish_payload_from_capacity() {
    let mut builder = FenceBuilder::with_capacity(4, 32);
    builder.push(b"key", 0, 32).unwrap();
    let index = builder.finish();
    let mut bloom = BlockedBloom::with_capacity(1, 10);
    bloom.bits.reserve_exact(8);
    let stats = resident_stats(&index, &RunFilter::Blocked(bloom), 0);
    assert_eq!(stats.fence_payload_bytes, 19);
    assert_eq!(stats.fence_bytes, 19);
    assert_eq!(stats.filter_payload_bytes, 64);
    assert_eq!(stats.filter_bytes, 128);
    assert_eq!(stats.total_bytes, stats.fence_bytes + stats.filter_bytes);
}

#[test]
#[ignore]
fn bloom_decode_allocation_measurement() {
    for entries in [10_000_000, 20_000_000, 37_036_223] {
        let bloom = BlockedBloom::with_capacity(entries, 10);
        let encoded = bloom.encode();
        let decoded = BlockedBloom::decode(&encoded).unwrap();
        let mut old = Vec::with_capacity(bloom.bits.len().min(1 << 20));
        for word in encoded[16..encoded.len() - 4].chunks_exact(8) {
            old.push(u64::from_le_bytes(word.try_into().unwrap()));
        }
        assert_eq!(decoded.bits, old);
        let payload = decoded.bits.len() * 8;
        let before = old.capacity() * 8;
        let after = decoded.bits.capacity() * 8;
        assert_eq!(after, payload);
        println!("entries={entries} payload={payload} before={before} after={after} before_B_per_entry={:.6} after_B_per_entry={:.6} saved_B_per_entry={:.6}",
            before as f64 / entries as f64, after as f64 / entries as f64,
            (before - after) as f64 / entries as f64);
    }
}

#[test]
fn hashed_filter_probes_match_keyed_probes() {
    let entries = firehose(3000);
    let keys = || entries.iter().take(1500).map(|e| e.key.as_slice());
    let filters = [
        RunFilter::Blocked(BlockedBloom::build(keys(), 1500, 12)),
        RunFilter::Classic(Bloom::build(keys(), 1500, 12)),
    ];
    for filter in &filters {
        let mut hits = 0;
        for e in &entries {
            let hk = BloomKey::new(&e.key);
            filter.prefetch(&hk);
            assert_eq!(filter.contains_hashed(&hk), filter.contains(&e.key));
            hits += filter.contains(&e.key) as usize;
        }
        assert!((1500..3000).contains(&hits));
    }
}

#[test]
fn blocked_bloom_fp_rate_within_budget() {
    let n = 50_000usize;
    let keys: Vec<Vec<u8>> = (0..n)
        .map(|i| format!("present-{i:08}").into_bytes())
        .collect();
    let bloom = BlockedBloom::build(keys.iter().map(|k| k.as_slice()), n, 12);
    let probes = 100_000usize;
    let fp = (0..probes)
        .filter(|i| bloom.contains(format!("absent-{i:08}").as_bytes()))
        .count();
    assert!(
        (fp as f64) / (probes as f64) <= 0.025,
        "blocked-bloom FP rate too high: {fp}/{probes}"
    );
}

#[test]
fn blocked_bloom_rejects_corruption_and_wrong_magic() {
    let bloom = BlockedBloom::build([b"a".as_slice(), b"b"].into_iter(), 2, 12);
    let enc = bloom.encode();
    let mut bad = enc.clone();
    bad[enc.len() / 2] ^= 0xff;
    assert_eq!(BlockedBloom::decode(&bad), Err("crc mismatch"));
    let mut wrong = enc.clone();
    wrong[0] ^= 0xff;
    let n = wrong.len();
    let crc = crc::crc32(&wrong[..n - 4]);
    wrong[n - 4..].copy_from_slice(&crc.to_le_bytes());
    assert_eq!(BlockedBloom::decode(&wrong), Err("bad blocked-bloom magic"));
    let classic = Bloom::build([b"a".as_slice(), b"b"].into_iter(), 2, 12).encode();
    assert_eq!(BlockedBloom::decode(&classic), Err("bad blocked-bloom magic"));
}

#[test]
fn v3_classic_bloom_run_reads_back() {
    let entries = firehose(300);
    let opts = RunBuildOpts {
        block_target_bytes: 512,
        bloom_bits_per_key: 10,
        num_buckets: 32,
    };
    let bytes = build_run_v3(&entries, opts);
    let mem = Run::parse(bytes.clone()).unwrap();
    assert_eq!(mem.version, 3);
    assert!(matches!(mem.bloom, RunFilter::Classic(_)));
    for e in &entries {
        assert_eq!(mem.lookup(&e.key).unwrap().as_ref(), Some(e));
    }
    assert_eq!(mem.lookup(b"zzz").unwrap(), None);
    let (dev, mut alloc) = dev_alloc(2048, 4);
    let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
    let run = DeviceRun::open(dev, &alloc, handle).unwrap();
    assert_eq!(run.format_version(), 3);
    assert!(matches!(run.bloom, RunFilter::Classic(_)));
    for e in &entries {
        assert_eq!(run.lookup(&alloc, &e.key).unwrap().as_ref(), Some(e));
    }
    assert_eq!(run.lookup(&alloc, b"zzz").unwrap(), None);
    assert_eq!(run.lookup(&alloc, b"app.aaa").unwrap(), None);
}

#[test]
fn v4_run_round_trips_with_blocked_bloom() {
    let entries = firehose(400);
    let opts = RunBuildOpts {
        block_target_bytes: 512,
        ..Default::default()
    };
    let bytes = build_run(&entries, opts);
    let mem = Run::parse(bytes.clone()).unwrap();
    assert_eq!(mem.version, RUN_VERSION);
    assert!(matches!(mem.bloom, RunFilter::Blocked(_)));
    for e in &entries {
        assert_eq!(mem.lookup(&e.key).unwrap().as_ref(), Some(e));
    }
    assert_eq!(mem.lookup(b"zzz").unwrap(), None);

    let (dev, mut alloc) = dev_alloc(2048, 4);
    let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
    let run = DeviceRun::open(dev.clone(), &alloc, handle).unwrap();
    assert_eq!(run.format_version(), RUN_VERSION);
    assert!(matches!(run.bloom, RunFilter::Blocked(_)));
    for e in &entries {
        assert_eq!(run.lookup(&alloc, &e.key).unwrap().as_ref(), Some(e));
        let loc = run
            .locate_block(&alloc, &e.key)
            .expect("present key must bloom-hit");
        let block = block_on(loc.read(&*dev)).unwrap();
        assert_eq!(
            DeviceRun::<MemBlockDevice>::block_lookup_bytes(&block, &e.key)
                .unwrap()
                .as_ref(),
            Some(e)
        );
    }
    assert_eq!(run.lookup(&alloc, b"zzz").unwrap(), None);

    let (_sdev, salloc, srun) = build_streamed(&entries, opts, entries.len() + 777, 4096, 8);
    assert_eq!(srun.format_version(), RUN_VERSION);
    assert!(matches!(srun.bloom, RunFilter::Blocked(_)));
    for e in &entries {
        assert_eq!(srun.lookup(&salloc, &e.key).unwrap().as_ref(), Some(e));
    }
    assert_eq!(srun.lookup(&salloc, b"zzz").unwrap(), None);
}

fn rand_entries(rng: &mut Rng, n: usize, max_tail: u64) -> Vec<SnapshotEntry> {
    let prefixes: [&[u8]; 5] = [
        b"",
        b"a",
        b"app.bsky.graph.follow/did:plc:abcdefghijklmnop/",
        b"app.bsky.feed.like/did:plc:abcdefghijklmnop/3k",
        b"app.bsky.feed.like/did:plc:abcdefghijklmnoq/",
    ];
    let mut keys: Vec<Vec<u8>> = (0..n)
        .map(|_| {
            let mut key = prefixes[rng.range(0, prefixes.len() as u64 - 1) as usize].to_vec();
            for _ in 0..rng.range(0, max_tail) {
                key.push(b"abz~"[rng.range(0, 3) as usize]);
            }
            key
        })
        .collect();
    keys.sort();
    keys.dedup();
    keys.into_iter()
        .enumerate()
        .map(|(i, key)| {
            let mut e = entry(&key, i as u64, ((rng.next() as u128) << 64) | rng.next() as u128);
            e.tombstone = rng.range(0, 3) == 0;
            e
        })
        .collect()
}

fn reserve_for(
    alloc: &mut Allocator,
    entries: &[SnapshotEntry],
    opts: RunBuildOpts,
    version: u16,
) -> (Vec<SegmentId>, Vec<u64>) {
    let bound = run_len_upper_bound_for(entries, opts, version);
    let segs = allocate_run_segs(alloc, BS, bound).unwrap();
    let lbas = segs.iter().map(|s| alloc.segment_range(*s).0).collect();
    (segs, lbas)
}

fn open_versioned(
    entries: &[SnapshotEntry],
    opts: RunBuildOpts,
    version: u16,
    streamed: bool,
) -> (Arc<MemBlockDevice>, Allocator, DeviceRun<MemBlockDevice>) {
    let (dev, mut alloc) = dev_alloc(8192, 8);
    let handle = if streamed {
        let seg_bytes = alloc.blocks_per_segment() as usize * BS;
        let (segs, lbas) = reserve_for(&mut alloc, entries, opts, version);
        let sink = SegmentStreamSink::new(&*dev, &segs, &lbas, seg_bytes, BS);
        let mut builder = RunStreamBuilder::new(sink, opts, entries.len()).version(version);
        for e in entries {
            builder.push(e);
        }
        block_on(builder.finish()).unwrap()
    } else {
        let bytes = build_run_versioned(entries, opts, version);
        assert!(bytes.len() <= run_len_upper_bound_for(entries, opts, version));
        write_run(&*dev, &mut alloc, &bytes).unwrap()
    };
    let run = DeviceRun::open(dev.clone(), &alloc, handle).unwrap();
    assert_eq!(run.format_version(), version);
    assert_eq!(run.has_sidecar(), version <= RUN_SIDECAR_VERSION);
    assert_eq!(run.sidecar_len() == 0, version > RUN_SIDECAR_VERSION);
    (dev, alloc, run)
}

fn seed_stream(run: &DeviceRun<MemBlockDevice>, alloc: &Allocator, rng: &mut Rng)
    -> Result<Vec<(Vec<u8>, u128)>, SnapshotError> {
    let mut stream = run.version_stream(alloc)?.expect("v3+ runs stream");
    let mut out = Vec::new();
    loop {
        let fail = rng.range(0, 4) == 0;
        match stream.next_step_reserving(|_| !fail)? {
            None => assert!(fail),
            Some(SidecarCursorStep::Entry(e)) => out.push(e),
            Some(SidecarCursorStep::Done) => return Ok(out),
            Some(SidecarCursorStep::Checkpoint) => {}
        }
    }
}

fn drain_version_source(
    dev: &MemBlockDevice,
    source: VersionSource,
    want: &std::collections::HashSet<u32>,
    bucket_of: &impl Fn(&[u8]) -> u32,
    after: Option<&[u8]>,
) -> Result<Vec<(Vec<u8>, u128)>, SnapshotError> {
    block_on(async {
        let mut out = Vec::new();
        match source {
            VersionSource::Sidecar { loc, version } => {
                assert!(version >= 3);
                let mut cursor = SidecarCursor::new(loc)?;
                loop {
                    match cursor.next_matching_step(dev, want, bucket_of, after).await? {
                        SidecarCursorStep::Entry(e) => out.push(e),
                        SidecarCursorStep::Done => return Ok(out),
                        SidecarCursorStep::Checkpoint => {}
                    }
                }
            }
            VersionSource::Blocks(mut cursor) => loop {
                match cursor.next_matching_step(dev, want, bucket_of, after).await? {
                    SidecarCursorStep::Entry(e) => out.push(e),
                    SidecarCursorStep::Done => return Ok(out),
                    SidecarCursorStep::Checkpoint => {}
                }
            },
        }
    })
}

fn digest_pages(
    run: &DeviceRun<MemBlockDevice>,
    alloc: &Allocator,
    want: &std::collections::HashSet<u32>,
    after: Option<&[u8]>,
    max_bytes: usize,
    overhead: usize,
) -> Vec<(Vec<Digest>, Option<Vec<u8>>)> {
    let mut pages = Vec::new();
    let mut cursor = after.map(<[u8]>::to_vec);
    loop {
        let (page, stop) = run
            .want_bucket_digests_page(alloc, want, test_bucket, cursor.as_deref(), max_bytes, overhead)
            .unwrap();
        let next = page.last().map(|(k, _)| k.clone());
        let done = stop.is_none();
        pages.push((page, stop));
        if done {
            return pages;
        }
        cursor = next;
    }
}

fn assert_block_stream_matches_sidecar(entries: &[SnapshotEntry], opts: RunBuildOpts, rng: &mut Rng) {
    let model: Vec<(Vec<u8>, u128)> = entries.iter().map(|e| (e.key.clone(), e.version)).collect();
    let streamed = rng.range(0, 1) == 1;
    let (dev4, alloc4, v4) = open_versioned(entries, opts, RUN_SIDECAR_VERSION, streamed);
    let (dev5, alloc5, v5) = open_versioned(entries, opts, RUN_VERSION, streamed);
    let ctx = format!("n={} opts={opts:?} streamed={streamed}", entries.len());
    assert_eq!(v4.entry_count(), v5.entry_count());
    assert_eq!(v4.num_buckets(), v5.num_buckets());

    let seed4 = seed_stream(&v4, &alloc4, rng).unwrap();
    let seed5 = seed_stream(&v5, &alloc5, rng).unwrap();
    assert_eq!(seed4, model, "{ctx}");
    assert_eq!(seed5, seed4, "seed stream {ctx}");

    let mut all4 = Vec::new();
    v4.for_each_bucket_digest(&alloc4, |k, v| all4.push((k.to_vec(), v))).unwrap();
    let mut all5 = Vec::new();
    v5.for_each_bucket_digest(&alloc5, |k, v| all5.push((k.to_vec(), v))).unwrap();
    assert_eq!(all5, all4, "for_each_bucket_digest {ctx}");
    assert_eq!(all4, model);

    for b in 0..opts.num_buckets {
        assert_eq!(
            v5.bucket_digests(&alloc5, b, test_bucket).unwrap(),
            v4.bucket_digests(&alloc4, b, test_bucket).unwrap(),
            "bucket {b} {ctx}"
        );
    }
    assert!(v5.bucket_digests(&alloc5, opts.num_buckets, test_bucket).is_err());
    assert!(v4.bucket_digests(&alloc4, opts.num_buckets, test_bucket).is_err());

    let mut source4 = v4.key_source(&alloc4);
    let mut source5 = v5.key_source(&alloc5);
    assert!(source4.first_sidecar_record_location(BS).is_some());
    assert!(source5.first_sidecar_record_location(BS).is_none());
    let chunk = rng.range(1, 70_000) as usize;
    let mut keys4 = Vec::new();
    while !source4.step(&*dev4, chunk, |k| keys4.push(k.to_vec())).unwrap() {}
    let mut keys5 = Vec::new();
    while !source5.step(&*dev5, chunk, |k| keys5.push(k.to_vec())).unwrap() {}
    assert_eq!(keys5, keys4, "key source {ctx}");
    assert_eq!(keys5.len() as u64, v5.entry_count());

    let afters: Vec<Option<Vec<u8>>> = vec![
        None,
        entries.get(rng.range(0, entries.len() as u64) as usize).map(|e| e.key.clone()),
        entries.first().map(|e| e.key.clone()),
        entries.last().map(|e| e.key.clone()),
        Some(b"app.bsky.feed.like/".to_vec()),
        Some(Vec::new()),
    ];
    for after in &afters {
        let after = after.as_deref();
        let want: std::collections::HashSet<u32> =
            (0..opts.num_buckets).filter(|_| rng.range(0, 2) > 0).collect();
        assert_eq!(
            v5.want_bucket_digests(&alloc5, &want, test_bucket).unwrap(),
            v4.want_bucket_digests(&alloc4, &want, test_bucket).unwrap(),
            "want {want:?} {ctx}"
        );
        let max_bytes = if entries.len() > 500 {
            [16_384usize, usize::MAX][rng.range(0, 1) as usize]
        } else {
            [0usize, 1, 64, 4096, usize::MAX][rng.range(0, 4) as usize]
        };
        let overhead = rng.range(0, 24) as usize;
        assert_eq!(
            digest_pages(&v5, &alloc5, &want, after, max_bytes, overhead),
            digest_pages(&v4, &alloc4, &want, after, max_bytes, overhead),
            "digest pages want {want:?} after {after:?} max {max_bytes} {ctx}"
        );
        let raw_want: std::collections::HashSet<u32> = (0..16).filter(|_| rng.range(0, 1) == 1).collect();
        let bucket_of = |k: &[u8]| test_bucket(k) % 16;
        let src4 = v4.version_source(&alloc4, after);
        let src5 = v5.version_source(&alloc5, after);
        assert_eq!(matches!(src4, VersionSource::Sidecar { .. }), after.is_none());
        assert!(matches!(src5, VersionSource::Blocks(_)));
        let side4 = VersionSource::Sidecar { loc: v4.locate_sidecar(&alloc4), version: RUN_SIDECAR_VERSION };
        let got4 = drain_version_source(&dev4, side4, &raw_want, &bucket_of, after).unwrap();
        let seek4 = drain_version_source(&dev4, src4, &raw_want, &bucket_of, after).unwrap();
        assert_eq!(seek4, got4, "v4 continuation source after {after:?} {ctx}");
        let got5 = drain_version_source(&dev5, src5, &raw_want, &bucket_of, after).unwrap();
        let expect: Vec<(Vec<u8>, u128)> = model
            .iter()
            .filter(|(k, _)| after.is_none_or(|a| k.as_slice() > a) && raw_want.contains(&bucket_of(k)))
            .cloned()
            .collect();
        assert_eq!(got4, expect, "async sidecar cursor {ctx}");
        assert_eq!(got5, got4, "async block cursor after {after:?} {ctx}");
    }
}

#[test]
fn block_version_streams_match_sidecar_streams_for_every_consumer() {
    let mut rng = Rng(0x0365_5eed_0000_0001);
    let mut shapes: Vec<(usize, u64)> = vec![(0, 8), (1, 8), (1, 0), (2, 3), (5_000, 200)];
    for _ in 0..24 {
        shapes.push((rng.range(2, 400) as usize, rng.range(0, 40)));
    }
    for (n, max_tail) in shapes {
        let entries = rand_entries(&mut rng, n, max_tail);
        let opts = RunBuildOpts {
            block_target_bytes: [1usize, 64, 512, 4096][rng.range(0, 3) as usize],
            bloom_bits_per_key: 10,
            num_buckets: rng.range(1, 64) as u32,
        };
        if n == 5_000 {
            let data: usize = entries.iter().map(|e| e.key.len() + 48).sum();
            assert!(data > 2 * SIDECAR_CURSOR_WINDOW, "blocks must span several stream windows");
            assert!(entries.iter().any(|e| e.tombstone));
        }
        assert_block_stream_matches_sidecar(&entries, opts, &mut rng);
    }
}

#[test]
fn v5_streams_keep_tombstones_for_every_consumer() {
    let mut rng = Rng(0x0371_7063_0000_0001);
    let mut entries = rand_entries(&mut rng, 600, 30);
    for (i, e) in entries.iter_mut().enumerate() {
        e.tombstone = i % 4 != 1;
    }
    let model: Vec<(Vec<u8>, u128)> = entries.iter().map(|e| (e.key.clone(), e.version)).collect();
    let tombstones = entries.iter().filter(|e| e.tombstone).count();
    assert!(tombstones > model.len() / 2 && tombstones < model.len());
    let opts = RunBuildOpts { block_target_bytes: 512, bloom_bits_per_key: 10, num_buckets: 8 };
    let all: std::collections::HashSet<u32> = (0..opts.num_buckets).collect();
    let bucket_of = |k: &[u8]| test_bucket(k) % opts.num_buckets;
    let mid = entries.len() / 2;
    for streamed in [false, true] {
        let (dev, alloc, run) = open_versioned(&entries, opts, RUN_VERSION, streamed);
        let ctx = format!("streamed={streamed}");
        assert_eq!(seed_stream(&run, &alloc, &mut rng).unwrap(), model, "seed stream {ctx}");
        let mut folded = Vec::new();
        run.for_each_bucket_digest(&alloc, |k, v| folded.push((k.to_vec(), v))).unwrap();
        assert_eq!(folded, model, "for_each_bucket_digest {ctx}");
        let per_bucket: usize = (0..opts.num_buckets)
            .map(|b| run.bucket_digests(&alloc, b, test_bucket).unwrap().len())
            .sum();
        assert_eq!(per_bucket, model.len(), "bucket_digests {ctx}");
        let (page, stop) = run.want_bucket_digests_page(&alloc, &all, test_bucket, None, usize::MAX, 0).unwrap();
        assert!(stop.is_none());
        let page: Vec<(Vec<u8>, u128)> = page
            .into_iter()
            .map(|(k, v)| (k, u128::from_le_bytes(v.as_slice().try_into().unwrap())))
            .collect();
        assert_eq!(page, model, "digest page {ctx}");
        let full = drain_version_source(&dev, run.version_source(&alloc, None), &all, &bucket_of, None).unwrap();
        assert_eq!(full, model, "async block cursor {ctx}");
        let after = entries[mid].key.as_slice();
        let tail = drain_version_source(&dev, run.version_source(&alloc, Some(after)), &all, &bucket_of, Some(after)).unwrap();
        assert_eq!(tail, model[mid + 1..].to_vec(), "async block cursor after {ctx}");
        let mut keys = Vec::new();
        let mut source = run.key_source(&alloc);
        while !source.step(&*dev, 4096, |k| keys.push(k.to_vec())).unwrap() {}
        assert_eq!(keys, model.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(), "key source {ctx}");
    }
}

#[test]
fn block_cursor_after_starts_in_the_straddling_block_and_skips_nothing() {
    let entries = backlink_entries(3_000, 0x0371_0000_0000_0002);
    let opts = RunBuildOpts { block_target_bytes: 4096, bloom_bits_per_key: 10, num_buckets: 16 };
    let bytes = build_run_versioned(&entries, opts, RUN_VERSION);
    let (dev, mut alloc) = dev_alloc((bytes.len() / BS) as u64 * 2 + 1024, 256);
    let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
    let run = DeviceRun::open(dev.clone(), &alloc, handle).unwrap();
    let n_blocks = run.index_len();
    assert!(n_blocks >= 8, "{n_blocks}");
    let all: std::collections::HashSet<u32> = (0..16).collect();
    let bucket_of = |k: &[u8]| test_bucket(k) % 16;
    let firsts: Vec<Vec<u8>> = (0..n_blocks).map(|b| run.block_first_key(b).to_vec()).collect();
    let mut afters: Vec<Vec<u8>> = vec![Vec::new(), entries.last().unwrap().key.clone()];
    for b in [1, 2, n_blocks / 2, n_blocks - 1] {
        let i = entries.iter().position(|e| e.key == firsts[b]).unwrap();
        afters.push(entries[i - 1].key.clone());
        afters.push(entries[i].key.clone());
        afters.push(entries[(i + 1).min(entries.len() - 1)].key.clone());
        let mut between = firsts[b].clone();
        between.push(0);
        afters.push(between);
    }
    for after in &afters {
        let expect: Vec<(Vec<u8>, u128)> = entries
            .iter()
            .filter(|e| e.key.as_slice() > after.as_slice())
            .map(|e| (e.key.clone(), e.version))
            .collect();
        let straddling = firsts.partition_point(|fk| fk.as_slice() <= after.as_slice()).saturating_sub(1);
        let VersionSource::Blocks(cursor) = run.version_source(&alloc, Some(after)) else {
            panic!("v5 runs stream blocks");
        };
        assert_eq!(cursor.walker.next_block, straddling, "start block for after {after:?}");
        assert_eq!(cursor.base, run.index.block_ref(straddling).0, "cursor base for after {after:?}");
        let got = drain_version_source(&dev, VersionSource::Blocks(cursor), &all, &bucket_of, Some(after)).unwrap();
        assert_eq!(got, expect, "keys after {after:?}");
        let (page, stop) = run.want_bucket_digests_page(&alloc, &all, test_bucket, Some(after), usize::MAX, 0).unwrap();
        assert!(stop.is_none());
        assert_eq!(page.len(), expect.len(), "digest page after {after:?}");
    }
}

#[test]
fn v5_runs_drop_the_sidecar_and_shrink() {
    let mut entries: Vec<SnapshotEntry> = (0..2_000u64)
        .map(|i| {
            let mut key = format!("at://did:plc:{:024}/app.bsky.feed.like/3k{i:011}", i % 37).into_bytes();
            key.resize(148, b'x');
            entry(&key, i, i as u128)
        })
        .collect();
    entries.sort_by(|a, b| a.key.cmp(&b.key));
    let opts = RunBuildOpts::default();
    let v4 = build_run_versioned(&entries, opts, RUN_SIDECAR_VERSION);
    let v5 = build_run_versioned(&entries, opts, RUN_BLOCK_VERSION);
    let f4 = decode_footer(&v4[v4.len() - FOOTER_LEN..]).unwrap();
    let f5 = decode_footer(&v5[v5.len() - FOOTER_LEN..]).unwrap();
    assert_eq!((f4.version, f5.version), (RUN_SIDECAR_VERSION, RUN_BLOCK_VERSION));
    assert_eq!(f5.sidecar_len, 0);
    assert_eq!(f5.sidecar_off, f5.index_off + f5.index_len);
    assert_eq!(v4.len() - v5.len(), f4.sidecar_len as usize);
    assert_eq!(f4.sidecar_len as usize, 8 + entries.iter().map(|e| 20 + e.key.len()).sum::<usize>());
    assert_eq!(&v4[..f4.sidecar_off as usize], &v5[..f5.sidecar_off as usize]);
    assert!(v5.len() <= run_len_upper_bound_for(&entries, opts, RUN_BLOCK_VERSION));
    assert!(run_len_upper_bound_for(&entries, opts, RUN_BLOCK_VERSION)
        < run_len_upper_bound_for(&entries, opts, RUN_SIDECAR_VERSION));
    let mem = Run::parse(v5).unwrap();
    for b in 0..opts.num_buckets.min(64) {
        assert_eq!(
            mem.bucket_digests(b, test_bucket).unwrap(),
            Run::parse(v4.clone()).unwrap().bucket_digests(b, test_bucket).unwrap()
        );
    }
}

#[test]
fn run_len_upper_bound_ignores_num_buckets_and_counts_the_v4_sidecar_exactly() {
    let opts = |num_buckets| RunBuildOpts { num_buckets, ..RunBuildOpts::default() };
    for n in [0usize, 1, 37, 900] {
        let entries = firehose(n);
        for version in [RUN_SIDECAR_VERSION, RUN_BLOCK_VERSION, RUN_VERSION] {
            let bounds = [1, 4096, 65535].map(|nb| run_len_upper_bound_for(&entries, opts(nb), version));
            assert!(bounds.iter().all(|&b| b == bounds[0]), "v{version} n={n}: {bounds:?}");
            assert!(build_run_versioned(&entries, opts(65535), version).len() <= bounds[0]);
        }
        let v4 = build_run_versioned(&entries, opts(65535), RUN_SIDECAR_VERSION);
        let f4 = decode_footer(&v4[v4.len() - FOOTER_LEN..]).unwrap();
        assert_eq!(
            run_len_upper_bound_for(&entries, opts(65535), RUN_SIDECAR_VERSION)
                - run_len_upper_bound_for(&entries, opts(65535), RUN_BLOCK_VERSION),
            f4.sidecar_len as usize,
            "n={n}"
        );
    }
}

#[test]
fn max_bucket_run_streams_into_a_device_sized_to_its_reservation() {
    let entries = firehose(900);
    let opts = RunBuildOpts { block_target_bytes: 512, num_buckets: 65535, ..RunBuildOpts::default() };
    let seg_blocks = 8u64;
    let seg_bytes = seg_blocks as usize * BS;
    let legacy_header_slack = 4 + 65535 * 4 + 4;
    for version in [RUN_SIDECAR_VERSION, RUN_VERSION] {
        let bound = run_len_upper_bound_for(&entries, opts, version);
        let segs = bound.div_ceil(seg_bytes) as u64;
        let (_, mut tight) = dev_alloc(segs * seg_blocks, seg_blocks);
        assert!(matches!(
            allocate_run_segs(&mut tight, BS, bound + legacy_header_slack),
            Err(SnapshotError::DeviceFull)
        ));
        assert_eq!(tight.free_segments(), segs);

        let (dev, mut alloc) = dev_alloc(segs * seg_blocks, seg_blocks);
        let (rsegs, lbas) = reserve_for(&mut alloc, &entries, opts, version);
        assert_eq!(alloc.free_segments(), 0);
        let sink = SegmentStreamSink::new(&*dev, &rsegs, &lbas, seg_bytes, BS);
        let mut builder = RunStreamBuilder::new(sink, opts, entries.len()).version(version);
        for e in &entries {
            builder.push(e);
        }
        let handle = block_on(builder.finish()).unwrap();
        let run = DeviceRun::open(dev, &alloc, handle).unwrap();
        assert_eq!((run.format_version(), run.num_buckets()), (version, 65535));
        for e in &entries {
            assert_eq!(run.lookup(&alloc, &e.key).unwrap().as_ref(), Some(e), "v{version}");
        }
    }
}

#[test]
fn readers_capped_at_older_versions_reject_newer_runs_and_write_version_parses() {
    let entries = firehose(50);
    for version in [RUN_SIDECAR_VERSION, RUN_BLOCK_VERSION, RUN_VERSION] {
        let bytes = build_run_versioned(&entries, RunBuildOpts::default(), version);
        let footer = &bytes[bytes.len() - FOOTER_LEN..];
        assert_eq!(decode_footer(footer).unwrap().version, version);
        for reader in [RUN_SIDECAR_VERSION, RUN_BLOCK_VERSION, RUN_VERSION] {
            let old = decode_footer_capped(footer, reader);
            assert_eq!(old.is_ok(), version <= reader, "v{version} run, v{reader} reader");
            if version > reader {
                assert_eq!(old.err(), Some("unsupported run version"));
            }
        }
    }
    let mut f = build_run(&entries, RunBuildOpts::default());
    let n = f.len();
    stamp_footer_version(&mut f[n - FOOTER_LEN..], RUN_VERSION + 1);
    assert!(Run::parse(f).is_err());
    assert_eq!(parse_run_write_version(None), RUN_VERSION);
    assert_eq!(parse_run_write_version(Some("6")), RUN_VERSION);
    assert_eq!(parse_run_write_version(Some("5")), RUN_BLOCK_VERSION);
    assert_eq!(parse_run_write_version(Some(" 4 ")), RUN_SIDECAR_VERSION);
    assert_eq!(parse_run_write_version(Some("v4")), RUN_SIDECAR_VERSION);
    assert_eq!(parse_run_write_version(Some("")), RUN_SIDECAR_VERSION);
    assert_eq!(parse_run_write_version(Some(" ")), RUN_SIDECAR_VERSION);
    assert_eq!(run_write_version(), RUN_VERSION);
    assert_eq!(with_run_write_version(RUN_SIDECAR_VERSION, run_write_version), RUN_SIDECAR_VERSION);
    assert_eq!(run_write_version(), RUN_VERSION);
    let merge_as = |configured, inputs: &[u16]| with_run_write_version(configured, || merge_write_version(inputs.iter().copied()));
    assert_eq!(merge_as(RUN_SIDECAR_VERSION, &[3, RUN_SIDECAR_VERSION]), RUN_SIDECAR_VERSION);
    assert_eq!(merge_as(RUN_SIDECAR_VERSION, &[RUN_SIDECAR_VERSION, RUN_BLOCK_VERSION]), RUN_BLOCK_VERSION);
    assert_eq!(merge_as(RUN_SIDECAR_VERSION, &[RUN_BLOCK_VERSION, RUN_VERSION]), RUN_VERSION);
    assert_eq!(merge_as(RUN_BLOCK_VERSION, &[RUN_SIDECAR_VERSION]), RUN_BLOCK_VERSION);
    assert_eq!(merge_as(RUN_BLOCK_VERSION, &[RUN_BLOCK_VERSION]), RUN_BLOCK_VERSION);
    assert_eq!(merge_as(RUN_BLOCK_VERSION, &[RUN_VERSION, RUN_SIDECAR_VERSION]), RUN_VERSION);
    assert_eq!(merge_as(RUN_VERSION, &[RUN_SIDECAR_VERSION]), RUN_VERSION);
    assert_eq!(merge_write_version([]), RUN_VERSION);
}

struct WriteCorruptDev {
    inner: MemBlockDevice,
    corrupt_first_write: std::sync::atomic::AtomicBool,
}

impl BlockDevice for WriteCorruptDev {
    fn block_size(&self) -> usize { self.inner.block_size() }
    fn num_blocks(&self) -> u64 { self.inner.num_blocks() }
    fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
        self.inner.read_at(lba, buf)
    }
    fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
        if self.corrupt_first_write.swap(false, std::sync::atomic::Ordering::Relaxed) {
            let mut bad = buf.to_vec();
            bad[100] ^= 0x10;
            return self.inner.write_at(lba, &bad);
        }
        self.inner.write_at(lba, buf)
    }
    fn flush(&self) -> vals_spdk::Result<()> { self.inner.flush() }
}

#[test]
fn v5_streamed_build_verifies_written_blocks() {
    let entries = firehose(3_000);
    let opts = RunBuildOpts { block_target_bytes: 512, ..Default::default() };
    for (corrupt, verify) in [(false, true), (true, true), (true, false)] {
        let dev = WriteCorruptDev {
            inner: MemBlockDevice::new(BS, 8192),
            corrupt_first_write: std::sync::atomic::AtomicBool::new(corrupt),
        };
        let mut alloc = Allocator::new(8192, 8, 0);
        let seg_bytes = alloc.blocks_per_segment() as usize * BS;
        let (segs, lbas) = reserve_for(&mut alloc, &entries, opts, RUN_VERSION);
        let (bytes_before, failures_before) = run_build_verify_totals();
        let sink = SegmentStreamSink::new(&dev, &segs, &lbas, seg_bytes, BS);
        let mut builder = RunStreamBuilder::new(sink, opts, entries.len())
            .version(RUN_VERSION)
            .verify(verify);
        for e in &entries {
            builder.push(e);
        }
        let result = block_on(builder.finish());
        let (bytes_after, failures_after) = run_build_verify_totals();
        match (corrupt, verify) {
            (false, true) => {
                let handle = result.unwrap();
                assert!(handle.segs.len() > 2);
                assert!(bytes_after - bytes_before >= seg_bytes as u64);
            }
            (true, true) => {
                assert!(matches!(result, Err(SnapshotError::Corrupt("run block crc mismatch on build verify"))));
                assert!(failures_after > failures_before);
            }
            _ => {
                let handle = result.expect("an unverified build publishes the corrupt block");
                let run = DeviceRun::open(Arc::new(dev.inner.share()), &alloc, handle).unwrap();
                let mut stream = run.version_stream(&alloc).unwrap().unwrap();
                let err = loop {
                    match stream.next_step() {
                        Ok(SidecarCursorStep::Done) => panic!("corrupt block streamed clean"),
                        Ok(_) => {}
                        Err(e) => break e,
                    }
                };
                assert!(matches!(err, SnapshotError::Corrupt("run block crc mismatch")));
            }
        }
    }
}

type WriteMutation = Box<dyn FnMut(u64, &mut [u8]) -> bool + Send>;

struct MutateOnWriteDev {
    inner: MemBlockDevice,
    mutate: std::sync::Mutex<Option<WriteMutation>>,
}

impl BlockDevice for MutateOnWriteDev {
    fn block_size(&self) -> usize { self.inner.block_size() }
    fn num_blocks(&self) -> u64 { self.inner.num_blocks() }
    fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
        self.inner.read_at(lba, buf)
    }
    fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
        let mut slot = self.mutate.lock().unwrap();
        if let Some(f) = slot.as_mut() {
            let mut bad = buf.to_vec();
            if f(lba * self.inner.block_size() as u64, &mut bad) {
                *slot = None;
                return self.inner.write_at(lba, &bad);
            }
        }
        self.inner.write_at(lba, buf)
    }
    fn flush(&self) -> vals_spdk::Result<()> { self.inner.flush() }
}

#[derive(Clone, Copy)]
enum TailFault {
    None,
    Flip(u64),
    ResealFooter(u64),
}

fn write_mutation(fault: TailFault, lbas: &[u64], seg_bytes: usize) -> Option<WriteMutation> {
    let dev_byte = |run_off: u64| {
        lbas[run_off as usize / seg_bytes] * BS as u64 + run_off % seg_bytes as u64
    };
    match fault {
        TailFault::None => None,
        TailFault::Flip(run_off) => {
            let at = dev_byte(run_off);
            Some(Box::new(move |start, buf| {
                if !(start..start + buf.len() as u64).contains(&at) {
                    return false;
                }
                buf[(at - start) as usize] ^= 0x80;
                true
            }))
        }
        TailFault::ResealFooter(run_off) => {
            let at = dev_byte(run_off);
            Some(Box::new(move |start, buf| {
                if at < start || at + FOOTER_LEN as u64 > start + buf.len() as u64 {
                    return false;
                }
                let f = &mut buf[(at - start) as usize..][..FOOTER_LEN];
                f[8] ^= 0x01;
                let crc = crc::crc32(&f[..FOOTER_LEN - 4]);
                f[FOOTER_LEN - 4..].copy_from_slice(&crc.to_le_bytes());
                true
            }))
        }
    }
}

#[test]
fn v5_streamed_build_verifies_index_filter_and_footer_from_the_device() {
    let opts = RunBuildOpts::default();
    let seg_bytes = 8 * BS;
    let (entries, reference) = (2_000..4_000)
        .step_by(100)
        .map(|n| {
            let entries = firehose(n);
            let reference = build_run_versioned(&entries, opts, RUN_VERSION);
            (entries, reference)
        })
        .find(|(_, r)| {
            let ft = decode_footer(&r[r.len() - FOOTER_LEN..]).unwrap();
            let (index, _) = decode_index(section(r, ft.index_off, ft.index_len).unwrap(), ft.version).unwrap();
            let final_seg = (r.len() - 1) / seg_bytes;
            *index.last().unwrap().1 as usize / seg_bytes == final_seg
                && (r.len() - FOOTER_LEN) / seg_bytes == final_seg
        })
        .expect("a run whose last data block shares the final, sink-buffered segment");
    let ft = decode_footer(&reference[reference.len() - FOOTER_LEN..]).unwrap();
    let (index, _) = decode_index(section(&reference, ft.index_off, ft.index_len).unwrap(), ft.version).unwrap();
    let last_block = index.last().unwrap().1;
    let footer_off = (reference.len() - FOOTER_LEN) as u64;
    let index_crc = "index crc mismatch";
    let cases = [
        (TailFault::None, true, None),
        (TailFault::Flip(ft.index_off + 12 + 10), true, Some(index_crc)),
        (TailFault::Flip(ft.index_off + ft.index_len - 2), true, Some(index_crc)),
        (TailFault::Flip(ft.bloom_off + ft.bloom_len / 2), true, Some("crc mismatch")),
        (TailFault::Flip(footer_off + 30), true, Some("footer crc mismatch")),
        (TailFault::ResealFooter(footer_off), true,
            Some("run tail differs from its encoded bytes on build verify")),
        (TailFault::Flip(last_block + 5), true, Some("run block crc mismatch on build verify")),
        (TailFault::Flip(ft.index_off + 12 + 10), false, Some(index_crc)),
        (TailFault::Flip(last_block + 5), false, None),
    ];
    for (fault, verify, expect) in cases {
        let mut alloc = Allocator::new(8192, 8, 0);
        assert_eq!(alloc.blocks_per_segment() as usize * BS, seg_bytes);
        let (segs, lbas) = reserve_for(&mut alloc, &entries, opts, RUN_VERSION);
        let dev = MutateOnWriteDev {
            inner: MemBlockDevice::new(BS, 8192),
            mutate: std::sync::Mutex::new(write_mutation(fault, &lbas, seg_bytes)),
        };
        let (bytes_before, failures_before) = run_build_verify_totals();
        let sink = SegmentStreamSink::new(&dev, &segs, &lbas, seg_bytes, BS);
        let mut builder = RunStreamBuilder::new(sink, opts, entries.len())
            .version(RUN_VERSION)
            .verify(verify);
        for e in &entries {
            builder.push(e);
        }
        let result = block_on(builder.finish_with_parts());
        let (bytes_after, failures_after) = run_build_verify_totals();
        assert!(dev.mutate.lock().unwrap().is_none(), "fault was not injected");
        match (expect, result) {
            (None, Ok((handle, parts))) => {
                assert_eq!(handle.len, reference.len() as u64);
                assert_eq!(parts.index.len(), index.len());
                if matches!(fault, TailFault::None) {
                    let on_dev = read_run_range_lbas(&dev, &lbas, seg_bytes, 0, reference.len()).unwrap();
                    assert!(on_dev == reference);
                    assert!(bytes_after - bytes_before >= reference.len() as u64);
                }
            }
            (Some(msg), Err(SnapshotError::Corrupt(got))) => {
                assert_eq!(got, msg);
                if verify {
                    assert!(failures_after > failures_before, "{msg}: failure not counted");
                }
            }
            (expect, got) => panic!("verify={verify} expected {expect:?}, got {:?}", got.map(|(h, _)| h)),
        }
    }
}

#[test]
#[ignore]
fn build_verify_cost_microbench() {
    let n = 400_000usize;
    let entries: Vec<SnapshotEntry> = (0..n)
        .map(|i| {
            let key = format!(
                "at://did:plc:{:024x}/app.bsky.feed.post/3l{:011}\0at://did:plc:{:024x}/app.bsky.feed.like/3l{:011}\0subject.uri",
                i / 7, i % 7, i.wrapping_mul(2654435761) % (1 << 40), i
            );
            entry(key.as_bytes(), i as u64, i as u128)
        })
        .collect();
    let opts = RunBuildOpts::default();
    let mut alloc = Allocator::new(1 << 17, 256, 0);
    let seg_bytes = alloc.blocks_per_segment() as usize * BS;
    let dev = MemBlockDevice::new(BS, 1 << 17);
    let (segs, lbas) = reserve_for(&mut alloc, &entries, opts, RUN_VERSION);
    for round in 0..3 {
        for verify in [false, true] {
            let (bytes_before, _) = run_build_verify_totals();
            let sink = SegmentStreamSink::new(&dev, &segs, &lbas, seg_bytes, BS);
            let mut builder = RunStreamBuilder::new(sink, opts, n).version(RUN_VERSION).verify(verify);
            let t = std::time::Instant::now();
            for e in &entries {
                builder.push(e);
            }
            let push_s = t.elapsed().as_secs_f64();
            let (handle, parts) = block_on(builder.finish_with_parts()).unwrap();
            let finish_s = t.elapsed().as_secs_f64() - push_s;
            let (bytes_after, _) = run_build_verify_totals();
            let ft_bytes = read_run_range_lbas(&dev, &lbas, seg_bytes, handle.len - FOOTER_LEN as u64, FOOTER_LEN).unwrap();
            let ft = decode_footer(&ft_bytes).unwrap();
            let tail = ft.bloom_len + ft.index_len + FOOTER_LEN as u64;
            println!(
                "round {round} verify={verify}: run {:.1} MiB, {} blocks, data {:.1} MiB, index {:.2} MiB, \
                 filter {:.2} MiB, tail {:.2}% of run; push {push_s:.3}s finish {finish_s:.3}s; \
                 verify bytes {:.1} MiB",
                handle.len as f64 / 1048576.0, parts.index.len(), ft.bloom_off as f64 / 1048576.0,
                ft.index_len as f64 / 1048576.0, ft.bloom_len as f64 / 1048576.0,
                100.0 * tail as f64 / handle.len as f64, (bytes_after - bytes_before) as f64 / 1048576.0,
            );
            if verify {
                let lbas = &lbas[..handle.segs.len()];
                let read = |off: u64, len: u64| read_run_range_lbas(&dev, lbas, seg_bytes, off, len as usize).unwrap();
                let (bloom, idx) = (read(ft.bloom_off, ft.bloom_len), read(ft.index_off, ft.index_len));
                let t = std::time::Instant::now();
                for _ in 0..10 {
                    read_run_parts(&dev, &handle, lbas, seg_bytes).unwrap();
                }
                let plain = t.elapsed().as_secs_f64() / 10.0;
                let expects: Vec<[Vec<u8>; 4]> =
                    (0..10).map(|_| [ft_bytes.clone(), bloom.clone(), idx.clone(), Vec::new()]).collect();
                let t = std::time::Instant::now();
                for expect in expects {
                    verify_run_tail(&dev, &handle, lbas, seg_bytes, expect).unwrap();
                }
                let checked = t.elapsed().as_secs_f64() / 10.0;
                let t = std::time::Instant::now();
                for _ in 0..10 {
                    verify_blocks(&dev, lbas, seg_bytes, &parts.index).unwrap();
                }
                let blocks = t.elapsed().as_secs_f64() / 10.0;
                println!(
                    "  tail readback+decode {:.2} ms, with compare {:.2} ms (+{:.2} ms); data-block verify {:.2} ms",
                    plain * 1e3, checked * 1e3, (checked - plain) * 1e3, blocks * 1e3,
                );
            }
        }
    }
}

#[test]
#[ignore]
fn bloom_probe_microbench() {
    let n = 32_000_000usize;
    let keys: Vec<Vec<u8>> = (0..n)
        .map(|i| format!("app.bsky.feed.like/did:plc:bench/{i:012}").into_bytes())
        .collect();
    let classic = Bloom::build(keys.iter().map(|k| k.as_slice()), n, 12);
    let blocked = BlockedBloom::build(keys.iter().map(|k| k.as_slice()), n, 12);
    let probes: Vec<Vec<u8>> = (0..2_000_000usize)
        .map(|i| format!("app.bsky.feed.like/did:plc:absent/{i:012}").into_bytes())
        .collect();
    for (name, f) in [
        (
            "classic",
            &(|k: &[u8]| classic.contains(k) as u64) as &dyn Fn(&[u8]) -> u64,
        ),
        (
            "blocked",
            &(|k: &[u8]| blocked.contains(k) as u64) as &dyn Fn(&[u8]) -> u64,
        ),
    ] {
        let t = std::time::Instant::now();
        let mut hits = 0u64;
        for p in &probes {
            hits += f(p);
        }
        let el = t.elapsed();
        println!(
            "{name}: {:.1} ns/probe ({hits} false positives / {} probes)",
            el.as_nanos() as f64 / probes.len() as f64,
            probes.len()
        );
    }
}

struct IndexCorruptDev {
    inner: MemBlockDevice,
    target: std::sync::Mutex<Option<u64>>,
    corrupt_next: std::sync::atomic::AtomicBool,
    vary_reads: std::sync::atomic::AtomicBool,
    target_reads: std::sync::atomic::AtomicUsize,
}

impl BlockDevice for IndexCorruptDev {
    fn block_size(&self) -> usize { self.inner.block_size() }
    fn num_blocks(&self) -> u64 { self.inner.num_blocks() }
    fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
        self.inner.write_at(lba, buf)
    }
    fn flush(&self) -> vals_spdk::Result<()> { self.inner.flush() }
    fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
        use std::sync::atomic::Ordering::Relaxed;
        self.inner.read_at(lba, buf)?;
        let start = lba * self.block_size() as u64;
        if let Some(target) = *self.target.lock().unwrap() {
            if target >= start && target < start + buf.len() as u64 {
                let n = self.target_reads.fetch_add(1, Relaxed);
                if self.vary_reads.load(Relaxed) {
                    buf[(target - start) as usize] ^= (n + 1) as u8;
                }
                if self.corrupt_next.swap(false, Relaxed) {
                    buf[(target - start) as usize] ^= 0x80;
                }
            }
        }
        Ok(())
    }
}

#[test]
fn index_crc_attribution_transient_and_persistent_at_both_sites() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
    for site in ["open", "readback"] {
        for persistent in [false, true] {
            let bytes = build_run(&firehose(600), RunBuildOpts {
                block_target_bytes: 128, bloom_bits_per_key: 10, num_buckets: 8,
            });
            let ft = decode_footer(&bytes[bytes.len() - FOOTER_LEN..]).unwrap();
            let dev = Arc::new(IndexCorruptDev {
                inner: MemBlockDevice::new(512, 8192),
                target: std::sync::Mutex::new(None),
                corrupt_next: AtomicBool::new(false),
                vary_reads: AtomicBool::new(false),
                target_reads: AtomicUsize::new(0),
            });
            let mut alloc = Allocator::new(8192, 8, 3);
            for i in (0..alloc.num_segments()).step_by(2) {
                alloc.mark_allocated(SegmentId(i)).unwrap();
            }
            let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
            let seg_bytes = 4096;
            assert!(ft.index_len > 2 * seg_bytes as u64);
            let seg_lbas: Vec<u64> = handle.segs.iter()
                .map(|s| alloc.segment_range(*s).0).collect();
            let run_off = ft.index_off + seg_bytes as u64 + 17;
            let seg_index = run_off as usize / seg_bytes;
            let within = run_off as usize % seg_bytes;
            let lba = seg_lbas[seg_index] + (within / 512) as u64;
            *dev.target.lock().unwrap() = Some(lba * 512 + (within % 512) as u64);
            if persistent {
                let mut block = vec![0; 512];
                dev.inner.read_at(lba, &mut block).unwrap();
                block[within % 512] ^= 0x80;
                dev.inner.write_at(lba, &block).unwrap();
            }
            dev.corrupt_next.store(!persistent, Relaxed);
            let first = read_run_range_lbas(&*dev, &seg_lbas, seg_bytes,
                ft.index_off, ft.index_len as usize).unwrap();
            assert_eq!(decode_index(&first, ft.version).unwrap_err(), "index crc mismatch");
            let evidence = inspect_index_crc(&*dev, &seg_lbas, seg_bytes, &ft, &first);
            assert_eq!(evidence.verdict, if persistent { "persistent" } else { "transient" });
            assert!(!evidence.first_crc.passes());
            assert_eq!(evidence.reread_crc[0].as_ref().unwrap().passes(), !persistent);
            assert_eq!(evidence.reread_crc[0], evidence.reread_crc[1]);
            let diff = evidence.diff.as_ref().unwrap();
            assert_eq!(diff.bytes, usize::from(!persistent));
            if !persistent {
                let location = IndexDiffLocation {
                    run_off, seg_index, lba, byte_in_block: within % 512,
                };
                assert_eq!(diff.first.as_ref(), Some(&location));
                assert_eq!(diff.last.as_ref(), Some(&location));
                assert!(!diff.first_read_all_zero);
                assert!(!diff.span_4k_aligned);
                assert!(!diff.span_128k_aligned);
            } else {
                assert_eq!(diff.first, None);
                assert_eq!(diff.last, None);
            }
            let warning = IndexCrcWarning {
                site, handle: &handle, seg_lbas: &seg_lbas, seg_bytes,
                block_size: 512, device_key: dev.maintenance_sibling_cache_key(),
                ft: &ft, evidence: &evidence,
            }.to_string();
            assert!(warning.starts_with(&format!("WARN run index crc mismatch site={site}")));
            assert!(warning.contains(&format!("verdict={}", evidence.verdict)));
            dev.corrupt_next.store(!persistent, Relaxed);
            dev.target_reads.store(0, Relaxed);
            let result = if site == "open" {
                DeviceRun::open(dev.clone(), &alloc, handle.clone()).map(|_| ())
            } else {
                read_run_parts(&*dev, &handle, &seg_lbas, seg_bytes).map(|_| ())
            };
            assert!(matches!(result, Err(SnapshotError::Corrupt("index crc mismatch"))));
            assert_eq!(dev.target_reads.load(Relaxed), 3);
            let retry = read_run_parts(&*dev, &handle, &seg_lbas, seg_bytes);
            assert_eq!(retry.is_ok(), !persistent);
            if !persistent {
                dev.target_reads.store(0, Relaxed);
                dev.vary_reads.store(true, Relaxed);
                let first = read_run_range_lbas(&*dev, &seg_lbas, seg_bytes,
                    ft.index_off, ft.index_len as usize).unwrap();
                let evidence = inspect_index_crc(&*dev, &seg_lbas, seg_bytes, &ft, &first);
                assert_eq!(evidence.verdict, "unstable");
                assert!(evidence.reread_crc.iter().all(|r| !r.as_ref().unwrap().passes()));
                assert_eq!(evidence.diff.unwrap().bytes, 1);
            }
        }
    }
}

#[test]
fn index_crc_diff_zero_span_crosses_noncontiguous_segments() {
    let dev = MemBlockDevice::new(512, 1024);
    let seg_bytes = 4096;
    let seg_lbas = [3, 35, 19, 67];
    let mut builder = FenceBuilder::default();
    builder.push(&vec![b'x'; 12300], 0, 32).unwrap();
    let index = encode_index_bytes(&builder.finish(), None);
    let ft = Footer {
        entry_count: 1, index_off: 0, index_len: index.len() as u64,
        bloom_off: 0, bloom_len: 0, sidecar_off: 0, sidecar_len: 0,
        num_buckets: 1, version: RUN_VERSION,
    };
    for (i, &lba) in seg_lbas.iter().enumerate() {
        dev.write_at(lba, &run_seg_write_buf(&index, i, seg_bytes, 512)).unwrap();
    }
    let mut first = index.clone();
    first[4096..12288].fill(0);
    let evidence = inspect_index_crc(&dev, &seg_lbas, seg_bytes, &ft, &first);
    assert_eq!(evidence.verdict, "transient");
    let diff = evidence.diff.unwrap();
    assert_eq!(diff.bytes, 8192);
    assert_eq!(diff.first, Some(IndexDiffLocation {
        run_off: 4096, seg_index: 1, lba: 35, byte_in_block: 0,
    }));
    assert_eq!(diff.last, Some(IndexDiffLocation {
        run_off: 12287, seg_index: 2, lba: 26, byte_in_block: 511,
    }));
    assert!(diff.first_read_all_zero);
    assert!(diff.span_4k_aligned);
    assert!(!diff.span_128k_aligned);
}

struct BatchCountingDev {
    inner: MemBlockDevice,
    batches: std::sync::Mutex<Vec<usize>>,
}

impl BatchCountingDev {
    fn new(block_size: usize, num_blocks: u64) -> Self {
        BatchCountingDev {
            inner: MemBlockDevice::new(block_size, num_blocks),
            batches: std::sync::Mutex::new(Vec::new()),
        }
    }
    fn take_batches(&self) -> Vec<usize> {
        std::mem::take(&mut self.batches.lock().unwrap())
    }
}

impl BlockDevice for BatchCountingDev {
    fn block_size(&self) -> usize {
        self.inner.block_size()
    }
    fn num_blocks(&self) -> u64 {
        self.inner.num_blocks()
    }
    fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
        self.inner.read_at(lba, buf)
    }
    fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
        self.inner.write_at(lba, buf)
    }
    fn flush(&self) -> vals_spdk::Result<()> {
        self.inner.flush()
    }
    fn read_batch(&self, ops: &mut [vals_spdk::ReadOp<'_>]) -> vals_spdk::Result<()> {
        self.batches.lock().unwrap().push(ops.len());
        self.inner.read_batch(ops)
    }
}

fn collect_prefetch<D: BlockDevice>(mut s: RunPrefetchScan<'_, D>) -> Vec<SnapshotEntry> {
    let mut out = Vec::new();
    while let Some(e) = s.next_entry().unwrap() {
        out.push(e);
    }
    out
}

#[test]
fn prefetch_scan_matches_block_scan_and_issues_qd_batches() {
    let entries = firehose(600);
    let opts = RunBuildOpts {
        block_target_bytes: 256,
        bloom_bits_per_key: 10,
        num_buckets: 8,
    };
    let bytes = build_run(&entries, opts);
    let dev = Arc::new(BatchCountingDev::new(512, 8192));
    let mut alloc = Allocator::new(8192, 8, 0);
    let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
    assert!(handle.segs.len() > 2, "expected a multi-segment run");
    let run = DeviceRun::open(dev.clone(), &alloc, handle).unwrap();

    let mut want = Vec::new();
    let mut s = run.block_scan(None, None);
    while let Some(e) = s.next(&alloc).unwrap() {
        want.push(e);
    }
    assert_eq!(want, entries, "sanity: full scan yields every entry");

    dev.take_batches();
    let got = collect_prefetch(run.prefetch_scan(&alloc, 4096, 8));
    assert_eq!(got, want);
    let batches = dev.take_batches();
    assert!(!batches.is_empty(), "read-ahead must go through read_batch");
    assert!(
        batches.iter().any(|&n| n > 1),
        "expected a read_batch with ops > 1 (QD > 1), got {batches:?}"
    );
}

#[test]
fn prefetch_scan_edge_cases() {
    let opts = RunBuildOpts {
        block_target_bytes: 256,
        bloom_bits_per_key: 10,
        num_buckets: 4,
    };

    let one = vec![entry(b"only-key", 1, 42)];
    let (dev, mut alloc) = dev_alloc(256, 4);
    let handle = write_run(&*dev, &mut alloc, &build_run(&one, opts)).unwrap();
    let run = DeviceRun::open(dev, &alloc, handle).unwrap();
    assert_eq!(collect_prefetch(run.prefetch_scan(&alloc, 1024, 4)), one);

    let (dev, mut alloc) = dev_alloc(256, 4);
    let handle = write_run(&*dev, &mut alloc, &build_run(&[], opts)).unwrap();
    let run = DeviceRun::open(dev, &alloc, handle).unwrap();
    assert_eq!(collect_prefetch(run.prefetch_scan(&alloc, 1024, 4)), vec![]);

    let entries = firehose(200);
    let (dev, mut alloc) = dev_alloc(2048, 4);
    let handle = write_run(&*dev, &mut alloc, &build_run(&entries, opts)).unwrap();
    let run = DeviceRun::open(dev, &alloc, handle).unwrap();
    assert_eq!(collect_prefetch(run.prefetch_scan(&alloc, 1, 1)), entries);

    assert_eq!(
        collect_prefetch(run.prefetch_scan(&alloc, usize::MAX, 8)),
        entries
    );
}

#[test]
fn read_run_range_batched_matches_sequential() {
    let entries = firehose(400);
    let opts = RunBuildOpts {
        block_target_bytes: 512,
        bloom_bits_per_key: 10,
        num_buckets: 8,
    };
    let bytes = build_run(&entries, opts);
    let dev = Arc::new(BatchCountingDev::new(512, 8192));
    let mut alloc = Allocator::new(8192, 8, 0);
    let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
    let seg_bytes = alloc.blocks_per_segment() as usize * dev.block_size();
    let seg_lbas: Vec<u64> = handle
        .segs
        .iter()
        .map(|s| alloc.segment_range(*s).0)
        .collect();
    let len = handle.len as usize;
    let cases: [(u64, usize); 7] = [
        (0, len),
        (0, 1),
        (37, 1000),
        (seg_bytes as u64 - 100, 200),
        (0, seg_bytes * 2 + 513),
        (len as u64 - 1, 1),
        (513, seg_bytes),
    ];
    for (off, l) in cases {
        let l = l.min(len - off as usize);
        let want =
            read_run_range_lbas(&*dev, &seg_lbas, seg_bytes, off, l).unwrap();
        for qd in [1usize, 3, 8, 64] {
            let got =
                read_run_range_batched(&*dev, &seg_lbas, seg_bytes, off, l, qd).unwrap();
            assert_eq!(got, want, "range ({off}, {l}) at qd {qd}");
        }
    }
    assert!(read_run_range_batched(&*dev, &seg_lbas, seg_bytes, handle.len, len, 8).is_err());
}

#[test]
#[ignore]
fn bench_v4_v5_run_bytes_and_version_stream_cpu() {
    let n = 400_000usize;
    let mut rng = Rng(0x0365_bac0_0000_0001);
    let mut keys: Vec<Vec<u8>> = (0..n)
        .map(|i| {
            let subject = (i / 4) as u64;
            let mut key = format!(
                "at://did:plc:{:024x}/app.bsky.feed.post/3k{:011}|app.bsky.feed.like|did:plc:{:024x}/3l",
                splitmix64(subject) as u128 * 7, subject, rng.next() as u128
            ).into_bytes();
            while key.len() < 148 {
                key.push(b"234567abcdefghijklmnopqrstuvwxyz"[rng.range(0, 31) as usize]);
            }
            key
        })
        .collect();
    keys.sort();
    keys.dedup();
    let entries: Vec<SnapshotEntry> = keys
        .into_iter()
        .enumerate()
        .map(|(i, key)| {
            let mut e = entry(&key, i as u64, ((rng.next() as u128) << 64) | i as u128);
            e.ptr.len = 9 + 40;
            e
        })
        .collect();
    let n = entries.len();
    let avg_key = entries.iter().map(|e| e.key.len()).sum::<usize>() as f64 / n as f64;
    println!("entries={n} avg_key={avg_key:.1}B");
    let opts = RunBuildOpts::default();
    for version in [RUN_SIDECAR_VERSION, RUN_VERSION] {
        let bytes = build_run_versioned(&entries, opts, version);
        let dev = Arc::new(MemBlockDevice::new(BS, (bytes.len() as u64 * 3) / BS as u64 + 4096));
        let mut alloc = Allocator::new(dev.num_blocks(), 256, 0);
        let seg_bytes = alloc.blocks_per_segment() as usize * BS;
        let (segs, lbas) = reserve_for(&mut alloc, &entries, opts, version);
        struct ReadBytes(MemBlockDevice, std::sync::atomic::AtomicU64);
        let reads = ReadBytes(dev.share(), std::sync::atomic::AtomicU64::new(0));
        impl BlockDevice for ReadBytes {
            fn block_size(&self) -> usize { self.0.block_size() }
            fn num_blocks(&self) -> u64 { self.0.num_blocks() }
            fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
                self.1.fetch_add(buf.len() as u64, std::sync::atomic::Ordering::Relaxed);
                self.0.read_at(lba, buf)
            }
            fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> { self.0.write_at(lba, buf) }
            fn flush(&self) -> vals_spdk::Result<()> { self.0.flush() }
        }
        let t = std::time::Instant::now();
        let sink = SegmentStreamSink::new(&reads, &segs, &lbas, seg_bytes, BS);
        let mut builder = RunStreamBuilder::new(sink, opts, n).version(version);
        for e in &entries {
            builder.push(e);
        }
        let handle = block_on(builder.finish()).unwrap();
        let build = t.elapsed();
        let readback = reads.1.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(handle.len as usize, bytes.len());
        let run = DeviceRun::open(dev.clone(), &alloc, handle).unwrap();
        let mut best_seed = f64::MAX;
        let mut best_async = f64::MAX;
        let mut best_fold = f64::MAX;
        for _ in 0..3 {
            let t = std::time::Instant::now();
            let mut stream = run.version_stream(&alloc).unwrap().unwrap();
            let mut count = 0usize;
            loop {
                match stream.next_step().unwrap() {
                    SidecarCursorStep::Entry(_) => count += 1,
                    SidecarCursorStep::Done => break,
                    SidecarCursorStep::Checkpoint => {}
                }
            }
            assert_eq!(count, n);
            best_seed = best_seed.min(t.elapsed().as_nanos() as f64 / n as f64);
            let want: std::collections::HashSet<u32> = (0..8).collect();
            let bucket_of = |k: &[u8]| test_bucket(k) % 16;
            let t = std::time::Instant::now();
            let got = drain_version_source(&dev, run.version_source(&alloc, None), &want, &bucket_of, None).unwrap();
            assert!(got.len() > n / 3);
            best_async = best_async.min(t.elapsed().as_nanos() as f64 / n as f64);
            let t = std::time::Instant::now();
            let mut folded = 0usize;
            run.for_each_bucket_digest(&alloc, |_, _| folded += 1).unwrap();
            assert_eq!(folded, n);
            best_fold = best_fold.min(t.elapsed().as_nanos() as f64 / n as f64);
        }
        println!(
            "v{version}: run {:.1} B/entry (sidecar {:.1} B/entry), streamed build {:.0} ns/entry with {:.1} B/entry build readback; seed stream {best_seed:.0} ns/entry, async digest cursor {best_async:.0} ns/entry, for_each_bucket_digest {best_fold:.0} ns/entry",
            bytes.len() as f64 / n as f64,
            run.sidecar_len() as f64 / n as f64,
            build.as_nanos() as f64 / n as f64,
            readback as f64 / n as f64,
        );
    }
}

#[test]
#[ignore]
fn bench_prefetch_scan_file_device() {
    use vals_spdk::FileBlockDevice;
    let n = 1_000_000usize;
    let entries = firehose(n);
    let opts = RunBuildOpts::default();
    let bytes = build_run(&entries, opts);
    let path = std::env::temp_dir().join(format!(
        "vals-bench-prefetch-{}.img",
        std::process::id()
    ));
    let seg_blocks = 512u64;
    let blocks = (bytes.len() as u64 / BS as u64 + 2 * seg_blocks) / seg_blocks * seg_blocks
        + seg_blocks;
    let dev = Arc::new(FileBlockDevice::create(&path, BS, blocks).unwrap());
    let mut alloc = Allocator::new(blocks, seg_blocks, 0);
    let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
    let run = DeviceRun::open(dev.clone(), &alloc, handle.clone()).unwrap();
    let seg_bytes = alloc.blocks_per_segment() as usize * BS;
    let seg_lbas: Vec<u64> = handle
        .segs
        .iter()
        .map(|s| alloc.segment_range(*s).0)
        .collect();
    let (_, last_off, last_len) = run.index.last().unwrap();
    let data_len = (*last_off + *last_len as u64) as usize;
    let mibs = data_len as f64 / (1024.0 * 1024.0);
    println!(
        "run: {n} entries, {:.1} MiB data blocks, {} segments",
        mibs,
        handle.segs.len()
    );
    let rate = |d: std::time::Duration| data_len as f64 / d.as_secs_f64() / 1e6;

    for round in 0..3 {
        let t = std::time::Instant::now();
        let mut sink = 0usize;
        for (_, off, len) in run.index.iter() {
            sink += read_run_range_lbas(&*dev, &seg_lbas, seg_bytes, *off, *len as usize)
                .unwrap()
                .len();
        }
        let per_block = t.elapsed();
        let t = std::time::Instant::now();
        let mut off = 0usize;
        while off < data_len {
            let l = (data_len - off).min(2 * 1024 * 1024);
            sink += read_run_range_batched(&*dev, &seg_lbas, seg_bytes, off as u64, l, 8)
                .unwrap()
                .len();
            off += l;
        }
        let windowed = t.elapsed();
        let t = std::time::Instant::now();
        let mut scan = run.block_scan(None, None);
        let mut count = 0usize;
        while let Some(e) = scan.next(&alloc).unwrap() {
            count += e.key.len();
        }
        let scan_drain = t.elapsed();
        let t = std::time::Instant::now();
        let mut pre = run.prefetch_scan(&alloc, 2 * 1024 * 1024, 8);
        let mut count2 = 0usize;
        while let Some(e) = pre.next_entry().unwrap() {
            count2 += e.key.len();
        }
        let pre_drain = t.elapsed();
        assert_eq!(count, count2);
        println!(
            "round {round}: raw per-block {:.0} MB/s | raw windowed {:.0} MB/s ({:.2}x) | \
             RunScan drain {:.0} MB/s | prefetch drain {:.0} MB/s ({:.2}x) [sink {sink}]",
            rate(per_block),
            rate(windowed),
            rate(windowed) / rate(per_block),
            rate(scan_drain),
            rate(pre_drain),
            rate(pre_drain) / rate(scan_drain),
        );
    }
    drop(run);
    let _ = std::fs::remove_file(&path);
}

fn block_lookup_alloc_reference(bytes: &[u8], key: &[u8]) -> Result<Option<SnapshotEntry>, &'static str> {
    let entries = decode_block(bytes)?;
    for e in entries {
        match e.key.as_slice().cmp(key) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => return Ok(Some(e)),
            std::cmp::Ordering::Greater => return Ok(None),
        }
    }
    Ok(None)
}

fn rand_key(rng: &mut Rng) -> Vec<u8> {
    let len = rng.range(0, 6) as usize;
    (0..len).map(|_| b"abc\x00\xff"[rng.range(0, 4) as usize]).collect()
}

#[test]
fn in_place_block_lookup_matches_decoding_lookup_over_random_blocks() {
    let mut rng = Rng(0x0378_b10c_0000_0001);
    for _ in 0..3000 {
        let n = rng.range(0, 40) as usize;
        let mut keys: Vec<Vec<u8>> = (0..n).map(|_| rand_key(&mut rng)).collect();
        keys.sort();
        if rng.next() & 1 == 0 {
            keys.dedup();
        }
        let entries: Vec<SnapshotEntry> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                let mut e = entry(k, i as u64, rng.next() as u128);
                e.tombstone = rng.next() & 3 == 0;
                e
            })
            .collect();
        let block = encode_block(&entries);
        for _ in 0..20 {
            let probe = if !keys.is_empty() && rng.next() & 1 == 0 {
                keys[rng.range(0, keys.len() as u64 - 1) as usize].clone()
            } else {
                rand_key(&mut rng)
            };
            assert_eq!(block_lookup(&block, &probe), block_lookup_alloc_reference(&block, &probe), "keys {keys:?} probe {probe:?}");
        }
    }
    let mut bad = encode_block(&[entry(b"ab", 1, 1), entry(b"ac", 2, 2)]);
    bad[4..8].copy_from_slice(&0u32.to_le_bytes());
    let body = bad.len() - 4;
    let at = 4 + 8 + 2 + 40;
    bad[at..at + 4].copy_from_slice(&9u32.to_le_bytes());
    let crc = crc::crc32(&bad[..body]).to_le_bytes();
    bad[body..].copy_from_slice(&crc);
    assert_eq!(block_lookup(&bad, b"zz"), Err("shared prefix longer than previous key"));
    assert_eq!(block_lookup(&bad, b"zz"), block_lookup_alloc_reference(&bad, b"zz"));
}

#[test]
fn block_cursor_seeks_in_place_and_decodes_lazily_like_decode_block() {
    let mut rng = Rng(0x0378_b10c_0000_0002);
    for _ in 0..3000 {
        let n = rng.range(0, 40) as usize;
        let mut keys: Vec<Vec<u8>> = (0..n).map(|_| rand_key(&mut rng)).collect();
        keys.sort();
        keys.dedup();
        let entries: Vec<SnapshotEntry> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                let mut e = entry(k, i as u64, rng.next() as u128);
                e.tombstone = rng.next() & 3 == 0;
                e
            })
            .collect();
        let block = encode_block(&entries);
        for _ in 0..12 {
            let lo = if rng.next().is_multiple_of(5) {
                None
            } else if !keys.is_empty() && rng.next() & 1 == 0 {
                Some(keys[rng.range(0, keys.len() as u64 - 1) as usize].clone())
            } else {
                Some(rand_key(&mut rng))
            };
            let exclusive = rng.next() & 1 == 0;
            let want: Vec<SnapshotEntry> = entries
                .iter()
                .filter(|e| lo.as_ref().is_none_or(|l| if exclusive { e.key > *l } else { e.key >= *l }))
                .cloned()
                .collect();
            let mut cursor = BlockCursor::new(block.clone(), lo.as_deref(), exclusive).unwrap();
            let mut got = Vec::new();
            while let Some(e) = cursor.next_entry().unwrap() {
                got.push(e);
            }
            assert_eq!(got, want, "keys {keys:?} lo {lo:?} exclusive {exclusive}");
        }
    }
    let mut bad = encode_block(&firehose(8));
    let n = bad.len();
    bad[n / 2] ^= 0xff;
    assert_eq!(BlockCursor::new(bad, None, false).err(), Some("crc mismatch"));
    assert!(BlockCursor::new(vec![0; 4], None, false).is_err());
}

#[test]
fn block_target_and_bloom_knobs_parse_within_bounds() {
    assert_eq!(parse_run_block_target_bytes(None), 32 * 1024);
    assert_eq!(parse_run_block_target_bytes(Some(" ")), 32 * 1024);
    assert_eq!(parse_run_block_target_bytes(Some("4096")), 4096);
    assert_eq!(parse_run_block_target_bytes(Some("262144")), 262144);
    assert_eq!(parse_run_block_target_bytes(Some("65536 ")), 65536);
    assert_eq!(parse_run_block_target_bytes(Some("100")), 4096);
    assert_eq!(parse_run_block_target_bytes(Some("262145")), 262144);
    assert_eq!(parse_run_block_target_bytes(Some("32k")), 32 * 1024);
    assert_eq!(parse_run_block_target_bytes(Some("-1")), 32 * 1024);
    assert_eq!(parse_run_bloom_bits_per_key(None), 10);
    assert_eq!(parse_run_bloom_bits_per_key(Some("12")), 12);
    assert_eq!(parse_run_bloom_bits_per_key(Some("1")), 4);
    assert_eq!(parse_run_bloom_bits_per_key(Some("99")), 24);
    assert_eq!(parse_run_bloom_bits_per_key(Some("x")), 10);
    assert_eq!(with_run_block_target(4096, || RunBuildOpts::default().block_target_bytes), 4096);
    assert_eq!(RunBuildOpts::default().block_target_bytes, run_block_target_bytes());
}

fn backlink_entries(n: usize, seed: u64) -> Vec<SnapshotEntry> {
    let mut rng = Rng(seed);
    let mut keys: Vec<Vec<u8>> = (0..n)
        .map(|i| {
            let subject = (i / 4) as u64;
            let mut key = format!(
                "at://did:plc:{:024x}/app.bsky.feed.post/3k{:011}|app.bsky.feed.like|did:plc:{:024x}/3l",
                splitmix64(subject) as u128 * 7, subject, rng.next() as u128
            ).into_bytes();
            while key.len() < 148 {
                key.push(b"234567abcdefghijklmnopqrstuvwxyz"[rng.range(0, 31) as usize]);
            }
            key
        })
        .collect();
    keys.sort();
    keys.dedup();
    keys.iter().enumerate().map(|(i, k)| entry(k, i as u64, i as u128 + 1)).collect()
}

fn record_entries(n: usize, seed: u64) -> Vec<SnapshotEntry> {
    let mut rng = Rng(seed);
    let mut keys: Vec<Vec<u8>> = (0..n)
        .map(|i| {
            let did = splitmix64((i / 16) as u64);
            format!("at://did:plc:{did:024x}/app.bsky.feed.post/3k{:011}", rng.next() % 100_000_000_000)
                .into_bytes()
        })
        .collect();
    keys.sort();
    keys.dedup();
    keys.iter().enumerate().map(|(i, k)| entry(k, i as u64, i as u128 + 1)).collect()
}

fn open_built(entries: &[SnapshotEntry], opts: RunBuildOpts) -> (Arc<MemBlockDevice>, Allocator, DeviceRun<MemBlockDevice>) {
    let bytes = build_run_versioned(entries, opts, RUN_VERSION);
    let (dev, mut alloc) = dev_alloc((bytes.len() / BS) as u64 * 2 + 64, 256);
    let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
    let run = DeviceRun::open(dev.clone(), &alloc, handle).unwrap();
    (dev, alloc, run)
}

#[test]
fn scan_open_matches_serving_scans_with_tombstones() {
    let mut entries = backlink_entries(2_000, 0x0397);
    for e in entries.iter_mut().step_by(7) { e.tombstone = true; }
    for version in RUN_SIDECAR_VERSION..=RUN_VERSION {
        let bytes = build_run_versioned(&entries, RunBuildOpts::default(), version);
        let (dev, mut alloc) = dev_alloc((bytes.len() / BS) as u64 * 2 + 1024, 256);
        let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
        let scan_run = DeviceRun::open_scan(dev.clone(), &alloc, handle.clone()).unwrap();
        let serving = DeviceRun::open(dev, &alloc, handle).unwrap();
        assert_eq!(scan_run.resident_stats().filter_bytes, 0);
        assert!(serving.resident_stats().filter_bytes > 0);
        let mut scan = scan_run.block_scan(None, None);
        let mut prefetched = scan_run.prefetch_scan(&alloc, 64 << 10, 2);
        for expected in &entries {
            assert_eq!(scan.next(&alloc).unwrap().as_ref(), Some(expected));
            assert_eq!(prefetched.next_entry().unwrap().as_ref(), Some(expected));
            assert_eq!(serving.lookup(&alloc, &expected.key).unwrap().as_ref(), Some(expected));
        }
        assert!(scan.next(&alloc).unwrap().is_none());
        assert!(prefetched.next_entry().unwrap().is_none());
    }
}

#[test]
fn default_runs_build_32k_fences_and_10_bit_blooms() {
    let entries = backlink_entries(20_000, 0x0378_0000_0000_0002);
    let n = entries.len();
    assert_eq!(n, 20_000);
    let (_dev, alloc, run) = open_built(&entries, RunBuildOpts::default());
    let per_block = n / run.index_len();
    assert!((150..=200).contains(&per_block), "{per_block} entries per default block");
    assert!((12 * 1024..=34 * 1024).contains(&run.max_block_bytes()), "max block {}", run.max_block_bytes());
    let stats = run.resident_stats();
    let fence_per_entry = stats.fence_bytes as f64 / n as f64;
    assert!(fence_per_entry < 1.2, "{fence_per_entry} fence B/entry");
    let RunFilter::Blocked(bloom) = &run.bloom else { panic!("v5 runs use the blocked bloom") };
    assert_eq!(bloom.k, 7);
    assert_eq!(bloom.num_bits, (n as u64 * 10).next_multiple_of(BLOOM_BLOCK_BITS));
    for e in entries.iter().step_by(37) {
        assert_eq!(run.lookup(&alloc, &e.key).unwrap().as_ref(), Some(e));
    }
}

#[test]
fn old_12_bit_4k_runs_still_read_and_10_bits_hold_fpr() {
    let entries = record_entries(30_000, 0x0378_0000_0000_0003);
    let n = entries.len();
    let old = RunBuildOpts { block_target_bytes: 4096, bloom_bits_per_key: 12, num_buckets: 4096 };
    let (_d12, a12, r12) = open_built(&entries, old);
    let (_d10, a10, r10) = open_built(&entries, RunBuildOpts { block_target_bytes: 32 * 1024, bloom_bits_per_key: 10, num_buckets: 4096 });
    let (RunFilter::Blocked(b12), RunFilter::Blocked(b10)) = (&r12.bloom, &r10.bloom) else { panic!() };
    assert_eq!((b12.k, b10.k), (8, 7));
    assert!(r12.index_len() > 6 * r10.index_len());
    for e in entries.iter().step_by(11) {
        assert_eq!(r12.lookup(&a12, &e.key).unwrap().as_ref(), Some(e));
        assert_eq!(r10.lookup(&a10, &e.key).unwrap().as_ref(), Some(e));
    }
    let probes = 200_000usize;
    let fpr = |r: &DeviceRun<MemBlockDevice>| {
        (0..probes).filter(|i| r.might_contain(format!("absent-{i:09}").as_bytes())).count() as f64 / probes as f64
    };
    let (f12, f10) = (fpr(&r12), fpr(&r10));
    eprintln!("run bloom fpr: 12 bits/key {f12:.5}, 10 bits/key {f10:.5} over {n} keys");
    assert!(f12 < 0.006, "12-bit fpr {f12}");
    assert!(f10 < 0.013, "10-bit fpr {f10}");
}

#[test]
fn block_larger_than_the_stream_window_streams_seeds_digests_and_keys() {
    let entries = backlink_entries(8_000, 0x0378_0000_0000_0004);
    let opts = RunBuildOpts { block_target_bytes: SIDECAR_CURSOR_WINDOW * 3, bloom_bits_per_key: 10, num_buckets: 16 };
    let (dev, alloc, run) = build_streamed(&entries, opts, entries.len(), 8192, 16);
    assert!(run.max_block_bytes() > SIDECAR_CURSOR_WINDOW, "max block {}", run.max_block_bytes());
    assert!(run.index_len() >= 2);
    let want_pairs: Vec<(Vec<u8>, u128)> = entries.iter().map(|e| (e.key.clone(), e.version)).collect();
    let mut stream = run.version_stream(&alloc).unwrap().unwrap();
    let mut got = Vec::new();
    let mut reserved_max = 0u64;
    let mut bound_max = 0u64;
    loop {
        match stream.next_step_reserving(|b| { reserved_max = reserved_max.max(b); true }).unwrap().unwrap() {
            SidecarCursorStep::Entry(pair) => got.push(pair),
            SidecarCursorStep::Checkpoint => bound_max = bound_max.max(stream.resident_bound_bytes()),
            SidecarCursorStep::Done => break,
        }
    }
    assert_eq!(got, want_pairs);
    assert!(bound_max >= run.max_block_bytes() as u64, "resident bound {bound_max} covers the oversized window");
    assert!(reserved_max >= 2 * SIDECAR_CURSOR_WINDOW as u64, "an oversized window reserves its full length");
    let source = run.version_source(&alloc, None);
    assert!(source.resident_bound_bytes() >= 3 * run.max_block_bytes());
    let all: std::collections::HashSet<u32> = (0..16).collect();
    let bucket_of = |k: &[u8]| test_bucket(k) % 16;
    let drained = drain_version_source(&dev, source, &all, &bucket_of, None).unwrap();
    assert_eq!(drained, want_pairs);
    let mid = &entries[entries.len() / 2].key;
    let after = drain_version_source(&dev, run.version_source(&alloc, Some(mid)), &all, &bucket_of, Some(mid)).unwrap();
    assert_eq!(after.len(), entries.len() - entries.len() / 2 - 1);
    let mut folded = Vec::new();
    run.for_each_bucket_digest(&alloc, |k, v| folded.push((k.to_vec(), v))).unwrap();
    assert_eq!(folded, want_pairs);
    let mut keys = Vec::new();
    let mut source = run.key_source(&alloc);
    while !source.step(&*dev, 4096, |k| keys.push(k.to_vec())).unwrap() {}
    assert_eq!(keys.len(), entries.len());
    let (page, stop) = run.want_bucket_digests_page(&alloc, &all, bucket_of, Some(mid), 4096, 0).unwrap();
    assert!(!page.is_empty() && stop.is_some());
    assert!(page[0].0.as_slice() > mid.as_slice());
    for e in entries.iter().step_by(97) {
        assert_eq!(run.lookup(&alloc, &e.key).unwrap().as_ref(), Some(e));
    }
}

#[test]
#[ignore]
fn bench_block_target_fence_filter_and_point_lookup() {
    let n = 400_000usize;
    let shapes: [(&str, Vec<SnapshotEntry>); 2] = [
        ("backlinks", backlink_entries(n, 0x0378_bbbb_0000_0001)),
        ("records", record_entries(n, 0x0378_cccc_0000_0001)),
    ];
    let device_bs = 4096usize;
    for (name, entries) in &shapes {
        let n = entries.len();
        let avg_key = entries.iter().map(|e| e.key.len()).sum::<usize>() as f64 / n as f64;
        for (block, bits) in [(4096usize, 12usize), (32 * 1024, 10)] {
            let opts = RunBuildOpts { block_target_bytes: block, bloom_bits_per_key: bits, num_buckets: 4096 };
            let bytes = build_run_versioned(entries, opts, RUN_VERSION);
            let dev = Arc::new(MemBlockDevice::new(device_bs, (bytes.len() / device_bs) as u64 * 2 + 64));
            let mut alloc = Allocator::new(dev.num_blocks(), 256, 0);
            let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
            let run = DeviceRun::open(dev.clone(), &alloc, handle).unwrap();
            let stats = run.resident_stats();
            let malloc_fence = [run.index.iter().map(|(k, _, _)| k.len()).sum::<usize>(), run.index.len() * 16]
                .into_iter().filter(|n| *n != 0).map(|n| (n + 8).next_multiple_of(16).max(32)).sum::<usize>();
            let mut rng = Rng(0x0378_1000 ^ block as u64);
            let probes: Vec<&[u8]> = (0..200_000).map(|_| entries[rng.range(0, n as u64 - 1) as usize].key.as_slice()).collect();
            let mut stored = 0u64;
            let mut aligned = 0u64;
            for k in &probes {
                let loc = run.locate_block(&alloc, k).unwrap();
                stored += loc.len() as u64;
                let start = loc.off as usize / device_bs;
                let end = (loc.off as usize + loc.len()).div_ceil(device_bs);
                aligned += ((end - start) * device_bs) as u64;
            }
            let blocks: Vec<Vec<u8>> = probes.iter().map(|k| run.locate_block(&alloc, k).map(|l| block_on(l.read(&*dev)).unwrap()).unwrap()).collect();
            let mut best = [f64::MAX; 4];
            for _ in 0..3 {
                let t = std::time::Instant::now();
                for b in &blocks {
                    assert!(block_crc_ok(b));
                }
                best[3] = best[3].min(t.elapsed().as_nanos() as f64 / probes.len() as f64);
                let t = std::time::Instant::now();
                for (k, b) in probes.iter().zip(&blocks) {
                    assert!(block_lookup(b, k).unwrap().is_some());
                }
                best[0] = best[0].min(t.elapsed().as_nanos() as f64 / probes.len() as f64);
                let t = std::time::Instant::now();
                for (k, b) in probes.iter().zip(&blocks) {
                    assert!(block_lookup_alloc_reference(b, k).unwrap().is_some());
                }
                best[1] = best[1].min(t.elapsed().as_nanos() as f64 / probes.len() as f64);
                let t = std::time::Instant::now();
                for k in &probes {
                    assert!(run.lookup(&alloc, k).unwrap().is_some());
                }
                best[2] = best[2].min(t.elapsed().as_nanos() as f64 / probes.len() as f64);
            }
            println!(
                "BLOCKBENCH {name} avg_key={avg_key:.1}B entries={n} block_target={block} bloom_bits={bits}: entries/block={:.1} fence={:.3} B/entry (gauge) {:.3} B/entry (malloc est) filter={:.3} B/entry; get bytes/lookup stored={:.0} aligned={:.0}; block_lookup in-place={:.0} ns (crc alone {:.0} ns) alloc-ref={:.0} ns; DeviceRun::lookup (mem device read + crc + search)={:.0} ns",
                n as f64 / run.index_len() as f64,
                stats.fence_bytes as f64 / n as f64,
                malloc_fence as f64 / n as f64,
                stats.filter_bytes as f64 / n as f64,
                stored as f64 / probes.len() as f64,
                aligned as f64 / probes.len() as f64,
                best[0], best[3], best[1], best[2],
            );
        }
    }
}

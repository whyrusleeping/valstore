use super::*;
use crate::hlc::Version;
use std::time::{Duration, Instant};
use vals_spdk::{FileBlockDevice, MemBlockDevice};
use vals_store::{Config, Engine, Ordering as SOrdering};

const BUCKETS: u32 = crate::antientropy::DEFAULT_BUCKETS;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn bench_key(i: u64) -> Vec<u8> {
    let h = (i ^ 0x9e37_79b9_7f4a_7c15).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    format!("did:plc:{h:016x}/app.bsky.feed.post/{i:012}").into_bytes()
}

fn next_after(digests: &[(Vec<u8>, Version)], more: bool, resume: Option<Vec<u8>>) -> Option<Vec<u8>> {
    if !more {
        return None;
    }
    resume.or_else(|| Some(digests.last().expect("truncated digest page has an entry").0.clone()))
}

#[test]
fn expired_bucket_digest_serves_resume_to_the_unbounded_page() {
    let engine = Engine::create_prewiped(
        MemBlockDevice::new(256, 262_144),
        Config {
            seg_blocks: 256,
            reserved_blocks: 0,
            ordering: SOrdering::Unsorted,
        },
    );
    let mut store = VersionedStore::new(engine, 1).unwrap();
    for run in 0..4u64 {
        for i in (run * 2_000..24_000).filter(|i| (i + run) % 3 != 0) {
            let version = Version { hlc: (run + 1) << 20 | i, node: 1u32.into() };
            store.apply(&bench_key(i), Some(b"value-08"), version).unwrap();
        }
        store.flush().unwrap();
    }
    for i in (0..24_000u64).step_by(97) {
        let version = Version { hlc: 9 << 20 | i, node: 1u32.into() };
        store.apply(&bench_key(i), Some(b"newer"), version).unwrap();
    }
    assert!(store.run_count() >= 4);
    let ids = vec![7u32, 2_000];
    let (expected, more) = store.bucket_digest_page(BUCKETS, &ids, None, usize::MAX).unwrap();
    assert!(!more);
    assert!(!expected.is_empty());
    let store = RefCell::new(store);
    let dev = store.borrow().device_handle();
    let gate = ReadGate::new();
    let truncations = crate::observe::counters().ae_digest_serve_time_truncations.get();
    let (mut got, mut after, mut pages, mut resumed) = (Vec::new(), None::<Vec<u8>>, 0usize, 0usize);
    loop {
        let response = vals_rt::block_on(
            serve_bucket_digests_inner(&store, &*dev, &gate, BUCKETS, &ids, after.as_deref(), Some(Instant::now())),
            || {},
        );
        pages += 1;
        assert!(pages < 10_000);
        let Response::BucketDigests { digests, more, resume } = response else {
            panic!("expected bucket digests, got {response:?}");
        };
        if let Some(resume) = &resume {
            assert!(more);
            assert!(digests.iter().all(|(key, _)| key <= resume));
            resumed += 1;
        }
        assert!(digests.iter().all(|(key, _)| after.as_ref().is_none_or(|a| key > a)));
        let next = next_after(&digests, more, resume);
        got.extend(digests);
        match next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    assert_eq!(got, expected);
    let legacy = vals_rt::block_on(
        serve_bucket_digests_inner(&store, &*dev, &gate, BUCKETS, &ids, None, None),
        || {},
    );
    assert_eq!(
        legacy,
        Response::BucketDigests { digests: expected, more: false, resume: None },
        "a peer without the resumable-digest feature gets the unbounded page"
    );
    let legacy_peer = crate::proto::negotiate(
        crate::proto::PROTO_VERSION,
        crate::proto::FEATURE_OPAQUE_RANGE_MAP | crate::proto::FEATURE_MIXED_BATCH_READ,
    )
    .unwrap();
    assert_eq!(legacy_peer.features & crate::proto::FEATURE_RESUMABLE_DIGESTS, 0);
    assert!(resumed > 1, "expired pages must return resume keys ({pages} pages)");
    assert!(crate::observe::counters().ae_digest_serve_time_truncations.get() >= truncations + resumed as u64);
    assert_eq!(gate.inflight(), 0);
}

#[test]
fn bucket_digest_pages_end_at_the_hold_slice_while_the_deadline_is_far() {
    let engine = Engine::create_prewiped(
        MemBlockDevice::new(256, 262_144),
        Config { seg_blocks: 256, reserved_blocks: 0, ordering: SOrdering::Unsorted },
    );
    let mut store = VersionedStore::new(engine, 1).unwrap();
    for run in 0..4u64 {
        for i in (run * 2_000..24_000).filter(|i| (i + run) % 3 != 0) {
            let version = Version { hlc: (run + 1) << 20 | i, node: 1u32.into() };
            store.apply(&bench_key(i), Some(b"value-08"), version).unwrap();
        }
        store.flush().unwrap();
    }
    let ids = vec![7u32, 2_000];
    let (expected, _) = store.bucket_digest_page(BUCKETS, &ids, None, usize::MAX).unwrap();
    let store = RefCell::new(store);
    let dev = store.borrow().device_handle();
    let gate = ReadGate::new();
    struct Slice;
    impl Drop for Slice {
        fn drop(&mut self) {
            DIGEST_SERVE_TEST_HOLD_SLICE.with(|slice| slice.set(None));
        }
    }
    let _slice = Slice;
    for (slice, cut) in [(Duration::from_secs(3_600), false), (Duration::ZERO, true)] {
        DIGEST_SERVE_TEST_HOLD_SLICE.with(|s| s.set(Some(slice)));
        let (mut got, mut after, mut pages) = (Vec::new(), None::<Vec<u8>>, 0usize);
        loop {
            let deadline = Some(Instant::now() + Duration::from_secs(3_600));
            let response = vals_rt::block_on(
                serve_bucket_digests_inner(&store, &*dev, &gate, BUCKETS, &ids, after.as_deref(), deadline),
                || {},
            );
            pages += 1;
            assert!(pages < 10_000);
            let Response::BucketDigests { digests, more, resume } = response else {
                panic!("expected bucket digests, got {response:?}");
            };
            assert_eq!(resume.is_some(), cut && more, "slice {slice:?}: a page ends early only at the hold slice");
            let next = next_after(&digests, more, resume);
            got.extend(digests);
            match next {
                Some(next) => after = Some(next),
                None => break,
            }
        }
        assert_eq!(got, expected, "slice {slice:?}");
        assert_eq!(pages > 1, cut, "slice {slice:?}: {pages} pages");
    }
}

#[test]
fn served_bucket_digests_exclude_accounted_covered_winners() {
    let engine = Engine::create_prewiped(
        MemBlockDevice::new(256, 262_144),
        Config { seg_blocks: 256, reserved_blocks: 0, ordering: SOrdering::Sorted },
    );
    let mut store = VersionedStore::new(engine, 1).unwrap().with_max_skew_ms(0);
    for run in 0..4u64 {
        for i in (run * 2_000..24_000).filter(|i| (i + run) % 3 != 0) {
            let version = Version { hlc: (run + 1) << 20 | i, node: 1u32.into() };
            store.apply(&bench_key(i), Some(b"value-08"), version).unwrap();
        }
        store.flush().unwrap();
    }
    let rt = Version { hlc: 10 << 20, node: 1u32.into() };
    store.apply_range_delete(Some(b"did:plc:4"), Some(b"did:plc:c"), rt, 0).unwrap();
    store.invalidate_leaf_seed();
    while !store.seed_step().unwrap() {}
    store.apply_range_delete(Some(b"did:plc:d"), Some(b"did:plc:e"), rt, 0).unwrap();
    for i in (0..24_000u64).step_by(97) {
        let version = Version { hlc: 11 << 20 | i, node: 1u32.into() };
        store.apply(&bench_key(i), Some(b"newer"), version).unwrap();
    }
    let (accounted, _) = store.accounted_range_tombstones();
    let ids: Vec<u32> = (0..BUCKETS).step_by(16).collect();
    let want: std::collections::HashSet<u32> = ids.iter().copied().collect();
    let bucket = |key: &[u8]| crate::antientropy::bucket_of(key, BUCKETS);
    let mut expected: Vec<(Vec<u8>, Version)> = store
        .engine()
        .recovered_versions()
        .unwrap()
        .into_iter()
        .filter(|(key, raw)| want.contains(&bucket(key)) && accounted.cover(key).is_none_or(|cover| *raw > cover))
        .map(|(key, raw)| (key.to_vec(), crate::replica::unpack_version(raw)))
        .collect();
    expected.sort();
    assert!(expected.iter().any(|(key, _)| key.starts_with(b"did:plc:d")), "an unaccounted range tombstone filters nothing");
    assert!(expected.iter().any(|(key, _)| key.starts_with(b"did:plc:5")), "newer writes stay");
    let store = RefCell::new(store);
    let dev = store.borrow().device_handle();
    let gate = ReadGate::new();
    for resumable in [true, false] {
        let (mut got, mut after) = (Vec::new(), None::<Vec<u8>>);
        loop {
            let deadline = resumable.then(Instant::now);
            let response = vals_rt::block_on(
                serve_bucket_digests_inner(&store, &*dev, &gate, BUCKETS, &ids, after.as_deref(), deadline),
                || {},
            );
            let Response::BucketDigests { digests, more, resume } = response else {
                panic!("expected bucket digests, got {response:?}");
            };
            let next = next_after(&digests, more, resume);
            got.extend(digests);
            match next {
                Some(next) => after = Some(next),
                None => break,
            }
        }
        assert_eq!(got, expected, "served pages, resumable {resumable}");
    }
}

#[test]
fn tree_and_digest_requests_answer_busy_while_a_range_tombstone_fold_is_pending() {
    let engine = Engine::create_prewiped(
        MemBlockDevice::new(256, 1 << 14),
        Config { seg_blocks: 8, reserved_blocks: 0, ordering: SOrdering::Sorted },
    );
    let mut store = VersionedStore::new(engine, 1).unwrap().with_max_skew_ms(0);
    for i in 0..100u64 {
        store.apply(format!("k{i:03}").as_bytes(), Some(b"v"), Version { hlc: i + 1, node: 1u32.into() }).unwrap();
    }
    store.flush().unwrap();
    while !store.seed_step().unwrap() {}
    let rt = Version { hlc: 1_000, node: 1u32.into() };
    store.apply_range_delete(Some(b"k020"), Some(b"k060"), rt, 0).unwrap();
    let shared = ServerShared::single_zone_on(1, store, Ring::from_members(1, 64, [1]));
    shared.attach_seed_driver(true);
    let store = shared.zone_store(DEFAULT_ZONE).unwrap();
    assert!(store.borrow().rt_fold_pending());
    let ids: Vec<u32> = (0..BUCKETS).collect();
    let scope = std::collections::HashSet::from([0]);
    let sync_tree = || OwnedRequest::SyncTree { zone_id: DEFAULT_ZONE, level: 1, scope_g: 1, scope: vec![0], requester: None };
    let digests = || OwnedRequest::SyncBucketDigests { zone_id: DEFAULT_ZONE, num_buckets: BUCKETS, ids: ids.clone(), after: None };
    let dev = store.borrow().device_handle();
    let gate = ReadGate::new();
    let served = |store: &RefCell<VersionedStore<MemBlockDevice>>| {
        (
            serve_request_mode(&shared, sync_tree(), crate::replica::unconditional_apply_mode()),
            serve_request_mode(&shared, digests(), crate::replica::unconditional_apply_mode()),
            vals_rt::block_on(serve_sync_tree(store, 1, 1, &scope, None), || {}),
            vals_rt::block_on(serve_bucket_digests_inner(store, &*dev, &gate, BUCKETS, &ids, None, None), || {}),
        )
    };
    let (tree, page, coop_tree, coop_page) = served(&store);
    for response in [tree, page, coop_tree, coop_page] {
        assert!(matches!(response, Response::Busy), "{response:?}");
    }
    while vals_rt::block_on(crate::replica::rt_fold_turn_with(&store, Some(&gate), || {}), || {}).unwrap()
        != crate::replica::RecountStep::Done
    {}
    assert!(!store.borrow().rt_fold_pending());
    let (tree, page, coop_tree, coop_page) = served(&store);
    assert!(matches!(tree, Response::Tree(_)) && matches!(coop_tree, Response::Tree(_)), "{tree:?} {coop_tree:?}");
    for response in [page, coop_page] {
        let Response::BucketDigests { digests, more: false, .. } = response else { panic!("{response:?}") };
        assert_eq!(digests.len(), 60, "the folded range tombstone hides 40 winners");
    }

    let held = vals_rt::block_on(gate.acquire(), || {});
    let mut waiting = Box::pin(serve_bucket_digests_inner(&store, &*dev, &gate, BUCKETS, &ids, None, None));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(waiting.as_mut().poll(&mut cx).is_pending(), "test premise: the serve waits for the read gate");
    let rt = Version { hlc: 2_000, node: 1u32.into() };
    store.borrow_mut().apply_range_delete(Some(b"k070"), Some(b"k080"), rt, 0).unwrap();
    drop(held);
    let response = loop {
        if let std::task::Poll::Ready(response) = waiting.as_mut().poll(&mut cx) {
            break response;
        }
    };
    assert!(matches!(response, Response::Busy), "a range tombstone applied during the gate wait: {response:?}");
}

#[test]
#[ignore]
fn bucket_digest_serve_duration_measurement() {
    let keys = env_u64("VALS_DIGEST_BENCH_KEYS", 2_000_000);
    let runs = env_u64("VALS_DIGEST_BENCH_RUNS", 50).max(1);
    let dir = std::env::var_os("VALS_DIGEST_BENCH_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let path = dir.join(format!("vals-digest-bench-{}.img", std::process::id()));
    let blocks = (keys * 512 / 4096).max(1 << 16);
    let dev = FileBlockDevice::create(&path, 4096, blocks).unwrap();
    let engine = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 256,
            reserved_blocks: 0,
            ordering: SOrdering::Unsorted,
        },
    );
    let mut store = VersionedStore::new(engine, 1).unwrap();
    let per_run = keys.div_ceil(runs);
    let started = Instant::now();
    for i in 0..keys {
        let version = Version { hlc: (i + 1) << 16, node: 1u32.into() };
        store.apply(&bench_key(i), Some(b"value-08"), version).unwrap();
        if (i + 1) % per_run == 0 {
            store.flush().unwrap();
        }
    }
    store.flush().unwrap();
    eprintln!(
        "DIGEST411 built keys={keys} runs={} ingest_s={:.3}",
        store.run_count(),
        started.elapsed().as_secs_f64()
    );
    let store = RefCell::new(store);
    let dev = store.borrow().device_handle();
    let gate = ReadGate::new();
    for want in [1u32, 16, BUCKETS] {
        let ids: Vec<u32> = (0..want).map(|i| i * (BUCKETS / want)).collect();
        let mut after: Option<Vec<u8>> = None;
        let (mut requests, mut entries, mut longest) = (0u64, 0u64, Duration::ZERO);
        let total = Instant::now();
        loop {
            let one = Instant::now();
            let response = vals_rt::block_on(
                serve_bucket_digests(&store, &*dev, &gate, BUCKETS, &ids, after.as_deref(), true),
                || {},
            );
            longest = longest.max(one.elapsed());
            requests += 1;
            let Response::BucketDigests { digests, more, resume } = response else {
                panic!("expected bucket digests, got {response:?}");
            };
            entries += digests.len() as u64;
            match next_after(&digests, more, resume) {
                Some(next) => after = Some(next),
                None => break,
            }
        }
        let seconds = total.elapsed().as_secs_f64();
        eprintln!(
            "DIGEST411 want_buckets={want} requests={requests} digests={entries} total_s={seconds:.3} longest_request_s={:.3} engine_keys_per_total_s={:.0}",
            longest.as_secs_f64(),
            keys as f64 / seconds
        );
    }
    drop(store);
    let _ = std::fs::remove_file(&path);
}

fn rpc(addr: SocketAddr, request: &wire::Request) -> Response {
    use std::io::{Read, Write};
    let payload = wire::encode_request_payload(request);
    let mut frame = ((8 + payload.len()) as u32).to_le_bytes().to_vec();
    frame.extend_from_slice(&1u64.to_le_bytes());
    frame.extend_from_slice(&payload);
    let mut stream = std::net::TcpStream::connect(addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    crate::proto::client_handshake_blocking(&mut stream).unwrap();
    stream.write_all(&frame).unwrap();
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).expect("the node answers instead of dying");
    let mut block = vec![0u8; u32::from_le_bytes(len) as usize];
    stream.read_exact(&mut block).unwrap();
    wire::decode_response(&block[8..]).unwrap()
}

#[test]
fn bad_tree_and_bucket_geometry_is_refused_without_taking_the_node_down() {
    let engine = Engine::create_prewiped(
        MemBlockDevice::new(256, 4096),
        Config { seg_blocks: 8, reserved_blocks: 0, ordering: SOrdering::Unsorted },
    );
    let mut store = VersionedStore::new(engine, 1).unwrap();
    for i in 0..50u64 {
        store.apply(format!("k{i:03}").as_bytes(), Some(b"v"), Version { hlc: i + 1, node: 1u32.into() }).unwrap();
    }
    let (addr, _server) = ReplicaServer::spawn_with_handle(store).unwrap();
    let tree = |level, scope_g, scope: &[u32]| {
        rpc(addr, &wire::Request::SyncTree { zone_id: DEFAULT_ZONE, level, scope_g, scope, requester: None })
    };
    let digests = |num_buckets, ids: &[u32]| {
        rpc(addr, &wire::Request::SyncBucketDigests { zone_id: DEFAULT_ZONE, num_buckets, ids, after: None })
    };
    let ids: Vec<u32> = (0..BUCKETS).collect();
    for (level, scope_g, scope) in [
        (16, 0, &[0][..]),
        (0, 1, &[0][..]),
        (u32::MAX, 1, &[0][..]),
        (1 << 20, 1, &[0][..]),
        (48, 16, &[0][..]),
        (16, 32, &[0][..]),
        (16, 3, &[0][..]),
        (16, 1, &[1][..]),
        (256, 16, &[3, 16][..]),
    ] {
        let reply = tree(level, scope_g, scope);
        assert!(
            matches!(&reply, Response::Err(why) if why.starts_with("bad tree geometry")),
            "level {level} scope_g {scope_g} scope {scope:?}: {reply:?}"
        );
    }
    let reply = digests(0, &ids);
    assert!(matches!(&reply, Response::Err(why) if why.starts_with("bad bucket geometry")), "{reply:?}");

    let deadline = Instant::now() + Duration::from_secs(10);
    let tree_reply = loop {
        match tree(16, 1, &[0]) {
            Response::Busy if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(20)),
            reply => break reply,
        }
    };
    assert!(matches!(&tree_reply, Response::Tree(nodes) if nodes.len() == 16), "{tree_reply:?}");
    match digests(BUCKETS, &ids) {
        Response::BucketDigests { digests, .. } => assert_eq!(digests.len(), 50),
        other => panic!("valid geometry is still served: {other:?}"),
    }
}

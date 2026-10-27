use super::Node;
use crate::coord::{Coordinator, WriteConcern};
use crate::hlc::{physical_now_ms, Version};
use crate::net::NetTransport;
use crate::partition::RangeMap;
use crate::replica::{Applied, Stored, VersionedStore};
use crate::ring::{NodeId, Ring};
use crate::wire::{self, Request, Response};
use crate::zone::{Catalog, Ordering, ZoneConfig, ZoneId, DEFAULT_ZONE};
use crate::zonestore::{FileZoneStore, ZoneGeom, ZoneStore};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use vals_spdk::FileBlockDevice;

type Census = BTreeMap<Vec<u8>, Stored>;
const KEYS: usize = 1024;
const SORTED_ZONE: ZoneId = 7;

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!("vals-0330-{}-{nonce}", std::process::id()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn provider(&self, node: NodeId) -> FileZoneStore {
        FileZoneStore::new(node, self.0.join(format!("node-{node}")), ZoneGeom {
            block_size: 256, seg_blocks: 64, reserved_blocks: 0, region_blocks: 65536,
        }).unwrap()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn rpc(addr: SocketAddr, request: &Request<'_>) -> Response {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
    crate::proto::client_handshake_blocking(&mut stream).unwrap();
    let payload = wire::encode_request_payload(request);
    let mut frame = ((8 + payload.len()) as u32).to_le_bytes().to_vec();
    frame.extend_from_slice(&1u64.to_le_bytes());
    frame.extend_from_slice(&payload);
    stream.write_all(&frame).unwrap();
    let mut len = [0; 4];
    stream.read_exact(&mut len).unwrap();
    let mut block = vec![0; u32::from_le_bytes(len) as usize];
    stream.read_exact(&mut block).unwrap();
    wire::decode_response(&block[8..]).unwrap()
}

fn push_members(peers: &[(NodeId, SocketAddr)], epoch: u64) {
    let members: Vec<_> = peers.iter().map(|(n, _)| (*n, 1)).collect();
    let addrs: Vec<_> = peers.iter().map(|(n, a)| (*n, vec![*a])).collect();
    for (_, addr) in peers {
        let reply = rpc(*addr, &Request::SetMembers {
            auth: &[], epoch, vnodes: 64, rf: 2, members: &members, addrs: &addrs,
            incarnation: None,
        });
        assert!(!matches!(reply, Response::Err(_)), "membership push: {reply:?}");
    }
}

fn push_map(peers: &[(NodeId, SocketAddr)], epoch: u64, map: &RangeMap) {
    let maps = [wire::WireRangeMap {
        zone_id: SORTED_ZONE, splits: map.splits(), tokens: map.tokens(),
        pending: map.pending(), placement_debt: map.placement_debt(),
        opaque_tokens: map.supports_opaque_tokens(),
    }];
    for (_, addr) in peers {
        let reply = rpc(*addr, &Request::SetRangeMaps { auth: &[], epoch, maps: &maps, incarnation: None });
        assert!(!matches!(reply, Response::Err(_)), "range-map push: {reply:?}");
    }
}

fn tree(addr: SocketAddr, zone_id: ZoneId, counterpart: NodeId) -> Response {
    rpc(addr, &Request::SyncTree {
        zone_id, level: crate::antientropy::DEFAULT_BUCKETS,
        scope_g: 1, scope: &[0], requester: Some(counterpart),
    })
}

fn wait_equal(peers: &[(NodeId, SocketAddr)], zone_id: ZoneId, pairs: &[(NodeId, NodeId)]) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut ready = true;
        let mut last = Vec::new();
        for &(a, b) in pairs {
            let left = tree(peers[(a - 1) as usize].1, zone_id, b);
            let right = tree(peers[(b - 1) as usize].1, zone_id, a);
            let equal = match (&left, &right) {
                (Response::Tree(l), Response::Tree(r)) => {
                    l.len() == crate::antientropy::DEFAULT_BUCKETS as usize
                        && l == r && l.iter().any(|(_, digest)| *digest != 0)
                }
                _ => false,
            };
            ready &= equal;
            last.push((a, b, equal, format!("{left:?}"), format!("{right:?}")));
        }
        if ready { return; }
        assert!(Instant::now() < deadline,
            "stream AE did not seed and converge in 30s for zone {zone_id}: {last:?}");
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn value(i: usize, round: usize) -> Vec<u8> {
    (0..(257 + i % 769)).map(|offset| {
        ((i * 31 + round * 73 + offset * 17) % 256) as u8
    }).collect()
}

fn corpus(keys: &[Vec<u8>], base_ms: u64) -> Vec<Census> {
    (0..3).map(|round| keys.iter().enumerate().map(|(i, key)| {
        (key.clone(), Stored {
            version: Version { hlc: (base_ms + round as u64 * 1000 + i as u64) << 16, node: 99 },
            value: if round == 2 && i % 11 == 0 { None } else { Some(value(i, round)) },
        })
    }).collect()).collect()
}

fn populate(store: &mut VersionedStore<FileBlockDevice>, rounds: &[Census], keep: impl Fn(&[u8]) -> bool) {
    for (round, entries) in rounds.iter().enumerate() {
        for (key, stored) in entries {
            if keep(key) {
                assert_eq!(store.apply(key, stored.value.as_deref(), stored.version).unwrap(), Applied::Won);
            }
        }
        store.flush().unwrap();
        if round == 1 {
            assert!(store.engine().run_count() >= 2, "oracle must exercise flushed overlapping runs");
            let stats = store.compact_runs(physical_now_ms()).unwrap();
            assert!(stats.runs_merged >= 2);
            assert_eq!(store.engine().run_count(), 1, "oracle must exercise a merge");
        }
    }
    store.sync().unwrap();
    assert!(store.engine().run_count() >= 2, "oracle source must remain run-backed");
}

fn census(scratch: &Scratch, node: NodeId, zone_id: ZoneId, ordering: Ordering) -> Census {
    let store = scratch.provider(node).open(zone_id, ordering).unwrap();
    let versions = store.engine().recovered_versions().unwrap();
    let mut result = Census::new();
    for (key, raw_version) in versions {
        let stored = store.get_versioned(&key).unwrap().expect("local engine winner must be readable");
        assert_eq!(stored.version, crate::replica::unpack_version(raw_version));
        assert!(result.insert(key.to_vec(), stored).is_none(), "duplicate local winner");
    }
    result
}

fn assert_census(actual: &Census, expected: &Census, label: &str) {
    let actual_keys: BTreeSet<_> = actual.keys().collect();
    let expected_keys: BTreeSet<_> = expected.keys().collect();
    assert_eq!(actual_keys, expected_keys, "{label}: missing or extra local keys");
    for (key, expected) in expected {
        let actual = &actual[key];
        assert_eq!(actual.version, expected.version, "{label}: stale version at {key:?}");
        assert_eq!(actual.value, expected.value, "{label}: value bytes/tombstone at {key:?}");
    }
}

fn assert_stream_seed_exercised() {
    let counters = crate::observe::snapshot();
    assert!(counters.ae_seed_fold_ops > 0, "oracle must exercise AE seed folding");
}

fn start(
    scratch: &Scratch, node: NodeId, listener: TcpListener, peers: &[(NodeId, SocketAddr)],
    ring: &Ring, sorted: bool,
) -> crate::testutil::Running {
    let provider = scratch.provider(node);
    let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
    let mut catalog = Catalog::new();
    if sorted {
        catalog.insert(SORTED_ZONE, ZoneConfig {
            name: "seed-oracle".into(), rf: 2, ordering: Ordering::Sorted, scope: None,
        });
    }
    crate::testutil::running(Node::serve_zoned_with_settings(
        node, listener, zone0, provider, catalog, ring.clone(), peers.to_vec(),
        Duration::from_millis(25), None,
        crate::node_settings::NodeSettings { rehome_copy_page_entries: 64, ..Default::default() },
    ))
}

#[test]
fn stream_seed_rebuilt_replica_exact_value_census() {
    let scratch = Scratch::new();
    let ring = Ring::from_members(2, 64, [1, 2, 3]);
    let keys: Vec<_> = (0..KEYS).map(|i| format!("rebuild-{i:05}").into_bytes()).collect();
    let rounds = corpus(&keys, physical_now_ms() - 10_000);
    let model = rounds.last().unwrap();
    let groups: BTreeSet<_> = keys.iter().map(|k| {
        let mut owners = ring.replicas(k);
        owners.sort_unstable();
        owners
    }).collect();
    assert_eq!(groups.len(), 3, "fixture must cover all RF2 placement groups");
    for node in 1..=3 {
        let mut store = scratch.provider(node).open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        populate(&mut store, &rounds, |key| ring.replicas(key).contains(&node));
    }
    let binds: Vec<_> = (0..3).map(|_| Node::bind().unwrap()).collect();
    let mut peers: Vec<_> = binds.iter().enumerate().map(|(i, (_, a))| ((i + 1) as NodeId, *a)).collect();
    let mut nodes: Vec<_> = binds.into_iter().enumerate().map(|(i, (listener, _))| {
        Some(start(&scratch, (i + 1) as NodeId, listener, &peers, &ring, false))
    }).collect();
    push_members(&peers, 1);
    wait_equal(&peers, DEFAULT_ZONE, &[(1, 2), (1, 3), (2, 3)]);
    nodes[2].take().unwrap().shutdown();
    assert!(!census(&scratch, 3, DEFAULT_ZONE, Ordering::Unsorted).is_empty());
    std::fs::remove_dir_all(scratch.0.join("node-3")).unwrap();
    assert!(census(&scratch, 3, DEFAULT_ZONE, Ordering::Unsorted).is_empty(), "replacement must be empty");
    let (listener, addr) = Node::bind().unwrap();
    peers[2].1 = addr;
    nodes[2] = Some(start(&scratch, 3, listener, &peers, &ring, false));
    push_members(&peers, 2);
    wait_equal(&peers, DEFAULT_ZONE, &[(1, 3), (2, 3)]);
    for node in nodes { node.unwrap().shutdown(); }
    let rebuilt = census(&scratch, 3, DEFAULT_ZONE, Ordering::Unsorted);
    let expected: Census = model.iter().filter(|(key, _)| ring.replicas(key).contains(&3))
        .map(|(k, v)| (k.clone(), v.clone())).collect();
    assert!(expected.values().any(|s| s.value.is_none()));
    assert_census(&rebuilt, &expected, "rebuilt replica vs model");
    for peer in [1, 2] {
        let survivor = census(&scratch, peer, DEFAULT_ZONE, Ordering::Unsorted);
        let shared = |entries: &Census| -> Census {
            entries.iter().filter(|(key, _)| {
                let owners = ring.replicas(key);
                owners.contains(&3) && owners.contains(&peer)
            }).map(|(k, v)| (k.clone(), v.clone())).collect()
        };
        assert_census(&shared(&rebuilt), &shared(&survivor), "rebuilt vs surviving local peer");
    }
    assert_stream_seed_exercised();
}

#[test]
fn stream_seed_populated_sorted_rehome_exact_value_census() {
    let scratch = Scratch::new();
    let ring = Ring::from_members(2, 64, [1, 2, 3]);
    let current = ring.replicas_in_zone(SORTED_ZONE, b"", 2, None);
    let mut boundary = b"m".to_vec();
    while ring.replicas_in_zone(SORTED_ZONE, &boundary, 2, None).iter().all(|n| current.contains(n)) {
        boundary.push(b'x');
        assert!(boundary.len() < 64, "could not find a moving canonical placement");
    }
    let next = ring.replicas_in_zone(SORTED_ZONE, &boundary, 2, None);
    let target = *next.iter().find(|n| !current.contains(n)).unwrap();
    let retained = *next.iter().find(|n| current.contains(n)).unwrap();
    let keys: Vec<_> = (0..KEYS).map(|i| {
        [boundary.as_slice(), format!("/{i:05}").as_bytes()].concat()
    }).collect();
    let rounds = corpus(&keys, physical_now_ms() - 10_000);
    let mut model = rounds.last().unwrap().clone();
    for &node in &current {
        let mut store = scratch.provider(node).open(SORTED_ZONE, Ordering::Sorted).unwrap();
        populate(&mut store, &rounds, |_| true);
    }
    assert!(census(&scratch, target, SORTED_ZONE, Ordering::Sorted).is_empty(),
        "new owner must not already hold the populated range");
    let binds: Vec<_> = (0..3).map(|_| Node::bind().unwrap()).collect();
    let peers: Vec<_> = binds.iter().enumerate().map(|(i, (_, a))| ((i + 1) as NodeId, *a)).collect();
    let nodes: Vec<_> = binds.into_iter().enumerate().map(|(i, (listener, _))| {
        start(&scratch, (i + 1) as NodeId, listener, &peers, &ring, true)
    }).collect();
    push_members(&peers, 1);
    wait_equal(&peers, SORTED_ZONE, &[(current[0], current[1])]);
    let mut prepared = RangeMap::single();
    assert!(prepared.split(&boundary));
    assert!(prepared.set_pending(1, boundary.clone()));
    push_map(&peers, 10, &prepared);
    let coordinator = Coordinator::new(100, ring.clone(), 2, NetTransport::connect(&peers));
    vals_rt::block_on(coordinator.refresh_catalog(1), crate::reactor::poll_io).unwrap();
    coordinator.set_range_map(SORTED_ZONE, prepared.clone());
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let stats = match rpc(peers[(target - 1) as usize].1, &Request::RangeStats) {
            Response::RangeStats { ranges, .. } => ranges,
            other => panic!("expected rehome stats: {other:?}"),
        };
        if stats.iter().any(|s| s.zone_id == SORTED_ZONE && s.lower_bound == boundary
            && s.rehome_copy_progress > 0 && s.rehome_copy_complete_epoch < 10) { break; }
        assert!(Instant::now() < deadline, "populated rehome never started: {stats:?}");
        std::thread::sleep(Duration::from_millis(10));
    }
    for &i in &[0, 17, KEYS - 1] {
        let bytes = value(i, 4);
        let version = vals_rt::block_on(coordinator.write_in(
            SORTED_ZONE, &keys[i], &bytes, WriteConcern::All,
        ), crate::reactor::poll_io).unwrap();
        model.insert(keys[i].clone(), Stored { version, value: Some(bytes) });
    }
    loop {
        let stats = match rpc(peers[(target - 1) as usize].1, &Request::RangeStats) {
            Response::RangeStats { ranges, .. } => ranges,
            other => panic!("expected rehome stats: {other:?}"),
        };
        if let Some(stat) = stats.iter().find(|s| s.zone_id == SORTED_ZONE
            && s.lower_bound == boundary && s.rehome_copy_complete_epoch >= 10)
        {
            assert!(stat.rehome_copy_progress >= KEYS as u64, "copy skipped populated records");
            break;
        }
        assert!(Instant::now() < deadline, "populated rehome did not complete: {stats:?}");
        std::thread::sleep(Duration::from_millis(20));
    }
    let mut committed = prepared;
    assert!(committed.commit_pending(1));
    assert!(committed.replicas_of_range(&ring, SORTED_ZONE, 1, 2, None).contains(&target));
    push_map(&peers, 11, &committed);
    coordinator.set_range_map(SORTED_ZONE, committed);
    let bytes = value(5, 5);
    let version = vals_rt::block_on(coordinator.write_in(
        SORTED_ZONE, &keys[5], &bytes, WriteConcern::All,
    ), crate::reactor::poll_io).unwrap();
    model.insert(keys[5].clone(), Stored { version, value: Some(bytes) });
    wait_equal(&peers, SORTED_ZONE, &[(target, retained)]);
    for node in nodes { node.shutdown(); }
    let moved = census(&scratch, target, SORTED_ZONE, Ordering::Sorted);
    let survivor = census(&scratch, retained, SORTED_ZONE, Ordering::Sorted);
    assert_census(&moved, &model, "new populated-range owner vs model");
    assert_census(&moved, &survivor, "new owner vs retained source local engine");
    assert_stream_seed_exercised();
}

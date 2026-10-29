use super::*;
use crate::backup::{capture_from_inspection, create_backup, open_backup, open_records, NetCluster, StaleCopies};
use crate::hlc::Version;
use crate::liveness::MemLivenessLog;
use crate::pd::{PdDaemon, RingConfig};
use crate::wire::{self, Request, Response};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Instant;
use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine, Ordering as SOrdering};

fn store(node: u32) -> VersionedStore<MemBlockDevice> {
    let cfg = Config { seg_blocks: 64, reserved_blocks: 0, ordering: SOrdering::Unsorted };
    VersionedStore::new(Engine::create_prewiped(MemBlockDevice::new(512, 65_536), cfg), node).unwrap()
}

fn at(ms: u64, node: u32) -> Version {
    Version { hlc: ms << crate::hlc::COUNTER_BITS, node: node.into() }
}

fn rpc(addr: SocketAddr, req: &Request) -> Response {
    let mut s = TcpStream::connect(addr).unwrap();
    crate::proto::client_handshake_blocking(&mut s).unwrap();
    let payload = wire::encode_request_payload(req);
    let mut frame = ((8 + payload.len()) as u32).to_le_bytes().to_vec();
    frame.extend_from_slice(&1u64.to_le_bytes());
    frame.extend_from_slice(&payload);
    s.write_all(&frame).unwrap();
    let mut lenb = [0u8; 4];
    s.read_exact(&mut lenb).unwrap();
    let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
    s.read_exact(&mut block).unwrap();
    wire::decode_response(&block[8..]).unwrap()
}

fn held(addr: SocketAddr, key: &[u8]) -> Option<Vec<u8>> {
    match rpc(addr, &Request::Read { epoch: u64::MAX, zone_id: DEFAULT_ZONE, key }) {
        Response::Read(stored) => stored.and_then(|s| s.value),
        _ => None,
    }
}

fn served(addr: SocketAddr, key: &[u8]) -> Option<Vec<u8>> {
    match rpc(addr, &Request::Read { epoch: u64::MAX, zone_id: DEFAULT_ZONE, key }) {
        Response::Read(stored) => stored.and_then(|s| s.value),
        other => panic!("{addr} did not serve the read: {other:?}"),
    }
}

fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(Instant::now() < deadline, "timed out: {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn a_former_owner_copy_older_than_the_gc_floor_never_reaches_the_owners_or_a_backup() {
    let ring = Ring::from_members(2, 64, [1, 2, 3]);
    let now = physical_now_ms();
    let grace = 60_000u64;
    let stray = |prefix: &str| {
        (0..10_000)
            .map(|i| format!("{prefix}-{i}").into_bytes())
            .find(|k| !ring.replicas(k).contains(&3))
            .unwrap()
    };
    let (zombie, fresh, aged) = (stray("zombie"), stray("fresh"), stray("aged"));
    let mut stores: BTreeMap<NodeId, VersionedStore<MemBlockDevice>> = (1..=3).map(|n| (n, store(n))).collect();
    stores.get_mut(&3).unwrap().apply(&zombie, Some(b"zombie"), at(now - 600_000, 9)).unwrap();
    for i in 0..60 {
        let key = format!("anchor-{i}").into_bytes();
        for owner in ring.replicas(&key) {
            stores.get_mut(&owner).unwrap().apply(&key, Some(b"a"), at(now - 1_000, 9)).unwrap();
        }
    }
    for owner in ring.replicas(&aged) {
        stores.get_mut(&owner).unwrap().apply(&aged, Some(b"aged"), at(now - 600_000, 9)).unwrap();
    }
    stores.get_mut(&3).unwrap().apply(&fresh, Some(b"fresh"), at(now - 1_000, 9)).unwrap();

    let bound: Vec<(NodeId, std::net::TcpListener, SocketAddr)> = (1..=3)
        .map(|n| {
            let (l, a) = Node::bind().unwrap();
            (n, l, a)
        })
        .collect();
    let peers: Vec<(NodeId, SocketAddr)> = bound.iter().map(|(n, _, a)| (*n, *a)).collect();
    let addr = |n: NodeId| peers.iter().find(|(p, _)| *p == n).unwrap().1;
    let mut nodes = Vec::new();
    for (n, listener, _) in bound {
        let log = MemLivenessLog::new();
        log.store(now - 1_000).unwrap();
        nodes.push(crate::testutil::running(Node::serve_with_liveness(
            n,
            listener,
            stores.remove(&n).unwrap(),
            ring.clone(),
            peers.clone(),
            Duration::from_millis(25),
            grace,
            log,
        )));
    }

    let owners = ring.replicas(&zombie);
    wait_for("the young former-owner copy is handed to its owners", || {
        ring.replicas(&fresh).iter().all(|&o| held(addr(o), &fresh).as_deref() == Some(&b"fresh"[..]))
    });
    std::thread::sleep(Duration::from_millis(300));
    for &o in &owners {
        assert_eq!(served(addr(o), &zombie), None, "owner {o} admitted the stale former-owner copy");
    }
    assert_eq!(
        held(addr(3), &zombie).as_deref(),
        Some(&b"zombie"[..]),
        "the former owner keeps a copy no owner confirmed"
    );

    let pd = crate::testutil::running_pd(
        PdDaemon::spawn_single(
            1,
            Engine::create_prewiped(MemBlockDevice::new(4096, 4096), Config::default()),
            RingConfig { vnodes: 64, rf: 2 },
            peers.iter().map(|&(n, a)| (n, vec![a])).collect(),
        )
        .unwrap(),
    );
    pd.bootstrap_members(peers.iter().map(|&(n, a)| (n, 1, vec![a])).collect());
    wait_for("the PD mints an incarnation", || {
        pd.capture_state()
            .and_then(|view| capture_from_inspection(view).ok())
            .is_some_and(|c| c.view().incarnation.is_some() && c.view().members.len() == 3)
    });
    let dir = std::env::temp_dir().join(format!("vals-former-owner-backup-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut cluster = NetCluster::new(|| {
        capture_from_inspection(pd.capture_state().ok_or_else(|| "no PD inspection".to_string())?)
    });
    let mut backup = |name: &str, stale: StaleCopies| {
        let out = dir.join(name);
        let mut lines = Vec::new();
        let made = create_backup(&out, &mut cluster, "test", stale, &mut |line| lines.push(line.to_string()));
        made.map(|manifest| {
            let mut got = BTreeMap::new();
            let mut reader = open_records(&out.join(&manifest.zones[0].file.name)).unwrap();
            while let Some(record) = reader.next_record().unwrap() {
                got.insert(record.key, record.value);
            }
            assert!(open_backup(&out).is_ok());
            (got, lines)
        })
    };
    let refused = backup("default", StaleCopies::Fail).unwrap_err();
    assert!(refused.contains("1 keys have a put held only by nodes that are not current owners"), "{refused}");
    assert!(refused.contains("node 3 shard 0"), "{refused}");
    assert!(!dir.join("default").exists());

    let (got, lines) = backup("omit", StaleCopies::Omit).unwrap();
    assert!(!got.contains_key(&zombie), "the backup resurrected a copy the owners refuse: {lines:?}");
    assert_eq!(got.get(&fresh), Some(&Some(b"fresh".to_vec())));
    assert_eq!(got.get(&aged), Some(&Some(b"aged".to_vec())), "old data the owners hold is kept");
    assert_eq!(got.len(), 62, "{lines:?}");
    assert!(lines.iter().any(|l| l.contains("omitted 1 puts held only by non-owners")), "{lines:?}");

    let (got, _) = backup("keep", StaleCopies::Keep).unwrap();
    assert_eq!(got.get(&zombie), Some(&Some(b"zombie".to_vec())), "keep backs up every replica's copy");
    drop(nodes);
    let _ = std::fs::remove_dir_all(&dir);
}

fn gate(
    peers: &[(NodeId, SocketAddr)],
    change: crate::pd::MembershipChange,
    incoming: &[SocketAddr],
) -> Result<String, String> {
    let members: crate::pd::AddressedMembers = peers.iter().map(|&(n, a)| (n, 1, vec![a])).collect();
    crate::pd::check_membership_change(&members, &Default::default(), change, incoming)
}

fn set_members(peers: &[(NodeId, SocketAddr)], to: &[(NodeId, SocketAddr)], epoch: u64) {
    set_members_rf(peers, to, epoch, 1);
}

fn set_members_rf(peers: &[(NodeId, SocketAddr)], to: &[(NodeId, SocketAddr)], epoch: u64, rf: u32) {
    let members: Vec<(NodeId, u32)> = to.iter().map(|&(n, _)| (n, 1)).collect();
    let addrs: Vec<(NodeId, Vec<SocketAddr>)> = to.iter().map(|&(n, a)| (n, vec![a])).collect();
    for &(_, a) in peers {
        let reply = rpc(a, &Request::SetMembers {
            auth: &[],
            epoch,
            vnodes: 64,
            rf,
            members: &members,
            addrs: &addrs,
            incarnation: None,
        });
        assert!(matches!(reply, Response::Members { epoch: e, .. } if e == epoch), "{reply:?}");
    }
}

fn exported(peers: &[(NodeId, SocketAddr)], key: &[u8]) -> (Option<Option<Vec<u8>>>, u64) {
    let cfg = crate::zone::ZoneConfig::default_config(1);
    exported_zone(peers, DEFAULT_ZONE, &cfg, std::collections::HashMap::new(), key)
}

fn exported_zone(
    peers: &[(NodeId, SocketAddr)],
    zone_id: ZoneId,
    cfg: &crate::zone::ZoneConfig,
    range_maps: std::collections::HashMap<ZoneId, crate::partition::RangeMap>,
    key: &[u8],
) -> (Option<Option<Vec<u8>>>, u64) {
    use crate::backup::{merge_replicas_into, NetReplicaSource, ReplicaSource, ZonePlacement};
    let view = crate::pd::PdStateView {
        incarnation: Some(1),
        lineage: None,
        members: peers.iter().map(|&(n, a)| (n, 1, vec![a])).collect(),
        zones: crate::zone::Catalog::new(),
        range_maps,
    };
    let place = ZonePlacement::new(&view, (64, 1), zone_id, cfg);
    let sources: Vec<Box<dyn ReplicaSource>> = peers
        .iter()
        .map(|&(n, a)| Box::new(NetReplicaSource::new(n, 0, a, zone_id)) as Box<dyn ReplicaSource>)
        .collect();
    let mut found = None;
    let stats = merge_replicas_into(sources, Some(&place), &mut |r| {
        if r.key == key {
            found = Some(r.value.clone());
        }
        Ok(())
    })
    .unwrap();
    (found, stats.stale_copies_omitted)
}

fn collected_owner(
    node: u32,
    key: &[u8],
    now: u64,
    grace: u64,
    plant: &[(&[u8], &[u8], u64)],
) -> VersionedStore<MemBlockDevice> {
    let mut owner = store(node).with_gc_grace_ms(grace);
    for &(k, v, ms) in plant {
        owner.apply_at(k, Some(v), at(ms, 9), ms).unwrap();
    }
    owner.apply_at(key, None, at(now - 500_000, 9), now - 500_000).unwrap();
    owner.apply_at(b"anchor", Some(b"a"), at(now, 9), now).unwrap();
    owner.flush().unwrap();
    owner.advance_durable_gc_watermark(now);
    let plan = owner.compact_plan_tombstone_gc(now).unwrap().unwrap();
    let written = vals_rt::block_on(
        vals_store::compact_and_write(plan, Arc::new(owner.device_sibling().unwrap())),
        || {},
    )
    .unwrap();
    let collected = owner.compact_commit(written).unwrap();
    assert!(collected.current_dropped_tombstones.iter().any(|(k, _)| k == key), "the delete was collected");
    assert!(owner.get_versioned(key).unwrap().is_none());
    owner
}

fn serve_all(
    stores: Vec<VersionedStore<MemBlockDevice>>,
    ring: &Ring,
    now: u64,
    grace: u64,
    fresh: &[NodeId],
) -> (Vec<(NodeId, SocketAddr)>, Vec<crate::testutil::Running>) {
    let bound: Vec<(NodeId, std::net::TcpListener, SocketAddr)> = (1..=stores.len() as NodeId)
        .map(|n| {
            let (l, a) = Node::bind().unwrap();
            (n, l, a)
        })
        .collect();
    let peers: Vec<(NodeId, SocketAddr)> = bound.iter().map(|(n, _, a)| (*n, *a)).collect();
    let mut nodes = Vec::new();
    for ((n, listener, _), store) in bound.into_iter().zip(stores) {
        let log = MemLivenessLog::new();
        if !fresh.contains(&n) {
            log.store(now).unwrap();
        }
        nodes.push(crate::testutil::running(Node::serve_with_liveness(
            n,
            listener,
            store,
            ring.clone(),
            peers.clone(),
            Duration::from_millis(25),
            grace,
            log,
        )));
    }
    (peers, nodes)
}

fn owned_by(ring: &Ring, prefix: &str, want: &[NodeId], after: Option<(&Ring, &[NodeId])>) -> Vec<u8> {
    let sorted = |ring: &Ring, k: &[u8]| {
        let mut owners = ring.replicas(k);
        owners.sort_unstable();
        owners
    };
    (0..100_000)
        .map(|i| format!("{prefix}-{i}").into_bytes())
        .find(|k| sorted(ring, k) == want && after.is_none_or(|(ring, want)| sorted(ring, k) == want))
        .unwrap()
}

#[test]
fn a_forced_removal_that_makes_a_stale_former_owner_an_owner_again_keeps_the_key_deleted() {
    use crate::pd::MembershipChange::Remove;
    let before = Ring::from_members(2, 64, [1, 2, 3]);
    let zombie = owned_by(&before, "zombie", &[1, 2], None);
    let aged = owned_by(&before, "aged", &[1, 2], None);
    let mine = owned_by(&before, "mine", &[1, 3], None);
    let now = physical_now_ms();
    let old = now - 600_000;
    let grace = 60_000u64;
    let one = collected_owner(1, &zombie, now, grace, &[(&aged, b"aged", old), (&mine, b"mine", old)]);
    let two = collected_owner(2, &zombie, now, grace, &[(&aged, b"aged", old)]);
    let mut holder = store(3);
    holder.apply(&zombie, Some(b"deleted-value"), at(old, 9)).unwrap();
    holder.apply(&mine, Some(b"mine"), at(old, 9)).unwrap();
    holder.apply(b"anchor", Some(b"a"), at(now, 9)).unwrap();
    let (peers, nodes) = serve_all(vec![one, two, holder], &before, now, grace, &[]);

    let mut refusal = String::new();
    wait_for("node 3 reports the copy its owners refuse", || {
        refusal = gate(&peers, Remove(1), &[]).err().unwrap_or_default();
        refusal.contains("node 3 shard 0") && refusal.contains("kept 1 copies")
    });
    assert_eq!(served(peers[0].1, &zombie), None);
    assert_eq!(served(peers[1].1, &zombie), None);

    let remaining = peers[1..].to_vec();
    set_members_rf(&peers, &remaining, 10, 2);
    assert_eq!(served(peers[2].1, &zombie), None, "the node that owns the key again serves the deleted value");
    wait_for("the surviving owners hold the aged values", || {
        [&remaining[0], &remaining[1]].iter().all(|&&(_, a)| {
            held(a, &aged).as_deref() == Some(&b"aged"[..]) && held(a, &mine).as_deref() == Some(&b"mine"[..])
        })
    });
    std::thread::sleep(Duration::from_millis(300));
    for &(n, a) in &remaining {
        assert_eq!(served(a, &zombie), None, "node {n} serves the deleted value");
    }
    drop(nodes);
}

fn records(addr: SocketAddr) -> u64 {
    match rpc(addr, &Request::CleanupStatus) {
        Response::CleanupStatus { records, .. } => records,
        other => panic!("expected a cleanup status, got {other:?}"),
    }
}

#[test]
fn a_copy_one_owner_still_holds_is_never_recorded_as_refused() {
    let ring = Ring::from_members(2, 64, [1, 2, 3]);
    let key = owned_by(&ring, "valid", &[1, 2], None);
    let now = physical_now_ms();
    let grace = 60_000u64;
    let old = at(now - 600_000, 9);
    let refused_with = |owner_one_holds: bool| {
        let mut cluster = crate::coord::LocalCluster::new();
        for owner in [1, 2] {
            let mut s = store(owner).with_gc_grace_ms(grace);
            if owner == 1 && owner_one_holds {
                s.apply(&key, Some(b"valid"), old).unwrap();
            }
            s.apply_at(b"anchor", Some(b"a"), at(now, 9), now).unwrap();
            assert_eq!(s.apply(&key, Some(b"valid"), old).unwrap(), crate::replica::Applied::Rejected);
            cluster.insert(owner, s);
        }
        let mut holder = store(3);
        holder.apply(&key, Some(b"valid"), old).unwrap();
        let holder = std::cell::RefCell::new(holder.with_gc_grace_ms(grace));
        let progress = vals_rt::block_on(
            crate::antientropy::cleanup_scoped(
                &holder, &cluster, &ring, 3, usize::MAX, (0, 1), None, None, || true, || true, || false,
            ),
            || {},
        )
        .unwrap();
        assert_eq!(progress.kept, 1);
        progress.refused
    };
    assert_eq!(refused_with(false), vec![(key.clone(), old)], "every owner refuses and none holds the value");
    assert_eq!(refused_with(true), vec![], "owner 1 still serves the value, so it is not a deleted copy");
}

#[test]
fn an_aged_value_follows_trusted_handoffs_to_a_new_owner_and_back() {
    let before = Ring::from_members(1, 64, [1, 2]);
    let with_three = Ring::from_members(1, 64, [1, 2, 3]);
    let aged = owned_by(&before, "aged", &[1], Some((&with_three, &[3])));
    let now = physical_now_ms();
    let mut stores: Vec<_> = (1..=3).map(store).collect();
    stores[0].apply(&aged, Some(b"aged"), at(now - 600_000, 9)).unwrap();
    for s in &mut stores {
        s.apply(b"anchor", Some(b"a"), at(now, 9)).unwrap();
    }
    let (peers, nodes) = serve_all(stores, &before, now, 60_000, &[]);
    wait_for("node 1 serves", || held(peers[0].1, &aged).is_some());

    set_members(&peers, &peers, 10);
    wait_for("the new owner takes the aged value and node 1 releases it", || {
        held(peers[2].1, &aged).as_deref() == Some(&b"aged"[..]) && held(peers[0].1, &aged).is_none()
    });

    set_members(&peers, &peers[..2], 11);
    wait_for("removing node 3 hands the aged value back to node 1", || {
        held(peers[0].1, &aged).as_deref() == Some(&b"aged"[..])
    });
    drop(nodes);
}

#[test]
fn a_copy_the_owners_refused_is_not_offered_to_an_empty_new_owner() {
    use crate::pd::MembershipChange::Add;
    let before = Ring::from_members(1, 64, [1, 2, 3]);
    let with_four = Ring::from_members(1, 64, [1, 2, 3, 4]);
    let zombie = owned_by(&before, "zombie", &[1], Some((&with_four, &[4])));
    let now = physical_now_ms();
    let grace = 60_000u64;
    let owner = collected_owner(1, &zombie, now, grace, &[]);
    let mut holder = store(3);
    holder.apply(&zombie, Some(b"deleted-value"), at(now - 600_000, 9)).unwrap();
    let (peers, nodes) = serve_all(vec![owner, store(2), holder, store(4)], &before, now, grace, &[4]);
    wait_for("node 3 reports the copy its owner refuses", || {
        let refusal = gate(&peers[..3], Add(4), &[peers[3].1]).err().unwrap_or_default();
        refusal.contains("node 3 shard 0") && refusal.contains("kept 1 copies")
    });
    assert_eq!(records(peers[3].1), 0);

    set_members(&peers, &peers, 10);
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(served(peers[3].1, &zombie), None, "the empty new owner admitted the copy its owner refused");
    assert_eq!(served(peers[2].1, &zombie).as_deref(), Some(&b"deleted-value"[..]), "a non-owner keeps the copy");
    drop(nodes);
}

#[test]
fn a_regained_copy_stays_deleted_when_an_owner_that_refused_it_is_down_and_reads_wait_for_the_delete() {
    use crate::pd::MembershipChange::Remove;
    use std::sync::atomic::Ordering::SeqCst;
    let before = Ring::from_members(2, 64, [1, 2, 3]);
    let zombie = owned_by(&before, "fence-fails", &[1, 2], None);
    let now = physical_now_ms();
    let grace = 60_000u64;
    let one = collected_owner(1, &zombie, now, grace, &[]);
    let two = collected_owner(2, &zombie, now, grace, &[]);
    let mut holder = store(3);
    holder.apply(&zombie, Some(b"deleted-value"), at(now - 600_000, 9)).unwrap();
    let (peers, mut nodes) = serve_all(vec![one, two, holder], &before, now, grace, &[]);
    wait_for("node 3 reports the copy its owners refuse", || {
        let refusal = gate(&peers, Remove(1), &[]).err().unwrap_or_default();
        refusal.contains("node 3 shard 0") && refusal.contains("kept 1 copies")
    });
    drop(nodes.remove(1));
    std::thread::sleep(Duration::from_millis(200));

    crate::net::FAIL_FENCE_WRITES.store(true, SeqCst);
    let remaining = [peers[1], peers[2]];
    let members: Vec<(NodeId, u32)> = remaining.iter().map(|&(n, _)| (n, 1)).collect();
    let addrs: Vec<(NodeId, Vec<SocketAddr>)> = remaining.iter().map(|&(n, a)| (n, vec![a])).collect();
    for &(_, a) in [peers[0], peers[2]].iter() {
        let reply = rpc(a, &Request::SetMembers { auth: &[], epoch: 10, vnodes: 64, rf: 2, members: &members, addrs: &addrs, incarnation: None });
        assert!(matches!(reply, Response::Members { epoch: 10, .. }), "{reply:?}");
    }
    let read = rpc(peers[2].1, &Request::Read { epoch: u64::MAX, zone_id: DEFAULT_ZONE, key: &zombie });
    crate::net::FAIL_FENCE_WRITES.store(false, SeqCst);
    assert!(matches!(&read, Response::Err(e) if e.contains("could not be deleted")), "{read:?}");
    wait_for("a later sweep pass deletes the copy", || {
        matches!(rpc(peers[2].1, &Request::Read { epoch: u64::MAX, zone_id: DEFAULT_ZONE, key: &zombie }), Response::Read(_))
    });
    assert_eq!(served(peers[2].1, &zombie), None, "the node that owns the key again serves the deleted value");
    drop(nodes);
}

fn stale_holder_cluster() -> (Vec<u8>, Vec<(NodeId, SocketAddr)>, Vec<crate::testutil::Running>) {
    let before = Ring::from_members(1, 64, [1, 2, 3]);
    let after = Ring::from_members(1, 64, [2, 3]);
    let zombie = (0..10_000)
        .map(|i| format!("zombie-{i}").into_bytes())
        .find(|k| before.replicas(k) == vec![1] && after.replicas(k) == vec![3])
        .unwrap();
    let now = physical_now_ms();
    let grace = 60_000u64;
    let owner = collected_owner(1, &zombie, now, grace, &[]);
    let mut holder = store(3);
    holder.apply(&zombie, Some(b"deleted-value"), at(now - 600_000, 9)).unwrap();
    let (peers, nodes) = serve_all(vec![owner, store(2), holder], &before, now, grace, &[]);
    (zombie, peers, nodes)
}

#[test]
fn removing_the_owner_that_collected_a_delete_is_refused_while_a_former_owner_keeps_the_value() {
    use crate::pd::MembershipChange::{Add, Remove};
    let (zombie, peers, nodes) = stale_holder_cluster();
    let mut refusal = String::new();
    wait_for("node 3 reports the copy no owner accepts", || {
        refusal = gate(&peers, Remove(1), &[]).err().unwrap_or_default();
        refusal.contains("node 3 shard 0") && refusal.contains("kept 1 copies")
    });
    assert!(refusal.contains(&format!("{:?}", String::from_utf8_lossy(&zombie))), "{refusal}");
    let (_, empty) = Node::bind().unwrap();
    assert!(gate(&peers, Add(4), &[empty]).is_err(), "adding a node can move the key to node 3 too");
    assert_eq!(served(peers[2].1, &zombie).as_deref(), Some(&b"deleted-value"[..]));
    assert_eq!(served(peers[0].1, &zombie), None);
    assert_eq!(exported(&peers, &zombie), (None, 1), "before the change the backup fences the copy");

    let deleted = rpc(peers[0].1, &Request::Apply {
        epoch: 0,
        zone_id: DEFAULT_ZONE,
        key: &zombie,
        value: None,
        version: at(physical_now_ms(), 9),
    });
    assert!(matches!(deleted, Response::Apply(crate::replica::Applied::Won)), "{deleted:?}");
    wait_for("cleanup releases the copy once the owner holds a newer delete", || gate(&peers, Remove(1), &[]).is_ok());
    assert_eq!(served(peers[2].1, &zombie), None);

    let remaining = peers[1..].to_vec();
    set_members(&peers, &remaining, 10);
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(served(peers[2].1, &zombie), None, "the new owner does not serve the deleted value");
    let (found, omitted) = exported(&remaining, &zombie);
    assert!(found.as_ref().is_none_or(|value| value.is_none()), "the backup brings the deleted value back: {found:?}");
    assert_eq!(omitted, 0);
    drop(nodes);
}

#[test]
fn a_removed_node_that_kept_a_stale_copy_cannot_rejoin_until_it_holds_no_data() {
    use crate::pd::MembershipChange::{Add, Remove};
    let (zombie, peers, nodes) = stale_holder_cluster();
    let rest = peers[..2].to_vec();
    wait_for("the nodes that stay are settled", || gate(&peers, Remove(3), &[]).is_ok());
    set_members(&rest, &rest, 10);
    assert_eq!(served(peers[2].1, &zombie).as_deref(), Some(&b"deleted-value"[..]), "the removed node keeps the copy");
    let mut refusal = String::new();
    wait_for("only the rejoining node's data blocks the change", || {
        refusal = gate(&rest, Add(3), &[peers[2].1]).err().unwrap_or_default();
        refusal.starts_with("incoming node 3 shard 0")
    });
    assert!(refusal.contains("holds") && refusal.contains("records"), "{refusal}");
    drop(nodes);
}

fn push_range_map(peers: &[(NodeId, SocketAddr)], zone_id: ZoneId, map: &crate::partition::RangeMap, epoch: u64) {
    let maps = [wire::WireRangeMap {
        zone_id,
        splits: map.splits(),
        tokens: map.tokens(),
        pending: map.pending(),
        placement_debt: map.placement_debt(),
        opaque_tokens: map.supports_opaque_tokens(),
    }];
    for &(_, a) in peers {
        let reply = rpc(a, &Request::SetRangeMaps { auth: &[], epoch, maps: &maps, incarnation: None });
        assert!(matches!(reply, Response::RangeMaps { .. }), "{reply:?}");
    }
}

#[test]
fn a_membership_change_waits_for_a_pending_rehome_whose_target_holds_a_stale_copy() {
    use crate::pd::MembershipChange::Remove;
    use crate::partition::RangeMap;
    use crate::zonestore::{MemZoneStore, ZoneGeom, ZoneStore};
    let zone_id: ZoneId = 11;
    let ring = Ring::from_members(1, 64, [1, 2, 3]);
    let source = ring.replicas_in_zone(zone_id, b"", 1, None)[0];
    let target = (1..=3).find(|&n| n != source).unwrap();
    let boundary = (0..10_000)
        .map(|i| format!("m{i}").into_bytes())
        .find(|t| ring.replicas_in_zone(zone_id, t, 1, None) == vec![target])
        .unwrap();
    let mut zombie = boundary.clone();
    zombie.extend_from_slice(b"/zombie");
    let now = physical_now_ms();
    let grace = 60_000u64;
    let cfg = crate::zone::ZoneConfig {
        name: "pending".into(),
        rf: 1,
        ordering: crate::zone::Ordering::Sorted,
        scope: None,
    };
    let bound: Vec<(NodeId, std::net::TcpListener, SocketAddr)> = (1..=3)
        .map(|n| {
            let (l, a) = Node::bind().unwrap();
            (n, l, a)
        })
        .collect();
    let peers: Vec<(NodeId, SocketAddr)> = bound.iter().map(|(n, _, a)| (*n, *a)).collect();
    let mut nodes = Vec::new();
    for (n, listener, _) in bound {
        let geom = ZoneGeom { block_size: 512, seg_blocks: 64, reserved_blocks: 0, region_blocks: 65_536 };
        let provider = MemZoneStore::new(n, geom);
        let mut zone = provider.open(zone_id, crate::zone::Ordering::Sorted).unwrap();
        if n == source {
            zone = zone.with_gc_grace_ms(grace);
            zone.apply_at(&zombie, None, at(now - 500_000, 9), now - 500_000).unwrap();
            zone.apply_at(b"anchor", Some(b"a"), at(now, 9), now).unwrap();
            zone.flush().unwrap();
            zone.advance_durable_gc_watermark(now);
            let plan = zone.compact_plan_tombstone_gc(now).unwrap().unwrap();
            let written = vals_rt::block_on(
                vals_store::compact_and_write(plan, Arc::new(zone.device_sibling().unwrap())),
                || {},
            )
            .unwrap();
            let collected = zone.compact_commit(written).unwrap();
            assert!(collected.current_dropped_tombstones.iter().any(|(k, _)| k == &zombie));
            assert!(zone.get_versioned(&zombie).unwrap().is_none());
        }
        if n == target {
            zone.apply(&zombie, Some(b"deleted-value"), at(now - 600_000, 9)).unwrap();
        }
        zone.sync().unwrap();
        drop(zone);
        let zone0 = provider.open(DEFAULT_ZONE, crate::zone::Ordering::Unsorted).unwrap();
        let mut catalog = crate::zone::Catalog::new();
        catalog.insert(zone_id, cfg.clone());
        let log = MemLivenessLog::new();
        log.store(now).unwrap();
        nodes.push(crate::testutil::running(Node::serve_zoned_with_liveness_and_settings(
            n,
            listener,
            zone0,
            provider,
            catalog,
            ring.clone(),
            peers.clone(),
            Duration::from_millis(25),
            grace,
            log,
            None,
            crate::node_settings::NodeSettings { rehome_copy_hold_ms: 1_000, ..Default::default() },
        )));
    }
    set_members(&peers, &peers, 1);
    let pending = RangeMap::from_splits_tokens_and_pending(
        vec![boundary.clone()],
        vec![Vec::new(), Vec::new()],
        vec![None, Some(boundary.clone())],
    )
    .unwrap();
    push_range_map(&peers, zone_id, &pending, 12);
    let members: crate::pd::AddressedMembers = peers.iter().map(|&(n, a)| (n, 1, vec![a])).collect();
    let pd_maps = std::collections::HashMap::from([(zone_id, pending)]);
    let in_pd = crate::pd::check_membership_change(&members, &pd_maps, Remove(source), &[]).unwrap_err();
    assert!(in_pd.contains(&format!("zone {zone_id} has a pending re-home in the PD state")), "{in_pd}");
    let on_node = gate(&peers, Remove(source), &[]).unwrap_err();
    assert!(on_node.contains(&format!("zone {zone_id}: a re-home is pending")), "{on_node}");

    let aborted = RangeMap::from_splits_tokens_and_pending(
        vec![boundary.clone()],
        vec![Vec::new(), Vec::new()],
        vec![None, None],
    )
    .unwrap();
    push_range_map(&peers, zone_id, &aborted, 13);
    let mut refusal = String::new();
    wait_for("the aborted target reports the copy no owner accepts", || {
        let result = gate(&peers, Remove(source), &[]);
        assert!(result.is_err(), "a clean lap from before the abort certified the change: {result:?}");
        refusal = result.unwrap_err();
        refusal.contains(&format!("node {target} shard 0")) && refusal.contains("kept 1 copies")
    });
    assert!(refusal.contains(&format!("{:?}", String::from_utf8_lossy(&zombie))), "{refusal}");
    let maps = std::collections::HashMap::from([(zone_id, aborted)]);
    assert_eq!(
        exported_zone(&peers, zone_id, &cfg, maps, &zombie),
        (None, 1),
        "the leftover stays on the aborted target and the backup fences it"
    );
    drop(nodes);
}

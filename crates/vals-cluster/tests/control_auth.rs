use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

use vals_cluster::wire::{self, Request, Response};
use vals_cluster::{init_cluster_secret, Node, PdDaemon, Ring, RingConfig, VersionedStore};
use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine, Ordering};

const SECRET: &[u8] = b"itsasecrettoeverybody";

fn store(node: u32) -> VersionedStore<MemBlockDevice> {
    let dev = MemBlockDevice::new(256, 512);
    let engine = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    VersionedStore::new(engine, node).unwrap()
}

fn pd_engine() -> Engine<MemBlockDevice> {
    let dev = MemBlockDevice::new(256, 512);
    Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    )
}

fn rpc(addr: SocketAddr, req: &Request) -> Response {
    let mut s = TcpStream::connect(addr).unwrap();
    vals_cluster::proto::client_handshake_blocking(&mut s).unwrap();
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

fn members_of(addr: SocketAddr) -> (u64, Vec<u32>) {
    match rpc(addr, &Request::Members) {
        Response::Members { epoch, members, .. } => {
            (epoch, members.into_iter().map(|(n, _)| n).collect())
        }
        other => panic!("expected Members reply, got {other:?}"),
    }
}

#[test]
fn control_ops_require_the_cluster_secret() {
    assert!(init_cluster_secret(SECRET), "first init must win");
    assert!(
        !init_cluster_secret(b"other"),
        "the secret is set-once; a second init must be refused"
    );

    let mut ring = Ring::new(1);
    ring.set_members_weighted([(1, 1)]);
    let (l1, a1) = Node::bind().unwrap();
    Node::serve(
        1,
        l1,
        store(1),
        ring,
        vec![(1, a1)],
        Duration::from_secs(3600),
    );

    let denied_before = vals_cluster::observe::snapshot().control_denied;

    let hostile = Request::SetMembers {
        auth: &[],
        epoch: u64::MAX,
        vnodes: 64,
        rf: 1,
        members: &[(666, 1)],
        addrs: &[],
        incarnation: None,
    };
    assert_eq!(rpc(a1, &hostile), Response::Denied);

    let guessed = Request::SetMembers {
        auth: b"itsasecrettoeverybodz",
        epoch: u64::MAX,
        vnodes: 64,
        rf: 1,
        members: &[(666, 1)],
        addrs: &[],
        incarnation: None,
    };
    assert_eq!(rpc(a1, &guessed), Response::Denied);

    assert_eq!(
        rpc(
            a1,
            &Request::SetZones {
                auth: &[],
                epoch: u64::MAX,
                zones: &[],
                incarnation: None,
            }
        ),
        Response::Denied
    );
    assert_eq!(
        rpc(
            a1,
            &Request::SetRangeMaps {
                auth: &[],
                epoch: u64::MAX,
                maps: &[],
                incarnation: None,
            }
        ),
        Response::Denied
    );

    let (epoch, members) = members_of(a1);
    assert_eq!(epoch, 0, "hostile epoch must not be adopted");
    assert_eq!(members, vec![1], "hostile membership must not be adopted");
    assert!(vals_cluster::observe::snapshot().control_denied >= denied_before + 4);

    let legit = Request::SetMembers {
        auth: SECRET,
        epoch: 1,
        vnodes: 64,
        rf: 1,
        members: &[(1, 1), (2, 1)],
        addrs: &[],
        incarnation: None,
    };
    match rpc(a1, &legit) {
        Response::Members { epoch, members, .. } => {
            assert_eq!(epoch, 1);
            assert_eq!(members.len(), 2, "authenticated push must be adopted");
        }
        other => panic!("expected Members reply, got {other:?}"),
    }

    let (l2, a2) = Node::bind().unwrap();
    let mut ring2 = Ring::new(1);
    ring2.set_members_weighted([(2, 1)]);
    Node::serve(
        2,
        l2,
        store(2),
        ring2,
        vec![(2, a2)],
        Duration::from_secs(3600),
    );
    let pd =
        PdDaemon::spawn_single(1, pd_engine(), RingConfig::default(), vec![(2, vec![a2])]).unwrap();
    pd.bootstrap_members(vec![
        (2, 1, Vec::new()),
        (3, 1, Vec::new()),
        (4, 1, Vec::new()),
    ]);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (epoch, members) = members_of(a2);
        if epoch > 0 && members == vec![2, 3, 4] {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "node 2 never adopted the PD's authenticated push (epoch {epoch}, {members:?})"
        );
        thread::sleep(Duration::from_millis(20));
    }

    let (l3, a3) = Node::bind().unwrap();
    let mut ring3 = Ring::new(1);
    ring3.set_members_weighted([(3, 1)]);
    Node::serve(
        3,
        l3,
        store(3),
        ring3,
        vec![(3, a3)],
        Duration::from_secs(3600),
    );
    let (pl1, pa1) = PdDaemon::bind().unwrap();
    let (pl2, pa2) = PdDaemon::bind().unwrap();
    let pd_peers = vec![(1u64, pa1), (2u64, pa2)];
    let data_nodes = vec![(3u32, vec![a3])];
    let ring_cfg = RingConfig { vnodes: 128, rf: 2 };
    let pd1 = PdDaemon::spawn(
        1,
        pl1,
        pd_peers.clone(),
        data_nodes.clone(),
        pd_engine(),
        ring_cfg,
        true,
    )
    .unwrap();
    let pd2 = PdDaemon::spawn(
        2,
        pl2,
        pd_peers,
        data_nodes,
        pd_engine(),
        ring_cfg,
        false,
    )
    .unwrap();

    let denied_before = vals_cluster::observe::snapshot().control_denied;
    let ver = vals_cluster::proto::PROTO_VERSION.to_le_bytes();
    let mut bad = TcpStream::connect(pa1).unwrap();
    let mut hello = ((4 + ver.len() + 5) as u32).to_le_bytes().to_vec();
    hello.extend_from_slice(b"VPDA");
    hello.extend_from_slice(&ver);
    hello.extend_from_slice(b"wrong");
    bad.write_all(&hello).unwrap();
    bad.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut byte = [0u8; 1];
    assert_eq!(
        bad.read(&mut byte).unwrap(),
        0,
        "a bad raft hello must get the connection dropped (EOF)"
    );
    assert!(vals_cluster::observe::snapshot().control_denied > denied_before);

    let mut good = TcpStream::connect(pa1).unwrap();
    let mut hello = ((4 + ver.len() + SECRET.len()) as u32)
        .to_le_bytes()
        .to_vec();
    hello.extend_from_slice(b"VPDA");
    hello.extend_from_slice(&ver);
    hello.extend_from_slice(SECRET);
    good.write_all(&hello).unwrap();
    good.set_read_timeout(Some(Duration::from_millis(300)))
        .unwrap();
    let err = good
        .read(&mut byte)
        .expect_err("an authenticated raft conn must stay open (read times out, not EOF)");
    assert!(
        matches!(
            err.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
        ),
        "unexpected read error: {err:?}"
    );

    let membership = vec![(3, 1, Vec::new()), (5, 1, Vec::new())];
    pd1.bootstrap_members(membership.clone());
    pd2.bootstrap_members(membership);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (epoch, members) = members_of(a3);
        if epoch > 0 && members == vec![3, 5] {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "2-voter PD group never reconfigured node 3 (epoch {epoch}, {members:?})"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

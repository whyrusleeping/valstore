use super::server::{catalog_to_wire, serve_request, ConnDriver, ReadGate, ServerOut};
use super::*;
use super::{connect_nonblocking, flush_dirty, ConnState, MuxConn, MuxRpc, Waiter};

use std::cell::RefCell;
use std::future::Future;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::antientropy::AePeer;
use crate::coord::{
    BatchAck, BatchEntry, CoordError, Coordinator, WriteConcern, WriteOptions, ReadConcern, ReadFanout, Transport,
    TransportError, WriteOutcome,
};
use crate::partition::RangeMap;
use crate::reactor;
use crate::replica::VersionedStore;
use crate::ring::{NodeId, Ring};
use crate::wire::{self, OwnedRequest, Request, Response};
use crate::zone::Ordering;
use crate::zone::{Catalog, ZoneConfig, ZoneId, DEFAULT_ZONE};
use crate::zonestore::ZoneStore;
use vals_rt::LocalExecutor;
use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine, Ordering as SOrdering};

mod reverse_scan;

#[test]
fn push_resp_frames_identically_to_encode_then_frame() {
    use crate::hlc::Version;
    use crate::replica::Stored;
    let framed = |reqid: u64, resp: &Response| {
        let payload = wire::encode_response_payload(resp);
        let mut f = Vec::new();
        f.extend_from_slice(&((8 + payload.len()) as u32).to_le_bytes());
        f.extend_from_slice(&reqid.to_le_bytes());
        f.extend_from_slice(&payload);
        f
    };
    let responses = [
        Response::Read(Some(Stored {
            version: Version { hlc: 5, node: 2 },
            value: Some(b"hello world".to_vec()),
        })),
        Response::Read(None),
        Response::Err("boom".into()),
    ];
    let out = ServerOut::default();
    let mut expect = Vec::new();
    for (i, resp) in responses.iter().enumerate() {
        let reqid = 100 + i as u64;
        out.push_resp(reqid, resp);
        expect.extend_from_slice(&framed(reqid, resp));
    }

    for (i, val) in [Some(&b"hello world"[..]), None].into_iter().enumerate() {
        let reqid = 200 + i as u64;
        let v = Version { hlc: 5, node: 2 };
        out.push_read_reply(reqid, Some((v, val)));
        expect.extend_from_slice(&framed(
            reqid,
            &Response::Read(Some(Stored {
                version: v,
                value: val.map(<[u8]>::to_vec),
            })),
        ));
    }
    out.push_read_reply(255, None);
    expect.extend_from_slice(&framed(255, &Response::Read(None)));
    assert_eq!(&*out.buf.borrow(), &expect, "in-place framing must match");
}

fn replica(node: NodeId) -> VersionedStore<MemBlockDevice> {
    let dev = MemBlockDevice::new(256, 256);
    let engine = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: SOrdering::Unsorted,
        },
    );
    VersionedStore::new(engine, node).unwrap()
}

fn run<F: Future>(f: F) -> F::Output {
    vals_rt::block_on(f, reactor::poll_io)
}

fn build(n: u32, rf: usize, r: usize) -> Coordinator<NetTransport> {
    let mut ring = Ring::with_vnodes(rf, 64);
    let mut addrs = Vec::new();
    for node in 1..=n {
        let addr = ReplicaServer::spawn(replica(node)).unwrap();
        addrs.push((node, addr));
        ring.add_node(node);
    }
    Coordinator::new(0, ring, r, NetTransport::connect(&addrs))
}

struct FlagWake(std::sync::atomic::AtomicBool);
impl std::task::Wake for FlagWake {
    fn wake(self: Arc<Self>) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}
impl FlagWake {
    fn pair() -> (Arc<FlagWake>, Waker) {
        let flag = Arc::new(FlagWake(std::sync::atomic::AtomicBool::new(false)));
        let waker = Waker::from(flag.clone());
        (flag, waker)
    }
    fn fired(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[test]
fn scan_gate_sheds_at_limit_and_releases_on_all_exits() {
    let shared = server::ServerShared::single_zone(replica(1));
    let mut permits = Vec::new();
    for _ in 0..8 {
        permits.push(shared.try_scan().expect("scan should admit below default limit"));
    }
    let before = crate::observe::counters().scan_admission_busy.get();
    assert!(shared.try_scan().is_none());
    assert!(crate::observe::counters().scan_admission_busy.get() >= before + 1);
    drop(permits.pop());
    let permit = shared.try_scan().expect("success releases scan capacity");
    drop(permit);
    drop(permits.pop());
    let permit = shared.try_scan().expect("error releases scan capacity");
    drop(permit);
    drop(permits);
    let restored: Vec<_> = (0..8).map(|_| shared.try_scan().expect("all scan permits returned")).collect();
    drop(restored);
}

#[test]
fn scan_dispatch_replies_busy_at_limit_then_serves_after_release() {
    use std::net::{TcpListener, TcpStream};

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (server, _) = listener.accept().unwrap();
    server.set_nonblocking(true).unwrap();
    let dev = MemBlockDevice::new(256, 256);
    let engine = Engine::create_prewiped(dev, Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: SOrdering::Sorted,
    });
    let shared = Rc::new(ServerShared::single_zone(VersionedStore::new(engine, 1).unwrap()));
    let permits: Vec<_> = (0..8).map(|_| shared.try_scan().unwrap()).collect();
    let ex = LocalExecutor::new();
    let out = Rc::new(ServerOut::default());
    let driver = ConnDriver {
        fd: server.as_raw_fd(),
        stream: Rc::new(Sock::Tcp(server)),
        reader: FrameRead::default(),
        out: out.clone(),
        shared: shared.clone(),
        spawner: ex.spawner(),
        negotiated: Some(crate::proto::test_negotiated()),
        apply_mode_override: None,
    };
    let request = |zone_id| {
        let payload = wire::encode_request_payload(&Request::Scan {
            epoch: 0, zone_id, lo: None, hi: None, max: 1,
        });
        let mut block = 1u64.to_le_bytes().to_vec();
        block.extend_from_slice(&payload);
        block
    };
    let reply = || {
        let buf = out.buf.borrow();
        wire::decode_response(&buf[12..]).unwrap()
    };
    let before = crate::observe::counters().scan_admission_busy.get();
    assert!(driver.dispatch_frame(&request(DEFAULT_ZONE)));
    ex.run(reactor::poll_io);
    assert!(matches!(reply(), Response::Busy));
    assert!(crate::observe::counters().scan_admission_busy.get() >= before + 1);
    out.buf.borrow_mut().clear();
    drop(permits);
    assert!(driver.dispatch_frame(&request(12345)));
    ex.run(reactor::poll_io);
    assert!(matches!(reply(), Response::Err(_)));
    out.buf.borrow_mut().clear();
    assert!(driver.dispatch_frame(&request(DEFAULT_ZONE)));
    ex.run(reactor::poll_io);
    assert!(matches!(reply(), Response::Scan { .. }));
}

#[test]
fn read_gate_pause_parks_admission_until_resumed() {
    let gate = ReadGate::new();
    let (flag, waker) = FlagWake::pair();
    let mut cx = Context::from_waker(&waker);

    let mut fut = gate.acquire();
    let Poll::Ready(guard) = Pin::new(&mut fut).poll(&mut cx) else {
        panic!("unpaused acquire must admit immediately");
    };
    assert_eq!(gate.inflight(), 1);
    drop(guard);
    assert_eq!(gate.inflight(), 0);

    let pause = gate.pause();
    let mut fut = gate.acquire();
    assert!(Pin::new(&mut fut).poll(&mut cx).is_pending());
    assert_eq!(
        gate.inflight(),
        0,
        "a parked read must not count as admitted"
    );
    assert!(!flag.fired());
    drop(pause);
    assert!(flag.fired(), "lifting the pause must wake parked reads");
    let Poll::Ready(_guard) = Pin::new(&mut fut).poll(&mut cx) else {
        panic!("acquire must admit once the pause lifts");
    };
    assert_eq!(gate.inflight(), 1);
}

#[test]
fn read_gate_limit_parks_and_wakes_on_release() {
    let gate = ReadGate::with_limit(2);
    let (flag, waker) = FlagWake::pair();
    let mut cx = Context::from_waker(&waker);
    let g1 = run(gate.acquire());
    let g2 = run(gate.acquire());
    let mut third = gate.acquire();
    assert!(Pin::new(&mut third).poll(&mut cx).is_pending());
    assert_eq!(gate.inflight(), 2);
    drop(g1);
    assert!(flag.fired(), "freeing a limited slot must wake a parked acquire");
    let Poll::Ready(g3) = Pin::new(&mut third).poll(&mut cx) else {
        panic!("parked acquire must take the released slot");
    };
    assert_eq!(gate.inflight(), 2);
    drop((g2, g3));
    assert_eq!(gate.inflight(), 0);
}

#[test]
fn read_gate_signals_drained_only_when_paused_and_empty() {
    let gate = ReadGate::new();
    let (_flag, waker) = FlagWake::pair();
    let mut cx = Context::from_waker(&waker);

    let g = run(gate.acquire());
    drop(g);
    let mut d = gate.drained();
    assert!(
        Pin::new(&mut d).poll(&mut cx).is_pending(),
        "drained must not fire for an unpaused drop to zero"
    );

    let g1 = run(gate.acquire());
    let g2 = run(gate.acquire());
    let _pause = gate.pause();
    drop(g1);
    assert!(
        Pin::new(&mut d).poll(&mut cx).is_pending(),
        "drained must not fire while a read is still in flight"
    );
    drop(g2);
    assert!(
        Pin::new(&mut d).poll(&mut cx).is_ready(),
        "the last guard out under a pause must signal drained"
    );
}

#[test]
fn read_gate_checkpoint_fast_path_keeps_admission() {
    let gate = ReadGate::new();
    let mut guard = run(gate.acquire());
    assert!(!run(guard.checkpoint()));
    assert_eq!(gate.inflight(), 1);
    drop(guard);
    assert_eq!(gate.inflight(), 0);
}

#[test]
fn read_gate_checkpoint_yields_to_pause_and_resumes() {
    let gate = ReadGate::with_limit(2);
    let mut g1 = run(gate.acquire());
    let mut g2 = run(gate.acquire());
    let pause = gate.pause();
    let (flag, waker) = FlagWake::pair();
    let mut cx = Context::from_waker(&waker);
    let mut c1 = Box::pin(g1.checkpoint());
    let mut c2 = Box::pin(g2.checkpoint());

    assert!(c1.as_mut().poll(&mut cx).is_pending());
    assert_eq!(gate.inflight(), 1);
    assert!(c2.as_mut().poll(&mut cx).is_pending());
    assert_eq!(gate.inflight(), 0);
    run(gate.drained());

    drop(pause);
    assert!(flag.fired());
    assert_eq!(c1.as_mut().poll(&mut cx), Poll::Ready(true));
    assert_eq!(c2.as_mut().poll(&mut cx), Poll::Ready(true));
    drop((c1, c2));
    assert_eq!(gate.inflight(), 2);
    drop((g1, g2));
    assert_eq!(gate.inflight(), 0);
}

#[test]
fn sharded_node_routes_keys_to_shards() {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    fn read_direct(addr: SocketAddr, key: &[u8]) -> Option<Vec<u8>> {
        let mut s = TcpStream::connect(addr).unwrap();
        crate::proto::client_handshake_blocking(&mut s).unwrap();
        let payload = wire::encode_request_payload(&Request::Read {
            epoch: 0,
            zone_id: DEFAULT_ZONE,
            key,
        });
        let len = (8 + payload.len()) as u32;
        let mut frame = len.to_le_bytes().to_vec();
        frame.extend_from_slice(&1u64.to_le_bytes());
        frame.extend_from_slice(&payload);
        s.write_all(&frame).unwrap();
        let mut lenb = [0u8; 4];
        s.read_exact(&mut lenb).unwrap();
        let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
        s.read_exact(&mut block).unwrap();
        match wire::decode_response(&block[8..]).unwrap() {
            Response::Read(Some(st)) => st.value,
            _ => None,
        }
    }

    let shards = ShardedServer::spawn(vec![replica(1), replica(1), replica(1)]).unwrap();
    let mut ring = Ring::with_vnodes(1, 64);
    ring.set_members([1]);
    let c = Coordinator::new(
        0,
        ring,
        1,
        NetTransport::connect_sharded([(1, shards.clone())]),
    );

    let keys: Vec<String> = (0..40).map(|i| format!("k{i}")).collect();
    for k in &keys {
        run(c.write(k.as_bytes(), b"v")).unwrap();
    }
    for k in &keys {
        assert_eq!(
            run(c.read(k.as_bytes())).unwrap().unwrap().value.as_deref(),
            Some(&b"v"[..]),
            "{k} roundtrips through its shard"
        );
    }

    let mut owners = std::collections::HashSet::new();
    for k in &keys {
        let owner = (crate::ring::hash64(k.as_bytes()) % shards.len() as u64) as usize;
        owners.insert(owner);
        assert_eq!(
            read_direct(shards[owner], k.as_bytes()).as_deref(),
            Some(&b"v"[..])
        );
        let other = (owner + 1) % shards.len();
        assert_eq!(read_direct(shards[other], k.as_bytes()), None);
    }
    assert!(owners.len() >= 2, "keys spread across shards: {owners:?}");
}

#[test]
fn net_w0_write_reaches_the_replica() {
    let c = build(1, 1, 1);
    run(c.write_with(b"k", b"v", WriteConcern::Unacknowledged)).unwrap();
    let mut got = None;
    for _ in 0..200 {
        if let Some(st) = run(c.read(b"k")).unwrap() {
            got = st.value;
            break;
        }
    }
    assert_eq!(
        got.as_deref(),
        Some(&b"v"[..]),
        "a W=0 write's bytes reach the replica and apply"
    );
}

#[test]
fn accept_loop_survives_transient_accept_errors() {
    let faults_before = crate::observe::snapshot().accept_transient_errors;
    let addr = ReplicaServer::spawn_with_accept_faults(replica(1), 3).unwrap();
    let mut ring = Ring::with_vnodes(1, 64);
    ring.add_node(1);
    let c = Coordinator::new(0, ring, 1, NetTransport::connect(&[(1, addr)]));

    let mut applied = false;
    for _ in 0..300 {
        if run(c.write(b"acc-k", b"acc-v")).is_ok() {
            applied = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert!(
        applied,
        "accept loop recovered from transient accept errors and served a write"
    );

    let got = run(c.read(b"acc-k")).unwrap().and_then(|s| s.value);
    assert_eq!(got.as_deref(), Some(&b"acc-v"[..]));

    assert!(
        crate::observe::snapshot().accept_transient_errors >= faults_before + 3,
        "each shrugged-off transient accept error bumps the counter"
    );
}

#[test]
fn oversized_frame_is_rejected_without_buffering() {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    let addr = ReplicaServer::spawn(replica(1)).unwrap();
    let oversized_before = crate::observe::snapshot().oversized_frames;
    let mut s = TcpStream::connect(addr).unwrap();
    crate::proto::client_handshake_blocking(&mut s).unwrap();

    let bogus_len = (MAX_FRAME + 1) as u32;
    s.write_all(&bogus_len.to_le_bytes()).unwrap();
    s.flush().unwrap();

    s.set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    let mut buf = [0u8; 16];
    let n = s.read(&mut buf).unwrap_or(0);
    assert_eq!(n, 0, "server must close the conn on an over-cap frame");

    assert_eq!(
        crate::observe::snapshot().oversized_frames,
        oversized_before + 1,
        "an over-cap frame must increment oversized_frames"
    );
}

#[test]
fn handshake_rejects_a_bad_magic_and_counts_it() {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    let addr = ReplicaServer::spawn(replica(1)).unwrap();
    let denied_before = crate::observe::snapshot().handshake_denied;

    let mut s = TcpStream::connect(addr).unwrap();
    let mut frame = (crate::proto::HELLO_BODY_LEN as u32).to_le_bytes().to_vec();
    frame.extend_from_slice(b"BADX");
    frame.extend_from_slice(&crate::proto::PROTO_VERSION.to_le_bytes());
    frame.extend_from_slice(&0u64.to_le_bytes());
    s.write_all(&frame).unwrap();

    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut buf = [0u8; 16];
    assert_eq!(
        s.read(&mut buf).unwrap_or(0),
        0,
        "server must drop a bad-magic handshake (EOF), not serve it"
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    while crate::observe::snapshot().handshake_denied == denied_before {
        assert!(
            Instant::now() < deadline,
            "handshake_denied counter was not incremented on the refusal"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn handshake_negotiates_down_for_a_newer_client() {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    let addr = ReplicaServer::spawn(replica(1)).unwrap();
    let mut s = TcpStream::connect(addr).unwrap();

    let mut hello = (crate::proto::HELLO_BODY_LEN as u32).to_le_bytes().to_vec();
    hello.extend_from_slice(&crate::proto::HELLO_MAGIC);
    hello.extend_from_slice(&(crate::proto::PROTO_VERSION + 9).to_le_bytes());
    hello.extend_from_slice(&u64::MAX.to_le_bytes());
    s.write_all(&hello).unwrap();

    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut lenb = [0u8; 4];
    s.read_exact(&mut lenb).unwrap();
    let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
    s.read_exact(&mut block).unwrap();
    let (v, _f) = crate::proto::decode_hello(&block).expect("server hello");
    assert_eq!(
        v,
        crate::proto::PROTO_VERSION,
        "server advertises its version"
    );

    let payload = wire::encode_request_payload(&Request::Members);
    let mut frame = ((8 + payload.len()) as u32).to_le_bytes().to_vec();
    frame.extend_from_slice(&1u64.to_le_bytes());
    frame.extend_from_slice(&payload);
    s.write_all(&frame).unwrap();
    s.read_exact(&mut lenb).unwrap();
    let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
    s.read_exact(&mut block).unwrap();
    assert!(
        matches!(
            wire::decode_response(&block[8..]).unwrap(),
            Response::Members { .. }
        ),
        "a request on a negotiated connection is served normally"
    );
}

#[test]
fn frame_at_the_cap_boundary_is_accepted() {
    use std::io::Write;
    use std::net::{TcpListener, TcpStream};

    let lst = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = lst.local_addr().unwrap();
    let mut writer = TcpStream::connect(addr).unwrap();
    let (server, _) = lst.accept().unwrap();
    server.set_nonblocking(true).unwrap();

    let block = [7u8; 16];
    let len = block.len() as u32;
    writer.write_all(&len.to_le_bytes()).unwrap();
    writer.write_all(&block).unwrap();
    writer.flush().unwrap();

    let server = Sock::Tcp(server);
    let mut fr = FrameRead::default();
    let got = loop {
        match fr.read_block(&server) {
            Poll::Ready(Ok((off, len))) => break fr.frame_at(off, len).to_vec(),
            Poll::Ready(Err(e)) => panic!("a within-cap frame must not be rejected: {e:?}"),
            Poll::Pending => continue,
        }
    };
    assert_eq!(got, block);
}

#[test]
fn oversized_payload_is_refused_at_the_framing_point() {
    let mut payload = vec![0u8; MAX_FRAME - 7];
    let mut out = Vec::new();
    assert_eq!(write_framed(&mut out, 1, &payload), Err(()));
    assert!(out.is_empty(), "a refused frame must write nothing");
    assert!(frame_msg(1, &payload).is_err());

    payload.truncate(MAX_FRAME - 8);
    assert_eq!(write_framed(&mut out, 1, &payload), Ok(()));
    assert_eq!(out.len(), 4 + MAX_FRAME);
    assert_eq!(out[..4], (MAX_FRAME as u32).to_le_bytes());
}

#[test]
fn client_send_buffer_high_water_fails_a_stalled_conn() {
    use std::net::TcpListener;

    let lst = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = lst.local_addr().unwrap();
    let accepted = std::thread::spawn(move || lst.accept().map(|(s, _)| s));

    let sock = connect_nonblocking(addr).unwrap();
    let _peer = accepted.join().unwrap().unwrap();
    let fd = sock.as_raw_fd();

    let mut st = ConnState::default();
    st.fd = fd;
    st.sock = Some(Sock::Tcp(sock));
    st.pending.insert(7, Waiter::default());
    st.out = vec![0u8; MAX_CONN_SEND_BUF + 64 * 1024 * 1024];

    let mut wake = Vec::new();
    let need_write = st.drive(&mut wake);

    assert!(!need_write, "an over-cap conn must not ask to keep writing");
    assert!(
        st.sock.is_none(),
        "the wedged conn is torn down, not buffered"
    );
    assert!(st.out.is_empty(), "the reset clears the send buffer");
    assert!(
        st.pending.is_empty(),
        "every in-flight request is failed on the reset"
    );
}

#[test]
fn w0_send_probe_ready_only_after_flush_to_socket() {
    use std::net::TcpListener;

    let lst = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = lst.local_addr().unwrap();
    let accepted = std::thread::spawn(move || lst.accept().map(|(s, _)| s));
    let sock = connect_nonblocking(addr).unwrap();
    let _peer = accepted.join().unwrap().unwrap();
    let fd = sock.as_raw_fd();

    let mut st = ConnState::default();
    st.fd = fd;
    st.sock = Some(Sock::Tcp(sock));
    st.set_negotiated_for_test();
    st.queue_send_probe(vec![0u8; 64], 7);

    assert_eq!(
        st.waiter_outcome(7),
        None,
        "a send probe is not ready while its payload sits queued in `out`"
    );

    let mut wake = Vec::new();
    st.drive(&mut wake);

    assert_eq!(
        st.waiter_outcome(7),
        Some(true),
        "the send probe resolves Ok once its bytes are flushed to the socket"
    );
    assert!(!wake.is_empty(), "resolving a send probe wakes its waiter");
}

#[test]
fn w0_send_probe_failed_on_conn_reset() {
    use std::net::TcpListener;

    let lst = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = lst.local_addr().unwrap();
    let accepted = std::thread::spawn(move || lst.accept().map(|(s, _)| s));
    let sock = connect_nonblocking(addr).unwrap();
    let _peer = accepted.join().unwrap().unwrap();
    let fd = sock.as_raw_fd();

    let mut st = ConnState::default();
    st.fd = fd;
    st.sock = Some(Sock::Tcp(sock));
    st.queue_send_probe(vec![0u8; MAX_CONN_SEND_BUF + 64 * 1024 * 1024], 7);

    let mut wake = Vec::new();
    st.drive(&mut wake);

    assert!(st.sock.is_none(), "the wedged conn is torn down");
    assert!(
        st.pending.is_empty(),
        "the in-flight send probe is failed and drained on reset"
    );
}

#[test]
fn server_send_buffer_high_water_closes_a_stalled_conn() {
    use std::net::{TcpListener, TcpStream};

    let lst = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = lst.local_addr().unwrap();
    let _client = TcpStream::connect(addr).unwrap();
    let (server, _) = lst.accept().unwrap();
    server.set_nonblocking(true).unwrap();
    let fd = server.as_raw_fd();

    let ex = LocalExecutor::new();
    let out = Rc::new(ServerOut::default());
    out.buf
        .borrow_mut()
        .resize(MAX_CONN_SEND_BUF + 64 * 1024 * 1024, 0);

    let mut driver = ConnDriver {
        stream: Rc::new(Sock::Tcp(server)),
        fd,
        reader: FrameRead::default(),
        out,
        shared: Rc::new(ServerShared::single_zone(replica(1))),
        spawner: ex.spawner(),
        negotiated: Some(crate::proto::test_negotiated()),
        apply_mode_override: None,
    };
    let w = Waker::noop();
    let mut cx = Context::from_waker(w);
    let polled = Pin::new(&mut driver).poll(&mut cx);

    assert!(
        matches!(polled, Poll::Ready(())),
        "an over-cap server conn must close rather than buffer unboundedly"
    );
}

#[test]
fn pipelined_reads_past_the_cap_are_not_admitted() {
    use std::io::Write;
    use std::net::{TcpListener, TcpStream};

    let lst = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = lst.local_addr().unwrap();
    let mut client = TcpStream::connect(addr).unwrap();
    let (server, _) = lst.accept().unwrap();
    server.set_nonblocking(true).unwrap();
    let fd = server.as_raw_fd();

    let extra = 8;
    let payload = wire::encode_request_payload(&Request::Read {
        epoch: 0,
        zone_id: DEFAULT_ZONE,
        key: b"k",
    });
    let mut burst = Vec::new();
    for reqid in 0..(MAX_CONN_INFLIGHT_READS + extra) as u64 {
        burst.extend_from_slice(&((8 + payload.len()) as u32).to_le_bytes());
        burst.extend_from_slice(&reqid.to_le_bytes());
        burst.extend_from_slice(&payload);
    }
    client.write_all(&burst).unwrap();

    let ex = LocalExecutor::new();
    let out = Rc::new(ServerOut::default());
    let mut driver = ConnDriver {
        stream: Rc::new(Sock::Tcp(server)),
        fd,
        reader: FrameRead::default(),
        out: out.clone(),
        shared: Rc::new(ServerShared::single_zone(replica(1))),
        spawner: ex.spawner(),
        negotiated: Some(crate::proto::test_negotiated()),
        apply_mode_override: None,
    };
    let w = Waker::noop();
    let mut cx = Context::from_waker(w);
    assert!(Pin::new(&mut driver).poll(&mut cx).is_pending());
    assert_eq!(
        out.reads_inflight.get(),
        MAX_CONN_INFLIGHT_READS,
        "driver must stop admitting reads at the per-conn cap"
    );

    out.reads_inflight.set(MAX_CONN_INFLIGHT_READS - extra);
    assert!(Pin::new(&mut driver).poll(&mut cx).is_pending());
    assert_eq!(
        out.reads_inflight.get(),
        MAX_CONN_INFLIGHT_READS,
        "freed slots must resume admission of the deferred frames"
    );
    reactor::deregister(fd);
}

#[test]
fn client_config_validates_and_defaults_are_installed() {
    let invalid = ClientConfig {
        connect_deadline: Some(Duration::ZERO),
        ..ClientConfig::default()
    };
    assert!(matches!(
        NetTransport::connect_with_config(&[], invalid),
        Err(ClientConfigError::ZeroConnectDeadline)
    ));

    let bounded = Coordinator::new(0, Ring::new(1), 1, NetTransport::connect(&[]));
    let stats = bounded.credit_stats();
    assert_eq!(stats.max_ops, DEFAULT_CLIENT_MAX_OPS);
    assert_eq!(stats.max_bytes, DEFAULT_CLIENT_MAX_BYTES);
    assert_eq!(stats.node_max_ops, DEFAULT_CLIENT_NODE_MAX_OPS);
    assert_eq!(stats.node_max_bytes, DEFAULT_CLIENT_NODE_MAX_BYTES);
    let stats = bounded.transport().transport_credit_stats().unwrap();
    assert_eq!(stats.max_ops, DEFAULT_CLIENT_MAX_OPS);
    assert_eq!(stats.max_bytes, DEFAULT_CLIENT_MAX_BYTES);
    assert_eq!(stats.node_max_ops, DEFAULT_CLIENT_NODE_MAX_OPS);
    assert_eq!(stats.node_max_bytes, DEFAULT_CLIENT_NODE_MAX_BYTES);

    let unbounded = Coordinator::new(0, Ring::new(1), 1, NetTransport::connect_unbounded(&[]));
    let stats = unbounded.credit_stats();
    assert_eq!(stats.max_ops, usize::MAX);
    assert_eq!(stats.max_bytes, usize::MAX);
    assert_eq!(stats.node_max_ops, usize::MAX);
    assert_eq!(stats.node_max_bytes, usize::MAX);
    assert!(unbounded.transport().transport_credit_stats().is_none());
}

#[test]
fn rpc_deadline_fails_a_black_holing_peer() {
    use std::net::TcpListener;

    let lst = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = lst.local_addr().unwrap();
    let bg = std::thread::spawn(move || {
        if let Ok((s, _)) = lst.accept() {
            std::thread::sleep(Duration::from_secs(30));
            drop(s);
        }
    });

    let t = NetTransport::connect(&[(1, addr)]).with_rpc_deadline(Duration::from_millis(150));
    let start = Instant::now();
    let r = run(t.read(1, DEFAULT_ZONE, b"k"));
    assert!(
        matches!(r, Err(TransportError::Timeout)),
        "a black-holed RPC must fail with Timeout, got {r:?}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "the deadline must fire promptly (took {:?})",
        start.elapsed()
    );
    let r = run(t.read(1, DEFAULT_ZONE, b"k"));
    assert!(matches!(r, Err(TransportError::Timeout)), "got {r:?}");
    assert_eq!(
        t.connection_stats()[0].reconnects,
        1,
        "a connection silent for a whole deadline is replaced, not reused"
    );
    drop(bg);
}

#[test]
fn a_handshake_silent_past_the_connect_deadline_is_abandoned_even_with_no_request_waiting() {
    use std::net::TcpListener;

    let lst = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = lst.local_addr().unwrap();
    let t = NetTransport::connect(&[(1, addr)]).with_rpc_deadline(Duration::from_millis(200));
    {
        let mut read = Box::pin(t.read(1, DEFAULT_ZONE, b"k"));
        assert!(read.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    }
    std::thread::sleep(Duration::from_millis(300));
    reactor::poll_io_nonblocking();
    let r = run(t.read(1, DEFAULT_ZONE, b"k"));
    assert!(matches!(r, Err(TransportError::Timeout)), "got {r:?}");
    assert_eq!(
        t.connection_stats()[0].reconnects,
        1,
        "the second request dialed afresh instead of joining the stale handshake"
    );
    drop(lst);
}

#[test]
fn withdrawing_an_unsent_request_does_not_shorten_the_next_requests_deadline() {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};

    fn frame(s: &mut TcpStream) -> Vec<u8> {
        let mut n = [0; 4];
        s.read_exact(&mut n).unwrap();
        let mut b = vec![0; u32::from_le_bytes(n) as usize];
        s.read_exact(&mut b).unwrap();
        b
    }
    fn reply(s: &mut TcpStream, id: &[u8]) {
        let payload = wire::encode_response_payload(&Response::Read(None));
        let mut out = ((8 + payload.len()) as u32).to_le_bytes().to_vec();
        out.extend_from_slice(id);
        out.extend_from_slice(&payload);
        s.write_all(&out).unwrap();
    }
    let lst = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = lst.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut s, _) = lst.accept().unwrap();
        frame(&mut s);
        s.write_all(&crate::proto::encode_hello()).unwrap();
        let b = frame(&mut s);
        reply(&mut s, &b[..8]);
        let b = frame(&mut s);
        std::thread::sleep(Duration::from_millis(150));
        reply(&mut s, &b[..8]);
    });
    let t = NetTransport::connect(&[(1, addr)]).with_rpc_deadline(Duration::from_millis(400));
    run(t.read(1, DEFAULT_ZONE, b"warm")).unwrap();
    {
        let mut cancelled = Box::pin(t.read(1, DEFAULT_ZONE, b"withdrawn"));
        assert!(cancelled.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    }
    std::thread::sleep(Duration::from_millis(300));
    let r = run(t.read(1, DEFAULT_ZONE, b"healthy"));
    assert!(r.is_ok(), "a reply 150 ms into a 400 ms deadline must arrive: {r:?}");
    assert_eq!(t.connection_stats()[0].reconnects, 0);
    server.join().unwrap();
}

#[test]
fn a_connection_silent_for_a_deadline_is_replaced_and_the_next_request_succeeds() {
    let ring = Ring::from_members(1, 64, [1]);
    let server = ReplicaServer::spawn(replica(1)).unwrap();
    let proxy = BlackholeProxy::spawn(server);
    let t = NetTransport::connect(&[(1, proxy.addr)]).with_rpc_deadline(Duration::from_millis(300));
    let coord = Coordinator::new(9, ring, 1, t);
    run(coord.write_in(DEFAULT_ZONE, b"k", b"v", WriteConcern::All)).unwrap();

    proxy.hang();
    assert!(run(coord.read_in(DEFAULT_ZONE, b"k")).is_err());
    proxy.forward();
    let got = run(coord.read_in(DEFAULT_ZONE, b"k")).unwrap();
    assert_eq!(got.and_then(|s| s.value), Some(b"v".to_vec()));
    assert_eq!(coord.transport().connection_stats()[0].reconnects, 1);
}

#[test]
fn cancelled_black_holed_hello_releases_all_credits_and_healthy_node_progresses() {
    use std::net::TcpListener;

    let lst = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = lst.local_addr().unwrap();
    let config = ClientConfig {
        connect_deadline: Some(Duration::from_secs(1)),
        rpc_deadline: Some(Duration::from_secs(1)),
        budgets: Some(crate::coord::ClientBudgets {
            max_ops: 1,
            max_bytes: 1024,
            node_max_ops: 1,
            node_max_bytes: 1024,
        }),
    };
    let healthy_addr = ReplicaServer::spawn(replica(2)).unwrap();
    let transport =
        NetTransport::connect_with_config(&[(1, addr), (2, healthy_addr)], config).unwrap();
    let mut ring = Ring::new(1);
    ring.add_node(1);
    let coord = Coordinator::new(9, ring, 1, transport);
    let mut write = Box::pin(coord.write_with(b"key", b"value", WriteConcern::All));
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    assert!(write.as_mut().poll(&mut cx).is_pending());
    assert_eq!(coord.credit_stats().in_flight_ops, 1);
    assert_eq!(
        coord
            .transport()
            .connection_stats()
            .iter()
            .map(|stats| stats.pending)
            .sum::<usize>(),
        1
    );
    drop(write);
    assert_eq!(coord.credit_stats().in_flight_ops, 0);
    assert_eq!(coord.credit_stats().in_flight_bytes, 0);
    assert_eq!(
        coord
            .transport()
            .connection_stats()
            .iter()
            .map(|stats| stats.pending)
            .sum::<usize>(),
        0
    );
    let transport_stats = coord.transport().transport_credit_stats().unwrap();
    assert_eq!(transport_stats.in_flight_ops, 0);
    assert_eq!(transport_stats.in_flight_bytes, 0);
    run(coord.transport().apply(
        2,
        DEFAULT_ZONE,
        b"healthy",
        Some(b"value"),
        crate::hlc::Version { hlc: 1, node: 9 },
    ))
    .unwrap();
}

#[test]
fn transport_overload_does_not_open_failure_detector_or_create_hint() {
    let addr = ReplicaServer::spawn(replica(1)).unwrap();
    let config = ClientConfig {
        connect_deadline: Some(Duration::from_secs(1)),
        rpc_deadline: Some(Duration::from_secs(1)),
        budgets: Some(crate::coord::ClientBudgets {
            max_ops: 8,
            max_bytes: 128,
            node_max_ops: 8,
            node_max_bytes: 128,
        }),
    };
    let transport = NetTransport::connect_with_config(&[(1, addr)], config).unwrap();
    let mut ring = Ring::new(1);
    ring.add_node(1);
    let coord = Coordinator::new(9, ring, 1, transport).with_failure_detector(
        crate::failure::FdConfig {
            failure_threshold: 1,
            open_cooldown_ms: 60_000,
        },
    );

    let oversized = vec![b'x'; 120];
    assert_eq!(
        run(coord.write_with(b"k", &oversized, WriteConcern::All)),
        Err(CoordError::Overloaded)
    );
    assert_eq!(coord.pending_hints(1), 0);
    run(coord.write_with(b"k", b"v", WriteConcern::All)).unwrap();
}

#[test]
fn reply_side_overload_is_per_entry_and_does_not_damage_failure_detection() {
    let addr = ReplicaServer::spawn(replica(1)).unwrap();
    let seed = NetTransport::connect_unbounded(&[(1, addr)]);
    run(seed.apply(
        1,
        DEFAULT_ZONE,
        b"large",
        Some(&vec![b'x'; 800]),
        crate::hlc::Version { hlc: 1, node: 9 },
    ))
    .unwrap();
    drop(seed);

    let config = ClientConfig {
        connect_deadline: Some(Duration::from_secs(1)),
        rpc_deadline: Some(Duration::from_millis(50)),
        budgets: Some(crate::coord::ClientBudgets {
            max_ops: 8,
            max_bytes: 256,
            node_max_ops: 8,
            node_max_bytes: 256,
        }),
    };
    let transport = NetTransport::connect_with_config(&[(1, addr)], config).unwrap();
    let mut ring = Ring::new(1);
    ring.add_node(1);
    let coord = Coordinator::new(9, ring, 1, transport)
        .with_read_fanout(ReadFanout::All)
        .with_failure_detector(crate::failure::FdConfig {
            failure_threshold: 1,
            open_cooldown_ms: 60_000,
        });

    let keys = vec![b"large".to_vec()];
    let result = run(coord.read_many(&keys)).unwrap();
    assert!(matches!(result.as_slice(), [Err(CoordError::Overloaded)]));
    assert!(!coord.is_node_down(1));
    assert_eq!(coord.pending_hints(1), 0);
    run(coord.write_with(b"small", b"v", WriteConcern::All)).unwrap();
    assert!(!coord.is_node_down(1));
    assert_eq!(coord.pending_hints(1), 0);
}

#[test]
fn timed_out_network_write_releases_credits() {
    use std::net::TcpListener;

    let lst = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = lst.local_addr().unwrap();
    let bg = std::thread::spawn(move || {
        if let Ok((stream, _)) = lst.accept() {
            std::thread::sleep(Duration::from_secs(2));
            drop(stream);
        }
    });
    let config = ClientConfig {
        connect_deadline: Some(Duration::from_millis(100)),
        rpc_deadline: Some(Duration::from_millis(100)),
        budgets: Some(crate::coord::ClientBudgets {
            max_ops: 1,
            max_bytes: 1024,
            node_max_ops: 1,
            node_max_bytes: 1024,
        }),
    };
    let healthy_addr = ReplicaServer::spawn(replica(2)).unwrap();
    let transport =
        NetTransport::connect_with_config(&[(1, addr), (2, healthy_addr)], config).unwrap();
    let mut ring = Ring::new(1);
    ring.add_node(1);
    let coord = Coordinator::new(9, ring, 1, transport);
    assert_eq!(
        run(coord.write_with(b"key", b"value", WriteConcern::All)),
        Err(CoordError::QuorumFailed { needed: 1, got: 0 })
    );
    let stats = coord.credit_stats();
    assert_eq!(stats.in_flight_ops, 0);
    assert_eq!(stats.in_flight_bytes, 0);
    assert_eq!(stats.waiters, 0);
    let stats = coord.transport().transport_credit_stats().unwrap();
    assert_eq!(stats.in_flight_ops, 0);
    assert_eq!(stats.in_flight_bytes, 0);
    assert_eq!(stats.waiters, 0);
    run(coord.transport().apply(
        2,
        DEFAULT_ZONE,
        b"healthy",
        Some(b"value"),
        crate::hlc::Version { hlc: 1, node: 9 },
    ))
    .unwrap();
    drop(bg);
}

#[test]
fn rpc_deadline_starts_after_a_completed_handshake() {
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let lst = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = lst.local_addr().unwrap();
    let bg = std::thread::spawn(move || {
        let (mut stream, _) = lst.accept().unwrap();
        let mut header = [0u8; 4];
        stream.read_exact(&mut header).unwrap();
        let len = u32::from_le_bytes(header) as usize;
        let mut hello = vec![0u8; len];
        stream.read_exact(&mut hello).unwrap();
        stream.write_all(&crate::proto::encode_hello()).unwrap();
        std::thread::sleep(Duration::from_secs(2));
    });
    let config = ClientConfig {
        connect_deadline: Some(Duration::from_secs(1)),
        rpc_deadline: Some(Duration::from_millis(100)),
        budgets: None,
    };
    let transport = NetTransport::connect_with_config(&[(1, addr)], config).unwrap();
    let started = Instant::now();
    assert!(matches!(
        run(transport.read(1, DEFAULT_ZONE, b"key")),
        Err(TransportError::Timeout)
    ));
    assert!(started.elapsed() < Duration::from_secs(1));
    drop(bg);
}

#[test]
fn batch_apply_preserves_rpc_timeout_class() {
    use crate::hlc::Version;
    use std::net::TcpListener;

    let lst = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = lst.local_addr().unwrap();
    let bg = std::thread::spawn(move || {
        if let Ok((s, _)) = lst.accept() {
            std::thread::sleep(Duration::from_secs(30));
            drop(s);
        }
    });
    let t = NetTransport::connect(&[(1, addr)]).with_rpc_deadline(Duration::from_millis(150));
    let entry = [BatchEntry {
        key: b"mirror",
        value: Some(b"value"),
        version: Version { hlc: 1, node: 1 },
    }];
    let acks = run(t.apply_batch(1, DEFAULT_ZONE, false, &entry));
    assert_eq!(acks, vec![BatchAck::Timeout]);
    drop(bg);
}

#[test]
fn rpc_deadline_does_not_fail_a_healthy_peer() {
    use crate::hlc::Version;

    let addr = ReplicaServer::spawn(replica(1)).unwrap();
    let t = NetTransport::connect(&[(1, addr)]).with_rpc_deadline(Duration::from_secs(5));
    run(t.apply(
        1,
        DEFAULT_ZONE,
        b"k",
        Some(b"v"),
        Version { hlc: 1, node: 1 },
    ))
    .unwrap();
    let got = run(t.read(1, DEFAULT_ZONE, b"k")).unwrap();
    assert_eq!(got.unwrap().value.as_deref(), Some(&b"v"[..]));
}

fn count_waker() -> (Waker, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    struct W(Arc<AtomicUsize>);
    impl std::task::Wake for W {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let n = Arc::new(AtomicUsize::new(0));
    (Waker::from(Arc::new(W(n.clone()))), n)
}

#[test]
fn a_cancelled_handoff_target_re_hands_the_driver_baton() {
    use std::io::Write;
    use std::sync::atomic::Ordering as AOrd;

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let conn = Rc::new(MuxConn::new(addr));
    struct DrainDirty;
    impl Drop for DrainDirty {
        fn drop(&mut self) {
            flush_dirty();
        }
    }
    let _drain = DrainDirty;

    let (wa, _ca) = count_waker();
    let (wb, cb) = count_waker();
    let (wc, cc) = count_waker();

    let mut a = MuxRpc::new(conn.clone(), b"ra".to_vec());
    let mut b = MuxRpc::new(conn.clone(), b"rb".to_vec());
    let mut c = MuxRpc::new(conn.clone(), b"rc".to_vec());

    assert!(Pin::new(&mut a)
        .poll(&mut Context::from_waker(&wa))
        .is_pending());
    assert!(Pin::new(&mut b)
        .poll(&mut Context::from_waker(&wb))
        .is_pending());
    assert!(Pin::new(&mut c)
        .poll(&mut Context::from_waker(&wc))
        .is_pending());

    let (mut srv, _) = listener.accept().unwrap();
    srv.write_all(&crate::proto::encode_hello()).unwrap();
    let payload = b"done";
    let mut frame = ((8 + payload.len()) as u32).to_le_bytes().to_vec();
    frame.extend_from_slice(&0u64.to_le_bytes());
    frame.extend_from_slice(payload);
    srv.write_all(&frame).unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match Pin::new(&mut a).poll(&mut Context::from_waker(&wa)) {
            Poll::Ready(r) => {
                assert_eq!(r.unwrap(), payload.to_vec());
                break;
            }
            Poll::Pending => {
                assert!(Instant::now() < deadline, "reply never arrived");
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
    let (b_woken, c_woken) = (cb.load(AOrd::SeqCst), cc.load(AOrd::SeqCst));
    assert_eq!(b_woken + c_woken, 1, "exactly one hand-off target expected");

    if b_woken == 1 {
        drop(b);
        assert_eq!(
            cc.load(AOrd::SeqCst),
            1,
            "cancelled hand-off target stranded the remaining waiter"
        );
    } else {
        drop(c);
        assert_eq!(
            cb.load(AOrd::SeqCst),
            1,
            "cancelled hand-off target stranded the remaining waiter"
        );
    }
    drop(srv);
}

#[test]
fn coordinator_refreshes_ring_over_tcp() {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    let addr = ReplicaServer::spawn(replica(1)).unwrap();
    let mut s = TcpStream::connect(addr).unwrap();
    crate::proto::client_handshake_blocking(&mut s).unwrap();
    let payload = wire::encode_request_payload(&Request::SetMembers {
        auth: &[],
        epoch: 0,
        vnodes: 0,
        rf: 0,
        members: &[(1, 1), (2, 1), (3, 1)],
        addrs: &[],
        incarnation: None,
    });
    let len = (8 + payload.len()) as u32;
    let mut frame = len.to_le_bytes().to_vec();
    frame.extend_from_slice(&1u64.to_le_bytes());
    frame.extend_from_slice(&payload);
    s.write_all(&frame).unwrap();
    let mut lenb = [0u8; 4];
    s.read_exact(&mut lenb).unwrap();
    let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
    s.read_exact(&mut block).unwrap();

    let mut ring = Ring::with_vnodes(3, 64);
    ring.set_members([1]);
    let c = Coordinator::new(0, ring, 1, NetTransport::connect(&[(1, addr)]));
    let now = run(c.refresh_ring(1)).unwrap();
    assert_eq!(now, vec![1, 2, 3]);
    assert_eq!(c.ring().members().collect::<Vec<_>>(), vec![1, 2, 3]);
}

#[test]
fn config_epoch_propagates_and_converges_the_ring_over_tcp() {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    fn push(addr: SocketAddr, epoch: u64, vnodes: u32, rf: u32, members: &[(NodeId, u32)]) {
        let mut s = TcpStream::connect(addr).unwrap();
        crate::proto::client_handshake_blocking(&mut s).unwrap();
        let payload = wire::encode_request_payload(&Request::SetMembers {
            auth: &[],
            epoch,
            vnodes,
            rf,
            members,
            addrs: &[],
            incarnation: None,
        });
        let len = (8 + payload.len()) as u32;
        let mut frame = len.to_le_bytes().to_vec();
        frame.extend_from_slice(&1u64.to_le_bytes());
        frame.extend_from_slice(&payload);
        s.write_all(&frame).unwrap();
        let mut lenb = [0u8; 4];
        s.read_exact(&mut lenb).unwrap();
        let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
        s.read_exact(&mut block).unwrap();
    }

    let addr = ReplicaServer::spawn(replica(1)).unwrap();
    push(addr, 5, 64, 3, &[(1, 1), (2, 1), (3, 1)]);

    let c = Coordinator::new(
        0,
        {
            let mut r = Ring::with_vnodes(1, 128);
            r.set_members([1]);
            r
        },
        2,
        NetTransport::connect(&[(1, addr)]),
    );
    let now = run(c.refresh_ring(1)).unwrap();
    assert_eq!(now, vec![1, 2, 3]);
    assert_eq!(
        c.ring_epoch(),
        5,
        "coordinator adopted the authoritative epoch"
    );
    assert_eq!(c.ring().rf(), 3, "coordinator adopted the authoritative rf");

    let reference = Ring::from_weighted(3, 64, [(1, 1), (2, 1), (3, 1)]);
    for i in 0..200u32 {
        let k = format!("k{i}").into_bytes();
        assert_eq!(
            c.ring().replicas(&k),
            reference.replicas(&k),
            "key {i} routes to the identical replica set"
        );
    }

    push(addr, 3, 16, 2, &[(9, 1)]);
    let c2 = Coordinator::new(0, Ring::new(3), 2, NetTransport::connect(&[(1, addr)]));
    let now2 = run(c2.refresh_ring(1)).unwrap();
    assert_eq!(
        now2,
        vec![1, 2, 3],
        "the stale push was ignored; membership is still epoch 5's"
    );
    assert_eq!(c2.ring_epoch(), 5);
}

#[test]
fn stale_coordinator_data_rpc_is_refused_until_it_refreshes() {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    fn push(addr: SocketAddr, epoch: u64, vnodes: u32, rf: u32, members: &[(NodeId, u32)]) {
        let mut s = TcpStream::connect(addr).unwrap();
        crate::proto::client_handshake_blocking(&mut s).unwrap();
        let payload = wire::encode_request_payload(&Request::SetMembers {
            auth: &[],
            epoch,
            vnodes,
            rf,
            members,
            addrs: &[],
            incarnation: None,
        });
        let len = (8 + payload.len()) as u32;
        let mut frame = len.to_le_bytes().to_vec();
        frame.extend_from_slice(&1u64.to_le_bytes());
        frame.extend_from_slice(&payload);
        s.write_all(&frame).unwrap();
        let mut lenb = [0u8; 4];
        s.read_exact(&mut lenb).unwrap();
        let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
        s.read_exact(&mut block).unwrap();
    }

    let addr = ReplicaServer::spawn(replica(1)).unwrap();
    push(addr, 10, 64, 1, &[(1, 1)]);

    let nt = NetTransport::connect(&[(1, addr)]);
    nt.set_route_epoch(5);
    let ring = {
        let mut r = Ring::with_vnodes(1, 64);
        r.set_members([1]);
        r
    };
    let c = Coordinator::new(0, ring, 1, nt).with_manual_refresh();

    assert_eq!(
        run(c.write_with(b"k", b"v", WriteConcern::One)),
        Err(CoordError::StaleRing { epoch: 10 }),
        "a stale coordinator's write is refused, not silently routed"
    );
    assert_eq!(
        run(c.read(b"k")),
        Err(CoordError::StaleRing { epoch: 10 }),
        "a stale coordinator's read is refused too"
    );
    let res = run(c.write_batch(&[(&b"k"[..], &b"v"[..])], WriteConcern::One)).unwrap();
    match &res.outcomes[0] {
        WriteOutcome::Failed { error, .. } => {
            assert_eq!(
                *error,
                CoordError::StaleRing { epoch: 10 },
                "a stale coordinator's batch write surfaces StaleRing per write"
            );
        }
        other => panic!("expected Failed, got {other:?}"),
    }
    assert_eq!(
        c.pending_hints(1),
        0,
        "the healthy stale-refusing replica must not be hinted"
    );

    run(c.refresh_ring(1)).unwrap();
    assert_eq!(c.ring_epoch(), 10);
    run(c.write_with(b"k", b"v", WriteConcern::One)).unwrap();
    assert_eq!(
        run(c.read(b"k")).unwrap().unwrap().value.as_deref(),
        Some(&b"v"[..]),
        "a fresh coordinator routes successfully"
    );

    let nt0 = NetTransport::connect(&[(1, addr)]);
    let ring0 = {
        let mut r = Ring::with_vnodes(1, 64);
        r.set_members([1]);
        r
    };
    let c0 = Coordinator::new(0, ring0, 1, nt0).with_manual_refresh();
    assert_eq!(
        run(c0.write_with(b"k2", b"v2", WriteConcern::One)),
        Err(CoordError::StaleRing { epoch: 10 }),
        "an unrefreshed (epoch-0) coordinator must be refused by a PD-managed replica"
    );
    run(c0.refresh_ring(1)).unwrap();
    run(c0.write_with(b"k2", b"v2", WriteConcern::One))
        .expect("after one refresh the coordinator is a first-class participant");
}

#[test]
fn stale_coordinator_self_heals_by_default() {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    fn push(addr: SocketAddr, epoch: u64, vnodes: u32, rf: u32, members: &[(NodeId, u32)]) {
        let mut s = TcpStream::connect(addr).unwrap();
        crate::proto::client_handshake_blocking(&mut s).unwrap();
        let payload = wire::encode_request_payload(&Request::SetMembers {
            auth: &[],
            epoch,
            vnodes,
            rf,
            members,
            addrs: &[],
            incarnation: None,
        });
        let len = (8 + payload.len()) as u32;
        let mut frame = len.to_le_bytes().to_vec();
        frame.extend_from_slice(&1u64.to_le_bytes());
        frame.extend_from_slice(&payload);
        s.write_all(&frame).unwrap();
        let mut lenb = [0u8; 4];
        s.read_exact(&mut lenb).unwrap();
        let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
        s.read_exact(&mut block).unwrap();
    }

    let addr = ReplicaServer::spawn(replica(1)).unwrap();
    push(addr, 10, 64, 1, &[(1, 1)]);

    let ring = {
        let mut r = Ring::with_vnodes(1, 64);
        r.set_members([1]);
        r
    };
    let c = Coordinator::new(0, ring, 1, NetTransport::connect(&[(1, addr)]));
    run(c.write_with(b"k", b"v", WriteConcern::One))
        .expect("the write self-heals through a refresh, not an error");
    assert_eq!(c.ring_epoch(), 10, "the op adopted the authoritative epoch");
    assert_eq!(
        run(c.read(b"k")).unwrap().unwrap().value.as_deref(),
        Some(&b"v"[..])
    );

    let c2 = {
        let mut r = Ring::with_vnodes(1, 64);
        r.set_members([1]);
        Coordinator::new(0, r, 1, NetTransport::connect(&[(1, addr)]))
    };
    let res = run(c2.write_batch(&[(&b"k2"[..], &b"v2"[..])], WriteConcern::One)).unwrap();
    assert!(
        res.all_ok(),
        "the batch self-heals through a refresh: {:?}",
        res.outcomes
    );
    assert_eq!(c2.ring_epoch(), 10);
}

#[test]
fn weighted_membership_propagates_over_tcp() {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    let addr = ReplicaServer::spawn(replica(1)).unwrap();
    let mut s = TcpStream::connect(addr).unwrap();
    crate::proto::client_handshake_blocking(&mut s).unwrap();
    let payload = wire::encode_request_payload(&Request::SetMembers {
        auth: &[],
        epoch: 0,
        vnodes: 0,
        rf: 0,
        members: &[(1, 1), (2, 3), (3, 1)],
        addrs: &[],
        incarnation: None,
    });
    let len = (8 + payload.len()) as u32;
    let mut frame = len.to_le_bytes().to_vec();
    frame.extend_from_slice(&1u64.to_le_bytes());
    frame.extend_from_slice(&payload);
    s.write_all(&frame).unwrap();
    let mut lenb = [0u8; 4];
    s.read_exact(&mut lenb).unwrap();
    let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
    s.read_exact(&mut block).unwrap();

    let mut ring = Ring::with_vnodes(3, 64);
    ring.set_members([1]);
    let c = Coordinator::new(0, ring, 1, NetTransport::connect(&[(1, addr)]));
    run(c.refresh_ring(1)).unwrap();
    assert_eq!(
        c.ring().member_weight(2),
        3,
        "weight survived the round-trip"
    );
    assert_eq!(c.ring().member_weight(1), 1);
    let mut owns2 = 0;
    let total = 3000;
    for i in 0..total {
        let k = format!("k{i}");
        if c.ring().primary(k.as_bytes()) == Some(2) {
            owns2 += 1;
        }
    }
    assert!(
        owns2 as f64 / total as f64 > 0.45,
        "weight-3 node should own ~3/5 of primaries: {owns2}/{total}"
    );
}

#[test]
fn add_peer_reaches_a_node_after_construction() {
    let a1 = ReplicaServer::spawn(replica(1)).unwrap();
    let a2 = ReplicaServer::spawn(replica(2)).unwrap();
    let nt = NetTransport::connect(&[(1, a1)]);
    assert!(!nt.has_peer(2));
    assert!(nt.add_peer(2, &[a2]));
    assert!(!nt.add_peer(2, &[a2]), "an already-known peer is a no-op");
    assert!(nt.has_peer(2));
    run(nt.members(2)).expect("added peer is reachable");
}

#[test]
fn refresh_ring_learns_a_new_members_address_and_routes_to_it() {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    let a1 = ReplicaServer::spawn(replica(1)).unwrap();
    let a2 = ReplicaServer::spawn(replica(2)).unwrap();

    let addrs: Vec<(NodeId, Vec<SocketAddr>)> = vec![(1, vec![a1]), (2, vec![a2])];
    let mut s = TcpStream::connect(a1).unwrap();
    crate::proto::client_handshake_blocking(&mut s).unwrap();
    let payload = wire::encode_request_payload(&Request::SetMembers {
        auth: &[],
        epoch: 3,
        vnodes: 64,
        rf: 2,
        members: &[(1, 1), (2, 1)],
        addrs: &addrs,
        incarnation: None,
    });
    let len = (8 + payload.len()) as u32;
    let mut frame = len.to_le_bytes().to_vec();
    frame.extend_from_slice(&1u64.to_le_bytes());
    frame.extend_from_slice(&payload);
    s.write_all(&frame).unwrap();
    let mut lenb = [0u8; 4];
    s.read_exact(&mut lenb).unwrap();
    let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
    s.read_exact(&mut block).unwrap();

    let mut ring = Ring::with_vnodes(2, 64);
    ring.set_members([1]);
    let c = Coordinator::new(0, ring, 2, NetTransport::connect(&[(1, a1)]));
    run(c.refresh_ring(1)).unwrap();

    run(c.write_with(b"grow", b"v", WriteConcern::All)).unwrap();
    assert_eq!(
        run(c.read(b"grow")).unwrap().unwrap().value.as_deref(),
        Some(&b"v"[..])
    );
}

#[test]
fn write_read_over_tcp() {
    let c = build(5, 3, 2);
    let v = run(c.write(b"k", b"hello")).unwrap();
    let got = run(c.read(b"k")).unwrap().unwrap();
    assert_eq!(got.value.as_deref(), Some(&b"hello"[..]));
    assert_eq!(got.version, v);
    assert_eq!(run(c.read(b"missing")).unwrap(), None);
}

#[test]
fn single_write_over_tcp_device_persistent_roundtrip() {
    let c = build(5, 3, 2);
    let version = run(c.write_with(
        b"device-key",
        b"device-value",
        WriteOptions::device_durable(WriteConcern::Majority),
    ))
    .unwrap();
    let stored = run(c.read(b"device-key")).unwrap().unwrap();
    assert_eq!(stored.version, version);
    assert_eq!(stored.value.as_deref(), Some(&b"device-value"[..]));
}

#[test]
fn write_batch_over_tcp_durable_roundtrip() {
    let c = build(5, 3, 2);
    let writes: &[(&[u8], &[u8])] = &[
        (b"a", b"1"),
        (b"b", b"2"),
        (b"c", b"3"),
        (b"d", b"4"),
        (b"e", b"5"),
    ];
    let res = run(c.write_batch(
        writes,
        WriteOptions::device_durable(WriteConcern::All),
    ))
    .unwrap();
    assert!(res.all_ok(), "all writes device-durable at RF: {:?}", res.outcomes);
    for &(key, val) in writes {
        assert_eq!(
            run(c.read(key)).unwrap().unwrap().value.as_deref(),
            Some(val),
            "{key:?} roundtrips"
        );
    }
}

#[test]
fn write_batch_over_tcp_reports_partial_failure() {
    let c = build(5, 3, 2);
    let down = 3;
    c.transport().set_down(down, true);
    let writes: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")];
    let res = run(c.write_batch(writes, WriteConcern::All)).unwrap();

    for (i, &(key, _)) in writes.iter().enumerate() {
        let touches_down = c.ring().replicas(key).contains(&down);
        match &res.outcomes[i] {
            WriteOutcome::Ok { .. } => assert!(!touches_down),
            WriteOutcome::Failed { .. } => assert!(touches_down),
        }
    }
    for (i, &(key, val)) in writes.iter().enumerate() {
        if matches!(res.outcomes[i], WriteOutcome::Ok { .. }) {
            assert_eq!(
                run(c.read_quorum(key, 2))
                    .unwrap()
                    .unwrap()
                    .value
                    .as_deref(),
                Some(val)
            );
        }
    }
}

#[test]
fn delete_over_tcp_reads_absent() {
    let c = build(3, 3, 2);
    run(c.write(b"k", b"v")).unwrap();
    run(c.delete(b"k", WriteConcern::All)).unwrap();
    assert_eq!(run(c.read(b"k")).unwrap(), None);
}

#[test]
fn w_plus_r_overlap_over_tcp() {
    let c = build(3, 3, 2);
    let reps = c.ring().replicas(b"k");
    let v = run(c.write_with(b"k", b"v1", WriteConcern::Quorum(2))).unwrap();
    c.transport().set_down(reps[0], true);
    let got = run(c.read_quorum(b"k", 2)).unwrap().unwrap();
    assert_eq!(got.version, v);
    assert_eq!(got.value.as_deref(), Some(&b"v1"[..]));
}

#[test]
fn read_repair_heals_over_tcp() {
    let c = build(3, 3, 2);
    let reps = c.ring().replicas(b"k");
    let laggard = reps[2];

    c.transport().set_down(laggard, true);
    run(c.write_with(b"k", b"fresh", WriteConcern::Quorum(2))).unwrap();

    c.transport().set_down(laggard, false);
    run(c.read_quorum(b"k", 3)).unwrap();

    c.transport().set_down(reps[0], true);
    c.transport().set_down(reps[1], true);
    let got = run(c.read_quorum(b"k", 1)).unwrap().unwrap();
    assert_eq!(got.value.as_deref(), Some(&b"fresh"[..]));
}

#[test]
fn digest_read_falls_back_on_mismatch_over_tcp() {
    let c = build(3, 3, 2);
    let reps = c.ring().replicas(b"k");
    run(c.write_with(b"k", b"v1", WriteConcern::All)).unwrap();

    c.transport().set_down(reps[0], true);
    let v2 = run(c.write_with(b"k", b"v2", WriteConcern::Quorum(2))).unwrap();
    c.transport().set_down(reps[0], false);

    let got = run(c.read_quorum(b"k", 3)).unwrap().unwrap();
    assert_eq!(got.version, v2, "fallback returns the latest version");
    assert_eq!(got.value.as_deref(), Some(&b"v2"[..]));

    c.transport().set_down(reps[1], true);
    c.transport().set_down(reps[2], true);
    let got = run(c.read_quorum(b"k", 1)).unwrap().unwrap();
    assert_eq!(
        got.value.as_deref(),
        Some(&b"v2"[..]),
        "read-repair healed the stale value primary"
    );
}

#[test]
fn digest_read_many_batches_and_falls_back_over_tcp() {
    let c = build(3, 3, 2);
    let reps = c.ring().replicas(b"a");
    run(c.write_with(b"a", b"old", WriteConcern::All)).unwrap();
    run(c.write_with(b"b", b"live", WriteConcern::All)).unwrap();
    run(c.delete(b"b", WriteConcern::All)).unwrap();
    c.transport().set_down(reps[0], true);
    let fresh = run(c.write_with(b"a", b"fresh", WriteConcern::Quorum(2))).unwrap();
    c.transport().set_down(reps[0], false);

    let keys = vec![b"a".to_vec(), b"b".to_vec(), b"a".to_vec(), b"missing".to_vec()];
    let initial_nodes: std::collections::HashSet<_> = keys
        .iter()
        .flat_map(|key| c.ring().replicas(key).into_iter().take(2))
        .collect();
    let stats_before = c.transport().mixed_read_stats();
    let got = run(c.read_many(&keys)).unwrap();
    let stats_after = c.transport().mixed_read_stats();
    for i in [0, 2] {
        let stored = got[i].as_ref().unwrap().as_ref().unwrap();
        assert_eq!(stored.version, fresh);
        assert_eq!(stored.value.as_deref(), Some(&b"fresh"[..]));
    }
    assert_eq!(got[1], Ok(None));
    assert_eq!(got[3], Ok(None));
    assert_eq!(
        stats_after.0 - stats_before.0,
        initial_nodes.len() as u64 + 1,
        "one grouped frame per initial node plus one grouped fallback frame; stats {stats_before:?} -> {stats_after:?}"
    );
    assert_eq!(stats_after.1 - stats_before.1, 0);
}

#[test]
fn mixed_read_featureless_connection_falls_back_to_scalar() {
    let c = build(1, 1, 1);
    run(c.write_with(b"a", b"one", WriteConcern::All)).unwrap();
    run(c.write_with(b"b", b"two", WriteConcern::All)).unwrap();
    assert!(c.transport().override_peer_features(1, 0, 0));
    let before = c.transport().mixed_read_stats();
    let got = run(c.read_many(&[b"a".to_vec(), b"b".to_vec()])).unwrap();
    let after = c.transport().mixed_read_stats();
    assert!(got.iter().all(|result| result.as_ref().unwrap().is_some()));
    assert_eq!(after.0 - before.0, 0);
    assert_eq!(after.1 - before.1, 2);
}

#[test]
fn mixed_read_single_value_above_page_budget_succeeds_over_tcp() {
    let dev = MemBlockDevice::new(4096, 16384);
    let engine = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 4096,
            reserved_blocks: 0,
            ordering: SOrdering::Unsorted,
        },
    );
    let mut store = VersionedStore::new(engine, 1).unwrap();
    let value = vec![6u8; 9 * 1024 * 1024];
    store
        .apply(
            b"wide",
            Some(&value),
            crate::hlc::Version { hlc: 1, node: 1 },
        )
        .unwrap();
    store.flush().unwrap();
    store.engine_mut().rebuild_winner_directory().unwrap();
    let addr = ReplicaServer::spawn(store).unwrap();
    let transport = NetTransport::connect(&[(1, addr)]);
    run(transport.read(1, DEFAULT_ZONE, b"wide")).unwrap();
    let entries = [vals_client::types::MixedReadEntry {
        key: b"wide",
        role: vals_client::types::MixedReadRole::Value,
    }];
    let got = run(transport.read_mixed(1, DEFAULT_ZONE, &entries));
    let Ok(vals_client::types::MixedReadReply::Value(Some(stored))) = &got[0] else {
        panic!("expected the wide value, got {:?}", got[0]);
    };
    assert_eq!(stored.value.as_ref().unwrap().len(), value.len());
}

#[test]
fn mixed_read_paging_uses_one_frame_per_page() {
    let dev = MemBlockDevice::new(4096, 16384);
    let engine = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 4096,
            reserved_blocks: 0,
            ordering: SOrdering::Unsorted,
        },
    );
    let mut store = VersionedStore::new(engine, 1).unwrap();
    let value = vec![5u8; 3 * 1024 * 1024];
    for (i, key) in [b"page-a".as_slice(), b"page-b", b"page-c"]
        .into_iter()
        .enumerate()
    {
        store
            .apply(
                key,
                Some(&value),
                crate::hlc::Version {
                    hlc: i as u64 + 1,
                    node: 1,
                },
            )
            .unwrap();
    }
    store.flush().unwrap();
    store.engine_mut().rebuild_winner_directory().unwrap();
    let addr = ReplicaServer::spawn(store).unwrap();
    let config = ClientConfig {
        connect_deadline: Some(Duration::from_secs(5)),
        rpc_deadline: Some(Duration::from_secs(5)),
        budgets: Some(crate::coord::ClientBudgets {
            max_ops: 3,
            max_bytes: 128 * 1024 * 1024,
            node_max_ops: 3,
            node_max_bytes: 128 * 1024 * 1024,
        }),
    };
    let transport = NetTransport::connect_with_config(&[(1, addr)], config).unwrap();
    run(transport.read(1, DEFAULT_ZONE, b"page-a")).unwrap();
    let entries = [
        vals_client::types::MixedReadEntry {
            key: b"page-a",
            role: vals_client::types::MixedReadRole::Value,
        },
        vals_client::types::MixedReadEntry {
            key: b"page-b",
            role: vals_client::types::MixedReadRole::Value,
        },
        vals_client::types::MixedReadEntry {
            key: b"page-c",
            role: vals_client::types::MixedReadRole::Value,
        },
    ];
    let before = transport.mixed_read_stats();
    let got = run(transport.read_mixed(1, DEFAULT_ZONE, &entries));
    let after = transport.mixed_read_stats();
    assert!(got.iter().all(Result::is_ok));
    assert_eq!(after.0 - before.0, 2);
    assert_eq!(after.1 - before.1, 0);
    assert_eq!(
        transport.transport_credit_stats().unwrap().in_flight_ops,
        0
    );
}

#[test]
fn anti_entropy_reconciles_over_tcp() {
    use crate::antientropy::{reconcile, DEFAULT_BUCKETS};
    use crate::hlc::Version;

    let mut peer_store = replica(2);
    peer_store
        .apply(b"a", Some(b"newer"), Version { hlc: 200, node: 2 })
        .unwrap();
    peer_store
        .apply(b"b", Some(b"only-peer"), Version { hlc: 50, node: 2 })
        .unwrap();
    let peer_addr = ReplicaServer::spawn(peer_store).unwrap();
    let transport = NetTransport::connect(&[(2, peer_addr)]);

    let local = std::cell::RefCell::new(replica(1));
    local
        .borrow_mut()
        .apply(b"a", Some(b"older"), Version { hlc: 100, node: 1 })
        .unwrap();

    let pulled = run(reconcile(&local, &transport.peer(2), DEFAULT_BUCKETS)).unwrap();
    assert_eq!(pulled, 2, "pulled newer 'a' and missing 'b' over TCP");
    assert_eq!(
        local.borrow().get(b"a").unwrap().as_deref(),
        Some(&b"newer"[..])
    );
    assert_eq!(
        local.borrow().get(b"b").unwrap().as_deref(),
        Some(&b"only-peer"[..])
    );
}

fn spawn_zoned_server(node: NodeId, zid: ZoneId, cfg: ZoneConfig) -> SocketAddr {
    use crate::zonestore::{MemZoneStore, ZoneGeom};
    let geom = ZoneGeom {
        block_size: 256,
        seg_blocks: 4,
        reserved_blocks: 0,
        region_blocks: 256,
    };
    let provider = MemZoneStore::new(node, geom);
    let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
    let mut catalog = Catalog::new();
    catalog.insert(zid, cfg);
    ReplicaServer::spawn_zoned(node, zone0, provider, catalog).unwrap()
}

#[test]
fn zone_data_path_over_tcp_isolates_and_honors_rf() {
    let zid = 7;
    let zcfg = ZoneConfig {
        name: "z7".into(),
        rf: 2,
        ordering: Ordering::Unsorted,
        scope: None,
    };
    let mut ring = Ring::with_vnodes(3, 64);
    let mut addrs = Vec::new();
    for node in 1..=3u32 {
        addrs.push((node, spawn_zoned_server(node, zid, zcfg.clone())));
        ring.add_node(node);
    }
    let c = Coordinator::new(0, ring, 2, NetTransport::connect(&addrs));
    run(c.refresh_catalog(1)).unwrap();

    run(c.write_with(b"k", b"in-default", WriteConcern::Quorum(2))).unwrap();
    run(c.write_in(zid, b"k", b"in-z7", WriteConcern::Quorum(2))).unwrap();
    assert_eq!(
        run(c.read(b"k")).unwrap().unwrap().value.as_deref(),
        Some(&b"in-default"[..])
    );
    assert_eq!(
        run(c.read_in(zid, b"k")).unwrap().unwrap().value.as_deref(),
        Some(&b"in-z7"[..])
    );

    assert_eq!(c.replicas_for(zid, b"k").unwrap().len(), 2);
    run(c.write_in(zid, b"k2", b"v2", WriteConcern::All)).unwrap();
    assert_eq!(
        run(c.read_in(zid, b"k2"))
            .unwrap()
            .unwrap()
            .value
            .as_deref(),
        Some(&b"v2"[..])
    );
}

#[test]
fn sorted_zone_range_scan_over_tcp() {
    let zid = 7;
    let zcfg = ZoneConfig {
        name: "sorted".into(),
        rf: 3,
        ordering: Ordering::Sorted,
        scope: None,
    };
    let mut ring = Ring::with_vnodes(3, 64);
    let mut addrs = Vec::new();
    for node in 1..=3u32 {
        addrs.push((node, spawn_zoned_server(node, zid, zcfg.clone())));
        ring.add_node(node);
    }
    let c = Coordinator::new(0, ring, 2, NetTransport::connect(&addrs));
    run(c.refresh_catalog(1)).unwrap();

    for k in ["delta", "alpha", "charlie", "bravo", "echo"] {
        run(c.write_in(zid, k.as_bytes(), k.as_bytes(), WriteConcern::All)).unwrap();
    }
    run(c.delete_in(zid, b"charlie", WriteConcern::All)).unwrap();

    let all = run(c.scan_in(zid, None, None, 2, None)).unwrap();
    let keys: Vec<&[u8]> = all.iter().map(|(k, _)| &k[..]).collect();
    assert_eq!(keys, vec![&b"alpha"[..], b"bravo", b"delta", b"echo"]);
    assert_eq!(all[0].1, b"alpha");

    let mid = run(c.scan_in(zid, Some(b"bravo"), Some(b"echo"), 2, None)).unwrap();
    let mid_keys: Vec<&[u8]> = mid.iter().map(|(k, _)| &k[..]).collect();
    assert_eq!(mid_keys, vec![&b"bravo"[..], b"delta"]);

    c.set_range_map(zid, RangeMap::from_splits([b"c".to_vec()]));
    run(c.write_in(zid, b"foxtrot", b"foxtrot", WriteConcern::All)).unwrap();
    let split = run(c.scan_in(zid, None, None, 2, None)).unwrap();
    let split_keys: Vec<&[u8]> = split.iter().map(|(k, _)| &k[..]).collect();
    assert_eq!(
        split_keys,
        vec![&b"alpha"[..], b"bravo", b"delta", b"echo", b"foxtrot"]
    );
}

#[test]
fn multi_scan_matches_scalar_across_splits_and_rt_pages_over_tcp() {
    let zid = 482;
    let zcfg = ZoneConfig { name: "multi-scan".into(), rf: 3, ordering: Ordering::Sorted, scope: None };
    let mut ring = Ring::with_vnodes(3, 64);
    let mut addrs = Vec::new();
    for node in 1..=3u32 {
        addrs.push((node, spawn_zoned_server(node, zid, zcfg.clone())));
        ring.add_node(node);
    }
    let c = Coordinator::new(0, ring, 2, NetTransport::connect(&addrs));
    run(c.refresh_catalog(1)).unwrap();
    for i in 0..80 {
        let key = format!("k{i:03}");
        run(c.write_in(zid, key.as_bytes(), key.as_bytes(), WriteConcern::All)).unwrap();
    }
    run(c.delete_in(zid, b"k020", WriteConcern::All)).unwrap();
    run(c.delete_range_in(zid, Some(b"k030"), Some(b"k050"), WriteConcern::All)).unwrap();
    run(c.write_in(zid, b"k040", b"new", WriteConcern::All)).unwrap();
    c.set_range_map(zid, RangeMap::from_splits([b"k025".to_vec(), b"k060".to_vec()]));
    let queries = vec![
        (None, None, None),
        (Some(&b"k010"[..]), Some(&b"k070"[..]), Some(27)),
        (Some(&b"k030"[..]), Some(&b"k049"[..]), None),
        (Some(&b"absent"[..]), Some(&b"b"[..]), None),
        (None, None, Some(0)),
    ];
    super::server::set_zone_scan_reply_budget(zid, Some(100));
    let full = run(c.scan_many_in(zid, &queries, 3, false)).unwrap();
    let keys = run(c.scan_many_in(zid, &queries, 3, true)).unwrap();
    super::server::set_zone_scan_reply_budget(zid, None);
    for (i, &(lo, hi, limit)) in queries.iter().enumerate() {
        let expected = if limit == Some(0) { Vec::new() } else { run(c.scan_in(zid, lo, hi, 3, limit)).unwrap() };
        assert_eq!(full[i].iter().map(|(k, s)| (k.clone(), s.value.clone().unwrap())).collect::<Vec<_>>(), expected);
        assert_eq!(keys[i].iter().map(|(k, s)| (k, s.version)).collect::<Vec<_>>(),
            full[i].iter().map(|(k, s)| (k, s.version)).collect::<Vec<_>>());
        assert!(keys[i].iter().all(|(_, s)| s.value.as_deref() == Some(&[])));
    }
    let repeated = vec![(Some(&b"k000"[..]), Some(&b"k005"[..]), Some(2)); 270];
    let before = c.transport().rpc_attempts();
    let pages = run(c.scan_many_in(zid, &repeated, 2, false)).unwrap();
    assert_eq!(pages.len(), 270);
    assert!(pages.iter().all(|p| p.len() == 2));
    assert!(c.transport().rpc_attempts() - before < 30, "requests are batched and chunked");
}

#[test]
fn sorted_scan_merges_replicas_by_lww_over_tcp() {
    let zid = 7;
    let zcfg = ZoneConfig {
        name: "sorted".into(),
        rf: 3,
        ordering: Ordering::Sorted,
        scope: None,
    };
    let mut ring = Ring::with_vnodes(3, 64);
    let mut addrs = Vec::new();
    for node in 1..=3u32 {
        addrs.push((node, spawn_zoned_server(node, zid, zcfg.clone())));
        ring.add_node(node);
    }
    let transport = NetTransport::connect(&addrs);
    let c = Coordinator::new(0, ring, 2, transport);
    run(c.refresh_catalog(1)).unwrap();

    run(c.write_in(zid, b"k", b"v1", WriteConcern::All)).unwrap();
    let reps = c.replicas_for(zid, b"k").unwrap();
    c.transport().set_down(reps[2], true);
    run(c.write_in(zid, b"k", b"v2", WriteConcern::Quorum(2))).unwrap();
    c.transport().set_down(reps[2], false);

    let all = run(c.scan_in(zid, None, None, 3, None)).unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(
        all[0].1, b"v2",
        "newest version wins the cross-replica merge"
    );
}

#[test]
fn dense_range_scan_pages_past_the_reply_budget_over_tcp() {
    use crate::zonestore::{MemZoneStore, ZoneGeom};
    let zid = 7;
    let zcfg = ZoneConfig {
        name: "dense".into(),
        rf: 1,
        ordering: Ordering::Sorted,
        scope: None,
    };
    let geom = ZoneGeom {
        block_size: 4096,
        seg_blocks: 1024,
        reserved_blocks: 0,
        region_blocks: 16384,
    };
    let provider = MemZoneStore::new(1, geom);
    let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
    let mut catalog = Catalog::new();
    catalog.insert(zid, zcfg);
    let addr = ReplicaServer::spawn_zoned(1, zone0, provider, catalog).unwrap();

    let mut ring = Ring::with_vnodes(1, 64);
    ring.add_node(1);
    let c = Coordinator::new(0, ring, 1, NetTransport::connect(&[(1, addr)]));
    run(c.refresh_catalog(1)).unwrap();

    const N: usize = 50;
    const VSIZE: usize = 256 * 1024;
    for i in 0..N {
        let key = format!("k{i:02}").into_bytes();
        let val = vec![b'a' + (i % 26) as u8; VSIZE];
        run(c.write_in(zid, &key, &val, WriteConcern::All)).unwrap();
    }

    let first_page = run(c.transport().scan(1, zid, None, None, 0)).unwrap();
    assert!(first_page.more, "first page must hit the reply budget");
    assert!(first_page.entries.len() < N);
    assert!(!first_page.entries.is_empty());

    let all = run(c.scan_in(zid, None, None, 1, None)).unwrap();
    assert_eq!(all.len(), N);
    for (i, (key, val)) in all.iter().enumerate() {
        assert_eq!(key, format!("k{i:02}").as_bytes());
        assert_eq!(val.len(), VSIZE);
        assert!(val.iter().all(|&b| b == b'a' + (i % 26) as u8));
    }

    let three = run(c.scan_in(zid, None, None, 1, Some(3))).unwrap();
    assert_eq!(three.len(), 3);
    assert_eq!(three[0].0, b"k00");
}

#[test]
fn scan_page_prefetch_overlaps_the_client_merge_over_tcp() {
    use crate::zonestore::{MemZoneStore, ZoneGeom};
    let zid = 7;
    let zcfg = ZoneConfig {
        name: "prefetch".into(),
        rf: 3,
        ordering: Ordering::Sorted,
        scope: None,
    };
    let geom = ZoneGeom {
        block_size: 4096,
        seg_blocks: 1024,
        reserved_blocks: 0,
        region_blocks: 16384,
    };
    let mut ring = Ring::with_vnodes(3, 64);
    let mut addrs = Vec::new();
    for node in 1..=3u32 {
        let provider = MemZoneStore::new(node, geom);
        let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        let mut catalog = Catalog::new();
        catalog.insert(zid, zcfg.clone());
        let addr = ReplicaServer::spawn_zoned(node, zone0, provider, catalog).unwrap();
        addrs.push((node, addr));
        ring.add_node(node);
    }
    let c = Coordinator::new(0, ring, 2, NetTransport::connect(&addrs));
    run(c.refresh_catalog(1)).unwrap();

    const N: usize = 30_000;
    const VSIZE: usize = 1024;
    let keys: Vec<Vec<u8>> = (0..N).map(|i| format!("k{i:06}").into_bytes()).collect();
    let vals: Vec<Vec<u8>> = (0..N)
        .map(|i| {
            let mut v = vec![b'a' + (i % 26) as u8; VSIZE];
            v[..6].copy_from_slice(&format!("v{i:05}").into_bytes());
            v
        })
        .collect();
    for chunk in (0..N).collect::<Vec<_>>().chunks(500) {
        let batch: Vec<(&[u8], &[u8])> = chunk
            .iter()
            .map(|&i| (&keys[i][..], &vals[i][..]))
            .collect();
        run(c.write_batch_in(zid, &batch, WriteConcern::All)).unwrap();
    }

    let first_page = run(c.transport().scan(1, zid, None, None, 0)).unwrap();
    assert!(first_page.more, "first page must hit the reply budget");
    assert!(first_page.entries.len() < N);

    c.set_scan_prefetch(false);
    let t0 = Instant::now();
    let off = run(c.scan_in(zid, None, None, 2, None)).unwrap();
    let off_ms = t0.elapsed().as_secs_f64() * 1e3;

    c.set_scan_prefetch(true);
    let t1 = Instant::now();
    let on = run(c.scan_in(zid, None, None, 2, None)).unwrap();
    let on_ms = t1.elapsed().as_secs_f64() * 1e3;

    assert_eq!(on.len(), N);
    assert_eq!(
        on, off,
        "prefetched scan is byte-identical to the serial path"
    );

    eprintln!(
        "scan prefetch over TCP: off={off_ms:.1}ms on={on_ms:.1}ms ratio={:.2}",
        on_ms / off_ms.max(1e-9)
    );
    assert!(
        on_ms <= off_ms * 1.5 + 5.0,
        "prefetch regressed the scan: off={off_ms:.1}ms on={on_ms:.1}ms"
    );
}

#[test]
fn anti_entropy_pages_buckets_past_the_reply_budget_over_tcp() {
    let big_store = |node: u32| {
        let dev = MemBlockDevice::new(4096, 16384);
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 1024,
                reserved_blocks: 0,
                ordering: SOrdering::Unsorted,
            },
        );
        VersionedStore::new(engine, node).unwrap()
    };
    const N: usize = 50;
    const VSIZE: usize = 256 * 1024;
    let mut peer_store = big_store(1);
    for i in 0..N {
        let key = format!("k{i:02}").into_bytes();
        let val = vec![b'a' + (i % 26) as u8; VSIZE];
        peer_store
            .apply(
                &key,
                Some(&val),
                crate::hlc::Version {
                    hlc: 100 + i as u64,
                    node: 1,
                },
            )
            .unwrap();
    }
    peer_store
        .apply(b"dead", None, crate::hlc::Version { hlc: 500, node: 1 })
        .unwrap();
    let addr = ReplicaServer::spawn(peer_store).unwrap();

    let transport = NetTransport::connect(&[(1, addr)]);
    let peer = transport.peer(1);

    let all_ids: Vec<u32> = (0..crate::antientropy::DEFAULT_BUCKETS).collect();
    let (digests, next) = run(peer.bucket_digests(
        DEFAULT_ZONE,
        crate::antientropy::DEFAULT_BUCKETS,
        &all_ids,
        None,
    ))
    .unwrap();
    assert!(next.is_none(), "all {} digests fit one page", N + 1);
    assert_eq!(digests.len(), N + 1);

    let keys: Vec<Vec<u8>> = digests.iter().map(|(k, _)| k.clone()).collect();
    let (first, more) = run(peer.values(DEFAULT_ZONE, &keys)).unwrap();
    assert!(more, "first value page must hit the reply budget");
    assert!(first.len() < N + 1);

    let local = RefCell::new(big_store(2));
    let applied = run(crate::antientropy::reconcile(
        &local,
        &peer,
        crate::antientropy::DEFAULT_BUCKETS,
    ))
    .unwrap();
    assert_eq!(applied, N + 1, "every entry incl. the tombstone pulled");
    let local = local.borrow();
    for i in 0..N {
        let key = format!("k{i:02}").into_bytes();
        let got = local.get(&key).unwrap().unwrap();
        assert_eq!(got.len(), VSIZE);
        assert!(got.iter().all(|&b| b == b'a' + (i % 26) as u8));
    }
    assert!(local.get(b"dead").unwrap().is_none(), "tombstone wins");
}

fn spawn_sorted_shards(node: NodeId, zid: ZoneId, nshards: usize) -> Vec<SocketAddr> {
    use crate::zonestore::{MemZoneStore, ZoneGeom};
    let geom = ZoneGeom {
        block_size: 256,
        seg_blocks: 4,
        reserved_blocks: 0,
        region_blocks: 4096,
    };
    let mut catalog = Catalog::new();
    catalog.insert(
        zid,
        ZoneConfig {
            name: "sorted".into(),
            rf: 1,
            ordering: Ordering::Sorted,
            scope: None,
        },
    );
    (0..nshards)
        .map(|_| {
            let provider = MemZoneStore::new(node, geom);
            let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
            ReplicaServer::spawn_zoned(node, zone0, provider, catalog.clone()).unwrap()
        })
        .collect()
}

fn raw_shard_keys(addr: SocketAddr, zone: ZoneId) -> Vec<Vec<u8>> {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    let payload = wire::encode_request_payload(&Request::Scan {
        epoch: 0,
        zone_id: zone,
        lo: None,
        hi: None,
        max: 0,
    });
    let len = (8 + payload.len()) as u32;
    let mut frame = len.to_le_bytes().to_vec();
    frame.extend_from_slice(&1u64.to_le_bytes());
    frame.extend_from_slice(&payload);
    let mut s = TcpStream::connect(addr).unwrap();
    crate::proto::client_handshake_blocking(&mut s).unwrap();
    s.write_all(&frame).unwrap();
    let mut lenb = [0u8; 4];
    s.read_exact(&mut lenb).unwrap();
    let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
    s.read_exact(&mut block).unwrap();
    match wire::decode_response(&block[8..]).unwrap() {
        Response::Scan { entries, .. } => entries.into_iter().map(|(k, _)| k).collect(),
        other => panic!("expected scan reply, got {other:?}"),
    }
}

#[test]
fn sorted_zone_scan_is_complete_on_a_multi_shard_node() {
    let zid = 7;
    let shards = spawn_sorted_shards(1, zid, 4);
    let mut ring = Ring::with_vnodes(1, 64);
    ring.set_members([1]);
    let c = Coordinator::new(0, ring, 1, NetTransport::connect_sharded([(1, shards)]));
    run(c.refresh_catalog(1)).unwrap();

    const N: usize = 300;
    for i in 0..N {
        let k = format!("k{i:04}").into_bytes();
        run(c.write_in(zid, &k, &k, WriteConcern::All)).unwrap();
    }
    let all = run(c.scan_in(zid, None, None, 1, None)).unwrap();
    assert_eq!(
        all.len(),
        N,
        "a sorted-zone scan must see every key even on a multi-shard node"
    );
    for (i, (k, v)) in all.iter().enumerate() {
        let want = format!("k{i:04}").into_bytes();
        assert_eq!(k, &want, "ascending order");
        assert_eq!(v, &want, "value intact");
    }
}

#[test]
fn mixed_read_groups_one_frame_per_target_shard() {
    let zid = 7;
    const NSHARDS: usize = 4;
    let shards = spawn_sorted_shards(1, zid, NSHARDS);
    let mut ring = Ring::with_vnodes(1, 64);
    ring.set_members([1]);
    let c = Coordinator::new(
        0,
        ring,
        1,
        NetTransport::connect_sharded([(1, shards)]),
    );
    run(c.refresh_catalog(1)).unwrap();
    let map = RangeMap::from_splits([
        b"g".to_vec(),
        b"n".to_vec(),
        b"t".to_vec(),
    ]);
    c.set_range_map(zid, map.clone());
    let keys = vec![
        b"alpha".to_vec(),
        b"hotel".to_vec(),
        b"oscar".to_vec(),
        b"zulu".to_vec(),
    ];
    for key in &keys {
        run(c.write_in(zid, key, key, WriteConcern::All)).unwrap();
    }
    let target_shards: std::collections::HashSet<_> = keys
        .iter()
        .map(|key| crate::partition::data_shard(Some(&map), key, NSHARDS))
        .collect();
    assert!(target_shards.len() >= 2);
    let before = c.transport().mixed_read_stats();
    let got = run(c.read_many_in(zid, &keys)).unwrap();
    let after = c.transport().mixed_read_stats();
    assert!(got.iter().all(|result| result.as_ref().unwrap().is_some()));
    assert_eq!(after.0 - before.0, target_shards.len() as u64);
    assert_eq!(after.1 - before.1, 0);
    let queries: Vec<_> = keys.iter().map(|k| (Some(k.as_slice()), None, Some(1))).collect();
    let before = c.transport().rpc_attempts();
    let got = run(c.scan_many_in(zid, &queries, 1, false)).unwrap();
    assert_eq!(c.transport().rpc_attempts()-before, target_shards.len() as u64);
    assert_eq!(got.iter().map(|p| p[0].0.clone()).collect::<Vec<_>>(), keys);
    for &shard in &target_shards {
        assert!(c.transport().override_peer_features(1, shard, crate::proto::FEATURES & !crate::proto::FEATURE_MULTI_SCAN));
        let fallback = run(c.scan_many_in(zid, &queries, 1, false)).unwrap();
        assert_eq!(fallback, got, "mixed-version per-shard fallback");
    }
}

#[test]
fn low_op_budget_multi_shard_values_read_completes_and_preserves_duplicates() {
    let zid = 7;
    let shards = spawn_sorted_shards(1, zid, 4);
    let config = ClientConfig {
        connect_deadline: Some(Duration::from_secs(1)),
        rpc_deadline: Some(Duration::from_secs(1)),
        budgets: Some(crate::coord::ClientBudgets {
            max_ops: 2,
            max_bytes: 1024 * 1024,
            node_max_ops: 2,
            node_max_bytes: 1024 * 1024,
        }),
    };
    let mut ring = Ring::with_vnodes(1, 64);
    ring.set_members([1]);
    let c = Coordinator::new(
        0,
        ring,
        1,
        NetTransport::connect_sharded_with_config([(1, shards)], config).unwrap(),
    )
    .with_read_fanout(crate::coord::ReadFanout::All);
    run(c.refresh_catalog(1)).unwrap();
    c.set_range_map(
        zid,
        RangeMap::from_splits([b"g".to_vec(), b"n".to_vec(), b"t".to_vec()]),
    );
    run(c.write_in(zid, b"alpha", b"one", WriteConcern::All)).unwrap();
    run(c.write_in(zid, b"zulu", b"two", WriteConcern::All)).unwrap();

    let keys = vec![b"alpha".to_vec(), b"alpha".to_vec(), b"zulu".to_vec()];
    let got = run(c.read_many_in(zid, &keys)).unwrap();
    assert_eq!(
        got.into_iter()
            .map(|result| result.unwrap().unwrap().value.unwrap())
            .collect::<Vec<_>>(),
        vec![b"one".to_vec(), b"one".to_vec(), b"two".to_vec()]
    );
    let stats = c.transport().transport_credit_stats().unwrap();
    assert_eq!(stats.in_flight_ops, 0);
}

#[test]
fn sorted_zone_splits_spread_ranges_across_a_nodes_shards() {
    let zid = 7;
    const NSHARDS: usize = 4;
    let shard_addrs = spawn_sorted_shards(1, zid, NSHARDS);
    let mut ring = Ring::with_vnodes(1, 64);
    ring.set_members([1]);
    let c = Coordinator::new(
        0,
        ring,
        1,
        NetTransport::connect_sharded([(1, shard_addrs.clone())]),
    );
    run(c.refresh_catalog(1)).unwrap();

    const N: usize = 400;
    let splits: Vec<Vec<u8>> = (1..8)
        .map(|r| format!("k{:04}", r * 50).into_bytes())
        .collect();
    let map = RangeMap::from_splits(splits);
    c.set_range_map(zid, map.clone());
    for i in 0..N {
        let k = format!("k{i:04}").into_bytes();
        run(c.write_in(zid, &k, &k, WriteConcern::All)).unwrap();
    }

    let all = run(c.scan_in(zid, None, None, 1, None)).unwrap();
    assert_eq!(all.len(), N, "scan reassembles all ranges");
    for (i, (k, _)) in all.iter().enumerate() {
        assert_eq!(k, &format!("k{i:04}").into_bytes(), "globally sorted");
    }

    let mut shards_used = std::collections::BTreeSet::new();
    let mut total = 0;
    for (s, &addr) in shard_addrs.iter().enumerate() {
        let keys = raw_shard_keys(addr, zid);
        if !keys.is_empty() {
            shards_used.insert(s);
        }
        total += keys.len();
        for k in &keys {
            let token = map.range_token(k);
            let want = (crate::ring::hash64(token) % NSHARDS as u64) as usize;
            assert_eq!(
                s, want,
                "key {k:?} (token {token:?}) must live on shard {want}, found on {s}"
            );
        }
    }
    assert_eq!(total, N, "every key is on exactly one shard");
    assert!(
        shards_used.len() > 1,
        "a split sorted zone must spread its ranges across multiple shards \
             (intra-node drive spread); used {shards_used:?}"
    );
}

fn push_range_map(addr: SocketAddr, epoch: u64, zone_id: ZoneId, splits: &[Vec<u8>]) {
    let map = RangeMap::from_splits(splits.to_vec());
    push_range_map_full(addr, epoch, zone_id, &map);
}

fn push_range_map_full(addr: SocketAddr, epoch: u64, zone_id: ZoneId, map: &RangeMap) {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    let maps = [wire::WireRangeMap {
        zone_id,
        splits: map.splits(),
        tokens: map.tokens(),
        pending: map.pending(),
        placement_debt: map.placement_debt(),
        opaque_tokens: map.supports_opaque_tokens(),
    }];
    let payload = wire::encode_request_payload(&Request::SetRangeMaps {
        auth: &[],
        epoch,
        maps: &maps,
        incarnation: None,
    });
    let len = (8 + payload.len()) as u32;
    let mut frame = len.to_le_bytes().to_vec();
    frame.extend_from_slice(&1u64.to_le_bytes());
    frame.extend_from_slice(&payload);
    let mut s = TcpStream::connect(addr).unwrap();
    crate::proto::client_handshake_blocking(&mut s).unwrap();
    s.write_all(&frame).unwrap();
    let mut lenb = [0u8; 4];
    s.read_exact(&mut lenb).unwrap();
    let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
    s.read_exact(&mut block).unwrap();
}

fn rpc_request(addr: SocketAddr, request: &Request) -> Response {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    let payload = wire::encode_request_payload(request);
    let mut frame = ((8 + payload.len()) as u32).to_le_bytes().to_vec();
    frame.extend_from_slice(&1u64.to_le_bytes());
    frame.extend_from_slice(&payload);
    let mut stream = TcpStream::connect(addr).unwrap();
    crate::proto::client_handshake_blocking(&mut stream).unwrap();
    stream.write_all(&frame).unwrap();
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).unwrap();
    let mut block = vec![0u8; u32::from_le_bytes(len) as usize];
    stream.read_exact(&mut block).unwrap();
    wire::decode_response(&block[8..]).unwrap()
}

fn push_members_full(addr: SocketAddr, epoch: u64, peers: &[(NodeId, SocketAddr)]) {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    let members: Vec<(NodeId, u32)> = peers.iter().map(|(node, _)| (*node, 1)).collect();
    let addrs: Vec<(NodeId, Vec<SocketAddr>)> = peers.iter()
        .map(|(node, addr)| (*node, vec![*addr])).collect();
    let payload = wire::encode_request_payload(&Request::SetMembers {
        auth: &[],
        epoch,
        vnodes: 64,
        rf: 1,
        members: &members,
        addrs: &addrs,
        incarnation: None,
    });
    let mut frame = ((8 + payload.len()) as u32).to_le_bytes().to_vec();
    frame.extend_from_slice(&1u64.to_le_bytes());
    frame.extend_from_slice(&payload);
    let mut stream = TcpStream::connect(addr).unwrap();
    crate::proto::client_handshake_blocking(&mut stream).unwrap();
    stream.write_all(&frame).unwrap();
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).unwrap();
    let mut block = vec![0u8; u32::from_le_bytes(len) as usize];
    stream.read_exact(&mut block).unwrap();
}

fn range_stats(addr: SocketAddr) -> (u64, Vec<wire::OwnedRangeStat>) {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    let payload = wire::encode_request_payload(&Request::RangeStats);
    let mut frame = ((8 + payload.len()) as u32).to_le_bytes().to_vec();
    frame.extend_from_slice(&1u64.to_le_bytes());
    frame.extend_from_slice(&payload);
    let mut stream = TcpStream::connect(addr).unwrap();
    crate::proto::client_handshake_blocking(&mut stream).unwrap();
    stream.write_all(&frame).unwrap();
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).unwrap();
    let mut block = vec![0u8; u32::from_le_bytes(len) as usize];
    stream.read_exact(&mut block).unwrap();
    match wire::decode_response(&block[8..]).unwrap() {
        Response::RangeStats { epoch, ranges } => (epoch, ranges),
        other => panic!("expected RangeStats, got {other:?}"),
    }
}

#[test]
fn rehome_real_transport_copy_mirror_cutover_is_zero_gap() {
    use crate::node::Node;
    use crate::zonestore::{MemZoneStore, ZoneGeom};

    let zone_id = 7;
    let cfg = ZoneConfig {
        name: "rehome".into(),
        rf: 1,
        ordering: Ordering::Sorted,
        scope: None,
    };
    let ring = Ring::from_members(1, 64, [1, 2, 3]);
    let binds: Vec<_> = (0..3).map(|_| Node::bind().unwrap()).collect();
    let peers: Vec<(NodeId, SocketAddr)> = binds.iter().enumerate()
        .map(|(i, (_, addr))| ((i + 1) as NodeId, *addr)).collect();
    let mut nodes = Vec::new();
    for (i, (listener, _)) in binds.into_iter().enumerate() {
        let node = (i + 1) as NodeId;
        let geom = ZoneGeom {
            block_size: 256,
            seg_blocks: 4,
            reserved_blocks: 0,
            region_blocks: 1024,
        };
        let provider = MemZoneStore::new(node, geom);
        let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        let mut catalog = Catalog::new();
        catalog.insert(zone_id, cfg.clone());
        nodes.push(crate::testutil::running(Node::serve_zoned(
            node,
            listener,
            zone0,
            provider,
            catalog,
            ring.clone(),
            peers.clone(),
            Duration::from_millis(50),
            None,
        )));
    }
    for (_, addr) in &peers {
        push_members_full(*addr, 1, &peers);
    }
    let coordinator = Coordinator::new(
        99,
        ring.clone(),
        1,
        NetTransport::connect(&peers),
    );
    run(coordinator.refresh_catalog(1)).unwrap();
    let mut boundary = b"m".to_vec();
    while ring.replicas_in_zone(zone_id, b"", 1, None)
        == ring.replicas_in_zone(zone_id, &boundary, 1, None)
    {
        boundary.push(b'x');
    }
    let current = ring.replicas_in_zone(zone_id, b"", 1, None)[0];
    let pending = ring.replicas_in_zone(zone_id, &boundary, 1, None)[0];
    assert_ne!(current, pending);

    let initial: Vec<Vec<u8>> = (0..80).map(|i| {
        let mut key = boundary.clone();
        key.extend_from_slice(format!("/{i:04}").as_bytes());
        key
    }).collect();
    for key in &initial {
        run(coordinator.write_in(zone_id, key, b"initial", WriteConcern::All)).unwrap();
    }
    let mut prepared = RangeMap::single();
    assert!(prepared.split(&boundary));
    assert!(prepared.set_pending(1, boundary.clone()));
    for (_, addr) in &peers {
        push_range_map_full(*addr, 10, zone_id, &prepared);
    }
    coordinator.set_range_map(zone_id, prepared.clone());

    let mut acked: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = initial.iter()
        .map(|key| (key.clone(), b"initial".to_vec())).collect();
    let writer_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer_stop_rx = writer_stop.clone();
    let (acked_tx, acked_rx) = std::sync::mpsc::channel();
    let writer_ring = ring.clone();
    let writer_peers = peers.clone();
    let writer_map = prepared.clone();
    let writer_keys = initial.clone();
    let writer = std::thread::spawn(move || {
        let coordinator = Coordinator::new(
            100,
            writer_ring,
            1,
            NetTransport::connect(&writer_peers),
        );
        coordinator.set_range_map(zone_id, writer_map);
        let mut round = 0u32;
        while !writer_stop_rx.load(std::sync::atomic::Ordering::Acquire) {
            let key = writer_keys[round as usize % writer_keys.len()].clone();
            let value = format!("v{round:05}").into_bytes();
            if run(coordinator.write_in(
                zone_id, &key, &value, WriteConcern::All
            )).is_ok() {
                if acked_tx.send((key, value)).is_err() {
                    return;
                }
                round += 1;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    });

    let deadline = Instant::now() + Duration::from_secs(20);
    let mut passes = 0u32;
    loop {
        while let Ok((key, value)) = acked_rx.try_recv() {
            acked.insert(key, value);
        }
        if passes % 3 == 0 {
            for key in acked.keys().take(8) {
                let got = run(coordinator.read_in(zone_id, key)).unwrap().unwrap();
                assert!(got.value.is_some(), "acked key {key:?} returned not-found");
            }
        }
        let (_, pending_stats) = range_stats(peers[(pending - 1) as usize].1);
        let copied = pending_stats.iter().any(|stat| {
            stat.zone_id == zone_id
                && stat.lower_bound == boundary
                && stat.rehome_copy_complete_epoch >= 10
        });
        let (_, current_stats) = range_stats(peers[(current - 1) as usize].1);
        let clean = current_stats.iter().any(|stat| {
            stat.zone_id == zone_id
                && stat.lower_bound == boundary
                && stat.rehome_mirror_epoch >= 10
                && stat.rehome_mirror_failures == 0
        });
        if copied && clean && acked.len() >= initial.len() {
            break;
        }
        assert!(Instant::now() < deadline, "real re-home did not complete: pending={pending_stats:?} current={current_stats:?}");
        passes += 1;
        std::thread::sleep(Duration::from_millis(10));
    }

    let mut committed = prepared;
    assert!(committed.commit_pending(1));
    for (_, addr) in &peers {
        push_range_map_full(*addr, 11, zone_id, &committed);
    }
    coordinator.set_range_map(zone_id, committed);
    writer_stop.store(true, std::sync::atomic::Ordering::Release);
    writer.join().unwrap();
    while let Ok((key, value)) = acked_rx.try_recv() {
        acked.insert(key, value);
    }
    for _ in 0..4 {
        std::thread::sleep(Duration::from_millis(70));
        for (key, value) in &acked {
            let got = run(coordinator.read_in(zone_id, key)).unwrap().unwrap();
            assert_eq!(got.value.as_deref(), Some(value.as_slice()), "key {key:?}");
        }
    }
    let scan = run(coordinator.scan_in(zone_id, Some(&boundary), None, 1, None)).unwrap();
    let scanned: std::collections::BTreeMap<_, _> = scan.into_iter().collect();
    for (key, value) in acked {
        assert_eq!(scanned.get(&key), Some(&value));
    }
    let pending_keys = run(coordinator.scan_in(
        zone_id, Some(&boundary), None, 1, None
    )).unwrap();
    assert!(initial.iter().all(|key| pending_keys.iter().any(|(got, _)| got == key)));
    drop(nodes);
}

#[test]
fn range_delete_over_the_wire_hides_keys_fences_foreign_spans_and_mirrors_to_pending_owner() {
    use crate::hlc::Version;
    use crate::node::Node;
    use crate::replica::{Applied, Stored};
    use crate::zonestore::{MemZoneStore, ZoneGeom};

    let zone_id = 7;
    let cfg = ZoneConfig { name: "rt".into(), rf: 1, ordering: Ordering::Sorted, scope: None };
    let ring = Ring::from_members(1, 64, [1, 2, 3]);
    let binds: Vec<_> = (0..3).map(|_| Node::bind().unwrap()).collect();
    let peers: Vec<(NodeId, SocketAddr)> = binds.iter().enumerate()
        .map(|(i, (_, addr))| ((i + 1) as NodeId, *addr)).collect();
    let mut nodes = Vec::new();
    for (i, (listener, _)) in binds.into_iter().enumerate() {
        let node = (i + 1) as NodeId;
        let provider = MemZoneStore::new(node, ZoneGeom {
            block_size: 256,
            seg_blocks: 4,
            reserved_blocks: 0,
            region_blocks: 1024,
        });
        let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        let mut catalog = Catalog::new();
        catalog.insert(zone_id, cfg.clone());
        nodes.push(crate::testutil::running(Node::serve_zoned(
            node, listener, zone0, provider, catalog, ring.clone(), peers.clone(),
            Duration::from_millis(50), None,
        )));
    }
    for (_, addr) in &peers {
        push_members_full(*addr, 1, &peers);
    }
    let coordinator = Coordinator::new(99, ring.clone(), 1, NetTransport::connect(&peers));
    run(coordinator.refresh_catalog(1)).unwrap();
    let mut boundary = b"m".to_vec();
    while ring.replicas_in_zone(zone_id, b"", 1, None)
        == ring.replicas_in_zone(zone_id, &boundary, 1, None)
    {
        boundary.push(b'x');
    }
    let current = ring.replicas_in_zone(zone_id, b"", 1, None)[0];
    let pending = ring.replicas_in_zone(zone_id, &boundary, 1, None)[0];
    let other = (1..=3).find(|n| *n != current && *n != pending).unwrap();
    let addr_of = |node: NodeId| peers[(node - 1) as usize].1;
    let key = |i: u32| [boundary.as_slice(), format!("/{i:04}").as_bytes()].concat();
    for i in 0..40 {
        run(coordinator.write_in(zone_id, &key(i), b"old", WriteConcern::All)).unwrap();
    }
    run(coordinator.write_in(zone_id, b"a", b"old", WriteConcern::All)).unwrap();
    let mut prepared = RangeMap::single();
    assert!(prepared.split(&boundary));
    assert!(prepared.set_pending(1, boundary.clone()));
    for (_, addr) in &peers {
        push_range_map_full(*addr, 10, zone_id, &prepared);
    }

    let version = Version { hlc: (crate::hlc::physical_now_ms() + 5) << 16, node: 99 };
    let (lo, hi) = (key(10), key(30));
    let slices: Vec<wire::WireSlice> =
        vec![(None, Some(b"b".as_slice())), (Some(lo.as_slice()), Some(hi.as_slice()))];
    let range_delete = |node: NodeId, sync: bool, slices: &[wire::WireSlice]| {
        rpc_request(addr_of(node), &Request::RangeDelete { epoch: 1, zone_id, sync, version, slices })
    };
    assert!(matches!(range_delete(other, false, &slices), Response::StaleEpoch(_)));
    let read = |node: NodeId, k: &[u8]| match rpc_request(
        addr_of(node),
        &Request::Read { epoch: wire::INSPECT_EPOCH, zone_id, key: k },
    ) {
        Response::Read(stored) => stored,
        other => panic!("read failed: {other:?}"),
    };
    assert_eq!(read(other, &key(20)), None, "a fenced request applied nothing");
    assert_eq!(
        range_delete(current, true, &slices),
        Response::Batch(vec![Applied::Won, Applied::Won])
    );
    let tombstone = Some(Stored { version, value: None });
    for node in [current, pending] {
        assert_eq!(read(node, &key(10)), tombstone, "node {node}");
        assert_eq!(read(node, &key(29)), tombstone, "node {node}");
        assert_eq!(read(node, b"z-uncovered"), None);
        assert_eq!(read(node, &[key(15), b"/new".to_vec()].concat()), tombstone, "node {node}");
    }
    assert_eq!(read(current, b"a"), tombstone);
    assert_eq!(read(current, &key(30)).unwrap().value.as_deref(), Some(b"old" as &[u8]));
    let (_, stats) = range_stats(addr_of(current));
    assert!(stats.iter().any(|stat| stat.zone_id == zone_id
        && stat.lower_bound == boundary
        && stat.rehome_mirror_ok >= 1
        && stat.rehome_mirror_failures == 0), "{stats:?}");
    let empty = (Some(hi.as_slice()), Some(lo.as_slice()));
    assert!(matches!(range_delete(current, false, &[empty]), Response::Err(_)));
    drop(nodes);
}

#[test]
fn coordinator_range_delete_over_net_transport_hides_older_keys_and_newer_writes_win() {
    use crate::node::Node;
    use crate::zonestore::{MemZoneStore, ZoneGeom};

    let zone_id = 7;
    let cfg = ZoneConfig { name: "rt".into(), rf: 3, ordering: Ordering::Sorted, scope: None };
    let ring = Ring::from_members(3, 64, [1, 2, 3]);
    let binds: Vec<_> = (0..3).map(|_| Node::bind().unwrap()).collect();
    let peers: Vec<(NodeId, SocketAddr)> = binds.iter().enumerate()
        .map(|(i, (_, addr))| ((i + 1) as NodeId, *addr)).collect();
    let mut nodes = Vec::new();
    for (i, (listener, _)) in binds.into_iter().enumerate() {
        let node = (i + 1) as NodeId;
        let provider = MemZoneStore::new(node, ZoneGeom {
            block_size: 256,
            seg_blocks: 4,
            reserved_blocks: 0,
            region_blocks: 1024,
        });
        let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        let mut catalog = Catalog::new();
        catalog.insert(zone_id, cfg.clone());
        nodes.push(crate::testutil::running(Node::serve_zoned(
            node, listener, zone0, provider, catalog, ring.clone(), peers.clone(),
            Duration::from_millis(50), None,
        )));
    }
    for (_, addr) in &peers {
        push_members_full(*addr, 1, &peers);
    }
    let coordinator = Coordinator::new(99, ring.clone(), 2, NetTransport::connect(&peers));
    run(coordinator.refresh_catalog(1)).unwrap();
    let key = |i: u32| format!("k/{i:03}").into_bytes();
    for i in 0..40 {
        run(coordinator.write_in(zone_id, &key(i), b"old", WriteConcern::All)).unwrap();
    }
    let version = run(coordinator.delete_range_in(
        zone_id, Some(&key(10)), Some(&key(30)), WriteOptions::device_durable(WriteConcern::All),
    )).unwrap();
    let newer = run(coordinator.write_in(zone_id, &key(20), b"new", WriteConcern::All)).unwrap();
    assert!(newer > version);
    let got = run(coordinator.scan_in(zone_id, None, None, 2, None)).unwrap();
    let want: Vec<(Vec<u8>, Vec<u8>)> = (0..10)
        .map(|i| (key(i), b"old".to_vec()))
        .chain(std::iter::once((key(20), b"new".to_vec())))
        .chain((30..40).map(|i| (key(i), b"old".to_vec())))
        .collect();
    assert_eq!(got, want);
    assert_eq!(run(coordinator.read_in(zone_id, &key(15))).unwrap(), None);
    assert_eq!(
        run(coordinator.delete_range_in(zone_id, Some(&key(5)), Some(&key(5)), WriteConcern::All)),
        Err(CoordError::InvalidRange)
    );
    assert_eq!(
        run(coordinator.delete_range_in(DEFAULT_ZONE, None, None, WriteConcern::All)),
        Err(CoordError::NotSorted(DEFAULT_ZONE))
    );
    drop(nodes);
}

struct BlackholeProxy {
    addr: SocketAddr,
    mode: Arc<std::sync::atomic::AtomicU8>,
    clients: Arc<std::sync::Mutex<Vec<std::net::TcpStream>>>,
}

impl BlackholeProxy {
    const FORWARD: u8 = 0;
    const HANG: u8 = 1;

    fn spawn(target: SocketAddr) -> Self {
        use std::io::{Read, Write};
        use std::net::{TcpListener, TcpStream};
        use std::sync::atomic::{AtomicU8, Ordering::SeqCst};

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mode = Arc::new(AtomicU8::new(Self::FORWARD));
        let clients = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (m, c) = (mode.clone(), clients.clone());
        std::thread::spawn(move || {
            for client in listener.incoming() {
                let Ok(client) = client else { break };
                c.lock().unwrap().push(client.try_clone().unwrap());
                if m.load(SeqCst) != Self::FORWARD {
                    continue;
                }
                let Ok(server) = TcpStream::connect(target) else { continue };
                let pipes = [
                    (client.try_clone().unwrap(), server.try_clone().unwrap()),
                    (server, client),
                ];
                for (mut from, mut to) in pipes {
                    let m = m.clone();
                    std::thread::spawn(move || {
                        let mut buf = vec![0u8; 64 * 1024];
                        loop {
                            let n = match from.read(&mut buf) {
                                Ok(0) | Err(_) => break,
                                Ok(n) => n,
                            };
                            while m.load(SeqCst) != Self::FORWARD {
                                std::thread::sleep(Duration::from_millis(50));
                            }
                            if to.write_all(&buf[..n]).is_err() {
                                break;
                            }
                        }
                        let _ = to.shutdown(std::net::Shutdown::Both);
                    });
                }
            }
        });
        BlackholeProxy { addr, mode, clients }
    }

    fn hang(&self) {
        self.mode.store(Self::HANG, std::sync::atomic::Ordering::SeqCst);
    }

    fn forward(&self) {
        self.mode.store(Self::FORWARD, std::sync::atomic::Ordering::SeqCst);
    }

    fn reset_client_connections(&self) {
        for client in self.clients.lock().unwrap().drain(..) {
            let _ = client.shutdown(std::net::Shutdown::Both);
        }
    }
}

#[test]
fn quorum_writes_keep_pace_while_one_replica_host_is_black_holed() {
    use crate::node::Node;
    use crate::zonestore::{MemZoneStore, ZoneGeom};

    let zone_id = 7;
    let cfg = ZoneConfig { name: "blackhole".into(), rf: 3, ordering: Ordering::Sorted, scope: None };
    let ring = Ring::from_members(3, 64, [1, 2, 3]);
    let binds: Vec<_> = (0..3).map(|_| Node::bind().unwrap()).collect();
    let proxy = BlackholeProxy::spawn(binds[2].1);
    let peers: Vec<(NodeId, SocketAddr)> =
        vec![(1, binds[0].1), (2, binds[1].1), (3, proxy.addr)];
    let mut nodes = Vec::new();
    for (i, (listener, _)) in binds.into_iter().enumerate() {
        let node = (i + 1) as NodeId;
        let provider = MemZoneStore::new(node, ZoneGeom {
            block_size: 256,
            seg_blocks: 4,
            reserved_blocks: 0,
            region_blocks: 16384,
        });
        let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        let mut catalog = Catalog::new();
        catalog.insert(zone_id, cfg.clone());
        nodes.push(crate::testutil::running(Node::serve_zoned(
            node, listener, zone0, provider, catalog, ring.clone(), peers.clone(),
            Duration::from_millis(50), None,
        )));
    }
    for (_, addr) in &peers {
        push_members_full(*addr, 1, &peers);
    }
    let deadline = Duration::from_secs(3);
    let coordinator =
        Coordinator::new(99, ring.clone(), 2, NetTransport::connect(&peers).with_rpc_deadline(deadline));
    run(coordinator.refresh_catalog(1)).unwrap();
    let key = |i: u32| format!("k/{i:05}").into_bytes();
    let value = vec![b'v'; 512];
    let batch = |round: u32| -> Vec<(Vec<u8>, Vec<u8>)> {
        (round * 16..round * 16 + 16).map(|i| (key(i), value.clone())).collect()
    };
    let write_round = |round: u32| {
        let owned = batch(round);
        let writes: Vec<(&[u8], &[u8])> =
            owned.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
        let res = run(coordinator.write_batch_in(zone_id, &writes, WriteConcern::Majority)).unwrap();
        assert!(res.all_ok(), "round {round}: {:?}", res.failures().collect::<Vec<_>>());
        run(coordinator.delete_range_in(
            zone_id, Some(&key(round * 16)), Some(&key(round * 16 + 8)), WriteConcern::Majority,
        ))
        .unwrap();
    };

    write_round(0);
    assert_eq!(coordinator.transport().range_tombstones_supported(3), Some(true));

    proxy.hang();
    let start = Instant::now();
    for round in 1..=6 {
        write_round(round);
    }
    let hung = start.elapsed();

    proxy.reset_client_connections();
    let start = Instant::now();
    for round in 7..=12 {
        write_round(round);
    }
    let reconnecting = start.elapsed();

    assert!(
        hung < deadline && reconnecting < deadline,
        "W=2 writes and range deletes must not wait for the black-holed third replica \
         (6 rounds took {hung:?} on the hung connection and {reconnecting:?} while \
         reconnecting; RPC deadline {deadline:?})"
    );
    for round in [1, 12] {
        let got = run(coordinator.scan_in(
            zone_id, Some(&key(round * 16)), Some(&key(round * 16 + 16)), 2, None,
        ))
        .unwrap();
        let want: Vec<Vec<u8>> = (round * 16 + 8..round * 16 + 16).map(key).collect();
        assert_eq!(got.into_iter().map(|(k, _)| k).collect::<Vec<_>>(), want);
    }
    drop(nodes);
}

#[test]
fn batched_reads_fail_over_after_one_deadline_on_a_black_holed_replica() {
    use crate::node::Node;
    use crate::zonestore::{MemZoneStore, ZoneGeom};

    let zone_id = 7;
    let cfg = ZoneConfig { name: "blackhole".into(), rf: 3, ordering: Ordering::Unsorted, scope: None };
    let ring = Ring::from_members(3, 64, [1, 2, 3]);
    let binds: Vec<_> = (0..3).map(|_| Node::bind().unwrap()).collect();
    let proxy = BlackholeProxy::spawn(binds[2].1);
    let peers: Vec<(NodeId, SocketAddr)> =
        vec![(1, binds[0].1), (2, binds[1].1), (3, proxy.addr)];
    let mut nodes = Vec::new();
    for (i, (listener, _)) in binds.into_iter().enumerate() {
        let node = (i + 1) as NodeId;
        let provider = MemZoneStore::new(node, ZoneGeom {
            block_size: 256,
            seg_blocks: 4,
            reserved_blocks: 0,
            region_blocks: 16384,
        });
        let zone0 = provider.open(zone_id, Ordering::Unsorted).unwrap();
        let mut catalog = Catalog::new();
        catalog.insert(zone_id, cfg.clone());
        nodes.push(crate::testutil::running(Node::serve_zoned(
            node, listener, zone0, provider, catalog, ring.clone(), peers.clone(),
            Duration::from_millis(50), None,
        )));
    }
    for (_, addr) in &peers {
        push_members_full(*addr, 1, &peers);
    }
    let deadline = Duration::from_secs(1);
    let coordinator = Coordinator::new(
        99, ring.clone(), ReadConcern::Majority,
        NetTransport::connect(&peers).with_rpc_deadline(deadline),
    );
    run(coordinator.refresh_catalog(1)).unwrap();
    let keys: Vec<Vec<u8>> = (0..32u32).map(|i| format!("k{i}").into_bytes()).collect();
    for key in &keys {
        run(coordinator.write_in(zone_id, key, key, WriteConcern::All)).unwrap();
    }
    let behind = keys
        .iter()
        .filter(|k| coordinator.replicas_for(zone_id, k).unwrap()[..2].contains(&3))
        .count();
    assert!(behind >= 8, "only {behind} keys read node 3 first");

    proxy.hang();
    let start = Instant::now();
    let got = run(coordinator.read_many_in(zone_id, &keys)).unwrap();
    let elapsed = start.elapsed();
    for (key, got) in keys.iter().zip(got) {
        assert_eq!(got.unwrap().and_then(|s| s.value), Some(key.clone()));
    }
    assert!(
        elapsed < deadline * 2,
        "a batched read waited {elapsed:?} for the black-holed replica (deadline {deadline:?})"
    );
    drop(nodes);
}

#[test]
fn rehome_cutover_fences_stale_coordinator_writes() {
    use crate::node::Node;
    use crate::zonestore::{MemZoneStore, ZoneGeom};

    let zone_id = 7;
    let cfg = ZoneConfig {
        name: "rehome-fence".into(),
        rf: 1,
        ordering: Ordering::Sorted,
        scope: None,
    };
    let ring = Ring::from_members(1, 64, [1, 2, 3]);
    let binds: Vec<_> = (0..3).map(|_| Node::bind().unwrap()).collect();
    let peers: Vec<(NodeId, SocketAddr)> = binds
        .iter()
        .enumerate()
        .map(|(i, (_, addr))| ((i + 1) as NodeId, *addr))
        .collect();
    let mut nodes = Vec::new();
    for (i, (listener, _)) in binds.into_iter().enumerate() {
        let node = (i + 1) as NodeId;
        let provider = MemZoneStore::new(
            node,
            ZoneGeom {
                block_size: 256,
                seg_blocks: 4,
                reserved_blocks: 0,
                region_blocks: 1024,
            },
        );
        let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        let mut catalog = Catalog::new();
        catalog.insert(zone_id, cfg.clone());
        nodes.push(crate::testutil::running(Node::serve_zoned(
            node,
            listener,
            zone0,
            provider,
            catalog,
            ring.clone(),
            peers.clone(),
            Duration::from_millis(50),
            None,
        )));
    }
    for (_, addr) in &peers {
        push_members_full(*addr, 1, &peers);
    }

    let seed = Coordinator::new(
        99,
        ring.clone(),
        1,
        NetTransport::connect(&peers),
    );
    run(seed.refresh_catalog(1)).unwrap();
    let mut boundary = b"m".to_vec();
    while ring.replicas_in_zone(zone_id, b"", 1, None)
        == ring.replicas_in_zone(zone_id, &boundary, 1, None)
    {
        boundary.push(b'x');
    }
    let current = ring.replicas_in_zone(zone_id, b"", 1, None)[0];
    let pending = ring.replicas_in_zone(zone_id, &boundary, 1, None)[0];
    let mut key = boundary.clone();
    key.extend_from_slice(b"/stale-write");
    run(seed.write_in(zone_id, &key, b"old", WriteConcern::All)).unwrap();

    let mut stale_map = RangeMap::single();
    assert!(stale_map.split(&boundary));
    let mut prepared = stale_map.clone();
    assert!(prepared.set_pending(1, boundary.clone()));
    for (_, addr) in &peers {
        push_range_map_full(*addr, 10, zone_id, &prepared);
    }

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (_, pending_stats) = range_stats(peers[(pending - 1) as usize].1);
        let copied = pending_stats.iter().any(|stat| {
            stat.zone_id == zone_id
                && stat.lower_bound == boundary
                && stat.rehome_copy_complete_epoch >= 10
        });
        let (_, current_stats) = range_stats(peers[(current - 1) as usize].1);
        let clean = current_stats.iter().any(|stat| {
            stat.zone_id == zone_id
                && stat.lower_bound == boundary
                && stat.rehome_mirror_epoch >= 10
                && stat.rehome_mirror_failures == 0
        });
        if copied && clean {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "re-home did not become safe: pending={pending_stats:?} current={current_stats:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let mut committed = prepared;
    assert!(committed.commit_pending(1));
    for (_, addr) in &peers {
        push_range_map_full(*addr, 11, zone_id, &committed);
    }

    let fresh = Coordinator::new(
        101,
        ring.clone(),
        1,
        NetTransport::connect(&peers),
    );
    run(fresh.refresh_catalog(1)).unwrap();
    fresh.set_range_map(zone_id, committed.clone());

    let stale_manual = Coordinator::new(
        102,
        ring.clone(),
        1,
        NetTransport::connect(&peers),
    )
    .with_manual_refresh();
    run(stale_manual.refresh_ring(1)).unwrap();
    run(stale_manual.refresh_catalog(1)).unwrap();
    stale_manual.set_range_map(zone_id, stale_map.clone());
    assert!(matches!(
        run(stale_manual.write_in(
            zone_id,
            &key,
            b"must-not-ack",
            WriteConcern::All,
        )),
        Err(CoordError::StaleRing { .. })
    ));
    assert_eq!(
        run(fresh.read_in(zone_id, &key))
            .unwrap()
            .unwrap()
            .value
            .as_deref(),
        Some(b"old" as &[u8])
    );

    let stale_auto = Coordinator::new(
        103,
        ring,
        1,
        NetTransport::connect(&peers),
    );
    run(stale_auto.refresh_ring(1)).unwrap();
    run(stale_auto.refresh_catalog(1)).unwrap();
    stale_auto.set_range_map(zone_id, stale_map);
    run(stale_auto.write_in(zone_id, &key, b"refreshed", WriteConcern::All))
        .unwrap();
    assert_eq!(
        run(fresh.read_in(zone_id, &key))
            .unwrap()
            .unwrap()
            .value
            .as_deref(),
        Some(b"refreshed" as &[u8])
    );
    drop(nodes);
}

#[test]
fn rehome_cutover_fences_stale_coordinator_reads_and_scans() {
    use crate::node::Node;
    use crate::zonestore::{MemZoneStore, ZoneGeom};

    let zone_id = 7;
    let cfg = ZoneConfig {
        name: "rehome-read-fence".into(),
        rf: 1,
        ordering: Ordering::Sorted,
        scope: None,
    };
    let ring = Ring::from_members(1, 64, [1, 2, 3]);
    let binds: Vec<_> = (0..3).map(|_| Node::bind().unwrap()).collect();
    let peers: Vec<(NodeId, SocketAddr)> = binds
        .iter()
        .enumerate()
        .map(|(i, (_, addr))| ((i + 1) as NodeId, *addr))
        .collect();
    let mut nodes = Vec::new();
    for (i, (listener, _)) in binds.into_iter().enumerate() {
        let node = (i + 1) as NodeId;
        let provider = MemZoneStore::new(
            node,
            ZoneGeom {
                block_size: 256,
                seg_blocks: 4,
                reserved_blocks: 0,
                region_blocks: 1024,
            },
        );
        let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        let mut catalog = Catalog::new();
        catalog.insert(zone_id, cfg.clone());
        nodes.push(crate::testutil::running(Node::serve_zoned(
            node,
            listener,
            zone0,
            provider,
            catalog,
            ring.clone(),
            peers.clone(),
            Duration::from_millis(50),
            None,
        )));
    }
    for (_, addr) in &peers {
        push_members_full(*addr, 1, &peers);
    }

    let mut boundary = b"m".to_vec();
    while ring.replicas_in_zone(zone_id, b"", 1, None)
        == ring.replicas_in_zone(zone_id, &boundary, 1, None)
    {
        boundary.push(b'x');
    }
    let current = ring.replicas_in_zone(zone_id, b"", 1, None)[0];
    let pending = ring.replicas_in_zone(zone_id, &boundary, 1, None)[0];
    let key = |seq: u32| {
        let mut k = boundary.clone();
        k.extend_from_slice(format!("/q/{seq:04}").as_bytes());
        k
    };
    let (lo, hi) = {
        let mut lo = boundary.clone();
        lo.extend_from_slice(b"/q/");
        let mut hi = boundary.clone();
        hi.extend_from_slice(b"/q0");
        (lo, hi)
    };

    let mut stale_map = RangeMap::single();
    assert!(stale_map.split(&boundary));
    for (_, addr) in &peers {
        push_range_map_full(*addr, 9, zone_id, &stale_map);
    }
    let seed = Coordinator::new(99, ring.clone(), 1, NetTransport::connect(&peers));
    run(seed.refresh_catalog(1)).unwrap();
    seed.set_range_map(zone_id, stale_map.clone());
    for seq in 0..5 {
        run(seed.write_in(zone_id, &key(seq), b"old", WriteConcern::All)).unwrap();
    }

    let mut prepared = stale_map.clone();
    assert!(prepared.set_pending(1, boundary.clone()));
    for (_, addr) in &peers {
        push_range_map_full(*addr, 10, zone_id, &prepared);
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (_, pending_stats) = range_stats(peers[(pending - 1) as usize].1);
        let copied = pending_stats.iter().any(|stat| {
            stat.zone_id == zone_id
                && stat.lower_bound == boundary
                && stat.rehome_copy_complete_epoch >= 10
        });
        let (_, current_stats) = range_stats(peers[(current - 1) as usize].1);
        let clean = current_stats.iter().any(|stat| {
            stat.zone_id == zone_id
                && stat.lower_bound == boundary
                && stat.rehome_mirror_epoch >= 10
                && stat.rehome_mirror_failures == 0
        });
        if copied && clean {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "re-home did not become safe: pending={pending_stats:?} current={current_stats:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let during = Coordinator::new(100, ring.clone(), 1, NetTransport::connect(&peers));
    run(during.refresh_catalog(1)).unwrap();
    during.set_range_map(zone_id, prepared.clone());
    let queries = [(Some(lo.as_slice()), Some(hi.as_slice()), Some(10)); 2];
    let scalar = run(during.scan_in(zone_id, Some(&lo), Some(&hi), 1, Some(10))).unwrap();
    for page in run(during.scan_many_in(zone_id, &queries, 1, false)).unwrap() {
        assert_eq!(page.into_iter().map(|(k, s)| (k, s.value.unwrap())).collect::<Vec<_>>(), scalar, "pending re-home reads use the current owner");
    }
    let mut descending = scalar.clone();
    descending.reverse();
    assert_eq!(run(during.scan_in_rev(zone_id, Some(&lo), Some(&hi), 1, None)).unwrap(), descending);
    for page in run(during.scan_many_in_rev(zone_id, &queries, 1, false)).unwrap() {
        assert_eq!(page.into_iter().map(|(k, s)| (k, s.value.unwrap())).collect::<Vec<_>>(), descending);
    }
    let mut committed = prepared;
    assert!(committed.commit_pending(1));
    for (_, addr) in &peers {
        push_range_map_full(*addr, 11, zone_id, &committed);
    }

    let fresh = Coordinator::new(101, ring.clone(), 1, NetTransport::connect(&peers));
    run(fresh.refresh_catalog(1)).unwrap();
    fresh.set_range_map(zone_id, committed.clone());
    run(fresh.delete_range_in(zone_id, Some(&lo), Some(&key(3)), WriteConcern::All)).unwrap();
    run(fresh.write_in(zone_id, &key(5), b"new", WriteConcern::All)).unwrap();
    let want: Vec<Vec<u8>> = (3..6).map(key).collect();
    let keys_of = |page: Vec<(Vec<u8>, Vec<u8>)>| page.into_iter().map(|(k, _)| k).collect::<Vec<_>>();
    assert_eq!(keys_of(run(fresh.scan_in(zone_id, Some(&lo), Some(&hi), 1, Some(10))).unwrap()), want);

    let stale_manual = Coordinator::new(102, ring.clone(), 1, NetTransport::connect(&peers))
        .with_manual_refresh();
    run(stale_manual.refresh_ring(1)).unwrap();
    run(stale_manual.refresh_catalog(1)).unwrap();
    stale_manual.set_range_map(zone_id, stale_map.clone());
    let stale_scan = run(stale_manual.scan_in(zone_id, Some(&lo), Some(&hi), 1, Some(10)));
    assert!(
        matches!(stale_scan, Err(CoordError::StaleRing { .. })),
        "a scan routed by the pre-cutover map must be refused, got {stale_scan:?}"
    );
    let stale_many = run(stale_manual.scan_many_in(zone_id, &queries, 1, true));
    assert!(matches!(stale_many, Err(CoordError::StaleRing { .. })), "every MultiScan span is fenced: {stale_many:?}");
    let stale_reverse = run(stale_manual.scan_many_in_rev(zone_id, &queries, 1, true));
    assert!(matches!(stale_reverse, Err(CoordError::StaleRing { .. })), "reverse spans are fenced: {stale_reverse:?}");
    for seq in [0, 5] {
        let stale_read = run(stale_manual.read_in(zone_id, &key(seq)));
        assert!(
            matches!(stale_read, Err(CoordError::StaleRing { .. })),
            "a read routed by the pre-cutover map must be refused, got {stale_read:?}"
        );
    }

    let stale_auto = Coordinator::new(103, ring, 1, NetTransport::connect(&peers));
    run(stale_auto.refresh_ring(1)).unwrap();
    run(stale_auto.refresh_catalog(1)).unwrap();
    for limit in [Some(1), Some(10), None] {
        stale_auto.set_range_map(zone_id, stale_map.clone());
        let page = run(stale_auto.scan_in(zone_id, Some(&lo), Some(&hi), 1, limit)).unwrap();
        assert_eq!(keys_of(page), want[..limit.unwrap_or(want.len()).min(want.len())].to_vec(), "limit {limit:?}");
        stale_auto.set_range_map(zone_id, stale_map.clone());
        let page = run(stale_auto.scan_in_rev(zone_id, Some(&lo), Some(&hi), 1, limit)).unwrap();
        assert_eq!(keys_of(page), want.iter().rev().take(limit.unwrap_or(want.len())).cloned().collect::<Vec<_>>(), "reverse limit {limit:?}");
    }
    stale_auto.set_range_map(zone_id, stale_map.clone());
    assert_eq!(run(stale_auto.read_in(zone_id, &key(0))).unwrap(), None);
    stale_auto.set_range_map(zone_id, stale_map);
    assert_eq!(
        run(stale_auto.read_in(zone_id, &key(5))).unwrap().and_then(|s| s.value).as_deref(),
        Some(b"new" as &[u8])
    );
    drop(nodes);
}

fn probe_unsplit_fence(addr: SocketAddr, zone_id: ZoneId, epoch: u64) -> [bool; 4] {
    use crate::hlc::Version;
    let stale = |resp: Response| match resp {
        Response::StaleEpoch(_) => true,
        Response::Read(_) | Response::Scan { .. } | Response::Apply(_) | Response::Batch(_) => false,
        other => panic!("unexpected reply {other:?}"),
    };
    let version = Version { hlc: crate::hlc::physical_now_ms() << 16, node: 77 };
    [
        stale(rpc_request(addr, &Request::Read { epoch, zone_id, key: b"k" })),
        stale(rpc_request(addr, &Request::Scan { epoch, zone_id, lo: None, hi: None, max: 0 })),
        stale(rpc_request(
            addr,
            &Request::Apply { epoch, zone_id, key: b"k", value: Some(b"v"), version },
        )),
        stale(rpc_request(
            addr,
            &Request::RangeDelete {
                epoch,
                zone_id,
                sync: false,
                version,
                slices: &[(Some(b"a".as_slice()), Some(b"b".as_slice()))],
            },
        )),
    ]
}

#[test]
fn unsplit_sorted_zone_fences_non_owner_nodes_and_wrong_shards() {
    use crate::drive::{MemDriveSet, PartitionGeom};
    use crate::node::Node;
    use crate::zonestore::{MemZoneStore, ZoneGeom};

    let zone_id = 7;
    let cfg = ZoneConfig { name: "unsplit".into(), rf: 1, ordering: Ordering::Sorted, scope: None };
    let ring = Ring::from_members(1, 64, [1, 2, 3]);
    let binds: Vec<_> = (0..3).map(|_| Node::bind().unwrap()).collect();
    let peers: Vec<(NodeId, SocketAddr)> = binds.iter().enumerate()
        .map(|(i, (_, addr))| ((i + 1) as NodeId, *addr)).collect();
    let mut nodes = Vec::new();
    for (i, (listener, _)) in binds.into_iter().enumerate() {
        let node = (i + 1) as NodeId;
        let provider = MemZoneStore::new(node, ZoneGeom {
            block_size: 256, seg_blocks: 4, reserved_blocks: 0, region_blocks: 1024,
        });
        let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        let mut catalog = Catalog::new();
        catalog.insert(zone_id, cfg.clone());
        nodes.push(crate::testutil::running(Node::serve_zoned(
            node, listener, zone0, provider, catalog, ring.clone(), peers.clone(),
            Duration::from_secs(3600), None,
        )));
    }
    for (_, addr) in &peers {
        push_members_full(*addr, 1, &peers);
    }
    let owner = ring.replicas_in_zone(zone_id, b"", 1, None)[0];
    for adopted in [false, true] {
        if adopted {
            for (_, addr) in &peers {
                let _ = rpc_request(*addr, &Request::SetRangeMaps { auth: &[], epoch: 1, maps: &[], incarnation: None });
            }
        }
        for (node, addr) in &peers {
            let want = [*node != owner; 4];
            assert_eq!(
                probe_unsplit_fence(*addr, zone_id, 1),
                want,
                "node {node} (owner {owner}, empty map adopted: {adopted}): [read, scan, apply, range delete] stale"
            );
        }
    }
    let coordinator = Coordinator::new(99, ring, 1, NetTransport::connect(&peers));
    run(coordinator.refresh_ring(1)).unwrap();
    run(coordinator.refresh_catalog(1)).unwrap();
    run(coordinator.write_in(zone_id, b"m", b"v", WriteConcern::All)).unwrap();
    assert_eq!(run(coordinator.scan_in(zone_id, None, None, 1, None)).unwrap().len(), 2);
    drop(nodes);

    const SHARDS: usize = 2;
    let listeners = Node::bind_n(SHARDS).unwrap();
    let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, addr)| *addr).collect();
    let ring = Ring::from_members(1, 64, [1]);
    let (_, handle) = Node::serve_multidrive_zoned(
        1,
        listeners.into_iter().map(|(listener, _)| listener).collect(),
        MemDriveSet::uniform(256, 4096, 2),
        512,
        8,
        128,
        PartitionGeom { seg_blocks: 4, reserved_blocks: 0, ordering: Ordering::Unsorted },
        ring.clone(),
        vec![(1, addrs.clone())],
        Duration::from_secs(3600),
        true,
    ).unwrap();
    let _node = crate::testutil::running(handle);
    let zones = [wire::WireZone {
        id: zone_id,
        rf: 1,
        ordering: crate::zone::ordering_to_u8(Ordering::Sorted),
        scope: None,
        name: "unsplit",
    }];
    let members = [(1, 1)];
    let member_addrs = [(1, addrs.clone())];
    for addr in &addrs {
        let _ = rpc_request(*addr, &Request::SetZones { auth: &[], epoch: 1, zones: &zones, incarnation: None });
        let _ = rpc_request(*addr, &Request::SetRangeMaps { auth: &[], epoch: 1, maps: &[], incarnation: None });
        let _ = rpc_request(*addr, &Request::SetMembers {
            auth: &[], epoch: 1, vnodes: 64, rf: 1,
            members: &members, addrs: &member_addrs,
            incarnation: None,
        });
    }
    let owner_shard = (crate::ring::hash64(b"") % SHARDS as u64) as usize;
    for (shard, addr) in addrs.iter().enumerate() {
        assert_eq!(
            probe_unsplit_fence(*addr, zone_id, 1),
            [shard != owner_shard; 4],
            "shard {shard} (owner shard {owner_shard}): [read, scan, apply, range delete] stale"
        );
    }
    let coordinator = Coordinator::new(99, ring, 1, NetTransport::connect_sharded([(1, addrs.clone())]));
    run(coordinator.refresh_ring(1)).unwrap();
    run(coordinator.refresh_catalog(1)).unwrap();
    run(coordinator.write_in(zone_id, b"m", b"v", WriteConcern::All)).unwrap();
    assert_eq!(run(coordinator.read_in(zone_id, b"m")).unwrap().and_then(|s| s.value).as_deref(), Some(b"v" as &[u8]));
    assert_eq!(run(coordinator.scan_in(zone_id, None, None, 1, None)).unwrap().len(), 2);
}

#[test]
fn rehome_single_node_cross_shard_copy_mirror_cutover_is_zero_gap() {
    use crate::drive::{MemDriveSet, PartitionGeom};
    use crate::node::Node;

    const SHARDS: usize = 2;
    let zone_id = 9;
    let listeners = Node::bind_n(SHARDS).unwrap();
    let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, addr)| *addr).collect();
    let shard_listeners = listeners.into_iter().map(|(listener, _)| listener).collect();
    let ring = Ring::from_members(1, 64, [1]);
    let (_, handle) = Node::serve_multidrive_zoned(
        1,
        shard_listeners,
        MemDriveSet::uniform(256, 4096, 2),
        512,
        8,
        128,
        PartitionGeom { seg_blocks: 4, reserved_blocks: 0, ordering: Ordering::Unsorted },
        ring.clone(),
        vec![(1, addrs.clone())],
        Duration::from_millis(50),
        true,
    ).unwrap();
    let _node = crate::testutil::running(handle);
    let cfg = [wire::WireZone {
        id: zone_id,
        rf: 1,
        ordering: crate::zone::ordering_to_u8(Ordering::Sorted),
        scope: None,
        name: "cross-shard",
    }];
    for addr in &addrs {
        let req = Request::SetZones { auth: &[], epoch: 1, zones: &cfg, incarnation: None };
        let _ = rpc_request(*addr, &req);
        let _ = rpc_request(*addr, &Request::SetRangeMaps { auth: &[], epoch: 0, maps: &[], incarnation: None });
    }
    let members = [(1, 1)];
    let member_addrs = [(1, addrs.clone())];
    for addr in &addrs {
        let _ = rpc_request(*addr, &Request::SetMembers {
            auth: &[], epoch: 1, vnodes: 64, rf: 1,
            members: &members, addrs: &member_addrs,
            incarnation: None,
        });
    }
    let coordinator = Coordinator::new(
        99, ring, 1, NetTransport::connect_sharded([(1, addrs.clone())])
    );
    run(coordinator.refresh_catalog(1)).unwrap();
    let mut boundary = b"m".to_vec();
    while crate::ring::hash64(b"") % SHARDS as u64
        == crate::ring::hash64(&boundary) % SHARDS as u64
    { boundary.push(b'x'); }
    let old_shard = (crate::ring::hash64(b"") % SHARDS as u64) as usize;
    let new_shard = (crate::ring::hash64(&boundary) % SHARDS as u64) as usize;
    assert_ne!(old_shard, new_shard);
    let keys: Vec<Vec<u8>> = (0..40).map(|i| [
        boundary.as_slice(), format!("/{i:04}").as_bytes()
    ].concat()).collect();
    for key in &keys {
        run(coordinator.write_in(zone_id, key, b"v", WriteConcern::All)).unwrap();
    }
    let mut prepared = RangeMap::single();
    assert!(prepared.split(&boundary));
    assert!(prepared.set_pending(1, boundary.clone()));
    for addr in &addrs { push_range_map_full(*addr, 10, zone_id, &prepared); }
    coordinator.set_range_map(zone_id, prepared.clone());
    for (i, key) in keys.iter().enumerate() {
        let value = format!("v{i}").into_bytes();
        run(coordinator.write_in(zone_id, key, &value, WriteConcern::All)).unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (_, stats) = range_stats(addrs[new_shard]);
        if stats.iter().any(|stat| stat.lower_bound == boundary
            && stat.shard_index as usize == new_shard
            && stat.rehome_copy_complete_epoch >= 10) { break; }
        assert!(Instant::now() < deadline, "pending shard never completed copy");
        std::thread::sleep(Duration::from_millis(20));
    }
    let mut committed = prepared;
    assert!(committed.commit_pending(1));
    for addr in &addrs { push_range_map_full(*addr, 11, zone_id, &committed); }
    coordinator.set_range_map(zone_id, committed);
    for _ in 0..4 {
        std::thread::sleep(Duration::from_millis(70));
        for (i, key) in keys.iter().enumerate() {
            let got = run(coordinator.read_in(zone_id, key)).unwrap().unwrap();
            assert_eq!(got.value.as_deref(), Some(format!("v{i}").as_bytes()));
        }
    }
    let scan = run(coordinator.scan_in(zone_id, Some(&boundary), None, 1, None)).unwrap();
    assert_eq!(scan.len(), keys.len());
}

#[test]
fn rehome_real_transport_mirror_failure_is_not_acked_and_abort_is_safe() {
    use crate::node::Node;
    use crate::zonestore::{MemZoneStore, ZoneGeom};

    let zone_id = 8;
    let cfg = ZoneConfig { name: "abort".into(), rf: 1, ordering: Ordering::Sorted, scope: None };
    let ring = Ring::from_members(1, 64, [1, 2]);
    let binds: Vec<_> = (0..2).map(|_| Node::bind().unwrap()).collect();
    let peers: Vec<(NodeId, SocketAddr)> = binds.iter().enumerate()
        .map(|(i, (_, addr))| ((i + 1) as NodeId, *addr)).collect();
    let mut handles = Vec::new();
    for (i, (listener, _)) in binds.into_iter().enumerate() {
        let node = (i + 1) as NodeId;
        let provider = MemZoneStore::new(node, ZoneGeom {
            block_size: 256, seg_blocks: 4, reserved_blocks: 0, region_blocks: 512,
        });
        let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        let mut catalog = Catalog::new();
        catalog.insert(zone_id, cfg.clone());
        handles.push(Some(crate::testutil::running(Node::serve_zoned(
            node, listener, zone0, provider, catalog, ring.clone(), peers.clone(),
            Duration::from_millis(50), None,
        ))));
    }
    for (_, addr) in &peers {
        push_members_full(*addr, 1, &peers);
    }
    let coordinator = Coordinator::new(99, ring.clone(), 1, NetTransport::connect(&peers));
    run(coordinator.refresh_catalog(1)).unwrap();
    let mut boundary = b"m".to_vec();
    while ring.replicas_in_zone(zone_id, b"", 1, None)
        == ring.replicas_in_zone(zone_id, &boundary, 1, None)
    { boundary.push(b'x'); }
    let current = ring.replicas_in_zone(zone_id, b"", 1, None)[0];
    let pending = ring.replicas_in_zone(zone_id, &boundary, 1, None)[0];
    let key = [boundary.as_slice(), b"/key"].concat();
    run(coordinator.write_in(zone_id, &key, b"before", WriteConcern::All)).unwrap();
    handles[(pending - 1) as usize].take().unwrap().shutdown();

    let mut prepared = RangeMap::single();
    assert!(prepared.split(&boundary));
    assert!(prepared.set_pending(1, boundary.clone()));
    push_range_map_full(peers[(current - 1) as usize].1, 20, zone_id, &prepared);
    coordinator.set_range_map(zone_id, prepared.clone());
    assert!(run(coordinator.write_in(
        zone_id, &key, b"must-not-ack", WriteConcern::All
    )).is_err());
    let failures = || {
        let (_, stats) = range_stats(peers[(current - 1) as usize].1);
        stats.iter().find(|stat| stat.lower_bound == boundary).map_or(0, |stat| stat.rehome_mirror_failures)
    };
    let wait_failures_above = |floor: u64, what: &str| {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if failures() > floor { return; }
            assert!(Instant::now() < deadline, "{what} mirror failure not reported");
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    wait_failures_above(0, "write");
    let before = failures();
    assert!(run(coordinator.delete_range_in(
        zone_id, Some(&boundary), None, WriteConcern::All
    )).is_err(), "a range delete the pending owner did not receive must not ack");
    wait_failures_above(before, "range delete");
    let mut aborted = prepared;
    assert!(aborted.clear_pending(1));
    push_range_map_full(peers[(current - 1) as usize].1, 21, zone_id, &aborted);
    coordinator.set_range_map(zone_id, aborted);
    run(coordinator.write_in(zone_id, &key, b"after", WriteConcern::All)).unwrap();
    let got = run(coordinator.read_in(zone_id, &key)).unwrap().unwrap();
    assert_eq!(got.value.as_deref(), Some(&b"after"[..]));
    run(coordinator.delete_range_in(zone_id, Some(&boundary), None, WriteConcern::All)).unwrap();
    assert_eq!(run(coordinator.read_in(zone_id, &key)).unwrap(), None);
}

struct RehomeCopyFixture {
    zone_id: ZoneId,
    peers: Vec<(NodeId, SocketAddr)>,
    handles: Vec<Option<crate::testutil::Running>>,
    coordinator: Coordinator<NetTransport>,
    boundary: Vec<u8>,
    current: NodeId,
    pending: NodeId,
    prepared: RangeMap,
    keys: Vec<Vec<u8>>,
}

fn rehome_copy_fixture(zone_id: ZoneId, key_count: usize) -> RehomeCopyFixture {
    rehome_copy_fixture_with(zone_id, key_count, crate::node_settings::NodeSettings {
        rehome_copy_page_entries: 64,
        ..Default::default()
    })
}

fn rehome_copy_fixture_with(
    zone_id: ZoneId,
    key_count: usize,
    settings: crate::node_settings::NodeSettings,
) -> RehomeCopyFixture {
    use crate::node::Node;
    use crate::zonestore::{MemZoneStore, ZoneGeom};

    let cfg = ZoneConfig {
        name: "rehome-resume".into(),
        rf: 1,
        ordering: Ordering::Sorted,
        scope: None,
    };
    let ring = Ring::from_members(1, 64, [1, 2, 3]);
    let binds: Vec<_> = (0..3).map(|_| Node::bind().unwrap()).collect();
    let peers: Vec<(NodeId, SocketAddr)> = binds.iter().enumerate()
        .map(|(i, (_, addr))| ((i + 1) as NodeId, *addr)).collect();
    let mut handles = Vec::new();
    for (i, (listener, _)) in binds.into_iter().enumerate() {
        let node = (i + 1) as NodeId;
        let provider = MemZoneStore::new(node, ZoneGeom {
            block_size: 256, seg_blocks: 4, reserved_blocks: 0, region_blocks: 8192,
        });
        let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        let mut catalog = Catalog::new();
        catalog.insert(zone_id, cfg.clone());
        handles.push(Some(crate::testutil::running(Node::serve_zoned_with_settings(
            node, listener, zone0, provider, catalog, ring.clone(), peers.clone(),
            Duration::from_secs(3600), None,
            settings.clone(),
        ))));
    }
    for (_, addr) in &peers {
        push_members_full(*addr, 1, &peers);
    }
    let coordinator = Coordinator::new(99, ring.clone(), 1, NetTransport::connect(&peers));
    run(coordinator.refresh_catalog(1)).unwrap();
    let mut boundary = b"m".to_vec();
    while ring.replicas_in_zone(zone_id, b"", 1, None)
        == ring.replicas_in_zone(zone_id, &boundary, 1, None)
    {
        boundary.push(b'x');
    }
    let current = ring.replicas_in_zone(zone_id, b"", 1, None)[0];
    let pending = ring.replicas_in_zone(zone_id, &boundary, 1, None)[0];
    assert_ne!(current, pending);
    let keys: Vec<Vec<u8>> = (0..key_count).map(|i| {
        let mut key = boundary.clone();
        key.extend_from_slice(format!("/{i:05}").as_bytes());
        key
    }).collect();
    for key in &keys {
        run(coordinator.write_in(zone_id, key, b"v0", WriteConcern::All)).unwrap();
    }
    let mut prepared = RangeMap::single();
    assert!(prepared.split(&boundary));
    assert!(prepared.set_pending(1, boundary.clone()));
    RehomeCopyFixture {
        zone_id,
        peers,
        handles,
        coordinator,
        boundary,
        current,
        pending,
        prepared,
        keys,
    }
}

impl RehomeCopyFixture {
    fn push_map(&self, epoch: u64, map: &RangeMap) {
        for (_, addr) in &self.peers {
            push_range_map_full(*addr, epoch, self.zone_id, map);
        }
    }

    fn target_stat(&self) -> Option<wire::OwnedRangeStat> {
        let (_, stats) = range_stats(self.peers[(self.pending - 1) as usize].1);
        stats.into_iter().find(|stat| {
            stat.zone_id == self.zone_id && stat.lower_bound == self.boundary
        })
    }

    fn wait_progress_at_least(&self, floor: u64, prepare_epoch: u64) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(stat) = self.target_stat() {
                assert!(
                    stat.rehome_copy_complete_epoch < prepare_epoch,
                    "copy completed before the abort could be injected; \
                     raise key_count"
                );
                if stat.rehome_copy_progress >= floor {
                    return stat.rehome_copy_progress;
                }
            }
            assert!(Instant::now() < deadline, "copy made no progress");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn wait_complete(&self, epoch: u64) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(stat) = self.target_stat() {
                if stat.rehome_copy_complete_epoch >= epoch {
                    return stat.rehome_copy_progress;
                }
            }
            assert!(Instant::now() < deadline, "copy did not complete");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

#[test]
fn rehome_copy_resumes_across_abort_when_no_writes_hit_the_range() {
    let resumes_before = crate::observe::snapshot().rehome_copy_resumes;
    let f = rehome_copy_fixture(9, 1600);
    f.push_map(10, &f.prepared);
    f.coordinator.set_range_map(f.zone_id, f.prepared.clone());
    f.wait_progress_at_least(192, 10);

    let mut aborted = f.prepared.clone();
    assert!(aborted.clear_pending(1));
    f.push_map(11, &aborted);
    std::thread::sleep(Duration::from_millis(300));

    f.push_map(12, &f.prepared);
    let final_progress = f.wait_complete(12);
    assert!(
        final_progress <= f.keys.len() as u64 + 128,
        "resumed copy re-scanned the range: progress {final_progress} for \
         {} keys",
        f.keys.len()
    );
    assert!(
        crate::observe::snapshot().rehome_copy_resumes > resumes_before,
        "cursor was not resumed"
    );

    let mut committed = f.prepared.clone();
    assert!(committed.commit_pending(1));
    f.push_map(13, &committed);
    f.coordinator.set_range_map(f.zone_id, committed);
    let scanned = run(f.coordinator.scan_in(
        f.zone_id, Some(&f.boundary), None, 1, None
    )).unwrap();
    assert_eq!(scanned.len(), f.keys.len(), "post-cutover scan lost keys");
}

#[test]
fn rehome_copy_restarts_when_an_unmirrored_write_lands_in_the_abort_gap() {
    let restarts_before = crate::observe::snapshot().rehome_copy_restarts;
    let f = rehome_copy_fixture(10, 1600);
    f.push_map(10, &f.prepared);
    f.coordinator.set_range_map(f.zone_id, f.prepared.clone());
    f.wait_progress_at_least(192, 10);

    let mut aborted = f.prepared.clone();
    assert!(aborted.clear_pending(1));
    f.push_map(11, &aborted);
    std::thread::sleep(Duration::from_millis(300));
    let p_abort = self::range_stats(f.peers[(f.pending - 1) as usize].1).1
        .into_iter()
        .find(|stat| stat.zone_id == f.zone_id && stat.lower_bound == f.boundary)
        .map(|stat| stat.rehome_copy_progress)
        .unwrap_or(0);
    assert!(p_abort >= 192);
    f.coordinator.set_range_map(f.zone_id, aborted);
    run(f.coordinator.write_in(
        f.zone_id, &f.keys[0], b"gap-write", WriteConcern::All
    )).unwrap();

    f.push_map(12, &f.prepared);
    f.coordinator.set_range_map(f.zone_id, f.prepared.clone());
    let final_progress = f.wait_complete(12);
    assert!(
        final_progress >= p_abort + f.keys.len() as u64 - 64,
        "copy resumed past an unmirrored gap write: progress {final_progress}, \
         p_abort {p_abort}"
    );
    assert!(
        crate::observe::snapshot().rehome_copy_restarts > restarts_before,
        "the unmirrored-write mark did not force a restart"
    );

    let mut committed = f.prepared.clone();
    assert!(committed.commit_pending(1));
    f.push_map(13, &committed);
    f.coordinator.set_range_map(f.zone_id, committed);
    let got = run(f.coordinator.read_in(f.zone_id, &f.keys[0])).unwrap().unwrap();
    assert_eq!(
        got.value.as_deref(),
        Some(&b"gap-write"[..]),
        "gap write lost at cutover"
    );
}

struct CopyOnlyRangeTombstones(ZoneId);

impl CopyOnlyRangeTombstones {
    fn set(zone_id: ZoneId, reply_budget: usize) -> Self {
        super::server::set_zone_scan_reply_budget(zone_id, Some(reply_budget));
        crate::rt_sync::pause_exchange(zone_id, true);
        CopyOnlyRangeTombstones(zone_id)
    }
}

impl Drop for CopyOnlyRangeTombstones {
    fn drop(&mut self) {
        super::server::set_zone_scan_reply_budget(self.0, None);
        crate::rt_sync::pause_exchange(self.0, false);
    }
}

#[test]
fn rehome_copy_applies_page_fragments_follows_resume_keys_and_sees_mirrored_range_deletes() {
    use crate::replica::Stored;

    let zone_id = 61;
    let f = rehome_copy_fixture_with(zone_id, 400, crate::node_settings::NodeSettings {
        rehome_copy_page_entries: 64,
        rehome_copy_pages_per_tick: 64,
        ..Default::default()
    });
    let _copy_only = CopyOnlyRangeTombstones::set(zone_id, 100);
    let addr_of = |node: NodeId| f.peers[(node - 1) as usize].1;
    let delete = |lo: usize, hi: Option<usize>| {
        let hi = hi.map(|i| f.keys[i].as_slice());
        run(f.coordinator.delete_range_in(zone_id, Some(&f.keys[lo]), hi, WriteConcern::All)).unwrap()
    };
    let covers: Vec<(usize, usize, crate::hlc::Version)> = (0..40)
        .map(|j| (100 + 5 * j, 105 + 5 * j))
        .map(|(lo, hi)| (lo, hi, delete(lo, Some(hi))))
        .collect();
    let tail = delete(350, None);
    run(f.coordinator.write_in(zone_id, &f.keys[200], b"new", WriteConcern::All)).unwrap();
    let resumes_before = crate::observe::snapshot().scan_rt_resume_pages_total;

    f.push_map(10, &f.prepared);
    f.coordinator.set_range_map(zone_id, f.prepared.clone());
    let mirrored = delete(0, Some(50));
    let (_, stats) = range_stats(addr_of(f.current));
    assert!(stats.iter().any(|stat| stat.zone_id == zone_id
        && stat.lower_bound == f.boundary
        && stat.rehome_mirror_ok >= 1
        && stat.rehome_mirror_failures == 0), "{stats:?}");
    f.wait_complete(10);
    assert!(
        crate::observe::snapshot().scan_rt_resume_pages_total > resumes_before,
        "the copy read reply-budget pages with resume keys"
    );

    let read = |i: usize| match rpc_request(
        addr_of(f.pending),
        &Request::Read { epoch: 1, zone_id, key: &f.keys[i] },
    ) {
        Response::Read(stored) => stored,
        other => panic!("read failed: {other:?}"),
    };
    let tombstone = |version| Some(Stored { version, value: None });
    for &(lo, hi, version) in &covers {
        for i in [lo, hi - 1].into_iter().filter(|&i| i != 200) {
            assert_eq!(read(i), tombstone(version), "copied fragment over key {i}");
        }
    }
    for i in [350, 399] {
        assert_eq!(read(i), tombstone(tail), "copied tail fragment over key {i}");
    }
    for i in [0, 49] {
        assert_eq!(read(i), tombstone(mirrored), "mirrored fragment over key {i}");
    }
    assert_eq!(read(200).and_then(|s| s.value).as_deref(), Some(b"new" as &[u8]));
    for i in [50, 99, 300, 349] {
        assert_eq!(read(i).and_then(|s| s.value).as_deref(), Some(b"v0" as &[u8]), "key {i}");
    }

    let mut committed = f.prepared.clone();
    assert!(committed.commit_pending(1));
    f.push_map(11, &committed);
    f.coordinator.set_range_map(zone_id, committed);
    let scanned = run(f.coordinator.scan_in(zone_id, Some(&f.boundary), None, 1, None)).unwrap();
    let mut want: Vec<(Vec<u8>, Vec<u8>)> = (50..100)
        .chain(300..350)
        .map(|i| (f.keys[i].clone(), b"v0".to_vec()))
        .chain(std::iter::once((f.keys[200].clone(), b"new".to_vec())))
        .collect();
    want.sort();
    assert_eq!(scanned, want);
}

#[test]
fn rehome_slow_copy_survives_report_deadline_and_dead_owner_aborts() {
    use crate::pd::{
        range_stats_poll, BalanceExecutorState, BalancePollAuthority, PdCmd, PushState,
        RecoveryHold, RehomeReportState, RingConfig,
    };
    use std::collections::HashMap;

    let gap_aborts_before = crate::observe::snapshot().pd_rehomes_aborted_report_gap;
    let mut f = rehome_copy_fixture(11, 1600);
    f.push_map(10, &f.prepared);
    f.coordinator.set_range_map(f.zone_id, f.prepared.clone());

    let mut snap = PushState::new();
    snap.member_epoch = 1;
    snap.members = f.peers.iter().map(|&(n, a)| (n, 1, vec![a])).collect();
    snap.zone_epoch = 1;
    snap.zones.insert(f.zone_id, ZoneConfig {
        name: "rehome-resume".into(),
        rf: 1,
        ordering: Ordering::Sorted,
        scope: None,
    });
    snap.range_map_epoch = 10;
    snap.range_maps.insert(f.zone_id, f.prepared.clone());
    snap.leader = true;
    snap.auto_split_threshold = 0;
    snap.auto_merge_threshold = 0;
    snap.single_range_warn_entries = 0;
    snap.rehome_report_deadline_ms = 1000;
    let targets: Vec<(NodeId, Vec<SocketAddr>)> =
        f.peers.iter().map(|&(n, a)| (n, vec![a])).collect();
    let confirmed_range_maps: HashMap<SocketAddr, u64> = targets.iter()
        .flat_map(|(_, addrs)| addrs.iter().map(|addr| (*addr, snap.range_map_epoch)))
        .collect();
    let ring_cfg = RingConfig { vnodes: 64, rf: 1 };
    let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
    let mut last_warn = HashMap::new();
    let mut rehome_reports: HashMap<(ZoneId, Vec<u8>), RehomeReportState> = HashMap::new();
    let mut rehome_backoff: HashMap<(ZoneId, Vec<u8>), (std::time::Instant, u32)> =
        HashMap::new();
    let mut balance_executor = BalanceExecutorState::default();
    let balance_poll_authority = std::sync::Mutex::new(BalancePollAuthority::default());

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut committed = false;
    while !committed {
        assert!(
            Instant::now() < deadline,
            "PD never saw the slow copy complete"
        );
        range_stats_poll(
            &targets,
            &snap,
            &cmd_tx,
            &mut last_warn,
            &confirmed_range_maps,
            true,
            false,
            ring_cfg,
            &mut rehome_reports,
            &mut rehome_backoff,
            &mut balance_executor,
            &balance_poll_authority,
            &mut RecoveryHold::default(),
        );
        while let Ok(cmd) = cmd_rx.try_recv() {
            match cmd {
                PdCmd::AbortRehome(..) => panic!(
                    "PD aborted a slow copy that was reporting steady progress"
                ),
                PdCmd::CommitRehome(zone_id, lower) => {
                    assert_eq!((zone_id, lower.as_slice()), (f.zone_id, f.boundary.as_slice()));
                    committed = true;
                }
                _ => {}
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    f.handles[(f.pending - 1) as usize].take().unwrap().shutdown();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut aborted = false;
    while !aborted {
        assert!(
            Instant::now() < deadline,
            "PD never aborted after the pending owner died"
        );
        range_stats_poll(
            &targets,
            &snap,
            &cmd_tx,
            &mut last_warn,
            &confirmed_range_maps,
            true,
            false,
            ring_cfg,
            &mut rehome_reports,
            &mut rehome_backoff,
            &mut balance_executor,
            &balance_poll_authority,
            &mut RecoveryHold::default(),
        );
        while let Ok(cmd) = cmd_rx.try_recv() {
            if matches!(cmd, PdCmd::AbortRehome(..)) {
                aborted = true;
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        crate::observe::snapshot().pd_rehomes_aborted_report_gap > gap_aborts_before,
        "abort was not attributed to the report gap"
    );
}

#[test]
fn range_maps_push_and_coordinator_pull_over_tcp() {
    let zid = 7;
    let zcfg = ZoneConfig {
        name: "sorted".into(),
        rf: 3,
        ordering: Ordering::Sorted,
        scope: None,
    };
    let mut ring = Ring::with_vnodes(3, 64);
    let mut addrs = Vec::new();
    for node in 1..=3u32 {
        let addr = spawn_zoned_server(node, zid, zcfg.clone());
        push_range_map(addr, 1, zid, &[b"m".to_vec()]);
        addrs.push((node, addr));
        ring.add_node(node);
    }
    for (_, addr) in &addrs {
        push_members_full(*addr, 1, &addrs);
    }
    let c = Coordinator::new(0, ring, 2, NetTransport::connect(&addrs));
    run(c.refresh_catalog(1)).unwrap();
    assert_eq!(
        c.range_map(zid).splits(),
        &[b"m".to_vec()],
        "coordinator pulled the pushed split set"
    );

    for k in ["alpha", "mike", "bravo", "zulu"] {
        run(c.write_in(zid, k.as_bytes(), k.as_bytes(), WriteConcern::All)).unwrap();
    }
    let all = run(c.scan_in(zid, None, None, 2, None)).unwrap();
    let keys: Vec<&[u8]> = all.iter().map(|(k, _)| &k[..]).collect();
    assert_eq!(keys, vec![&b"alpha"[..], b"bravo", b"mike", b"zulu"]);
}

fn push_catalog(addr: SocketAddr, epoch: u64, cat: &Catalog) {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    let wire = catalog_to_wire(cat);
    let borrowed: Vec<wire::WireZone> = wire
        .iter()
        .map(|z| wire::WireZone {
            id: z.id,
            rf: z.rf,
            ordering: z.ordering,
            scope: z.scope.as_deref(),
            name: &z.name,
        })
        .collect();
    let payload = wire::encode_request_payload(&Request::SetZones {
        auth: &[],
        epoch,
        zones: &borrowed,
        incarnation: None,
    });
    let len = (8 + payload.len()) as u32;
    let mut frame = len.to_le_bytes().to_vec();
    frame.extend_from_slice(&1u64.to_le_bytes());
    frame.extend_from_slice(&payload);
    let mut s = TcpStream::connect(addr).unwrap();
    crate::proto::client_handshake_blocking(&mut s).unwrap();
    s.write_all(&frame).unwrap();
    let mut lenb = [0u8; 4];
    s.read_exact(&mut lenb).unwrap();
    let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
    s.read_exact(&mut block).unwrap();
}

#[test]
fn drop_zone_over_tcp_tears_down_the_engine() {
    let zid = 7;
    let zcfg = ZoneConfig {
        name: "z7".into(),
        rf: 1,
        ordering: Ordering::Unsorted,
        scope: None,
    };
    let addr = spawn_zoned_server(1, zid, zcfg.clone());
    let mut ring = Ring::with_vnodes(1, 64);
    ring.set_members([1]);
    let c = Coordinator::new(0, ring, 1, NetTransport::connect(&[(1, addr)]));
    run(c.refresh_catalog(1)).unwrap();

    run(c.write_in(zid, b"k", b"v", WriteConcern::One)).unwrap();
    assert_eq!(
        run(c.read_in(zid, b"k")).unwrap().unwrap().value.as_deref(),
        Some(&b"v"[..])
    );

    push_catalog(addr, 1, &Catalog::new());
    let mut redefined = Catalog::new();
    redefined.insert(zid, zcfg);
    push_catalog(addr, 2, &redefined);
    assert_eq!(
        run(c.read_in(zid, b"k")).unwrap(),
        None,
        "dropped zone's engine was torn down (data gone on re-create)"
    );
}

fn zoned_shared(
    zid: ZoneId,
) -> (
    ServerShared<MemBlockDevice>,
    Rc<crate::zonestore::MemZoneStore>,
) {
    zoned_shared_ordered(zid, Ordering::Unsorted)
}

fn zoned_shared_ordered(
    zid: ZoneId,
    ordering: Ordering,
) -> (
    ServerShared<MemBlockDevice>,
    Rc<crate::zonestore::MemZoneStore>,
) {
    use crate::zonestore::{MemZoneStore, ZoneGeom};
    let geom = ZoneGeom {
        block_size: 256,
        seg_blocks: 4,
        reserved_blocks: 0,
        region_blocks: 256,
    };
    let provider = Rc::new(MemZoneStore::new(1, geom));
    let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
    let mut catalog = Catalog::new();
    catalog.insert(
        zid,
        ZoneConfig {
            name: "z".into(),
            rf: 1,
            ordering,
            scope: None,
        },
    );
    let shared = ServerShared::zoned(1, zone0, provider.clone(), catalog, Ring::new(1));
    (shared, provider)
}

fn adopt_catalog(shared: &ServerShared<MemBlockDevice>, epoch: u64, cat: &Catalog) {
    let resp = serve_request(
        shared,
        OwnedRequest::SetZones {
            auth: Vec::new(),
            epoch,
            zones: catalog_to_wire(cat),
            incarnation: None,
        },
    );
    assert!(
        matches!(resp, Response::Zones { .. }),
        "unexpected SetZones reply: {resp:?}"
    );
}

#[test]
fn control_pushes_bind_a_node_to_one_cluster_incarnation_across_restart_and_reenrol() {
    use crate::zonestore::{FileZoneStore, ZoneGeom};
    let dir = std::env::temp_dir().join(format!("vals-incarnation-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let geom = ZoneGeom {
        block_size: 256,
        seg_blocks: 4,
        reserved_blocks: 0,
        region_blocks: 256,
    };
    let a = 0xaaaa_0000_0000_0000_0000_0000_0000_0001u128;
    let b = 0xbbbb_0000_0000_0000_0000_0000_0000_0002u128;
    let boot = || {
        let provider = Rc::new(FileZoneStore::new(1, &dir, geom).unwrap());
        let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        ServerShared::zoned(1, zone0, provider, Catalog::new(), Ring::new(1))
    };
    let set_members = |shared: &ServerShared<_>, epoch, incarnation| {
        serve_request(
            shared,
            OwnedRequest::SetMembers {
                auth: Vec::new(),
                epoch,
                vnodes: 8,
                rf: 1,
                members: vec![(1, 1)],
                addrs: Vec::new(),
                incarnation,
            },
        )
    };
    let refused = |resp: Response| match resp {
        Response::Err(e) => assert!(e.contains("incarnation mismatch"), "{e}"),
        other => panic!("a foreign incarnation must be refused, got {other:?}"),
    };
    {
        let shared = boot();
        assert!(matches!(set_members(&shared, 1, None), Response::Members { epoch: 1, .. }));
        assert_eq!(shared.incarnation(), None, "a pre-token push binds nothing");
        assert!(matches!(set_members(&shared, 2, Some(a)), Response::Members { epoch: 2, .. }));
        assert_eq!(shared.incarnation(), Some(a));

        let rejected = crate::observe::counters().control_incarnation_rejected.get();
        refused(set_members(&shared, 100, Some(b)));
        refused(set_members(&shared, 101, None));
        refused(serve_request(
            &shared,
            OwnedRequest::SetZones {
                auth: Vec::new(),
                epoch: 100,
                zones: Vec::new(),
                incarnation: Some(b),
            },
        ));
        refused(serve_request(
            &shared,
            OwnedRequest::SetRangeMaps {
                auth: Vec::new(),
                epoch: 100,
                maps: Vec::new(),
                incarnation: Some(b),
            },
        ));
        assert!(crate::observe::counters().control_incarnation_rejected.get() >= rejected + 4);
        assert_eq!(
            (shared.ring_epoch(), shared.catalog_epoch(), shared.range_map_epoch()),
            (2, 0, 0),
            "refused pushes change nothing, whatever their epoch"
        );
        assert!(matches!(set_members(&shared, 3, Some(a)), Response::Members { epoch: 3, .. }));
    }
    {
        let shared = boot();
        assert_eq!(shared.incarnation(), Some(a), "the binding survives a node restart");
        refused(set_members(&shared, 200, Some(b)));
        assert!(matches!(set_members(&shared, 4, Some(a)), Response::Members { epoch: 4, .. }));
    }
    FileZoneStore::new(1, &dir, geom)
        .unwrap()
        .persist_incarnation(Some(b))
        .unwrap();
    {
        let shared = boot();
        assert_eq!(shared.incarnation(), Some(b), "offline re-enrol rebinds the node");
        refused(set_members(&shared, 300, Some(a)));
        assert!(matches!(set_members(&shared, 5, Some(b)), Response::Members { epoch: 5, .. }));
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn set_zones_persists_catalog_for_next_boot() {
    use crate::catalog_store::{CatalogStore, FileCatalogStore};
    use crate::zonestore::{MemZoneStore, ZoneGeom};
    let dir = std::env::temp_dir().join(format!("vals-catpersist-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let cat_path = dir.join("catalog-adopt");
    let _ = std::fs::remove_file(&cat_path);

    let geom = ZoneGeom {
        block_size: 256,
        seg_blocks: 4,
        reserved_blocks: 0,
        region_blocks: 256,
    };
    let provider = Rc::new(MemZoneStore::new(1, geom));
    let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
    let shared = ServerShared::zoned(1, zone0, provider, Catalog::new(), Ring::new(1))
        .with_catalog_store(Box::new(FileCatalogStore::new(&cat_path)));

    assert!(
        FileCatalogStore::new(&cat_path).load().is_none(),
        "nothing persisted before the first adoption"
    );

    let mut cat = Catalog::new();
    cat.insert(
        7,
        ZoneConfig {
            name: "z7".into(),
            rf: 1,
            ordering: Ordering::Sorted,
            scope: None,
        },
    );
    adopt_catalog(&shared, 1, &cat);

    let loaded = FileCatalogStore::new(&cat_path)
        .load()
        .expect("adoption persists the catalog for the next boot");
    assert!(loaded.contains(7));
    assert_eq!(loaded.get(7).map(|c| c.ordering), Some(Ordering::Sorted));
}

#[test]
fn persisted_catalog_reopens_zone_at_boot() {
    use crate::catalog_store::{CatalogStore, FileCatalogStore};
    use crate::zonestore::{FileZoneStore, ZoneGeom};
    let dir = std::env::temp_dir().join(format!("vals-bootreopen-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let zones_dir = dir.join("zones");
    let cat_path = dir.join("catalog");
    let geom = ZoneGeom {
        block_size: 256,
        seg_blocks: 4,
        reserved_blocks: 0,
        region_blocks: 1024,
    };

    {
        let provider = FileZoneStore::new(1, &zones_dir, geom).unwrap();
        let mut z7 = provider.open(7, Ordering::Sorted).unwrap();
        z7.put_at(b"hello", b"world", 1000).unwrap();
        z7.sync().unwrap();
        let mut cat = Catalog::new();
        cat.insert(
            7,
            ZoneConfig {
                name: "z7".into(),
                rf: 1,
                ordering: Ordering::Sorted,
                scope: None,
            },
        );
        FileCatalogStore::new(&cat_path).store(&cat).unwrap();
    }

    {
        let provider = Rc::new(FileZoneStore::new(1, &zones_dir, geom).unwrap());
        let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        let shared = ServerShared::zoned(1, zone0, provider, Catalog::new(), Ring::new(1));
        assert!(
            !shared.sweep_zone_ids(false).contains(&7),
            "empty boot catalog leaves the persisted zone unopened at boot"
        );
    }

    {
        let provider = Rc::new(FileZoneStore::new(1, &zones_dir, geom).unwrap());
        let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        let boot_cat = FileCatalogStore::new(&cat_path)
            .load()
            .expect("persisted catalog loads at boot");
        let shared = ServerShared::zoned(1, zone0, provider, boot_cat, Ring::new(1));
        assert!(
            shared.sweep_zone_ids(false).contains(&7),
            "loaded boot catalog reopens the zone at boot, before serving"
        );
        let z7 = shared.zone_store(7).expect("reopened zone is accessible");
        assert_eq!(
            z7.borrow().get(b"hello").unwrap().as_deref(),
            Some(&b"world"[..]),
            "reopened zone recovers its data"
        );
    }
}

#[test]
fn boot_reopens_zone_from_provider_ordering_with_empty_catalog() {
    use crate::zonestore::{MemZoneStore, ZoneGeom};
    let geom = ZoneGeom {
        block_size: 256,
        seg_blocks: 4,
        reserved_blocks: 0,
        region_blocks: 256,
    };
    let provider = Rc::new(MemZoneStore::new(1, geom));
    {
        let mut z9 = provider.open(9, Ordering::Sorted).unwrap();
        z9.put_at(b"k", b"v", 1000).unwrap();
        z9.sync().unwrap();
    }
    let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
    let shared = ServerShared::zoned(1, zone0, provider, Catalog::new(), Ring::new(1));
    assert!(
        shared.sweep_zone_ids(false).contains(&9),
        "provider-reported ordering must reopen the zone at boot despite an empty catalog"
    );
    let z9 = shared.zone_store(9).expect("reopened at boot");
    assert_eq!(z9.borrow().get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
}

#[test]
fn persisted_region_zones_rebind_before_shard_boot_reopen_io() {
    use crate::zonestore::{RegionZoneConfig, RegionZoneStore};
    use vals_spdk::{AsyncRead, BlockDevice};

    struct AffinityDevice {
        inner: MemBlockDevice,
        binding: Arc<std::sync::Mutex<(Option<std::thread::ThreadId>, bool)>>,
    }

    impl AffinityDevice {
        fn new(block_size: usize, num_blocks: u64) -> Self {
            AffinityDevice {
                inner: MemBlockDevice::new(block_size, num_blocks),
                binding: Arc::new(std::sync::Mutex::new((None, false))),
            }
        }

        fn share(&self) -> Self {
            AffinityDevice {
                inner: self.inner.share(),
                binding: self.binding.clone(),
            }
        }

        fn assert_owner(&self) {
            let current = std::thread::current().id();
            let mut binding = self.binding.lock().unwrap();
            match binding.0 {
                Some(owner) => assert_eq!(owner, current, "device used outside its owner thread"),
                None => binding.0 = Some(current),
            }
        }
    }

    impl BlockDevice for AffinityDevice {
        fn block_size(&self) -> usize {
            self.inner.block_size()
        }

        fn num_blocks(&self) -> u64 {
            self.inner.num_blocks()
        }

        fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.assert_owner();
            self.inner.read_at(lba, buf)
        }

        fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
            self.assert_owner();
            self.inner.write_at(lba, buf)
        }

        fn flush(&self) -> vals_spdk::Result<()> {
            self.assert_owner();
            self.inner.flush()
        }

        fn bind_io_owner(&self) {
            let current = std::thread::current().id();
            let mut binding = self.binding.lock().unwrap();
            assert!(!binding.1 || binding.0 == Some(current));
            *binding = (Some(current), true);
        }
    }

    impl AsyncRead for AffinityDevice {
        async fn read_async(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.assert_owner();
            self.inner.read_at(lba, buf)
        }
    }

    let cfg = RegionZoneConfig {
        super_blocks: 8,
        zone_region_blocks: 128,
        zone_region_overrides: Vec::new(),
        seg_blocks: 4,
        reserved_blocks: 0,
    };
    let total_blocks = cfg.super_blocks + 4 * cfg.zone_region_blocks;
    let backing = AffinityDevice::new(256, total_blocks);
    {
        let provider = RegionZoneStore::open(Arc::new(backing.share()), &cfg, 1, true).unwrap();
        for zone_id in [DEFAULT_ZONE, 7, 9] {
            let mut zone = provider.open(zone_id, Ordering::Unsorted).unwrap();
            for i in 0..16 {
                zone.put_at(
                    format!("zone-{zone_id}-key-{i}").as_bytes(),
                    format!("value-{i}").as_bytes(),
                    1_000 + i,
                )
                .unwrap();
            }
            zone.sync().unwrap();
        }
    }

    let provider = RegionZoneStore::open(Arc::new(backing.share()), cfg, 1, false).unwrap();
    let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
    std::thread::Builder::new()
        .name("vals-shard-test".into())
        .spawn(move || {
            let shared = ServerShared::zoned(
                1,
                zone0,
                Rc::new(provider),
                Catalog::new(),
                Ring::new(1),
            );
            shared.bind_device_owners();
            for zone_id in [DEFAULT_ZONE, 7, 9] {
                let zone = shared.zone_store(zone_id).unwrap();
                assert_eq!(
                    zone.borrow()
                        .get(format!("zone-{zone_id}-key-15").as_bytes())
                        .unwrap()
                        .as_deref(),
                    Some(&b"value-15"[..])
                );
            }
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn zone_teardown_defers_the_region_free_while_the_engine_is_referenced() {
    let zid = 7;
    let (shared, provider) = zoned_shared(zid);
    let held = shared.zone_store(zid).unwrap();
    adopt_catalog(&shared, 1, &Catalog::new());
    let teardown_rejections = crate::observe::counters().zone_teardown_rejections.get();
    assert!(
        shared.zone_store(zid).is_err(),
        "dropped zone must not be servable"
    );
    assert!(
        crate::observe::counters().zone_teardown_rejections.get() > teardown_rejections,
        "teardown-pending rejection was not distinguished from generic server errors"
    );
    assert!(
        provider.existing().contains(&zid),
        "storage freed while the engine was still referenced (live teardown race)"
    );
    shared.complete_pending_drops();
    assert!(provider.existing().contains(&zid));
    drop(held);
    shared.complete_pending_drops();
    assert!(
        !provider.existing().contains(&zid),
        "deferred teardown never completed"
    );
}

fn wait_for_maint_done(
    handle: &crate::maint::MaintHandle<MemBlockDevice>,
) -> crate::maint::MaintDone<MemBlockDevice> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(done) = handle.drain().pop() {
            return done;
        }
        assert!(Instant::now() < deadline, "maintenance job timed out");
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn zone_teardown_waits_for_an_offcore_flush() {
    let zid = 7;
    let (shared, provider) = zoned_shared(zid);
    let store = shared.zone_store(zid).unwrap();
    store
        .borrow_mut()
        .apply(b"k", Some(b"v"), crate::Version { hlc: 100, node: 1 })
        .unwrap();
    let plan = store.borrow_mut().flush_plan().unwrap().unwrap();
    let (handle, mut join) = crate::maint::spawn::<MemBlockDevice>();
    handle
        .submit(crate::maint::MaintJob::Flush(zid, plan))
        .unwrap();
    shared.begin_maint_job(zid);
    drop(store);

    adopt_catalog(&shared, 1, &Catalog::new());
    assert!(
        provider.existing().contains(&zid),
        "storage freed while an off-core flush could still write"
    );

    let done = wait_for_maint_done(&handle);
    assert!(
        matches!(done, crate::maint::MaintDone::Flushed(id, Ok(_)) if id == zid),
        "off-core flush did not complete successfully"
    );
    shared.finish_maint_job(zid);
    assert!(
        !provider.existing().contains(&zid),
        "deferred teardown did not drain after the flush finished"
    );
    drop(handle);
    join.join();
}

#[test]
fn zone_teardown_waits_for_an_offcore_compaction() {
    let zid = 7;
    let (shared, provider) = zoned_shared_ordered(zid, Ordering::Sorted);
    let store = shared.zone_store(zid).unwrap();
    for round in 0..4u64 {
        for i in 0..8u64 {
            let key = format!("k{round}-{i:02}");
            store
                .borrow_mut()
                .apply(
                    key.as_bytes(),
                    Some(b"value"),
                    crate::Version {
                        hlc: round * 100 + i + 1,
                        node: 1,
                    },
                )
                .unwrap();
        }
        store.borrow_mut().flush().unwrap().unwrap();
    }
    let plan = store
        .borrow_mut()
        .compact_plan(0)
        .unwrap()
        .expect("four runs plan a compaction");
    let (handle, mut join) = crate::maint::spawn::<MemBlockDevice>();
    handle
        .submit(crate::maint::MaintJob::Compact(zid, plan))
        .unwrap();
    shared.begin_maint_job(zid);
    drop(store);

    adopt_catalog(&shared, 1, &Catalog::new());
    assert!(
        provider.existing().contains(&zid),
        "storage freed while an off-core compaction could still write"
    );

    let done = wait_for_maint_done(&handle);
    assert!(
        matches!(done, crate::maint::MaintDone::Compacted(id, _, Ok(_)) if id == zid),
        "off-core compaction did not complete successfully"
    );
    shared.finish_maint_job(zid);
    assert!(
        !provider.existing().contains(&zid),
        "deferred teardown did not drain after compaction finished"
    );
    drop(handle);
    join.join();
}

#[test]
fn zone_teardown_waits_for_an_offcore_cover_scan() {
    let zid = 7;
    let (shared, provider) = zoned_shared(zid);
    let store = shared.zone_store(zid).unwrap();
    for round in 0..2u64 {
        for i in 0..8u64 {
            let key = format!("k{round}-{i:02}");
            store
                .borrow_mut()
                .apply(key.as_bytes(), Some(b"value"), crate::Version { hlc: round * 100 + i + 1, node: 1 })
                .unwrap();
        }
        store.borrow_mut().flush().unwrap().unwrap();
    }
    store.borrow_mut().engine_mut().reset_key_filter_for_test();
    let plan = store.borrow_mut().engine_mut().cover_scan_plan().expect("uncovered runs plan a cover scan");
    let (handle, mut join) = crate::maint::spawn::<MemBlockDevice>();
    handle
        .submit(crate::maint::MaintJob::CoverScan(zid, Box::new(plan)))
        .unwrap();
    shared.begin_maint_job(zid);
    drop(store);

    adopt_catalog(&shared, 1, &Catalog::new());
    assert!(
        provider.existing().contains(&zid),
        "storage freed while an off-core cover scan could still read it"
    );

    let crate::maint::MaintDone::CoverScanned(id, plan, result) = wait_for_maint_done(&handle) else {
        panic!("expected a cover-scan completion");
    };
    assert_eq!(id, zid);
    assert!(provider.existing().contains(&zid));
    crate::node::finish_cover_scan_chunk(&shared, &handle.submitter(), zid, plan, result);
    assert!(
        !provider.existing().contains(&zid),
        "deferred teardown did not drain after the cover scan finished"
    );
    drop(handle);
    join.join();
}

#[test]
fn zone_recreate_waits_out_a_pending_teardown() {
    let zid = 7;
    let (shared, _provider) = zoned_shared(zid);
    let held = shared.zone_store(zid).unwrap();
    held.borrow_mut()
        .apply(b"k", Some(b"old"), crate::Version { hlc: 100, node: 1 })
        .unwrap();
    adopt_catalog(&shared, 1, &Catalog::new());
    let mut recreated = Catalog::new();
    recreated.insert(
        zid,
        ZoneConfig {
            name: "z".into(),
            rf: 1,
            ordering: Ordering::Unsorted,
            scope: None,
        },
    );
    adopt_catalog(&shared, 2, &recreated);
    assert!(
        shared.zone_store(zid).is_err(),
        "rematerialized over a pending teardown"
    );
    drop(held);
    shared.complete_pending_drops();
    let fresh = shared.zone_store(zid).unwrap();
    assert_eq!(
        fresh.borrow().get(b"k").unwrap(),
        None,
        "recreated zone leaked the old incarnation's data"
    );
}

#[test]
fn liveness_grace_is_stamped_onto_the_engines() {
    let store = replica(1).with_gc_grace_ms(5_000);
    let shared = ServerShared::single_zone_on(1, store, Ring::new(1))
        .with_liveness(1_000, Box::new(crate::liveness::MemLivenessLog::new()));
    assert_eq!(
        shared
            .zone_store(DEFAULT_ZONE)
            .unwrap()
            .borrow()
            .gc_grace_ms(),
        1_000,
        "the lease grace must override a mismatched engine grace"
    );
}

#[test]
fn a_store_at_its_reserve_floor_answers_apply_and_batchapply_busy() {
    use crate::hlc::Version;
    let store = replica(1);
    let shared = ServerShared::single_zone_on(1, store, Ring::new(1));
    shared.arm_maint_reserve();

    let mut hit_busy = false;
    for i in 0..200_000u32 {
        let resp = serve_request(
            &shared,
            OwnedRequest::Apply {
                epoch: 0,
                zone_id: DEFAULT_ZONE,
                key: format!("k{:04}", i % 64).into_bytes(),
                value: Some(vec![7u8; 200]),
                version: Version {
                    hlc: i as u64 + 1,
                    node: 1,
                },
            },
        );
        match resp {
            Response::Apply(_) => {}
            Response::Busy => {
                let at_floor = shared
                    .zone_store(DEFAULT_ZONE)
                    .unwrap()
                    .borrow()
                    .engine()
                    .at_maintenance_floor();
                if at_floor {
                    hit_busy = true;
                    break;
                }
            }
            other => panic!("reserve-floor Apply must be Busy, got {other:?}"),
        }
    }
    assert!(
        hit_busy,
        "an armed, churned store must eventually refuse the data path with Busy at the floor"
    );

    let resp = serve_request(
        &shared,
        OwnedRequest::BatchApply {
            epoch: 0,
            zone_id: DEFAULT_ZONE,
            sync: true,
            writes: vec![(
                b"batchkey".to_vec(),
                Some(vec![9u8; 200]),
                Version {
                    hlc: 10_000_000,
                    node: 1,
                },
            )],
        },
    );
    assert!(
        matches!(resp, Response::Busy),
        "reserve-floor BatchApply must be Busy, got {resp:?}"
    );
}

#[test]
fn a_dropped_zone_is_no_longer_current_for_the_sweep() {
    let zid = 7;
    let (shared, _provider) = zoned_shared(zid);
    let held = shared.zone_store(zid).unwrap();
    assert!(shared.zone_is_current(zid, &held));
    adopt_catalog(&shared, 1, &Catalog::new());
    assert!(!shared.zone_is_current(zid, &held));
}

#[test]
fn zone_anti_entropy_heals_a_stale_node_over_tcp() {
    use crate::antientropy::reconcile_scoped_zone;

    let zid = 7;
    let zcfg = ZoneConfig {
        name: "z7".into(),
        rf: 2,
        ordering: Ordering::Unsorted,
        scope: None,
    };
    let peer_addr = spawn_zoned_server(2, zid, zcfg.clone());
    let ring = Ring::from_members(2, 64, [1, 2]);
    let cp = Coordinator::new(0, ring.clone(), 1, NetTransport::connect(&[(2, peer_addr)]));
    run(cp.refresh_catalog(2)).unwrap();
    let keys: Vec<String> = (0..30).map(|i| format!("z{i:02}")).collect();
    for k in &keys {
        run(cp.write_in(zid, k.as_bytes(), b"v", WriteConcern::One)).unwrap();
    }

    let transport = NetTransport::connect(&[(2, peer_addr)]);
    let local = std::cell::RefCell::new(replica(1));
    let (pulled, _) = run(reconcile_scoped_zone(
        &local,
        &transport.peer(2),
        crate::antientropy::DEFAULT_BUCKETS,
        &ring,
        1,
        2,
        zid,
        2,
        None,
        None,
        (0, 1),
        None,
    ))
    .unwrap();
    assert!(pulled > 0, "healed zone-7 keys over TCP");
    for k in &keys {
        assert_eq!(
            local.borrow().get(k.as_bytes()).unwrap().as_deref(),
            Some(&b"v"[..]),
            "{k} healed into zone 7"
        );
    }
}

#[test]
fn unreachable_node_counts_as_down() {
    let mut ring = Ring::with_vnodes(3, 64);
    let mut addrs = Vec::new();
    for node in 1..=3u32 {
        addrs.push((node, ReplicaServer::spawn(replica(node)).unwrap()));
        ring.add_node(node);
    }
    addrs.push((4, "127.0.0.1:1".parse().unwrap()));
    ring.add_node(4);
    let c = Coordinator::new(0, ring, 2, NetTransport::connect(&addrs));

    run(c.write_with(b"k", b"v", WriteConcern::Quorum(2))).unwrap();
    assert_eq!(
        run(c.read_quorum(b"k", 2))
            .unwrap()
            .unwrap()
            .value
            .as_deref(),
        Some(&b"v"[..])
    );
}

#[test]
fn server_handle_shutdown_drains_and_joins() {
    let (addr, handle) = ReplicaServer::spawn_with_handle(replica(1)).unwrap();

    let mut ring = Ring::with_vnodes(1, 64);
    ring.add_node(1);
    let c = Coordinator::new(0, ring, 1, NetTransport::connect(&[(1, addr)]));
    run(c.write_with(b"k", b"v", WriteConcern::All)).unwrap();
    assert_eq!(
        run(c.read(b"k")).unwrap().unwrap().value.as_deref(),
        Some(&b"v"[..])
    );
    drop(c);

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        handle.shutdown();
        let _ = tx.send(());
    });
    rx.recv_timeout(Duration::from_secs(10))
        .expect("ServerHandle::shutdown must return (graceful drain + thread join)");

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut refused = false;
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(addr).is_err() {
            refused = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        refused,
        "a shut-down server must stop accepting connections"
    );
}

#[test]
fn dropping_server_handle_keeps_serving() {
    let (addr, handle) = ReplicaServer::spawn_with_handle(replica(1)).unwrap();
    drop(handle);

    let mut ring = Ring::with_vnodes(1, 64);
    ring.add_node(1);
    let c = Coordinator::new(0, ring, 1, NetTransport::connect(&[(1, addr)]));
    run(c.write_with(b"k", b"v", WriteConcern::All)).unwrap();
    assert_eq!(
        run(c.read(b"k")).unwrap().unwrap().value.as_deref(),
        Some(&b"v"[..]),
        "server must keep serving after its handle is dropped"
    );
}

#[test]
fn sharded_server_handle_shutdown_joins_all() {
    let (addrs, handle) = ShardedServer::spawn_with_handle(vec![replica(1), replica(1)]).unwrap();
    assert_eq!(addrs.len(), 2);

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        handle.shutdown();
        let _ = tx.send(());
    });
    rx.recv_timeout(Duration::from_secs(10))
        .expect("ShardedServer handle shutdown must join every shard");

    for addr in addrs {
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut refused = false;
        while Instant::now() < deadline {
            if std::net::TcpStream::connect(addr).is_err() {
                refused = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(refused, "every shard must stop accepting after shutdown");
    }
}

#[test]
fn transport_bounds_drop_slow_and_stalled_clients_without_starving_healthy_clients() {
    const CHILD: &str = "VALS_TRANSPORT_BOUNDS_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("transport_bounds_drop_slow_and_stalled_clients_without_starving_healthy_clients")
            .arg("--nocapture")
            .env(CHILD, "1")
            .env("VALS_TRANSPORT_UNBOUNDED", "0")
            .env("VALS_TRANSPORT_MAX_CONNECTIONS", "2")
            .env("VALS_TRANSPORT_MAX_REQUESTS", "16")
            .env("VALS_TRANSPORT_MAX_QUEUED_BYTES", "4096")
            .env("VALS_TRANSPORT_INITIAL_TIMEOUT_MS", "100")
            .env("VALS_TRANSPORT_FRAME_TIMEOUT_MS", "100")
            .env("VALS_TRANSPORT_IDLE_TIMEOUT_MS", "2000")
            .env("VALS_TRANSPORT_WRITE_TIMEOUT_MS", "100")
            .status()
            .unwrap();
        assert!(status.success(), "isolated transport-bounds test failed: {status}");
        return;
    }

    use std::io::{Read, Write};
    use std::net::TcpStream;

    crate::transport::configure(crate::transport::TransportLimits::from_env().unwrap()).unwrap();
    let addr = ReplicaServer::spawn(replica(1)).unwrap();
    let mut slow1 = TcpStream::connect(addr).unwrap();
    let mut slow2 = TcpStream::connect(addr).unwrap();
    let mut rejected = TcpStream::connect(addr).unwrap();
    rejected.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let mut one = [0u8; 1];
    assert_eq!(rejected.read(&mut one).unwrap_or(0), 0);
    std::thread::sleep(Duration::from_millis(400));
    slow1.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    slow2.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    assert_eq!(slow1.read(&mut one).unwrap_or(0), 0);
    assert_eq!(slow2.read(&mut one).unwrap_or(0), 0);

    let mut partial = TcpStream::connect(addr).unwrap();
    crate::proto::client_handshake_blocking(&mut partial).unwrap();
    let payload = wire::encode_request_payload(&Request::Read {
        epoch: 0,
        zone_id: DEFAULT_ZONE,
        key: b"complete-before-partial",
    });
    let mut complete_and_partial = ((8 + payload.len()) as u32).to_le_bytes().to_vec();
    complete_and_partial.extend_from_slice(&1u64.to_le_bytes());
    complete_and_partial.extend_from_slice(&payload);
    complete_and_partial.extend_from_slice(&100u32.to_le_bytes());
    complete_and_partial.push(0);
    partial.write_all(&complete_and_partial).unwrap();
    std::thread::sleep(Duration::from_millis(400));
    partial.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let mut drained = Vec::new();
    partial.read_to_end(&mut drained).unwrap();
    assert!(!drained.is_empty());

    let mut ring = Ring::with_vnodes(1, 64);
    ring.add_node(1);
    let client = Coordinator::new(0, ring.clone(), 1, NetTransport::connect(&[(1, addr)]));
    let held_requests: Vec<_> = (0..16)
        .map(|_| crate::transport::acquire_request().unwrap())
        .collect();
    assert!(run(client.read(b"request-budget")).is_err());
    drop(held_requests);
    run(client.write(b"large", &vec![7u8; 512])).unwrap();
    assert_eq!(run(client.read(b"large")).unwrap().unwrap().value.unwrap().len(), 512);
    drop(client);

    let mut stalled = TcpStream::connect(addr).unwrap();
    stalled.set_write_timeout(Some(Duration::from_secs(2))).unwrap();
    crate::proto::client_handshake_blocking(&mut stalled).unwrap();
    let payload = wire::encode_request_payload(&Request::Read {
        epoch: 0,
        zone_id: DEFAULT_ZONE,
        key: b"large",
    });
    let mut frame = ((8 + payload.len()) as u32).to_le_bytes().to_vec();
    frame.extend_from_slice(&1u64.to_le_bytes());
    frame.extend_from_slice(&payload);
    for _ in 0..20_000 {
        if stalled.write_all(&frame).is_err() {
            break;
        }
    }
    std::thread::sleep(Duration::from_millis(400));

    let healthy = Coordinator::new(2, ring, 1, NetTransport::connect(&[(1, addr)]));
    run(healthy.write(b"healthy", b"ok")).unwrap();
    assert_eq!(
        run(healthy.read(b"healthy")).unwrap().unwrap().value.as_deref(),
        Some(&b"ok"[..])
    );

    let counters = crate::observe::snapshot();
    assert!(counters.transport_connection_rejections >= 1);
    assert!(counters.transport_request_rejections >= 1);
    assert!(counters.transport_initial_timeouts >= 2);
    assert!(counters.transport_frame_timeouts >= 1);
    assert!(counters.transport_queued_byte_rejections >= 1);
    let limits = crate::transport::limits();
    let usage = crate::transport::usage();
    assert!(usage.0 <= limits.max_connections);
    assert!(usage.1 <= limits.max_requests);
    assert!(usage.2 <= limits.max_queued_bytes);
}

#[test]
fn rehome_copy_holds_while_the_target_keeps_an_old_leftover_the_source_lacks() {
    use crate::liveness::{LivenessLog, MemLivenessLog};
    use crate::node::Node;
    use crate::replica::Applied;
    use crate::zonestore::{MemZoneStore, ZoneGeom};

    let zone_id = 11;
    let cfg = ZoneConfig { name: "leftover".into(), rf: 1, ordering: Ordering::Sorted, scope: None };
    let ring = Ring::from_members(1, 64, [1, 2, 3]);
    let mut boundary = b"m".to_vec();
    while ring.replicas_in_zone(zone_id, b"", 1, None) == ring.replicas_in_zone(zone_id, &boundary, 1, None) {
        boundary.push(b'x');
    }
    let source = ring.replicas_in_zone(zone_id, b"", 1, None)[0];
    let target = ring.replicas_in_zone(zone_id, &boundary, 1, None)[0];
    let binds: Vec<_> = (0..3).map(|_| crate::node::Node::bind().unwrap()).collect();
    let peers: Vec<(NodeId, SocketAddr)> =
        binds.iter().enumerate().map(|(i, (_, addr))| ((i + 1) as NodeId, *addr)).collect();
    let now = crate::hlc::physical_now_ms();
    let mut handles = Vec::new();
    for (i, (listener, _)) in binds.into_iter().enumerate() {
        let node = (i + 1) as NodeId;
        let provider =
            MemZoneStore::new(node, ZoneGeom {
                block_size: 256,
                seg_blocks: 4,
                reserved_blocks: 0,
                region_blocks: 8192,
            });
        let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        let mut catalog = Catalog::new();
        catalog.insert(zone_id, cfg.clone());
        let log = MemLivenessLog::new();
        if node != target {
            log.store(now - 1_000).unwrap();
        }
        handles.push(crate::testutil::running(Node::serve_zoned_with_liveness_and_settings(
            node,
            listener,
            zone0,
            provider,
            catalog,
            ring.clone(),
            peers.clone(),
            Duration::from_secs(3600),
            60_000,
            log,
            None,
            crate::node_settings::NodeSettings { rehome_copy_page_entries: 64, ..Default::default() },
        )));
    }
    for (_, addr) in &peers {
        push_members_full(*addr, 1, &peers);
    }
    let addr = |node: NodeId| peers[(node - 1) as usize].1;
    let map = |token: &[u8], pending: Option<&[u8]>| {
        RangeMap::from_splits_tokens_and_pending(
            vec![boundary.clone()],
            vec![Vec::new(), token.to_vec()],
            vec![None, pending.map(<[u8]>::to_vec)],
        )
        .unwrap()
    };
    let push = |epoch: u64, map: &RangeMap| {
        for (_, a) in &peers {
            push_range_map_full(*a, epoch, zone_id, map);
        }
    };
    let apply = |node: NodeId, key: &[u8], value: Option<&[u8]>, ms: u64| {
        rpc_request(addr(node), &Request::Apply {
            epoch: u64::MAX,
            zone_id,
            key,
            value,
            version: crate::hlc::Version { hlc: ms << crate::hlc::COUNTER_BITS, node: 9 },
        })
    };
    let eventually = |what: &str, mut ok: Box<dyn FnMut() -> bool + '_>| {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !ok() {
            assert!(Instant::now() < deadline, "timed out: {what}");
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    let mut zombie = boundary.clone();
    zombie.extend_from_slice(b"/zombie");
    let mut live = boundary.clone();
    live.extend_from_slice(b"/live");

    push(10, &map(&boundary, None));
    eventually("the source takes writes", Box::new(|| apply(source, b"a-anchor", Some(b"a"), now) == Response::Apply(Applied::Won)));
    eventually(
        "the old owner takes the value",
        Box::new(|| apply(target, &zombie, Some(b"zombie"), now - 600_000) == Response::Apply(Applied::Won)),
    );
    push(11, &map(b"", None));
    eventually("the source owns the range", Box::new(|| apply(source, &live, Some(b"live"), now) == Response::Apply(Applied::Won)));
    assert_eq!(
        apply(source, &zombie, Some(b"zombie"), now - 600_000),
        Response::Apply(Applied::Rejected),
        "the source refuses the old value, as if it had collected its delete"
    );

    let blocks_before = crate::observe::snapshot().rehome_copy_target_only_blocks;
    push(12, &map(b"", Some(&boundary)));
    eventually(
        "the copy notices the leftover",
        Box::new(|| crate::observe::snapshot().rehome_copy_target_only_blocks > blocks_before),
    );
    std::thread::sleep(Duration::from_millis(500));
    let (_, stats) = range_stats(addr(target));
    let stat = stats.into_iter().find(|s| s.zone_id == zone_id && s.lower_bound == boundary).unwrap();
    assert!(stat.rehome_copy_complete_epoch < 12, "the copy completed over a stale leftover: {stat:?}");

    assert_eq!(apply(target, &zombie, None, now), Response::Apply(Applied::Won), "the operator deletes the leftover");
    eventually(
        "the copy completes once the leftover is gone",
        Box::new(|| {
            let (_, stats) = range_stats(addr(target));
            stats.iter().any(|s| s.zone_id == zone_id && s.lower_bound == boundary && s.rehome_copy_complete_epoch >= 12)
        }),
    );
    drop(handles);
}

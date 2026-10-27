use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use vals_cluster::reactor;
use vals_cluster::{Coordinator, WriteConcern, NetTransport, Ring, ZoneId};

fn run<F: std::future::Future>(f: F) -> F::Output {
    vals_rt::block_on(f, reactor::poll_io)
}

fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn wait_listening(addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "{addr} never started listening");
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn spawn(role: &str, config: &Path) -> Child {
    let log = std::fs::File::create(config.with_extension("log")).unwrap();
    Command::new(env!("CARGO_BIN_EXE_valsd"))
        .args([role, "-c", config.to_str().unwrap()])
        .env("VALS_LOG", "info")
        .env("VALS_FLUSH_TAIL_EPOCHS", "1")
        .env("VALS_FLUSH_INTERVAL_MS", "100")
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .expect("spawn valsd")
}

fn stop(mut child: Child, who: &str) {
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
    }
    let status = child.wait().unwrap();
    assert!(status.success(), "{who} did not exit cleanly: {status}");
}

fn admin(sock: &Path, cmd: &str) -> String {
    let mut conn = UnixStream::connect(sock).expect("admin socket");
    conn.write_all(cmd.as_bytes()).unwrap();
    conn.write_all(b"\n").unwrap();
    conn.shutdown(std::net::Shutdown::Write).unwrap();
    let mut reply = String::new();
    conn.read_to_string(&mut reply).unwrap();
    reply.trim_end().to_string()
}

fn eventually<T>(what: &str, mut check: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(v) = check() {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out: {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

const MAGIC: &[u8; 6] = b"VDUMP\x01";
const TOMB_SENTINEL: u32 = 0xFFFF_FFFF;

enum DumpRec {
    Put(Vec<u8>, Vec<u8>),
    Tomb(Vec<u8>),
}

fn parse_vdump(bytes: &[u8]) -> (String, Vec<DumpRec>) {
    let mut c = Cursor { b: bytes, off: 0 };
    assert_eq!(c.take(6), MAGIC, "bad magic");
    let nlen = c.take(1)[0] as usize;
    let name = String::from_utf8(c.take(nlen).to_vec()).unwrap();
    let mut recs = Vec::new();
    while c.off < bytes.len() {
        let klen = c.u32() as usize;
        let key = c.take(klen).to_vec();
        let vlen = c.u32();
        if vlen == TOMB_SENTINEL {
            recs.push(DumpRec::Tomb(key));
        } else {
            recs.push(DumpRec::Put(key, c.take(vlen as usize).to_vec()));
        }
    }
    (name, recs)
}

fn replay(recs: &[DumpRec]) -> HashMap<Vec<u8>, Option<Vec<u8>>> {
    let mut m: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();
    for r in recs {
        match r {
            DumpRec::Put(k, v) => {
                m.insert(k.clone(), Some(v.clone()));
            }
            DumpRec::Tomb(k) => {
                m.insert(k.clone(), None);
            }
        }
    }
    m
}

struct Cursor<'a> {
    b: &'a [u8],
    off: usize,
}
impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> &'a [u8] {
        let s = &self.b[self.off..self.off + n];
        self.off += n;
        s
    }
    fn u32(&mut self) -> u32 {
        u32::from_le_bytes(self.take(4).try_into().unwrap())
    }
}

fn bring_up(
    dir: &Path,
    addr: SocketAddr,
    node_admin: &Path,
    pd_admin: &Path,
    zone: &str,
) -> (Child, Child, Coordinator<NetTransport>, ZoneId) {
    std::fs::write(
        dir.join("node.conf"),
        format!(
            "node_id = 1\nlisten = {addr}\ndata_dir = node\nrf = 1\nsweep = 200ms\n\
             secret_file = secret\nseg_blocks = 2\nadmin_socket = {}\n",
            node_admin.display()
        ),
    )
    .unwrap();
    std::fs::write(
        dir.join("pd.conf"),
        format!(
            "pd_id = 1\ndata_dir = pd\ndata_node = 1 @ {addr}\nrf = 1\nvnodes = 64\n\
             zone = {zone} 1 unsorted\nadmin_socket = {}\nsecret_file = secret\n",
            pd_admin.display()
        ),
    )
    .unwrap();

    let node = spawn("node", &dir.join("node.conf"));
    let pd = spawn("pd", &dir.join("pd.conf"));
    wait_listening(addr);
    eventually("node admin socket up", || {
        UnixStream::connect(node_admin).ok().map(|_| ())
    });
    eventually("pd admin socket up", || {
        UnixStream::connect(pd_admin).ok().map(|_| ())
    });

    let mut ring = Ring::new(1);
    ring.set_members_weighted([(1, 1)]);
    let transport =
        NetTransport::connect(&[(1u32, addr)]).with_rpc_deadline(Duration::from_secs(5));
    let coord = Coordinator::new(100, ring, 1, transport);
    eventually("PD membership adopted", || {
        (run(coord.refresh_ring(1)).ok()? == vec![1]).then_some(())
    });
    let zone_id = eventually("boot zone adopted", || {
        run(coord.refresh_catalog(1)).ok()?;
        coord.catalog().by_name(zone)
    });
    (node, pd, coord, zone_id)
}

#[test]
fn live_node_admin_dump_round_trips_while_serving() {
    let dir = std::env::temp_dir().join(format!("valsd-live-dump-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("secret"), "live-dump-secret\n").unwrap();

    let zone = "records";
    let addr = free_addr();
    let node_admin = dir.join("node.sock");
    let pd_admin = dir.join("pd.sock");
    let (node, pd, coord, zid) = bring_up(&dir, addr, &node_admin, &pd_admin, zone);

    assert_eq!(admin(&node_admin, "ping"), "ok");

    let mut expected: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();
    let pad = "x".repeat(400);
    run(async {
        for i in 0..30u32 {
            let k = format!("k{i:03}");
            let v = format!("v{i}-0-{pad}");
            coord
                .write_in(zid, k.as_bytes(), v.as_bytes(), WriteConcern::Majority)
                .await
                .unwrap();
            expected.insert(k.into_bytes(), Some(v.into_bytes()));
        }
        for round in 1..=2u32 {
            for i in 0..10u32 {
                let k = format!("k{i:03}");
                let v = format!("v{i}-{round}-{pad}");
                coord
                    .write_in(zid, k.as_bytes(), v.as_bytes(), WriteConcern::Majority)
                    .await
                    .unwrap();
                expected.insert(k.into_bytes(), Some(v.into_bytes()));
            }
        }
        for i in 20..25u32 {
            let k = format!("k{i:03}");
            coord
                .delete_in(zid, k.as_bytes(), WriteConcern::Majority)
                .await
                .unwrap();
            expected.insert(k.into_bytes(), None);
        }
        coord
            .delete_in(zid, b"k021", WriteConcern::Majority)
            .await
            .unwrap();
        let revived = format!("revived-{pad}");
        coord
            .write_in(zid, b"k021", revived.as_bytes(), WriteConcern::Majority)
            .await
            .unwrap();
        expected.insert(b"k021".to_vec(), Some(revived.into_bytes()));
    });

    let out = dir.join("live.vdump");
    let out_s = out.to_str().unwrap();
    let live = eventually("live dump captures the full corpus", || {
        let reply = admin(&node_admin, &format!("dump {zone} {out_s}"));
        assert!(reply.starts_with("ok "), "dump reply: {reply}");
        let (name, recs) = parse_vdump(&std::fs::read(&out).unwrap());
        assert_eq!(name, zone, "header zone name");
        let got = replay(&recs);
        (got == expected).then_some((recs.len(), got))
    });
    let (rec_count, _got) = live;
    let live_keys = expected.values().filter(|v| v.is_some()).count();
    assert!(
        rec_count > live_keys,
        "raw dump must contain more records than live keys: {rec_count} records, {live_keys} live"
    );

    run(async {
        let got = coord.read_in(zid, b"k000").await.unwrap();
        assert!(got.and_then(|s| s.value).is_some(), "k000 must still read");
        coord
            .write_in(zid, b"post-dump", b"alive", WriteConcern::Majority)
            .await
            .unwrap();
        let got = coord.read_in(zid, b"post-dump").await.unwrap();
        assert_eq!(got.unwrap().value.as_deref(), Some(&b"alive"[..]));
    });
    expected.insert(b"post-dump".to_vec(), Some(b"alive".to_vec()));

    let out_par = dir.join("live-parallel.vdump");
    let out_par_s = out_par.to_str().unwrap();
    eventually("parallel live dump matches expected", || {
        let reply = admin(
            &node_admin,
            &format!("dump {zone} {out_par_s} parallel=4 rate_mib=1048576"),
        );
        assert!(reply.starts_with("ok "), "parallel dump reply: {reply}");
        assert!(
            reply.contains("rate limit 1048576 MiB/s"),
            "paced dump summary: {reply}"
        );
        let (_, recs) = parse_vdump(&std::fs::read(&out_par).unwrap());
        (replay(&recs) == expected).then_some(())
    });

    let bad_zone = admin(&node_admin, &format!("dump no-such-zone {out_s}"));
    assert!(bad_zone.starts_with("err:"), "missing zone: {bad_zone}");
    let bad_opt = admin(&node_admin, &format!("dump {zone} {out_s} bogus=1"));
    assert!(bad_opt.starts_with("err:"), "bad option: {bad_opt}");
    for rate in ["0", "nope", "-1"] {
        let bad_rate = admin(
            &node_admin,
            &format!("dump {zone} {out_s} rate_mib={rate}"),
        );
        assert!(
            bad_rate.starts_with("err: bad rate_mib="),
            "bad rate {rate:?}: {bad_rate}"
        );
    }
    let by_id = admin(&node_admin, &format!("dump {zid} {out_s}"));
    assert!(by_id.starts_with("ok "), "dump by id: {by_id}");

    stop(node, "node");
    stop(pd, "pd");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ping_answers_while_dump_in_flight() {
    let dir = std::env::temp_dir().join(format!("valsd-ping-mid-dump-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("secret"), "ping-mid-dump-secret\n").unwrap();

    let zone = "records";
    let addr = free_addr();
    let node_admin = dir.join("node.sock");
    let pd_admin = dir.join("pd.sock");
    let (node, pd, coord, zid) = bring_up(&dir, addr, &node_admin, &pd_admin, zone);

    let mut expected: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();
    let pad = "y".repeat(300);
    run(async {
        for i in 0..300u32 {
            let k = format!("k{i:04}");
            let v = format!("v{i}-{pad}");
            coord
                .write_in(zid, k.as_bytes(), v.as_bytes(), WriteConcern::Majority)
                .await
                .unwrap();
            expected.insert(k.into_bytes(), Some(v.into_bytes()));
        }
    });

    let base = dir.join("baseline.vdump");
    let base_s = base.to_str().unwrap();
    eventually("baseline dump captures the full corpus", || {
        let reply = admin(&node_admin, &format!("dump {zone} {base_s}"));
        assert!(reply.starts_with("ok "), "baseline dump reply: {reply}");
        let (name, recs) = parse_vdump(&std::fs::read(&base).unwrap());
        assert_eq!(name, zone, "header zone name");
        (replay(&recs) == expected).then_some(())
    });

    let fifo = dir.join("dump.fifo");
    let fifo_c = std::ffi::CString::new(fifo.to_str().unwrap()).unwrap();
    let rc = unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) };
    assert_eq!(rc, 0, "mkfifo failed: {}", std::io::Error::last_os_error());

    let mut dump_conn = UnixStream::connect(&node_admin).expect("admin socket");
    dump_conn
        .write_all(format!("dump {zone} {}\n", fifo.display()).as_bytes())
        .unwrap();
    dump_conn.shutdown(std::net::Shutdown::Write).unwrap();

    let mut ping_conn = UnixStream::connect(&node_admin).expect("admin socket");
    ping_conn
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    ping_conn.write_all(b"ping\n").unwrap();
    ping_conn.shutdown(std::net::Shutdown::Write).unwrap();
    let mut ping_reply = String::new();
    ping_conn
        .read_to_string(&mut ping_reply)
        .expect("ping must answer while a dump is in flight (not queue behind it)");
    assert_eq!(
        ping_reply.trim_end(),
        "ok",
        "ping reply while dump in flight"
    );

    let out2 = dir.join("concurrent.vdump");
    let out2_s = out2.to_str().unwrap();
    let reply2 = admin(&node_admin, &format!("dump {zone} {out2_s}"));
    assert!(reply2.starts_with("ok "), "concurrent dump reply: {reply2}");
    let (_, recs2) = parse_vdump(&std::fs::read(&out2).unwrap());
    assert_eq!(
        replay(&recs2),
        expected,
        "concurrent dump replays to expected"
    );

    let mut fifo_bytes = Vec::new();
    std::fs::File::open(&fifo)
        .expect("open fifo for read")
        .read_to_end(&mut fifo_bytes)
        .expect("drain fifo");

    let mut dump_reply = String::new();
    dump_conn
        .set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    dump_conn.read_to_string(&mut dump_reply).unwrap();
    assert!(
        dump_reply.starts_with("ok "),
        "in-flight dump reply: {dump_reply}"
    );
    let (name, recs) = parse_vdump(&fifo_bytes);
    assert_eq!(name, zone, "fifo dump header zone name");
    assert_eq!(replay(&recs), expected, "fifo dump replays to expected");

    stop(node, "node");
    stop(pd, "pd");
    let _ = std::fs::remove_dir_all(&dir);
}

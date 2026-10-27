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
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap()
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
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(v) = check() {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out: {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn write_cluster_configs(dir: &Path, tag: &str, addr: SocketAddr, admin_sock: &str, zone: &str) {
    std::fs::write(
        dir.join(format!("{tag}-node.conf")),
        format!(
            "node_id = 1\nlisten = {addr}\ndata_dir = {tag}-node\n\
             rf = 1\nsweep = 200ms\nsecret_file = secret\nseg_blocks = 2\n"
        ),
    )
    .unwrap();
    std::fs::write(
        dir.join(format!("{tag}-pd.conf")),
        format!(
            "pd_id = 1\ndata_dir = {tag}-pd\ndata_node = 1 @ {addr}\nrf = 1\nvnodes = 64\n\
             zone = {zone} 1 unsorted\nadmin_socket = {admin_sock}\nsecret_file = secret\n"
        ),
    )
    .unwrap();
}

fn bring_up(
    dir: &Path,
    tag: &str,
    addr: SocketAddr,
    admin_sock: &Path,
    zone: &str,
    client_id: u32,
) -> (Child, Child, Coordinator<NetTransport>, ZoneId) {
    let node = spawn("node", &dir.join(format!("{tag}-node.conf")));
    let pd = spawn("pd", &dir.join(format!("{tag}-pd.conf")));
    wait_listening(addr);
    eventually("admin socket up", || {
        UnixStream::connect(admin_sock).ok().map(|_| ())
    });

    let mut ring = Ring::new(1);
    ring.set_members_weighted([(1, 1)]);
    let transport =
        NetTransport::connect(&[(1u32, addr)]).with_rpc_deadline(Duration::from_secs(5));
    let coord = Coordinator::new(client_id, ring, 1, transport);
    eventually("PD membership adopted", || {
        (run(coord.refresh_ring(1)).ok()? == vec![1]).then_some(())
    });
    let zone_id = eventually("boot zone adopted", || {
        run(coord.refresh_catalog(1)).ok()?;
        coord.catalog().by_name(zone)
    });
    (node, pd, coord, zone_id)
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
            let val = c.take(vlen as usize).to_vec();
            recs.push(DumpRec::Put(key, val));
        }
    }
    (name, recs)
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

#[test]
fn dump_round_trips_to_exact_final_state() {
    let dir = std::env::temp_dir().join(format!("valsd-dump-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("secret"), "dump-e2e-secret\n").unwrap();

    let zone = "records";
    let addr_a = free_addr();
    let admin_a = dir.join("a-pd.sock");
    write_cluster_configs(&dir, "a", addr_a, "a-pd.sock", zone);

    let (node_a, pd_a, coord_a, zid_a) = bring_up(&dir, "a", addr_a, &admin_a, zone, 100);
    assert_eq!(admin(&admin_a, "ping"), "ok");

    let mut expected: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();
    let pad = "x".repeat(400);
    run(async {
        for i in 0..30u32 {
            let k = format!("k{i:03}");
            let v = format!("v{i}-0-{pad}");
            coord_a
                .write_in(zid_a, k.as_bytes(), v.as_bytes(), WriteConcern::Majority)
                .await
                .unwrap();
            expected.insert(k.into_bytes(), Some(v.into_bytes()));
        }
        for round in 1..=2u32 {
            for i in 0..10u32 {
                let k = format!("k{i:03}");
                let v = format!("v{i}-{round}-{pad}");
                coord_a
                    .write_in(zid_a, k.as_bytes(), v.as_bytes(), WriteConcern::Majority)
                    .await
                    .unwrap();
                expected.insert(k.into_bytes(), Some(v.into_bytes()));
            }
        }
        for i in 20..25u32 {
            let k = format!("k{i:03}");
            coord_a
                .delete_in(zid_a, k.as_bytes(), WriteConcern::Majority)
                .await
                .unwrap();
            expected.insert(k.into_bytes(), None);
        }
        coord_a
            .delete_in(zid_a, b"k005", WriteConcern::Majority)
            .await
            .unwrap();
        expected.insert(b"k005".to_vec(), None);
        let revived = format!("revived-{pad}");
        coord_a
            .write_in(zid_a, b"k021", revived.as_bytes(), WriteConcern::Majority)
            .await
            .unwrap();
        expected.insert(b"k021".to_vec(), Some(revived.into_bytes()));
    });

    stop(node_a, "node A");
    stop(pd_a, "pd A");

    let out = dir.join("records.vdump");
    let node_a_conf = dir.join("a-node.conf");
    let status = Command::new(env!("CARGO_BIN_EXE_valsd"))
        .args([
            "dump",
            "-c",
            node_a_conf.to_str().unwrap(),
            "--zone",
            zone,
            "--out",
            out.to_str().unwrap(),
        ])
        .env("VALS_LOG", "info")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .expect("run valsd dump");
    assert!(status.success(), "valsd dump failed: {status}");

    let out_par = dir.join("records-parallel.vdump");
    let status = Command::new(env!("CARGO_BIN_EXE_valsd"))
        .args([
            "dump",
            "-c",
            node_a_conf.to_str().unwrap(),
            "--zone",
            zone,
            "--out",
            out_par.to_str().unwrap(),
            "--parallel",
            "4",
        ])
        .env("VALS_LOG", "info")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .expect("run valsd dump --parallel 4");
    assert!(status.success(), "valsd dump --parallel failed: {status}");
    assert_eq!(
        std::fs::read(&out).unwrap(),
        std::fs::read(&out_par).unwrap(),
        "--parallel 4 dump must be byte-identical to the serial dump"
    );

    let out_by_id = dir.join("records-byid.vdump");
    let status = Command::new(env!("CARGO_BIN_EXE_valsd"))
        .args([
            "dump",
            "-c",
            node_a_conf.to_str().unwrap(),
            "--zone-id",
            &zid_a.to_string(),
            "--out",
            out_by_id.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("run valsd dump --zone-id");
    assert!(status.success(), "valsd dump --zone-id failed: {status}");
    let (_, recs_by_name) = parse_vdump(&std::fs::read(&out).unwrap());
    let (id_name, recs_by_id) = parse_vdump(&std::fs::read(&out_by_id).unwrap());
    assert_eq!(id_name, format!("zone-{zid_a}"), "--zone-id header name");
    assert_eq!(
        recs_by_name.len(),
        recs_by_id.len(),
        "--zone and --zone-id must dump the same records"
    );

    let bad = Command::new(env!("CARGO_BIN_EXE_valsd"))
        .args([
            "dump",
            "-c",
            node_a_conf.to_str().unwrap(),
            "--zone-id",
            "9999",
            "--out",
            dir.join("nope.vdump").to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("run valsd dump (missing zone)");
    assert!(!bad.success(), "dump of a nonexistent zone must fail");

    let bytes = std::fs::read(&out_par).unwrap();
    let (dumped_name, recs) = parse_vdump(&bytes);
    assert_eq!(dumped_name, zone, "header zone name");
    let live_keys = expected.values().filter(|v| v.is_some()).count();
    assert!(
        recs.len() > live_keys,
        "raw dump must contain more records than live keys (overwrites included): \
         {} records, {} live keys",
        recs.len(),
        live_keys
    );

    let addr_b = free_addr();
    let admin_b = dir.join("b-pd.sock");
    write_cluster_configs(&dir, "b", addr_b, "b-pd.sock", zone);
    let (node_b, pd_b, coord_b, zid_b) = bring_up(&dir, "b", addr_b, &admin_b, zone, 200);

    run(async {
        for rec in &recs {
            match rec {
                DumpRec::Put(k, v) => {
                    coord_b
                        .write_in(zid_b, k, v, WriteConcern::Majority)
                        .await
                        .unwrap();
                }
                DumpRec::Tomb(k) => {
                    coord_b
                        .delete_in(zid_b, k, WriteConcern::Majority)
                        .await
                        .unwrap();
                }
            }
        }
    });

    run(async {
        for (k, want) in &expected {
            let got = coord_b.read_in(zid_b, k).await.unwrap();
            match want {
                Some(v) => {
                    let got = got.unwrap_or_else(|| panic!("key {k:?} missing after replay"));
                    assert_eq!(
                        got.value.as_deref(),
                        Some(v.as_slice()),
                        "key {k:?} value mismatch after replay"
                    );
                }
                None => {
                    assert!(
                        got.is_none() || got.unwrap().value.is_none(),
                        "deleted key {k:?} must be absent after replay"
                    );
                }
            }
        }
    });

    stop(node_b, "node B");
    stop(pd_b, "pd B");
    let _ = std::fs::remove_dir_all(&dir);
}

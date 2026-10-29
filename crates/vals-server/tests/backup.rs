use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use vals_cluster::backup::{self, Record};
use vals_cluster::reactor;
use vals_cluster::wire::{self, Request, Response, RtFragment};
use vals_cluster::{Applied, Coordinator, WriteConcern, NetTransport, Ring, Version, ZoneId, DEFAULT_ZONE};

fn run<F: std::future::Future>(f: F) -> F::Output {
    vals_rt::block_on(f, reactor::poll_io)
}

fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

fn wait_listening(addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(addr).is_err() {
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

fn stop(mut child: Child) {
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
    }
    let _ = child.wait();
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

fn valsd(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_valsd")).args(args).output().expect("run valsd")
}

fn raw_request(addr: SocketAddr, request: &Request) -> Response {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    vals_cluster::proto::client_handshake_blocking(&mut s).unwrap();
    let payload = wire::encode_request_payload(request);
    let mut frame = ((8 + payload.len()) as u32).to_le_bytes().to_vec();
    frame.extend_from_slice(&1u64.to_le_bytes());
    frame.extend_from_slice(&payload);
    s.write_all(&frame).unwrap();
    let mut len = [0u8; 4];
    s.read_exact(&mut len).unwrap();
    let mut block = vec![0u8; u32::from_le_bytes(len) as usize];
    s.read_exact(&mut block).unwrap();
    wire::decode_response(&block[8..]).unwrap()
}

fn apply_request<'a>(zone_id: ZoneId, key: &'a [u8], value: Option<&'a [u8]>, version: Version) -> Request<'a> {
    Request::Apply { epoch: u64::MAX, zone_id, key, value, version }
}

fn raw_apply(addr: SocketAddr, zone_id: ZoneId, key: &[u8], value: Option<&[u8]>, version: Version) {
    let response = raw_request(addr, &apply_request(zone_id, key, value, version));
    assert_eq!(response, Response::Apply(Applied::Won), "direct replica apply");
}

struct Cluster {
    dir: PathBuf,
    addrs: Vec<SocketAddr>,
    nodes: Vec<Child>,
    pd: Option<Child>,
    admin_sock: PathBuf,
}

fn write_configs(dir: &Path, addrs: &[SocketAddr]) {
    write_configs_with(dir, addrs, "sweep = 3600s\n", "");
}

fn write_configs_with(dir: &Path, addrs: &[SocketAddr], node_extra: &str, pd_extra: &str) {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(dir.join("secret"), "backup-test-secret\n").unwrap();
    for id in 1..=3usize {
        let peers: String = (1..=3usize)
            .filter(|&p| p != id)
            .map(|p| format!("peer = {p} @ {}\n", addrs[p - 1]))
            .collect();
        std::fs::write(
            dir.join(format!("node{id}.conf")),
            format!(
                "node_id = {id}\nlisten = {}\ndata_dir = node{id}\n{peers}rf = 3\n{node_extra}\
                 secret_file = secret\n",
                addrs[id - 1]
            ),
        )
        .unwrap();
    }
    let data_nodes: String =
        (1..=3usize).map(|id| format!("data_node = {id} @ {}\n", addrs[id - 1])).collect();
    std::fs::write(
        dir.join("pd.conf"),
        format!(
            "pd_id = 1\ndata_dir = pd\n{data_nodes}rf = 3\nvnodes = 64\nzone = events 2 sorted\n{pd_extra}\
             admin_socket = pd-admin.sock\nsecret_file = secret\n"
        ),
    )
    .unwrap();
}

impl Cluster {
    fn start(name: &str) -> Cluster {
        let dir = std::env::temp_dir().join(format!("valsd-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let addrs: Vec<SocketAddr> = (0..3).map(|_| free_addr()).collect();
        write_configs(&dir, &addrs);
        Cluster::launch(dir, addrs)
    }

    fn launch(dir: PathBuf, addrs: Vec<SocketAddr>) -> Cluster {
        let nodes = (1..=3).map(|id| spawn("node", &dir.join(format!("node{id}.conf")))).collect();
        let pd = Some(spawn("pd", &dir.join("pd.conf")));
        for &addr in &addrs {
            wait_listening(addr);
        }
        let admin_sock = dir.join("pd-admin.sock");
        eventually("PD admin socket", || UnixStream::connect(&admin_sock).ok().map(|_| ()));
        Cluster { dir, addrs, nodes, pd, admin_sock }
    }

    fn stop_all(&mut self) {
        for node in self.nodes.drain(..) {
            stop(node);
        }
        if let Some(pd) = self.pd.take() {
            stop(pd);
        }
    }

    fn coordinator(&self) -> Coordinator<NetTransport> {
        let peers: Vec<(u32, SocketAddr)> = (1..=3u32).map(|id| (id, self.addrs[id as usize - 1])).collect();
        let mut ring = Ring::new(3);
        ring.set_members_weighted([(1, 1), (2, 1), (3, 1)]);
        let transport = NetTransport::connect(&peers).with_rpc_deadline(Duration::from_secs(2));
        let coord = Coordinator::new(100, ring, 2, transport);
        eventually("PD membership adopted", || {
            (run(coord.refresh_ring(1)).ok()? == vec![1, 2, 3]).then_some(())
        });
        coord
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.stop_all();
        if !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

type Expected = BTreeMap<ZoneId, BTreeMap<Vec<u8>, (Version, Option<Vec<u8>>)>>;

struct Populated {
    events: ZoneId,
    sessions: ZoneId,
    expected: Expected,
    ranges: Vec<RtFragment>,
    covered: Vec<Vec<u8>>,
    straggler: (Vec<u8>, Version),
}

fn range(lo: &str, hi: &str, version: Version) -> RtFragment {
    RtFragment { lo: Some(lo.as_bytes().to_vec()), hi: Some(hi.as_bytes().to_vec()), version }
}

fn read_backup_ranges(dir: &Path) -> BTreeMap<ZoneId, Vec<RtFragment>> {
    let backup = backup::open_backup(dir).unwrap();
    let mut out = BTreeMap::new();
    for zone in &backup.manifest.zones {
        let mut reader = backup::open_records(&dir.join(&zone.file.name)).unwrap();
        while reader.next_record().unwrap().is_some() {}
        out.insert(zone.id, reader.range_tombstones().to_vec());
    }
    out
}

fn read_backup(dir: &Path) -> Expected {
    let backup = backup::open_backup(dir).unwrap();
    let mut out = Expected::new();
    for zone in &backup.manifest.zones {
        let records = out.entry(zone.id).or_default();
        let mut reader = backup::open_records(&dir.join(&zone.file.name)).unwrap();
        while let Some(Record { key, version, value }) = reader.next_record().unwrap() {
            records.insert(key, (version, value));
        }
    }
    out
}

fn populate(cluster: &Cluster) -> Populated {
    let coord = cluster.coordinator();
    let events = eventually("boot zone adopted", || {
        run(coord.refresh_catalog(1)).ok()?;
        coord.catalog().by_name("events")
    });
    let created = admin(&cluster.admin_sock, "create-zone sessions 2 unsorted");
    assert!(created.starts_with("ok"), "{created}");
    let sessions = eventually("sessions zone adopted", || {
        run(coord.refresh_catalog(1)).ok()?;
        coord.catalog().by_name("sessions")
    });

    let mut expected = Expected::new();
    let mut put = |zone: ZoneId, key: String, value: String| {
        let version = run(coord.write_in(zone, key.as_bytes(), value.as_bytes(), WriteConcern::Majority))
            .unwrap_or_else(|e| panic!("write {key}: {e:?}"));
        expected.entry(zone).or_default().insert(key.into_bytes(), (version, Some(value.into_bytes())));
    };
    for i in 0..60 {
        put(DEFAULT_ZONE, format!("k-{i:03}"), format!("v-{i}"));
        put(events, format!("e-{i:03}"), format!("event-{i}"));
        put(sessions, format!("s-{i:03}"), format!("session-{i}"));
    }
    let split = admin(&cluster.admin_sock, "split-zone events e-030");
    assert!(split.starts_with("ok"), "{split}");
    eventually("events split committed", || {
        let ranges = admin(&cluster.admin_sock, "ranges events");
        ranges.contains("hex=").then_some(())
    });
    for i in (0..60).step_by(7) {
        put(DEFAULT_ZONE, format!("k-{i:03}"), format!("v-{i}-overwritten"));
        put(events, format!("e-{i:03}"), format!("event-{i}-overwritten"));
        put(sessions, format!("s-{i:03}"), format!("session-{i}-overwritten"));
    }
    for (zone, prefix) in [(DEFAULT_ZONE, "k"), (events, "e"), (sessions, "s")] {
        for i in (3..60).step_by(11) {
            let key = format!("{prefix}-{i:03}");
            let version = run(coord.delete_in(zone, key.as_bytes(), WriteConcern::Majority)).unwrap();
            expected.entry(zone).or_default().insert(key.into_bytes(), (version, None));
        }
    }
    let base = expected[&DEFAULT_ZONE][&b"k-010"[..]].0;
    let newer = Version { hlc: base.hlc + 5, node: 999 };
    raw_apply(cluster.addrs[2], DEFAULT_ZONE, b"k-010", Some(b"only-on-node-3"), newer);
    expected.get_mut(&DEFAULT_ZONE).unwrap().insert(b"k-010".to_vec(), (newer, Some(b"only-on-node-3".to_vec())));
    raw_apply(cluster.addrs[1], DEFAULT_ZONE, b"solo", Some(b"node-2"), newer);
    expected.get_mut(&DEFAULT_ZONE).unwrap().insert(b"solo".to_vec(), (newer, Some(b"node-2".to_vec())));

    let mut covered = Vec::new();
    let mut delete_range = |lo: u32, hi: u32| {
        for i in lo..hi {
            let key = format!("e-{i:03}").into_bytes();
            expected.get_mut(&events).unwrap().remove(&key);
            covered.push(key);
        }
    };
    let first = run(coord.delete_range_in(events, Some(b"e-010"), Some(b"e-020"), WriteConcern::Majority)).unwrap();
    delete_range(10, 20);
    let straggler = (b"e-015".to_vec(), Version { hlc: first.hlc - 1, node: 999 });
    let holder = coord.replicas_for(events, &straggler.0).unwrap()[0];
    assert_eq!(
        raw_request(cluster.addrs[holder as usize - 1], &apply_request(events, &straggler.0, Some(b"late"), straggler.1)),
        Response::Apply(Applied::Lost(first)),
        "a straggler older than the range delete loses"
    );
    let across_split =
        run(coord.delete_range_in(events, Some(b"e-025"), Some(b"e-035"), WriteConcern::Majority)).unwrap();
    delete_range(25, 35);
    let one_replica = Version { hlc: across_split.hlc + 1, node: 999 };
    let holder = coord.replicas_for(events, b"e-040").unwrap()[0];
    let slices: [wire::WireSlice<'_>; 1] = [(Some(&b"e-040"[..]), Some(&b"e-045"[..]))];
    let reply = raw_request(
        cluster.addrs[holder as usize - 1],
        &Request::RangeDelete { epoch: u64::MAX, zone_id: events, sync: true, version: one_replica, slices: &slices },
    );
    assert_eq!(reply, Response::Batch(vec![Applied::Won]), "a range delete only one replica holds");
    delete_range(40, 45);
    let newer = "event-12-after-the-range-delete";
    let version = run(coord.write_in(events, b"e-012", newer.as_bytes(), WriteConcern::Majority)).unwrap();
    expected.get_mut(&events).unwrap().insert(b"e-012".to_vec(), (version, Some(newer.as_bytes().to_vec())));
    covered.retain(|key| key != b"e-012");
    let ranges = vec![
        range("e-010", "e-020", first),
        range("e-025", "e-035", across_split),
        range("e-040", "e-045", one_replica),
    ];

    Populated { events, sessions, expected, ranges, covered, straggler }
}

#[test]
fn backup_captures_every_zone_once_with_versions_and_tombstones() {
    let cluster = Cluster::start("backup-create");
    let Populated { events, sessions, expected, ranges, .. } = populate(&cluster);

    let out = cluster.dir.join("bk");
    let sock = cluster.admin_sock.to_str().unwrap();
    let created = valsd(&["backup", "create", "-s", sock, "--out", out.to_str().unwrap()]);
    assert!(
        created.status.success(),
        "backup create failed: {}{}",
        String::from_utf8_lossy(&created.stdout),
        String::from_utf8_lossy(&created.stderr)
    );
    assert!(String::from_utf8_lossy(&created.stdout).starts_with("ok backup "));
    let again = valsd(&["backup", "create", "-s", sock, "--out", out.to_str().unwrap()]);
    assert_eq!(again.status.code(), Some(1), "an existing output directory is refused");

    let verified = valsd(&["backup", "verify", out.to_str().unwrap()]);
    assert!(verified.status.success(), "{}", String::from_utf8_lossy(&verified.stdout));

    let manifest = backup::open_backup(&out).unwrap().manifest;
    let status = admin(&cluster.admin_sock, "status");
    assert!(
        status.contains(&format!("incarnation={:032x}", manifest.source_incarnation)),
        "{status}"
    );
    assert_eq!((manifest.ring_vnodes, manifest.ring_rf), (64, 3));
    let ids: Vec<ZoneId> = manifest.zones.iter().map(|zone| zone.id).collect();
    assert_eq!(ids, vec![DEFAULT_ZONE, events, sessions]);
    let got = read_backup(&out);
    for zone in [DEFAULT_ZONE, events, sessions] {
        assert_eq!(got[&zone], expected[&zone], "zone {zone}: backup != logical state");
    }
    let got_ranges = read_backup_ranges(&out);
    assert_eq!(got_ranges[&events], ranges, "the backup holds every replica's range tombstones, merged");
    assert!(got_ranges[&DEFAULT_ZONE].is_empty() && got_ranges[&sessions].is_empty());

    let zone_file = out.join(backup::zone_file_name(events));
    let mut bytes = std::fs::read(&zone_file).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0x40;
    std::fs::write(&zone_file, &bytes).unwrap();
    let corrupt = valsd(&["backup", "verify", out.to_str().unwrap()]);
    assert_eq!(corrupt.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&corrupt.stdout).starts_with("err: "));

    let refused = admin(&cluster.admin_sock, "export-state relative/path");
    assert!(refused.starts_with("err: export-state needs an absolute path"), "{refused}");
}

fn ok(output: &Output, what: &str) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    assert!(
        output.status.success() && stdout.starts_with("ok "),
        "{what} failed: {stdout}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    stdout
}

fn incarnation_of(status: &str) -> String {
    let at = status.find("incarnation=").expect("status shows the incarnation") + "incarnation=".len();
    status[at..].split_whitespace().next().unwrap_or_default().to_string()
}

#[test]
fn restore_drill_rebuilds_an_exact_fenced_copy_from_a_backup() {
    let mut source = Cluster::start("restore-source");
    let Populated { events, sessions, ranges, covered, straggler, .. } = populate(&source);
    let bk = source.dir.join("bk");
    let bk_arg = bk.to_str().unwrap();
    let source_sock = source.admin_sock.to_str().unwrap().to_string();
    ok(&valsd(&["backup", "create", "-s", &source_sock, "--out", bk_arg]), "backup create");
    ok(&valsd(&["backup", "verify", bk_arg]), "backup verify");
    let expected = read_backup(&bk);
    assert_eq!(read_backup_ranges(&bk)[&events], ranges);
    let source_incarnation = incarnation_of(&admin(&source.admin_sock, "status"));
    source.stop_all();

    let target_dir = std::env::temp_dir().join(format!("valsd-restore-target-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&target_dir);
    let target_addrs: Vec<SocketAddr> = (0..3).map(|_| free_addr()).collect();
    write_configs(&target_dir, &target_addrs);
    let target_pd_conf = target_dir.join("pd.conf");
    let target_conf = target_pd_conf.to_str().unwrap();
    let wrong = target_dir.join("pd-wrong-ring.conf");
    std::fs::write(&wrong, std::fs::read_to_string(&target_pd_conf).unwrap().replace("vnodes = 64", "vnodes = 32"))
        .unwrap();
    let refused = valsd(&["backup", "restore-pd", "-c", wrong.to_str().unwrap(), bk_arg]);
    assert_eq!(refused.status.code(), Some(1), "{}", String::from_utf8_lossy(&refused.stdout));
    assert!(String::from_utf8_lossy(&refused.stdout).contains("vnodes 32"));
    let restored = ok(&valsd(&["backup", "restore-pd", "-c", target_conf, bk_arg]), "restore-pd");
    let again = valsd(&["backup", "restore-pd", "-c", target_conf, bk_arg]);
    assert!(String::from_utf8_lossy(&again.stdout).contains("is not empty"), "restore-pd never overwrites a PD");

    let target = Cluster::launch(target_dir.clone(), target_addrs);
    let target_sock = target.admin_sock.to_str().unwrap().to_string();
    let target_incarnation = eventually("restored PD leads", || {
        let status = admin(&target.admin_sock, "status");
        Some(incarnation_of(&status)).filter(|id| id.len() == 32)
    });
    assert_ne!(target_incarnation, source_incarnation, "a restore mints a new cluster incarnation");
    assert!(restored.contains(&format!("incarnation {target_incarnation}")), "{restored}");

    ok(&valsd(&["backup", "load", "-s", &target_sock, bk_arg]), "backup load");
    ok(&valsd(&["backup", "load", "-s", &target_sock, bk_arg]), "a repeated load is idempotent");
    let compared = ok(&valsd(&["backup", "compare", "-s", &target_sock, bk_arg]), "backup compare");
    assert!(compared.contains("target matches"), "{compared}");

    let coord = target.coordinator();
    eventually("restored catalog", || {
        run(coord.refresh_catalog(1)).ok()?;
        (coord.catalog().by_name("events") == Some(events) && coord.catalog().by_name("sessions") == Some(sessions))
            .then_some(())
    });
    for zone in [DEFAULT_ZONE, events, sessions] {
        for (key, (_, value)) in &expected[&zone] {
            let got = run(coord.read_in(zone, key)).unwrap().and_then(|stored| stored.value);
            assert_eq!(&got, value, "zone {zone} key {}", String::from_utf8_lossy(key));
        }
    }
    for key in &covered {
        let got = run(coord.read_in(events, key)).unwrap().and_then(|stored| stored.value);
        assert_eq!(got, None, "covered key {} stays deleted after the restore", String::from_utf8_lossy(key));
    }
    let scanned = run(coord.scan_in(events, None, None, 2, None)).unwrap();
    let want: Vec<&Vec<u8>> =
        expected[&events].iter().filter(|(_, (_, value))| value.is_some()).map(|(key, _)| key).collect();
    assert_eq!(scanned.iter().map(|(key, _)| key).collect::<Vec<_>>(), want, "a scan of the restored zone");
    for replica in coord.replicas_for(events, &straggler.0).unwrap() {
        let reply = raw_request(
            target.addrs[replica as usize - 1],
            &apply_request(events, &straggler.0, Some(b"late"), straggler.1),
        );
        assert!(
            matches!(reply, Response::Apply(Applied::Lost(_))),
            "a straggler still loses on restored replica {replica}: {reply:?}"
        );
    }

    let old_conf = source.dir.join("pd.conf");
    let mut text = std::fs::read_to_string(&old_conf).unwrap();
    for (from, to) in source.addrs.iter().zip(&target.addrs) {
        text = text.replace(&from.to_string(), &to.to_string());
    }
    std::fs::write(&old_conf, text).unwrap();
    source.pd = Some(spawn("pd", &old_conf));
    eventually("target nodes refuse the obsolete PD", || {
        let logs: String = (1..=3)
            .map(|id| std::fs::read_to_string(target_dir.join(format!("node{id}.log"))).unwrap_or_default())
            .collect();
        logs.contains(&format!("push carries {source_incarnation}")).then_some(())
    });
    eventually("the obsolete PD's admin socket", || UnixStream::connect(&source.admin_sock).ok().map(|_| ()));
    let rogue = admin(&source.admin_sock, "create-zone rogue 2 unsorted");
    assert!(rogue.starts_with("ok"), "{rogue}");
    std::thread::sleep(Duration::from_secs(2));
    run(coord.refresh_catalog(1)).unwrap();
    assert_eq!(coord.catalog().by_name("rogue"), None, "the obsolete PD's catalog never reaches the target");
    ok(&valsd(&["backup", "compare", "-s", &target_sock, bk_arg]), "compare with the old PD running");

    let (key, (version, _)) = expected[&events].iter().find(|(_, (_, value))| value.is_some()).unwrap();
    let newer = Version { hlc: version.hlc + 1, node: 999 };
    let owner = coord.replicas_for(events, key).unwrap()[0];
    raw_apply(target.addrs[owner as usize - 1], events, key, Some(b"diverged"), newer);
    let diverged = valsd(&["backup", "compare", "-s", &target_sock, bk_arg]);
    let stdout = String::from_utf8_lossy(&diverged.stdout);
    assert_eq!(diverged.status.code(), Some(1), "{stdout}");
    assert!(stdout.contains("1 differences"), "{stdout}");
    drop(target);
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
}

fn at_ms(ms: u64) -> Version {
    Version { hlc: ms << 16, node: 999 }
}

fn value_on(addr: SocketAddr, zone_id: ZoneId, key: &[u8]) -> Option<Vec<u8>> {
    match raw_request(addr, &Request::Read { epoch: u64::MAX, zone_id, key }) {
        Response::Read(stored) => stored.and_then(|s| s.value),
        other => panic!("read from {addr}: {other:?}"),
    }
}

#[test]
fn a_former_owner_copy_whose_delete_aged_out_is_neither_backed_up_nor_restored() {
    let dir = std::env::temp_dir().join(format!("valsd-former-owner-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let addrs: Vec<SocketAddr> = (0..3).map(|_| free_addr()).collect();
    write_configs_with(&dir, &addrs, "sweep = 1s\ngc_grace = 60s\n", "zone = sessions 2 unsorted\n");
    let mut source = Cluster::launch(dir, addrs);
    let coord = source.coordinator();
    let sessions = eventually("boot zone adopted", || {
        run(coord.refresh_catalog(1)).ok()?;
        coord.catalog().by_name("sessions")
    });
    let stray = |prefix: &str| -> (Vec<u8>, Vec<u32>) {
        (0..10_000)
            .map(|i| format!("{prefix}-{i}").into_bytes())
            .map(|key| {
                let owners = coord.replicas_for(sessions, &key).unwrap();
                (key, owners)
            })
            .find(|(_, owners)| !owners.contains(&3))
            .unwrap()
    };
    let (zombie, owners) = stray("zombie");
    let (fresh, _) = stray("fresh");
    let addr = |node: u32| source.addrs[node as usize - 1];
    let now = now_ms();
    for &owner in &owners {
        eventually("owner takes an old value before any newer write", || {
            let reply = raw_request(addr(owner), &apply_request(sessions, b"zz-aged", Some(b"aged"), at_ms(now - 3_600_000)));
            (reply == Response::Apply(Applied::Won)).then_some(())
        });
    }
    for &owner in &owners {
        let anchored = raw_request(addr(owner), &apply_request(sessions, b"anchor", Some(b"a"), at_ms(now)));
        assert!(matches!(anchored, Response::Apply(Applied::Won | Applied::Lost(_))), "{anchored:?}");
        let reply = raw_request(addr(owner), &apply_request(sessions, &zombie, None, at_ms(now - 3_000_000)));
        assert_eq!(reply, Response::Apply(Applied::Won), "a delete older than the grace is acknowledged");
        assert_eq!(value_on(addr(owner), sessions, &zombie), None);
    }
    eventually("the former owner serves the zone", || {
        let reply = raw_request(addr(3), &apply_request(sessions, &zombie, Some(b"zombie"), at_ms(now - 3_600_000)));
        (reply == Response::Apply(Applied::Won)).then_some(())
    });
    raw_apply(addr(3), sessions, &fresh, Some(b"fresh"), at_ms(now));
    std::thread::sleep(Duration::from_secs(3));
    for &owner in &owners {
        assert_eq!(value_on(addr(owner), sessions, &zombie), None, "owner {owner} took the stale copy back");
    }
    assert_eq!(value_on(addr(3), sessions, &zombie).as_deref(), Some(&b"zombie"[..]), "the stale copy stays put");
    let refused = eventually("removing an owner is refused while node 3 keeps the copy", || {
        let reply = admin(&source.admin_sock, &format!("remove-node {}", owners[0]));
        (reply.contains("node 3 shard 0") && reply.contains("kept 1 copies")).then_some(reply)
    });
    assert!(refused.starts_with("err: membership change refused, nothing applied"), "{refused}");
    assert!(refused.contains(&format!("zone {sessions}")) && refused.contains("--force"), "{refused}");
    let members = admin(&source.admin_sock, "members");
    assert_eq!(members.lines().skip(1).count(), 3, "nothing was removed: {members}");

    let bk = source.dir.join("bk");
    let bk_arg = bk.to_str().unwrap();
    let source_sock = source.admin_sock.to_str().unwrap().to_string();
    let refused = valsd(&["backup", "create", "-s", &source_sock, "--out", bk_arg]);
    let stdout = String::from_utf8_lossy(&refused.stdout);
    assert_eq!(refused.status.code(), Some(1), "an ambiguous former-owner copy fails the default: {stdout}");
    assert!(stdout.contains("--stale-copies omit"), "{stdout}");
    assert!(!bk.exists());
    let created =
        ok(&valsd(&["backup", "create", "-s", &source_sock, "--out", bk_arg, "--stale-copies", "omit"]), "backup create");
    let got = read_backup(&bk);
    assert!(!got[&sessions].contains_key(&zombie), "the backup holds the stale copy: {created}");
    assert_eq!(got[&sessions].get(&fresh).map(|(_, v)| v.clone()), Some(Some(b"fresh".to_vec())));
    assert_eq!(got[&sessions].get(&b"zz-aged"[..]).map(|(_, v)| v.clone()), Some(Some(b"aged".to_vec())));
    source.stop_all();

    let target_dir = std::env::temp_dir().join(format!("valsd-former-owner-target-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&target_dir);
    let target_addrs: Vec<SocketAddr> = (0..3).map(|_| free_addr()).collect();
    write_configs_with(&target_dir, &target_addrs, "sweep = 3600s\ngc_grace = 60s\n", "zone = sessions 2 unsorted\n");
    let target_conf = target_dir.join("pd.conf");
    ok(&valsd(&["backup", "restore-pd", "-c", target_conf.to_str().unwrap(), bk_arg]), "restore-pd");
    let target = Cluster::launch(target_dir, target_addrs);
    let target_sock = target.admin_sock.to_str().unwrap().to_string();
    eventually("restored PD leads", || {
        Some(incarnation_of(&admin(&target.admin_sock, "status"))).filter(|id| id.len() == 32)
    });
    ok(&valsd(&["backup", "load", "-s", &target_sock, bk_arg]), "backup load");
    let compared = ok(&valsd(&["backup", "compare", "-s", &target_sock, bk_arg]), "backup compare");
    assert!(compared.contains("target matches"), "{compared}");
    let coord = target.coordinator();
    eventually("restored catalog", || {
        run(coord.refresh_catalog(1)).ok()?;
        (coord.catalog().by_name("sessions") == Some(sessions)).then_some(())
    });
    assert_eq!(run(coord.read_in(sessions, &zombie)).unwrap().and_then(|s| s.value), None, "deleted stays deleted");
    assert_eq!(run(coord.read_in(sessions, &fresh)).unwrap().and_then(|s| s.value), Some(b"fresh".to_vec()));
    assert_eq!(
        run(coord.read_in(sessions, b"zz-aged")).unwrap().and_then(|s| s.value),
        Some(b"aged".to_vec()),
        "a value older than the target's tombstone-GC grace is restored"
    );
    drop(target);
}

struct PdGroup {
    dir: PathBuf,
    nodes: Vec<Child>,
    pds: Vec<Child>,
}

impl PdGroup {
    fn write_configs(dir: &Path, data_addrs: &[SocketAddr], raft_addrs: &[SocketAddr]) {
        write_configs(dir, data_addrs);
        let data_nodes: String =
            (1..=3usize).map(|id| format!("data_node = {id} @ {}\n", data_addrs[id - 1])).collect();
        let peers: String = (1..=3usize).map(|p| format!("pd_peer = {p} @ {}\n", raft_addrs[p - 1])).collect();
        for id in 1..=3usize {
            std::fs::write(
                dir.join(format!("pd{id}.conf")),
                format!(
                    "pd_id = {id}\ndata_dir = pd{id}\nlisten = {}\n{peers}{data_nodes}rf = 3\nvnodes = 64\n\
                     zone = events 2 sorted\nadmin_socket = pd{id}.sock\nsecret_file = secret\n",
                    raft_addrs[id - 1]
                ),
            )
            .unwrap();
        }
    }

    fn seed(dir: &Path, bk: &str, incarnations: [&str; 3]) {
        for (id, incarnation) in (1..=3).zip(incarnations) {
            let conf = dir.join(format!("pd{id}.conf"));
            let seeded = ok(
                &valsd(&["backup", "restore-pd", "-c", conf.to_str().unwrap(), "--incarnation", incarnation, bk]),
                "restore-pd",
            );
            assert!(seeded.contains("state digest"), "{seeded}");
            assert!(dir.join(format!("pd{id}/restore-seed")).exists(), "restore-pd records the seed it wrote");
        }
    }

    fn launch(dir: &Path, data_addrs: &[SocketAddr]) -> PdGroup {
        let nodes = (1..=3).map(|id| spawn("node", &dir.join(format!("node{id}.conf")))).collect();
        let pds = (1..=3).map(|id| spawn("pd", &dir.join(format!("pd{id}.conf")))).collect();
        for &addr in data_addrs {
            wait_listening(addr);
        }
        PdGroup { dir: dir.to_path_buf(), nodes, pds }
    }

    fn leader_sock(&self) -> String {
        eventually("a PD leader", || {
            (1..=3).map(|id| self.dir.join(format!("pd{id}.sock"))).find(|sock| {
                UnixStream::connect(sock).is_ok() && admin(sock, "status").contains("role=leader")
            })
        })
        .to_str()
        .unwrap()
        .to_string()
    }

    fn stop_all(&mut self) {
        for child in self.nodes.drain(..).chain(self.pds.drain(..)) {
            stop(child);
        }
    }
}

impl Drop for PdGroup {
    fn drop(&mut self) {
        self.stop_all();
    }
}

#[test]
fn a_three_pd_restore_refuses_voters_seeded_differently_and_loads_identical_seeds() {
    let mut source = Cluster::start("restore3-source");
    let Populated { events, .. } = populate(&source);
    let bk = source.dir.join("bk");
    let bk_arg = bk.to_str().unwrap().to_string();
    ok(&valsd(&["backup", "create", "-s", source.admin_sock.to_str().unwrap(), "--out", &bk_arg]), "backup create");
    let expected = read_backup(&bk);
    source.stop_all();

    let dir = std::env::temp_dir().join(format!("valsd-restore3-target-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let data_addrs: Vec<SocketAddr> = (0..3).map(|_| free_addr()).collect();
    let raft_addrs: Vec<SocketAddr> = (0..3).map(|_| free_addr()).collect();
    PdGroup::write_configs(&dir, &data_addrs, &raft_addrs);
    let unseeded = valsd(&["backup", "restore-pd", "-c", dir.join("pd1.conf").to_str().unwrap(), &bk_arg]);
    assert!(String::from_utf8_lossy(&unseeded.stdout).contains("--incarnation"), "a voter group needs one incarnation");
    let a = "0123456789abcdef0123456789abcdef";
    let b = "fedcba9876543210fedcba9876543210";
    PdGroup::seed(&dir, &bk_arg, [a, a, b]);
    let mut group = PdGroup::launch(&dir, &data_addrs);
    let leader = group.leader_sock();
    for what in ["load", "compare"] {
        let refused = eventually("the divergent voter's confirmation", || {
            let out = valsd(&["backup", what, "-s", &leader, &bk_arg]);
            let stdout = String::from_utf8_lossy(&out.stdout).to_string();
            stdout.contains("seeded differently").then_some((out.status.code(), stdout))
        });
        assert_eq!(refused.0, Some(1), "{what}: {}", refused.1);
        assert!(refused.1.contains("--incarnation"), "{what} says how to reseed: {}", refused.1);
    }
    eventually("the PD group reports the divergence itself", || {
        let logs: String =
            (1..=3).map(|id| std::fs::read_to_string(dir.join(format!("pd{id}.log"))).unwrap_or_default()).collect();
        logs.contains("hold divergent PD state").then_some(())
    });

    group.stop_all();
    for name in ["pd1", "pd2", "pd3", "node1", "node2", "node3"] {
        std::fs::remove_dir_all(dir.join(name)).unwrap();
    }
    PdGroup::seed(&dir, &bk_arg, [a, a, a]);
    group = PdGroup::launch(&dir, &data_addrs);
    let leader = group.leader_sock();
    assert!(admin(Path::new(&leader), "status").contains(&format!("incarnation={a}")));
    ok(&valsd(&["backup", "load", "-s", &leader, &bk_arg]), "backup load");
    let compared = ok(&valsd(&["backup", "compare", "-s", &leader, &bk_arg]), "backup compare");
    assert!(compared.contains("target matches"), "{compared}");
    let peers: Vec<(u32, SocketAddr)> = (1..=3u32).map(|id| (id, data_addrs[id as usize - 1])).collect();
    let mut ring = Ring::new(3);
    ring.set_members_weighted([(1, 1), (2, 1), (3, 1)]);
    let coord = Coordinator::new(100, ring, 2, NetTransport::connect(&peers).with_rpc_deadline(Duration::from_secs(2)));
    eventually("restored catalog", || {
        run(coord.refresh_catalog(1)).ok()?;
        (coord.catalog().by_name("events") == Some(events)).then_some(())
    });
    for (key, (_, value)) in &expected[&events] {
        let got = run(coord.read_in(events, key)).unwrap().and_then(|stored| stored.value);
        assert_eq!(&got, value, "key {}", String::from_utf8_lossy(key));
    }
    drop(group);
    let _ = std::fs::remove_dir_all(&dir);
}

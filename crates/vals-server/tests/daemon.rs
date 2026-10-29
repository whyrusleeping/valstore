use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use vals_cluster::reactor;
use vals_cluster::{Coordinator, WriteConcern, NetTransport, Ring};
use vals_cluster::coord::Transport;

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

fn sigterm(child: &Child) {
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
    }
}

fn sighup(child: &Child) {
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGHUP);
    }
}

fn stop(mut child: Child, who: &str) {
    sigterm(&child);
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

#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[test]
fn check_config_reports_allocator_cadence_and_disable() {
    let dir = std::env::temp_dir().join(format!("valsd-check-allocator-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let config = dir.join("node.conf");
    std::fs::write(&config, "node_id=1\nlisten=127.0.0.1:1\ndata_dir=node-data\nrf=1\n").unwrap();
    let check = |value: Option<&str>| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_valsd"));
        command.args(["node", "-c", config.to_str().unwrap(), "--check-config"])
            .env_remove("VALS_MALLOC_TRIM_SECS");
        if let Some(value) = value { command.env("VALS_MALLOC_TRIM_SECS", value); }
        command.output().unwrap()
    };
    for (value, effective) in [(None, 0), (Some("0"), 0), (Some("120"), 120)] {
        let output = check(value);
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        assert!(String::from_utf8_lossy(&output.stdout).contains(&format!("malloc_trim_secs={effective}")));
    }
    for value in ["-1", "bad", "4294967296"] {
        let output = check(Some(value));
        assert_eq!(output.status.code(), Some(2));
        assert!(String::from_utf8_lossy(&output.stderr).contains("VALS_MALLOC_TRIM_SECS"));
    }
    assert!(!dir.join("node-data").exists());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn check_config_reports_batch_apply_gate() {
    let dir = std::env::temp_dir().join(format!("valsd-check-batch-gate-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let config = dir.join("node.conf");
    let check = |configured: Option<usize>, alias: Option<&str>| {
        let mut text = "node_id=1\nlisten=127.0.0.1:1\ndata_dir=node-data\nrf=1\n".to_string();
        if let Some(limit) = configured { text.push_str(&format!("batch_apply_gate={limit}\n")); }
        std::fs::write(&config, text).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_valsd"));
        command.args(["node", "-c", config.to_str().unwrap(), "--check-config"])
            .env_remove("VALS_BATCH_APPLY_GATE");
        if let Some(value) = alias { command.env("VALS_BATCH_APPLY_GATE", value); }
        command.output().unwrap()
    };
    for (configured, alias, effective) in [
        (None, None, 64), (Some(32), None, 32), (None, Some("128"), 128),
        (Some(256), Some("256"), 256),
    ] {
        let output = check(configured, alias);
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        assert!(String::from_utf8_lossy(&output.stdout).contains(&format!("batch_apply_gate: {effective}")));
    }
    for (configured, alias) in [(Some(0), None), (Some(257), None), (None, Some("0")),
        (None, Some("257")), (Some(64), Some("32"))] {
        let output = check(configured, alias);
        assert_eq!(output.status.code(), Some(2));
        assert!(String::from_utf8_lossy(&output.stderr).contains("batch_apply_gate"));
    }
    assert!(!dir.join("node-data").exists());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn check_config_is_strict_and_side_effect_free() {
    let dir = std::env::temp_dir().join(format!("valsd-check-config-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let node_config = dir.join("node.conf");
    std::fs::write(
        &node_config,
        "node_id=1\nlisten=127.0.0.1:1\ndata_dir=node-data\nrf=1\n",
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_valsd"))
        .args(["node", "-c", node_config.to_str().unwrap(), "--check-config"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("effective geometry"), "{stdout}");
    assert!(stdout.contains("device_dealloc_segments_per_tick: 0"), "{stdout}");
    assert!(stdout.contains("require restart"), "{stdout}");
    assert!(!dir.join("node-data").exists());

    let check_node = |alias: Option<&str>| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_valsd"));
        command.args(["node", "-c", node_config.to_str().unwrap(), "--check-config"])
            .env_remove("VALS_LOG").env_remove("VALS_TRANSPORT_UNBOUNDED")
            .env_remove("VALS_SCAN_MAX_INFLIGHT");
        if let Some(value) = alias { command.env("VALS_SCAN_MAX_INFLIGHT", value); }
        command.output().unwrap()
    };
    let fallback = check_node(Some("3"));
    assert!(fallback.status.success());
    assert!(String::from_utf8_lossy(&fallback.stdout).contains("scan_max_inflight: 3"));
    assert!(String::from_utf8_lossy(&fallback.stderr).contains("deprecated node environment settings"));
    std::fs::OpenOptions::new().append(true).open(&node_config).unwrap()
        .write_all(b"scan_max_inflight=5\n").unwrap();
    let configured = check_node(None);
    assert!(configured.status.success());
    assert!(String::from_utf8_lossy(&configured.stdout).contains("scan_max_inflight: 5"));
    let equal = check_node(Some("5"));
    assert!(equal.status.success());
    assert!(!String::from_utf8_lossy(&equal.stderr).contains("VALS_SCAN_MAX_INFLIGHT"));
    for value in ["3", "bad", "0"] {
        let output = check_node(Some(value));
        assert_eq!(output.status.code(), Some(2));
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains("scan_max_inflight") && error.contains("VALS_SCAN_MAX_INFLIGHT"), "{error}");
    }
    assert!(!dir.join("node-data").exists());
    for (value, success) in [("debug", true), ("typo", false)] {
        let output = Command::new(env!("CARGO_BIN_EXE_valsd"))
            .args(["node", "-c", node_config.to_str().unwrap(), "--check-config"])
            .env("VALS_LOG", value).output().unwrap();
        assert_eq!(output.status.success(), success);
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains("VALS_LOG"), "{error}");
    }
    std::fs::OpenOptions::new().append(true).open(&node_config).unwrap()
        .write_all(b"log_level=info\n").unwrap();
    for (value, success) in [("info", true), ("debug", false), ("typo", false)] {
        let output = Command::new(env!("CARGO_BIN_EXE_valsd"))
            .args(["node", "-c", node_config.to_str().unwrap(), "--check-config"])
            .env("VALS_LOG", value).output().unwrap();
        assert_eq!(output.status.success(), success);
        if !success {
            let error = String::from_utf8_lossy(&output.stderr);
            assert!(error.contains("VALS_LOG") && error.contains("log_level"), "{error}");
        }
    }
    assert!(!dir.join("node-data").exists());

    let spdk_config = dir.join("spdk.conf");
    std::fs::write(
        &spdk_config,
        "node_id=2\nlisten=127.0.0.1:2\nbackend=spdk\npci=0000:ff:00.0\nrf=1\n",
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_valsd"))
        .args(["node", "--check-config", "-c", spdk_config.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("devices were not attached or formatted"), "{stdout}");
    assert!(stdout.contains("valsd provision"), "{stdout}");

    let pd_config = dir.join("pd.conf");
    std::fs::write(
        &pd_config,
        "pd_id=1\ndata_dir=pd-data\ndata_node=1@127.0.0.1:1\nrf=1\nzone=z 1 sorted\nadmin_socket=pd.sock\n",
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_valsd"))
        .args(["pd", "-c", pd_config.to_str().unwrap(), "--check-config"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("did not bind sockets, create directories"), "{stdout}");
    assert!(!dir.join("pd-data").exists());
    assert!(!dir.join("pd.sock").exists());

    for value in ["false", "invalid"] {
        let output = Command::new(env!("CARGO_BIN_EXE_valsd"))
            .args(["pd", "-c", pd_config.to_str().unwrap(), "--check-config"])
            .env("VALS_PD_REHOME_ENABLED", value)
            .output()
            .unwrap();
        if value == "false" {
            assert!(output.status.success());
            assert!(String::from_utf8_lossy(&output.stdout).contains("rehome_enabled: false"));
            assert!(String::from_utf8_lossy(&output.stderr).contains("deprecated PD environment settings"));
        } else {
            assert_eq!(output.status.code(), Some(2));
            assert!(String::from_utf8_lossy(&output.stderr).contains("VALS_PD_REHOME_ENABLED"));
        }
    }
    std::fs::OpenOptions::new().append(true).open(&pd_config).unwrap()
        .write_all(b"rehome_enabled=true\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_valsd"))
        .args(["pd", "-c", pd_config.to_str().unwrap(), "--check-config"])
        .env("VALS_PD_REHOME_ENABLED", "0")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("conflicting `rehome_enabled`"), "{stderr}");
    assert!(stderr.contains("VALS_PD_REHOME_ENABLED"), "{stderr}");
    assert!(!dir.join("pd-data").exists());

    let invalid_node = Command::new(env!("CARGO_BIN_EXE_valsd"))
        .args(["node", "-c", node_config.to_str().unwrap()])
        .env("VALS_SCAN_MAX_INFLIGHT", "bad").env_remove("VALS_LOG")
        .output().unwrap();
    assert_eq!(invalid_node.status.code(), Some(2));
    assert!(!dir.join("node-data").exists());

    let invalid = Command::new(env!("CARGO_BIN_EXE_valsd"))
        .args(["pd", "-c", pd_config.to_str().unwrap(), "--check-config", "extra"])
        .output()
        .unwrap();
    assert_eq!(invalid.status.code(), Some(2));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn three_node_cluster_with_pd_serves_zones_and_survives_restart() {
    let dir = std::env::temp_dir().join(format!("valsd-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("secret"), "e2e-test-secret\n").unwrap();

    let addrs: Vec<SocketAddr> = (0..3).map(|_| free_addr()).collect();
    let admin_sock = dir.join("pd-admin.sock");

    let metrics_addr = free_addr();
    for id in 1..=3u32 {
        let me = addrs[id as usize - 1];
        let peers: String = (1..=3u32)
            .filter(|&p| p != id)
            .map(|p| format!("peer = {p} @ {}\n", addrs[p as usize - 1]))
            .collect();
        let metrics = if id == 1 {
            format!("metrics = {metrics_addr}\n")
        } else {
            String::new()
        };
        let profile = match id {
            1 => "profile = latency\n",
            2 => "profile = capacity\n",
            _ => "",
        };
        std::fs::write(
            dir.join(format!("node{id}.conf")),
            format!(
                "node_id = {id}\nlisten = {me}\ndata_dir = node{id}\n{peers}{metrics}{profile}\
                 rf = 3\nsweep = 200ms\nsecret_file = secret\n"
            ),
        )
        .unwrap();
    }
    let data_nodes: String = (1..=3u32)
        .map(|id| {
            let lhs = if id == 2 {
                format!("{id}*2")
            } else {
                id.to_string()
            };
            format!("data_node = {lhs} @ {}\n", addrs[id as usize - 1])
        })
        .collect();
    std::fs::write(
        dir.join("pd.conf"),
        format!(
            "pd_id = 1\ndata_dir = pd\n{data_nodes}rf = 3\nvnodes = 64\n\
             zone = events 3 sorted\nadmin_socket = pd-admin.sock\nsecret_file = secret\n"
        ),
    )
    .unwrap();

    let mut nodes: Vec<Child> = (1..=3)
        .map(|id| spawn("node", &dir.join(format!("node{id}.conf"))))
        .collect();
    let pd = spawn("pd", &dir.join("pd.conf"));
    for &a in &addrs {
        wait_listening(a);
    }
    eventually("admin socket up", || {
        UnixStream::connect(&admin_sock).ok().map(|_| ())
    });
    assert_eq!(admin(&admin_sock, "ping"), "ok");

    let peers: Vec<(u32, SocketAddr)> = (1..=3u32).map(|id| (id, addrs[id as usize - 1])).collect();
    let mut ring = Ring::new(3);
    ring.set_members_weighted([(1, 1), (2, 1), (3, 1)]);
    let transport = NetTransport::connect(&peers).with_rpc_deadline(Duration::from_secs(2));
    let coord = Coordinator::new(100, ring.clone(), 2, transport);
    eventually("PD membership push adopted", || {
        let members = run(coord.refresh_ring(1)).ok()?;
        (members == vec![1, 2, 3]).then_some(())
    });
    let members_reply = eventually("PD boot membership committed", || {
        let reply = admin(&admin_sock, "members");
        (reply.lines().count() == 4).then_some(reply)
    });
    assert!(members_reply.starts_with("ok"), "{members_reply}");
    let node2_line = members_reply
        .lines()
        .find(|l| l.starts_with("2 "))
        .unwrap_or_else(|| panic!("no member line for node 2 in {members_reply}"));
    assert_eq!(
        node2_line.split_whitespace().nth(1),
        Some("2"),
        "node 2 should carry weight 2: {node2_line}"
    );

    run(async {
        for i in 0..50u32 {
            let k = format!("key-{i:03}");
            coord
                .write_with(
                    k.as_bytes(),
                    format!("val-{i}").as_bytes(),
                    WriteConcern::Majority,
                )
                .await
                .unwrap();
        }
        for i in 0..50u32 {
            let k = format!("key-{i:03}");
            let got = coord.read(k.as_bytes()).await.unwrap().expect("present");
            assert_eq!(got.value.as_deref(), Some(format!("val-{i}").as_bytes()));
        }
    });

    {
        let deadline = Instant::now() + Duration::from_secs(60);
        let reply = loop {
            let mut conn = TcpStream::connect(metrics_addr).expect("metrics endpoint up");
            conn.write_all(b"GET /metrics HTTP/1.0\r\n\r\n").unwrap();
            let mut reply = String::new();
            conn.read_to_string(&mut reply).unwrap();
            let ops: u64 = reply.lines().find_map(|l| l.strip_prefix("vals_server_ops "))
                .expect("vals_server_ops present").parse().unwrap();
            if ops >= 100 { break reply; }
            assert!(Instant::now() < deadline, "server_ops should reflect real traffic: {ops}");
            run(coord.transport().read(1, vals_cluster::zone::DEFAULT_ZONE, b"key-000")).unwrap();
            std::thread::sleep(Duration::from_millis(50));
        };
        assert!(reply.starts_with("HTTP/1.0 200 OK"), "{reply}");
        assert!(
            reply.contains("vals_profile_info{profile=\"latency\"} 1"),
            "{reply}"
        );
        assert!(reply.contains("vals_blind_append_enabled 1"), "{reply}");
        assert!(reply.contains("vals_winner_directory_enabled 1"), "{reply}");
        let ops: u64 = reply
            .lines()
            .find_map(|l| l.strip_prefix("vals_server_ops "))
            .expect("vals_server_ops present")
            .parse()
            .unwrap();
        assert!(ops >= 100, "server_ops should reflect real traffic: {ops}");
    }

    {
        let scrape_live_keys = || -> Option<u64> {
            let mut conn = TcpStream::connect(metrics_addr).ok()?;
            conn.write_all(b"GET /metrics HTTP/1.0\r\n\r\n").ok()?;
            let mut reply = String::new();
            conn.read_to_string(&mut reply).ok()?;
            reply
                .lines()
                .find_map(|l| l.strip_prefix("vals_live_keys "))
                .and_then(|v| v.parse().ok())
        };
        eventually("vals_live_keys reflects the 50 written keys", || {
            (scrape_live_keys() == Some(50)).then_some(())
        });
    }

    let events = eventually("boot zone committed + adopted", || {
        run(coord.refresh_catalog(1)).ok()?;
        coord.catalog().by_name("events")
    });
    let created = admin(&admin_sock, "create-zone sessions 2 unsorted");
    assert!(created.starts_with("ok"), "{created}");
    let sessions = eventually("admin-created zone adopted", || {
        run(coord.refresh_catalog(1)).ok()?;
        coord.catalog().by_name("sessions")
    });

    run(async {
        for i in [3u32, 1, 2, 0] {
            let k = format!("2026-06-{i:02}");
            coord
                .write_in(events, k.as_bytes(), b"e", WriteConcern::Majority)
                .await
                .unwrap();
        }
        let rows = coord.scan_in(events, None, None, 2, None).await.unwrap();
        let keys: Vec<&[u8]> = rows.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(
            keys,
            vec![
                &b"2026-06-00"[..],
                b"2026-06-01",
                b"2026-06-02",
                b"2026-06-03"
            ]
        );

        coord
            .write_in(sessions, b"sess", b"alive", WriteConcern::Majority)
            .await
            .unwrap();
        let got = coord.read_in(sessions, b"sess").await.unwrap().unwrap();
        assert_eq!(got.value.as_deref(), Some(&b"alive"[..]));
    });

    for n in nodes.drain(..) {
        stop(n, "node");
    }
    let nodes: Vec<Child> = (1..=3)
        .map(|id| spawn("node", &dir.join(format!("node{id}.conf"))))
        .collect();
    for &a in &addrs {
        wait_listening(a);
    }

    let transport2 = NetTransport::connect(&peers).with_rpc_deadline(Duration::from_secs(2));
    let coord2 = Coordinator::new(101, ring, 2, transport2);
    eventually("restarted nodes re-adopt the pushed config", || {
        let members = run(coord2.refresh_ring(1)).ok()?;
        (members == vec![1, 2, 3]).then_some(())
    });
    eventually("post-restart strong reads serve recovered data", || {
        run(async {
            coord2.refresh_ring(1).await.ok()?;
            for i in 0..50u32 {
                let k = format!("key-{i:03}");
                let got = coord2.read(k.as_bytes()).await.ok()??;
                if got.value.as_deref() != Some(format!("val-{i}").as_bytes()) {
                    return None;
                }
            }
            Some(())
        })
    });
    let rows = eventually("post-restart sorted scan serves recovered rows", || {
        run(async {
            coord2.refresh_ring(1).await.ok()?;
            coord2.refresh_catalog(1).await.ok()?;
            coord2.catalog().by_name("events")?;
            coord2.scan_in(events, None, None, 2, None).await.ok()
        })
    });
    assert_eq!(rows.len(), 4, "sorted zone lost rows across restart");
    let got = eventually("post-restart unsorted zone read", || {
        run(async { coord2.read_in(sessions, b"sess").await.ok()? })
    });
    assert_eq!(got.value.as_deref(), Some(&b"alive"[..]));

    for n in nodes {
        stop(n, "node");
    }
    stop(pd, "pd");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn node_reloads_config_on_sighup() {
    let dir = std::env::temp_dir().join(format!("valsd-reload-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let addr = free_addr();
    let conf = dir.join("node1.conf");
    let log = conf.with_extension("log");

    let read_log = || std::fs::read_to_string(&log).unwrap_or_default();
    let wait_log = |needle: &str| {
        eventually(&format!("log contains {needle:?}"), || {
            read_log().contains(needle).then_some(())
        });
    };

    std::fs::write(
        &conf,
        format!(
            "node_id = 1\nlisten = {addr}\ndata_dir = node1\n\
             backend = file\nprofile = capacity\nrf = 1\nsweep = 200ms\nlog_level = info\n"
        ),
    )
    .unwrap();

    let node = spawn("node", &conf);
    wait_listening(addr);

    std::fs::write(
        &conf,
        format!(
            "node_id = 1\nlisten = {addr}\ndata_dir = node1\n\
             backend = file\nprofile = latency\nrf = 1\nsweep = 100ms\nlog_level = debug\nregion_mib = 512\n"
        ),
    )
    .unwrap();
    sighup(&node);

    wait_log("config reloaded (applied:");
    let after_reload = read_log();
    assert!(
        after_reload.contains("node 1: log level now DEBUG"),
        "log level should have applied:\n{after_reload}"
    );
    assert!(
        after_reload.contains("sweep cadence 200ms -> 100ms"),
        "sweep should have applied:\n{after_reload}"
    );
    assert!(
        after_reload
            .contains("`region_mib` changed but is not reloadable; keeping the running value"),
        "region_mib change should be logged-and-ignored:\n{after_reload}"
    );
    let summary = after_reload
        .lines()
        .find(|l| l.contains("config reloaded (applied:"))
        .expect("reload summary line");
    assert!(
        summary.contains("log_level") && summary.contains("sweep"),
        "{summary}"
    );
    assert!(summary.contains("region_mib"), "{summary}");
    assert!(
        after_reload.contains("`profile` changed but is not reloadable; keeping the running value"),
        "profile change should require restart:\n{after_reload}"
    );
    assert!(summary.contains("profile"), "{summary}");

    std::fs::write(&conf, "node_id = 1\nlisten = this-is-not-an-address\n").unwrap();
    sighup(&node);
    wait_log("config reload failed");
    let after_bad = read_log();
    assert!(
        after_bad.contains("keeping the running config"),
        "a bad reload must keep the running config:\n{after_bad}"
    );
    assert!(
        TcpStream::connect(addr).is_ok(),
        "node must keep serving after a failed reload"
    );

    stop(node, "node");
    let _ = std::fs::remove_dir_all(&dir);
}

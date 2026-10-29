use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use vals_cluster::reactor;
use vals_cluster::{Coordinator, WriteConcern, NetTransport, Ring};

const KEYS: u32 = 3000;

fn run<F: std::future::Future>(f: F) -> F::Output {
    vals_rt::block_on(f, reactor::poll_io)
}

fn wait_listening(addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(addr).is_err() {
        assert!(Instant::now() < deadline, "{addr} never started listening");
        std::thread::sleep(Duration::from_millis(25));
    }
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

fn eventually<T>(what: &str, timeout: Duration, mut check: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(v) = check() {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out: {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn scrape(addr: SocketAddr, name: &str) -> Option<u64> {
    let mut conn = TcpStream::connect(addr).ok()?;
    conn.write_all(b"GET /metrics HTTP/1.0\r\n\r\n").ok()?;
    let mut reply = String::new();
    conn.read_to_string(&mut reply).ok()?;
    let prefix = format!("{name} ");
    reply.lines().find_map(|l| l.strip_prefix(&prefix)).and_then(|v| v.trim().parse().ok())
}

fn key(i: u32) -> Vec<u8> {
    format!("e-{i:05}").into_bytes()
}

struct Cluster {
    dir: PathBuf,
    children: Vec<Child>,
}

impl Cluster {
    fn spawn(&mut self, role: &str, config: &Path) {
        let log = std::fs::File::create(config.with_extension("log")).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_valsd"))
            .args([role, "-c", config.to_str().unwrap()])
            .env("VALS_LOG", "info")
            .env_remove("VALS_AE_SPAN_REGROUP")
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .expect("spawn valsd");
        self.children.push(child);
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for child in &self.children {
            unsafe {
                libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
            }
        }
        let deadline = Instant::now() + Duration::from_secs(20);
        for child in &mut self.children {
            loop {
                match child.try_wait() {
                    Ok(Some(_)) | Err(_) => break,
                    Ok(None) if Instant::now() >= deadline => {
                        let _ = child.kill();
                        let _ = child.wait();
                        break;
                    }
                    Ok(None) => std::thread::sleep(Duration::from_millis(50)),
                }
            }
        }
        if !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

#[derive(Debug)]
struct Deltas {
    seed_starts: Vec<u64>,
    invalidations: Vec<u64>,
    regroups: Vec<u64>,
    regroup_completions: Vec<u64>,
    regroup_fallbacks: Vec<u64>,
    regroup_compositions: Vec<u64>,
    settle_ae_applied: Vec<u64>,
    settle_ae_digests: Vec<u64>,
    settle_ae_tree_nodes: Vec<u64>,
    settle_ae_sync_errors: Vec<u64>,
    settle_ae_reconcile_skips: Vec<u64>,
    load_writes: u64,
}

const COUNTERS: [&str; 12] = [
    "vals_ae_seed_starts",
    "vals_ae_seed_placement_invalidations",
    "vals_ae_seed_placement_regroups",
    "vals_ae_regroup_completions",
    "vals_ae_regroup_fallbacks",
    "vals_ae_regroup_compositions",
    "vals_sweep_passes",
    "vals_ae_entries_applied",
    "vals_ae_digests_examined",
    "vals_ae_tree_nodes_examined",
    "vals_ae_sync_errors",
    "vals_ae_seed_reconcile_skips",
];

type Row = [u64; COUNTERS.len()];

struct StopLoad {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for StopLoad {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn rehome_under_load(tag: &str, span_regroup: bool) -> Deltas {
    let dir = std::env::temp_dir().join(format!("valsd-regroup-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("secret"), "regroup-test-secret\n").unwrap();
    let mut cluster = Cluster { dir: dir.clone(), children: Vec::new() };

    let ports: Vec<TcpListener> = (0..7).map(|_| TcpListener::bind("127.0.0.1:0").unwrap()).collect();
    let addrs: Vec<SocketAddr> = ports[..3].iter().map(|p| p.local_addr().unwrap()).collect();
    let metrics: Vec<SocketAddr> = ports[3..6].iter().map(|p| p.local_addr().unwrap()).collect();
    let pd_metrics = ports[6].local_addr().unwrap();
    let admin_sock = dir.join("pd-admin.sock");
    for id in 1..=3u32 {
        let peers: String = (1..=3u32)
            .filter(|&p| p != id)
            .map(|p| format!("peer = {p} @ {}\n", addrs[p as usize - 1]))
            .collect();
        std::fs::write(
            dir.join(format!("node{id}.conf")),
            format!(
                "node_id = {id}\nlisten = {}\ndata_dir = node{id}\n{peers}metrics = {}\n\
                 rf = 2\nsweep = 200ms\nsecret_file = secret\nrehome_copy_hold_ms = 1000\n\
                 ae_span_regroup = {}\n",
                addrs[id as usize - 1],
                metrics[id as usize - 1],
                u8::from(span_regroup),
            ),
        )
        .unwrap();
    }
    let data_nodes: String = (1..=3u32)
        .map(|id| format!("data_node = {id} @ {}\n", addrs[id as usize - 1]))
        .collect();
    std::fs::write(
        dir.join("pd.conf"),
        format!(
            "pd_id = 1\ndata_dir = pd\n{data_nodes}rf = 2\nvnodes = 64\n\
             zone = events 2 sorted\nadmin_socket = pd-admin.sock\nsecret_file = secret\n\
             metrics = {pd_metrics}\nopaque_range_maps = yes\nrange_split_threshold = 0\n\
             range_split_poll_ms = 200\nrehome_enabled = yes\n"
        ),
    )
    .unwrap();
    drop(ports);
    for id in 1..=3 {
        cluster.spawn("node", &dir.join(format!("node{id}.conf")));
    }
    cluster.spawn("pd", &dir.join("pd.conf"));
    for &a in &addrs {
        wait_listening(a);
    }
    eventually("admin socket up", Duration::from_secs(15), || UnixStream::connect(&admin_sock).ok());

    let peers: Vec<(u32, SocketAddr)> = (1..=3u32).map(|id| (id, addrs[id as usize - 1])).collect();
    let mut ring = Ring::new(2);
    ring.set_members_weighted([(1, 1), (2, 1), (3, 1)]);
    let coord = Coordinator::new(100, ring.clone(), 2, NetTransport::connect(&peers).with_rpc_deadline(Duration::from_secs(2)));
    eventually("PD membership adopted", Duration::from_secs(15), || {
        (run(coord.refresh_ring(1)).ok()? == vec![1, 2, 3]).then_some(())
    });
    let events = eventually("events zone adopted", Duration::from_secs(15), || {
        run(coord.refresh_catalog(1)).ok()?;
        coord.catalog().by_name("events")
    });
    run(async {
        for i in 0..KEYS {
            let mut attempt = 0;
            while let Err(e) = coord.write_in(events, &key(i), format!("v{i}").as_bytes(), WriteConcern::Majority).await {
                attempt += 1;
                assert!(attempt < 50, "initial load write {i}: {e:?}");
                let _ = coord.refresh_ring(1).await;
                let _ = coord.refresh_catalog(1).await;
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    });

    let snapshot = || -> Option<Vec<Row>> {
        metrics.iter().map(|&m| {
            let mut row = [0u64; COUNTERS.len()];
            for (slot, name) in row.iter_mut().zip(COUNTERS) {
                *slot = scrape(m, name)?;
            }
            Some(row)
        }).collect()
    };
    let placement = |rows: &[Row]| -> Vec<[u64; 6]> {
        rows.iter().map(|row| [row[0], row[1], row[2], row[3], row[4], row[5]]).collect()
    };
    let passes_since = |rows: &[Row], from: &[u64]| rows.iter().zip(from).all(|(row, from)| row[6] >= from + 2);
    let completed = scrape(pd_metrics, "vals_pd_rehomes_completed").unwrap_or(0);
    let split = admin(&admin_sock, &format!("split-zone events {}", String::from_utf8(key(KEYS / 4)).unwrap()));
    assert!(split.starts_with("ok"), "{split}");
    eventually("the PD re-homes the first split's debt range", Duration::from_secs(90), || {
        (scrape(pd_metrics, "vals_pd_rehomes_completed")? > completed).then_some(())
    });
    let mut previous = None;
    let baseline = eventually("seeds settle on every node", Duration::from_secs(30), || {
        let idle = metrics.iter().all(|&m| scrape(m, "vals_ae_seed_active") == Some(0));
        let now = snapshot()?;
        let stable = idle && previous.as_ref().is_some_and(|(at, rows): &(Instant, Vec<[u64; 6]>)| {
            *rows == placement(&now) && at.elapsed() >= Duration::from_secs(2)
        });
        if previous.as_ref().is_none_or(|(_, rows)| *rows != placement(&now)) {
            previous = Some((Instant::now(), placement(&now)));
        }
        stable.then_some(now)
    });

    let stop = Arc::new(AtomicBool::new(false));
    let written = Arc::new(AtomicU64::new(0));
    let mut load = StopLoad { stop: stop.clone(), thread: None };
    load.thread = Some({
        let (stop, written, peers, ring) = (stop.clone(), written.clone(), peers.clone(), ring.clone());
        std::thread::spawn(move || {
            let coord = Coordinator::new(101, ring, 2, NetTransport::connect(&peers).with_rpc_deadline(Duration::from_secs(2)));
            let _ = run(coord.refresh_catalog(1));
            let mut i = 0u32;
            while !stop.load(Ordering::Relaxed) {
                let k = key((i * 7919) % (KEYS + 500));
                let ok = run(coord.write_in(events, &k, format!("w{i}").as_bytes(), WriteConcern::Majority)).is_ok();
                if ok {
                    written.fetch_add(1, Ordering::Relaxed);
                } else {
                    let _ = run(coord.refresh_catalog(1));
                    std::thread::sleep(Duration::from_millis(20));
                }
                i = i.wrapping_add(1);
            }
        })
    });
    eventually("write load running", Duration::from_secs(10), || (written.load(Ordering::Relaxed) > 50).then_some(()));

    let completed_before = scrape(pd_metrics, "vals_pd_rehomes_completed").unwrap_or(0);
    let split = admin(&admin_sock, &format!("split-zone events {}", String::from_utf8(key(KEYS / 2)).unwrap()));
    assert!(split.starts_with("ok"), "{split}");
    eventually("the PD re-homes the split's debt range", Duration::from_secs(90), || {
        (scrape(pd_metrics, "vals_pd_rehomes_completed")? > completed_before).then_some(())
    });
    let mut adopted: Vec<Option<u64>> = vec![None; 3];
    let after = eventually("every node prepares the commit epoch and drains its regroup", Duration::from_secs(60), || {
        let now = snapshot()?;
        for (i, node) in (1..=3u32).enumerate() {
            if adopted[i].is_none() && run(coord.refresh_range_maps(node)).is_ok() {
                let map = coord.range_map(events);
                if map.splits().contains(&key(KEYS / 2)) && map.active_rehomes() == 0 && map.unrehomed_ranges() == 0 {
                    adopted[i] = Some(now[i][6]);
                }
            }
        }
        let prepared = adopted.iter().zip(&now).all(|(at, row)| at.is_some_and(|at| row[6] >= at + 2));
        let drained = now.iter().zip(&baseline).all(|(a, b)| {
            let d = |c: usize| a[c] - b[c];
            d(2) - d(5) == d(3) + d(4)
        });
        (prepared && drained).then_some(now)
    });
    drop(load);
    let load_writes = written.load(Ordering::Relaxed);
    let settled = eventually("load stopped and a sweep pass ran on every node", Duration::from_secs(30), || {
        let now = snapshot()?;
        let from: Vec<u64> = after.iter().map(|row| row[6]).collect();
        passes_since(&now, &from).then_some(now)
    });
    let converged = eventually("AE runs several passes after the rehome settles", Duration::from_secs(60), || {
        let now = snapshot()?;
        let from: Vec<u64> = settled.iter().map(|row| row[6] + 4).collect();
        passes_since(&now, &from).then_some(now)
    });
    assert_eq!(scrape(pd_metrics, "vals_pd_rehomes_aborted"), Some(0));
    run(async {
        coord.refresh_catalog(1).await.unwrap();
        for i in (0..KEYS).step_by(97) {
            let got = coord.read_in(events, &key(i)).await.unwrap();
            assert!(got.is_some(), "key {i} survives the rehome");
        }
    });
    drop(cluster);
    let delta = |to: &[Row], from: &[Row], column: usize| -> Vec<u64> {
        to.iter().zip(from).map(|(a, b)| a[column] - b[column]).collect()
    };
    Deltas {
        seed_starts: delta(&after, &baseline, 0),
        invalidations: delta(&after, &baseline, 1),
        regroups: delta(&after, &baseline, 2),
        regroup_completions: delta(&after, &baseline, 3),
        regroup_fallbacks: delta(&after, &baseline, 4),
        regroup_compositions: delta(&after, &baseline, 5),
        settle_ae_applied: delta(&converged, &settled, 7),
        settle_ae_digests: delta(&converged, &settled, 8),
        settle_ae_tree_nodes: delta(&converged, &settled, 9),
        settle_ae_sync_errors: delta(&converged, &settled, 10),
        settle_ae_reconcile_skips: delta(&converged, &settled, 11),
        load_writes,
    }
}

#[test]
fn single_range_rehome_under_load_starts_no_peer_seed() {
    let d = rehome_under_load("on", true);
    assert!(d.load_writes > 100, "{d:?}");
    assert!(d.regroups.iter().sum::<u64>() >= 2, "the rehome changed a replica set over keys some stores hold: {d:?}");
    let chains: Vec<u64> = d.regroups.iter().zip(&d.regroup_compositions).map(|(r, c)| r - c).collect();
    assert_eq!(d.regroup_completions, chains, "{d:?}");
    assert_eq!(d.regroup_fallbacks, vec![0; 3], "{d:?}");
    assert_eq!(d.invalidations, vec![0; 3], "{d:?}");
    assert_eq!(d.seed_starts, vec![0; 3], "no ae_seed start on any healthy node across the rehome: {d:?}");
    assert!(d.settle_ae_tree_nodes.iter().all(|&n| n > 0), "AE compared trees after the rehome: {d:?}");
    assert_eq!(d.settle_ae_sync_errors, vec![0; 3], "every reconcile in the settle window ran: {d:?}");
    assert_eq!(d.settle_ae_reconcile_skips, vec![0; 3], "no reconcile was skipped in the settle window: {d:?}");
    assert_eq!(d.settle_ae_applied, vec![0; 3], "AE repairs nothing once the rehome settles: {d:?}");
    assert_eq!(d.settle_ae_digests, vec![0; 3], "the regrouped trees agree, so AE descends to no digest: {d:?}");
}

#[test]
fn knockout_whole_engine_invalidation_reseeds_peers_on_a_rehome() {
    let d = rehome_under_load("off", false);
    assert!(d.seed_starts.iter().sum::<u64>() >= 2, "the pre-0377 path reseeds owner stores: {d:?}");
    assert_eq!(d.regroups, vec![0; 3], "{d:?}");
}

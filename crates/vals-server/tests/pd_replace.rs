use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use vals_cluster::reactor;
use vals_cluster::{Coordinator, NetTransport, Ring};

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
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(config.with_extension("log"))
        .unwrap();
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

fn kill(mut child: Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn admin(sock: &Path, cmd: &str) -> String {
    let Ok(mut conn) = UnixStream::connect(sock) else {
        return "unreachable".into();
    };
    conn.write_all(cmd.as_bytes()).unwrap();
    conn.write_all(b"\n").unwrap();
    conn.shutdown(std::net::Shutdown::Write).unwrap();
    let mut reply = String::new();
    let _ = conn.read_to_string(&mut reply);
    reply.trim_end().to_string()
}

fn eventually<T>(what: &str, mut check: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(v) = check() {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out: {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn field<'a>(status: &'a str, name: &str) -> &'a str {
    let key = format!("{name}=");
    let at = status.find(&key).unwrap_or_else(|| panic!("{name} missing in {status:?}")) + key.len();
    status[at..].split_whitespace().next().unwrap_or_default()
}

struct Drill {
    dir: PathBuf,
    data_addrs: Vec<SocketAddr>,
    raft_addrs: Vec<SocketAddr>,
    nodes: Vec<Child>,
    pds: Vec<Option<Child>>,
}

impl Drill {
    fn start(name: &str) -> Drill {
        let dir = std::env::temp_dir().join(format!("valsd-pd-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("secret"), "pd-replace-test-secret\n").unwrap();
        let data_addrs: Vec<SocketAddr> = (0..3).map(|_| free_addr()).collect();
        let raft_addrs: Vec<SocketAddr> = (0..4).map(|_| free_addr()).collect();
        for id in 1..=3usize {
            let peers: String = (1..=3usize)
                .filter(|&p| p != id)
                .map(|p| format!("peer = {p} @ {}\n", data_addrs[p - 1]))
                .collect();
            std::fs::write(
                dir.join(format!("node{id}.conf")),
                format!(
                    "node_id = {id}\nlisten = {}\ndata_dir = node{id}\n{peers}rf = 3\nsweep = 3600s\n\
                     secret_file = secret\n",
                    data_addrs[id - 1]
                ),
            )
            .unwrap();
        }
        let data_nodes: String =
            (1..=3usize).map(|id| format!("data_node = {id} @ {}\n", data_addrs[id - 1])).collect();
        for id in 1..=4usize {
            let members: &[usize] = if id == 4 { &[1, 2, 4] } else { &[1, 2, 3] };
            let peers: String = members
                .iter()
                .map(|&p| format!("pd_peer = {p} @ {}\n", raft_addrs[p - 1]))
                .collect();
            let role = match id {
                1 => "campaign = true\n",
                4 => "join = true\n",
                _ => "",
            };
            std::fs::write(
                dir.join(format!("pd{id}.conf")),
                format!(
                    "pd_id = {id}\ndata_dir = pd{id}\nlisten = {}\n{peers}{role}{data_nodes}rf = 3\n\
                     vnodes = 64\nzone = events 2 sorted\nadmin_socket = pd{id}.sock\n\
                     secret_file = secret\n",
                    raft_addrs[id - 1]
                ),
            )
            .unwrap();
        }
        let nodes = (1..=3).map(|id| spawn("node", &dir.join(format!("node{id}.conf")))).collect();
        for &addr in &data_addrs {
            wait_listening(addr);
        }
        let mut drill = Drill { dir, data_addrs, raft_addrs, nodes, pds: (0..4).map(|_| None).collect() };
        for id in 1..=3 {
            drill.start_pd(id);
        }
        drill
    }

    fn sock(&self, id: usize) -> PathBuf {
        self.dir.join(format!("pd{id}.sock"))
    }

    fn start_pd(&mut self, id: usize) {
        let _ = std::fs::remove_file(self.sock(id));
        self.pds[id - 1] = Some(spawn("pd", &self.dir.join(format!("pd{id}.conf"))));
        let sock = self.sock(id);
        eventually(&format!("pd {id} admin socket"), || UnixStream::connect(&sock).ok().map(|_| ()));
    }

    fn admin(&self, id: usize, cmd: &str) -> String {
        admin(&self.sock(id), cmd)
    }

    fn status(&self, id: usize) -> String {
        self.admin(id, "status")
    }

    fn leader_among(&self, ids: &[usize]) -> usize {
        eventually(&format!("a caught-up leader among {ids:?}"), || {
            ids.iter().copied().find(|&id| {
                let status = self.status(id);
                status.contains("role=leader") && status.contains("caught_up=true")
            })
        })
    }

    fn wait_voters(&self, ids: &[usize], voters: &str, learners: &str) {
        for &id in ids {
            eventually(&format!("pd {id} sees voters={voters} learners={learners}"), || {
                let status = self.status(id);
                (status.starts_with("ok")
                    && field(&status, "voters") == voters
                    && field(&status, "learners") == learners)
                    .then_some(())
            });
        }
    }

    fn log(&self, id: usize) -> String {
        std::fs::read_to_string(self.dir.join(format!("pd{id}.log"))).unwrap_or_default()
    }

    fn coordinator(&self) -> Coordinator<NetTransport> {
        let peers: Vec<(u32, SocketAddr)> =
            (1..=3u32).map(|id| (id, self.data_addrs[id as usize - 1])).collect();
        let mut ring = Ring::new(3);
        ring.set_members_weighted([(1, 1), (2, 1), (3, 1)]);
        let transport = NetTransport::connect(&peers).with_rpc_deadline(Duration::from_secs(2));
        Coordinator::new(100, ring, 2, transport)
    }
}

impl Drop for Drill {
    fn drop(&mut self) {
        for node in self.nodes.drain(..) {
            stop(node);
        }
        for pd in self.pds.iter_mut().filter_map(Option::take) {
            stop(pd);
        }
        if !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

fn ok(reply: String, what: &str) -> String {
    assert!(reply.starts_with("ok"), "{what}: {reply}");
    reply
}

fn refused(reply: String, needle: &str) {
    assert!(reply.starts_with("err:") && reply.contains(needle), "expected a refusal with {needle:?}: {reply}");
}

#[test]
fn lost_pd_voter_is_replaced_under_the_surviving_quorum() {
    let mut drill = Drill::start("replace");
    assert_eq!(drill.leader_among(&[1]), 1);
    drill.wait_voters(&[1, 2, 3], "1,2,3", "-");
    let incarnation = eventually("incarnation minted", || {
        let status = drill.status(1);
        let id = field(&status, "incarnation").to_string();
        (id != "-").then_some(id)
    });
    ok(drill.admin(1, "create-zone before 2 unsorted"), "create-zone before");

    kill(drill.pds[2].take().unwrap());
    let stale = drill.dir.join("pd3-stale");
    std::fs::rename(drill.dir.join("pd3"), &stale).unwrap();

    refused(drill.admin(1, "pd-replace 3 4"), "not a learner");
    refused(drill.admin(1, &format!("pd-add-learner 3 {}", drill.raft_addrs[2])), "already a voter");
    let follower = drill.admin(2, &format!("pd-add-learner 4 {}", drill.raft_addrs[3]));
    assert!(follower.contains("not leader"), "{follower}");

    drill.start_pd(4);
    let added = ok(drill.admin(1, &format!("pd-add-learner 4 {}", drill.raft_addrs[3])), "add learner");
    assert!(added.contains("committed") && added.contains("learners=4"), "{added}");
    let again = ok(drill.admin(1, &format!("pd-add-learner 4 {}", drill.raft_addrs[3])), "re-add learner");
    assert!(again.contains("unchanged"), "{again}");
    drill.wait_voters(&[1, 2, 4], "1,2,3", "4");

    let replaced = eventually("learner caught up and replaced pd 3", || {
        let reply = drill.admin(1, "pd-replace 3 4");
        reply.starts_with("ok").then_some(reply)
    });
    assert!(replaced.contains("voters=1,2,4") && replaced.contains("learners=-"), "{replaced}");
    drill.wait_voters(&[1, 2, 4], "1,2,4", "-");
    let rerun = ok(drill.admin(1, "pd-replace 3 4"), "rerun pd-replace");
    assert!(rerun.contains("already done"), "{rerun}");
    refused(drill.admin(1, &format!("pd-add-learner 3 {}", drill.raft_addrs[2])), "never reused");
    for id in [1, 2, 4] {
        assert_eq!(field(&drill.status(id), "incarnation"), incarnation, "pd {id} keeps the group's incarnation");
    }
    let zones = drill.admin(4, "zones");
    assert!(zones.contains(" before 2 unsorted"), "the replacement holds the catalog: {zones}");

    std::fs::rename(&stale, drill.dir.join("pd3")).unwrap();
    drill.start_pd(3);
    let term = field(&drill.status(1), "term").to_string();
    std::thread::sleep(Duration::from_secs(3));
    let status = drill.status(1);
    assert!(status.contains("role=leader") && field(&status, "term") == term, "the removed replica disrupted the group: {status}");
    assert!(!drill.status(3).contains("role=leader"));
    ok(drill.admin(1, "create-zone with-stale 2 unsorted"), "commit beside a stale removed replica");
    stop(drill.pds[2].take().unwrap());

    kill(drill.pds[0].take().unwrap());
    let leader = drill.leader_among(&[2, 4]);
    ok(drill.admin(leader, "create-zone after-loss 2 unsorted"), "commit after losing an original voter");
    let coord = drill.coordinator();
    eventually("data nodes adopt the zone pushed by the repaired group", || {
        run(coord.refresh_catalog(1)).ok()?;
        coord.catalog().by_name("after-loss").map(|_| ())
    });

    drill.start_pd(1);
    drill.wait_voters(&[1], "1,2,4", "-");
    eventually("restarted pd 1 caught up", || drill.admin(1, "zones").contains(" after-loss ").then_some(()));
    stop(drill.pds[3].take().unwrap());
    drill.start_pd(4);
    drill.wait_voters(&[4], "1,2,4", "-");
    eventually("restarted pd 4 caught up", || drill.admin(4, "zones").contains(" after-loss ").then_some(()));

    stop(drill.pds[3].take().unwrap());
    std::fs::remove_dir_all(drill.dir.join("pd4")).unwrap();
    let mut wiped = spawn("pd", &drill.dir.join("pd4.conf"));
    eventually("a wiped voter restarted empty stops", || wiped.try_wait().unwrap().map(|_| ()));
    assert!(drill.log(4).contains("cannot be recreated"), "{}", drill.log(4));
    let leader = drill.leader_among(&[1, 2]);
    ok(drill.admin(leader, "create-zone after-wipe 2 unsorted"), "commit with the wiped voter down");
}

#[test]
fn restarted_campaign_replica_rejoins_without_deposing_the_leader() {
    let mut drill = Drill::start("rejoin");
    assert_eq!(drill.leader_among(&[1]), 1);
    drill.wait_voters(&[1, 2, 3], "1,2,3", "-");
    stop(drill.pds[0].take().unwrap());
    let leader = drill.leader_among(&[2, 3]);
    for round in 0..2 {
        let zone = format!("while-away-{round}");
        ok(drill.admin(leader, &format!("create-zone {zone} 2 unsorted")), "commit while pd 1 is down");
        let term = field(&drill.status(leader), "term").to_string();
        drill.start_pd(1);
        eventually("restarted pd 1 caught up", || drill.admin(1, "zones").contains(&format!(" {zone} ")).then_some(()));
        std::thread::sleep(Duration::from_secs(3));
        for id in 1..=3 {
            let status = drill.status(id);
            assert_eq!(field(&status, "term"), term, "pd {id}: the restarted campaign replica forced an election: {status}");
            assert_eq!(field(&status, "leader"), leader.to_string(), "pd {id}: {status}");
        }
        stop(drill.pds[0].take().unwrap());
    }
}

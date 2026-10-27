use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use vals_cluster::{observe, LivenessLog, MemLivenessLog, Node, Ring, Version, VersionedStore};
use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine, Ordering};

static LINES: Mutex<Vec<String>> = Mutex::new(Vec::new());

struct Capture;

impl log::Log for Capture {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }
    fn log(&self, record: &log::Record) {
        LINES
            .lock()
            .unwrap()
            .push(format!("{} {}", record.target(), record.args()));
    }
    fn flush(&self) {}
}

fn captured(needle: &str) -> bool {
    LINES.lock().unwrap().iter().any(|l| l.contains(needle))
}

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

fn physical_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[test]
fn liveness_transitions_log_and_count() {
    static LOGGER: Capture = Capture;
    log::set_logger(&LOGGER).unwrap();
    log::set_max_level(log::LevelFilter::Info);

    let before = observe::snapshot();

    let ring = {
        let mut r = Ring::with_vnodes(2, 64);
        r.set_members_weighted([(1u32, 1u32), (2, 1)]);
        r
    };
    let now = physical_now_ms();
    let grace = 60_000u64;
    let recent = now - 1_000;
    let old = now - 600_000;

    let mut s1 = store(1);
    for i in 0..10u32 {
        s1.apply(
            format!("k{i}").as_bytes(),
            Some(b"v"),
            Version {
                hlc: recent << 16,
                node: 1,
            },
        )
        .unwrap();
    }
    let mut s2 = store(2);
    s2.apply(
        b"victim",
        Some(b"v1"),
        Version {
            hlc: old << 16,
            node: 2,
        },
    )
    .unwrap();

    let log1 = MemLivenessLog::new();
    log1.store(recent).unwrap();
    let log2 = MemLivenessLog::new();
    log2.store(old).unwrap();

    let (l1, a1) = Node::bind().unwrap();
    let (l2, a2) = Node::bind().unwrap();
    let peers: Vec<(u32, SocketAddr)> = vec![(1, a1), (2, a2)];
    let interval = Duration::from_millis(25);
    Node::serve_with_liveness(
        1,
        l1,
        s1,
        ring.clone(),
        peers.clone(),
        interval,
        grace,
        log1,
    );
    Node::serve_with_liveness(2, l2, s2, ring, peers, interval, grace, log2);

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let snap = observe::snapshot();
        if snap.wipes > before.wipes && snap.unfences >= before.unfences + 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "wipe/unfence counters did not move in time: {:?} -> {:?}",
            (before.wipes, before.unfences),
            (snap.wipes, snap.unfences)
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    assert!(captured("born fenced"), "liveness-on line missing");
    assert!(captured("liveness wipe"), "wipe warn line missing");
    assert!(captured("unfenced"), "unfence line missing");
    assert!(
        captured("1 keys across 1 zones"),
        "wipe line lacks pre-wipe stats: {:?}",
        LINES
            .lock()
            .unwrap()
            .iter()
            .filter(|l| l.contains("wipe"))
            .collect::<Vec<_>>()
    );
}

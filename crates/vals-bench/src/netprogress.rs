use super::*;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtOrd};
use std::sync::Mutex;

use vals_cluster::{
    reactor, ClientConfig, CoordError, Coordinator, CoordinatorService, FdConfig, NetTransport,
    Persistence,
    ReadConcern, Ring, WriteConcern, WriteOptions, ZoneId, DEFAULT_ZONE,
};

const OPS: [&str; 3] = ["write", "read", "multiread"];
const CLIENT_COUNTERS: [&str; 7] = [
    "detector_trips",
    "detector_recoveries",
    "hints_recorded",
    "hints_dropped",
    "credit_blocked",
    "stale_ring_refreshes",
    "idle_conn_retries",
];

#[derive(Clone)]
struct NpConfig {
    threads: usize,
    slots: usize,
    keys: usize,
    secs: f64,
    sample_secs: f64,
    read_pct: u64,
    multi_pct: u64,
    multi: usize,
    rate: f64,
    vsize: usize,
    fd: Option<FdConfig>,
    hedge: Duration,
    client: ClientConfig,
    persistence: Persistence,
    prefix: String,
    prefill: bool,
    existing: bool,
    verify_all: bool,
    seq_base: u64,
    busy_backoff: Duration,
}

impl NpConfig {
    fn from_env() -> Self {
        let ms = |name, default| Some(Duration::from_millis(env_u64(name, default).max(1)));
        let fd = match std::env::var("VALS_NP_FD").as_deref() {
            Err(_) | Ok("off") => None,
            Ok("default") => Some(FdConfig::default()),
            Ok(spec) => {
                let (threshold, cooldown) = spec
                    .split_once(',')
                    .expect("VALS_NP_FD is off, default, or THRESHOLD,COOLDOWN_MS");
                Some(FdConfig {
                    failure_threshold: threshold.trim().parse().expect("FD threshold"),
                    open_cooldown_ms: cooldown.trim().parse().expect("FD cooldown ms"),
                })
            }
        };
        let persistence = match std::env::var("VALS_NP_PERSISTENCE").as_deref() {
            Err(_) | Ok("buffered") => Persistence::Buffered,
            Ok("device") => Persistence::Device,
            Ok(other) => panic!("VALS_NP_PERSISTENCE must be buffered or device, got {other}"),
        };
        NpConfig {
            threads: (env_u64("VALS_NP_THREADS", 8) as usize).max(1),
            slots: (env_u64("VALS_NP_SLOTS", 8) as usize).max(1),
            keys: (env_u64("VALS_NP_KEYS", 256) as usize).max(1),
            secs: env_f64("VALS_NP_SECS", 60.0),
            sample_secs: env_f64("VALS_NP_SAMPLE_SECS", 1.0).max(0.1),
            read_pct: env_u64("VALS_NP_READ_PCT", 45).min(100),
            multi_pct: env_u64("VALS_NP_MULTI_PCT", 10).min(100),
            multi: (env_u64("VALS_NP_MULTI", 8) as usize).max(1),
            rate: env_f64("VALS_NP_RATE", 2000.0).max(0.0),
            vsize: (env_u64("VALS_NP_VSIZE", 256) as usize).max(20),
            fd,
            hedge: Duration::from_millis(env_u64("VALS_NP_HEDGE_MS", 0)),
            client: ClientConfig {
                connect_deadline: ms("VALS_NP_CONNECT_DEADLINE_MS", 5000),
                rpc_deadline: ms("VALS_NP_RPC_DEADLINE_MS", 5000),
                ..ClientConfig::default()
            },
            persistence,
            prefix: std::env::var("VALS_NP_PREFIX").unwrap_or_else(|_| "np/".into()),
            prefill: env_u64("VALS_NP_PREFILL", 0) != 0,
            existing: env_u64("VALS_NP_EXISTING", 0) != 0,
            verify_all: env_u64("VALS_NP_VERIFY_ALL", 0) != 0,
            busy_backoff: Duration::from_millis(env_u64("VALS_NP_BUSY_BACKOFF_MS", 0)),
            seq_base: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after 1970")
                .as_micros() as u64,
        }
    }

    fn json(&self) -> String {
        format!(
            "{{\"threads\":{},\"slots\":{},\"keys_per_slot\":{},\"secs\":{},\"read_pct\":{},\"multi_pct\":{},\"multi\":{},\"rate\":{},\"vsize\":{},\"fd\":{},\"hedge_ms\":{},\"connect_deadline_ms\":{},\"rpc_deadline_ms\":{},\"write\":\"majority\",\"read\":\"majority\",\"persistence\":\"{}\",\"prefix\":{},\"prefill\":{},\"existing\":{},\"seq_base\":{},\"busy_backoff_ms\":{}}}",
            self.threads,
            self.slots,
            self.keys,
            self.secs,
            self.read_pct,
            self.multi_pct,
            self.multi,
            self.rate,
            self.vsize,
            self.fd.map_or("null".into(), |fd| format!(
                "{{\"threshold\":{},\"cooldown_ms\":{}}}",
                fd.failure_threshold, fd.open_cooldown_ms
            )),
            self.hedge.as_millis(),
            self.client.connect_deadline.map_or(0, |d| d.as_millis()),
            self.client.rpc_deadline.map_or(0, |d| d.as_millis()),
            if self.persistence == Persistence::Device { "device" } else { "buffered" },
            json_str(&self.prefix),
            self.prefill,
            self.existing,
            self.seq_base,
            self.busy_backoff.as_millis(),
        )
    }
}

#[derive(Default)]
struct OpStats {
    ok: u64,
    busy_retries: u64,
    errs: BTreeMap<&'static str, u64>,
    lat_us: Vec<u32>,
}

impl OpStats {
    fn merge(&mut self, other: OpStats) {
        self.ok += other.ok;
        self.busy_retries += other.busy_retries;
        for (kind, n) in other.errs {
            *self.errs.entry(kind).or_default() += n;
        }
        self.lat_us.extend(other.lat_us);
    }

    fn json(&mut self) -> String {
        self.lat_us.sort_unstable();
        let pct = |p: usize| {
            if self.lat_us.is_empty() {
                0.0
            } else {
                self.lat_us[(self.lat_us.len() * p / 1000).min(self.lat_us.len() - 1)] as f64 / 1e3
            }
        };
        let errs: Vec<String> = self.errs.iter().map(|(k, n)| format!("\"{k}\":{n}")).collect();
        format!(
            "{{\"ok\":{},\"busy_retries\":{},\"err\":{{{}}},\"p50_ms\":{:.2},\"p95_ms\":{:.2},\"p99_ms\":{:.2},\"max_ms\":{:.2}}}",
            self.ok,
            self.busy_retries,
            errs.join(","),
            pct(500),
            pct(950),
            pct(990),
            self.lat_us.last().map_or(0.0, |&v| v as f64 / 1e3)
        )
    }
}

#[derive(Default)]
struct ThreadStats {
    ops: [OpStats; 3],
    bad: u64,
    bad_samples: Vec<String>,
    stuck_1s: usize,
    credit_ops: usize,
    credit_bytes: usize,
    credit_waiters: usize,
    queued_bytes: usize,
    pending: usize,
}

impl ThreadStats {
    fn take_ops(&mut self) -> [OpStats; 3] {
        std::mem::take(&mut self.ops)
    }

    fn record(&mut self, op: usize, start: Instant, retries: u64, result: Result<(), &'static str>) {
        let stats = &mut self.ops[op];
        stats.busy_retries += retries;
        stats.lat_us.push(start.elapsed().as_micros().min(u32::MAX as u128) as u32);
        match result {
            Ok(()) => stats.ok += 1,
            Err(kind) => *stats.errs.entry(kind).or_default() += 1,
        }
    }

    fn bad(&mut self, what: String) {
        self.bad += 1;
        if self.bad_samples.len() < 8 {
            eprintln!("netprogress: BAD {what}");
            self.bad_samples.push(what);
        }
    }
}

#[derive(Clone, Copy, Default)]
struct KeyState {
    acked: u64,
    attempted: u64,
}

fn err_kind(e: &CoordError) -> &'static str {
    match e {
        CoordError::QuorumFailed { .. } => "quorum_failed",
        CoordError::Overloaded => "overloaded",
        CoordError::Busy { .. } => "busy",
        CoordError::StaleRing { .. } => "stale_ring",
        CoordError::Unavailable => "unavailable",
        CoordError::SkewRejected { .. } => "skew_rejected",
        _ => "other",
    }
}

fn retryable(e: &CoordError) -> bool {
    matches!(e, CoordError::Busy { .. } | CoordError::Overloaded)
}

/// Waits before retrying a request refused with `Busy`/`Overloaded`, doubling
/// from `VALS_NP_BUSY_BACKOFF_MS` up to 32x; false when retries are off or the
/// run is stopping.
async fn busy_wait(cfg: &NpConfig, stop: &AtomicBool, attempt: &mut u64) -> bool {
    if cfg.busy_backoff.is_zero() || stop.load(AtOrd::Relaxed) {
        return false;
    }
    let delay = cfg.busy_backoff * (1u32 << (*attempt).min(5));
    *attempt += 1;
    reactor::Timer::every(delay).expect("timerfd").tick().await;
    true
}

fn np_key(cfg: &NpConfig, thread: usize, slot: usize, idx: usize) -> Vec<u8> {
    format!("{}t{thread:03}/s{slot:03}/k{idx:06}", cfg.prefix).into_bytes()
}

fn np_value(vsize: usize, seq: u64, thread: usize, slot: usize, idx: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(vsize);
    v.extend_from_slice(&seq.to_le_bytes());
    v.extend_from_slice(&(thread as u32).to_le_bytes());
    v.extend_from_slice(&(slot as u32).to_le_bytes());
    v.extend_from_slice(&(idx as u32).to_le_bytes());
    for i in v.len()..vsize {
        v.push((seq.wrapping_add(i as u64) & 0xFF) as u8);
    }
    v
}

fn check_read(
    cfg: &NpConfig,
    state: KeyState,
    got: Option<&[u8]>,
    thread: usize,
    slot: usize,
    idx: usize,
) -> Result<(), String> {
    let Some(value) = got else {
        return if state.acked == 0 {
            Ok(())
        } else {
            Err(format!("t{thread}/s{slot}/k{idx}: absent after acked seq {}", state.acked))
        };
    };
    let seq = value
        .get(..8)
        .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
        .unwrap_or(0);
    if value != np_value(cfg.vsize, seq, thread, slot, idx).as_slice() {
        return Err(format!("t{thread}/s{slot}/k{idx}: corrupt value (len {})", value.len()));
    }
    if seq < state.acked || seq > state.attempted || seq == 0 {
        return Err(format!(
            "t{thread}/s{slot}/k{idx}: read seq {seq}, acked {} attempted {}",
            state.acked, state.attempted
        ));
    }
    Ok(())
}

struct Xorshift(u64);

impl Xorshift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

type SlotFuture<'a> = Pin<Box<dyn Future<Output = Vec<KeyState>> + 'a>>;

fn np_coordinator(
    cfg: &NpConfig,
    nodes: &[(u32, Vec<SocketAddr>)],
    id: u32,
) -> Coordinator<NetTransport> {
    let mut ring = Ring::new(nodes.len().clamp(1, 3));
    ring.set_members_weighted(nodes.iter().map(|(id, _)| (*id, 1)));
    let transport = NetTransport::connect_sharded_with_config(nodes.iter().cloned(), cfg.client)
        .expect("valid client config");
    let mut coord = Coordinator::new(id, ring, ReadConcern::Majority, transport);
    if let Some(fd) = cfg.fd {
        coord = coord.with_failure_detector(fd);
    }
    coord = coord.with_digest_hedge(cfg.hedge);
    let adopted = poll_until(Duration::from_secs(30), Duration::from_millis(200), || {
        vals_rt::block_on(coord.adopt_from_any_member(), reactor::poll_io)
    });
    if !adopted {
        eprintln!("netprogress: could not adopt the ring/catalog from any peer");
        std::process::exit(2);
    }
    coord
}

fn np_zone(coord: &Coordinator<NetTransport>, name: &str) -> ZoneId {
    if name == "default" {
        return DEFAULT_ZONE;
    }
    match coord.catalog().by_name(name) {
        Some(id) => id,
        None => {
            eprintln!("netprogress: zone {name:?} not found in the catalog");
            std::process::exit(2);
        }
    }
}

async fn np_write(
    cfg: &NpConfig,
    coord: &Coordinator<NetTransport>,
    zone: ZoneId,
    stop: &AtomicBool,
    (thread, slot, i): (usize, usize, usize),
    seq: u64,
) -> (Result<(), &'static str>, u64) {
    let options = WriteOptions { concern: WriteConcern::Majority, persistence: cfg.persistence };
    let (key, value) = (np_key(cfg, thread, slot, i), np_value(cfg.vsize, seq, thread, slot, i));
    let mut retries = 0;
    loop {
        match coord.write_in(zone, &key, &value, options).await {
            Ok(_) => return (Ok(()), retries),
            Err(e) if retryable(&e) && busy_wait(cfg, stop, &mut retries).await => {}
            Err(e) => return (Err(err_kind(&e)), retries),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn np_slot<'a>(
    cfg: &'a NpConfig,
    coord: &'a Coordinator<NetTransport>,
    zone: ZoneId,
    thread: usize,
    slot: usize,
    stats: &'a Mutex<ThreadStats>,
    busy_since: &'a RefCell<Vec<Option<Instant>>>,
    stop: &'a AtomicBool,
    done: &'a AtomicUsize,
) -> SlotFuture<'a> {
    Box::pin(async move {
        let interval = (cfg.rate > 0.0)
            .then(|| Duration::from_secs_f64((cfg.threads * cfg.slots) as f64 / cfg.rate));
        let timer = match interval {
            Some(period) => {
                let slots = cfg.threads * cfg.slots;
                let offset = period.mul_f64((thread * cfg.slots + slot) as f64 / slots as f64);
                if !offset.is_zero() {
                    reactor::Timer::every(offset).expect("timerfd").tick().await;
                }
                Some(reactor::Timer::every(period).expect("timerfd"))
            }
            None => None,
        };
        let mut rng = Xorshift(0x9E37_79B9_7F4A_7C15 ^ (((thread as u64) << 32) | (slot as u64 + 1)));
        let initial = if cfg.existing {
            KeyState { acked: 1, attempted: cfg.seq_base }
        } else {
            KeyState::default()
        };
        let mut keys = vec![initial; cfg.keys];
        let mut seq = cfg.seq_base;
        if cfg.prefill {
            for i in 0..cfg.keys {
                if stop.load(AtOrd::Relaxed) {
                    break;
                }
                if let Some(timer) = &timer {
                    timer.tick().await;
                }
                let start = Instant::now();
                busy_since.borrow_mut()[slot] = Some(start);
                seq += 1;
                keys[i].attempted = seq;
                let (result, retries) = np_write(cfg, coord, zone, stop, (thread, slot, i), seq).await;
                if result.is_ok() {
                    keys[i].acked = seq;
                }
                stats.lock().unwrap().record(0, start, retries, result);
                busy_since.borrow_mut()[slot] = None;
            }
            done.fetch_add(1, AtOrd::Relaxed);
            return keys;
        }
        while !stop.load(AtOrd::Relaxed) {
            if let Some(timer) = &timer {
                timer.tick().await;
            }
            let roll = rng.next() % 100;
            let start = Instant::now();
            busy_since.borrow_mut()[slot] = Some(start);
            if roll < cfg.multi_pct {
                let idxs: Vec<usize> =
                    (0..cfg.multi).map(|_| rng.next() as usize % cfg.keys).collect();
                let ks: Vec<Vec<u8>> = idxs.iter().map(|&i| np_key(cfg, thread, slot, i)).collect();
                let mut retries = 0;
                let result = loop {
                    let r = coord.read_many_in(zone, &ks).await;
                    let busy = match &r {
                        Ok(results) => results.iter().any(|r| matches!(r, Err(e) if retryable(e))),
                        Err(e) => retryable(e),
                    };
                    if busy && busy_wait(cfg, stop, &mut retries).await {
                        continue;
                    }
                    break r;
                };
                let mut st = stats.lock().unwrap();
                match result {
                    Ok(results) => {
                        let mut first_err = None;
                        for (&i, r) in idxs.iter().zip(&results) {
                            match r {
                                Ok(got) => {
                                    let got = got.as_ref().and_then(|s| s.value.as_deref());
                                    if let Err(what) = check_read(cfg, keys[i], got, thread, slot, i) {
                                        st.bad(what);
                                    }
                                }
                                Err(e) => {
                                    first_err.get_or_insert(err_kind(e));
                                }
                            }
                        }
                        st.record(2, start, retries, first_err.map_or(Ok(()), Err));
                    }
                    Err(e) => st.record(2, start, retries, Err(err_kind(&e))),
                }
            } else if roll < cfg.multi_pct + cfg.read_pct {
                let i = rng.next() as usize % cfg.keys;
                let mut retries = 0;
                let result = loop {
                    let r = coord.read_in(zone, &np_key(cfg, thread, slot, i)).await;
                    if matches!(&r, Err(e) if retryable(e)) && busy_wait(cfg, stop, &mut retries).await {
                        continue;
                    }
                    break r;
                };
                let mut st = stats.lock().unwrap();
                match result {
                    Ok(got) => {
                        let got = got.as_ref().and_then(|s| s.value.as_deref());
                        if let Err(what) = check_read(cfg, keys[i], got, thread, slot, i) {
                            st.bad(what);
                        }
                        st.record(1, start, retries, Ok(()));
                    }
                    Err(e) => st.record(1, start, retries, Err(err_kind(&e))),
                }
            } else {
                let i = rng.next() as usize % cfg.keys;
                seq += 1;
                keys[i].attempted = seq;
                let (result, retries) = np_write(cfg, coord, zone, stop, (thread, slot, i), seq).await;
                if result.is_ok() {
                    keys[i].acked = seq;
                }
                stats.lock().unwrap().record(0, start, retries, result);
            }
            busy_since.borrow_mut()[slot] = None;
        }
        keys
    })
}

fn np_publisher<'a>(
    cfg: &'a NpConfig,
    coord: &'a Coordinator<NetTransport>,
    stats: &'a Mutex<ThreadStats>,
    busy_since: &'a RefCell<Vec<Option<Instant>>>,
    stop: &'a AtomicBool,
) -> SlotFuture<'a> {
    Box::pin(async move {
        let timer = reactor::Timer::every(Duration::from_secs_f64(cfg.sample_secs / 4.0))
            .expect("timerfd");
        while !stop.load(AtOrd::Relaxed) {
            timer.tick().await;
            let now = Instant::now();
            let stuck = busy_since
                .borrow()
                .iter()
                .flatten()
                .filter(|&&since| now.duration_since(since) >= Duration::from_secs(1))
                .count();
            let credit = coord.transport().transport_credit_stats();
            let conns = coord.transport().connection_stats();
            let mut st = stats.lock().unwrap();
            st.stuck_1s = stuck;
            if let Some(credit) = credit {
                st.credit_ops = credit.in_flight_ops;
                st.credit_bytes = credit.in_flight_bytes;
                st.credit_waiters = credit.waiters;
            }
            st.queued_bytes = conns.iter().map(|c| c.queued_bytes).sum();
            st.pending = conns.iter().map(|c| c.pending).sum();
        }
        Vec::new()
    })
}

fn rss_mb() -> f64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1).and_then(|p| p.parse::<f64>().ok()))
        .map_or(0.0, |pages| pages * 4096.0 / 1e6)
}

fn cpu_secs() -> f64 {
    // SAFETY: rusage holds only integers, so all-zero is a valid value, and
    // getrusage only writes the struct we pass it.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return 0.0;
    }
    let secs = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    secs(usage.ru_utime) + secs(usage.ru_stime)
}

fn client_counters() -> BTreeMap<&'static str, u64> {
    vals_cluster::observe::snapshot_named()
        .into_iter()
        .filter(|(name, _, _)| CLIENT_COUNTERS.contains(name))
        .map(|(name, _, value)| (name, value))
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn np_thread(
    cfg: &NpConfig,
    nodes: &[(u32, Vec<SocketAddr>)],
    zone_name: &str,
    thread: usize,
    stats: &Mutex<ThreadStats>,
    started: &std::sync::Barrier,
    stop: &AtomicBool,
    done: &AtomicUsize,
) -> (u64, u64, u64) {
    let svc = CoordinatorService::new(np_coordinator(cfg, nodes, 4_000_000 + thread as u32));
    let coord = svc.coordinator().clone();
    let zone = np_zone(&coord, zone_name);
    let busy_since = RefCell::new(vec![None; cfg.slots]);
    started.wait();
    let mut futs: Vec<SlotFuture<'_>> = (0..cfg.slots)
        .map(|slot| np_slot(cfg, &coord, zone, thread, slot, stats, &busy_since, stop, done))
        .collect();
    futs.push(np_publisher(cfg, &coord, stats, &busy_since, stop));
    let mut models = svc.run(vals_rt::join_all(futs));
    models.pop();

    let (mut checked, mut bad, mut errors) = (0u64, 0u64, 0u64);
    for (slot, keys) in models.iter().enumerate() {
        let idxs: Vec<usize> = (0..keys.len())
            .filter(|&i| keys[i].attempted > cfg.seq_base || (cfg.verify_all && keys[i].attempted > 0))
            .collect();
        for chunk in idxs.chunks(64) {
            let ks: Vec<Vec<u8>> = chunk.iter().map(|&i| np_key(cfg, thread, slot, i)).collect();
            let results = match svc.run(coord.read_many_in(zone, &ks)) {
                Ok(results) => results,
                Err(e) => {
                    eprintln!("netprogress: verify read failed: {e}");
                    errors += chunk.len() as u64;
                    continue;
                }
            };
            for (&i, result) in chunk.iter().zip(results) {
                checked += 1;
                match result {
                    Ok(got) => {
                        let got = got.as_ref().and_then(|s| s.value.as_deref());
                        if let Err(what) = check_read(cfg, keys[i], got, thread, slot, i) {
                            bad += 1;
                            stats.lock().unwrap().bad(format!("verify {what}"));
                        }
                    }
                    Err(e) => {
                        errors += 1;
                        eprintln!("netprogress: verify read of t{thread}/s{slot}/k{i} failed: {e}");
                    }
                }
            }
        }
    }
    (checked, bad, errors)
}

/// Point writes, reads and batch reads at majority concern from many
/// production-bounded clients, sampled every `VALS_NP_SAMPLE_SECS`, then a
/// read-back of every key against the acknowledged history. Used to watch
/// client progress through replica faults (task 0513) and by
/// `scripts/file-bench.py` (task 0510): `VALS_NP_PREFILL=1` writes every key
/// once and stops, and `VALS_NP_EXISTING=1` runs over keys an earlier run from
/// the same client host wrote with the same prefix, value size and keys per
/// slot, verifying only this run's writes unless `VALS_NP_VERIFY_ALL=1`.
pub(super) fn workload_netprogress(spec: Option<&str>, zone: Option<&str>) {
    let (Some(spec), Some(zone_name)) = (spec, zone) else {
        eprintln!("usage: vals-bench vals netprogress <id@ip:port,...|id@a1,a2;id@...> <zone|default>");
        std::process::exit(2);
    };
    let cfg = NpConfig::from_env();
    let nodes = parse_peers(spec);
    println!("NP_CONFIG {}", cfg.json());
    let stop = AtomicBool::new(false);
    let started = std::sync::Barrier::new(cfg.threads + 1);
    let stats: Vec<Mutex<ThreadStats>> = (0..cfg.threads).map(|_| Mutex::default()).collect();
    let mut totals: [OpStats; 3] = Default::default();
    let mut bad_total = 0u64;
    let done = AtomicUsize::new(0);
    let (mut window_secs, mut client_cpu_secs) = (0.0, 0.0);
    let (checked, bad_verify, verify_errors) = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..cfg.threads)
            .map(|thread| {
                let (cfg, nodes, stats, started, stop, done) =
                    (&cfg, &nodes, &stats[thread], &started, &stop, &done);
                scope.spawn(move || np_thread(cfg, nodes, zone_name, thread, stats, started, stop, done))
            })
            .collect();
        started.wait();
        let t0 = Instant::now();
        let cpu0 = cpu_secs();
        let mut counters0 = client_counters();
        let mut next = cfg.sample_secs;
        loop {
            let elapsed = t0.elapsed().as_secs_f64();
            let all_done = done.load(AtOrd::Relaxed) >= cfg.threads * cfg.slots;
            if elapsed < next && !all_done {
                std::thread::sleep(Duration::from_millis(10));
                continue;
            }
            let mut ops: [OpStats; 3] = Default::default();
            let (mut stuck, mut credit, mut queued, mut pending, mut bad) = (0, [0usize; 3], 0, 0, 0);
            for st in &stats {
                let mut st = st.lock().unwrap();
                for (merged, op) in ops.iter_mut().zip(st.take_ops()) {
                    merged.merge(op);
                }
                stuck += st.stuck_1s;
                credit[0] += st.credit_ops;
                credit[1] += st.credit_bytes;
                credit[2] += st.credit_waiters;
                queued += st.queued_bytes;
                pending += st.pending;
                bad += st.bad;
            }
            let counters = client_counters();
            let deltas: Vec<String> = counters
                .iter()
                .filter_map(|(name, v)| {
                    let d = v - counters0.get(name).copied().unwrap_or(0);
                    (d > 0).then(|| format!("\"{name}\":{d}"))
                })
                .collect();
            counters0 = counters;
            let per_op: Vec<String> = OPS
                .iter()
                .zip(ops.iter_mut())
                .map(|(name, op)| format!("\"{name}\":{}", op.json()))
                .collect();
            println!(
                "NP_SAMPLE {{\"elapsed\":{elapsed:.2},\"ops\":{{{}}},\"stuck_1s\":{stuck},\"bad\":{bad},\"transport_credit\":{{\"ops\":{},\"bytes\":{},\"waiters\":{}}},\"queued_bytes\":{queued},\"pending\":{pending},\"rss_mb\":{:.1},\"counters\":{{{}}}}}",
                per_op.join(","),
                credit[0],
                credit[1],
                credit[2],
                rss_mb(),
                deltas.join(",")
            );
            for (total, op) in totals.iter_mut().zip(ops) {
                total.merge(op);
            }
            bad_total = bad;
            next += cfg.sample_secs;
            if elapsed >= cfg.secs || all_done {
                window_secs = elapsed;
                break;
            }
        }
        client_cpu_secs = cpu_secs() - cpu0;
        println!("NP_WINDOW_END {{\"elapsed\":{window_secs:.2}}}");
        stop.store(true, AtOrd::Relaxed);
        handles
            .into_iter()
            .map(|h| h.join().expect("netprogress thread"))
            .fold((0, 0, 0), |a, b| (a.0 + b.0, a.1 + b.1, a.2 + b.2))
    });
    let per_op: Vec<String> = OPS
        .iter()
        .zip(totals.iter_mut())
        .map(|(name, op)| format!("\"{name}\":{}", op.json()))
        .collect();
    println!(
        "NP_SUMMARY {{\"elapsed\":{window_secs:.2},\"ops\":{{{}}},\"bad_during_run\":{bad_total},\"client_cpu_s\":{client_cpu_secs:.2},\"client_rss_mb\":{:.1}}}",
        per_op.join(","),
        rss_mb()
    );
    let ok = bad_total == 0 && bad_verify == 0 && verify_errors == 0;
    println!(
        "NP_VERIFY {{\"ok\":{ok},\"checked\":{checked},\"bad\":{bad_verify},\"errors\":{verify_errors}}}"
    );
    if !ok {
        std::process::exit(1);
    }
}

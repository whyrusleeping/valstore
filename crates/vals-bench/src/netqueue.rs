use super::*;

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering as AtOrd};
use std::sync::{Arc, Mutex};

const HIST_SUB_BITS: u32 = 6;
const HIST_SUB: usize = 1 << HIST_SUB_BITS;
const HIST_BUCKETS: usize = (64 - HIST_SUB_BITS as usize + 1) * HIST_SUB;

#[derive(Clone)]
struct Hist {
    buckets: Vec<u64>,
    count: u64,
    sum: u128,
    max: u64,
}

impl Hist {
    fn new() -> Self {
        Hist { buckets: vec![0; HIST_BUCKETS], count: 0, sum: 0, max: 0 }
    }

    fn bucket(v: u64) -> usize {
        if v < HIST_SUB as u64 {
            v as usize
        } else {
            let shift = 63 - v.leading_zeros() - HIST_SUB_BITS;
            (shift as usize + 1) * HIST_SUB + ((v >> shift) as usize - HIST_SUB)
        }
    }

    fn bucket_high(i: usize) -> u64 {
        if i < HIST_SUB {
            return i as u64;
        }
        let shift = (i / HIST_SUB - 1) as u32;
        let sub = (i % HIST_SUB + HIST_SUB) as u128;
        (((sub + 1) << shift) - 1).min(u64::MAX as u128) as u64
    }

    fn record(&mut self, v: u64) {
        self.buckets[Self::bucket(v)] += 1;
        self.count += 1;
        self.sum += v as u128;
        self.max = self.max.max(v);
    }

    fn merge(&mut self, o: &Hist) {
        if o.count == 0 {
            return;
        }
        for (a, b) in self.buckets.iter_mut().zip(&o.buckets) {
            *a += *b;
        }
        self.count += o.count;
        self.sum += o.sum;
        self.max = self.max.max(o.max);
    }

    fn reset(&mut self) {
        if self.count != 0 {
            self.buckets.fill(0);
            self.count = 0;
            self.sum = 0;
            self.max = 0;
        }
    }

    fn quantile_permille(&self, q: u64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let rank = ((q as u128 * self.count as u128).div_ceil(1000) as u64).max(1);
        let mut seen = 0u64;
        for (i, n) in self.buckets.iter().enumerate() {
            seen += n;
            if seen >= rank {
                return Self::bucket_high(i).min(self.max);
            }
        }
        self.max
    }

    fn json(&self) -> String {
        let us = |ns: u64| ns as f64 / 1000.0;
        let mean = if self.count == 0 { 0.0 } else { self.sum as f64 / self.count as f64 / 1000.0 };
        format!(
            "{{\"n\":{},\"mean\":{:.1},\"p50\":{:.1},\"p90\":{:.1},\"p99\":{:.1},\"p999\":{:.1},\"max\":{:.1}}}",
            self.count,
            mean,
            us(self.quantile_permille(500)),
            us(self.quantile_permille(900)),
            us(self.quantile_permille(990)),
            us(self.quantile_permille(999)),
            us(self.max)
        )
    }
}

const H_SCHED: usize = 0;
const H_SVC: usize = 1;
const H_SCAN: usize = 2;
const H_DELETE: usize = 3;
const H_TRIM: usize = 4;
const H_E2E: usize = 5;
const H_PROBE: usize = 6;
const HIST_NAMES: [&str; 7] = ["produce_sched", "produce_svc", "scan", "delete", "trim", "e2e", "probe"];

#[derive(Clone)]
struct HistSet([Hist; 7]);

impl HistSet {
    fn new() -> Self {
        HistSet(std::array::from_fn(|_| Hist::new()))
    }

    fn merge(&mut self, o: &HistSet) {
        for (a, b) in self.0.iter_mut().zip(&o.0) {
            a.merge(b);
        }
    }

    fn reset(&mut self) {
        for h in &mut self.0 {
            h.reset();
        }
    }

    fn json(&self) -> String {
        let parts: Vec<String> = HIST_NAMES
            .iter()
            .zip(&self.0)
            .map(|(name, h)| format!("\"{name}\":{}", h.json()))
            .collect();
        format!("{{{}}}", parts.join(","))
    }
}

macro_rules! nq_counters {
    ($($name:ident),* $(,)?) => {
        #[derive(Default)]
        struct Counters {
            $($name: AtomicU64,)*
        }

        impl Counters {
            fn snapshot(&self) -> Vec<u64> {
                vec![$(self.$name.load(AtOrd::Relaxed),)*]
            }
        }

        const COUNTER_NAMES: &[&str] = &[$(stringify!($name),)*];
    };
}

nq_counters!(
    produced_msgs,
    produce_batches,
    acked_msgs,
    acked_batches,
    produce_retries,
    produce_abandoned,
    write_err_busy,
    write_err_other,
    redis_wait_short,
    consumed_msgs,
    scans,
    empty_scans,
    gap_stops,
    dups,
    out_of_order,
    bad_values,
    scan_err_busy,
    scan_err_other,
    deleted_msgs,
    delete_ops,
    delete_err_busy,
    delete_err_other,
    ack_ops,
    trim_ops,
    trimmed_msgs,
    trim_err_busy,
    trim_err_other,
    probes,
    probe_entries,
    probe_violations,
    probe_err,
);

fn bump(c: &AtomicU64, v: u64) {
    if v != 0 {
        c.fetch_add(v, AtOrd::Relaxed);
    }
}

fn counter_index(name: &str) -> usize {
    COUNTER_NAMES.iter().position(|n| *n == name).expect("known counter")
}

fn counts_json(values: &[u64]) -> String {
    let parts: Vec<String> = COUNTER_NAMES
        .iter()
        .zip(values)
        .map(|(n, v)| format!("\"{n}\":{v}"))
        .collect();
    format!("{{{}}}", parts.join(","))
}

pub(super) fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ErrKind {
    Busy,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeleteMode {
    Range,
    PerKey,
    None,
}

impl DeleteMode {
    fn parse(s: &str) -> Self {
        match s {
            "range" => DeleteMode::Range,
            "perkey" => DeleteMode::PerKey,
            "none" => DeleteMode::None,
            other => panic!("VALS_NQ_DELETE must be range, perkey or none, got {other}"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            DeleteMode::Range => "range",
            DeleteMode::PerKey => "perkey",
            DeleteMode::None => "none",
        }
    }
}

#[derive(Clone, Debug)]
struct NqConfig {
    partitions: usize,
    producers: usize,
    consumers: usize,
    batch: usize,
    inflight: usize,
    vsize: usize,
    rate: f64,
    w: usize,
    r: usize,
    device: bool,
    scan_limit: usize,
    delete: DeleteMode,
    delete_every: u64,
    poll_us: u64,
    ring_capacity: u64,
    trim_ms: u64,
    probe_ms: u64,
    probe_parts: usize,
    start_backlog: u64,
    drain_backlog: u64,
    secs: f64,
    warmup_secs: f64,
    total: u64,
    sample_secs: f64,
    verify: bool,
    drain_wait_secs: f64,
    perkey_conc: usize,
    prefix: String,
    redis_wait: usize,
    client_id: u32,
    bounded: bool,
    deleter_clock_skew_ms: u64,
}

pub(super) fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(default)
}

impl NqConfig {
    fn from_env() -> Self {
        let partitions = (env_u64("VALS_NQ_PARTITIONS", 64) as usize).clamp(1, 9999);
        let producers = (env_u64("VALS_NQ_PRODUCERS", 8) as usize).clamp(1, partitions);
        let consumers = (env_u64("VALS_NQ_CONSUMERS", 8) as usize).min(partitions);
        let batch = (env_u64("VALS_NQ_BATCH", 64) as usize).max(1);
        let vsize = (env_u64("VALS_NQ_VSIZE", 512) as usize).max(16);
        NqConfig {
            partitions,
            producers,
            consumers,
            batch,
            inflight: (env_u64("VALS_NQ_INFLIGHT", 1) as usize).max(1),
            vsize,
            rate: env_f64("VALS_NQ_RATE", 0.0).max(0.0),
            w: (env_u64("VALS_NQ_W", 2) as usize).max(1),
            r: (env_u64("VALS_NQ_R", 2) as usize).max(1),
            device: std::env::var("VALS_NQ_PERSISTENCE").is_ok_and(|v| v == "device"),
            scan_limit: (env_u64("VALS_NQ_SCAN_LIMIT", 1024) as usize).max(1),
            delete: DeleteMode::parse(&std::env::var("VALS_NQ_DELETE").unwrap_or_else(|_| "range".into())),
            delete_every: env_u64("VALS_NQ_DELETE_EVERY", 0),
            poll_us: env_u64("VALS_NQ_POLL_US", 1000),
            ring_capacity: env_u64("VALS_NQ_RING_CAPACITY", 0),
            trim_ms: env_u64("VALS_NQ_TRIM_MS", 100).max(1),
            probe_ms: env_u64("VALS_NQ_PROBE_MS", 1000),
            probe_parts: (env_u64("VALS_NQ_PROBE_PARTS", 4) as usize).clamp(1, partitions),
            start_backlog: env_u64("VALS_NQ_CONSUMER_START_BACKLOG", 0),
            drain_backlog: env_u64("VALS_NQ_DRAIN_BACKLOG", (partitions * batch) as u64),
            secs: env_f64("VALS_NQ_SECS", 60.0),
            warmup_secs: env_f64("VALS_NQ_WARMUP_SECS", 5.0),
            total: env_u64("VALS_NQ_TOTAL", 0),
            sample_secs: env_f64("VALS_NQ_SAMPLE_SECS", 5.0).max(0.1),
            verify: env_u64("VALS_NQ_VERIFY", 1) != 0,
            drain_wait_secs: env_f64("VALS_NQ_DRAIN_WAIT_SECS", 60.0),
            perkey_conc: (env_u64("VALS_NQ_PERKEY_CONC", 64) as usize).max(1),
            prefix: std::env::var("VALS_NQ_PREFIX").unwrap_or_default(),
            redis_wait: env_u64("VALS_NQ_REDIS_WAIT", 0) as usize,
            client_id: env_u64("VALS_NQ_CLIENT_ID", 0) as u32,
            bounded: std::env::var_os("VALS_BENCH_BOUNDED_CLIENT").is_some(),
            deleter_clock_skew_ms: env_u64("VALS_NQ_DELETER_CLOCK_SKEW_MS", 0),
        }
    }

    fn json(&self) -> String {
        format!(
            "{{\"partitions\":{},\"producers\":{},\"consumers\":{},\"batch\":{},\"inflight\":{},\"vsize\":{},\"rate\":{},\"w\":{},\"r\":{},\"persistence\":\"{}\",\"scan_limit\":{},\"delete\":\"{}\",\"delete_every\":{},\"poll_us\":{},\"ring_capacity\":{},\"trim_ms\":{},\"probe_ms\":{},\"probe_parts\":{},\"consumer_start_backlog\":{},\"drain_backlog\":{},\"secs\":{},\"warmup_secs\":{},\"total\":{},\"sample_secs\":{},\"verify\":{},\"drain_wait_secs\":{},\"perkey_conc\":{},\"prefix\":{},\"redis_wait\":{},\"client_id\":{},\"bounded_client\":{},\"deleter_clock_skew_ms\":{}}}",
            self.partitions,
            self.producers,
            self.consumers,
            self.batch,
            self.inflight,
            self.vsize,
            self.rate,
            self.w,
            self.r,
            if self.device { "device" } else { "buffered" },
            self.scan_limit,
            self.delete.name(),
            self.delete_every,
            self.poll_us,
            self.ring_capacity,
            self.trim_ms,
            self.probe_ms,
            self.probe_parts,
            self.start_backlog,
            self.drain_backlog,
            self.secs,
            self.warmup_secs,
            self.total,
            self.sample_secs,
            self.verify,
            self.drain_wait_secs,
            self.perkey_conc,
            json_str(&self.prefix),
            self.redis_wait,
            self.client_id,
            self.bounded,
            self.deleter_clock_skew_ms
        )
    }
}

fn make_msg(vsize: usize, ts_ns: u64, seq: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(vsize);
    v.extend_from_slice(&ts_ns.to_le_bytes());
    v.extend_from_slice(&seq.to_le_bytes());
    for i in 16..vsize {
        v.push((seq.wrapping_add(i as u64) & 0xFF) as u8);
    }
    v
}

fn check_msg(v: &[u8], vsize: usize, seq: u64) -> Option<u64> {
    if v.len() != vsize {
        return None;
    }
    let ts = u64::from_le_bytes(v[0..8].try_into().ok()?);
    let s = u64::from_le_bytes(v[8..16].try_into().ok()?);
    if s != seq {
        return None;
    }
    let filler_ok = v[16..]
        .iter()
        .enumerate()
        .all(|(i, b)| *b == (seq.wrapping_add(i as u64 + 16) & 0xFF) as u8);
    filler_ok.then_some(ts)
}

#[derive(Debug, Default, PartialEq, Eq)]
struct PrefixTake {
    taken: Vec<usize>,
    dups: u64,
    out_of_order: u64,
    gap: bool,
}

fn take_prefix(expected: u64, seqs: &[u64]) -> PrefixTake {
    let mut out = PrefixTake::default();
    let mut next = expected;
    let mut prev: Option<u64> = None;
    for (i, &s) in seqs.iter().enumerate() {
        if prev.is_some_and(|p| s < p) {
            out.out_of_order += 1;
        }
        prev = Some(prev.map_or(s, |p| p.max(s)));
        if s < next {
            out.dups += 1;
            continue;
        }
        if s > next {
            out.gap = true;
            break;
        }
        out.taken.push(i);
        next += 1;
    }
    out
}

fn vals_key(prefix: &str, part: usize, seq: u64) -> Vec<u8> {
    format!("{prefix}p{part:04}/{seq:016}").into_bytes()
}

fn vals_part_lo(prefix: &str, part: usize) -> Vec<u8> {
    format!("{prefix}p{part:04}/").into_bytes()
}

fn vals_part_hi(prefix: &str, part: usize) -> Vec<u8> {
    format!("{prefix}p{part:04}0").into_bytes()
}

fn vals_key_seq(key: &[u8]) -> Option<u64> {
    if key.len() < 17 || key[key.len() - 17] != b'/' {
        return None;
    }
    std::str::from_utf8(&key[key.len() - 16..]).ok()?.parse().ok()
}

fn stream_id(seq: u64) -> String {
    format!("0-{}", seq + 1)
}

fn stream_id_seq(id: &[u8]) -> Option<u64> {
    let s = std::str::from_utf8(id).ok()?;
    let (ms, n) = s.split_once('-')?;
    if ms != "0" {
        return None;
    }
    n.parse::<u64>().ok()?.checked_sub(1)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Resp {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Resp>>),
}

fn resp_encode(buf: &mut Vec<u8>, args: &[&[u8]]) {
    let _ = write!(buf, "*{}\r\n", args.len());
    for a in args {
        let _ = write!(buf, "${}\r\n", a.len());
        buf.extend_from_slice(a);
        buf.extend_from_slice(b"\r\n");
    }
}

fn resp_invalid(what: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, format!("resp: {what}"))
}

fn resp_read<R: BufRead>(r: &mut R) -> std::io::Result<Resp> {
    let mut line = Vec::new();
    r.read_until(b'\n', &mut line)?;
    if line.len() < 3 || !line.ends_with(b"\r\n") {
        return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "resp: short line"));
    }
    let body = &line[1..line.len() - 2];
    let num = || -> std::io::Result<i64> {
        std::str::from_utf8(body)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| resp_invalid("bad integer"))
    };
    match line[0] {
        b'+' => Ok(Resp::Simple(String::from_utf8_lossy(body).into_owned())),
        b'-' => Ok(Resp::Error(String::from_utf8_lossy(body).into_owned())),
        b':' => Ok(Resp::Int(num()?)),
        b'$' => {
            let n = num()?;
            if n < 0 {
                return Ok(Resp::Bulk(None));
            }
            let mut data = vec![0u8; n as usize + 2];
            r.read_exact(&mut data)?;
            if !data.ends_with(b"\r\n") {
                return Err(resp_invalid("bulk not terminated"));
            }
            data.truncate(n as usize);
            Ok(Resp::Bulk(Some(data)))
        }
        b'*' => {
            let n = num()?;
            if n < 0 {
                return Ok(Resp::Array(None));
            }
            let mut items = Vec::with_capacity((n as usize).min(1 << 16));
            for _ in 0..n {
                items.push(resp_read(r)?);
            }
            Ok(Resp::Array(Some(items)))
        }
        _ => Err(resp_invalid("unknown type byte")),
    }
}

fn redis_entries(entries: &Resp) -> Vec<(u64, Option<Vec<u8>>)> {
    let mut out = Vec::new();
    let Resp::Array(Some(items)) = entries else {
        return out;
    };
    for item in items {
        let Resp::Array(Some(pair)) = item else { continue };
        let Some(Resp::Bulk(Some(id))) = pair.first() else { continue };
        let Some(seq) = stream_id_seq(id) else { continue };
        let mut value = None;
        if let Some(Resp::Array(Some(fields))) = pair.get(1) {
            for kv in fields.chunks(2) {
                if let [Resp::Bulk(Some(k)), Resp::Bulk(Some(v))] = kv {
                    if k.as_slice() == b"v" {
                        value = Some(v.clone());
                    }
                }
            }
        }
        out.push((seq, value));
    }
    out
}

fn redis_read_entries(reply: &Resp) -> Vec<(u64, Option<Vec<u8>>)> {
    let Resp::Array(Some(streams)) = reply else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for s in streams {
        if let Resp::Array(Some(kv)) = s {
            if let Some(entries) = kv.get(1) {
                out.extend(redis_entries(entries));
            }
        }
    }
    out
}

fn redis_err_kind(e: &str) -> ErrKind {
    let code = e.split(' ').next().unwrap_or("");
    if matches!(code, "BUSY" | "LOADING" | "TRYAGAIN") {
        ErrKind::Busy
    } else {
        ErrKind::Other
    }
}
struct AppendReq {
    part: usize,
    first: u64,
    values: Vec<Vec<u8>>,
}

enum Release<'a> {
    Ack,
    Upto(u64),
    Keys(&'a [u64]),
}

#[derive(Default)]
struct Released {
    round_trip: bool,
    ops: u64,
    failed: Vec<u64>,
    err: Option<ErrKind>,
}

trait QueueBackend {
    fn prepare(&mut self, partitions: usize, groups: bool) -> Result<(), String>;
    fn append(&mut self, reqs: &[&AppendReq]) -> Vec<(Result<(), ErrKind>, Instant)>;
    fn scan(&mut self, part: usize, from: u64, limit: usize) -> Result<Vec<(u64, Vec<u8>)>, ErrKind>;
    fn release(&mut self, part: usize, consumed: std::ops::Range<u64>, action: Release<'_>) -> Released;
    fn trim(&mut self, part: usize, upto: u64) -> Result<(), ErrKind>;
    fn head(&mut self, part: usize, limit: usize) -> Result<Vec<u64>, ErrKind>;
    fn verify_scan(&mut self, part: usize, visit: &mut dyn FnMut(u64, Option<&[u8]>)) -> Result<(), String>;
    fn live_count(&mut self, _part: usize) -> Option<u64> {
        None
    }
    fn take_wait_short(&mut self) -> u64 {
        0
    }
}

#[derive(Clone)]
enum Target {
    Vals {
        nodes: Vec<(u32, Vec<SocketAddr>)>,
        zone: vals_cluster::ZoneId,
    },
    Redis {
        endpoints: Vec<String>,
    },
}

impl Target {
    fn name(&self) -> &'static str {
        match self {
            Target::Vals { .. } => "vals",
            Target::Redis { .. } => "redis",
        }
    }

    fn connect(&self, cfg: &NqConfig, role: u32, deleter: bool) -> Box<dyn QueueBackend> {
        match self {
            Target::Vals { nodes, zone } => {
                let mut coord = vals_coordinator(nodes, cfg, 3_000_000 + cfg.client_id * 10_000 + role);
                if deleter && cfg.deleter_clock_skew_ms > 0 {
                    let skew = cfg.deleter_clock_skew_ms;
                    coord = coord.with_clock(move || {
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_or(0, |d| d.as_millis() as u64)
                            + skew
                    });
                }
                let persistence = if cfg.device {
                    vals_cluster::Persistence::Device
                } else {
                    vals_cluster::Persistence::Buffered
                };
                Box::new(ValsQueue {
                    coord,
                    zone: *zone,
                    opts: vals_cluster::WriteOptions {
                        concern: vals_cluster::WriteConcern::Quorum(cfg.w),
                        persistence,
                    },
                    r: cfg.r,
                    prefix: cfg.prefix.clone(),
                    perkey_conc: cfg.perkey_conc,
                })
            }
            Target::Redis { endpoints } => Box::new(RedisQueue {
                endpoints: endpoints.clone(),
                conns: endpoints.iter().map(|_| None).collect(),
                prefix: cfg.prefix.clone(),
                wait: cfg.redis_wait,
                consumer: format!("c{}-{role}", cfg.client_id).into_bytes(),
                delivered: HashMap::new(),
                wait_short: 0,
            }),
        }
    }
}

pub(super) fn parse_peers(spec: &str) -> Vec<(u32, Vec<SocketAddr>)> {
    let sep = if spec.contains(';') { ';' } else { ',' };
    let groups: Vec<&str> = if sep == ';' {
        spec.split(';').collect()
    } else {
        spec.split(',').collect()
    };
    groups
        .into_iter()
        .filter(|s| !s.trim().is_empty())
        .map(|s| {
            let (id, addrs) = s.split_once('@').expect("peer must be id@addr[,addr...]");
            let addrs: Vec<SocketAddr> = addrs
                .split(',')
                .filter(|a| !a.trim().is_empty())
                .map(|a| a.trim().parse().expect("peer addr (ip:port)"))
                .collect();
            (id.trim().parse().expect("peer id"), addrs)
        })
        .collect()
}

fn vals_coordinator(
    nodes: &[(u32, Vec<SocketAddr>)],
    cfg: &NqConfig,
    id: u32,
) -> vals_cluster::Coordinator<vals_cluster::NetTransport> {
    use vals_cluster::{reactor, Coordinator, NetTransport, Ring};
    let mut ring = Ring::new(nodes.len().clamp(1, 3));
    ring.set_members_weighted(nodes.iter().map(|(id, _)| (*id, 1)));
    let transport = if cfg.bounded {
        NetTransport::connect_sharded(nodes.iter().cloned())
    } else {
        NetTransport::connect_sharded_unbounded(nodes.iter().cloned())
    };
    let coord = Coordinator::new(id, ring, cfg.r, transport);
    let adopted = poll_until(Duration::from_secs(30), Duration::from_millis(200), || {
        vals_rt::block_on(coord.adopt_from_any_member(), reactor::poll_io)
    });
    if !adopted {
        eprintln!("netqueue: could not adopt the ring/catalog from any peer");
        std::process::exit(2);
    }
    coord
}

fn vals_err_kind(e: &vals_cluster::CoordError) -> ErrKind {
    use vals_cluster::{CoordError, TransportError};
    match e {
        CoordError::Busy { .. } | CoordError::Overloaded => ErrKind::Busy,
        CoordError::ScanQuorumFailed { replicas, .. }
            if replicas
                .iter()
                .any(|r| matches!(r.error, TransportError::Busy | TransportError::Overloaded)) =>
        {
            ErrKind::Busy
        }
        CoordError::RangeDeleteIncomplete { missed, .. }
            if missed
                .iter()
                .any(|m| matches!(m.error, CoordError::Busy { .. } | CoordError::Overloaded)) =>
        {
            ErrKind::Busy
        }
        _ => ErrKind::Other,
    }
}

type KvPage = Vec<(Vec<u8>, Vec<u8>)>;

struct ValsQueue {
    coord: vals_cluster::Coordinator<vals_cluster::NetTransport>,
    zone: vals_cluster::ZoneId,
    opts: vals_cluster::WriteOptions,
    r: usize,
    prefix: String,
    perkey_conc: usize,
}

impl ValsQueue {
    fn run<F: std::future::Future>(&self, fut: F) -> F::Output {
        vals_rt::block_on(fut, vals_cluster::reactor::poll_io)
    }

    fn scan_range(&self, lo: &[u8], hi: &[u8], limit: usize) -> Result<KvPage, vals_cluster::CoordError> {
        self.run(self.coord.scan_in(self.zone, Some(lo), Some(hi), self.r, Some(limit)))
    }

    fn delete_upto(&self, part: usize, upto: u64) -> Result<(), ErrKind> {
        let lo = vals_part_lo(&self.prefix, part);
        let hi = vals_key(&self.prefix, part, upto);
        self.run(self.coord.delete_range_in(self.zone, Some(&lo), Some(&hi), self.opts))
            .map(|_| ())
            .map_err(|e| vals_err_kind(&e))
    }
}

impl QueueBackend for ValsQueue {
    fn prepare(&mut self, partitions: usize, _groups: bool) -> Result<(), String> {
        let lo = vals_part_lo(&self.prefix, 0);
        let hi = vals_part_hi(&self.prefix, 0);
        let mut last = String::new();
        let ready = poll_until(Duration::from_secs(60), Duration::from_millis(250), || {
            match self.scan_range(&lo, &hi, 1) {
                Ok(_) => true,
                Err(e) => {
                    last = e.to_string();
                    false
                }
            }
        });
        if !ready {
            return Err(format!("zone not serving scans after 60s: {last}"));
        }
        for part in 0..partitions {
            let lo = vals_part_lo(&self.prefix, part);
            let hi = vals_part_hi(&self.prefix, part);
            let entries = self
                .scan_range(&lo, &hi, 1)
                .map_err(|e| format!("preflight scan of partition {part}: {e}"))?;
            if !entries.is_empty() {
                return Err(format!(
                    "partition {part} is not empty (use a fresh zone or VALS_NQ_PREFIX)"
                ));
            }
        }
        Ok(())
    }

    fn append(&mut self, reqs: &[&AppendReq]) -> Vec<(Result<(), ErrKind>, Instant)> {
        let keys: Vec<Vec<Vec<u8>>> = reqs
            .iter()
            .map(|q| {
                (0..q.values.len() as u64)
                    .map(|i| vals_key(&self.prefix, q.part, q.first + i))
                    .collect()
            })
            .collect();
        let writes: Vec<Vec<(&[u8], &[u8])>> = reqs
            .iter()
            .zip(&keys)
            .map(|(q, ks)| {
                ks.iter()
                    .zip(&q.values)
                    .map(|(k, v)| (k.as_slice(), v.as_slice()))
                    .collect()
            })
            .collect();
        let coord = &self.coord;
        let (zone, opts) = (self.zone, self.opts);
        let futs: Vec<_> = writes
            .iter()
            .map(|w| async move {
                let result = match coord.write_batch_in(zone, w, opts).await {
                    Ok(batch) => match batch.failures().next() {
                        None => Ok(()),
                        Some((_, e)) => Err(vals_err_kind(e)),
                    },
                    Err(e) => Err(vals_err_kind(&e)),
                };
                (result, Instant::now())
            })
            .collect();
        self.run(vals_rt::join_all(futs))
    }

    fn scan(&mut self, part: usize, from: u64, limit: usize) -> Result<Vec<(u64, Vec<u8>)>, ErrKind> {
        let lo = vals_key(&self.prefix, part, from);
        let hi = vals_part_hi(&self.prefix, part);
        let entries = self.scan_range(&lo, &hi, limit).map_err(|e| vals_err_kind(&e))?;
        Ok(entries
            .into_iter()
            .filter_map(|(k, v)| vals_key_seq(&k).map(|s| (s, v)))
            .collect())
    }

    fn release(&mut self, part: usize, _consumed: std::ops::Range<u64>, action: Release<'_>) -> Released {
        match action {
            Release::Ack => Released::default(),
            Release::Upto(upto) => {
                let r = self.delete_upto(part, upto);
                Released { round_trip: true, ops: 1, failed: Vec::new(), err: r.err() }
            }
            Release::Keys(seqs) => {
                let mut out = Released { round_trip: !seqs.is_empty(), ..Released::default() };
                for chunk in seqs.chunks(self.perkey_conc) {
                    let keys: Vec<Vec<u8>> = chunk.iter().map(|s| vals_key(&self.prefix, part, *s)).collect();
                    let futs: Vec<_> = keys
                        .iter()
                        .map(|k| self.coord.delete_in(self.zone, k, self.opts))
                        .collect();
                    let results = self.run(vals_rt::join_all(futs));
                    out.ops += chunk.len() as u64;
                    for (s, r) in chunk.iter().zip(results) {
                        if let Err(e) = r {
                            out.failed.push(*s);
                            out.err.get_or_insert(vals_err_kind(&e));
                        }
                    }
                }
                out
            }
        }
    }

    fn trim(&mut self, part: usize, upto: u64) -> Result<(), ErrKind> {
        self.delete_upto(part, upto)
    }

    fn head(&mut self, part: usize, limit: usize) -> Result<Vec<u64>, ErrKind> {
        let lo = vals_part_lo(&self.prefix, part);
        let hi = vals_part_hi(&self.prefix, part);
        let entries = self.scan_range(&lo, &hi, limit).map_err(|e| vals_err_kind(&e))?;
        Ok(entries.iter().filter_map(|(k, _)| vals_key_seq(k)).collect())
    }

    fn verify_scan(&mut self, part: usize, visit: &mut dyn FnMut(u64, Option<&[u8]>)) -> Result<(), String> {
        let hi = vals_part_hi(&self.prefix, part);
        let mut lo = vals_part_lo(&self.prefix, part);
        loop {
            let mut page = Err(String::new());
            for _ in 0..20 {
                page = self.scan_range(&lo, &hi, 4096).map_err(|e| e.to_string());
                if page.is_ok() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(250));
            }
            let page = page?;
            let Some((last, _)) = page.last() else { return Ok(()) };
            let mut next = last.clone();
            next.push(0);
            for (k, v) in &page {
                match vals_key_seq(k) {
                    Some(s) => visit(s, Some(v)),
                    None => visit(u64::MAX, None),
                }
            }
            lo = next;
        }
    }
}

struct RedisConn {
    w: TcpStream,
    r: BufReader<TcpStream>,
    out: Vec<u8>,
}

impl RedisConn {
    fn connect(addr: &str) -> std::io::Result<Self> {
        let s = TcpStream::connect(addr)?;
        s.set_nodelay(true)?;
        Ok(RedisConn { r: BufReader::with_capacity(1 << 16, s.try_clone()?), w: s, out: Vec::new() })
    }

    fn push(&mut self, args: &[&[u8]]) {
        resp_encode(&mut self.out, args);
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let r = self.w.write_all(&self.out);
        self.out.clear();
        r
    }

    fn read(&mut self) -> std::io::Result<Resp> {
        resp_read(&mut self.r)
    }
}

const REDIS_GROUP: &[u8] = b"g";

struct RedisQueue {
    endpoints: Vec<String>,
    conns: Vec<Option<RedisConn>>,
    prefix: String,
    wait: usize,
    consumer: Vec<u8>,
    delivered: HashMap<usize, u64>,
    wait_short: u64,
}

impl RedisQueue {
    fn ep(&self, part: usize) -> usize {
        part % self.endpoints.len()
    }

    fn stream_key(&self, part: usize) -> Vec<u8> {
        format!("{}q:{part:04}", self.prefix).into_bytes()
    }

    fn conn(&mut self, ep: usize) -> std::io::Result<&mut RedisConn> {
        let slot = &mut self.conns[ep];
        if slot.is_none() {
            *slot = Some(RedisConn::connect(&self.endpoints[ep])?);
        }
        Ok(slot.as_mut().expect("connected"))
    }

    fn rpc(&mut self, part: usize, cmds: &[Vec<&[u8]>]) -> Result<Vec<Resp>, ErrKind> {
        let ep = self.ep(part);
        let c = self.conn(ep).map_err(|_| ErrKind::Other)?;
        for cmd in cmds {
            c.push(cmd);
        }
        let mut replies = Vec::with_capacity(cmds.len());
        let mut ok = c.flush().is_ok();
        if ok {
            for _ in cmds {
                match c.read() {
                    Ok(r) => replies.push(r),
                    Err(_) => {
                        ok = false;
                        break;
                    }
                }
            }
        }
        if !ok {
            self.conns[ep] = None;
            return Err(ErrKind::Other);
        }
        Ok(replies)
    }

    fn first_error(replies: &[Resp]) -> Option<ErrKind> {
        replies.iter().find_map(|r| match r {
            Resp::Error(e) => Some(redis_err_kind(e)),
            _ => None,
        })
    }
}

impl QueueBackend for RedisQueue {
    fn prepare(&mut self, partitions: usize, groups: bool) -> Result<(), String> {
        for part in 0..partitions {
            let key = self.stream_key(part);
            let mut cmds: Vec<Vec<&[u8]>> = vec![vec![b"DEL", &key]];
            if groups {
                cmds.push(vec![b"XGROUP", b"CREATE", &key, REDIS_GROUP, b"0", b"MKSTREAM"]);
            }
            let replies = self
                .rpc(part, &cmds)
                .map_err(|_| format!("redis endpoint {} unreachable", self.endpoints[self.ep(part)]))?;
            for r in replies {
                if let Resp::Error(e) = r {
                    return Err(format!("redis prepare of partition {part}: {e}"));
                }
            }
        }
        Ok(())
    }

    fn append(&mut self, reqs: &[&AppendReq]) -> Vec<(Result<(), ErrKind>, Instant)> {
        let n = self.endpoints.len();
        let mut broken = vec![false; n];
        let mut used = vec![false; n];
        let wait = self.wait;
        let wait_arg = wait.to_string();
        for q in reqs {
            let ep = self.ep(q.part);
            if broken[ep] {
                continue;
            }
            let key = self.stream_key(q.part);
            match self.conn(ep) {
                Ok(c) => {
                    used[ep] = true;
                    for (i, v) in q.values.iter().enumerate() {
                        let id = stream_id(q.first + i as u64);
                        c.push(&[b"XADD", &key, id.as_bytes(), b"v", v]);
                    }
                    if wait > 0 {
                        c.push(&[b"WAIT", wait_arg.as_bytes(), b"1000"]);
                    }
                }
                Err(_) => broken[ep] = true,
            }
        }
        for ep in 0..n {
            if used[ep] && !broken[ep] {
                let ok = self.conns[ep].as_mut().is_some_and(|c| c.flush().is_ok());
                if !ok {
                    broken[ep] = true;
                    self.conns[ep] = None;
                }
            }
        }
        let mut out = Vec::with_capacity(reqs.len());
        for q in reqs {
            let ep = self.ep(q.part);
            if broken[ep] {
                out.push((Err(ErrKind::Other), Instant::now()));
                continue;
            }
            let c = self.conns[ep].as_mut().expect("flushed connection");
            let mut result = Ok(());
            let mut io_failed = false;
            let mut short = false;
            for _ in 0..q.values.len() {
                match c.read() {
                    Ok(Resp::Error(e)) if !e.contains("equal or smaller") => result = Err(redis_err_kind(&e)),
                    Ok(_) => {}
                    Err(_) => {
                        io_failed = true;
                        break;
                    }
                }
            }
            if !io_failed && wait > 0 {
                match c.read() {
                    Ok(Resp::Int(k)) if k >= 0 && k as usize >= wait => {}
                    Ok(_) => short = true,
                    Err(_) => io_failed = true,
                }
            }
            if short {
                self.wait_short += 1;
            }
            if io_failed {
                broken[ep] = true;
                self.conns[ep] = None;
                result = Err(ErrKind::Other);
            }
            out.push((result, Instant::now()));
        }
        out
    }

    fn scan(&mut self, part: usize, from: u64, limit: usize) -> Result<Vec<(u64, Vec<u8>)>, ErrKind> {
        let key = self.stream_key(part);
        let delivered = self.delivered.get(&part).copied().unwrap_or(0);
        let start = if delivered > from { format!("0-{from}") } else { ">".to_string() };
        let count = limit.to_string();
        let consumer = self.consumer.clone();
        let cmd: Vec<&[u8]> = vec![
            b"XREADGROUP",
            b"GROUP",
            REDIS_GROUP,
            &consumer,
            b"COUNT",
            count.as_bytes(),
            b"STREAMS",
            &key,
            start.as_bytes(),
        ];
        let reply = self.rpc(part, &[cmd])?.pop().unwrap_or(Resp::Array(None));
        if let Resp::Error(e) = &reply {
            return Err(redis_err_kind(e));
        }
        let entries: Vec<(u64, Vec<u8>)> = redis_read_entries(&reply)
            .into_iter()
            .filter_map(|(s, v)| v.map(|v| (s, v)))
            .collect();
        if let Some(max) = entries.iter().map(|(s, _)| *s).max() {
            let d = self.delivered.entry(part).or_insert(0);
            *d = (*d).max(max + 1);
        }
        Ok(entries)
    }

    fn release(&mut self, part: usize, consumed: std::ops::Range<u64>, action: Release<'_>) -> Released {
        let key = self.stream_key(part);
        let ack_ids: Vec<String> = consumed.map(stream_id).collect();
        let mut cmds: Vec<Vec<&[u8]>> = Vec::new();
        if !ack_ids.is_empty() {
            let mut cmd: Vec<&[u8]> = vec![b"XACK", &key, REDIS_GROUP];
            cmd.extend(ack_ids.iter().map(|s| s.as_bytes()));
            cmds.push(cmd);
        }
        let mut ops = 0;
        let minid;
        let del_ids: Vec<String>;
        let mut keys: &[u64] = &[];
        match action {
            Release::Ack => {}
            Release::Upto(upto) => {
                minid = stream_id(upto);
                cmds.push(vec![b"XTRIM", &key, b"MINID", minid.as_bytes()]);
                ops = 1;
            }
            Release::Keys(seqs) => {
                keys = seqs;
                del_ids = seqs.iter().map(|s| stream_id(*s)).collect();
                if !del_ids.is_empty() {
                    let mut cmd: Vec<&[u8]> = vec![b"XDEL", &key];
                    cmd.extend(del_ids.iter().map(|s| s.as_bytes()));
                    cmds.push(cmd);
                    ops = 1;
                }
            }
        }
        if cmds.is_empty() {
            return Released::default();
        }
        let ack_only = ops == 0;
        let err = match self.rpc(part, &cmds) {
            Ok(replies) => Self::first_error(&replies),
            Err(k) => Some(k),
        };
        Released {
            round_trip: true,
            ops: if ack_only { 0 } else { ops },
            failed: if err.is_some() { keys.to_vec() } else { Vec::new() },
            err,
        }
    }

    fn trim(&mut self, part: usize, upto: u64) -> Result<(), ErrKind> {
        let key = self.stream_key(part);
        let minid = stream_id(upto);
        let replies = self.rpc(part, &[vec![b"XTRIM", &key, b"MINID", minid.as_bytes()]])?;
        match Self::first_error(&replies) {
            Some(k) => Err(k),
            None => Ok(()),
        }
    }

    fn head(&mut self, part: usize, limit: usize) -> Result<Vec<u64>, ErrKind> {
        let key = self.stream_key(part);
        let count = limit.to_string();
        let reply = self
            .rpc(part, &[vec![b"XRANGE", &key, b"-", b"+", b"COUNT", count.as_bytes()]])?
            .pop()
            .unwrap_or(Resp::Array(None));
        if let Resp::Error(e) = &reply {
            return Err(redis_err_kind(e));
        }
        Ok(redis_entries(&reply).into_iter().map(|(s, _)| s).collect())
    }

    fn verify_scan(&mut self, part: usize, visit: &mut dyn FnMut(u64, Option<&[u8]>)) -> Result<(), String> {
        let key = self.stream_key(part);
        let mut start = "-".to_string();
        loop {
            let reply = self
                .rpc(part, &[vec![b"XRANGE", &key, start.as_bytes(), b"+", b"COUNT", b"4096"]])
                .map_err(|_| format!("XRANGE on partition {part} failed"))?
                .pop()
                .unwrap_or(Resp::Array(None));
            if let Resp::Error(e) = &reply {
                return Err(format!("XRANGE on partition {part}: {e}"));
            }
            let entries = redis_entries(&reply);
            let Some(last) = entries.last().map(|(s, _)| *s) else { return Ok(()) };
            for (s, v) in &entries {
                visit(*s, v.as_deref());
            }
            start = format!("({}", stream_id(last));
        }
    }

    fn live_count(&mut self, part: usize) -> Option<u64> {
        let key = self.stream_key(part);
        match self.rpc(part, &[vec![b"XLEN", &key]]).ok()?.pop()? {
            Resp::Int(n) if n >= 0 => Some(n as u64),
            _ => None,
        }
    }

    fn take_wait_short(&mut self) -> u64 {
        std::mem::take(&mut self.wait_short)
    }
}
struct Shared {
    cfg: NqConfig,
    t0: Instant,
    c: Counters,
    head: Vec<AtomicU64>,
    attempted: Vec<AtomicU64>,
    cursor: Vec<AtomicU64>,
    floor: Vec<AtomicU64>,
    del_attempted: Vec<AtomicU64>,
    stop_producers: AtomicBool,
    stop_consumers: AtomicBool,
    stop_aux: AtomicBool,
    consumers_go: AtomicBool,
    producers_done: AtomicUsize,
    hists: Mutex<Vec<Arc<Mutex<HistSet>>>>,
}

fn atomics(n: usize) -> Vec<AtomicU64> {
    (0..n).map(|_| AtomicU64::new(0)).collect()
}

impl Shared {
    fn new(cfg: NqConfig) -> Self {
        let n = cfg.partitions;
        Shared {
            cfg,
            t0: Instant::now(),
            c: Counters::default(),
            head: atomics(n),
            attempted: atomics(n),
            cursor: atomics(n),
            floor: atomics(n),
            del_attempted: atomics(n),
            stop_producers: AtomicBool::new(false),
            stop_consumers: AtomicBool::new(false),
            stop_aux: AtomicBool::new(false),
            consumers_go: AtomicBool::new(false),
            producers_done: AtomicUsize::new(0),
            hists: Mutex::new(Vec::new()),
        }
    }

    fn now_ns(&self) -> u64 {
        self.t0.elapsed().as_nanos() as u64
    }

    fn ns_of(&self, t: Instant) -> u64 {
        t.saturating_duration_since(self.t0).as_nanos() as u64
    }

    fn register(&self) -> Arc<Mutex<HistSet>> {
        let h = Arc::new(Mutex::new(HistSet::new()));
        self.hists.lock().unwrap().push(h.clone());
        h
    }

    fn harvest(&self) -> HistSet {
        let mut out = HistSet::new();
        for h in self.hists.lock().unwrap().iter() {
            let mut h = h.lock().unwrap();
            out.merge(&h);
            h.reset();
        }
        out
    }

    fn sum(v: &[AtomicU64]) -> u64 {
        v.iter().map(|a| a.load(AtOrd::Relaxed)).sum()
    }

    fn backlog(&self) -> Option<u64> {
        (self.cfg.consumers > 0).then(|| Self::sum(&self.head).saturating_sub(Self::sum(&self.cursor)))
    }

    fn live(&self) -> u64 {
        Self::sum(&self.head).saturating_sub(Self::sum(&self.floor))
    }
}

fn nap(stop: &AtomicBool, dur: Duration) {
    let end = Instant::now() + dur;
    loop {
        if stop.load(AtOrd::Relaxed) {
            return;
        }
        let now = Instant::now();
        if now >= end {
            return;
        }
        std::thread::sleep((end - now).min(Duration::from_millis(20)));
    }
}

fn count_err(kind: ErrKind, busy: &AtomicU64, other: &AtomicU64, n: u64) {
    match kind {
        ErrKind::Busy => bump(busy, n),
        ErrKind::Other => bump(other, n),
    }
}

struct Planned {
    slot: usize,
    part: usize,
    first: u64,
    n: u64,
    sched_ns: u64,
}

fn producer_loop(sh: &Shared, be: &mut dyn QueueBackend, parts: Vec<usize>, quota: u64) {
    let hs = sh.register();
    let cfg = &sh.cfg;
    let c = &sh.c;
    let inflight = cfg.inflight.min(parts.len()).max(1);
    let interval_ns = if cfg.rate > 0.0 {
        cfg.batch as f64 * cfg.producers as f64 / cfg.rate * 1e9
    } else {
        0.0
    };
    let mut next_seq = vec![0u64; parts.len()];
    let mut k: u64 = 0;
    let mut produced = 0u64;
    let start_ns = sh.now_ns();
    'run: loop {
        if sh.stop_producers.load(AtOrd::Relaxed) {
            break;
        }
        let mut group: Vec<Planned> = Vec::new();
        while group.len() < inflight {
            let planned: u64 = group.iter().map(|g| g.n).sum();
            if quota > 0 && produced + planned >= quota {
                break;
            }
            let now = sh.now_ns();
            let sched_ns = if interval_ns > 0.0 { start_ns + (k as f64 * interval_ns) as u64 } else { now };
            if !group.is_empty() && sched_ns > now {
                break;
            }
            let slot = (k as usize) % parts.len();
            let n = if quota > 0 { (cfg.batch as u64).min(quota - produced - planned) } else { cfg.batch as u64 };
            group.push(Planned { slot, part: parts[slot], first: next_seq[slot], n, sched_ns });
            k += 1;
        }
        if group.is_empty() {
            break;
        }
        let now = sh.now_ns();
        if group[0].sched_ns > now {
            nap(&sh.stop_producers, Duration::from_nanos(group[0].sched_ns - now));
            if sh.stop_producers.load(AtOrd::Relaxed) {
                break;
            }
        }
        let reqs: Vec<AppendReq> = group
            .iter()
            .map(|g| AppendReq {
                part: g.part,
                first: g.first,
                values: (g.first..g.first + g.n).map(|s| make_msg(cfg.vsize, g.sched_ns, s)).collect(),
            })
            .collect();
        for g in &group {
            next_seq[g.slot] += g.n;
            produced += g.n;
            sh.attempted[g.part].fetch_max(g.first + g.n, AtOrd::Relaxed);
            bump(&c.produced_msgs, g.n);
            bump(&c.produce_batches, 1);
        }
        let send_ns = sh.now_ns();
        let mut todo: Vec<usize> = (0..reqs.len()).collect();
        let mut attempt = 0u32;
        let mut retry_since: Option<Instant> = None;
        while !todo.is_empty() {
            let sub: Vec<&AppendReq> = todo.iter().map(|&i| &reqs[i]).collect();
            let results = be.append(&sub);
            bump(&c.redis_wait_short, be.take_wait_short());
            let mut failed = Vec::new();
            {
                let mut h = hs.lock().unwrap();
                for (&i, (result, done)) in todo.iter().zip(results) {
                    match result {
                        Ok(()) => {
                            let done_ns = sh.ns_of(done);
                            let g = &group[i];
                            h.0[H_SCHED].record(done_ns.saturating_sub(g.sched_ns));
                            h.0[H_SVC].record(done_ns.saturating_sub(send_ns));
                            sh.head[g.part].store(g.first + g.n, AtOrd::Relaxed);
                            bump(&c.acked_msgs, g.n);
                            bump(&c.acked_batches, 1);
                        }
                        Err(kind) => {
                            count_err(kind, &c.write_err_busy, &c.write_err_other, 1);
                            failed.push(i);
                        }
                    }
                }
            }
            if failed.is_empty() {
                break;
            }
            bump(&c.produce_retries, failed.len() as u64);
            let since = *retry_since.get_or_insert_with(Instant::now);
            if sh.stop_producers.load(AtOrd::Relaxed) && since.elapsed() > Duration::from_secs(30) {
                bump(&c.produce_abandoned, failed.len() as u64);
                break 'run;
            }
            std::thread::sleep(Duration::from_millis((1u64 << attempt.min(7)).min(100)));
            attempt += 1;
            todo = failed;
        }
        if quota > 0 && produced >= quota {
            break;
        }
    }
    sh.producers_done.fetch_add(1, AtOrd::Relaxed);
}

struct ConsumerPart {
    part: usize,
    cursor: u64,
    since_delete: u64,
    deleted_upto: u64,
    failed_keys: Vec<u64>,
}

fn consumer_release(
    sh: &Shared,
    be: &mut dyn QueueBackend,
    hs: &Mutex<HistSet>,
    cp: &mut ConsumerPart,
    consumed: std::ops::Range<u64>,
    final_flush: bool,
) {
    let cfg = &sh.cfg;
    let c = &sh.c;
    let n = consumed.end - consumed.start;
    cp.since_delete += n;
    let keys: Vec<u64>;
    let action = match cfg.delete {
        DeleteMode::None => Release::Ack,
        DeleteMode::Range => {
            let due = if final_flush {
                cp.cursor > cp.deleted_upto
            } else {
                n > 0 && (cfg.delete_every == 0 || cp.since_delete >= cfg.delete_every)
            };
            if due {
                sh.del_attempted[cp.part].fetch_max(cp.cursor, AtOrd::Relaxed);
                Release::Upto(cp.cursor)
            } else {
                Release::Ack
            }
        }
        DeleteMode::PerKey => {
            let mut all = std::mem::take(&mut cp.failed_keys);
            all.extend(consumed.clone());
            keys = all;
            if let Some(max) = keys.iter().max() {
                sh.del_attempted[cp.part].fetch_max(max + 1, AtOrd::Relaxed);
            }
            Release::Keys(&keys)
        }
    };
    let upto = match action {
        Release::Upto(u) => Some(u),
        _ => None,
    };
    let requested = match &action {
        Release::Keys(k) => k.len() as u64,
        _ => 0,
    };
    let t = Instant::now();
    let out = be.release(cp.part, consumed, action);
    let dt = t.elapsed().as_nanos() as u64;
    if out.round_trip {
        hs.lock().unwrap().0[H_DELETE].record(dt);
    }
    if out.ops == 0 && out.round_trip && out.err.is_none() {
        bump(&c.ack_ops, 1);
    }
    bump(&c.delete_ops, out.ops);
    match cfg.delete {
        DeleteMode::None => {}
        DeleteMode::Range => {
            if let Some(u) = upto {
                match out.err {
                    None => {
                        bump(&c.deleted_msgs, u - cp.deleted_upto);
                        cp.deleted_upto = u;
                        cp.since_delete = 0;
                        sh.floor[cp.part].fetch_max(u, AtOrd::Relaxed);
                    }
                    Some(kind) => count_err(kind, &c.delete_err_busy, &c.delete_err_other, 1),
                }
            }
        }
        DeleteMode::PerKey => {
            let failed = out.failed.len() as u64;
            bump(&c.deleted_msgs, requested - failed);
            if let Some(kind) = out.err {
                count_err(kind, &c.delete_err_busy, &c.delete_err_other, failed.max(1));
            }
            cp.failed_keys = out.failed;
            let floor = cp.failed_keys.iter().copied().min().unwrap_or(cp.cursor).min(cp.cursor);
            sh.floor[cp.part].fetch_max(floor, AtOrd::Relaxed);
            cp.since_delete = 0;
        }
    }
}

fn consumer_loop(sh: &Shared, be: &mut dyn QueueBackend, parts: Vec<usize>) {
    let hs = sh.register();
    let cfg = &sh.cfg;
    let c = &sh.c;
    while !sh.consumers_go.load(AtOrd::Relaxed) {
        if sh.stop_consumers.load(AtOrd::Relaxed) {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let mut state: Vec<ConsumerPart> = parts
        .iter()
        .map(|&part| ConsumerPart { part, cursor: 0, since_delete: 0, deleted_upto: 0, failed_keys: Vec::new() })
        .collect();
    let mut seqs: Vec<u64> = Vec::new();
    while !sh.stop_consumers.load(AtOrd::Relaxed) {
        let mut progressed = false;
        for cp in state.iter_mut() {
            let t = Instant::now();
            let res = be.scan(cp.part, cp.cursor, cfg.scan_limit);
            let dt = t.elapsed().as_nanos() as u64;
            bump(&c.scans, 1);
            let entries = match res {
                Ok(e) => e,
                Err(kind) => {
                    count_err(kind, &c.scan_err_busy, &c.scan_err_other, 1);
                    hs.lock().unwrap().0[H_SCAN].record(dt);
                    continue;
                }
            };
            seqs.clear();
            seqs.extend(entries.iter().map(|(s, _)| *s));
            let take = take_prefix(cp.cursor, &seqs);
            bump(&c.dups, take.dups);
            bump(&c.out_of_order, take.out_of_order);
            if take.gap {
                bump(&c.gap_stops, 1);
            }
            if entries.is_empty() {
                bump(&c.empty_scans, 1);
            }
            let now_ns = sh.now_ns();
            let mut bad = 0u64;
            {
                let mut h = hs.lock().unwrap();
                h.0[H_SCAN].record(dt);
                for &i in &take.taken {
                    let (s, v) = &entries[i];
                    match check_msg(v, cfg.vsize, *s) {
                        Some(ts) => h.0[H_E2E].record(now_ns.saturating_sub(ts)),
                        None => bad += 1,
                    }
                }
            }
            if bad > 0 {
                bump(&c.bad_values, bad);
            }
            let taken = take.taken.len() as u64;
            if taken == 0 {
                continue;
            }
            progressed = true;
            let from = cp.cursor;
            cp.cursor += taken;
            sh.cursor[cp.part].store(cp.cursor, AtOrd::Relaxed);
            bump(&c.consumed_msgs, taken);
            consumer_release(sh, be, &hs, cp, from..cp.cursor, false);
        }
        if !progressed {
            std::thread::sleep(Duration::from_micros(cfg.poll_us));
        }
    }
    for cp in state.iter_mut() {
        let needs = match cfg.delete {
            DeleteMode::Range => cp.cursor > cp.deleted_upto,
            DeleteMode::PerKey => !cp.failed_keys.is_empty(),
            DeleteMode::None => false,
        };
        if needs {
            let at = cp.cursor;
            consumer_release(sh, be, &hs, cp, at..at, true);
        }
    }
}

fn trimmer_loop(sh: &Shared, be: &mut dyn QueueBackend) {
    let hs = sh.register();
    let cfg = &sh.cfg;
    let c = &sh.c;
    let mut trimmed = vec![0u64; cfg.partitions];
    while !sh.stop_aux.load(AtOrd::Relaxed) {
        nap(&sh.stop_aux, Duration::from_millis(cfg.trim_ms));
        if sh.stop_aux.load(AtOrd::Relaxed) {
            break;
        }
        for (part, done) in trimmed.iter_mut().enumerate() {
            let head = sh.head[part].load(AtOrd::Relaxed);
            if head <= *done + cfg.ring_capacity {
                continue;
            }
            let upto = head - cfg.ring_capacity;
            sh.del_attempted[part].fetch_max(upto, AtOrd::Relaxed);
            let t = Instant::now();
            let r = be.trim(part, upto);
            hs.lock().unwrap().0[H_TRIM].record(t.elapsed().as_nanos() as u64);
            match r {
                Ok(()) => {
                    bump(&c.trim_ops, 1);
                    bump(&c.trimmed_msgs, upto - *done);
                    *done = upto;
                    sh.floor[part].fetch_max(upto, AtOrd::Relaxed);
                }
                Err(kind) => count_err(kind, &c.trim_err_busy, &c.trim_err_other, 1),
            }
        }
    }
}

fn probe_parts(cfg: &NqConfig) -> Vec<usize> {
    let mut parts: Vec<usize> = (0..cfg.probe_parts).map(|i| i * cfg.partitions / cfg.probe_parts).collect();
    parts.dedup();
    parts
}

fn probe_loop(sh: &Shared, be: &mut dyn QueueBackend) {
    let hs = sh.register();
    let cfg = &sh.cfg;
    let c = &sh.c;
    let parts = probe_parts(cfg);
    while !sh.stop_aux.load(AtOrd::Relaxed) {
        nap(&sh.stop_aux, Duration::from_millis(cfg.probe_ms));
        if sh.stop_aux.load(AtOrd::Relaxed) {
            break;
        }
        for &part in &parts {
            let floor = sh.floor[part].load(AtOrd::Relaxed);
            let t = Instant::now();
            let r = be.head(part, 10);
            let dt = t.elapsed().as_nanos() as u64;
            match r {
                Ok(seqs) => {
                    hs.lock().unwrap().0[H_PROBE].record(dt);
                    bump(&c.probes, 1);
                    bump(&c.probe_entries, seqs.len() as u64);
                    if let Some(bad) = seqs.iter().find(|s| **s < floor) {
                        bump(&c.probe_violations, 1);
                        eprintln!("netqueue: head probe of partition {part} returned covered seq {bad} (deleted floor {floor})");
                    }
                }
                Err(_) => bump(&c.probe_err, 1),
            }
        }
    }
}

struct Monitor<'a> {
    sh: &'a Shared,
    start: Instant,
    last_sample: Instant,
    last_counts: Vec<u64>,
    interval: HistSet,
    all: HistSet,
    measured: HistSet,
    measure_start: Option<(Instant, Vec<u64>)>,
    measure_end: Option<(Instant, Vec<u64>)>,
    consumers_started: Option<(f64, u64, u64)>,
    drained: Option<(f64, u64)>,
}

impl<'a> Monitor<'a> {
    fn new(sh: &'a Shared) -> Self {
        let now = Instant::now();
        Monitor {
            sh,
            start: now,
            last_sample: now,
            last_counts: sh.c.snapshot(),
            interval: HistSet::new(),
            all: HistSet::new(),
            measured: HistSet::new(),
            measure_start: None,
            measure_end: None,
            consumers_started: None,
            drained: None,
        }
    }

    fn elapsed(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }

    fn harvest(&mut self) {
        let h = self.sh.harvest();
        self.interval.merge(&h);
        self.all.merge(&h);
        if self.measure_start.is_some() && self.measure_end.is_none() {
            self.measured.merge(&h);
        }
    }

    fn poll(&mut self) {
        let sh = self.sh;
        let cfg = &sh.cfg;
        if cfg.consumers > 0 && self.consumers_started.is_none() {
            let backlog = sh.backlog().unwrap_or(0);
            if cfg.start_backlog == 0 || backlog >= cfg.start_backlog {
                sh.consumers_go.store(true, AtOrd::Relaxed);
                let consumed = sh.c.consumed_msgs.load(AtOrd::Relaxed);
                self.consumers_started = Some((self.elapsed(), backlog, consumed));
                if cfg.start_backlog > 0 {
                    println!(
                        "NQ_EVENT {{\"event\":\"consumers_start\",\"elapsed\":{:.3},\"backlog\":{backlog}}}",
                        self.elapsed()
                    );
                }
            }
        }
        if cfg.start_backlog > 0 && self.consumers_started.is_some() && self.drained.is_none() {
            let backlog = sh.backlog().unwrap_or(0);
            if backlog <= cfg.drain_backlog {
                let consumed = sh.c.consumed_msgs.load(AtOrd::Relaxed);
                self.drained = Some((self.elapsed(), consumed));
                println!(
                    "NQ_EVENT {{\"event\":\"drained\",\"elapsed\":{:.3},\"backlog\":{backlog}}}",
                    self.elapsed()
                );
            }
        }
        if self.measure_start.is_none() && self.elapsed() >= cfg.warmup_secs {
            self.harvest();
            self.measure_start = Some((Instant::now(), sh.c.snapshot()));
        }
        if self.last_sample.elapsed().as_secs_f64() >= cfg.sample_secs {
            self.sample();
        }
    }

    fn phase(&self) -> &'static str {
        if self.measure_end.is_some() {
            "drain"
        } else if self.measure_start.is_some() {
            "measure"
        } else {
            "warmup"
        }
    }

    fn sample(&mut self) {
        self.harvest();
        let interval = self.last_sample.elapsed().as_secs_f64();
        self.last_sample = Instant::now();
        let now = self.sh.c.snapshot();
        let delta: Vec<u64> = now.iter().zip(&self.last_counts).map(|(a, b)| a.saturating_sub(*b)).collect();
        let per_s = |name: &str| delta[counter_index(name)] as f64 / interval.max(1e-9);
        println!(
            "NQ_SAMPLE {{\"elapsed\":{:.3},\"interval\":{:.3},\"phase\":\"{}\",\"backlog\":{},\"live\":{},\"rates\":{{\"acked_msgs\":{:.0},\"consumed_msgs\":{:.0},\"deleted_msgs\":{:.0}}},\"delta\":{},\"total\":{},\"lat_us\":{}}}",
            self.elapsed(),
            interval,
            self.phase(),
            self.sh.backlog().map_or("null".to_string(), |b| b.to_string()),
            self.sh.live(),
            per_s("acked_msgs"),
            per_s("consumed_msgs"),
            per_s("deleted_msgs"),
            counts_json(&delta),
            counts_json(&now),
            self.interval.json()
        );
        self.interval.reset();
        self.last_counts = now;
    }

    fn end_measure(&mut self) {
        self.harvest();
        if self.measure_start.is_none() {
            self.measure_start = Some((self.start, vec![0; COUNTER_NAMES.len()]));
            self.measured = self.all.clone();
        }
        self.measure_end = Some((Instant::now(), self.sh.c.snapshot()));
    }
}

fn run_threads(sh: &Arc<Shared>, target: &Target) -> Vec<(&'static str, std::thread::JoinHandle<()>)> {
    let cfg = &sh.cfg;
    let mut handles = Vec::new();
    let mut role = 0u32;
    let total = cfg.total;
    for i in 0..cfg.producers {
        let parts: Vec<usize> = (0..cfg.partitions).filter(|p| p % cfg.producers == i).collect();
        let quota = if total == 0 {
            0
        } else {
            total / cfg.producers as u64 + u64::from((i as u64) < total % cfg.producers as u64)
        };
        let (sh, target) = (sh.clone(), target.clone());
        role += 1;
        let id = role;
        handles.push((
            "producer",
            std::thread::spawn(move || {
                let mut be = target.connect(&sh.cfg, id, false);
                if total > 0 && quota == 0 {
                    sh.producers_done.fetch_add(1, AtOrd::Relaxed);
                    return;
                }
                producer_loop(&sh, be.as_mut(), parts, quota);
            }),
        ));
    }
    for i in 0..cfg.consumers {
        let parts: Vec<usize> = (0..cfg.partitions).filter(|p| p % cfg.consumers == i).collect();
        let (sh, target) = (sh.clone(), target.clone());
        role += 1;
        let id = role;
        handles.push((
            "consumer",
            std::thread::spawn(move || {
                let mut be = target.connect(&sh.cfg, id, true);
                consumer_loop(&sh, be.as_mut(), parts);
            }),
        ));
    }
    if cfg.ring_capacity > 0 {
        let (sh, target) = (sh.clone(), target.clone());
        role += 1;
        let id = role;
        handles.push((
            "aux",
            std::thread::spawn(move || {
                let mut be = target.connect(&sh.cfg, id, true);
                trimmer_loop(&sh, be.as_mut());
            }),
        ));
    }
    if cfg.probe_ms > 0 {
        let (sh, target) = (sh.clone(), target.clone());
        role += 1;
        let id = role;
        handles.push((
            "aux",
            std::thread::spawn(move || {
                let mut be = target.connect(&sh.cfg, id, false);
                probe_loop(&sh, be.as_mut());
            }),
        ));
    }
    handles
}

fn join_role(handles: &mut Vec<(&'static str, std::thread::JoinHandle<()>)>, role: &str) {
    let mut keep = Vec::new();
    for (r, h) in handles.drain(..) {
        if r == role {
            if h.join().is_err() {
                eprintln!("netqueue: a {role} thread panicked");
            }
        } else {
            keep.push((r, h));
        }
    }
    *handles = keep;
}

#[derive(Default)]
struct PartCheck {
    live: u64,
    covered_visible: u64,
    partial_delete_visible: u64,
    missing: u64,
    unacked_visible: u64,
    unexpected: u64,
    disorder: u64,
    bad_values: u64,
}

fn check_partition(
    be: &mut dyn QueueBackend,
    part: usize,
    vsize: usize,
    floor: u64,
    del_attempted: u64,
    head: u64,
    attempted: u64,
) -> Result<PartCheck, String> {
    let mut pc = PartCheck::default();
    let mut prev: Option<u64> = None;
    let mut present_required = 0u64;
    be.verify_scan(part, &mut |s, v| {
        pc.live += 1;
        if prev.is_some_and(|p| s <= p) {
            pc.disorder += 1;
        }
        prev = Some(s);
        if v.is_none_or(|v| check_msg(v, vsize, s).is_none()) {
            pc.bad_values += 1;
        }
        if s < floor {
            pc.covered_visible += 1;
        } else if s < del_attempted {
            pc.partial_delete_visible += 1;
        } else if s < head {
            present_required += 1;
        } else if s < attempted {
            pc.unacked_visible += 1;
        } else {
            pc.unexpected += 1;
        }
    })?;
    pc.missing = head.saturating_sub(del_attempted).saturating_sub(present_required);
    Ok(pc)
}

fn verify_all(sh: &Shared, be: &mut dyn QueueBackend) -> (bool, String) {
    let cfg = &sh.cfg;
    let mut sum = PartCheck::default();
    let mut errors: Vec<String> = Vec::new();
    let mut count_mismatch = 0u64;
    let mut failing_parts = Vec::new();
    let mut expected_live = 0u64;
    for part in 0..cfg.partitions {
        let floor = sh.floor[part].load(AtOrd::Relaxed);
        let head = sh.head[part].load(AtOrd::Relaxed);
        let del_attempted = sh.del_attempted[part].load(AtOrd::Relaxed).max(floor).min(head.max(floor));
        let attempted = sh.attempted[part].load(AtOrd::Relaxed).max(head);
        expected_live += head.saturating_sub(floor);
        match check_partition(be, part, cfg.vsize, floor, del_attempted, head, attempted) {
            Ok(pc) => {
                if let Some(n) = be.live_count(part) {
                    if n != pc.live {
                        count_mismatch += 1;
                    }
                }
                let bad = pc.covered_visible + pc.missing + pc.unexpected + pc.disorder + pc.bad_values;
                if bad > 0 && failing_parts.len() < 16 {
                    failing_parts.push(format!(
                        "{{\"part\":{part},\"floor\":{floor},\"del_attempted\":{del_attempted},\"head\":{head},\"attempted\":{attempted},\"live\":{},\"covered_visible\":{},\"missing\":{},\"unexpected\":{},\"disorder\":{},\"bad_values\":{}}}",
                        pc.live, pc.covered_visible, pc.missing, pc.unexpected, pc.disorder, pc.bad_values
                    ));
                }
                sum.live += pc.live;
                sum.covered_visible += pc.covered_visible;
                sum.partial_delete_visible += pc.partial_delete_visible;
                sum.missing += pc.missing;
                sum.unacked_visible += pc.unacked_visible;
                sum.unexpected += pc.unexpected;
                sum.disorder += pc.disorder;
                sum.bad_values += pc.bad_values;
            }
            Err(e) => errors.push(format!("partition {part}: {e}")),
        }
    }
    let ok = errors.is_empty()
        && count_mismatch == 0
        && sum.covered_visible == 0
        && sum.missing == 0
        && sum.unexpected == 0
        && sum.disorder == 0
        && sum.bad_values == 0;
    let errs: Vec<String> = errors.iter().take(16).map(|e| json_str(e)).collect();
    let json = format!(
        "{{\"ok\":{ok},\"partitions\":{},\"expected_live\":{expected_live},\"live\":{},\"covered_visible\":{},\"partial_delete_visible\":{},\"missing\":{},\"unacked_visible\":{},\"unexpected\":{},\"disorder\":{},\"bad_values\":{},\"count_mismatch\":{count_mismatch},\"scan_errors\":[{}],\"failing\":[{}]}}",
        cfg.partitions,
        sum.live,
        sum.covered_visible,
        sum.partial_delete_visible,
        sum.missing,
        sum.unacked_visible,
        sum.unexpected,
        sum.disorder,
        sum.bad_values,
        errs.join(","),
        failing_parts.join(",")
    );
    (ok, json)
}

fn resolve_vals_zone(nodes: &[(u32, Vec<SocketAddr>)], cfg: &NqConfig, name: &str) -> vals_cluster::ZoneId {
    use vals_cluster::reactor;
    let coord = vals_coordinator(nodes, cfg, 3_000_000 + cfg.client_id * 10_000 + 9_999);
    let mut zone = None;
    let found = poll_until(Duration::from_secs(30), Duration::from_millis(250), || {
        let from = coord.ring().members().next();
        if let Some(from) = from {
            let _ = vals_rt::block_on(coord.refresh_catalog(from), reactor::poll_io);
        }
        zone = coord.catalog().by_name(name);
        zone.is_some()
    });
    let Some(id) = zone.filter(|_| found) else {
        eprintln!("netqueue: zone {name:?} not found in the catalog (create it on the PD first)");
        std::process::exit(2);
    };
    let sorted = coord
        .catalog()
        .get(id)
        .is_some_and(|z| z.ordering == vals_cluster::Ordering::Sorted);
    if !sorted {
        eprintln!("netqueue: zone {name:?} (id {id}) is not a sorted zone");
        std::process::exit(2);
    }
    id
}

pub(super) fn workload_netqueue(backend: &str, spec: Option<&str>, zone: Option<&str>) {
    let usage = "usage: vals-bench vals netqueue <id@ip:port,...|id@a1,a2;id@...> <zone>\n       vals-bench redis netqueue <host:port[,host:port...]>";
    let Some(spec) = spec else {
        eprintln!("{usage}");
        std::process::exit(2);
    };
    let cfg = NqConfig::from_env();
    let target = match backend {
        "vals" => {
            let Some(zone_name) = zone else {
                eprintln!("{usage}");
                std::process::exit(2);
            };
            let nodes = parse_peers(spec);
            let zone = resolve_vals_zone(&nodes, &cfg, zone_name);
            println!("netqueue: zone {zone_name:?} resolved to id {zone}");
            Target::Vals { nodes, zone }
        }
        "redis" => Target::Redis {
            endpoints: spec.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect(),
        },
        other => {
            eprintln!("netqueue backend must be vals or redis, got {other}\n{usage}");
            std::process::exit(2);
        }
    };
    println!(
        "NQ_CONFIG {{\"backend\":\"{}\",\"spec\":{},\"zone\":{},\"cfg\":{}}}",
        target.name(),
        json_str(spec),
        zone.map_or("null".to_string(), json_str),
        cfg.json()
    );
    {
        let mut be = target.connect(&cfg, 9_998, false);
        if let Err(e) = be.prepare(cfg.partitions, cfg.consumers > 0) {
            eprintln!("netqueue: prepare failed: {e}");
            std::process::exit(2);
        }
    }

    let sh = Arc::new(Shared::new(cfg.clone()));
    let mut handles = run_threads(&sh, &target);
    let mut mon = Monitor::new(&sh);
    loop {
        mon.poll();
        let done = if cfg.total > 0 {
            sh.producers_done.load(AtOrd::Relaxed) >= cfg.producers
        } else {
            mon.elapsed() >= cfg.warmup_secs + cfg.secs
        };
        if done {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    mon.sample();
    mon.end_measure();
    sh.stop_producers.store(true, AtOrd::Relaxed);
    join_role(&mut handles, "producer");
    let drain_start = Instant::now();
    let mut drained_all = cfg.consumers == 0;
    if cfg.verify && cfg.consumers > 0 {
        sh.consumers_go.store(true, AtOrd::Relaxed);
        while drain_start.elapsed().as_secs_f64() < cfg.drain_wait_secs {
            mon.poll();
            if sh.backlog() == Some(0) {
                drained_all = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    let drain_secs = drain_start.elapsed().as_secs_f64();
    sh.stop_consumers.store(true, AtOrd::Relaxed);
    join_role(&mut handles, "consumer");
    sh.stop_aux.store(true, AtOrd::Relaxed);
    join_role(&mut handles, "aux");
    mon.sample();

    let (m0, c0) = mon.measure_start.clone().expect("measure start");
    let (m1, c1) = mon.measure_end.clone().expect("measure end");
    let secs = m1.saturating_duration_since(m0).as_secs_f64().max(1e-9);
    let measured: Vec<u64> = c1.iter().zip(&c0).map(|(a, b)| a.saturating_sub(*b)).collect();
    let totals = sh.c.snapshot();
    let rate = |name: &str| measured[counter_index(name)] as f64 / secs;
    let burst = match (cfg.start_backlog > 0, mon.consumers_started) {
        (true, Some((at, backlog, consumed0))) => format!(
            "{{\"consumers_start_s\":{at:.3},\"backlog_at_start\":{backlog},\"drained_s\":{},\"drain_secs\":{},\"drain_consumed\":{},\"drain_msgs_per_s\":{},\"drain_threshold\":{}}}",
            mon.drained.map_or("null".to_string(), |(d, _)| format!("{d:.3}")),
            mon.drained.map_or("null".to_string(), |(d, _)| format!("{:.3}", d - at)),
            mon.drained.map_or("null".to_string(), |(_, c)| (c - consumed0).to_string()),
            mon.drained.map_or("null".to_string(), |(d, c)| format!("{:.0}", (c - consumed0) as f64 / (d - at).max(1e-9))),
            cfg.drain_backlog
        ),
        (true, None) => "{\"consumers_start_s\":null}".to_string(),
        _ => "null".to_string(),
    };
    println!(
        "NQ_SUMMARY {{\"backend\":\"{}\",\"mode\":\"{}\",\"ring_capacity\":{},\"measured_secs\":{secs:.3},\"wall_secs\":{:.3},\"rates\":{{\"produced_msgs\":{:.0},\"acked_msgs\":{:.0},\"consumed_msgs\":{:.0},\"deleted_msgs\":{:.0},\"delete_ops\":{:.1},\"trimmed_msgs\":{:.0},\"trim_ops\":{:.1}}},\"measured\":{},\"total\":{},\"backlog_end\":{},\"live_end\":{},\"final_drain\":{{\"drained\":{drained_all},\"secs\":{drain_secs:.3}}},\"burst\":{burst},\"lat_us\":{},\"lat_us_all\":{}}}",
        target.name(),
        cfg.delete.name(),
        cfg.ring_capacity,
        mon.elapsed(),
        rate("produced_msgs"),
        rate("acked_msgs"),
        rate("consumed_msgs"),
        rate("deleted_msgs"),
        rate("delete_ops"),
        rate("trimmed_msgs"),
        rate("trim_ops"),
        counts_json(&measured),
        counts_json(&totals),
        sh.backlog().map_or("null".to_string(), |b| b.to_string()),
        sh.live(),
        mon.measured.json(),
        mon.all.json()
    );

    let correctness_counters = ["dups", "out_of_order", "bad_values", "probe_violations"];
    let mut ok = correctness_counters.iter().all(|n| totals[counter_index(n)] == 0);
    if !ok {
        eprintln!("netqueue: correctness counters non-zero during the run");
    }
    if cfg.verify {
        let mut be = target.connect(&cfg, 9_997, false);
        let (vok, json) = verify_all(&sh, be.as_mut());
        println!("NQ_VERIFY {json}");
        ok &= vok;
    }
    if !ok {
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn netqueue_hist_buckets_are_contiguous_and_bounded() {
        let mut prev_high = None;
        for i in 0..HIST_BUCKETS {
            let high = Hist::bucket_high(i);
            assert_eq!(Hist::bucket(high), i, "bucket {i}");
            if let Some(p) = prev_high {
                assert_eq!(Hist::bucket(p + 1), i, "bucket {i} starts right after {p}");
            }
            prev_high = (high != u64::MAX).then_some(high);
        }
        assert_eq!(Hist::bucket(u64::MAX), HIST_BUCKETS - 1);
        for v in [0u64, 1, 63, 64, 65, 1000, 123_456, 9_999_999_999] {
            let high = Hist::bucket_high(Hist::bucket(v));
            assert!(high >= v);
            assert!(high - v <= v / (HIST_SUB as u64 / 2) + 1, "{v} -> {high}");
        }
    }

    #[test]
    fn netqueue_hist_percentiles() {
        let mut h = Hist::new();
        assert_eq!(h.quantile_permille(500), 0);
        for v in 1..=1000u64 {
            h.record(v * 1000);
        }
        let near = |got: u64, want: u64| {
            let err = (got as f64 - want as f64).abs() / want as f64;
            assert!(err < 0.02, "got {got}, want ~{want}");
        };
        near(h.quantile_permille(500), 500_000);
        near(h.quantile_permille(900), 900_000);
        near(h.quantile_permille(990), 990_000);
        near(h.quantile_permille(999), 999_000);
        assert_eq!(h.quantile_permille(1000), 1_000_000);
        assert_eq!(h.max, 1_000_000);

        let mut small = Hist::new();
        for v in [5u64, 7, 9, 11] {
            small.record(v);
        }
        assert_eq!(small.quantile_permille(500), 7);
        assert_eq!(small.quantile_permille(999), 11);

        let mut merged = Hist::new();
        merged.merge(&small);
        merged.merge(&small);
        assert_eq!(merged.count, 8);
        assert_eq!(merged.quantile_permille(500), 7);
        merged.reset();
        assert_eq!(merged.count, 0);
        assert_eq!(merged.quantile_permille(500), 0);
        assert!(small.json().contains("\"n\":4"));
    }

    #[test]
    fn netqueue_contiguous_prefix_stops_at_gap() {
        let t = take_prefix(10, &[10, 11, 12]);
        assert_eq!(t.taken, vec![0, 1, 2]);
        assert!(!t.gap);

        let t = take_prefix(10, &[10, 11, 13, 14]);
        assert_eq!(t.taken, vec![0, 1]);
        assert!(t.gap);

        let t = take_prefix(10, &[12, 13]);
        assert!(t.taken.is_empty());
        assert!(t.gap);

        let t = take_prefix(10, &[8, 9, 10, 11]);
        assert_eq!(t.taken, vec![2, 3]);
        assert_eq!(t.dups, 2);
        assert!(!t.gap);

        let t = take_prefix(0, &[0, 1, 1, 2]);
        assert_eq!(t.taken, vec![0, 1, 3]);
        assert_eq!(t.dups, 1);
        assert_eq!(t.out_of_order, 0);

        let t = take_prefix(0, &[0, 2, 1]);
        assert_eq!(t.taken, vec![0]);
        assert!(t.gap);

        let t = take_prefix(0, &[0, 1, 0]);
        assert_eq!(t.out_of_order, 1);
        assert_eq!(t.dups, 1);
        assert_eq!(take_prefix(5, &[]), PrefixTake::default());
    }

    #[test]
    fn netqueue_messages_and_keys_round_trip() {
        let m = make_msg(64, 123_456, 42);
        assert_eq!(m.len(), 64);
        assert_eq!(check_msg(&m, 64, 42), Some(123_456));
        assert_eq!(check_msg(&m, 64, 43), None);
        assert_eq!(check_msg(&m, 65, 42), None);
        let mut bad = m.clone();
        bad[40] ^= 1;
        assert_eq!(check_msg(&bad, 64, 42), None);
        assert_eq!(check_msg(&make_msg(16, 1, 2), 16, 2), Some(1));

        let k = vals_key("", 7, 99);
        assert_eq!(k, b"p0007/0000000000000099");
        assert_eq!(vals_key_seq(&k), Some(99));
        assert_eq!(vals_key_seq(&vals_key("run1/", 7, 5)), Some(5));
        assert!(vals_part_lo("", 7).as_slice() < k.as_slice());
        assert!(k.as_slice() < vals_part_hi("", 7).as_slice());
        assert!(vals_key("", 7, u64::MAX).as_slice() < vals_part_hi("", 7).as_slice());
        assert!(vals_key("", 7, 9) < vals_key("", 7, 10));
        assert_eq!(vals_key_seq(b"p0007/xyz"), None);

        assert_eq!(stream_id(0), "0-1");
        assert_eq!(stream_id_seq(b"0-1"), Some(0));
        assert_eq!(stream_id_seq(stream_id(77).as_bytes()), Some(77));
        assert_eq!(stream_id_seq(b"0-0"), None);
        assert_eq!(stream_id_seq(b"1-5"), None);
    }

    #[test]
    fn netqueue_peer_specs() {
        let p = parse_peers("1@127.0.0.1:7001,2@127.0.0.1:7002");
        assert_eq!(p.len(), 2);
        assert_eq!(p[1].0, 2);
        let p = parse_peers("1@10.0.0.1:7000,10.0.0.1:7001;2@10.0.0.2:7000");
        assert_eq!(p.len(), 2);
        assert_eq!(p[0].1.len(), 2);
        assert_eq!(p[1].1.len(), 1);
    }

    #[test]
    fn netqueue_resp_encode() {
        let mut buf = Vec::new();
        resp_encode(&mut buf, &[b"XADD", b"q:0001", b"0-1", b"v", b""]);
        assert_eq!(buf, b"*5\r\n$4\r\nXADD\r\n$6\r\nq:0001\r\n$3\r\n0-1\r\n$1\r\nv\r\n$0\r\n\r\n");
    }

    #[test]
    fn netqueue_resp_parse() {
        let wire = b"+OK\r\n-ERR boom\r\n:42\r\n$-1\r\n*-1\r\n$5\r\nab\r\nc\r\n*2\r\n:1\r\n*1\r\n$0\r\n\r\n";
        let mut r = std::io::Cursor::new(&wire[..]);
        assert_eq!(resp_read(&mut r).unwrap(), Resp::Simple("OK".into()));
        assert_eq!(resp_read(&mut r).unwrap(), Resp::Error("ERR boom".into()));
        assert_eq!(resp_read(&mut r).unwrap(), Resp::Int(42));
        assert_eq!(resp_read(&mut r).unwrap(), Resp::Bulk(None));
        assert_eq!(resp_read(&mut r).unwrap(), Resp::Array(None));
        assert_eq!(resp_read(&mut r).unwrap(), Resp::Bulk(Some(b"ab\r\nc".to_vec())));
        assert_eq!(
            resp_read(&mut r).unwrap(),
            Resp::Array(Some(vec![Resp::Int(1), Resp::Array(Some(vec![Resp::Bulk(Some(Vec::new()))]))]))
        );
        assert!(resp_read(&mut r).is_err());
        let mut trunc = std::io::Cursor::new(&b"$5\r\nab"[..]);
        assert!(resp_read(&mut trunc).is_err());
        let mut junk = std::io::Cursor::new(&b"?x\r\n"[..]);
        assert!(resp_read(&mut junk).is_err());
    }

    #[test]
    fn netqueue_resp_stream_replies() {
        let bulk = |b: &[u8]| Resp::Bulk(Some(b.to_vec()));
        let entry = |id: &[u8], v: &[u8]| Resp::Array(Some(vec![bulk(id), Resp::Array(Some(vec![bulk(b"v"), bulk(v)]))]));
        let deleted = Resp::Array(Some(vec![bulk(b"0-9"), Resp::Array(None)]));
        let reply = Resp::Array(Some(vec![Resp::Array(Some(vec![
            bulk(b"q:0003"),
            Resp::Array(Some(vec![entry(b"0-1", b"a"), entry(b"0-2", b"b"), deleted])),
        ]))]));
        let got = redis_read_entries(&reply);
        assert_eq!(got, vec![(0, Some(b"a".to_vec())), (1, Some(b"b".to_vec())), (8, None)]);
        assert!(redis_read_entries(&Resp::Array(None)).is_empty());
        assert!(redis_read_entries(&Resp::Bulk(None)).is_empty());
        let range = Resp::Array(Some(vec![entry(b"0-5", b"x")]));
        assert_eq!(redis_entries(&range), vec![(4, Some(b"x".to_vec()))]);

        let mut wire = Vec::new();
        wire.extend_from_slice(b"*1\r\n*2\r\n$6\r\nq:0003\r\n*1\r\n*2\r\n$3\r\n0-3\r\n*2\r\n$1\r\nv\r\n$2\r\nhi\r\n");
        let parsed = resp_read(&mut std::io::Cursor::new(&wire[..])).unwrap();
        assert_eq!(redis_read_entries(&parsed), vec![(2, Some(b"hi".to_vec()))]);
        assert_eq!(redis_err_kind("BUSY Redis is busy running a script"), ErrKind::Busy);
        assert_eq!(redis_err_kind("BUSYGROUP Consumer Group name already exists"), ErrKind::Other);
        assert_eq!(redis_err_kind("ERR nope"), ErrKind::Other);
    }
}

use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NetClientMode {
    FiniteDefault,
    ExplicitUnbounded,
}

impl NetClientMode {
    fn from_env() -> Self {
        if std::env::var_os("VALS_BENCH_BOUNDED_CLIENT").is_some() {
            Self::FiniteDefault
        } else {
            Self::ExplicitUnbounded
        }
    }

    fn connect_sharded(
        self,
        nodes: impl IntoIterator<Item = (u32, Vec<std::net::SocketAddr>)>,
    ) -> vals_cluster::NetTransport {
        match self {
            Self::FiniteDefault => vals_cluster::NetTransport::connect_sharded(nodes),
            Self::ExplicitUnbounded => {
                vals_cluster::NetTransport::connect_sharded_unbounded(nodes)
            }
        }
    }

    fn print_config(self) {
        match self {
            Self::FiniteDefault => {
                let config = vals_cluster::ClientConfig::default();
                let budgets = config.budgets.expect("default client budgets");
                println!(
                    "  client mode       : finite-default connect_deadline_ms={} rpc_deadline_ms={} logical+transport_limits={{global_ops:{},global_bytes:{},node_ops:{},node_bytes:{}}}",
                    config.connect_deadline.unwrap().as_millis(),
                    config.rpc_deadline.unwrap().as_millis(),
                    budgets.max_ops,
                    budgets.max_bytes,
                    budgets.node_max_ops,
                    budgets.node_max_bytes
                );
            }
            Self::ExplicitUnbounded => println!(
                "  client mode       : explicit-unbounded connect_deadline=none rpc_deadline=none logical_limits=none transport_limits=none"
            ),
        }
    }
}

fn netread_fanout(value: Option<&str>) -> vals_cluster::ReadFanout {
    match value.unwrap_or("digest") {
        "digest" => vals_cluster::ReadFanout::Digest,
        "quorum" => vals_cluster::ReadFanout::Quorum,
        "all" => vals_cluster::ReadFanout::All,
        other => panic!("VALS_NETREAD_FANOUT must be digest, quorum, or all, got {other}"),
    }
}

fn netread_fanout_name(fanout: vals_cluster::ReadFanout) -> &'static str {
    match fanout {
        vals_cluster::ReadFanout::Digest => "digest",
        vals_cluster::ReadFanout::Quorum => "quorum",
        vals_cluster::ReadFanout::All => "all",
    }
}

pub(super) fn workload_netload(
    peers_arg: &str,
    n: usize,
    vsize: usize,
    concurrency: usize,
    rf: usize,
    ncoord: usize,
    do_write: bool,
    read_batch_width: usize,
    write_batch_width: usize,
    write_persistence: vals_cluster::Persistence,
) {
    use std::net::SocketAddr;
    use std::sync::{Arc, Barrier};
    use vals_cluster::{Coordinator, Ring};

    let node_shards: Vec<(u32, Vec<SocketAddr>)> = peers_arg
        .split(';')
        .filter(|s| !s.trim().is_empty())
        .map(|s| {
            let (id, addrs) = s.split_once('@').expect("peer must be id@addr[,addr...]");
            let addrs: Vec<SocketAddr> = addrs
                .split(',')
                .filter(|a| !a.trim().is_empty())
                .map(|a| a.trim().parse().expect("peer addr (host:port)"))
                .collect();
            (id.trim().parse().expect("peer id"), addrs)
        })
        .collect();
    assert!(
        node_shards.len() >= rf,
        "need at least RF={rf} nodes, got {}",
        node_shards.len()
    );
    let r = rf / 2 + 1;
    let ncoord = ncoord.max(1);
    let per = n / ncoord;
    let lat_sample_every = std::env::var("VALS_NETREAD_LAT_SAMPLE_EVERY")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|n| *n != 0)
        .unwrap_or(0);
    let write_lat_sample_every = std::env::var("VALS_NETWRITE_LAT_SAMPLE_EVERY")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|n| *n != 0)
        .unwrap_or(0);
    let do_read = !do_write || std::env::var_os("VALS_NETLOAD_SKIP_READ_PHASE").is_none();
    let client_mode = NetClientMode::from_env();
    let read_fanout = netread_fanout(std::env::var("VALS_NETREAD_FANOUT").ok().as_deref());
    let key_prefix = std::env::var("VALS_NETLOAD_KEY_PREFIX").unwrap_or_else(|_| "nl".into());
    let write_secs: f64 = std::env::var("VALS_NETWRITE_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);
    let write_rate: f64 = std::env::var("VALS_NETWRITE_RATE")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|rate| *rate > 0.0)
        .unwrap_or(0.0);
    let nshards: usize = node_shards.iter().map(|(_, a)| a.len()).max().unwrap_or(1);
    let read_batch_width = read_batch_width.max(1);
    let write_batch_width = write_batch_width.max(1);
    assert!(
        concurrency >= write_batch_width && concurrency.is_multiple_of(write_batch_width),
        "logical concurrency {concurrency} must be a multiple of write batch width {write_batch_width}"
    );
    let write_call_concurrency = concurrency / write_batch_width;
    let persistence_name = match write_persistence {
        vals_cluster::Persistence::Buffered => "buffered",
        vals_cluster::Persistence::Device => "device",
    };
    let phase = if do_write && do_read {
        "write+read".to_string()
    } else if do_write {
        format!("write+sample-verify (batch={write_batch_width}, persistence={persistence_name})")
    } else if read_batch_width == 1 {
        "read-only".to_string()
    } else {
        format!("batched-read-only (batch={read_batch_width})")
    };
    println!(
        "--- netload: {} nodes x {nshards} shards, RF={rf}, W=Quorum({r}), R={r}, read_fanout={}, \
         n={n} (={per}/coord), vsize={vsize}, {ncoord} coords x {concurrency} logical conc ({write_call_concurrency} calls/coord), phase={phase} ---",
        node_shards.len(),
        netread_fanout_name(read_fanout)
    );
    client_mode.print_config();

    let barrier = Arc::new(Barrier::new(ncoord));
    let handles: Vec<_> = (0..ncoord)
        .map(|c| {
            let node_shards = node_shards.clone();
            let barrier = barrier.clone();
            let key_prefix = key_prefix.clone();
            std::thread::spawn(move || {
                netload_thread(
                    c,
                    per,
                    vsize,
                    concurrency,
                    write_call_concurrency,
                    ncoord,
                    rf,
                    r,
                    &node_shards,
                    &barrier,
                    do_write,
                    do_read,
                    read_batch_width,
                    write_batch_width,
                    write_persistence,
                    write_secs,
                    write_rate,
                    &key_prefix,
                    lat_sample_every,
                    write_lat_sample_every,
                    client_mode,
                    read_fanout,
                )
            })
        })
        .collect();
    let mut agg = NetloadStats::default();
    for h in handles {
        agg.merge(h.join().expect("coord thread panicked"));
    }

    let mut ring = Ring::with_vnodes(rf, 128);
    for (id, _) in &node_shards {
        ring.add_node(*id);
    }
    let transport = client_mode.connect_sharded(node_shards.iter().cloned());
    let coord = Coordinator::new(2_000_000, ring, r, transport).with_read_fanout(read_fanout);
    let _ = vals_rt::block_on(
        coord.refresh_ring(node_shards[0].0),
        vals_cluster::reactor::poll_io,
    );
    let val = make_value(vsize, 0);
    let (mut ok, mut bad) = (0usize, 0usize);
    let verify_ranges = if write_secs > 0.0 && do_write {
        agg.w_verify_ranges.clone()
    } else {
        std::iter::once(0..n).collect()
    };
    for i in netwrite_verify_indexes(&verify_ranges, 1000) {
        let key = format!("{key_prefix}-{i:012}").into_bytes();
        match vals_rt::block_on(coord.read(&key), vals_cluster::reactor::poll_io) {
            Ok(Some(s)) if s.value.as_deref() == Some(val.as_slice()) => ok += 1,
            _ => bad += 1,
        }
    }
    agg.receipts
        .push(netload_receipt(ncoord, "verification", &coord));

    if do_write {
        let logical = if write_secs > 0.0 {
            agg.w_ops
        } else {
            per * ncoord
        };
        println!(
            "  write Quorum({r})  : {:.0} logical writes/s, {:.0} replica writes/s, {:.0} calls/s aggregate  ({} logical writes, {} calls, {} errors, batch={write_batch_width}, persistence={persistence_name})",
            logical as f64 / agg.w_secs.max(1e-9),
            logical as f64 * r as f64 / agg.w_secs.max(1e-9),
            agg.w_calls as f64 / agg.w_secs.max(1e-9),
            logical,
            agg.w_calls,
            agg.w_errs
        );
        if std::env::var_os("VALS_NET_TRANSPORT_STATS").is_some() {
            println!(
                "  transport         : pending_peak={} queued_bytes_peak={} write_syscalls={} read_drains={} reconnects={}",
                agg.conn_pending_peak,
                agg.conn_queued_bytes_peak,
                agg.conn_write_calls,
                agg.conn_read_drains,
                agg.conn_reconnects
            );
        }
        if write_lat_sample_every != 0 {
            let samples = std::mem::take(&mut agg.w_ack_lat_ns);
            let lat = netread_latency_summary(samples.clone());
            println!(
                "  write ack latency : {} samples (every {write_lat_sample_every} calls/worker), p50={} p99={} p99.9={} max={} ns",
                lat.count, lat.p50_ns, lat.p99_ns, lat.p999_ns, lat.max_ns
            );
            if let Some(path) = std::env::var_os("VALS_NETWRITE_LAT_SAMPLES_FILE") {
                let body = samples
                    .iter()
                    .map(u64::to_string)
                    .collect::<Vec<_>>()
                    .join("\n");
                std::fs::write(path, format!("{body}\n")).expect("write latency samples");
            }
        }
    }
    if do_read {
        println!(
            "  read  R={r} strong : {:.0} keys/s aggregate  ({} keys, {} batch calls, {:.0} calls/s, {} errors, {} misses, {} mismatches)",
            agg.r_ops as f64 / agg.r_secs.max(1e-9),
            agg.r_ops,
            agg.r_round_trips,
            agg.r_round_trips as f64 / agg.r_secs.max(1e-9),
            agg.r_errs,
            agg.r_miss,
            agg.r_mismatch
        );
        if std::env::var_os("VALS_NET_TRANSPORT_STATS").is_some() {
            println!(
                "  read transport    : pending_peak_cumulative={} queued_bytes_peak_cumulative={} write_syscalls_read_phase={} read_drains_read_phase={}",
                agg.read_conn_pending_peak,
                agg.read_conn_queued_bytes_peak,
                agg.read_conn_write_calls,
                agg.read_conn_read_drains
            );
        }
    }
    if do_read && lat_sample_every != 0 {
        let samples = std::mem::take(&mut agg.r_lat_ns);
        let lat = netread_latency_summary(samples.clone());
        let label = if read_batch_width == 1 {
            "read latency "
        } else {
            "batch latency"
        };
        println!(
            "  {label}      : {} samples (every {lat_sample_every} calls/worker), p50={} p99={} p99.9={} max={} ns",
            lat.count, lat.p50_ns, lat.p99_ns, lat.p999_ns, lat.max_ns
        );
        if let Some(path) = std::env::var_os("VALS_NETREAD_LAT_SAMPLES_FILE") {
            let body = samples
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join("\n");
            std::fs::write(path, format!("{body}\n")).expect("write read latency samples");
        }
        if read_batch_width > 1 {
            let equiv = netread_latency_summary(std::mem::take(
                &mut agg.r_key_equiv_lat_ns,
            ));
            println!(
                "  key-equiv latency  : {} samples (batch ns / requested keys), p50={} p99={} p99.9={} max={} ns",
                equiv.count, equiv.p50_ns, equiv.p99_ns, equiv.p999_ns, equiv.max_ns
            );
        }
    }
    agg.receipts.sort_by_key(|receipt| receipt.coordinator);
    for receipt in &agg.receipts {
        let logical = receipt.logical;
        println!("  coord {} role     : {}", receipt.coordinator, receipt.role);
        println!(
            "  coord {} logical  : terminal_ops={} terminal_bytes={} waiters={} blocked_total={} peak_ops={} limits={{global_ops:{},global_bytes:{},node_ops:{},node_bytes:{}}}",
            receipt.coordinator,
            logical.in_flight_ops,
            logical.in_flight_bytes,
            logical.waiters,
            logical.blocked_total,
            logical.peak_ops,
            logical.max_ops,
            logical.max_bytes,
            logical.node_max_ops,
            logical.node_max_bytes
        );
        match receipt.transport {
            Some(transport) => println!(
                "  coord {} transport: terminal_ops={} terminal_bytes={} waiters={} blocked_total={} peak_ops={} limits={{global_ops:{},global_bytes:{},node_ops:{},node_bytes:{}}}",
                receipt.coordinator,
                transport.in_flight_ops,
                transport.in_flight_bytes,
                transport.waiters,
                transport.blocked_total,
                transport.peak_ops,
                transport.max_ops,
                transport.max_bytes,
                transport.node_max_ops,
                transport.node_max_bytes
            ),
            None => println!(
                "  coord {} transport: unbounded (no credit pool)",
                receipt.coordinator
            ),
        }
        println!(
            "  coord {} conns    : terminal_pending={} terminal_queued_bytes={} pending_peak={} queued_bytes_peak={} current_generation_enqueued_bytes={} current_generation_sent_bytes={} reconnects={} mixed_batch_rpcs={} mixed_scalar_probes={}",
            receipt.coordinator,
            receipt.pending,
            receipt.queued_bytes,
            receipt.pending_peak,
            receipt.queued_bytes_peak,
            receipt.enqueued_bytes,
            receipt.sent_bytes,
            receipt.reconnects,
            receipt.mixed_batch_rpcs,
            receipt.mixed_scalar_probes
        );
        for connection in &receipt.connections {
            println!(
                "  coord {} conn     : node={} shard={} terminal_pending={} terminal_queued_bytes={} pending_peak={} queued_bytes_peak={} current_generation_enqueued_bytes={} current_generation_sent_bytes={} reconnects={} write_syscalls={} read_drains={}",
                receipt.coordinator,
                connection.node,
                connection.shard,
                connection.pending,
                connection.queued_bytes,
                connection.pending_peak,
                connection.queued_bytes_peak,
                connection.enqueued_bytes,
                connection.sent_bytes,
                connection.reconnects,
                connection.write_calls,
                connection.read_drains
            );
        }
    }
    println!(
        "  correctness       : {ok} matched / {bad} mismatched (of {} sampled)",
        ok + bad
    );
}

fn connection_counter_delta(
    before: &[vals_cluster::ConnectionStats],
    after: &[vals_cluster::ConnectionStats],
) -> (u64, u64) {
    let before_writes: u64 = before.iter().map(|stats| stats.write_calls).sum();
    let after_writes: u64 = after.iter().map(|stats| stats.write_calls).sum();
    let before_drains: u64 = before.iter().map(|stats| stats.read_drains).sum();
    let after_drains: u64 = after.iter().map(|stats| stats.read_drains).sum();
    (
        after_writes.saturating_sub(before_writes),
        after_drains.saturating_sub(before_drains),
    )
}

#[derive(Default)]
struct NetloadStats {
    w_secs: f64,
    w_ack_lat_ns: Vec<u64>,
    w_ops: usize,
    w_calls: usize,
    w_verify_ranges: Vec<std::ops::Range<usize>>,
    conn_pending_peak: usize,
    conn_queued_bytes_peak: usize,
    conn_write_calls: u64,
    conn_read_drains: u64,
    conn_reconnects: u64,
    read_conn_pending_peak: usize,
    read_conn_queued_bytes_peak: usize,
    read_conn_write_calls: u64,
    read_conn_read_drains: u64,
    r_secs: f64,
    r_ops: usize,
    r_round_trips: usize,
    w_errs: usize,
    r_errs: usize,
    r_miss: usize,
    r_mismatch: usize,
    r_lat_ns: Vec<u64>,
    r_key_equiv_lat_ns: Vec<u64>,
    receipts: Vec<NetloadReceipt>,
}

struct NetloadReceipt {
    coordinator: usize,
    role: &'static str,
    logical: vals_cluster::CreditStats,
    transport: Option<vals_cluster::CreditStats>,
    pending: usize,
    queued_bytes: usize,
    pending_peak: usize,
    queued_bytes_peak: usize,
    enqueued_bytes: u64,
    sent_bytes: u64,
    reconnects: u64,
    mixed_batch_rpcs: u64,
    mixed_scalar_probes: u64,
    connections: Vec<vals_cluster::ConnectionStats>,
}

impl NetloadStats {
    fn merge(&mut self, mut o: NetloadStats) {
        self.w_secs = self.w_secs.max(o.w_secs);
        self.w_ack_lat_ns.append(&mut o.w_ack_lat_ns);
        self.w_ops += o.w_ops;
        self.w_calls += o.w_calls;
        self.w_verify_ranges.append(&mut o.w_verify_ranges);
        self.conn_pending_peak = self.conn_pending_peak.max(o.conn_pending_peak);
        self.conn_queued_bytes_peak = self.conn_queued_bytes_peak.max(o.conn_queued_bytes_peak);
        self.conn_write_calls += o.conn_write_calls;
        self.conn_read_drains += o.conn_read_drains;
        self.conn_reconnects += o.conn_reconnects;
        self.read_conn_pending_peak = self
            .read_conn_pending_peak
            .max(o.read_conn_pending_peak);
        self.read_conn_queued_bytes_peak = self
            .read_conn_queued_bytes_peak
            .max(o.read_conn_queued_bytes_peak);
        self.read_conn_write_calls += o.read_conn_write_calls;
        self.read_conn_read_drains += o.read_conn_read_drains;
        self.r_secs = self.r_secs.max(o.r_secs);
        self.r_ops += o.r_ops;
        self.r_round_trips += o.r_round_trips;
        self.w_errs += o.w_errs;
        self.r_errs += o.r_errs;
        self.r_miss += o.r_miss;
        self.r_mismatch += o.r_mismatch;
        self.r_lat_ns.append(&mut o.r_lat_ns);
        self.r_key_equiv_lat_ns.append(&mut o.r_key_equiv_lat_ns);
        self.receipts.append(&mut o.receipts);
    }
}

fn netload_receipt(
    coordinator: usize,
    role: &'static str,
    coord: &vals_cluster::Coordinator<vals_cluster::NetTransport>,
) -> NetloadReceipt {
    let connections = coord.transport().connection_stats();
    let (mixed_batch_rpcs, mixed_scalar_probes) = coord.transport().mixed_read_stats();
    NetloadReceipt {
        coordinator,
        role,
        logical: coord.credit_stats(),
        transport: coord.transport().transport_credit_stats(),
        pending: connections.iter().map(|stats| stats.pending).sum(),
        queued_bytes: connections.iter().map(|stats| stats.queued_bytes).sum(),
        pending_peak: connections
            .iter()
            .map(|stats| stats.pending_peak)
            .max()
            .unwrap_or(0),
        queued_bytes_peak: connections
            .iter()
            .map(|stats| stats.queued_bytes_peak)
            .max()
            .unwrap_or(0),
        enqueued_bytes: connections.iter().map(|stats| stats.enqueued_bytes).sum(),
        sent_bytes: connections.iter().map(|stats| stats.sent_bytes).sum(),
        reconnects: connections.iter().map(|stats| stats.reconnects).sum(),
        mixed_batch_rpcs,
        mixed_scalar_probes,
        connections,
    }
}

#[derive(Debug, PartialEq, Eq)]
struct NetreadLatencySummary {
    count: usize,
    p50_ns: u64,
    p99_ns: u64,
    p999_ns: u64,
    max_ns: u64,
}

fn netread_should_sample(every: usize, worker_ops: usize) -> bool {
    every != 0 && worker_ops.is_multiple_of(every)
}

pub(super) fn netmultiread_batch_width(value: Option<&str>) -> usize {
    value
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&width| width != 0)
        .unwrap_or(32)
}

pub(super) fn netbatchwrite_width(value: Option<&str>) -> usize {
    value
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&width| matches!(width, 1 | 8 | 32 | 128 | 512))
        .unwrap_or(1)
}

pub(super) fn netbatchwrite_persistence(value: Option<&str>) -> vals_cluster::Persistence {
    match value.unwrap_or("buffered") {
        "buffered" => vals_cluster::Persistence::Buffered,
        "device" => vals_cluster::Persistence::Device,
        other => panic!("VALS_NETBATCHWRITE_PERSISTENCE must be buffered or device, got {other}"),
    }
}

fn netread_batch_range(next: usize, total: usize, width: usize) -> std::ops::Range<usize> {
    next.min(total)..next.saturating_add(width).min(total)
}

fn netwrite_verify_indexes(
    ranges: &[std::ops::Range<usize>],
    max_samples: usize,
) -> Vec<usize> {
    let total: usize = ranges.iter().map(|range| range.len()).sum();
    if total == 0 || max_samples == 0 {
        return Vec::new();
    }
    let step = total.div_ceil(max_samples).max(1);
    let mut logical = 0usize;
    let mut next = 0usize;
    let mut out = Vec::new();
    for range in ranges {
        for key in range.clone() {
            if logical == next {
                out.push(key);
                next = next.saturating_add(step);
            }
            logical += 1;
        }
    }
    out
}

fn netread_timed_batch(
    total: usize,
    width: usize,
    concurrency: usize,
    worker: usize,
    worker_round_trip: usize,
) -> Vec<usize> {
    if total == 0 {
        return Vec::new();
    }
    let width = width.min(total);
    let base = worker
        .wrapping_add(worker_round_trip.wrapping_mul(concurrency))
        .wrapping_mul(width);
    (0..width)
        .map(|lane| base.wrapping_add(lane) % total)
        .collect()
}

#[derive(Debug, Default, PartialEq, Eq)]
struct NetreadResultCounts {
    hits: usize,
    errors: usize,
    misses: usize,
    mismatches: usize,
}

fn netread_count_result(
    counts: &mut NetreadResultCounts,
    result: Result<Option<&[u8]>, ()>,
    expected: &[u8],
) {
    match result {
        Ok(Some(value)) if value == expected => counts.hits += 1,
        Ok(Some(_)) => counts.mismatches += 1,
        Ok(None) => counts.misses += 1,
        Err(()) => counts.errors += 1,
    }
}

fn netread_latency_summary(mut samples: Vec<u64>) -> NetreadLatencySummary {
    samples.sort_unstable();
    NetreadLatencySummary {
        count: samples.len(),
        p50_ns: percentile(&samples, 50.0),
        p99_ns: percentile(&samples, 99.0),
        p999_ns: percentile(&samples, 99.9),
        max_ns: samples.last().copied().unwrap_or(0),
    }
}

#[allow(clippy::too_many_arguments)]
fn netload_thread(
    c: usize,
    per: usize,
    vsize: usize,
    concurrency: usize,
    write_call_concurrency: usize,
    ncoord: usize,
    rf: usize,
    r: usize,
    node_shards: &[(u32, Vec<std::net::SocketAddr>)],
    barrier: &std::sync::Barrier,
    do_write: bool,
    do_read: bool,
    read_batch_width: usize,
    write_batch_width: usize,
    write_persistence: vals_cluster::Persistence,
    write_secs: f64,
    write_rate: f64,
    key_prefix: &str,
    lat_sample_every: usize,
    write_lat_sample_every: usize,
    client_mode: NetClientMode,
    read_fanout: vals_cluster::ReadFanout,
) -> NetloadStats {
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use vals_cluster::{reactor, Coordinator, Ring, WriteConcern, WriteOptions};
    use vals_rt::LocalExecutor;

    let mut ring = Ring::with_vnodes(rf, 128);
    for (id, _) in node_shards {
        ring.add_node(*id);
    }
    let transport = client_mode.connect_sharded(node_shards.iter().cloned());
    let coord = Rc::new(
        Coordinator::new(1_000_000 + c as u32, ring, r, transport)
            .with_read_fanout(read_fanout),
    );
    let _ = vals_rt::block_on(coord.refresh_ring(node_shards[0].0), reactor::poll_io);

    let base = c * per;
    let key_prefix = Rc::new(key_prefix.to_string());
    let val = make_value(vsize, 0);

    let mut w_secs = 0.0;
    let mut w_ops = 0usize;
    let mut w_calls = 0usize;
    let mut w_verify_ranges = Vec::new();
    let mut w_errs = 0usize;
    let mut w_ack_lat_ns = Vec::new();
    if do_write {
        let werr = Rc::new(Cell::new(0usize));
        let wc = Rc::new(Cell::new(0usize));
        let wops = Rc::new(Cell::new(0usize));
        let wcalls = Rc::new(Cell::new(0usize));
        let wlat = Rc::new(RefCell::new(Vec::new()));
        let wrate_seq = Rc::new(Cell::new(0u64));
        let wrate_start = std::time::Instant::now();
        let ex = LocalExecutor::new();
        for _ in 0..write_call_concurrency {
            let (coord, wc, wops, wcalls, werr, wlat, wrate_seq, val, key_prefix) = (
                coord.clone(),
                wc.clone(),
                wops.clone(),
                wcalls.clone(),
                werr.clone(),
                wlat.clone(),
                wrate_seq.clone(),
                val.clone(),
                key_prefix.clone(),
            );
            ex.spawn(async move {
                let mut worker_calls = 0usize;
                let deadline = (write_secs > 0.0)
                    .then(|| std::time::Instant::now() + std::time::Duration::from_secs_f64(write_secs));
                loop {
                    if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
                        break;
                    }
                    let range = netread_batch_range(wc.get(), per, write_batch_width);
                    if range.is_empty() {
                        if deadline.is_some() {
                            wc.set(0);
                            continue;
                        }
                        break;
                    }
                    wc.set(range.end);
                    worker_calls += 1;
                    wcalls.set(wcalls.get() + 1);
                    let sample = netread_should_sample(
                        write_lat_sample_every,
                        worker_calls,
                    );
                    let keys: Vec<Vec<u8>> = range
                        .map(|i| format!("{key_prefix}-{:012}", base + i).into_bytes())
                        .collect();
                    wops.set(wops.get() + keys.len());
                    if write_rate > 0.0 {
                        let logical = wrate_seq.get().saturating_add(keys.len() as u64);
                        wrate_seq.set(logical);
                        let due = wrate_start + std::time::Duration::from_secs_f64(
                            logical as f64 * ncoord as f64 / write_rate,
                        );
                        if let Some(wait) = due.checked_duration_since(std::time::Instant::now()) {
                            if let Ok(timer) = reactor::Timer::every(wait) {
                                timer.tick().await;
                            }
                        }
                    }
                    let sample_started = sample.then(std::time::Instant::now);
                    let result = if write_batch_width == 1 {
                        coord
                            .write_with(
                                &keys[0],
                                &val,
                                WriteOptions { concern: WriteConcern::Quorum(r), persistence: write_persistence },
                            )
                            .await
                            .map(|_| 0usize)
                    } else {
                        let writes: Vec<(&[u8], &[u8])> = keys
                            .iter()
                            .map(|key| (key.as_slice(), val.as_slice()))
                            .collect();
                        coord
                            .write_batch(
                                &writes,
                                WriteOptions { concern: WriteConcern::Quorum(r), persistence: write_persistence },
                            )
                            .await
                            .map(|result| result.failures().count())
                    };
                    if let Some(started) = sample_started {
                        let elapsed_ns =
                            started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                        wlat.borrow_mut().push(elapsed_ns);
                    }
                    match result {
                        Ok(failures) => werr.set(werr.get() + failures),
                        Err(_) => werr.set(werr.get() + keys.len()),
                    }
                }
            });
        }
        barrier.wait();
        let wstart = std::time::Instant::now();
        ex.run(reactor::poll_io);
        barrier.wait();
        w_secs = wstart.elapsed().as_secs_f64();
        w_ops = wops.get();
        w_calls = wcalls.get();
        let local_high = wc.get();
        if w_ops >= per {
            w_verify_ranges.push(base..base + per);
        } else if local_high != 0 {
            w_verify_ranges.push(base..base + local_high);
        }
        w_errs = werr.get();
        w_ack_lat_ns = std::mem::take(&mut *wlat.borrow_mut());
    }
    let conn_stats = coord.transport().connection_stats();
    let conn_pending_peak = conn_stats.iter().map(|s| s.pending_peak).max().unwrap_or(0);
    let conn_queued_bytes_peak = conn_stats
        .iter()
        .map(|s| s.queued_bytes_peak)
        .max()
        .unwrap_or(0);
    let conn_write_calls = conn_stats.iter().map(|s| s.write_calls).sum();
    let conn_read_drains = conn_stats.iter().map(|s| s.read_drains).sum();
    let conn_reconnects = conn_stats.iter().map(|s| s.reconnects).sum();

    if !do_read {
        return NetloadStats {
            w_secs,
            w_ack_lat_ns,
            w_ops,
            w_calls,
            w_verify_ranges,
            conn_pending_peak,
            conn_queued_bytes_peak,
            conn_write_calls,
            conn_read_drains,
            conn_reconnects,
            w_errs,
            receipts: vec![netload_receipt(c, "worker", &coord)],
            ..NetloadStats::default()
        };
    }

    let read_secs: f64 = std::env::var("VALS_NETREAD_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);
    let rerr = Rc::new(Cell::new(0usize));
    let miss = Rc::new(Cell::new(0usize));
    let mismatch = Rc::new(Cell::new(0usize));
    let rops = Rc::new(Cell::new(0usize));
    let rrounds = Rc::new(Cell::new(0usize));
    let rc = Rc::new(Cell::new(0usize));
    let rlat = Rc::new(RefCell::new(Vec::new()));
    let rkeylat = Rc::new(RefCell::new(Vec::new()));
    let ex = LocalExecutor::new();
    for t in 0..concurrency {
        let coord = coord.clone();
        let rc = rc.clone();
        let rerr = rerr.clone();
        let miss = miss.clone();
        let mismatch = mismatch.clone();
        let rops = rops.clone();
        let rrounds = rrounds.clone();
        let rlat = rlat.clone();
        let rkeylat = rkeylat.clone();
        let expected = (read_batch_width > 1).then(|| val.clone());
        let key_prefix = key_prefix.clone();
        ex.spawn(async move {
            let deadline = (read_secs > 0.0)
                .then(|| std::time::Instant::now() + std::time::Duration::from_secs_f64(read_secs));
            if read_batch_width == 1 {
                let mut j = t;
                let mut worker_ops = 0usize;
                loop {
                    let i = if let Some(dl) = deadline {
                        if std::time::Instant::now() >= dl {
                            break;
                        }
                        let i = j % per;
                        j += concurrency;
                        i
                    } else {
                        let i = rc.get();
                        if i >= per {
                            break;
                        }
                        rc.set(i + 1);
                        i
                    };
                    let sample_started = if lat_sample_every == 0 {
                        None
                    } else {
                        worker_ops += 1;
                        netread_should_sample(lat_sample_every, worker_ops)
                            .then(std::time::Instant::now)
                    };
                    let key = format!("{key_prefix}-{:012}", base + i).into_bytes();
                    let result = coord.read(&key).await;
                    if let Some(started) = sample_started {
                        let elapsed_ns =
                            started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                        rlat.borrow_mut().push(elapsed_ns);
                    }
                    match result {
                        Ok(Some(_)) => {}
                        Ok(None) => miss.set(miss.get() + 1),
                        Err(_) => rerr.set(rerr.get() + 1),
                    }
                    rops.set(rops.get() + 1);
                    rrounds.set(rrounds.get() + 1);
                }
            } else {
                let expected = expected.unwrap();
                let mut worker_round_trips = 0usize;
                loop {
                    let indexes = if let Some(dl) = deadline {
                        if std::time::Instant::now() >= dl {
                            break;
                        }
                        netread_timed_batch(
                            per,
                            read_batch_width,
                            concurrency,
                            t,
                            worker_round_trips,
                        )
                    } else {
                        let range = netread_batch_range(rc.get(), per, read_batch_width);
                        rc.set(range.end);
                        range.collect()
                    };
                    if indexes.is_empty() {
                        break;
                    }
                    worker_round_trips += 1;
                    let keys: Vec<Vec<u8>> = indexes
                        .into_iter()
                        .map(|i| format!("{key_prefix}-{:012}", base + i).into_bytes())
                        .collect();
                    let sample_started = netread_should_sample(
                        lat_sample_every,
                        worker_round_trips,
                    )
                    .then(std::time::Instant::now);
                    let mut counts = NetreadResultCounts::default();
                    match coord.read_many(&keys).await {
                        Ok(results) => {
                            counts.errors += keys.len().saturating_sub(results.len());
                            for result in results.iter().take(keys.len()) {
                                let value = match result {
                                    Ok(Some(stored)) => Ok(stored.value.as_deref()),
                                    Ok(None) => Ok(None),
                                    Err(_) => Err(()),
                                };
                                netread_count_result(&mut counts, value, &expected);
                            }
                        }
                        Err(_) => counts.errors = keys.len(),
                    }
                    if let Some(started) = sample_started {
                        let elapsed_ns =
                            started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                        rlat.borrow_mut().push(elapsed_ns);
                        rkeylat.borrow_mut().push(elapsed_ns / keys.len() as u64);
                    }
                    rerr.set(rerr.get() + counts.errors);
                    miss.set(miss.get() + counts.misses);
                    mismatch.set(mismatch.get() + counts.mismatches);
                    rops.set(rops.get() + keys.len());
                    rrounds.set(rrounds.get() + 1);
                }
            }
        });
    }
    barrier.wait();
    let rstart = std::time::Instant::now();
    ex.run(reactor::poll_io);
    barrier.wait();
    let r_secs = rstart.elapsed().as_secs_f64();
    let r_lat_ns = std::mem::take(&mut *rlat.borrow_mut());
    let r_key_equiv_lat_ns = std::mem::take(&mut *rkeylat.borrow_mut());
    let read_conn_stats = coord.transport().connection_stats();
    let read_conn_pending_peak = read_conn_stats
        .iter()
        .map(|stats| stats.pending_peak)
        .max()
        .unwrap_or(0);
    let read_conn_queued_bytes_peak = read_conn_stats
        .iter()
        .map(|stats| stats.queued_bytes_peak)
        .max()
        .unwrap_or(0);
    let (read_conn_write_calls, read_conn_read_drains) =
        connection_counter_delta(&conn_stats, &read_conn_stats);

    NetloadStats {
        w_secs,
        w_ack_lat_ns,
        w_ops,
        w_calls,
        w_verify_ranges,
        conn_pending_peak,
        conn_queued_bytes_peak,
        conn_write_calls,
        conn_read_drains,
        conn_reconnects,
        read_conn_pending_peak,
        read_conn_queued_bytes_peak,
        read_conn_write_calls,
        read_conn_read_drains,
        r_secs,
        r_ops: rops.get(),
        r_round_trips: rrounds.get(),
        w_errs,
        r_errs: rerr.get(),
        r_miss: miss.get(),
        r_mismatch: mismatch.get(),
        r_lat_ns,
        r_key_equiv_lat_ns,
        receipts: vec![netload_receipt(c, "worker", &coord)],
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn netread_fanout_selection_is_explicit() {
        assert_eq!(netread_fanout(None), vals_cluster::ReadFanout::Digest);
        assert_eq!(
            netread_fanout(Some("digest")),
            vals_cluster::ReadFanout::Digest
        );
        assert_eq!(
            netread_fanout(Some("quorum")),
            vals_cluster::ReadFanout::Quorum
        );
        assert_eq!(netread_fanout(Some("all")), vals_cluster::ReadFanout::All);
    }

    #[test]
    #[should_panic(expected = "must be digest, quorum, or all")]
    fn netread_fanout_rejects_unknown_mode() {
        netread_fanout(Some("one"));
    }

    #[test]
    fn netmultiread_batch_selection_covers_requested_keys() {
        assert_eq!(netmultiread_batch_width(None), 32);
        assert_eq!(netmultiread_batch_width(Some("64")), 64);
        assert_eq!(netmultiread_batch_width(Some("0")), 32);
        assert_eq!(netmultiread_batch_width(Some("bad")), 32);

        assert_eq!(netread_batch_range(0, 10, 4), 0..4);
        assert_eq!(netread_batch_range(8, 10, 4), 8..10);
        assert_eq!(netread_batch_range(10, 10, 4), 10..10);
        assert_eq!(netread_timed_batch(20, 3, 2, 0, 0), vec![0, 1, 2]);
        assert_eq!(netread_timed_batch(20, 3, 2, 1, 0), vec![3, 4, 5]);
        assert_eq!(netread_timed_batch(20, 3, 2, 0, 1), vec![6, 7, 8]);
        assert_eq!(netread_timed_batch(4, 4, 2, 0, 0), vec![0, 1, 2, 3]);
        assert_eq!(netread_timed_batch(2, 8, 1, 0, 0), vec![0, 1]);
        assert!(netread_timed_batch(0, 8, 1, 0, 0).is_empty());
    }

    #[test]
    fn timed_write_verification_never_selects_coordinator_holes() {
        let ranges = vec![0..3, 10..12, 20..21];
        let selected = netwrite_verify_indexes(&ranges, 1000);
        assert_eq!(selected, vec![0, 1, 2, 10, 11, 20]);
        assert!(!selected.iter().any(|key| (3..10).contains(key) || (12..20).contains(key)));
        assert_eq!(netwrite_verify_indexes(&ranges, 3), vec![0, 2, 11]);
        assert!(netwrite_verify_indexes(&[], 1000).is_empty());
    }

    #[test]
    fn netbatchwrite_options_are_bounded_and_explicit() {
        assert_eq!(netbatchwrite_width(None), 1);
        for width in [1, 8, 32, 128, 512] {
            assert_eq!(netbatchwrite_width(Some(&width.to_string())), width);
        }
        assert_eq!(netbatchwrite_width(Some("0")), 1);
        assert_eq!(netbatchwrite_width(Some("64")), 1);
        assert_eq!(
            netbatchwrite_persistence(None),
            vals_cluster::Persistence::Buffered
        );
        assert_eq!(
            netbatchwrite_persistence(Some("device")),
            vals_cluster::Persistence::Device
        );
    }

    #[test]
    #[should_panic(expected = "must be buffered or device")]
    fn netbatchwrite_rejects_unknown_persistence() {
        netbatchwrite_persistence(Some("syncish"));
    }

    #[test]
    fn netmultiread_accounts_each_result_and_verifies_values() {
        let expected = b"expected";
        let mut counts = NetreadResultCounts::default();
        netread_count_result(&mut counts, Ok(Some(expected)), expected);
        netread_count_result(&mut counts, Ok(Some(b"wrong")), expected);
        netread_count_result(&mut counts, Ok(None), expected);
        netread_count_result(&mut counts, Err(()), expected);
        assert_eq!(
            counts,
            NetreadResultCounts {
                hits: 1,
                errors: 1,
                misses: 1,
                mismatches: 1,
            }
        );
    }

    #[test]
    fn connection_counters_are_read_phase_deltas_and_saturate() {
        let stats = |node, shard, write_calls, read_drains| vals_cluster::ConnectionStats {
            node,
            shard,
            pending: 0,
            pending_peak: 4,
            queued_bytes: 0,
            queued_bytes_peak: 128,
            enqueued_bytes: 0,
            sent_bytes: 0,
            write_calls,
            read_drains,
            reconnects: 0,
        };
        let before = [stats(1, 0, 10, 20), stats(2, 0, 30, 40)];
        let after = [stats(1, 0, 17, 35), stats(2, 0, 42, 49)];
        assert_eq!(connection_counter_delta(&before, &after), (19, 24));
        assert_eq!(connection_counter_delta(&after, &before), (0, 0));
    }

    #[test]
    fn netread_latency_samples_merge_and_summarize() {
        assert!(!(1..=10).any(|op| netread_should_sample(0, op)));
        assert_eq!(
            (1..=10)
                .filter(|op| netread_should_sample(3, *op))
                .collect::<Vec<_>>(),
            vec![3, 6, 9]
        );

        let mut aggregate = NetloadStats::default();
        aggregate.merge(NetloadStats {
            r_lat_ns: vec![40, 10, 30],
            r_key_equiv_lat_ns: vec![10, 2, 7],
            ..NetloadStats::default()
        });
        aggregate.merge(NetloadStats {
            r_lat_ns: vec![50, 20],
            r_key_equiv_lat_ns: vec![12, 5],
            ..NetloadStats::default()
        });
        assert_eq!(
            netread_latency_summary(aggregate.r_lat_ns),
            NetreadLatencySummary {
                count: 5,
                p50_ns: 30,
                p99_ns: 50,
                p999_ns: 50,
                max_ns: 50,
            }
        );
        assert_eq!(
            netread_latency_summary(aggregate.r_key_equiv_lat_ns),
            NetreadLatencySummary {
                count: 5,
                p50_ns: 7,
                p99_ns: 12,
                p999_ns: 12,
                max_ns: 12,
            }
        );
        assert_eq!(
            netread_latency_summary(Vec::new()),
            NetreadLatencySummary {
                count: 0,
                p50_ns: 0,
                p99_ns: 0,
                p999_ns: 0,
                max_ns: 0,
            }
        );
    }
}

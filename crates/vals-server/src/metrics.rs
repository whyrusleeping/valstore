use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

use vals_cluster::observe;

static PROFILE_LATENCY: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

pub fn set_profile(latency: bool) {
    PROFILE_LATENCY.store(latency, std::sync::atomic::Ordering::Release);
}

pub fn spawn(addr: SocketAddr) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    log::info!("metrics endpoint listening on {addr}");
    observe::spawn_abort_on_panic("vals-metrics", move || serve(listener));
    Ok(())
}

fn serve(listener: TcpListener) {
    let mut err_streak: u32 = 0;
    for conn in listener.incoming() {
        let conn = match conn {
            Ok(conn) => {
                err_streak = 0;
                conn
            }
            Err(e) => {
                err_streak = err_streak.saturating_add(1);
                if err_streak == 1 || err_streak.is_multiple_of(256) {
                    log::warn!(
                        "metrics endpoint hit a transient accept error (retrying, streak {err_streak}): {e}"
                    );
                }
                let ms = (1u64 << (err_streak - 1).min(20)).min(50);
                std::thread::sleep(Duration::from_millis(ms));
                continue;
            }
        };
        let Some(_connection_credit) = vals_cluster::transport::acquire_connection() else {
            observe::counters().transport_connection_rejections.inc();
            continue;
        };
        let _ = scrape(conn);
    }
}

fn scrape(mut conn: TcpStream) -> std::io::Result<()> {
    let limits = vals_cluster::transport::limits();
    conn.set_read_timeout(limits.initial_timeout)?;
    conn.set_write_timeout(limits.write_timeout)?;
    let Some(_request_credit) = vals_cluster::transport::acquire_request() else {
        observe::counters().transport_request_rejections.inc();
        return Ok(());
    };
    let mut buf = [0u8; 1024];
    if let Err(e) = conn.read(&mut buf) {
        if matches!(e.kind(), std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock) {
            observe::counters().transport_initial_timeouts.inc();
        }
        return Err(e);
    }
    let body = render();
    let header = format!(
        "HTTP/1.0 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let Some(_byte_credit) =
        vals_cluster::transport::acquire_queued_bytes(header.capacity() + body.capacity())
    else {
        observe::counters().transport_queued_byte_rejections.inc();
        return Ok(());
    };
    let written = conn
        .write_all(header.as_bytes())
        .and_then(|_| conn.write_all(body.as_bytes()));
    if let Err(e) = &written {
        if matches!(e.kind(), std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock) {
            observe::counters().transport_write_timeouts.inc();
        }
    }
    written
}

fn render() -> String {
    let mut out = String::new();
    let latency = PROFILE_LATENCY.load(std::sync::atomic::Ordering::Acquire);
    out.push_str("# HELP vals_profile_info Selected deployment profile; exactly one profile label has value 1.\n");
    out.push_str("# TYPE vals_profile_info gauge\n");
    out.push_str(&format!(
        "vals_profile_info{{profile=\"capacity\"}} {}\nvals_profile_info{{profile=\"latency\"}} {}\n",
        u8::from(!latency),
        u8::from(latency),
    ));
    out.push_str("# HELP vals_blind_append_enabled Whether unconditional writes use optimistic blind append.\n");
    out.push_str("# TYPE vals_blind_append_enabled gauge\n");
    let blind = vals_cluster::replica::blind_append_enabled();
    let directory = vals_store::winner_directory_operational();
    out.push_str(&format!("vals_blind_append_enabled {}\n", u8::from(blind)));
    out.push_str("# HELP vals_winner_directory_enabled Whether the requested compact winner directory remains process-wide operational; 0 in degraded fail-closed state.\n");
    out.push_str("# TYPE vals_winner_directory_enabled gauge\n");
    out.push_str(&format!("vals_winner_directory_enabled {}\n", u8::from(directory)));
    for (name, help, value) in observe::snapshot_named() {
        let metric = format!("vals_{name}");
        out.push_str(&format!("# HELP {metric} {}\n", help.trim()));
        out.push_str(&format!("# TYPE {metric} counter\n"));
        out.push_str(&format!("{metric} {value}\n"));
    }
    for (name, help, value) in observe::gauges_named() {
        let metric = format!("vals_{name}");
        out.push_str(&format!("# HELP {metric} {}\n", help.trim()));
        out.push_str(&format!("# TYPE {metric} gauge\n"));
        out.push_str(&format!("{metric} {value}\n"));
    }
    for (name, help, value) in vals_store::attribution::counters() {
        out.push_str(&format!("# HELP vals_{name} {help}\n# TYPE vals_{name} counter\nvals_{name} {value}\n"));
    }
    out.push_str("# HELP vals_physical_write_bytes_total Aligned bytes submitted to device writes by source, including failed attempts; catalog includes manifests and their invalidation writes, other includes format and reclaim wipes. Excludes filesystem catalog IO and backend-internal writes.\n# TYPE vals_physical_write_bytes_total counter\n");
    for (source, bytes) in vals_store::attribution::writes() {
        out.push_str(&format!("vals_physical_write_bytes_total{{source=\"{source}\"}} {bytes}\n"));
    }
    out.push_str("# HELP vals_run_output_part_bytes_total Logical encoded run bytes by path and part, before LBA padding; completed encodes including outputs later abandoned. Sidecar reported separately for v4.\n# TYPE vals_run_output_part_bytes_total counter\n");
    for (path, part, bytes) in vals_store::attribution::parts() {
        out.push_str(&format!("vals_run_output_part_bytes_total{{path=\"{path}\",part=\"{part}\"}} {bytes}\n"));
    }
    out.push_str("# HELP vals_range_delete_refused_total Range-delete requests or slices a replica refused, by reason: stale (whole request outside this shard's ranges), skew (slice beyond the clock-skew bound), cap (fragment cap), unsupported (node writes pre-v6 runs), space (backpressure or full device), mirror (re-home mirror failed), error (other engine or sync error).\n# TYPE vals_range_delete_refused_total counter\n");
    for (reason, count) in observe::range_delete_refused_metrics() {
        out.push_str(&format!("vals_range_delete_refused_total{{reason=\"{reason}\"}} {count}\n"));
    }
    out.push_str("# HELP vals_ae_rt_exchanges_total Anti-entropy range-tombstone set exchanges (one per sorted zone and peer shard per sweep pass), by result: same (scoped fragment digests matched), pulled (fragments received and applied), error (RPC or apply failure; retried next pass).\n# TYPE vals_ae_rt_exchanges_total counter\n");
    for (result, count) in observe::rt_exchange_metrics() {
        out.push_str(&format!("vals_ae_rt_exchanges_total{{result=\"{result}\"}} {count}\n"));
    }
    for (name, help, value) in [
        ("rt_max_tree_built_blocks_total", "Run blocks whose max-HLC tree was built for range-tombstone block skips: at run install, open and an engine's first range-tombstone fold; never on the scan path.", vals_store::range_tombstone::rt_max_tree_built_blocks_total()),
        ("rt_covered_dropped_total", "Point entries flushes and run rewrites dropped because an accounted range tombstone covers them.", vals_store::engine::rt_covered_dropped_total()),
        ("rt_collected_total", "Range-tombstone records collected by run rewrites once older than the GC cutoff and no longer covering any stored entry.", vals_store::engine::rt_collected_total()),
        ("rt_dominated_dropped_total", "Range-tombstone records flushes and run rewrites dropped because a newer accounted record in the same output covers their whole span.", vals_store::engine::rt_dominated_dropped_total()),
    ] {
        out.push_str(&format!("# HELP vals_{name} {help}\n# TYPE vals_{name} counter\nvals_{name} {value}\n"));
    }
    let shard_writes = observe::shard_write_metrics();
    for (name, help, ops) in [
        ("shard_synced_writes_total", "Writes in successful synced batches by serving shard; same semantics as synced_writes, excluding standalone stores without shard state.", false),
        ("shard_synced_ops_total", "Successful synced batch requests by serving shard; one per request, not per shared device flush.", true),
    ] {
        out.push_str(&format!("# HELP vals_{name} {help}\n# TYPE vals_{name} counter\n"));
        for (shard, writes, batches) in &shard_writes {
            out.push_str(&format!("vals_{name}{{shard=\"{shard}\"}} {}\n", if ops { batches } else { writes }));
        }
    }
    out.push_str(&format!("# HELP vals_merge_pending_debt_input_bytes Input bytes of the latest RAM-deferred merge plans with tracked broker waiters; plans have been aborted, not queued. Cleared on grant, cancellation or existing stale-waiter reap; excludes unsampled eligible work and scheduler backlog.\n# TYPE vals_merge_pending_debt_input_bytes gauge\nvals_merge_pending_debt_input_bytes {}\n", observe::pending_merge_input_bytes()));
    let ram_classes = observe::ram_class_metrics();
    for (name, help, field, ty) in [
        ("ram_deferred_bytes_total", "Requested RAM bytes on each failed reservation attempt by class; retries are counted again.", 0, "counter"),
        ("ram_unique_wait_ms_total", "Milliseconds from first defer to grant, cancellation or stale-waiter expiration by class, counted once per waiter episode; expiration measured when reaped.", 1, "counter"),
        ("ram_unique_waits_total", "Completed unique RAM waiter episodes by class (grant, cancellation or expiration).", 2, "counter"),
        ("ram_pending_deferred_bytes", "Latest requested RAM bytes of tracked deferred waiters by class; merge is unscheduled RAM reservation debt, NOT pending merge input bytes. Stale entries disappear on existing broker reap cadence.", 3, "gauge"),
    ] {
        out.push_str(&format!("# HELP vals_{name} {help}\n# TYPE vals_{name} {ty}\n"));
        for (class, bytes, wait, count, pending) in &ram_classes {
            out.push_str(&format!("vals_{name}{{class=\"{class}\"}} {}\n", [bytes, wait, count, pending][field]));
        }
    }
    let engines = observe::engine_metrics();
    for (name, help, field) in [
        ("engine_resident_runs", "Resident runs by engine (shard/zone), sampled on sweep and off-core flush/merge completion.", 0),
        ("engine_frozen_runs", "Frozen runs by engine (shard/zone), sampled on sweep and off-core flush/merge completion.", 1),
        ("engine_stranded_runs", "Non-frozen runs older than the newest frozen run by engine (shard/zone), sampled on sweep and off-core flush/merge completion.", 3),
        ("engine_free_fraction_ppm", "Free segment fraction in parts per million by engine (shard/zone), sampled on sweep and off-core flush/merge completion.", 2),
        ("engine_rt_records", "Range-tombstone records held by engine (shard/zone), in runs and the unflushed tail, sampled on sweep and off-core flush/merge completion.", 4),
    ] {
        out.push_str(&format!("# HELP vals_{name} {help}\n# TYPE vals_{name} gauge\n"));
        for (engine, m) in &engines {
            out.push_str(&format!("vals_{name}{{engine=\"{engine}\"}} {}\n", [m.resident_runs, m.frozen_runs, m.free_fraction_ppm, m.stranded_runs, m.rt_records][field]));
        }
    }
    let lookup_totals = observe::lookup_metrics();
    for (field, name, help) in [
        (0, "lookup_calls", "Engine lookup attempts by caller across sampled serving shard threads, including retries; exists is version/existence queries including local anti-entropy probes, apply is staged write-side version resolution and authoritative-put directory lookup, digest is server digest engine resolution. Thread-lifetime totals sampled on sweep and maintenance completion; directory-fast Digest bypasses the engine and is excluded."),
        (1, "lookup_bloom_probes", "In-range run bloom probes by engine lookup caller across sampled serving shard threads. Thread-lifetime totals sampled on sweep and maintenance completion."),
        (2, "lookup_bloom_positives", "Actual run bloom positives by engine lookup caller across sampled serving shard threads, including positives with no candidate fence block. Thread-lifetime totals sampled on sweep and maintenance completion."),
        (3, "lookup_covered_positives", "Run bloom positives in key-filter-covered runs for version, synchronous and staged GET lookups; definitive key-filter negatives skip these runs. Thread-lifetime totals sampled on sweep and maintenance completion."),
        (4, "lookup_uncovered_positives", "Run bloom positives in key-filter-uncovered runs for version, synchronous and staged GET lookups; key-filter negatives do not skip these runs. Thread-lifetime totals sampled on sweep and maintenance completion."),
    ] {
        out.push_str(&format!("# HELP vals_{name} {help}\n# TYPE vals_{name} gauge\n"));
        for (i, caller) in vals_store::attribution::LOOKUP_CALLERS.iter().enumerate() {
            let value = lookup_totals[i][field];
            out.push_str(&format!("vals_{name}{{caller=\"{caller}\"}} {value}\n"));
        }
    }
    out.push_str("# HELP vals_run_merge_output_bytes_by_kind_total Output bytes of committed off-core merges by kind, using the same whole-segment convention as run_merge_output_bytes (including rewritten value bytes).\n# TYPE vals_run_merge_output_bytes_by_kind_total counter\n");
    for (kind, bytes) in observe::merge_output_kind_metrics() {
        out.push_str(&format!("vals_run_merge_output_bytes_by_kind_total{{kind=\"{kind}\"}} {bytes}\n"));
    }
    out.push_str(&format!("# HELP vals_store_corrupt_reads_total {}\n# TYPE vals_store_corrupt_reads_total counter\n", observe::CORRUPT_READS_HELP));
    for (kind, reads) in observe::corrupt_read_metrics() {
        out.push_str(&format!("vals_store_corrupt_reads_total{{kind=\"{kind}\"}} {reads}\n"));
    }
    let range_map_endpoints = observe::pd_range_map_endpoint_metrics();
    out.push_str("# HELP vals_pd_range_map_preflight_attempts_total Opaque range-map capability preflight handshakes attempted by endpoint.\n");
    out.push_str("# TYPE vals_pd_range_map_preflight_attempts_total counter\n");
    for endpoint in &range_map_endpoints {
        out.push_str(&format!(
            "vals_pd_range_map_preflight_attempts_total{{endpoint=\"{}\"}} {}\n",
            endpoint.endpoint, endpoint.preflight_attempts
        ));
    }
    out.push_str("# HELP vals_pd_range_map_preflight_blocks_total Opaque range-map deliveries blocked because an endpoint did not freshly certify V4 support.\n");
    out.push_str("# TYPE vals_pd_range_map_preflight_blocks_total counter\n");
    for endpoint in &range_map_endpoints {
        out.push_str(&format!(
            "vals_pd_range_map_preflight_blocks_total{{endpoint=\"{}\"}} {}\n",
            endpoint.endpoint, endpoint.preflight_blocks
        ));
    }
    out.push_str("# HELP vals_pd_range_map_push_attempts_total SetRangeMaps requests attempted after any required preflight, by endpoint.\n");
    out.push_str("# TYPE vals_pd_range_map_push_attempts_total counter\n");
    for endpoint in &range_map_endpoints {
        out.push_str(&format!(
            "vals_pd_range_map_push_attempts_total{{endpoint=\"{}\"}} {}\n",
            endpoint.endpoint, endpoint.push_attempts
        ));
    }
    out.push_str("# HELP vals_pd_range_map_push_failures_total SetRangeMaps requests that failed or did not confirm the requested epoch, by endpoint.\n");
    out.push_str("# TYPE vals_pd_range_map_push_failures_total counter\n");
    for endpoint in &range_map_endpoints {
        out.push_str(&format!(
            "vals_pd_range_map_push_failures_total{{endpoint=\"{}\"}} {}\n",
            endpoint.endpoint, endpoint.push_failures
        ));
    }
    out.push_str("# HELP vals_pd_range_map_last_confirmed_age_seconds Seconds since the PD last received a SetRangeMaps confirmation from an endpoint.\n");
    out.push_str("# TYPE vals_pd_range_map_last_confirmed_age_seconds gauge\n");
    for endpoint in &range_map_endpoints {
        if let Some(age) = endpoint.last_confirmed_age_seconds {
            out.push_str(&format!(
                "vals_pd_range_map_last_confirmed_age_seconds{{endpoint=\"{}\"}} {age}\n",
                endpoint.endpoint
            ));
        }
    }
    let merge_kinds = observe::merge_kind_metrics();
    out.push_str(&format!("# HELP vals_run_merges_by_kind_total {}\n", observe::RUN_MERGES_BY_KIND_HELP));
    out.push_str("# TYPE vals_run_merges_by_kind_total counter\n");
    for (kind, merges, _) in &merge_kinds {
        out.push_str(&format!("vals_run_merges_by_kind_total{{kind=\"{kind}\"}} {merges}\n"));
    }
    out.push_str(&format!(
        "# HELP vals_run_merge_input_bytes_by_kind_total {}\n",
        observe::RUN_MERGE_INPUT_BYTES_BY_KIND_HELP
    ));
    out.push_str("# TYPE vals_run_merge_input_bytes_by_kind_total counter\n");
    for (kind, _, bytes) in &merge_kinds {
        out.push_str(&format!("vals_run_merge_input_bytes_by_kind_total{{kind=\"{kind}\"}} {bytes}\n"));
    }
    for h in observe::histograms_named() {
        let metric = format!("vals_{}", h.name);
        out.push_str(&format!("# HELP {metric} {}\n", h.help.trim()));
        out.push_str(&format!("# TYPE {metric} histogram\n"));
        let mut cumulative = 0u64;
        for (bound, count) in h.bounds.iter().zip(h.buckets.iter()) {
            cumulative += count;
            out.push_str(&format!("{metric}_bucket{{le=\"{bound}\"}} {cumulative}\n"));
        }
        out.push_str(&format!("{metric}_bucket{{le=\"+Inf\"}} {}\n", h.count));
        let sum_seconds = h.sum_us as f64 / h.sum_units_per_second;
        out.push_str(&format!("{metric}_sum {sum_seconds}\n"));
        out.push_str(&format!("{metric}_count {}\n", h.count));
    }
    let (flush_depth_count, flush_depth_sum) = observe::domain_flush_vlog_writes_inflight().snapshot();
    out.push_str(&format!(
        "# HELP vals_domain_flush_vlog_writes_inflight {}\n# TYPE vals_domain_flush_vlog_writes_inflight summary\nvals_domain_flush_vlog_writes_inflight_count {flush_depth_count}\nvals_domain_flush_vlog_writes_inflight_sum {flush_depth_sum}\n",
        observe::DOMAIN_FLUSH_VLOG_WRITES_INFLIGHT_HELP,
    ));
    let (vlog_stalls, vlog_stall_us) = vals_store::vlog_stall_totals();
    out.push_str("# HELP vals_vlog_stalls Times a value-log append blocked on a full inflight-write queue (process-wide).\n");
    out.push_str("# TYPE vals_vlog_stalls counter\n");
    out.push_str(&format!("vals_vlog_stalls {vlog_stalls}\n"));
    out.push_str("# HELP vals_vlog_stall_us_total Total microseconds value-log appends spent blocked on a full inflight-write queue (process-wide).\n");
    out.push_str("# TYPE vals_vlog_stall_us_total counter\n");
    out.push_str(&format!("vals_vlog_stall_us_total {vlog_stall_us}\n"));
    let (replay_skips, replay_skipped_bytes) = vals_store::vlog_replay_skip_totals();
    out.push_str("# HELP vals_vlog_replay_skips_total Damaged value-log regions that replay skipped because intact records of the same segment incarnation follow them: mid-segment corruption, or blocks a crash left unwritten after the last sync (process-wide, counted at open and by segment reclaim).\n");
    out.push_str("# TYPE vals_vlog_replay_skips_total counter\n");
    out.push_str(&format!("vals_vlog_replay_skips_total {replay_skips}\n"));
    out.push_str("# HELP vals_vlog_replay_skipped_bytes_total Bytes in the damaged value-log regions counted by vals_vlog_replay_skips_total (process-wide).\n");
    out.push_str("# TYPE vals_vlog_replay_skipped_bytes_total counter\n");
    out.push_str(&format!("vals_vlog_replay_skipped_bytes_total {replay_skipped_bytes}\n"));
    let (drain_count, drain_us, tail_count, tail_us) = vals_store::vlog_sync_totals();
    let (verify_bytes, verify_failures) = vals_store::run::run_build_verify_totals();
    out.push_str(&format!(
        "# HELP vals_run_write_version Run format version new flushes and merges write (4 = with sidecar, 5 = without, 6 = adds per-block version bounds and range tombstones; VALS_RUN_WRITE_VERSION).\n# TYPE vals_run_write_version gauge\nvals_run_write_version {}\n",
        vals_store::run::run_write_version()
    ));
    out.push_str(&format!(
        "# HELP vals_run_block_target_bytes Estimated encoded bytes at which new run data blocks close (VALS_RUN_BLOCK_TARGET_BYTES; existing runs keep their block size).\n# TYPE vals_run_block_target_bytes gauge\nvals_run_block_target_bytes {}\n# HELP vals_run_bloom_bits_per_key Bloom bits per entry for new runs (VALS_RUN_BLOOM_BITS_PER_KEY).\n# TYPE vals_run_bloom_bits_per_key gauge\nvals_run_bloom_bits_per_key {}\n",
        vals_store::run::run_block_target_bytes(),
        vals_store::run::run_bloom_bits_per_key()
    ));
    for (name, help, value) in [
        ("run_build_verify_bytes_total", "Bytes of new v5 merge-output runs read back from the device after their last write and checked before publication: data-block CRCs plus footer, filter and index CRCs and byte compare (VALS_RUN_BUILD_VERIFY, process-wide).", verify_bytes),
        ("run_build_verify_failures_total", "New v5 merge-output runs whose pre-publication device readback failed a data-block, footer, filter or index check (process-wide). Must stay 0.", verify_failures),
        ("memtable_integrity_failures_total", "Front-coded memtable block integrity failures detected before flush emission, including retries (process-wide). Must stay 0.", vals_store::index::integrity_failures()),
        ("vlog_write_bytes_total", "Bytes passed to value-log full-block async writes and successful partial-tail writes (process-wide, includes headers and tail rewrites; async failures included, excludes maintenance rewrites). Divide by domain_flushes for bytes per flush only for domain-backed device-sync workloads after draining; buffered traffic is also counted.", vals_store::vlog::vlog_write_bytes()),
        ("run_merge_inner_windows_total", "Recurring or forced merges committed on an inner window: the window ends before the newest run, and its output takes the window's place (task 0381, process-wide).", vals_store::inner_window_merges_total()),
        ("vlog_sync_drain_count", "Sync-prepare value-log inflight drains (process-wide).", drain_count),
        ("vlog_sync_drain_us_total", "Microseconds spent in sync-prepare value-log inflight drains (process-wide).", drain_us),
        ("vlog_sync_tail_count", "Sync-prepare partial-tail device writes (process-wide).", tail_count),
        ("vlog_sync_tail_us_total", "Microseconds spent in sync-prepare partial-tail device writes (process-wide).", tail_us),
    ] {
        out.push_str(&format!("# HELP vals_{name} {help}\n# TYPE vals_{name} counter\nvals_{name} {value}\n"));
    }
    out.push_str("# HELP vals_encode_integrity_failures_total New run builds aborted before publication because the encoded blocks did not decode to the attested source entries in strictly ascending key order, or the index did not match the encoded blocks (path=flush|merge, process-wide). Must stay 0.\n");
    out.push_str("# TYPE vals_encode_integrity_failures_total counter\n");
    for (path, n) in vals_store::run::encode_integrity_failures() {
        out.push_str(&format!("vals_encode_integrity_failures_total{{path=\"{path}\"}} {n}\n"));
    }
    let dealloc = vals_spdk::device_dealloc_totals();
    for (name, help, value) in [
        ("device_dealloc_commands_total", "Device deallocation commands attempted, including unsupported attempts (process-wide).", dealloc.commands),
        ("device_dealloc_ranges_total", "LBA ranges in attempted deallocation commands (process-wide).", dealloc.ranges),
        ("device_dealloc_bytes_total", "Bytes in successfully completed deallocation commands (process-wide).", dealloc.bytes),
        ("device_dealloc_errors_total", "Device deallocation command errors (process-wide).", dealloc.errors),
        ("device_dealloc_unsupported_total", "Unsupported device deallocation attempts (process-wide).", dealloc.unsupported),
        ("device_dealloc_latency_us_total", "Microseconds spent in device deallocation commands (process-wide).", dealloc.latency_us),
    ] {
        out.push_str(&format!("# HELP vals_{name} {help}\n# TYPE vals_{name} counter\nvals_{name} {value}\n"));
    }
    let (pace_charged_ns, pace_slept_ns) = vals_store::maint_pace_totals();
    out.push_str("# HELP vals_maint_pace_charged_ms Total milliseconds of off-core maintenance device work observed by the duty-cycle pacer (process-wide).\n");
    out.push_str("# TYPE vals_maint_pace_charged_ms counter\n");
    out.push_str(&format!(
        "vals_maint_pace_charged_ms {}\n",
        pace_charged_ns / 1_000_000
    ));
    out.push_str("# HELP vals_maint_pace_slept_ms Total milliseconds off-core maintenance jobs slept to honor VALS_MAINT_DUTY_PCT (process-wide).\n");
    out.push_str("# TYPE vals_maint_pace_slept_ms counter\n");
    out.push_str(&format!(
        "vals_maint_pace_slept_ms {}\n",
        pace_slept_ns / 1_000_000
    ));
    out.push_str("# HELP vals_maint_pace_backlog_unpaced_ms Total milliseconds of paced maintenance work run without pacing because another maintenance job was queued (process-wide).\n");
    out.push_str("# TYPE vals_maint_pace_backlog_unpaced_ms counter\n");
    out.push_str(&format!(
        "vals_maint_pace_backlog_unpaced_ms {}\n",
        vals_store::maint_pace_backlog_unpaced_ns() / 1_000_000
    ));
    let io = vals_spdk::spdk_io_stats();
    out.push_str("# HELP vals_spdk_api_write_ops Rust SPDK write API operations accepted/submitted by instrumented paths; rejected submissions and failed synchronous operations are excluded, and an operation may not equal one controller command (process-wide).\n");
    out.push_str("# TYPE vals_spdk_api_write_ops counter\n");
    out.push_str(&format!("vals_spdk_api_write_ops {}\n", io.api_write_ops));
    out.push_str("# HELP vals_spdk_api_write_bytes Bytes passed by accepted/submitted Rust SPDK write API operations, not physical media bytes (process-wide).\n");
    out.push_str("# TYPE vals_spdk_api_write_bytes counter\n");
    out.push_str(&format!("vals_spdk_api_write_bytes {}\n", io.api_write_bytes));
    out.push_str("# HELP vals_spdk_api_write_completions Rust SPDK write API operations observed complete; failed synchronous operations are not counted (process-wide).\n");
    out.push_str("# TYPE vals_spdk_api_write_completions counter\n");
    out.push_str(&format!("vals_spdk_api_write_completions {}\n", io.api_write_completions));
    out.push_str("# HELP vals_spdk_api_write_completion_errors Asynchronous Rust SPDK write operations whose completion reported failure; synchronous write failures are not counted (process-wide).\n");
    out.push_str("# TYPE vals_spdk_api_write_completion_errors counter\n");
    out.push_str(&format!("vals_spdk_api_write_completion_errors {}\n", io.api_write_completion_errors));
    out.push_str("# HELP vals_spdk_api_writes_outstanding Instrumented Rust SPDK write API operations between submit accounting and completion accounting; successful synchronous operations increment and decrement inline (process-wide).\n");
    out.push_str("# TYPE vals_spdk_api_writes_outstanding gauge\n");
    out.push_str(&format!("vals_spdk_api_writes_outstanding {}\n", io.api_writes_outstanding));
    out.push_str("# HELP vals_spdk_max_api_writes_outstanding Process-lifetime high-water mark of instrumented Rust SPDK write API operations between submit and completion accounting.\n");
    out.push_str("# TYPE vals_spdk_max_api_writes_outstanding gauge\n");
    out.push_str(&format!("vals_spdk_max_api_writes_outstanding {}\n", io.max_api_writes_outstanding));
    out.push_str("# HELP vals_spdk_async_poll_calls Rust-side asynchronous SPDK qpair poll calls; internal C retry and synchronous polling is excluded (process-wide).\n");
    out.push_str("# TYPE vals_spdk_async_poll_calls counter\n");
    out.push_str(&format!("vals_spdk_async_poll_calls {}\n", io.async_poll_calls));
    out.push_str("# HELP vals_spdk_async_poll_completions Completions returned by Rust-side asynchronous SPDK qpair polls; internal C retry and synchronous completions are excluded (process-wide).\n");
    out.push_str("# TYPE vals_spdk_async_poll_completions counter\n");
    out.push_str(&format!("vals_spdk_async_poll_completions {}\n", io.async_poll_completions));
    out.push_str("# HELP vals_spdk_async_io_pending_underflows Asynchronous I/O completions accounted on a thread with no pending submission of its own, i.e. a completion that ran off its submitting shard thread; nonzero means some shard's pending-I/O count leaked and it may busy-poll (process-wide).\n");
    out.push_str("# TYPE vals_spdk_async_io_pending_underflows counter\n");
    out.push_str(&format!("vals_spdk_async_io_pending_underflows {}\n", io.async_io_pending_underflows));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exported_registry_is_complete_and_unique() {
        use std::collections::BTreeMap;

        let rendered = render();
        let mut types = BTreeMap::new();
        let mut helps = BTreeMap::new();
        for line in rendered.lines() {
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                let (name, kind) = rest.split_once(' ').unwrap();
                assert!(types.insert(name, kind).is_none(), "duplicate TYPE {name}");
            }
            if let Some(rest) = line.strip_prefix("# HELP ") {
                let (name, help) = rest.split_once(' ').unwrap();
                assert!(helps.insert(name, help).is_none(), "duplicate HELP {name}");
            }
        }
        assert_eq!(types.keys().collect::<Vec<_>>(), helps.keys().collect::<Vec<_>>());
        let mut expected: Vec<_> = observe::snapshot_named().into_iter()
            .map(|(name, help, _)| (name, help, "counter")).collect();
        expected.extend(observe::gauges_named().into_iter()
            .map(|(name, help, _)| (name, help, "gauge")));
        expected.extend(vals_store::attribution::counters().into_iter()
            .map(|(name, help, _)| (name, help, "counter")));
        expected.extend(observe::histograms_named().into_iter()
            .map(|hist| (hist.name, hist.help, "histogram")));
        expected.push(("domain_flush_vlog_writes_inflight", observe::DOMAIN_FLUSH_VLOG_WRITES_INFLIGHT_HELP, "summary"));
        let mut names = std::collections::BTreeSet::new();
        for (name, help, kind) in expected {
            assert!(names.insert(name), "duplicate registry name {name}");
            let metric = format!("vals_{name}");
            assert_eq!(types.get(metric.as_str()), Some(&kind), "{name}");
            assert_eq!(helps.get(metric.as_str()), Some(&help.trim()), "{name}");
        }
    }

    #[test]
    fn scrape_returns_prometheus_text_with_every_counter() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || serve(listener));

        vals_cluster::observe::counters().server_ops.inc();
        vals_cluster::observe::gauges().live_keys.set(42);
        vals_cluster::observe::histograms()
            .scan_latency_us
            .observe_us(123);
        let endpoint: SocketAddr = "127.0.0.1:41999".parse().unwrap();
        vals_cluster::observe::record_pd_range_map_delivery(
            endpoint,
            vals_cluster::observe::PdRangeMapDeliveryEvent::PreflightBlock,
        );
        vals_cluster::observe::confirm_pd_range_map_delivery(endpoint);

        let mut conn = TcpStream::connect(addr).unwrap();
        conn.write_all(b"GET /metrics HTTP/1.0\r\n\r\n").unwrap();
        let mut reply = String::new();
        conn.read_to_string(&mut reply).unwrap();

        assert!(reply.starts_with("HTTP/1.0 200 OK"), "{reply}");
        assert!(reply.contains("text/plain"), "{reply}");
        assert!(reply.contains("# TYPE vals_server_ops counter"), "{reply}");
        assert!(reply.contains("# HELP vals_server_ops "), "{reply}");
        assert!(reply.contains("\nvals_graceful_shutdowns "), "{reply}");
        assert!(reply.contains("# TYPE vals_integrity_fail_fast_total counter"), "{reply}");
        assert!(reply.contains("# TYPE vals_live_keys gauge"), "{reply}");
        for name in ["domain_flush_device_us", "domain_flush_publish_us"] {
            assert!(reply.contains(&format!("# TYPE vals_{name} histogram")), "{reply}");
            assert!(reply.contains(&format!("\nvals_{name}_sum ")), "{reply}");
        }
        assert!(reply.contains("# TYPE vals_domain_flush_vlog_writes_inflight summary"), "{reply}");
        assert!(reply.contains("\nvals_domain_flush_vlog_writes_inflight_count "), "{reply}");
        assert!(reply.contains("\nvals_domain_flush_vlog_writes_inflight_sum "), "{reply}");
        for name in ["domain_flush_busy_us_total", "vlog_write_bytes_total", "synced_writes"] {
            assert!(reply.contains(&format!("# TYPE vals_{name} counter")), "{reply}");
        }
        assert!(
            reply.contains(
                "vals_pd_range_map_preflight_blocks_total{endpoint=\"127.0.0.1:41999\"} 1"
            ),
            "{reply}"
        );
        assert!(
            reply.contains(
                "vals_pd_range_map_last_confirmed_age_seconds{endpoint=\"127.0.0.1:41999\"} 0"
            ),
            "{reply}"
        );
        assert!(
            reply.contains("# TYPE vals_scan_latency_us histogram"),
            "{reply}"
        );
        assert!(
            reply.contains("vals_scan_latency_us_bucket{le=\"+Inf\"} "),
            "{reply}"
        );
        assert!(reply.contains("vals_scan_latency_us_count "), "{reply}");
        assert!(reply.contains("# HELP vals_spdk_api_write_ops Rust SPDK write API operations accepted/submitted"), "{reply}");
        assert!(reply.contains("# TYPE vals_spdk_api_write_ops counter"), "{reply}");
        assert!(reply.contains("# TYPE vals_spdk_async_poll_calls counter"), "{reply}");
        assert!(reply.contains("# TYPE vals_memtable_integrity_failures_total counter"), "{reply}");
        assert!(reply.contains("\nvals_memtable_integrity_failures_total "), "{reply}");
        assert!(reply.contains("# TYPE vals_encode_integrity_failures_total counter"), "{reply}");
        for path in ["flush", "merge"] {
            assert!(reply.contains(&format!("\nvals_encode_integrity_failures_total{{path=\"{path}\"}} ")), "{reply}");
        }
        assert!(reply.contains("# TYPE vals_run_build_verify_bytes_total counter"), "{reply}");
        assert!(reply.contains("\nvals_run_build_verify_failures_total "), "{reply}");
        assert!(reply.contains("\nvals_run_write_version 6\n"), "{reply}");
        assert!(reply.contains("\nvals_run_block_target_bytes 32768\n"), "{reply}");
        assert!(reply.contains("\nvals_run_bloom_bits_per_key 10\n"), "{reply}");
        for kind in vals_store::vlog::CORRUPT_READ_KINDS {
            assert!(reply.contains(&format!("\nvals_store_corrupt_reads_total{{kind=\"{kind}\"}} ")), "{reply}");
        }
        for kind in ["recurring", "forced", "pressure", "shadow", "vlog_gc", "lonegc", "forget"] {
            assert!(reply.contains(&format!("vals_run_merges_by_kind_total{{kind=\"{kind}\"}} ")), "{reply}");
            assert!(reply.contains(&format!("vals_run_merge_input_bytes_by_kind_total{{kind=\"{kind}\"}} ")), "{reply}");
        }
        for name in ["frozen_runs", "stranded_runs", "max_run_bytes", "wipe_pending"] {
            assert!(reply.contains(&format!("# TYPE vals_{name} gauge\nvals_{name} ")), "{reply}");
        }
        for name in [
            "run_fence_resident_bytes", "run_filter_resident_bytes", "run_entries",
            "run_fence_payload_bytes", "run_filter_payload_bytes",
            "key_filter_bytes", "key_filter_retired_bytes", "key_filter_retired_resident_bytes",
        ] {
            assert!(reply.contains(&format!("# TYPE vals_{name} gauge\nvals_{name} ")), "{reply}");
        }
        for name in ["read_probe_bytes", "apply_probe_bytes"] {
            assert!(reply.contains(&format!("# TYPE vals_{name} counter\nvals_{name} ")), "{reply}");
        }
        assert!(!reply.contains("vals_spdk_write_commands"), "{reply}");
        assert!(!reply.contains("vals_spdk_qpair_poll_calls"), "{reply}");
        for line in reply.lines().skip_while(|l| !l.starts_with("# HELP")) {
            if line.starts_with('#') || line.is_empty() {
                continue;
            }
            let (name, value) = line.split_once(' ').expect("metric line");
            assert!(name.starts_with("vals_"), "{line}");
            if name.ends_with("_sum") {
                let v = value.parse::<f64>().expect("histogram sum");
                assert!(v >= 0.0 && v.is_finite(), "{line}");
            } else {
                value.parse::<u64>().expect("integer metric value");
            }
        }
    }
}

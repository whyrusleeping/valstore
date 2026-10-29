# Configuration: supported settings and diagnostic switches

`valsd` reads one flat `key = value` file per daemon (`valsd node -c FILE`,
`valsd pd -c FILE`). The core keys (ids, addresses, storage backend, device
geometry, secrets) are described in [`deploy.md`](deploy.md#node-config-reference)
and [`deploy.md`](deploy.md#pd-config-reference). This document covers the rest:

- the **supported tuning keys** for the PD and for data nodes, each with a
  legacy environment-variable alias, and the rules for how the two combine;
- the **diagnostic environment variables** that exist for experiments and
  incident work. They are not a supported configuration surface. Their
  defaults describe the current code and may change without notice.
- the environment variables read only by tests, benchmarks and the build.

A switch is not made a config key just because it changes resource use or can
affect correctness. Only the keys in the two tables below are supported.

## PD placement and rehome settings

These optional keys go in the PD config file. The legacy alias of each one is
`VALS_PD_` plus the key in upper case. Defaults match the PD's built-in
placement policy, with automatic rehome **on**.

| PD config key | Legacy environment alias | Default | Units / validation |
| --- | --- | ---: | --- |
| `range_split_threshold` | `VALS_PD_RANGE_SPLIT_THRESHOLD` | 1000000 | entries; 0 disables size splits |
| `range_load_split_ops` | `VALS_PD_RANGE_LOAD_SPLIT_OPS` | 0 | replica operations/s; 0 disables load splits |
| `range_split_poll_ms` | `VALS_PD_RANGE_SPLIT_POLL_MS` | 5000 | milliseconds; >0 |
| `range_splits_per_poll` | `VALS_PD_RANGE_SPLITS_PER_POLL` | 16 | splits per zone per poll; >0 |
| `range_merge_threshold` | `VALS_PD_RANGE_MERGE_THRESHOLD` | 0 | combined pair entries; 0 disables threshold merges (covered merges are separate) |
| `range_merge_covered` | `VALS_PD_RANGE_MERGE_COVERED` | true | boolean; merges adjacent ranges that are empty and wholly range-deleted on every owner (queue/ring debris behind a deleted head; see "Covered merges" in [`zones.md`](zones.md)); false turns them off |
| `single_range_warn_entries` | `VALS_PD_SINGLE_RANGE_WARN_ENTRIES` | 1000000 | entries; 0 disables warning guardrails |
| `range_concentration_warn_pct` | `VALS_PD_RANGE_CONCENTRATION_WARN_PCT` | 80 | percentage; 0 disables concentration warnings |
| `rehome_enabled` | `VALS_PD_REHOME_ENABLED` | true | boolean; false stops new automatic rehomes |
| `max_concurrent_rehomes` | `VALS_PD_MAX_CONCURRENT_REHOMES` | 1 | >0 |
| `rehome_report_deadline_ms` | `VALS_PD_REHOME_REPORT_DEADLINE_MS` | 60000 | progress-aware report deadline in milliseconds |
| `rehome_recovery_defer_ms` | `VALS_PD_REHOME_RECOVERY_DEFER_MS` | 1800000 | total hold, in milliseconds per recovery episode, on starting re-homes and balance proposals while a data node is unreachable, reports a restart-caused (recovery) seed, or was seen by the beacon to restart and has not yet reported its recovery seed finishing (at most max(30 s, 2 range-stats polls) if it reports none; see "Recovery hold" in [`zones.md`](zones.md)); commits are never held; an episode ends after 60 s of clean polls; 0 disables the hold |
| `opaque_range_maps` | `VALS_PD_OPAQUE_RANGE_MAPS` | false | boolean; requires cross-node readiness, see below |
| `balance_execute` | `VALS_PD_BALANCE_EXECUTE` | false | boolean; independent mutation opt-in |
| `balance_high_pct` | `VALS_PD_BALANCE_HIGH_PCT` | 120 | load/target ×100; >=100 |
| `balance_low_pct` | `VALS_PD_BALANCE_LOW_PCT` | 105 | load/target ×100; >=100 and <= high |
| `balance_min_improvement_pct` | `VALS_PD_BALANCE_MIN_IMPROVEMENT_PCT` | 5 | ratio percentage points |
| `balance_token_search_limit` | `VALS_PD_BALANCE_TOKEN_SEARCH_LIMIT` | 4096 | nonce search budget; >0 |

Integers must fit their unsigned implementation types (`usize` for
`range_splits_per_poll` and `max_concurrent_rehomes`, `u64` otherwise). The
boolean keys in this table (`range_merge_covered`, `rehome_enabled`,
`opaque_range_maps`, `balance_execute`) accept exactly `true/false`, `yes/no`
or `1/0`, in config and environment. This differs from the core PD keys
`campaign` and `join`, which accept `true/yes/on` and `false/no/off` but not
`1/0`; the node settings in the next section have no booleans, only integers
(`ae_seed_isolation` and `ae_span_regroup` take `0` or `1`). Thresholds and
warning percentages have no upper cap beyond their types. Separately from
parsing, the PD caps the effective merge ceiling at half the split threshold so
that splits and merges cannot thrash.

### How a key and its alias combine

The same rules apply to the node settings in the next section.

1. Each setting is resolved **once at startup**, before the daemon creates
   directories, opens persisted state or binds listeners. A config key is
   authoritative. When the key is absent, the legacy environment alias is used;
   when both are absent, the table default applies.
2. If both are present with equal parsed values (for example `false` and `0`),
   the value is accepted. Different parsed values are a startup error that
   names the key and the alias. Remove the alias or make it equal; a conflict
   is never silently ignored.
3. Malformed, overflowing or out-of-range input from **either** source is an
   error, even when the other source is valid. Nothing is clamped or silently
   replaced by a default. A low balance ratio above the high ratio is an error,
   not a clamp: if you lower `balance_high_pct` below the default
   `balance_low_pct`, lower the low value too. Errors in the config file carry
   line numbers; key/alias conflicts name both.
4. Startup logs every effective setting and warns about each alias that was
   used as a fallback. `valsd pd --check-config -c FILE` and
   `valsd node --check-config -c FILE` run the same resolution against the real
   environment and print the effective settings without opening persisted state,
   creating files or binding sockets.
5. These are startup-only settings. `SIGHUP` reloads only `log_level` (and, on
   nodes, `sweep`; see [`deploy.md`](deploy.md#config-reload-sighup)); a changed
   startup-only key is logged and ignored. Set the same policy on every PD
   replica so that a leadership change keeps the same behaviour.

If you still rely on an environment alias, copy the value into the config
file, check the result with `--check-config` under the real service
environment, and then remove the alias. Aliases remain accepted; there is no
removal date.

The library entry points (`PdDaemon::spawn*`, `Node::serve_*`) use the built-in
defaults and never read the environment. Applications that need a different
policy pass validated `PdPlacementSettings` or node settings to the
settings-taking variants; `PdPlacementOverrides::resolve` accepts an injected
lookup function for deterministic tests. The in-process `vals-bench` workloads
resolve the legacy aliases once before starting their nodes.

### Mixed versions, capabilities and rollback

These keys change no Raft command, wire format or on-disk format. They also do
not grant capability on their own: `opaque_range_maps = true` still requires a
fresh feature handshake and beacon from **every** endpoint, acknowledged current
maps, and certification of the exact membership/catalog/map epoch tuple. Upgrade
stateless coordinators before enabling it; the PD cannot certify clients it does
not talk to. A stale, missing or old endpoint conservatively gets legacy
placement where that is allowed, and opaque operations are withheld. The
prepare-time revalidation, report/copy/mirror confirmation, concurrency bounds
and placement-debt skew guards are unchanged. `balance_execute` is a separate,
default-off gate and does not bypass opaque readiness.

Older PD binaries reject unknown config keys. Before rolling a binary back,
remove keys it does not know and restore the equivalent environment aliases.
Turning a gate off stops new gated operations; it does not undo committed opaque
maps or an outstanding rehome recovery. Do not downgrade nodes or coordinators
below committed map or wire capabilities, and observe the
[zone rollout rules](zones.md). Rolling back an environment variable never
makes incompatible persisted state readable.

### Run format version

Run files (the engine's sorted index files) have an on-disk version. Current
builds write version 6, which adds per-block version bounds and range-tombstone
records; version 5 has no sidecar file; version 4 is the older sidecar format.
A build refuses runs newer than it understands, so a rollback window needs the
cluster to keep writing the older version. `VALS_RUN_WRITE_VERSION` (read once
at startup) selects the format new runs are written in:

- `6` or unset: version 6 (the default).
- `5`: version 5, for a rollback window to a release that cannot read version
  6. It must be set before the first version-6 run is written. A merge with any
  version-6 input still writes version 6. Range deletes are refused in this
  mode.
- `4`: the sidecar format, for a rollback window to a release that cannot read
  version 5. A merge with any version-5 input writes version 5, and with any
  version-6 input writes version 6.
- Any other value, including an empty string (an unexpanded template variable),
  logs an error and writes version 4.

The `vals_run_write_version` metric reports the active value. See
[deploy.md](deploy.md#on-disk-format-and-compatibility) for the compatibility
rules and [format-history.md](format-history.md) for how the formats changed.

## Node resource and maintenance settings

`valsd node` accepts the following keys. They follow the
[same rules](#how-a-key-and-its-alias-combine) as the PD keys: a key is
authoritative; an absent key can use its legacy alias with a deprecation
warning; equal parsed values are accepted; differing key/alias values or
malformed or out-of-range input fail startup with an error naming the key and
alias. `--check-config` uses the same resolution without binding or creating
files. Older node binaries reject these keys: remove them (and restore any
needed aliases) before rolling back. None of them changes a storage or wire
format.

Settings resolve once at startup. Rehome, maintenance and batch-apply admission
settings are per node instance. RAM, anti-entropy seed policy (permits,
isolation, duty, journal cap), scan admission, transport and admission bands are
process-wide: every node in one process must use the same values. `SIGHUP` does
not reload them; the reloadable `sweep` still retunes the derived (unset)
maintenance intervals, and `log_level` stays reloadable.

| Config key | Legacy alias | Default / behavior | Validation |
| --- | --- | --- | --- |
| `rehome_copy_page_entries` | `VALS_REHOME_COPY_PAGE_ENTRIES` | 4096 | u32 > 0 |
| `rehome_copy_pages_per_tick` | `VALS_REHOME_COPY_PAGES_PER_TICK` | 1 | u32 > 0 |
| `rehome_copy_hold_ms` | `VALS_REHOME_COPY_HOLD_MS` | 60000 | u64 |
| `flush_tail_epochs` | `VALS_FLUSH_TAIL_EPOCHS` | 256 | u64 |
| `flush_max_bytes` | `VALS_FLUSH_MAX_BYTES` | 4294967296 (4 GiB), divided across live engines | u64 |
| `flush_interval_ms` | `VALS_FLUSH_INTERVAL_MS` | absent: clamp(4 × sweep, 100..1000 ms) | explicit u64 >= 100 |
| `run_compact_threshold` | `VALS_RUN_COMPACT_THRESHOLD` | 8: run count at which each maintenance tick asks the engine for a merge plan (no effect when nothing is selectable; merges never bypass the RAM broker) | usize >= 2 |
| `compact_step_segments` | `VALS_COMPACT_STEP_SEGMENTS` | 4 | usize >= 1 |
| `device_dealloc_segments_per_tick` | `VALS_DEVICE_DEALLOC_SEGMENTS_PER_TICK` | 0 (off) | u32; 0 disables provision-time and runtime deallocation; a nonzero value is experimental and not recommended |
| `compact_interval_ms` | `VALS_COMPACT_INTERVAL_MS` | absent: max(20 × sweep, 200 ms) | explicit u64 >= 200 |
| `maint_compact_lanes` | `VALS_MAINT_COMPACT_LANES` | 2 | usize in 1..=4 |
| `cleanup_min_confirms` | `VALS_CLEANUP_MIN_CONFIRMS` | absent: all required replicas (`usize::MAX`); not permission to weaken safety | explicit usize >= 1 |
| `cleanup_idle_burst_ms` | `VALS_CLEANUP_IDLE_BURST_MS` | 1000: once a sweep pass has run a zone's over-replication cleanup page, it keeps running further pages of that zone's traversal while the shard sees no foreground request, until the pass has spent min(this, half the sweep interval) in cleanup across all its zones (each zone still gets its first page); any foreground request, deferral or forget plan ends the burst; 0 restores one page (8,192 keys) per zone per pass | u64 |
| `ram_budget_bytes` | `VALS_RAM_BUDGET_BYTES` | absent: 70% of Linux MemTotal; 0 disables; unavailable MemTotal disables with a warning | u64 |
| `ram_maint_reserve_bytes` | `VALS_RAM_MAINT_RESERVE_BYTES` | absent: budget / 100 × 19; disabled together with RAM admission | u64 |
| `ram_dma_scratch_bytes` | `VALS_RAM_DMA_SCRATCH_BYTES` | 0; charged once against the broker | u64 |
| `ae_seed_concurrency` | `VALS_AE_SEED_CONCURRENCY` | 4; 0 removes the cap; with `ae_seed_isolation=1` seeds driven by a shard's accounting task skip the cap (at most one per shard) but still honour the shed cooldown | usize |
| `ae_seed_shed_cooldown_ms` | `VALS_AE_SEED_SHED_COOLDOWN_MS` | 10000; 0 disables the cooldown | u64 |
| `ae_seed_isolation` | `VALS_AE_SEED_ISOLATION` | 1: the shard accounting task is the only seed driver (AE handlers answer Busy until seeded), in-flight seeds survive split/merge generation bumps, and the seed journal is charged and capped; 0 is the older behaviour, kept for comparison | u8 in 0..=1 |
| `ae_seed_duty_pct` | `VALS_AE_SEED_DUTY_PCT` | 25: share of wall time the shard's seed driver (seed steps and split recounts) may use while the shard has foreground work (a request seen in the last 200 ms, or BatchApply, reads or scans in flight), enforced over a sliding budget rather than per step; full speed when idle; doubles per run-churn restart of the same seed up to 50% (or the configured value if higher); 100 disables the budget | u64 in 1..=100 |
| `ae_seed_journal_max_bytes` | `VALS_AE_SEED_JOURNAL_MAX_BYTES` | 268435456 (256 MiB), node-wide: an exact bound on the bytes reserved by all in-flight seed journals (vector capacity plus key bytes; only a re-entrant journal write during a seed step may overdraw it); a write that cannot reserve room is applied directly to its staged seed instead; 0 removes the cap; ignored when `ae_seed_isolation=0` | u64 |
| `ae_seed_pin_max_bytes` | `VALS_AE_SEED_PIN_MAX_BYTES` | 68719476736 (64 GiB), node-wide: run bytes that in-flight AE seeds may keep allocated after merges or forgets removed them, so a seed continues across merges instead of restarting; the cap is shared in pin order: the oldest pinned seed is never restarted by it, and each younger seed may hold the cap minus the older seeds' bytes (85% of the cap after an overshoot, until the node is back under 85%), checked at each step and at the retiring merge commit; an engine near its space floor revokes its pins regardless; 0 disables pinning (every run removal restarts the seed) | u64 |
| `ae_span_regroup` | `VALS_AE_SPAN_REGROUP` | 1: a re-home (prepare, commit or abort) that changes the replica set of ranges a seeded store holds keys in regroups only those spans' leaf digests in bounded pages; 0 is the older whole-engine leaf invalidation and reseed, kept for comparison | u8 in 0..=1 |
| `transport_max_connections` | `VALS_TRANSPORT_MAX_CONNECTIONS` | 4096 | usize > 0 |
| `transport_max_requests` | `VALS_TRANSPORT_MAX_REQUESTS` | 32768 | usize > 0 |
| `transport_max_queued_bytes` | `VALS_TRANSPORT_MAX_QUEUED_BYTES` | 536870912 (512 MiB) | usize > 0 |
| `transport_initial_timeout_ms` | `VALS_TRANSPORT_INITIAL_TIMEOUT_MS` | 5000 | u64 > 0 |
| `transport_frame_timeout_ms` | `VALS_TRANSPORT_FRAME_TIMEOUT_MS` | 30000 | u64 > 0 |
| `transport_idle_timeout_ms` | `VALS_TRANSPORT_IDLE_TIMEOUT_MS` | 120000 | u64 > 0 |
| `transport_write_timeout_ms` | `VALS_TRANSPORT_WRITE_TIMEOUT_MS` | 30000 | u64 > 0 |
| `scan_max_inflight` | `VALS_SCAN_MAX_INFLIGHT` | 8 | usize > 0 |
| `batch_apply_gate` | `VALS_BATCH_APPLY_GATE` | 64 concurrent staged BatchApply requests per shard; a full gate sheds Busy instead of queueing | usize in 1..=256 |
| `admit_band_pct` | `VALS_ADMIT_BAND_PCT` | 6, relative to region size | u64 in 1..=100 |
| `integrity_fail_fast_attempts` | `VALS_INTEGRITY_FAIL_FAST_ATTEMPTS` | 3: consecutive failed flush attempts of the same in-RAM memtable block before the node fails fast and exits 75; 0 turns off this trigger | u32; 0 or >= 2 (a single mismatch never fails fast) |
| `integrity_fail_fast_secs` | `VALS_INTEGRITY_FAIL_FAST_SECS` | 30: seconds the same block may keep failing (needs at least two attempts) before failing fast; 0 turns off this trigger | u64 |
| `log_level` (node and PD) | `VALS_LOG` | info | off/error/warn/info/debug/trace |

All numeric values must be representable unsigned integers of the indicated
type. `VALS_TRANSPORT_UNBOUNDED` is a diagnostic-only 0/1 switch; setting it to
1 together with any `transport_*` node config key fails startup. The PD's
transport reads the `VALS_TRANSPORT_*` environment variables directly (with the
same strict parsing); it has no `transport_*` config keys. The strict
`log_level`/`VALS_LOG` precedence applies to both daemon roles; other binaries
keep their own logger behaviour. The RAM reserve default is a share of the
budget, not of physical RAM. The default budget is a share of the whole
machine's MemTotal, so set `ram_budget_bytes` explicitly when a node shares its
host with other memory consumers or runs under a cgroup memory limit. Effective
node settings and derived intervals are logged at startup.

`batch_apply_gate` is startup-only. Size it for your client's batch-write
depth, with headroom for shard fan-out and rehome mirrors: a client that keeps
more batch applies in flight per shard than the gate allows gets `Busy`. On
the file backend, a device-durable request gives its permit back once its
writes are applied, while it waits for the group commit's `fdatasync`, so the
gate bounds applies in progress rather than writes waiting for the disk. On
SPDK the request keeps its permit until it is acknowledged.

### Integrity fail-fast

The two `integrity_fail_fast_*` keys are startup-only and set per node. Either
trigger fails the node fast, whichever comes first. Setting both to 0 makes a
corrupt memtable flush retry forever instead. Prefer the config keys; their
`VALS_` aliases exist only because every node key gets one. The policy reacts
only to the in-RAM front-coded memtable guard and to off-core flush
encode-verification failures of the same memtable block. It ignores merge
encode failures, on-disk CRC/corruption errors and recovery failures. Exit code,
supervisor settings and what the operator sees:
[deploy.md](deploy.md#integrity-fail-fast-exit-75).

### Domain flush metrics

These metrics describe the device-sync (domain flush) path and help interpret
the flush settings above.

`vals_domain_flush_latency_us` measures submit-to-publish. The
`vals_domain_flush_device_us` and `vals_domain_flush_publish_us` histograms
split that interval at the SPDK completion callback. Their buckets are
microseconds and their exported `_sum` is **seconds**, like the other latency
histograms. The device interval includes C shim submission retries and the time
until the owning qpair is polled; it is not controller-internal service time.
Immediate submit failures without callbacks are excluded from the split but
remain in the latency histogram and the flush count.

`vals_domain_flush_vlog_writes_inflight` is a summary without quantiles:
`_count` is sampled domain flush submits and `_sum` is outstanding vlog write
operations (not bytes). It scans the submitting shard's local zones in that
controller domain, counting handles whose callbacks have not completed, without
locks or write-path accounting. Other shards and maintenance qpairs are
excluded, so this is not node-wide queue depth.

`vals_domain_flush_busy_us_total` sums submit-to-publication/slot-release
microseconds, including failures and abandoned claims. Compute busy duty with
`rate(vals_domain_flush_busy_us_total[1m]) / 1e6`; this sums over domains and
can exceed 1 on multi-controller nodes. It includes the callback-to-publish
delay, which prevents the next domain flush from starting.

For domain-backed device-sync ingest after draining, divide the deltas of
`vals_synced_writes` and `vals_vlog_write_bytes_total` by `vals_domain_flushes`
for records and vlog bytes per flush. The byte total counts full-block async
writes plus successful partial-tail writes, including headers, padding and tail
rewrites, but not maintenance rewrite streams. Async failures are counted as
attempted bytes. These are workload-level means, not exact durable coverage of
each command: buffered traffic, non-domain syncs, sync fallbacks and
maintenance-driven vlog syncs can skew them. Check errors, stalls and the
workload mix before interpreting the ratios.

## Diagnostic / A-B controls: do not promote to public config

These environment variables exist for experiments and comparisons. They are
not a supported tuning surface, and the defaults below describe the current
code, not a contract.

| Switch | Current default | Reason / source |
| --- | --- | --- |
| `VALS_AE_SEED_TRIM` | on; exactly 0 disables | glibc seed trim comparison, `replica/seed_permit.rs` |
| `VALS_SEED_SCAN_PRIORITY` | 1 (on); numeric 0 disables | seed-versus-scan scheduler comparison, `node/sweep.rs` |
| `VALS_TRANSPORT_UNBOUNDED` | 0; 1 removes limits and deadlines | explicit diagnostic escape, not a supported production resource policy |
| `VALS_MALLOC_TRIM_SECS` | unset or 0: off | node only: a positive value starts a low-priority thread that calls glibc `malloc_trim(0)` every that many seconds; invalid values fail startup. Opt-in for RAM-constrained hosts; it measured as a small throughput loss, so watch RAM and write tails and set 0 if they regress. `vals-server/allocator.rs`; see [deploy.md](deploy.md) |
| `VALS_BATCH_DIGEST_READS` | on; 0/false/off disables | client mixed-batch/scalar comparison, `vals-client/net/client/transport.rs` |
| `VALS_UDS_DIR` | absent/empty means TCP | local transport diagnostic routing override, `vals-client/net/mod.rs` |
| `VALS_MERGE_TIER_K` | 6 (minimum 2) | merge policy experiments, `vals-store/engine/merge.rs` |
| `VALS_MERGE_TIER_MIN_K` | 4, clamped to 2..K | same |
| `VALS_MERGE_FORCE_RUNS` | 6 (minimum 2): runs in the active tail (newer than the newest frozen run) at which the valve forces a capped merge | same |
| `VALS_MERGE_TIER_SIZE_RATIO` | 1.5 (positive) | same |
| `VALS_MERGE_INPUT_CAP_BYTES` | 2147483648 (positive): hard limit on recurring and forced merge input | merge input sizing A-B; may affect reclaim progress, same |
| `VALS_MERGE_FREEZE_BYTES` | the input cap (positive) | runs at or above it are never recurring/forced inputs (frozen top tier); runs too large for a balanced capped window are frozen regardless, same |
| `VALS_VICTIM_DEAD_FRACTION` | 0.5 (0..1) | reclaim selection A-B, same |
| `VALS_SHADOW_EST` / `VALS_SHADOW_FOLD` | on; exactly 0 disables | estimator/fold comparisons, same |
| `VALS_SHADOW_SAMPLE_BLOCKS` | 2 (positive) | estimator sampling, same |
| `VALS_SHADOW_FOLD_MIN_GAIN_SEGS` | 8 (positive) | absolute fold payoff floor; a fold also needs estimated garbage of at least half its window's index segments, same |
| `VALS_MERGE_READAHEAD` | on; exactly 0 disables | merge I/O A-B, same |
| `VALS_MERGE_READ_WINDOW_BYTES` | 2097152 (positive) | merge I/O A-B, same |
| `VALS_MERGE_READ_QD` | 8 (>=1) | merge I/O A-B, same |
| `VALS_VLOG_CO_COMPACT` | off; exactly 1 enables | optional rewrite comparison, same; the benchmark engine workload enables it when unset |
| `VALS_VLOG_GC` | on; exactly 0 disables | GC opt-out / test seam, same; disabling can exhaust capacity |
| `VALS_MAINT_DUTY_PCT` | 33; 0 or >=100 disables pacing | maintenance pacing A-B, `vals-store/pace.rs` |
| `VALS_MAINT_FLUSH_BYTES` | 16777216 (16 MiB) | pacer accounting flush granularity, same |
| `VALS_RUN_BUILD_VERIFY` | on; exactly 0 disables (read once) | pre-publication device readback of streamed merge outputs after their last write completes (no reads from the writer's buffer): every data block's CRC, plus the footer, filter and index CRCs and a byte compare against the encoded copies (see [deploy.md](deploy.md#merge-output-verification)); device-read cost A-B (`vals_run_build_verify_bytes_total`, `_failures_total`), `vals-store/run/build.rs`. Off still CRC-checks footer, filter and index from the device before publication, uncounted |
| `VALS_RUN_BLOCK_TARGET_BYTES` | 32768; clamped to 4096..=262144, non-numeric logs an error and uses the default (read once) | estimated encoded bytes at which new run data blocks close. Trades fence RAM (one first-key copy per block) against bytes read per run probe. New flushes and merges only; old runs keep their block size and mixed sizes are read normally (no format change). Overwrite-heavy workloads pay 15–20% (release build, 20k hot keys) for old-version probes at 32 KiB and may prefer 8192–16384; see [deploy.md](deploy.md#memory-sizing). `vals-store/run/build.rs` |
| `VALS_RUN_BLOOM_BITS_PER_KEY` | 10; clamped to 4..=24 (read once) | per-run blocked-bloom bits per entry for new runs (k = round(0.69 × bits)). Old filters carry their own size and k. Same file |
| `VALS_KEY_FILTER` | on; exactly 0 disables | turns off the engine-wide key filter (RAM-only, rebuilt by cover scans at startup). `vals-store/keyfilter.rs`; see [out-of-core-index.md](out-of-core-index.md) |
| `VALS_KEY_FILTER_MIN_KEYS` | 16777216; 0 means the default | minimum key capacity of the first sub-filter, and the inserted-key floor below which a filter is never considered stale. Same file |
| `VALS_KEY_FILTER_BITS_PER_KEY` | 12; clamped to 4..=18 | bits/key of the first sub-filter of the engine-wide key filter. Each later sub-filter adds one bit up to `VALS_KEY_FILTER_MAX_BITS_PER_KEY`; k = round(0.6 × bits), also for explicit values. Same file |
| `VALS_KEY_FILTER_MAX_BITS_PER_KEY` | 12; clamped to bits_per_key..=18 (so `VALS_KEY_FILTER_BITS_PER_KEY` ≥ 12 alone gives a constant schedule at that value) | cap for the per-sub-filter bit growth above. The default 12/12 is a constant 12 bits/key. Same file |
| `VALS_KEY_FILTER_GROWTH_PCT` | 200; clamped to 110..=400 | legacy capacity growth relative to the previous sub-filter, used only with `VALS_KEY_FILTER_REBUILD=0`. Same file |
| `VALS_KEY_FILTER_REBUILD` | on; exactly 0 disables | small growth steps and an incremental shadow-generation rebuild; 0 restores geometric growth and stale-generation resets. Probes use only the active generation until every run is covered by the shadow generation. RAM-only. Same file |
| `VALS_KEY_FILTER_STEP_PCT` | 20; clamped to 5..=100 | new sub-filter capacity as a percentage of total active-generation capacity in rebuild mode, with a floor of the sink's expected keys. Same rule for the shadow generation. Same file |
| `VALS_KEY_FILTER_REBUILD_SUBS` | 4; clamped to 2..=16 | start a rebuild once the active generation has this many sub-filters (or meets the stale-key condition), all runs are covered, and no cover scan is in flight. Same file |
| `VALS_KEY_FILTER_REBUILD_CONCURRENCY` | 1; clamped to 1..=64 | process-wide maximum simultaneous shadow rebuilds; a permit is released on swap, reset or engine drop. Engines awaiting a permit continue growing and retry on maintenance. Limits overlap count, not aggregate byte size. Same file |
| `VALS_KEY_FILTER_REBUILD_URGENT_FPR_PPM` | 25000; clamped to 1000..=500000; 0 disables | an engine whose estimated key-filter false-positive rate (parts per million) is at or above this counts as urgent and may start a rebuild while the concurrency limit is full. Same file |
| `VALS_KEY_FILTER_REBUILD_URGENT_EXTRA` | 1; at most 8 | how many rebuilds beyond `VALS_KEY_FILTER_REBUILD_CONCURRENCY` urgent engines may add. Same file |

## Incident instrumentation

The SPDK qpair/thread ownership checks in `vals-spdk/src/spdk.rs` are always
on: the `SpdkBlockDevice` `Send`/`Sync` soundness argument rests on them, so
`VALS_SPDK_AFFINITY_CHECK` no longer exists and setting it does nothing.

`VALS_STORE_REBUILD_LOST_MANIFEST` is unset by default. A store whose run
manifest is damaged fails to open with an error naming the manifest
generation, so the node does not start. Setting the variable to exactly `1`
for one restart makes such a store rebuild from its value log instead: every
run is dropped, data and deletes held only by runs are lost, and deleted keys
can come back. Each rebuild logs a
warning and counts in `vals_manifest_rebuilds_total`; stores with an intact
manifest are unaffected. On a replicated zone, prefer replacing the node so its
replicas are copied from healthy peers. Unset the variable once the node is
back: while it is set, a later manifest loss rebuilds instead of stopping the
store. See
[formats.md §2.4](formats.md#24-run-manifest-vman--vals-store).

## Test, benchmark and build-only inputs

These do not configure `valsd`. Their defaults live in the harness or build
sources, and they will not become daemon keys.

- `vals-bench/cluster_live.rs`: `VALS_DISTNODE_SWEEP_MS` (2000),
  `VALS_DISTNODE_GRACE_MS` (60000), `VALS_DISTNODE_CHURN_SECS` (20),
  `VALS_DISTNODE_HEAL_SWEEPS` (15), `VALS_DISTNODE_REB_SWEEPS` (20),
  `VALS_DISTNODE_REGION_MIB_MEM` (128), `VALS_DISTNODE_REGION_MIB` (2048).
- `cluster_passive.rs`: `VALS_READ_FANOUT` (digest), `VALS_DS_RF` (3),
  `VALS_DS_R` (2 capped by RF), `VALS_DS_NRANGES` (0 means derived layout),
  `VALS_BENCH_BOUNDED_CLIENT` (absent selects explicit-unbounded;
  presence selects finite-default in bounded-client workloads).
- `common.rs` / `engine.rs` / `spdk_device.rs`: `VALS_HEDGE_US` (no delay),
  `VALS_SLOW_NODE` (no injected delay; node:microseconds[:all]),
  `VALS_SCAN_REPEAT` (1 workload pass), `VALS_PIN` (empty).
- `capacity.rs`: `VALS_CAPACITY_OFFERED_OPS` (20000),
  `VALS_CAPACITY_LATE_US` (10000), `VALS_CAPACITY_SEED_INTERFERENCE` (absent).
- `seed_mass.rs`: `VALS_SEED_MASS_DEADLINE_MS` (120000),
  `VALS_SEED_MASS_SETTLE_MS` (0), `VALS_SEED_MASS_TRIM` (absent).
- `netload.rs` / benchmark main: `VALS_NETREAD_FANOUT` (digest),
  `VALS_NETMULTIREAD_BATCH` (32), `VALS_NETBATCHWRITE_WIDTH` (1),
  `VALS_NETBATCHWRITE_PERSISTENCE` (buffered; device changes benchmark requests),
  `VALS_NETREAD_SECS` / `VALS_NETWRITE_SECS` / `VALS_NETWRITE_RATE` (0),
  `VALS_NETLOAD_KEY_PREFIX` (nl), `VALS_NETLOAD_SKIP_READ_PHASE` (absent),
  `VALS_NETREAD_LAT_SAMPLE_EVERY` / `VALS_NETWRITE_LAT_SAMPLE_EVERY` (0),
  `VALS_NETREAD_LAT_SAMPLES_FILE` / `VALS_NETWRITE_LAT_SAMPLES_FILE` (absent),
  `VALS_NET_TRANSPORT_STATS` (absent).
- `profilecheck.rs`: `VALS_PROFILECHECK_ZONE` (default zone),
  `VALS_PROFILECHECK_STATE` / `VALS_PROFILECHECK_VERIFY_ONLY` (absent),
  `VALS_PROFILECHECK_ALLOW_DIRTY_RESUME` (off; exact 1 permits reconciliation),
  `VALS_PROFILECHECK_SEED_KEYS` (0), `VALS_PROFILECHECK_WORKLOAD_KEYS` (25000),
  `VALS_PROFILECHECK_GC_GRACE_SECS` (900), `VALS_PROFILECHECK_CLOCK_SKEW_MS` (1000),
  `VALS_PROFILECHECK_CONNECT_DEADLINE_SECS` (5),
  `VALS_PROFILECHECK_RPC_DEADLINE_SECS` (15),
  `VALS_PROFILECHECK_DIRECT_RETRY_SECS` (5),
  `VALS_PROFILECHECK_COORD_RETRY_SECS` / `VALS_PROFILECHECK_VERIFY_RETRY_SECS` (300),
  `VALS_PROFILECHECK_VERIFY_CONVERGE_SECS` / `VALS_PROFILECHECK_NODE_CONVERGE_SECS` /
  `VALS_PROFILECHECK_MAX_NODE_OUTAGE_SECS` / `VALS_PROFILECHECK_CHECKPOINT_SECS` (900),
  `VALS_PROFILECHECK_VERIFY_POLL_SECS` (10), `VALS_PROFILECHECK_SAMPLE_KEYS` (64),
  `VALS_PROFILECHECK_NODE_SAMPLE_KEYS` (16), `VALS_PROFILECHECK_SAMPLE_EVERY` (32),
  `VALS_PROFILECHECK_SEED_BATCH` (256 capped at 4096),
  `VALS_PROFILECHECK_MAX_MODEL_RSS_MIB` (4096),
  `VALS_PROFILECHECK_MAX_PENDING_KEYS` (256), `VALS_PROFILECHECK_MAX_RECONCILES` (65536),
  `VALS_PROFILECHECK_PROBE_NODE` (absent), `VALS_PROFILECHECK_PROBE_SECS` (20),
  `VALS_PROFILECHECK_GC_PROBE_SECS` (0, off), `VALS_PROFILECHECK_GC_PROBE_KEYS`
  (64), `VALS_PROFILECHECK_GC_OVERDUE_SECS` (2 × min(gc_grace, 3600 s) + 300),
  `VALS_PROFILECHECK_GC_MAX_HELD_PCT` (100).
  The queue workloads (`netqueue`, `rtqueue`) and the graph workloads read
  their own `VALS_NQ_*`, `VALS_RTQ_*` and `VALS_GRAPH_*` knobs; see the
  workload sources.
- Test child selectors (all absent by default): `VALS_CLUSTER_ENV_TEST_CHILD`,
  `VALS_STORE_ENV_TEST_CHILD`, `VALS_TRANSPORT_BOUNDS_TEST_CHILD`,
  `VALS_TRANSPORT_CREDIT_TEST_CHILD`, `VALS_ADMIN_TRANSPORT_TEST_CHILD`,
  `VALS_TEST_ABORT_CHILD`, `VALS_PROFILE_DEGRADE_TEST_CHILD`,
  `VALS_SEED_GROW_TEST_CHILD`, `VALS_FAIL_FAST_CHILD`, `VALS_HEAP_TEST_CHILD`.
  `VALS_SIM_SEED` selects a deterministic netsim seed, otherwise the test uses its
  built-in default. `VALS_SIM_SEEDS` sets how many seeds the sorted-zone GC
  simulation (`netsim_gc_tests`) runs (2 by default; raise it for a long sweep).
  Tests also use the real switches above in isolated subprocesses.
- Build inputs: `VALS_SPDK_LINK` (absent means no SPDK linking) and
  `VALS_SPDK_DIR` (the build script's default is `third_party/spdk`). They are
  not startup settings. C `VALS_*` defines are constants, not environment
  switches.

`VALS_STAGED_SCAN` and `VALS_SCAN_PREFETCH` were removed and are not supported
aliases.

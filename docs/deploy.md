# Deploying vals with `valsd`

`valsd` is the vals server daemon: one binary, three subcommands.

```sh
valsd node  -c /etc/vals/node.conf    # serve a data node
valsd pd    -c /etc/vals/pd.conf      # serve the placement driver
valsd admin -s /run/vals/pd.sock ...  # administer a running PD
```

Build it with `cargo build --release -p vals-server` (add `--features spdk`
with `VALS_SPDK_LINK=1` for raw-NVMe nodes; see [`SPDK.md`](SPDK.md)).
`SIGTERM`/`SIGINT` shut a daemon down gracefully — drain reads, flush
connections, sync every engine, persist the liveness watermark / raft state —
so a rolling restart is recoverable by construction. `SIGHUP` re-reads the
config file and applies the reloadable subset live (see [Config reload
(SIGHUP)](#config-reload-sighup)). Logs go to stderr; the level is set by
`log_level` in the config (reloadable) or, if absent, `VALS_LOG=info|debug|...`.

## Configuration scope

Use the node/PD config references below and the [supported settings and legacy
alias policy](configuration.md). Node resource and maintenance keys are startup-only;
`--check-config` resolves the real environment without side effects.
An environment variable found in source or an old benchmark command is not by itself
a supported deployment interface. Do not copy whole experiment environments into a
production unit: rollback switches and diagnostic controls can change correctness,
resource bounds, and placement behavior. Record effective overrides with each run.

`VALS_STAGED_SCAN` and `VALS_SCAN_PREFETCH` are retired. Server scans always use
staged selection, retaining the bounded-restart synchronous progress fallback.
Client prefetch defaults on; an embedding application can still explicitly call
`Coordinator::set_scan_prefetch(false)`.

The remaining startup-configuration consolidation is tracked by task 0265 in the
[backlog](../backlog.md). This cleanup does not claim that every legacy environment
parser validates malformed input or has been migrated into daemon configuration.

## Transport resource bounds

Every daemon uses one process-wide transport budget, including all data shards and
listeners. Defaults are 4,096 accepted inbound connections, 32,768 decoded or
queued requests, and 512 MiB of charged transport storage. Data input `Vec`
capacity, executing requests at their encoded frame size, retained data output
`Vec` capacity, admin command/output storage, metrics output, and encoded PD inbox
messages all charge the byte budget. A connection must complete its initial
protocol frame in 5 seconds; a partially received frame has 30 seconds to complete,
and stalled output has 30 seconds to make progress; an established idle connection with no executing
request is closed after 120 seconds. Clients (the Rust client and valsgo) resend
an idempotent read once on a freshly dialed connection when the reused, idle
connection it went out on fails before answering (the Rust client counts these in
`idle_conn_retries`); writes are never resent. Credits are released when storage is freed or
work completes, fails, times out, or is cancelled. The wire protocol is unchanged:
data requests refused at the request budget receive the existing retryable `Busy`
response; connections refused before protocol service or unable to reserve bytes
are closed.

The budget covers data TCP/optional UDS listeners, every data shard, the PD raft
listener and its decoded-message inbox, the metrics HTTP listener, and node/PD
admin Unix sockets. Metrics serves one scrape at a time and has the same initial
and write deadlines. Each admin connection reserves its 64 KiB command cap before
reading, and keeps that byte credit and its request credit until processing ends;
admin response credit is acquired before the response enters its channel. The
admin initial deadline is absolute, so byte-at-a-time input cannot extend it. PD
raft frames are not subject to the 64 MiB data protocol cap: they are accepted only
when the declared size can reserve the shared byte budget, preserving full-state
snapshot framing up to that configured budget. PD outbound peer writes retain
their tighter 250 ms deadline.

Kernel listen backlogs, outbound client/coordinator connection pools, each data
connection's fixed 32 KiB read scratch, and temporary allocations used while
encoding data/admin/metrics responses or decoding a charged PD frame are not
charged as queued bytes. Those allocations are respectively bounded by the
connection/request counts and protocol/application response behavior, rather than
byte-for-byte by the queued-byte counter. Retained data input and output are
charged by allocation capacity (not logical length); completed output buffers are
freed rather than retained at their high-water capacity.

The defaults can be lowered or raised before process start with
`VALS_TRANSPORT_MAX_CONNECTIONS`, `VALS_TRANSPORT_MAX_REQUESTS`,
`VALS_TRANSPORT_MAX_QUEUED_BYTES`, `VALS_TRANSPORT_INITIAL_TIMEOUT_MS`,
`VALS_TRANSPORT_FRAME_TIMEOUT_MS`, `VALS_TRANSPORT_IDLE_TIMEOUT_MS`, and
`VALS_TRANSPORT_WRITE_TIMEOUT_MS`. Values must be positive integers. Development
and benchmark runs that intentionally need the old unbounded behavior may set
`VALS_TRANSPORT_UNBOUNDED=1`; this disables all seven limits and overrides their
individual variables. The effective setting is logged at startup and is stable
for the process lifetime.

`VALS_SCAN_MAX_INFLIGHT` limits concurrent network scans per shard thread (default 8).
It must be a positive integer; invalid values fall back to 8. Requests over the limit
receive retryable Busy without entering the scan, counted by `vals_scan_admission_busy`.

Prometheus exports cumulative rejection counters
`vals_transport_connection_rejections`, `vals_transport_request_rejections`, and
`vals_transport_queued_byte_rejections`, plus
`vals_transport_initial_timeouts`, `vals_transport_frame_timeouts`,
`vals_transport_idle_timeouts`, and `vals_transport_write_timeouts`. These are
resource/lifetime controls only. They provide no authentication, encryption, or
authorization and must not be used as a substitute for the cluster secret or
network access controls.

The finite defaults passed the matched SPDK fleet adoption gate in task 0277 on
the two reachable RF=2 nodes: write/read throughput changed -1.08%/+1.61%, the
new response/output/accounting costs remained subdominant in CPU profiles, all
normal rejection/timeout counters stayed zero, full-value `kvchurn` passed, and
a low-limit abusive arm bounded resources while healthy writes progressed. See
[`../bench/0277-hardware-20260908/FINDINGS.md`](../bench/0277-hardware-20260908/FINDINGS.md)
for tails, profiles, exact binaries, and the deliberate lack of three-node
coverage. Local regression tests alone still do not establish hardware adoption.

## A three-node cluster in five minutes

One box, three data nodes + one PD (the same shape the end-to-end test
deploys). This is an explicitly non-HA development/test exception: changing the
addresses spreads the data nodes across machines, but does not make its one PD
highly available. Real and reference deployments use the three-PD posture below.

```sh
mkdir -p /tmp/vals && cd /tmp/vals
echo "demo-secret" > secret

for i in 1 2 3; do
  cat > node$i.conf <<EOF
node_id  = $i
listen   = 127.0.0.1:700$i
data_dir = node$i
$(for p in 1 2 3; do [ $p != $i ] && echo "peer = $p @ 127.0.0.1:700$p"; done)
secret_file = secret
EOF
done

cat > pd.conf <<EOF
pd_id     = 1
data_dir  = pd
data_node = 1 @ 127.0.0.1:7001
data_node = 2 @ 127.0.0.1:7002
data_node = 3 @ 127.0.0.1:7003
rf        = 3
zone      = events 3 sorted
admin_socket = pd.sock
secret_file  = secret
EOF

valsd node -c node1.conf & valsd node -c node2.conf & valsd node -c node3.conf &
valsd pd -c pd.conf &

valsd admin -s pd.sock ping
valsd admin -s pd.sock members
valsd admin -s pd.sock zones
```

Validate either daemon configuration before a restart with `valsd node -c
node1.conf --check-config` or `valsd pd -c pd.conf --check-config`. Check mode is
strict and side-effect-free: it reports effective settings and restart-only
fields without binding sockets, creating state directories, campaigning, or
attaching/formatting devices. For SPDK it also reminds the operator that any new
layout must be created separately with the offline `valsd provision` workflow.

PD check mode validates only the configuration file; it cannot certify an existing
raft catalog. Before upgrading, use the currently running release's `members` and
`zones` commands to inspect and repair old state that would violate the membership,
RF, scope, or unique-name rules below. Retain the old compatible binary for that
repair until the new release has started successfully, but only use it before the
first PD mutation written by the new release; do not downgrade a catalog after a
new-format mutation.

The PD atomically raft-commits the complete initial boot membership (`data_node`
entries; weight 1 unless a per-node `*<weight>` is given — see below) and then
commits boot zones, before pushing the ring +
catalog to every node, retried until confirmed. Clients connect with a
`Coordinator` over `NetTransport` exactly as in
[`getting-started.md`](getting-started.md) §Clients.

A `data_node` entry may carry an explicit ring **weight**:
`data_node = <id>[*<weight>] @ <addr>[,<addr>...]` (weight defaults to 1, must be
>= 1). The weight routes keys ∝ capacity — e.g. give an SPDK JBOD node its
drive-count weight so it takes proportionally more of the keyspace than a
single-drive peer:

```ini
data_node = 1        @ 127.0.0.1:7001   # weight 1 (default)
data_node = 2*4      @ 127.0.0.1:7002   # weight 4 (a 4-drive JBOD node)
```

## Node config reference

```ini
node_id   = 1                  # required: this node's cluster id
profile   = capacity           # capacity (default) | latency; restart-only
listen    = 0.0.0.0:7000       # required: repeatable — one per shard (spdk)
backend   = file               # file (default) | mem | spdk
data_dir  = /var/lib/vals/n1   # required for file: zones + liveness log
peer      = 2 @ host:7000      # repeatable: boot peers (PD extends at runtime)
rf        = 3                  # boot-ring RF (PD's pushed config overrides)
sweep     = 5s                 # anti-entropy cadence (reloadable via SIGHUP)
gc_grace  = 0s                 # >0 = tombstone GC + the liveness lease
secret_file = /etc/vals/secret # or: secret = <literal>
metrics   = 127.0.0.1:9464     # optional: Prometheus scrape endpoint
admin_socket = /run/vals/n1.sock # optional: live-node admin socket (ping, dump)
log_level = info               # optional: overrides VALS_LOG (reloadable via SIGHUP)

# file/mem engine geometry
block_size = 4096
seg_blocks = 256
region_mib = 1024              # per-zone region cap

# spdk (backend = spdk; needs the --features spdk build)
pci    = 0000:a1:00.0          # repeatable: one per drive
```

Notes:

- **file/mem** nodes serve **zoned** (the PD's `create-zone`s materialize
  lazily) on exactly one `listen` address. `mem` loses everything on exit —
  it exists for tests and throwaway clusters.
- **`rf` cannot exceed the node's configured boot membership (the node itself plus
  `peer` entries), even when a PD later manages membership.** This prevents the node
  from starting with an implicitly under-replicated ring before PD state arrives.
- **`sweep` must be greater than zero.** Anti-entropy and its maintenance cadence
  cannot be disabled by accidentally disarming the timer.
- **`gc_grace = 0s` explicitly disables tombstone GC and the liveness lease.**
  **gc_grace > 0** turns on tombstone GC *and* the wipe-before-rejoin liveness
  lease (`docs/../ARCHITECTURE.md` §Consistency). Use the same value on every
  node. A node that rejoins after being out longer than the grace wipes and
  re-syncs instead of resurrecting GC'd deletes. Supported on all backends; on
  `backend = spdk` the watermark persists **on-device** (on the zoned superblock),
  so no `data_dir` is needed — the SPDK single-drive path serves the full zone
  catalog and honors the lease per zone, the multi-drive JBOD path persists one
  watermark per partition superblock.
- **spdk** single-drive nodes serve the **full zone catalog** (named + sorted
  zones, lazily materialized) over one NVMe namespace — the raw-NVMe analogue of
  the file backend. spdk **multi-drive** (JBOD) nodes serve one partition per
  `listen` address, spread across the `pci` drives (see
  [`multi-drive.md`](multi-drive.md)) and serve the full named + sorted zone
  catalog (one set of per-zone engines per partition). Both honor the
  liveness lease. Either way the node logs its computed **ring weight** at
  startup — pass it to `data_node = <id>*<weight> @ ...` (or
  `valsd admin add-node <id> <weight> <addrs>`) so placement matches capacity.
- Daemon startup never formats SPDK devices. It opens an existing compatible
  layout and refuses blank, incompatible, or incompletely provisioned layouts.
  SPDK startup checks the persisted topology, `listen` count (partition count),
  node id, partition extents, and segment/superblock geometry before any engine can write.
  Changing these requires offline reprovisioning with `valsd provision`, which
  destroys the old data. Layouts provisioned before the V3 topology descriptor
  must also be reprovisioned; startup and checked offline tools refuse legacy layouts.
  A legacy `format = false` line is ignored to ease config migration;
  `format = true` is rejected with migration guidance.

## Offline SPDK provisioning

Provision a new or deliberately disposable SPDK node only while its daemon is
stopped. Record each physical controller's exact serial number independently,
then name every configured PCI address and serial on the command line:

```sh
valsd provision -c /etc/vals/node1.conf \
  --device '0000:a1:00.0=SERIAL_FROM_CONTROLLER_LABEL_OR_INVENTORY' \
  --confirm-destroy node-1
```

Repeat `--device` once for every `pci` line. The command requires an exact set
match with the node config, attaches and holds every controller before writing,
reads each attached controller serial through SPDK, and compares it byte for
byte. A missing/extra identity, serial mismatch, wrong node confirmation,
attach failure, or invalid layout aborts before layout writes. Successful
provisioning creates the on-device layout and default zone without binding
listeners or launching node services. The SPDK controller claim provides
exclusive ownership for the command lifetime; if the daemon or another SPDK
process owns any configured controller, provisioning fails.

After all serial identities are verified, and only when device deallocation is
enabled (see below; off by default), provisioning deallocates the **whole
namespace of every configured drive**, including space outside VALS regions,
using bounded NVMe DSM commands before writing the fresh layout. A deallocation
error aborts provisioning; unsupported devices log a warning and continue with
the header wipe. The file backend does not deallocate.

At runtime, freed value-log and run segments become eligible for deallocation
only after a subsequent manifest is durably written. Reused segments are removed
from the pending set. Bounded synchronous batches run on the shard maintenance
tick, not on the write fast path; deallocation errors are advisory and do not
poison the engine. Pending deallocations live in memory only: segments freed
shortly before a restart are not backfilled after reopen, and are deallocated
only once they are reused and freed again. `device_dealloc_segments_per_tick` defaults to **0**,
which disables both provision and runtime deallocation. A nonzero value opts in
and bounds runtime deallocation to that many segments per zone per
flush/snapshot tick (restart required). Enabling it is **experimental**: on
hardware (task 0353, `bench/0353-dealloc/FINDINGS.md`) it gave no throughput
gain, and the only long run with runtime deallocation failed correctness
checks; do not enable it in production until task 0355 resolves the cause.
Process-wide `vals_device_dealloc_*`
metrics report commands, ranges, successful bytes, errors, unsupported attempts,
and total latency.

This operation destroys all prior data on the configured namespaces. Remove
`format` from the node config and start normally only after the command reports
success. Re-provisioning follows the same offline workflow; there is no
startup-time format switch.

## Data-node deployment profiles

`profile` selects a supported, coupled write/index policy. It is process-wide,
restart-only, and logged before any zone engine opens. The Prometheus surfaces
`vals_profile_info` reports the selected/requested profile;
`vals_blind_append_enabled` and `vals_winner_directory_enabled` report the
effective process state.

| Profile | Unconditional Apply/BatchApply | Point index | Open/recovery |
|---|---|---|---|
| `capacity` (default) | Resolves the current maximum version before acknowledging; a flushed positive may read a run block. | Out of core. | Replays the tail and opens run metadata without building an O(dataset) RAM directory. |
| `latency` | Blind-appends an admitted LWW candidate and returns optimistic `Won`; skew, GC-floor, admission, append, and requested-sync boundaries are unchanged. | Compact collision-safe winner directory. | Synchronously scans all recovered runs and the replayed memtable before serving; failure aborts engine open. |

The latency profile couples both features deliberately; there is no supported
half-profile. Its directory is not free. On the retained mature hardware corpus
the measured process RSS increment was **62.9 bytes per physical key**. Size
headroom from that evidence plus the rest of the process RAM budget, key-count
growth, zones, shards, caches, and allocator slack. `vals_winner_directory_keys`
and `vals_winner_directory_bytes` expose logical coverage and actual allocated
directory capacities; the latter is not a complete RSS measure. Open rebuild is
**O(dataset)** in device reads and CPU, so include it in restart and rolling
maintenance budgets.

Both profiles use the same wire and on-disk formats and replicas may be mixed
during a rolling rollout. Replica-level clients must count both `Applied::Won`
and `Applied::Lost(_)` as unconditional-write acknowledgements. In latency mode
`Won` means "candidate accepted" rather than "proved logical winner"; only a
maximum-version read determines the winner. CAS remains resolving.

Switching a current binary from latency back to capacity is supported with a
restart: corrected readers/recovery still select maximum `Version`, and the
resolving writer stops adding unresolved losers. **Downgrading to an older
binary is unsafe** after any blind candidate may have been accepted, because
old mutable/newest-run precedence can expose a loser. Before such a binary
downgrade, perform and verify full canonicalizing compaction to one winner per
key with no uncovered tail, or rebuild the node from peers. A normal profile
switch does not require an on-disk rewrite. Roll out latency one replica at a
time, watch correctness and the metrics below through restart, then proceed:

- write probes: `vals_apply_run_reads`, `vals_batch_apply_run_reads`,
  `vals_apply_probe_runs`, and `vals_apply_probe_blocks` (approximately zero for
  latency-profile unconditional writes);
- directory size: `vals_winner_directory_keys` and
  `vals_winner_directory_bytes`;
- logical repair: `vals_ae_dirty_buckets`, oldest dirty age, rebuild
  requests/retries/errors, AE sweeps/errors;
- space/recovery: used/free segments, resident runs, merges, terminal/retryable
  admission errors, process RSS/swap, and restart duration.

A runtime winner-directory rebuild failure degrades the whole process fail
closed: the failing engine drops its directory, effective blind append switches
to resolving writes for all engines and all already-established connections on
their next decoded request, and both effective gauges become `0` while
`vals_profile_info{profile="latency"}` remains `1`. This state is permanent until
process restart; a successful rebuild in another engine cannot clear it.
Out-of-core maximum-version reads remain authoritative. Alert whenever a
latency-selected process has either effective gauge at zero, investigate the
rebuild/device error, and restart after correcting it; open recovery must rebuild
every requested directory successfully before serving.

Capacity remains the explicit default. The retained latency benchmarks prove a
substantial read/overwrite benefit, but the RAM and synchronous-open costs make
silent default promotion inappropriate. In particular, use **capacity** for a
large, predominantly unique bulk load unless a measured overwrite/read workload
justifies latency: at RF=2, 24 billion logical keys create about 48 billion
physical keys, so the latency directory's measured increment alone is about
3.0 TB cluster-wide (before the base index, memtables, caches, and allocator
slack), and every restart must rebuild it over the dataset. Capacity's fresh-key
bloom-negative path needs no run read; verify that assumption with the write-probe
metrics above. See [`blind-append-lww.md`](blind-append-lww.md),
[`out-of-core-index.md`](out-of-core-index.md), the 24B ATProto sizing pass in
[`../bench/task-0200-atproto-backfill/FINDINGS.md`](../bench/task-0200-atproto-backfill/FINDINGS.md),
and the repeatable mixed-profile validation in
[`../plans/mixed-profile-soak-runbook.md`](../plans/mixed-profile-soak-runbook.md).

## Range deletes (task 0439)

Sorted zones support range deletes (`delete_range_in`, [`range-deletes.md`](range-deletes.md),
API in [`getting-started.md`](getting-started.md)). Zones that never use them pay
nothing measurable: the write, read and scan paths and the resident fence memory
(+8 B per run block, the v6 `max_hlc`) were checked against the pre-feature build
(`bench/0449-rt-hot-path/FINDINGS.md`).

- **Rollout.** Range deletes need hello feature bit 3 on every replica of the
  span. A coordinator refuses the call (`Unsupported`, before stamping a version)
  while any target replica lacks it, so upgrade every node first. Clients without
  bit 3 (`valsgo` and `bootes` built before 0468) keep working on zones without
  range tombstones; a scan of theirs whose window meets one gets `Err("range tombstones present: client
  lacks RANGE_TOMBSTONES")` rather than covered keys.
- **Downgrade fails closed.** Before an engine stores its first range tombstone it
  commits a v6 run, and every later manifest keeps at least one. A pre-0451 binary
  therefore stops at run open (`unsupported run version`) instead of replaying a log
  record kind it does not know. Development builds between 0451 and this release
  know v6 runs but not every range-delete rule; they are not downgrade targets for a
  node that has accepted a range delete.
- **Rollback window.** A node started with `VALS_RUN_WRITE_VERSION=5` (or `4`)
  refuses range deletes with a per-slice `Err` (counted in
  `vals_range_delete_refused_total`), so it never needs the v6 fence. Once a v6 run
  exists the window is closed as described below.
- **Backups and dumps.** `valsd backup create` writes zone files as version 2,
  which pre-0448 builds refuse ([`backup-restore.md`](backup-restore.md)).
  `valsd dump` writes `VDUMP\x02` only for a zone holding range tombstones
  ([below](#bulk-vlog-export-valsd-dump)); `valsdump replay` reads `\x02` since
  0468 (earlier builds refuse it).
- **Fragment growth.** With `gc_grace = 0` (the default) fragments are kept
  forever, like point tombstones, up to 65,536 per zone engine (fixed); past that
  a slice is refused (`vals_range_delete_refused_total{reason="cap"}`).
  Queue-style deletes of a growing prefix `[base, cursor)` collapse into one
  fragment; deletes of disjoint spans add one each. There is no fragment-count
  gauge yet.
- **Metrics.** `vals_range_deletes_applied_total`,
  `vals_range_delete_refused_total`, the fold counters `vals_rt_fold_*`, the scan
  counters `vals_scan_rt_*`, `vals_rt_max_tree_built_blocks_total`,
  `vals_rt_covered_dropped_total`, `vals_rt_collected_total`,
  `vals_rt_dominated_dropped_total`, `vals_ae_rt_exchanges_total` and the
  per-engine record gauge `vals_engine_rt_records` (range-deletes.md §8).

## Run format v6 (task 0451)

New flushes and merges write **run format v6**. It adds a per-block version bound
(`max_hlc`, 8 B per block, resident with the fence index) and room for
range-tombstone records (docs/range-deletes.md); with no range tombstones it is
otherwise the v5 layout. Merges rewrite v5 inputs as v6.

- **Upgrade.** No action is needed; v4 and v5 runs stay readable.
- **Downgrade fails closed.** A pre-0451 binary rejects a v6 run footer
  (`unsupported run version`) before log replay, so a zone holding a v6 run will
  not open on it, as for v5 below.
- **Rollback window.** Start the new binary with `VALS_RUN_WRITE_VERSION=5`
  *before* any v6 run is written to keep writing v5. A merge with any v6 input
  still writes v6, so once a v6 run exists downgrade is closed until the node is
  rebuilt from peers. Range deletes are refused while runs are written as v5.

## Run format v5 (task 0365)

New flushes and merges write **run format v5**, which drops the per-run sidecar
(a second copy of every key and version). AE seeds, digest pages and cover-scans
read the same `(key, version)` stream from the run's data blocks. This saves
about 20 B plus the full key per entry stored and written. Older v1–v4 runs stay
readable, and merges convert them to v5.

- **Upgrade.** No action is needed. Mixed-format engines are supported; the
  `sidecar_disk_bytes` capacity statistic falls to zero as merges rewrite old runs.
- **Downgrade fails closed.** A pre-0365 binary rejects a v5 run footer
  (`unsupported run version`), so any zone holding a v5 run will not open.
  Verified (0371, file-backed, pre-0365 build 448ef977 against a v5 image written
  by the new build): `valsd node` logs `node 1 failed to start: corrupt snapshot:
  unsupported run version` and exits 1. Run footers are read before log replay,
  so the engine issued 514 reads and no writes or flushes; the zone file was
  byte-identical afterwards and no liveness or catalog files were created. The
  node can be restarted on the new binary with no repair step.
- **Rollback window.** To keep downgrade possible, start the new binary with
  `VALS_RUN_WRITE_VERSION=4` *before* any v5 run is written (it is read once at
  startup; `vals_run_write_version` shows the active value). New runs then keep
  their sidecars.
  - The setting does not convert existing v5 runs: a merge with any v5 input still
    writes v5, because its output reservation is sized from its inputs.
  - Once you unset it (or set it to 5), v5 runs appear and downgrade is closed
    until the node is rebuilt from peers.
  - Values other than `4`/`5`, including an empty string (an unexpanded template
    variable), log an error and write v4, so a misconfiguration keeps the rollback
    window open rather than closing it.
- **Build verify.** Before a streamed v5 merge output is published, the whole run
  is read back from the device and checked. This replaces the old sidecar-build
  readback. It is on by default; `VALS_RUN_BUILD_VERIFY=0` turns it off (read once).
  - **What is checked (task 0375).** The check starts only after every write of
    the run has completed, including the final partial segment. Every read goes
    to the device, and nothing is served from the writer's RAM buffer. Before
    0375, the final segment was checked from that buffer, so its device write
    was not verified (the 0371 caveat). The run's usual device flush still
    precedes the check, but that flush is about durability: reads after it may
    still come from the controller's cache.
    - Each data block's CRC is checked.
    - The footer, filter and index are CRC-checked. The CRC catches a bit flip
      that happens after the CRC was computed, whether in the encode buffer,
      the DMA staging buffer, the link or the device.
    - The footer, filter and index are also byte-compared with the encoded copies
      the builder wrote. This mainly adds detection of lost or stale tail writes
      and of a footer re-sealed with a valid CRC.
    - The published in-memory index and filter are decoded from these verified
      device bytes.
  - **What is not checked.** The check sees what the device returns right after
    the write. Media corruption that appears later is not covered, and neither
    is a bit flip in an entry before it is encoded (the CRC is then computed over
    the flipped bytes). v4 runs (`VALS_RUN_WRITE_VERSION=4`) keep the
    sidecar-build readback instead. Flushes and the synchronous full merge
    (`merge_all_runs`) don't re-read data blocks; they only get the footer, filter
    and index checks below.
  - **Footer, filter and index CRCs outside the switch.** Async flushes, and
    async merges with the switch off, CRC-check the footer, filter and index
    from the device before publication, as they did before 0375. Sync flushes
    and full merges (`merge_all_runs`) used to run that check after the manifest
    write. 0375 moves it before the manifest write, so a failing run is not
    published and its segments are freed. These checks are always on and are
    not counted in the metrics below.
  - **Cost.**
    - Data-block bytes read are the same as before 0375, except that the data
      blocks in the final segment (at most one segment) are now read from the
      device.
    - The footer, filter and index were already read back before publication.
      The byte compare, including freeing the encoded copies, adds about
      0.3–0.45 ms per 3.5 MiB of tail. That is about 10–15 ms for a FULL9-sized
      2.1 GB backlinks run with a ~113 MiB tail (release micro, backlinks-like
      run with the index at about 5% of the run).
    - Transient memory: the tail readback holds the device copy of the footer,
      filter and index (about 5% of the run) and the decoded index. The builder's
      own index and filter are freed before the readback starts. Each encoded
      copy is freed right after its compare, before that section is decoded. So
      the peak is no higher than the post-build readback before 0375.
  - `vals_run_build_verify_bytes_total` counts the bytes checked: data blocks
    plus footer, filter and index.
  - `vals_run_build_verify_failures_total` must stay 0. A failure aborts the
    merge: nothing is published, the reserved segments are released, the inputs
    stay live, and the merge is retried on the next maintenance tick.

## Run blocks, run blooms and key-filter bits (task 0378)

New flushes and merges write **32 KiB run blocks** (was 4 KiB) and **10-bit/key
run blooms** (was 12). The engine-wide key filter uses **12 bits/key in every
sub-filter, doubling capacity** per growth step (was 13 bits growing one bit per
sub-filter to 18, at 1.5× capacity). This cuts resident fence and filter RAM per
key; the [configuration](configuration.md#diagnostic--a-b-controls-do-not-promote-to-public-config)
table lists the knobs.

- **No format change.** Each run's index stores every block's length, and each
  bloom stores its own size and probe count. Old 4 KiB / 12-bit runs stay
  readable next to new ones, and merges rewrite them with the new settings.
  Downgrade is unaffected: a pre-0378 binary reads 32 KiB-block runs (the run
  format is still v5). The key filter is RAM-only and is rebuilt at startup.
- **Upgrade.** No action is needed. Resident fence bytes fall as merges rewrite
  old runs. Watch `vals_run_fence_resident_bytes / vals_run_entries` (about
  1.1 B for 148 B backlink keys and 0.4 B for 70 B record keys at 32 KiB, versus
  8.5 and 2.9 at 4 KiB) and `vals_run_filter_resident_bytes / vals_run_entries`
  (1.25 B payload at 10 bits). `vals_run_filter_resident_bytes` counts allocated
  vector capacity; `vals_run_filter_payload_bytes` counts bloom words only, without
  encoded headers or trailers. Their difference shows decode/builder capacity slack.
  Blooms now decode with exact capacity, including when reopening existing runs;
  no rewrite or format change is needed. `vals_run_fence_payload_bytes` counts the
  in-memory block-index tuples and first-key lengths without capacity slack;
  compare with `vals_run_fence_resident_bytes` for allocated tuple/key capacity.
  These gauges cover open serving runs, not worker copies or seed-held indices,
  and are already included in `vals_run_metadata_resident_bytes`.
- **Mutable and process memory gauges.** `vals_memtable_bytes_total` remains the
  logical key-plus-entry estimate used for flush thresholds. Instead use
  `vals_memtable_allocated_bytes_total` for estimated allocated mutable index bytes:
  retained hash buckets/control bytes, shared Arc key allocations, and estimated
  tree nodes (or packed block/vector capacities for sorted indices). The two
  gauges overlap; do not add them. Empty hash tables retain their useful high-water
  allocation and reset deleted-slot controls before the last entry is removed.
  Partially drained maps are not rebuilt; overlapping ingest can still cause
  deletion-driven capacity growth. Sorted indices use no hash reset.
  `vals_flush_protection_bytes` counts estimated allocated capacity of the
  deduplicated flush-protected segment sets. Both aggregate engines on the existing
  sweep cadence; neither changes RAM-broker charges or flush triggers.
  `vals_allocator_in_use_bytes` is process-wide glibc `uordblks + hblkhd`, and
  `vals_allocator_free_bytes` is `fordblks`, sampled on Linux/glibc when metrics are
  rendered, at most every 4 s and never on a shard thread, because `mallinfo2` walks
  every arena under its lock (zero on unsupported platforms). In-use includes serving, worker and runtime heap;
  it overlaps all heap gauges. Free bytes may include nonresident/madvised pages:
  neither allocator gauge is RSS and they must not be summed with resident classes.
  `vals_merge_worker_metadata_bytes` is a live process-wide atomic gauge covering
  reopened-input fences and builder/output blooms/fences in off-core merges through
  commit/drop, including cancellation cleanup. It excludes scan/IO buffers, seed
  retained indices and serving runs. Builders update it once per merge chunk;
  reopened inputs skip unused bloom decode, but retain scan-required fence offsets.
- **Allocator page reclamation (Linux/glibc nodes).** Periodic `malloc_trim(0)`
  is **off by default**. RAM-constrained hosts can opt in with
  `VALS_MALLOC_TRIM_SECS` (restart-only nonnegative seconds, u32); **`0` disables
  the thread**, including when unset. A positive interval starts a dedicated
  `vals-malloc-trim` thread at nice 19, waiting that interval after each call
  completes. It runs outside serving shard/ingest threads, including during
  post-ingest merge drain and shutdown flushing, and is stopped/joined at node exit.
  Invalid values fail startup and `--check-config`. Other platforms and `valsd pd`
  do not start it. No mmap-threshold or arena-cap policy is applied; existing seed
  trims are unchanged. This returns free arena pages, not live metadata, and does
  not change run format, flush thresholds or the RAM broker.
  [0398 hardware measurements](../bench/0398-ram-hardware/REPORT.md) found trim-on
  arms lost 5.6% whole 0–1B ACK throughput versus pre-cut, and 3.4% versus trim-off
  in outage-excluded eligible windows; calls took 290–420 ms. These observational
  comparisons include bad-RAM-host effects and do not establish causality.
  Background placement removes direct shard execution, **not allocator lock
  contention**. Monitor RAM and ACK tails when opting in; disable with
  `VALS_MALLOC_TRIM_SECS=0` if they regress.
  `vals_allocator_trim_calls_total` and `vals_allocator_trim_ns_total` count calls
  and cumulative wall nanoseconds inside the background trim (including lock
  waits, excluding RSS sampling); `vals_allocator_trim_max_ns` is the maximum
  duration since startup. `vals_allocator_trim_rss_returned_bytes_total` sums
  nonnegative process RSS drops across calls, a concurrent net estimate rather
  than exact pages freed. These metrics exclude seed-path trims; use RSS and
  allocator in-use/free gauges alongside them, not as additive resident classes.
- **Key filter memory gauges.** `vals_key_filter_bytes` is allocated capacity
  bytes, including shadow rebuilds and retired sub-filters held by in-flight sinks;
  `vals_key_filter_resident_bytes` estimates touched bytes (not measured RSS).
  `vals_key_filter_retired_{bytes,resident_bytes}` and
  `vals_key_filter_rebuild_{bytes,resident_bytes}` report subsets of those totals.
  Subtract both to see active-filter memory; do not add them again to run metadata
  or RAM charges. `vals_key_filter_capacity_keys` is active-generation key capacity,
  excluding shadow and retired generations; compare with `vals_key_filter_inserted_keys`
  and logical live keys to distinguish growth slack and stale bits.
  `vals_key_filter_rebuilding` counts engines with a shadow generation, and
  `vals_key_filter_rebuild_covered_runs` counts runs covered by those generations.
  `vals_key_filter_rebuilds_{started,completed,abandoned}` are monotonic engine-sourced
  counts exported as gauges (reset on engine reopen/drop, as with existing engine counters).
  `vals_key_filter_rebuild_peak_bytes` sums each engine's lifetime high water of total
  allocated filter bytes observed while rebuilding, including active, shadow and retired
  bytes. It is **not** the simultaneous node peak; record `vals_key_filter_bytes` over
  time for that. Rebuild overlap is charged, not hidden; the concurrency knob limits
  simultaneous rebuilding engines, not a byte budget. These gauges aggregate local
  zone engines on the existing maintenance cadence.
  `vals_key_filter_max_sub_filters` and `vals_key_filter_max_est_fpr_ppm` are
  independent maxima over local zone engines of active chain length and estimated
  active-generation FPR (ppm, rounded up); neither includes shadow/retired filters.
  `vals_key_filter_max_uncovered_runs` is the largest uncovered-run count in any
  engine. Only use the FPR-only absent-Apply read model when uncovered runs are zero;
  uncovered runs are always probed individually. These are maxima across shards,
  not sums or engine-labelled series, and the maxima need not describe the same engine.
  `vals_key_filter_rebuild_wait_seconds_max` is the longest current eligible engine's
  permit wait; `vals_key_filter_rebuild_open_seconds_max` is the oldest open shadow
  rebuild. Both use whole seconds and the same sampling cadence.
  `vals_key_filter_rebuild_wait_seconds_total` is process-wide cumulative completed
  plus ongoing permit wait in whole seconds. It remains monotonic through engine
  reset/drop and resets only with the process; do not sum shard copies.
  `vals_key_filter_rebuild_urgent_grants` is the process-wide monotonic count of
  permits granted beyond the normal concurrency cap; it resets only with the
  process, not engine reset/drop. Do not sum shard copies.
  Rebuild permits prefer the highest active FPR (ppm), breaking ties by earliest
  wait start. Waiters unrefreshed for 60 seconds are bypassed. The normal cap is
  `VALS_KEY_FILTER_REBUILD_CONCURRENCY` (default 1). Engines whose active estimated
  FPR is at or above `VALS_KEY_FILTER_REBUILD_URGENT_FPR_PPM` (default 25000,
  or 2.5%; 0 disables, nonzero values clamped to 1000–500000) may use up to
  `VALS_KEY_FILTER_REBUILD_URGENT_EXTRA` additional permits (default 1, range 0–8),
  highest urgent FPR first. The extra concurrent shadow fires only at or above
  this FPR threshold, not on chain length alone. This bounds concurrent shadows,
  **not chain length**:
  chains can still grow while waiting/scanning. One extra EOF shadow is estimated
  at 0.73 GiB (440M keys × 1.1 × 12 bits); validate measured peak RAM and worst
  FPR before relying on the read-cost gate.
- **Key filter at scale.** In a replay from empty (16M-key first sub-filter), the
  blocked-bloom model gives a compound FPR of 1.75% and 1.69 B per inserted key at
  440M keys/engine (5 sub-filters of 16–256M keys); a scaled-down test (20k floor,
  550k keys) measures 1.79% and 1.69 B/key. Each further full sub-filter adds about
  0.4%: 2.1% at 880M keys/engine, and 2.5% measured with 7 sub-filters. Right after
  a growth step the new, doubled sub-filter is allocated and touched quickly and
  stays allocated for the whole doubling: RAM per key is ~3.1–4.5 B right after a
  step and falls toward ~1.5 B as keys double, ~2.1 B/key on average. Plan RAM for
  the peak. After a restart the
  first sub-filter is sized from the engine's keys (1.25×), so one filter at
  12 bits gives about 0.1–0.4% FPR.
- **Read cost.** A run probe reads one whole block: about 19–24 KiB from the
  device instead of 2.5–6.5 KiB, plus about 2 µs more CPU per probed block,
  mostly the block CRC, computed once per block read (release micro).
  `vals_read_probe_bytes` and `vals_apply_probe_bytes` count the probed block
  bytes; divide by `vals_read_probe_blocks` / `vals_apply_probe_blocks` for bytes
  per block. Range scans seek inside a block in place and decode only the entries
  a page returns, but a page still reads each overlapping run's whole block.
- **Overwrite-heavy workloads pay most.** Every write to a key that is no longer
  in the memtable resolves its old version from a run block. With a small hot key
  set: release, 20k hot keys, 9.8–10.2 s at 32 KiB versus 8.2–8.9 s at 4 KiB
  (15–20% slower); the debug `churn_reclaim_wedge` workload took 31.7 s versus
  13.1 s with 64 KiB segments (60+ s versus 23 s with 4 KiB segments, so that
  test pins 4 KiB blocks). Overwrite-heavy deployments that do not need the RAM
  saving may prefer `VALS_RUN_BLOCK_TARGET_BYTES` of 8192–16384.
- **Estimator side effects.** The space estimator counts a sampled run entry as
  shadowed when any newer overlapping run's bloom says "maybe"
  (`entry_shadowed`), so 10-bit blooms (about 1% FPR, was about 0.4%) bias
  `vals_shadowed_value_bytes_est` upward by roughly 1% of a run's unshadowed
  value bytes per newer overlapping run (was about 0.4%).
  The range midpoint used for splits needs at least two blocks of a run inside the
  range, i.e. roughly 300–600 entries per run at production key sizes (it was
  about 40–80).
- **Rollback of the settings** (all read once at startup; already written runs keep
  their shape until merged again):
  - Old run shape: `VALS_RUN_BLOCK_TARGET_BYTES=4096` and
    `VALS_RUN_BLOOM_BITS_PER_KEY=12`.
  - Old key-filter schedule: `VALS_KEY_FILTER_BITS_PER_KEY=13`,
    `VALS_KEY_FILTER_MAX_BITS_PER_KEY=18` and `VALS_KEY_FILTER_GROWTH_PCT=150`.
    The cap is never below the bits knob, so with the default cap of 12, setting
    only `VALS_KEY_FILTER_BITS_PER_KEY=13` gives a constant 13. The probe count is
    now round(0.6 × bits) for explicit bit counts too (13 bits → 8 probes; it
    was bits/2 = 6), so the old filter is not reproduced bit for bit; its FPR is
    slightly lower.

## PD config reference

The production and reference-deployment default is **three voting PD replicas on
three independent hosts or failure domains**. Co-locating one lightweight PD with
each of three data nodes is the baseline; dedicated control hosts are also valid
when isolation or independent failure domains justify them. Each replica needs
its own durable `data_dir`. Three PD processes or directories on one host do not
provide host-failure HA. A single PD is supported only as an explicitly non-HA
dev, CI, or controlled performance-test exception.

```ini
pd_id     = 1                    # required: raft id of this PD replica
data_dir  = /var/lib/vals/pd1    # required: durable raft state (state survives restarts)
data_node = 1 @ host:7000        # repeatable: boot membership + push targets
                                 #   (comma-list the addrs of a sharded node;
                                 #   optional per-node ring weight: `1*4 @ ...`)
rf        = 3                    # authoritative ring shape, pushed to everyone
vnodes    = 128
state_capacity_mib = 256          # bounded live PD materialization; restart to change
zone      = events 3 sorted      # repeatable: boot zones (name rf ordering)
admin_socket = /run/vals/pd.sock # default: <data_dir>/admin.sock
secret_file  = /etc/vals/secret
metrics      = 127.0.0.1:9465    # optional: Prometheus scrape endpoint
log_level    = info              # optional: overrides VALS_LOG (reloadable via SIGHUP)

# multi-PD (a raft group of 3): identical full voter list on every replica
listen    = 0.0.0.0:7100         # local raft bind address; not peer advertisement
pd_peer   = 1 @ host1:7100       # advertised address for replica 1 (self in this example)
pd_peer   = 2 @ host2:7100       # repeatable
pd_peer   = 3 @ host3:7100
campaign  = true                 # exactly one replica: campaigns when it bootstraps an
                                 #   empty data_dir (a restart with state does not)
join      = false                # true: an EMPTY data_dir joins an existing group
                                 #   as a new learner (replacing a lost voter)
```

Every replica must use the same complete `pd_peer` voter list, including an
entry mapping its own `pd_id` to its peer-advertised address. `listen` is that
replica's local bind address and is separate from the advertised address; a
wildcard bind such as `0.0.0.0:7100` is never a peer destination. The
`pd_peer` list seeds the voter set only when a replica starts on an empty
`data_dir`; after that the committed Raft membership is authoritative and the
list supplies addresses. A replica whose `pd_peer` ids differ from the committed
voters logs that at `warn` (at `info` while a learner is being added); a member
missing from the list is reached at the address committed when it was added
([Replacing a lost PD voter](#replacing-a-lost-pd-voter)). `join = true` needs
`pd_peer` and excludes `campaign`; it only affects an empty `data_dir`.
`campaign` also only acts on an empty `data_dir`: a replica that restarts with
Raft state waits for the current leader's heartbeat like any follower, so
restarting the campaigning replica does not force an election.

Raft timing is fixed: the drive loop ticks every 100 ms, the leader heartbeats
every 3 ticks (300 ms) and a follower campaigns after 10–20 ticks (1–2 s)
without one. A leader stall shorter than about a second (for example the
opaque-token placement search of a 64-range presplit `create-zone`) does not
trigger an election. Before task 0280 a tick was one drive-loop iteration
(about 5 ms), so every presplit `create-zone` deposed the leader and lost its
balanced initial map.

Elections use Raft pre-vote and check-quorum (since 0498). A replica that
times out first asks for pre-votes without raising its term, and a voter that
heard from a leader within the last election timeout ignores both pre-votes and
votes. So a replica that cannot hear the leader (an isolated one, or a rejoining
one whose peers' connections to it are stale) cannot depose a healthy leader,
and a replica without a quorum keeps its term (`role=pre-candidate`
`leader=unknown`). A leader that hears from no quorum for an election timeout
steps down. Each peer gets its own sender thread with a bounded queue, so a
connect or write to an unreachable peer never stalls the drive loop; messages
that do not fit, or whose connect or write fails, are dropped (`vals_pd_raft_send_drops`)
and Raft retransmits them. Peer connections set `TCP_USER_TIMEOUT` to 1 s, so a
connection to a host that vanished is replaced about a second later instead of
when TCP's retransmissions give up. Pre-vote and check-quorum change no wire or
storage format; old and new replicas can be mixed during a rolling upgrade.

Boot state is idempotent: on every start the PD waits briefly for its
recovered (raft-committed) state, then proposes only the `data_node` members
and `zone` names that aren't already committed — a restart never duplicates
zones or churns the config epoch.

`state_capacity_mib` bounds the live key/value materialization rebuilt from the
durable Raft snapshot and log; it is not an append-only device allocation. The
default is 256 MiB and the value must be nonzero. Proposal admission reserves
worst-case growth for locally queued entries and refuses a mutation before used
plus reserved bytes would cross the bound. A committed entry that cannot fit on
a follower still fails the PD drive loop and requires restart instead of
continuing with divergent state. Monitor `vals_pd_state_capacity_bytes`,
`vals_pd_state_used_bytes`, `vals_pd_state_reserved_bytes`, and
`vals_pd_state_headroom_bytes`.

Range maps written by this version use versioned per-range keys and an atomic
conditional-batch Raft command. Existing whole-map state is read unchanged and
is migrated on that map's first mutation. The new command is not understood by
older PD binaries, and a new snapshot no longer contains an older binary's
whole-map key. Upgrade every PD peer before allowing range mutations; after the
first such mutation, do not downgrade or start an old peer against that Raft
directory.

Data nodes hold a sorted zone (no sweep reconcile or cleanup for it; its writes
get retriable `StaleEpoch`) from the catalog push until their first
`SetRangeMaps` push. This PD version sends that push even when no zone has a
range map yet; an older PD that has never split a sorted zone sends none, so
upgrade the PD before the data nodes. A node that boots without reaching a PD
leader is held as well: a file-backed node reloads its last adopted catalog and
holds its sorted zones until the first `SetRangeMaps` push, and an SPDK node,
which keeps no catalog, refuses writes, range deletes and fenced reads to every
re-opened sorted zone with retriable `StaleEpoch` until it has adopted both a
catalog push and a range-map push (logged at boot as "holding re-opened sorted
zones").

Once the PD has committed any V4 (opaque-token) range map, it must keep running
with `opaque_range_maps=true` (`VALS_PD_OPAQUE_RANGE_MAPS=1`). Without it the PD
withholds every range-map push, so a data node that restarts stays held (its
sorted-zone sweep paused and its sorted writes refused) until the PD is
restarted with the setting. The PD logs this once at error level ("every
range-map push is withheld").

### PD availability and recovery

The PD Raft state is authoritative for data-node membership, zone identity and
configuration, placement epochs, and sorted-zone range maps. The daemon stores
its snapshot, log, hard state, and Raft membership under `<data_dir>/raft` with
checksummed, synced file replacement. This is local durability, not a backup or
replicated filesystem: in a three-PD deployment Raft's other voters are the
independent copies. Persistence or state-machine apply failure is fail-stop; the
failed PD requires restart and does not keep serving or pushing after abandoning
a Raft Ready.

A PD is not on the read/write request path. **Already-running** nodes and clients
that share the last pushed membership, catalog, and range maps can therefore
keep serving during a PD outage. This is conditional continuity, not a promise
that every data operation remains available or that an outage is safe for an
unbounded time. A concurrent data-node failure can remove a read/write quorum;
a restarted data node may have only its static boot config and locally persisted
metadata rather than the last complete push; a restarted client must rediscover
routing from a reachable node; and stale placement cannot adapt to failures,
hotspots, or exhausted capacity.

When no caught-up PD leader has a Raft quorum, the effects last until a leader
and quorum return:

| Control-plane function | When it stalls and practical impact |
|---|---|
| Config push and restart healing | Immediately. Running nodes keep their current config, but a restarted, lagging, or newly provisioned node is not brought to the authoritative epochs. |
| `add-node` / `remove-node` | Immediately. Capacity cannot be added and a failed node cannot be routed around administratively; affected ranges run at reduced redundancy and can lose data-path quorum immediately after enough owner failures. |
| Create/drop/split/merge zone | Immediately. Existing zones remain usable on their current config. New namespaces and manual range changes wait. |
| Automatic split/merge | At the next policy poll or threshold crossing. Merge is cleanup; a blocked split becomes user-visible when an unsplit range concentrates enough load or capacity to hurt the current owners. |
| Range rehome/rebalancing | No new plan or PD commit can complete. Existing owners keep the last committed map, so skew and reduced redundancy persist; do not assume an in-flight multi-phase move will finish while authority is absent. |
| Failure-detector eviction | Automatic eviction is not shipped today; any future PD-mediated eviction would also require quorum. Manual `remove-node` is unavailable now, so the time-to-impact is set by the failed node's range ownership and the zone's read/write quorum. |
| Administrative inspection/mutation | `ping` only proves the selected process is alive. A follower rejects mutations; without quorum a proposal cannot commit, so no `ok committed` confirmation is possible. Inspection without quorum is non-authoritative, may lag, and is not an authority-recovery mechanism (see [`pd-admin.md`](pd-admin.md)). |

Recovery depends on what survived:

1. **Process restart or intact-state relocation.** Restart the same `pd_id`
   with the same peer set and its intact `data_dir`. The snapshot and log
   replay to committed state before normal service. Moving that directory to
   another host is the same operation only after the old process is stopped or
   network-fenced, and only if the replica keeps its advertised `pd_peer`
   address (move the DNS name or IP with it): the other voters address it by
   that entry.
2. **One lost replica in a three-PD group.** The other two voters retain quorum
   and elect a leader, measured at 1.0–1.3 s on the lab fleet
   ([Three-PD reference deployment](#three-pd-reference-deployment)); this is
   an observation, not an SLO. Preserve both surviving
   directories. Restore redundancy with an **empty-state voter replacement**
   ([below](#replacing-a-lost-pd-voter)): a new replica with a **new** `pd_id`
   joins the same group as a learner, catches up from a snapshot, and takes the
   lost voter's place. The group keeps its Raft log, its committed state and its
   cluster incarnation, so data nodes need nothing. Never restart a lost voter's
   `pd_id` on an empty `data_dir`, never bootstrap a new group, and never wipe a
   survivor.
3. **Quorum unavailable, original state still present.** Restore enough original
   replicas, with their original identities and directories, to recover the same
   group. Do not bootstrap a second group merely because the original group is
   temporarily unable to elect.
4. **All authoritative PD state lost.** Data-node copies are serving caches,
   not a complete, mutually consistent PD snapshot, and no tool reconstructs
   the catalog and range maps from them. A fresh `data_dir` bootstraps a new
   authority from config; it is not restoration of the old group. The
   supported recovery is a **restore from a backup** taken earlier with `valsd
   backup create`: `valsd backup restore-pd` seeds new PD directories from it,
   `valsd backup load` writes its records into empty, freshly provisioned data
   nodes and `valsd backup compare` checks the copy record for record
   ([`backup-restore.md`](backup-restore.md#restoring-a-backup)). The result is
   a new cluster with a new incarnation, holding the data as of the backup;
   writes after the backup are lost. Without a backup there is no supported
   recovery: keep the data path quiescent from topology changes and retain
   every surviving PD directory and node image. Loss of a single-PD host and
   its only `data_dir` is this case.
   A group bootstrapped or restored into a fresh `data_dir` has a new cluster
   incarnation, so nodes bound to the old group refuse it until they are
   re-enrolled (below).

Fencing is mandatory before any relocation or disaster recovery. Within one
healthy three-voter group, an isolated minority cannot become leader, and only
the current leader pushes config or accepts mutations. Across groups, the
**cluster incarnation id** (task 0283) fences data nodes, as described next.

#### Replacing a lost PD voter

Supported: replacing **one** voter of a three-voter group whose `data_dir` is
gone (or whose host will not return), while the other two voters form a quorum
with a caught-up leader. Everything goes through that leader, so the repair
cannot commit without the surviving quorum; nothing here repairs a lost quorum
(case 3 or 4 above).

1. **Fence the lost replica.** Make sure its process cannot run again: power off
   or reprovision the host, or block its raft port. Once removed (step 4), the
   group drops its replies and ignores its vote requests while its log is behind,
   but a copy of its old directory that starts again still holds a stale view;
   do not start it.
2. **Start the replacement** on an empty `data_dir` with a **new `pd_id`** that
   the group has never used, a copy of a survivor's config (same `data_node`,
   `rf`, `vnodes`, `zone`, secret) and:
   ```ini
   pd_id   = 4
   listen  = 0.0.0.0:7100
   pd_peer = 1 @ host1:7100          # the surviving voters ...
   pd_peer = 2 @ host2:7100
   pd_peer = 4 @ host4:7100          # ... and itself
   join    = true                    # no `campaign`
   ```
   It waits as a non-member (`status` shows `voters=- learners=-`): it neither
   campaigns nor answers vote requests. Its first state must arrive as a
   snapshot that lists it as a learner. If instead the group already tracks
   that id as a member (a wiped member restarted under its old id with `join =
   true`), the replica logs `cannot be recreated`, installs nothing and stops.
3. **Add it as a learner** on the leader:
   `valsd admin -s <leader pd.sock> pd-add-learner 4 host4:7100`. The address is
   committed with the change, so every voter can reach the learner without a
   restart or config edit. Applying any membership change makes each replica
   take a snapshot, and the leader sends that snapshot to the learner. A
   learner receives the log but does not vote, so it does not change the
   quorum.
4. **Replace the voter:** `valsd admin -s <leader pd.sock> pd-replace 3 4`. The
   leader refuses until learner 4 has installed the group's state (it has
   acknowledged part of the log, which an empty replica can only do after its
   snapshot) and is within 16 entries of the commit index, then commits two
   single-voter changes: remove voter 3 (the
   group runs on voters 1 and 2, the same quorum it already needed), then
   promote 4. The reply is `ok pd 3 replaced by pd 4 (...); voters=1,2,4
   learners=-`. The command is safe to rerun: after an interruption, a timeout
   or a leader restart between the two changes it continues from the committed
   state, and once done it replies `already done`. `status` on every replica
   shows `voters=1,2,4`; the `incarnation` is unchanged.
5. **Update every replica's config:** replace `pd_peer = 3 @ ...` with
   `pd_peer = 4 @ host4:7100` on the survivors and drop `join` on the new
   replica. The running replicas need no restart; the edit keeps later restarts
   and the warning about the `pd_peer` list quiet.

Guards: the leader refuses a learner whose id is or was a member (ids are never
reused; the group records each removed id), a replacement of itself (a
leader is not lost), a second replacement while
one is half done (fewer than three voters), and any change while another
membership change is still being applied. `pd-remove-learner <id>` drops a
learner that will not catch up (for example a wrong address); add a new id
instead. A learner that a half-done replacement has already chosen (voter 3
removed, 4 not yet promoted) cannot be removed: either finish with `pd-replace
3 4`, or add another learner and run `pd-replace 3 <its id>`, which promotes
that one in its place; then remove 4. The admin replies follow [`pd-admin.md`](pd-admin.md); an `unknown:`
reply means: check `status` on the leader, then rerun the same command.

Not supported (no tool; do not improvise one):

- **Changing the voter count**, including growing a single PD into a group.
  A single PD refuses these commands.
- **Changing a live voter's address in place.** Keep the address when
  relocating intact state, or replace the voter with a new id at the new
  address.
- **Reusing a removed or lost id**, or restarting a lost voter on an empty
  directory under its old id. A voter's vote and log are part of Raft's safety;
  an empty copy of it can elect a second leader in an old term.
- **Repairing a lost quorum** by forcing a smaller voter set: restore enough
  original replicas (case 3) or restore from a backup (case 4).

A snapshot travels in one raft frame, so the PD state must fit the transport's
process-wide queued-byte budget (`max_queued_bytes`). The workflow is validated
locally (three `valsd` PDs and three data nodes on loopback, task 0281). The
three-host election, failover and restart drills of task 0280 did not exercise a
voter replacement.

#### Cluster incarnation id

The first caught-up leader of a PD group draws a random non-zero 128-bit id and
commits it to PD Raft state (`valsd admin -s <pd.sock> status` shows it as
`incarnation=`). This covers a fresh bootstrap and an existing pre-0283 group
after its PD upgrade alike. Every control push (`SetMembers`, `SetZones`,
`SetRangeMaps`) carries it: a leader pushes nothing until the id is committed
and visible in its applied state. Each data-node shard adopts the id
carried by its first such push, persists it (the zone superblock, or
`<data_dir>/zones/incarnation` for the file backend) before applying that push,
and from then on refuses every control push carrying another id **or none**,
whatever its epoch. A refused push changes nothing. The node answers with an
`incarnation mismatch` error, the PD logs it as a failed push and retries, and
the node counts it in `vals_control_incarnation_rejected` and logs a
rate-limited ERROR. `vals_control_incarnation_adoptions` counts adoptions
(process-wide, one per bound shard).

| Event | Incarnation | Operator step |
|---|---|---|
| Bootstrap of a new group | minted once by its first leader | none |
| Leader failover, PD restart, intact-state relocation | kept (it is replicated Raft state) | none |
| Empty-state PD voter replacement (task 0281) | kept: same group | none |
| Data-node restart | kept (persisted per shard) | none |
| Fresh `valsd provision` of a node | cleared; the node adopts from its first push | none |
| Restore into a new cluster (`valsd backup restore-pd`) | **replaced**: restore-pd writes a new id | freshly provision every target node empty |

A restored cluster is a new lineage. It must not inherit the old id, because then
a surviving or partitioned old authority would be indistinguishable from it. To
move a node to a different group, stop the node and run

```
valsd reenrol -c /etc/vals/node1.conf --incarnation <32 hex digits from the new PD's status>
```

which rewrites the persisted id of every shard (file and SPDK backends; SPDK
takes the same exclusive device locks as `valsd node`) and prints old and new
values. `--clear` instead returns the node to the unbound state, so it adopts
whatever group pushes to it first. There is no online adoption of a different id.

What the id does **not** fence, so it stays external:

- **Two live copies of one PD replica** (same group, same `pd_id`, e.g. a
  relocated `data_dir` whose old process still runs) share the id. Stop or
  network-fence the old process before starting the copy.
- **Unbound nodes** accept whichever group pushes first: a freshly provisioned
  or `--clear`ed node, and every node until an upgraded PD has pushed to it.
  During a rolling upgrade, keep external stop/network fencing until every
  node is bound: `vals_control_incarnation_adoptions` equals the node's shard
  count (it is process-wide and resets on restart, so after a restart check
  the `bound to cluster incarnation` log line or a refused-push count instead).
  A node still running a pre-0283 binary ignores the id and enforces nothing.
- **Non-push traffic.** Only the three mutating control pushes are fenced.
  Client data requests, anti-entropy between nodes and the PD's read-only
  polls are not.

Upgrade rules:

- **Data nodes and PDs** may be upgraded in either order. Pre-0283 nodes ignore
  the trailing id on pushes, and upgraded nodes accept id-less pushes until they
  are bound.
- **PD voters.** The first upgraded voter that leads mints the id, and nodes
  bind as soon as it pushes. From then on a pre-0283 voter that becomes leader
  pushes without an id, so bound nodes refuse its pushes: no config change,
  repush or restart healing reaches them until an upgraded voter leads again.
  The data path is unaffected. Upgrade all voters in one pass, followers first
  and the current leader last, and finish the pass if an old voter takes over.
  Do not roll a PD voter back to a pre-0283 binary after the id exists.
- **Minting stalls pushes.** A leader pushes nothing until the id is committed.
  If it cannot commit it (for example a full bounded PD state), the PD logs
  `cluster incarnation proposal failed` every 10 s and config delivery stops;
  free PD state capacity.
- **Node downgrade.** A bound node's superblock is format v4, which pre-0283
  binaries refuse to open. To downgrade, stop the node and run `valsd reenrol
  --clear` with the new binary first: it rewrites both superblock slots as v3.

### Three-PD reference deployment

Task 0280 validated this layout on the three lab hosts: one voting PD per data
host, 3 SPDK data nodes × 8 shards, RF 3, 2,000 writes/s of sorted-zone load from
a separate client host (`bench/0280-three-pd/FINDINGS.md`).

**Generate and check configs.** `scripts/pd_topology.py` renders the replica
configs (`three`, the reference, or `single-perf`, the explicitly non-HA
single-PD performance-test mode, marked by a comment line). Its
`check_pd_confs` rejects a set unless every replica has the identical full
`pd_peer` voter list, including itself at its advertised (host) address, a
`listen` that is a wildcard or that address on the same port, distinct hosts,
exactly one `campaign = true`, no `join`, and identical shared settings
(`data_node`, `rf`, `vnodes`, `zone`, `secret_file`). The box harness
`bench/0471-queue-hw/hw.py` uses it: `provision --pd-mode three|single-perf
--reset-pd` starts a new group, `check-configs` checks the deployed files and runs
`valsd pd --check-config` on each host, and `admin` routes to whichever replica
reports `role=leader`.

**Secret and ports.** PDs and data nodes share one `secret_file` (mode 0600);
the PD Raft hello and every control push carry it, and a mismatch shows as
`DENIED raft peer connection` or `control_denied`. Each PD host needs its Raft
port (`listen`, 7100 here) reachable from the other two PD hosts, the data
nodes' ports reachable from every PD (pushes), and the admin socket local only.
An advertised `pd_peer` address must stay stable: use a static address or a
DHCP reservation, since changing a live voter's address is unsupported.

**Operator checks.** On all three replicas `valsd admin -s <pd.sock> status`
must show the same `leader`, `term` and `incarnation`, `voters=1,2,3`, and
`applied` equal to `commit` once idle (only the leader reports
`caught_up=true`). Each data node must log `bound to cluster incarnation <id>`
once per shard and report `vals_control_incarnation_rejected` 0. A `term` that
keeps rising on one replica means it cannot reach a quorum (before 0498; since
then such a replica keeps its term and shows `role=pre-candidate`).

**Measured behaviour** (single runs per drill; times from the daemons' own log
timestamps):

| Event | Control plane | Data path (client writes) |
|---|---|---|
| Follower PD stopped 20 s, restarted | no election; caught up 0.5 s after start | unaffected |
| Leader PD SIGKILLed (systemd restarts it after 10 s) | new leader after 0.97, 1.24, 1.26 s (one round each; with pre-vote, 0498: 1.09–1.78 s, median 1.25 s over 8 kills, all one round); a zone created on it reached all 24 shards in 0.35–0.38 s; the old leader rejoined as follower | unaffected, except one write p99 of 180–200 ms while the new leader re-pushed every config |
| Two PDs stopped (quorum lost) for 48 s | survivor stays candidate (`leader=unknown`; its term rose 18 → 49; since 0498 it stays pre-candidate and its term did not move in 40 s), `create-zone` refused with `not leader`, inspection `non-authoritative`; one voter back → leader after 1.1 s | unaffected |
| Data node restarted while quorum lost | no ring/catalog/range-map push until a leader returned, then all 8 shards were pushed 1.7 s later | the node served writes from its persisted zones meanwhile (see below) |
| All three PDs stopped and restarted | leader within 1.8 s of the start commands; same incarnation, catalog and members; commit +1 (the election entry) | unaffected |
| Host loss: leader host rebooted (PD + data node) | new leader after 1.3 s; a zone created during the outage took 23 s to reach the survivors; the host was back after 39 s and its PD caught up, but it campaigned twice and caused one more leader change (since 0498: the rejoining PD heard the leader 0.3 s after starting, no election) | the client's writes nearly stopped for the 54 s the host was unreachable (see below) |

Rolling restarts of all three PDs (followers first, leader last) kept the term
on every follower restart and cost one election for the leader. That is the
upgrade and rollback procedure: one replica at a time, checking `status` on all
three between steps. The Raft wire and storage formats did not change in 0280,
so old and new binaries can be mixed during the roll. A pre-0280 replica with
`campaign = true` still campaigns on every restart, so restart it while it is a
follower last, or accept one election.

What these drills showed outside the PD itself:

- **A restarted data node that cannot reach a PD leader** boots with an empty
  catalog. It re-opens its persisted zones and serves writes to them without the
  sorted-zone range fence, which needs the catalog. The fence returns once a
  leader pushes. The documented hold above applies only after the catalog push.
- **A host that drops off the network** (no RST) stalls control pushes: the
  leader retried 1 s connect timeouts to each of its 8 shard addresses, and a
  new zone reached the healthy nodes after 23 s instead of 0.35 s. Since 0497
  each address is pushed on its own thread, and the restart beacon and the
  range-stats poll no longer run on the push loop, so a dead host only delays
  its own pushes.
- **The Rust client with the bounded config** (`VALS_BENCH_BOUNDED_CLIENT=1`,
  5 s RPC deadline) stopped issuing writes while one of three hosts was
  unreachable. The surviving nodes received about 13 applies/s instead of
  2,000 until the host answered again. RF 3 with W 2 does not hide a
  black-holed replica from this client. A data-node process restart, which
  refuses connections at once, cost only 1–2 s.
- **A rejoining PD whose peers hold stale connections to it** loses their first
  messages until TCP notices the reboot. (A connection the peer closed, because its
  process restarted or because its 120 s idle timeout dropped a quiet
  follower-to-follower connection, is now detected and re-opened before the send; before 0280 that also lost the first vote of an
  election and cost an extra 1–2 s round.) Before 0498 it then campaigned and
  could depose a healthy leader: in a repeat drill the rebooted leader host's PD
  went through terms 57–62 and the group had no leader for 4.7 s. Since 0498 the
  stale connections are replaced within about a second (`TCP_USER_TIMEOUT`) and
  pre-vote with check-quorum keeps the leader; in the same drill the rejoining PD
  followed the leader without an election.

## Config reload (SIGHUP)

`SIGHUP` makes a running node or PD re-read its config file (the same path it
was started with, `-c`) and apply the **reloadable** keys live — no restart, no
dropped connections:

- **`log_level`** (node **and** PD) — retunes the running stderr logger. Handy
  to flip a wedged node to `debug` and back without a restart.
- **`sweep`** (node only) — retunes the anti-entropy cadence, and with it the
  compaction and flush/snapshot cadences that derive from it. The change takes
  effect on each background loop's next tick (bounded by the *old* period).

Every other key — `profile`, `backend`, `data_dir`, `listen`, `node_id`/`pd_id`, peers,
`rf`, `gc_grace`, `secret`, the block/segment/region geometry, the `pci`/`spdk_*`
knobs, `format`, `metrics`, `admin_socket`, and all remaining PD keys — is
**non-reloadable**: a change is logged at `warn` (`` `<key>` changed but is not
reloadable; keeping the running value``) and ignored. Apply those with a restart.

A reload that fails to parse or validate is logged (`config reload failed … ;
keeping the running config`) and changes nothing — the daemon keeps serving with
its last-good config. Each reload ends with one summary line naming what was
applied and what was ignored, e.g.:

```
node 1: config reloaded (applied: ["log_level", "sweep"]; ignored non-reloadable: ["region_mib"])
```

Signal the process directly (`kill -HUP <pid>`) or, under systemd, add
`ExecReload=/bin/kill -HUP $MAINPID` and `systemctl reload vals-node@1`.

## Administering a running cluster

`valsd admin -s <socket> <command>` — the socket is local to the PD host
(file permissions are the auth model; mutations still go through raft):

| Command | Effect |
|---|---|
| `ping` | liveness check |
| `status` | local replica view: `pd`, `role`, `term`, `leader`, `commit`, `applied`, `caught_up`, `incarnation`, `voters`, `learners` |
| `members` | committed membership: `id weight addrs` per line |
| `zones` | committed catalog: `id name rf ordering` per line |
| `ranges <zone>` | committed range boundaries of a zone, one `hex=<bytes>` key per line |
| `add-node <id> <weight> [<addr>,...]` | grow the cluster (addresses make the node reachable cluster-wide — no restarts anywhere) |
| `remove-node <id>` | shrink the ring (data re-replicates via anti-entropy) |
| `create-zone <name> <rf> <sorted\|unsorted> [presplit=K \| presplit=key,key,...]` | new zone, optionally with numeric first-byte ranges or explicit canonical real-key boundaries for a sorted zone, pushed to all nodes |
| `drop-zone <id\|name>` | tear a zone down everywhere |
| `split-zone <id\|name> <key> [canonical]` | split a sorted zone's range at `key`; `canonical` spreads an empty range only (populated ranges need re-home) |
| `merge-zone <id\|name> <key>` | merge the sorted range beginning at `key` into its predecessor |
| `export-state <absolute-path>` | write the complete PD state (confirmed leader only) to a new file on the PD host; used by `valsd backup create`, `load` and `compare` ([`backup-restore.md`](backup-restore.md)) |
| `pd-add-learner <pd_id> <addr>` | add a new, empty PD replica (started with `join = true`) to the PD raft group as a non-voting learner |
| `pd-replace <old_pd_id> <new_pd_id>` | replace a lost PD voter with a caught-up learner (rerunnable) |
| `pd-remove-learner <pd_id>` | drop a learner that was never promoted |

Each PD replica has its own host-local admin socket, and mutations are accepted
only by the current, caught-up PD leader. Locating the leader, the mutation
reply contract and exit codes, authoritative vs non-authoritative inspection,
and safe handling of ambiguous outcomes are described in
[`pd-admin.md`](pd-admin.md). Boot membership and zone intents are the
exception: startup retries them through delayed elections. PD validates
mutations again in the authoritative proposal path. Zone names must be unique,
nonempty, and whitespace-free; RF must be at least one and no greater than the current eligible membership (the explicit
scope, when present). A member cannot be removed when that would take membership
below the global RF, invalidate any zone's scope, or leave a zone RF
under-replicated. There is no implicit degraded
mode for administrative changes: explicitly change or drop affected zones first
if reduced durability is intended.

## Bulk vlog export (`valsd dump`)

`valsd dump -c <node-config> (--zone <name> | --zone-id <id>) --out <path>
[--skip-tombstones] [--parallel <N>]` exports one zone's value log to a flat
`.vdump` file at device-sequential rate. It reuses the boot-recovery sequential
scan (no merge/RPC/quorum, no quiesce gate), reading through an **independent
handle** on the zone's `.vlog` — so it is safe to run against a live file-backed
node without disturbing serving. (It is a point-in-time capture: any tail written
since the last sync may be absent.)

- `--zone <name>` resolves via the node-local persisted catalog
  (`<data_dir>/catalog`); `--zone-id <id>` bypasses it. A missing zone errors
  and lists the zone ids present on disk.
- `--parallel <N>` (default 1, capped at 64) scans with `N` per-segment workers,
  each on its own device handle. Segments are independently decodable, so this is
  read-bound rather than merge-bound. Output is emitted in strict epoch order, so
  a parallel dump is **byte-identical** to the serial one and round-trips to the
  same final state. To capture a whole cluster, run one `valsd dump` per node
  concurrently — nodes and zones are independent (no cross-node ordering); the
  aggregate wall time ≈ one node's (measured: 3 concurrent node dumps all
  finished within ~2 % of each other on separate NVMe drives).
- **Measured `--parallel` knee** (SPDK box, one NVMe drive, ~302 GiB / 2.53 B
  records; `bench/dump-box-notes.md`): serial `1` = 0.67 GiB/s (7.5 min); `4` =
  1.07 GiB/s (1.6×); `8` = 2.07 GiB/s (3.1×); `16` = 2.98 GiB/s (4.4×, 1.7 min) —
  strong, monotonic, no regression through 16. Even **serial** beats the
  merge-bound scan path (~76k rec/s ⇒ ~9.25 h for that record count) by ~74×;
  `--parallel 16` by ~329×. **Recommendation:** for an **offline/idle** drive use
  `--parallel 8`-`16` for the ~3-4× win; the default stays `1` because the same
  option drives the **live** node-admin `dump` below, where higher parallelism
  measurably contends for device bandwidth and dips serving (see there) — so
  raising it globally is not safe. Opt in per drive/workload.
- **On a re-used drive (resolved — task 0075):** boot recovery and the raw vlog
  scan share one rule — a segment is presumed live iff its header block decodes at
  the open geometry. On a re-used, non-zeroed drive that rule *by itself* is
  unsafe: a prior tenant's CRC-valid segments (and manifest chains) can survive a
  "format + write a smaller corpus" cycle **beyond the new log head**. This was a
  real correctness bug, not just dump inflation — an ordinary reopen of such a
  drive replayed the stale segments into the served index, and because
  replay winners are argmax(`version`) (with `seq` only an equal-version tie-break),
  stale records carrying higher LWW versions could **win** over live ones.
  Demonstrated (and fixed) by `crates/vals-store/tests/format_fence.rs`. On the
  **multi-drive JBOD partition path** the hole was reachable in production
  (`open_region(create=true)` deliberately does not wipe).

  **Now fenced:** every format path durably zeroes every segment-start header
  block before use — `Engine::create` (raw device, including the JBOD partition
  path as of this task), `wipe_region` (zone-region alloc), and `Engine::truncate`
  (logical wipe). So a valid header always postdates the last format: an
  ordinary reopen of a region provisioned by a **current build** cannot
  replay pre-format segments, and its dumps are clean 1:1. (A format-generation
  stamp was considered and rejected — it can't clean up already-contaminated
  drives and adds nothing over the wipe; see `docs/formats.md` §2.2.)

  **Drives last formatted by an older/pre-fence build are still contaminated on
  device** and are indistinguishable from live data (same format version, shared
  epoch/seq space) — no software change can filter them after the fact. Give such
  a drive **one offline `valsd provision` under a current build** (which wipes)
  before trusting an ordinary reopen or a dump. Until then, dumps inflate
  (bounded; last-writer-wins dedups overlapping keys on replay) and an ordinary
  reopen may resurrect stale records.
- `backend = mem` has nothing on disk to dump (error). `backend = spdk` requires
  `--zone-id` (zone names are not persisted on-device) and, for this **offline**
  subcommand, an idle drive no daemon is serving (it attaches the controller
  itself); it needs a `--features spdk` build. To dump a **live** SPDK (or file)
  node, use the node-admin `dump` command below instead.

### Live-node dump (node admin socket)

A **running** node with `admin_socket = <path>` set answers a `dump` command that
dumps a zone through the daemon's **own** handle — so it works for **file *and*
SPDK** backends and is live-safe by construction: the daemon runs the read-only
scan on a dedicated thread over a `sibling()` of the serving handle (a second
SPDK qpair / a dup'd file fd), never touching the serving handle, so serving is
undisturbed.

```
valsd admin -s /run/vals/n1.sock ping
valsd admin -s /run/vals/n1.sock dump <zone-id-or-name> <out-path> \
    [skip-tombstones] [parallel=N] [rate_mib=N] [shard=S]
```

- `<zone-id-or-name>` is a numeric zone id or a catalog name; `<out-path>` is on
  the **node host**. Options are `skip-tombstones`, `parallel=N` (per-segment
  workers, byte-identical output), `rate_mib=N` (a positive integer aggregate
  device-read limit in MiB/s), and `shard=S` (default 0, for a sharded node).
  Reply is `ok <summary>` or `err: <reason>`. The rate applies to the complete
  dump scan, including segment-header discovery, and is shared by all
  `parallel=` workers rather than multiplied per worker. Omitting `rate_mib=`
  leaves reads unlimited, as before.
- Same **point-in-time** caveats as the offline subcommand: records not yet
  synced to the device (the un-synced log tail) are absent, and a concurrent
  compaction can race the scan — the dump then fails with `err:` and should be
  **retried** (it never disturbs serving, and a graceful shutdown waits for an
  in-flight dump to finish before detaching the device).
- The node admin socket is **off unless `admin_socket` is set**, and is distinct
  from the PD admin socket — don't point both at the same path.
- **Live-safety, measured** (SPDK box; `bench/dump-box-notes.md`): dumping a live
  node while it served a sustained write workload kept serving **up and correct
  with zero client errors**. The disturbance scales with **dump duration ×
  instantaneous device-bandwidth share**: a multi-minute 302 GiB dump at
  `parallel=4` dipped serving throughput ~25 % (the dump's sibling qpair contends
  for the node's NVMe **device bandwidth** — not the serving handle, not CPU),
  whereas short dumps (~15 s, ~10 GiB) showed **no** measurable dip at either
  `parallel=1` or `4`. A clean long-window unpaced `1`-vs-`4` and paced A/B was
  not achievable (the box's large read fixture was shed by a reformat), so the
  exact throughput-dip tradeoff and a measured `rate_mib=` recommendation remain
  outstanding. Until that hardware measurement is repeated, keep live-dump
  `parallel=` **low (1-2)** and set `rate_mib=` conservatively when a bounded
  aggregate read rate is more important than finishing quickly. `parallel=` can
  still improve device utilization up to that one shared rate; it does not
  multiply the configured budget. The offline CLI's higher-parallelism
  recommendation above does **not** apply to a serving node. The admin socket
  serves connections **concurrently**: a `ping` (or any admin command) issued
  mid-dump replies promptly rather than queuing behind the in-flight dump, and
  concurrent dumps to different output paths run concurrently (each on its own
  dump thread) — the serving data path is unaffected either way.

For a cluster-wide, deduplicated, versioned backup use `valsd backup create`
([`backup-restore.md`](backup-restore.md)), not per-node dumps.

The dump is **raw** (not deduplicated): every record appears in vlog order, so a
replayer that applies them in file order — later record wins per key —
reconstructs the exact final state, overwrites and deletes included. This is the
corpus the sibling `valsdump replay` consumes.

**Format** (`.vdump`): `b"VDUMP\x01"` magic, `[u8 name_len][zone name]`, then a
flat stream of records until EOF: a put is `[u32-le key_len][key][u32-le
val_len][value]`; a **tombstone** is `[u32-le key_len][key][0xFFFF_FFFF]` with no
value bytes (a sentinel value-length a real value can never take). A put-only
replayer that doesn't understand the sentinel should be fed `--skip-tombstones`,
which omits tombstones entirely.

**Range tombstones** (sorted zones, [`range-deletes.md`](range-deletes.md)): a
zone holding range tombstones dumps as `b"VDUMP\x02"`; a zone without them keeps
the `\x01` layout byte for byte, so existing replayers still read it. Before
streaming, the dump reads the range tombstones of the zone's runs and of its
uncovered log tail, and writes their canonical fragment set first, one **range
record** per fragment: `[u32-le lo_len | 0xFFFF_FFFF][lo][0xFFFF_FFFE][u32-le
hi_len | 0xFFFF_FFFF][hi][u128-le version]` (`0xFFFF_FFFF` = unbounded, and
`0xFFFF_FFFE` in the value-length position marks a range record). `version` is
the packed engine version (`hlc << 64 | writer`). A replayer applies the range
records before the point records (for example as range deletes); every point
record that follows is newer than any range that holds its key, because the dump
**skips every log record the set covers** (`record version <= fragment
version`): value-log GC can relocate a covered value after its range tombstone,
so emitting it in place would replay it back to life. What remains replays in
file order to the zone's final state, as for `\x01`. The summary line counts the
range tombstones and the covered records skipped. On a live node, a range delete
applied after the dump has read that set is absent from the dump, like any
record written after the scan passed it. A `\x01`-only replayer refuses a `\x02`
file on its magic. `valsdump replay` (0468) reads the range records of every input
file first and applies them as range deletes, waits until the wall clock has
passed their versions, and then replays the point records, so every replayed
point is newer than every replayed range delete.

## systemd

On hosts using unattended-upgrades and needrestart, library upgrades can restart
valsd services, including transient benchmark units. Exclude vals units from
needrestart with `/etc/needrestart/conf.d/90-vals.conf` containing
`$nrconf{override_rc}{qr(^vals)} = 0;` (covers `vals-node`, `vals-pd`, and
`vals0297-…`). Keep unattended-upgrades enabled; schedule vals restarts explicitly.

```ini
# /etc/systemd/system/vals-node.service
[Unit]
Description=vals data node
After=network-online.target
# Restart-storm guard: at most 4 starts per 30 min, then the unit stays failed.
StartLimitIntervalSec=1800
StartLimitBurst=4

[Service]
ExecStart=/usr/local/bin/valsd node -c /etc/vals/node.conf
Restart=on-failure
RestartSec=10s
# Exit 2 is a config/usage error and exit 1 a startup/recovery failure (e.g.
# on-disk corruption): restarting cannot fix either. Exit 75 (integrity
# fail-fast) and crashes restart.
RestartPreventExitStatus=1 2
# Graceful drain on stop; the default SIGTERM is exactly right.
TimeoutStopSec=30
Environment=VALS_LOG=info

[Install]
WantedBy=multi-user.target
```

The PD unit is identical with `pd -c /etc/vals/pd.conf`. For SPDK nodes add
hugepage + vfio setup (see [`SPDK.md`](SPDK.md)) as `ExecStartPre` steps or a
separate oneshot unit.

### Integrity fail-fast (exit 75)

`valsd node` exits with code **75** (`EX_TEMPFAIL`) when an in-RAM integrity
check fails persistently (task 0370). This needs the unit's `Restart=on-failure`.
The trigger is the front-coded memtable guard refusing to flush the same block
of the same zone engine for `integrity_fail_fast_attempts` consecutive flush
attempts (default 3) or for `integrity_fail_fast_secs` (default 30 s, at least
two attempts); see [configuration.md](configuration.md). Before 0370 the node
retried that flush forever and stalled RF2/W2 ingest (non-ECC RAM bit flips,
0363 FULL3/AB01).

Since 0384 a second in-RAM check feeds the same trigger: an off-core flush
whose encoded run blocks do not decode back to the entries the memtable guard
attested (or are not strictly ascending) fails before publication, and the same
memtable block (first key of the attested group) failing on consecutive flush
attempts counts toward the same streak. Merge-output encode failures never
fail fast; the merge retries from its on-disk inputs.

What happens and what operators see:

- **Before the exit**, every failed attempt logs an ERROR
  `memtable flush aborted owner=… front-coded index integrity mismatch
  block_first_key=[…] entries=N packed_bytes=M diagnosis=…`. A single-bit
  diagnosis reads, for example,
  `diagnosis=single-bit region=key key_index=0 offset=19 actual=0x7f
  expected=0x6f xor=0x10 candidates=1 window@11=[…]`. `region` is one of `key`,
  `key_len`, `entry.segment`, `entry.offset`, `entry.len`, `entry.seq`,
  `entry.version`, `block_key` or `checksum`. `diagnosis=unexplained` means no
  single-bit flip explains the mismatch, which points to multi-bit damage or a
  software bug. `vals_memtable_integrity_failures_total` and
  `vals_snapshot_errors` rise.
- **Encode verification (0384)** logs an ERROR `run encode integrity mismatch
  path=flush|merge check=… block=B entry=E source_first_key=[…] … ; aborting
  the run build without publishing it`, followed by the usual `off-core flush
  write failed (retrying next tick)` or compaction WARN. `check` is `order`
  (the encoded key is not strictly above the previous one; the line carries
  both keys in hex), `source-hash` (the decoded entries do not hash to the
  source), `decode` (the block buffer does not parse) or `index`. For
  `source-hash`, `stage=encode` means the encoded bytes differ from the source
  entry (a byte-wise diff, with `diagnosis=single-bit region=key|entry.segment|
  entry.offset|entry.len|entry.seq|entry.version|entry.tombstone …` when one
  bit differs; offsets are within the key or within the run entry fields) and
  `stage=source`/`source-copy` means the source copy itself changed after it
  was attested (a single-bit search; offsets as in the 0370 diagnosis).
  `vals_encode_integrity_failures_total{path="flush"|"merge"}` counts every
  aborted build, including retries, and must stay 0. Nothing is published:
  the reserved run segments are freed, the memtable is untouched, and the next
  tick retries.
- **The trigger** logs one ERROR, `persistent in-RAM memtable integrity failure
  … failed its checksum or its flush encode verification … failing fast …`,
  with the attempt count, the elapsed time, the single-bit
  XOR and the full evidence line. It increments
  `vals_integrity_fail_fast_total`; a scrape catches it only during the short
  shutdown window, so the log and the exit status are authoritative. valsd then
  logs `integrity fail-fast: graceful shutdown without flushing the corrupt
  memtable, then exit 75`.
- **The shutdown** is the normal SIGTERM drain. Memtables are never flushed at
  shutdown: the drain syncs the value log (the client bytes as written, not the
  corrupt in-RAM copy) and persists the liveness watermark. No run or manifest
  derived from the corrupt memtable can be written **on this node**, because the
  guard refuses that block on every attempt. The guarantee is local: during the
  detection window, AE digests/cleanup and rehome still read the memtable
  unchecked, so a corrupt key can reach a peer and be stored there (AE and the
  0367 cleanup treat it as a wrong-shard or extra key). Client gets and scans
  of the corrupt key fail instead, because the record's key no longer matches
  (0339, below). If the drain takes longer than 30 s, a watchdog
  calls `_exit(75)`. That is crash-equivalent, with the same durability as a
  `kill -9`: durably acknowledged writes are already synced and are replayed.
  There is no panic and no core either way.
- **The exit and restart.** systemd logs `status=75` and restarts the unit after
  `RestartSec`. Recovery replays the value-log tail into a fresh memtable, and
  the next flush passes the guard (verified on hardware in the 0363 FULL3
  restart check). The kernel drops the SPDK device lock with the process on
  every exit path. Since 0424 the shim unlinks each `spdk<id>map_*` hugepage
  file right after creating it, so the kernel frees every hugepage when the
  process exits on any path and the restart needs no cleanup step (0337;
  [SPDK.md](SPDK.md)). On hardware, 8 `Restart=on-failure` restarts after
  exit 75, SIGSEGV and SIGABRT found 0 map files, and all 4096 hugepages
  were free after the final stop (0386 R4). Binaries built before 0424 leave
  the map files behind on every exit path. Their `ExecStartPre` must remove
  stale maps for the node's own prefix, but only when no `valsd` is running.
- **Restart storms.** A deterministic software bug, or a persistent memory
  fault on the host, would fail again after every restart. Restart storms are
  bounded by systemd's `StartLimitIntervalSec`/`StartLimitBurst` (above: 4
  starts per 30 min, then `start-limit-hit` and the unit stays failed). No
  persisted restart ledger is kept. Manual `systemctl restart`s count toward
  the same burst, so after rolling restarts run `systemctl reset-failed
  vals-node` before relying on the limit. Alert on unit failure or repeated
  `status=75`. A node that stays down is handled like any other node outage:
  peers keep serving and AE heals it once it returns. Run `memtester`, and
  compare the logged XOR/region across incidents before returning the host to
  service.
- **Opting out.** Set both thresholds to 0 to opt out. The node then retries
  the corrupt flush forever, as before 0370.

Never matched: a mismatch that a later retry clears resets the streak; run
index, merge-read and cover-scan CRC errors (on-disk signals, e.g. 0363 FULL4's
transient merge CRC) and recovery failures keep their existing handling. Library
embedders of `vals-cluster` get only the request flag
(`vals_cluster::integrity::fail_fast_requested()`); `valsd` performs the exit.

### Corrupt value reads (0339)

Every served value read checks the record it decodes against the index entry
that pointed at it: magic, kind, length and CRC (as before), plus, since 0339,
the record's key and version. This covers gets, scans, mixed batch reads and AE
sync-values (key only). A pointer that lands on a different valid record of the
same length (or on an older record of the same key) now fails the read instead
of returning the wrong value. No persisted format changed.

- The client gets an error reply such as `corrupt record: length mismatch` or
  `corrupt record: record key mismatch`; a quorum read can still succeed from
  the other replica.
- `vals_store_corrupt_reads_total{kind="magic"|"kind"|"length"|"crc"|"key"|"version"|"other"}`
  counts each failed reply. A read that raced with maintenance and was
  retried (storage generation changed) is not counted. It must stay 0.
- An ERROR line names the replica and the pointer, at most one line per 10 s per
  shard thread (the rest are counted and the next line reports how many were
  suppressed): `corrupt value read: zone Z shard S get|scan|mixed|sync-values
  key <hex, first 48 bytes>[…] (N bytes) segment G offset O len L: <reason>`.
- The damage stays until the key is rewritten. The 0336 case (one bit in a
  persisted `ValuePtr.len`) shows up as `kind="length"`. AE does not repair it,
  because digests cover (key, version) only.

Merges read values only for value-log GC rewrites (victim segments, or every
value when co-compaction is opted in). Since 0436 the rewrite runs the same
checks, key and version included, so a wrong-record pointer can no longer be
re-appended as a self-consistent record holding another key's value:

- The merge fails before publication. The pointer, the record and the input
  runs stay as they were (the evidence is kept), and reads of that key keep
  failing as above. The node does not exit: this is persisted damage that a
  restart cannot fix, so it is not an integrity fail-fast (exit 75) case.
- It is counted in the same `vals_store_corrupt_reads_total{kind=...}` and logged
  under the same rate limit, with path `merge rewrite`: `corrupt value read:
  zone Z shard S merge rewrite key <hex> (N bytes) segment G offset O len L:
  <reason>; failing the merge unpublished; the segment is excluded from
  value-log GC until restart`.
- The segment holding the bad record is then left out of GC victim selection and
  of later rewrites in RAM until restart (one `engine: value segment G holds a
  corrupt record ...` ERROR line per segment), so the zone's other merges and
  space reclaim go on. The segment's space is not reclaimed. After a restart the
  first GC merge that reaches it fails once and excludes it again.

## Operational notes

- **Secret everywhere.** Set the same `secret_file` on every node and PD; it
  gates the control ops and the PD raft port. Without it the cluster is open
  to any process that can reach the ports. It's cleartext on the wire — run
  on a private network or an overlay such as Tailscale/WireGuard; vals does
  not encrypt its transport.
- **Run three durable PD replicas in production/reference deployments**, one per
  independent host or failure domain, as specified under [PD availability and
  recovery](#pd-availability-and-recovery). A PD quorum outage conditionally
  preserves an already-running data path on its last pushed config, but blocks
  all reconfiguration and restart healing; it is not an unconditional data-path
  availability guarantee.
- **Restarts heal themselves.** The PD probes nodes every second and re-pushes
  config to any that restarted; nodes reopen their persisted zones at boot.
- **Capacity-weighted rings**: file nodes default to weight 1; SPDK JBOD nodes
  log their drive-count weight at startup — set it in the PD config
  (`data_node = <id>*<weight> @ ...`) or pass it to `add-node`, so placement
  tracks capacity.
- **Metrics**: set `metrics = <addr>` (node and/or PD config) to expose the
  process-wide counters as Prometheus text over HTTP — every counter becomes
  `vals_<name>` with its doc line as `# HELP`. Unauthenticated by design: bind
  a private address, like any metrics sidecar. In-process,
  `observe::snapshot()` remains the programmatic surface.

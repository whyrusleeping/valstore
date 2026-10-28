# On-disk format history

This is the history of on-disk format changes made before the first public
release of vals. Current byte layouts are in [`formats.md`](formats.md), and
the rules an operator still needs are in
[`deploy.md`](deploy.md#on-disk-format-and-compatibility).

Nobody runs the older versions described here outside the original
development cluster, so a new deployment can ignore this page. It is kept
because older notes, benchmarks and backlog tasks refer to these formats and
to the upgrade and rollback steps that were used at the time.

The general pattern: a newer binary reads every older format, and an older
binary refuses a format it does not know at open time (before it writes
anything) instead of misreading it. That is why a downgrade "fails closed"
below.

## Run format v6 (range-delete support)

New flushes and merges write run format v6. It adds a per-block version bound
(`max_hlc`, 8 bytes per block, kept in memory with the fence index) and room for
range-tombstone records ([`range-deletes.md`](range-deletes.md)). Without range
tombstones it is otherwise the v5 layout. Merges rewrite v5 inputs as v6.

- **Upgrade.** No action was needed; v4 and v5 runs stay readable.
- **Downgrade fails closed.** A binary that predates v6 rejects a v6 run footer
  (`unsupported run version`) before log replay, so a zone holding a v6 run
  does not open on it.
- **Rollback window.** Starting the new binary with `VALS_RUN_WRITE_VERSION=5`
  *before* any v6 run was written kept it writing v5. A merge with any v6 input
  still writes v6, so once a v6 run existed, downgrade was closed until the node
  was rebuilt from peers. Range deletes are refused while runs are written below
  v6.

## Range deletes rollout

Range deletes were introduced together with run format v6, a new log record
kind, and new dump and backup encodings.

- **Downgrade fails closed.** Before an engine stores its first range tombstone
  it commits a v6 run, and every later manifest keeps at least one. A binary
  without v6 support therefore stops at run open (`unsupported run version`)
  instead of replaying a log record kind it does not know. Development builds
  that knew v6 runs but not every range-delete rule were not valid downgrade
  targets for a node that had accepted a range delete.
- **Rollback window.** A node started with `VALS_RUN_WRITE_VERSION=5` (or `4`)
  refuses range deletes with a per-slice error (counted in
  `vals_range_delete_refused_total`), so it never needs the v6 fence.
- **Clients.** Clients built before range-tombstone support (hello feature bit 3)
  kept working on zones without range tombstones; their scans that met one
  failed instead of returning covered keys.
- **Backups and dumps.** `valsd backup create` began writing zone files as
  version 2, which older builds refuse. `valsd dump` writes `VDUMP\x02` only for
  a zone that holds range tombstones; older replayers refuse `\x02` on its
  magic.

## Run format v5 (no per-run sidecar)

Run format v5 dropped the per-run sidecar, which was a second copy of every
key and version. Anti-entropy seeds, digest pages and cover scans read the same
`(key, version)` stream from the run's data blocks instead. This saved about
20 bytes plus the full key per stored entry, both on disk and in write volume.
v1–v4 runs stay readable, and merges convert them.

- **Upgrade.** No action was needed. Mixed-format engines are supported; the
  `sidecar_disk_bytes` capacity statistic falls to zero as merges rewrite old
  runs.
- **Downgrade fails closed.** A binary from before v5 rejects a v5 run footer
  (`unsupported run version`). This was verified with a file-backed node: the
  old binary logged `node 1 failed to start: corrupt snapshot: unsupported run
  version` and exited 1. Run footers are read before log replay, so it issued
  only reads, no writes or flushes; the zone file was byte-identical afterwards,
  and the node restarted on the new binary with no repair step.
- **Rollback window.** `VALS_RUN_WRITE_VERSION=4`, set *before* any v5 run was
  written, kept new runs in the sidecar format. It does not convert existing v5
  runs: a merge with any v5 input still writes v5, because its output
  reservation is sized from its inputs. Values other than `4`, `5` or `6`,
  including an empty string (an unexpanded template variable), log an error and
  write v4, so a misconfiguration keeps a rollback window open rather than
  closing it. `vals_run_write_version` shows the active value.

### Build verification of merge output

With v5 the sidecar-build readback was replaced by a full readback of each
streamed merge output before publication (`VALS_RUN_BUILD_VERIFY`, on by
default). The current behaviour is summarized in
[`deploy.md`](deploy.md#merge-output-verification). Two later fixes changed
what it covers:

- Originally the final segment of a run was checked from the writer's RAM
  buffer, so its device write was not verified. The check now starts only after
  every write of the run has completed, and every read goes to the device.
- Sync flushes and full merges (`merge_all_runs`) used to CRC-check their
  footer, filter and index after the manifest write. The check now runs before
  the manifest write, so a failing run is never published and its segments are
  freed.

The added cost of the second change was a byte compare of footer, filter and
index of about 0.3–0.45 ms per 3.5 MiB of tail (about 10–15 ms for a 2.1 GB run
with a ~113 MiB tail). Peak transient memory did not grow: the builder's own
index and filter are freed before the readback, and each encoded copy is freed
right after its compare.

## Run blocks, run blooms and key-filter bits

This change kept run format v5 but changed the defaults for newly written
runs and for the in-memory key filter:

| Setting | Before | After |
|---|---|---|
| Run data block target | 4 KiB | 32 KiB |
| Run bloom | 12 bits/key | 10 bits/key |
| Engine key filter | 13 bits/key, growing one bit per sub-filter to 18, capacity ×1.5 per step | 12 bits/key in every sub-filter, capacity ×2 per step |

- **No format change.** Each run's index stores every block's length and each
  bloom stores its own size and probe count, so old and new runs are read side
  by side and merges rewrite old ones with the new settings. A binary from
  before the change reads 32 KiB-block runs. The key filter is RAM-only and is
  rebuilt at startup.
- **Upgrade.** Resident fence bytes fell as merges rewrote old runs: about
  1.1 bytes per entry for 148-byte keys and 0.4 for 70-byte keys at 32 KiB,
  versus 8.5 and 2.9 at 4 KiB.
- **Estimator side effects.** The space estimator counts a sampled run entry as
  shadowed when a newer overlapping run's bloom says "maybe", so 10-bit blooms
  (about 1% false positives, was about 0.4%) bias
  `vals_shadowed_value_bytes_est` upward by roughly 1% of a run's unshadowed
  value bytes per newer overlapping run. The range midpoint used for splits
  needs at least two blocks of a run inside the range, which is roughly 300–600
  entries per run at production key sizes (it was about 40–80).
- **Overwrite cost measured at the time.** Release build, 20k hot keys:
  9.8–10.2 s at 32 KiB versus 8.2–8.9 s at 4 KiB. The debug
  `churn_reclaim_wedge` test took 31.7 s versus 13.1 s with 64 KiB segments
  (60+ s versus 23 s with 4 KiB segments), so that test pins 4 KiB blocks.
- **Rolling back the settings** (all read once at startup; existing runs keep
  their shape until merged again):
  - Old run shape: `VALS_RUN_BLOCK_TARGET_BYTES=4096` and
    `VALS_RUN_BLOOM_BITS_PER_KEY=12`.
  - Old key-filter schedule: `VALS_KEY_FILTER_BITS_PER_KEY=13`,
    `VALS_KEY_FILTER_MAX_BITS_PER_KEY=18` and `VALS_KEY_FILTER_GROWTH_PCT=150`.
    The cap is never below the bits setting, so with the default cap of 12,
    setting only `VALS_KEY_FILTER_BITS_PER_KEY=13` gives a constant 13. The
    probe count is round(0.6 × bits) for explicit bit counts too (13 bits → 8
    probes; it used to be bits/2 = 6), so the old filter is not reproduced bit
    for bit; its false-positive rate is slightly lower.

## Cluster incarnation id (zone superblock v4)

The cluster incarnation id ([`deploy.md`](deploy.md#cluster-incarnation-id))
was added to an existing fleet:

- The first upgraded PD voter to lead minted the id for an existing group, the
  same way a new group mints it.
- Data nodes and PDs could be upgraded in either order: older nodes ignored the
  trailing id on pushes, and upgraded nodes accepted id-less pushes until bound.
  Every node stayed unbound, and so unfenced, until an upgraded PD had pushed
  to it, and a node still running an older binary enforced nothing.
- PD voters had to be upgraded in one pass, followers first and the current
  leader last. Once the id existed, an older voter that became leader pushed
  without an id, and bound nodes refused all its pushes (the data path was
  unaffected). A PD voter could not be rolled back after the id existed.
- A bound node's superblock is format v4, which older binaries refuse to open.
  To downgrade, the node was stopped and `valsd reenrol --clear` was run with the
  new binary first; it rewrites both superblock slots as v3.

## PD range maps as per-range keys

Range maps were moved from one whole-map key per zone to versioned per-range
keys written by an atomic conditional-batch Raft command. Existing whole-map
state was read unchanged and migrated on that map's first mutation. Older PD
binaries do not understand the new command, and a new snapshot no longer
contains the old whole-map key, so every PD peer had to be upgraded before any
range mutation, and no old peer could be started against that Raft directory
afterwards.

At the same time the PD started sending a `SetRangeMaps` push even when no zone
has a range map yet. An older PD that had never split a sorted zone sent none,
which left upgraded data nodes holding their sorted zones, so the PD had to be
upgraded before the data nodes.

## Segment-header wipe on format

Boot recovery and the raw value-log scan both treat a segment as live when its
header block decodes at the open geometry. On a reused drive that was not
zeroed, that rule alone was unsafe: a previous layout's CRC-valid segments (and
manifest chains) could survive a "format, then write a smaller data set" cycle
beyond the new log head. An ordinary reopen replayed them into the served
index, and because replay picks the highest `version` (with `seq` only breaking
ties), stale records with higher versions could win over live ones. The
multi-drive SPDK partition path could reach this in production because opening
a new region deliberately does not wipe it.

Every format path now durably zeroes every segment-start header block before
use (`Engine::create`, including the multi-drive partition path,
`wipe_region` for zone-region allocation, and `Engine::truncate`), so a valid
header always postdates the last format. The regression test is
`crates/vals-store/tests/format_fence.rs`. A format-generation stamp was
considered and rejected, since it cannot clean up drives that are already
contaminated and adds nothing over the wipe (see `formats.md` §2.2).

Drives last formatted by a build from before the wipe can still hold stale
segments that look exactly like live data; no software check can filter them.
Such a drive needs one offline `valsd provision` with a current build before it
is reopened or dumped. Until then its dumps contain extra records and a reopen
may bring stale records back.

## SPDK layouts before the V3 topology descriptor

SPDK layouts provisioned before the V3 topology descriptor are refused by
startup and by the checked offline tools; they must be reprovisioned. Startup
used to format devices when the config said `format = true`; that key is now
rejected with migration guidance, and `format = false` is ignored.

## Other changes from the same period

These changed no on-disk format, but were part of the same upgrade notes:

- **PD pre-vote and check-quorum** changed no wire or storage format, so old and
  new PD replicas could be mixed during a rolling upgrade. Before it, a replica
  that could not hear the leader kept raising its term and could depose a
  healthy leader when it rejoined.
- **PD raft tick.** A raft tick used to be one drive-loop iteration (about 5 ms)
  instead of 100 ms, so a slow presplit `create-zone` could depose the leader and
  lose its balanced initial map. An older replica with `campaign = true` also
  campaigned on every restart, not only on an empty `data_dir`.
- **SPDK hugepage map files.** Older builds left `spdk<id>map_*` hugepage files
  behind on every exit, so their units needed an `ExecStartPre` that removed
  stale maps for the node's own prefix while no `valsd` was running. The shim
  now unlinks each file right after creating it.
- **Retired scan switches.** `VALS_STAGED_SCAN` and `VALS_SCAN_PREFETCH` are no
  longer read. Server scans always use staged selection, and client prefetch is
  on unless an application calls `Coordinator::set_scan_prefetch(false)`.

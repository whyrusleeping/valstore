---
id: "0141"
title: Durability asymmetry — single writes never sync, batch writes sync once per shard per batch (8.6× throughput gap)
status: done
priority: 3
created: "2026-08-06"
updated: "2026-08-10"
depends_on: []
spec_refs: []
---

## Description

> Originally filed as "batch writes are 4-17× slower than single writes". Profiling
> the node during a batch load (task 0138) showed the comparison was not
> apples-to-apples, and turned up a semantic asymmetry that is the real issue.

### What the numbers looked like

Measured on the lab cluster, **unsorted default zone** (no range map involved):

| path | throughput |
|---|---|
| `vals-bench netload`, single `write_tier`, `Quorum(2)`, 1024 ops in flight | 790k keys/s |
| `valsmix` background writers, single writes | 191k keys/s |
| `valsmix` load, `write_batch_in`, 256/batch, `--tier majority` | 46.5k keys/s |

### What the profile showed

`perf` on a serving node during the batch load (`bench/lab-bench-20260806/prof/batch-write.*`):

```
93.6%  valsd userspace, 2.5% kernel      <- almost no network work
38.1%  spdk_nvme_qpair_process_completions
29.6%  nvme_pcie_qpair_process_completions
 7.2%  vals_store::vlog::ValueLog::sync
 3.5%  vals_spdk_flush
```

The shard is spinning on NVMe completions waiting for **durable syncs**, not doing
CPU work (2.4 CPUs busy, kernel/net negligible — versus 20-33% kernel in the
single-op read/write profiles).

### The asymmetry

- `Request::Apply` (the single-write path) carries **no sync flag at all**
  (`crates/vals-client/src/net/client.rs:657`, server at
  `crates/vals-cluster/src/net/server.rs:2524`). A single write is never forced to
  the device; the durability tier only decides *how many replicas must ack*.
- `Request::BatchApply` carries `sync = tier != Durability::NoSync`
  (`crates/vals-client/src/coord/mod.rs:1580`), so every tier except `nosync`
  forces a `ValueLog::sync` per batch, per shard.

So `write_tier(Quorum(2))` and `write_batch_in(..., Majority)` do **not** provide
the same durability, despite both being described by the same `Durability` enum —
and the benchmark was comparing a memory-durable path against a device-durable one.

### Confirming A/B

Same tool, same zone, same shape, only the tier changed (4M keys, 8 load threads):

| tier | load rate |
|---|---|
| `majority` (syncs) | 14,093 keys/s |
| `nosync` (no sync) | **120,506 keys/s** — 8.6× |

The remaining spread is explained by **how many shards a batch touches**: the sync
cost is paid once per shard per batch, so a hash-zone batch of 256 random keys hits
all 12 shards and pays 12 syncs, while a sorted zone's contiguous 256-key batch hits
1-2 ranges (measured: 214k keys/s on the sorted zone vs 46.5k on the hash zone, both
`--tier majority`).

## Why it matters

1. **Semantics**: `Durability` claims to describe durability, but for single writes
   it only selects a replica count — a caller asking for `Majority` on the single-write
   path gets memory durability on those replicas, while the same tier on the batch
   path gets device durability. Whichever behaviour is intended, both paths should
   mean the same thing, and the docs should say which.
2. **Performance**: if syncing per batch per shard is the intended behaviour, the
   cost should be amortised — group-commit across concurrent batches on a shard
   (one device sync serving N in-flight batches) rather than one sync per batch.

## Acceptance criteria
- Decide and document what each `Durability` tier means for *both* the single and
  batch write paths; make the two paths agree (`docs/batch-writes.md`, `docs/algorithms.md`).
- Amortise the device sync: a shard with N concurrent batch applies in flight should
  issue O(1) syncs, not N. Show the unsorted batch load rate at `--tier majority`
  moving materially toward the `nosync` number.
- Bench arm pinning batch-load throughput at a fixed tier so it does not regress.

## Artifacts
- `bench/lab-bench-20260806/notes.md` (§ "Where the time goes", § "Batch writes")
- `bench/lab-bench-20260806/prof/batch-write.{svg,selftime.txt,dso.txt,perfstat}`

## Decision (2026-08-10, user)

Promoted with direction: **replica-memory-ack is a legitimate durability tier** —
writes acked by memory on multiple machines are "safe enough" and that behavior
stays available as a reasonable option. The requirement is *clarity and
consistency*: each `Durability` tier must mean the same thing on the single-write
and batch paths, and the docs must say exactly what each tier buys (replica-ack
count vs device sync). Device-sync remains available (explicit tier or orthogonal
flag — design choice), and the per-shard sync cost gets group-commit amortization
(N in-flight batches per shard share O(1) syncs) rather than one sync per batch.

## Plan

1. Make the public durability contract explicit and consistent: treat `Durability` solely as the replica-memory acknowledgement breadth on both single and batch writes, preserve that behavior as the default, and add an orthogonal public persistence selector/API for callers that require a device-sync acknowledgement. Ensure both single and batch paths can request either buffered or device-synced acknowledgement without changing their replica-count semantics.
2. Carry the persistence request through the wire/server paths (including single `Apply`) and implement a per-shard, per-zone group-commit barrier. Concurrent device-persistent applies/batches that have installed their writes before one barrier should share one engine sync and all receive the same success/failure result; avoid letting an acknowledgement escape before the relevant barrier.
3. Add focused tests for tier/persistence mapping, wire round trips, single-vs-batch consistency, sync error propagation, and coalescing N concurrent durable requests into O(1) syncs. Preserve existing backpressure, re-home mirroring, and outcome behavior.
4. Update `docs/batch-writes.md` and `docs/algorithms.md` (and any public API docs/examples that currently imply otherwise) with an exact matrix of replica breadth versus persistence and the group-commit semantics.
5. Update the existing distributed batch benchmark so its comparison pins one replica tier and varies only persistence, providing a repeatable regression arm; run targeted tests plus the relevant client/cluster suites. If available in this environment, run the fixed-tier benchmark and record the result/artifact; otherwise leave an exact hardware run command and validate the coalescing/performance mechanism with deterministic instrumentation.

### Starting points
- crates/vals-client/src/coord/mod.rs: Durability and batch_once currently derive sync from tier
- crates/vals-client/src/wire.rs: Apply lacks sync; BatchApply carries bool
- crates/vals-cluster/src/net/server.rs: serve_apply/serve_batch_apply and ServerShared batch_apply_gate
- crates/vals-bench/src/main.rs: workload_distbatch currently varies NoSync vs Replicated, conflating axes
- docs/batch-writes.md already sketches breadth and persistence as orthogonal but contradicts as-built behavior

## Work log
- 2026-08-10 plan: 1. Make the public durability contract explicit and consistent: treat `Durability` solely as the replica-memory acknowledgement breadth on both single and batch writes, preserve that behavior as the d
…[truncated]
- 2026-08-10 context hints: 5 recorded with plan
- 2026-08-10 context hints: crates/vals-client/src/coord/mod.rs: Durability and batch_once currently derive sync from tier; crates/vals-client/src/wire.rs: Apply lacks sync; BatchApply carries bool; crates/vals-cluster/src/net/s
…[truncated]
- 2026-08-10 preload: 9 file(s), ~64 KiB seeded into implementer context
- 2026-08-10 implementer report: Implemented task 0141 end to end.  Changes: - Split write guarantees into replica breadth (`Durability`) and orthogonal persistence (`Persistence::{Buffered, Device}`). Existing single/batch/delete AP
…[truncated]
- 2026-08-10 review tier: high-powered — reviewers: sol, claude
- 2026-08-10 review (sol): revise — The implementation establishes an orthogonal persistence selector, carries it through single and batch wire paths, adds a shared per-zone/shard group-commit mechanism with focused tests, and fixes the
…[truncated]
- 2026-08-10 review (claude): revise — The implementation is well-designed and correct in-repo: Durability becomes breadth-only on both single and batch paths, Persistence::{Buffered,Device} is an orthogonal public selector with defaults p
…[truncated]
- 2026-08-10 revision: Addressed both reviews.  Revisions: - Restored the protocol-v1 `Apply` schema exactly: no sync field in `Request::Apply`, `OwnedRequest::Apply`, encoding, decoding, fixtures, fuzz generation, or serve
…[truncated]
- 2026-08-10 review (sol): accept — The revision resolves the documentation contradictions and preserves the existing protocol-v1 `Apply` format by routing device-persistent single writes through a one-entry `BatchApply`. The public doc
…[truncated]
- 2026-08-10 review (claude): accept — The revision fully addresses the prior review. The major wire-compatibility break is resolved: protocol-v1 Apply is byte-identical to before (no sync field anywhere), and explicitly device-persistent
…[truncated]
- 2026-08-10 decision: accept — commit: 0141: make write persistence explicit and coalesce syncs
- 2026-08-10 usage: 38,899,206 tok (in 4,275,681, out 106,021, cache_r 39,145,627, cache_w 213,745) · cost n/a (unpriced)
  implementer: 33,953,875 tok (in 2,281,409, out 48,018, cache_r 31,624,448, cache_w 0) · cost n/a (unpriced)
  coordinator: 2,491,314 tok (in 1,111,192, out 7,962, cache_r 1,372,160, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 2,415,397 tok (in 882,954, out 11,547, cache_r 1,520,896, cache_w 0) · cost n/a (unpriced)
  reviewer:claude: 38,620 tok (in 126, out 38,494, cache_r 4,628,123, cache_w 213,745) · cost n/a (unpriced)

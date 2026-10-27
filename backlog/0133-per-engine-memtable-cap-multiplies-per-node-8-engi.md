---
id: "0133"
title: 'Per-engine memtable cap multiplies per node: 8 engines × 4 GiB → OOM-killed nodes under replay'
status: done
priority: 1
created: "2026-08-06"
updated: "2026-08-07"
depends_on: []
spec_refs: []
---

## Description
`FLUSH_MAX_BYTES_DEFAULT` (4 GiB, `crates/vals-cluster/src/node.rs`) caps the memtable **per engine**, and its doc comment claims this bounds the flush-build peak to "~3× = ~12 GiB/node". That is only true for a single-engine node. A soak/replay node serves 4 SPDK partitions × 2 sorted zones = **8 engines**, each with an independent 4 GiB cap, so the true per-node worst case is ~8 × 12 GiB ≈ 96 GiB.

Observed in task 0132 arm 1 (corpus replay at ~90k logical rec/s): nodes 2 and 3 were OOM-killed at **47.1 GiB peak RSS** on 60 GiB boxes ~50 minutes in, while `live_keys` had plateaued (so this is flush/maintenance working set, not steady index residency) and node 3 had no region exhaustion at all. The reserve-floor `Busy` guardrail bounds *device* pressure only; nothing bounds aggregate RAM.

Fix direction: a node-wide memtable/flush byte budget shared across the node's engines (e.g. divide the configured budget by live engine count, or a shared admission token pool sized in bytes), so adding shards or zones cannot multiply the RAM ceiling. Should also be observable (per-node memtable bytes gauge) and documented.

Acceptance criteria:
- Per-node aggregate memtable/flush-build RAM is bounded by a single configured budget regardless of shard × zone count.
- Documented default is derived from the node's engine count; `VALS_FLUSH_MAX_BYTES` (or its successor) keeps a clear per-node meaning.
- A gauge exposes aggregate memtable bytes (and in-flight flush build bytes) per node.
- Unit/integration coverage for the shared budget under many engines.

**Scope correction (2026-08-06, after a controlled re-run):** re-running the same
replay with `VALS_FLUSH_MAX_BYTES=536870912` (8× smaller cap, verified in
`/proc/<valsd>/environ`) changed neither the RAM trajectory nor the outcome —
nodes 2 and 3 still OOM-killed at 47.1 GiB peak. The per-engine multiplication is
a real hazard worth fixing, but it is **not** what killed the nodes; the dominant
terms are the fully-resident per-key index (~240–400 B/key) plus an un-budgeted
maintenance working set. The "replay completes without an OOM kill" criterion
therefore belongs to task 0134 (RAM backpressure), not here. Evidence:
`bench/replay-lab/run-20260806T001507Z-tier-one-flushcap512m/FINDINGS.md`.

## Acceptance criteria

## Work log

- 2026-08-07 CLOSED (folded into task 0134, owner decision): the per-engine memtable-cap multiplication is real but the controlled `VALS_FLUSH_MAX_BYTES=512 MiB` re-run proved it is not the OOM cause, and the fix (a node-wide byte budget) is the same mechanism 0134 needs. Its acceptance criteria now live in 0134 under "Folded in from task 0133".

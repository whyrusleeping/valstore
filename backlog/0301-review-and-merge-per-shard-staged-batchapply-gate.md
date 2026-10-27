---
id: "0301"
title: Review and merge per-shard staged BatchApply gate 8 -> 32 (branch 0297-staged-gate-32)
status: done
priority: 1
created: "2026-09-22"
updated: "2026-09-22"
depends_on: []
spec_refs: []
---

## Description

## Description
## Why

Task 0297 SCALE01 (`bench/0297-ingest-1m/scale01/FINDINGS.md`): every pipelined PutBatch reaches every shard, so `MAX_STAGED_BATCH_APPLIES = 8` (`crates/vals-cluster/src/net/server/apply.rs`) caps the whole cluster at 8 in-flight PutBatches and sheds Busy beyond (PRESSURE01-K8 collapse to 39.6k; SCALE01-Y2 at depth 24 still shed 9–11 against 32). With 32, depth 8/16 run with zero sheds; master + this one constant (`8f2b4649`, attempt SCALE01-Y5) reached 930k ACK KV/s on the realistic two-node RF2/W2 device-sync fixture, bound by client egress.

## What

Review the single-constant change on `0297-staged-gate-32` (cherry-pick of `415e137c` onto `02f169a1`): confirm the original 0130/`df6599a5` concerns (reply deadlines, rehome mirror timeouts under queued work) still hold at 32 — bounded queued staged work ≈ 32 sub-batches (~100 ms of serving-core work at 1000 entries), far below the 5 s client deadline; run full vals-cluster debug suite; merge.

## Acceptance

- vals-spdk/vals-store/vals-cluster debug suites pass on the branch.
- Reviewer confirms mirror/rehome Busy semantics unchanged except the higher threshold (sheds still occur above 32).
- Merge to master with a short docs/backpressure.md note citing SCALE01 evidence.

Not in scope: async flush / node flush domain (separate proposal), client changes.

## Acceptance criteria

## Outcome

MAX_STAGED_BATCH_APPLIES raised 8->32 on master (content of branch 0297-staged-gate-32 / 8f2b4649) plus a docs/backpressure.md "Staged BatchApply gate" note citing SCALE01 Y5/Y2/K8 and the 2 s mirror / 5 s client deadlines. Debug vals-spdk/vals-store/vals-cluster suites pass on the branch (cluster 631 unit + all integration, store 312) and independently on the review snapshot; opus reviewer confirmed try_acquire-or-Busy shedding (batch_apply_gate_busy) is unchanged for client and rehome-mirror BatchApply, only the threshold moved.

Commit: perf(cluster): per-shard staged BatchApply gate 8 -> 32 (0301)

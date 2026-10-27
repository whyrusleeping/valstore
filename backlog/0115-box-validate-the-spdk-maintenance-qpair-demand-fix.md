---
id: "0115"
title: 'Box-validate the SPDK maintenance-qpair demand fix (0114): sustained multi-shard/zone soak shows 0 qpair-alloc failures'
status: done
priority: 2
created: "2026-08-01"
updated: "2026-08-02"
depends_on:
    - "0114"
spec_refs: []
---

## Description
## Context
Task 0114 fixed the root cause in code: `SubDev::sibling()` now shares ONE maintenance parent qpair per worker thread across all zones of a partition, so SPDK NVMe I/O-qpair demand scales as ~`shards × 3` (serving + 2 shared maint siblings) instead of `shards × 2 workers × zones`. It also added observability: a `SpdkController.open_qpairs` gauge that logs the live count on each `open_shard` and the ceiling reached on the alloc-failure branch, plus a dedicated `maint_qpair_errors` counter (de-conflated from `snapshot_errors`).

The two acceptance criteria below are hardware-gated and could NOT be validated in the code session — they need the lab 3-node SPDK cluster + bootes soak (see `plans/bootes-soak-runbook.md`). Remember to REBUILD the on-box `~/vals` tree before the soak (rsync + `VALS_SPDK_LINK=1 cargo build --release -p vals-server --features spdk`).

## Acceptance
- [x] Confirm the controller's negotiated/usable I/O-qpair ceiling on-box (read it off the new `open_shard` INFO/WARN stderr logs — the count reached at first failure = the ceiling) and note the drive model/count.
- [x] Over a sustained multi-shard multi-zone soak (4 shards/node, sorted `records`+`backlinks`, RF 2), `maint_qpair_errors` stays 0 and there are 0 "failed to allocate NVMe qpair" WARNs across all nodes.
- [x] Off-core compaction keeps up: `run_merges` proceed and runs-per-range stays bounded (contrast with the pre-fix soak where node1 logged 31,838 failures / ~28–40 run_merges).


## Acceptance criteria

## Plan

1. Audit the already-committed task-0117 post-0114 hardware-run artifacts against every 0115 acceptance criterion, including exact topology/duration, `maint_qpair_errors`, allocation-warning counts, `run_merges`, and per-range run bounds; do not repeat a costly soak if this run is representative and complete.
2. Identify the deployed Samsung 9100 PRO model/count from the captured artifacts or directly from all three boxes, and inspect complete startup logs for the new `open_shard` live-qpair counts. Determine whether the existing evidence truly establishes the controller's usable I/O-qpair ceiling rather than merely the normal peak demand.
3. If the ceiling is not established, perform the smallest safe on-box controlled validation that drives qpair allocation to first failure without jeopardizing the accepted soak evidence or scratch devices; record the first-failure `open_qpairs` count and method. If no responsible existing mechanism can do this, document the hardware/driver-negotiated limit from an authoritative SPDK/controller source and clearly distinguish it from observed steady-state demand.
4. Add a focused 0115 validation report (or extend the existing post-0114 findings without duplicating raw artifacts) that maps quantitative evidence to all criteria: 4 shards/node, RF2 sorted records+backlinks, sustained duration/load, drive inventory, observed ceiling/peak, zero dedicated errors and warning strings, merge progress, and bounded runs-per-range versus baseline.
5. Run consistency checks over raw metrics/logs and review the documentation-only diff against the acceptance criteria. Preserve unrelated working-tree changes, mark 0115 done only if all criteria are supported, and commit only task-0115-owned changes.

### Starting points
- bench/bootes-soak-lab/run-20260802-post0114/FINDINGS.md — existing 30m47s post-0114 hardware run likely satisfies most soak criteria
- plans/bootes-soak-runbook.md — authoritative deployment and artifact-capture procedure
- crates/vals-spdk/src/spdk.rs — open_shard live-qpair logs and allocation-failure ceiling signal
- Task 0117 already captured/committed the post-0114 run; avoid rerunning equivalent work unless the qpair-ceiling criterion needs additional evidence

## Work log
- 2026-08-02 plan: 1. Audit the already-committed task-0117 post-0114 hardware-run artifacts against every 0115 acceptance criterion, including exact topology/duration, `maint_qpair_errors`, allocation-warning counts, `
…[truncated]
- 2026-08-02 context hints: 4 recorded with plan
- 2026-08-02 context hints: bench/bootes-soak-lab/run-20260802-post0114/FINDINGS.md — existing 30m47s post-0114 hardware run likely satisfies most soak criteria; plans/bootes-soak-runbook.md — authoritative deployment and
…[truncated]

- 2026-08-02 validation: accepted the committed 30m47s post-0114 soak (4 shards/node, RF2, sorted `records` + presplit `backlinks`): `maint_qpair_errors=0`, zero qpair allocation/SPDK queue-ID warnings, and 50 successful run merges across 409 flush plans. A no-I/O sequential `open_shard` probe on all three Samsung 9100 PRO [PM9E1] scratch controllers established the usable ceiling at 15 live qpairs; normal shared-maintenance demand peaked at 12. Quantitative evidence and raw probe outputs: [`bench/bootes-soak-lab/run-20260802-post0114/VALIDATION-0115.md`](../bench/bootes-soak-lab/run-20260802-post0114/VALIDATION-0115.md).
- 2026-08-02 implementer report: Completed task 0115 and committed it as `83952d6` (`0115: validate SPDK maintenance qpair fix on boxes`).  Changed: - Added `VALIDATION-0115.md`, mapping all acceptance criteria to the existing 30m47s
…[truncated]
- 2026-08-02 review tier: single-opus — reviewers: sol
- 2026-08-02 review (sol): accept — The task is substantively complete. The committed report maps the existing 30m47s, 4-shard/node, RF2, two-sorted-zone soak to the acceptance criteria; the raw metrics and logs support zero maintenance
…[truncated]
- 2026-08-02 revision: Fixed the factual total in `VALIDATION-0115.md` without changing the accepted conclusions: - Result paragraph: `167,395,805` replicated writes. - Cluster table note: `167,395,805` from process start. 
…[truncated]
- 2026-08-02 review (sol): accept — The revision correctly addresses the prior finding: the report now gives 167,395,805 process-start replicated writes, exactly matching the sum of the three final node counters, while the timed delta r
…[truncated]
- 2026-08-02 decision: accept — commit: 0115: record accepted SPDK box validation
- 2026-08-02 usage: 6,886,084 tok (in 962,628, out 45,696, cache_r 5,877,760, cache_w 0) · cost n/a (unpriced)
  implementer: 5,144,869 tok (in 516,707, out 23,746, cache_r 4,604,416, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 1,377,543 tok (in 354,996, out 18,771, cache_r 1,003,776, cache_w 0) · cost n/a (unpriced)
  coordinator: 363,672 tok (in 90,925, out 3,179, cache_r 269,568, cache_w 0) · cost n/a (unpriced)

---
id: "0354"
title: Quantify synchronous runtime DSM shard cost with provision state held fixed
status: done
priority: 3
created: "2026-09-30"
updated: "2026-10-12"
depends_on:
    - "0353"
    - "0355"
spec_refs: []
---

## Description
0353's ONa hardware arm measured synchronous runtime DSM commands at about 4.4–5.0 ms/command. The summed deallocation latency was 1.1–1.2% of window wall seconds per node, or 0.15–0.18% of measured shard CPU seconds. This is visible shard time, not evidence that DSM caused the observed ACK-rate difference: the existing kill switch changes both provision-time and runtime deallocation, and the provision-time DSM completion did not imply low post-ingest NUSE. See `bench/0353-dealloc/FINDINGS.md` and `/data/dataset/task0353-results/DEALLOC0353-ONa-analysis.json` for counters and denominators.

## Work
- Defer hardware cost experiments until the 0355 correctness investigation/retention decision permits them; do not overwrite the retained 0353 LONG corpus.
- Hold provision-time deallocation, initial NUSE, temperature and ingest setup fixed while toggling only runtime deallocation. Keep the durable manifest-barrier invariant.
- Measure DSM latency distributions/batch sizes, synchronous shard occupancy, ACK rate and vlog waits through a matched churn span. Process-wide latency totals are elapsed call time, not exclusive CPU attribution.
- Only if the isolated cost is material, evaluate a smaller tick budget or cooperative/asynchronous DSM completion with an explicit completion/lifetime and crash-safety design. Do not add an unmeasured scheduling path by default.

## Acceptance
A bounded matched comparison identifies the runtime-only cost and records whether a scheduling change is justified, with no correctness regression. If a change is justified, capture its durable-barrier and qpair ownership/lifetime requirements before implementation.

## Work log
2026-10-12: closed as moot. Runtime deallocation is off by default (0356), the measured synchronous cost was 0.15–0.18% of shard CPU, and the RAM prerequisite (0362) is closed won't-fix. Reopen only if runtime dealloc is turned back on by default.
2026-10-01 (coordinator, work-loop): BLOCKED on owner. The task forbids overwriting the retained 0353 LONG corpus (lab-1+3), lab-2 is held for 0248, and 0361 proved uncorrected bit-12 RAM errors on lab-1/3 (0362), so the required "no correctness regression" cannot be established on these boxes. Unblock: owner releases the lab-1+3 corpus and 0362 re-qualifies the RAM (clean 6 h memtester).

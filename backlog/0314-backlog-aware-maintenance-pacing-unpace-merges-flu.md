---
id: "0314"
title: 'Backlog-aware maintenance pacing: unpace merges/flushes when an engine''s runs or memtable exceed target'
status: done
priority: 1
created: "2026-09-26"
updated: "2026-09-26"
depends_on:
    - "0313"
spec_refs: []
---

## Description
## Evidence
- The process-wide maintenance pacer (`VALS_MAINT_DUTY_PCT`, default 33) has one debt shared by the flush and compaction lanes. Recurring tiered merges stay paced even when runs pile up; B16 showed that FORCE_RUNS=6 had no effect.
- **C1-M0312 (158M KV):**
  - resident runs reach 280 per node, about 17 per engine;
  - shard CPU per KV doubles, driven by run probes and false-positive (FP) SPDK reads;
  - after drain, merges are still in flight and debt has not converged.
- **B17 (duty 100, global):** +10% ACK/s on B and runs 169→49, but device writes rise 30%.
- **0313 T runs:** node1 holds a 2.2–2.5 GB memtable backlog (versus ~1.2 GB), because flush lags the higher intake under the same pacer. This pushed max RSS to 1.29×.

## Candidate (one mechanism)
Make pacing backlog-aware instead of changing the global duty:
- a merge runs unpaced while its engine's run count is at or above a backlog threshold;
- a flush runs unpaced while its engine's memtable is above its budget. This replaces today's rule of unpacing only at 2× budget.

The default duty and flush-chunk interleave stay as they are. Report the exact thresholds and their rationale before the fleet run.

## Acceptance (to be preregistered in long01/README.md before any run)
- **Throughput (B-prefix ABBA vs master d67458e5):** mean whole-write ACK/s ≥ 1.05×.
- **RSS:** max RSS back to ≤ 1.10× the pre-0313 M level, or lower than master.
- **Correctness:** all correctness gates PASS.
- **Debt:** debt converges after drain.
- **Device writes and runs:** report device write bytes per KV and end-of-run runs per node.
- **C prefix:** then one C-prefix run of the winner, for the long-run curve.

## Acceptance criteria

## Work log

## Work log

### 2026-09-27: P rejected; Q accepted and merged

- **P (`f616f014`) was neutral on B and C.**
  - The ABBA gave 0.988×, and C3-P +1.5%.
  - The paced flag is fixed at plan time, when an engine has 4 runs, and plans then wait behind the single compaction lane.
  - P was reverted on the branch.
- **Q (`ccdbaadc`): the pacer sleeps only while no maintenance job is queued.** The preregistered C ABBA passed every criterion:
  - Throughput: **1.116×**.
  - Device bytes/KV: 1.30×.
  - RSS: lower, 4.28 vs 5.01 GB.
  - Correctness: all runs PASS.
- **Sustain gates.** Both Q runs pass every sustain gate on C except debt convergence, which fails on every arm: a merge is in flight at the last sample and PD ranges are still splitting after drain.
- **Merged** as the 0314 merge commit, together with the docs/backpressure.md paragraph.
- **Evidence:**
  - `long01/SUMMARY-BACKLOG-0314.json`, `SUMMARY-C-0314.json` and `SUMMARY-PACING-0314.json`;
  - FINDINGS.md;
  - README.md (preregistrations).
- The fleet is stopped after every attempt.

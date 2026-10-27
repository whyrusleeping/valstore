---
id: "0177"
title: 'Investigate unshaped-overload compaction corruption: AlreadyFree aborts and snapshot CRC mismatches'
status: done
priority: 2
created: "2026-08-10"
updated: "2026-08-11"
depends_on:
    - "0170"
spec_refs:
    - docs/backpressure.md#Zone-region capacity and maintenance reserve
---

## Description
Task 0170 hardware validation exposed two correctness failures under legal `tier=nosync` unshaped overload on lab-1's 1024-segment SPDK sorted zone, using binary SHA `3dba11a…`: (1) preliminary fresh-format runs with the intended 100k preload plus valsmix's accidental 4-writer/1-second overwrite churn twice aborted at `engine.rs:3311`, `compact_commit: freeing a merged input run segment: AlreadyFree(...)` (segments 286 and 631); (2) the corrected load-only arm completed but logged repeated `corrupt snapshot: crc mismatch` compaction failures, 20 snapshot errors, and a large server-error delta during bursts 5–6. The shaped arm had only 65 server errors and no reported snapshot corruption.

## Acceptance criteria

- Reproduce each observed symptom with a minimal preserved harness/raw log, or bound the trigger and document why it is no longer reproducible.
- Determine whether the AlreadyFree abort and CRC mismatches share a pressure/compaction race or are independent defects; do not assume one cause.
- Fix every reproduced allocator/snapshot correctness defect so legal overload returns retryable pressure/errors rather than corrupting snapshots or aborting the node.
- Add targeted regression coverage for the identified interleaving/state transition and run relevant engine/store suites plus a hardware stress recheck.
- Record the outcome and provenance in bench notes; preserve enough raw evidence for future diagnosis.

## Plan

1. Inventory all task-0170 evidence (including any remote-only preliminary crash logs), reconstruct exact binary/config/workload provenance, and preserve a task-0177 minimal reproducer plus raw logs without altering existing t0170 evidence.
2. Trace compaction snapshot ownership end to end: compact selection/snapshot reads, staged output allocation/write/abort, `compact_commit`, run replacement, allocator free/reclaim paths, and maintenance scheduling. Analyze the AlreadyFree and CRC observations independently first, then determine from segment/run lifetimes and timing whether they share one race.
3. Build deterministic local regression tests that force the suspected interleaving/state transition (using existing test devices/fault hooks where possible). Reproduce or tightly bound each symptom; ensure any failed/stale compaction output is cleaned up exactly once and any stale/reused input cannot be read or freed as if still owned.
4. Implement the smallest correctness fix at the ownership/lifetime boundary. Legal pressure, stale work, and I/O failure must surface as retryable/non-fatal outcomes rather than panic, double-free, or accepting corrupt snapshot data; retain strict detection for true invariant violations.
5. Run focused new regressions and relevant `vals-store`/engine and cluster/store suites in debug. Inspect failures in isolation rather than masking them.
6. Rebuild and deploy the exact candidate to the lab t0170-style 1024-segment SPDK rig, run a fresh-format stress recheck that covers the accidental churn trigger and corrected load-only unshaped overload, preserve logs/metrics/config/build SHA, and verify no AlreadyFree abort or snapshot CRC corruption. Clean up the box afterward.
7. Record diagnosis, whether the defects were shared or independent, exact reproduction/bounds, fix semantics, test commands/results, and hardware evidence under `bench/t0177/notes.md`; inspect the final diff and avoid touching unrelated concurrent workspace changes.

### Starting points
- crates/vals-store/src/engine.rs::compact_commit and nearby compaction snapshot/write paths
- crates/vals-store/src/run.rs snapshot CRC decode paths; crates/vals-store/src/snapshot_io.rs
- bench/t0170/notes.md lines ~120-185 and bench/t0170/raw/{A,B} evidence
- Remote rig oldbox / lab-1 conventions from bench/t0170/box/run_arm.sh; rebuild before every run

## Work log
- 2026-08-11 plan: 1. Inventory all task-0170 evidence (including any remote-only preliminary crash logs), reconstruct exact binary/config/workload provenance, and preserve a task-0177 minimal reproducer plus raw logs w
…[truncated]
- 2026-08-11 context hints: 4 recorded with plan
- 2026-08-11 context hints: Do not modify, stage, or revert unrelated dirty backlog files from concurrent sessions.; crates/vals-store/src/engine.rs::compact_commit and nearby compaction snapshot/write paths; Investigate CRC and
…[truncated]
- 2026-08-11 preload: 5 file(s), ~56 KiB seeded into implementer context
- 2026-08-11 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-11 review (sol): accept — The change identifies a coherent shared lifetime/segment-role failure mechanism, pins cooperative-flush value segments across yields and off-core build, hardens compaction commit/cleanup against stale
…[truncated]
- 2026-08-11 review (sol#2/sol): accept — The change provides a coherent ownership/lifetime fix for both observed manifestations: cooperative flush snapshots now pin captured value segments across yields and off-core build, co-compaction pres
…[truncated]
- 2026-08-11 review (sol): accept — The revision addresses both prior findings: the hardware table now matches the candidate churn raw log, and the recheck harness emits mtime and size as separate metadata lines. The generalized Compact
…[truncated]
- 2026-08-11 review (sol#2/sol): accept — The revision resolves the prior cleanliness issue by generalizing `CompactionConflict` reporting to cover both stale source/input state and changed output reservations. It also corrects the churn prel
…[truncated]
- 2026-08-11 decision: accept — commit: store: pin flush snapshot segments across compaction
- 2026-08-11 usage: 18,098,043 tok (in 3,683,445, out 59,654, cache_r 14,354,944, cache_w 0) · cost n/a (unpriced)
  implementer: 7,480,853 tok (in 1,191,434, out 23,051, cache_r 6,266,368, cache_w 0) · cost n/a (unpriced)
  coordinator: 7,256,681 tok (in 1,222,255, out 18,682, cache_r 6,015,744, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 1,882,924 tok (in 706,102, out 8,694, cache_r 1,168,128, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 1,477,585 tok (in 563,654, out 9,227, cache_r 904,704, cache_w 0) · cost n/a (unpriced)

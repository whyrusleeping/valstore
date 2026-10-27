---
id: "0025"
title: Deferred micro follow-ons (grooming umbrella)
status: done
priority: 5
created: "2026-07-02"
updated: "2026-07-05"
depends_on: []
spec_refs: []
---

## Description
Umbrella for the small scoped extensions in BACKLOG.md §Deferred follow-ons that don't warrant individual tasks yet (split out when scheduled):
- **Sorted zones**: token-based intra-node sharding; auto/size-based range splitting; wire range *merge* to a PD op.
- **Multi-drive**: live drive hot-swap (rebalance is offline today); zone × partition engine multiplier. (Relocate crash-safety is task 0013.)
- **Batch API**: fire-now / `await_tier`-later handle (unblocked by CoordinatorService); general streaming barrier only if a checkpointing workload appears.
- **Credit pool**: per-node sub-limit; stronger `W=0` "released once sent to live replicas" semantics.
- **Over-replication cleanup**: `min_confirms` quorum-threshold tunable; quiesce reads around the release `forget` via the ReadGate.
- **Runtime**: executor teardown with parked tasks leaks Rc cycles (benign today — process lifetime).
- **Compaction**: garbage-ratio trigger instead of fixed cadence (partially superseded by LSM run thresholds — re-check).

## Acceptance criteria
- Grooming task: items here get split into real tasks when picked up; this task closes when the list is empty or re-triaged.

**RESOLUTION (2026-07-05): fully re-triaged; umbrella closed.**
- Sorted zones — sharding DONE (0029); auto split + PD range merge → **task 0058**.
- Multi-drive — live hot-swap → **task 0059**; zone × partition engine multiplier re-triaged out (landed with 0039, no observed problem; RAM/engine-count pressure would surface in box task 0047).
- Batch API — fire-now/await-later handle → **task 0060**; streaming barrier stays workload-gated (no task).
- Credit pool — per-node sub-limit + stronger W=0 semantics → **task 0061**.
- Over-replication cleanup — min_confirms tunable + ReadGate quiesce → **task 0062**.
- Runtime executor-teardown Rc-cycle leak — re-triaged out (benign at process lifetime; revisit if executors gain shorter lifetimes).
- Compaction — garbage-ratio trigger largely superseded (`COMPACT_MIN_RECLAIM_FRACTION` + `RUN_COMPACT_THRESHOLD`); remaining incremental/bounded pass + GC-only lone-run trigger → **task 0063**.
BACKLOG.md §Deferred follow-ons annotated with the same dispositions.

## Acceptance criteria

## Plan

Grooming task — re-triage each umbrella item against the current codebase/backlog, split still-live items into real tasks, record dispositions, close the umbrella. Verified state first-hand before triage:

Dispositions (verified against code + done tasks):
1. Sorted zones — token-based intra-node sharding: DONE (task 0029). Auto/size-based range splitting + PD-wired range merge: still open → NEW TASK (p4).
2. Multi-drive — live drive hot-swap: still open (rebalance is offline; relocate crash-safety done in 0013) → NEW TASK (p5). "Zone × partition engine multiplier": landed with 0039 (RegionZoneStore per partition); no observed problem → re-triage out, note that any RAM/engine-count pressure will surface in box task 0047.
3. Batch API — fire-now / await_tier-later handle: still open (unblocked by CoordinatorService) → NEW TASK (p5). General streaming barrier: workload-dependent → re-triage out (revisit if a checkpointing workload appears).
4. Credit pool — per-node sub-limit + stronger W=0 "released once sent to live replicas" semantics: still open → NEW TASK (p5).
5. Over-replication cleanup — min_confirms quorum-threshold tunable + ReadGate quiesce around the release forget: still open (antientropy.rs uses min_confirms=owners-capped constant; forget defers under sustained reads) → NEW TASK (p5).
6. Runtime — executor teardown Rc-cycle leak with parked tasks: benign (process lifetime; leaks only at teardown) → re-triage out with note; revisit only if executors gain non-process lifetimes.
7. Compaction — garbage-ratio trigger: largely SUPERSEDED — COMPACT_MIN_RECLAIM_FRACTION (0.20 dead-fraction gate) + RUN_COMPACT_THRESHOLD already gate the passes. Remaining real gap per code comment ("Until incremental compaction lands…") and BACKLOG: incremental/bounded compaction (today a stop-the-world full rewrite per zone) + a GC-only trigger for a lone aged run → NEW TASK (p4).

Steps:
1. create_task × 6 for the live items above, each with clear scope + acceptance criteria.
2. Update BACKLOG.md §Deferred follow-ons annotating each bullet with its disposition/new task id so the doc stays accurate.
3. Record the triage in task 0025's description/work log; mark done.
4. Session hygiene: mark hardware-only todo tasks 0047/0050/0056 blocked with the same "requires the SPDK bench box" note used by 0041 et al., so future autonomous sessions don't pick them up.
5. Docs/backlog-only change → self-review (simple tier), then commit.

### Starting points
- BACKLOG.md §Deferred follow-ons (~line 921)
- crates/vals-cluster/src/node.rs: COMPACT_MIN_RECLAIM_FRACTION, RUN_COMPACT_THRESHOLD, COMPACT_EVERY_SWEEPS
- crates/vals-cluster/src/antientropy.rs: min_confirms
- done tasks 0013, 0029, 0038, 0039, 0040 cover several umbrella items

## Work log
- 2026-07-05 plan: Grooming task — re-triage each umbrella item against the current codebase/backlog, split still-live items into real tasks, record dispositions, close the umbrella. Verified state first-hand before t
…[truncated]
- 2026-07-05 context hints: 4 recorded with plan
- 2026-07-05 review tier: simple (coordinator self-review)
- 2026-07-05 decision: accept — commit: backlog: re-triage deferred-follow-ons umbrella into tasks 0058-0063; close 0025; block hardware-only box tasks 0047/0050/0056
- 2026-07-05 usage: 12,955 tok (in 44, out 12,911, cache_r 964,690, cache_w 25,991) · cost n/a (unpriced)

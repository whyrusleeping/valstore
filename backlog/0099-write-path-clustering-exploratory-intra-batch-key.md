---
id: "0099"
title: 'Write-path clustering (exploratory): intra-batch key sort, bucketed vlog append streams, trivial install for non-overlapping runs'
status: done
priority: 5
created: "2026-07-09"
updated: "2026-08-11"
depends_on:
    - "0132"
spec_refs: []
---

## Description
Split from task 0069 (leg F, user suggestion 2026-07-09). Make arrival order approximate key order so clustering is (partially) free instead of a rewrite:
- Intra-batch sort: sort each BatchApply's records by key before the vlog append (near-free; wins when a batch spans a narrow key range — dump replay, time-ordered keys).
- Bucketed vlog append streams: N open segments, one per key-range bucket → values land roughly clustered by construction. Costs to design through: multi-tail group-commit/sync, recovery/epoch bookkeeping, partial-segment waste.
- Trivial install for non-overlapping runs: bulk ingest of sorted, non-overlapping input (dump replay/backfill) installs runs without merging (RocksDB "trivial move" analogue) — makes restore-from-dump nearly rewrite-free.

Gate: design-sketch first; implement only if the 0097 replay measurements show A+C left a scan/ingest gap that placement would close.

Acceptance criteria:
- Design note in docs/ weighing the three mechanisms against 0097 measurements; implement the subset that pays (or record why none does).

## Acceptance criteria

## Plan

Produce a measurement-gated design note rather than changing the write path. 1. Extract the applicable post-0069 evidence from task 0132's completed tier-one/nosync replay and the existing co-compaction scan benchmark, explicitly separating ingest bottlenecks from scan/value-placement effects and noting the replay's lack of scan measurements. 2. Evaluate intra-batch stable key sorting, bucketed value-log append streams, and trivial install for sorted non-overlapping input against current BatchApply/value-log/run/recovery semantics, including expected benefit, correctness/operational costs, and the workload classes where each could become worthwhile. 3. Record the gate decision: implement none now because 0132 shows bounded runs/merges and no placement-driven pressure while the observed decline is cold LWW run resolution plus a client file-size tail; retain concrete re-open criteria and the measurements an A/B would need. 4. Add the note under docs/ and link it from the most relevant existing performance/design documentation if needed. Verify cited values and repository links, inspect the docs-only diff, and run lightweight formatting/link checks rather than code tests.

### Starting points
- bench/replay-lab/run-20260810T063959Z-tier-one/FINDINGS.md and run-20260810T083025Z-tier-nosync/FINDINGS.md contain the completed task-0132 evidence
- backlog/0069-incremental-cooperative-run-merge-bound-merge-ram.md lines 38-45 and 83-97 contain prior clustering measurements and original mechanism sketches
- docs/scan-performance.md likely provides the natural cross-link location

## Work log
- 2026-08-11 plan: Produce a measurement-gated design note rather than changing the write path. 1. Extract the applicable post-0069 evidence from task 0132's completed tier-one/nosync replay and the existing co-compacti
…[truncated]
- 2026-08-11 context hints: 3 recorded with plan
- 2026-08-11 context hints: Use task 0132 as the successor evidence because task 0097's original corpus/hardware was lost and task 0097 was closed as superseded.; Check BatchApply and value-log/run/recovery code only enough to a
…[truncated]
- 2026-08-11 preload: 4 file(s), ~32 KiB seeded into implementer context
- 2026-08-11 implementer report: Implemented task 0099 as a measurement-gated design decision with no write-path code changes.  - Added `docs/write-path-clustering.md`, grounding the gate in the completed 0132 tier-one/nosync replay 
…[truncated]
- 2026-08-11 review tier: single-opus — reviewers: sol
- 2026-08-11 review (sol): accept — Task 0099 is satisfied as a measurement-gated design decision. The new note accurately replaces the unavailable 0097 evidence with its documented 0132 successor, clearly distinguishes ingest findings 
…[truncated]
- 2026-08-11 decision: accept — commit: docs: close write-path clustering design gate
- 2026-08-11 usage: 2,866,720 tok (in 1,447,563, out 15,765, cache_r 1,403,392, cache_w 0) · cost n/a (unpriced)
  coordinator: 1,239,463 tok (in 601,408, out 5,223, cache_r 632,832, cache_w 0) · cost n/a (unpriced)
  implementer: 1,095,515 tok (in 485,484, out 8,175, cache_r 601,856, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 531,742 tok (in 360,671, out 2,367, cache_r 168,704, cache_w 0) · cost n/a (unpriced)

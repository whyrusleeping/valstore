---
id: "0094"
title: 'Bound the on-core flush monoliths: flush_commit (68–171 ms) and flush_plan''s snapshot clone (58–75 ms) — the burst p99+ tail'
status: done
priority: 4
created: "2026-07-08"
updated: "2026-08-04"
depends_on:
    - "0091"
spec_refs: []
---

## Description
Task 0091's attribution (bench/t0091/notes.md §1) measured the two remaining on-core flush monoliths on the box (100k-key memtable, 1 KiB values): `flush_commit` = 68–171 ms (manifest write + O(n) version-recheck drop pass + value-liveness walk) and `flush_plan`'s memtable snapshot CLONE = 58–75 ms (the sort was negligible and is already off-core since 0091). At ~1 flush per 10–15 s reclaim cycle these are only ~1–2 % duty cycle, but each stalls the single serving core for 60–170 ms straight and shows up as the burst p99/p99.9 blowout (8–16 ms client latency). Bound them: e.g. incremental/chunked snapshot clone (cooperative slices with a seq-consistency recheck), sliced commit drop-pass (the manifest write must stay atomic; the memtable drop pass can be chunked after the manifest commits), or a zero-copy/shared-entry snapshot. Keep the flush_in_flight gating and version-recheck-drop semantics correct throughout.

## Acceptance criteria
- flush_plan and flush_commit single-slice on-core durations bounded (target: ≤ ~10 ms per slice) on a box-scale memtable, measured via the vals_flush_plan_us / vals_flush_commit_us histograms added in 0091.
- Burst p99/p99.9 during flush commits measurably improved on the box vs bench/t0091/notes.md §2.
- No durability/ordering regression (log-tail-durable-before-manifest preserved; version-recheck drop semantics intact); suites green.

## Plan

1. Replace the production flush-plan monolith with an Engine-owned cooperative snapshot state. Starting a plan arms `flush_in_flight`; bounded steps copy a deterministic maintenance page (target about 4K entries) and return Pending/Ready. Finalization, in the same borrow as the terminal page, drains the value-log tail, records the covered watermark, computes the reservation from accumulated aggregates, and produces the existing borrow-free `FlushPlan`. Preserve a synchronous compatibility wrapper for direct Engine/tests where useful. Document why writes interleaved between pages are safe: copied entries are durable before the manifest, version rechecks retain overwritten entries, and keys inserted behind the cursor remain in the memtable for the next flush.
2. Move all O(n) immutable commit preparation (`value_liveness` and tombstone metadata) onto the maintenance worker while it already owns the sorted snapshot. Refactor commit into an atomic manifest/install start followed by bounded version-recheck drop steps. Keep `flush_in_flight` armed until the final drop step, and refresh reserve state only then. Preserve manifest failure cleanup and log-tail-durable-before-manifest ordering.
3. Drive both state machines cooperatively from the serving-core async loops: yield between snapshot pages and between post-manifest drop pages, never hold a store borrow across an await, honor shutdown safely, and record each individual on-core slice (including the atomic manifest slice) in the existing `vals_flush_plan_us` / `vals_flush_commit_us` histograms. Update histogram help/comments to describe per-slice rather than whole-operation timing.
4. Add focused store/cluster tests for multi-step behavior, interleaved overwrites/new keys, flush gate lifetime, manifest failure, and equivalence of the resulting run/reopen behavior. Run targeted tests plus the relevant workspace suites without crate-wide rustfmt (the tree has known rustfmt skew).
5. Validate on box-scale hardware with the task-0091 workload/metrics where practical: confirm plan/commit slice maxima are around or below 10 ms and compare burst p99/p99.9 against the t0091 baseline; record commands, histogram observations, and latency results in `bench/t0094/notes.md`. If external benchmark conditions prevent a responsible tail comparison, record the verified code/suite result and leave the task in review rather than claiming the acceptance criterion.

### Starting points
- crates/vals-store/src/engine.rs: FlushPlan, WrittenFlush, flush_plan_with, flush_commit
- crates/vals-store/src/index.rs: Index::maintenance_page provides bounded deterministic pages for both ordering modes
- crates/vals-cluster/src/node.rs: snapshot_loop and maint_loop production orchestration/metrics
- bench/t0091/notes.md: prior box workload and baseline

## Work log
- 2026-08-04 plan: 1. Replace the production flush-plan monolith with an Engine-owned cooperative snapshot state. Starting a plan arms `flush_in_flight`; bounded steps copy a deterministic maintenance page (target about
…[truncated]
- 2026-08-04 context hints: 4 recorded with plan
- 2026-08-04 context hints: crates/vals-store/src/engine.rs: FlushPlan, WrittenFlush, flush_plan_with, flush_commit; crates/vals-store/src/index.rs: Index::maintenance_page provides bounded deterministic pages for both ordering
…[truncated]
- 2026-08-04 implementer report: Implemented task 0094 cooperative flush slicing.  Changes: - Added Engine-owned paged snapshot state and `flush_plan_step` (4K-entry default slices), with the flush gate armed from the first page thro
…[truncated]
- 2026-08-04 review tier: high-powered — reviewers: sol
- 2026-08-04 review (sol): revise — The cooperative snapshot and version-recheck state machines are directionally sound, preserve the manifest-before-drop ordering, and move immutable O(n) metadata work to the maintenance worker. Howeve
…[truncated]
- 2026-08-04 revision: Addressed all review findings and completed hardware validation.  Review fixes: - Changed `flush_commit_step` to pop at most `max_entries` entries per call. Seq recheck, index removal, and owned key d
…[truncated]
- 2026-08-04 review (sol): accept — The revision addresses the prior blockers. Commit now yields after the manifest/install slice, incrementally consumes and destroys snapshot entries inside measured drop slices, and reaches `Ready` wit
…[truncated]
- 2026-08-04 revision: Completed final hygiene: - Trimmed trailing spaces from all generated metric lines in `bench/t0094/box/results.txt` without changing captured values. - Updated `bench/t0094/notes.md` to report the ver
…[truncated]
- 2026-08-04 usage: 12,665,485 tok (in 1,164,257, out 66,220, cache_r 11,435,008, cache_w 0) · cost n/a (unpriced)
  implementer: 9,339,073 tok (in 587,015, out 35,258, cache_r 8,716,800, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 1,671,760 tok (in 301,608, out 22,824, cache_r 1,347,328, cache_w 0) · cost n/a (unpriced)
  coordinator: 1,654,652 tok (in 275,634, out 8,138, cache_r 1,370,880, cache_w 0) · cost n/a (unpriced)

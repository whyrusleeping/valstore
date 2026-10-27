---
id: "0191"
title: Remove legacy runtime merge monolith
status: done
priority: 2
created: "2026-08-12"
updated: "2026-08-12"
depends_on: []
spec_refs:
    - docs/optional-feature-survey.md#Legacy merge monolith — remove
    - docs/scan-performance.md
---

## Description
Delete the `VALS_MERGE_LEGACY=1` pre-0069 merge-all/co-compact-all runtime policy retained for historical A/B. Preserve backward format decoding and the current tiered index-only plus victim-reclaim policy.

Acceptance criteria:
- Remove the legacy runtime planner/execution branches, variable, A/B-only tests/bench defaults, and stale docs.
- Preserve v1-v3 manifest/run decoding and any conservative behavior required when old metadata lacks liveness fields.
- Current recurring, pressure, aged-vlog, orphan, shadow, tombstone-GC, and floor-liveness tests remain green.
- Run vals-store/vals-cluster debug suites and workspace checks; do not run cargo fmt.

## Acceptance criteria

## Plan

1. Remove the `VALS_MERGE_LEGACY` environment reader and all planner decisions that use it, leaving recurring compaction unconditionally on the current tiered selector and co-compaction governed only by the existing current-policy mechanisms.
2. Remove or rewrite legacy-only unit/benchmark setup and assertions while retaining tests that validate tiered recurring selection, capped pressure/lone-GC behavior, index-only recurring merges, liveness, and backward metadata decoding.
3. Update current documentation and benchmark defaults so the removed runtime gate is no longer advertised or injected; preserve historical benchmark result records where they are clearly immutable evidence, but remove stale operational instructions.
4. Run targeted vals-store tests, then vals-store/vals-cluster debug suites and workspace checks without cargo fmt; inspect the final diff and verify no active runtime references remain.

### Starting points
- crates/vals-store/src/engine.rs: merge_legacy, recurring_merge_selectable, compact_plan_inner, legacy-only tests around lines 12053 and 12586
- crates/vals-bench/src/main.rs around line 5043 injects the old variable
- docs/algorithms.md and docs/scan-performance.md advertise the old gate
- docs/optional-feature-survey.md records the cleanup decision; update its disposition rather than erasing decision history
- Do not touch the pre-existing unrelated modification in backlog/0188-remove-transparent-single-write-combining.md

## Work log
- 2026-08-12 plan: 1. Remove the `VALS_MERGE_LEGACY` environment reader and all planner decisions that use it, leaving recurring compaction unconditionally on the current tiered selector and co-compaction governed only
…[truncated]
- 2026-08-12 context hints: 5 recorded with plan
- 2026-08-12 context hints: crates/vals-store/src/engine.rs: merge_legacy, recurring_merge_selectable, compact_plan_inner, legacy-only tests around lines 12053 and 12586; crates/vals-bench/src/main.rs around line 5043 injects th
…[truncated]
- 2026-08-12 preload: 9 file(s), ~27 KiB seeded into implementer context
- 2026-08-12 implementer report: Implemented task 0191.  Changed: - Removed the `VALS_MERGE_LEGACY` reader and all runtime planner/execution branches. - Recurring planning now always uses the tiered selector; full value clustering no
…[truncated]
- 2026-08-12 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-12 review (sol): revise — The runtime implementation is correctly simplified to the tiered selector and current co-compaction opt-in, backward manifest/run decoding remains intact, and the touched code is internally consistent
…[truncated]
- 2026-08-12 review (sol#2/sol): accept — The legacy runtime policy is cleanly removed: recurring selection now always uses the tiered selector, co-compaction is controlled solely by the current bottommost/opt-in policy, and no active runtime
…[truncated]
- 2026-08-12 revision: Addressed all review findings:  - Updated `bench/t0069-notes.md` to explicitly identify `VALS_MERGE_LEGACY=1` and its same-binary A/B procedure as historical and no longer executable. Removed the form
…[truncated]
- 2026-08-12 review (sol): accept — The revision resolves the prior finding. `bench/t0069-notes.md` now clearly treats the removed gate and A/B procedure as historical, removes it from the current switch table, and no longer gives execu
…[truncated]
- 2026-08-12 review (sol#2/sol): accept — The revision resolves the prior cleanliness findings. The cocompact benchmark now accurately explains tiered fan-in, bottommost graduation, opt-in, and headroom requirements; historical benchmark docu
…[truncated]
- 2026-08-12 decision: accept — commit: store: remove legacy merge monolith
- 2026-08-12 usage: 6,569,424 tok (in 2,299,102, out 48,882, cache_r 4,221,440, cache_w 0) · cost n/a (unpriced)
  implementer: 2,620,689 tok (in 674,414, out 18,083, cache_r 1,928,192, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 1,550,497 tok (in 600,714, out 16,151, cache_r 933,632, cache_w 0) · cost n/a (unpriced)
  coordinator: 1,467,231 tok (in 616,419, out 5,500, cache_r 845,312, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 931,007 tok (in 407,555, out 9,148, cache_r 514,304, cache_w 0) · cost n/a (unpriced)

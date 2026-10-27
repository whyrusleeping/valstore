---
id: "0192"
title: Remove legacy on-core flush preparation path
status: done
priority: 2
created: "2026-08-12"
updated: "2026-08-12"
depends_on: []
spec_refs:
    - docs/optional-feature-survey.md#Legacy on-core flush preparation — removed (0192)
    - docs/backpressure.md
---

## Description
Delete `VALS_FLUSH_ONCORE_PREP=1` and the old serving-core flush sync/sort preparation path retained after task 0091's off-core migration.

Acceptance criteria:
- Make the current off-core flush preparation path unconditional.
- Remove the variable, mode plumbing, legacy-only tests/bench branches, and stale rollback docs.
- Preserve flush durability ordering, generation/interleave safety, error recovery, and useful flush sub-phase histograms.
- Run vals-store/vals-cluster debug suites and workspace checks; do not run cargo fmt.

## Acceptance criteria

## Plan

1. Remove the `VALS_FLUSH_ONCORE_PREP` environment lookup and all on-core mode state/API plumbing from `vals-store`: make `flush_plan` and bounded `flush_plan_step` always use the current drain-and-flush-tail durability step, leave snapshot entries unsorted for off-core build, and delete the obsolete monolithic/on-core preparation helper.
2. Update cluster wrappers and tests to use the unconditional API; retain focused coverage that unsorted insertion produces correct run metadata, committed reads, bounded snapshot/interleave safety, sync/error recovery, and flush sub-phase timing behavior.
3. Update Prometheus HELP strings and canonical optional-feature documentation to remove the knob/rollback language and record the path as removed while preserving the histogram meanings.
4. Search production/docs/tests for stale references, then run targeted tests plus the required debug vals-store and vals-cluster suites and workspace checks without cargo fmt.

### Starting points
- crates/vals-store/src/engine.rs: FlushSnapshotState and flush_plan/flush_plan_step
- crates/vals-cluster/src/replica.rs: flush_plan_step wrapper
- crates/vals-cluster/src/maint.rs: dual-mode legacy test
- crates/vals-client/src/observe.rs: flush phase HELP strings
- docs/optional-feature-survey.md#Legacy on-core flush preparation — removed (0192)

## Work log
- 2026-08-12 plan: 1. Remove the `VALS_FLUSH_ONCORE_PREP` environment lookup and all on-core mode state/API plumbing from `vals-store`: make `flush_plan` and bounded `flush_plan_step` always use the current drain-and-fl
…[truncated]
- 2026-08-12 context hints: 5 recorded with plan
- 2026-08-12 context hints: crates/vals-store/src/engine.rs: FlushSnapshotState and flush_plan/flush_plan_step; crates/vals-cluster/src/replica.rs: flush_plan_step wrapper; crates/vals-cluster/src/maint.rs: dual-mode legacy test
…[truncated]
- 2026-08-12 preload: 5 file(s), ~19 KiB seeded into implementer context
- 2026-08-12 implementer report: Implemented task 0192.  Changes: - Removed `VALS_FLUSH_ONCORE_PREP` lookup, `oncore_prep` snapshot/API plumbing, `flush_plan_with`, and the obsolete monolithic preparation helper. - Made `flush_plan`/
…[truncated]
- 2026-08-12 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-12 review (sol): accept — The change correctly makes the migrated flush-plan path unconditional: it removes the environment lookup and mode/API state, always performs drain-and-flush-tail before handing the unsorted snapshot t
…[truncated]
- 2026-08-12 review (sol#2/sol): revise — The runtime removal itself is clean and correct: the off-core plan path is now unconditional, sorting remains in `build_and_write_flush`, the bounded snapshot/interleave logic and cleanup paths are un
…[truncated]
- 2026-08-12 revision: Addressed both review findings comprehensively.  Changes: - Retired the executable t0091 A/B branch from `bench/t0091/box/t0091_burst.sh`: removed `MODE`, all legacy environment manipulation, mode-dep
…[truncated]
- 2026-08-12 review (sol): accept — The revision resolves the earlier documentation-link finding and also removes the remaining executable task-0091 A/B plumbing: the environment wrapper no longer forwards the retired variable, the burs
…[truncated]
- 2026-08-12 review (sol#2/sol): accept — The revision resolves the substantive benchmark/documentation gap. The t0091 burst harness now has only the unconditional off-core path, the launcher no longer forwards the removed variable, and the b
…[truncated]
- 2026-08-12 decision: accept — commit: store: remove legacy on-core flush preparation
- 2026-08-12 usage: 10,320,894 tok (in 2,822,785, out 30,845, cache_r 7,467,264, cache_w 0) · cost n/a (unpriced)
  implementer: 5,811,953 tok (in 921,951, out 14,226, cache_r 4,875,776, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 1,643,486 tok (in 642,126, out 5,776, cache_r 995,584, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 1,523,652 tok (in 756,566, out 7,022, cache_r 760,064, cache_w 0) · cost n/a (unpriced)
  coordinator: 1,341,803 tok (in 502,142, out 3,821, cache_r 835,840, cache_w 0) · cost n/a (unpriced)

---
id: "0193"
title: Hard-wire mature correctness and liveness fixes on
status: done
priority: 2
created: "2026-08-12"
updated: "2026-08-12"
depends_on: []
spec_refs:
    - docs/optional-feature-survey.md#Correctness kill switches — remove in a staged pass
    - docs/backpressure.md
    - docs/zones.md
---

## Description
Remove production environment kill switches that deliberately restore known bad behavior: `VALS_PRESSURE_NET_GAIN`, `VALS_ORPHAN_RECLAIM`, `VALS_STALL_TERMINAL`, `VALS_RANGE_FENCE`, and `VALS_ADMIT_SHAPE`. Keep current fixed behavior unconditional; retain narrow injectable/pure test seams where kill-the-fix coverage needs to exercise old behavior. This task intentionally does not yet remove the more intertwined `VALS_VLOG_GC`, `VALS_SHADOW_EST`, or `VALS_SHADOW_FOLD` controls.

Acceptance criteria:
- Remove the five variables and production conditional branches without changing current default behavior.
- Replace environment-mutating tests with narrow explicit seams where necessary.
- Update operator docs so no runbook suggests disabling a correctness/liveness invariant.
- Preserve admission band tuning (`VALS_ADMIT_BAND_PCT`) unless separately decided.
- Run relevant engine, cluster fencing/re-home, capacity-floor, and workspace debug suites; do not run cargo fmt.

## Acceptance criteria

## Plan

1. Remove the five production kill-switch readers and make their current default-on behavior unconditional: always use net-gain-aware pressure planning, orphan reclamation, terminal stall detection (still subject to the shadow-estimator prerequisite), reserve-floor admission shaping (while retaining VALS_ADMIT_BAND_PCT), and sorted-range ownership fencing.
2. Refactor tests that currently mutate those environment variables. Preserve kill-the-fix coverage only through narrow pure/helper parameters or test-only seams where it materially proves the invariant; update fullness tests so their fabricated allocation state remains stable without disabling production orphan reclamation (e.g. protect/pin fabricated segments or use an explicit test seam). Do not retain production paths that restore unsafe behavior.
3. Update current operator/design documentation and the soak runbook to describe the invariants as unconditional and remove advice to disable them. Update the optional-feature survey accordingly. Preserve historical benchmark artifacts and completed backlog records as historical evidence unless they are active operator instructions.
4. Run targeted vals-store engine tests (including pressure planning, orphan reclaim, terminal capacity, and admission shaping), vals-cluster fencing/re-home tests, then the relevant full debug suites/workspace checks as practical. Do not run cargo fmt; hand-format changed regions only.

### Starting points
- crates/vals-store/src/engine.rs:832-851 kill-switch readers; call sites around 3049, 3812, 3937, 3983, 4079, 4089; affected tests around 5949+ and 10410-14801
- crates/vals-cluster/src/net/server.rs:3061 range-fence switch and rehome_write_fence call sites/tests
- docs/backpressure.md, docs/zones.md, docs/optional-feature-survey.md, plans/bootes-soak-runbook.md
- Do not modify unrelated pre-existing backlog/0192 work; historical bench/raw and completed backlog text need not be rewritten

## Work log
- 2026-08-12 plan: 1. Remove the five production kill-switch readers and make their current default-on behavior unconditional: always use net-gain-aware pressure planning, orphan reclamation, terminal stall detection (s
…[truncated]
- 2026-08-12 context hints: 4 recorded with plan
- 2026-08-12 context hints: crates/vals-store/src/engine.rs:832-851 kill-switch readers; call sites around 3049, 3812, 3937, 3983, 4079, 4089; affected tests around 5949+ and 10410-14801; crates/vals-cluster/src/net/server.rs:30
…[truncated]
- 2026-08-12 preload: 10 file(s), ~36 KiB seeded into implementer context
- 2026-08-12 implementer report: Implemented Task 0193.  Changes: - Removed production readers/branches for `VALS_PRESSURE_NET_GAIN`, `VALS_ORPHAN_RECLAIM`, `VALS_STALL_TERMINAL`, `VALS_ADMIT_SHAPE`, and `VALS_RANGE_FENCE`. - Hard-wi
…[truncated]
- 2026-08-12 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-12 review (sol): accept — The change correctly hard-wires all five default-on correctness/liveness behaviors, removes their production environment readers and conditional bypasses, retains `VALS_ADMIT_BAND_PCT`, and replaces o
…[truncated]
- 2026-08-12 review (sol#2/sol): accept — The change cleanly removes all five production kill-switch readers and branches while preserving the former default-on behavior. The orphan-reclamation exception is confined to a narrow `cfg(test)` en
…[truncated]
- 2026-08-12 decision: accept — commit: store: hard-wire correctness and liveness fixes
- 2026-08-12 usage: 4,518,398 tok (in 1,081,720, out 30,598, cache_r 3,406,080, cache_w 0) · cost n/a (unpriced)
  implementer: 3,329,283 tok (in 551,426, out 14,337, cache_r 2,763,520, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 507,891 tok (in 205,928, out 7,819, cache_r 294,144, cache_w 0) · cost n/a (unpriced)
  coordinator: 346,132 tok (in 184,807, out 3,629, cache_r 157,696, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 335,092 tok (in 139,559, out 4,813, cache_r 190,720, cache_w 0) · cost n/a (unpriced)

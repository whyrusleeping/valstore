---
id: "0189"
title: Remove disabled merge-concurrency permit
status: done
priority: 3
created: "2026-08-12"
updated: "2026-08-12"
depends_on: []
spec_refs:
    - docs/optional-feature-survey.md#Per-node merge concurrency permit — remove, preserve maintenance pacing
    - docs/backpressure.md
---

## Description
Delete the default-off `VALS_MERGE_NODE_CONCURRENCY` permit machinery, which task 0166 measured as net-negative at limit=1. Preserve the useful default-on maintenance duty-cycle pacing.

Acceptance criteria:
- Remove merge permit mutex/condvar/types, acquisition branches, variable, docs, metrics/tests that exist only for the permit.
- Preserve liveness exemptions and behavior of maintenance pacing (`VALS_MAINT_DUTY_PCT`, `VALS_MAINT_FLUSH_BYTES`).
- Run vals-store/vals-cluster debug suites and workspace checks; do not run cargo fmt.

## Acceptance criteria

## Plan

1. Remove the node-wide merge-permit implementation from `vals-store::pace` while leaving maintenance duty-cycle pacing APIs and tests intact.
2. Remove permit-only policy state and accessors from compact plans, and remove permit acquisition from cluster maintenance workers; retain existing pressure/forced-backlog pacing exemptions and active-merge observability.
3. Remove the retired environment variable and permit behavior from maintained documentation, updating the optional-feature survey to describe the completed removal without disturbing unrelated feature guidance.
4. Search the tree for all retired symbols/config references, then run targeted checks plus the required debug `vals-store` and `vals-cluster` suites and workspace checks without `cargo fmt`.

### Starting points
- crates/vals-store/src/pace.rs: permit machinery begins at MERGE_CONCURRENCY_DEFAULT
- crates/vals-store/src/engine.rs: CompactPlan.permit_gated and constructors
- crates/vals-cluster/src/maint.rs: permit acquisition in compact jobs
- docs/backpressure.md and docs/optional-feature-survey.md: retired gate documentation

## Work log
- 2026-08-12 plan: 1. Remove the node-wide merge-permit implementation from `vals-store::pace` while leaving maintenance duty-cycle pacing APIs and tests intact. 2. Remove permit-only policy state and accessors from com
…[truncated]
- 2026-08-12 context hints: 4 recorded with plan
- 2026-08-12 context hints: Do not disturb unrelated task 0194 changes already present in this shared working tree; touch only task 0189 scope.; Do not run cargo fmt; tree is intentionally not rustfmt-clean.; Preserve MaintPacer
…[truncated]
- 2026-08-12 preload: 6 file(s), ~19 KiB seeded into implementer context
- 2026-08-12 implementer report: Implemented task 0189 without disturbing the concurrent task 0194 changes.  Changes: - Deleted the process-wide merge-concurrency mutex/condvar, permit type/acquisition function, timeout, and `VALS_ME
…[truncated]
- 2026-08-12 review tier: single-opus — reviewers: sol
- 2026-08-12 review (sol): accept — Task 0189 is correctly implemented. The node-wide permit mutex/condvar, permit type/acquisition function, plan policy field/accessor, and maintenance-worker acquisition branches are removed. Maintenan
…[truncated]
- 2026-08-12 decision: accept — commit: store: remove merge concurrency permit
- 2026-08-12 usage: 1,610,905 tok (in 654,572, out 17,581, cache_r 938,752, cache_w 0) · cost n/a (unpriced)
  coordinator: 674,966 tok (in 233,942, out 6,592, cache_r 434,432, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 476,618 tok (in 247,806, out 5,324, cache_r 223,488, cache_w 0) · cost n/a (unpriced)
  implementer: 459,321 tok (in 172,824, out 5,665, cache_r 280,832, cache_w 0) · cost n/a (unpriced)

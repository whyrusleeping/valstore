---
id: "0188"
title: Remove transparent single-write combining
status: done
priority: 2
created: "2026-08-12"
updated: "2026-08-12"
depends_on: []
spec_refs:
    - docs/optional-feature-survey.md#Transparent single-write combining — remove
    - docs/batch-writes.md
---

## Description
Delete the default-off `VALS_WRITE_COMBINE` transport combiner after task 0186's matched hardware sweep found no throughput improvement and materially worse p99.

Acceptance criteria:
- Remove combiner queues/workers/autonomous-task integration and all six `VALS_WRITE_COMBINE*` variables.
- Remove combiner-only metrics/tests/docs while preserving explicit `BatchApply` and its validated semantics.
- Confirm scalar Apply and explicit BatchApply behavior, cancellation, persistence, and stale/error handling remain covered.
- Run affected client/cluster debug suites and workspace checks; do not run cargo fmt.

## Acceptance criteria

## Plan

1. Trace the transparent single-write combiner through the client transport, autonomous task lifecycle, metrics, tests, and docs, clearly separating it from explicit BatchApply.
2. Remove the combiner configuration/state, bounded queues, workers, lifecycle integration, six environment variables, and combiner-only observability/tests while leaving scalar Apply and explicit BatchApply paths intact.
3. Update current product documentation to remove the unsupported combiner interface, retaining historical benchmark evidence as appropriate.
4. Add or adjust focused tests only where needed to ensure scalar Apply and explicit BatchApply still cover success/persistence, cancellation, stale/error behavior, and request semantics.
5. Run affected vals-client and cluster debug test suites plus workspace checks without cargo fmt; inspect the final diff for unrelated changes and acceptance-criteria coverage.

### Starting points
- crates/vals-client/src/net/client.rs contains the environment gate, combiner state, queue/worker logic, metrics, and tests.
- docs/batch-writes.md documents both explicit BatchApply and the removable transparent combiner.
- bench/task-0186-blind-batchapply is historical decision evidence; preserve results unless references must be clarified.

## Work log
- 2026-08-12 plan: 1. Trace the transparent single-write combiner through the client transport, autonomous task lifecycle, metrics, tests, and docs, clearly separating it from explicit BatchApply. 2. Remove the combiner
…[truncated]
- 2026-08-12 context hints: 3 recorded with plan
- 2026-08-12 context hints: Remove only transparent scalar-write combining; explicit batch_apply around client.rs:1193 and wire BatchApply must remain.; Current worktree contains unrelated user/session changes in backlog/spec/me
…[truncated]
- 2026-08-12 preload: 6 file(s), ~59 KiB seeded into implementer context
- 2026-08-12 implementer report: Removed transparent single-write combining while preserving explicit BatchApply: - Deleted all combiner configuration/state, bounded queues, workers, result fanout, cancellation cleanup, scalar-Apply
…[truncated]
- 2026-08-12 review tier: single-opus — reviewers: sol
- 2026-08-12 review (sol): revise — The client-side combiner itself is removed cleanly and the ordinary scalar Apply and explicit BatchApply paths remain intact, with existing cluster coverage for successful scalar writes, durable batch
…[truncated]
- 2026-08-12 revision: Addressed both review findings:  - Fully removed the combiner-only current-executor machinery from `vals-rt`: deleted the thread-local `CURRENT`, `CurrentGuard`, its drop/install logic, and installati
…[truncated]
- 2026-08-12 review (sol): accept — The revision resolves the prior substantive issue: all combiner-specific current-executor/autonomous-task machinery is now removed from `vals-rt`, along with the client queues, workers, configuration,
…[truncated]
- 2026-08-12 decision: accept — commit: client: remove transparent write combining
- 2026-08-12 usage: 6,663,455 tok (in 2,046,333, out 29,602, cache_r 4,587,520, cache_w 0) · cost n/a (unpriced)
  implementer: 4,986,021 tok (in 1,165,062, out 18,847, cache_r 3,802,112, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 1,144,296 tok (in 597,574, out 7,330, cache_r 539,392, cache_w 0) · cost n/a (unpriced)
  coordinator: 533,138 tok (in 283,697, out 3,425, cache_r 246,016, cache_w 0) · cost n/a (unpriced)

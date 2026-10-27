---
id: "0125"
title: Stop range-map churn from repeatedly restarting Merkle leaf reseeds
status: done
priority: 2
created: "2026-08-03"
updated: "2026-08-03"
depends_on:
    - "0124"
spec_refs:
    - bench/bootes-soak-lab/run-20260803T174813Z-post0074/FINDINGS.md
    - crates/vals-cluster/src/node.rs#sweep_loop
    - backlog/0045-budgeted-off-core-incremental-merkle-leaf-seed-avo.md
---

## Description
Task 0124's post-0074 box validation removed the monolithic cleanup functions from profiles, but still produced repeated 5-second shard timeouts under frequent auto-splits. Every range-map epoch invalidates the entire per-zone replica-set-grouped Merkle leaf accumulator. A new epoch often arrives before the cooperative full-dataset seed completes, so progress is discarded and restarted; the soak recorded 891/1,072/1,225 seed steps, 291/417/582 seconds of cumulative seed-generation wall time, and `SeedProgress::fold_key`/`VersionIndex::apply_leaf` as major profile symbols. Rework placement-change seeding to coalesce superseded epochs and preserve or incrementally correct reusable work where safe, without serving stale/incorrect scoped digests.

Acceptance criteria:
- Frequent sorted range-map epochs do not restart a whole-zone full-key seed from zero for every epoch; superseded work is coalesced or incrementally repaired with a documented exactness argument.
- Scoped Merkle leaves remain exact under concurrent writes, split movement, run merges/removals, and a new map arriving mid-seed; no stale placement groups are served.
- Serving-path work is bounded and regression tests exercise faster epoch churn than seed completion.
- A comparable bootes A/B materially reduces 5-second data timeouts and shard control-ack spreads; capture seed progress/restart/wall telemetry and profiles.

## Acceptance criteria

## Plan

Rework cooperative Merkle seeding so placement epochs retarget/coalesce the in-flight seed instead of discarding its whole-zone source scan. Decouple the expensive canonical key→winning-version fold from placement projection: retain the folded winners across placement changes, project them cooperatively into replica-set leaf groups under a versioned placement snapshot, and if placement changes mid-projection restart only that bounded in-memory projection under the latest map. Record mutations during both phases without embedding a stale placement group; replay them under the final current placement, and atomically publish only after confirming the placement generation is still current. Preserve the existing run-removal restart/fail-closed behavior, and make invalidation distinguish a seeded/live index (which needs a fresh source fold) from an already in-flight seed (which can be coalesced safely). Add explicit counters/logging for placement coalesces/projection retries alongside existing seed steps/restarts/wall time. Add regression tests with placement epochs arriving faster than source scan completion and during projection, including concurrent writes/deletes/forget plus run merge/removal, asserting exact scoped/unscoped leaves, no stale groups served, bounded per-step work, and no repeated run folds from placement-only churn. Run targeted vals-cluster/vals-store tests, workspace build/tests as practical, and clippy at the existing warning baseline. Then rebuild/deploy current source to the lab SPDK cluster and run a comparable bootes auto-split A/B following the saved soak runbook; preserve artifacts/findings with seed telemetry, profiles, five-second timeout counts, and shard control-ack spreads against task 0124's baseline. Restore safe teardown/config state after the run.

### Starting points
- crates/vals-cluster/src/replica.rs: LeafOp, SeedProgress, seed_step/finalize_seed, set_placement/invalidate_leaf_seed and seed churn tests around lines 5410–6205
- crates/vals-cluster/src/node.rs: sweep_loop placement wiring/invalidation around lines 2982–3050 and 3240–3310
- crates/vals-cluster/src/observe.rs: ae_seed_steps/restarts/sync_fallbacks/journal_ops/wall_us metrics
- bench/bootes-soak-lab/run-20260803T174813Z-post0074/FINDINGS.md and plans/bootes-soak-runbook.md for baseline and A/B procedure
- backlog/0045-budgeted-off-core-incremental-merkle-leaf-seed-avo.md documents current exactness model and cooperative seed design

## Work log
- 2026-08-03 plan: Rework cooperative Merkle seeding so placement epochs retarget/coalesce the in-flight seed instead of discarding its whole-zone source scan. Decouple the expensive canonical key→winning-version fold
…[truncated]
- 2026-08-03 context hints: 5 recorded with plan
- 2026-08-03 context hints: crates/vals-cluster/src/replica.rs: LeafOp, SeedProgress, seed_step/finalize_seed, set_placement/invalidate_leaf_seed and seed churn tests around lines 5410–6205; crates/vals-cluster/src/node.rs: sw
…[truncated]
- 2026-08-03 implementer report: Implemented task 0125’s cooperative Merkle seed retargeting in `replica.rs` and telemetry in `vals-client/src/observe.rs`.  Changes: - Decoupled the expensive run-sidecar winner fold from placement
…[truncated]
- 2026-08-03 review tier: high-powered — reviewers: sol
- 2026-08-03 review (sol): revise — The core design is directionally sound: it separates placement-free winner folding from placement projection, journals mutations without stale groups, generation-gates publication, and adds useful coa
…[truncated]
- 2026-08-03 revision: Addressed all three review findings.  Code/state-machine changes: - Replaced the projection key clone with an incrementally enumerable canonical winner `Vec`; exact dedup uses a hash→numeric-slot co
…[truncated]
- 2026-08-03 review (sol): revise — The revision adds the requested sorted-range-map test shape, bounded in-memory phases, and a well-documented A/B with strong availability improvements. However, the newly chunked journal state machine
…[truncated]
- 2026-08-03 revision: Fixed the multi-budget replay blocker and completed exact-final-source validation.  Code fixes: - `SeedProgress::fold_key` now reports whether it appended a new canonical winner. - During bounded jour
…[truncated]
- 2026-08-03 revision: Removed both superseded intermediate benchmark directories: - `bench/bootes-soak-lab/run-20260803T200232Z-task0125` - `bench/bootes-soak-lab/run-20260803T205220Z-task0125-final`  Confirmed there
…[truncated]
- 2026-08-03 review (sol): revise — The previous exactness blocker is fixed: newly journaled winners advance the projection frontier, the new multi-budget regression covers insert/retire/re-add behavior, the range-map test now forces a
…[truncated]
- 2026-08-03 revision: Fixed the remaining lifecycle-return defect.  Changes: - `seed_step` now returns `Ok(false)` immediately after atomic publication while bounded post-publication disposal remains. - It returns `Ok(true
…[truncated]
- 2026-08-03 review (sol): accept — The remaining lifecycle defect is fixed: publication now returns incomplete until bounded disposal finishes, and the new regression verifies publication precedes driver completion and that `seed_progr
…[truncated]
- 2026-08-03 revision: Trimmed trailing whitespace from: - Task 0125 backlog work-log lines. - Final2 `monitor.txt` telemetry. - Other captured final2 text artifacts flagged by the full diff check (notably perf reports), pr
…[truncated]
- 2026-08-03 usage: 28,825,306 tok (in 2,837,998, out 142,316, cache_r 25,844,992, cache_w 0) · cost n/a (unpriced)
  implementer: 20,334,916 tok (in 899,689, out 69,083, cache_r 19,366,144, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 7,175,018 tok (in 1,403,098, out 64,144, cache_r 5,707,776, cache_w 0) · cost n/a (unpriced)
  coordinator: 1,315,372 tok (in 535,211, out 9,089, cache_r 771,072, cache_w 0) · cost n/a (unpriced)

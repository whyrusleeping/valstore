---
id: "0090"
title: churn_reclaim_wedge kill-the-fix test fails on HEAD (run_merges=0 yet churn survives) — diagnose post-0089 dynamics
status: done
priority: 3
created: "2026-07-07"
updated: "2026-07-08"
depends_on: []
spec_refs: []
---

## Description
Found during task 0088 wrap-up: `cargo test -p vals-cluster --release --test churn_reclaim_wedge` FAILS on clean HEAD (648e5ee, task 0089): the recovery probe passes (zone not wedged) but the assertion `run_merges >= 1` trips — the unpaced 3×-region overwrite churn now survives **without any run-merge committing**, which the test's premise says should be impossible. Verified identical failure with and without the 0088 instrumentation, in an isolated worktree, so it is pre-existing on main.

Suspicion: task 0089's retryable Busy at the reserve floor (and/or 0085's reserve) changed the churn dynamics — e.g. the writer is now throttled/refused at the floor such that the test's fixed write budget completes without ever accumulating the runs a merge needs, or reclamation happens through another path. Also suspicious: the test binary reports finishing in ~0.07 s, far too fast for ~229k loopback writes — the churn loop may be erroring out near-instantly, meaning the test no longer exercises what it was written to exercise.

## Acceptance criteria
- Root-cause why run_merges stays 0 while the zone survives (instrument or log the maintenance decisions during the test).
- Decide: real product regression (reclamation not engaging when it must) → fix it; or test-premise now invalid post-0085/0089 → update the test so it again meaningfully kills the fix it guards (a reverted reserve/backpressure must make it fail).
- Test passes deterministically in CI conditions afterwards.

## Plan

Diagnosis (done, coordinator-verified by instrumenting the test locally):
- NOT a product regression. Post-0089, reserve-floor rejections come back as the retryable `Busy` (BatchResult outcomes all `Failed{error: Busy{..}}`). The churn loop counts failed batches toward `written`, so once the region fills to the reserve floor (~batch 636, ~0.05 s), the remaining ~1650 batches all Busy-fail instantly and the loop exhausts its "3× region" budget in ~0.1 s having appended only ~1× region.
- The recovery probe then succeeds on the FIRST attempt (t=21 µs, before any scheduler tick) — the zone was never wedged (the 0085 reserve did its job; a single small write still fits). The test then immediately asserts `run_merges >= 1`, racing the ~1 s maintenance tick → fails.
- Lingering after the probe shows reclamation engaging exactly as designed: at t≈1.1 s, snapshots=1, run_merges=1, segments_freed=1946/2048.

Fix: update the test so it again meaningfully kills the fix it guards.
1. In crates/vals-cluster/tests/churn_reclaim_wedge.rs, change the churn loop so only SUCCESSFUL batches advance `written` (and `round`). On a failed batch (Busy/backpressure), do not advance the budget; sleep briefly (~1–2 ms) before retrying — this is the product's documented client contract post-0089 (Busy = retryable-with-backoff), and the writer remains strictly faster than reclaim between merges.
2. Give the churn loop an overall deadline (generous for CI, e.g. 60 s; expected completion ~5–10 s: fill-to-floor is ~0.05 s, each ~1 s tick's pressure flush + lone-run merge frees ~95% of the region, so ~3–4 cycles cover the 3× budget). On deadline expiry, break out.
3. After the loop, assert `written >= total_writes` with a message explaining that a wedged zone (reverted reserve) Busy-fails forever and can never complete the append budget — this becomes the primary kill-the-fix assertion. Keep the existing recovery probe and the `run_merges >= 1` / `segments_freed > 0` assertions: now that 3× region of appends genuinely landed in an ~8 MiB region, they are structurally guaranteed at loop completion (loop completion REQUIRES merges to have freed space), no tick race.
4. Update the module doc comment: post-0089 the reserve floor surfaces as retryable Busy; the writer retries without advancing its budget; the wedge detector is budget-completion-within-deadline (plus the probe), and run_merges/segments_freed are asserted only after real 3× appends.
5. Verify:
   a. `cargo test -p vals-cluster --release --test churn_reclaim_wedge` passes; run it ~3× to check it's deterministic and note wall time (must be CI-reasonable, well under the deadline).
   b. Kill-the-fix check: temporarily revert the 0085 reserve (in vals-store, make the data-path append use the plain allocator — flip `allocate_data` to `allocate` semantics or make refresh_data_reserve force reserve 0), rebuild, confirm the test now FAILS (budget never completes / probe fails). Then restore. Report the exact revert used and both outcomes.
   c. `cargo test -p vals-cluster --release --test churn_reclaim` (the paced sibling) still passes.

Keep the change confined to the test file (no product code changes should be needed).

### Starting points
- crates/vals-cluster/tests/churn_reclaim_wedge.rs — the failing test (loop at ~line 100, assertions at ~140-160)
- crates/vals-cluster/tests/churn_reclaim.rs — paced sibling, style reference
- crates/vals-cluster/src/node.rs:1768 PRESSURE_FREE_FRACTION / zone_maint_due — pressure flush+merge trigger; tick interval clamps to [100ms, 1s] (SNAPSHOT_MIN_INTERVAL/SNAPSHOT_CHECK_MAX_INTERVAL ~line 1745)
- vals-store Engine::refresh_data_reserve / Allocator::allocate_data — the 0085 reserve to temporarily revert for the kill-the-fix check
- Observed: failed batches are BatchResult outcomes Failed{error: Busy{needed:1,got:0,busy:1}}; r.all_ok() already distinguishes — match on res Ok(r) if r.all_ok()
- Observed timing: fill-to-floor ~batch 636 (~0.05s); merge commits at t≈1.1s freeing 1946/2048 segments

## Work log
- 2026-07-08 plan: Diagnosis (done, coordinator-verified by instrumenting the test locally): - NOT a product regression. Post-0089, reserve-floor rejections come back as the retryable `Busy` (BatchResult outcomes all `F
…[truncated]
- 2026-07-08 context hints: 6 recorded with plan
- 2026-07-08 context hints: crates/vals-cluster/tests/churn_reclaim_wedge.rs — the failing test (loop at ~line 100, assertions at ~140-160); crates/vals-cluster/tests/churn_reclaim.rs — paced sibling, style reference; crates
…[truncated]
- 2026-07-08 implementer report: Fixed task 0090 by updating the test (no product code change — git diff shows only `crates/vals-cluster/tests/churn_reclaim_wedge.rs` modified).  ## Root cause (confirmed the coordinator's diagnosis
…[truncated]
- 2026-07-08 review tier: single-opus — reviewers: Claude
- 2026-07-08 review (Claude): accept — The change correctly resolves task 0090 by updating the churn_reclaim_wedge test to match post-0089 dynamics: only successful batches advance the append budget, Busy/backpressure batches retry after a
…[truncated]
- 2026-07-08 decision: accept — commit: test: fix churn_reclaim_wedge post-0089 dynamics (task 0090) — not a product regression: retryable Busy at the reserve floor let the churn loop burn its 3×-region budget in ~0.1s without appending,
…[truncated]
- 2026-07-08 usage: 26,169 tok (in 92, out 26,077, cache_r 1,170,617, cache_w 97,781) · $3.0044
  coordinator: 15,649 tok (in 44, out 15,605, cache_r 808,555, cache_w 57,760) · $2.3112
  implementer: 7,016 tok (in 28, out 6,988, cache_r 264,879, cache_w 25,144) · $0.4644
  reviewer:Claude: 3,504 tok (in 20, out 3,484, cache_r 97,183, cache_w 14,877) · $0.2288

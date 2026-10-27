---
id: "0074"
title: Budget + resume-cursor for the over-replication cleanup pass (avoid monolithic O(keys) scans and from-scratch restarts)
status: done
priority: 4
created: "2026-07-06"
updated: "2026-08-03"
depends_on:
    - "0073"
spec_refs: []
---

## Description
Follow-on noticed while fixing task 0073 (graceful-shutdown wedge): the over-replication cleanup pass (`cleanup_with` in crates/vals-cluster/src/antientropy.rs) is still a **monolithic O(all keys) scan with O(unowned × owners) serial handoff RPCs per pass**, with no per-pass budget and no resume cursor — unlike the AE reconcile, which is budgeted (50k digests / 8192 value reads) and resumes forward across passes.

Consequences at large n (observed math from the distnode n=2M box run, task 0012):
- A single cleanup pass after a placement change can take tens of minutes (~2M serial RPCs on node 1 after the P4 join).
- Any RPC/engine error marks the pass unclean (`cleanup_ok = false`) and the next pass **restarts from scratch** — under churn or with a flaky/unreachable owner the cleanup may never complete a clean pass, so shed keys stay stranded and every sweep pass keeps re-scanning the whole keyspace.
- Task 0073's fix makes shutdown prompt, but an aborted/failed pass still restarts from key 0.

Proposed shape (design open):
- Budget the cleanup per pass (e.g. max keys scanned and/or max handoff RPCs), with a resume cursor threaded like the AE `ae_cursors` so successive passes cover the keyspace incrementally.
- Record per-epoch completion only when the cursor wraps with no deferrals/errors (preserving the existing `last_cleanup_epochs` semantics).
- Consider batching handoff applies per owner (the per-key-per-owner serial RPC is the dominant cost).

## Acceptance criteria
- Cleanup work per sweep pass is bounded (budget constants documented like the AE ones) and resumes across passes via a cursor instead of restarting from key 0.
- An error/deferral mid-pass resumes near where it stopped on the next pass (no full restart).
- Epoch-clean recording still only happens after a full covered wrap; existing cleanup safety tests pass (never drops the last copy, min_confirms semantics unchanged).
- Off-hardware test: a large unowned key set against a slow/flaky owner converges across multiple bounded passes and never monopolizes a pass.

## Plan

1. Introduce an explicit cleanup progress/result contract and documented per-pass budgets (keys scanned and handoff RPCs), with an exclusive resume cursor and a distinction between budget/deferral progress versus a fully covered wrap.
2. Replace cleanup’s eager `VersionedStore::keys() -> Engine::recovered_versions()` materialization with a bounded, cursor-driven key/version page sourced from the engine’s run sidecars and memtable without reading values; ensure the page path does not synchronously materialize the whole dataset under the zone borrow and can perform device I/O off-borrow where supported.
3. Update `cleanup_with` and its scoped wrappers to process only the budgeted page, preserve per-key handoff/min-confirm safety, advance near errors/deferrals, and only forget versions that still match. Bound/chunk release work so cleanup no longer invokes one all-runs rewrite over an unbounded release set on the serving path; use the existing off-core maintenance machinery where necessary rather than adding another synchronous full-run rewrite.
4. Thread per-zone cleanup cursors/completion state through the node sweep loop. Reset progress on placement fingerprint changes; retain it across ordinary failures/deferrals; record `last_cleanup_epochs` only after all relevant zones complete a full cursor wrap with no unresolved deferrals/errors.
5. Adapt existing cleanup tests to the progress API and add an off-hardware regression with a large unowned set and slow/flaky handoff transport, asserting each pass stays within its scan/RPC budget, resumes forward, preserves copies/min-confirms, and converges over multiple passes. Run focused vals-cluster/vals-store tests plus the relevant crate test suites/checks.

### Starting points
- crates/vals-cluster/src/antientropy.rs: cleanup_with/cleanup_scoped; existing AE budget constants and cursor contract
- crates/vals-cluster/src/node.rs: sweep_loop last_cleanup_epochs and ae_cursors
- crates/vals-cluster/src/replica.rs: VersionedStore::keys/forget and staged digest-page methods
- crates/vals-store/src/engine.rs: recovered_versions, DigestPagePlan, version_digest_page_from_plan, compact_plan/commit
- backlog/0074-budget-resume-cursor-for-the-over-replication-clea.md work-log hardware finding

## Work log

- 2026-08-03 hardware confirmation (task 0122): the fresh auto-splitting bootes soak promoted this from a theoretical long-pass issue to the likely root cause of 5-second serving timeouts. Every range-map epoch triggers cleanup; before `CLEANUP_YIELD_EVERY` can help, `cleanup_with` synchronously calls `VersionedStore::keys() -> Engine::recovered_versions()` under the zone borrow, materializing/deduplicating the entire 50–74M-key process dataset on serving shards. A cold persisted-dataset perf profile captured `recovered_versions` (2.88% of all process cycles over 150s), `HashMap::insert` (1.57%), `VersionedStore::keys`, and `sweep_loop`, alongside 16s spread in shard map acknowledgements. If releases are found, a second unbounded section runs: `forget -> forget_keys -> merge_all_runs` synchronously rewrites all runs under the serving borrow. The implementation must budget/page the **initial key enumeration itself**, not merely the later handoff loop, and move/chunk the run rewrite off-core. Evidence: [`bench/bootes-soak-lab/run-20260802T184023Z/ROOT-CAUSE-0122.md`](../bench/bootes-soak-lab/run-20260802T184023Z/ROOT-CAUSE-0122.md).
- 2026-08-03 plan: 1. Introduce an explicit cleanup progress/result contract and documented per-pass budgets (keys scanned and handoff RPCs), with an exclusive resume cursor and a distinction between budget/deferral pro
…[truncated]
- 2026-08-03 context hints: 5 recorded with plan
- 2026-08-03 context hints: crates/vals-cluster/src/antientropy.rs: cleanup_with/cleanup_scoped; existing AE budget constants and cursor contract; crates/vals-cluster/src/node.rs: sweep_loop last_cleanup_epochs and ae_cursors; c
…[truncated]
- 2026-08-03 implementer report: Implemented task 0074 across vals-store/vals-cluster.  Changes: - Added documented cleanup budgets (8,192 scanned keys / 256 handoff RPCs) and an explicit `CleanupProgress` contract with exclusive cur
…[truncated]
- 2026-08-03 review tier: high-powered — reviewers: sol
- 2026-08-03 review (sol): revise — The change introduces the intended cursor/progress plumbing and off-core release path, but it does not yet meet the bounded-work guarantee for persisted datasets and introduces a placement-race data-l
…[truncated]
- 2026-08-03 revision: Addressed all review findings.  Changes made: - Added placement-fenced cleanup maintenance jobs/results (`CleanupCompact` / `CleanupCompacted`) carrying the cleanup placement fingerprint through the w
…[truncated]
- 2026-08-03 review (sol): revise — The revision fixes the persisted-page scan by reusing the seekable/windowed staged scan path, adds a real slow-transport bound, and correctly fences off-core cleanup compaction results. Two important
…[truncated]
- 2026-08-03 revision: Addressed the remaining review findings.  Changes: - Placement-fenced immediate/memtable cleanup releases:   - Added a `placement_valid` probe to cleanup APIs.   - The node passes a probe comparing th
…[truncated]
- 2026-08-03 review (sol): revise — The latest revision addresses the previously reported placement fence, memtable full-log rewrite, and multi-deadline RPC issues. The main shape is now sound, but cleanup can still perform unbounded wo
…[truncated]
- 2026-08-03 revision: Addressed the final review findings comprehensively.  Changes: - Bounded pending forget state and rewrite plans:   - Added `Engine::can_plan_forget()` to cheaply reject planning while poisoned, runles
…[truncated]
- 2026-08-03 review (sol): revise — The revision now bounds staged-page restarts, prevents pending sets from accumulating behind other maintenance, and propagates successful prefixes from partial memtable forgets. One convergence bug re
…[truncated]
- 2026-08-03 revision: Fixed the pending cleanup chunk lifecycle/convergence bug.  Changes: - Successful cleanup compact commits now consume the entire submitted `pending_forget` chunk after commit reconciliation and leaf r
…[truncated]
- 2026-08-03 review (sol): revise — The terminal cleanup-job lifecycle is now handled correctly, including changed-version commits and worker/submit aborts. One closely related planning-error path still leaves the bounded pending chunk
…[truncated]
- 2026-08-03 revision: Fixed the final planning-error latch.  Changes: - `VersionedStore::forget_plan` now explicitly handles all engine planning outcomes:   - `Ok(Some(plan))`: retains the pending chunk for the submitted j
…[truncated]
- 2026-08-03 review (sol): accept — The revision now closes the remaining planning-error latch and, overall, satisfies the task: cleanup enumeration and handoffs are budgeted, cursors survive deferrals/errors, staged scans are seekable
…[truncated]
- 2026-08-03 usage: 124,868,281 tok (in 3,488,269, out 188,844, cache_r 121,191,168, cache_w 0) · cost n/a (unpriced)
  implementer: 101,150,802 tok (in 1,646,576, out 99,426, cache_r 99,404,800, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 18,193,346 tok (in 1,204,202, out 77,272, cache_r 16,911,872, cache_w 0) · cost n/a (unpriced)
  coordinator: 5,524,133 tok (in 637,491, out 12,146, cache_r 4,874,496, cache_w 0) · cost n/a (unpriced)

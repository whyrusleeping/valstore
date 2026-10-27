---
id: "0073"
title: Graceful node shutdown wedges behind continuous AE sweep churn at scale (>1h drain in distnode n=2M box run)
status: done
priority: 3
created: "2026-07-06"
updated: "2026-07-06"
depends_on: []
spec_refs: []
---

## Description
**Observed** during the task-0012 `distnode` box run (oldbox, 2026-07-06, n=2,000,000 vsize=128, 4×NVMe SPDK, sweep=2s): after all bench phases completed, teardown called `NodeHandle::shutdown()` on node 1 and the node logged `graceful shutdown requested; draining` (13:35:59Z) — and then **never finished draining for over an hour** (still 4 live `vals-shard` threads, process at 45–65% CPU, sweep passes still cycling). The run had ~2.75M distinct keys × 2 zones per node and a 500k-key un-healed AE backlog on node 1; sweeps were running back-to-back (per-pass AE budgets: 50k digests / 8,192 value reads, resumed forward each pass). Process had to be SIGKILLed and SPDK residue removed by hand (`/var/run/dpdk/spdk*`, `/dev/hugepages/spdk*`).

Off-hardware/small-scale graceful shutdown is fine (valsd A.3 in bench/spdk-liveness-box-notes.md; the mem `distnode` smoke tears down instantly), so this looks scale/backlog-dependent: the drain path appears to wait for a quiescence that continuous sweep churn never provides (or the sweep loop doesn't check the shutdown flag on its budget-pass boundaries).

**Where to look**: crates/vals-cluster/src/node.rs — the shard shutdown/drain sequence and the periodic/sweep loop's shutdown checks; `graceful_shutdowns` counter path. Reproduce off-hardware: a node with a large keyspace + a permanently-behind peer (so every sweep pass has work), request shutdown mid-sweep, assert the shard exits within a small bound.

## Acceptance criteria
- Root cause identified and fixed: a node with a continuously-busy AE sweep (budget-bounded passes always resuming) completes `NodeHandle::shutdown()` promptly (bounded by at most ~one budget pass, not by backlog convergence).
- Off-hardware regression test simulating the wedge (shutdown requested while sweep passes continuously resume) that fails before the fix.
- `graceful shutdown complete` is logged and `graceful_shutdowns` increments in that test.
- (Optional, box) re-run `distnode` at large n and confirm teardown completes in seconds — can fold into any later SPDK session.

## Plan

## Root cause (diagnosed & reproduced off-hardware)

The graceful-shutdown wedge is NOT the budget-bounded AE reconcile (a pure 40k-key
backlog shuts down in ~100ms — verified). It is the sweep's **over-replication
cleanup** stage:

- `cleanup_with` (crates/vals-cluster/src/antientropy.rs, ~line 666) walks the
  zone's ENTIRE key set and, for every key this node no longer owns, serially
  awaits an `apply` handoff RPC **per owner** (up to rf × `NODE_PEER_RPC_DEADLINE`
  = 3 × 5s per key). The loop has cooperative `yield_now` every 4096 keys but
  **no shutdown checkpoint anywhere** and no per-pass budget.
- `sweep_loop` (node.rs) checks `shared.is_shutdown()` only at pass start and at
  the top of each zone iteration; within a zone, cleanup runs after the reconcile
  with no check in between. So a shutdown requested mid-cleanup waits out
  O(unowned_keys × owners) serial RPCs.
- On the box run (task 0012, n=2M): P4's joiner join changed placement →
  `run_cleanup` true on every subsequent pass until one completes clean; node 1
  had ~690k newly-unowned keys × 3 owners ≈ 2M serial handoff RPCs per cleanup
  pass (restarted from scratch on any error — no resume cursor). Teardown arrived
  mid-cleanup → the >1h wedge. At small n (runs 2/3) cleanup finishes during the
  run, so teardown was instant. All other background loops (periodic, compact,
  snapshot, maint, ctl, accept, conn drivers, shutdown watcher) were audited and
  exit promptly.

**Reproduction (fails on current HEAD, verified):** node 1 preloaded with ~400
keys on a ring `Ring::from_members(1, 64, [1, 2])` (rf=1 → ~half the keys owned
by node 2), peer 2 is a black-holing listener (accepts, never replies),
sweep_every = 25ms. The sweep's first pass enters the cleanup handoff loop
(each key burns the 5s RPC deadline); `handle.shutdown()` then hangs past a 30s
watchdog.

## Fix

1. `antientropy.rs`: give `cleanup_with` a stop-probe parameter,
   `stop: impl Fn() -> bool` (mirroring how `gate` is threaded). Check it:
   - at the top of each key iteration of the handoff loop (cheap `Cell` read),
   - inside the per-owner apply loop (so an in-flight 5s deadline is the worst
     residual wait, not rf × 5s),
   - before the `gate.acquire()` / forget step.
   On `stop()` → return early with `Ok(released_so_far)` or `Ok(0)` (handoffs
   already made stand; nothing is forgotten without the gate; the pass is simply
   abandoned). Update the doc comment to document the shutdown checkpoint.
2. Thread the new parameter through `cleanup_scoped` and `cleanup_scoped_zone`
   (same file). Existing antientropy tests pass `|| false`.
3. `node.rs` `sweep_loop`: pass `|| shared.is_shutdown()` as the stop probe at
   both cleanup call sites (~lines 3013/3016). Also add an
   `if shared.is_shutdown() { return; }` between the per-peer reconcile loop and
   the cleanup stage, and one inside the `for (peer_node, peer) in &netpeers`
   loop, so a multi-peer pass exits at the next peer boundary too.
4. There is one more `cleanup_scoped` caller around node.rs:4201-4220 (a cleanup
   driver loop that already checks `is_shutdown()` around its ticks) — pass the
   matching `is_shutdown` probe there (or `|| false` if it's a test harness whose
   semantics shouldn't change; use judgement, keep behaviour identical
   otherwise).
5. Do NOT record the epoch clean on an aborted pass — already guaranteed by the
   existing `if run_cleanup && cleanup_ok && !shared.is_shutdown()` guard; leave
   as is.

## Regression test (kill-the-fix verified)

Add to the node.rs tests mod, next to
`shutdown_is_bounded_against_a_black_holing_peer` (~line 6410), e.g.
`shutdown_is_bounded_mid_cleanup_handoff_backlog`:

- Black-holing TCP listener as node 2 (copy the existing test's pattern).
- Build node 1's store on `MemBlockDevice::new(4096, 16384)`, seg_blocks 64,
  and `put` ~400 keys (`VersionedStore::put`).
- `Ring::from_members(1, 64, [1, 2])` (rf=1), `Node::serve(1, ...)` with peers
  `[(1, a1), (2, hole)]`, sweep 25ms.
- Sleep ~500ms so the sweep is inside the cleanup handoff loop, then
  `within_secs(20, move || handle.shutdown())` — pre-fix this hangs into the
  watchdog (~half of 400 keys × 5s ≈ 1000s); post-fix it completes in ≤ ~5s+slack
  (one in-flight RPC deadline).
- Assert `crate::observe::snapshot().graceful_shutdowns` incremented across the
  shutdown (before/after delta, like `graceful_shutdown_drains_persists_and_joins`)
  — this is the `finalize_shutdown` path that also logs
  "graceful shutdown complete", satisfying that acceptance criterion.
- Comment the test with the box-run provenance (task 0073, distnode run 1).

## Verification

- `cargo test -p vals-cluster --lib` (all tests, including the new one).
- Kill-the-fix: revert the stop-probe wiring locally (or run the new test on
  stash) and confirm the new test hangs into its watchdog — I already confirmed
  the repro fails on HEAD, so it is sufficient to confirm it passes post-fix and
  that the only behavioural delta is the stop probe.
- `cargo build -p vals-bench` still compiles (no API change leaks there).
- Note in the task work log that the optional box re-run (distnode large-n
  teardown in seconds) is deferred to the next SPDK session.

### Starting points
- crates/vals-cluster/src/antientropy.rs: cleanup_with (~666), cleanup_scoped (~788), cleanup_scoped_zone (~821), CLEANUP_YIELD_EVERY (~105)
- crates/vals-cluster/src/node.rs: sweep_loop cleanup call sites (~3006-3031), netpeers reconcile loop (~2900-2968), NODE_PEER_RPC_DEADLINE (~2573)
- crates/vals-cluster/src/node.rs tests: shutdown_is_bounded_against_a_black_holing_peer (~6410), within_secs (~6383), store() helper (~3088)
- repro recipe (verified failing on HEAD): rf=1 ring [1,2], node1 preloaded 400 keys on MemBlockDevice::new(4096,16384) seg_blocks=64, peer 2 = accept-and-never-reply listener, sweep 25ms, sleep 500ms, shutdown under within_secs watchdog
- node.rs ~4201-4220: additional cleanup_scoped caller (cleanup driver) — thread the stop probe there too

## Work log
- 2026-07-06 plan: ## Root cause (diagnosed & reproduced off-hardware)  The graceful-shutdown wedge is NOT the budget-bounded AE reconcile (a pure 40k-key backlog shuts down in ~100ms — verified). It is the sweep's **
…[truncated]
- 2026-07-06 context hints: 5 recorded with plan
- 2026-07-06 context hints: crates/vals-cluster/src/antientropy.rs: cleanup_with (~666), cleanup_scoped (~788), cleanup_scoped_zone (~821); crates/vals-cluster/src/node.rs: sweep_loop cleanup call sites (~3006-3031), netpeers re
…[truncated]
- 2026-07-06 implemented: threaded a `stop: impl Fn() -> bool` shutdown probe
  through `cleanup_with` / `cleanup_scoped` / `cleanup_scoped_zone`
  (crates/vals-cluster/src/antientropy.rs), checked at the top of each key
  iteration, between owners in the per-owner `apply` loop (worst residual wait =
  one in-flight RPC deadline), and before the gate acquire/forget — early-returns
  `Ok(0)` so handoffs already made stand and nothing is forgotten without the
  gate. `sweep_loop` (node.rs) passes `|| shared.is_shutdown()` at both cleanup
  call sites and gained `if shared.is_shutdown() { return; }` at the per-peer
  reconcile boundary and between the reconcile loop and the cleanup stage. The
  cleanup-driver test harness (node.rs ~4239) passes `|| false` to keep its
  semantics identical. Regression test
  `shutdown_is_bounded_mid_cleanup_handoff_backlog` added next to the existing
  black-holing-peer test: it waits past the first reconcile deadline so shutdown
  fires while the sweep is inside the cleanup handoff loop, then asserts shutdown
  completes within one RPC deadline + slack and that `graceful_shutdowns`
  increments (the `finalize_shutdown` path that logs "graceful shutdown
  complete"). Kill-the-fix verified: neutering only `cleanup_with`'s stop probe
  makes the new test hang into its watchdog (26.5s); restored, it passes (~10s).
  `cargo test -p vals-cluster --lib` all green (377 passed); `cargo build -p
  vals-bench` compiles. OPTIONAL box re-run (distnode large-n teardown in
  seconds) is DEFERRED to the next SPDK session.
- 2026-07-06 implementer report: Fixed the graceful-shutdown wedge (task 0073) caused by the sweep's over-replication cleanup handoff loop having no shutdown checkpoint.  ## Changes  **crates/vals-cluster/src/antientropy.rs** - `clea
…[truncated]
- 2026-07-06 review tier: single-opus — reviewers: Claude
- 2026-07-06 review (Claude): accept — The change correctly fixes the graceful-shutdown wedge by threading a shutdown stop-probe through the over-replication cleanup handoff loop (cleanup_with/cleanup_scoped/cleanup_scoped_zone) and adding
…[truncated]
- 2026-07-06 decision: accept — commit: cluster: shutdown checkpoint in the over-replication cleanup handoff loop — graceful drain no longer wedges behind O(unowned×owners) serial RPCs (task 0073)  Root cause of the >1h distnode n=2M tea
…[truncated]
- 2026-07-06 usage: 79,016 tok (in 284, out 78,732, cache_r 9,904,967, cache_w 413,380) · cost n/a (unpriced)
  coordinator: 52,863 tok (in 154, out 52,709, cache_r 7,934,708, cache_w 335,816) · cost n/a (unpriced)
  implementer: 21,581 tok (in 102, out 21,479, cache_r 1,776,369, cache_w 56,507) · cost n/a (unpriced)
  reviewer:Claude: 4,572 tok (in 28, out 4,544, cache_r 193,890, cache_w 21,057) · cost n/a (unpriced)

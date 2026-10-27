---
id: "0062"
title: 'Over-replication cleanup: min_confirms quorum tunable + ReadGate quiesce around release forget'
status: done
priority: 5
created: "2026-07-05"
updated: "2026-07-05"
depends_on: []
spec_refs: []
---

## Description
Split from grooming umbrella 0025 (BACKLOG.md §Deferred follow-ons, "Over-replication cleanup" bullet).

Two scoped extensions to the over-replication cleanup (antientropy.rs):
1. **`min_confirms` as a quorum-threshold tunable**: today the release requires confirmation from `min_confirms` owners capped at all owners; expose a quorum-style threshold (e.g. majority) as a tunable instead of the hardcoded value, with the safety reasoning documented (when is releasing on a quorum of owner confirmations safe vs all).
2. **Quiesce reads around the release `forget` via the ReadGate**: today the cleanup samples `inflight == 0`, so sustained reads can defer the forget indefinitely (bounded garbage: it doesn't grow with write traffic, unlike compaction — but it can persist). Use the same ReadGate pause-admission + bounded drain the compactor uses (see node.rs quiesce_wait) so the forget runs promptly even on read-busy nodes.

## Acceptance criteria
- Tunable threshold honored and defaulted to current behaviour; test that a release proceeds at the configured threshold.
- Under a sustained read workload, the release forget executes within a bounded number of ticks (test mirrors the compactor-quiesce test approach); no read-after-forget unsafety (reads parked, not racing the forget).
- BACKLOG.md bullet updated.

## Acceptance criteria

## Plan

Two scoped extensions to over-replication cleanup (crates/vals-cluster/src/antientropy.rs `cleanup_with` + crates/vals-cluster/src/node.rs `sweep_loop`), per the BACKLOG.md "Over-replication cleanup" deferred bullet (~line 1041).

## 1. `min_confirms` as a tunable (quorum threshold)

- Today node.rs hardcodes `CLEANUP_MIN_CONFIRMS: usize = usize::MAX` (= all owners after the `min(owners.len())` cap in `cleanup_with`).
- Add a helper `cleanup_min_confirms()` in node.rs following the existing env-tunable pattern (`compact_step_segments()` / `VALS_COMPACT_STEP_SEGMENTS`): read `VALS_CLEANUP_MIN_CONFIRMS`, parse usize, clamp `>= 1`, default `usize::MAX` (current behaviour). Use it at both `cleanup_scoped`/`cleanup_scoped_zone` call sites in `sweep_loop` (read once per pass is fine).
- Document the safety reasoning on the helper/const doc: releasing at all-owners guarantees the key sits at full intended replication before the local copy drops (safest, the default). Releasing at a quorum (e.g. majority of owners) is still *loss-safe* (≥ threshold owners hold a version ≥ ours, and a rightful owner never cleans up a key it owns, so the last copy is never dropped) and read-safe for quorum reads (a majority-confirmed release means any owner read quorum intersects a confirmed owner), but it opens a window where the owner set is below RF among themselves until anti-entropy heals the unconfirmed owners from the confirmed ones — the tunable trades faster garbage release on a partially-reachable owner set against that temporary under-replication window.
- Tests: (a) unit test the env parse/clamp/default of the helper (see how other env-tunable tests are done, or test the pure parsing logic); (b) an antientropy.rs test that a release *proceeds* at threshold 1 when one of two owners' `apply` fails (erroring transport for that owner), and the same scenario at `usize::MAX` keeps the key (deferred). Existing tests already pass explicit `min_confirms`, so the plumbing is proven — the new test pins the threshold-honored behaviour with a partially-failing owner set.

## 2. ReadGate quiesce around the release `forget`

- Today `cleanup_with` takes `can_forget: impl Fn() -> bool` and *samples* it (node passes `|| shared.inflight() == 0`) right before the synchronous forget; sustained reads defer the forget indefinitely. Restructure to the compactor's pause-admission + bounded-drain quiesce, held only around the forget step (the handoff `apply` awaits stay outside the pause, addressing the concern in the node.rs comment block ~lines 2449–2462 that a pause across network awaits would stall reads).
- Seam: replace the `can_forget` closure with a small trait in antientropy.rs (native AFIT, as `AePeer` already uses), e.g.
  `pub trait ForgetGate { type Hold; async fn acquire(&mut self) -> Option<Self::Hold>; }`
  `cleanup_with` calls `gate.acquire().await` only when the release set is non-empty; on `Some(hold)` it runs the existing re-check + `forget` synchronously (no `.await`) while the hold is alive, then drops it; on `None` it defers as today (returns with 0 forgotten). Provide a trivial always/never gate (or a sample-fn wrapper) for the existing antientropy tests (`|| true` / `|| false` call sites) and update `cleanup_scoped`/`cleanup_scoped_zone` signatures + docs.
- Node implementation (node.rs): a `QuiesceGate` holding `Rc<ServerShared>`/refs, the shutdown `Notify`, and a persistent consecutive-timeout counter (lives across sweep passes in `sweep_loop`, e.g. a `Cell<u32>` or `&mut` field). `acquire`: pause via `shared.read_gate().pause()`, `drain_reads(shared, stop, quiesce_wait(counter))`; on `Drained` reset counter and return `Some(pause_guard)` (held across the forget so parked reads cannot race it); on `TimedOut` increment counter, log + bump a new `cleanup_quiesce_timeouts` observe counter (mirror `compaction_quiesce_timeouts`), drop the pause, return `None`; on `Shutdown` return `None`. Reuse `quiesce_wait`'s escalation so a sustained bulk scan can't starve the forget (bounded ticks to success), same reasoning as the compactor.
- Retry semantics fix: today a forget deferred by the inflight sample still counts as a *clean* pass (`cleanup_ok` stays true → `last_cleanup_epochs` recorded → no retry until the next epoch change), contradicting "retried next tick". With the gate, surface the deferral (e.g. gate records "timed out this pass", or cleanup returns a deferred flag) and have `sweep_loop` leave `cleanup_ok = false` on a quiesce timeout so the pass retries next tick.
- Update the now-stale comment block in `sweep_loop` (~2444–2462: "Restructuring cleanup to quiesce just around the forget is a tracked follow-on") and the `cleanup_with`/`cleanup_scoped*` doc comments describing the `can_forget` sampling.
- Test (node.rs, mirroring `compaction_quiesces_inflight_reads_instead_of_starving` ~line 3290): a node whose store holds keys it no longer owns (ring places them on another node reachable via an in-process/loopback transport for the handoff `apply`), plus a reader task that holds ReadGate admissions back-to-back so `inflight` is never 0 at any sampled instant; drive cleanup with the real `QuiesceGate` on a `LocalExecutor` across repeated ticks and assert the keys are forgotten within a bounded number of ticks/time (the old sampling gate would starve — kill-the-fix), and that reads are re-admitted afterwards (gate unpaused). The pause held across the forget is the no-read-racing-forget guarantee; assert paused state during the forget if practical.

## 3. Docs
- BACKLOG.md ~line 1041: mark the "Over-replication cleanup" bullet landed (task 0062), consistent with how other landed bullets are annotated.

## Verification
- `cargo build` + targeted tests: `cargo test -p vals-cluster` (antientropy + node tests), then full `cargo test` and `cargo clippy` if the repo is clean about it.

### Starting points
- crates/vals-cluster/src/antientropy.rs:611-806 — cleanup_with / cleanup_scoped / cleanup_scoped_zone (can_forget seam, re-check + forget block)
- crates/vals-cluster/src/node.rs:2057 CLEANUP_MIN_CONFIRMS; :2444-2516 sweep_loop cleanup call sites + cleanup_ok/last_cleanup_epochs; :1352-1386 quiesce_wait + COMPACT_QUIESCE_WAIT[_MAX]; :2002-2050 Drain + drain_reads; :1556-1587 compact_loop pause+drain usage
- crates/vals-cluster/src/node.rs:3290 compaction_quiesces_inflight_reads_instead_of_starving (test to mirror); :3396 escalation test
- crates/vals-cluster/src/antientropy.rs:1315,1412,1477-1523 existing cleanup tests (run(...), transports, min_confirms args)
- crates/vals-cluster/src/net/server.rs ReadGate (pause/acquire/drained/paused), ServerShared::read_gate, inflight; observe counters: rg 'compaction_quiesce_timeouts' crates/vals-cluster/src/observe*
- env-tunable pattern: node.rs compact_step_segments() / VALS_COMPACT_STEP_SEGMENTS
- BACKLOG.md:1041-1046 the bullet to mark landed

## Work log
- 2026-07-05 plan: Two scoped extensions to over-replication cleanup (crates/vals-cluster/src/antientropy.rs `cleanup_with` + crates/vals-cluster/src/node.rs `sweep_loop`), per the BACKLOG.md "Over-replication cleanup" 
…[truncated]
- 2026-07-05 context hints: 7 recorded with plan
- 2026-07-05 context hints: crates/vals-cluster/src/antientropy.rs:611-806 — cleanup_with / cleanup_scoped / cleanup_scoped_zone (can_forget seam, re-check + forget block); AePeer trait uses native async-fn-in-trait; crates/va
…[truncated]
- 2026-07-05 implementer report: Implemented task 0062: min_confirms quorum tunable + ReadGate quiesce around the over-replication cleanup's release `forget`.  ## What changed  **1. `min_confirms` as a quorum-threshold tunable (crate
…[truncated]
- 2026-07-05 review tier: single-opus — reviewers: Claude
- 2026-07-05 review (Claude): accept — Task 0062 is implemented correctly and completely. Both scoped extensions are present: (1) `min_confirms` is now the `VALS_CLEANUP_MIN_CONFIRMS` release-quorum tunable via a pure `parse_cleanup_min_co
…[truncated]
- 2026-07-05 decision: accept — commit: cleanup: min_confirms release-quorum tunable + ReadGate quiesce around the forget (task 0062)  - VALS_CLEANUP_MIN_CONFIRMS: release quorum threshold (clamped >=1, default   all owners); safety reasoni
…[truncated]
- 2026-07-05 usage: 137,436 tok (in 514, out 136,922, cache_r 26,852,774, cache_w 676,255) · cost n/a (unpriced)
  implementer: 110,108 tok (in 410, out 109,698, cache_r 24,499,508, cache_w 503,382) · cost n/a (unpriced)
  coordinator: 20,349 tok (in 64, out 20,285, cache_r 1,880,609, cache_w 132,226) · cost n/a (unpriced)
  reviewer:Claude: 6,979 tok (in 40, out 6,939, cache_r 472,657, cache_w 40,647) · cost n/a (unpriced)

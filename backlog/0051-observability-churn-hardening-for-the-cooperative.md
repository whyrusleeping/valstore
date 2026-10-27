---
id: "0051"
title: Observability + churn hardening for the cooperative Merkle leaf seed
status: done
priority: 5
created: "2026-07-04"
updated: "2026-07-05"
depends_on:
    - "0045"
spec_refs: []
---

## Description
Follow-on from task 0045 (budgeted cooperative Merkle leaf seed). Two small hardening items noticed during implementation/review, deliberately kept out of the 0045 commit:

1. **Restart livelock guard**: `ensure_seeded_cooperative` restarts the whole fold whenever `Engine::runs_removed_epoch` bumps (a compaction/wipe removed a run mid-seed). Under pathological continuous compaction churn the seed could restart indefinitely and the AE sweep would never descend. Consider a restart counter with a bounded fallback (e.g. after N restarts log loudly and/or fall back to the one-shot sync `ensure_seeded`, accepting the stall as a last resort) — or at minimum prove the churn rate can't outpace the seed.

2. **Observability**: add counters (observe.rs) for seed steps, restarts, journal length at finalize, and total seed wall time, so the first-AE-touch behaviour is visible on the box (relevant to the deferred box validation of the 0005/0045 path).

## Acceptance criteria
- A bounded-restart policy (or a documented argument why livelock is impossible) with a test.
- Seed-progress counters exposed via the existing observe machinery.
- No behaviour change on the happy path; workspace tests stay green.

## Acceptance criteria

## Plan

Goal (task 0051, follow-on to 0045): (1) bound the cooperative Merkle-leaf seed's restart-on-churn behaviour so pathological continuous compaction can't livelock the seed, and (2) expose seed-progress counters via the existing observe machinery. No happy-path behaviour change.

1. Restart livelock guard (crates/vals-cluster/src/replica.rs):
   - Add `seed_restart_count: std::cell::Cell<u32>` (and a `seed_started: std::cell::Cell<Option<std::time::Instant>>` for the wall-time metric, see §2) to `VersionedStore`. Initialize in both constructors (`seed`/`lazy`).
   - In `seed_step`, at BOTH restart points (runs_removed_epoch changed; `fold_run_versions` returned Ok(false) because a run vanished): increment the restart count. If it exceeds a const `MAX_SEED_RESTARTS` (suggest 8, documented), do a bounded fallback instead of restarting: log a loud `log::warn!` (target-appropriate, mention run churn outpacing the cooperative seed and the accepted one-shot stall), bump the fallback counter, and run the existing one-shot sync `ensure_seeded()` inline (it is synchronous/atomic under the single-threaded executor — no yields — so churn cannot interleave and it is guaranteed to terminate; that is the documented livelock argument). On Ok return Ok(true); an Err propagates fail-closed exactly like today (index left unseeded, progress dropped).
   - Reset `seed_restart_count` to 0 whenever a seed generation completes or is invalidated: in `finalize_seed`, after the sync-fallback `ensure_seeded` succeeds (and inside `ensure_seeded` itself when it seeds), and in `invalidate_leaf_seed`/`clear` (a fresh generation). Do NOT reset it in `reset_seed_progress` (that's the restart path itself — it must keep counting).
   - Document the policy on `seed_step`'s doc comment (restart bound + why the sync fallback terminates).

2. Observability (crates/vals-client/src/observe.rs — re-exported as vals_cluster::observe):
   Add to the `counters!` list, following the existing ae_* family style, with clear help docs:
   - `ae_seed_steps` — cooperative Merkle-leaf seed steps executed (each folds at most one run's sidecar); bump once per `seed_step` invocation that does work (not the already-seeded early return).
   - `ae_seed_restarts` — cooperative seed restarts (a run was removed/merged mid-seed); bump at both restart points.
   - `ae_seed_sync_fallbacks` — bounded-restart guard trips: cooperative seed gave up after MAX_SEED_RESTARTS and fell back to the one-shot synchronous pass (accepting the serving stall).
   - `ae_seed_journal_ops` — mutations journaled during seeds and replayed at finalize; `add(journal.len() as u64)` in `finalize_seed`.
   - `ae_seed_wall_us` — total wall-clock microseconds from a seed's first step to its completion (including yields and restarts); set `seed_started` at the first START if `None`, and at completion (finalize_seed OR successful sync fallback / sync ensure_seeded when it had a started stamp) add the elapsed µs and clear the stamp. Clear the stamp too on invalidate/clear (abandoned generation — do not credit its time).
   Counter bumps happen in vals-cluster via `crate::observe::counters()`.

3. Tests (replica.rs test module, reusing seed_run_backed_store etc.):
   a. Livelock-guard test: run-backed store; drive `seed_step` manually, and between steps force a run merge/compaction so `runs_removed_epoch` bumps every time (the pattern of the existing restart test ~line 4251). Without the guard this would never finish; assert that within a bounded number of steps `seed_step` returns Ok(true) (the fallback fired), the index is seeded, and digests equal `recompute_leaves`. Also assert the `ae_seed_sync_fallbacks` counter increased (delta ≥ 1).
   b. Counter sanity: in an existing-style cooperative seed test, snapshot counters before/after and assert deltas ≥ expected (steps ≥ run count, journal ops ≥ number of mid-seed mutations applied, restarts ≥ 1 in the churn test). Use ≥ (deltas), not equality — counters are process-global and tests run concurrently.
   c. All existing lazy-seed/cooperative tests stay green (happy path unchanged).

4. Verify: `cargo build --workspace`, `cargo test -p vals-cluster` (full `cargo test --workspace` if time allows), `cargo clippy --workspace --all-targets` with no new warnings, `cargo fmt --check` clean for touched files (note: repo has pre-existing fmt drift tracked as task 0053 — don't fix unrelated files).

### Starting points
- crates/vals-cluster/src/replica.rs — seed_step ~852, finalize_seed ~926, ensure_seeded ~790, reset_seed_progress ~817, invalidate_leaf_seed ~1007, SeedProgress ~219, ensure_seeded_cooperative ~2309
- crates/vals-cluster/src/replica.rs tests: restart-on-epoch-bump test ~4251, cooperative tests ~4300-4380, seed_run_backed_store/recompute_leaves helpers ~3560+
- crates/vals-client/src/observe.rs — counters! macro list ends at digest_hedges_fired ~318; ae_* family ~218-226
- vals-cluster re-exports observe: `pub use vals_client::{observe, ...}` in lib.rs; other crates bump via crate::observe::counters()

## Work log
- 2026-07-05 plan: Goal (task 0051, follow-on to 0045): (1) bound the cooperative Merkle-leaf seed's restart-on-churn behaviour so pathological continuous compaction can't livelock the seed, and (2) expose seed-progress
…[truncated]
- 2026-07-05 context hints: 4 recorded with plan
- 2026-07-05 context hints: crates/vals-cluster/src/replica.rs — seed_step ~852, finalize_seed ~926, ensure_seeded ~790, reset_seed_progress ~817, invalidate_leaf_seed ~1007, VersionIndex::clear ~499, SeedProgress ~219, constr
…[truncated]
- 2026-07-05 implementer report: Implemented task 0051 (observability + churn hardening for the cooperative Merkle-leaf seed).  ## Changes  ### 1. Bounded-restart livelock guard (crates/vals-cluster/src/replica.rs) - Added a document
…[truncated]
- 2026-07-05 review tier: single-opus — reviewers: Claude
- 2026-07-05 review (Claude): accept — The change fully satisfies task 0051. It adds a bounded-restart livelock guard (MAX_SEED_RESTARTS=8) to the cooperative Merkle-leaf seed with a documented termination argument and a synchronous fallba
…[truncated]
- 2026-07-05 decision: accept — commit: feat(ae): bounded-restart guard + observe counters for the cooperative Merkle leaf seed (task 0051)  Pathological continuous compaction churn could restart the cooperative seed forever (every step see
…[truncated]
- 2026-07-05 usage: 37,959 tok (in 156, out 37,803, cache_r 3,101,660, cache_w 190,735) · cost n/a (unpriced)
  implementer: 23,693 tok (in 102, out 23,591, cache_r 2,288,724, cache_w 70,154) · cost n/a (unpriced)
  coordinator: 9,490 tok (in 30, out 9,460, cache_r 652,406, cache_w 99,941) · cost n/a (unpriced)
  reviewer:Claude: 4,776 tok (in 24, out 4,752, cache_r 160,530, cache_w 20,640) · cost n/a (unpriced)

---
id: "0045"
title: Budgeted/off-core incremental Merkle leaf seed (avoid one-time serving stall at first AE touch)
status: done
priority: 4
created: "2026-07-04"
updated: "2026-07-04"
depends_on:
    - "0005"
spec_refs: []
---

## Description
Task 0005 made the boot Merkle leaf-rebuild lazy: a store with runs boots unseeded and folds `Engine::recovered_versions()` into the leaf accumulator once, on the first anti-entropy leaf touch (`VersionedStore::ensure_seeded`). That deferred pass still runs **synchronously on the serving core** and is O(dataset) over the run sidecars — measured ~385 ms at 1.28M keys/64 runs (release, file-backed), so at very large run counts the first AE `SyncTree` serve (or first local sweep descent) stalls the serving core for seconds.

Make the one-time seed non-monopolizing: a budgeted/cooperative-chunked fold (e.g. seed a few runs' sidecars per slice, yielding between chunks, with mutations during the window either tracked into the fold or the affected buckets re-folded), or run it off-core like C.5b flush maintenance and swap the result in atomically. Until the seed completes, AE tree serves should either wait (bounded) or answer "not ready" without shipping zeroed digests.

## Acceptance criteria
- The first AE leaf touch after a large-run-count boot no longer blocks the serving core for the whole O(dataset) pass (demonstrated with a measurement: serving-path latency during the seed window bounded, vs the current multi-hundred-ms/seconds stall).
- Leaf exactness invariant preserved: after the incremental seed completes, digests equal the from-scratch recompute over `recovered_versions`, including mutations that landed mid-seed (test covering writes/deletes/GC during the seed window).
- Fail-closed behavior preserved: a sidecar read error mid-seed never results in served zeroed/partial digests.

## Acceptance criteria

## Plan

Goal: the one-time deferred Merkle leaf seed (VersionedStore::ensure_seeded, an O(dataset) run-sidecar pass) must no longer monopolize the serving core at the first AE leaf touch. Approach: a budgeted, cooperative-chunked incremental seed driven on the serving executor (one run's sidecar per step, yielding between steps), with mutations that land mid-seed captured in a journal and replayed exactly at completion; production AE paths (server SyncTree serve + local sweep descent) await the cooperative seed instead of the blocking one-shot pass. Fail-closed throughout.

DESIGN

1. vals-store engine API (crates/vals-store/src/engine.rs):
   - `pub fn run_handles(&self) -> Vec<RunHandle>` — snapshot of `self.runs[i].meta.handle` (RunHandle is Clone+Eq).
   - `pub fn fold_run_versions(&self, handle: &RunHandle, f: impl FnMut(&[u8], u128)) -> Result<bool, EngineError>` — find the open run whose `meta.handle == handle`; absent → `Ok(false)`; present → one streaming `for_each_bucket_digest(&self.alloc, f)` pass → `Ok(true)`. Error propagates.
   - `pub fn runs_removed_epoch(&self) -> u64` — a new engine counter bumped at every point a run is REMOVED or REPLACED (engine.rs lines ~2529 `self.runs = vec![...]`, ~2830 `self.runs = new_runs` (compact_commit), ~3515 `self.runs.clear()` (wipe); audit all `self.runs` mutations — pure pushes (flush_commit ~2078, ~2282 if add-only) do NOT bump). This is the restart signal: RunHandle equality alone could theoretically collide after segment reuse, the epoch cannot.

2. vals-cluster incremental seed state (crates/vals-cluster/src/replica.rs):
   - VersionIndex gains `seeding: Cell<bool>` and `journal: RefCell<Vec<LeafOp>>` where `enum LeafOp { Apply { key, group, old: Option<Version>, new: Version }, Retire { key, group, version: Version } }`.
     - `apply_leaf` / `retire_leaf`: if `seeded` → incremental XOR as today; else if `seeding` → push the op onto the journal (no leaf touch); else → no-op as today. (The ops carry the exact `old`/current version the caller already read, so replaying them chains exactly by XOR algebra.)
     - `invalidate()` and `clear()` must also reset seeding state: `seeding=false`, journal cleared (VersionedStore side drops its progress — see below; add a shared reset helper so no path leaves a stale journal).
   - VersionedStore gains `seed_progress: RefCell<Option<SeedProgress>>` with:
     - `pending: Vec<RunHandle>` (unfolded runs), `runs_epoch: u64` (engine.runs_removed_epoch at snapshot),
     - `by_key: HashMap<Box<[u8]>, u128>` (argmax winner so far — dedup across memtable+runs and the old-version source for XOR correction),
     - `staging: HashMap<Box<[NodeId]>, Box group-leaf array>` + `live: u64` + `high_ms: u64` (same shapes as VersionIndex leaves).
   - `pub fn is_leaf_seeded(&self) -> bool`.
   - `pub fn seed_step(&self) -> Result<bool /*done*/, EngineError>` (&self, interior mutability, sync, NO awaits inside — atomic under the single-threaded executor):
     - If already seeded → Ok(true).
     - If no progress: START — snapshot `runs_removed_epoch` + `run_handles()`; fold the memtable snapshot (`engine.index_versions()`, RAM-only) into staging/by_key; set `versions.seeding = true`, journal cleared. If `run_count()==0` finalize immediately.
     - Else CONTINUE — first check `engine.runs_removed_epoch() != progress.runs_epoch` → RESTART (drop progress, clear journal, seeding=false, begin fresh next call; a restart is exact because a re-snapshot reflects all mutations so far). Otherwise pop one pending RunHandle and `fold_run_versions`: for each (key, v): look up by_key; absent → insert, XOR entry_digest(key,v) into staging[group_of(key)], live+1; present with cur < v → XOR out digest(key,cur), XOR in digest(key,v), update by_key; cur >= v → skip. Track high_ms. `Ok(false)` from fold_run_versions (run vanished) → RESTART as above.
     - When pending is empty: FINALIZE (sync, atomic): replay the journal in order onto staging (Apply: XOR out old if Some else live+1, XOR in new; Retire: XOR out, live−1; fold journal versions into high_ms), then swap staging into `versions.leaves`, set `live`, `seeded=true`, `seeding=false`, drop progress, clear journal, raise `applied_high_ms` to max(current, high_ms). Return Ok(true).
     - Any engine error → ABORT fail-closed: drop progress, clear journal, `seeding=false`, index stays UNSEEDED, return Err (a later touch retries from scratch; zeroed/partial digests are never served because `seeded` stays false and leaf serves gate on it).
   - `pub async fn ensure_seeded_cooperative<D>(store: &RefCell<VersionedStore<D>>) -> Result<(), EngineError>` (free fn in replica.rs): loop { if seeded → Ok; `store.borrow().seed_step()?`; `vals_rt::yield_now().await }`. Borrow is dropped before each yield (no borrow across await). Safe under concurrent callers: each step is atomic and the state machine tolerates interleaved steppers.
   - Keep the existing sync `ensure_seeded` (one-shot full pass) as the fallback for direct callers/tests — but make it first discard any in-flight incremental progress + journal (it recomputes from then-current engine state, so the discard is exact).

3. Wire the cooperative path into production AE touch points:
   - crates/vals-cluster/src/net/server.rs: move the `SyncTree` serve from the inline sync arm to the spawned-task pattern used by `SyncValues`/`Digest` (~line 1598): spawn, `ensure_seeded_cooperative(&store).await` (error → Response::Err, never zeroed digests), then borrow + `scoped_leaf_digests` (RAM-only once seeded). Update the now-stale comment at ~line 1597 ("SyncTree ... stays inline"). Track reads_inflight like the neighbours if appropriate.
   - crates/vals-cluster/src/antientropy.rs `reconcile_with` (~line 396): `ensure_seeded_cooperative(local).await` before the descent loop (map err → TransportError::Engine). Same in `LocalPeer::tree` (~line 807) since it is async.
   - The sync serve arm in server.rs (~line 2779) and `version_leaf_digests`/`scoped_leaf_digests` keep the sync ensure_seeded fallback — behaviour-preserving for existing tests.

4. Tests (replica.rs test module; reuse seed_run_backed_store / CountingDevice / FailableDevice / recompute_leaves):
   a. Exactness with mid-seed mutations: build a run-backed store (e.g. 8 runs), drive `seed_step` manually; BETWEEN steps apply an overwrite of a run-resident key, a brand-new key, a delete (tombstone), and a `forget` (retire path); after completion assert `version_leaf_digests == recompute_leaves` and `live_keys` exact. This covers the journal replay (writes/deletes/GC during the seed window).
   b. Fail-closed mid-seed: FailableDevice; start stepping, inject the fault mid-way → `seed_step` returns Err, index unseeded, `version_leaf_digests` on a fresh fault still errs (no zeroed digests); clear fault → stepping (or sync fallback) completes and matches recompute.
   c. Run-set change mid-seed: between steps force a compaction (or wipe+reflush) so `runs_removed_epoch` bumps / a pending run vanishes → seed restarts and final digests match recompute.
   d. Cooperative liveness (the measurement criterion, deterministic form): on a vals_rt executor, spawn `ensure_seeded_cooperative` plus a witness task that increments a counter per turn (the maint.rs `serving_advances_while_an_offcore_flush_is_in_flight` pattern); assert the witness advanced at least ~run_count times while the seed was in flight (i.e. the seed yielded between chunks and other tasks interleaved), and that the number of seed steps ≈ run count (each step folds one run — the per-step work is O(one run's sidecar), not O(dataset)).
   e. Keep all existing lazy-seed tests green (they exercise the sync fallback).
   f. A measurement note: run a larger-scale comparison (e.g. 64 runs, release) locally comparing one-shot ensure_seeded duration vs max single seed_step duration; record numbers in the task work log (avoid fragile timing asserts in CI).

5. Verification: `cargo build --workspace`, `cargo test -p vals-cluster -p vals-store` (plus full `cargo test --workspace` if time permits), `cargo clippy --workspace --all-targets` stays at current warning level (no new warnings).

Notes / accepted trade-offs:
- Journal memory is bounded by write rate × seed duration (seconds) — strictly smaller than the transient by_key map; documented, not capped.
- Per-step budget = one run's sidecar (box-measured ~6 ms/run at 1.28M keys/64 runs). Sub-run chunking is possible later if single-run sidecars grow (note as follow-on if needed).
- The off-core (maint-worker) variant was considered and rejected for now: sidecar reads by the worker would race compaction freeing input segments (no run pin/refcount exists); the cooperative on-core fold with restart-on-epoch-change is safe with today's single-writer discipline.

### Starting points
- crates/vals-cluster/src/replica.rs — VersionIndex (seeded/apply_leaf/retire_leaf/seed_pass ~lines 230-395), ensure_seeded ~651, store() ~1805 (single mutation point, has exact `old`), lazy-seed tests ~3560-3800 (seed_run_backed_store, CountingDevice, FailableDevice, recompute_leaves)
- crates/vals-store/src/engine.rs — recovered_versions ~1287, index_versions ~1533, run_count ~2339, self.runs mutation points: 2078/2282 push, 2529/2830 replace, 3515 clear
- crates/vals-store/src/run.rs — RunHandle ~797 (Clone+Eq), DeviceRun::for_each_bucket_digest ~1519
- crates/vals-cluster/src/net/server.rs — spawned serve pattern ~1598 (SyncValues), inline SyncTree arm ~2779, stale comment ~1597
- crates/vals-cluster/src/antientropy.rs — reconcile_with ~385 (descent at 413-436), LocalPeer::tree ~807
- vals_rt::yield_now (crates/vals-rt/src, ~line 562); witness-task pattern: crates/vals-cluster/src/maint.rs test serving_advances_while_an_offcore_flush_is_in_flight ~584

## Work log
- 2026-07-04 plan: Goal: the one-time deferred Merkle leaf seed (VersionedStore::ensure_seeded, an O(dataset) run-sidecar pass) must no longer monopolize the serving core at the first AE leaf touch. Approach: a budgeted
…[truncated]
- 2026-07-04 context hints: 6 recorded with plan
- 2026-07-04 context hints: crates/vals-cluster/src/replica.rs — VersionIndex (seeded/apply_leaf/retire_leaf/seed_pass ~lines 230-395), ensure_seeded ~651, store() ~1805 (single mutation point, has exact `old`), lazy-seed test
…[truncated]
- 2026-07-04 implementer report: Implemented Task 0045: a budgeted, cooperative (on-serving-core, yielding) incremental Merkle-leaf seed that replaces the one-shot O(dataset) run-sidecar fold on the production anti-entropy serve path
…[truncated]
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change implements the budgeted/cooperative Merkle-leaf seed exactly as the task requires. It adds the engine per-run fold API plus a runs_removed_epoch counter (bumped at all run removal/replace p
…[truncated]
- 2026-07-04 decision: accept — commit: feat(ae): budgeted cooperative Merkle leaf seed — per-run yielding fold with mid-seed mutation journal, restart-on-run-removal epoch, fail-closed; SyncTree/reconcile/LocalPeer await it instead of th
…[truncated]
- 2026-07-04 usage: 108,443 tok (in 368, out 108,075, cache_r 14,303,716, cache_w 373,571) · cost n/a (unpriced)
  implementer: 58,744 tok (in 242, out 58,502, cache_r 10,919,554, cache_w 140,088) · cost n/a (unpriced)
  coordinator: 37,710 tok (in 74, out 37,636, cache_r 2,511,156, cache_w 182,988) · cost n/a (unpriced)
  reviewer:Claude: 11,989 tok (in 52, out 11,937, cache_r 873,006, cache_w 50,495) · cost n/a (unpriced)

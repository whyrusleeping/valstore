---
id: "0005"
title: Lazy/audited boot Merkle leaf-rebuild (today O(dataset) run-sidecar reads)
status: done
priority: 3
created: "2026-07-02"
updated: "2026-07-04"
depends_on: []
spec_refs: []
---

## Description
From `docs/out-of-core-index.md` C.6 follow-ups: boot leaf-rebuild reads all run sidecars — O(dataset) — partially undoing the Stage A recovery win at very large run counts. Make it lazy (rebuild leaves on first AE touch) or audited/persisted alongside the manifest.

## Acceptance criteria
- Boot time no longer scales with total run/sidecar volume for the leaf rebuild (measured on a large dataset).
- Merkle leaf accumulators remain exact vs `recovered_versions` (the invariant the C.5/C.6 holistic review pinned); tests cover the lazy/persisted path incl. crash between manifest and leaf state.

## Acceptance criteria

## Plan

Goal: boot must not pay the O(dataset) run-sidecar pass that today rebuilds the cluster Merkle leaf accumulator. Take the task's first suggested option: **lazy leaf-rebuild on first AE touch** (no new persisted state, so no manifest/leaf crash-consistency surface at all — the seed always recomputes from the engine's crash-recovered state, which is exactly the invariant source `recovered_versions`).

Design
1. `VersionIndex` (crates/vals-cluster/src/replica.rs) gains a `seeded: bool`. Unseeded means "leaves/live are not yet meaningful".
2. Construction becomes lazy:
   - `VersionedStore::open` uses plain `Engine::open` (drop the `open_with_versions` side-data pass) and builds an **unseeded** index.
   - `VersionedStore::new` stops calling `engine.recovered_versions()` — unseeded too (covers the zonestore SPDK `new(engine, node)` boot path). Keep the `Result` signature to avoid churn at ~15 call sites.
   - `from_recovered` stays eager (side-data already in hand) and marks seeded.
   - Watermark: at lazy construction, fold **memtable-only** versions (`engine.index_versions()`, RAM-only, O(memtable)) into `applied_high_ms` so the data-progress watermark stays near-exact without device reads (the newest writes live in the replayed tail); the full seed later raises it to exact. Node-level persisted watermark floor still covers liveness across restart. An unseeded/low high-water only *suppresses* tombstone GC (eff_now = min(wall, high_water)) — conservative, safe.
3. `ensure_seeded(&mut self) -> Result<(), EngineError>`: if unseeded, run the existing seed fold (`engine.recovered_versions()` → clear + `apply_leaf(key, None, v)` per key + watermark fold), set seeded. Single-threaded executor + sync fn ⇒ atomic w.r.t. mutations.
4. First-leaf-touch triggers: `version_leaf_digests` and `scoped_leaf_digests` become `&mut self` + `Result<_, EngineError>`, calling `ensure_seeded()` first. Fail-closed: a sidecar read error is surfaced as an error (never empty/zero digests). Update call sites:
   - net/server.rs `SyncTree` serve: `store.borrow_mut()`, map Err → `Response::Err`.
   - antientropy.rs `reconcile_with` local descent (line ~355): `local.borrow_mut()`, map Err → `TransportError::Engine`.
   - The `AePeer`-over-local test/utility impls (antientropy.rs ~718/~1388/~1575) and replica/antientropy unit tests.
5. Unseeded window: `apply_leaf`/`retire_leaf` maintenance is **skipped** entirely (guard in VersionIndex). Correct because the eventual seed recomputes from the engine's then-current keyset (XOR associativity — same argument as the existing recompute invariant); this also avoids the `retire_leaf` live-count underflow debug_assert for GC of flushed keys. `clear()` (wipe) marks seeded — the empty state is exactly known.
6. `live_keys()` while unseeded: return the engine `total_entries()` upper bound, documented as approximate until the first AE touch seeds the index (the gauge converges to exact within the first sweep, which the node kicks at startup).

Tests (replica.rs unit tests unless noted)
- Lazy boot does no sidecar pass: build a store with several flushed runs on a counting device (per-engine-test `CountingDevice` pattern), reopen via `VersionedStore::open`; assert the read count during open is far below / does not scale with sidecar volume, then the first `version_leaf_digests` call performs the reads and returns digests exactly equal to the from-scratch recompute (`leaf_digests_versioned` over `recovered_versions`).
- Unseeded-window mutations: reopen a run-backed store, do puts/overwrites/deletes/`forget`/GC-eligible `compact` BEFORE any leaf touch, then first leaf access equals the recompute and `live_keys` is exact after seeding.
- Seed failure is fail-closed: a device that fails sidecar reads makes the first leaf access return Err (and a later access can succeed after the fault clears, or at least never serves zeroed digests).
- Existing invariant tests (`incremental_leaves_match_recompute`, `version_index_tracks_every_mutation`, scoped-digest tests) keep passing — `new` is lazy now so they also exercise the lazy path.
- Full suite: cargo build + cargo test (workspace), clippy clean per repo pin.

Docs
- Update the `VersionIndex`/`VersionedStore` doc comments (recovery no longer seeds eagerly; seed-on-first-AE-touch).
- docs/out-of-core-index.md line ~822: mark the lazy boot leaf-rebuild follow-up landed (task 0005).

Measurement (acceptance #1): a quick local run over a FileBlockDevice dataset with many runs comparing open time / device reads before vs after (record numbers in the work log); the counting-device test pins the property in CI.

Non-goals / follow-on: the O(dataset) pass still happens once, synchronously, on the serving core at first AE touch — file a follow-on task for a budgeted/off-core incremental seed if the one-time stall matters at very large run counts.

### Starting points
- crates/vals-cluster/src/replica.rs: VersionIndex (~line 191), seed/new/open/from_recovered (~369-437), version_leaf_digests/scoped_leaf_digests (~991-1008), retire_leaf call sites (650/691/766/811/834), apply_leaf (~1402), live_keys (~871)
- crates/vals-cluster/src/net/server.rs:2559 OwnedRequest::SyncTree serve; :326 watermark()
- crates/vals-cluster/src/antientropy.rs:355 local scoped_leaf_digests in reconcile_with; AePeer-over-local impls at ~718/~1388/~1575
- crates/vals-store/src/engine.rs: open_inner want_versions (~1270), recovered_versions (~1287), index_versions (~1533); CountingDevice test pattern (~5001)
- crates/vals-cluster/src/zonestore.rs: VersionedStore::open/new call sites (~204/207/275/279/797/817)
- docs/out-of-core-index.md:822 follow-up note to update

## Work log
- 2026-07-04 plan: Goal: boot must not pay the O(dataset) run-sidecar pass that today rebuilds the cluster Merkle leaf accumulator. Take the task's first suggested option: **lazy leaf-rebuild on first AE touch** (no new
…[truncated]
- 2026-07-04 context hints: 6 recorded with plan
- 2026-07-04 context hints: crates/vals-cluster/src/replica.rs: VersionIndex (~line 191), seed/new/open/from_recovered (~369-437), version_leaf_digests/scoped_leaf_digests (~991-1008), retire_leaf call sites (650/691/766/811/834
…[truncated]
- 2026-07-04 implementer report: Implemented task 0005: lazy boot Merkle leaf-rebuild — boot no longer pays the O(dataset) run-sidecar pass to rebuild the cluster Merkle leaf accumulator. Took the task's first option (lazy seed on 
…[truncated]
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change implements the task's first option — lazy Merkle leaf-rebuild on first anti-entropy touch — cleanly and correctly. `VersionedStore::open`/`new` now use plain `Engine::open` (want_versio
…[truncated]
- 2026-07-04 decision: accept — commit: perf: lazy boot Merkle leaf-rebuild — seed on first AE touch, not O(dataset) at boot (task 0005)  VersionedStore::open/new no longer run the O(dataset) run-sidecar recovered_versions pass at boot; t
…[truncated]
- 2026-07-04 usage: 88,179 tok (in 320, out 87,859, cache_r 12,357,459, cache_w 304,070) · cost n/a (unpriced)
  implementer: 67,973 tok (in 216, out 67,757, cache_r 8,629,617, cache_w 132,834) · cost n/a (unpriced)
  coordinator: 10,836 tok (in 52, out 10,784, cache_r 2,947,554, cache_w 126,767) · cost n/a (unpriced)
  reviewer:Claude: 9,370 tok (in 52, out 9,318, cache_r 780,288, cache_w 44,469) · cost n/a (unpriced)

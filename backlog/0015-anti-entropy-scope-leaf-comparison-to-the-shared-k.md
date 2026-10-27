---
id: "0015"
title: 'Anti-entropy: scope leaf comparison to the shared keyspace under RF&lt;N'
status: done
priority: 3
created: "2026-07-02"
updated: "2026-07-04"
depends_on: []
spec_refs: []
---

## Description
Measured residual from the hierarchical-Merkle work (BACKLOG.md, 2026-06-19): with RF&lt;N each node holds a different subset of the keyspace, so peers' full-leaf roots **permanently differ on the non-shared keysets** — ~68k digests/s examined-then-discarded at steady state with zero churn (RF=2, 3 nodes). Dense divergence, so the hierarchical descent doesn't reduce it. Candidate fix: scope the compared leaves/digests to the keys both peers replicate (placement-intersection-aware leaf accumulators or filtered digest pages).

Note: re-baseline against post-LSM AE (leaf accumulators now maintained across flush/compact) before designing.

## Acceptance criteria
- Steady-state converged RF&lt;N cluster examines ~0 digests/s (vs ~68k) with no loss of healing coverage.
- Sparse-divergence heal behavior unchanged (the 13× hierarchical win preserved); suite green.

## Acceptance criteria

## Plan

Goal: eliminate the permanent RF<N anti-entropy residual (~68k digests/s examined-then-discarded at converged steady state) by scoping the Merkle comparison to the keyspace both peers replicate, without losing healing coverage or the hierarchical sparse-divergence win.

Note on re-baselining: no SPDK hardware is available in this session. The mechanism is structural and post-LSM code confirms it: `VersionIndex` leaf accumulators fold every locally-held key, so under RF<N two peers' trees permanently differ on the non-shared keysets and every sweep pass pulls digest pages up to budget. We validate the fix with deterministic in-process tests (counters / CountingPeer) instead of a box run; a box measurement can ride an existing hardware-validation task.

Design (placement-intersection-aware leaf accumulators):

1. Group the leaf accumulators by the key's replica set.
   - `VersionIndex.leaves` becomes a map `replica-set group → Box<[u64; INDEX_BUCKETS]>` (group key = the key's sorted replica set, e.g. `Box<[NodeId]>`; lazily created). A store with no placement wired uses a single catch-all group (behavior identical to today).
   - `VersionedStore` gains an optional placement fn (e.g. `RefCell<Option<Rc<dyn Fn(&[u8]) -> Vec<NodeId>>>>`), set by the node per zone: default zone → `ring.replicas(key)`; named zone → `ring.replicas_in_zone(zone, place, rf, scope)` with `place` = range token for sorted zones (must agree exactly with the scoping predicates already used in `reconcile_scoped`/`reconcile_scoped_zone`/cleanup).
   - `apply_leaf` / `retire_leaf` / `seed_pass` compute the group once per mutation and XOR in/out within that group. `live` count stays global. XOR-in and XOR-out for a key must always hit the same group; that is guaranteed by re-seeding on placement change (below).
   - Unscoped digests (existing `leaf_digests`, `scoped_leaf_digests` with no counterpart) = XOR-fold across all groups — same values as today.

2. Peer-scoped tree serve.
   - `scoped_leaf_digests` (VersionIndex + VersionedStore wrapper) gains a `counterpart: Option<NodeId>` parameter: with `Some(x)`, fold only groups whose replica set contains BOTH the local node and `x` (VersionedStore already knows its own node id). With `None` or no placement wired: current unscoped behavior.
   - `AePeer::tree` (vals-client `ae.rs`) gains the requester's node id; wire `Request::SyncTree`/`OwnedRequest::SyncTree` gain a `requester: NodeId` field (no wire back-compat needed — preproduction, same as the prior SyncTree change; update wire tests + wire_fuzz). Net server passes `Some(requester)` into the store's scoped serve. `LocalPeer` carries an optional counterpart (keep a way to build the old unscoped one for existing tests).
   - `reconcile_with` gains the counterpart node id (threaded from `reconcile_scoped`/`reconcile_scoped_zone`, which already receive/can receive the peer's NodeId from `sweep_loop`'s `netpeers`) and uses it both for its local `scoped_leaf_digests` and the peer `tree()` call. Semantics: both sides compare trees over exactly `{k held : local ∈ replicas(k) ∧ peer ∈ replicas(k)}` — equal at convergence ⇒ root round matches ⇒ 0 digests examined.
   - Phase-1 digest pages stay unscoped (puller's `keep()` already discards); at converged steady state no leaf differs so no pages are pulled — the acceptance criterion is met by tree scoping alone. Sparse-divergence descent is unchanged.

3. Re-seed on placement change.
   - Any change to placement (ring members/weights/rf, ring epoch, catalog epoch — the same fingerprint `sweep_loop` already computes as `cleanup_cfg` — plus sorted-zone range-map changes if not covered by catalog epoch) invalidates group assignment. On fingerprint change, mark each zone store's index unseeded (`VersionIndex.seeded = false`, clear groups) BEFORE that pass's reconcile/cleanup; the existing lazy `ensure_seeded` re-folds from `Engine::recovered_versions` on the next AE touch (an O(dataset) sidecar pass — same cost class as the existing first-touch seed; acceptable for rare membership changes; task 0045 already tracks budgeted seeding).
   - While unseeded, `apply_leaf`/`retire_leaf` remain no-ops (existing invariant) — this also makes cleanup's `forget` under the *new* placement safe (no stale-group residue).

4. Healing-coverage note (deliberate, document it): intersection scoping means AE between L and P no longer compares keys only ONE of them replicates. Keys a node holds but doesn't own (sloppy-quorum fallback, post-membership-change leftovers) are healed by hinted handoff and the cleanup handoff pass (which pushes to the rightful owners before forgetting) — AE's leaf comparison was pure waste for those. Genuine replica-pair divergence is still fully covered.

5. Tests (kill-the-fix):
   - New: converged RF=2, 3-node reconcile (each node holding exactly its owned subset, all versions equal) examines 0 digests and 0 tree-level mismatches beyond the root round — via CountingPeer digest_entries == 0 (this is the ~68k→0 acceptance criterion, in-process).
   - New: divergence within the shared keyspace (peer advanced one shared key) still heals, and the sparse-diff bound from `hierarchical_descent_localizes_a_sparse_diff` still holds with scoping active.
   - New: placement change (ring membership change) → index reseeds and reconcile still converges correctly (no stale-group residue: incremental leaves match recompute after re-seed).
   - Existing suite green: `scoped_reconcile_regains_owned_without_adopting_others`, `sweep_converges_a_rejoined_node_over_the_cluster`, zone/sorted-zone scoping tests, `incremental_leaves_match_recompute`, daemon e2e, wire tests.
   - `cargo build`, `cargo test` (workspace), clippy + doc clean per repo convention.

6. Docs: update the module docs in `antientropy.rs`/`replica.rs` and the BACKLOG/docs note (docs/algorithms.md if it describes AE) to describe intersection scoping; record the measured/tested outcome in the task work log.

### Starting points
- crates/vals-cluster/src/antientropy.rs — reconcile_with (descent + keep()), LocalPeer, DEFAULT_BUCKETS/tree_levels
- crates/vals-cluster/src/replica.rs:171-414 — VersionIndex (leaves, apply_leaf/retire_leaf/seed_pass, scoped_leaf_digests), ensure_seeded at line ~557, pub scoped_leaf_digests ~1161
- crates/vals-cluster/src/node.rs:1369-1660 — sweep_loop: netpeers with NodeId, cleanup_cfg placement fingerprint (reuse for reseed trigger), zone cfg/range_map resolution
- crates/vals-client/src/ae.rs — AePeer trait (tree signature)
- crates/vals-client/src/wire.rs — Request::SyncTree / OwnedRequest::SyncTree encode/decode + tests
- crates/vals-cluster/src/net/server.rs:2559 — SyncTree serve; net/client.rs AePeer impl
- crates/vals-cluster/tests/wire_fuzz.rs — SyncTree coverage
- Ring::replicas / Ring::replicas_in_zone in crates/vals-cluster/src/ring.rs — placement predicates that must agree with the grouping fn

## Work log
- 2026-07-04 plan: Goal: eliminate the permanent RF<N anti-entropy residual (~68k digests/s examined-then-discarded at converged steady state) by scoping the Merkle comparison to the keyspace both peers replicate, witho
…[truncated]
- 2026-07-04 context hints: 8 recorded with plan
- 2026-07-04 context hints: crates/vals-cluster/src/antientropy.rs — reconcile_with (tree descent + keep()), reconcile_scoped/_zone, LocalPeer, tests incl. CountingPeer; crates/vals-cluster/src/replica.rs — VersionIndex (lea
…[truncated]
- 2026-07-05 implemented: replica-set-grouped leaf accumulators (`VersionIndex.leaves: HashMap<Box<[NodeId]>, Box<[u64]>>`) + optional placement fn on `VersionedStore` (`PlacementFn = Arc<dyn Fn + Send+Sync>`, `set_placement`/`invalidate_leaf_seed`/`node_id`). `scoped_leaf_digests` gained a `counterpart: Option<NodeId>` (folds only groups holding both own node and the counterpart when placement wired; unscoped otherwise). Threaded a `requester` node id through `AePeer::tree`, `Request/OwnedRequest::SyncTree` (wire + roundtrip test + wire_fuzz), net server (serves `Some(requester)`) and net client, and `reconcile_with`/`reconcile_scoped`/`reconcile_scoped_zone`/`sweep`. `sweep_loop` wires the per-zone placement (default → `ring.replicas`; named → `ring.replicas_in_zone` with sorted range token) from a per-pass ring snapshot and re-seeds (`invalidate_leaf_seed`) on a placement-fingerprint change (`cleanup_cfg`) tracked by a new `last_placement_cfg`.
- 2026-07-05 verified (no SPDK hardware — in-process stand-ins per the plan): new tests — `converged_rf_lt_n_pair_examines_zero_digests` (CountingPeer `digest_entries == 0` and `applied == 0` for a converged RF=2/3-node pair; also asserts the unscoped roots *differ* so it kills the fix if reverted), `scoped_reconcile_still_heals_shared_divergence` (one shared-key divergence heals; ≤32 digests examined ⇒ hierarchical localization preserved), `placement_change_reseeds_without_stale_group_residue` (post-ring-change reseed: unscoped fold matches from-scratch recompute and a peer-scoped fold matches a scoped recompute). Full suite green: `cargo test --workspace` (0 failures), `cargo build --workspace --tests`, existing AE/replica/wire/wire_fuzz/net tests unchanged. Clippy: no new warnings (added `PlacementFn`/`LeafGroups` type aliases to avoid complex-type lint). Docs: updated `antientropy.rs`/`replica.rs` module docs and `docs/algorithms.md` §5 with the intersection-scoping + reseed story. Box measurement of the ~68k→0 digests/s deferred to a hardware-validation task.
- 2026-07-04 implementer report: Implemented task 0015: anti-entropy now scopes its Merkle leaf comparison to the keyspace both peers replicate under RF<N, via placement-intersection-aware (replica-set-grouped) leaf accumulators.  ##
…[truncated]
- 2026-07-04 review tier: high-powered — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change implements placement-intersection-aware (replica-set-grouped) Merkle leaf accumulators and scopes the anti-entropy tree exchange to the keyspace both peers replicate under RF<N. It threads 
…[truncated]
- 2026-07-04 revision: Brought docs/formats.md §1 (Wire protocol) current with wire.rs — targeted edits only, no rewrite. Every touched row was cross-checked against the actual encoders/decoders and the roundtrip tests. 
…[truncated]
- 2026-07-04 review (Claude): accept — The only substantive change since the prior (accepted) review is a docs/formats.md §1 update: it documents the in-scope SyncTree `opt<u32> requester` wire field for task 0015 and brings the surroundi
…[truncated]
- 2026-07-04 decision: accept — commit: ae: scope Merkle comparison to the shared keyspace under RF<N (task 0015)  Replica-set-grouped leaf accumulators (VersionIndex.leaves keyed by the key's sorted replica set) + an optional per-zone plac
…[truncated]
- 2026-07-04 usage: 163,966 tok (in 534, out 163,432, cache_r 39,654,882, cache_w 766,783) · cost n/a (unpriced)
  implementer: 137,989 tok (in 434, out 137,555, cache_r 36,207,852, cache_w 519,925) · cost n/a (unpriced)
  coordinator: 13,649 tok (in 46, out 13,603, cache_r 2,396,577, cache_w 130,035) · cost n/a (unpriced)
  reviewer:Claude: 12,328 tok (in 54, out 12,274, cache_r 1,050,453, cache_w 116,823) · cost n/a (unpriced)

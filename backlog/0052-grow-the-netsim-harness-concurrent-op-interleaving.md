---
id: "0052"
title: 'Grow the netsim harness: concurrent-op interleaving, crash-with-state-loss, epoch/membership invariants'
status: done
priority: 4
created: "2026-07-04"
updated: "2026-07-04"
depends_on:
    - "0046"
spec_refs: []
---

## Description
Task 0046 landed the first slice of the deterministic network-simulation harness (`crates/vals-cluster/tests/netsim.rs`): seeded SplitMix64 faults (request/reply drop, delay, duplication, one-way partitions, crash windows) on the coordinator→replica leg, a wall-clock-free injected HLC clock, and two invariants (LWW convergence after a healed partition + AE sweep; no lost majority-acked writes) across 80 seeds with `VALS_SIM_SEED` replay.

Grow it along the axes the 0046 slice deliberately deferred:
- **Concurrent ops / interleaving control**: issue multiple coordinator ops concurrently on the single-threaded executor (join set) so delayed legs interleave across ops, not just within one write's fanout; keep RNG consumption deterministic (roll fault decisions synchronously at first poll, as the current SimNet does).
- **Crash with state loss / restart**: model a node losing un-synced state on crash (or a fresh empty replica rejoining) and assert AE re-populates it without violating the acked-writes invariant.
- **Epoch/membership safety**: drive a ring config change (add/remove node, epoch bump) mid-schedule and assert stale-coordinator refusal / no acked write lost across the reshape.
- Optionally: sloppy-quorum writes + hinted handoff under the same schedules; multi-zone.

## Acceptance criteria
- At least two of the axes above simulated deterministically (seeded, replayable, no wall clock), each with an asserted invariant across many seeds.
- Existing netsim tests stay green and fast (<~30s total).

## Acceptance criteria

## Plan

Grow `crates/vals-cluster/tests/netsim.rs` along all three deferred axes, reusing the existing seeded SplitMix64 RNG, injected logical clock, SimNet fault transport, heal + all-pairs scoped AE sweep, and seed-replay (`VALS_SIM_SEED`) machinery. Keep the two existing tests untouched and green.

**Axis 1 — concurrent-op interleaving (`netsim_concurrent_ops_many_seeds`)**
- New scenario fn: same 3-node cluster + fault schedule, but ops are issued in RNG-sized batches (1..=4) of boxed write/delete futures driven together via `vals_rt::join_all`, so delayed legs interleave ACROSS ops on the single-threaded executor. Advance the logical clock once per batch (HLC logical counter disambiguates same-ms versions; every op still gets a unique, strictly-ordered version in stamp order).
- RNG determinism is preserved by the existing SimNet design (fault decisions rolled synchronously at first poll) — first polls happen in join_all's deterministic order.
- Record acked writes from join_all's in-order results (keep the max-version-per-key `record` logic; with concurrent ops the guard now genuinely bites). Invariants after heal + AE: convergence + no lost majority-acked write, across many seeds.

**Axis 2 — crash with state loss / empty rejoin (`netsim_crash_state_loss_many_seeds`)**
- Per scenario the RNG designates exactly ONE loss node and 1–2 wipe points (op indices). At a wipe point the driver replaces that node's store with a fresh empty replica (`*sim.store(n).borrow_mut() = replica(n)`) — a crash that lost all un-synced state, rejoining empty. Optionally precede the wipe with a short `down` window from the existing event machinery.
- Only one node is ever wiped per scenario, so every majority ack (W=2 of RF=3) survives on ≥1 never-wiped replica — the invariant "AE re-populates the wiped node without violating acked writes" is sound by construction. Assert full 3-way convergence (the wiped node ends identical to the others) + no lost acked writes.

**Axis 3 — epoch/membership reshape (`netsim_epoch_reshape_many_seeds`)**
- Extend SimNet with an authoritative config: `cluster_epoch: Cell<u64>`, authoritative member list (RefCell<RingView>-style), a `route_epoch: Cell<u64>` captured via `Transport::set_route_epoch`, and a stale-refusal counter. `apply` refuses with `TransportError::StaleEpoch(cluster_epoch)` when the coordinator's stamped epoch is older (nothing applied — the healthy stale-coordinator refusal). `members` serves the authoritative config-bearing view (epoch, vnodes=64, rf=3, members) so `refresh_ring` adopts it; keep control-plane reads fault-free as today.
- Scenario: LocalCluster hosts 4 stores; initial authoritative view is epoch 1 with members {1,2,3}; coordinator adopts it at start (refresh_ring). At an RNG-chosen op index in [10, N_OPS-10] the driver bumps to epoch 2 / members {1,2,3,4} mid-schedule (faults stay active). The coordinator's default auto-refresh turns the refusal into refresh+retry.
- Invariants: (a) stale-refusal counter > 0 (refusal path genuinely exercised) and post-run `coord.ring_epoch() == 2`; (b) OWNERSHIP-SCOPED convergence under the final adopted ring (clone `coord.ring()` after the run): for every key in the union of dumps, all `ring.replicas(key)` owners hold the identical (version, value/tombstone) entry; (c) no acked write lost: every majority-acked write present on all final owners at >= its acked version (safe across a single 3→4 reshape at RF=3/W=2: every majority of the old owner set intersects the new owner set). Use the final ring for the AE sweep's scoping too, all ordered pairs over nodes 1..=4.

**Cross-cutting**
- Fold the new scenarios into `netsim_deterministic_replay` (or add per-axis replay assertions) so a seed replays byte-identically on every axis.
- Update the module doc (scope section) — these axes are no longer "future growth".
- Distinct SEED_BASE-style offsets per axis so axes explore independent schedules; VALS_SIM_SEED replay support for each new test.
- Budget: existing suite runs 84 scenarios in ~2.5s debug; use ~40–60 seeds per new axis to stay well under the ~30s cap. Verify with `cargo test -p vals-cluster --test netsim` (+ a release run), plus `cargo test -p vals-cluster` for no regressions and clippy on the touched crate.

### Starting points
- crates/vals-cluster/tests/netsim.rs — the whole harness to extend (SimNet, schedule(), run_scenario(), pull(), dump())
- crates/vals-client/src/coord/mod.rs:1216-1276 refresh_ring/ring_epoch — adopts config-bearing RingView (vnodes!=0), stamps epoch via Transport::set_route_epoch
- Transport::set_route_epoch default no-op hook (coord/mod.rs ~line 174); TransportError::StaleEpoch(u64) -> CoordError::StaleRing -> auto refresh+retry via retry_stale/adopt_from_any_member
- RingView { epoch, vnodes, rf, members: Vec<(NodeId,u32)>, addrs } — vnodes=0 means members-only; use vnodes=64, rf=3 for authoritative views
- vals_rt::join_all(Vec<F>) for concurrent ops; box the mixed write/delete futures as Pin<Box<dyn Future<Output=Result<Version,CoordError>>>>
- LocalCluster::store(node) -> Option<&RefCell<VersionedStore<D>>> — state loss = *store.borrow_mut() = replica(node)
- reconcile_scoped keeps only keys where ring.replicas(key).contains(&local_node) — ownership-scoped convergence must use the final adopted ring (coord.ring().clone())
- timing: existing 2 tests = ~2.5s debug / 0.13s release for 84 scenarios

## Work log
- 2026-07-04 plan: Grow `crates/vals-cluster/tests/netsim.rs` along all three deferred axes, reusing the existing seeded SplitMix64 RNG, injected logical clock, SimNet fault transport, heal + all-pairs scoped AE sweep, 
…[truncated]
- 2026-07-04 context hints: 8 recorded with plan
- 2026-07-04 context hints: crates/vals-cluster/tests/netsim.rs — the whole harness to extend (SimNet, schedule(), faults_at(), run_scenario(), pull(), dump(), record(), seed_for()); crates/vals-client/src/coord/mod.rs:1216-12
…[truncated]
- 2026-07-04 implementer report: Grew the deterministic netsim harness (`crates/vals-cluster/tests/netsim.rs`) along all three deferred axes from task 0052. Existing tests untouched and green; whole file runs 5 tests in ~4.5s debug /
…[truncated]
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change grows the deterministic netsim harness along all three deferred axes (concurrent-op interleaving via join_all, crash-with-state-loss, and epoch/membership reshape) — exceeding the "at lea
…[truncated]
- 2026-07-04 decision: accept — commit: test(cluster): grow netsim harness — concurrent-op interleaving (join_all batches), crash-with-state-loss/empty-rejoin, and epoch/membership reshape with stale-coordinator refusal + ownership-scoped
…[truncated]
- 2026-07-04 usage: 58,674 tok (in 166, out 58,508, cache_r 3,311,642, cache_w 217,161) · cost n/a (unpriced)
  implementer: 37,306 tok (in 90, out 37,216, cache_r 2,239,931, cache_w 80,544) · cost n/a (unpriced)
  coordinator: 16,460 tok (in 50, out 16,410, cache_r 814,650, cache_w 104,359) · cost n/a (unpriced)
  reviewer:Claude: 4,908 tok (in 26, out 4,882, cache_r 257,061, cache_w 32,258) · cost n/a (unpriced)

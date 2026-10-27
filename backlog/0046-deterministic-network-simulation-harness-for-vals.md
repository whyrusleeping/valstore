---
id: "0046"
title: Deterministic network simulation harness for vals-cluster
status: done
priority: 4
created: "2026-07-04"
updated: "2026-07-04"
depends_on: []
spec_refs: []
---

## Description
Split out of task 0011 (adversarial testing remainder). Today's fault injection is a `set_down` bool on the test transport — no message-level adversity, no interleaving control, no reproducible schedules.

Build a deterministic in-process network simulation for cluster tests:
- A simulated transport implementing the cluster's transport trait(s), with a seeded (SplitMix64, consistent with wire_fuzz/record_fuzz) fault schedule: message drop, delay/reorder, duplication, one-way partitions, and node crash/restart events.
- Virtual time (no wall-clock sleeps) so schedules are fully deterministic and fast; a failing seed reproduces exactly.
- Cluster-level invariant checks run under the simulation: LWW convergence after healing (anti-entropy), no lost acknowledged writes at the configured consistency level, epoch/membership safety across partitions.
- Start with the smallest useful slice (drop + delay on the existing mock transport, one convergence property) and grow.

## Acceptance criteria
- A seeded, deterministic simulation harness exists in-tree (no wall-clock dependence) driving at least one multi-node cluster scenario with message drop/delay/partition.
- At least one cluster invariant (e.g. AE convergence after a healed partition) is asserted under randomized fault schedules across many seeds.
- A failing seed is replayable from the test output.

## Acceptance criteria

## Plan

Build the smallest-useful-slice deterministic network simulation harness as an integration test in `crates/vals-cluster/tests/netsim.rs` (precedent: `tests/wire_fuzz.rs`, which already establishes the in-tree seeded SplitMix64 pattern with replayable seeds).

Design:

1. **SimNet transport** — a struct wrapping `LocalCluster<MemBlockDevice>` and implementing `vals_cluster::Transport` (same pattern as `SlowCluster`/`CountingCluster` in `coord/tests.rs`). It holds:
   - a seeded SplitMix64 RNG (RefCell), copied verbatim from wire_fuzz.rs;
   - fault state: per-node `down` set (crash windows), per-node one-way partition flags (`drop_requests_to`, `drop_replies_from`), and probabilistic per-message faults: drop-request (return `TransportError::Unavailable` without touching the store), drop-reply (apply to the store, then return `Unavailable` — the adversarial "replica applied but coordinator saw failure" case), delay (0..k `vals_rt::yield_now()`s before executing, so a quorum can complete and cancel the straggler leg — genuine divergence), and duplication (apply twice; LWW must make it idempotent);
   - a `heal()` method clearing all fault state (probabilities to 0, partitions/down cleared).
   Implement `apply`, `read`, `members`, `zones` (the required trait methods — the coordinator's single-write/read path uses these; defaulted methods like `digest` route through `read` anyway). No wall-clock anywhere: "time" is poll/yield ordering plus a driver-side logical tick.

2. **Deterministic clock** — build the `Coordinator` with `.with_clock(...)` returning a monotonically incremented logical ms counter (shared `Cell<u64>`/`Rc`), so HLC versions are fully seed-deterministic (no `physical_now_ms`).

3. **Scenario driver** (`fn run_scenario(seed: u64) -> per-node final entries`):
   - 3-node cluster, RF=3, R=2 (mem-backed `VersionedStore`s, same `replica()` recipe as coord/tests.rs), ring `Ring::with_vnodes(3, 64)`.
   - From the seed, generate a fault schedule: random per-message drop/delay/dup probabilities plus down-windows and one-way-partition windows over the op index (virtual time = op index; driver flips SimNet state between ops).
   - Drive ~150-250 sequential ops (writes + some deletes over a small key universe, e.g. 48 keys, so overwrites contend) via `coordinator.write` / `coordinator.delete`, each under `vals_rt::block_on(f, || {})`. Record an **acked-writes ledger**: on `Ok(version)`, record (key → highest acked version + value/tombstone). Errors during the fault phase are expected and ignored (only acks carry the guarantee).
   - **Heal**: `simnet.heal()`, clear down/partitions.
   - **AE sweep**: for all ordered node pairs, run `antientropy::reconcile_scoped(local_cell, &LocalPeer(&peer_store), DEFAULT_BUCKETS, &ring, local, peer, resume)` looping on the resume cursor until `None` (the pub API is pass-bounded; the `#[cfg(test)]` unscoped `reconcile` is NOT visible to integration tests). Repeat all-pairs rounds until stores are identical or a small round cap, then assert.

4. **Invariants** (assert with `panic!("[netsim] seed {seed}: ...")` so any failure names its seed):
   - **Convergence**: all three stores' `entries()` dumps are identical (keys, versions, values, tombstones).
   - **No lost acknowledged writes**: for every key in the ledger, the converged store holds an entry with version >= the max acked version; if equal, the value (or tombstone-ness) matches the acked write.
   - **Determinism/replay**: run the same seed twice; final per-node dumps must be byte-identical.

5. **Test entry points**:
   - `#[test] netsim_convergence_many_seeds`: run a fixed seed range (e.g. 0..100 derived via a base seed) — honors `VALS_SIM_SEED` env var to replay exactly one seed (documented in the file header, mirroring wire_fuzz's replay story).
   - `#[test] netsim_deterministic_replay`: same-seed-twice identity for a couple of seeds.
   - Driver code uses BTreeMap/sorted iteration only (no HashMap order dependence).

6. Verify: `cargo test -p vals-cluster --test netsim` green and fast (<~30s), `cargo clippy -p vals-cluster --all-targets` introduces no new warnings, full `cargo test -p vals-cluster` still green.

Scope note: message-level adversity on the *coordinator→replica* transport plus AE-driven convergence is the deliberate first slice per the task ("start with the smallest useful slice ... and grow"); deeper interleaving control, node crash/restart with state loss, and more invariants (epoch/membership safety) remain future growth and can be follow-on tasks.

### Starting points
- crates/vals-cluster/tests/wire_fuzz.rs — SplitMix64 Rng struct to copy; seed-replay reporting pattern
- crates/vals-cluster/src/coord/tests.rs — SlowCluster/CountingCluster show the wrap-LocalCluster Transport pattern; `replica()` + `cluster()` builders; `run()` = vals_rt::block_on(f, || {})
- crates/vals-cluster/src/coord/local_cluster.rs — LocalCluster: insert/store/set_down; Transport impl
- crates/vals-cluster/src/antientropy.rs:273 reconcile_scoped (pub, pass-bounded, returns resume cursor; loop until None); LocalPeer at :810; note plain `reconcile` is #[cfg(test)] — unavailable to integration tests
- crates/vals-client/src/coord/mod.rs:174 Transport trait (required: apply/read/members/zones; digest/read_values/apply_batch have defaults)
- Coordinator::with_clock (coord/mod.rs ~:1099) for a deterministic HLC clock; Coordinator::new(0, ring, r, transport)
- vals_cluster exports: LocalCluster, Coordinator, Transport, TransportError, VersionedStore, Ring, DEFAULT_ZONE, antientropy (doc-hidden pub), hlc::Version; replica.rs:1559 VersionedStore::entries() for full dumps

## Work log
- 2026-07-04 plan: Build the smallest-useful-slice deterministic network simulation harness as an integration test in `crates/vals-cluster/tests/netsim.rs` (precedent: `tests/wire_fuzz.rs`, which already establishes the
…[truncated]
- 2026-07-04 context hints: 7 recorded with plan
- 2026-07-04 context hints: crates/vals-cluster/tests/wire_fuzz.rs — SplitMix64 Rng struct to copy; seed-replay reporting pattern; crates/vals-cluster/src/coord/tests.rs — SlowCluster/CountingCluster wrap-LocalCluster Transp
…[truncated]
- 2026-07-04 implementer report: ## Summary  Built a deterministic, wall-clock-free network-simulation harness for the vals-cluster coordinator and asserted cluster invariants under randomized fault schedules.  ### New file: `crates/
…[truncated]
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change delivers exactly the smallest-useful-slice deterministic network-simulation harness the task asks for. `crates/vals-cluster/tests/netsim.rs` is a seeded (SplitMix64), wall-clock-free, singl
…[truncated]
- 2026-07-04 decision: accept — commit: test(cluster): deterministic seeded network-simulation harness (netsim) — SimNet faults (drop/delay/dup/partition/crash) over LocalCluster, wall-clock-free HLC via with_clock, AE-convergence + no-lo
…[truncated]
- 2026-07-04 usage: 61,934 tok (in 208, out 61,726, cache_r 4,978,787, cache_w 230,344) · cost n/a (unpriced)
  implementer: 37,283 tok (in 118, out 37,165, cache_r 3,499,017, cache_w 91,634) · cost n/a (unpriced)
  coordinator: 17,273 tok (in 54, out 17,219, cache_r 1,139,083, cache_w 109,691) · cost n/a (unpriced)
  reviewer:Claude: 7,378 tok (in 36, out 7,342, cache_r 340,687, cache_w 29,019) · cost n/a (unpriced)

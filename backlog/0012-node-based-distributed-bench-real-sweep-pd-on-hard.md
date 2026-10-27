---
id: "0012"
title: Node-based distributed bench (real sweep + PD on hardware)
status: done
priority: 3
created: "2026-07-02"
updated: "2026-07-06"
depends_on: []
spec_refs: []
---

## Description
**UNBLOCKED (2026-07-05)**: owner approved tearing down the live 4-node valsd cluster on oldbox; the four vfio-bound NVMe drives are free to claim. Box access verified: `ssh oldbox` (repo checkout at `~/vals`, passwordless sudo). First box session must gracefully stop the old cluster (valsd pd + node1–4, root PIDs up since Jul 03) before claiming the drives.

BACKLOG.md §Hardware/benchmarking: every distributed bench (`distspdk`/`distspdkmc`/`distmdmc`/`distresil`) drives passive `ShardedServer`s with no anti-entropy sweep, so over-replication cleanup, eager rebalancing, per-zone AE, and the liveness lease are only validated off-hardware. A bench on `Node::serve`/`serve_zoned` (real sweep loops + a PD pushing membership) would hardware-exercise the full healing/rebalancing path. (The `rsweep.sh`/kvchurn recipes already do part of this ad hoc — consider promoting them.)

## Acceptance criteria
- A repeatable bench mode drives live `Node`s (sweep + compaction + PD) on the SPDK box and reports healing metrics (ae counters, cleanup, lease events) alongside throughput.
- Documented run recipe (plans/ runbook or bench README).

## Acceptance criteria

## Plan

Goal: a repeatable vals-bench workload ("distnode") that drives LIVE `Node`s — real anti-entropy sweep loops, background compaction, the liveness lease, and a PdDaemon pushing membership — instead of passive ShardedServers, then run it on the SPDK box and document the recipe. This hardware-exercises the full healing/rebalancing path (over-replication cleanup, eager rebalancing, per-zone AE, liveness lease) and reports healing metrics (observe counters) alongside throughput.

Prior box sessions already tore down the old cluster (task 0056) and validated the daemon liveness path (0041/0072); the four vfio drives (0000:05:00.0 0000:07:00.0 0000:c1:00.0 0000:c3:00.0) are free.

IMPLEMENTATION (crates/vals-bench/src/main.rs):

1. New `workload_distnode(backend, path, n, vsize)`, registered in main()'s dispatch. Two flavors sharing one generic core (or a small amount of duplication if the generics fight — match existing file style):
   - `mem` (and ideally `file`): MemZoneStore/FileZoneStore + MemLivenessLog, for off-hardware smoke testing — must run green locally.
   - `spdk` (feature-gated): `path` = comma PCIe list (≥3 addrs). Mirror valsd's run_node.rs spdk arm: SpdkZoneStore::open(node_id, controller, SpdkZoneConfig{super_blocks:16, region_blocks from VALS_DISTNODE_REGION_MIB (default ~2048), seg_blocks, reserved 0}, format=true), watermark on-device via provider.liveness_log().

2. Topology: initial cluster = first (len-1) drives as nodes 1..K (min 2), LAST drive reserved for a JOINER node. Each node: zone0 = provider.open(DEFAULT_ZONE, Unsorted); Node::bind(); `Node::serve_zoned_with_liveness(id, listener, zone0, provider, Catalog::new(), boot_ring, peers, sweep_every, grace_ms, liveness_log, None)`. Knobs via env: VALS_DISTNODE_SWEEP_MS (default 2000), VALS_DISTNODE_GRACE_MS (default 60000), VALS_DISTNODE_CHURN_SECS (default 20). Boot ring/peers = initial members only (mirror run_node.rs boot-ring shape).
   PD: `PdDaemon::spawn_single` (mem Engine) with RingConfig{rf:3 (cap at #initial nodes), default vnodes} and data_nodes = initial members' (id, [addr]); seed membership the way run_pd.rs does. Wait (poll observe::snapshot, ~30s timeout) for unfences ≥ K and ring_adoptions ≥ K before benching; report boot lease events.

3. Named zone for per-zone AE / eager rebalancing: pd.create_zone (one unsorted zone, rf=3) before loading; write a small slice (~n/10 keys) into it — crib the zone write/read path from workload_distzone. (If the coordinator-level zone plumbing turns out disproportionate, transport-level apply like distzone uses is fine.)

4. Phases — each prints throughput, and per-phase observe-counter deltas:
   - P1 load: n keys write_tier Quorum(2) via a Coordinator over NetTransport (adopt ring from a member first) → ops/s.
   - P2 read-verify all n (R=2) → ops/s, mismatches.
   - P3 outage-heal (sweep, NOT hint replay): transport().set_down(node1, true); write m = max(n/4, 2000) NEW keys at Quorum(2); report hints_recorded but DO NOT replay_hints; set node1 up; poll (250ms, timeout ~15 sweep periods) until all m keys read back from node1 ALONE (a verify Coordinator with R=1 and every other node set_down). Report time-to-heal, heal keys/s, deltas of sweep_passes / ae_digests_examined / ae_value_reads / ae_entries_applied.
   - P4 grow + rebalance + cleanup: spawn the joiner (pristine, boot ring = self); pd.add_node_addressed(J, 1, vec![addr]). Poll until a coordinator adopt_from_any_member sees J in the ring; compute which existing keys J now owns from the adopted ring; poll-verify those keys serve from J alone (R=1, others down) — this exercises eager rebalancing incl. lazy materialization of the named zone on J (verify a zone key too). Report time-to-rebalance, ae deltas, peers_added, and over-replication-cleanup evidence: live_keys gauge peak vs settled-back value plus cleanup_errors/cleanup_quiesce_timeouts (expect 0).
   - P5 steady churn: VALS_DISTNODE_CHURN_SECS of mixed overwrite Quorum(2) + read R=2 against the growed cluster while sweeps run → ops/s, mismatches.
   - Final: full healing-counter delta table (sweep_passes, ae_*, cleanup_*, compactions, segments_freed, fence_trips, unfences, wipes, ring/catalog adoptions, peers_added, pd_repushes, pd_push_failures, hints_recorded, records_written) and a `FLOWS: OK/CHECK` verdict (0 mismatches, heal+rebalance within timeout, unfences ≥ K+1, wipes == 0, cleanup_errors == 0). Timeouts print CHECK, not panic.
   - Teardown: drop coordinators/transports FIRST, then NodeHandle::shutdown each node, then pd.shutdown() — exit 0 with clean SPDK detach (see distspdk's teardown comment).

5. Runbook `plans/distnode-box-bench.md`: box prereqs (ssh oldbox, the 4 vfio PCIe addrs, verify no valsd/vrun running, no /var/run/dpdk|/dev/hugepages residue), rsync tree, build (`VALS_SPDK_LINK=1 cargo build --release -p vals-bench --features spdk`), run command (sudo + LD_LIBRARY_PATH=third_party/spdk/dpdk/build/lib, `vals-bench spdk distnode <n> <vsize> <pci-list>`), env knobs, what PASS looks like, and leave-the-box-clean checks.

VERIFICATION (implementer): cargo build + clippy clean; `cargo run -p vals-bench --release -- mem distnode 20000 64` passes all phases (FLOWS OK) locally; existing tests untouched. BOX RUN (coordinator, after review): rsync to oldbox, build with spdk, run distnode on the 4 drives at a real size (e.g. n=2,000,000 vsize=128), record output + numbers in bench/distnode-box-notes.md, leave the box clean.

### Starting points
- crates/vals-bench/src/main.rs — workload_distspdk (~L1149) for cluster/teardown shape, workload_distzone (~L2621) for SpdkZoneStore + zone writes, workload_distresil (~L1679) for 1-drive-per-node RF=3; main() dispatch at end of file
- crates/vals-server/src/run_node.rs ~L280-350 — the exact SpdkZoneStore::open + Node::serve_zoned_with_liveness wiring to mirror (provider.liveness_log(), empty boot Catalog)
- crates/vals-server/src/run_pd.rs — PdDaemon spawn + membership seeding to mirror
- crates/vals-cluster/src/pd.rs — PdDaemon::spawn_single (~L918), add_node_addressed (~L1529: the grow-the-cluster entry point), RingConfig (~L41)
- crates/vals-cluster/src/node.rs — serve_zoned_with_liveness (~L574)
- crates/vals-client/src/observe.rs — counters!{...} list (sweep_passes, ae_*, cleanup_*, fence_trips/unfences/wipes, ring_adoptions, peers_added, pd_repushes, hints_recorded) + gauges().live_keys; snapshot() for deltas
- crates/vals-cluster/src/coord/tests.rs ~L2709 adopt_from_any_member usage
- bench/spdk-liveness-box-notes.md — box conventions (sudo, LD_LIBRARY_PATH, drive list, hygiene)

## Work log
- 2026-07-06 plan: Goal: a repeatable vals-bench workload ("distnode") that drives LIVE `Node`s — real anti-entropy sweep loops, background compaction, the liveness lease, and a PdDaemon pushing membership — instead
…[truncated]
- 2026-07-06 context hints: 8 recorded with plan
- 2026-07-06 context hints: crates/vals-bench/src/main.rs — workload_distspdk (~L1149) cluster/teardown shape; workload_distzone (~L2621) SpdkZoneStore + zone writes; workload_distresil (~L1679) 1-drive-per-node RF=3; main() d
…[truncated]
- 2026-07-06 implementer report: Added a repeatable `distnode` bench mode to `vals-bench` that drives LIVE `Node`s (real anti-entropy sweep loops, background compaction, the wipe-before-rejoin liveness lease, and a `PdDaemon` pushing
…[truncated]
- 2026-07-06 review tier: single-opus — reviewers: Claude
- 2026-07-06 review (Claude): accept — The change adds a repeatable `distnode` bench mode to vals-bench that drives LIVE Nodes (real anti-entropy sweeps, background compaction, the wipe-before-rejoin liveness lease, and a PdDaemon pushing 
…[truncated]
- 2026-07-06 revision: Addressed both review items in `distnode_core` (crates/vals-bench/src/main.rs); build + clippy clean, mem smoke still FLOWS: OK.  ## Fix 1 — P4 live_keys peak now sampled from the join instant - Int
…[truncated]
- 2026-07-06 revision: Made the heal/rebalance verify windows env-tunable and documented the AE-budget sizing story — no product code changed.  ## crates/vals-bench/src/main.rs (distnode_core) - Added two env knobs: `VALS
…[truncated]
- 2026-07-06 review (Claude): accept — The revision resolves both prior findings and improves the change further. `peak_live` is now a running max bumped on every P4 poll tick from the moment the joiner is advertised (correct mechanism for
…[truncated]
- 2026-07-06 decision: accept — commit: bench: distnode — live-Node distributed bench (real sweep + compaction + liveness lease + PD) validated on the SPDK box (task 0012)  New vals-bench workload driving live Node::serve_zoned_with_liven
…[truncated]
- 2026-07-06 usage: 127,060 tok (in 414, out 126,646, cache_r 21,035,359, cache_w 1,918,938) · cost n/a (unpriced)
  implementer: 77,282 tok (in 236, out 77,046, cache_r 12,746,754, cache_w 494,872) · cost n/a (unpriced)
  coordinator: 39,328 tok (in 120, out 39,208, cache_r 7,369,613, cache_w 1,320,926) · cost n/a (unpriced)
  reviewer:Claude: 10,450 tok (in 58, out 10,392, cache_r 918,992, cache_w 103,140) · cost n/a (unpriced)

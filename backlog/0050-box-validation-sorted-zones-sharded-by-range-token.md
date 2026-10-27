---
id: "0050"
title: 'Box validation: sorted zones sharded by range token on the SPDK multi-drive path (~k× scaling)'
status: done
priority: 4
created: "2026-07-04"
updated: "2026-07-07"
depends_on:
    - "0029"
spec_refs: []
---

## Description
Task 0029 landed **sorted zones on the sharded (multi-drive / JBOD) path**, sharded
intra-node by range token (key `k` on shard `hash64(range_token(k)) % shards`,
matching the client's `data_shard`/`scan_shard` routing), with a shard-aware
anti-entropy sweep that hands split-orphaned keys off to their newly-owning shard
(including a sibling shard of the same node) on a runtime range-map split. Coverage
is off-hardware (MemDriveSet + mem-backed nodes); the SPDK hardware run and the
throughput-scaling measurement are this task.

On an SPDK box, with a multi-drive (JBOD, multiple `listen`/`pci`) node serving a
sorted zone with `SHARDS = k`:
- **Throughput scaling (the acceptance target):** with the zone split into `>= k`
  ranges spread across disjoint shards, drive point-read / point-write / scan load
  across the ranges and confirm the node sustains **~k×** the single-core (single-
  shard) rate on disjoint ranges (no single serving core saturated). Compare against
  a 1-shard sorted-zone baseline on the same drives.
- **Distributed scan spread:** a `scan_in` over the whole keyspace fans out across
  the shards (each range's scan hits the one shard its token routes to); result is
  complete and globally ordered.
- **Split migration on hardware:** push a `SetRangeMaps` split whose new token
  hashes to a different shard; verify the moved keys migrate to the new shard (raw
  per-shard scan), the old shard releases them, coordinator reads/scans stay
  complete, and nothing is lost — across nodes as well as sibling shards.
- **Restart / boot-reopen:** the sorted zone reopens from each partition's region
  table before any catalog re-push; keys are served on the shard their range token
  maps to.
- **Liveness variant (`gc_grace > 0`):** per-shard watermarks persist; node unfences
  normally with a sorted zone present.

## Acceptance criteria
- ~k× point-op and scan throughput on disjoint ranges vs the single-shard baseline,
  observed on real hardware; distributed `scan_in` spreads across shards; split
  migration loses nothing. Any deviation filed as a bug task.

**UNBLOCKED (2026-07-05)**: owner approved tearing down the live 4-node valsd cluster on oldbox; the four vfio-bound NVMe drives are free to claim. Box access verified: `ssh oldbox` (repo checkout at `~/vals`, passwordless sudo). First box session must gracefully stop the old cluster (valsd pd + node1–4, root PIDs up since Jul 03) before claiming the drives.

## Plan

## Goal
On the SPDK box (oldbox, 4 vfio drives), validate sorted zones sharded intra-node by range token (task 0029): (a) ~k× point-write / point-read / scan throughput with SHARDS=k vs a 1-shard baseline on the same hardware; (b) distributed scan_in spreads across shards, complete + globally ordered; (c) SetRangeMaps split migration loses nothing (across sibling shards); (d) restart/boot-reopen serves keys on the token-mapped shard; (e) liveness variant gc_grace>0 — per-shard watermarks persist, JBOD node unfences ×2 (on-box re-verify of the 0079/0080/0081 fixes, folded in from 0047 item 7).

## Part 1 — bench tooling gap (code change, implementer)
`workload_distsorted` hardcodes RF=3/R=2 and has no point-read phase; with only 4 drives it cannot put k>1 shards on one node at RF=3 (needs RF*dpn drives). Generalize minimally, keeping defaults identical so existing recipes (0066 A/B) are unchanged:
1. Env knobs `VALS_DS_RF` (default 3) and `VALS_DS_R` (default 2); relax the drive-count assert to `pcis.len() % dpn == 0 && pcis.len()/dpn >= RF` (RF=1 precedent: `workload_distmd` runs a 1-node RF=1 ring fine).
2. Add a POINT-READ phase between WRITE and the full-SCAN check: ncoord coordinator threads × CONC in-flight `coord.read_in(ZID, key(i))` over all n keys (strided by ncoord like the write phase), verifying the embedded LE index in the first 8 value bytes; report ops/s + errors + mismatches (mismatches ⇒ *** INVALID ***).
3. Print RF/R in the header banner. `cargo check --features spdk` (and workspace build/test) green.

## Part 2 — box session (coordinator-run, over ssh)
Prereqs verified: box clean (no valsd/vals-bench/spdk residue), repo checkout at ~/vals one commit behind (rsync first), 4 drives 0000:05:00.0/07/c1/c3 vfio-bound.

A. Sync + build: rsync working tree; `VALS_SPDK_LINK=1 cargo build --release -p vals-bench --features spdk` and `-p vals-server`; rebuild ~/vals-examples (kvchurn/valsq) — stale clients invalidate shard-spread results (0047 lesson).

B. Scaling A/B (distsorted, RF=1 R=1, 1 node, same drive family): baseline `dpn=1` on 1 drive (k=1) vs `dpn=2` (k=2) and `dpn=4` (k=4); n≈500k vsize=64, ncoord scaled up (4→8) until the client is not the bottleneck; ≥2 runs per point. Record WRITE / POINT-READ / RANGE-SCAN ops/s and expect ~k× vs baseline; full-SCAN correctness + per-drive distribution proves spread (acceptance item b). Any material deviation (< ~0.7·k, say) → file a bug/investigation task.

C. Split migration + restart + liveness (valsd cluster, 0047-style topology): JBOD node1 = 2 drives/2 listens, node2, node3 single-drive; sorted zone `events` rf=3, sweep=5s, gc_grace=45s. (i) load keys spanning the keyspace; pick a split key whose new range token hashes to node1's *other* shard (compute hash64 offline with the client's ring::hash64); `valsd admin split-zone events <key>`; await sweeps; verify via per-shard `admin dump` that moved keys arrived on the new shard and the old shard released them, cluster reads/scans stay complete, counts conserved. (ii) graceful restart node1 format=false: zones re-open from region tables before catalog push, keys served on the token-mapped shard, and — with non-zero watermarks on BOTH partitions — `unfenced` ×2 (the 0079 on-box re-verify). (iii) leave the box clean (graceful teardown, no dpdk/hugepage residue, drives vfio-bound).

D. Record everything in `bench/sorted-shard-scaling-box-notes.md` (methodology, exact commands, numbers, verdicts); note the k× result in the task; file follow-up tasks for any deviation; mark 0050 done and commit.

## Fallbacks
- If RF=1 trips some coordinator assumption, fall back to RF=2/R=1 with 2 nodes × dpn∈{1,2} (k=2 scaling) and note the limitation.
- If the client saturates before the node, report the ncoord-scaling curve and the per-shard busy evidence instead of a bare number.

### Starting points
- crates/vals-bench/src/main.rs: workload_distsorted ~l.3529 (RF/R consts l.3546-3547, assert l.3552, write phase l.3634, full scan l.3718, raw_shard_count l.3751, RANGE-SCAN l.3828)
- crates/vals-bench/src/main.rs: workload_distmd ~l.1913 — RF=1 ring precedent (Ring::with_vnodes(1,128), Coordinator::new(0, ring, 1, ...))
- coord.read_in(zone_id, key) — see main.rs l.1410 for usage incl. value check
- env-knob precedent: rg 'VALS_DISTNODE_' in main.rs (env::var parsing style)
- bench/b1-box-ab-notes.md — distsorted box recipe; bench/zoned-multidrive-box-notes.md — valsd JBOD harness + admin dump shard=N usage

## Work log
- 2026-07-07 plan: ## Goal On the SPDK box (oldbox, 4 vfio drives), validate sorted zones sharded intra-node by range token (task 0029): (a) ~k× point-write / point-read / scan throughput with SHARDS=k vs a 1
…[truncated]
- 2026-07-07 context hints: 5 recorded with plan
- 2026-07-07 context hints: crates/vals-bench/src/main.rs: workload_distsorted ~l.3529 (RF/R consts l.3546-3547, drive-count assert l.3552, region sizing l.3563 uses RF, write phase l.3634, full scan l.3718, RANGE-SCAN l.3828); 
…[truncated]
- 2026-07-07 implementer report: Implemented Part 1 of task 0050 — the bench tooling change to `workload_distsorted` in crates/vals-bench/src/main.rs. Scope confined to that function, its doc comment, and the distsorted mode's usag
…[truncated]
- 2026-07-07 review tier: simple (coordinator self-review)
- 2026-07-07 revision: Addressed the reviewer follow-up in crates/vals-bench/src/main.rs (workload_distsorted).  Change: added the `VALS_DS_NRANGES` env knob (parsed via `env_u64`, default 0). When 0, the range count keeps 
…[truncated]
- 2026-07-07 decision: accept — commit: bench: box-validate sorted-zone intra-node sharding on SPDK (task 0050) — distsorted gains VALS_DS_RF/VALS_DS_R/VALS_DS_NRANGES + a POINT-READ phase; k=4 scaling 2.5-3.0× in-process vs 3.8-3.9× pr
…[truncated]
- 2026-07-07 usage: 62,122 tok (in 172, out 61,950, cache_r 7,772,026, cache_w 373,433) · cost n/a (unpriced)
  coordinator: 52,496 tok (in 124, out 52,372, cache_r 7,230,924, cache_w 313,751) · cost n/a (unpriced)
  implementer: 9,626 tok (in 48, out 9,578, cache_r 541,102, cache_w 59,682) · cost n/a (unpriced)

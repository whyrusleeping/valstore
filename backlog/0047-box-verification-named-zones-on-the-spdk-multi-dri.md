---
id: "0047"
title: 'Box verification: named zones on the SPDK multi-drive (JBOD) path on hardware'
status: done
priority: 4
created: "2026-07-04"
updated: "2026-07-06"
depends_on:
    - "0039"
spec_refs: []
---

## Description
Task 0039 landed named (unsorted) zones on the `backend = spdk` multi-drive (JBOD) path — per-partition `RegionZoneStore` + `SubDev` over each partition's LBA region, wired via `Node::serve_multidrive_zoned{,_with_liveness}` and the `zone_region_mib` knob. Coverage is off-hardware (MemDriveSet); the SPDK hardware run is this task.

On the SPDK box, with a multi-drive (JBOD, multiple `listen`/`pci`) node:
- Format (`format = true`) and boot: verify the boot log reports the zoned multidrive path and `zone_region_mib` sizing; verify config validation rejects a `zone_region_mib` that doesn't fit `region_mib`.
- Push a catalog with an unsorted named zone; verify writes/reads round-trip across shards/drives and AE heals it across nodes.
- Restart WITHOUT a catalog re-push (`format = false`): every named zone boot-reopens from the per-partition region table with the right ordering before serving.
- Reopen a pre-zoned (old-format) multidrive node without reformat: must fail loud with the "no per-partition zone superblock" error, not clobber data.
- Sorted zone in the catalog: shard refuses with the clear sorted-specific error; unsorted + default zones keep serving.
- Zone drop (catalog removal): region freed and reused without data resurrection across a reboot.
- Liveness variant (`gc_grace > 0`): per-shard watermarks persist; node unfences normally.

## Acceptance criteria
- All of the above observed on real hardware, 0 errors; any deviation filed as a bug task.

**UNBLOCKED (2026-07-05)**: owner approved tearing down the live 4-node valsd cluster on oldbox; the four vfio-bound NVMe drives are free to claim. Box access verified: `ssh oldbox` (repo checkout at `~/vals`, passwordless sudo). First box session must gracefully stop the old cluster (valsd pd + node1–4, root PIDs up since Jul 03) before claiming the drives.

## Acceptance criteria

## Plan

Box-verification session on oldbox (reachable via `ssh oldbox`, repo at `~/vals`, passwordless sudo). Goal: exercise every 0047 checklist item for named zones on the SPDK multi-drive (JBOD) valsd path on real hardware, record evidence in a new `bench/zoned-multidrive-box-notes.md`, and leave the box clean. Expected deliverable is a notes file (+ possibly small doc touch-ups); NO product code change is anticipated — any deviation found on hardware is reported back (coordinator files bug tasks), not hot-patched.

IMPORTANT SCOPE CORRECTION (record it in the notes + the doc trail): the task's bullet "Sorted zone in the catalog: shard refuses with the clear sorted-specific error" is STALE — it was written when 0039 deferred sorted zones. Task 0029 (commit 37c70a4) landed sorted zones on the sharded multidrive path and removed `refuse_sorted`. So instead verify: a sorted zone in the catalog SERVES on the JBOD node (put + globally-sorted scan round-trips) while unsorted + default zones keep serving. Deep sorted validation (~k× scaling, split migration) remains task 0050 — do not absorb it.

Environment / prereqs
- Box state: verified clean right now (no valsd/vals-bench processes). Before starting confirm: `pgrep -a valsd`, `pgrep -a vals-bench`, no `/var/run/dpdk/spdk*` / `/dev/hugepages/spdk*` residue, four vfio-bound NVMe drives free: 0000:05:00.0 0000:07:00.0 0000:c1:00.0 0000:c3:00.0. NEVER touch the OS RAID nvme4/nvme5.
- Sync + build: `rsync -a --exclude target --exclude .git ~/code/vals/ oldbox:~/vals/` then on box `VALS_SPDK_LINK=1 cargo build --release -p vals-server --features spdk` (binary `valsd`). Client tooling from prior sessions (valsq, kvchurn from the sibling vals-examples repo) is already on the box — locate it (prior harness lived under `~/run/cluster/t0041/`; use a fresh `~/run/cluster/t0047/`). Daemons + `valsd admin` need `sudo env LD_LIBRARY_PATH=$PWD/third_party/spdk/dpdk/build/lib`.
- Follow the harness pattern in bench/spdk-liveness-box-notes.md Scenario B: JBOD node1 = two `listen` + two `pci` (c1 + 05), single-drive peers node2 (07) and node3 (c3), PD daemon, `backend = spdk`, no data_dir. Run with `gc_grace = 45s` (liveness on) from the start so the liveness bullet is covered in-line. rf=3 for named zones so every node holds every key (easy single-node read verification). Keep key counts modest (~50k) to avoid the known large-n teardown wedge (task 0073).

Checklist (order matters — destructive drive-zeroing goes LAST):
1. Format=true boot: capture the boot log reporting the zoned multidrive path + `zone_region_mib` sizing (run_node.rs logs "serving N SPDK partitions ... ({} MiB/zone; sorted zones sharded intra-node by range token)"). Also capture born-fenced → unfenced lines (liveness variant, per partition).
2. Config validation: a config with `zone_region_mib` too large for `region_mib` must be rejected with the clear error ("`zone_region_mib` (+ the per-partition zone superblock) must fit within `region_mib`..."). Verify on box with a deliberately bad config (fails at config load, before touching drives).
3. Unsorted named zone: create via PD (`valsd admin create-zone sessions 3 unsorted` or boot `zone =` line); write/read round-trip via valsq addressed through the cluster; confirm keys spread across the JBOD node's shards/drives (per-shard metrics e.g. vals_records_written per listen port, or shard-addressed reads). AE heal across nodes: SIGTERM node2, write fresh named-zone keys to the survivors, restart node2 format=false, wait sweeps, verify the new keys readable from node2 ALONE (r=1); record ae/hint counters.
4. Restart WITHOUT catalog re-push: restart the JBOD node with format=false and no PD zone re-push needed; expect "re-opened persisted zone <id> at boot" for every named zone on each partition BEFORE serving, correct ordering preserved (sorted zone still scans sorted); data still served.
5. Sorted zone serves (scope correction above): create `events 3 sorted`, put keys, verify a full sorted scan returns global order; unsorted + default zones unaffected.
6. Zone drop: `valsd admin drop-zone` on a filled zone → zone gone from `admin zones` and unreadable; create a new zone afterwards and confirm the allocator reuses the freed region (region-table/boot-log evidence); reboot format=false and confirm NO resurrection of the dropped zone or its data.
7. Liveness watermarks: graceful SIGTERM of the JBOD node logs one "graceful shutdown complete (engines synced, watermark ...)" per partition; restart recovers each partition's own watermark and unfences normally with named zones present (Scenario-B.2 pattern, now with named zones).
8. Old-format/blank reopen (DESTRUCTIVE — LAST): gracefully stop node1; release its drives from vfio (spdk setup.sh reset), dd-zero the superblock area of ONE of node1's two drives, rebind (setup.sh); boot node1 format=false → must fail LOUD with the "no zone superblock: drive holds a pre-zoned layout or is blank" error (zonestore.rs:893) and refuse to serve/clobber. Then reformat to restore a clean state.
9. Teardown: graceful stop of all daemons + PD, verify no dpdk/hugepage residue, drives still vfio-bound. Record everything (exact log lines, metrics, PASS/FAIL per item) in `bench/zoned-multidrive-box-notes.md`; add a short pointer in the 0047 context if a doc mentions the box validation as pending.

Report back: per-item PASS/FAIL with evidence; any deviation described precisely (I will file bug tasks); confirmation the box is clean.

### Starting points
- bench/spdk-liveness-box-notes.md — prior valsd box harness (Scenario B = JBOD node config pattern, exact log strings, sudo/LD_LIBRARY_PATH incantations)
- plans/distnode-box-bench.md — box prereqs, rsync+build recipe, drive PCI addrs, residue cleanup
- crates/vals-server/src/run_node.rs:156-260 — multidrive zoned serve path + boot log line with zone_region_mib
- crates/vals-server/src/config.rs:421-499 — zone_region_mib parse + must-fit validation error text
- crates/vals-server/src/admin.rs — admin commands: zones/create-zone/drop-zone/split-zone
- crates/vals-cluster/src/zonestore.rs:893 — 'no zone superblock: drive holds a pre-zoned layout or is blank' error
- commit 37c70a4 — sorted zones on the sharded multidrive path landed (refuse_sorted removed): the task's 'sorted refuses' bullet is stale
- bench/b1-box-ab-notes.md — setup.sh reset → dd-zero → setup.sh drive-recycling recipe

## Work log
- 2026-07-06 plan: Box-verification session on oldbox (reachable via `ssh oldbox`, repo at `~/vals`, passwordless sudo). Goal: exercise every 0047 checklist item for named zones on the SPDK multi
…[truncated]
- 2026-07-06 context hints: 8 recorded with plan
- 2026-07-06 context hints: bench/spdk-liveness-box-notes.md — prior valsd box harness (Scenario B = JBOD node config pattern, exact log strings, sudo/LD_LIBRARY_PATH incantations); plans/distnode-box-bench.md — box prereqs,
…[truncated]
- 2026-07-06 implementer report: Completed the SPDK multi-drive (JBOD) named-zone box verification (task 0047) on oldbox. Deliverable: new `bench/zoned-multidrive-box-notes.md` (full evidence, exact log lines, metrics, PASS
…[truncated]
- 2026-07-06 review tier: simple (coordinator self-review)
- 2026-07-06 decision: accept — commit: bench: box-verify named zones on the SPDK multi-drive (JBOD) path — 8/9 PASS incl. both-drive shard spread, catalog-free zone reopen, drop→reuse→no-resurrection, blank-drive loud refusal; JBOD s
…[truncated]
- 2026-07-06 usage: 136,217 tok (in 300, out 135,917, cache_r 16,105,601, cache_w 296,643) · cost n/a (unpriced)
  implementer: 123,561 tok (in 272, out 123,289, cache_r 15,219,160, cache_w 211,795) · cost n/a (unpriced)
  coordinator: 12,656 tok (in 28, out 12,628, cache_r 886,441, cache_w 84,848) · cost n/a (unpriced)

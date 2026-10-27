---
id: "0041"
title: 'Box verification: SPDK daemon liveness lease + zoned path + config weight on hardware'
status: done
priority: 3
created: "2026-07-04"
updated: "2026-07-06"
depends_on:
    - "0006"
spec_refs: []
---

## Description
Task 0006 landed the SPDK liveness lease (single-drive zoned + multi-drive per-partition watermarks) and per-node `data_node = <id>*<weight>` PD config, verified off-hardware only (MemDriveSet unit test, file-backend daemon e2e, `cargo check --features spdk` type-check). Needs a pass on a real SPDK box (same class as tasks 0012/0014/0027):

- `valsd node` `backend = spdk`, single drive, `gc_grace > 0`: boots fenced, unfences after the first sweep, serves named + sorted zones; restart recovers zones and the watermark; a node held down past the grace wipes + re-syncs.
- Multi-drive JBOD with `gc_grace > 0`: per-partition `liveness-shard{s}` watermarks persist across graceful restart.
- PD config with `data_node = <id>*<weight>` matching the JBOD node's logged drive-count weight; `admin members` shows it and placement skews accordingly.

## Acceptance criteria
- The above scenarios pass on hardware; findings recorded (fix-or-file for any regression).

**UNBLOCKED (2026-07-05)**: owner approved tearing down the live 4-node valsd cluster on oldbox; the four vfio-bound NVMe drives are free to claim. Box access verified: `ssh oldbox` (repo checkout at `~/vals`, passwordless sudo). First box session must gracefully stop the old cluster (valsd pd + node1–4, root PIDs up since Jul 03) before claiming the drives.

## Acceptance criteria

## Plan

Hardware verification session on oldbox (ssh, passwordless sudo; repo at ~/vals; run harness at ~/run/cluster). Old cluster already torn down (task 0056); four vfio-bound NVMe drives free: 0000:05:00.0, 0000:07:00.0, 0000:c1:00.0, 0000:c3:00.0. NEVER touch nvme4/nvme5 (OS RAID).

IMPORTANT scope correction: the task text's "per-partition liveness-shard{s} watermarks" (file-based, from task 0006) is STALE — commit 64d42d0 (task 0040) moved the watermark ON-DEVICE onto the zoned/per-partition SPDK superblock (SuperblockLivenessLog; backend=spdk + gc_grace>0 no longer needs data_dir). Verify the CURRENT on-device semantics.

0) Setup
- rsync the local repo HEAD (742d58d) to box:~/vals — exclude .git and target, do NOT use --delete (box has untracked scratch worth keeping). SPDK toolchain already built at third_party/spdk.
- Build on box: VALS_SPDK_LINK=1 cargo build --release --features spdk -p vals-server (takes minutes — run as a background job). Run daemons with sudo env LD_LIBRARY_PATH=~/vals/third_party/spdk/dpdk/build/lib, per rsweep.sh.
- Work in a fresh subdir of ~/run/cluster (e.g. t0041/) copying the conf/secret pattern; hygiene per bench/b1-box-ab-notes.md: pkill leftover daemons, clean /var/run/dpdk/spdk* + /dev/hugepages/spdk*map_* residue between runs. format=true now reformats over any stale superblock (task 0067) — no dd wipe needed. region_mib = 8192 is a proven size.

A) Single-drive zoned + liveness lease (gc_grace > 0)
- 3 single-drive SPDK nodes (drives 05/07/c1) + PD, rf=3, sweep=5s, gc_grace short (~45-60s) to make the expiry test fast. NO data_dir on the nodes (proves on-device watermark). PD boot zones: at least one named sorted zone and one named unsorted zone (pd.conf `zone = <name> <rf> <ordering>` — check config.rs for exact keywords).
- Verify: nodes boot FENCED and unfence after the first sweep (find the exact log lines in the source first, e.g. rg fence/liveness in crates/vals-cluster); named + sorted zones are served — write/read real data (check ~/vals-examples tools: kvchurn for the default zone; valsq/valsmix or a zone flag for named zones; the daemon e2e test tests/daemon.rs shows what "serving a zone" means — at minimum `valsd admin zones` + a real put/get/scan against the sorted zone via whatever client supports zones; if no client can address named zones, record that as a finding and prove materialization + recovery via logs/metrics).
- Graceful restart (SIGTERM, then restart with format=false): zones reopen from the on-device catalog, watermark recovered (log evidence), node re-fences then unfences after a sweep, data still readable.
- Held-down past grace: stop node1; keep cluster serving (kvchurn writes against the ring); wait > gc_grace; restart node1 → it must detect the stale watermark, WIPE, and re-sync via AE. Evidence: wipe log line, node1's ae_entries_applied / records climbing on its metrics port, then a read-verify passing.

B) Multi-drive JBOD + liveness (gc_grace > 0)
- Tear down A (graceful). One JBOD node: 2 listen addrs + 2 pci drives + format=true + gc_grace>0, plus 2 single-drive peers (remaining drives... note only 4 drives total: JBOD gets 2, peers get 1 each). JBOD path serves the default zone only (known limitation — task 0047 covers named zones there).
- Load data (kvchurn), graceful SIGTERM restart with format=false: each partition's superblock watermark persists and is recovered (per-partition log evidence), node unfences after sweep, data intact.

C) Per-node weight in PD config
- The JBOD node logs its computed ring weight (should be 2 for 2 drives) at startup — capture the line. PD conf: data_node = <id>*2 @ addr1,addr2 with rf=2 (so placement can actually skew across 3 nodes of weights 2/1/1).
- `valsd admin -s <sock> members` shows weight 2. Load ~1M keys via kvchurn and compare per-node vals_records_written (metrics ports): the weight-2 node should take roughly 2× a weight-1 peer's share (rough proportionality is enough; record the numbers).

D) Deliverables
- Findings notes committed in-repo as bench/spdk-liveness-box-notes.md (same style as bench/b1-box-ab-notes.md): per-scenario evidence (log excerpts, metrics numbers) + verdicts.
- Fix-or-file: small regressions → fix in this repo (off-hardware testable) and re-verify on box; anything bigger → describe precisely in the report so the coordinator can file follow-up tasks. Update BACKLOG.md's "on-hardware box verification of the SPDK liveness" remaining note (~line 647) if all passes.
- Leave the box clean: all daemons stopped gracefully, no stray hugepage/dpdk residue.

Constraints: ssh non-interactively (BatchMode); each Bash call is a fresh shell — no state carries; long-running box commands (build, kvchurn runs) via nohup/setsid + log files, then poll the logs, or run as background jobs.

### Starting points
- box: ssh oldbox; harness ~/run/cluster (node1.conf/pdc.conf/secret/rsweep.sh show the working pattern: sudo env LD_LIBRARY_PATH=~/vals/third_party/spdk/dpdk/build/lib setsid valsd ...)
- drives (vfio-bound, free): 0000:05:00.0 0000:07:00.0 0000:c1:00.0 0000:c3:00.0 — OS RAID nvme4/nvme5 off limits
- kvchurn client: ~/vals-examples/target/release/kvchurn --nodes 1@127.0.0.1:7001,... --count N --rf 2 --rounds R --threads 8 --pipeline 64; metrics: curl 127.0.0.1:946x/metrics | grep vals_
- watermark is ON-DEVICE since 64d42d0 (SuperblockLivenessLog, superblock record v2) — crates/vals-cluster/src/zonestore.rs / spdk store liveness_log; no data_dir needed for spdk+gc_grace
- crates/vals-server/src/run_node.rs — serve_spdk_zoned (single-drive) + multidrive branch; crates/vals-server/src/config.rs — zone/pd data_node weight syntax
- fence/unfence + wipe log lines: rg -n "fenc|wipe|watermark" crates/vals-cluster/src
- bench/b1-box-ab-notes.md — box hygiene recipe (dpdk/hugepage residue cleanup; format=true over stale superblock works since task 0067; region_mib=8192)
- docs/deploy.md — node/PD config reference incl. weight syntax and spdk liveness notes

## Work log
- 2026-07-06 plan: Hardware verification session on oldbox (ssh, passwordless sudo; repo at ~/vals; run harness at ~/run/cluster). Old cluster already torn down (task 0056); four vfio-bound NVMe drives
…[truncated]
- 2026-07-06 context hints: 8 recorded with plan
- 2026-07-06 context hints: box: ssh oldbox; harness ~/run/cluster (node1.conf/pdc.conf/secret/rsweep.sh show the working pattern: sudo env LD_LIBRARY_PATH=~/vals/third_party/spdk/dpdk/build/lib setsid valsd ..
…[truncated]
- 2026-07-06 implementer report: Hardware verification of the SPDK liveness lease on oldbox complete. Synced local HEAD (742d58d) to the box; the `--features spdk` release binary was already current. Ran three scenarios aga
…[truncated]
- 2026-07-06 review tier: single-opus — reviewers: Claude
- 2026-07-06 review (Claude): accept — This is an on-hardware SPDK liveness verification task; its deliverables are the bench notes, BACKLOG/backlog updates, and fix-or-file follow-ups. The bench notes are thorough and the load-bearing cla
…[truncated]
- 2026-07-06 decision: accept — commit: bench: SPDK liveness lease verified on hardware — on-device watermark + zones + weight PASS; wipe-before-rejoin startup race found, filed as 0070 (task 0041)  Box verification on oldbox (b
…[truncated]
- 2026-07-06 usage: 98,329 tok (in 218, out 98,111, cache_r 7,858,062, cache_w 242,366) · cost n/a (unpriced)
  implementer: 83,503 tok (in 184, out 83,319, cache_r 7,275,718, cache_w 149,038) · cost n/a (unpriced)
  coordinator: 11,539 tok (in 20, out 11,519, cache_r 495,414, cache_w 70,778) · cost n/a (unpriced)
  reviewer:Claude: 3,287 tok (in 14, out 3,273, cache_r 86,930, cache_w 22,550) · cost n/a (unpriced)

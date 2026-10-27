---
id: "0027"
title: 'Box A/B: validate B1 front-coded index on the SPDK cluster (read/write CPU regression)'
status: done
priority: 2
created: "2026-07-02"
updated: "2026-07-05"
depends_on:
    - "0001"
spec_refs:
    - Current focus areas
---

## Description
**UNBLOCKED (2026-07-05)**: owner approved tearing down the live 4-node valsd cluster on oldbox; the four vfio-bound NVMe drives are free to claim. Box access verified: `ssh oldbox` (repo checkout at `~/vals`, passwordless sudo). First box session must gracefully stop the old cluster (valsd pd + node1–4, root PIDs up since Jul 03) before claiming the drives.

Stage B1 (front-coded sorted-zone in-RAM index, task 0001) landed with the RAM win measured locally (3.44× marginal RSS, `vals-bench memprof`) but the acceptance criterion "Box A/B on the SPDK cluster: no read/write-CPU regression outside noise" requires hardware and remains open. The named risk is real: the CPU-bound file-backend microbench shows sorted-zone memtable *insert* throughput dropping ~3–6× (O(block≈32) re-encode per insert; expected to amortize under device latency on the real write path, but must be measured).

## What to do
- A/B HEAD vs the pre-B1 parent on the SPDK cluster (oldbox recipe / rsweep.sh + kvchurn, per BACKLOG.md's AE digest serve item): read and write throughput + serving-core CPU, kvchurn value-verified, 0 errors.
- Watch specifically: sorted-zone write throughput under churn (insert re-encode CPU), point-read p99 (O(block) scan vs BTreeMap point lookup), and AE digest-page serving (`version_digest_page` still materializes the full memtable `versions()` per page — pre-existing O(memtable) shape, now with owned-key copies; if the box shows it hot, file a targeted fix to filter during iteration instead of materializing).
- Record the outcome in docs/out-of-core-index.md (B1 box-validated, or the regression + chosen mitigation).

## Acceptance criteria
- A/B numbers recorded (read/write throughput, CPU) HEAD vs parent, same recipe, noise-bounded.
- kvchurn value-verified with 0 errors on HEAD.
- docs/out-of-core-index.md B1 section updated with the box result; regressions (if any) get follow-up tasks.

## Acceptance criteria

## Plan

Goal: box A/B of Stage B1 (front-coded sorted-zone in-RAM index, commit 5077227) on the SPDK cluster at oldbox — confirm no read/write-CPU regression outside noise, kvchurn value-verified 0 errors on HEAD, and record the outcome in docs/out-of-core-index.md.

Key design decision (HEAD has drifted ~15 commits past B1): the isolating A/B is **A = 48b4c98 (pre-B1 parent) vs B = 5077227 (B1)** — that pair differs only by B1. Additionally run **HEAD (24b87a3)** with the same recipe to satisfy the "kvchurn value-verified 0 errors on HEAD" criterion and to confirm no regression persists in the current tree (HEAD numbers are informational, not attribution).

Box facts (verified 2026-07-06 by coordinator):
- `ssh oldbox` works non-interactively; passwordless sudo. Box is idle: no valsd/vrun running, drives unclaimed, /var/run/dpdk clean (task 0056 tore the old cluster down).
- Repo copy at `~/vals` (NOT a git repo — rsync'd; third_party/spdk is built there and is gitignored, so making it a git repo and checking out commits in place will not disturb the SPDK build). NEVER `rsync --delete` into it (0056 note: wipes SPDK artifacts).
- 4 vfio-bound PM9A3s: 0000:05:00.0, 07:00.0, c1:00.0, c3:00.0. nvme4/nvme5 are the OS RAID1 — do not touch.
- Cluster recipe: `~/run/cluster/rsweep.sh <bin> <label> <sweep> <count> <rounds> <timeout>` — 3-node SPDK RF=2 valsd cluster (node1..3.conf, backend=spdk, format=true) + prebuilt kvchurn (`~/vals-examples/target/release/kvchurn`, built Jun 18 — post-hello-handshake, should speak the current proto; if rejected, rebuild it from ~/vals-examples sources). Verify node1..3.conf PCI addrs are all within the 4 vfio-bound set before running.
- Build recipe: `VALS_SPDK_LINK=1 cargo build --release -p vals-server --features spdk` (and `-p vals-bench --features spdk` for distsorted); run with `LD_LIBRARY_PATH=third_party/spdk/dpdk/build/lib`.

Steps:
1. Bring git history to the box: rsync the workspace `.git` into `~/vals/.git` (no --delete on the work tree), then `git checkout -f <commit>` per build. Confirm `git status` sanity and that third_party/spdk artifacts survive.
2. Build three valsd binaries on the box: /tmp/valsd.A (48b4c98), /tmp/valsd.B1 (5077227), /tmp/valsd.HEAD (24b87a3), each release + spdk. Also build vals-bench (--features spdk) at A and B1 for the sorted-zone workload. If the spdk-featured bench fails to compile at A/B1 (task 0055 fixed 8 such errors later), cherry-pick bee1403 onto both sides identically (bench-only; keeps the A/B fair). Same for the 0056 bench fixes (4-drive topology / distsorted hello) if needed — apply identical bench-side patches to both sides, never store-side.
3. Main A/B (hash-zone churn + AE digest serving): `rsweep.sh /tmp/valsd.<X> <label> 5s 5000000 2 240` per BACKLOG.md's recipe. Interleave runs A,B1,A,B1 (≥2 per side) then HEAD ×2. During each run's steady window, sample per-process CPU of the 3 vrun nodes (e.g. `pidstat -h -u -p $(pgrep -x vrun -d,) 5`) so serving-core CPU is comparable. Record wr/s, rd/s, aeVal/s, aeDig/s time series, kvchurn verify tail (must be value-verified, 0 errors — REQUIRED on HEAD), and CPU.
4. Sorted-zone A/B (the named B1 risk — insert re-encode CPU, O(block) point scans): run distsorted at A and B1 (same params, 4 drives, ≥2 runs per side, interleaved), record write/scan throughput + process CPU. If distsorted at those commits needs the hello fix, patch both sides identically.
5. Judge noise: within-side spread vs between-side delta. Regression = between-side delta clearly outside within-side spread on read/write throughput or CPU.
6. Record results incrementally in a scratch notes file in the workspace as runs complete (nothing lost if the session ends), then write the final outcome into docs/out-of-core-index.md's B1 section ("B1 — BOX A/B (2026-07-06)" style entry: recipe, numbers, verdict). If a real regression shows (esp. sorted-zone insert or version_digest_page hotness), record it precisely — the coordinator will file follow-up tasks.
7. Leave the box clean: no vrun/valsd left running, hugepages returned, `git -C ~/vals checkout -f` back to HEAD's commit so the box tree matches current master.

Cautions: everything on the box is over ssh as root against real hardware — stay scoped to the 4 vfio drives. Long remote commands (builds, 4-min bench runs) exceed the 2-min shell timeout: run them as background jobs or nohup on the box and poll/tail logs. format=true wipes the drives — approved (0044 regenerates its corpus).

### Starting points
- box: ssh oldbox; repo at ~/vals (not a git repo yet); recipe ~/run/cluster/rsweep.sh; kvchurn at ~/vals-examples/target/release/kvchurn
- A/B commits: A=48b4c98 (pre-B1 parent), B=5077227 (B1), HEAD=24b87a3; bench-only cherry-pick if needed: bee1403 (0055 spdk bench compile fix)
- build: VALS_SPDK_LINK=1 cargo build --release -p vals-server --features spdk; run env LD_LIBRARY_PATH=third_party/spdk/dpdk/build/lib
- task 0056 work log in backlog/0056-*.md — box state, teardown notes, rsync --delete warning
- docs/out-of-core-index.md — B1 sections at lines ~183-245 (IMPLEMENTED / measured win / known cost & box A/B follow-on)
- recipe reference: BACKLOG.md line ~409: rsweep.sh /tmp/valsd.<bin> <label> 5s 5000000 2 240

## Work log
- 2026-07-05 plan: Goal: box A/B of Stage B1 (front-coded sorted-zone in-RAM index, commit 5077227) on the SPDK cluster at oldbox — confirm no read/write-CPU regression outside noise, kvchurn value-verified 
…[truncated]
- 2026-07-05 context hints: 6 recorded with plan
- 2026-07-05 context hints: box: ssh oldbox; repo copy at ~/vals (not a git repo; third_party/spdk built there, gitignored); recipe ~/run/cluster/rsweep.sh; kvchurn at ~/vals-examples/target/release/kvchurn
…[truncated]
- 2026-07-05 implementer report: Box A/B of Stage B1 (front-coded sorted-zone index) completed on oldbox. Result: **B1 is box-validated with NO regression on the Unsorted/HashIndex + AE-digest-serving path, but a real, repr
…[truncated]
- 2026-07-05 review tier: single-opus — reviewers: Claude
- 2026-07-05 review (Claude): accept — This is a hardware validation/benchmarking task; the deliverables are docs, a scratch-notes file, an A/B harness script, and a follow-up backlog task — no store-side code was modified, correctly kee
…[truncated]
- 2026-07-05 decision: accept — commit: bench: B1 box A/B on the SPDK cluster — no hash/AE regression; sorted insert 1.55× (task 0027)  Isolating A/B on oldbox (A=f39104e true pre-B1 parent vs B1=5077227, plus HEAD informationa
…[truncated]
- 2026-07-05 usage: 156,138 tok (in 392, out 155,746, cache_r 28,540,455, cache_w 343,590) · cost n/a (unpriced)
  implementer: 143,874 tok (in 348, out 143,526, cache_r 27,560,838, cache_w 249,041) · cost n/a (unpriced)
  coordinator: 8,736 tok (in 32, out 8,704, cache_r 918,493, cache_w 71,754) · cost n/a (unpriced)
  reviewer:Claude: 3,528 tok (in 12, out 3,516, cache_r 61,124, cache_w 22,795) · cost n/a (unpriced)

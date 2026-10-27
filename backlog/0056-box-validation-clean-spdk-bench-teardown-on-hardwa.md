---
id: "0056"
title: 'Box validation: clean SPDK bench teardown on hardware (repeat runs, exit 0, no /var/run/dpdk residue)'
status: done
priority: 4
created: "2026-07-05"
updated: "2026-07-05"
depends_on:
    - "0020"
    - "0055"
spec_refs: []
---

## Description
Task 0020 gave the passive bench servers a graceful-shutdown handle (`ServerHandle` via `ReplicaServer::spawn_with_handle` / `spawn_zoned_with_handle` / `ShardedServer::spawn_with_handle`, reusing `shutdown_loop` + `finalize_shutdown`) and wired every dist* bench workload to shut its servers down after results print. Verified locally with file/mem backends only. This task is the deferred on-hardware half of 0020's acceptance criteria: validate on the SPDK box that bench teardown is actually clean.

Requires the SPDK NVMe test box (and task 0055 — the spdk-featured bench must compile).

## Acceptance criteria
- On the box, the dist* SPDK bench workloads (at least distspdk, distmdmc, distzone, distsorted) exit 0 with a clean SPDK detach (no segfault racing process exit).
- Back-to-back repeated runs succeed with no manual `/var/run/dpdk/*` cleanup and no stuck claimed controller between runs.

**UNBLOCKED (2026-07-05)**: owner approved tearing down the live 4-node valsd cluster on oldbox; the four vfio-bound NVMe drives are free to claim. Box access verified: `ssh oldbox` (repo checkout at `~/vals`, passwordless sudo). First box session must gracefully stop the old cluster (valsd pd + node1–4, root PIDs up since Jul 03) before claiming the drives.

## Acceptance criteria

## Plan

Goal: the deferred on-hardware half of task 0020's acceptance — prove the dist* SPDK bench workloads tear down cleanly on the real box (exit 0, clean SPDK detach, repeatable back-to-back with no /var/run/dpdk residue or stuck claimed controllers).

Access & preconditions (verified 2026-07-05 by PM session):
- `ssh oldbox` works non-interactively from this environment; passwordless sudo; repo checkout at `~/vals` on the box; 4 NVMe drives vfio-bound (0000:05:00.0, 07:00.0, c1:00.0, c3:00.0).
- A live 4-node valsd cluster (valsd pd + node1–4, root PIDs 4117269/4118526/4118533/4118540/4118547, up since Jul 03) still holds the drives. Owner has approved taking it down. Its on-drive data does NOT need preserving (task 0044 will regenerate its corpus), but stop it gracefully (sudo kill -TERM, wait for exit) — don't kill -9.

Steps:
1. On the box: gracefully stop the old cluster (TERM the node PIDs then the pd, confirm all exited and no valsd remains). Note anything odd (e.g. residue those daemons themselves leave) but their teardown is not the acceptance target.
2. Bring the box checkout to current HEAD (push/rsync from the workspace repo; check `git -C ~/vals status/log` first — do not clobber uncommitted work without looking). Build `vals-bench` release with `--features spdk` on the box (see docs/SPDK.md for SPDK v24.09 env/hugepages expectations).
3. Baseline check: `sudo ls /var/run/dpdk/` before any run (record pre-existing residue; clean it once so the experiment starts fresh).
4. For each workload — at least distspdk, distmdmc, distzone, distsorted — run at modest scale (small key counts; this validates teardown, not throughput) as root against the vfio-bound drives:
   - run → confirm exit code 0 and no segfault/abort racing process exit (check dmesg/journal for a crash if in doubt);
   - immediately re-run the same workload back-to-back with NO manual /var/run/dpdk cleanup → must succeed (no stuck claimed controller, no EBUSY/probe failure);
   - between workloads, verify `/var/run/dpdk` carries no growing residue.
5. Do at least one back-to-back pair per workload plus one interleaved sequence (e.g. distspdk → distmdmc → distspdk) to catch cross-workload residue.
6. Record the results (per-workload exit codes, residue observations) in the task work log / commit a short note; any deviation gets filed as a bug task rather than papered over with manual cleanup.

Acceptance: all four workloads exit 0 with clean detach; repeated runs need no manual /var/run/dpdk cleanup; deviations filed.

Caution: everything on the box runs over ssh as root against real hardware — keep runs scoped to the four vfio-bound drives; nothing here should touch the box's OS drive.

## Results (2026-07-06, oldbox)

**PASS — all acceptance criteria met on hardware.** 13 SPDK bench invocations against the 4 vfio-bound PM9A3s (0000:05:00.0, 07:00.0, c1:00.0, c3:00.0):

- distspdk ×4 (back-to-back pair + both ends of the interleaved sequence), distmdmc ×3 (pair + interleave middle), distzone ×2 (pair), distsorted ×2 (pair, post-fix): **every run exit 0**, per-node "graceful shutdown complete (engines synced)" logged, no segfault racing exit (dmesg clean across the whole session).
- **Zero manual `/var/run/dpdk` cleanup between runs; no stuck claimed controller** — every back-to-back re-attach succeeded, including the interleaved distspdk → distmdmc → distspdk sequence.
- Hugepages fully returned after every run (`HugePages_Free` back to 24576/24576; `/dev/hugepages` empty) — including the two *panicked* pre-fix distsorted runs (see bug below), so even abnormal exit didn't leak hugepage mappings.
- Benign artifact: each run leaves one **empty** `/var/run/dpdk/spdk_pid<pid>/` dir (0 bytes; DPDK removes the contents on clean exit but not the per-pid dir itself; pid-unique so it never wedges a later run). Contrast the pre-existing baseline residue from the box's old unclean SPDK deaths: `spdk1..4` dirs *with* config/fbarray files plus 33 GiB of orphaned `/dev/hugepages/spdk*map_*` (16384 hugepages held). Baseline recorded and cleaned once before the experiment; empty pid dirs tidied after.

Two in-repo fixes were needed to run at all (bench-only, committed with this task):
1. **4-drive topology**: the box now has only 4 data drives (the other two NVMe are the OS RAID1 — the 8×PM9A3 provisioning is gone), but distspdk/distmdmc/distzone hard-asserted ≥6 addrs. They now drop to 1 drive-per-node when <6 addrs are given (≥6 keeps the original 2-per-node JBOD topology).
2. **distsorted raw-scan handshake bug** (surfaced on first hardware run, exit 101 ×2 reproducible): `raw_shard_count` opened a raw `TcpStream` and sent a Scan frame without the client hello, so the handshake-enforcing server dropped the connection ("client handshake rejected (Malformed)") and `read_exact` hit UnexpectedEof. Fixed by exchanging hellos (`vals_cluster::proto::encode_hello`/`decode_hello`) before the scan loop. Post-fix distsorted passes ×2 (spread OK 600000/600000 replica copies, range scans 0 errs).

Box state after: no valsd cluster running (owner-approved teardown of the old file-backend cluster, done gracefully via SIGTERM), drives unclaimed, `/var/run/dpdk` empty, all hugepages free. Notes for later box sessions: build with `VALS_SPDK_LINK=1`; run with `LD_LIBRARY_PATH=third_party/spdk/dpdk/build/lib`; beware `rsync --delete` into `~/vals` — it wipes SPDK build artifacts outside `build/` (isa-l `.libs`, `mk/config.mk`); re-run `./configure && make` in `third_party/spdk` if hit.

### Starting points
- ssh oldbox — repo at ~/vals, passwordless sudo; drives vfio-bound: 0000:05:00.0, 07:00.0, c1:00.0, c3:00.0
- old cluster PIDs to stop: valsd pd 4117269; nodes 4118526, 4118533, 4118540, 4118547 (root)
- docs/SPDK.md — box setup, SPDK v24.09, hugepages, run recipes
- crates/vals-bench/src/main.rs — dist* workload arms; graceful shutdown via spawn_with_handle handles (task 0020)
- task 0020 landed ServerHandle/shutdown_loop/finalize_shutdown; task 0055 made --features spdk bench compile

## Work log
- 2026-07-05 plan: Goal: the deferred on-hardware half of task 0020's acceptance — prove the dist* SPDK bench workloads tear down cleanly on the real box (exit 0, clean SPDK detach, repeatable back-to-back with no /va
…[truncated]
- 2026-07-05 context hints: 5 recorded with plan

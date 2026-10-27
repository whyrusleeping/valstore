---
id: "0143"
title: 'Box-validate the split fix: 20M-key sorted load with auto-split on ends with 0 missing keys'
status: done
priority: 2
created: "2026-08-07"
updated: "2026-08-07"
depends_on:
    - "0139"
spec_refs: []
---

## Description
Repeat the failing flow-3 load from the 2026-08-06 lab benchmark session (task 0138 / bug 0139) on a rebuilt cluster and confirm the data-stranding is gone.

## Steps
- Fresh 3-node lab SPDK cluster (RF=2, 4 shards/node), `plans/bootes-soak-runbook.md` for setup. REBUILD the rsync'd `~/vals` tree first (see memory: stale binaries have re-run fixed bugs).
- `create-zone smix 2 sorted presplit=8` plus explicit `split-zone` boundaries matching the key pattern (see task 0140 — `presplit=K` alone does not spread a prefixed keyspace).
- Bulk-load 20M keys x 256 B via `write_batch_in` at `tier=majority` with auto-split default-on; expect `vals_pd_range_splits > 0`.
- Verify: sampled windows (`valsq --r 2`) return the full expected count, and `valsmix --workload scan` for 120 s logs **0** consistency violations.
- Control arm (for the record): same load with `VALS_PD_RANGE_SPLIT_THRESHOLD=0`.

## Acceptance criteria
- 0 missing keys and 0 consistency violations with auto-split on.
- Artifacts under `bench/` and the result attached to 0139's work log.

## Plan

Box validation of the 0139 metadata-only split fix on the lab 3-node SPDK cluster. This is an **operational run**, not a code change: the only tree edits are new artifacts under `bench/lab-split-fix-20260807/`, a notes file, and a work-log line appended to `backlog/0139-*.md`.

## 0. Preconditions (do not skip)
- A cluster from the 2026-08-06 session is still running with a **stale** binary (built 2026-08-06 18:41 from HEAD `537a4f9`, i.e. *before* the fix). It must be stopped and rebuilt.
- `plans/bootes-soak-runbook.md` §0–§2 is the authoritative setup procedure (single-line `sudo systemd-run` launches; `LD_LIBRARY_PATH` on every valsd/admin invocation).
- Boxes verified reachable, sudo-ok, HugePages_Total 4096, panic_on_oops=1, `format = true` in each node.conf (checked by the coordinator just now).

## 1. Rebuild everything from current HEAD
- rsync `./crates ./Cargo.toml ./Cargo.lock ./rust-toolchain.toml` to `user@{10.0.0.114,10.0.0.18,10.0.0.200}:/home/user/vals/` (excludes per the runbook) and rebuild `VALS_SPDK_LINK=1 cargo build --release -p vals-server --features spdk` on each box; confirm `DONE_RC=0` and a fresh `stat` on `target/release/valsd`.
- **Rebuild the clients too**: `~/code/vals-examples` (`valsmix`, `valsq`) depends on `vals-cluster` by path through the `~/code/vals → valstore` symlink. 0139 changed the range-map wire format (`RangeMap::encode` switches to a magic+version framing once a map is non-canonical, and `OwnedRangeStat` is keyed by lower bound), so a stale client binary will **fail to decode** a split zone. `cd ~/code/vals-examples && cargo build --release -p valsmix -p valsq`. Sanity: `stat` both binaries, confirm they are newer than the last `crates/vals-client/src/partition.rs` mtime.

## 2. Arm A — exact 0139 repro (the primary acceptance arm)
Fresh cluster (PD raft wipe + node restart, `format = true` reformats the drives).
- `create-zone smix 2 sorted presplit=8` and **no** explicit boundaries — this reproduces the 0140 footgun exactly as in the failing run: all 20M keys land in one range, which then blows past the default 1M-entry auto-split threshold and forces PD to auto-split repeatedly. That is precisely the flow that stranded 88% of the data.
- Wait for the zone to commit (`until $V zones | grep -q ' smix '`).
- Load: `valsmix --nodes "$NODES" --zone smix --ordering sorted --count 20000000 --vsize 256 --rf 2 --r 2 --tier majority --writers 8 --pipeline 64 --duration 120 --workload read` — the load phase bulk-writes 20M × 256 B via `write_batch_in` at `tier=majority`, then flow 1 (reads under write churn) runs. Expect the load to be slow (single range/shard, ~46 k keys/s ⇒ ~7 min) — that is expected post-fix, since splits are now metadata-only and no longer spread.
- Capture PD metrics *during* and after: `vals_pd_range_splits` (must be > 0 — if it is 0 the arm proved nothing, drop the threshold via `VALS_PD_RANGE_SPLIT_THRESHOLD` and rerun), plus `pd_sorted_ranges`, `pd_max_range_entries`, `pd_range_skew_pct`, and the new `pd_unrehomed_ranges` gauge (expected > 0 — that is the 0142 re-home debt, not a fault).
- **Verify (the acceptance evidence):**
  1. `valsq --nodes "$NODES" --zone smix --r 2 count` over ≥10 windows of 1000 keys spread across the keyspace (`valsmix/2a/%012d`), e.g. lo=`valsmix/2a/000005000000`, hi=`valsmix/2a/000005001000`. **Every window must return exactly 1000.** (The 0139 failure showed 104–121.)
  2. `valsmix … --skip-load --workload scan --scan-width 512 --duration 120 …` ⇒ **0 consistency violations, 0 transport errors** (the failing run logged 98,620 in 120 s).
  3. Also run `--skip-load --workload read --duration 120` ⇒ 0 read misses / 0 consistency errors (this samples millions of keys, the strongest "0 missing keys" signal).
  4. Node metrics: `server_errors` 0, `busy_rejections` (note the value), `live_keys` ≈ 40M cluster-wide, node RAM bounded.

## 3. Arm B — realistic spread + many splits
Fresh cluster again. `create-zone smix 2 sorted presplit=8`, then **7 explicit `split-zone` boundaries** on the real key pattern (`valsmix/2a/%012d` at i·20 000 000/8, i = 1..7) ⇒ 8 ranges of 2.5 M entries each, all above the 1 M threshold, so auto-split fires several times per range (expect `vals_pd_range_splits` ≥ ~24) while the load is genuinely spread across replica sets. Same load and the same three verifications as Arm A, plus record the load/read/scan throughput for comparison against arm D of the 2026-08-06 session (214 k keys/s load, 244 k reads/s, 2.6 k scans/s).

## 4. Arm C — control, only if arms A+B are clean and time allows
Same as arm B with `VALS_PD_RANGE_SPLIT_THRESHOLD=0` on the PD (auto-split off) → expect 0 splits and 0 violations, as a baseline on the *new* binary. Optional; skip it rather than rushing A/B.

## 5. Artifacts and write-up
- `bench/lab-split-fix-20260807/`: `A-*.log`, `B-*.log` (valsmix output), `A-windows.txt`/`B-windows.txt` (valsq window counts), `metrics.*.txt` (per-node + PD `/metrics` snapshots after each arm), `notes.md` summarising setup, per-arm results, split counts, and an explicit verdict versus the 0139 symptom table.
- Append a dated result bullet to the `## Work log` of `backlog/0139-auto-split-silently-strands-data-88-of-a-20m-key-s.md` referencing the artifacts.
- Leave the cluster **stopped** at the end (`systemctl stop vals-node` on all three, then `vals-pd`).

## Failure handling
If any window comes back short or valsmix logs consistency violations, **stop and report** with the evidence (window counts, PD split count, `pd_unrehomed_ranges`, node `live_keys`, whether the client binary is the rebuilt one) rather than iterating on fixes — that would mean 0139 is not actually fixed and needs a new bug task.
Do not run `cargo fmt`. Do not commit; the coordinator commits.

## Work log
- 2026-08-07 plan: Box validation of the 0139 metadata-only split fix on the lab 3-node SPDK cluster. This is an **operational run**, not a code change: the only tree edits are new artifacts under `bench/lab-split
…[truncated]
- 2026-08-07 implementer report: Completed task 0143’s lab box validation and left the cluster stopped.  - Rebuilt SPDK `valsd` on all 3 boxes from HEAD `6cc4300`; every build ended `DONE_RC=0`. Rebuilt `valsmix` and `valsq` aga
…[truncated]
- 2026-08-07 revision: Updated only the requested write-up paths:  - Corrected the Arm B paragraph in `bench/lab-split-fix-20260807/notes.md` to state that both creation-time presplits and later explicit splits inherited
…[truncated]
- 2026-08-07 review tier: simple (coordinator self-review)
- 2026-08-07 usage: 2,949,202 tok (in 760,315, out 40,535, cache_r 3,928,284, cache_w 120,525) · cost n/a (unpriced)
  implementer: 2,927,832 tok (in 760,265, out 19,215, cache_r 2,148,352, cache_w 0) · cost n/a (unpriced)
  coordinator: 21,370 tok (in 50, out 21,320, cache_r 1,779,932, cache_w 120,525) · cost n/a (unpriced)

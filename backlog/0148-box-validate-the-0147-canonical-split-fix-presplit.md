---
id: "0148"
title: 'Box-validate the 0147 canonical-split fix: presplit=8 spreads a 20M-key sorted load on lab'
status: done
priority: 2
created: "2026-08-07"
updated: "2026-08-07"
depends_on:
    - "0147"
spec_refs: []
---

## Description
Task 0147 restored canonical placement for empty-range splits (unit-tested only). Close the loop on the hardware where the regression was measured (bench/lab-split-fix-20260807).

Re-run the task-0143 arm on a fresh 3-node lab cluster with the rebuilt binaries:

1. `create-zone smix 2 sorted presplit=8`; check PD metrics (`:9465`) show `vals_pd_sorted_ranges 8` and `vals_pd_unrehomed_ranges 0` before any write.
2. Load 20M keys and record per-node `live_keys` and the bulk-load rate.
3. Optionally repeat the arm-B shape: `create-zone` then seven `admin split-zone <zone> <key> canonical` boundaries on the real key pattern, before loading.

## Acceptance criteria
- `pd_unrehomed_ranges == 0` after presplit-at-creation on the box.
- `live_keys` spread across all three nodes (not 20M/0/20M) for RF=2.
- Load rate materially above the 52.8k keys/s regression baseline (2026-08-06 spread-zone reference: ~214k keys/s).
- Results written to `bench/…/notes.md` and the outcome noted in memory if the "nothing spreads a sorted zone" gotcha needs another correction.

## Plan

Box validation of the 0147 canonical-split fix on the 3-node lab SPDK cluster. This is an **operational run**: the only tree edits are new artifacts under `bench/lab-canonical-split-20260807/` (logs, metric snapshots, `notes.md`). No code changes, no `cargo fmt`, no commits (the coordinator commits).

## Design decision (read before running — do not re-litigate)
`even_boundaries(K)` emits **single-byte** boundaries (`crates/vals-client/src/partition.rs:286`), and every valsmix key starts with `v` (0x76). So `presplit=8` on a valsmix keyspace puts all 20M keys in ONE range even now that the tokens are canonical — that is the still-open **task 0140**, not a 0147 failure. Therefore:
- **Arm A** (`presplit=8`) validates the 0147 *canonical map* claim (`pd_unrehomed_ranges == 0` pre-load) and is expected to still land ~20M/0/20M at ~52k keys/s → box evidence for 0140.
- **Arm B** (explicit canonical boundaries on the real key pattern) is the arm that must show the spread and the throughput recovery.
Report both honestly; do not "fix" arm A by changing the key pattern.

## 0. Preconditions
Boxes are up and idle (checked: all three `vals-node`/`vals-pd` inactive, HugePages_Total 4096, `panic_on_oops=1`, `format = true`). `plans/bootes-soak-runbook.md` §0–§2 is the authoritative setup procedure — **single-line `sudo systemd-run`** launches, `LD_LIBRARY_PATH` on every valsd/admin invocation, always wrap box `ssh` in `timeout`.

## 1. Rebuild — DO NOT SKIP
Box binaries are from 2026-08-07 03:18 = pre-0147 HEAD `6cc4300`; they would re-run the regression.
- rsync `./crates ./Cargo.toml ./Cargo.lock ./rust-toolchain.toml` to `user@{10.0.0.114,10.0.0.18,10.0.0.200}:/home/user/vals/` per runbook §0.5, then `VALS_SPDK_LINK=1 cargo build --release -p vals-server --features spdk` on each box; confirm `DONE_RC=0` and fresh `stat` on `target/release/valsd`.
- Sanity that the fix is actually on the box: `grep -c split_canonical ~/vals/crates/vals-client/src/partition.rs` (≥1) and `grep -c 'canonical' ~/vals/crates/vals-server/src/admin.rs`.
- **Rebuild the clients**: `cd ~/code/vals-examples && cargo build --release -p valsmix -p valsq` (path dep on `vals-cluster` through the `~/code/vals → valstore` symlink). Record local HEAD + all mtimes in `builds.txt`.

## 2. Arm A — `create-zone smix 2 sorted presplit=8`
Fresh cluster (PD raft wipe + node restart; `format = true` reformats the drives).
- Wait for the zone to commit (`until $V zones | grep -q ' smix '`).
- **Before a single write**, snapshot PD `:9465` metrics: acceptance is `vals_pd_sorted_ranges 8` **and `vals_pd_unrehomed_ranges 0`** (0143 measured 8/7). Save as `A-preload-metrics.txt`. If unrehomed != 0, STOP and report — 0147 is not effective on the box.
- Load 20M × 256 B (same shape as 0143, directly comparable):
  `valsmix --nodes "$NODES" --zone smix --ordering sorted --count 20000000 --vsize 256 --rf 2 --r 2 --tier majority --writers 8 --pipeline 64 --duration 60 --workload read` (`NODES='1@10.0.0.114:7001,2@10.0.0.18:7001,3@10.0.0.200:7001'`), auto-split default-on.
- Record the bulk-load rate and per-node `vals_live_keys` (`:9464`) plus PD range metrics after the load. Expected (0140): ~52k keys/s, 20M/0/20M.

## 3. Arm B — canonical boundaries on the real key pattern (the acceptance arm)
Fresh cluster again (raft wipe + node restart).
- `create-zone smix 2 sorted` (no presplit), wait for commit, then **7 canonical splits** on the actual key pattern (`valsmix/{seed:x}/{idx:012}`, default seed 42 → `2a`), at 2.5M-key intervals:
  `$V split-zone smix valsmix/2a/000002500000 canonical` … through `valsmix/2a/000017500000`.
- Snapshot PD metrics before any write: expect `vals_pd_sorted_ranges 8`, **`vals_pd_unrehomed_ranges 0`** → `B-preload-metrics.txt`.
- Same 20M load command as arm A. Record the load rate.
- **Acceptance evidence**: per-node `vals_live_keys` shows all three nodes non-zero and roughly balanced (RF=2, 40M total ⇒ ideal ~13.3M/node; anything clearly better than 20M/0/20M counts as spread — report the actual numbers), and the load rate is materially above 52.8k keys/s (2026-08-06 spread-zone reference ~214k keys/s).
- Correctness spot-checks (regression guard, since auto-split will now fire on the 2.5M-entry ranges and those inherited splits are metadata-only):
  1. `valsq --nodes "$NODES" --zone smix --r 2 count` over ≥8 windows of 1000 keys spread across the keyspace (each must return exactly 1000) → `B-windows.txt`;
  2. full-range count = 20,000,000 → `B-full-count.txt`;
  3. `valsmix … --skip-load --workload scan --scan-width 512 --duration 120` ⇒ 0 consistency violations;
  4. `valsmix … --skip-load --workload read --duration 120` ⇒ 0 consistency violations / misses.
- Also record final PD `vals_pd_range_splits`, `vals_pd_sorted_ranges`, `vals_pd_unrehomed_ranges`, `vals_pd_range_skew_pct` (unrehomed > 0 at the END is expected — runtime auto-splits inherit by design, task 0142 debt — say so in the notes).

## 4. Artifacts and write-up
- `bench/lab-canonical-split-20260807/`: `builds.txt`, `A-*` / `B-*` valsmix+valsq logs, `metrics.{A,B}.{pre,after}.{114,18,200,pd}.txt`, service logs, and **`notes.md`** with: setup, a per-arm table (pre-load sorted/unrehomed ranges, load rate, per-node live_keys, final split counts, consistency/transport errors), a comparison row against the 0143 regression baseline (52.8k keys/s, 8/7 ranges, 20M/0/20M) and the 2026-08-06 spread reference (~214k keys/s), and an explicit verdict per acceptance criterion.
- The notes must state plainly whether `presplit=K` alone spreads a *prefixed* keyspace (expected: no — task 0140) and that `split-zone … canonical` does.
- Leave the cluster **stopped** (`systemctl stop vals-node` ×3, then `vals-pd`); record final states in `stopped.txt`.

## Failure handling
If arm A shows `unrehomed != 0`, or arm B does not spread / stays near 52k keys/s, or any window/count comes back short or valsmix reports consistency violations: **stop and report with the evidence** (metric snapshots, live_keys, whether the box binary and clients are the rebuilt ones). Do not iterate on code fixes — that would be a new bug task.

### Starting points
- plans/bootes-soak-runbook.md §0–§2 (setup), §3 (zone creation, canonical split syntax)
- bench/lab-split-fix-20260807/notes.md + A-during.txt / A-after-summary.txt = the regression baseline to compare against
- crates/vals-client/src/partition.rs:286 even_boundaries() — single-byte boundaries (task 0140)
- crates/vals-server/src/admin.rs:154-164 — `split-zone <zone> <key> [canonical]`
- valsmix key format: valsmix/{seed:x}/{idx:012}, default seed 42 → prefix `valsmix/2a/`
- clients: ~/code/vals-examples (cargo build --release -p valsmix -p valsq); NODES='1@10.0.0.114:7001,2@10.0.0.18:7001,3@10.0.0.200:7001'; PD metrics :9465, node metrics :9464

## Work log
- 2026-08-07 plan: Box validation of the 0147 canonical-split fix on the 3-node lab SPDK cluster. This is an **operational run**: the only tree edits are new artifacts under `bench/lab-canonical-split-20260807/` (
…[truncated]
- 2026-08-07 context hints: 6 recorded with plan
- 2026-08-07 context hints: plans/bootes-soak-runbook.md §0–§2 setup, §3 zone creation (canonical split syntax already documented there); bench/lab-split-fix-20260807/notes.md, A-during.txt, A-after-summary.txt, builds.t
…[truncated]
- 2026-08-07 implementer report: Completed task 0148’s two-arm lab hardware validation with rebuilt SPDK servers and rebuilt valsmix/valsq clients.  - Added full artifacts under `bench/lab-canonical-split-20260807/`, includin
…[truncated]
- 2026-08-07 revision: Closed the reviewer gap with a fresh Arm C and corrected the interpretation.  - Reconfirmed all three box `valsd` mtimes/sizes exactly matched `builds.txt`; did not rebuild. - Ran Arm C on a fresh clu
…[truncated]
- 2026-08-07 review tier: single-opus — reviewers: sol
- 2026-08-07 review (sol): accept — The hardware-validation artifacts and write-up satisfy task 0148. Arm A confirms the rebuilt 0147 fix produces 8 sorted / 0 unrehomed ranges before writes and honestly demonstrates that generic single
…[truncated]
- 2026-08-07 decision: accept — commit: 0148: box-validate canonical splits on lab — 0 unrehomed at creation, spread RF=2 load, 288k keys/s like-for-like
- 2026-08-07 usage: 3,221,265 tok (in 1,035,115, out 39,078, cache_r 3,426,184, cache_w 200,842) · cost n/a (unpriced)
  implementer: 2,421,288 tok (in 729,080, out 18,992, cache_r 1,673,216, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 786,439 tok (in 306,003, out 6,580, cache_r 473,856, cache_w 0) · cost n/a (unpriced)
  coordinator: 13,538 tok (in 32, out 13,506, cache_r 1,279,112, cache_w 200,842) · cost n/a (unpriced)

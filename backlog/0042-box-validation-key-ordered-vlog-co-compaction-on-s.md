---
id: "0042"
title: 'Box validation: key-ordered vlog co-compaction on SPDK (aged sorted zone ≥5× scan, reclaim, serving impact)'
status: done
priority: 3
created: "2026-07-04"
updated: "2026-07-06"
depends_on:
    - "0028"
spec_refs:
    - Current focus areas
---

## Description
Split from task 0028 (the in-repo implementation of key-ordered vlog co-compaction). Hardware-bound validation on the SPDK cluster:

- On an SPDK node holding an aged (arrival-order/firehose) sorted zone, run a full-range scan before and after co-compaction: post-compaction rate must be ≥5× the pre-compaction rate, value-verified.
- Confirm space reclaim on the box (superseded/tombstoned value bytes actually freed; used_segments drops and stays dropped across a reboot).
- Observe serving impact while the off-core co-compaction runs (writes/reads keep serving; note any p99 effect — cooperative chunking is a known deferred refinement).

Requires 0028 landed (done). **UNBLOCKED (2026-07-05)**: owner approved tearing down the live 4-node valsd cluster on oldbox; drives free to claim. Box access verified: `ssh oldbox` (repo at `~/vals`, passwordless sudo). Gracefully stop the old cluster (valsd pd + node1–4, root PIDs up since Jul 03) before claiming the drives.

## Acceptance criteria

## Plan

Goal: hardware-validate key-ordered vlog co-compaction (task 0028) on the SPDK box (oldbox): on an aged (arrival-order) sorted zone, a full-range scan after off-core co-compaction must be ≥5× the pre-compaction rate (value-verified), superseded value bytes must actually be reclaimed (used_segments drops and stays dropped across a reboot), and serving must keep going while the off-core co-compaction runs.

Interpretation (pinned): a "sorted zone" on an SPDK node IS an Engine over the device with Ordering::Sorted — measuring at engine level on the raw namespace is the right layer for attributable scan rates (no wire/merge noise), and driving compact_plan → off-thread compact_and_write (sibling qpair) → compact_commit reproduces exactly the production off-core maintenance shape (maint.rs / node.rs). No live Node/cluster needed.

## Part 1 — new vals-bench workload `cocompact` (backends: mem | file | spdk)

Add `workload_cocompact(backend, path, n, vsize, nruns)` to crates/vals-bench/src/main.rs, modeled on `workload_scanlsm` (aged-corpus setup + paged scan) and the off-core drive pattern in replica.rs tests (~L3843: compact_plan → Arc(device sibling) → block_on(compact_and_write) → compact_commit) and maint.rs (worker thread polls its own qpair via write_dev.poll_device()).

Phases (each prints one labeled line):
1. **Age**: bsky-shaped keys (memprof_key), Fisher-Yates shuffled write order, values derived DETERMINISTICALLY from the key (e.g. seeded by a hash of the key bytes) so any later scan can value-verify without the key vec. Flush every n/(nruns+1) keys → nruns runs + memtable tail. Then OVERWRITE a deterministic subset (e.g. keys where hash(key)%4==0) with a second-generation value (higher version; verifier recomputes which generation wins). Flush + sync. Report used_segments + logical live bytes.
2. **Pre-scan**: full paged scan via scan_bounded (8 MiB reply budget, per-page cap = batch arg), value-verify EVERY value against the deterministic expectation; report keys/s, MiB/s, 0 mismatches required.
3. **Co-compact off-core with serving load**: engine.compact_plan(...) on the main thread (assert the plan has co_compact — surface if the vlog reservation fell back to plain merge; fail the run if so); hand the plan + a sibling device handle (Arc<D> via the BlockDevice sibling(), second qpair on SPDK) to a worker thread running vals_rt::block_on(compact_and_write, || write_dev.poll_device()). Meanwhile the main thread keeps serving: a random-key get loop (keys drawn from the corpus), sampling ops/s once per second and recording latency percentiles; print serving ops/s + p50/p99 BEFORE (a few seconds baseline) and DURING the compaction so the serving-impact criterion is directly observable. Join worker, compact_commit on the main thread. Report compaction wall time, bytes copied, used_segments after (expect a drop ~ the superseded generation-1 bytes).
4. **Post-scan**: same value-verified full scan; report keys/s and the post/pre ratio; the spdk box run must show ≥5×.
5. **Reboot**: drop the engine (and controller), re-open the SAME device with Engine::open (recover path — see workload_recover for the pattern; spdk backend included), assert used_segments is still the dropped value (reclaim survives reboot per the manifest-driven liveness rule), and re-run the value-verified scan once more (rate + 0 mismatches).

Notes/cautions:
- compact_plan/compact_and_write/compact_commit are public (engine.rs ~L2807/513/2949); CompactPlan is Send; the worker must poll its own qpair (maint.rs ~L205 pattern). Verify Box<dyn BlockDevice> supports sibling() for all three backends or plumb the concrete device type as scanlsm does.
- Engine::create for phase 1 (fresh format), Engine::open for phase 5. bench_config(Ordering::Sorted).
- Keep memory bounded: winners vec ≈ n entries in RAM during merge — fine at n ≤ 50M on the box (755 GB), but size the device_blocks headroom for old+new vlog copies resident transiently (co-compaction reserves output ≈ value_bytes; use ≥3× like device_blocks already does, bump if DeviceFull).
- If compact_plan returns None or plan falls back to plain merge, print a loud CHECK and exit nonzero — the whole point is validating the co-compaction path (kill-switch VALS_VLOG_CO_COMPACT stays default-on).
- Verdict line at the end: `COCOMPACT: OK` iff 0 mismatches in all scans, post/pre ratio printed, used_segments dropped and persisted; ratio threshold check (≥5×) only enforced/reported for the spdk backend (mem/file print the ratio informationally).

## Part 2 — off-hardware smoke (must be green before the box)
- `cargo run -p vals-bench --release -- mem cocompact 200000 128` → COCOMPACT: OK.
- `cargo run -p vals-bench --release -- file cocompact 1000000 128 /tmp/vals-cocompact.img` → OK, ratio > 1 expected on file too.
- `cargo build` + existing test suite untouched (`cargo test -p vals-bench` if it has tests; at minimum the workspace builds).

## Part 3 — box run (oldbox)
Box state already verified clean: no valsd/vals-bench, no spdk residue, 4 drives vfio-bound (0000:05:00.0, 0000:07:00.0, 0000:c1:00.0, 0000:c3:00.0), 755 GB RAM. Use ONE drive (0000:05:00.0).
1. `rsync -a --exclude target --exclude .git ~/code/vals/ oldbox:~/vals/` (run from the workstation side; the workspace root IS ~/code/vals).
2. On the box: `cd ~/vals && VALS_SPDK_LINK=1 cargo build --release -p vals-bench --features spdk`.
3. Probe run small (n=2M, vsize=128) to sanity-check wall time, then the measurement run at n=20M (or 50M if the probe says wall time is reasonable — pre-scan at scattered-read rates is the long pole):
   `sudo env LD_LIBRARY_PATH=$PWD/third_party/spdk/dpdk/build/lib ./target/release/vals-bench spdk cocompact 20000000 128 0000:05:00.0`
   (long-running: use a background job / nohup + log file so the ssh session isn't the weak link).
4. Record: pre keys/s, post keys/s, ratio (must be ≥5×), value-verified 0 mismatches, compaction wall time, used_segments before/after/post-reboot, serving ops/s + p99 baseline vs during. If ratio < 5×, investigate before declaring (e.g. memtable tail too large a fraction — raise n or flush the tail).
5. Leave the box clean: no vals-bench, no /var/run/dpdk|hugepages residue, drives still vfio-bound.

## Part 4 — record results
- New `bench/cocompact-box-notes.md`: topology, exact commands, full numbers, verdicts, plus the serving-impact observation (note any p99 effect — cooperative chunking is a known deferred refinement, just record it).
- docs/scan-performance.md: add the box-measured post-co-compaction scan number where the doc discusses aged/arrival-order data (short status update, not a rewrite).
- If serving impact during compaction is severe (e.g. p99 in seconds), note it in the bench notes and reference proposed task 0069 (incremental/cooperative run-merge) rather than fixing it here.

Acceptance = the three criteria in the task description, all measured on the box, with numbers recorded in the repo.

### Starting points
- crates/vals-bench/src/main.rs — workload_scanlsm (~L5124: aged corpus + paged scan pattern), workload_recover (~L3876: reopen pattern), open_engine (~L346: spdk backend), memprof_key (~L5054), make_value (~L331), device_blocks (~L339)
- crates/vals-cluster/src/replica.rs ~L3843-3847 — the plan → Arc(sibling) → block_on(compact_and_write) → compact_commit drive pattern
- crates/vals-cluster/src/maint.rs ~L192-210 — worker-thread compact_and_write with write_dev.poll_device() polling; resolve_handle_b/sibling (~L229)
- crates/vals-store/src/engine.rs — compact_plan (~L2807, needs bucket_of + gc args; see replica.rs:1377), compact_and_write (~L513, free fn), compact_commit (~L2949), used_segments (~L3299), co_compact_enabled (~L864)
- crates/vals-spdk/src/spdk.rs ~L717 — SpdkBlockDevice::sibling (second qpair)
- plans/distnode-box-bench.md — box prereqs/build/run/leave-clean conventions (rsync, VALS_SPDK_LINK=1, LD_LIBRARY_PATH, drive list)
- bench/distnode-box-notes.md — notes-file format precedent
- box: ssh oldbox, repo ~/vals, passwordless sudo; verified clean 2026-07-06 (no valsd/bench, no residue, 4 drives vfio-bound, 755 GB RAM)

## Work log
- 2026-07-06 plan: Goal: hardware-validate key-ordered vlog co-compaction (task 0028) on the SPDK box (oldbox): on an aged (arrival-order) sorted zone, a full-range scan after off-core co-compaction must be �
…[truncated]
- 2026-07-06 context hints: 8 recorded with plan
- 2026-07-06 context hints: crates/vals-bench/src/main.rs — workload_scanlsm (~L5124), workload_recover (~L3876), open_engine (~L346), memprof_key (~L5054), device_blocks (~L339), main() dispatch (~L5240); crates/vals-cluster/
…[truncated]
- 2026-07-06 implementer report: BLOCKED — Criterion 1 of task 0042 states: "post-compaction rate must be ≥5× the pre-compaction rate, value-verified." On the box (oldbox, 1 SPDK drive, 20M keys × 128B) the hardware result contra
…[truncated]
- 2026-07-06 revision: Addressed all four reviewer follow-ups (decision accepted: interpretation (a)).  ## 1. Key-ordered-write control (box, decisive) Added a `sorted` order knob to the `cocompact` workload (arg 8): it wri
…[truncated]
- 2026-07-06 review tier: single-opus — reviewers: Claude
- 2026-07-06 review (Claude): accept — High-quality, well-documented implementation of the task 0042 hardware validation. The new `cocompact` bench workload faithfully reproduces the production off-core co-compaction path (compact_plan →
…[truncated]
- 2026-07-06 decision: accept — commit: bench: SPDK box validation of key-ordered vlog co-compaction — 64× fewer scan device reads at the key-ordered ceiling, durable reclaim, serving continuity (task 0042)  New `cocompact` vals-bench wo
…[truncated]
- 2026-07-06 usage: 143,707 tok (in 410, out 143,297, cache_r 28,036,222, cache_w 459,761) · cost n/a (unpriced)
  implementer: 125,083 tok (in 358, out 124,725, cache_r 27,064,912, cache_w 262,428) · cost n/a (unpriced)
  coordinator: 12,260 tok (in 20, out 12,240, cache_r 589,998, cache_w 157,262) · cost n/a (unpriced)
  reviewer:Claude: 6,364 tok (in 32, out 6,332, cache_r 381,312, cache_w 40,071) · cost n/a (unpriced)

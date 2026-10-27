---
id: "0088"
title: 'Quantify run-merge cost: monolith duration vs data size + steady-state write amplification under churn'
status: done
priority: 3
created: "2026-07-07"
updated: "2026-07-07"
depends_on: []
spec_refs: []
---

## Description
Follow-on perf question from task 0084 (bench/t0084/notes.md): the fix makes run-merge co-compaction the load-bearing reclamation path under overwrite churn (normal 8-run merges plus pressure merges from 2 runs up), but its costs were never measured:

- **Merge monolith duration vs data size on hardware** — the 0084 production-shape run freed 2889 segments (~2.9 GB) in one merge, but wall time wasn't captured. Merge-all duration grows with live data; if it ever exceeds the pressure window (free half / write bandwidth), large zones re-enter the wedge race that task 0085's reserve is meant to backstop. Quantify the curve so 0069 (incremental/cooperative merge) and 0085 (headroom) can be sized against reality.
- **Steady-state write amplification under churn** — pressure merges from 2 runs up trade rewrite bytes for headroom. Measure bytes rewritten (run index + co-compacted values) per byte of reclaimed garbage across a sustained churn run, at both the 8-run and pressure-triggered cadences, and the foreground-latency dip during merges (the `vals_snapshots++` dip-window methodology from bench/resize-stall-1b-revalidation.md applies).

## Acceptance criteria
- Box-measured merge duration vs input size (a few points: ~0.5 GB, ~2 GB, ~10 GB inputs) and the foreground write/read latency impact while a merge is in flight.
- Measured write-amplification factor under sustained overwrite churn (steady state, both trigger cadences).
- Numbers recorded in a bench note; thresholds (PRESSURE_FREE_FRACTION / RUN_COMPACT_THRESHOLD) re-examined against them, and findings fed into tasks 0069/0085 sizing.

## Plan

Goal: box-measured (a) run-merge monolith duration vs input size (~0.5/2/10 GB points), (b) foreground read/write impact while a merge is in flight, (c) steady-state write amplification under overwrite churn at both trigger cadences (normal 8-run + pressure), written up in bench/t0088/notes.md with thresholds re-examined and findings fed to tasks 0069/0085.

Part 1 — instrumentation (small, in-repo):
1. vals-store: extend `CompactRunsStats` with `input_bytes: u64` (Σ merged inputs' index-segment footprint + Σ their `value_bytes` — the data the merge must read) and `output_bytes: u64` (merged run's index-segment footprint + co-compacted vlog output segment footprint when `co_compacted`; whole-segment footprints, computed in `compact_commit` where seg_bytes is known — document the convention).
2. vals-cluster maint.rs: time `compact_and_write` in the compact worker (Instant around the await in run_job) and carry the elapsed Duration in `MaintDone::Compacted`.
3. vals-cluster node.rs (S3 commit arm): add observe counters `run_merge_ms` (cumulative wall ms of off-core merge passes), `run_merge_output_bytes`, `run_merge_input_bytes` (cumulative); promote the merge-commit log line to info and enrich it: runs merged, entries, input_bytes, output_bytes, segs freed, merge wall ms. New counters documented in observe.rs `counters!` (they auto-export as vals_* on the metrics endpoint).
4. Sanity: cargo build + the compaction/churn test suites (vals-store engine compact tests, vals-cluster churn_reclaim*); optionally a quick local file-backend run (bench/t0084/run.sh style) to see the new log line + counters move.

Part 2 — box measurements (oldbox; the t0084 rig at ~/run/cluster/t0083 — start.sh/stop.sh, nodeA1.conf topology A: single valsd, SPDK, pci 0000:c1:00.0, sorted zone smix rf=1, sweep=10s gc_grace=0 seg=1MiB, valsmix over loopback):
0. rsync vals + vals-examples to the box (exclude target/.git); rebuild `VALS_SPDK_LINK=1 cargo build --release -p vals-server --features spdk` + release valsmix. Verify box idle (no valsd/vals-bench, no dpdk/hugepage residue) before and after.
1. Duration-vs-size curve: fresh node with region_mib=32768 (copy of nodeA1.conf); unique-key load via valsmix (--zone smix --ordering sorted --rf 1 --r 1 --tier one --count 12000000 --vsize 1024, mixed workload with some reads so foreground read+write latency is sampled across merge windows, --interval 1..5). Default cadence gives successive merge-alls with growing inputs (~2/3.75/5.5/7.25/9/10.75 GB) — each merge logs input/output bytes + wall ms. A second short run with VALS_FLUSH_TAIL_EPOCHS=64 gets the ~0.5/1 GB points. 1 Hz metrics sampler (used_segments, run_merges, run_merge_ms, snapshots, segments_freed, process_resident_bytes) for dip-window correlation + merge RSS spike (0069 cares about both).
2. WA, normal cadence: t0084 production shape (region 8192 MiB, 100k keys × 1 KiB, 300 s 100%-write churn, tier=one). Compute per merge and cumulatively: rewrite bytes (run_merge_output_bytes) per byte of reclaimed garbage (segments_freed×1MiB − output), plus end-to-end WA vs user bytes; confirm pressure_* stays 0.
3. WA, pressure cadence: same churn on a small region (512–1024 MiB) — post-0085/0089 the reserve + Busy backpressure should keep it alive where t0084 item 3 wedged; expect pressure_flushes/merges > 0. Measure the same WA ratios, busy_rejections, sustained throughput, latency. (This doubles as hardware revalidation of 0085/0089 — call that out.)
4. Teardown: stop.sh, hygiene check (no valsd, no /var/run/dpdk|/dev/hugepages residue, 4 data drives still vfio-bound).

Part 3 — bench/t0088/notes.md: duration-vs-input-size table + fitted rate (GB/s), foreground dip depth/duration during merges, WA at both cadences, RSS spike; re-examine PRESSURE_FREE_FRACTION (50%) / RUN_COMPACT_THRESHOLD (8) against measured merge duration vs the pressure window (free_half / write_bandwidth) and state at what region size / bandwidth the monolith merge stops fitting; explicit sizing inputs for 0069 (increment size to keep pauses/dips bounded, RAM spike) and 0085. Update task 0069's description if the numbers materially change its premise (I'll handle backlog edits at wrap-up if needed).

### Starting points
- crates/vals-store/src/engine.rs: CompactRunsStats (~line 103), compact_and_write (~523), compact_commit (~3224), compact_plan_inner (~3118, co_compact reservation)
- crates/vals-cluster/src/maint.rs: run_job MaintJob::Compact arm (~189), MaintDone::Compacted (~130)
- crates/vals-cluster/src/node.rs: S3 commit arm (~2297, run_merges counter + debug log), zone_maint_due scheduling (~2440)
- crates/vals-client/src/observe.rs: counters! macro (~210) — new counters auto-export via snapshot_named
- box rig: ssh oldbox, ~/run/cluster/t0083/{start.sh,stop.sh,env.sh,nodeA1.conf,pdcA.conf,burst0084.sh} — topology A = single SPDK valsd on 0000:c1:00.0, zone smix sorted rf=1; valsmix at ~/vals-examples/target/release/valsmix
- bench/t0084/notes.md §Box validation — methodology + prior numbers (47.7k wr/s sustained, first merge freed 2889 segs)
- plans/distnode-box-bench.md §Leave the box clean — residue recovery commands
- memory: valsd rebuild on box needs VALS_SPDK_LINK=1 cargo build --release -p vals-server --features spdk

## Work log
- 2026-07-07 plan: Goal: box-measured (a) run-merge monolith duration vs input size (~0.5/2/10 GB points), (b) foreground read/write impact while a merge is in flight, (c) steady-state write amplification under overwrit
…[truncated]
- 2026-07-07 context hints: 8 recorded with plan
- 2026-07-07 context hints: crates/vals-store/src/engine.rs: CompactRunsStats ~103, compact_and_write ~523, compact_plan_inner ~3118 (co_compact reservation), compact_commit ~3224; crates/vals-cluster/src/maint.rs: MaintDone::Co
…[truncated]
- 2026-07-07 implementer report: Task 0088 complete: instrumented run-merge cost, measured it on the box across all three axes, and wrote the bench note.  ## Part 1 — instrumentation (code, all builds + clippy-clean + tests pass) -
…[truncated]
- 2026-07-07 review tier: single-opus — reviewers: Claude
- 2026-07-07 review (Claude): accept — Task 0088 is an instrumentation + box-measurement task, and the change delivers both. The code adds `input_bytes`/`output_bytes` to `CompactRunsStats` (computed correctly in both the synchronous `merg
…[truncated]
- 2026-07-07 decision: accept — commit: bench+observe: quantify run-merge cost on hardware (task 0088) — new vals_run_merge_ms/input_bytes/output_bytes counters + timed off-core merge with enriched info commit log; box-measured monolith d
…[truncated]
- 2026-07-07 usage: 99,533 tok (in 322, out 99,211, cache_r 10,998,504, cache_w 361,307) · $13.7551
  implementer: 62,061 tok (in 212, out 61,849, cache_r 7,644,718, cache_w 144,201) · $6.2709
  coordinator: 32,318 tok (in 84, out 32,234, cache_r 3,134,085, cache_w 182,804) · $7.0317
  reviewer:Claude: 5,154 tok (in 26, out 5,128, cache_r 219,701, cache_w 34,302) · $0.4526

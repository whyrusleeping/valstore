---
id: "0068"
title: LSM run-merge blocks flush on the single maintenance worker → periodic tens-of-seconds cluster-wide ingest stall at scale
status: done
priority: 3
created: "2026-07-06"
updated: "2026-07-06"
depends_on: []
spec_refs: []
---

## Description
Found by the task-0008 re-validation (`bench/resize-stall-1b-revalidation.md`,
oldbox, HEAD 664397b, 3-node SPDK, sweep off, flush decoupled at 1 s,
1B-key kvchurn load). Task 0008 confirmed the **original** diagnosis of the
periodic whole-cluster ingest collapse — an in-memory `HashMap`/`BTreeMap` O(n)
resize on the single serving core — is **dissolved** (the byte-capped memtable
keeps the live index bounded; the per-key version index was retired). But the
**symptom it was filed under still reproduces at scale**, now with a *different*
root cause, so it is split out here.

**Observed:** during sustained ingest the cluster held **~390–480 k/s summed
replica-writes (~130–160 k/s per node × 3; median ~405 k/s, min 243 k)**, with post-dip
catch-up bursts to ~1.47 M/s, but suffered **periodic deep dips to ~0 for tens of
seconds**, recurring and **lengthening as the run grew**: ~10 s @ 47 M keys/node,
~30 s @ 95 M, ~40 s @ 117 M, ~20 s @ 125 M, then more frequent past ~200 M. Every
dip coincides with the `vals_snapshots` counter advancing (a flush and/or a
run-merge commit) and with `vals_compactions` flat at 0 (vlog GC is not involved).
Unlike the original symptom (one node pegging a core while the other two idle),
**all three nodes stall in lockstep** — because at RF=3 they hold identical data
and cross the flush / run-merge thresholds together. The dips are not just slowdowns:
under kvchurn's 5 s RPC deadline a stall long enough to exhaust it makes some
quorum writes actually **fail** (10 `load: write failed` at the first ~10 s dip), so
this is an availability event, not merely a throughput dip.

**Mechanism (code-confirmed):** the per-shard maintenance worker
(`crates/vals-cluster/src/maint.rs` `worker_loop`) is a **single thread** that
pulls flush and run-merge (`compact_and_write`) jobs off **one mpsc channel and
runs them serially**. Flush build+write and run-merge build+write are both
off-core (commits 21da291 / c7a1516, task 0004) — but they share that one worker.
When runs reach `RUN_COMPACT_THRESHOLD` (8) the worker starts a run-merge whose
cost grows with data (it materializes the merged entry set — `merged: Vec<..>` —
whole, per task 0004's out-of-scope note, and streams up to ~8×`FLUSH_MAX_BYTES`
= up to 32 GiB to the device). While that long merge runs, **queued flush jobs
wait**; the frozen memtable can't drain, the live memtable fills to the 4 GiB
`FLUSH_MAX_BYTES` cap, and the write path stalls cluster-wide until the worker
finishes the merge and drains the flush. The stall lengthens as the merged-run
size grows with total keys — matching the observed deepening dips. This is the
same *failure class* as the original item (an unbounded O(n) op that blocks
ingest) but the O(n) is now the **serial run-merge on the shared maintenance
worker**, not an index resize.

## Acceptance criteria
- A/B ingest measurement at ≥ hundreds of millions of keys/node showing the
  periodic tens-of-seconds cluster-wide dips are gone (or bounded to a small,
  non-deepening wobble), with the dip table from `bench/resize-stall-1b-revalidation.md`
  as the before-baseline.
- The fix keeps the crash-safety / single-writer-no-await discipline of the
  maintenance worker and does not regress flush/merge correctness (store +
  cluster suites green).

## Plan (candidate directions — pick at plan time)
1. **Don't let run-merge starve flush.** Flush is the RAM-relief path and must
   stay responsive. Options: (a) give flush priority over merge on the worker (a
   two-queue / priority `recv`, run merge only when no flush is pending); (b) a
   dedicated flush worker separate from the merge worker (two threads / two
   handle-Bs — mind the SPDK qpair-per-thread rule); (c) yield-chunk the merge so
   a pending flush can interleave.
2. **Make run-merge incremental / cooperative** so no single merge is a
   tens-of-seconds monolith — needs an incremental run builder (bloom/sidecar
   currently need the full merged entry set; task 0004 flagged this as the
   remaining refinement). This also bounds the merge-input RAM spike.
3. **Bound the merged-run size / retune `RUN_COMPACT_THRESHOLD`** so each merge is
   shorter (trades more resident runs — more RAM + point-read fan-out — for shorter
   stalls); a stopgap, not a structural fix.

Re-run the task-0008 harness (`bench/r1b.sh` — sweep off, `VALS_FLUSH_INTERVAL_MS=1000`,
`region_mib=1048576`, kvchurn `--count 1e9 --rounds 0`) after the fix and compare
the dip table.

### Starting points
- `crates/vals-cluster/src/maint.rs` `worker_loop` / `spawn` — the single serial
  worker + its mpsc job channel (the choke point).
- `crates/vals-cluster/src/node.rs` ~1148 (worker spawn), ~2057 `maint_loop`
  (drains `MaintDone`, `snapshots.inc()` on both flush and merge commit),
  `RUN_COMPACT_THRESHOLD`=8, `FLUSH_MAX_BYTES_DEFAULT`=4 GiB, `snapshot_interval`
  / `VALS_FLUSH_INTERVAL_MS`.
- `crates/vals-store/src/engine.rs` `compact_and_write` / `build_run` — merged-run
  materialization + write (task 0004 streamed the write; the merge input Vec is
  still whole).
- `bench/resize-stall-1b-revalidation.md` + `bench/r1b.sh` — the measurement and
  reusable harness (box oldbox).

## Plan

Goal: eliminate the periodic tens-of-seconds cluster-wide ingest stalls tied to flush/run-merge maintenance at scale, keeping the single-writer-no-await discipline.

Assessment of mechanism (verified in code this session):
- The single maintenance worker (crates/vals-cluster/src/maint.rs worker_loop) serializes flush behind a long run-merge: while a merge runs (grows with total data), flush_in_flight stays set for the queued flush, so the memtable grows unbounded; the eventual flush_plan (O(memtable) snapshot+sort under borrow_mut on the serving core) and flush_commit (O(entries) version-recheck drop) then get arbitrarily large. 
- Additionally, commit-time work on the SERVING core grows with data: flush_commit/compact_commit call DeviceRun::open, which synchronously reads the run footer + bloom + block index through the serving handle (qpair-A) — for a merged run holding all keys/node this is hundreds of MB of sync QD1 reads under borrow_mut, blocking every write (BatchApply takes borrow_mut on the same store). compact_commit also does an O(memtable) walk for vlog liveness. These commit-time stalls coincide exactly with the observed vals_snapshots++ dip windows.

Fix (two coordinated parts):
1. Dedicated flush worker: split maintenance into TWO worker threads per shard — one for Flush jobs, one for Compact jobs — each with its own job channel and its own per-zone handle-B sibling (created on its own thread; SPDK sibling() opens a fresh qpair, File dups the fd, Mem shares the buffer). Both post into the shared done channel + wake; maint_loop is unchanged (commits stay serialized on the serving core). MaintHandle routes submit by job type; MaintJoin joins both threads. Correctness of flush/merge commit reordering is already established: compact_commit reconciles survivors = runs \ merged_inputs (test offcore_compact_keeps_run_flushed_during_merge_and_frees_only_inputs covers a flush committing mid-merge). Update the stale "single FIFO worker … commit in submission order" comment in node.rs snapshot_loop accordingly.
2. Off-core run open: stop doing DeviceRun::open's footer/bloom/index device reads on the serving core at commit. Have the worker read + decode the footer, bloom, and block index through handle-B right after writing the run, and carry the decoded parts in WrittenFlush/WrittenCompact; add a DeviceRun constructor that assembles the run over the SERVING handle from those pre-read parts (no device I/O at commit). Bloom/index bytes are immutable once the run is durable, so reading them via handle-B is safe.

Non-goals (out of scope, keep as-is): incremental/streaming merge (the merged Vec), leveled compaction / write amplification, RUN_COMPACT_THRESHOLD retuning.

Verification:
- cargo build + full store/cluster test suites green; add/extend unit tests: flush completes while a slow compact is in flight (two-worker interleave), and a from-parts run open serves lookups identically to DeviceRun::open.
- Box A/B on oldbox per bench/r1b.sh (sweep off, VALS_FLUSH_INTERVAL_MS=1000, region_mib=1048576, kvchurn 1B keys, stop past ~200–250M keys/node): compare dip table vs bench/resize-stall-1b-revalidation.md — acceptance is no recurring tens-of-seconds ~0 dips (bounded, non-deepening wobble ok) and no write failures.
- Record results in a new bench/*.md note.

### Starting points
- crates/vals-cluster/src/maint.rs — single worker_loop + MaintJob{Flush,Compact}; split into flush/compact workers sharing done channel
- crates/vals-cluster/src/node.rs ~1149 (worker spawn), ~2057 maint_loop (commit path), ~2201 snapshot_loop (submit gates; stale FIFO-order comment ~2324)
- crates/vals-store/src/engine.rs flush_commit (~2356) / compact_commit (~2913) — DeviceRun::open on serving core is the commit-time stall
- crates/vals-store/src/run.rs DeviceRun::open (~1235) — footer/bloom/index sync reads; add from-parts constructor
- crates/vals-store/src/engine.rs build_and_write_flush / compact_and_write + WrittenFlush/WrittenCompact — carry pre-read bloom/index back
- vals_spdk sibling(): spdk.rs:717 (fresh qpair per call, thread-bound), lib.rs:551 (Mem), lib.rs:634 (File)

## Work log
- 2026-07-06 plan: Goal: eliminate the periodic tens-of-seconds cluster-wide ingest stalls tied to flush/run-merge maintenance at scale, keeping the single-writer-no-await discipline.  Assessment of mechanism (verified 
…[truncated]
- 2026-07-06 context hints: 6 recorded with plan
- 2026-07-06 context hints: crates/vals-cluster/src/maint.rs — spawn/worker_loop/run_job/MaintHandle/MaintSubmit/MaintJoin; crates/vals-cluster/src/node.rs ~1149 worker spawn; ~2057 maint_loop; ~2201 snapshot_loop (submit gate
…[truncated]
- 2026-07-06 implementer report: Implemented the code half of task 0068 (periodic cluster-wide ingest stalls from LSM flush blocking behind serial run-merge on the single maintenance worker). Two coordinated changes, both preserving 
…[truncated]
- 2026-07-06 review tier: high-powered — reviewers: Claude
- 2026-07-06 review (Claude): revise — The change is two coherent parts: (a) relocating DeviceRun::open's footer/bloom/index reads off the serving core via new RunParts/read_run_parts/DeviceRun::from_parts (carried in WrittenFlush/WrittenC
…[truncated]
- 2026-07-06 revision: All review findings addressed. Summary of the changes:  ## Review findings addressed  **1. [major] flush_commit made order-independent (crates/vals-store/src/engine.rs)** - Previously `flush_commit` b
…[truncated]
- 2026-07-06 review (Claude): accept — The revision resolves the major ordering concern from the prior review. Engine::flush_commit no longer carries a plan-time run-set snapshot (prev_run_metas) or plan-time generation; both fields were r
…[truncated]
- 2026-07-06 decision: accept — commit: cluster/store: dedicated flush worker + off-core run open dissolve the merge-blocks-flush ingest stalls (task 0068)  Split the per-shard maintenance worker into flush + compact threads (own job channe
…[truncated]
- 2026-07-06 usage: 159,282 tok (in 404, out 158,878, cache_r 19,146,290, cache_w 1,231,620) · cost n/a (unpriced)
  implementer: 70,561 tok (in 198, out 70,363, cache_r 11,400,584, cache_w 302,515) · cost n/a (unpriced)
  coordinator: 47,117 tok (in 108, out 47,009, cache_r 4,929,374, cache_w 700,105) · cost n/a (unpriced)
  reviewer:Claude: 41,604 tok (in 98, out 41,506, cache_r 2,816,332, cache_w 229,000) · cost n/a (unpriced)

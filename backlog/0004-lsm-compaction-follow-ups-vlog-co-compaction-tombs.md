---
id: "0004"
title: 'LSM compaction follow-ups: vlog co-compaction, tombstone GC over runs, cooperative chunking'
status: done
priority: 2
created: "2026-07-02"
updated: "2026-11-11"
depends_on: []
spec_refs: []
---

## Description
**Outcome (done 2026-07-04):** `compact_and_write` now streams the merged run through `run::SegmentStreamSink`, byte-identical to the old path. The other two grouped items went to task 0028 (key-ordered vlog co-compaction, done) and task 0038 (tombstone GC over runs, done).

Tracked follow-ups from the completed Stage C LSM (`docs/out-of-core-index.md` §C.5/C.6 status).

**Re-scoped at plan time (2026-07-04)** — the original three grouped items were split, per the task's own note:
1. **Vlog co-compaction** (space reclaim) → covered by **task 0028** (key-ordered vlog co-compaction), which explicitly subsumes it ("This also *is* the vlog space reclaim follow-up (task 0004's 'vlog co-compaction')"). The "space actually reclaimed (measured)" criterion moves there.
2. **Tombstone GC over runs** (kind-promotion; unblocks the "F2 gap": `forget`/`compact_filtered` no-op while runs exist, so over-replication cleanup over flushed keys stalls) → split to **task 0038**, with the resurrection-vector / kill-the-fix criteria.
3. **Cooperative-chunked run-merge compaction** — the off-core half already landed before this task started (commit `21da291` moved flush + run-merge compaction onto the maintenance worker thread, off the serving core; `c7a1516` streamed the flush write). The box measurement of maintenance stalls under sustained load rides with the box-validation tasks (0012 live-Node distributed bench, 0014 multi-machine real-NIC bench, 0027 B1 box A/B; all since done) when hardware is available. **The remaining code scope — this task — is the symmetric follow-on**: `compact_and_write` still materializes the merged run whole via `build_run` + `write_run_segs`; adopt the streaming `run::SegmentStreamSink` like the flush did (BACKLOG.md (the free-form backlog that preceded `backlog/`; its open items became tasks on 2026-07-02 and it was deleted 2026-08-01, so it is not in the public history) "Stream `compact_and_write` to the device", 2026-06-26).

## Acceptance criteria
- `compact_and_write` streams the merged run to its pre-reserved segments via `run::SegmentStreamSink` (no full `run_bytes` materialization; run copy bounded to ~one segment + sidecar, same as flush).
- On-device output byte-for-byte identical to the old `build_run` + `write_run_segs` path (equivalence regression test, as the flush change had).
- Error contract preserved: on a device write error, ALL reserved segments (used + unused) are returned in the `Err` for the serving core to free via `free_compact_segs`.
- Existing store + cluster test suites green.

## Plan

**Re-scope (recorded in the task description):** the original three grouped items were split per the task's own note. Item 1 (vlog co-compaction) is subsumed by task 0028; item 2 (tombstone GC over runs) split to new task 0038; item 3's off-core half already landed (commits 21da291/c7a1516). Remaining scope for THIS task: stream `compact_and_write`'s merged-run write through `run::SegmentStreamSink`, exactly as the flush path (`build_and_write_flush`) already does — BACKLOG.md's "Stream `compact_and_write` to the device" follow-on.

**Implementation:**
1. In `crates/vals-store/src/engine.rs`, `compact_and_write` (~line 398): replace the `run::build_run(&merged, ...)` + manual `n_used` computation + `run::write_run_segs(...)` block with the streaming pattern from `build_and_write_flush` (~lines 220–245):
   - `vals_rt::yield_now().await` before the serialize burst (pre-write half of the yield cadence, matching flush).
   - Construct `run::SegmentStreamSink::new(&*write_dev, &plan.segs, &plan.seg_lbas, plan.seg_bytes, plan.block_size)`, call `run::build_run_into(&merged, plan.opts, |key| bucket_fn(key, num_buckets), &mut sink)`, then `sink.finish().await`.
   - On `finish()` error: return `Err((plan.segs, EngineError::from(e)))` — the FULL reservation (used + unused), preserving the existing error contract for `free_compact_segs`.
   - Compute `n_used = handle.segs.len()`; `unused_segs = plan.segs[n_used..].to_vec()` (as flush does).
2. Update the now-stale NOTE comment (engine.rs ~line 437) that said compaction was left on the in-RAM path; also fix the doc comment on `compact_and_write` that references `write_run_segs`.
3. Tests: existing sink-equivalence tests (`stream_sink_bytes_identical_to_write_run_segs` etc.) already prove byte-identity at the run layer. Add one compaction-level regression test exercising the off-core plan→`compact_and_write`→`compact_commit` path with a merged run spanning multiple segments **with a partial final segment** (exercises padding + `unused_segs` accounting), asserting: read-back correctness after commit (get/scan over merged keys), the merged run's on-device bytes equal a reference `build_run` + `write_run_segs` of the same merged entries on a second device, and unused reserved segments are freed. Existing compaction crash-safety/equivalence tests must stay green.
4. Run the full workspace test suite (`cargo test` for vals-store and vals-cluster at minimum) + `cargo clippy` + `cargo fmt --check` under the pinned toolchain.

**Out of scope (noted, not done):** the merge input `merged: Vec<SnapshotEntry>` stays fully resident — streaming the *merge* would need an incremental run builder (bloom/sidecar need the full entry set); that is a separate refinement already noted in BACKLOG.md.

### Starting points
- crates/vals-store/src/engine.rs:398-474 — compact_and_write; the build_run+write_run_segs block to replace is ~lines 432-463
- crates/vals-store/src/engine.rs:212-260 — build_and_write_flush: the exact streaming template (SegmentStreamSink + build_run_into + finish + unused_segs from handle.segs.len())
- crates/vals-store/src/run.rs:884-1010 — SegmentStreamSink (finish returns RunHandle naming used prefix; errors latched)
- crates/vals-store/src/run.rs:1998 — stream_sink_bytes_identical_to_write_run_segs (existing equivalence test pattern)
- crates/vals-cluster/src/maint.rs:168-190 — MaintJob::Compact caller (no changes expected there)
- rust-toolchain.toml pins 1.96; keep clippy/fmt clean per commit f1d946a

## Work log
- 2026-07-04 plan: recorded in the Plan section above.
- 2026-07-04 context hints: 6 recorded with plan
- 2026-07-04 implementer report: Streamed `compact_and_write`'s merged-run device write through `run::SegmentStreamSink`, symmetric to the flush path — the remaining code scope of task 0004 (items 1 & 2 were re-scoped to tasks 0028 and 0038).
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change streams `compact_and_write`'s merged-run write through `run::SegmentStreamSink`, symmetric to the already-landed flush path, satisfying all acceptance criteria.
- 2026-07-04 decision: accept — commit: perf(store): stream compact_and_write merged run to device via SegmentStreamSink (task 0004). Per the commit message: the worker's run copy is bounded to ~one segment + the sidecar; the error contract is preserved; byte-for-byte identity vs the old build_run+write_run_segs path is regression-tested with a multi-segment merged run and partial final segment.
- 2026-07-04 usage: 34,140 tok (in 124, out 34,016, cache_r 2,069,501, cache_w 154,754) · cost n/a (unpriced)
  implementer: 22,242 tok (in 68, out 22,174, cache_r 1,102,513, cache_w 56,673) · cost n/a (unpriced)
  coordinator: 8,225 tok (in 30, out 8,195, cache_r 816,821, cache_w 79,179) · cost n/a (unpriced)
  reviewer:Claude: 3,673 tok (in 26, out 3,647, cache_r 150,167, cache_w 18,902) · cost n/a (unpriced)
- 2026-11-11 groom: added an outcome summary; glossed F2, BACKLOG.md and the box-validation task ids (dropped "blocked": 0012/0014/0027 are done); removed a duplicate empty Acceptance criteria heading; replaced cut-off work-log entries (plan and context hints are in the sections above; implementer/review lines trimmed to complete text; decision line restored from the pre-release commit message).

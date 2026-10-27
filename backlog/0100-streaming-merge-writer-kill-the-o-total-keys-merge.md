---
id: "0100"
title: 'Streaming merge writer: kill the O(total-keys) merged Vec on the off-core merge path (0069 leg D)'
status: done
priority: 3
created: "2026-07-09"
updated: "2026-07-09"
depends_on:
    - "0069"
spec_refs: []
---

## Description
Split from task 0069 (leg D, Phase 3 of its plan — deferred at implementation time per the plan's fallback). The off-core merge still materializes the full merged entry set (`merged: Vec<SnapshotEntry>`, ~224 B/entry, bench/t0088). After 0069's tiered selection this is bounded to the folded k-run subset on recurring merges, but a **bottommost graduation merge still materializes all keys of the zone** — mandatory to fix at 100M+ keys/node.

Shape (from 0069's plan): turn the k-way merge (`merge_sources`) into a pull iterator (preserve the fail-closed progress guard + drop_winner seam); feed `build_run_into` from the iterator — bloom sized from the Σ input entry_counts upper bound and inserted incrementally, block index/sidecar accumulated per block, min/max/entry_count/tombstone_min tracked streaming; the Victims/All value rewrite happens chunk-wise on the stream (CO_COMPACT_CHUNK) before the run writer. Note: `build_run_into` also accumulates the AE bucket sidecar in RAM — bound or stream that too, else it remains O(total keys).

Acceptance criteria:
- Off-core merge RAM is O(chunk + per-block state), not O(total merged entries): no `merged: Vec` on the compact_and_write path.
- Streaming build parity: merged run read-equivalent to the materialized path (contents, bloom behavior, sidecar); progress guard still fails closed (test).
- Store + cluster suites green; merge stays read-transparent.

## Acceptance criteria

## Plan

Goal: off-core merge RAM O(chunk + per-block state + bloom), no `merged: Vec<SnapshotEntry>` in `compact_and_write`, merged run read-equivalent to the materialized path, progress guard still fails closed.

Design (4 pieces):

1) Pull-based merge iterator. Refactor `merge_sources` (engine.rs ~1172) into a `MergeStream<S: MergeSource>` struct with `fn next(&mut self) -> Result<Option<SnapshotEntry>, EngineError>` yielding winners one at a time. Preserve exactly: heads-array k-way argmax (newest source wins version ties), the fail-closed progress guard (`min_key` must strictly exceed last processed key — emitted OR dropped — else `Corrupt`), and the `drop_winner` seam (dropped `(key,version)` accumulated internally, retrievable via `into_dropped()`/`take_dropped()` after exhaustion). Reimplement `merge_sources`/`merge_cursors`/`merge_runs` as thin collect-wrappers over `MergeStream` so the synchronous callers (`Engine::compact_runs`/`merge_all_runs`) and all existing tests are unchanged.

2) Streaming vlog rewrite. Restructure `co_compact_values` into a chunked adapter between the `MergeStream` and the run builder: pull up to `CO_COMPACT_CHUNK` entries into a chunk buffer; for `VlogRewrite::All`/`Victims` do the existing span-coalesced old-value read + `VlogWriter` re-append + ptr repoint for the chunk's rewrite subset (order preserved — key-ordered output falls out as today); for `None` it's a passthrough. Then feed the chunk to the run builder and recycle the buffer, `vals_rt::yield_now().await` between chunks (same cadence as today). While streaming, accumulate what the old code computed over `merged` afterwards: `entry_count`, `min_key` (first emitted), `max_key` (last emitted), `tombstone_min_version`, `value_liveness` (per-segment live-bytes as a `HashMap<SegmentId,u64>` over FINAL ptrs — O(#segments)), `bytes_rewritten`. `writer.finish()` after the stream ends. Error paths keep returning `all_reserved` for the serving core to free.

3) Streaming run builder (run.rs). Add a push-style `RunStreamBuilder` writing into a `RunSink` (used by compact_and_write over `SegmentStreamSink`): `push(&SnapshotEntry)` packs data blocks exactly as `build_run_into` (same block_target_bytes estimate, same `encode_block`), accumulates the block index, inserts the key into a bloom **pre-sized from an upper-bound entry count** (Σ input runs' `entry_count` — never a false negative; FP rate only improves; needs a `Bloom::with_capacity(n_upper, bits_per_key)` + pub(crate) insert), tracks a debug assert on key monotonicity. `finish()` writes bloom, block index, then the sidecar, then the footer, returning what `build_run_into` implies today (offsets/len for the footer; caller gets `RunHandle` from `SegmentStreamSink::finish` as now).

4) Sidecar: v3 key-ordered encoding + read-back pass (this is the part that kills the last O(total-keys) buffer). Bump `RUN_VERSION` 2→3. v3 sidecar body = the run's `(key_len u32, key, version u128)` records in ascending key order (NO bucket grouping, no per-bucket counts), + crc32 as before. Writer in `RunStreamBuilder::finish`: re-read the just-written data blocks in order via the in-RAM block index (device read through the sink's seg_lbas for flushed segments + the sink's in-RAM tail buffer for the not-yet-flushed segment — give SegmentStreamSink a small "read logical range" helper), decode each block, emit sidecar records, computing crc incrementally. RAM stays O(block). (~60 B/entry extra sequential read per merge — cheap; runs hold keys+ptrs, not values.) `build_run_into` (flush path, entries already in RAM) emits the same v3 sidecar directly from its slice — one on-disk codec for all new runs; its bloom stays sized by exact n so flush runs are unchanged except the sidecar section.
   Readers: carry the footer version through `Run`/`RunParts`/`DeviceRun`; `decode_footer` accepts 1|2|3. `Run::bucket_digests` and `parse_want_bucket_digests` gain version dispatch: v1/v2 = existing bucket-grouped walk; v3 = single pass filtering by a caller-supplied `bucket_of` closure (already globally key-ascending — skip the re-sort). Callers already have the function: `Engine::for_each_bucket_digest`/`want_bucket_digests` (engine.rs ~2008–2062, already re-filters by `bucket_of`) and the cluster's async path (`DeviceRun::locate_sidecar` → `parse_want_bucket_digests` in net/server.rs `serve_bucket_digests`, with `antientropy::bucket_of` + run's `num_buckets`). Keep `num_buckets` in the footer as-is (readers still use it).
   Sizing: v3 sidecar ≤ v2 sidecar (drops the per-bucket count words), so the merge output reservation (Σ input handle.len) and `run_len_upper_bound_agg` remain valid upper bounds — leave the bound formula alone (still an over-estimate), but double-check and note it in comments.

5) Rewire `compact_and_write`: open inputs → `MergeStream` (with gc drop_winner) → chunked vlog-rewrite adapter → `RunStreamBuilder` over `SegmentStreamSink` → `sink.finish()` → `read_run_parts` (unchanged) → `WrittenCompact` fields from the streamed aggregates. No `merged` Vec anywhere on this path (the CO_COMPACT_CHUNK buffer + per-block state + bloom + block index are the only O(>1) allocations; `dropped_tombstones` stays O(#GC-dropped), as today).

Tests:
- Existing `merge_sources_*` unit tests keep passing via the collect-wrapper (progress guard, cross-source dups, tie-break).
- New: streaming path progress-guard fails closed end-to-end (scripted non-ascending MergeSource through the MergeStream + builder).
- Parity test: same input runs → (a) legacy materialized `merge` + `build_run` vs (b) the streaming path; assert identical entry sequence on full scan, identical entry_count/min/max/tombstone_min, bloom has no false negatives over all keys, and bucket digests per bucket identical (v3 read vs v2 read of the materialized build if kept, else semantic equality).
- v2 back-compat: keep the v2 sidecar encoder as a test-only helper (or fixture bytes) and assert `bucket_digests`/`parse_want_bucket_digests` still decode v2 correctly, and that a v2 run and v3 run of the same entries serve identical digests.
- Full store + cluster suites green (`cargo test -p vals-store -p vals-cluster`), incl. churn_reclaim (off-core reclaim path) — merge stays read-transparent.

Out of scope (already filed / to file): merge read QD + CRC batching is task 0098; do not restructure read I/O here beyond the sidecar read-back.

### Starting points
- crates/vals-store/src/engine.rs:646-793 compact_and_write (the path to rewire)
- crates/vals-store/src/engine.rs:1015-1071 co_compact_values (chunked rewrite to adapt)
- crates/vals-store/src/engine.rs:1092-1228 merge_runs/merge_cursors/merge_sources + MergeSource/ScanSource
- crates/vals-store/src/engine.rs:1296,1321 value_liveness / tombstone_min_version (make streaming)
- crates/vals-store/src/run.rs:464-557 build_run_into (block packing/bloom/index/sidecar/footer layout)
- crates/vals-store/src/run.rs:272-315 Bloom::build/insert (needs pre-sized capacity ctor)
- crates/vals-store/src/run.rs:669-710 Run::bucket_digests; 1228-1267 parse_want_bucket_digests; 757-786 decode_footer (RUN_VERSION gate)
- crates/vals-store/src/run.rs:938+ SegmentStreamSink (append-only; needs a logical-range read helper for the sidecar read-back)
- crates/vals-cluster/src/net/server.rs:2744 serve_bucket_digests + crates/vals-cluster/src/antientropy.rs:144 bucket_of (v3 reader-side filter)
- crates/vals-store/src/engine.rs:2008-2062 for_each_bucket_digest (already re-filters by bucket_of)
- engine.rs:3635 merge output reservation = Σ input handle.len (v3 sidecar must stay ≤ v2 size)

## Work log
- 2026-07-09 plan: Goal: off-core merge RAM O(chunk + per-block state + bloom), no `merged: Vec<SnapshotEntry>` in `compact_and_write`, merged run read-equivalent to the materialized path, progress guard still fails clo
…[truncated]
- 2026-07-09 context hints: 11 recorded with plan
- 2026-07-09 context hints: crates/vals-store/src/engine.rs:646-793 compact_and_write (the path to rewire); crates/vals-store/src/engine.rs:1015-1071 co_compact_values (chunked rewrite to adapt); crates/vals-store/src/engine.rs:
…[truncated]
- 2026-07-09 implementer report: Task 0100 complete: the off-core merge no longer materializes the merged entry set — RAM on `compact_and_write` is O(CO_COMPACT_CHUNK + per-block state + block index + bloom), and full store+cluster
…[truncated]
- 2026-07-09 review tier: high-powered — reviewers: Claude
- 2026-07-09 review (Claude): accept — Task 0100 is fully and correctly implemented. The off-core merge path (compact_and_write) now pulls a MergeStream iterator in CO_COMPACT_CHUNK-sized chunks and feeds a push-style RunStreamBuilder over
…[truncated]
- 2026-07-09 decision: accept — commit: store: streaming merge writer (task 0100) — off-core merge streams MergeStream → chunked vlog rewrite → RunStreamBuilder, no O(total-keys) merged Vec; run format v3 (flat key-ordered AE sidecar,
…[truncated]
- 2026-07-09 usage: 146,823 tok (in 304, out 146,519, cache_r 25,987,285, cache_w 505,859) · $38.2013
  implementer: 116,858 tok (in 206, out 116,652, cache_r 23,180,555, cache_w 299,485) · $32.7588
  coordinator: 21,544 tok (in 34, out 21,510, cache_r 1,353,916, cache_w 125,959) · $4.0042
  reviewer:Claude: 8,421 tok (in 64, out 8,357, cache_r 1,452,814, cache_w 80,415) · $1.4382

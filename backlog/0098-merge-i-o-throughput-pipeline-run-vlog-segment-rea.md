---
id: "0098"
title: 'Merge I/O throughput: pipeline run/vlog segment reads at real QD, batch/cheapen CRC on the merge path'
status: done
priority: 4
created: "2026-07-09"
updated: "2026-07-09"
depends_on:
    - "0069"
spec_refs: []
---

## Description
Split from task 0069 (leg E). On the 1B ingest merges ran at 16–64 MB/s on 3 GB/s drives: QD1-ish completion polling ate ~50% of the maint thread and crc::update_hw up to 20.8%. After 0069's index-only tiered merges land, whatever rewrite volume remains (index bytes, victim-reclaim value copies, opt-in clustering) still pays this. Pipeline the off-core merge's segment reads at a real queue depth and batch/cheapen CRC work.

Acceptance criteria:
- Off-core merge read path issues overlapping I/O (measurable QD > 1) instead of read-then-decode lockstep; CRC cost reduced or overlapped.
- Measured merge input throughput improvement on hardware (or a documented microbench) vs the 0069 baseline; no correctness change (store suites green, merge read-transparent).

Sizing inputs: bench/t0088/notes.md (~0.19 GB/s under load, ~0.38 GB/s quiescent).

## Acceptance criteria

## Plan

Goal (task 0098): the off-core merge read path issues overlapping I/O (measurable QD > 1) instead of per-block read-then-decode lockstep; CRC cost reduced; no correctness change; improvement documented via microbench (box A/B rides on the 0097 replay baseline later).

Where the QD1 lockstep lives today: merge input runs are read via `RunScan::next` → one `read_at` per ~4 KiB data block, decoding between reads (the 16–64 MB/s path from bench/t0088); the 0100 sidecar read-back reads similarly small ranges. The vlog span reads (`SpanReadPlan::execute_sync`) already use `BlockDevice::read_batch` — reuse that primitive rather than adding async bounds to `compact_and_write` (which is generic over `D: BlockDevice` only).

Design:

1) Windowed, batched run reader (run.rs): a `RunBlockWindowReader` (name flexible) that streams a run's data-block region ([0 .. bloom_off)) sequentially with a read-ahead window: refills read `VALS_MERGE_READ_WINDOW_BYTES` (default ~2 MiB, block-aligned) as `VALS_MERGE_READ_QD` (default 8) sub-spans submitted in ONE `read_batch` call → real QD on SPDK, identical semantics on Mem/File devices via the default sequential fallback. Sub-spans must not cross segment boundaries (mirror `read_run_range_core`'s per-segment splitting; LBAs are discontiguous across segments); buffers via the same aligned-buffer helper the vlog uses (`io_buf`). It yields decoded blocks (or per-index-entry block slices) in order, using the in-RAM block index for block offsets/lens. RAM: k inputs × window ≈ 12 MiB at k=6 — trivial next to the Vec task 0100 removed.

2) Merge path rewiring (engine.rs): a new `MergeSource` impl backed by the window reader for `compact_and_write`'s inputs (the off-core path ONLY — serving-path `RunScan` and the sync `compact_runs`/`merge_all_runs` stay untouched). Kill switch `VALS_MERGE_READAHEAD=0` falls back to the existing per-block `ScanSource` (one-binary A/B on the box, and a field rollback — same pattern as VALS_MERGE_LEGACY / VALS_STAGED_BATCH_APPLY).

3) Sidecar read-back (run.rs, from task 0100): fetch large contiguous windows (~1–2 MiB) per `SegmentStreamSink::read_logical` call and decode the many blocks inside, instead of small per-block ranges. (Optionally batch sub-spans too if read_logical's flushed-segment path makes that natural; windowing alone removes the lockstep.)

4) CRC cheapening (crc.rs): 3-stream interleaved hardware CRC-32C for large buffers — split the buffer into three lanes folded with `_mm_crc32_u64` in one loop (breaking the 3-cycle latency chain), then recombine via a GF(2) shift/combine (zlib-style crc32_combine, table or clmul based). Keep: identical values to the scalar path (exhaustive equality tests across lengths/splits/alignments), scalar fallback unchanged, one-shot `crc32` and streaming `Crc32::update` both benefit (route large updates through the 3-way body, keep the simple path for small slices). This is self-contained; do it carefully with tests, or if the combine math proves risky, an acceptable fallback is to keep single-stream CRC and document that CRC is now overlapped with device time via the batched reads — but attempt the 3-way first.

5) Measurement (documented microbench, bench/t0098/notes.md): (a) an instrumented test device (wrapping MemBlockDevice, recording read_batch op counts) proving the merge path issues batches with ops > 1 and that merge output is byte/read-equivalent with readahead on vs off; (b) a local wall-time microbench of the merge input phase (vals-bench scanlsm/cocompact-style: build N runs, time compact_and_write) with `VALS_MERGE_READAHEAD=0` vs on, on a file device (large sequential reads still show a win at QD1) — record numbers + methodology; (c) CRC throughput microbench numbers (bytes/s single-stream vs 3-way). Note in the bench file that the hardware A/B vs the 0069 baseline lands with task 0097's replay run.

Tests: parity (readahead on/off produce identical merged runs — reuse 0100's parity harness), segment-boundary + tiny-run + window-smaller-than-block edge cases, QD assertion via instrumented device, CRC equality tests, full `cargo test --workspace` green.

Out of scope: serving-path read QD, flush-path writes, async trait bound changes.

### Starting points
- crates/vals-store/src/run.rs:2136-2188 RunScan::next (the QD1 per-block lockstep read)
- crates/vals-store/src/engine.rs ScanSource/MergeSource + MergeStream (task 0100) — swap in a windowed source on the off-core path only
- crates/vals-spdk/src/lib.rs:186-195 BlockDevice::read_batch + ReadOp (QD = batch size on SPDK; default sequential fallback)
- crates/vals-store/src/vlog.rs:538-566 execute_sync — existing read_batch usage pattern + io_buf aligned buffers
- crates/vals-store/src/run.rs read_run_range_core — segment-boundary splitting logic to mirror in the window reader
- crates/vals-store/src/run.rs RunStreamBuilder::finish sidecar read-back + SegmentStreamSink::read_logical (make it windowed)
- crates/vals-store/src/crc.rs update_hw (single-stream _mm_crc32_u64; 3-way interleave + combine is the classic speedup)
- bench/t0088/notes.md merge throughput sizing (~0.19 GB/s under load, ~0.38 GB/s quiescent)

## Work log
- 2026-07-09 plan: Goal (task 0098): the off-core merge read path issues overlapping I/O (measurable QD > 1) instead of per-block read-then-decode lockstep; CRC cost reduced; no correctness change; improvement documente
…[truncated]
- 2026-07-09 context hints: 8 recorded with plan
- 2026-07-09 context hints: crates/vals-store/src/run.rs:2136-2188 RunScan::next (the QD1 per-block lockstep read); crates/vals-store/src/engine.rs ScanSource/MergeSource + MergeStream (task 0100) — swap in a windowed source o
…[truncated]
- 2026-07-10 implemented: (1) `run::RunPrefetchScan` — off-core merge inputs read as 2 MiB read-ahead windows submitted as ONE `read_batch` of ~8 block-aligned ≤128 KiB sub-spans (QD>1 on SPDK; byte-identical on mem/file); wired into `compact_and_write` via `CompactSource` enum, kill switch `VALS_MERGE_READAHEAD=0` (+ `VALS_MERGE_READ_WINDOW_BYTES`, `VALS_MERGE_READ_QD`). (2) v3 sidecar read-back now windowed (1 MiB per `read_logical`, batched device part). (3) `crc::update_hw` 3-way interleaved for buffers ≥3 KiB with fixed-lane GF(2) recombine tables — 1.88× @4 KiB, 2.75× @1 MiB, identical values (exhaustive equality tests). Local microbench (bench/t0098/notes.md): raw merge-input read pattern 2.7–3.2× on file device; QD>1 proven via instrumented device; merged run byte-identical readahead on/off. Box A/B rides task 0097.
- 2026-07-09 implementer report: Task 0098 implemented in vals-store; full workspace suite green (debug + release), clippy clean for my code (3 pre-existing test lints untouched, verified via git stash).  **What changed**  1. **Windo
…[truncated]
- 2026-07-09 review tier: high-powered — reviewers: Claude
- 2026-07-09 review (Claude): accept — The change fully and correctly implements task 0098. It adds (1) a windowed batched read-ahead cursor (`run::RunPrefetchScan`) wired into the off-core `compact_and_write` via a `CompactSource` enum wi
…[truncated]
- 2026-07-09 decision: accept — commit: store: pipeline off-core merge reads at real QD + 3-way hw CRC-32C (task 0098) — RunPrefetchScan batched 2 MiB read-ahead windows (VALS_MERGE_READAHEAD kill switch), windowed v3 sidecar read-back, i
…[truncated]
- 2026-07-09 usage: 94,219 tok (in 236, out 93,983, cache_r 14,475,768, cache_w 394,491) · $23.5278
  implementer: 73,629 tok (in 184, out 73,445, cache_r 12,638,129, cache_w 191,503) · $18.7060
  coordinator: 14,854 tok (in 26, out 14,828, cache_r 1,480,773, cache_w 161,507) · $4.2413
  reviewer:Claude: 5,736 tok (in 26, out 5,710, cache_r 356,866, cache_w 41,481) · $0.5806

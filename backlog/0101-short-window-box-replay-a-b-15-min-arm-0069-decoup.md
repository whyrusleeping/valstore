---
id: "0101"
title: 'Short-window box replay A/B (~15 min/arm): 0069 decoupled merge vs VALS_MERGE_LEGACY baseline'
status: done
priority: 2
created: "2026-07-09"
updated: "2026-07-09"
depends_on:
    - "0069"
spec_refs: []
---

## Description
User-requested (2026-07-09): validate 0069's merge decoupling with a SHORT box A/B (~10–15 min per arm), not the multi-hour full-corpus replay (that stays task 0097).

Design: same binary (HEAD incl. 0069), two arms on identically fresh (TRIMmed, formatted) clusters on oldbox, replaying the same backfill .vdump subset under `--tier one`, with the memtable cap shrunk so many flush→merge cycles fit in the window (the merge regime, compressed):
- Arm A (decoupled, default env)
- Arm B (legacy, VALS_MERGE_LEGACY=1 = pre-0069 merge-all + co-compact-always)

Acceptance criteria:
- Both arms run ≥10 merge cycles per engine in the window; equal wall budget per arm.
- Record throughput-vs-progress (vals_records_written over time), run_merge counters (vals_run_merges, vals_run_merge_ms, vals_run_merge_input_bytes — noting the 0069 semantics change), run-count pileup, busy_rejections/server_errors (0 expected), replay errs (0).
- Signal: legacy arm shows superlinear cumulative merge time/traffic and declining apply throughput; decoupled arm quasi-flat at matched progress.
- Notes file in bench/ with exact corpus subset, knobs, and curves; committed.

## Acceptance criteria

## Plan

Short-window box replay A/B validating 0069 (user-requested ~10–15 min/arm), on oldbox, coordinator-driven.

Design:
- Corpus: dump-ab/ = the 8 backfill .vdump files each truncated to 7 GiB (~133M records total, ~56 GiB) — every arm replays the identical record stream to completion (truncation yields one deterministic benign read-error line per thread at EOF; final per-run totals + errs still print).
- Compressed merge regime so the 15-min window spans ~17 merge cycles/engine: FLUSH_TAIL_EPOCHS=100 (~100 MiB memtable at 1 MiB segments), RUN_COMPACT_THRESHOLD=4 (harness default, matches the 1B ingest), REGION_MIB=1 TiB (fits 5 zones even if drop-zone doesn't free; free_fraction stays ~1 ⇒ pure growth workload, no pressure merges — exactly the recurring-merge path 0069 changed).
- Arm A "decoupled": default env (index-only tiered merges). Arm B "legacy": VALS_MERGE_LEGACY=1 (pre-0069 merge-all + co-compact-always). Same binary (HEAD 3ef7318), fresh TRIMmed+formatted cluster per arm, `valsdump replay --in dump-ab/node --batch 1000 --tier one --r 1 --rf 1`.
- Measurement: collect.sh raw scrapes (obs/raw) give per-tick vals_records_written, vals_snapshots, vals_run_merges/_ms/_input_bytes/_output_bytes, batch_apply histograms, busy/server_errors; node logs give per-merge commit lines. Compare throughput-vs-progress at matched progress, cumulative merge time/traffic shape (legacy superlinear vs decoupled quasi-flat), wall time per arm, run pileup. Note the 0069 run_merge_input_bytes semantics change (legacy counts value bytes co-compacted; decoupled counts only bytes actually re-read) — report vals_run_merge_ms and output_bytes as the apples-to-apples merge-load measures alongside.
- Validation: errs=0 write errors both arms; server_errors 0; spot valsq count range equality between arms (same corpus ⇒ same final state).

Execution: per arm — sudo cluster.sh setup (TRIM); sudo env <knobs+arm env> ./cluster.sh start; chmod pd.sock; drop records/backlinks, create+split zone-1/zone-2 per runbook; start collect.sh/sys.sh; replay to completion (timeout ceiling 40 min); final metrics snapshot; archive obs/ + node logs + replay log per arm; cluster.sh stop + dpdk/hugepage cleanup. Afterwards: pull artifacts locally to bench/t0101/, compute curves, write bench/t0101/notes.md, self-review (simple tier: bench note + no product code), commit.

### Starting points
- plans/backfill-replay-runbook.md — zone-1/zone-2 header trap, splits, tier one
- bench/t0069-notes.md — switch table; VALS_MERGE_LEGACY=1 = legacy arm
- valsdump replay prints NO progress between header and final line — curves come from collect.sh obs/raw/node*-<ts>.txt (full /metrics bodies per tick)
- box: dump-ab/ = 8× head -c 7G truncated .vdump (~133M records total); truncation ⇒ one benign 'read error after N records' per thread at EOF, deterministic ⇒ arms replay identical records
- knobs both arms: FLUSH_TAIL_EPOCHS=100 (~100MiB memtable, 1MiB segments), RUN_COMPACT_THRESHOLD=4 (harness default), REGION_MIB=1048576 (1TiB; 5 zones fit even if drop-zone doesn't free)
- cluster.sh start must run as root: sudo env VALS_MERGE_LEGACY=1 FLUSH_TAIL_EPOCHS=100 REGION_MIB=1048576 ./cluster.sh start (sudo strips plain exports)

## Work log
- 2026-07-09 plan: Short-window box replay A/B validating 0069 (user-requested ~10–15 min/arm), on oldbox, coordinator-driven.  Design: - Corpus: dump-ab/ = the 8 backfill .vdump files each truncated to 7 Gi
…[truncated]
- 2026-07-09 context hints: 6 recorded with plan
- 2026-07-09 review tier: simple (coordinator self-review)
- 2026-07-09 decision: accept — commit: bench: short-window box replay A/B validates 0069 on hardware (task 0101) — same binary, equal 2400s wall, 8×7GiB truncated backfill corpus, compressed merge cadence (FLUSH_TAIL_EPOCHS=100): decoup
…[truncated]
- 2026-07-09 usage: 61,750 tok (in 132, out 61,618, cache_r 11,973,739, cache_w 667,833) · $23.4039

---
id: "0084"
title: Sorted-zone overwrite churn never compacts (vals_compactions=0, log grows unboundedly) — confirm bug vs harness artifact
status: done
priority: 4
created: "2026-07-07"
updated: "2026-07-07"
depends_on: []
spec_refs: []
---

## Description
Observed during task 0083's box measurement (bench/sorted-shard-scaling-box-notes.md, "Task 0083" section, WRITE footnote): on a sorted zone (rf=1, gc_grace=0, 500k keys × 64B, 8 GB region), valsmix's sustained in-place LWW rewrite churn drove `vals_live_keys` from 500k to ~1.5M and `used_segments` to ~6.2 GB of the 8 GB region with `vals_compactions=0` — the value log grew unboundedly under pure point-overwrite churn (no inserts/deletes) and write throughput collapsed within seconds on repeated bursts.

If a production sorted zone under steady overwrite churn never triggers compaction/run-merge until the region fills, that's a latent operability bug (a zone can wedge on writes despite a constant live-key working set). It may instead be a harness artifact (gc_grace=0 / sweep config suppressing the merge path, or thresholds simply not yet reached) — that needs confirming either way.

## Acceptance criteria
- Reproduce (file/mem backend is fine) a sorted zone under sustained in-place overwrite churn and determine whether compaction/run-merge ever reclaims superseded versions before the region exhausts, under a production-representative config (sweep on, gc_grace > 0).
- If it reclaims under production config: record the 0083 observation as a harness-config artifact (note in bench/sorted-shard-scaling-box-notes.md) and close.
- If it does not: fix or file the design work so overwrite churn triggers reclamation before write throughput collapses (relate to task 0069's run-merge work), with a regression test.

## Work log

- 2026-07-07/08: **Confirmed real bug (not a harness artifact) and fixed.** Full narrative in
  `bench/t0084/notes.md`; repro harness `bench/t0084/run.sh`.
  - **Diagnosis:** for a flushed zone the only vlog reclaim path is run-merge co-compaction,
    and every trigger feeding it was absolute-byte/cadence-denominated (flush at 256 tail
    epochs, merge at 8 runs ≈ 2 GiB/cycle, checks on the sweep-derived tick) with no region-
    pressure coupling; the legacy vlog compactor skips flushed zones. Overwrite churn's
    garbage rate is the full write bandwidth → region fills → at 0 free segments
    `flush_plan`/`compact_plan` can't reserve outputs → **permanent write wedge**. Local
    repro (3-node file cluster, scaled 256 MiB region): wedged in ~9 s, and with
    `RUN_COMPACT_THRESHOLD=2` the merge became *due* but could no longer *plan*. This is
    also the kvchurn "write rate sinking to ~17-31k/s" seen on the box (`/tmp/bw_*.log`:
    bursts 865k → 385k → 92k → 31k/s). Two observability gaps masked it: run-merge commits
    counted as `vals_snapshots`, and their frees were NOT added to `vals_segments_freed`.
  - **Fix:** region-pressure trigger `zone_maint_due` (node.rs, pure + unit-tested): free
    ≤ 50% AND `Engine::lsm_dead_fraction()` ≥ 10% ⇒ flush any non-empty tail + run-merge
    from 2 runs up; merge planned BEFORE flush in the tick (its bigger reservation gets the
    headroom — the 25%/flush-first first cut lost the race on 1 of 3 nodes); flush-check
    cadence clamped ≤ 1 s. New `Engine::free_fraction()`/`lsm_dead_fraction()` (honest dead
    estimate for flushed zones via run `value_bytes` + memtable `live_bytes`). Metrics:
    `vals_run_merges` (unconflated), merge frees → `vals_segments_freed`,
    `vals_pressure_flushes`/`vals_pressure_merges`.
  - **Regression:** `vals-cluster/tests/churn_reclaim.rs` — ~3× region of overwrite churn at
    production-default thresholds; kill-the-fix verified both ways (pressure terms reverted
    ⇒ wedge ⇒ test fails). Post-fix the 9-s-wedge repro runs 40 kvchurn rounds to ALL CHECKS
    PASSED, `used_segments` sawtooths ≤ ~130/256, `run_merges`=5, `segments_freed`=414.
  - **Follow-ups filed:** task 0085 (guaranteed maintenance headroom — residual race can
    still fill the free half within one trigger→commit window), task 0086 (hot-key vlog
    leak hypothesis: segments referenced by no run are never freed by co-compaction).
  - **Box validation on oldbox (2026-07-08, SPDK topology A): PASS** — details in
    notes.md §Box validation. Production shape (8 GiB region, 300 s sustained 1 KiB churn,
    ~1.75× region appended): 47.7k writes/s sustained with **no sinking trend**, used
    sawtooths 3043→855 / 4207→1508 segs, `run_merges=5`, `segments_freed=13316` (~13 GB),
    0 errors, consistency clean; kvchurn value-verify ALL CHECKS PASSED. Also explained the
    pre-fix 0083 numbers: at sweep=10s the old flush check ran every **40 s**, so the whole
    burst session never produced enough runs to merge — the 1 s cadence clamp is the
    decisive piece at that config. A deliberately tiny 512 MiB region at ~250 MB/s churn
    reproduced the 0085 residual regime live (fills within one tick → wedge incl. legacy
    compactor `headroom_skips`) — evidence attached to task 0085.

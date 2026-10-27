---
id: "0417"
title: '0391 L2a remainder: on-box-timed perf captures + preregistered shadow-refresh A/B as a short continuation on the retained R3 corpus'
status: done
priority: 3
created: "2026-10-10"
updated: "2026-10-12"
depends_on:
    - "0404"
    - "0418"
spec_refs: []
---

## Description

This is the L2a remainder of 0391, split out by validation amendment item 2 (owner-approved 2026-10-08). 0391 completed the L0 attribution from retained SOAK5B-V5-R3 data (`bench/0363-compact-values/DECAY-R3-0391.md`). Two required measurements are still **unconfirmed**, because they need a short continuation on the retained R3 corpus through 0404's from-corpus mode, which isn't done yet:

1. **New perf captures with true on-box endpoints.** R3's existing captures recorded only the pre-SSH launch time and the return time (9.5–9.7 s apart around an 8 s capture). 0391 bounded each window both ways; cycles/write differ by ≤ 7% between the bounds. Take cpu_core leaf captures plus fp/lbr call graphs if the build has frame pointers. Record on the box: the capture start/end (`date +%s%N` around `perf record`) and the scrape counter endpoints that bracket them.
2. **Shadow-refresh A/B at the real ≤60%-free trigger.** This is 0382 item 1 / 0385 fix #3. Every R3 engine is below 60% free.
   - Arm A: the current incremental refresh.
   - Arm B: `VALS_SHADOW_EST=0`. This also disables shadow-fold planning, so record merge kinds and disk.
   - Same corpus state and binary, order A, B, then A again, ≤ ~1 h each, whole-arm distributions.
   - Primary signal: shard µs per synced write.
   - Secondary signals: `vals_shadow_refresh_cpu_ns_total` per write, and ACK/s.

**0391's L0 prediction (registered before any run):** refresh recompute CPU is ≤ 0.6% of shard CPU, inclusive, at 6–10B in R3, so the A/B should be null. A short hardware A/B can't resolve differences that small (≈ 13% run-to-run variance). If the owner agrees that this ceiling settles item 2, this task can be closed without running. Otherwise run it as the cheapest piggyback on the first planned R3 reopen with a write continuation, and do not schedule a dedicated campaign (validation.md, task-sized contracts).

## Outcome

Both acceptance items met. Item 1: 0418 step 5 took nine node1 lbr captures (c03–c11), each with on-box date +%s%N, vals_synced_writes and shard ticks around perf record (receipts /data/dataset/task0363-results/R3-SESSION-0418/perf/perf-receipts.jsonl, re-verified: c03 41 s after bootes start, 571k writes/s, 11.28 shard-µs/write; c11 near EOF, 12.52 µs/write). RangeMap::clone from mirror_batch_response 19→24% inclusive, recorded for 0420. Item 2: owner decision (2026-10-10) that 0391's L0 ceiling (≤0.6% of shard CPU) settles the shadow-refresh A/B; not run. docs/validation.md 0391 row and R3 ordering note updated.

Commit: docs(0417): close 0391 L2a remainder — on-box-timed captures from 0418 step 5; shadow-refresh A/B settled by owner via 0391 L0 ceiling

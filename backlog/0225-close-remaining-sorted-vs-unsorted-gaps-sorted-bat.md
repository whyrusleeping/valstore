---
id: "0225"
title: 'Close remaining sorted-vs-unsorted gaps: sorted batched-read ceiling + reads-under-churn diagnosis'
status: done
priority: 1
created: "2026-08-19"
updated: "2026-08-19"
depends_on:
    - "0224"
spec_refs: []
---

## Description

Task 0222 showed sorted zones at parity with unsorted for quiescent point reads (1.498M vs 1.364M/s) and bulk load (89%), leaving three gaps: scans under churn (fixed by 0223, hardware gate = 0224), sorted point reads under churn (225.8k vs 971.2k/s, 4x, undiagnosed), and the sorted batched-read ceiling (never measured; unsorted netmultiread does 3.9-5.18M keys/s).

Run in the same lab session as 0224 (same 20M×256B 24-canonical-range corpus, latency profile):

- **Arm B — sorted batched-read ceiling:** valsmix `--batch-reads` (`read_many_in`) against the sorted zone, quiescent and at ~60k bg writes/s, matched against the identical unsorted shape. `read_many_in`/digest fanout are zone-generic so no code should be needed; verify shard routing for sorted range tokens behaves under batching.
- **Arm C — reads-under-churn isolation:** sorted point reads with writers on the 0223-fixed binary. Capture per-shard thread CPU deltas (`/proc/PID/task/*/stat`, per the 0169 method) to detect range-token shard skew (24 ranges hashed onto 12 node-shards), staged-read gate occupancy, flush/run-merge activity, and the four 0223 scan counters. Name the mechanism behind the 4x gap.

## Acceptance criteria

- Sorted batched-read numbers (quiescent + churn) recorded next to unsorted at matched shape; if sorted reaches the same knee, record parity as the finding.
- The sorted reads-under-churn gap is either shown closed by the 0223 fix, or its mechanism is identified with evidence (per-shard CPU / gate / merge data) and a fix task filed.
- Findings artifact in bench/, cross-referenced with 0224.

## Outcome

Gaps resolved on the preserved 0222/0224 lab corpus (bench/lab-sorted-gaps-20260820-0225/FINDINGS.md, no source changes). Arm B: sorted batched-read ceiling at quiescent parity (2.201M keys/s @ width 256 vs unsorted 2.322M @ 512, −5.2% at knees, 0 errors; read_many_in sorted routing verified); matched ~90k wr/s churn 846k vs 936k/s, both clean. Arm C: 0222's 4× gap CLOSED at writers=2 (811k vs 783k rd/s) but persists at writers=8 (108–135k vs 309–362k, repeatable both orders); mechanism = coarse canonical placement: 48 replica slots land 18/18/12 → deterministic 37.5/37.5/25% write split (1.50× hot/cold vs 1.03%), shard CPU max/mean 1.44–1.51 with idle shards, p99 65 vs 16 ms; merge/scan alternatives excluded, read-gate noted unobserved. Fix task filed: 0226. 105-min warm-up honored; cluster left stopped, format=false, corpus intact; backlog.md regenerated.

Commit: bench: sorted batched reads reach parity; high-churn sorted read gap diagnosed as replica placement skew, fix filed as 0226 (0225)

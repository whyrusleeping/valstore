---
id: "0428"
title: '0328 L1: shuffled census window order (verify --census-order) + local pacing harness and findings'
status: done
priority: 4
created: "2026-10-11"
updated: "2026-10-11"
depends_on: []
spec_refs: []
---

## Description

Split from 0328 so the L1 result and client change land before 0418. 0328 keeps the L2a census A/B on the drained R3 corpus (0418 optional slot) and its final keep/remove decision.

Scope:
- real-client15 `verify --census-order interleaved|shuffled` (default interleaved, receipt `census_order`), per-zone client Scan latency histogram `scan_micros_log2`; `scanbench` subcommand (synthetic load + census scan with exact count/digest).
- bench/0328-scan-pacing: run_l1.py (24 single-shard file-backed nodes, spread/grouped/inherited layouts), analyze_l1.py, results, README with the decision, l1-file-read-yield.patch (experiment-only server patch so file-backed shards interleave scans like SPDK).

Acceptance:
- L1 arms with exact count/digest correctness gate on an R3/F4-like layout, decision per the 0328 rule (Busy ≥10× fewer at keys/s ≥ A) recorded.
- Measured-negative option (jitter backoff) not shipped.
- real-client15 go test passes; default census behaviour unchanged.

## Outcome

real-client15 verify gains --census-order interleaved|shuffled (default unchanged) plus receipt census_order and per-zone scan_micros_log2; scanbench subcommand and bench/0328-scan-pacing (run_l1.py, analyze_l1.py, results for spread/grouped/inherited layouts, file-read-yield experiment patch, README). L1 on 24 file-backed shards, 170M keys, exact count/digest gate: grouped (R3/F4-like) A 587k keys/s 6.6k Busy vs shuffled 48w 1.70M (2.9x) 632 Busy (10.5x fewer) vs shuffled 24w 1.53M (2.6x) 10 Busy; jitter backoff 0.92x with more Busy (removed); 6 workers 0 Busy at 0.67x. Cause: key-ordered windows pile onto one inherited-token group's shards. 0328 returned to todo for the 0418 hardware A/B (A vs --census-order shuffled --census-workers 24).

Commit: bench(0328): L1 census pacing — shuffled window order (verify --census-order) 2.9x keys/s, 10.5x fewer Busy on R3-like layout; jitter rejected; 0328 keeps 0418 A/B

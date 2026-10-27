---
id: "0486"
title: Measure aged short-scan run-count scaling (0483 Step 1)
status: done
priority: 3
created: "2026-10-17"
updated: "2026-10-17"
depends_on:
    - "0481"
spec_refs: []
---

## Description

Necessary measurement-only split of accepted task 0483. Parent explicitly reserves selection of an optimization to the owner after measurements; this task does not authorize choosing or implementing one.

Acceptance:
- Commit Step 1 FINDINGS measuring run cursors, run blocks/bytes and value-log reads per short adjacency scan plus p50/p99 across low, medium and >=50 runs/engine (aim roughly 5/25/50/100; report actual counts).
- Use the 0481 graph/counters, fixed graph/query seeds and correctness checks; retain reproducible commands, settings and concise result artifacts.
- Inspect the serving path and establish whether initial run-block reads overlap or are serial; distinguish it from synchronous scan_winners_page and merge prefetch_scan.
- Summarize measured evidence and owner decision still required for parent 0483, without claiming the optimization or its A/B criteria complete.

## Outcome

Completed measurement-only Step 1 of 0483: opt-in exact adjacency probe, ten file-backed arms spanning 5–148 ge runs/engine, retained findings/receipts and serving-path concurrency analysis. Confirmed ~one block per overlapping run/replica; p99 ~2.45 ms at low counts versus 5.48–7.86 ms at high counts. Release build, 42 benchmark tests, 10,000 exact pages and all exhaustive pair checks pass; independent reviewer rebuilt and repeated a high-run arm successfully, accepted. Parent 0483 remains blocked on its explicit owner approach-selection gate; implementation/write-neutrality/kill-the-fix criteria are not claimed.

Commit: bench(0486): measure aged short-scan cost across run counts

---
id: "0483"
title: 'Short scans on aged engines read one block per overlapping run: make adjacency-sized scans cheap'
status: blocked
priority: 3
created: "2026-10-17"
updated: "2026-10-17"
depends_on:
    - "0481"
spec_refs: []
---

## Description
## Why
`Engine::scan_winners_page` (engine/scan.rs) opens a cursor on every run whose `[min_key, max_key]` overlaps the window. Each cursor seeks through the resident fence index (`block_scan`) and reads and CRC-checks one data block (~20–32 KiB, `VALS_RUN_BLOCK_TARGET_BYTES`) before it knows whether the window holds any of its keys.

Data written in arrival order gives every run an extrema range spanning the whole keyspace, so nearly every run overlaps every window. Run blooms are full-key only and don't help scans.

Since 0381 the frozen tier grows ~linearly with data ÷ merge cap (~100 runs/engine at mature scale). A 10-edge adjacency scan can therefore cost tens to ~100 block reads (MiBs of device reads) plus a value-log read per entry. Bulk scans amortise this; graph neighbour lookups, "who references X" backlink scans and queue head reads don't. Reasoned from code, not measured yet.

## Step 1: measure (0481)
Using 0481's per-scan counters, report run cursors opened, blocks read and value-log reads per short scan, and p50/p99 latency, as a function of runs/engine (e.g. 5 / 25 / 50 / 100). Also check whether opening the cursors issues block reads concurrently (`prefetch_scan`) or one at a time on the serving path. Confirm or refute the cost model before designing anything.

## Step 2: candidates (owner picks after step 1)
- **Per-run prefix filter** (RocksDB prefix-extractor style):
  - A per-zone prefix rule (fixed length, or up to the k-th delimiter byte) goes in the zone catalog's reserved `flags`/options.
  - Flush and merge build a bloom over the distinct prefixes. A scan whose window lies within one prefix probes each run and skips the misses.
  - Cheap in RAM: bits per distinct prefix, not per key.
  - Needs an optional run block, so a run-format bump plus a downgrade fence (formats.md §2.2a; see the 0450 rules). Flush/merge CPU cost must be measured (write throughput is the top priority).
- **Range filter** (SuRF/Rosetta-like): no prefix rule needed, but heavier to build and hold.
- **Key-range-partitioned frozen runs** (leveled-style split outputs): extrema skipping then makes a short window overlap O(levels) runs, not O(runs). This is the structural fix, but a large compaction change that interacts with 0381/0383 caps and folds.
- **Engine-wide prefix filter** like the 0364 key filter: only skips covered runs when the prefix is absent everywhere, so useful for absent-vertex checks only. Listed for completeness.
- **Value side:**
  - keys-only scans (the batched-scan task) remove value-log reads entirely where the data is in the key;
  - check how much 0028 key-ordered co-compaction still helps once runs freeze (only space-reclaim rewrites touch frozen runs).

## Acceptance
- Step 1 FINDINGS committed with the measured per-scan cost model.
- The owner picks an approach, or rejects all of them if the measured cost is small.
- The chosen approach is implemented with tests, including kill-the-fix.
- 0481 A/B at ≥ 50 runs/engine shows a material drop in blocks read per short scan and in p99, with neutral write-path cost on the A/B (flush/merge CPU, ACK/s). Use lab confirmation if SPDK behaviour matters.
- Any format change follows the downgrade-fence rules and is documented (formats.md, deploy.md, ARCHITECTURE.md).

## Acceptance criteria

## Work log

- 2026-10-17: Step 1 split into accepted measurement task **0486**. 0481 and 0482 findings inspected: aged mixed scans already show ~one run block/cursor, but isolated short-scan scaling across run counts was not measured. 0486 adds that sweep and serving-path inspection. Step 1 results: `bench/0483-short-scans/FINDINGS.md` and `results.json`; ten arms span 5–148 ge runs/engine, all exact probe/pair checks pass. Short scans read ~one block per overlapping run per replica; p99 rises from ~2.45 ms to 5.48–7.86 ms. Initial SPDK reads are already concurrent (`join_all`), file reads synchronous. **Blocked on owner decision:** choose a Step 2 approach (or explicitly reject the candidates) after reviewing these findings. No optimization is selected or authorized by the measurement split. Implementation, kill-the-fix, >=50-run improvement/write-cost A/B and any necessary format documentation remain unmet. Unblock by recording the owner's choice and returning this task to todo/in_progress; do not treat completion of 0486 as completion of 0483.

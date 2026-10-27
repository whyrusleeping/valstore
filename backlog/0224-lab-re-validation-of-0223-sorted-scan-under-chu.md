---
id: "0224"
title: lab re-validation of 0223 sorted-scan-under-churn fix
status: done
priority: 2
created: "2026-08-19"
updated: "2026-08-19"
depends_on:
    - "0223"
spec_refs: []
---

## Description

Task 0223 fixed the sorted-scan-under-churn collapse found in 0222 (scan validity checks restarted on `mutable_gen`, which bumps every put; now storage-gen fencing + per-key finalize). Local file-backed proof was mechanism-level only (bench/local-scan-churn-20260819-0223/FINDINGS.md): the file backend is CPU-contention-bound and cannot reproduce the SPDK deadline-miss storm, so the hardware acceptance number is still owed.

Re-run the 0222 sorted-scan arms on lab (latency profile, 4 shards/node, 20M×256B sorted `smix` zone, 24 canonical ranges — recipe in bench/lab-sorted-vs-unsorted-20260819-0222/FINDINGS.md; remember `split-zone ... canonical`):

- P4 probe: scans width 512, 24 readers × pipeline 64, writers=2 (~60k bg writes/s) and writers=0.
- Optionally valsmix flow 3 (sorted scans under churn) and flow 4 (mixed 70r/20s/10w).
- Capture the new counters: vals_scan_staged_restarts, vals_scan_sync_fallbacks, vals_scan_span_retries, vals_scan_finalize_patches.

## Acceptance criteria

- ≥ ~2k scans/s clean at ~60k bg writes/s (same order as the 2026-08-06 baseline of 2,621/s), no deadline-miss storm, 0 consistency errors, ~0 staged restarts/sync fallbacks in the counters; OR a documented finding of what still limits it.
- Findings artifact in bench/, referenced from this task.

## Outcome

0223 fix ACCEPTED on lab hardware (bench/lab-scan-fix-20260819-0224/FINDINGS.md). Same 20M×256B smix corpus/probes as pre-fix 0222: at matched ~57k bg writes/s scans went 353/s + 25,960 deadline misses → 2,483/s + 0 misses (7.0×, 95% of the 2026-08-06 2,621/s baseline), 0 consistency errors; mechanism counters show 0 sync fallbacks everywhere, restarts in the low tens, finalize patches ~0.003-0.01% of rows. Flow 3 (151k churn): 1,612 scans/s vs 207 pre-fix; flow 4 mixed: 121.5k rd + 538 sc + 17.5k fg wr vs 55.4k/55/8k. Caveat documented: first ~60-90 min after re-opening the corpus are environmentally depressed (boot maint compaction + AE + suspected drive GC) — warm before measuring. Cluster left stopped, corpus intact for 0225.

Commit: bench: lab re-validation accepts the 0223 sorted-scan-under-churn fix (0224)

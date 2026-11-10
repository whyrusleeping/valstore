---
id: "0568"
title: 'Point reads on v7 run blocks are bound by zstd decompression (cloud v1: ~60% of node CPU, 72k reads/s vs 204k engine ceiling)'
status: proposed
priority: 3
created: "2026-11-10"
updated: "2026-11-10"
depends_on: []
spec_refs: []
---

## Description
Evidence: first AWS run (task 0567, bench/cloud/runs/v1-20261110/FINDINGS.md): i4i.2xlarge, 1 drive, 4 SPDK partitions, RF 1, artifact 65d1d16 (run format v7 default from 0564). Strong point reads 72k/s (p50 2.7 ms) while vals-bench mdread on the same drive does 204k/s (AWS 200k IOPS cap). perf during reads: ZSTD_decompressSequences 48.8% + buildFSETable 5.9% + decompressMultiFrame 4.4%, _int_malloc 6%. Device reads ~16 KB per key read (smart-log delta), i.e. each probe reads and decompresses a whole run block. 0564 accepted "run point probe ~3 -> ~20 us CPU"; on a read-heavy workload that is the ceiling.

Proposed: (1) A/B the same read steps with VALS_RUN_WRITE_VERSION=6 to confirm attribution (local L1 or the v1 cloud profile, ~$0.50); (2) if confirmed, options: smaller compressed blocks or a per-block sub-index so a probe decompresses less, a small decompressed-block cache, or keeping hot/recent runs uncompressed; measure point-read CPU and B/write together since 0564 was kept for write throughput.

## Acceptance criteria

## Work log

---
id: "0568"
title: 'Point reads on v7 run blocks are bound by zstd decompression (cloud v1: ~60% of node CPU, 72k reads/s vs 204k engine ceiling)'
status: done
priority: 3
created: "2026-11-10"
updated: "2026-11-10"
depends_on: []
spec_refs: []
---

## Description

Evidence: first AWS run (task 0567, bench/cloud/runs/v1-20261110/FINDINGS.md): i4i.2xlarge, 1 drive, 4 SPDK partitions, RF 1, artifact 65d1d16 (run format v7 default from 0564). Strong point reads 72k/s (p50 2.7 ms) while vals-bench mdread on the same drive does 204k/s (AWS 200k IOPS cap). perf during reads: ZSTD_decompressSequences 48.8% + buildFSETable 5.9% + decompressMultiFrame 4.4%, _int_malloc 6%. Device reads ~16 KB per key read (smart-log delta), i.e. each probe reads and decompresses a whole run block. 0564 accepted "run point probe ~3 -> ~20 us CPU"; on a read-heavy workload that is the ceiling.

Proposed: (1) A/B the same read steps with VALS_RUN_WRITE_VERSION=6 to confirm attribution (local L1 or the v1 cloud profile, ~$0.50); (2) if confirmed, options: smaller compressed blocks or a per-block sub-index so a probe decompresses less, a small decompressed-block cache, or keeping hot/recent runs uncompressed; measure point-read CPU and B/write together since 0564 was kept for write throughput.

## Outcome

Attribution confirmed on a local one-shard L1 A/B (bench/0568): strong point reads 42k/s with one-frame v7 blocks (cloud v1 layout), 70k with 0570's 16 KiB frames, 119k at 8 KiB, 125-131k with v6; ~1 run probe/block per read, shard-thread bound. Frames are the sub-index option; block cache and uncompressed hot runs rejected. Default stays 16 KiB (8 KiB ≈ +2% device B/write on insert-only N2 for no gain); deploy.md documents read numbers and the trade. Per-run zstd dictionaries proposed as 0571.

Commit: 0568: point-read A/B confirms zstd attribution (L1 one shard: 42k one-frame, 70k 16 KiB frames, 119k 8 KiB, 125-131k v6); default stays 16 KiB, deploy.md gives read numbers and 8 KiB insert-only cost; dictionaries proposed as 0571

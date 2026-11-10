---
id: "0571"
title: 'Per-run zstd dictionaries for v7 run blocks: 16 KiB-frame bytes at better-than-8 KiB probe CPU'
status: proposed
priority: 3
created: "2026-11-10"
updated: "2026-11-10"
depends_on: []
spec_refs: []
---

## Description
Evidence (0568, 0570): run probes (point reads, capacity-profile overwrites) are bound by zstd-decompressing the routed frame. L1 one shard: point reads 42k/s (one 32 KiB frame), 70k (16 KiB frames, default), 119k (8 KiB), 125-131k (v6 uncompressed); overwrites 42k / 71k / 116k. 8 KiB frames cost +9-11% run bytes vs 16 KiB on real N2 blocks (~2% device B/write on insert-only N2 with no benefit), so the default stayed 16 KiB. On sampled N2 blocks (bench/0570/dict-z2.txt) a per-run trained dictionary (<=16 KiB, ~17 ms training per MiB of samples) gives 8 KiB frames 4.8 us/frame at ratio 0.321 (incl. dict) vs plain 16 KiB 10.3 us at 0.318 and plain 8 KiB 6.4 us at 0.342.

Proposal: run format v8 storing a trained zstd dictionary per run (built from the first blocks' bodies), frames compressed with it; readers hold a DDict per open run and pass it to every block decode path (~26 call sites: lookup, cursors, scans, merge streams, version walkers, encode check). VALS_RUN_WRITE_VERSION=7 rollback. Measure: L1 read/overwrite rates, runbench bytes/lookup/build/scan on real N2 blocks, RSS per run, then an N2 A/B for device B/write and ingest time (keep only if N2 is no worse than 16 KiB frames).

## Acceptance criteria

## Work log

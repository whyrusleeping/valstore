---
id: "0570"
title: 'Make capacity-profile overwrites cheaper (cloud v1: overwrites 239k/s vs fresh inserts 803k/s, bound by run probes)'
status: done
priority: 2
created: "2026-11-10"
updated: "2026-11-10"
depends_on: []
spec_refs: []
---

## Description

Owner request (2026-11-10): make overwrites faster.

## Outcome

Attribution: ~2/3 of overwrite CPU was zstd-decompressing a whole 32 KiB run block per probe (L1 one shard: fresh 700k/s vs overwrites 42k/s; v6 137k/s; cloud v1 overwrite phase 51-96k/s). Smaller default blocks (fence RAM), LZ4, negative levels, stop-early streaming rejected on numbers. Kept: compressed v7 blocks split into ~16 KiB zstd frames (VALS_RUN_BLOCK_FRAME_BYTES, 0 = one frame) restarting front-coding, with a CRC-checked separator index in a zstd skippable frame; still v7, older v7 readers decode it. Lookups decompress the routed frame, seeks decode from it on, build verifies the index. L1 overwrites 41-43k -> 70-71.5k/s (8 KiB: 116k), inserts unchanged; N2-block probes 20 -> 12 us, short scans faster, engine full scan unchanged, run bytes +5-7%, build CPU +11%, RSS unchanged. Kill-the-fix tests, fuzz, docs. Hardware/N2 confirmation not done.

Commit: run blocks (0570): framed v7 blocks, ~16 KiB zstd frames behind a CRC-checked separator index in a skippable frame; point probes decompress one frame; one-shard overwrites ~42k -> ~71k/s, +5-7% run bytes, no format bump

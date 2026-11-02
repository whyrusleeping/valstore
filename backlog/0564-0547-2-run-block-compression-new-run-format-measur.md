---
id: "0564"
title: '0547.2: Run-block compression (new run format): measure, implement, N2 A/B'
status: done
priority: 2
created: "2026-11-02"
updated: "2026-11-02"
depends_on: []
spec_refs: []
---

## Description

Child of 0547 (item 2). Runs are ~43% of N2 node1 device bytes (flush 98 + merge 125 of 522 B/write). A run entry is 48 B of fixed fields (shared/suffix u32s, segment u64, offset/len u32, seq u64, version u128) plus the key suffix (~96 B/entry observed), so a compact entry encoding and/or per-32 KiB-block LZ4/zstd (lz4_flex/zstd crates are in the local registry cache but not workspace deps; box builds must resolve them) could shrink runs a lot.

Acceptance (from 0547): measure the achievable ratio on real run blocks first; new run format with decompression on read/merge/scan paths and format negotiation/rollback as for earlier run-format versions; old runs stay readable; measure B/write, CPU per write, point-read and scan cost; N2 A/B; keep or reject with numbers; kept work ships with docs and tests.

## Outcome

Real N2 run blocks: zstd 1 leaves 29% (LZ4 43%). v7 default: zstd-1 data blocks (self-identifying, checked by decompress-and-verify before write), uncompressed-length index trailer; block coding on VALS_RUN_CODEC_THREADS (4) threads; merge policy and reservations on uncompressed lengths; streams reserve before decompressing. v4-v6 readable, pre-v7 binaries refuse v7, VALS_RUN_WRITE_VERSION=6 rolls back. Same-binary N2 A/B: 4.5B 3,332.9 vs 3,931.4 s (-15.2%), both PASS; node1 device B/write 445.7 -> 328.2, flush runs -70%, merge runs -61%, merge CPU +6.8%, RSS 40.7 -> 22.7 GB. Costs: run point probe ~3 -> ~20 us CPU, scans 3-5x CPU. Docs and tests included.

Commit: 0564: run format v7 (zstd run blocks) default; N2 4.5B -15.2% vs same-binary v6, node1 device B/write 446 -> 328 (-26%); v6 via VALS_RUN_WRITE_VERSION=6

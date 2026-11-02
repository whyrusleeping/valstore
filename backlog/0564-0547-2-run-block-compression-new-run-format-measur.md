---
id: "0564"
title: '0547.2: Run-block compression (new run format): measure, implement, N2 A/B'
status: todo
priority: 2
created: "2026-11-02"
updated: "2026-11-02"
depends_on: []
spec_refs: []
---

## Description
Child of 0547 (item 2). Runs are ~43% of N2 node1 device bytes (flush 98 + merge 125 of 522 B/write). A run entry is 48 B of fixed fields (shared/suffix u32s, segment u64, offset/len u32, seq u64, version u128) plus the key suffix (~96 B/entry observed), so a compact entry encoding and/or per-32 KiB-block LZ4/zstd (lz4_flex/zstd crates are in the local registry cache but not workspace deps; box builds must resolve them) could shrink runs a lot.

Acceptance (from 0547): measure the achievable ratio on real run blocks first; new run format with decompression on read/merge/scan paths and format negotiation/rollback as for earlier run-format versions; old runs stay readable; measure B/write, CPU per write, point-read and scan cost; N2 A/B; keep or reject with numbers; kept work ships with docs and tests.

## Acceptance criteria

## Work log

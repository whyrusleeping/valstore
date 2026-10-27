---
id: "0414"
title: '0390 follow-ups: remaining pre-step shard growth callers (KeyFilter::excludes on apply, per-shard mallinfo2 per sweep, shard-side flush snapshot)'
status: done
priority: 3
created: "2026-10-10"
updated: "2026-10-13"
depends_on:
    - "0390"
spec_refs: []
---

## Description

Split from 0390 (bench/0390-shard-cpu/FINDINGS.md). After the 0390 victim-list cache, the one-shard L1 model still grows +1,684 shard cycles/write from 25M to 167M node writes (≙ R3 300M → 2B ACK):

1. `probe_version_for` +593: `KeyFilter::excludes` +432 (R3 2B leaf: +508), run-block reads after bloom positives +139. Attribute what grows inside `excludes`: sub-filter count, cache misses, or the hash. Then run a preregistered L1 A/B with `bench/0390-shard-cpu/arm.sh`.
2. R3 `__GI___libc_mallinfo2` 104 → 354 cycles/write (300M → 2B). Every shard calls `mallinfo2` once per sweep (`ServerShared` gauge publish, `net/server/shared.rs`). Each call walks every process arena under its lock, so the cost scales with shards × heap fragmentation, and the call can stall other threads' malloc. Candidate: publish once per node, or less often. The one-shard model cannot show the 8× factor; needs an 8-shard file-backed node or an L2 piggyback.
3. `snapshot_loop` +253 (flush planning/snapshot on the shard).

Acceptance: each item has a measured attribution and either an A/B verdict (fix kept with evidence, or the path removed as neutral) or a recorded reason it doesn't matter.

## Outcome

All three items attributed with verdicts (bench/0414-shard-growth/FINDINGS.md). (1) excludes growth is page-walk cost as the filter outgrows caches/TLB, not chain length; sub-filters ≥ 2 MiB now use 2 MiB-aligned MADV_HUGEPAGE mappings. Preregistered L1 A/B (4 arms to 341M): excludes −25/−30%, walk_active in it −92%, shard µs/write 167–333M −3.1% net of item 2; kept (RAM snapshot bound missed by 0.2 pt from rebuild phase; time-averaged RSS −3.2%). (2) mallinfo2 (~14 ms per shard per sweep) now sampled at metrics render, ≤ every 4 s, never on a shard; kept, 8-shard size is a hardware piggyback. (3) snapshot_loop flat (+74 cycles/write, below noise): no A/B. Tests pass (0431 flake rerun ok); comprehensive review accepted.

Commit: perf(0414): huge-page key-filter sub-filters and allocator gauges off the shard; L1 4-arm A/B: excludes −25…−30%, page-walk cycles in it −92%, shard µs/write −3.1% over 167–333M; snapshot_loop attributed flat

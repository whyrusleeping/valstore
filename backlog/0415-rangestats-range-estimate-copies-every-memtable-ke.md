---
id: "0415"
title: RangeStats range_estimate copies every memtable key in each range on each PD poll (O(memtable) allocations per 5 s per engine)
status: done
priority: 3
created: "2026-10-10"
updated: "2026-10-13"
depends_on: []
spec_refs: []
---

## Description

Found in 0390's saturated pilots (bench/0390-shard-cpu/FINDINGS.md). `range_stats_reply` → `Engine::range_estimate` calls `self.index.scan(lo, hi)` for every range on every PD `RangeStatsV3` poll (default 5 s). That builds a `Vec` holding a copy of every memtable key in the range, just to count them and pick a midpoint. It runs even when logical counts are available, because the estimate is computed before the match.

With large memtables this took 57–81% of the shard (file-backed one-shard pilots with 1–11 GB memtables). It stalls the shard synchronously, which starves flush-commit slices, so memtables grow further: positive feedback. R3's normal-sized memtables keep it under the 0.2% perf cutoff.

Candidate: count and pick the midpoint without materializing keys (counting scan, or index rank/size), and skip the estimate when logical counts answer the request.

Acceptance: an L0 test that the reply is unchanged, plus a microbench or L1 measurement showing poll cost no longer scales with memtable bytes.

## Outcome

RangeStats no longer copies every in-range memtable key per poll. FrontCodedIndex::range_count_median sums whole-block entry counts, ranks only boundary blocks via probe, and decodes only up to the median in its block; Engine::range_estimate uses it, and the new Engine::range_midpoint (used by range_stats_reply when logical counts are ready) reads the memtable only when no run offers a midpoint. Reply unchanged: L0 index test vs the old scan formula (12k random/boundary/None bounds, mutants fail) plus engine midpoint-equivalence test. memtable_bench --range-stats (64 ranges, release): scan 30/231/1093/5636 ms/poll at 250k/1M/4M/16M entries → 0.19/0.70/3.09/26 ms, 64 allocs/poll, cost independent of key bytes (backlinks 2x bytes ≈ same); residual is a BTreeMap block walk (~1 node per ~46 entries). vals-store lib (only the known 0431 flake, passes on rerun) and vals-cluster lib 841 pass; standard review accepted.

Commit: perf(0415): RangeStats counts memtable entries and picks the midpoint without materializing keys; skip the memtable when logical counts are ready and a run offers a midpoint (4M entries: 1093 → 3.1 ms/poll)

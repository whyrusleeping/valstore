---
id: "0473"
title: 'RT fold target walks every range-tombstone record the engine holds: queue throughput decays as cumulative deletes accumulate'
status: done
priority: 2
created: "2026-10-15"
updated: "2026-10-15"
depends_on: []
spec_refs:
    - docs/range-deletes.md
---

## Description
## Found by
0471 hardware queue benchmark, arm CAL-2 (lab, 3 SPDK nodes × 8 shards, RF 3, 64 partitions, consumers range-deleting `[p/, cursor)` after every scan, offered 600k msgs/s). Throughput decayed from 600k to 380k msgs/s within 15 minutes, while produce, scan and delete latency grew linearly (delete p50 0.5 → 12.8 ms).

perf on lab-1 shard threads:
- `Engine::range_tombstone_records_between` ← `VersionedStore::fold_target` ← `begin_rt_fold` was 55% of shard cycles at t = 60 s and 74% at t = 840 s.
- It is reached from both the `RangeDelete` handler's inline fold turn (0465) and `rt_fold_page`.

## Cause
`range_tombstone_records_between(after, through)` filters `rt_records_tagged()`, which walks every RT record the engine holds: every run's records plus the tail. Each fold therefore costs O(all records held). With `gc_grace = 0` records are never collected, so a queue issuing about 500 cumulative deletes/s per engine adds about 1.8M records per engine per hour, and every delete gets slower. Other per-op walks of all records include `max seq` (range_delete.rs ~283) and the `all(seq <= …)` check (~287); audit `rt_filter_set` and `rt_records_through` as well.

## Fix (implementation only, no format/semantic change)
Keep the engine's RT records indexed by seq, so that `range_tombstone_records_between`, the max-seq query and similar lookups cost O(log n + k) instead of O(n). Maintain the index across append, flush, every run rewrite, collection, cleanup clipping and reopen.

## Acceptance
- An engine-level test or microbench shows the fold-target cost no longer depends on the number of held records, e.g. N = 1k vs 100k cumulative records.
- Existing range-delete tests pass.
- Re-run 0471 CAL-2 on lab: throughput holds at the offered rate for 15 minutes, and shard µs/msg is flat.

Unbounded record growth itself (RAM, flush/merge copying) is a separate design question: proposed dominated-record pruning.

## Acceptance criteria

## Work log

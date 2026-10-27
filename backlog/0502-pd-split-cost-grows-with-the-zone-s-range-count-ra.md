---
id: "0502"
title: 'PD split cost grows with the zone''s range count: range_map_of rebuilds the map from raft records on every split'
status: done
priority: 4
created: "2026-10-23"
updated: "2026-10-27"
depends_on: []
spec_refs: []
---

## Description

Observed in 0469 (bench/0469-range-map-share/FINDINGS.md §4): splitting one sorted zone from 168 to 25,000 ranges through `split-zone` took about 40 minutes on lab-1 (≈20 splits/s at first, slowing as the map grows), and the 50,000-range setup took 5,437 s. A system-wide perf sample of the PD during the splits shows `pd::driver::range_map_of`, `valid_token`, `memcmp` and allocator leaves dominating: each split rebuilds and revalidates the whole zone map from its raft state records. Under the owner's scaling rule (cost should follow what changed, not total range count), per-split PD work should not be O(ranges).

Candidates: cache the decoded map per zone in the driver keyed by the records' apply index and update it incrementally on split/merge; or validate only the changed neighbourhood.

Acceptance: split throughput at 25,000 ranges per zone measured before/after with bench/0435-range-residual/ab.py (`--split-par 8`, `split_s` in events.jsonl), or recorded as not worth fixing.

## Outcome

vals-raft Partition logs keys written per apply (take_state_changes; snapshot restore or >64k keys reports "lost"); PlacementDriver keeps a StateIndex (control keys + per-zone decoded range records and assembled RangeMap), rebuilt on open/snapshot/lost log and otherwise updated from changed keys after each drive(); range_map(s), members, zones, orphan scan and drop read it, with a full-rebuild fallback if behind the applied index; set_range_map_checked diffs maps in one sorted pass. PD test helpers assert index == full rebuild after every settle; new multi-PD test covers snapshot catch-up, merges, zone drop/new zone. lab-1 ab.py --ranges 25000 --split-par 8 (bench/0502-pd-split-cost/FINDINGS.md): split_s BASE 5,372 s vs FIX 1,918/1,914 s (2.8x); per-range split cost ~4.1-7.5 µs -> ~0.5-0.7 µs; residual O(ranges) per split (RangeMap flat Arc COW copy + reassembly) and node-side whole-map push decode recorded as not worth fixing now. Artifacts in /data/dataset/task0502-results/.

Commit: perf(0502): PD keeps an incrementally updated state index (raft apply change log) so splits no longer rebuild every zone map from all state keys; lab-1 split_s at 2x25,000 ranges 5,372 s -> 1,918/1,914 s

---
id: "0312"
title: Hash-once + prefetch run filter probes on the write path
status: done
priority: 1
created: "2026-09-26"
updated: "2026-09-26"
depends_on:
    - "0309"
spec_refs: []
---

## Description
## Evidence (LONG01, 2026-09-26)

Setup: capacity profile, 95M-KV run, master with the 0310 fix. At t=60 s, `DeviceRun::locate_block` costs about 7.9k cycles per KV, roughly 20% of shard cycles, with about 10 runs per engine.

Diagnostic arms, one run each (B15–B18):

| Arm | ACK/s | Change | Notes |
|---|---|---|---|
| Control | 827k | — | |
| `VALS_MERGE_FORCE_RUNS=6` | 834k | none | Normal tiering selection always wins, so the setting has no effect. |
| Duty 100 (unpaced merges with readahead) | 908k | +10% | Runs drop 169 → 49 and locate_block cycles halve, but device writes rise 92 → 120 GB. |
| Diagnostic bloom at 20 bits per key | 855k | +3% | False-positive run reads drop 2.69M → 0.70M; locate_block CPU unchanged. |

The probe CPU cost comes from hashing, not false-positive reads. `BlockedBloom::contains` re-hashes the whole key with byte-at-a-time FNV-1a for every run, and the dependent hash chain serializes the filter-line misses.

A local release microbenchmark (10 blocked filters of 6M keys each, 2M absent probes) measured:
- 1.67 µs per key today;
- 0.36 µs per key when hashing once;
- 0.33 µs per key when hashing once and prefetching every filter line first.

## Change

Branch `0312-probe-hash` (`5d0f9f3f`, worktree `.wt/0312h`) adds `BloomKey` (h1, z), computed once per key, plus `contains_hashed`, `locate_block_hashed`, `lookup_hashed` and a filter-line prefetch. These are used in `probe_version`, `probe_get` and `lookup_runs`. The on-disk format and filter semantics are unchanged, covered by the test `hashed_filter_probes_match_keyed_probes`.

## Acceptance

A preregistered ABBA on LONG01 B against MASTER0310 must show:
- all correctness gates pass;
- whole-write ACK/s at least 1.05× the mean;
- locate_block cycles per KV at least 50% lower at t=60 s.

Compaction keep-up (backlog-aware pacing) is a separate, later candidate.

## Acceptance criteria

## Work log

---
id: "0397"
title: Profile and cut the ~9 GB unexplained RSS remainder at EOF (merge-worker metadata reopen, seed-retained indices, allocator slack); compact fences if still short (0395 split)
status: done
priority: 2
created: "2026-10-07"
updated: "2026-10-07"
depends_on:
    - "0395"
spec_refs: []
---

## Description

Split from 0395 (2026-10-07).

## Outcome

Profiled the 100M-key local remainder with the test-only heap-profile allocator and the rss_bench harness. Explained share: unsorted 87.3% (retained HashIndex capacity plus allocator pages); sorted replay-shaped 84.8%, almost all glibc free pages. Shipped: - valsd 60 s nice-19 background malloc_trim (VALS_MALLOC_TRIM_SECS=0 disables). Saves ~46 MB mean, ~116 MB peak and 304–422 MB EOF RSS; request rate within ±0.15%. - Merge inputs skip bloom decode (−32.7 MB worker peak). - flush_value_segs reserve fix (18.9 MB). - Hash memtable control reset when drained (clean unsorted A/B −147.8 MB EOF). - New gauges, documented. Rejected: partial-drain map rebuild (77–117 ms shard stalls), static mmap thresholds, 10 s trim, arena cap. Hardware RSS and ACK-tail checks moved to 0398; compact fences split to 0399.

Commit: feat(0397): profile local RSS remainder (mostly glibc free pages); background malloc_trim, scan-only merge-input open, flush-set reserve fix, drained HashIndex reset, new gauges

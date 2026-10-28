---
id: "0524"
title: Anti-entropy catch-up after a write burst stalls file-backed reads for ~1.2 s every sweep
status: proposed
priority: 3
created: "2026-10-28"
updated: "2026-10-28"
depends_on: []
spec_refs: []
---

## Description
Found by 0510's file-backed benchmarks (local evidence in bench/0510-file-bench/probe2 and run1-results-single-nosettle, gitignored).

Setup: three file-backed nodes on one lab host (Core Ultra 9 285HX, 990 PRO), RF 3, default node settings (`sweep = 5s`), 4M keys of 1 KiB, client at 256 in flight with majority reads.

After 2 minutes of buffered majority overwrites (about 54k/s, then 27k/s), a read-only run started straight away. All 256 in-flight reads stalled for about 1.2 s at 5 s intervals (1-second samples dropped from about 100k reads to under 1k, with p99 1.19–1.31 s). This lasted about 15 s. Afterwards reads ran at about 100k/s with p99 about 5 ms. In an earlier run, after a device-durable write phase, the stalls lasted the whole 2-minute read phase: 12.3k reads/s, p99 1.1 s.

Node 1's metrics during the stall windows: `vals_ae_digests_examined` about 18.6k/s and `vals_lookup_calls{caller="exists"}` about 21k/s. Both were zero in quiet windows. No disk I/O. Anti-entropy seed counters were idle. This points at sweep reconcile work running on the shard thread in one long step.

Acceptance:
- Reproduce with `scripts/file-bench.py run --settle-max 0 --phases write-buffered,read-warm` (or bench/0510-file-bench/probe2) and identify the step that holds the shard.
- Bound or yield that work so that foreground p99 stays within a small multiple of its quiet value while anti-entropy catches up, without stalling convergence.
- Re-measure on the same host.

## Acceptance criteria

## Work log

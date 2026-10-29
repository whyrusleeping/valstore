---
id: "0524"
title: Anti-entropy catch-up after a write burst stalls file-backed reads for ~1.2 s every sweep
status: done
priority: 3
created: "2026-10-28"
updated: "2026-10-29"
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

## Outcome

Reproduced on lab-1 by stopping node 3 for the last ~45 s of a buffered write phase (the original 0510 trigger no longer reproduces at HEAD): reads stalled 1.0-1.3 s every 5 s sweep because node 3's reconcile checked a full 8 MiB page (209,715 digests) with inline file reads without yielding, and digest serving held peers 30-75 ms (memtable planning) up to 1 s (sparse scans). Fix: reconcile paces local ops (yield every 128), stops probing once need reaches the remaining value-read budget (resume at the last needed key); serve cuts resumable pages after 5 ms of uninterrupted run and caps planning at 16,384 memtable entries (checked before filtering; resumable empty pages carry the last examined key as resume). Re-measured same host: worst one-second read p99 17 ms (idle 5 ms) vs 1.0-1.3 s, max read 45 ms, saturated 102.7k vs 64.4k reads/s during catch-up, node 3 caught up in 146 s vs 166 s. vals-store/vals-cluster/vals-server tests and clippy pass; sol accepted round 2.

Commit: 0524: AE yields every 128 local ops and stops probing at the value budget; digest serve cuts pages after 5 ms without a device wait and caps memtable planning at 16,384 entries

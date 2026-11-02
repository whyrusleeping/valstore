---
id: "0547"
title: 'Write fewer device bytes per record: merge threshold, run-block compression, compact value-log header (key schema/value compression noted for bootes)'
status: done
priority: 2
created: "2026-11-01"
updated: "2026-11-02"
depends_on:
    - "0563"
    - "0564"
    - "0565"
spec_refs: []
---

## Description

## Description
## Why
Ingest after the drive's steady-state step is device-bound, so writing fewer bytes per record raises the sustainable rate. Current master (86983dc, 3-node R8, node1) writes ~503 B of device data per synced write, the same before and after the step:

| | B/write | share |
|---|---:|---:|
| value log (WAL): ~50 B fixed header + ~115 B key + ~130–140 B value | ~297 | 59% |
| index runs, first write (memtable flush) | ~95 | 19% |
| index runs, merge rewrites | ~100–120 | 21% |

Keys are written about three times (log, flush run, merge rewrites). In the 0546 commitwrite microbenchmark, post-step commit time grows with the bytes per commit, and merge/flush writes compete with commits for the drive.

## Work, in order
1. **Merge threshold (config only):** an N2 (two-node) A/B of a higher `run_compact_threshold` (8 → 12/16). Measure device B/write, ingest seconds to 4.5B, read probes per Get, key-filter RAM.
2. **Run-block compression (new run format):** per-32 KiB-block LZ4 or zstd in runs, with decompression on read/merge/scan paths and format negotiation/rollback as for earlier run-format versions. Measure B/write, CPU per write, point-read and scan cost; A/B on N2.
3. **Compact value-log record header (new vlog record format):** drop the per-record epoch (fold it into the CRC seed), varint lengths and seq, compact version; target ≤ 20 B. Recovery and replay read both formats.
4. **For the owner, not vals:** a bootes key schema with binary DIDs and coded collection NSIDs (largest single lever, ~25–30%; needs re-ingest and key-consumer changes), and dictionary value compression.

## Acceptance
- Each item is measured with device B/write and an N2 ingest A/B (keep or reject with numbers).
- Kept items ship with format compatibility (old data still readable), docs and tests.
- A summary of the B/write reduction achieved.

## Acceptance criteria

## Outcome

All three vals items kept and default at 27f15c4 (0563 merge fan-in K = min K = valve = 16; 0565 value-log format 2; 0564 run format v7 zstd blocks), each with old-format reads, a rollback knob, docs and tests. N2 replay to 4.5B, post-drain device B/write node1 522.5 -> 469.7 -> 438.7 -> 328.2 (-37.2%: vlog 294.8 -> 262.3, flush runs 97.9 -> 29.2, merge runs 125.1 -> 31.8); node3 542.7 -> 329.3 (-39.3%). Same-binary A/B time steps -5.2%, -4.7%, -15.2%; 4.5B 4,290 -> 3,333 s across builds. Costs: v7 run probe ~20 us CPU, scans 3-5x CPU. Item 4 (owner, bootes, unmeasured): device bytes are now ~80% value log, so a compact key schema mostly saves the ~115 B vlog key copy (halving it ~17%) and value dictionary compression ~4% per 10% of value size.

Commit: 0547: B/write summary: K16 merge fan-in + vlog format 2 + run v7 cut N2 node1 device bytes per write 522.5 -> 328.2 (-37%), node3 542.7 -> 329.3 (-39%), time to 4.5B 4,290 -> 3,333 s

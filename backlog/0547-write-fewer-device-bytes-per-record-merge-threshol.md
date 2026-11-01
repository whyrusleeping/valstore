---
id: "0547"
title: 'Write fewer device bytes per record: merge threshold, run-block compression, compact value-log header (key schema/value compression noted for bootes)'
status: todo
priority: 2
created: "2026-11-01"
updated: "2026-11-01"
depends_on: []
spec_refs: []
---

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

## Work log

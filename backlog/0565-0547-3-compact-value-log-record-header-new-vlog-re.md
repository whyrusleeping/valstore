---
id: "0565"
title: '0547.3: Compact value-log record header (new vlog record format, ≤ 20 B), N2 A/B'
status: done
priority: 2
created: "2026-11-02"
updated: "2026-11-02"
depends_on: []
spec_refs: []
---

## Description

Child of 0547 (item 3). Value log is ~298 of 522 device B/write on N2 node1 (~50 B fixed header per record). Drop the per-record epoch (fold it into the CRC seed), varint lengths and seq, compact version; target ≤ 20 B. Recovery and replay read both formats. Measure B/write and an N2 ingest A/B; keep or reject with numbers; kept work ships with format compatibility, docs and tests.

## Outcome

Format 2 is the default value-log format: 50-byte segment header (v1's first 22 bytes + base seq/version + CRC, so v1-only binaries refuse it), records with tag, varint lengths, zigzag seq/hlc deltas from the segment's first record, writer elided when equal, 32-bit epoch tag, CRC over header + decoded version + key + value (value reads verify against the index entry's version; reports "record crc or version mismatch"). Format 1 stays readable and writable (VALS_VLOG_WRITE_VERSION=1, vals_vlog_write_version gauge). Merge rewrite writer that fills its reservation leaves remaining values in place; maintenance reserve never below flush+manifest headroom. N2 same-binary A/B (R6 vs R8): both PASS, 0 crashes; 4.5B 3,921.7 vs 4,116.3 s (-4.7%), node1 device B/write 471.6 -> 438.7, vlog 294.9 -> 262.1. Control R5 had two unexplained early memtable integrity crashes, not reproduced in R6/R7/R8. Docs: formats, configuration, deploy, ARCHITECTURE.

Commit: 0565: value-log format 2 (compact ~17 B record headers vs 50); format 1 still read and writable via VALS_VLOG_WRITE_VERSION=1; N2 4.5B -4.7% time, vlog B/write -11.1%, device -7.0%/-5.6%

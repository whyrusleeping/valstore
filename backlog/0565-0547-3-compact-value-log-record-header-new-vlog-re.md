---
id: "0565"
title: '0547.3: Compact value-log record header (new vlog record format, ≤ 20 B), N2 A/B'
status: todo
priority: 2
created: "2026-11-02"
updated: "2026-11-02"
depends_on: []
spec_refs: []
---

## Description
Child of 0547 (item 3). Value log is ~298 of 522 device B/write on N2 node1 (~50 B fixed header per record). Drop the per-record epoch (fold it into the CRC seed), varint lengths and seq, compact version; target ≤ 20 B. Recovery and replay read both formats. Measure B/write and an N2 ingest A/B; keep or reject with numbers; kept work ships with format compatibility, docs and tests.

## Acceptance criteria

## Work log

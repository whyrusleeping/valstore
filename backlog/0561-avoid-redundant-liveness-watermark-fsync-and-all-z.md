---
id: "0561"
title: Avoid redundant liveness-watermark fsync and all-zone walks on the serving shard
status: done
priority: 3
created: "2026-11-02"
updated: "2026-11-05"
depends_on: []
spec_refs: []
---

## Description

0548 source review: node/maintenance.rs:690-695 invokes shared.persist_gc_watermark inside per-zone snapshot loop. shared.rs:953-974 computes watermark across all zones and advances durable floor across all zones, making repeated O(zones²) work per tick; when watermark advances, FileLivenessLog::store synchronously creates/writes/fsyncs temporary file, renames, and directory fsyncs (liveness.rs:123-142), on the serving shard. record_contact_inner (shared.rs:985-1004) also stores unconditionally even if watermark unchanged. Enabled when liveness/gc_grace configured; 0544/0546 gc_grace=0 perf arms don't cover this. No new wall-time measurement (do not treat reviewer's ms estimates as established).

Suggested acceptance: once-per-tick rather than per-zone persistence/floor work, skip provably redundant stores, measure fsync frequency/time under gc_grace>0; consider bounded coalescing/off-thread persistence while publishing durable GC floor ONLY after durable completion and maintaining restart/wipe safety. Regression counting store calls and checking GC never passes durable watermark. This is not authorization to relax fsync durability.

## Outcome

snapshot_loop persists the GC watermark once per tick (was per zone with an all-zone walk each); record_contact skips the store when the watermark hasn't passed the durable one; durable GC floor still published only after a successful synchronous store. New counters vals_watermark_persists / vals_watermark_persist_us; regression test covers store counts and GC staying behind the durable watermark. Measured on devbox ext4 FileLivenessLog, gc_grace=60s: idle stores 1.00/s (8 zones) and 6.33/s (32 zones, 100ms sweep) -> 0; advancing-watermark store rate unchanged (~1 per tick + 1 per contact).

Commit: liveness (0561): persist the GC watermark once per snapshot tick, skip unchanged contact stores, count store calls/time

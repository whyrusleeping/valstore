---
id: "0557"
title: Avoid draining unrelated value-log writes before sealed-record reads and read planning
status: done
priority: 2
created: "2026-11-02"
updated: "2026-11-02"
depends_on: []
spec_refs: []
---

## Description

0548: vals-store/src/vlog.rs:1244,1259,1276,1299,1326,1346 unconditionally call drain_inflight before reads and read plans. drain_inflight:1036-1043 synchronously waits for all inflight writes and writes the staged coalescing buffer, even if the requested value is in an unrelated sealed segment. Reaches staged server get probes under store borrow as well as sync APIs and scans. Concrete ignored scratch repro bench/0548/repro/src/bin/read_drain.rs: old value in segment0, staged newest value in segment2, get(old) emits one synchronous device write. No measured production latency/throughput claim; prior 0544 drain timings are not a mixed-read/write benchmark. Suggested acceptance: track inflight/staged LBA ranges and only enforce read-after-write dependencies for overlaps (or overlay staged bytes safely); tests old sealed read emits no new write and recent/inflight/staged reads stay correct; fault handling must retain 0545 fail-closed semantics rather than losing errors; measure mixed-load read latency/ingest and device write sizes before adoption.

## Outcome

ValueLog reads and read plans no longer drain: in-flight writes carry LBA ranges and a read waits only for overlapping ones; staged blocks and the dirty tail are copied into overlays at plan time; completed writes are still reaped so 0545 WriteLost fail-closed holds. New unit test covers all five read APIs (old/staged/tail/in-flight, plan after submit) and fails under three mutations; read_drain repro 1 -> 0 writes. SPDK ABBA on alphac-1 (vals-bench mixed, patched for split latency): ops/s +1..+4%, get p99 -10..-13%, device writes 3.7-3.9x fewer at 16 KiB, put mean +0.01..0.1 us. vals-store tests (608) and workspace clippy pass; docs/algorithms.md updated.

Commit: vlog: reads wait only for overlapping in-flight writes and overlay staged blocks/tail from memory instead of draining (0557); SPDK mixed get p99 -10..-13%, 3.7-3.9x fewer device writes

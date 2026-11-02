---
id: "0554"
title: Validate record key and version in synchronous engine scan materialization
status: done
priority: 2
created: "2026-11-02"
updated: "2026-11-02"
depends_on: []
spec_refs: []
---

## Description

0548: vals-store/src/engine/scan.rs:22-32,44-54 use unchecked SpanReadPlan::execute_sync then pair rec.value with selected index key/version, discarding decoded identity. get and served staged scans validate identity; this is residual scope beyond completed 0339. Confirmed scratch repro at bench/0548/repro/src/bin/scan_identity.rs redirects alpha/first's pointer to a valid equal-length bravo/other record via test fixture. get(alpha) returns record key mismatch, but scan_bounded_versioned succeeds with alpha/first=value-bravo version=1 and bravo/other=value-bravo version=2. This requires pointer corruption/misreference, not an ordinary valid write sequence. Acceptance if accepted: use existing execute_sync_checked with expected (key,version) in both materializers, test wrong-key and same-key stale-version record pointers; tests must fail without fix; verify ordinary scan API behavior/performance.

## Outcome

scan_bounded/scan_bounded_versioned read through read_winner_records (execute_sync_checked with each winner's key, version); a misdirected or stale pointer fails with record key/version mismatch like get. Unused unchecked execute_sync removed. Regression test covers wrong-key and stale-version pointers in memtable and runs and fails without the fix. Release paged-scan bench (100k keys) shows no measurable cost (19.1 vs 19.4-19.7 ms).

Commit: 0554: synchronous engine scans (scan_bounded, scan_bounded_versioned) validate each value record's key and version via execute_sync_checked, as get does; unchecked SpanReadPlan::execute_sync removed

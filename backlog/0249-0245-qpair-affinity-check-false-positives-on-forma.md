---
id: "0249"
title: 0245 qpair affinity check false-positives on format=false recovery boot — node cannot restart with check on
status: done
priority: 2
created: "2026-08-24"
updated: "2026-09-05"
depends_on: []
spec_refs: []
---

## Description

Repro (2026-08-24 23:48Z, lab-1): after the node-1 crash, relaunch with `format = false` (crash recovery of a ~150M-record catalog) and default affinity enforcement → all four `vals-shard` threads panic at `crates/vals-spdk/src/spdk.rs:416` (`SPDK qpair used outside its bound owner thread; left: ThreadId(1)`), i.e. recovery-path boot I/O on the main thread first-use-binds the serving qpairs before `ServerShared::bind_device_owners` reseals them to shard threads (or the reseal doesn't run on this path). Fresh `format=true` boots are fine (0232 gate). Workaround in production use now: `VALS_SPDK_AFFINITY_CHECK=0` on the relaunch.

Fix: make the recovery boot path bind/reseal qpair ownership the same way the fresh-format path does (or defer first-use binding until after bind_device_owners), and add a format=false reopen to whatever gate covers 0245.

Acceptance: node restarts cleanly with affinity check ON over a non-trivial persisted catalog; check still panics on genuine cross-thread serving I/O.

## Outcome

Moved default-zone device ownership bind/seal before ServerShared::zoned performs persisted-zone recovery I/O; strict sealed affinity behavior retained. Added persisted three-zone boot-to-shard regression and SPDK-feature cross-thread rejection test; documented non-destructive format=false gate. Independent reviewer accepted. Focused regression, full debug cluster suite (572 unit tests passed, 3 ignored, integration/doc suites passed), SPDK-feature workspace all-target check, and final diff check passed. Live SPDK restart and linked runtime test split into accepted task 0256 to avoid disrupting active 0248 diagnostics; no hardware certification claimed. Existing actively collected bench/metrics-0248-driver.promz was not edited by this task but is included by the whole-tree commit as a concurrent metrics snapshot.

Commit: fix(cluster): bind shard qpair before persisted-zone recovery

---
id: "0532"
title: Crash-simulation block device and crash-point harness for the storage engine
status: todo
priority: 2
created: "2026-10-29"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description
Found in the 2026-10-29 project review. `MemBlockDevice::flush` is a no-op and every write is durable immediately (crates/vals-spdk/src/lib.rs ~586), so no test loses unflushed writes. Fault devices are ad hoc (`ManifestCrashDevice`, `CrashDevice` in vals-store engine/tests/mod.rs ~229/550); only vals-cluster drive.rs (~1375) enumerates crash points. Bugs like 0528, 0529 and 0531 are exactly what a systematic crash harness finds.

Scope:
- A test block device with a volatile write-back cache: `flush` persists; `crash()` keeps a seeded random subset of unflushed blocks and can optionally tear a block at 512-byte sector granularity.
- A seeded harness (vals-store tests) that runs a workload covering put/delete/range-delete, flush, merge, vlog reclaim/rewrite and manifest commit, crashes at every write index (or a seeded sample in the default lane, exhaustive behind an env var), reopens, and checks: open never panics; every key acknowledged as synced before the crash reads back its latest synced value or a newer one; no deleted key resurrects; the engine's storage audit (`storage_audit_for_test`) passes.
- Default `cargo test` cost stays small (seconds); the exhaustive mode is documented in plans/testing-notes.md.

Acceptance: harness lands with the default lane green (failures it finds become their own tasks or are fixed in place if trivial); demonstrating at least one of 0528/0529/0531 being caught when its fix is reverted (or before it lands).

Note: crates/vals-spdk/src/lib.rs may hold another session's uncommitted edits; put the device in vals-store test code or coordinate before editing that file.

## Acceptance criteria

## Work log

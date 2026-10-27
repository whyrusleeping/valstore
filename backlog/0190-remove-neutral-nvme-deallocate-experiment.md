---
id: "0190"
title: Remove neutral NVMe Deallocate experiment
status: done
priority: 3
created: "2026-08-12"
updated: "2026-08-12"
depends_on: []
spec_refs:
    - docs/optional-feature-survey.md#NVMe Deallocate / TRIM — remove
    - docs/SPDK.md
---

## Description
Delete default-off `VALS_NVME_DEALLOC` plumbing. Task 0169's whole-namespace and free-time TRIM A/B neither helped nor hurt sustained-overwrite behavior; the actual serving-path issue was fixed elsewhere.

Acceptance criteria:
- Remove the environment gate, allocator pending-discard tracking, block-device discard API where now unused, SPDK Rust/C Deallocate FFI, format/slab/free-time calls, counters, tests, and operator docs.
- Preserve format/truncate correctness without relying on advisory deallocation.
- Verify non-SPDK debug suites and SPDK-feature compilation/build procedure; run a short format/write/reopen smoke when hardware is available.
- Do not run cargo fmt.

## Acceptance criteria

## Plan

1. Trace every live NVMe Deallocate path across the store allocator/engine, block-device abstraction, SPDK Rust/C shim, cluster format/slab setup, metrics, tests, and operator documentation, distinguishing historical benchmark artifacts from maintained code/docs.
2. Remove the default-off environment gate and all now-unused discard plumbing end to end: allocator free tracking, device trait/API and implementations, SPDK FFI/shim command, engine and cluster call sites, counters/metrics, and feature-specific tests. Keep format/truncate semantics based on metadata/data writes rather than advisory deallocation.
3. Update maintained documentation and the optional-feature survey to describe the experiment as removed; do not rewrite immutable benchmark outputs or project memory.
4. Verify with targeted searches, non-SPDK debug tests, SPDK-feature compilation using the documented available procedure, and a local file-device format/write/reopen test or equivalent existing smoke. Do not run cargo fmt.

### Starting points
- crates/vals-store/src/{alloc.rs,engine.rs,device.rs,lib.rs}
- crates/vals-spdk/src/spdk.rs and csrc/spdk_shim.c
- crates/vals-cluster/src/{drive.rs,zonestore.rs}
- crates/vals-server/src/metrics.rs
- docs/{SPDK.md,optional-feature-survey.md}

## Work log
- 2026-08-12 plan: 1. Trace every live NVMe Deallocate path across the store allocator/engine, block-device abstraction, SPDK Rust/C shim, cluster format/slab setup, metrics, tests, and operator documentation, distingui
…[truncated]
- 2026-08-12 context hints: 5 recorded with plan
- 2026-08-12 context hints: The working tree contains unrelated task 0194 and other changes; preserve all pre-existing edits and stage only task 0190 files/hunks where possible.; BlockDevice::discard currently appears in vals-sp
…[truncated]
- 2026-08-12 preload: 9 file(s), ~34 KiB seeded into implementer context
- 2026-08-12 implementation: Removed allocator free tracking, the BlockDevice discard API, SPDK Rust/C Deallocate commands, engine/format/slab call sites, metrics, feature tests, and operator documentation. Format and truncate continue to fence stale state with superblock/segment-header writes.
- 2026-08-12 verification: `cargo test --workspace`, `cargo check --workspace --features spdk`, `cargo build -p vals-spdk --features spdk`, targeted file-device reopen/truncate and zonestore format/reopen tests, and `git diff --check` passed. A full workspace SPDK-feature binary build without `VALS_SPDK_LINK=1` predictably cannot link the intentionally absent C shim; docs now distinguish the off-hardware check/library-build procedures. No SPDK hardware smoke was available.
- 2026-08-12 implementer report: Implemented task 0190 and marked its backlog item done.  Changed: - Removed allocator free-recording/pending-discard state and its tests. - Removed `BlockDevice::discard`, wrapper forwarding, SubDev s
…[truncated]
- 2026-08-12 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-12 review (sol): accept — Task 0190 is complete and internally consistent. The change removes the environment gate, allocator free/discard bookkeeping, BlockDevice/DriveSet discard APIs, SPDK Rust FFI and C Deallocate implemen
…[truncated]
- 2026-08-12 review (sol#2/sol): accept — Task 0190 is correctly and cleanly implemented. The change removes the environment gate, allocator free/discard tracking, BlockDevice API and forwarding, SPDK Rust/C deallocate implementation, whole-d
…[truncated]
- 2026-08-12 decision: accept — commit: store: remove NVMe Deallocate experiment
- 2026-08-12 usage: 6,437,945 tok (in 1,766,907, out 36,414, cache_r 4,634,624, cache_w 0) · cost n/a (unpriced)
  implementer: 4,324,640 tok (in 725,289, out 13,815, cache_r 3,585,536, cache_w 0) · cost n/a (unpriced)
  coordinator: 1,470,969 tok (in 575,289, out 17,344, cache_r 878,336, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 340,868 tok (in 244,236, out 2,424, cache_r 94,208, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 301,468 tok (in 222,093, out 2,831, cache_r 76,544, cache_w 0) · cost n/a (unpriced)

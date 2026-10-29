---
id: "0535"
title: SPDK read/write futures borrow caller buffers, so mem::forget of a polled future is a use-after-free in safe code
status: todo
priority: 4
created: "2026-10-29"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description
Found in the 2026-10-29 project review (speculative; confirm first). `ReadAt`/`WriteAt` futures in crates/vals-spdk/src/spdk.rs (~222–283) borrow the caller's buffer and rely on `Drop` waiting for the I/O. Safe code can `mem::forget` a polled future, after which the device DMA-writes into freed memory, so the API is unsound in safe Rust. The `Send`/`Sync` impls (~405–411) rest on owner-thread asserts that `VALS_SPDK_AFFINITY_CHECK=0` disables (~387–391).

Acceptance:
- Either the futures own their buffer (e.g. take a `Scratch`/DMA buffer and return it on completion) or the borrowing constructors become `unsafe fn` with a documented contract; callers updated.
- The affinity check cannot be disabled in release builds, or the impls are restricted so soundness does not depend on it.
- SPDK-linked build and a short box run confirm no throughput regression (hot path).

## Acceptance criteria

## Work log

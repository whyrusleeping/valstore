---
id: "0535"
title: SPDK read/write futures borrow caller buffers, so mem::forget of a polled future is a use-after-free in safe code
status: done
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

## Outcome

SPDK async read buffer UAF closed: SpdkBlockDevice::read_at_async is unsafe fn with a documented leak contract; AsyncRead gains unsafe read_async_in_place (default forwards to safe read_async; Arc/SubDev forward) used by vlog and run range reads on future-owned buffers; SPDK's safe read_async owns a DMA scratch. Writes stay safe (shim copies at submit). Affinity checks always on (VALS_SPDK_AFFINITY_CHECK removed), every qpair FFI call holds the owner lock, bind_io_owner refuses ownership moves with aio in flight (shim vals_spdk_aio_inflight), SubmittedIo drop aborts on unwind (regression test). Evidence: SPDK-linked vals-spdk tests 20/20 on lab-3; 6 ABBA+BA arms on the restored 0511 cluster: panic-free format=false restarts, 0 mismatches over 4.5M verify reads, medians base/fix verify 13.2/13.4 s, scans 9.6/9.7 s, writes 529k/566k/s — no regression. Docs: configuration.md, SPDK.md, bootes-soak-runbook §4.1.

Commit: 0535: SPDK read_at_async is unsafe (leak contract) with safe owned-scratch read_async and zero-copy read_async_in_place; qpair calls under owner lock, affinity checks always on, no ownership move with aio in flight

---
id: "0550"
title: Make scratch-buffer initialization sound at the safe BlockDevice API boundary
status: done
priority: 1
created: "2026-11-02"
updated: "2026-11-02"
depends_on: []
spec_refs: []
---

## Description

0548 source review: vals-spdk/src/lib.rs:531-541 heap_take clears/reserves Vec<u8>, then unsafe set_len without initializing fresh/grown storage, with clippy::uninit_vec suppressed. BlockDevice::take_scratch (357-359) and Scratch's public Deref/as_mut_slice expose it through safe code; no successful device read is required first. `MemBlockDevice::new(4096, 1).take_scratch(4096)[0]` is safe code reading uninitialized memory. A u8 accepting all bit patterns does not make uninitialized bytes valid. Also audit vals-store/src/vlog.rs:54-63 io_buf's identical pattern and DMA scratch initialization contract. Static Rust safety finding; Miri is not installed and was not run. Suggested acceptance: initialize newly exposed bytes or use an actual MaybeUninit/raw I/O buffer with a checked initialization boundary before returning readable slices; cover fresh/grown/reused buffers and failed/partial reads, validate under Miri where possible; benchmark hot-path changes rather than assuming zero-fill cost is acceptable.

## Outcome

Heap Scratch is {buf, len} with buf.len() as the initialized high-water; heap_take resize(len, 0)s only growth, so steady-state reuse is unchanged. vlog::io_buf is vec![0; len]. DMA scratch audited (spdk_dma_zmalloc, pooled reuse only exposes zeroed/device-written bytes) and documented. New test covers fresh/reused/grown/shrunk buffers and failed/partial reads; passes under cargo +nightly miri, and Miri reports uninitialized-read UB with the old body. Zero-fill measured: +40 ns per 4 KiB (microbench); page-cache file-device run window/prefetch scan bench old vs new within noise; not measured on NVMe. R14 note added in docs/production-readiness-review-2026-08.md.

Commit: 0550: scratch buffers never expose uninitialized bytes (heap Scratch zero-fills only growth, keeps initialized high-water on reuse; vlog io_buf zero-fills; DMA zmalloc contract documented); Miri-checked test

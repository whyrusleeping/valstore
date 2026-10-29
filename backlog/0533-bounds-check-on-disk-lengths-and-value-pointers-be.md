---
id: "0533"
title: Bounds-check on-disk lengths and value pointers before allocating or reading (run footers, ValuePtr)
status: done
priority: 3
created: "2026-10-29"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description

Found in the 2026-10-29 project review (code read). Lengths and pointers read from disk are used before they are bounds-checked:
- Run footer `bloom_len` / `index_len` are cast to `usize` (crates/vals-store/src/run.rs ~3254, 3265, 3462, 3474), and `read_run_range_core` allocates `Vec::with_capacity(len)` (~2076) before checking against the run's segments; nothing compares section extents with `handle.len`.
- `span_blocks` (vlog.rs ~1050) and `read_record` (~1377) underflow when a `ValuePtr` has `len == 0` (space.rs ~609 shows such entries exist), which in release becomes a huge I/O buffer. Only diagnostic.rs (~31–33) validates a `ValuePtr`.

Acceptance:
- Every run footer section is checked to lie within `[0, handle.len − FOOTER_LEN]` before allocation/read; violations return a corruption error.
- One shared `ValuePtr` validity check is used before every read plan (including `len == 0` handling).
- Regression tests with crafted footers/pointers that fail (panic or over-allocate) with the checks removed.
- Hot-path cost checked (the checks are a few compares; confirm no measurable regression in the existing engine micro-benchmarks, hardware not required unless it shows up).

## Outcome

Run footer bloom/index/sidecar extents are checked (with overflow) against [0, handle.len − FOOTER_LEN] in every footer consumer (DeviceRun::open/open_scan, read_run_parts, read_run_range_tombstones, Run::parse). Shared vlog::check_value_ptr (len ≥ HEADER_LEN, offset+len ≤ segment) runs in every vlog read/plan path; engine callers check the segment index before alloc.segment_range; merge reports bad pointers as CorruptValueRecord. New regression tests fail with the checks removed (panics). vals-store tests and clippy pass; ABBA mem micro-benchmarks (read, batchread, scanlsm) are within code-layout noise. Reviewer (sol) accepted.

Commit: 0533: bounds-check run footer sections against the run length and value pointers against their segment before allocating or reading; refuse with a corruption error instead of panicking/over-allocating

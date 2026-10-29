---
id: "0533"
title: Bounds-check on-disk lengths and value pointers before allocating or reading (run footers, ValuePtr)
status: todo
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

## Acceptance criteria

## Work log

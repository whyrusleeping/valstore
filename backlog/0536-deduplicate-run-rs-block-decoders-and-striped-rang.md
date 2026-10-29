---
id: "0536"
title: Deduplicate run.rs block decoders and striped-range readers and split the module
status: todo
priority: 4
created: "2026-10-29"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description
Found in the 2026-10-29 project review. crates/vals-store/src/run.rs has ~4.3k production lines (tests start ~4336) with four hand-written block-entry decoders (`decode_block_body` ~97, `block_lookup` ~138, `BlockCursor::seek` ~230, `next_entry` ~266) and four copies of the "read a range striped over segments" loop (`read_logical` ~1673, `read_run_range_core` ~2068, `read_run_range_batched` ~2129, `RangeLocator::read_range` ~2220). Divergent copies of on-disk parsing are where bounds/validation fixes (e.g. the on-disk bounds-check task) get applied to three of four paths.

Acceptance:
- One block-entry parser and one striped-span iterator, used by all current callers; run.rs split into submodules (e.g. run/{block,filter,footer,io,stream,sidecar}.rs) without behaviour or format changes.
- Existing tests pass unchanged; run decoder fuzzing (record_fuzz-style, resealed checksums) covers the shared parser.
- Engine micro-benchmarks show no regression; hot-path box confirmation per docs/validation.md if any measurable change.

Do after (or together with) the on-disk bounds-check task so the checks land once.

## Acceptance criteria

## Work log

---
id: "0536"
title: Deduplicate run.rs block decoders and striped-range readers and split the module
status: done
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

## Outcome

Every block decoder (decode_block/_body, block_lookup, BlockCursor, block_first_key, BlockWalker, EncodeCheck) uses one parser in run/block.rs; the three striped readers use one StripedSpans iterator. run.rs is split into run/{block,build,filter,footer,io,sidecar,stream}.rs plus run/tests.rs; public paths and format unchanged, only corrupt-input error details differ (see work log). New run/tests/decode_fuzz.rs fuzzes all decoders with 4,000 resealed mutations and cross-checks the striped readers byte for byte. Store/cluster tests, clippy and docs pass. Local ABBA: no regression, decode_block 17% and scans 7% faster. SPDK box ABBA: 4.5M checked reads per arm, 0 mismatches, no slower. Reviewer sol accepted (round 3).

Commit: 0536: one shared run block-entry parser and striped-span iterator for all decoders and striped readers; split run.rs into run/{block,build,filter,footer,io,sidecar,stream}.rs; resealed-CRC decoder fuzz

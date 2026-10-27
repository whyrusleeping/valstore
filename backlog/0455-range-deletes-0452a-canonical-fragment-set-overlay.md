---
id: "0455"
title: 'Range deletes 0452a: canonical fragment set (overlay/delta/cover/cursor/cap) and RT record value codec'
status: done
priority: 3
created: "2026-10-14"
updated: "2026-10-14"
depends_on:
    - "0451"
spec_refs:
    - docs/range-deletes.md
---

## Description

Part of 0452 (docs/range-deletes.md §1 "In-RAM fragment set"). Pure data structures in vals-store `range_tombstone`, no engine wiring.

## Outcome

range_tombstone::FragmentSet: canonical (lo, hi, version) fragments keyed by lo; overlay is a join returning the spans where cover rose, refuses (without mutating) growth past max_fragments (default 65,536), from_records rebuild, O(log) cover, CoverCursor for key-ordered walks. encode/decode_rt_value for the kind-2 record value, sharing bound validation with the RT block codec. Property tests vs a pointwise model (deltas, canonical form, cursor, join in any order/split, idempotence, cap); they caught a left-adjacent coalescing bug during development. Fragment seq dropped (order-dependent); design doc updated.

Commit: feat(0455): canonical range-tombstone fragment set (pointwise-max overlay with exact deltas, cover lookup, monotone cursor, fragment cap) and RT vlog value codec

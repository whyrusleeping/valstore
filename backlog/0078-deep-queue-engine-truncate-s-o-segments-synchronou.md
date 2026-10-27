---
id: "0078"
title: Deep-queue Engine::truncate's O(segments) synchronous header wipe (same spin class as task 0067)
status: done
priority: 4
created: "2026-07-06"
updated: "2026-08-11"
depends_on:
    - "0075"
spec_refs: []
---

## Description
Noticed during task 0075: `Engine::truncate` (crates/vals-store/src/engine.rs ~4016) still issues one **synchronous** `write_at` per segment header across its two wipe phases — the same O(region) serial-round-trip spin class task 0067 fixed for `wipe_region` (and task 0075 avoided in `Engine::create::format_device`). On a large SPDK region (e.g. 1 TiB / 1 MiB segments ≈ 1M writes) a wipe-before-rejoin truncate can stall the shard thread for minutes at QD1.

Scope: deep-queue truncate's header writes through the async seam (`submit_write` + bounded in-flight window + reap, like `format_device`), **preserving the two-phase crash ordering exactly** (phase A: non-used headers, drain + flush; phase B: used headers, drain + flush). Consider factoring a shared helper with `format_device`.

## Acceptance criteria
- `Engine::truncate` no longer performs O(segments) synchronous per-block writes; writes are deep-queued with completion checking.
- The two-phase durability ordering (phase A durable before phase B starts; each phase drained + flushed) is preserved and documented.
- Existing truncate/stale-manifest tests (`stale_manifest_repro.rs`, engine truncate tests) stay green.

## Plan

1. Factor the existing bounded async segment-header wipe loop into a shared Engine helper that submits through `submit_write`, caps in-flight writes, polls/reaps every handle, propagates completion errors, and drains before returning.
2. Use that helper in `format_device` and in each of `truncate`'s two wipe phases, retaining a flush between phases and after phase B before allocator/in-memory state is reset.
3. Add focused device-instrumented truncate tests that prove writes are submitted at queue depth (not via synchronous `write_at`), completions are drained before each flush, and no used-segment phase-B header is submitted before phase A's flush.
4. Update the format documentation to state the exact phase-A drain/flush then phase-B drain/flush crash ordering.
5. Run focused truncate/stale-manifest tests plus the vals-store test suite (or the broadest practical equivalent).

### Starting points
- crates/vals-store/src/engine.rs:1156 format_device async wipe loop
- crates/vals-store/src/engine.rs:4549 Engine::truncate two serial wipe phases
- docs/formats.md#Segment liveness & format discipline
- crates/vals-store/tests/stale_manifest_repro.rs

## Work log
- 2026-08-11 plan: 1. Factor the existing bounded async segment-header wipe loop into a shared Engine helper that submits through `submit_write`, caps in-flight writes, polls/reaps every handle, propagates completion er
…[truncated]
- 2026-08-11 context hints: 4 recorded with plan
- 2026-08-11 context hints: Keep code comments minimal per project preference; put ordering design prose in docs/formats.md.; Preserve unrelated working-tree changes, especially backlog task 0077.
- 2026-08-11 preload: 6 file(s), ~18 KiB seeded into implementer context
- 2026-08-11 implementer report: Implemented task 0078.  Changes: - Factored the bounded async header-wipe loop into `Engine::wipe_segment_headers`, using `submit_write`, a 256-write in-flight cap, polling/reaping, deferred completio
…[truncated]
- 2026-08-11 review tier: single-opus — reviewers: sol
- 2026-08-11 review (sol): accept — The change correctly factors the existing bounded async header-wipe path into a shared helper, uses it for both truncate phases and device formatting, drains and checks all submitted completions, and 
…[truncated]
- 2026-08-11 decision: accept — commit: store: deep-queue truncate header wipes
- 2026-08-11 usage: 1,916,266 tok (in 793,548, out 16,798, cache_r 1,105,920, cache_w 0) · cost n/a (unpriced)
  implementer: 765,916 tok (in 292,929, out 8,859, cache_r 464,128, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 599,542 tok (in 287,313, out 4,517, cache_r 307,712, cache_w 0) · cost n/a (unpriced)
  coordinator: 550,808 tok (in 213,306, out 3,422, cache_r 334,080, cache_w 0) · cost n/a (unpriced)

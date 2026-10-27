---
id: "0036"
title: snapshot_io::write_chain leaks reserved manifest segments on non-DeviceFull I/O write errors
status: done
priority: 3
created: "2026-07-04"
updated: "2026-07-04"
depends_on: []
spec_refs:
    - ARCHITECTURE.md#Storage engine (single node)
---

## Description
## Bug (noted while fixing task 0033)

`snapshot_io::write_chain` reserves segments for the manifest chain and only frees them on the `DeviceFull` shortfall path — a plain I/O write error (`BlockError::Io`) bails without returning the reserved segments to the allocator, despite its doc comment claiming "any I/O error before commit" returns them. The leaked segments stay allocated until process restart/recovery (recovery reclaims any segment no manifest names).

This affects both callers: `Engine::flush_commit` and `Engine::compact_commit` — their own error-path cleanup (added/verified in 0033) frees the *run's* segments, but the manifest chain's own reserved segments still leak inside `write_chain`.

Evidence: the 0033 kill-the-fix test (`flush_commit_manifest_write_failure_clears_gate_and_reclaims_segments`) had to compare `used_segments()` against the post-build footprint rather than the pre-plan baseline specifically because of this leak.

## Acceptance criteria

- `write_chain` (or `write_manifest`) frees its reserved manifest segments on every pre-commit error path (I/O errors included), matching its doc comment.
- Test: inject an I/O write failure on the manifest chain write and assert `used_segments()` returns to the pre-call value.

## Acceptance criteria

## Plan

Fix the reserved-segment leak in `snapshot_io::write_chain` on pre-commit I/O errors, and add kill-the-fix tests.

1. In `crates/vals-store/src/snapshot_io.rs::write_chain`, the pre-commit write phase (`write_seg(i)?` for non-head segs, `dev.flush()?`, `write_seg(0)?`, final `dev.flush()?`) currently propagates errors without returning the reserved `segs` to the allocator. Restructure so that ANY error in that phase frees all reserved segments before returning Err — e.g. wrap the write phase in an inner closure/block returning `Result<(), SnapshotError>`, and on Err free every seg in `segs` (`let _ = alloc.free(*s)`) then propagate. Do NOT touch the post-commit prev-scrub logic (invariant: Err ⇒ nothing durable changed, prev still live — that must keep holding, and it does since head-write/flush failing means the head was never durably committed... note the head-write-then-flush-fails case: the head block may have hit media; consider best-effort scrubbing the written segments' header blocks (one zeroed block write each, errors ignored, no flush) before freeing, mirroring the prev-scrub hygiene, so a torn same-or-higher-generation head can't linger while its segments return to the allocator. Use judgement: at minimum scrub the head segment's header block on the error path after the head write may have landed; scrubbing all written segments is fine too.
2. Unit test in the `snapshot_io` tests module: a wrapper device over the existing mem test device that fails `write_at` (and/or `flush`) with `BlockError::Io` — inject failure at various points (at least: a non-head segment write for a multi-segment payload, and the head write/final flush) and assert `alloc.free_segments()` returns to the pre-call value and Err is `SnapshotError::Io`. Also assert a previously-written `prev` chain is still loadable afterwards (prev untouched on Err).
3. Tighten the 0033 engine test `flush_commit_manifest_write_failure_clears_gate_and_reclaims_segments` (crates/vals-store/src/engine.rs ~line 6373): it currently asserts `used_segments() < used_after_build` because of this leak; capture `used_segments()` BEFORE `flush_plan` and assert the footprint returns exactly to that pre-plan baseline after the failed `flush_commit`, updating the comment that documented the weaker assertion.
4. Update `write_chain`'s doc comment if needed (it already claims the fixed behavior — make sure it now matches, incl. any scrub note).
5. Verify: `cargo test -p vals-store` (plus `cargo build`/clippy if quick) passes.

### Starting points
- crates/vals-store/src/snapshot_io.rs:170-267 — write_chain; leak is the `?` on write_seg/flush in the write phase after the up-front reservation
- existing DeviceFull rollback pattern at snapshot_io.rs:186-196
- existing shortfall test `write_chain` DeviceFull test near snapshot_io.rs:585
- crates/vals-store/src/engine.rs:6326-6459 — ManifestWriteFailDevice + flush_commit_manifest_write_failure_clears_gate_and_reclaims_segments (assertion to tighten at ~6423-6428)

## Work log
- 2026-07-04 plan: Fix the reserved-segment leak in `snapshot_io::write_chain` on pre-commit I/O errors, and add kill-the-fix tests.  1. In `crates/vals-store/src/snapshot_io.rs::write_chain`, the pre-commit write phase
…[truncated]
- 2026-07-04 context hints: 4 recorded with plan
- 2026-07-04 context hints: crates/vals-store/src/snapshot_io.rs:170-267 — write_chain; leak is the `?` on write_seg/flush in the write phase after the up-front reservation; existing DeviceFull rollback pattern at snapshot_io.
…[truncated]
- 2026-07-04 implementer report: Fixed the reserved-manifest-segment leak in `snapshot_io::write_chain` on pre-commit I/O errors and added kill-the-fix tests.  ## Changes  **crates/vals-store/src/snapshot_io.rs** - Restructured the p
…[truncated]
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change correctly fixes the reserved-manifest-segment leak in `snapshot_io::write_chain`. The pre-commit write phase is wrapped in an inner closure (`write_phase`), and on any error (I/O included) 
…[truncated]
- 2026-07-04 decision: accept — commit: fix(store): write_chain frees reserved manifest segments on any pre-commit error (task 0036)  A plain I/O write/flush error during the chain write phase used to bail without returning the up-front-res
…[truncated]
- 2026-07-04 usage: 19,081 tok (in 90, out 18,991, cache_r 884,573, cache_w 67,736) · cost n/a (unpriced)
  implementer: 13,169 tok (in 48, out 13,121, cache_r 531,353, cache_w 33,608) · cost n/a (unpriced)
  reviewer:Claude: 2,985 tok (in 30, out 2,955, cache_r 187,049, cache_w 19,433) · cost n/a (unpriced)
  coordinator: 2,927 tok (in 12, out 2,915, cache_r 166,171, cache_w 14,695) · cost n/a (unpriced)

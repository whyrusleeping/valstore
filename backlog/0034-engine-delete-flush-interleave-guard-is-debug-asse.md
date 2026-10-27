---
id: "0034"
title: Engine::delete flush-interleave guard is debug_assert-only — silent key resurrection possible in release builds
status: done
priority: 3
created: "2026-07-04"
updated: "2026-07-04"
depends_on: []
spec_refs:
    - ARCHITECTURE.md#Storage engine (single node)
---

## Description
## Finding (from the 2026-07 correctness review)

`Engine::delete` (crates/vals-store/src/engine.rs, ~line 1359) must not interleave with an in-flight non-blocking flush: unlike a cluster tombstone-*put*, it **removes** the key from the memtable, so a flush that snapshotted the key commits a run holding its old value and the version-recheck drop in `flush_commit` cannot shadow it — a later read resurrects the deleted key.

Today this is guarded only by:

- `debug_assert!(!self.flush_in_flight, ...)` — **compiled out in release builds**, so a production caller gets silent key resurrection instead of a panic;
- the convention that the cluster layer never engine-deletes (it tombstone-puts). Nothing enforces this for a future direct caller of the public `Engine::delete` API (the crate is a sealed public API — external users can reach it).

## Fix

Make the guard real: return an error (e.g. a new `EngineError::FlushInFlight`, or reuse `Poisoned`-style refusal semantics) when `flush_in_flight` is set, keeping the debug_assert if desired. Alternatively, make `delete` correct under an in-flight flush (append the tombstone *and* leave a high-seq marker the version-recheck keeps) — but the cheap refusal is enough given the cluster never calls it.

## Acceptance criteria

- `Engine::delete` during an in-flight flush fails loudly in release builds (test compiled without debug assertions, or a test asserting the `Err` return).
- Doc comment updated to state the enforced behavior.


## Acceptance criteria

## Plan

1. Add a new `EngineError::FlushInFlight` variant (doc comment explaining the resurrection hazard: `Engine::delete` removes the memtable entry, so a run committed by an in-flight flush snapshot would resurrect the key; the delete is refused rather than risking that).
2. In `Engine::delete` (crates/vals-store/src/engine.rs ~line 1657): replace the `debug_assert!(!self.flush_in_flight, ...)` with a real check that returns `Err(EngineError::FlushInFlight)` when `self.flush_in_flight` is set. Drop the debug_assert (it would panic in debug builds before the Err is observable, making the behavior inconsistent between build profiles and untestable). Check ordering vs the `poisoned` check — keep `poisoned` first (it is the broader refusal), then `flush_in_flight`.
3. Update `Engine::delete`'s doc comment to state the enforced behavior: returns `EngineError::FlushInFlight` while a non-blocking flush is in flight; callers should retry after the flush commits/aborts.
4. Update the `Display` impl (and any exhaustive `match` on `EngineError` elsewhere in the crate/workspace) for the new variant. Grep the workspace for matches on `EngineError` to catch all sites; confirm no cluster-layer caller of `Engine::delete` can now hit the new error path in normal operation (the cluster tombstone-puts and never engine-deletes).
5. Test: in engine.rs tests, arm `flush_in_flight` via `flush_plan` (see the existing pattern around line 4709 / 6322 in tests), call `delete` on a present key, and assert it returns `Err(EngineError::FlushInFlight)` AND that the key is still readable and no tombstone got appended (delete was fully refused). Then complete/abort the flush and assert delete succeeds. This asserts the Err return, so it fails loudly regardless of debug assertions (satisfying the release-build criterion).
6. Run `cargo build` and the vals-store test suite (plus clippy if cheap).

### Starting points
- crates/vals-store/src/engine.rs:1647-1678 — Engine::delete with the debug_assert guard
- crates/vals-store/src/engine.rs:876-912 — EngineError enum; Display impl right below
- test pattern for arming flush_in_flight: flush_plan without commit, see engine.rs tests ~4709 and ~6322
- rg -n 'EngineError::' across the workspace to find exhaustive matches

## Work log
- 2026-07-04 plan: 1. Add a new `EngineError::FlushInFlight` variant (doc comment explaining the resurrection hazard: `Engine::delete` removes the memtable entry, so a run committed by an in-flight flush snapshot would 
…[truncated]
- 2026-07-04 context hints: 4 recorded with plan
- 2026-07-04 context hints: crates/vals-store/src/engine.rs:1647-1678 — Engine::delete with the debug_assert guard; crates/vals-store/src/engine.rs:876-912 — EngineError enum; Display impl right below; test pattern for armin
…[truncated]
- 2026-07-04 implementer report: Replaced the release-compiled-out `debug_assert!` guard in `Engine::delete` with a real, enforced error return.  ## Changes (all in crates/vals-store/src/engine.rs) 1. **New `EngineError::FlushInFligh
…[truncated]
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change replaces the release-compiled-out `debug_assert!` in `Engine::delete` with a real enforced guard: a new `EngineError::FlushInFlight` variant (with a documented resurrection-hazard rationale
…[truncated]
- 2026-07-04 decision: accept — commit: fix(store): Engine::delete refuses with EngineError::FlushInFlight during an in-flight flush (task 0034)  Replace the debug_assert-only guard (compiled out in release, risking silent key resurrection)
…[truncated]
- 2026-07-04 usage: 17,858 tok (in 106, out 17,752, cache_r 922,732, cache_w 55,655) · cost n/a (unpriced)
  implementer: 12,563 tok (in 68, out 12,495, cache_r 682,990, cache_w 31,077) · cost n/a (unpriced)
  coordinator: 3,603 tok (in 18, out 3,585, cache_r 172,252, cache_w 13,884) · cost n/a (unpriced)
  reviewer:Claude: 1,692 tok (in 20, out 1,672, cache_r 67,490, cache_w 10,694) · cost n/a (unpriced)

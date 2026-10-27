---
id: "0278"
title: Complete client operation and byte budgets beyond coordinator writes
status: done
priority: 2
created: "2026-09-08"
updated: "2026-09-08"
depends_on:
    - "0264"
spec_refs:
    - docs/production-readiness-review-2026-08.md#R9 — Client liveness and memory protection are opt-in (P1)
---

## Description
Accepted remaining scope split from 0264, not speculative hardening. 0264 implements finite network deadlines and strict logical coordinator write/delete/batch admission; reads/scans, direct transport calls, retained encoded frames and response allocations remain outside that credit pool, and sloppy fallbacks charge home destinations rather than actual fallback destinations.

Acceptance:
- Extend validated client operation/byte/per-node pressure protection to read/scan and direct transport paths, defining precisely admitted vs waiting vs retained allocations and avoiding double charging with coordinator write credits.
- Charge actual fallback destinations where relevant; a stalled node must not starve disjoint healthy destinations.
- Bound outstanding transport work and retained client-owned request/response memory across cancellation and timeout, releasing credits only when the accounted resource is actually released.
- Preserve explicit benchmark opt-outs and coordinate/document consumer migration.
- Deterministic failure/cancellation/partial-failure accounting and healthy-node-progress tests; request-path measurement according to project hardware gate policy.
- Do not alter write-concern/persistence semantics (0242) or imply independent Go clients have acquired these Rust limits.

## Acceptance criteria

## Plan

Trace coordinator, mux, and response ownership; define a precise finite-budget contract that covers direct/read/scan RPCs, actual destinations, retained encoded buffers and responses without double charging logical write credits. Implement the smallest coherent ownership-based admission solution, preserving benchmark opt-outs and existing write semantics. Add deterministic accounting, cancellation/timeout, partial-failure, and healthy-node progress regression tests, document Rust consumer migration and budget boundaries, and run workspace checks. Assess request-path measurement against spec hardware policy; if hardware validation is a separable gate, track it explicitly as accepted dependent follow-on rather than claiming unmeasured adoption.

### Starting points
- crates/vals-client/src/coord/credits.rs
- crates/vals-client/src/net/client/transport.rs
- docs/backpressure.md
- spec.md:64-84; plans/box-provisioning-runbook.md

## Work log

- 2026-09-08 final fresh review: **accepted software slice**. Reviewer independently audited all B1/B2/B3/A1 fixes and watermark/credit/deadline invariants, reproduced 147 client tests five times plus repeated splice tests, and 611 cluster tests (4 ignored) with bounded test parallelism. No remaining acceptance blocker. Non-digest read-many now uses borrowed MixedBatchRead instead of SyncValues (scalar fallback for peers without that capability); task 0279 includes this shape and cancellation splice cost in the gate tooling. Hardware performance adoption remains conditional on task 0287, not implied by this closure. Old 'not accepted' and stash-isolation notes below describe the superseded checkpoint; stash remains preserved for recovery.

- 2026-09-08 coordinated revision: restored only 0278's stash-parent deltas in vals-client, four vals-cluster paths, and docs/backpressure.md; preserved the stash and all later 0261/store work. Fixed the three prior blockers: borrowed mixed-read fanout keys (no waiting owned key clones), per-entry digest winner/fallback retention overload, and tri-state FD accounting with local overload neutral. Added five targeted regressions.
- Fresh independent review additionally found cancellation of wholly unsent multiplexed requests reset the shared connection and classified innocent sibling requests as remote failures. Revised cancellation/timeout to synchronously excise the exact unsent frame, reclaim capacity before dropping credits, adjust subsequent watermarks/deadlines, and preserve sibling progress. Partial-send cancellation still tears down but collateral errors are local Overloaded (FD/hint neutral). Added bounded/unbounded sibling-survival, timeout-splice, and partial-send tests; documented decoded-response transient expansion. No orphan/reaper retention or dependency on future polling was introduced.
- Revision validation: 147 vals-client unit tests + 2 doctests, full `cargo build --workspace && cargo test --workspace`, workspace clippy (existing warnings), and diff check pass. A first-round timing-sensitive delayed-hello test failed once, passed isolated, and passed subsequent full runs. Fresh independent re-review requested; no hardware adoption claim, gate 0279 remains required. No fleet changes or commit made by the implementer.

- 2026-09-08 task 0261 isolation: prior unaccepted code/docs changes are preserved in git stash `b68db4bc2d5b6cc268a0d75f71260604e7f445b4` (`pre-existing 0278 unaccepted client-budget implementation preserved during 0261`), not shipped by 0261. Before resuming, restore only `crates/vals-client`, the changed `crates/vals-cluster` paths, and `docs/backpressure.md` from that stash; do not apply its whole index/backlog snapshot over newer task state. Existing review blockers below still apply. Task 0279 dependency/membership updates and this work log remain in the backlog.

- Coordinator checkpoint after three implementation/review rounds: **not accepted; leave in_review and do not commit as complete**. Remaining blockers from final review: (1) `coord/read.rs::read_many_in` clones per-destination key bytes before admission and retains them uncharged in waiting futures; borrow them or admit actual-destination byte ownership before cloning. (2) `read_many_digest_once` winner/fallback retention overload returns outer Err, discarding unrelated successful per-key results; preserve per-entry overload and fallback overload classification. (3) `coord/write.rs` FD wrappers count local Overloaded as success, clearing real failure streaks without network contact; make local overload neutral (neither success nor failure), including batch classification. Add targeted regressions and obtain fresh review before acceptance. Final coordinator check: 138 vals-client unit tests and 2 doctests pass; diff check passes. Implementer reports workspace tests/check passing. Required hardware gate remains 0279. Changes intentionally remain uncommitted.

- 2026-09-08: Added a finite per-`NetTransport` ownership pool for every read,
  scan, write, control, anti-entropy, and direct wire RPC. Actual destination
  operations and encoded frames are admitted before retention; inbound frame
  capacity and mux replies remain charged through decode/handoff. Cancellation,
  timeout, and connection failure release only after queued/accounted storage is
  released. The existing coordinator pool remains the distinct logical-write
  ownership bound, avoiding duplicate accounting of caller inputs.
- Added deterministic cancellation, pre-send timeout, connection-failure, and
  disjoint-node progress tests. Updated Rust migration/boundary documentation.
  Task 0279 prepares its tooling; the required SPDK request-path performance and
  fault gate remains explicitly tracked by dependent task 0287. No local result
  is claimed as that gate.
- 2026-09-08 implementer report: Implemented task 0278.  - Added a separate finite `NetTransport` ownership pool, using validated `ClientConfig` global/per-node op and byte limits for every read, scan, write, control, anti-entropy, a
…[truncated]
- 2026-09-08 revision: Revised task 0278’s existing implementation to close the review blockers:  - Made abandoned unsent mux work self-tearing-down: cancellation or pre-send timeout now resets the connection, physically
…[truncated]
- 2026-09-08 revision: Completed the third-pass 0278 revision while preserving the existing implementation.  Changes: - Reader resets now always physically discard partial-frame storage, even when no reader bytes were charg
…[truncated]

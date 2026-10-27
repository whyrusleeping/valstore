---
id: "0013"
title: 'Multi-drive relocate: crash journal + streaming (data-loss risk at RF=1)'
status: done
priority: 2
created: "2026-07-02"
updated: "2026-07-04"
depends_on: []
spec_refs: []
---

## Description
BACKLOG.md §Deferred (multi-drive) — but this one is a **data-loss risk**, not a nicety: `relocate` (the offline multi-drive rebalance step) buffers each moved partition fully in RAM and has **no crash journal — a crash mid-relocate can destroy a not-yet-copied source region; real loss at RF=1**. Also slow (offline per-key copy).

## Acceptance criteria
- A crash at any point during `relocate` leaves the source recoverable (journal or copy-then-commit ordering); kill-the-fix crash tests.
- Moved partitions stream instead of buffering fully in RAM.
- (Stretch) live drive hot-swap remains a separate item; don't scope-creep into it.

## Acceptance criteria

## Plan

Rewrite `relocate` in crates/vals-cluster/src/drive.rs to be crash-safe and streaming.

Problems today: (1) it reads EVERY moving partition's entries fully into RAM before writing any destination (because a destination region can be another move's not-yet-copied source), and (2) it has no crash journal — a crash after a destination format wipes a source region that only existed in RAM → real data loss at RF=1.

Design:

1. **Copy-then-commit move ordering (streaming, no bulk RAM buffering).**
   - Model the moved partitions as moves src(drive,base) → dst(drive,base). Each region is the src of ≤1 move and the dst of ≤1 move, so the "dst == another move's src" dependency graph is chains + cycles.
   - Process moves so a destination region is formatted only after the move sourcing from it has durably completed: repeatedly run any move whose dst is not the src of a still-pending move. Per move: open src (create=false; missing source ⇒ empty, as today), open dst (create=true), stream entries via existing `VersionedStore::keys()` + `stored()` + `apply()` (one entry at a time — RAM drops from O(all moved data) to O(keys), which the engine index already holds), `sync()` the dst, then mark the move done.
   - **Cycles** (in-place slot swaps): break by staging one cycle member's src into a spare **temp region** — a slot on some drive unused by both old and new layouts — copy src→temp (streamed, synced), mark it staged, which releases its src; the rest of the cycle then drains as a chain, and the staged move finally copies temp→dst. If no spare slot exists anywhere, return a new `LayoutError` (refuse to run unsafely) rather than silently risking the swap.

2. **Crash journal via the DriveSet seam** (same pattern as the existing layout-fingerprint seam):
   - New defaulted trait methods `read_relocate_journal() -> Ok(None)`, `write_relocate_journal(&[u8])` no-op, `clear_relocate_journal()` no-op. `MemDriveSet` implements them (RefCell-backed, like the fingerprint). SPDK durable persistence stays a follow-on, same as the fingerprint (documented).
   - Journal record (magic/version/crc envelope like `Layout::encode`): old layout encoding, new layout encoding, per-move status (pending / staged@temp(drive,base) / done). Persisted BEFORE any destructive write; re-persisted after each status transition (status only advances after the corresponding data is synced). On success: write the new layout fingerprint, then clear the journal.
   - **Resume**: `relocate` first reads the journal. If present it must match the passed old/new layouts (else a corrupt/mismatch error); done moves are skipped, staged moves resume from their temp region, pending moves redo from their (still-intact) src. Invariant: a src region is only ever overwritten after the move reading it is durably done/staged, so at any crash point every partition's data exists durably at exactly one known place.
   - **Reopen guard**: `verify_layout` additionally refuses to open (new `LayoutError::RelocateInProgress` or similar) when a relocate journal is present — the operator must re-run `relocate` to completion first. Prevents reopening against half-moved regions.

3. Keep the public signature `relocate(ds, old, new, geom, node) -> Result<usize>` (bench caller unaffected). Assert old/new region_blocks match. Update the doc comment + docs/multi-drive.md rebalance paragraph.

4. **Tests (kill-the-fix)** in drive.rs `mod tests`:
   - Crash-at-every-point sweep: a fault-injecting DriveSet wrapper that errors on the k-th destructive operation (open_region with create=true, and/or journal write); for every k: run relocate (fails), then re-run relocate to completion, reopen with create=false, and assert every key readable at the new layout. This fails on the old code (no journal ⇒ rerun after a partial move copies wrong/empty data).
   - Forced-cycle test: hand-craft old/new `Layout`s (same module ⇒ private fields accessible) where two/three partitions swap regions; assert data survives, including with a crash injected mid-cycle (after staging, before the swap completes).
   - No-spare-slot cycle refuses with the new error.
   - Journal mismatch (journal from a different relayout) errors; `verify_layout` refuses reopen while a journal is present; completed relocate clears the journal and reopen passes.
   - Existing tests (relocate_moves_only…, drive_failure…, offline_rebalance…) must stay green unchanged.

5. Verify: `cargo test -p vals-cluster` and workspace build; `cargo clippy -p vals-cluster` clean for touched code.

Out of scope (per task): live hot-swap; durable SPDK journal/fingerprint block (already tracked with task 0006's ride-alongs).

### Starting points
- crates/vals-cluster/src/drive.rs — relocate() at ~line 435; DriveSet trait ~294 (fingerprint seam to mimic); verify_layout ~344; Layout::encode/decode ~665; tests module has MemDriveSet + NoFpDriveSet wrapper pattern
- crates/vals-cluster/src/replica.rs — VersionedStore::keys() (line ~959), stored() (~971), apply() (~1400), sync() (~555): the streaming per-entry copy primitives; Stored.value None == tombstone, apply(key, None, ver) preserves it
- Layout fields are private but tests are in the same module — hand-craft Layouts to force swap cycles
- caller to keep compiling: crates/vals-bench/src/main.rs:1193 drive::relocate(&ds, &before, &after, geom, 1)

## Work log
- 2026-07-04 plan: Rewrite `relocate` in crates/vals-cluster/src/drive.rs to be crash-safe and streaming.  Problems today: (1) it reads EVERY moving partition's entries fully into RAM before writing any destination (bec
…[truncated]
- 2026-07-04 context hints: 4 recorded with plan
- 2026-07-04 context hints: crates/vals-cluster/src/drive.rs — relocate() ~line 435; DriveSet trait ~294 with the fingerprint seam to mimic; verify_layout ~344; Layout::encode/decode ~665; MemDriveSet ~493; tests' NoFpDriveSet
…[truncated]
- 2026-07-04 implementer report: Rewrote `relocate` in crates/vals-cluster/src/drive.rs to be crash-safe and streaming (Task 0013 — the RF=1 data-loss risk).  ## What changed  **crates/vals-cluster/src/drive.rs** - **Streaming copy
…[truncated]
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change fully satisfies the task: relocate now streams per-key (no full-partition RAM buffering) and is crash-safe via a per-move journal persisted before destructive writes and advanced only after
…[truncated]
- 2026-07-04 decision: accept — commit: Multi-drive relocate: crash journal + streaming copy (fixes data-loss risk at RF=1) (task 0013)
- 2026-07-04 usage: 64,429 tok (in 158, out 64,271, cache_r 4,372,213, cache_w 210,561) · cost n/a (unpriced)
  implementer: 45,545 tok (in 96, out 45,449, cache_r 2,973,013, cache_w 86,970) · cost n/a (unpriced)
  reviewer:Claude: 10,617 tok (in 30, out 10,587, cache_r 310,758, cache_w 38,151) · cost n/a (unpriced)
  coordinator: 8,267 tok (in 32, out 8,235, cache_r 1,088,442, cache_w 85,440) · cost n/a (unpriced)

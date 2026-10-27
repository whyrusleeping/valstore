---
id: "0033"
title: flush_commit error path leaves flush_in_flight set forever → zone flushes permanently disabled → unbounded memtable growth
status: done
priority: 2
created: "2026-07-03"
updated: "2026-07-04"
depends_on: []
spec_refs:
    - ARCHITECTURE.md#Storage engine (single node)
---

## Description
## Bug (code-confirmed)

`Engine::flush_commit` (crates/vals-store/src/engine.rs) propagates errors with `?` from both fallible steps — `write_manifest` and `DeviceRun::open` — **without clearing `flush_in_flight` and without freeing the written run's segments**. The caller (`node.rs::maint_loop`, `MaintDone::Flushed → Err(e)` arm at ~line 918) merely logs "retrying next tick", but no retry is possible: `snapshot_loop` gates on `!flush_in_flight()`, which stays `true` forever, and the `WrittenFlush` was consumed. Net effect for that zone until process restart:

- **No further flushes**: the memtable grows without bound under sustained writes — the exact OOM (249 GB soak) the byte-cap flush trigger was added to prevent.
- The written run's segments stay allocated but unreferenced (space leak until recovery).
- `Engine::delete`'s flush-interleave guard (debug_assert on `flush_in_flight`) would also trip spuriously forever in debug builds.

Realistic trigger: `write_manifest` returns `SnapshotError::DeviceFull` on a nearly-full zone region — not an exotic device fault. Note the asymmetry: `compact_commit` handles exactly this correctly (frees the reserved segs and clears the gate via `free_compact_segs` on both of its fallible steps); `flush_commit` predates that discipline.

## Fix

Mirror `compact_commit`: on `write_manifest` failure, free `written.handle.segs` (+ already-freed unused tail is fine) and clear `flush_in_flight` before returning `Err`; on `DeviceRun::open` failure *after* the manifest committed, decide explicitly (probably: keep the manifest, log, clear the gate — the memtable still holds the keys so serving is correct; or roll the manifest back by re-emitting the previous run set). Add a kill-the-fix test: inject a manifest-write failure (device full), assert `flush_in_flight() == false` afterward and that a later flush succeeds once space frees.

## Acceptance criteria

- A failed `flush_commit` leaves the engine able to flush again (gate cleared, segments reclaimed).
- Kill-the-fix test that fails with the current code.


## Acceptance criteria

## Plan

Fix `Engine::flush_commit` (crates/vals-store/src/engine.rs, ~line 1828) to mirror `compact_commit`'s error discipline so a failed flush commit leaves the engine able to flush again:

1. **Reorder the two fallible steps** to match `compact_commit` (~line 2181): open the `DeviceRun` over the serving handle FIRST (fallible, mutates nothing durable), THEN write the manifest. This eliminates the awkward "open failed after the manifest committed" case entirely — both fallible steps now occur before the commit point.
   - On `DeviceRun::open` failure: free `written.handle.segs` (the used prefix; the over-reserved tail is already freed at the top of the fn) and clear `flush_in_flight` — reuse `free_flush_segs` which does exactly this — then return `Err`. The prior manifest and runs remain live; the memtable still holds all the keys, so serving is correct.
   - On `write_manifest` failure: drop the opened run, `free_flush_segs(&written.handle.segs)`, return `Err`. Nothing durable changed.
2. **After the commit point** (manifest durable): everything is infallible — install `manifest_gen`, `live_manifest`, `last_flush_epoch/seq`, push the `OpenRun`, do the version-recheck memtable drop, clear `flush_in_flight`. Keep the existing doc comments accurate; add a short comment noting the commit-point discipline mirrors `compact_commit`.
3. **Kill-the-fix test** (in engine.rs's test module): add a `ManifestWriteFailDevice` modeled on the existing `ManifestCrashDevice` (~line 4998) but returning `Err(BlockError::Io(...))` from `write_at` when armed and the buffer starts with the manifest chain magic (`[0x56,0x4d,0x4e,0x43]`), instead of silently dropping the write. Test flow:
   - Create engine, put keys, take `flush_plan` + `build_and_write_flush` (as in the `run_flush` helper ~line 3720).
   - Arm the device, call `flush_commit` → assert it returns `Err`.
   - Assert `e.flush_in_flight() == false` (fails with current code — kill-the-fix).
   - Assert the reserved segments were reclaimed (e.g. via an allocator free/used-count accessor if one exists, or indirectly by the subsequent flush succeeding on a tight device).
   - Disarm, drive a full flush again (`run_flush`) → assert it succeeds and the keys still resolve via `get`.
   - Optionally also assert the engine still serves the keys from the memtable after the failed commit.
4. Check the `maint_loop` caller in node.rs (~line 918, `MaintDone::Flushed → Err(e)` arm): with the gate now cleared, its "retrying next tick" log message becomes accurate — adjust the comment/message only if it's misleading; no behavior change needed there.
5. Verify: `cargo test -p vals-store` (at least the engine tests) and `cargo build` pass; confirm the new test fails when the fix is reverted (mentally or by inspection of the assertion on `flush_in_flight`).

### Starting points
- crates/vals-store/src/engine.rs:1828 Engine::flush_commit — the buggy fn (two `?` without cleanup)
- crates/vals-store/src/engine.rs:2181 Engine::compact_commit — the correct pattern to mirror (open-before-manifest, free_compact_segs on both error arms)
- crates/vals-store/src/engine.rs:1889 free_flush_segs — frees segs + clears flush_in_flight; reuse on error paths
- crates/vals-store/src/engine.rs:4998 ManifestCrashDevice — template for a ManifestWriteFailDevice (return Err instead of swallowing)
- crates/vals-store/src/engine.rs:3720 run_flush test helper — how tests drive flush_plan → build_and_write_flush → flush_commit
- node.rs maint_loop MaintDone::Flushed Err arm (~line 918) — caller that logs 'retrying next tick'

## Work log
- 2026-07-04 plan: Fix `Engine::flush_commit` (crates/vals-store/src/engine.rs, ~line 1828) to mirror `compact_commit`'s error discipline so a failed flush commit leaves the engine able to flush again:  1. **Reorder the
…[truncated]
- 2026-07-04 context hints: 6 recorded with plan
- 2026-07-04 context hints: crates/vals-store/src/engine.rs:1828 Engine::flush_commit — the buggy fn (two `?` without cleanup); crates/vals-store/src/engine.rs:2181 Engine::compact_commit — the correct pattern to mirror (ope
…[truncated]
- 2026-07-04 implementer report: Fixed the `flush_commit` error-path bug and added a kill-the-fix test.  ## Change: `Engine::flush_commit` (crates/vals-store/src/engine.rs ~line 1828) Reordered the two fallible steps to mirror `compa
…[truncated]
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change correctly fixes the flush_commit error-path bug by mirroring compact_commit's discipline: it opens the DeviceRun before writing the manifest (both fallible steps now precede the commit poin
…[truncated]
- 2026-07-04 decision: accept — commit: Fix flush_commit error path: clear flush_in_flight + reclaim run segments on failure (task 0033)  - Engine::flush_commit now mirrors compact_commit's commit-point discipline:   DeviceRun::open runs be
…[truncated]
- 2026-07-04 usage: 30,238 tok (in 116, out 30,122, cache_r 1,394,029, cache_w 98,199) · cost n/a (unpriced)
  implementer: 18,831 tok (in 58, out 18,773, cache_r 847,335, cache_w 50,086) · cost n/a (unpriced)
  coordinator: 7,682 tok (in 30, out 7,652, cache_r 388,460, cache_w 28,144) · cost n/a (unpriced)
  reviewer:Claude: 3,725 tok (in 28, out 3,697, cache_r 158,234, cache_w 19,969) · cost n/a (unpriced)

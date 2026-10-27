---
id: "0065"
title: 'Live hot-swap: close (or document) the post-swap-pre-Done journal durability window'
status: done
priority: 5
created: "2026-07-05"
updated: "2026-07-05"
depends_on:
    - "0059"
spec_refs: []
---

## Description
Follow-on from task 0059 (live drive hot-swap). During a live rebalance there is a narrow window between a shard's swap ack (it now serves from the new region) and the relocate journal recording that move `Done` on the orchestration thread. A process crash inside that window leaves the journal saying `Pending`, so the offline resume re-streams the OLD region into the destination with `create=true` — wiping any writes admitted post-swap. Mostly this matches the store's normal acked-but-unsynced crash semantics (nothing synced the new engine yet), but if a group-commit sync fires inside the window, durably-synced acked writes can be lost at RF=1 (RF≥2 heals via anti-entropy).

Possible directions (judgement): persist the `Done` journal status inside the shard's synchronous no-await stretch (copy → sync → persist Done → swap), which requires the DriveSet journal seam to be reachable from the shard thread (DS: Send+Sync — MemDriveSet now is); or make the offline resume non-destructive (apply-if-newer into an existing dst engine instead of reformatting) when the dst region already holds a valid engine; or document + accept with an explicit RF=1 caveat.

## Acceptance criteria
- The window is either closed (with an off-hardware test forcing a crash between swap and Done-persist, proving post-swap synced writes survive the offline resume) or explicitly evaluated and documented as accepted semantics (in drive.rs/node.rs docs + BACKLOG.md), with reasoning about group-commit sync timing.
- Existing live/offline relocate tests keep passing.

## Acceptance criteria

## Plan

Close the post-swap-pre-Done durability window by making the relocate journal record a move complete BEFORE the shard's swap goes live, using a two-phase (prepare/commit) handshake. The journal persist stays on the orchestration thread (which already holds `&DS`), so no `DriveSet: Send+Sync` plumbing is needed; the shard simply blocks (no `.await`) between its durable sync and the swap, so no write can be admitted inside the reordered window.

New per-move ordering: quiesce → copy live engine to dst → sync dst → [shard blocks] → orchestrator persists journal (move Done/Staged) → commit → shard installs the swap → ack.

Crash analysis (to be documented in code):
- crash before the commit-persist: swap never installed (shard blocked or aborts on dropped channel), old region intact and complete → journal Pending → offline resume re-streams. Safe.
- crash after commit-persist, before install: the shard was blocked in the synchronous stretch since the sync, so no write was admitted after the copy; dst holds the complete synced copy; journal Done → resume skips the move. Safe.
- crash after install: journal already Done → post-swap group-commit-synced writes in dst survive the offline resume (the window is closed). Acked-but-unsynced post-swap writes are lost per the store's normal crash semantics (RF≥2 heals via anti-entropy).

Changes:
1. crates/vals-cluster/src/drive.rs — `relocate_with`: extend the mover signature with a commit callback, e.g. `FnMut(&Partition, &Partition, &mut dyn FnMut() -> Result<(), LayoutError>) -> Result<(), LayoutError>`. Before each mover call, relocate_with builds the post-move status vector (Done for a normal move, Staged{temp} for the cycle-staging call) and passes a commit closure that persists that journal. The offline mover (`relocate` → `stream_partition`) ignores the callback (relocate_with still persists after the mover returns — idempotent, and required for the offline path). Update docs to state the mover contract: calling commit makes the move durable-complete, so the mover must guarantee no acked write can fall between the data it synced and the swap going live.
2. crates/vals-cluster/src/node.rs — `NodeCtl::SwapStore` gains the handshake: add `prepared: mpsc::Sender<Result<(), String>>` (sent after copy+sync, pre-swap) and `commit: mpsc::Receiver<()>` alongside the final `reply`. In `swap_store`: after the existing quiesce, run the prepare step (fallible); on success send Ok on `prepared`, then block on `commit.recv()` — a std blocking recv, still inside the synchronous no-await stretch with the read pause held. `Ok(())` → install the swap (infallible) → reply Ok. `RecvErr` (orchestrator dropped it: journal persist failed or it died) → abort: drop the prepared engine, keep serving the OLD engine, reply Err — journal stays Pending, offline resume handles it. Deliberately NO timeout on the recv: a timeout-abort could race an already-persisted Done (journal says Done while data keeps landing in the old region) — document this. Prepare failure → send Err on prepared (and reply Err) without swapping.
3. crates/vals-cluster/src/node.rs — live mover in `rebalance_shards`: open dst → send SwapStore{dev, geom, node, prepared_tx, commit_rx, reply_tx} → recv prepared; on Ok call the commit callback (journal persist). If persist fails, drop commit_tx (shard aborts, keeps old engine), drain reply, return Err. If it succeeds, send () on commit_tx, then recv the final reply as today.
4. crates/vals-cluster/src/net/server.rs — split `swap_default_zone` into `prepare_default_zone_swap` (sync old engine → create new engine over new_dev → stream keys → sync new; returns the prepared VersionedStore; fallible) and `commit_default_zone_swap` (replace the Rc contents + devices-map entry; infallible). Rewrite the now-stale ordering comment (lines ~1012-1022) to describe the new commit-before-swap ordering and why the old-engine sync is still needed (Pending-resume path).
5. Test (node.rs tests, modeled on `multidrive_live_rebalance_crash_mid_move_recovers_offline`, reusing `FaultDrives`): force the "crash" right after the first swap goes live by failing the first post-mover journal write (with the new code the journal-write sequence per move is: commit-persist from the mover, then relocate_with's post-mover persist; plus the initial Pending-vector persist as write #1 — so fail_at = 3 hits the first post-swap write). The rebalance errors out with move 1 swapped and live. Then write a fresh batch of keys through the coordinator (post-swap writes; enough keys to hit every partition), shut the node down cleanly (sync — modeling "a group-commit sync fired inside the window"), run the offline `relocate(&ds, &old, &new, geom, 1)` resume, reopen with `build_stores`, and assert every key — pre-rebalance AND post-swap — is present. (Under the old ordering this exact injection leaves the move Pending and the resume re-streams the old region over the post-swap writes — the bug.) Keep the existing live-swap and crash-resume tests passing; their journal-write counts shift (each live move now persists twice), which the existing test tolerates by design.
6. Docs: update BACKLOG.md's live hot-swap entry (~line 963) to note the window is closed (copy → sync → persist Done → swap via the prepare/commit handshake) and record task 0065.
7. Verify: cargo fmt --check, cargo clippy on vals-cluster, cargo test -p vals-cluster (at least the multidrive_* and relocate tests plus the new one).

### Starting points
- crates/vals-cluster/src/drive.rs: relocate_with (~line 546), persist_journal, MoveStatus, FaultDrives-compatible journal seam
- crates/vals-cluster/src/node.rs: NodeCtl::SwapStore (~line 122), rebalance_shards mover (~line 359), swap_store (~line 1293), tests: FaultDrives (~4148), multidrive_live_rebalance_crash_mid_move_recovers_offline (~4437)
- crates/vals-cluster/src/net/server.rs: swap_default_zone (~line 1000) — split into prepare/commit
- BACKLOG.md ~line 963: live hot-swap entry to update
- crates/vals-cluster/src/lib.rs line 152 re-exports relocate_with (signature change is workspace-internal)

## Work log
- 2026-07-05 plan: Close the post-swap-pre-Done durability window by making the relocate journal record a move complete BEFORE the shard's swap goes live, using a two-phase (prepare/commit) handshake. The journal persis
…[truncated]
- 2026-07-05 context hints: 5 recorded with plan
- 2026-07-05 context hints: crates/vals-cluster/src/drive.rs: relocate_with (~line 546), persist_journal (~753), MoveStatus (~679); crates/vals-cluster/src/node.rs: NodeCtl::SwapStore (~line 122), rebalance_shards mover (~line 3
…[truncated]
- 2026-07-05 implementer report: Closed the live hot-swap post-swap-pre-`Done` journal durability window by making the relocate journal record each move complete BEFORE the shard's swap goes live, via a prepare/commit handshake. No `
…[truncated]
- 2026-07-05 review tier: single-opus — reviewers: Claude
- 2026-07-05 review (Claude): accept — The change closes the post-swap-pre-Done durability window exactly as planned, via a prepare/commit handshake: the shard copies+syncs the live engine, posts `prepared`, blocks (no `.await`, read pause
…[truncated]
- 2026-07-05 decision: accept — commit: cluster: close the live hot-swap post-swap-pre-Done journal durability window (task 0065)  Reorder each live relocate move to copy -> sync -> persist Done -> swap via a prepare/commit handshake: the s
…[truncated]
- 2026-07-05 usage: 61,824 tok (in 182, out 61,642, cache_r 4,261,429, cache_w 245,683) · cost n/a (unpriced)
  implementer: 31,674 tok (in 94, out 31,580, cache_r 2,794,881, cache_w 82,002) · cost n/a (unpriced)
  coordinator: 24,756 tok (in 50, out 24,706, cache_r 1,132,366, cache_w 134,676) · cost n/a (unpriced)
  reviewer:Claude: 5,394 tok (in 38, out 5,356, cache_r 334,182, cache_w 29,005) · cost n/a (unpriced)

---
id: "0070"
title: 'Liveness lease: node held down past gc_grace unfences without wiping (startup sweep races PD membership push)'
status: done
priority: 2
created: "2026-07-06"
updated: "2026-07-06"
depends_on:
    - "0041"
spec_refs: []
---

## Description
**Safety bug found on hardware** (task 0041 A.4, oldbox 2026-07-06; full evidence + root cause in `bench/spdk-liveness-box-notes.md`). A `backend = spdk` node stopped gracefully and held down ~565 s past a 45 s `gc_grace` did **not** wipe on restart: it unfenced 1 ms after boot with its own stale watermark (`vals_wipes 0`) and AE-synced new data on top of the stale data — defeating the wipe-before-rejoin lease (tombstone-GC resurrection risk). Affects all backends whose peers arrive via the PD membership push; SPDK just boots fast enough to lose the race reliably.

**Root cause**: `run_node` calls the eager startup `shared.kick_sweep()` (crates/vals-cluster/src/node.rs ~L2616) before PD's `SetMembers` push lands, so `netpeers` is transiently empty. The liveness block (~L2769) treats empty `netpeers` as "lone node has nothing to be stale against" → `rejoin_decision(local, &[], grace) == Recover` → `record_contact(now)` unfences permanently. The wipe branch is gated on `fenced()`, and `refence_if_stale` can't re-trip because `last_contact` was just refreshed. Log ordering proof: `unfenced (watermark 1783333449980ms)` at 10:31:11.730, `adopting ring config epoch 6` at 10:31:11.948.

**Fix direction** (from the notes; final call is the implementer's): don't let the transiently-empty `netpeers` unfence a born-fenced node — gate the empty-peer "lone node" unfence on the *adopted membership* actually having ≤ 1 member (or on having ever adopted a membership), so a node stays born-fenced until it either verifies against a real peer or confirms it is genuinely solo. Whole-cluster-restart recovery semantics (all peers froze together → Recover untouched) must be preserved.

## Acceptance criteria
- A node with a stale watermark that boots before the membership push arrives stays fenced until it verifies against a real peer; once peers are known and it is past grace, it wipes + re-syncs (fence held through the refill, as today).
- A genuinely solo node (membership of 1 / no peers configured or adopted) still unfences and serves.
- Whole-cluster restart (no peer ahead) still recovers untouched.
- Off-hardware regression test that reproduces the race by delaying membership/peer wiring until after the first sweep (the existing `serve_with_liveness` tests wire peers synchronously and cannot catch this).
- Re-verify scenario A.4 on the SPDK box (wipe fires, node re-syncs, read-verify passes) — or, if no box session is possible, file the box re-check as a follow-on and note it in `bench/spdk-liveness-box-notes.md`.

## Plan

**Bug**: In `sweep_loop`'s liveness block (crates/vals-cluster/src/node.rs ~L2759-2810), `netpeers.is_empty()` is treated as "lone node → safe to unfence". On the PD-managed path the first (eager) sweep runs before the PD's `SetMembers` push lands, so `netpeers` is transiently empty and a born-fenced node with a stale watermark unfences permanently (record_contact refreshes `last_contact`, so `refence_if_stale` can never re-trip) — defeating wipe-before-rejoin.

**Fix (semantics)**: a node with empty `netpeers` may take the "in contact / lone node" branch only when it is *affirmatively* solo:
1. a membership push has been adopted this run (`SetMembers` handler applied a push — track a new `membership_adopted: Cell<bool>` on `ServerShared`, set in the accepted branch of the handler in net/server.rs, both the reconfigure and members-only paths) AND the *current* ring has ≤ 1 member (read the live ring, not the pass snapshot, to be race-proof); OR
2. the node is pristine: its liveness log had **no persisted watermark at boot** (`load()` returned `None` — track `had_persisted_watermark: Cell<bool>`, set in `load_persisted_watermark`). A first-boot node has no prior life and nothing stale to serve, so unfencing it is safe; this preserves the existing lone-node tests (`multidrive_with_liveness_unfences_...`, `multidrive_zoned_with_liveness_...`) which use fresh logs and never receive a push.

Otherwise (empty netpeers, prior watermark, no adopted membership): **stay fenced** and do nothing this pass — no `record_contact`, no `refence_if_stale` needed (the fence is already up / nothing changed). The `SetMembers` push kicks the sweep, so the decision re-runs immediately when membership arrives. Log (debug or once-per-run info) that the node is holding its born-fence waiting for membership.

Encapsulate as `ServerShared::lone_unfence_allowed(&self) -> bool` with a thorough doc comment explaining the race this closes (reference the A.4 box finding). Initialize the new `Cell`s in **both** `ServerShared` constructors.

**Preserved semantics** (must not regress):
- peers known + gossip succeeds → existing wipe/recover/unfence path unchanged;
- peers known but unreachable → `refence_if_stale` path unchanged;
- whole-cluster restart (peers in ring, all watermarks frozen) → Recover untouched (existing test);
- genuinely solo: fresh node unfences on first sweep (pristine rule); a solo node with a prior watermark unfences once its 1-member membership is adopted (PD push). Deliberate safety choice: a no-PD solo node restarting with a persisted watermark now stays fenced until a (1-member) membership push — document this in the code comment.

**New regression tests** (crates/vals-cluster/src/node.rs tests module, modeled on `liveness_straggler_wipes_instead_of_resurrecting_a_gcd_delete` ~L3496 and its helpers `set_members_config`/`read_raw`/`is_fenced_reply`):
1. **The A.4 race**: node 1 serves with liveness, ring of just `[1]`, empty/self-only peer list, `MemLivenessLog` seeded with an old watermark, holding a stale pre-delete key; node 2 serves separately with an advanced watermark (and the tombstone GC'd, as in the existing straggler test). Assert node 1 **stays fenced across several sweep intervals** (reads refused, no wipe). Then push `SetMembers` (epoch ≥1, members [1,2], **with addrs** so node 1 learns node 2's address — push to node 2 first, then node 1, mirroring the PD). Assert node 1 then wipes (log's `wipes()` / wipe counter), re-syncs, unfences, and the deleted key does NOT resurrect.
2. **Solo with prior life**: node ring `[1]`, log seeded with an old watermark → stays fenced over several sweeps; push a 1-member `SetMembers` (epoch 1, members [1]) → unfences and serves.
3. (Cheap) **pristine lone node** unfences on its first sweep without any push — direct single-node test if not clearly covered by the existing multidrive tests.

**Also**:
- Update the stale comment above the `!peer_wms.is_empty() || netpeers.is_empty()` condition in node.rs and the liveness.rs module docs ("lone node" semantics).
- Append a short update to `bench/spdk-liveness-box-notes.md` under A.4: fix landed (this task), off-hardware regression test added, on-box re-verify filed as a follow-on task (I'll create that task).
- Run `cargo test -p vals-cluster` (full crate) + `cargo build --workspace` to make sure nothing else regressed.

### Starting points
- crates/vals-cluster/src/node.rs ~L2616 (eager kick_sweep) and ~L2751-2811 (liveness block in sweep_loop) — the condition to change
- crates/vals-cluster/src/net/server.rs ~L2879-2929 (SetMembers handler — set membership_adopted), ~L599-651 (load_persisted_watermark / record_contact), constructors at ~L235 and ~L886 (init new Cells)
- crates/vals-cluster/src/node.rs tests: liveness_straggler_wipes_instead_of_resurrecting_a_gcd_delete (~L3496), set_members_config (~L5798), set_members (~L3217), is_fenced_reply (~L3197), multidrive_with_liveness_unfences_and_persists_per_shard_watermarks (~L4797, lone-node pristine unfence to preserve)
- bench/spdk-liveness-box-notes.md — root-cause writeup (A.4) + fix direction
- netpeers is built from the ring snapshot minus self, so empty netpeers ⇒ ring has ≤1 member; peers learned via shared.peer_addrs_of from the push's addrs

## Work log
- 2026-07-06 plan: **Bug**: In `sweep_loop`'s liveness block (crates/vals-cluster/src/node.rs ~L2759-2810), `netpeers.is_empty()` is treated as "lone node → safe to unfence". On the PD-managed path the first (eager) s
…[truncated]
- 2026-07-06 context hints: 5 recorded with plan
- 2026-07-06 context hints: crates/vals-cluster/src/node.rs ~L2616 (eager kick_sweep) and ~L2751-2811 (liveness block in sweep_loop) — the condition to change; crates/vals-cluster/src/net/server.rs ~L2879-2929 (SetMembers hand
…[truncated]
- 2026-07-06 implementer report: Fixed the liveness-lease startup race (task 0070): a node held down past gc_grace that boots before the PD's SetMembers push arrives no longer unfences on its own stale watermark.  ## Changes  **crate
…[truncated]
- 2026-07-06 review tier: high-powered — reviewers: Claude
- 2026-07-06 review (Claude): accept — The change correctly closes the liveness-lease startup race (task 0070). The sweep's empty-`netpeers` "lone node" unfence is now gated on the new `ServerShared::lone_unfence_allowed()`, which allows u
…[truncated]
- 2026-07-06 decision: accept — commit: liveness: hold the born-fence until membership is adopted — close the startup race that let a stale node unfence before the PD push (task 0070)  The eager startup sweep runs before the PD's SetMembe
…[truncated]
- 2026-07-06 usage: 69,968 tok (in 288, out 69,680, cache_r 7,182,184, cache_w 300,125) · cost n/a (unpriced)
  implementer: 33,569 tok (in 164, out 33,405, cache_r 4,330,189, cache_w 88,059) · cost n/a (unpriced)
  coordinator: 30,311 tok (in 82, out 30,229, cache_r 2,480,618, cache_w 182,581) · cost n/a (unpriced)
  reviewer:Claude: 6,088 tok (in 42, out 6,046, cache_r 371,377, cache_w 29,485) · cost n/a (unpriced)

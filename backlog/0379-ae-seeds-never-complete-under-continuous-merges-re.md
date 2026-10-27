---
id: "0379"
title: 'AE seeds never complete under continuous merges (restart on every runs_removed_epoch bump): one pinned shard per seed costs −23% ACK at scale (SOAK5B)'
status: done
priority: 1
created: "2026-10-05"
updated: "2026-10-06"
depends_on: []
spec_refs: []
---

## Description
## Evidence
SOAK5B: 3 nodes, master 92e25363 with 0374. Raw data in `/data/dataset/task0363-results/SOAK5B/`; read-only attribution by agent_7, 2026-10-05.
- **Trigger:** a PD rehome at ~1.60B ACK (prepare 15:34:31Z, commit 15:35:17Z, zone 2) invalidated AE leaves on every node. `vals_ae_seed_active` went 0 → 2/1/1 and stayed there until the drain; a second rehome at ~3.0B raised it to 3/1/2.
- **Seeds never completed under ingest:**
  - `vals_ae_seed_wall_us` frozen from 1.6B to the drain;
  - `vals_ae_seed_restarts` 1 → 161 (node1) and 1 → 60 (node2);
  - journal at its 256 MiB cap, spilling.
- **Cause:** `lifecycle.rs:657` restarts the whole fold on any `runs_removed_epoch` bump, and every merge bumps it (`merge.rs:1006/1951`). Merges are continuously in flight (2.0) under ingest.
- **Effect:**
  - each stuck seed pins its shard at ~95% CPU (others 0.6–0.7);
  - ACK is gated by the slowest shard, falling from 1.575M to **1.21M (−23%)** from 1.67B to 2.40B;
  - probably also part of the post-step rate;
  - the affected shards have no AE coverage for the whole ingest.
- 0374 removed the synchronous fallback, so under sustained merge churn nothing ever completes the seed. The 0374 review flagged this as a minor "no backstop" item.

## Fix options (pick, justify, test)
- **(a) Pin the seed's input runs:** protect their segments from reuse, as keep-sets do, until the seed publishes. A merge then doesn't invalidate the seed's snapshot. Bound the extra disk held and fall back if it exceeds a cap.
- **(b) Resume from a key cursor** across run-set changes instead of restarting, if exactness can be proven: fold the new run set from the cursor key, and account for keys moved by the merge.
- **(c) Skip the full invalidation on a rehome** when this store's local spans are unchanged (the `local_reuse` check at `lifecycle.rs:1132-1156`). This narrows the trigger (related to 0377) but does not fix restart-on-merge for post-restart recovery seeds.

Prefer (a) or (b), plus (c) if cheap.

## Acceptance (preregister)
Run the SOAK5B arm to 2.4B ACK with at least one rehome commit (natural or forced) between 1.4B and 1.7B.
- **P1:** every post-rehome seed completes within 900 s under full ingest: `vals_ae_seed_active` back to 0, `wall_us` increments, ≤ 8 restarts per seed, 0 sync fallbacks.
- **P2:** in each 100M bucket after the commit, the busiest `vals-shard` thread is ≤ 1.15× the median (SOAK5B ~1.42×).
- **P3:** mean ACK 1.7–2.4B ≥ 1.45M (SOAK5B 1.21M), and the 300M after the commit ≥ 0.92× the 300M before it.
- **Guards:**
  - correctness (cursor, samplecheck, exact counts, owned aediag);
  - RSS at matched ACK ≤ SOAK5B + 1 GB;
  - disk held by pinned runs is reported.
- **Unit tests:** a seed completes under continuous merges (deterministic); exactness vs a from-scratch seed; knockout (the old restart rule never completes).

## Also observed (next walls, not this task)
- Merge RAM reservations grow 1–2 → 4–8 GB against a fixed 8.71 GB maintenance pool. Beyond ~3B keys per node the broker throttles merges (node2 at 3.9–4.0B: 977 deferrals, merges per 100M 30 → 13).
- Merge lanes are CPU-bound (`BlockedBloom::insert` is the top merge-lane symbol).
- malloc/free is ~29% of shard cycles (flat).

## Acceptance criteria

## Work log

### 2026-10-05: (a) implemented — seeds pin their input runs (agent, local only; uncommitted)
- **Design.** At start a seed takes `Engine::pin_runs`: a pin token over the current run handles plus per-run stream sources (sidecar locator, or block index + LBAs for v5). A merge/forget commit that removes a pinned run keeps its segments allocated in the engine's retired list (protected from orphan reclaim, staged frees and log compaction) instead of freeing them. The seed then rides through `runs_removed_epoch` bumps (`ae_seed_merge_carries`). Retired runs are freed by a reap once no live pin holds them (seed driver after every step; maintenance tick). In-memory only, so restarts cannot leak; truncate and store drop discard them.
- **Exactness.** Snapshot fold (T0 memtable + pinned T0 runs) + journal replay = live state, because a merge changes logical winners only via journaled retirements: shadowed versions are never winners; a GC-dropped tombstone or forgotten winner is reported in `current_dropped_tombstones` / forget's dropped list and journaled as a retire (fold +1, retire −1); a drop a survivor run would resurrect is refused at commit. Tombstones are winners in leaves and counts on both sides.
- **Bounds.** Pinned bytes ≤ the seed's starting run set. `ae_seed_pin_max_bytes` (64 GiB, node-wide; 0 = old restart rule) → seed restarts if its engine holds pinned runs while the node is over the cap (`ae_seed_pin_cap_fallbacks`); seeds pin at start only below the cap. Region pressure (admission band, maintenance floor, or held ≥ free segments) revokes the engine's pins at the next merge commit/reap (`ae_seed_pin_revocations`). Metrics: `ae_seed_pinned_bytes` gauge, `ae_seed_merge_restarts` (unpinned), `ae_seed_completions`, `ae_seed_duration_ms` histogram (ms bounds, s sum); placement/generation resets keep their counters.
- **(c) not implemented — premise is wrong.** Leaves are XORs per (sorted replica set, bucket). A peer-only change ({A,B}→{A,C}) moves A's keys to another group, so A's leaves must be regrouped; skipping the invalidation would leave them under {A,B} and A would mismatch both B and C. The existing rule (keep leaves only when the store holds no key in any changed span, 0322) is already the exact skip condition. Span regroup stays 0377. Test `peer_only_rehome_changes_groups_so_local_leaves_are_reseeded` pins this.
- **Tests.** vals-store `engine::tests::pins` (segments held and unread-changed across sync and off-core merges, not reused, freed on release with no leak vs a control engine, truncate/pressure revoke). vals-cluster `replica::seed_pin_tests`: seed completes with every step interleaved with a merge commit (sync, off-core, forget, GC tombstone drops, flushes, memtable writes), exact vs recompute and vs a from-scratch seed; knockout `ae_seed_pin_max_bytes=0` never completes in 400 steps; pinned bytes released on publish/reset/clear/drop; cap and revocation restarts counted. Existing churn-fallback tests now run under the knockout.

### 2026-10-05: review (agent_5: merge with minors); hardware (SOAK5B-V2); committed
- **Review:**
  - segment safety holds (retired segments stay allocated, are protected from orphan reclaim, are freed only by reap/revoke/truncate, and the crash state is clean);
  - exactness holds for every merge path;
  - the `compact` guard only applies with zero runs (dead code), so run merges are never blocked.
- **Minors filed as 0380:**
  - cap thrash: when over the cap, every holder restarts, so two seeds each needing > cap/2 never finish; the cap is only checked per step;
  - uncharged block-index RAM for pinned v5 sources (~0.2% of pinned bytes);
  - metric gaps (`segments_freed` vs reap; carries count epoch changes, not merges);
  - `revoke_run_pins` is pub without a storage-gen bump;
  - test gaps (cleanup-forget, lone-GC, spill + carry, real revocation, node-level reap wiring).
- **Hardware:** `bench/0363-compact-values/SOAK5B-V2-FINDINGS.md`: master 64720509 + 0379 (pin e2a3f730, valsd 225e9d35…), 3 nodes, 5B cache to 2.4B ACK.
  - P1–P3 were NOT exercised: no rehome between 1.0B and 1.7B (only one, at 0.10B).
  - Descriptive evidence that pins work under ingest: after node2's exit-75 (RAM flip `xor=0x04`) at 2.14B, 8 recovery seeds rode through 28 merge carries under live ingest and completed within ~4 min. Totals: 0 restarts, 0 merge restarts, 0 cap fallbacks, pinned peak 7.09 GB. In SOAK5B, seeds never completed.
  - The no-rehome baseline 1.7–2.1B ran at 1.523M ACK/s.
  - P2's 1.15 limit has no margin: the noise floor is already 1.08–1.17.
- A rehome-window test (P1–P3) is still owed; the full SOAK5B-V3 run will exercise it if PD rehomes there. Note that starts − completions = 8 with 0 restarts, unexplained.

### 2026-10-06: in_review (backlog cleanup, owner-approved)
The code is committed and reviewed. What's left is hardware: P1–P3 across a rehome window. The next full-replay run should report it (it can share that run). Close once that run, or a forced rehome, gives the evidence.

### 2026-10-06: second independent review (agent_2, sol): two error-path exactness bugs, fixed; closed
- **Verified sound:** segment safety on every free path (orphan reclaim, staged frees, TRIM candidates, truncate, reopen), pins honoured for off-core and inner-window commits, pin and ledger release on every path, reap wiring.
- **MAJOR 1:** `compact_commit` and `merge_all_runs` installed the run set and bumped the epoch, then ran a fallible `rebuild_winner_directory()?`. On error, the dropped tombstones never reached the cluster journal, so a pinned seed could publish a removed winner.
  - Fix: `refresh_winner_directory_after_commit()` (winner.rs). After a commit, a rebuild failure drops the directory (None, degraded, warning) and returns Ok with the full stats.
- **MAJOR 2:** after `merge_all_runs` committed, a failed delete-append in sync `forget_keys` returned a plain Err, so the run-removed winners were never journaled.
  - Fix: return `PartialForget` with exactly the removed winners (reported so far, plus unreached run-only keys with no mutable entry).
- **Tests:**
  - engine: `merge_all_runs_keeps_committed_drops_when_the_directory_rebuild_fails`, `compact_commit_keeps_committed_drops_when_the_directory_rebuild_fails`, `forget_keys_append_failure_after_the_run_commit_reports_exactly_the_removed_winners`;
  - cluster `seed_pin_tests`: `failed_post_commit_directory_rebuilds_still_journal_retired_winners`, `partial_sync_forget_after_the_run_commit_journals_every_removed_winner`;
  - each knockout (fix reverted) fails; debug suites pass.
- **Same pattern, not fixed (added to 0380):** no-runs `compact_filtered` / `compact_step_filtered` drop keys, then poison on a zeroing failure, and the wrappers `?` before journaling (bounded: the engine is poisoned); `compact_commit` refreshes only when the directory is requested, not when it merely exists (test-only exposure); its forget loop swallows append errors without logging.
- **Acceptance:** P1–P3 across a rehome window were never exercised. They are carried to 0386, not passed. Recovery-seed evidence (SOAK5B-V2) supports the mechanism.


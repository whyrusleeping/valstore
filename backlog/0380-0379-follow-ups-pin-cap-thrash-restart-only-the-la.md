---
id: "0380"
title: '0379 follow-ups: pin cap thrash (restart only the largest holder), charge pinned index RAM, metric/counter reconciliation, revoke API, test gaps, flaky pd test'
status: done
priority: 3
created: "2026-10-05"
updated: "2026-10-11"
depends_on:
    - "0379"
spec_refs: []
---

## Description

Minors from the agent_5 review of 0379 (2026-10-05), plus SOAK5B-V2 observations.

1. **Cap thrash** (`lifecycle.rs` ~688-690). When the node is over `ae_seed_pin_max_bytes`, every engine holding retired bytes restarts its seed, not just the newest or largest holder. Two seeds each needing > cap/2 retired bytes restart forever, which is the old failure mode. The cap is also only checked at step time, so one large merge can overshoot by up to an engine's whole run set.
   - Fix: restart only the largest/newest holder (or apply hysteresis); check at retire time.
   - Consider sizing the cap relative to free region space instead of a fixed 64 GiB.
   - At 5B scale an engine's run set is ~100+ GB.
2. **Uncharged RAM.** Pinned v5 sources keep the removed run's block index alive (`run.rs` ~3522), ~0.2% of pinned bytes, not charged to the RAM broker. Charge it.
3. **Metrics:**
   - `segments_freed` misses reap frees and counts revoke frees inside later commits;
   - `ae_seed_merge_carries` counts epoch changes seen per step, not merges (document or fix);
   - SOAK5B-V2: node2 starts − completions = 8 with 0 restarts, unexplained. Reconcile the counters (are seeds reset by publish-less paths not counted?).
4. **`revoke_run_pins` is pub** without a storage-gen bump or data-reserve refresh (only `reap` does). Make it crate-private, or bump.
5. **Stale reject:** a just-restarted seed can see over-cap from its engine's not-yet-reaped retired bytes (one wasted step).
6. **Tests:**
   - a carry through 0367 cleanup-forget and lone-GC plans;
   - journal spill + carry;
   - revocation via a real admission-band / maintenance-floor condition;
   - node-level reap wiring (`sweep.rs` ~220, the snapshot tick);
   - fix the trivially-true `e.compact()` assertion in `pinned_runs_survive_merges_until_released`;
   - rename the stale test `cooperative_seed_restarts_on_run_set_change`.
7. **Flaky test:** `pd::tests::placement_change_reseeds_never_hold_commits_starts_or_splits` shares the global counter `pd_rehome_starts_deferred_recovery` with another pd test (`pd/tests.rs` ~2066-2075). Use a per-test registry or a delta assert.
8. **Same-pattern exactness gaps** (from the second 0379 review; the two MAJORs it found were fixed in 0379):
   - No-runs `compact_filtered` / `compact_step_filtered` (`reclaim.rs`) remove GC-dropped keys from the index, then poison the engine and return Err on a segment-zeroing failure. The cluster wrappers `compact` / `compact_step` (`replica/maintenance.rs` ~227, ~255) `?` before journaling `dropped`. Bounded by the poison (no further writes), but return the drops (as with `PartialForget`) and journal them.
   - `compact_commit` refreshes the winner directory only when it is globally requested, while `merge_all_runs` uses `is_some() || requested`. An engine with a hand-built directory keeps stale entries after an off-core forget or GC drop. Test-only exposure; make the two consistent.
   - `compact_commit`'s forget loop drops append errors silently (still exact); log them, rate-limited.

## Outcome

Items 1, 4, 5, 7 were done earlier. This change covers the vals-store parts: item 2 (held pinned v5 indexes counted in run_resident_bytes, so the RAM broker charges them), review minor (a) (the commit limit counts held_segs), items 8b and 8c, and the store-side item 6 tests (real admission-band revocation; the compact guard assert is now non-trivial). Each fix has a knockout-verified test. In a clean HEAD+change worktree: vals-store lib 459 passed, vals-cluster lib 806 passed. One reviewer accepted. Item 3 counters, the cluster tests from item 6 and item 8a need files that hold 0372/0377 uncommitted hunks, so they are split to 0427.

Commit: fix(0380): charge held pinned v5 run indexes to the RAM broker; pin commit limit counts held bytes; compact_commit refreshes existing winner directory and logs failed forgets; remainder split to 0427

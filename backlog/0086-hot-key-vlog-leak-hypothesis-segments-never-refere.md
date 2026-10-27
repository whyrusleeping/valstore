---
id: "0086"
title: 'Hot-key vlog leak hypothesis: segments never referenced by any run are never freed by co-compaction'
status: done
priority: 3
created: "2026-07-07"
updated: "2026-07-07"
depends_on: []
spec_refs: []
---

## Description
Suspected leak found while diagnosing task 0084 (unverified — needs a repro or a disproof):

Run-merge value-log co-compaction frees `(∪ merged inputs' value_segs) − keep`. A vlog segment whose **every** record was superseded *before the covering flush* is referenced by **no** run's `value_segs` (the flush's run entries point only at each key's newest record at flush time) — so it is in no input set and can never be freed by any merge. The legacy vlog compactor skips flushed zones, so nothing else frees it either.

Under uniform churn this is statistically rare (a 1 MiB segment holds thousands of records; some are almost surely a flush-time-newest for their key). But an adversarial/skewed workload — one hot key rewritten millions of times between flushes — produces whole segments containing only that key's dead versions → an unbounded, unreclaimable leak that the 0084 pressure trigger cannot fix (dead estimate rises, merges fire, but the orphan segments are outside every input's value_segs).

## Acceptance criteria
- Engine-level test: hammer ONE key with enough overwrites to fill several segments between two flushes, then merge; check whether the wholly-dead tail segments are freed. If they are (some mechanism I missed — e.g. watermark-based tail truncation), document why and close.
- If the leak is real: fix (candidates: include all segments in `(covered_epoch_prev, covered_epoch]` in the flush run's `value_segs` accounting; or an epoch-watermark vlog sweep that frees header-valid segments older than the oldest run-referenced epoch), with a kill-the-fix test.

## Plan

## Diagnosis (confirmed by code reading)

- Flush (`Engine::flush` ~L2379 and `flush_commit` ~L2563) sets the run's `value_segs = value_liveness(entries)` — only segments referenced by each key's flush-time-newest record.
- A vlog segment whose every record was superseded before the covering flush is in NO run's `value_segs`. Merge reclaim (`compact_commit` ~L3326) frees only `(∪ inputs' value_segs) − keep`, so it can never free such an orphan. Legacy `compact`/`compact_step` are no-ops once runs exist. Only the boot-time manifest-driven liveness rule (~L1369) reclaims orphans — i.e. a runtime leak, healed only by restart, exactly as hypothesized. The pressure trigger sees the dead bytes but merges cannot free them.

## Fix (task's candidate (a): include the covered tail in the flush run's value_segs)

1. **Tail-segment tracking in `ValueLog`** (crates/vals-store/src/vlog.rs): add `tail_segs: Vec<(u64 epoch, SegmentId)>`, pushed in `roll()` (the single choke point for log segment allocation). API:
   - `seed_tail_segs(...)` for recovery,
   - `tail_segs_through(epoch) -> impl Iterator/Vec<SegmentId>` (entries with epoch <= arg),
   - `prune_tail_segs_through(epoch)` (drop entries with epoch <= arg),
   - `forget_tail_seg(seg: SegmentId)` (remove by id).
   RAM cost: one pair per un-flushed tail segment — bounded by the flush cadence.

2. **Flush paths** (both `flush()` and `flush_commit()`): when co-compaction is enabled (`co_compact_enabled()`), set the new run's `value_segs = dedupe(value_liveness(entries) ∪ log.tail_segs_through(covered_epoch))`; `value_bytes` unchanged (orphans contribute 0 live bytes — correctly raising the dead estimate). After the manifest commit SUCCEEDS, `log.prune_tail_segs_through(covered_epoch)` (prune <=, not <: the boundary/open segment is already in this run's value_segs, and merge keep-sets protect the open segment + memtable-referenced segs anyway). No prune on failure paths. Gating on `co_compact_enabled()` matters: with the `VALS_VLOG_CO_COMPACT=0` kill switch only reboot-reclaim exists, and unioning would turn the runtime leak into a permanent one — keep status quo there.

3. **Recovery** (`open_with_versions` ~L1487, after constructing the ValueLog): seed the tracker with the recovered replayed tail — ordinary (`!not_tail`) `log_segs` with `epoch >= min_replay_epoch.unwrap_or(0)` — so orphans that form in a recovered tail are also covered by the next flush.

4. **Interactions with legacy compaction** (safety-critical — never let a FREED segment linger in the tracker, else a later flush pins it in value_segs and a future merge could free a reused, in-use segment):
   - `compact_step_filtered` (~L4120 free loop): after freeing each victim, `self.log.forget_tail_seg(victim)`.
   - Full-pass `compact`/`compact_filtered` (~L3879): swaps `self.log` for `newlog` whose own rolls were tracked — old tracker dies with the old log; nothing to do (verify).
   - `rollback_failed_compaction` (~L4374): replaces `self.log` with a fresh ValueLog while the OLD segments stay live — carry the old log's tracker over into the replacement so the leak fix survives a failed-compaction rollback.
   - `truncate` (~L4462): fresh log + empty tracker is correct (everything freed).
   - Merge/plain-merge paths (`compact_runs`, `compact_and_write`, `forget_keys`): NO change to merged-run value_segs (plain merge stays status quo; co-compact merge output is fresh segments). The tracker only ever holds segments with epoch > last covered_epoch, which (epochs never reused) can't appear in any run's value_segs, so merge dead-set frees can't race the tracker.

5. **Docs**: short addition to docs/out-of-core-index.md's co-compaction paragraph: flush's `value_segs` = referenced segments ∪ covered ordinary tail, closing the hot-key orphan-segment leak (wholly-superseded segments are now freed by the first co-compacting merge instead of leaking until reboot).

## Tests (engine.rs, kill-the-fix required by acceptance criteria)

- `hot_key_orphan_vlog_segments_freed_by_merge`: small geometry (MemBlockDevice); hammer ONE key with values sized to roll through several segments, plus a couple of other keys; flush; hammer more; flush again; `compact_runs` over both runs (assert the plan co-compacts); assert the wholly-dead segments are freed — e.g. `free_segments_for_test()` returns to within a small constant of total. Must FAIL without the fix (verify by reverting the union locally before finalizing).
- Reopen soundness: after a flush whose value_segs include orphans, reopen the engine (into_device + open) — no double-mark panic, keys read back correctly; a subsequent merge frees the orphans.
- `compact_step_frees_never_pinned_by_later_flush`: no-runs engine; churn across segments; run `compact_step` until victims are freed; then flush; assert every seg in the new run's `value_segs` is currently allocated (invariant: value_segs ⊆ allocated), and reopen is sane.
- Full suite: `cargo test -p vals-store` (plus a workspace `cargo build`) green.

### Starting points
- crates/vals-store/src/engine.rs: value_liveness() ~L934; flush() ~L2339; flush_commit() ~L2555; compact_commit dead-set ~L3326-3377; recovery liveness ~L1369-1441; ValueLog::new seed point ~L1487; compact full-pass log swap ~L3879; compact_step free loop ~L4120; rollback_failed_compaction ~L4374; truncate ~L4462
- crates/vals-store/src/vlog.rs: ValueLog struct ~L869; roll() ~L1329 (allocates via alloc.allocate_data)
- co_compact_enabled() ~engine.rs L900 (VALS_VLOG_CO_COMPACT kill switch)
- test helpers: free_segments_for_test() ~L3515, existing compact_runs tests ~L7234+, MemBlockDevice
- FlushPlan carries plan-time covered_epoch (~L184/L205/L303) — use written.covered_epoch in flush_commit
- docs/out-of-core-index.md ~L850: manifest-driven liveness paragraph to update

## Work log
- 2026-07-07 plan: ## Diagnosis (confirmed by code reading)  - Flush (`Engine::flush` ~L2379 and `flush_commit` ~L2563) sets the run's `value_segs = value_liveness(entries)` — only segments referenced by each key's fl
…[truncated]
- 2026-07-07 context hints: 6 recorded with plan
- 2026-07-07 context hints: crates/vals-store/src/engine.rs: value_liveness() ~L934; flush() ~L2339; flush_commit() ~L2555; compact_commit dead-set ~L3326-3377; recovery liveness ~L1369-1441; ValueLog::new seed point ~L1487; com
…[truncated]
- 2026-07-07 implementer report: ## Task 0086 — Hot-key vlog orphan-segment leak: CONFIRMED and FIXED  ### Diagnosis (confirmed by code reading) The hypothesis holds. `Engine::flush`/`flush_commit` set a run's `value_segs = value_l
…[truncated]
- 2026-07-07 review tier: single-opus — reviewers: Claude
- 2026-07-07 review (Claude): accept — The change correctly confirms and fixes the hot-key vlog orphan-segment leak. A per-log `tail_segs` tracker (pushed at the single `roll()` allocation choke point) lets flush union wholly-superseded ta
…[truncated]
- 2026-07-07 decision: accept — commit: store: fix hot-key vlog orphan-segment leak (task 0086) — flush unions the covered un-flushed tail into the run's value_segs (ValueLog tail tracker), so wholly-superseded segments are freed by the f
…[truncated]
- 2026-07-07 usage: 88,247 tok (in 344, out 87,903, cache_r 12,425,776, cache_w 368,790) · $13.5105
  implementer: 43,984 tok (in 216, out 43,768, cache_r 9,418,373, cache_w 156,895) · $6.7851
  coordinator: 29,543 tok (in 72, out 29,471, cache_r 2,129,673, cache_w 158,622) · $5.5867
  reviewer:Claude: 14,720 tok (in 56, out 14,664, cache_r 877,730, cache_w 53,273) · $1.1387

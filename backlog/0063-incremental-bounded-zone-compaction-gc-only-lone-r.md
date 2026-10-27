---
id: "0063"
title: Incremental (bounded) zone compaction + GC-only lone-run trigger
status: done
priority: 4
created: "2026-07-05"
updated: "2026-07-05"
depends_on: []
spec_refs: []
---

## Description
Split from grooming umbrella 0025 (BACKLOG.md §Deferred follow-ons, "Compaction" bullet). The *garbage-ratio trigger* half of that bullet is already largely superseded: `COMPACT_MIN_RECLAIM_FRACTION` (0.20 dead-fraction gate on the full-rewrite pass) and `RUN_COMPACT_THRESHOLD` (run-merge trigger) both exist in `crates/vals-cluster/src/node.rs`. What remains — and is called out in the code itself ("Until incremental compaction lands (a bounded pass that doesn't stall), this trigger is what keeps reclamation from being disruptive"):

1. **Incremental / bounded zone compaction**: the vlog full-rewrite compaction is a synchronous stop-the-world pass per zone — it quiesces reads and copies *all* live data forward on the serving core in one go. Make it a bounded pass (e.g. N segments per tick, resumable cursor) so reclamation never stalls serving for O(zone) time. The existing quiesce/ReadGate machinery bounds each step instead of the whole pass.
2. **GC-only compaction trigger for a lone run** (deferred from task 0038): tombstone GC only rides threshold-triggered ≥2-run merges, so an aged tombstone in a single quiescent run is never collected. Add a trigger that schedules a 1-run rewrite when it holds collectable aged tombstones past the grace period.

## Acceptance criteria
- Compaction of a large zone proceeds in bounded steps: max single serving-core stall bounded and asserted in a test (or measured off-hardware); total reclaim equals the old pass (equivalence test).
- A lone run containing only aged tombstones gets rewritten and its tombstones GC'd + Merkle leaves retired, without a second run appearing; test pins it.
- No regression in existing compaction/GC tests; BACKLOG.md bullet updated.

## Acceptance criteria

## Plan

Two halves, both in vals-store (engine) + vals-cluster (replica/node wiring).

## Part 1 — incremental (bounded) vlog compaction

New engine capability: a bounded, resumable compaction step that replaces the stop-the-world full rewrite on the node's compact_loop path (the full `compact()`/`compact_filtered()` stays — it is still used by the no-runs `forget` path, tests, and the equivalence test).

**Engine (`crates/vals-store/src/engine.rs`):**
- Add `compact_step_filtered(max_segments: usize, drop_live: impl Fn(&[u8], u128, &[u8]) -> bool) -> Result<(CompactStats, bool /*done*/), EngineError>` (+ thin `compact_step` wrapper without GC if convenient).
- Same guards as `compact_filtered`: poisoned → Err; `flush_in_flight` or `!runs.is_empty()` → no-op (done=true).
- Victim selection: used log segments in id order with id ≥ an internal non-persistent cursor field (`compact_cursor`), excluding (a) live-manifest chain segs, (b) the value log's currently-open segment (`ValueLog::cur_seg()` accessor already exists ~vlog.rs:1024). Take at most `max_segments`; advance the cursor past the last victim. Empty victim set → reset cursor to 0, return done=true.
- Copy phase differs from the full pass: live records / retained tombstones / GC tombstones are appended into the **active** log (`self.log`) — not a fresh newlog — using split field borrows (dev, alloc, log, index). Liveness/retention/GC predicates identical to `copy_live_records`.
- Commit order per step: append copies → `self.log.sync(&self.dev)` (copies durable before any old header is wiped) → repoint index / apply GC removals in-memory → wipe victim headers + `dev.flush()` (poison on failure, exactly like the full pass) → free victims in the allocator.
- Failure before the wipe needs NO rollback machinery: the appended records are same-version duplicates with higher seq (crash replay picks them; logical state identical), and a GC tombstone appended before a failed commit only means an allowed-early collection on crash replay. Document this reasoning in the method docs.
- Headroom precondition per step: victims' live bytes ≤ max_segments payloads, so require ~`max_segments + 1` free segments (shrink the victim count if fewer are free but ≥ 2). This means incremental compaction proceeds in regions too full for the full rewrite — note that in docs/log messages.

**Replica (`crates/vals-cluster/src/replica.rs`):** `VersionedStore::compact_step(wall_ms, max_segments) -> Result<(CompactStats, bool), _>` mirroring `compact()`: same grace/watermark GC predicate, collect dropped tombstones, retire Merkle leaves after the step.

**Node (`crates/vals-cluster/src/node.rs` compact_loop):** replace the single quiesce + full per-zone pass with a stepped loop per compactable zone: { pause ReadGate + bounded drain (keep the escalating `consecutive_timeouts` logic across steps); one `compact_step(K)`; drop the pause guard; check shutdown; if done or error → next zone; otherwise yield/await briefly so parked reads run before the next step }. K default ~4 segments, overridable via `VALS_COMPACT_STEP_SEGMENTS` (same pattern as `run_compact_threshold`). Keep the InsufficientHeadroom / error handling and counters; update the now-stale doc comments ("Limitation (v1): synchronous stop-the-world…", the COMPACT_MIN_RECLAIM_FRACTION "Until incremental compaction lands…" note).

## Part 2 — GC-only lone-run trigger

**Manifest v3 (`crates/vals-store/src/manifest.rs`):** add `RunMeta.tombstone_min_version: Option<u128>` — min packed version over tombstone-flagged entries (None = no tombstones, or legacy-unknown). Bump MAN_VERSION to 3; encode presence-flagged u128 appended per run (same pattern as the v2 tail); decode accepts v1/v2 (field None) and v3. Round-trip + legacy-decode tests.
- Populate it at every RunMeta construction site (engine.rs ~2146, ~2328, ~2634 merge_all_runs, ~2852 compact_commit; snapshot_io.rs ~481 legacy conversion → None). For streamed/off-core builds accumulate the min alongside the existing value_segs/value_bytes accounting (`value_liveness` or the build sink) rather than re-scanning.

**Engine:** allow a single-run GC rewrite: a lone-run-aware plan entry point (e.g. `compact_plan` gains an internal `min_input_runs`/flag with a new public method `compact_plan_lone_gc` or similar) that permits `runs.len() == 1` when the caller certifies GC is due; the merge/commit machinery already handles k=1 (bottommost merge, survivors kept, `current_dropped_tombstones` filtered vs memtable + survivor runs — same safety story as ≥2-run GC).

**Replica:** `lone_run_gc_due(wall_ms) -> bool` = exactly one run && `gc_collect_before(wall_ms)` is Some(cutoff) && that run's `tombstone_min_version` is Some(min) with min < cutoff. Legacy None = no trigger (documented; such a run gets GC'd on its eventual ≥2-run merge). Plus the plan wrapper.

**Node (snapshot_loop S3 section, ~node.rs:1887):** extend the compact trigger: fire also when `run_count() == 1 && !compact_in_flight() && lone_run_gc_due(now)`, planning via the lone-gc-aware planner; submit the same `MaintJob::Compact`. Self-limiting: after the rewrite the new meta reflects only surviving (younger) tombstones, so it re-fires only when they age past grace; no tombstones → None → never.

## Tests / acceptance
1. Engine: bounded-step test — a dirty multi-segment engine takes >1 step, each step touches ≤ K victims (assert via stats), and the final logical state (every key's `get`) + reclaim equals a clone that ran the full `compact()` (equivalence; allow the open-append-segment residual).
2. Engine: step-path aged-tombstone GC matches full-pass GC; step proceeds where full rewrite refuses headroom.
3. Manifest v3 round-trip + v2-legacy-decodes-None.
4. Replica/engine: lone run with aged tombstones (cutoff covers) → rewrite drops them, leaves retired, still exactly one run; younger tombstones survive; no trigger when GC disabled/no tombstones.
5. Existing compaction/GC/node quiesce tests keep passing (adapt node tests only if they assume the monolithic pass).
6. BACKLOG.md "Compaction" deferred-follow-ons bullet updated (both halves landed, task 0063).

Verify: `cargo build --workspace` (or check) + `cargo test -p vals-store -p vals-cluster` (full workspace test if time permits).

### Starting points
- crates/vals-store/src/engine.rs: compact_filtered ~L3265, copy_live_records ~L3418, merge_all_runs ~L2607, compact_plan ~L2720, compact_runs ~L2552; RunMeta construction sites ~L2146/2328/2634/2852
- crates/vals-store/src/vlog.rs ~L1024: ValueLog::cur_seg() accessor ('currently-open value-log segment... excluded from co-compaction')
- crates/vals-store/src/manifest.rs: MAN_VERSION v1/v2 pattern — presence-flagged appended tail is the established versioning idiom
- crates/vals-cluster/src/replica.rs: VersionedStore::compact ~L1440 (GC predicate + retire_leaf), gc_collect_before ~L1169, compact_plan ~L1367, compact_commit ~L1384
- crates/vals-cluster/src/node.rs: compact_loop ~L1415 (quiesce/drain/escalation), snapshot_loop S3 compact trigger ~L1887, run_compact_threshold ~L1281 (env-override pattern), COMPACT_MIN_RECLAIM_FRACTION ~L1324
- node tests to keep green: compaction_quiesces_inflight_reads_instead_of_starving ~L3217, compaction_escalates_quiesce_bound_under_sustained_bulk_reads ~L3323
- BACKLOG.md ~L993: 'Compaction — incremental…' deferred-follow-ons bullet to update

## Work log
- 2026-07-05 plan: Two halves, both in vals-store (engine) + vals-cluster (replica/node wiring).  ## Part 1 — incremental (bounded) vlog compaction  New engine capability: a bounded, resumable compaction step that rep
…[truncated]
- 2026-07-05 context hints: 7 recorded with plan
- 2026-07-05 context hints: crates/vals-store/src/engine.rs: compact_filtered ~L3265, copy_live_records ~L3418, merge_all_runs ~L2607, compact_plan ~L2720, compact_runs ~L2552; RunMeta construction sites ~L2146/2328/2634/2852; c
…[truncated]
- 2026-07-05 implementer report: Implemented task 0063 — incremental (bounded) zone compaction + GC-only lone-run trigger. Both halves landed across vals-store (engine) and vals-cluster (replica/node).  ## Part 1 — incremental (b
…[truncated]
- 2026-07-05 review tier: high-powered — reviewers: Claude
- 2026-07-05 review (Claude): accept — Both halves of task 0063 are implemented correctly, completely, and match the plan. Part 1 adds a bounded, resumable compaction step (engine + replica + node compact_loop reworked into a stepped quies
…[truncated]
- 2026-07-05 decision: accept — commit: compaction: incremental (bounded) vlog compaction + GC-only lone-run trigger (task 0063)  - Engine::compact_step_filtered/compact_step: bounded, resumable vlog compaction   (≤ max_segments victims p
…[truncated]
- 2026-07-05 usage: 108,172 tok (in 420, out 107,752, cache_r 16,665,529, cache_w 410,000) · cost n/a (unpriced)
  implementer: 68,976 tok (in 292, out 68,684, cache_r 12,849,877, cache_w 166,989) · cost n/a (unpriced)
  coordinator: 29,769 tok (in 86, out 29,683, cache_r 3,159,331, cache_w 192,278) · cost n/a (unpriced)
  reviewer:Claude: 9,427 tok (in 42, out 9,385, cache_r 656,321, cache_w 50,733) · cost n/a (unpriced)

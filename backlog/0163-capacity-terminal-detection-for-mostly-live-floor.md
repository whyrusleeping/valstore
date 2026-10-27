---
id: "0163"
title: Capacity-terminal detection for mostly-live floor engines (region_exhausted gated on empty memtable never fires; producers spin on Busy)
status: done
priority: 2
created: "2026-08-09"
updated: "2026-08-10"
depends_on: []
spec_refs:
    - docs/backpressure.md#Zone-region capacity and maintenance reserve
---

## Description
The 0161 hardware arm (`bench/bootes-soak-lab/run-20260809T163755Z-0161-shadow/FINDINGS.md`) showed the current floor behavior for a **mostly-live** engine: a bounded 1/s `compaction_plan_failures` WARN loop, client Busy (producers spin on 30 s batch retries indefinitely), and `region_exhausted` NEVER fires because `Engine::region_exhausted()` requires an empty memtable — a stalled engine that stopped admitting with a non-empty memtable can't satisfy it. Operators and clients get no crisp "this is capacity, stop retrying" signal; the only tell is `min_zone_free_segments` flat at the reserve.

Wanted: detect the capacity-stalled state (at floor + shadow estimate ≈ 0 + visible dead low + pressure plans persistently infeasible, memtable EMPTY-OR-NOT) and surface it as terminal `Response::Err("zone region exhausted…")`/`region_exhausted_rejections` (or a distinct capacity signal) so clients stop treating it as transient Busy, while never false-terminating a state that reclaim (aged GC / shadow fold / orphan sweep) could still recover. The 0161 estimator gives exactly the missing discriminator (garbage vs live).

Acceptance criteria:
- A mostly-live floor engine (repro shape: unique-key fill to the floor) transitions to the terminal capacity signal within a bounded window instead of looping Busy+WARN forever.
- Kill-the-fix: a mostly-dead floor engine (0161 repro shape) is NOT terminal (stays retryable and recovers via reclaim).
- Memtable-nonempty stalled states are covered (the exact 0161-arm signature).
- Docs (backpressure.md availability section) updated; runbook gotcha updated.

## Acceptance criteria

## Plan

Goal: make `Engine::region_exhausted()` fire for a capacity-stalled floor engine whose memtable is NON-empty (the 0161-arm signature), using the 0161 shadow estimator as the garbage-vs-live discriminator, without ever false-terminating a state reclaim could still recover.

Design (all in crates/vals-store/src/engine.rs unless noted):

1. Persistence signal — new engine field `pressure_infeasible_streak: u32` (init 0 in all constructors):
   - In `compact_plan_inner`, for `PlanKind::Pressure` only: increment (saturating) on every `Err(InsufficientHeadroom)` return (~lines 2565, 2632, 2725) while `at_maintenance_floor()`.
   - Reset to 0 when: any plan kind returns `Ok(Some(_))` (including the aged-GC early return ~2487); the Pressure-path orphan sweep (~2485) frees > 0 segments; `flush_commit`/`compact_commit` succeed; `free_flush_segs`/`free_compact_segs` return reserved segments.
   This guarantees terminal only after ≥N consecutive pressure attempts that each ran the orphan sweep and the aged-GC check against current state and still couldn't plan.

2. New env kill switch `stall_terminal_enabled()` reading `VALS_STALL_TERMINAL` (default on), styled like `pressure_net_gain_enabled()`. New const `STALL_TERMINAL_MIN_PLAN_FAILURES: u32 = 3` near the other consts (~line 665).

3. `region_exhausted()` (~3486): replace the unconditional `!self.index.is_empty()` bail with `if !self.index.is_empty() && !self.stalled_memtable_capacity() { return false; }`. New private helper `stalled_memtable_capacity()` requires ALL of:
   - `stall_terminal_enabled()` and `shadow_est_enabled()` (estimator off ⇒ cannot discriminate ⇒ stay Busy);
   - `pressure_infeasible_streak >= STALL_TERMINAL_MIN_PLAN_FAILURES`;
   - visible dead low: `lsm_dead_fraction() < MAINT_RESERVE_DEAD_MIN_FRACTION`;
   - fresh shadow cache (`computed && gen == manifest_gen && !gated`) with `total` below one segment payload (`seg_bytes - SEG_HEADER_LEN`). Consult the cache ONLY — never trigger sampling I/O from this predicate (it runs on the server write-rejection path); the node maintenance loop refreshes the cache every tick via `lsm_dead_fraction_gate`, so a genuinely stalled engine always has a fresh cache. Stale/uncomputed/gated ⇒ false (conservative).
   The rest of the existing predicate (floor, no in-flight maintenance, aged-GC infeasible, minimal-plan-doesn't-fit / single-run zero-net-gain) runs unchanged for both the empty and non-empty paths. Note `minimal_pressure_needed_segs` already includes flush headroom, so a non-empty memtable makes `needed` honestly larger.
   NOTE the memtable-tombstone case is safe: `entry_shadowed` probes the memtable index, so a memtable full of deletes/overwrites of on-disk keys yields a HIGH shadow estimate ⇒ retryable.

4. No server change needed: `engine_space_rejection` (crates/vals-cluster/src/net/server.rs:1958) already maps the predicate to the terminal `Response::Err("zone region exhausted…")` + `region_exhausted_rejections`.

5. Tests (engine.rs, mirror the existing floor-test style; pin `VALS_ORPHAN_RECLAIM=0` when fabricating fullness via bare `alloc.allocate()` — see memory gotcha; reuse the env-mutex guard pattern like `orphan_reclaim_off_env`/`co_compact_env`):
   a. Acceptance: unique-key (mostly-live) engine, one flushed run, memtable NON-empty, drained to floor; drive `compact_plan_pressure` ≥3 times (each `Err(InsufficientHeadroom)`); refresh estimate via `lsm_dead_fraction_gate()`; assert `region_exhausted()` true with `!index.is_empty()`, and false before the streak threshold.
   b. Kill-the-fix (mostly-dead / shadowed): overwrite-heavy shape so the shadow estimate is ≥ one segment ⇒ stays false (retryable).
   c. Kill-the-fix (visible dead): state with `lsm_dead_fraction ≥ 0.10` ⇒ stays false.
   d. Streak reset: after a successful plan or commit the predicate clears until failures re-accumulate.
   e. `VALS_STALL_TERMINAL=0` restores the old never-terminal-with-nonempty-memtable behavior.
   Existing empty-memtable region_exhausted tests must pass unchanged.

6. Docs:
   - docs/backpressure.md availability section (~lines 232–247): document the stalled-memtable capacity signal (conditions above, bounded window ≈ N maintenance ticks, `VALS_STALL_TERMINAL=0` kill switch, estimator-off ⇒ Busy only).
   - plans/bootes-soak-runbook.md ~lines 366–377: update the gotcha — `region_exhausted` now DOES fire for the stalled non-empty-memtable mostly-live floor (0163), after ~3 failed pressure-plan ticks.

Verification: `cargo build`, `cargo test -p vals-store` (debug, NOT --release per memory), plus targeted `cargo test -p vals-cluster` for the server region_exhausted tests. Hand-format new code only (tree is not rustfmt-clean); no explanatory comments in code per user preference.

### Starting points
- crates/vals-store/src/engine.rs:3486 region_exhausted; 3445 minimal_pressure_needed_segs; 2837 aged_vlog_gc_feasible; 2470 compact_plan_inner (InsufficientHeadroom at 2565/2632/2725; orphan sweep at 2485); 3722 refresh_shadow_estimates; 3801 entry_shadowed (probes memtable); 1032 ShadowCache; 665 MAINT_RESERVE_DEAD_MIN_FRACTION=0.10
- crates/vals-cluster/src/node.rs:1200 MaintSignals::sample calls lsm_dead_fraction_gate each tick (refreshes shadow cache); 1741-1759 plan-failure counter path
- crates/vals-cluster/src/net/server.rs:1958 engine_space_rejection (no change needed)
- existing test patterns: engine.rs:9235 region_exhausted_identifies_a_zero_net_gain_single_victim; 12815 region_exhausted_clears_when_garbage_appears; 5283 orphan_reclaim_off_env
- gotcha: tests fabricating fullness via bare alloc.allocate() must pin VALS_ORPHAN_RECLAIM=0
- docs/backpressure.md lines 229-256; plans/bootes-soak-runbook.md lines 366-377

## Work log
- 2026-08-10 plan: Goal: make `Engine::region_exhausted()` fire for a capacity-stalled floor engine whose memtable is NON-empty (the 0161-arm signature), using the 0161 shadow estimator as the garbage-vs-live discrimina
…[truncated]
- 2026-08-10 context hints: 6 recorded with plan
- 2026-08-10 context hints: engine.rs symbols: region_exhausted (3486), minimal_pressure_needed_segs (3445), aged_vlog_gc_feasible (2837), compact_plan_inner (2470; InsufficientHeadroom returns 2565/2632/2725; orphan sweep call 
…[truncated]
- 2026-08-10 preload: 4 file(s), ~20 KiB seeded into implementer context
- 2026-08-10 review tier: high-powered — reviewers: sol, claude
- 2026-08-10 review (sol): unknown — reviewer error: turn 1: API returned non-200 status code 400: {"detail":"Unsupported parameter: max_output_tokens"}
- 2026-08-10 review (claude): accept — The change implements task 0163 exactly per the recorded plan: a `pressure_infeasible_streak` counter incremented only on Pressure-plan InsufficientHeadroom while at the maintenance floor (all three r
…[truncated]
- 2026-08-10 decision: accept — commit: 0163: capacity-terminal detection for stalled non-empty-memtable floor engines — region_exhausted now covers the 0161-arm signature via a pressure-plan infeasibility streak + shadow-estimate discrim
…[truncated]
- 2026-08-10 usage: 77,929 tok (in 184, out 77,745, cache_r 7,155,839, cache_w 185,895) · cost n/a (unpriced)
  coordinator: 59,250 tok (in 138, out 59,112, cache_r 6,275,572, cache_w 125,448) · cost n/a (unpriced)
  reviewer:claude: 18,679 tok (in 46, out 18,633, cache_r 880,267, cache_w 60,447) · cost n/a (unpriced)

---
id: "0092"
title: 'Smooth reserve-floor backpressure: graceful admission shaping tied to reclaim progress instead of a hard Busy floor'
status: done
priority: 3
created: "2026-07-08"
updated: "2026-08-10"
depends_on: []
spec_refs: []
---

## Description
From task 0087's attribution (bench/t0087/notes.md): at `tier=nosync`, unthrottled writes overshoot region fill to ~1018/1024 segments before the reserve floor bites, then the workload collapses into an all-`Busy` storm (landed ≈ 0, p99.9 131 ms of retry backoff) and oscillates between the core-bound fast regime and the backpressure collapse regime. The hard floor lets fill overshoot, then over-corrects.

## Acceptance criteria
- Design + implement earlier/steeper admission shaping tied to reclaim progress (e.g. probabilistic/graduated Busy as free approaches the floor, or admission rate coupled to the pressure-merge's reclaim rate) so a burst that transiently outruns reclaim degrades gracefully.
- Preserve the 0085/0089 liveness guarantees (maintenance reserve still always plannable; Busy still retryable, never data loss).
- A/B on hardware vs the bench/t0087/notes.md §2 tier=nosync baseline: gyration amplitude reduced, no all-Busy collapse bursts, comparable or better sustained throughput.

## Plan

Goal: replace the hard reserve-floor cliff (intake at full rate until free <= maint_reserve, then all-Busy collapse at ~1018/1024 fill) with graduated admission shaping: as free segments approach the floor, an increasing fraction of wire record intake is refused early with retryable Busy, so admitted rate degrades smoothly toward the reclaim rate instead of overshooting and collapsing.

Design (engine-level probability, per-wire-request draw):

1. crates/vals-store/src/engine.rs:
   - Env knobs following the existing pattern (see fns near line 578): `admit_shape_enabled()` = `VALS_ADMIT_SHAPE` != "0" (default ON), `admit_band_pct()` = `VALS_ADMIT_BAND_PCT` parsed u64 in 1..=100, default const ADMIT_BAND_PCT = 6 (percent of region segments).
   - `pub fn admission_shed_probability(&self) -> f64`: 0.0 unless `maint_reserve_enabled` && shaping enabled && floor > 0. floor = alloc.maint_reserve(), free = alloc.free_segments(), total = alloc.num_segments(). band = max(floor, total*pct/100, 1); top = floor + band. p = 0 for free >= top; 1 for free <= floor; else ((top-free)/band)^2 (quadratic ramp: gentle onset, steep near the floor).
   - `pub fn admission_shed(&mut self) -> bool`: Bernoulli draw against that probability using a tiny inline xorshift64* PRNG state field (`shed_rng: u64`, nonzero seed) added to the Engine struct (all constructor init sites); no new crate deps. p<=0 → false, p>=1 → true without consuming randomness.
2. crates/vals-cluster/src/net/server.rs — exactly ONE draw per wire write request (per-record draws would inflate whole-batch rejection by ~n×, so batches draw once like singles):
   - helper `admission_shed_rejection(store) -> Option<Response>`: if `store.borrow_mut().engine_mut().admission_shed()` → increment new `admit_shed_rejections` counter AND existing `busy_rejections` (a shed IS a busy rejection; keeps the t0087 busy==client-transport-error correlation), return Some(Response::Busy).
   - Call it after zone_store resolution in all four write entry points: dispatch_frame Apply fast-path arm (~line 1572), dispatch_frame staged BatchApply arm (~line 1674), serve_request Apply arm (~line 3455), serve_request BatchApply arm (~line 3525). The non-staged dispatch BatchApply arm delegates to serve_request (covered). serve_apply/serve_batch_apply helpers are only reached after a draw passed — do NOT draw again there.
   - Internal producers that call replica.apply directly (AE heal, rehome copy) stay unshaped by design — they already handle hard-floor Backpressure with bounded retries.
3. crates/vals-client/src/observe.rs: add `admit_shed_rejections` counter with HELP text ("Writes refused early by graduated reserve-floor admission shaping (probabilistic retryable Busy in the pre-floor band); also counted in busy_rejections.").
4. docs/backpressure.md: paragraph in "Zone-region capacity and maintenance reserve" describing the shaping band, quadratic ramp, one-draw-per-request semantics, the two knobs (`VALS_ADMIT_SHAPE=0` kill switch, `VALS_ADMIT_BAND_PCT`), the new counter, and that liveness invariants are untouched (shaping only returns retryable Busy earlier; alloc/planner/floor semantics unchanged; below-floor state still recovers as reclaim lifts free back through the band).
5. Tests (vals-store engine.rs in-module tests; deterministic where possible):
   - Probability curve: armed engine, fabricate free counts (allocate from alloc directly; pin VALS_ORPHAN_RECLAIM=0 if any sweep could run — see memory gotcha), assert p==0 above band top, monotone non-decreasing as free falls, ==1 at/below floor; VALS_ADMIT_SHAPE=0 forces 0 (follow the existing set_var/remove_var test pattern used for VALS_SHADOW_EST).
   - Draw behavior: p==0 never sheds, p==1 always sheds; mid-band, over ~10k draws the shed fraction is within p±0.15 (loose, no flake).
   - Existing reserve-floor tests (engine.rs ~9786, server.rs ~4466) must still pass — they assert eventually-Busy, which shaping only makes earlier.
6. `cargo build` + run vals-store and vals-cluster suites in DEBUG (memory: --release is timing-flaky; fencing tests may flake once under full parallel load — re-run in isolation before treating as broken).

Liveness argument (acceptance criterion 2): shaping never allocates, holds, or blocks anything; the maintenance reserve computation, pressure planning, terminal detection (0163), and hard alloc gate are untouched. A shed is a retryable Busy identical to today's floor rejection, just issued earlier with probability < 1, so no data loss and no new wedge state.

Out of scope (follow-on task): hardware A/B vs bench/t0087/notes.md §2 tier=nosync baseline to validate reduced gyration amplitude and no all-Busy collapse — needs the box rig; ships default-ON with the `VALS_ADMIT_SHAPE=0` kill switch so the A/B can toggle it per arm.

### Starting points
- crates/vals-store/src/engine.rs: at_maintenance_floor() ~3491, refresh_data_reserve() ~3681, env-flag fns ~570-640, put_kind ~1597
- crates/vals-store/src/alloc.rs: free_segments/maint_reserve/num_segments accessors; allocate_data gates on data_reserve
- crates/vals-cluster/src/net/server.rs: engine_space_rejection ~2081, dispatch_frame Apply arm ~1572, staged BatchApply arm ~1674, serve_request Apply/BatchApply arms ~3455/~3525
- crates/vals-cluster/src/replica.rs: engine_mut() ~856
- crates/vals-client/src/observe.rs: counters! macro list ~150 (busy_rejections entry)
- memory: tree is NOT rustfmt-clean — hand-format new regions only; no explanatory comments in code; run suites in DEBUG

## Work log
- 2026-08-10 plan: Goal: replace the hard reserve-floor cliff (intake at full rate until free <= maint_reserve, then all-Busy collapse at ~1018/1024 fill) with graduated admission shaping: as free segments approach the 
…[truncated]
- 2026-08-10 context hints: 6 recorded with plan
- 2026-08-10 context hints: crates/vals-store/src/engine.rs: at_maintenance_floor ~3491, refresh_data_reserve ~3681, env fns ~570-640, Engine struct ~1100-1240 (multiple constructors init sites); crates/vals-store/src/alloc.rs: 
…[truncated]
- 2026-08-10 review tier: high-powered — reviewers: sol, claude
- 2026-08-10 review (sol): unknown — reviewer error: turn 1: API returned non-200 status code 400: {"detail":"Unsupported parameter: max_output_tokens"}
- 2026-08-10 review (claude): accept — The change implements graduated reserve-floor admission shaping exactly as planned: a quadratic shed-probability ramp across a pre-floor band in Engine (VALS_ADMIT_SHAPE kill switch, VALS_ADMIT_BAND_P
…[truncated]
- 2026-08-10 decision: accept — commit: 0092: graduated reserve-floor admission shaping — probabilistic pre-floor Busy replaces the hard-cliff overshoot. Each armed zone engine sheds a rising fraction of wire record intake across a pre-fl
…[truncated]
- 2026-08-10 usage: 63,452 tok (in 160, out 63,292, cache_r 6,585,302, cache_w 152,454) · cost n/a (unpriced)
  coordinator: 51,678 tok (in 132, out 51,546, cache_r 6,160,606, cache_w 105,955) · cost n/a (unpriced)
  reviewer:claude: 11,774 tok (in 28, out 11,746, cache_r 424,696, cache_w 46,499) · cost n/a (unpriced)

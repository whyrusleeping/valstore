---
id: "0159"
title: 'Pressure-floor compaction livelock: zero-net-gain plan loops at ~500 MB/s forever, terminal region-exhausted never fires'
status: done
priority: 1
created: "2026-08-09"
updated: "2026-08-09"
depends_on:
    - "0157"
spec_refs: []
---

## Description
Found by the 0130 post-0157 soak arm (`bench/bootes-soak-lab/run-20260809T063307Z-post0157/FINDINGS.md`, valsd HEAD ccb8052): when an overwrite-heavy small-value sorted zone (bootes backlinks) fills its 32 GiB region to the 0157 reserve floor, the engine loops the SAME degraded pressure plan forever — single-run merge `6,443,649 entries, 2,050,489,321 B in → 2,051,014,656 B out, 1956 segs freed` every ~4 s: output ≥ input, frees exactly what it re-consumes, **zero net gain**, ~500 MB/s sustained device rewrite of identical data (~580 GiB churned in the ~22-min wedge). The peer replica loops a 2-run variant at ~1/min. `region_exhausted()` never fires because a "viable" plan exists, so clients get permanent retryable Busy from both replicas (never the 0157 terminal "zone region exhausted" Err), `region_exhausted_rejections` stays 0, `compaction_plan_failures` stays 0 (plans "succeed"), and the producer wedges — same availability outcome as the pre-0157 wall, now with the device burning instead of idling.

Space anomaly feeding task 0158: the wedged engine held ONE live run of ~2.05 GiB / 6.44M entries in a 32 GiB region with ~2 free segments — ~30 GiB unattributed, presumably dead vlog versions from backlink key overwrites that the index-only floor rewrite never reclaims.

Acceptance criteria:
- Local file-device repro (vals-store test) of the zero-net-gain pressure-plan loop at the reserve floor with an overwrite-heavy small-value workload; root cause established for why floor-pressure planning does not reclaim dead vlog space (co-compaction not engaged in the degraded one-run path? dead-space accounting blind to the pattern?) and, as far as the repro allows, where the ~30 GiB went.
- Net-gain-aware planning: a pressure plan with projected zero/negative net free-segment gain is never re-selected in a tight loop; plans with genuine expected reclaim (vlog victims carrying dead bytes) are preferred at the floor so intake can recover when garbage exists.
- Genuine terminal classification: at/below the floor with no positive-net-gain plan, `region_exhausted()` returns true (terminal Err + `region_exhausted_rejections`) and the sweep loop backs off via the throttled-WARN + `compaction_plan_failures` machinery — no 4 s re-plan spin, no unbounded identical rewrites. The state clears when garbage later appears (deletes/overwrites/AE).
- 0157 guarantees preserved: reserve floor never collapses, no DeviceFull retry loops, transient floor states stay retryable Busy; existing region_exhausted_*/floor tests keep passing.
- Kill-the-fix tests: bounded consecutive zero-gain merges; mostly-dead-vlog-at-floor reclaims net segments and intake resumes; all-live-at-floor is terminal; garbage-appears-later clears the terminal state.
- `cargo test -p vals-store -p vals-cluster -p vals-server` green.

Follow-on (stays with 0130): hardware soak re-validation past the wall once this lands.

## Acceptance criteria

## Plan

## Context (coordinator's code reading)

The pressure planner (`Engine::compact_plan_inner`, PlanKind::Pressure, engine.rs ~2381-2619) at the maintenance floor admits a single-run plan only when `select_victims` returns a non-empty victim set (mode `VlogRewrite::Victims`); a single-run plan with mode `None` is rejected (InsufficientHeadroom, line ~2590), and empty-victims + runs==1 returns Ok(None). The soak wedge showed a *successful* single-run pressure merge repeating every ~4 s with identical stats (2.05 GB index in ≈ out, 1956 segs freed and instantly re-consumed, zero net gain), so a non-empty victim set must regenerate every cycle.

**Leading hypothesis (verify by repro, do not assume):** self-regenerating tail victim. A victim rewrite packs `victim_live` bytes into fresh vlog segments; whenever the live bytes end in an underfilled tail segment (< (1-VICTIM_DEAD_FRACTION)=50% full), that fresh segment immediately re-qualifies as a victim (`select_victims` dead_frac ≥ 0.5, engine.rs ~2621-2652). So every cycle frees N victims, writes ceil(live/usable) segs whose tail is a new victim → plan is always "viable", net gain ≈ 0, and the 2 GB index rewrite burns the device. `region_exhausted()` (engine.rs ~3064) never fires because its single-run arm requires `victims.is_empty()` (and `compact_in_flight` is ~always true mid-loop). The peer's 2-run/1-min variant is likely the 2-run index-only shape (vlog rewrite dropped to `None` when vlog_n > available — allowed for ≥2 inputs) with no duplicate keys → zero gain. The ~30 GiB unattributed space is likely vlog segments fully dereferenced by earlier index-only merges (co_compacted=false ⇒ no dead-sweep at commit; sweep only covers merged_inputs' value_segs), i.e. leaked orphans invisible to both victim selection and the sweep — confirm as far as the repro allows and note findings (deep attribution stays with task 0158).

## Phase 1 — file-device repro + root cause
Add a vals-store engine test (pattern: existing floor harnesses around engine.rs ~8290-8460): small file-device region, small values routed through the vlog, overwrite-heavy workload driven to the reserve floor until one (or few) runs remain; then loop plan→compact_and_write→compact_commit as the sweep would. Assert the defect first (kill-the-fix): consecutive pressure cycles each re-select a plan and realize net free-segment gain ≤ 0, with no terminal signal. Use the repro to pin the actual regeneration mechanism (tail-victim or otherwise) before writing the fix, and measure where the dead vlog space sits (orphaned segs vs referenced-but-dead).

## Phase 2 — fix (engine.rs, minimal surface)
1. **Projected net gain gate for floor-pressure plans.** At plan time compute projected freed (input runs' index segs + victim segs + input value_segs that become fully dereferenced by the rewrite) vs projected consumed (output index seg bound + ceil(victim_live/usable) actual vlog output — not the 2× reservation). If projected net gain < 1 segment for every admissible shape, return `InsufficientHeadroom` instead of a plan, so the existing throttled-WARN + `compaction_plan_failures`/`compaction_headroom_skips` machinery (node.rs ~1737-1756) absorbs it — no rewrite issued, no 4 s device churn.
2. **Prefer genuine reclaim.** Extend 0157's shape degradation (one-run victim rewrite ↔ two-run merge across the input cap): when one shape projects non-positive gain but another projects positive, select the positive one.
3. **Bounded futility for unprojectable shapes.** Multi-run merges reclaim via duplicate drops that can't be projected statically. Track realized net gain of floor-pressure merges at `compact_commit` (freed vs consumed); after a small bound of consecutive ≤0-gain floor-pressure merges (e.g. 2), refuse further floor-pressure planning (InsufficientHeadroom) until the run set / memtable changes from a non-pressure source (flush, deletes, AE). This bounds consecutive zero-gain merges by construction.
4. **Genuine terminal classification.** Rework `region_exhausted()`'s single-run `victims.is_empty()` arm to align with the planner: at floor + empty memtable + nothing in flight + no positive-projected-gain pressure plan (and/or futility bound tripped) ⇒ true → terminal "zone region exhausted" Err + `region_exhausted_rejections`. Keep `minimal_pressure_needed_segs` and the planner exactly aligned (0157 invariant). The state must clear when garbage later appears (overwrites/deletes/AE make projection positive again or reset the futility counter).
5. **Optionally** harden `select_victims` against the tail-fragment pathology directly (whatever the repro shows), with the plan-level gate as the principled backstop. Consider a `VALS_PRESSURE_NET_GAIN=0` kill switch reverting to pre-fix admission (house pattern), and a counter for futile-plan rejections if it aids observability.

## Phase 3 — kill-the-fix tests + suites
- Repro test now asserts the loop terminates: bounded consecutive zero-gain merges, then terminal region_exhausted (no spin).
- Mostly-dead-vlog-at-floor: victims carrying real dead bytes reclaim net segments; intake resumes (floor Busy clears).
- All-live-at-floor: terminal Err + region_exhausted_rejections (extend existing vals-cluster terminal test).
- Garbage-appears-later: from terminal state, deletes/overwrites clear it and reclaim proceeds.
- 0157 regression: existing region_exhausted_*/floor/reserve tests untouched and green.
- `cargo test -p vals-store -p vals-cluster -p vals-server` green.

## Constraints
- Reserve floor guarantees from 0157 must hold: floor never collapses, no DeviceFull retry loops, transient floor states stay retryable Busy.
- House style: no explanatory comments (SAFETY notes/doctests/Prometheus HELP only); hand-format new code, never `cargo fmt`.

### Starting points
- crates/vals-store/src/engine.rs ~2381-2619: compact_plan_inner (pressure shapes, victim fallback, single-run mode!=None check)
- crates/vals-store/src/engine.rs ~2621-2675: select_victims / smallest_victim (dead_frac vs VICTIM_DEAD_FRACTION=0.5)
- crates/vals-store/src/engine.rs ~2841-2916: compact_commit seg freeing + co_compacted dead-sweep (only merged_inputs' value_segs)
- crates/vals-store/src/engine.rs ~3010-3090: one_run_victim_needed_segs / minimal_pressure_needed_segs / region_exhausted
- crates/vals-store/src/engine.rs ~3156-3187: maint_reserve_segs (0157 floor)
- crates/vals-cluster/src/node.rs ~1680-1760: sweep plan dispatch, plan-failure counters/throttled WARN; ~1565-1623 commit path (realized segments_freed)
- crates/vals-cluster/src/node.rs ~1221-1243: zone_maint_due pressure signal
- crates/vals-store/src/engine.rs ~8290-8460: existing floor/terminal test harness patterns
- bench/bootes-soak-lab/run-20260809T063307Z-post0157/FINDINGS.md: observed wedge signature
- vlog_n doubling at engine.rs ~2547 is a reservation; unused segs are returned at commit — project consumption from ceil(live/usable), not 2x

## Work log
- 2026-08-09 plan: ## Context (coordinator's code reading)  The pressure planner (`Engine::compact_plan_inner`, PlanKind::Pressure, engine.rs ~2381-2619) at the maintenance floor admits a single-run plan only when `sele
…[truncated]
- 2026-08-09 context hints: 10 recorded with plan
- 2026-08-09 context hints: crates/vals-store/src/engine.rs ~2381-2619 compact_plan_inner: pressure shapes, pressure_floor_min_two, victim fallback, single-run mode!=None reject at ~2590; crates/vals-store/src/engine.rs ~2621-26
…[truncated]
- 2026-08-09 preload: 3 file(s), ~29 KiB seeded into implementer context
- 2026-08-09 context hints: crates/vals-store/src/engine.rs ~2381-2619 compact_plan_inner: pressure shapes, pressure_floor_min_two, victim fallback, single-run mode!=None reject at ~2590; crates/vals-store/src/engine.rs ~2621-26
…[truncated]
- 2026-08-09 preload: 3 file(s), ~29 KiB seeded into implementer context
- 2026-08-09 review tier: high-powered — reviewers: sol, claude
- 2026-08-09 review (sol): unknown — reviewer error: turn 1: API returned non-200 status code 400: {"detail":"Unsupported parameter: max_output_tokens"}
- 2026-08-09 review (claude): accept — The change correctly implements net-gain-aware floor-pressure planning and genuine terminal classification for the 0159 livelock. The gainful-prefix projection (k victims freed > ceil(sum_live/usable)
…[truncated]
- 2026-08-09 review (sol): unknown — reviewer error: turn 1: API returned non-200 status code 400: {"detail":"Unsupported parameter: max_output_tokens"}
- 2026-08-09 review (claude): accept — The revision cleanly resolves the one substantive issue from the prior round: affordable_victim_prefix now uses a zero reservation for zero-live gainful prefixes, exactly matching one_run_victim_neede
…[truncated]
- 2026-08-10 root cause (local MemBlockDevice repro, rotating-slice overwrite workload): the livelock is a self-regenerating zero-net victim rewrite. A single-run floor-pressure plan is admitted whenever select_victims finds ANY segment ≥50% dead; rewriting a victim set whose live bytes don't save at least one whole segment (V ≤ ceil(ΣL/usable), e.g. the lone sparse tail victim) frees exactly what it re-consumes, and the rewrite output regenerates the same victim — identical plan forever, full index rewritten each cycle (soak: 2.05 GB every ~4 s), region_exhausted() false because victims are non-empty and compact_in_flight is ~always true. Repro also attributed the "missing" ~30 GiB: ~80% of the region (1663/2048 segs) was ORPHANED vlog — segments fully dereferenced by index-only merges are freed by nothing (dead-sweep only runs on co-compacting merges and only over run-referenced segs) and are invisible to victim selection → filed task 0160.
- 2026-08-10 fix: net-gain-aware single-run floor-pressure planning. Victim lives sorted ascending; net gain of a prefix = k − ceil(S_k/usable) (monotonic, so the full affordable prefix is optimal and the smallest gainful prefix has minimal reservation). Planner admits the largest affordable gainful prefix; no gainful prefix ⇒ InsufficientHeadroom at the floor (throttled WARN + compaction_plan_failures) / quiet Ok(None) off-floor. one_run_victim_needed_segs and region_exhausted() use the same projection (exact 0157 alignment; unknown metadata stays retryable). VALS_PRESSURE_NET_GAIN=0 kill switch restores old admission at all three sites. Legacy tests updated: the "low-aggregate hot victim retryable" state is precisely the livelock and is now terminal.
- 2026-08-10 futility-counter omission rationale (plan phase-2 item 3): realized-gain tracking for multi-run shapes is unnecessary — every multi-run merge consumes a run and floor intake is backpressured, so an identical zero-gain multi-run plan cannot regenerate without a new flush (which is new input, not a loop); the churn test bounds zero-gain commits ≤ 3 end-to-end and would catch a regression.
- 2026-08-10 verification: cargo test -p vals-store -p vals-cluster -p vals-server green (257+449+e2e; vals-cluster release-mode cleanup_quiesces_inflight_reads stall-bound failure is pre-existing on this box — fails on base tree too; debug green). Kill-the-fix: rotating-overwrite churn stays live 800 rounds with zero_gain_commits=0 (pre-fix wedged at round ~279); lone-victim state refuses re-selection 3× and classifies terminal; gainful victims at floor net-reclaim and lift the engine off the floor; terminal clears when scattered garbage appears.
- 2026-08-10 note: implementer backend down both days (400 max_output_tokens) — coordinator implemented directly per plan after pinning root cause by repro; high-powered review = claude accept ×2 (sol unreachable, same backend 400).
- 2026-08-09 decision: accept — commit: 0159: zero-net-gain pressure livelock at the reserve floor fixed — floor-pressure planning is now net-gain aware. Root cause pinned by local repro: a single-run pressure plan was admitted whenever A
…[truncated]
- 2026-08-09 usage: 142,942 tok (in 212, out 142,730, cache_r 15,444,373, cache_w 463,868) · cost n/a (unpriced)
  coordinator: 116,255 tok (in 168, out 116,087, cache_r 14,362,610, cache_w 378,959) · cost n/a (unpriced)
  reviewer:claude: 26,687 tok (in 44, out 26,643, cache_r 1,081,763, cache_w 84,909) · cost n/a (unpriced)

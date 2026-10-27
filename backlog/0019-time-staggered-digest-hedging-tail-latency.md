---
id: "0019"
title: Time-staggered digest hedging (tail latency)
status: done
priority: 4
created: "2026-07-02"
updated: "2026-07-05"
depends_on: []
spec_refs: []
---

## Description
BACKLOG.md §Hardware/benchmarking: the digest read escalates from its reserve only when a probe *fails*; a slow-but-not-failed replica stretches tail latency. A real hedge timer (issue the reserve digest after a short p99 delay) needs a cheap per-read timing source — per-read timerfd is too costly; a shared timer wheel or coarse logical tick is the open design question. The executor mechanism exists (a single shared hedge-timer task on the `CoordinatorService` executor, ticking via `run_until`).

## Acceptance criteria
- Hedged reserve fires after a configurable delay without a per-read timerfd; measured tail-latency improvement under an induced slow replica; no throughput regression at peak (box A/B).

## Acceptance criteria

## Plan

Goal: give the digest strong read (`ReadFanout::Digest`) a real time-staggered hedge — after a configurable delay, activate one held-back reserve digest probe even when nothing has *failed* — without a per-read timerfd. Off-hardware implementation + deterministic tests here; the box A/B (measured tail improvement / no peak regression) becomes a follow-on box-validation task, matching the backlog's established pattern.

Design (resolves the open "cheap per-read timing source" question with a coarse shared logical tick):

1. vals-rt: new primitive `quorum_collect_reserve_hedged(futures, k, active, hedge)` alongside `quorum_collect_reserve` (which keeps its exact semantics). `hedge: impl FnMut() -> HF, HF: Future<Output = ()>` is a re-armable trigger source: while the collect is pending, quorum not reached, and un-activated reserve futures remain, poll the armed hedge future; when it completes, activate ONE reserve future and re-arm by calling the factory again (that's the time-*staggering* — each further escalation waits another delay). Failure-driven activation stays exactly as today (immediate back-fill). Implement by generalizing `ReserveCollect` internally; do not change the behavior of the un-hedged path. Unit tests in vals-rt (hedge fires → reserve starts; quorum reached first → hedge never activates anything; failures still back-fill immediately; stagger of two hedge firings; no-reserve input → hedge not polled/no-op; empty input).

2. vals-client — shared hedge clock (no per-read timerfd): a small `HedgeClock` (coarse logical tick): `Cell<u64>` tick counter + `RefCell<Vec<Waker>>` waiters; `now()`, `advance()` (bump tick, drain+wake waiters), and `wait_until(deadline_tick)`/`wait_ticks(n)` future (registers waker, Ready when tick >= deadline). Pure logical — tests drive it by calling `advance()` directly, so hedging tests are fully deterministic. Also a reusable ticker future (one shared timerfd for the whole coordinator): `hedge_tick_loop(clock, period, stop)` in the CoordinatorService module, same shape as `proactive_refresh_loop` (Timer::every + race with stop; on timerfd-creation failure log and return — hedge degrades to today's failure-only escalation).

3. Coordinator: builder `with_digest_hedge(delay: Duration)` storing `Option<DigestHedge { clock: Rc<HedgeClock>, delay_ticks, tick_period }>`. Choose tick_period ≈ clamp(delay/4, min ~100µs, delay) and delay_ticks = ceil(delay/period)+1 so the hedge fires in (delay, delay+period] — never early for a read that starts mid-tick. `#[doc(hidden)]` accessor for the hedge config/clock (the white-box tests live in vals-cluster and only see the pub API — same pattern as the existing `#[doc(hidden)] pub transport`).

4. `read_winner_digest`: when the hedge is configured (and reps.len() > needed so a reserve exists), collect via the hedged primitive with hedge factory = wait-on-clock for delay_ticks, wrapping the wait so a firing increments a new observe counter (e.g. `digest_hedges_fired`). Unconfigured (default) path is byte-for-byte today's `quorum_collect_reserve` — no behavior change by default.

5. CoordinatorService: spawn the ticker automatically in `new()` when the coordinator has a digest hedge configured (single shared timer task on the service executor, advanced as a passenger of client ops via run_until — exactly the mechanism BACKLOG.md says is in place). Document that ops driven via bare `block_on` (no service / no ticker) leave the clock static, so the hedge is simply inert there.

6. Observability: add counter(s) in vals-client observe.rs (`digest_hedges_fired`; keep it minimal).

7. Tests (deterministic, no real-time dependence except the ticker test):
   - vals-rt unit tests per (1).
   - HedgeClock unit tests (advance wakes waiter; deadline math).
   - vals-cluster coord/tests.rs white-box test: RF=3, R=2, custom Transport counting digests per node where the chosen digest replica's future never resolves (self-waking Pending) and the transport advances the shared HedgeClock after a few polls: with `with_digest_hedge`, the read completes with the correct value, the reserve node received a digest, and the hedge counter rose — i.e. a slow-but-not-failed replica no longer stretches the read. Control: same topology, all replicas fast → reserve node never probed and no hedge fired (a transient/fast read never escalates).
   - Service test (like the existing proactive-refresh test at coord/tests.rs:2259): `with_digest_hedge` + CoordinatorService → the ticker advances the clock while ops run.

8. Docs/bookkeeping: update the `ReadFanout::Digest` doc comment ("pulled in only if one of the R fails" → also after the hedge delay), and flip the BACKLOG.md §Hardware/benchmarking hedging item to done with a short summary of the mechanism, noting box A/B is a follow-on task.

9. Coordinator follow-up (mine): create a follow-on backlog task "Box A/B: digest hedging tail-latency validation" (depends_on 0019) covering bench wiring (hedge knob + ticker on the bench executor) and the on-hardware induced-slow-replica measurement + peak-throughput regression check.

Verification: cargo fmt --check on touched files stays clean, cargo clippy --workspace --all-targets zero new warnings, cargo test --workspace green.

### Starting points
- crates/vals-rt/src/lib.rs:416-499 — quorum_collect_reserve / ReserveCollect (the primitive to generalize)
- crates/vals-client/src/coord/mod.rs:2437-2554 — read_winner_digest (call site)
- crates/vals-client/src/coord/mod.rs:909-939 — ReadFanout docs to update; ~1145 with_read_fanout builder pattern to mirror
- crates/vals-client/src/coord/service.rs — proactive_refresh_loop + CoordinatorService (ticker task host; reuse the Timer::every + race(stop) pattern)
- crates/vals-client/src/reactor.rs — Timer / Notify
- crates/vals-client/src/observe.rs:171-330 — counters macro/declaration site
- crates/vals-cluster/src/coord/tests.rs — cluster() helper, SlowCluster/CountingCluster transports, proactive-refresh service test at ~2259
- BACKLOG.md ~line 868 — the hedging backlog entry to flip
- note: coord white-box tests live in vals-cluster and see only vals-client's pub API — expose the hedge clock via a #[doc(hidden)] pub accessor like `pub transport`

## Work log
- 2026-07-04 plan: Goal: give the digest strong read (`ReadFanout::Digest`) a real time-staggered hedge — after a configurable delay, activate one held-back reserve digest probe even when nothing has *failed* — with
…[truncated]
- 2026-07-04 context hints: 9 recorded with plan
- 2026-07-04 context hints: crates/vals-rt/src/lib.rs:416-499 — quorum_collect_reserve / ReserveCollect (generalize this); crates/vals-client/src/coord/mod.rs:2437-2554 — read_winner_digest (call site); 909-939 ReadFanout do
…[truncated]
- 2026-07-05 implementer report: Implemented Task 0019 — time-staggered digest hedging for `ReadFanout::Digest`, off-hardware with deterministic tests. Box A/B measurement is filed as a follow-on backlog task.  ## What changed  **v
…[truncated]
- 2026-07-05 review tier: single-opus — reviewers: Claude
- 2026-07-05 review (Claude): accept — The change correctly and completely implements time-staggered digest hedging for ReadFanout::Digest: a re-armable hedge in the vals-rt reserve-collect primitive, a cheap shared logical HedgeClock (no 
…[truncated]
- 2026-07-05 decision: accept — commit: feat(client): time-staggered digest hedging for ReadFanout::Digest (task 0019)  - vals-rt: quorum_collect_reserve_hedged — re-armable hedge trigger activates one   reserve future per firing (stagger
…[truncated]
- 2026-07-05 usage: 84,559 tok (in 294, out 84,265, cache_r 9,482,533, cache_w 293,154) · cost n/a (unpriced)
  implementer: 56,137 tok (in 178, out 55,959, cache_r 7,082,536, cache_w 115,979) · cost n/a (unpriced)
  coordinator: 21,123 tok (in 68, out 21,055, cache_r 1,750,203, cache_w 127,768) · cost n/a (unpriced)
  reviewer:Claude: 7,299 tok (in 48, out 7,251, cache_r 649,794, cache_w 49,407) · cost n/a (unpriced)

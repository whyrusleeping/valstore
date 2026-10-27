---
id: "0057"
title: HedgeClock waiter re-registration grows unboundedly under busy-poll callers (dedupe/slot wakers)
status: done
priority: 4
created: "2026-07-05"
updated: "2026-07-05"
depends_on: []
spec_refs: []
---

## Description
Surfaced during task 0054 bench wiring: `WaitTicks::poll` (crates/vals-client/src/coord/hedge.rs) pushes a fresh waker into `HedgeClock.waiters` on EVERY pending poll. A caller that busy-polls its future (e.g. a self-waking delay future on the same executor, or any spin-polling composition) grows the waiter vec without bound between ticks — O(n²) drain work per `advance()` and unbounded allocation. Production callers today park on real socket wakes, so at most one registration per tick occurs and the behavior is benign; the bench hit it only with a (since-replaced) busy-spin delay. Still a latent footgun for any future composition that re-polls while pending.

Fix direction: make waiter registration idempotent per waiter — e.g. `WaitTicks` remembers a slot index into the waiter list and calls `Waker::clone_from`/`will_wake` to update in place rather than pushing anew, or keep a slab keyed by waiter with removal on drop.

## Acceptance criteria
- Re-polling a pending `WaitTicks` many times between two `advance()` calls registers O(1) waiters (unit test polls a pending WaitTicks N times and asserts the waiter count stays bounded / the hedge pass shows no quadratic blowup).
- Existing hedge semantics unchanged: wake on advance, deadline math, multiple distinct waiters all woken. Existing vals-rt/vals-client/vals-cluster hedge tests stay green.

## Acceptance criteria

## Plan

Make WaitTicks waker registration idempotent per future using a generation+slot scheme:

1. In `crates/vals-client/src/coord/hedge.rs`:
   - Add a `generation: Cell<u64>` to `HedgeClock`, incremented in `advance()` when the waiter vec is drained (each drain invalidates all outstanding slot indices).
   - Give `WaitTicks` a `slot: Option<(u64 /*generation*/, usize /*index*/)>` field (plain field is fine; `WaitTicks` is Unpin so `poll` can use `self.get_mut()` / direct field access via Pin since all fields are Unpin).
   - In `WaitTicks::poll`, when pending: if `slot` records the *current* generation, update the existing entry in place — `waiters[idx].clone_from(cx.waker())` (guarded by `will_wake` to skip a clone when unchanged); otherwise push the waker and record `(generation, len-1)` in `slot`.
   - Update `wait_until`/`wait_ticks` constructors to initialize `slot: None`.
2. Add a test-support accessor (e.g. `#[cfg(test)] fn waiter_count(&self) -> usize`) or use direct field access from the same-module tests.
3. New unit test: create a `WaitTicks` with a future deadline, poll it pending N (e.g. 1000) times with a noop waker between two `advance()` calls, assert the clock's waiter count stays at 1 the whole time; then `advance()` past the deadline and assert the future resolves. Also assert re-registration works across an advance that doesn't reach the deadline (waiter count returns to 1 after re-poll, not stale-slot corruption).
4. Verify existing semantics untouched: run `cargo test -p vals-client -p vals-rt -p vals-cluster` (hedge-related tests) plus `cargo build` of the workspace.

Notes: single-threaded client (Rc/RefCell), so no atomics needed. Do not change `advance()`'s drain-and-wake-all semantics — multiple distinct waiters must all still be woken. The vals-rt miniature clock in its tests is a separate copy and out of scope unless trivially affected.

### Starting points
- crates/vals-client/src/coord/hedge.rs — the whole file (~165 lines), fix + tests live here
- WaitTicks::poll currently does `self.clock.waiters.borrow_mut().push(cx.waker().clone())` on every pending poll
- HedgeClock::advance drains waiters: `for w in self.waiters.borrow_mut().drain(..)` — a drain invalidates slot indices, hence the generation counter
- Waker::will_wake / Waker::clone_from for cheap in-place update
- existing tests in hedge.rs use vals_rt::block_on(fut, on_pending_closure) — the closure runs each time the executor would park

## Work log
- 2026-07-05 plan: Make WaitTicks waker registration idempotent per future using a generation+slot scheme:  1. In `crates/vals-client/src/coord/hedge.rs`:    - Add a `generation: Cell<u64>` to `HedgeClock`, incremented 
…[truncated]
- 2026-07-05 context hints: 5 recorded with plan
- 2026-07-05 context hints: crates/vals-client/src/coord/hedge.rs — whole fix + tests live here (~165 lines); WaitTicks::poll currently: `self.clock.waiters.borrow_mut().push(cx.waker().clone())` every pending poll; HedgeClock
…[truncated]
- 2026-07-05 implementer report: Implemented idempotent per-future waker registration in `crates/vals-client/src/coord/hedge.rs` to fix the unbounded waiter-vec growth under busy-poll callers (task 0057).  Changes: - `HedgeClock` gai
…[truncated]
- 2026-07-05 review tier: single-opus — reviewers: Claude
- 2026-07-05 review (Claude): accept — The change makes WaitTicks waker registration idempotent via a generation+slot scheme exactly as planned. HedgeClock gains a generation counter bumped on each non-empty drain in advance(); WaitTicks r
…[truncated]
- 2026-07-05 decision: accept — commit: client: idempotent HedgeClock waiter registration — generation+slot dedupe in WaitTicks::poll (task 0057)  Re-polling a pending WaitTicks now updates its existing waker slot in place (will_wake/clon
…[truncated]
- 2026-07-05 usage: 10,308 tok (in 46, out 10,262, cache_r 287,245, cache_w 33,583) · cost n/a (unpriced)
  implementer: 5,539 tok (in 26, out 5,513, cache_r 133,965, cache_w 14,759) · cost n/a (unpriced)
  coordinator: 2,869 tok (in 12, out 2,857, cache_r 128,371, cache_w 10,389) · cost n/a (unpriced)
  reviewer:Claude: 1,900 tok (in 8, out 1,892, cache_r 24,909, cache_w 8,435) · cost n/a (unpriced)

---
id: "0082"
title: 'Digest-hedge fixed per-read client overhead in the latency-bound regime (distmdmc ncoord=4: −10% strong-read ops/s, +115µs p50, hedges≈0)'
status: done
priority: 4
created: "2026-07-06"
updated: "2026-07-07"
depends_on: []
spec_refs: []
---

## Description
Filed from task 0054's box run (oldbox, 2026-07-07, 4-node × 1-drive × 4-partition SPDK cluster, RF=3 R=2, digest fanout, conc 64/coordinator).

**Finding:** with the digest hedge merely *enabled* (never firing — `digest_hedges_fired`≈0 on the healthy cluster), the sub-saturated / latency-bound regime (`distmdmc 2000000 64 <4 pci> 4 4`) reproducibly loses ~10% strong-read throughput: 320.2/322.0/320.5 kops/s hedge-off vs 288.1/288.4 (delay 1 ms), 288.8/287.3 (3 ms), 285.7/282.2 (100 ms). The deficit is **independent of hedge delay** → a fixed per-read cost of the hedge machinery (arming the reserve + HedgeClock waiter registration), not tick-frequency and not hedge firings. p50 rises 737µs → 838-855µs.

At **saturation** the deficit disappears: `distmdmc ... 8 4` gives 633.4/649.8 kops/s off vs 647.6/646.8 on — no peak-capacity regression (that's why 0054's acceptance passed); the overhead only shows when the client executor is the bottleneck. Get-any and write phases unaffected in all regimes.

**Secondary observation:** the HedgeClock tick period shapes the healthy tail — `advance()` wakes *every* parked waiter each tick, so all in-flight hedged reads are re-polled per tick (thundering herd). At tick=250µs (delay 1ms) healthy p99=1.70ms; at tick=25ms (delay 100ms) healthy p99 balloons to 3.3ms.

**Suspected mechanism (from code read, unverified):** `WaitTicks` (crates/vals-client/src/coord/hedge.rs) never deregisters its waker slot on drop — every completed strong read leaves a stale waker that the next `advance()` drains and wakes (≈1 spurious executor wake per read, constant per-read cost regardless of tick period), on top of per-poll armed-hedge maintenance in `quorum_collect_reserve_hedged` (vals-rt).

## Acceptance criteria
- Root-cause the fixed per-read overhead (e.g. profile distmdmc ncoord=4 hedge on/off, or targeted counters for stale-waker wakes / re-poll counts).
- Either land a mitigation (candidates: deregister/tombstone the waker slot on `WaitTicks` drop; deadline-bucketed waiters so `advance()` wakes only due waiters) and show the ncoord=4 gap closed (within run noise) on the box with the tail win of 0054 preserved, OR record a reasoned accept of the overhead in the latency-bound regime (no peak-capacity cost).
- No change to default (hedge-off) path behavior.

Repro: box oldbox, `/tmp/vbench.0054`, logs `/tmp/hedge.peak.*.log`, `/tmp/hedge.sat.*.log`; full tables in bench/hedge-box-notes.md (task 0054).

## Plan

Root cause (confirmed by code read of crates/vals-client/src/coord/hedge.rs): every hedged strong read that completes before its hedge deadline drops its armed `WaitTicks` WITHOUT deregistering the waker it parked in `HedgeClock::waiters` (a flat `Vec<Waker>`). The next `advance()` drains the whole vec and wakes everything — so each completed read costs ~1 spurious executor wake/re-poll (constant per read, independent of tick period → matches the measured delay-independent −10% ops/s at ncoord=4). Secondary: `advance()` wakes *every* parked waiter each tick, not just due ones (thundering herd) — matches the tick-period-shaped healthy p99 (1.70ms @250µs tick vs 3.3ms @25ms tick).

Mitigation (lands both fixes in HedgeClock; no vals-rt collect changes, no default hedge-off path changes):
1. Replace `waiters: RefCell<Vec<Waker>>` with deadline-bucketed storage: `RefCell<BTreeMap<u64, Vec<Option<Waker>>>>` keyed by absolute deadline tick. `advance()` bumps the tick then removes-and-wakes ONLY buckets with key <= new tick, skipping `None` entries. Waiters at a future deadline are left parked → no thundering herd; healthy tail no longer shaped by tick period.
2. `WaitTicks` gets `impl Drop`: if it registered a slot and its bucket still exists, tombstone the entry (`bucket[idx] = None`, dropping the Waker). → completed reads leave no stale waker, eliminating the ~1 spurious wake per read.
3. Slot bookkeeping simplifies: a bucket is only drained once tick >= deadline, at which point the future polls Ready and never touches its slot; so the slot index stays valid while pending and the `generation` counter can be removed. In-place `will_wake` waker update on re-poll is kept (task 0057 O(1) invariant).

Tests (crates/vals-client/src/coord/hedge.rs):
- Keep/adapt existing: now/advance, wait_ticks resolves, wait 0 immediate, multiple waiters at same deadline, O(1) re-poll registration, distinct waiters get distinct slots.
- Rewrite the "advance drains waiters" expectation: an advance BELOW the deadline must now leave the waiter parked and NOT wake it (use a counting waker); an advance reaching the deadline wakes it exactly once.
- New: dropping a pending WaitTicks deregisters — after drop, advancing past its deadline wakes nothing (counting waker) and waiter_count is 0.
- New: mixed deadlines — advance wakes only due waiters; the later one stays parked and fires at its own deadline.
- Confirm crates/vals-rt hedged-collect tests and crates/vals-cluster coord tests still pass unchanged (they use their own mock clocks / the public API only).

Local verification: cargo test -p vals-client -p vals-rt -p vals-cluster; cargo build --release -p vals-bench (mem features) as compile check.

Box validation (I, the coordinator, run this after review passes, before commit): on oldbox (~/vals checkout), sync the change, build `VALS_SPDK_LINK=1 cargo build --release -p vals-bench --features spdk`, then interleaved A/B per bench/hedge-box-notes.md:
- Latency-bound regime (the 0082 gap): `distmdmc 2000000 64 <4 pci> 4 4` hedge off vs VALS_HEDGE_US=1000 vs 100000, ≥2 runs/side interleaved. Accept if the on/off gap closes to within run noise (±~1%) and p50 recovers toward ~737µs; also check p99 no longer balloons at the 25ms tick.
- Tail win preserved (0054): `VALS_SLOW_NODE=2:5000:digest` ncoord=1 500k, hedge off vs on 1ms — hedge must still cut p99/p99.9 materially and hedges_fired ≈ 27% of reads.
- Record results in bench/hedge-box-notes.md (new section) and the task work log. Leave the box clean.

Fallback: if the gap does NOT close within noise after the fix, profile further (the remaining candidate is the per-read Box::pin hedge-arming alloc in vals-rt's ReserveCollect) or record a reasoned partial-accept per the acceptance criteria.

### Starting points
- crates/vals-client/src/coord/hedge.rs — HedgeClock (waiters Vec<Waker>, generation) + WaitTicks (slot, no Drop impl); the whole fix lives here
- crates/vals-rt/src/lib.rs:496-574 — ReserveCollect / quorum_collect_reserve_hedged (arming site; do NOT change)
- crates/vals-client/src/coord/service.rs hedge_tick_loop — the ticker calling advance(); unchanged
- task 0057 invariant: re-polling a pending WaitTicks must not grow the waiter storage (in-place waker update)
- bench/hedge-box-notes.md — measured regime tables + exact box run recipe

## Work log
- 2026-07-06 plan: Root cause (confirmed by code read of crates/vals-client/src/coord/hedge.rs): every hedged strong read that completes before its hedge deadline drops its armed `WaitTicks` WITHOUT deregistering the wa
…[truncated]
- 2026-07-06 context hints: 5 recorded with plan
- 2026-07-06 context hints: crates/vals-client/src/coord/hedge.rs — the whole change lives here (HedgeClock + WaitTicks + tests); crates/vals-rt/src/lib.rs:496-574 — ReserveCollect / quorum_collect_reserve_hedged: the consum
…[truncated]
- 2026-07-07 implementer report: Implemented the task 0082 fix entirely in `crates/vals-client/src/coord/hedge.rs`; no changes to vals-rt, the service ticker, or any hedge-off path. Public API of `HedgeClock` (new/now/advance/wait_un
…[truncated]
- 2026-07-07 review tier: single-opus — reviewers: Claude
- 2026-07-07 review (Claude): accept — The change lands both planned mitigations for the digest-hedge fixed per-read overhead entirely within crates/vals-client/src/coord/hedge.rs: (1) HedgeClock's flat `Vec<Waker>` is replaced with a dead
…[truncated]
- 2026-07-07 decision: accept — commit: client: deadline-bucketed HedgeClock waiters + WaitTicks drop-deregistration — kills the fixed per-read hedge overhead (ncoord=4 gap −10% → within noise) and the per-tick thundering herd (25ms-t
…[truncated]
- 2026-07-07 usage: 38,997 tok (in 98, out 38,899, cache_r 2,236,857, cache_w 179,936) · cost n/a (unpriced)
  coordinator: 18,104 tok (in 60, out 18,044, cache_r 1,796,378, cache_w 111,979) · cost n/a (unpriced)
  implementer: 14,577 tok (in 18, out 14,559, cache_r 238,586, cache_w 34,553) · cost n/a (unpriced)
  reviewer:Claude: 6,316 tok (in 20, out 6,296, cache_r 201,893, cache_w 33,404) · cost n/a (unpriced)

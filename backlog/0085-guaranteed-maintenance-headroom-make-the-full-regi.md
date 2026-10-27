---
id: "0085"
title: 'Guaranteed maintenance headroom: make the full-region write wedge structurally impossible'
status: done
priority: 3
created: "2026-07-07"
updated: "2026-07-07"
depends_on: []
spec_refs:
    - docs/out-of-core-index.md#Maintenance scheduling — keeping compaction off the hot path
---

## Description
Task 0084's region-pressure trigger makes overwrite-churn reclamation engage before the region fills in practice, but a **residual race** remains: a writer can fill the last 25% of the region within one trigger→merge-commit window (scheduler tick ≤1s + plan + off-core merge duration, which grows with data). At 0 free segments `flush_plan`/`compact_plan` cannot reserve output segments, so the zone deadlocks permanently (writes fail forever; restart doesn't help) — reclamation needs space to make space.

Design direction (either or both):
- **Reserved maintenance headroom**: N segments in the allocator usable only by flush/merge reservations. Data-path vlog appends fail with DeviceFull while free ≤ N, so a pressure merge can always plan and the zone self-heals after a bounded error window. Sizing: the merge output bound is proportional to *live* data (run index upper bound + `2*ceil(live_value_bytes/usable)+2` vlog output), so a static reserve only covers mostly-dead zones — consider a dynamic reserve computed from current memtable/run metas (both O(1)-available).
- **Admission backpressure tied to reclaim**: refuse/queue data-path appends earlier (free below the dynamic reserve estimate) so clients see clean backpressure errors instead of a cliff, and the error window shrinks to the in-flight merge's duration.

## Acceptance criteria
- A mem-backend test drives unthrottled overwrite churn (writer strictly faster than reclaim) into a small region: writes may error transiently, but the zone must ALWAYS recover once the in-flight merge commits — no permanent wedge, no matter the timing. (Today this can still wedge if the region fills before any merge planned.)
- The dynamic reserve/backpressure never rejects writes on a mostly-live zone with ample free space (no false positives at low fill).
- Doc note in docs/out-of-core-index.md §Maintenance scheduling replacing the "Residual (follow-up)" caveat.

## Plan

Goal: make the full-region write wedge structurally impossible. Guarantee: overwrite churn may cause transient, retryable write errors, but the zone ALWAYS self-heals once the in-flight pressure merge commits — never a permanent deadlock. (A mostly-live full zone — dead < 10% — remains legitimate device-full, out of scope per 0084's design.)

Mechanism 1 — dynamic maintenance reserve (the must-have):
- Compute a reserve estimate: the number of segments a pressure flush + run-merge needs to plan, derived from O(1)-available live state (memtable live_bytes + Σ run metas' index/value_bytes; merge output bound ≈ run index upper bound + 2*ceil(live_value_bytes/usable)+2 vlog output — mirror the existing flush_plan/compact_plan reservation math so the reserve is an honest upper bound on what plans will actually request).
- Data-path vlog segment allocation fails (DeviceFull-class error) while free ≤ reserve; flush/merge plan reservations MAY consume the reserve. This guarantees a pressure merge can always plan, so reclamation always has space to make space.
- Keep it off the hot path: recompute the estimate only on structural events (flush/merge commit, segment alloc/free, maintenance tick) and cache it; the per-append check is a single integer comparison, and it only binds at segment-allocation boundaries (~once per 1 MiB of appends), not per record. No per-op computation over run metas.

Mechanism 2 — early backpressure (same manifestation, earlier threshold):
- When free drops below the dynamic estimate, data-path appends get the SAME retryable rejection before the hard floor — clients see a short, orderly error window (bounded by the in-flight merge's duration) instead of a cliff.
- Surface as a distinct retryable wire error (e.g. a Busy/Backpressure response variant, not today's generic Response::Err string) so the quorum coordinator and clients can distinguish "back off and retry" from real failure. NO server-side queuing/parking of writes — explicitly out of scope (merge duration is seconds; parking = unbounded memory + timeouts anyway). Client retry/backoff is the slowdown.

Tests / acceptance:
- Kill-the-fix mem-backend test: small region, unthrottled overwrite churn with the writer strictly faster than reclaim (the regime that wedged oldbox live — region fills within one scheduler tick). Writes may error transiently but the zone MUST always recover once the merge commits, regardless of timing. Reverting the reserve must wedge the test.
- No-false-positive test: mostly-live zone at low fill with ample free space never gets rejected.
- Write-perf A/B: no measurable regression on the write path from the reserve check (user's explicit constraint).
- Spec: replace the "Residual (follow-up, task 0085)" caveat in docs/out-of-core-index.md §Maintenance scheduling with the reserve/backpressure design.

Likely touch points: allocator + vlog append path in crates/vals-store (vlog.rs:1333 allocate→DeviceFull is the existing hard floor), plan reservation math in engine.rs (flush_plan/compact_plan reserve sizing to mirror), pressure trigger context in crates/vals-cluster/src/node.rs (zone_maint_due), wire error variant in crates/vals-cluster/src/net/server.rs (Response::Err today), regression test alongside crates/vals-cluster/tests/churn_reclaim.rs.

### Starting points
- crates/vals-store/src/vlog.rs:1333 — alloc.allocate().ok_or(LogError::DeviceFull): the existing hard floor where the data-path reserve check belongs
- crates/vals-store/src/engine.rs flush_plan/compact_plan — reservation sizing math to mirror for the dynamic reserve estimate (run index upper bound + 2*ceil(live_value_bytes/usable)+2)
- crates/vals-cluster/src/node.rs zone_maint_due — 0084 pressure trigger (free ≤ 50% AND lsm_dead_fraction ≥ 10%); merge is planned BEFORE flush so it gets first claim on headroom
- crates/vals-cluster/tests/churn_reclaim.rs — existing kill-the-fix churn regression test to model the new test on
- crates/vals-cluster/src/net/server.rs — writes fail as Response::Err(String) today; add a distinct retryable Busy/Backpressure variant
- docs/out-of-core-index.md ~line 523 — the 'Residual (follow-up, task 0085)' caveat to replace
- bench/t0084/notes.md §Box validation item 3 — the live wedge repro (512 MiB region, ~250 MB/s churn, fills within one 1 s tick)

## Work log

- 2026-07-08: **The residual regime was confirmed live on oldbox** during task
  0084's box validation (bench/t0084/notes.md §Box validation item 3): a 512 MiB region
  under ~250 MB/s NVMe overwrite churn filled **within one 1 s scheduler tick** — every
  pressure flush plan failed from the very first attempt ("no free segments"), and the
  legacy compactor's later attempt hit `InsufficientHeadroom`
  (`compaction_headroom_skips=1`). Permanent wedge, zero recovery paths, exactly the
  deadlock this task exists to make impossible. Note for sizing intuition: the trigger
  path wins whenever region_size/write_bandwidth spans many ticks (8 GiB at the same rate
  = 32 s, validated fine); the reserve is needed for the small-region/fast-writer corner.
- 2026-07-07 plan: Goal: make the full-region write wedge structurally impossible. Guarantee: overwrite churn may cause transient, retryable write errors, but the zone ALWAYS self-heals once the in-flight pressure merge
…[truncated]
- 2026-07-07 context hints: 7 recorded with plan
- 2026-07-07 context hints: crates/vals-store/src/vlog.rs:1333 — roll(): alloc.allocate().ok_or(LogError::DeviceFull) — the existing hard floor; data-path reserve check belongs here (and any other data-path allocate in vlog.
…[truncated]
- 2026-07-07 implementer report: Implemented Mechanism 1 (the "must-have" dynamic maintenance-headroom reserve) that makes the full-region write wedge structurally impossible, plus the kill-the-fix test and the doc note. All acceptan
…[truncated]
- 2026-07-07 review tier: high-powered — reviewers: Claude
- 2026-07-07 review (Claude): accept — The change implements Mechanism 1 (the must-have dynamic maintenance-headroom reserve) correctly and completely against the acceptance criteria. The allocator gains a data-path-only reserve floor (`al
…[truncated]
- 2026-07-07 decision: accept — commit: store+cluster: dynamic maintenance-headroom reserve — full-region write wedge structurally impossible: data-path vlog appends back off at a live-state-derived reserve floor (Allocator::allocate_data
…[truncated]
- 2026-07-07 usage: 209,931 tok (in 648, out 209,283, cache_r 51,806,773, cache_w 465,372) · $35.2238
  implementer: 184,564 tok (in 504, out 184,060, cache_r 49,202,440, cache_w 326,777) · $31.2476
  reviewer:Claude: 14,573 tok (in 94, out 14,479, cache_r 1,736,301, cache_w 62,806) · $1.6231
  coordinator: 10,794 tok (in 50, out 10,744, cache_r 868,032, cache_w 75,789) · $2.3531

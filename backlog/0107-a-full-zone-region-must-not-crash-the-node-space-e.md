---
id: "0107"
title: A full zone region must not crash the node — space exhaustion must be graceful + fault-isolated
status: done
priority: 2
created: "2026-07-31"
updated: "2026-07-31"
depends_on: []
spec_refs:
    - docs/backpressure.md#Backpressure
    - docs/zones.md#Zones
---

## Description
## Symptom (from the 2026-07-31 bootes soak, `bench/bootes-soak-lab/`)
The `backlinks` sorted zone had no range splits → single range, token `""` → RF-2 placed **all** backlinks on one replica pair (nodes 1+2), and intra-node all on **one of 4 partitions**. That one 32 GiB region filled in ~1 h. Then:
- `zone 2 memtable flush plan failed (retrying next tick): no free segments for snapshot` looped every ~300 ms, alongside `compaction step skipped — N in-flight reads did not drain within 200ms`.
- Both replica-pair nodes **died** within ~1 min of each other (~21:10–21:11). node1 ended a **5 MB process husk with all four shard listeners (7001-7004) AND the metrics endpoint (9464) refusing connections**; node2's process was gone. No OOM (dmesg clean), no panic line in the log — the log just froze mid-spam.
- Blast radius = the **whole node** (every shard, every zone, observability), and via RF the **whole cluster** (RF-2 quorum loss → all ingest stalled; the surviving node3 went idle; bootes dropped every batch: `connection refused`).

## The defect
Space exhaustion of ONE zone should degrade gracefully and stay isolated. Instead it went fatal and node-wide. Two things are wrong:
1. **Escalation to process death.** "Flush can't snapshot" should never become "process husk / all listeners refused." Find the actual mechanism — likely an `unwrap`/`abort` on the region-full write/flush path, or a poller/executor thread exiting and taking the shard listeners (and the shared process) with it. Fix it to a handled, retryable outcome.
2. **No hard space backpressure.** The 0085/0089 maintenance reserve returns `Busy` at a *soft* free-segment threshold, but it did not prevent a genuine zero-free-segment region here (either not armed on this flush-snapshot path, outrun by the concentrated write rate, or the flush snapshot has no reserved segments guaranteed). A region approaching full must reject writes to *that zone* (retryable `Busy`/`ZoneFull`) before flush can be starved of a snapshot target.

## Acceptance
- A zone whose region hits (near-)zero free segments returns a **retryable** error to writers of that zone; the node process does **not** panic/abort/exit.
- While one zone is full: **reads on that zone, and all ops on other zones and other shards on the same node, and the metrics endpoint, keep serving.** Fault isolation is per-zone (and per-shard), not per-process.
- **Kill-the-fix regression test** (file/mem backend, tiny region): drive one zone's region to exhaustion under sustained writes and assert (i) writes to that zone get a retryable error, (ii) the process stays up, (iii) a second zone on the same node keeps serving reads+writes. Reverting the fix fails it.
- The specific escalation path (husk / listeners refused) is root-caused and documented in the fix.

## Notes
Repro + full autopsy: `bench/bootes-soak-lab/notes.md`. Relates to 0085/0089 (maintenance reserve → Busy), 0092 (smooth reserve-floor backpressure), and the placement-skew design task (below/sibling). This is the *fatality* half; the sibling auto-split task is the *don't-concentrate-in-the-first-place* half — either alone leaves a sharp edge.

## Acceptance criteria

## Plan

## Root cause (documented in the fix)

A zone whose region fills with genuinely **live** data (distinct keys, ~no overwrite garbage — e.g. bootes `backlinks`) never engages the maintenance-headroom reserve: `Engine::maint_reserve_segs()` returns **0** unless `dead ≥ 10% AND free ≤ 50%` (mirrors node.rs `PRESSURE_*`). With the reserve at 0 the data path (`Vlog::roll` → `allocate_data`) draws the region to genuine **zero free**, and two bad things happen:

1. **Hard, non-retryable error.** With `maint_reserve()==0`, `Vlog::roll` returns `LogError::DeviceFull` (not `Backpressure`). At the server (`serve_apply`/`serve_batch_apply`/sync apply arm) only `is_backpressure()` maps to the retryable `Response::Busy`; `DeviceFull` falls through to `Response::Err(..)` — a **hard failure**. The coordinator counts a hard `Err` against quorum, treats the replica as a hint candidate, and **trips the failure detector**. With RF-2 both replicas of the concentrated region return `Err` → quorum loss → failure-detector marks healthy nodes down → cluster-wide ingest collapse. That is the blast-radius amplifier (whole cluster from one full zone). The final process "husk / all listeners refused" is the terminal state of that hard-failure + retry/hint storm; the fix removes the **first domino** (hard error) so the cascade cannot start. (The accept-loop resilience angle — a transient `accept()` error permanently killing a shard listener — is split into a follow-on task; see below.)

2. **Flush starved of a snapshot target.** With reserve 0 the data path consumes every last segment, so `flush_plan` can no longer reserve a run target → the `no free segments for snapshot` spin loop every ~300 ms, and the memtable can never drain (a permanent write wedge; task 0084's reserve was meant to prevent exactly this but is gated on `dead ≥ 10%`, so it never armed for a live-full zone).

## Fix

**A. Always-on flush reserve (`crates/vals-store/src/engine.rs`, `maint_reserve_segs`).** Restructure so the **flush term** (`flush_segs`, sized from the O(1) index aggregates exactly as today) is reserved **whenever the memtable is non-empty**, independent of the `dead ≥ 10% / free ≤ 50%` pressure gate. The **merge term** (`merge_segs`) stays pressure-gated (a merge only helps when there's reclaimable garbage). Result: `reserve = flush_segs + (pressure ? merge_segs : 0)`.
   - Guarantees the memtable can **always** flush (its reserved run target is untouchable by the data path) → no `no free segments for snapshot` starvation, no wedge, memtable RAM stays bounded.
   - Makes the data path back off with the **retryable** `LogError::Backpressure` as free approaches the flush footprint, **before** the flush is starved.
   - Preserves the task-0085 no-false-positive guarantee: `flush_segs` is tiny vs a healthy region, so it only binds within a few segments of full; a fresh/append/migrating zone with ample free is never rejected. A **disarmed** engine (`maint_reserve_enabled == false`) is unchanged (reserve stays 0 → still `DeviceFull`), so bare-Engine unit tests keep their semantics.

**B. Space exhaustion is retryable at the serving write paths (`crates/vals-store/src/engine.rs` + `crates/vals-cluster/src/net/server.rs`).** Add `EngineError::is_device_full()` (true for `EngineError::Log(LogError::DeviceFull)`). At every **write** serving site that currently maps `e.is_backpressure()` → `Response::Busy` (`serve_apply`, `serve_batch_apply`, and the synchronous `serve_request` apply/put/CAS arms), also map `e.is_device_full()` → `Response::Busy`. This closes the residual window (memtable-empty + zero-free, where reserve is momentarily 0) so a writer of a full zone **always** gets a retryable `Busy`, never a hard `Err`. Do **not** touch read or control-op error mapping, and leave `is_backpressure()` semantics elsewhere (coordinator ack-counting, maint loop) unchanged.

Document the escalation chain (points 1–2 above) in a comment at the reserve change and/or the server mapping site so the root cause is captured in the fix (acceptance requirement).

## C. Kill-the-fix regression test (cluster level, MemBlockDevice, tiny region)
Add a test (in `crates/vals-cluster/src/net/server.rs` tests, modeled on `staged_batch_answers_busy_at_the_reserve_floor`, but writing **distinct** keys so the region fills with *live* data rather than churn garbage):
- Materialize/arm a store on a tiny region (armed reserve). Drive one zone under sustained **distinct-key** writes until the region hits (near-)zero free.
- Assert (i) the write is refused with the **retryable** `Response::Busy` (never `Response::Err`); (ii) the process/test does not panic/abort; (iii) a **second zone/store** on the same node still serves reads **and** writes while the first is full.
- Confirm it fails on revert: without fix A the live-full region yields `DeviceFull`; without fix B that surfaces as `Response::Err` → the `Busy` assertion fails.

## Verification
`cargo build` + `cargo test -p vals-store -p vals-cluster` (all existing reserve/backpressure tests must still pass — esp. `reserve_floor_rejection_is_backpressure_true_full_is_device_full`, `maint_reserve_lets_a_pressure_merge_plan_from_a_filled_region`, `staged_batch_answers_busy_at_the_reserve_floor`).

## Follow-on (separate task, not this commit)
The terminal "process husk / all four shard listeners + metrics refusing, log frozen, no panic" state points at `accept_loop` (`net/server.rs`) treating a transient `accept()` error (`EMFILE`/`ENOMEM`/`ECONNABORTED`) as **permanent** (`Err(_) => { deregister; return; }`), silently killing a shard's listener while the process lingers. Harden it (log + backoff + retry instead of returning) under its own task with its own test.

### Starting points
- crates/vals-store/src/engine.rs: maint_reserve_segs() ~L4483-4575 (pressure gate to relax for the flush term), refresh_data_reserve() ~L4460, is_backpressure() ~L1638
- crates/vals-store/src/vlog.rs: Vlog::roll ~L1463-1473 (Backpressure vs DeviceFull decision via alloc.maint_reserve())
- crates/vals-store/src/alloc.rs: allocate_data refuses when free_count <= data_reserve (~L235); allocate() (unchecked) is what flush/merge use
- crates/vals-cluster/src/net/server.rs: serve_apply ~L2440-2480, serve_batch_apply, sync serve_request apply arms ~L3600-3720 (each `is_backpressure()` -> Response::Busy site); arm_maint_reserve ~L406; model test staged_batch_answers_busy_at_the_reserve_floor ~L4234
- node.rs PRESSURE_FREE_FRACTION=0.50 / PRESSURE_DEAD_MIN_FRACTION=0.10 mirror MAINT_RESERVE_* in engine.rs
- Response::Busy semantics: crates/vals-client/src/wire.rs ~L672-682; coordinator treats Busy as retryable/no-quorum-count/no-failure-detector-trip in crates/vals-client/src/coord/mod.rs ~L74-80

## Work log
- 2026-07-31 plan: ## Root cause (documented in the fix)  A zone whose region fills with genuinely **live** data (distinct keys, ~no overwrite garbage — e.g. bootes `backlinks`) never engages the maintenance-headroom 
…[truncated]
- 2026-07-31 context hints: 6 recorded with plan
- 2026-07-31 implementer report: Implemented task 0107 — space exhaustion of one zone now degrades gracefully + fault-isolated, never fatal/node-wide.  ## Changes  **A. Always-on flush reserve** — `crates/vals-store/src/engine.rs
…[truncated]
- 2026-07-31 review tier: high-powered — reviewers: claude
- 2026-07-31 review (claude): accept — The change correctly and completely implements task 0107. Fix A makes the memtable-flush reserve always-on (independent of the dead≥10%/free≤50% pressure gate) so a live-full zone backs off with r
…[truncated]
- 2026-07-31 decision: accept — commit: fix(0107): graceful, fault-isolated zone space exhaustion  A zone filling with live data (no reclaimable garbage) never tripped the pressure-gated maintenance reserve, so the data path drew the region
…[truncated]
- 2026-07-31 usage: 97,630 tok (in 312, out 97,318, cache_r 8,797,364, cache_w 348,204) · $9.0095
  implementer: 47,033 tok (in 140, out 46,893, cache_r 4,466,513, cache_w 101,805) · $4.0426
  coordinator: 37,097 tok (in 110, out 36,987, cache_r 3,443,508, cache_w 198,392) · $3.8869
  reviewer:claude: 13,500 tok (in 62, out 13,438, cache_r 887,343, cache_w 48,007) · $1.0800

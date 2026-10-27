---
id: "0060"
title: 'Batch API: fire-now / await-later write handle on CoordinatorService'
status: done
priority: 5
created: "2026-07-05"
updated: "2026-07-05"
depends_on: []
spec_refs: []
---

## Description
Split from grooming umbrella 0025 (BACKLOG.md §Deferred follow-ons, "Batch API" bullet). Unblocked since 2026-06-15 by `CoordinatorService`: spawn the write as a task on the service executor, return a handle, `run_until` its completion later — same primitive as proactive refresh.

Add a fire-now / await-later batch-write handle to the client API: submit a batch write immediately (tier chosen at submit time, as today), get back a handle instead of blocking; the caller can continue issuing work and later `await_tier`/join the handle for the write's outcome.

Notes:
- Single-threaded client (Rc/RefCell); the handle is a same-thread future/task joined via the service executor, not a Send handle.
- Error/backpressure semantics: credit acquisition still happens at submit (so a full pool still exerts backpressure at fire time); the handle resolves to the same success/error the blocking path returns today.
- The **general streaming barrier** from the same bullet stays out of scope (re-triaged: only if a checkpointing workload appears).

## Acceptance criteria
- Client can fire a batch write, do other work, then await the handle; outcome matches the blocking API.
- Handle dropped without awaiting: write still completes (or is documented/asserted otherwise); no leak.
- Tests cover success, replica-error, and backpressure-at-submit paths.

## Acceptance criteria

## Plan

Add a fire-now / await-later batch-write handle to CoordinatorService (crates/vals-client/src/coord/service.rs).

Design:
1. New API on CoordinatorService<T> (spawning needs the executor, so the service is the seam):
   - `submit_write_batch(&self, writes: Vec<(Vec<u8>, Vec<u8>)>, tier: Durability) -> BatchWriteHandle` and a `_in(zone_id, ...)` variant. Owned key/value Vecs because the spawned task must be 'static (the blocking API keeps its borrowed-slice signature).
2. Submission semantics — credits acquired at fire time:
   - Spawn a task on the service executor: `async move { let (ops, bytes) = cost; let _credit = coord.acquire(ops, bytes).await; <signal admitted>; let views: Vec<(&[u8],&[u8])> = ...; let res = coord.batch_inner(zone_id, &views, tier).await; <store result + wake joiner> }`. Note: service.rs is a child module of coord, so it can call the parent's private `acquire`/`batch_inner` directly (or make them pub(crate) if preferred).
   - `submit_*` then pumps `self.ex.run_until(<admitted future>, reactor::poll_io)` before returning the handle, so a full credit pool still exerts backpressure at submit time (FIFO ticket order preserved by the existing Acquire future). Note batch_inner (not write_batch_in) so credits aren't acquired twice.
3. BatchWriteHandle: a same-thread (!Send) handle over `Rc<RefCell<...>>` shared state holding `Option<Result<BatchResult, CoordError>>` plus a stored Waker. Prefer a plain waker cell (no eventfd needed — completion is same-executor); implement Future for the handle (Output = Result<BatchResult, CoordError>) or an async `join(self)`. Caller awaits via `svc.run(handle)`.
   - Dropped-without-await: no cancellation — the spawned task owns the writes and its Rc state, keeps running as a passenger of later `run` turns and is drained to completion by `shutdown`/`Drop` (ex.run runs until no task remains). Document this on the handle.
   - Handle polled after completion / double-take: make join consume the result once (Output stored as Option; panic or return a clear state on re-poll after Ready — document choice).
4. Docs: extend the service module docs (the lazy passenger model applies: a submitted write only progresses while the client runs ops or at shutdown — document that explicitly). Update BACKLOG.md line ~982 ("Batch API" deferred bullet) to note the handle landed (task 0060), streaming barrier still workload-gated.
5. Tests (crates/vals-cluster/src/coord/tests.rs, alongside existing CoordinatorService tests ~line 2259):
   - Success: submit a batch, issue other ops via svc.run, then await the handle; outcomes match what the blocking write_batch returns (values readable, per-write Ok versions).
   - Replica-error: same fault-injection as existing write_batch partial-failure tests (down replicas → quorum shortfall) — handle resolves to the same per-write Failed outcomes as the blocking path.
   - Backpressure-at-submit: coordinator with a tiny credit pool + a stalled/gated transport holding the pool; assert submit does not return until the credit frees (e.g. observe completion ordering or credit_stats.blocked_total increments and in-flight caps hold).
   - Drop-without-await: submit, drop the handle, drive the service (subsequent ops and/or shutdown); the write still lands (readable) and nothing leaks/panics.
   - Empty batch: handle resolves to empty BatchResult.
Run: cargo test -p vals-cluster (plus cargo build/clippy for the client crate).

### Starting points
- crates/vals-client/src/coord/service.rs — CoordinatorService (executor host; run/run_until/shutdown pattern)
- crates/vals-client/src/coord/mod.rs:2078-2136 — write_batch/write_batch_in/batch_inner; :1249 acquire(), :704 batch_cost()
- crates/vals-client/src/coord/mod.rs:712-780 — Credit/Acquire (FIFO, cancellation-safe Drop)
- crates/vals-cluster/src/coord/tests.rs — existing write_batch tests (~1168-1386) and CoordinatorService tests (~2259, 2453)
- BACKLOG.md:982-988 — the 'Batch API' deferred bullet to update
- service.rs is a child module of coord, so parent-private fns (acquire, batch_inner) are directly callable

## Work log
- 2026-07-05 plan: Add a fire-now / await-later batch-write handle to CoordinatorService (crates/vals-client/src/coord/service.rs).  Design: 1. New API on CoordinatorService<T> (spawning needs the executor, so the servi
…[truncated]
- 2026-07-05 context hints: 6 recorded with plan
- 2026-07-05 context hints: crates/vals-client/src/coord/service.rs — CoordinatorService (executor host; run/run_until/shutdown pattern); crates/vals-client/src/coord/mod.rs:2078-2136 — write_batch/write_batch_in/batch_inner
…[truncated]
- 2026-07-05 implementer report: Implemented the fire-now / await-later batch-write handle on `CoordinatorService` (task 0060).  ## Changes - **crates/vals-client/src/coord/service.rs**   - New API: `CoordinatorService::submit_write_
…[truncated]
- 2026-07-05 review tier: single-opus — reviewers: Claude
- 2026-07-05 review (Claude): accept — The change adds a fire-now/await-later batch-write handle to CoordinatorService exactly as the task specifies. submit_write_batch[_in] spawn the write on the service executor and return a BatchWriteHa
…[truncated]
- 2026-07-05 revision: Addressed the reviewer nit: replaced the `Rc::strong_count(&self.slot) == 1` heuristic in `BatchWriteHandle::poll` with an explicit `taken: bool` flag on `Slot`.  ## Changes (crates/vals-client/src/co
…[truncated]
- 2026-07-05 review (Claude): accept — The implementation fully satisfies task 0060. CoordinatorService::submit_write_batch[_in] spawn the batch write on the service executor and return a BatchWriteHandle, acquiring credits at submit (back
…[truncated]
- 2026-07-05 decision: accept — commit: client: fire-now / await-later batch-write handle on CoordinatorService (task 0060)  submit_write_batch[_in] spawns the batch write as a task on the service executor and returns a BatchWriteHandle (a 
…[truncated]
- 2026-07-05 usage: 60,877 tok (in 170, out 60,707, cache_r 4,240,158, cache_w 202,760) · cost n/a (unpriced)
  implementer: 45,656 tok (in 102, out 45,554, cache_r 3,115,822, cache_w 96,903) · cost n/a (unpriced)
  reviewer:Claude: 8,025 tok (in 40, out 7,985, cache_r 424,299, cache_w 38,508) · cost n/a (unpriced)
  coordinator: 7,196 tok (in 28, out 7,168, cache_r 700,037, cache_w 67,349) · cost n/a (unpriced)

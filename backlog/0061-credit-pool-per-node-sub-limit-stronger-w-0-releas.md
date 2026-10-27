---
id: "0061"
title: 'Credit pool: per-node sub-limit + stronger W=0 release semantics'
status: done
priority: 5
created: "2026-07-05"
updated: "2026-07-05"
depends_on: []
spec_refs: []
---

## Description
Split from grooming umbrella 0025 (BACKLOG.md §Deferred follow-ons, "Credit pool" bullet). The write credit pool is global-only (v1) and `W=0` credits release at "handed to transport".

Two scoped extensions:
1. **Per-node sub-limit**: bound the credits any single node's in-flight writes may hold, so one slow/dead replica can't absorb the whole global pool and starve writes to healthy nodes. Tunable; default preserves current behaviour (no sub-limit or a generous one — justify).
2. **Stronger `W=0` semantics**: release the credit only once the write has been *sent to live replicas* (transport write completed towards every currently-live owner), not merely queued to the transport, so `W=0` backpressure actually reflects network progress.

## Acceptance criteria
- A stalled node saturates only its sub-limit; writes routed to other nodes keep flowing (test with a blackholed/slow replica).
- `W=0` credit release ordering pinned by a test (credit not released while the payload is still queued locally).
- No regression in the existing credit-pool tests; BACKLOG.md bullet updated.

## Acceptance criteria

## Plan

Two scoped extensions to the coordinator write credit pool (crates/vals-client/src/coord/mod.rs), plus tests and doc updates.

## 1. Per-node sub-limit (opt-in, default = current behaviour)

- Extend `CreditPool` with per-node bounds `node_max_ops` / `node_max_bytes` (both default `usize::MAX` = disabled) and a per-node in-flight map `HashMap<NodeId, (ops, bytes)>`.
- Charging model: a write charges its full `(ops, bytes)` against **each replica node it targets** (its home preference list); `Credit` remembers the node list and releases the per-node counts on drop. Approximate accounting is fine (e.g. sloppy fallbacks not charged) as long as reserve/release balance exactly.
- Fit rule: global fits AND, for every charged node, `node_in_flight + cost <= node_max` — with a per-node idle escape mirroring the global one (a node with zero in-flight always admits, so one oversized write can't deadlock).
- FIFO relaxation (essential — otherwise a waiter parked on a saturated node's sub-limit blocks writes to healthy nodes behind it, defeating the feature): waiters store `(ops, bytes, nodes, waker)`. Admission rule for ticket T: T fits now AND no earlier waiter is **global-blocked** (an earlier waiter blocked only by a per-node limit does not gate T; one blocked on global capacity does — preserving the existing large-write fairness). Wake logic: on release (and on Acquire cancel/admit cascade) wake, in ticket order, the first waiter that would actually be admitted under this rule; fall back to waking the front. Must remain cancellation-safe (Acquire::drop) and free of lost wakeups.
- API: keep `acquire(ops, bytes)`/`try_acquire` as global-only (charging no nodes) so the batch/service paths compile unchanged; add a node-charging variant (e.g. `acquire_for(nodes, ops, bytes)` + try_ counterpart). New builder `Coordinator::with_node_credits(max_ops, max_bytes)`; `with_credits` unchanged. Default disabled — justification: preserves v1 behaviour and zero overhead on the default path; await-first-W already bounds the common quorum head-of-line case, so the sub-limit is an opt-in knob for W=RF / W=0 workloads.
- Single-write paths (`write_tier_in`, `try_write_tier`, `delete_in`, `write_sloppy`, `delete_sloppy`, CAS write): when node limits are enabled, `resolve_replicas(zone_id, key)` **before** acquiring and charge those nodes; when disabled, skip resolution (no behaviour change). Batch path stays global-only in this iteration — document that in `docs/backpressure.md`.
- Extend `CreditStats` with the node bounds (and optionally a saturated-node signal); only coord tests construct/consume it, so this is safe.

## 2. Stronger W=0 release semantics ("sent to live replicas")

Today `put_once` with `needed == 0` runs `quorum_ok(applies, 0)`, which resolves after a single poll of each apply future — i.e. the frame is merely queued into the mux conn's `out` buffer ("handed to transport") and the credit releases immediately. Implement the documented chosen semantics from docs/backpressure.md §"W=0 credit semantics":

- New `Transport` method with a default impl: `apply_send(...) -> Future<Output = Result<(), TransportError>>` — resolves when the write has been *sent* toward the node. Default implementation delegates to `apply(...)` and discards the ack (correct-and-stronger; in-proc `LocalCluster` completes synchronously anyway, and all existing mock/wrapper transports keep compiling).
- `put_once`: special-case `needed == 0` — fan out `apply_send` to every home replica (still gated by `fd_admit`, so an FD-down node fails fast and becomes a hint exactly as today), await **all** of them (`vals_rt::join_all`-style), collect `Unreachable`/failed sends into `failed` for hint recording, then return `Committed { version, home_acks: 0, .. }`. The write's credit (held by the caller across `put_inner`) thus releases only when the transport writes toward every currently-live owner completed. Do not feed send-only successes into the failure detector (a local flush proves nothing about the peer).
- `NetTransport` override with true sent semantics: on `ConnState`, track monotone counters `enqueued_total` (bumped when a frame is appended to `out`) and `sent_total` (bumped by `n` on each successful socket write in `drive`). A send-waiter records its frame-end watermark; it resolves `Ok` once `sent_total >= watermark`, and is failed on conn reset (`reset_failed`) and by the per-RPC deadline sweep (`fail_expired`) so a black-holed peer can't hang W=0 forever. Reuse the existing `pending`/`Waiter` machinery where practical (a send-waiter that resolves at its watermark simply abandons the eventual reply — unmatched reply ids are already dropped silently in `drain_reads`). Wake send-waiters from `drive` when the watermark advances.

## 3. Tests

- Per-node sub-limit (crates/vals-cluster/src/coord/tests.rs, reusing the `SlowCluster` pattern): a stalled/blackholed replica (apply futures that never complete until signalled) saturates only its sub-limit — concurrent writes to keys owned by healthy nodes keep flowing while waiters for the stalled node stay parked; stats/accounting return to zero after drain; FIFO-bypass behaviour pinned (a later write to a healthy node admits past an earlier per-node-blocked waiter, while a global-blocked waiter still gates).
- CreditPool unit tests for the new fit/wake rules incl. per-node idle escape and cancellation safety.
- W=0 ordering, coordinator level: with a deferred-apply wrapper transport and `with_credits(1, ...)`, a `NoSync` write's credit is NOT released while the sends are incomplete (second acquire stays parked), and releases once they complete; an FD-down replica does not stall the producer (goes to hint).
- W=0 ordering, net level (white-box, crates/vals-cluster/src/net/tests.rs): a send-watermark waiter is not ready while the payload sits in `ConnState::out` (queued locally), becomes ready after `drive()` flushes it to the socket; failed on reset.
- Run existing credit-pool + coord + net test suites — no regressions (`cargo test -p vals-cluster`, plus `-p vals-client` if it has tests).

## 4. Docs

- `docs/backpressure.md`: mark the chosen W=0 semantics as implemented (drop the "we currently do the weaker variant" framing), and document the per-node sub-limit knob (default off + justification) and that the batch path charges global-only.
- `BACKLOG.md` line ~991 ("Credit pool" deferred bullet): update to note both halves are now built (task 0061); also touch the §"Backpressure / credit pool" as-built bullet (~line 1303) to mention the sub-limit knob and the W=0 release point.

### Starting points
- crates/vals-client/src/coord/mod.rs — CreditPool/Credit/Acquire (~lines 612-790), put_once (~1933), write paths acquiring credits (~1599, 1613, 1634, 1668, 1682, 2094, 2107), Transport trait (~176)
- crates/vals-client/src/net/client.rs — ConnState (out/out_pos, pending, Waiter), drive() flush loop (~260), reset_failed (~233), fail_expired (~211), MuxRpc::poll queues frame into st.out (~1175)
- crates/vals-cluster/src/coord/tests.rs — SlowCluster wrapper + existing credit tests (~line 50-560): patterns for deferred applies and CreditPool unit tests
- crates/vals-cluster/src/net/tests.rs — white-box ConnState tests (builds ConnState directly, ~line 451)
- vals_rt::quorum_ok resolves immediately at k=0 (crates/vals-rt/src/lib.rs ~310) — why W=0 currently releases at 'handed to transport'
- docs/backpressure.md §'W=0 (fire-and-forget) credit semantics' — the chosen design to implement
- BACKLOG.md ~line 991 (deferred bullet) and ~1303 (as-built credit pool bullet)

## Work log
- 2026-07-05 plan: Two scoped extensions to the coordinator write credit pool (crates/vals-client/src/coord/mod.rs), plus tests and doc updates.  ## 1. Per-node sub-limit (opt-in, default = current behaviour)  - Extend 
…[truncated]
- 2026-07-05 context hints: 7 recorded with plan
- 2026-07-05 context hints: crates/vals-client/src/coord/mod.rs — CreditPool/Credit/Acquire (~lines 612-790), put_once (~1933), write paths acquiring credits (~1599, 1613, 1634, 1668, 1682, 2094, 2107), Transport trait (~176);
…[truncated]
- 2026-07-05 implementer report: Implemented Task 0061 — credit-pool per-node sub-limit + stronger W=0 release semantics — in the coordinator write credit pool, with tests and docs.  ## What changed  ### 1. Per-node sub-limit (op
…[truncated]
- 2026-07-05 review tier: single-opus — reviewers: Claude
- 2026-07-05 review (Claude): accept — The change correctly and completely implements both scoped extensions. The per-node sub-limit adds a per-node in-flight map, per-node idle escape, and a relaxed-FIFO admission rule (can_admit) that le
…[truncated]
- 2026-07-05 decision: accept — commit: client: credit-pool per-node sub-limit + W=0 release at "sent to live replicas" (task 0061)  Per-node sub-limit (opt-in, Coordinator::with_node_credits, default off): a charged write counts against ea
…[truncated]
- 2026-07-05 usage: 142,932 tok (in 398, out 142,534, cache_r 19,907,864, cache_w 429,663) · cost n/a (unpriced)
  implementer: 102,443 tok (in 256, out 102,187, cache_r 16,625,686, cache_w 199,460) · cost n/a (unpriced)
  coordinator: 28,693 tok (in 86, out 28,607, cache_r 2,317,935, cache_w 169,651) · cost n/a (unpriced)
  reviewer:Claude: 11,796 tok (in 56, out 11,740, cache_r 964,243, cache_w 60,552) · cost n/a (unpriced)

---
id: "0089"
title: Distinct retryable Busy/Backpressure wire variant for reserve-floor write rejections (0085 Mechanism 2)
status: done
priority: 3
created: "2026-07-07"
updated: "2026-07-07"
depends_on:
    - "0085"
spec_refs: []
---

## Description
Split off from task 0085 (which shipped Mechanism 1, the dynamic maintenance-headroom reserve). Under region pressure the data-path vlog append now fails with a retryable DeviceFull-class error, but on the wire it still surfaces as the generic `Response::Err(String)` — clients and the quorum coordinator cannot distinguish "back off and retry, an in-flight merge will free space" from a real failure.

Scope:
- Add a distinct retryable wire variant (e.g. `Response::Busy`/`Backpressure`) in crates/vals-client/src/wire.rs, mapped from the reserve-floor DeviceFull in the server write path (crates/vals-cluster/src/net/server.rs, `Response::Err` today).
- Teach the quorum coordinator / client to treat it as retryable-with-backoff, distinct from hard errors.
- NO server-side queuing/parking of writes (explicitly out of scope — merge duration is seconds; parking = unbounded memory + timeouts). Client retry/backoff is the slowdown.
- Mind wire compatibility (new variant tag; see wire_fuzz.rs conventions).

## Acceptance criteria
- Reserve-floor rejections surface on the wire as the distinct retryable variant, not `Response::Err`.
- Client/coordinator classify it as retryable; a churn client backing off on it rides out the merge window without treating it as failure.
- Wire round-trip/fuzz coverage for the new variant.

## Plan

Goal: reserve-floor (task 0085 Mechanism 1) write rejections must surface on the wire as a distinct retryable variant (`Response::Busy`), which the client/coordinator classify as retryable-with-backoff — not as generic `Response::Err`, not as an unreachable replica, and never as a hint/failure-detector event. No server-side queuing.

1. vals-store — distinguish the reserve-floor rejection from genuine device-full:
   - Add `LogError::Backpressure` (Display: something like "write backpressure: free segments held for maintenance reclaim; retry"). In `vlog.rs` where `alloc.allocate_data().ok_or(LogError::DeviceFull)` sits (~line 1396): when `allocate_data()` returns None, return `Backpressure` iff `alloc.maint_reserve() > 0` (the floor is armed — free segments exist but are held for the pressure flush/merge), else `DeviceFull`. This is exact: `allocate_data` refuses when `free_count <= data_reserve`; with `data_reserve == 0` that means genuinely zero free.
   - Add `EngineError::is_backpressure(&self) -> bool` helper (true for `EngineError::Log(LogError::Backpressure)`).
   - Update any existing tests that assert `DeviceFull` on the reserve-floor path (e.g. engine.rs ~9519); add/adjust a unit test asserting: reserve armed + floor hit → Backpressure; reserve disabled + truly full → DeviceFull.

2. Wire protocol (crates/vals-client/src/wire.rs):
   - New response kind `RESP_BUSY: u8 = 16` and unit variant `Response::Busy` with doc comment: retryable — the replica is healthy but refused the write at its maintenance-reserve floor; back off and retry, an in-flight/imminent reclaim merge will free space. The op was NOT applied.
   - Encode/decode arms + round-trip unit test.
   - crates/vals-cluster/tests/wire_fuzz.rs: add a generator arm for `Response::Busy` (follow the existing tag-mapping style; note `13`/`14` etc. are already used arbitrarily there — pick the next free generator tag).
   - docs/formats.md §1 (wire protocol): document the new response kind byte.

3. Server mapping (crates/vals-cluster/src/net/server.rs, serve_request):
   - `OwnedRequest::Apply` arm: `Err(e)` where `e.is_backpressure()` → `Response::Busy`, else keep `Response::Err(format!("{e}"))`.
   - `OwnedRequest::BatchApply` arm: same mapping for a per-write apply error and for the `store.sync()` error (whole sub-batch answers `Busy`).
   - Dispatch-level `server_errors` counter only fires on `Response::Err`, so Busy won't inflate it; add a dedicated counter (e.g. `busy_rejections`) in crate::observe so the backpressure regime is observable.

4. Client transport (crates/vals-client):
   - New `TransportError::Busy` variant + Display ("busy: replica at maintenance-reserve floor, retry with backoff" or similar). Doc: healthy refusal — does not count toward quorum, not a hint candidate, must not trip the failure detector.
   - net/client.rs: map `Response::Busy` → `Err(TransportError::Busy)` in the apply and batch-apply reply paths (and anywhere a data-op reply is decoded, mirroring how `Response::Err` is handled).
   - In-process transport parity: vals-cluster's LocalCluster (coord/local_cluster.rs) maps engine errors to TransportError — map backpressure engine errors to `TransportError::Busy` there too.

5. Coordinator classification (crates/vals-client/src/coord/mod.rs):
   - `AckKind::Busy` in `classify_apply` for `Err(TransportError::Busy)`: not an ack, NOT a hint candidate (replica healthy), counted separately.
   - Failure detector: fd_apply etc. record `ok = true` when the error is `Busy` (a real reply from a healthy node). Do NOT change StaleEpoch/other FD behavior.
   - Quorum shortfall: thread a `busy` count through `quorum_shortfall_error`; new `CoordError::Busy { needed, got, busy }` (name it Busy or Backpressure — pick one and use consistently) meaning "healthy replicas refused on backpressure — retry with backoff". Precedence: StaleRing first (needs refresh), then Busy, then SkewRejected, then QuorumFailed.
   - Batch path: add `BatchAck::Busy`; whole-batch `Response::Busy` / `TransportError::Busy` → per-entry Busy acks; the batch quorum classification counts them as busy (not unreachable, no hints).
   - CoordError Display arm.

6. vals-bench: `coord_err_variant` is an exhaustive match — add the new variant name. Optionally (small, judgement): in the churn/write loop, on `CoordError::Busy` retry the op with capped exponential backoff instead of tallying it as a failure, so a box churn run rides out merge windows; keep it simple and clearly commented.

7. Tests:
   - Wire round-trip for Busy + fuzz arm (criterion 3).
   - Store test: reserve-floor rejection yields Backpressure, true full yields DeviceFull.
   - Server/serve test: a store at its reserve floor answers Apply/BatchApply with `Response::Busy` (adapt the 0085 test rigs that fill a small device and arm `set_maint_reserve_enabled(true)`).
   - Coordinator test with a scripted transport: (a) replicas returning Busy → write fails with `CoordError::Busy`, no hints stored, FD not tripped; (b) a client loop that backs off on Busy while the transport returns Busy for a bounded window then accepts → all writes eventually succeed with no hard failures (the "rides out the merge window" criterion at unit scale).
   - `cargo build` + workspace tests for the touched crates (vals-store, vals-client, vals-cluster, vals-bench build).

Out of scope: any server-side parking/queuing of writes; changing FD semantics for existing error kinds; box/SPDK validation.

### Starting points
- crates/vals-store/src/vlog.rs:1396 allocate_data().ok_or(LogError::DeviceFull) — the reserve-floor choke point; LogError enum at vlog.rs:201
- crates/vals-store/src/alloc.rs: allocate_data()/maint_reserve()/set_data_reserve()
- crates/vals-store/src/engine.rs:9446-9550 maint-reserve tests; engine.rs:9519 matches EngineError::Log(LogError::DeviceFull)
- crates/vals-client/src/wire.rs: RESP_* consts 0..15 (next free = 16), Response enum, encode_response_into, decode_response, tests module
- crates/vals-cluster/src/net/server.rs:3100 (Apply arm), :3177 (BatchApply arm) in serve_request
- crates/vals-client/src/coord/mod.rs: TransportError (line ~48), AckKind + classify_apply (~957), quorum_shortfall_error (~987), BatchAck (~135), CoordError (~444), fd_record (~2041)
- crates/vals-client/src/net/client.rs:955-1010 apply/batch reply decode paths
- crates/vals-cluster/tests/wire_fuzz.rs:281-340 response generator tags
- crates/vals-bench/src/main.rs:5974 coord_err_variant exhaustive match
- docs/formats.md §1 wire protocol response kinds
- crates/vals-cluster/src/coord/local_cluster.rs in-process transport EngineError mapping

## Work log
- 2026-07-07 plan: Goal: reserve-floor (task 0085 Mechanism 1) write rejections must surface on the wire as a distinct retryable variant (`Response::Busy`), which the client/coordinator classify as retryable-with-backof
…[truncated]
- 2026-07-07 context hints: 11 recorded with plan
- 2026-07-07 context hints: crates/vals-store/src/vlog.rs:1396 allocate_data().ok_or(LogError::DeviceFull) — the reserve-floor choke point; LogError enum at vlog.rs:201; crates/vals-store/src/alloc.rs: allocate_data()/maint_re
…[truncated]
- 2026-07-07 implementer report: Implemented Task 0089: a distinct retryable Busy/backpressure wire variant for reserve-floor write rejections, classified as retryable-with-backoff end-to-end.  ## Changes  **vals-store** (`vlog.rs`, 
…[truncated]
- 2026-07-07 review tier: single-opus — reviewers: Claude
- 2026-07-07 review (Claude): accept — The change fully satisfies task 0089. Reserve-floor write rejections now surface as a distinct retryable wire variant (Response::Busy, tag 16) rather than Response::Err, threaded end-to-end: LogError:
…[truncated]
- 2026-07-07 decision: accept — commit: cluster: distinct retryable Busy wire variant for reserve-floor write rejections (task 0089) — LogError::Backpressure at the vlog reserve floor, Response::Busy (tag 16) on the wire, TransportError/B
…[truncated]
- 2026-07-07 usage: 75,034 tok (in 410, out 74,624, cache_r 13,516,046, cache_w 375,843) · $13.7713
  implementer: 49,786 tok (in 296, out 49,490, cache_r 10,755,732, cache_w 135,721) · $7.4649
  coordinator: 18,073 tok (in 58, out 18,015, cache_r 2,157,365, cache_w 202,763) · $5.5932
  reviewer:Claude: 7,175 tok (in 56, out 7,119, cache_r 602,949, cache_w 37,359) · $0.7132

---
id: "0185"
title: 'Batched digest-quorum reads: one value plus version-only replicas per key'
status: done
priority: 2
created: "2026-08-11"
updated: "2026-08-12"
depends_on:
    - "0183"
spec_refs:
    - ARCHITECTURE.md#Status
    - docs/formats.md#1. Wire protocol (`vals-cluster`)
---

## Description
Replace the current multi-get shape that fetches full values from every R replica with a vectorized digest-quorum protocol. For each key, one selected replica returns the value and the remaining quorum replicas return versions; requests and replies are grouped by node/shard, with a fallback value fetch only when the digest-only replica owns the winner.

Acceptance criteria:
- Preserve W+R>RF quorum semantics, version argmax, stale-epoch handling, per-key errors, tombstones, read repair, hedging/fallback behavior, mixed-version compatibility, and response-size bounds.
- Batch width amortizes frame parsing, socket operations, scheduling, and response flushing without serial per-key device waits.
- With task 0183 enabled, digest owners perform zero device reads and value owners approximately one direct value read per key in steady state.
- A/B widths 1/8/32/128 on a mature RF=2 corpus; record keys/s, calls/s, device reads/key, CPU/instructions/key, network/syscalls, p50/p99, errors, and correctness. The gate is a material win over scalar digest reads, not merely over the known-slower full-value SyncValues path.
- Update wire/API documentation and add codec, fallback, partial-failure, and integration tests.

## Acceptance criteria

## Plan

Implement a bounded, vectorized strong-read protocol end to end, keeping the scalar Digest path as the compatibility fallback. Add new wire request/reply variants for a batch of keys with a per-key role (full value or version only), explicit per-key result/error representation, route epoch, strict count/size validation, and reply truncation/resumption semantics that cannot split or reorder results. Extend `Transport` with a batched mixed-read operation whose default implementation fans out through existing scalar `read`/`digest`, and implement the TCP transport with capability/fallback handling so an old/unsupported peer transparently uses scalar operations while new peers use one frame per grouped node request. On the server, group the full-value subset into the existing async batched engine read machinery, answer digest-only entries from the winner directory without device I/O when enabled, overlap all staged reads instead of awaiting keys serially, preserve stale epoch/Busy/remote failures per key, and enforce the existing frame/reply budget.

Replace `Coordinator::read_many_in`'s full-value-on-every-replica behavior when `ReadFanout::Digest` is selected: assign one value owner and the remaining quorum probes as digest owners per key, group all probes by node/shard, collect a per-key quorum with version argmax, issue grouped fallback value fetches only from digest replicas that own missing winners, preserve tombstones, observe winning HLCs, run per-key read repair from the version view, and retain per-key quorum/stale errors plus scalar hedging/fallback behavior. Keep the legacy full-value batch behavior for `All`/`Quorum` modes and ensure duplicate keys and heterogeneous replica sets remain position-stable.

Add codec round trips and malformed/bounds tests; coordinator unit tests for winner selection, tombstones, fallback, partial failures, stale epochs, repair and mixed old/new transport behavior; TCP/server integration tests proving grouped request counts and winner-directory I/O shape. Update `docs/formats.md`, `ARCHITECTURE.md`, and user-facing API docs with opcodes, bounds, fallback, and compatibility behavior. Run focused tests and full debug `vals-client`, `vals-store`, and `vals-cluster` suites. Add a reproducible hardware benchmark directory derived from the task-0183 lab harness, run mature RF=2 widths 1/8/32/128 against both scalar digest and batched digest with the winner directory enabled, collect the requested throughput/RPC/device-read/CPU/instructions/network/syscall/latency/error/correctness metrics, and retain raw results plus findings; only accept the implementation if batching materially beats scalar digest without correctness or tail-latency regression.

### Starting points
- crates/vals-client/src/coord/mod.rs: Transport and Coordinator::read_many_in/read_winner_digest
- crates/vals-client/src/wire.rs and crates/vals-client/src/net/client.rs: protocol codec and TCP SyncValues batching pattern
- crates/vals-cluster/src/net/server.rs: async Read/Digest/SyncValues serving and winner-directory I/O tests
- docs/formats.md#1. Wire protocol (`vals-cluster`)
- bench/lab-winner-directory-20260811-0183/: mature RF=2 harness and metric collection

## Work log
- 2026-08-12 plan: Implement a bounded, vectorized strong-read protocol end to end, keeping the scalar Digest path as the compatibility fallback. Add new wire request/reply variants for a batch of keys with a per-key ro
…[truncated]
- 2026-08-12 context hints: 5 recorded with plan
- 2026-08-12 context hints: crates/vals-client/src/coord/mod.rs: Transport and Coordinator::read_many_in/read_winner_digest; crates/vals-client/src/wire.rs and crates/vals-client/src/net/client.rs: protocol codec and TCP SyncVal
…[truncated]
- 2026-08-12 preload: 11 file(s), ~64 KiB seeded into implementer context
- 2026-08-12 implementer report: Implemented task 0185 end to end.  Changed: - Added feature-negotiated mixed batch reads (opcode 19 / response 18) with value/digest roles, per-key replies/errors, route epochs, 4096-entry bounds, str
…[truncated]
- 2026-08-12 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-12 review (sol): revise — The wire shape and basic grouped coordinator/server path are substantially implemented, but the task is not complete: the mandatory hardware gate has explicitly not been run, and there are material av
…[truncated]
- 2026-08-12 review (sol#2/sol): revise — The wire shape and position-stable batching are a solid start, but the implementation does not yet preserve important scalar-read behavior, the server value path has correctness and boundedness regres
…[truncated]
- 2026-08-12 revision: Addressed all correctness-review findings for task 0185.  Revisions: - Replaced initial grouped-probe `join_all` with a per-node future collector that tracks each key’s quorum independently. Explici
…[truncated]
- 2026-08-12 review (sol): revise — The revision materially improves the implementation: grouped hedging, alternate winner fallbacks, staged asynchronous probes, checked value reads, response-size estimation, and integration coverage we
…[truncated]
- 2026-08-12 review (sol#2/sol): revise — The revision substantially improves the implementation: grouped hedging/reserves and multi-owner fallback are now covered, staged checked value reads replace the unsafe path, paging is conservatively 
…[truncated]
- 2026-08-12 revision: Fixed the remaining mixed-read paging blockers.  Changes: - A single value larger than the normal 8 MiB page budget now makes progress and is returned whenever its conservatively bounded framed respon
…[truncated]
- 2026-08-12 review (sol): revise — The oversized-value regression is now addressed correctly: a single result may exceed the normal page budget when it still fits `MAX_FRAME`, and the revised server stages only bounded lookahead. The c
…[truncated]
- 2026-08-12 review (sol#2/sol): revise — The latest revision resolves the remaining code-correctness concerns from the prior review: oversized-but-frameable values now succeed, response bounds are preserved, and staging is limited to bounded
…[truncated]
- 2026-08-12 revision: Audited and documented the completed lab hardware gate without changing the implementation.  Evidence audit: - Re-ran a fail-closed analyzer over exactly 24 expected arms (scalar/batched × widths 
…[truncated]
- 2026-08-12 review (sol): accept — The revised change satisfies the task. The implementation preserves digest-quorum correctness and compatibility behavior, adds bounded position-stable wire batching, asynchronous server-side probe/val
…[truncated]
- 2026-08-12 review (sol#2/sol): accept — The revision completes the remaining acceptance gate. The retained 24-arm lab sweep covers both modes at widths 1/8/32/128 with three repetitions, and the fail-closed analyzer reproduces the checke
…[truncated]
- 2026-08-12 decision: accept — commit: cluster: batch digest-quorum reads
- 2026-08-12 usage: 70,857,688 tok (in 8,790,054, out 149,682, cache_r 61,917,952, cache_w 0) · cost n/a (unpriced)
  implementer: 57,719,774 tok (in 4,454,336, out 101,150, cache_r 53,164,288, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 5,489,253 tok (in 1,873,767, out 21,502, cache_r 3,593,984, cache_w 0) · cost n/a (unpriced)
  coordinator: 4,777,383 tok (in 1,143,906, out 12,613, cache_r 3,620,864, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 2,871,278 tok (in 1,318,045, out 14,417, cache_r 1,538,816, cache_w 0) · cost n/a (unpriced)

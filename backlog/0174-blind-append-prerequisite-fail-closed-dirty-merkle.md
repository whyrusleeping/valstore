---
id: "0174"
title: 'Blind-append prerequisite: fail-closed dirty Merkle buckets and exact rebuilds'
status: done
priority: 3
created: "2026-08-10"
updated: "2026-08-11"
depends_on:
    - "0167"
spec_refs:
    - docs/blind-append-lww.md#Anti-entropy Merkle strategy
    - docs/blind-append-lww.md#Implementation and rollout sequence
---

## Description
Implement phase 3 of `docs/blind-append-lww.md` while writes still use the legacy old-winner resolve.

Acceptance criteria:
- Each logical-winner Merkle bucket has mutation-generation-tracked clean/dirty state; an update without an exact old winner dirties it rather than publishing a guessed digest.
- Tree equality never uses stale, zero, or shared-sentinel data for a dirty bucket; an exact, cooperative sidecar rebuild publishes only after mutable/manifest/placement generations revalidate, otherwise retries/fails Busy.
- Startup seeding, concurrent-write journal, placement changes, flush, merge, tombstone retirement, cancellation, and restart preserve fail-closed behavior.
- Tests prove divergent dirty nodes cannot compare equal, same-winner/different-loser histories do compare equal after rebuild, and append/flush/merge/placement/crash races cannot publish a stale digest. Metrics expose dirty backlog, age, rebuild cost, retries, and errors.

## Acceptance criteria

## Plan

1. Redesign the in-memory Merkle leaf cache around explicit finest-bucket metadata: a wrapping mutation generation, clean/dirty state (with first-dirty time), exact logical-key count, and placement-group digests. Distinguish an exact absent old winner from an unknown old winner in leaf mutations. Every logical mutation bumps its bucket generation; exact deltas update only clean buckets, unknown-old mutations dirty before they can be served, and retire/flush/merge-related updates never clean a dirty bucket by inference.
2. Extend startup/lazy cooperative seeding and its concurrent mutation journal so exact operations replay as deltas while unknown-old operations publish the affected bucket dirty. Keep restart/cancellation fail-closed: unpublished state is discarded, and placement invalidation or source churn cannot expose stale leaves.
3. Add an exact cooperative dirty-bucket rebuild using Engine version sidecars/digest-page plans. For each requested dirty finest bucket, page through mutable plus all run sidecars, collapse duplicate keys by maximum Version, recompute placement-group XORs and logical counts, and track metadata work. Publish only after the captured bucket mutation generations, storage/manifest generation, mutable generation, and placement generation all still match; otherwise discard and retry. Keep dirty state on I/O error, cancellation, RAM shedding, or exhausted retries.
4. Integrate fail-closed readiness into both network SyncTree and local anti-entropy tree descent. Tree/scoped digest APIs must refuse dirty buckets rather than using cached, zero, or sentinel data; cooperative rebuild retries return Busy/retriable failure after a bounded attempt count. SyncBucketDigests remains the exact sidecar fallback.
5. Add observability for dirty-bucket backlog and oldest age, rebuild requests/retries/errors, metadata bytes/blocks (or the closest exact sidecar-work units available), and rebuild wall cost. Ensure per-store lifecycle/invalidation updates process-wide gauges without underflow or falsely clearing another store’s backlog.
6. Add focused tests for unknown-old dirtying, divergent dirty stores never comparing equal, identical logical winners with different loser histories converging to equal rebuilt trees, exact counts/digests, and publication races against append, flush, merge/retirement, placement changes, cancellation/error, and reopen. Preserve legacy resolving Apply/BatchApply behavior in this phase and expose only the narrow internal hook future blind-append work needs.
7. Run targeted vals-cluster/vals-store tests first, then the full debug workspace test suite (without cargo fmt, since the existing tree is not rustfmt-clean), inspect the diff against docs/blind-append-lww.md phase 3, and fix regressions.

### Starting points
- crates/vals-cluster/src/replica.rs: LeafOp, VersionIndex, SeedProgress, seed_step, scoped_leaf_digests, store/retire paths
- crates/vals-cluster/src/net/server.rs: serve_sync_tree and serve_bucket_digests cooperative generation/retry pattern
- crates/vals-cluster/src/antientropy.rs: reconcile_with local tree descent
- crates/vals-store/src/engine.rs: version_digest_page / plan_version_digest_page / cooperative page execution, storage_generation and mutable_generation
- crates/vals-client/src/observe.rs: counters, gauges, and histograms exported to Prometheus

## Work log
- 2026-08-11 plan: 1. Redesign the in-memory Merkle leaf cache around explicit finest-bucket metadata: a wrapping mutation generation, clean/dirty state (with first-dirty time), exact logical-key count, and placement-gr
…[truncated]
- 2026-08-11 context hints: 5 recorded with plan
- 2026-08-11 context hints: crates/vals-cluster/src/replica.rs: LeafOp, VersionIndex, SeedProgress, seed_step, scoped_leaf_digests, store/retire paths; crates/vals-cluster/src/net/server.rs: serve_sync_tree and serve_bucket_dige
…[truncated]
- 2026-08-11 preload: 8 file(s), ~64 KiB seeded into implementer context
- 2026-08-11 implementer report: Implemented Task 0174 fail-closed dirty Merkle buckets.  Changes: - Redesigned `VersionIndex` with per-finest-bucket wrapping mutation generations and exact logical-key counts, plus sparse clean/dirty
…[truncated]
- 2026-08-11 review tier: high-powered — reviewers: sol, claude
- 2026-08-11 review (sol): revise — The core dirty-state, cooperative rebuild, generation fencing, fail-closed digest APIs, and seed-journal work is substantial, and the current 483-test vals-cluster binary passes (480 passed, 3 ignored
…[truncated]
- 2026-08-11 review (claude): unknown — reviewer error: turn 1: API returned non-200 status code 429: {"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your account's rate limit. Please try again later.
…[truncated]
- 2026-08-11 revision: Addressed all review findings.  Changes: - Added scoped Merkle tree tokens containing every represented finest-bucket mutation generation plus placement generation. - Local anti-entropy now captures t
…[truncated]
- 2026-08-11 review (sol): accept — The revision addresses the prior findings: local tree descent now captures and revalidates scoped bucket/placement generations across the peer await with bounded retries and a focused race test; dirty
…[truncated]
- 2026-08-11 review (claude): unknown — reviewer error: turn 1: API returned non-200 status code 429: {"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your account's rate limit. Please try again later.
…[truncated]
- 2026-08-11 decision: accept — commit: 0174: fail closed dirty Merkle buckets
- 2026-08-11 usage: 28,623,753 tok (in 3,375,828, out 68,277, cache_r 25,179,648, cache_w 0) · cost n/a (unpriced)
  implementer: 20,364,698 tok (in 1,494,976, out 43,738, cache_r 18,825,984, cache_w 0) · cost n/a (unpriced)
  coordinator: 4,428,707 tok (in 1,082,132, out 9,871, cache_r 3,336,704, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 3,830,348 tok (in 798,720, out 14,668, cache_r 3,016,960, cache_w 0) · cost n/a (unpriced)

---
id: "0467"
title: 'AE reconcile gives up (Busy) on zones with sustained writes: every leaf change during the tree round trip invalidates the scope token'
status: done
priority: 3
created: "2026-10-15"
updated: "2026-10-21"
depends_on: []
spec_refs: []
---

## Description

Found while measuring 0465 (bench/0465-rt-ae-busy/FINDINGS.md).

`antientropy::reconcile_with` takes the local leaves and a `tree_scope_token` (generations of every bucket in scope), awaits `peer.tree(...)`, and retries the level if any in-scope bucket generation moved; after 3 attempts it returns `TransportError::Busy`. Every write (and every range-tombstone fold retire) bumps its bucket generation, and at the first level the scope is all buckets. So any zone with sustained writes loses most reconciles.

Evidence (vals-bench `rtqueue`, 3 in-process nodes + PD, sorted zone RF 3, sweep 200 ms, no range deletes, devbox):
- 200 appends/s: 0% of the zone's reconcile slots fail.
- 1,000 appends/s: 57–78% fail. At 2,000/s 94–100% fail, and unpaced (~21k/s) 100% fail.
- With a temporary log in the `attempts == 3` branch, every failure was this branch at the first level (`level 16`), with no fold pending. No counter records it today: `ae_sync_errors` counts it with every other reconcile failure.

Impact: a hot zone is repaired by AE only between bursts. The 2026-08-10 lab tier-one replay metrics (`bench/replay-lab/run-20260810T063959Z-tier-one`, about 0.1 `ae_sync_errors` per sweep pass) suggest that real 8-shard nodes see it far less, so measure it on a real deployment before designing a fix.

Candidate directions (decide after measuring):
- Treat buckets that changed during the round trip as differing and descend into them, instead of retrying the whole level. They are re-checked at the next level or at digest time.
- Or compare the peer's tree against a local snapshot of the leaves taken at the same time, and accept a small staleness, because the digest phase re-validates.
- Count the token-exhaustion outcome separately (e.g. `ae_tree_token_exhaustions`).

Acceptance: a counter for this outcome; a measured failure fraction at 1k–2k writes/s per zone before and after any fix; kill-the-fix tests showing that convergence is unchanged.

## Outcome

Step 1 done: added the vals_ae_tree_token_exhaustions counter with a regression test that fails if the counter is removed. Measured it on the lab SPDK cluster (bench/0467-ae-token-busy/FINDINGS.md). It is rare on real nodes: a hot store loses 0.9/6.4/9.1% of its reconcile slots at 1k/2k/4k writes/s, and a presplit zone loses 0.84% at 2k and 0.31% at 50k msgs/s, against 57–100% in process. Token exhaustion was the only AE failure in every arm, and every arm verified exactly. No fix was made: per the owner's condition, the descend-into-changed-buckets fix is proposed 0494 and awaits the owner's go-ahead.

Commit: feat(ae): count tree-token exhaustions; measured rare on lab SPDK nodes (0467)

---
id: "0472"
title: 'Coordinator scans don''t advance the HLC: a delete issued right after scanning a key can lose to that key''s write'
status: done
priority: 2
created: "2026-10-15"
updated: "2026-10-15"
depends_on: []
spec_refs: []
---

## Description
## Found by
0471's netqueue smoke (vals perkey arm, local 3-node file cluster): verification found 1,029 consumed-and-deleted keys still live on all 3 replicas.

## Cause
Point reads feed the winning version into the coordinator HLC (`read.rs` `hlc.observe`), but `scan_in` (crates/vals-client/src/coord/scan.rs) observes nothing. A consumer that scans a producer's batch and deletes it within the same millisecond stamps its deletes from its own HLC: (same ms, counter 0..). These tie with the write's counter, the tie goes to the higher writer id, and the delete loses. It is still acked (`Applied::Lost`), so the client cannot tell. Evidence from the smoke dump: `p0006/256..282` written at ms …051 with counters 0–26 were still live on all nodes; the consumer's tombstones for 283–319 were stamped at …052 and won.

Range deletes are exposed in the same way (an RT stamped below an entry's version doesn't cover it). Cumulative queue deletes heal on the next delete; a final delete, a per-key delete, or a clock-ahead writer do not. The client also can't work around it, because `scan_in` returns no versions.

## Fix
Observe the maximum version seen in each scan round (entries, tombstones and fragments) into the coordinator HLC, as point reads do.

## Acceptance
- A coordinator test writes a key at a version ahead of the coordinator's clock directly on the replicas of a sorted zone, scans it, then deletes it (point delete, and range delete over it). The key must be gone, and the test must fail with the observe removed.
- The vals-cluster coord tests and range-delete tests pass.

## Acceptance criteria

## Work log

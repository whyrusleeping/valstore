---
id: "0543"
title: AE re-pulls a peer's expired tombstone every pass until the holder's own GC watermark passes it
status: proposed
priority: 4
created: "2026-10-29"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description
Found by 0534's sorted-zone GC simulation (crates/vals-cluster/src/netsim_gc_tests.rs). When node A has collected a tombstone and peer B still holds it (B's data watermark, and so its durable GC cutoff, is behind A's, for example because B owns fewer ranges after a join), every `reconcile_scoped_zone` pass from A to B finds different digests. A pulls the tombstone, `apply_blind_with` skips it as expired (`expired_tombstones_skipped`) and reports it as applied. The next pass does the same. Nothing converges until B's own watermark passes the tombstone, and on a quiet node it never does. So AE never reaches a fixed point: every pass does one value fetch per such key, and the AE "applied" counts never settle. Correctness is not affected; the sim's convergence check had to compare effective states instead of `applied == 0`.

Reproduce: in the sim, make about half the nodes never flush and drive heal-phase AE rounds. With seed 6840129133180087808 (v6 parameters) the pulls 1<-3 and 4<-3 repeat forever on k02/k10/k17/k23.

Possible directions (decide when accepted): leave the expired tombstone out of the serving side's digests, or count expired skips as no-ops so the caller can tell there was no progress.

## Acceptance criteria

## Work log

---
id: "0317"
title: Rehome commit must not refold every zone store when only replica order changes
status: done
priority: 1
created: "2026-09-27"
updated: "2026-09-27"
depends_on:
    - "0315"
spec_refs: []
---

## Description
## Evidence (LONG01-D7-SPLITPOLL1000, 2026-09-27)

This was a diagnostic D-prefix run on BUILD-LANES with `VALS_PD_RANGE_SPLIT_POLL_MS=1000`. It was stopped at about 187M ACK with a clean owned stop.

- The faster poll left the PD's split loop idle on some polls, so the rehome loop could finally act. On all earlier D runs, one split per zone per poll had used up the zone's single range action every 5 s, and `pd_rehomes_started` was 0.
- There were two rehomes. Each commit bumped `ae_seed_placement_invalidations` by 8 on node1 (every shard store of the zone) and started full AE seeds: folds went 0 → 52M → 121M, and seed wall time was 178 s. The prepare epochs did not invalidate.
- ACK/s fell from about 1.1–1.3M to 50–300k for about 250 s, then recovered to about 850k once the seeds finished.

## Mechanism

`placement_equivalent` (replica/lifecycle.rs) compares `replicas_for_key_while_rehoming` vectors in ring-walk order. Leaves and every other consumer use `group_of`, which sorts the set.

On 2-node RF2, every range's replica set is always {1,2}. A rehome commit to a new token can reorder [1,2] into [2,1]. The code then treats this as a placement change and runs `invalidate_leaf_seed` on every store of the zone, although no leaf key changes. A prepare keeps the order, because the union appends the target set, which is why prepares were not invalidating.

## Candidate

Compare sorted replica sets in `placement_equivalent`. Add a unit test in which the rehome commit flips the order and must reuse the seed and counts. Real node-set changes (more nodes than RF) still invalidate. Any range-local reprojection for them is a separate design.

## Acceptance

- Unit tests pass, including the new order-flip test and the full vals-cluster suite.
- A preregistered D-prefix diagnostic with the 1 s poll (so rehomes happen) shows 0 placement invalidations after boot and 0 seed folds, with throughput and gates reported.
- Then decide the split-cadence and rehome-starvation policy on that evidence.

## Acceptance criteria

## Work log

### 2026-09-27: accepted and merged (`f29ea91c`)

- **Change.** `8622f3cf` makes `placement_equivalent` compare sorted replica sets. `e6263f8a` updates the epoch-seed test oracle to treat replica sets as unordered, matching `group_of`. `8d42769b` updates the docs in `docs/algorithms.md`.
- **Tests.** New test `rehome_commit_that_only_reorders_replicas_keeps_seed_and_counts`: it fails on master and passes with the fix. A real node-set change, on 3 nodes with RF2, still invalidates. The full vals-cluster suite passes: 706 tests, 0 failed.
- **Hardware.** LONG01-D9 against D7, both with a 1 s split poll and both reaching rehome. D9 committed one rehome with `ae_seed_placement_invalidations` flat and 0 seed folds, against 121M folds and a 50–300k ACK trough on D7. D9 PASSed every correctness gate, and its settle converged in 166 s. Evidence: `bench/0297-ingest-1m/long01/FINDINGS.md`, section "Split cadence, rehome refolds and the recount prefetch".
- **Out of scope.** Real node-set changes (more nodes than RF) still refold every zone store on rehome. A range-local reprojection would be a separate design. The split cadence policy follows from 0318.

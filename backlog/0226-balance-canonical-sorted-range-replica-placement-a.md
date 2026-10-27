---
id: "0226"
title: Balance canonical sorted-range replica placement across nodes/shards (fix 0225 high-churn skew)
status: done
priority: 2
created: "2026-08-19"
updated: "2026-08-20"
depends_on:
    - "0225"
spec_refs: []
---

## Description

Task 0225 (bench/lab-sorted-gaps-20260820-0225/FINDINGS.md) diagnosed the remaining sorted reads-under-churn gap (writers=8 scalar: sorted 108–135k rd/s vs unsorted 309–362k, repeatable in both run orders) as coarse canonical sorted range-token placement: 24 ranges × RF2 = 48 replica slots land 18/18/12 on the three nodes — sorted physical writes deterministically 37.5%/37.5%/25.0% (1.50× hot/cold vs unsorted 1.03×), shard CPU max/mean 1.44–1.51 with one shard idling at 13–26% while peers run 38–48%, and sorted p99 65 ms vs unsorted 16 ms with fleet CPU far from saturated. Quiescent batched reads are already at parity (2.20M vs 2.32M keys/s), so this is specifically a high-concurrency placement bottleneck.

Make canonical sorted placement balance replica copies AND primary/value-read roles across physical nodes and their shards, instead of relying on opaque range tokens hashing evenly by chance (consider round-robin slot assignment at canonical split/re-home time; related prior art: 0155 populated-range re-home, 0182 blocked opt-in balanced re-home gate).

## Acceptance criteria

- Replica-slot distribution for a 24-canonical-range sorted zone on 3 nodes is balanced (hottest/coldest node physical-write share ≈1.0×, not 1.50×).
- Hardware gates on the 0225 corpus/shapes: writers=8 scalar sorted reads within ~1.3× of unsorted at matched churn (vs 2.3–3.3× today), and high-width batched reads under churn (B7/B8 shape) no longer nonlinear vs unsorted.
- No regression to quiescent sorted parity or writers=2 arms.

## Outcome

PD now assigns deterministic nonce-searched opaque tokens at canonical placement events (new-zone presplit, empty canonical split on V4 maps, debt re-home on V4 maps), balancing replica slots, primaries, and per-engine slots: 24 ranges × RF2 on 3 nodes/4 shards → 16/16/16 slots, 8/8/8 primaries, engine max−min ≤ 1 (vs legacy 18/18/12). No placement-function/wire changes; zero request-path cost. Fail-closed: capability certified per exact epoch tuple + leadership; initial-map baking deferred until controls settle and published==committed membership; one-way legacy downgrade; multi-voter PD proposals don't self-downgrade. Counters pd_canonical_placements_{balanced,fallback}; docs updated; reviewer accepted round 5; 704 tests pass in debug. Hardware gates split to task 0227; legacy zones (0225 smix) need zone recreation or the balance executor to converge.

Commit: pd: balance canonical sorted-range placement via searched opaque tokens (0226)

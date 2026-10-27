---
id: "0289"
title: A/B exact bounded-memory AE seeding and lifecycle reservations
status: done
priority: 2
created: "2026-09-09"
updated: "2026-09-27"
depends_on:
    - "0285"
spec_refs: []
---

## Description

Task 0285 identifies independent O(keys) restart memory: a complete sidecar plus winner/key vector and hash table; the 500k baseline peaked at ~261 B/current key above process baseline. Proposed experiment only: compare current exact AE seed with streaming or bounded partitions that avoid a corpus-sized winner directory and complete-sidecar allocation. Coordinate with proposed 0288 but do not require its prototype to study seeding. No fleet operations or irreversible format choice authorized.

Acceptance on promotion: exact digest/winner/version/tombstone equivalence across overlap, realistic rare-overwrite/delete history, reopen and interrupted/restarted seed; bounded peak allocations inclusive of serving copies, sidecars, DMA/cache and maintenance under 8-core/32GiB then 4-core/16GiB per-node ledgers. Include churn/rehome-seed and concurrent maintenance interference with explicit global reservations/debt and rebuild completion measurements; degraded target throughput may be lower but rebuild must fit the same envelope. Scaled tests must distinguish measured RSS from source-accounted bounds and cannot claim 100B hardware feasibility. Decide smallest viable approach, or reject, before adoption or a coordinated format migration.

## Outcome

Opt-in VALS_AE_SEED_MODE=stream (default stays baseline). It k-way merges windowed, CRC- and order-checked v3 sidecar streams with a sorted memtable snapshot, so there is no winner directory or complete sidecar in memory. Journal replay uses exact deltas, and the fallback stays streaming. Differential tests prove exactness against baseline. Local A/B (bench/0289-ae-seed-ab): seed RSS +291 MB baseline vs +1.8 MB stream at 1M keys, flat from 100k to 1M, ~30% faster. Decision: viable candidate, not adopted; the global reservation and fixed-RAM rebuild gate moves to 0316. Review accepted.

Commit: ae(0289): opt-in exact streaming AE seed (windowed sidecar k-way merge) + local A/B vs winner-directory seed

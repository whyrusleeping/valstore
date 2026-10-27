---
id: "0422"
title: 'Post-restart logical-accounting seed on the shard: linear two-pass min over run heads, per-key ring walk in group_of; 35-77% of restart excess, still running at +45 min'
status: done
priority: 3
created: "2026-10-11"
updated: "2026-10-13"
depends_on:
    - "0416"
spec_refs: []
---

## Description

Split from 0416 (bench/0416-late-shard-cpu/FINDINGS.md, sections 3 and "Restart control"). After a restart the logical-accounting counts are reseeded by `seed_step_stream` (crates/vals-cluster/src/replica/lifecycle.rs) streaming every run on the serving shard. In the L1 model at the 8B-equivalent state this costs +9.5k cycles/write at R+5 min, +11.0k at R+20 and +6.0k at R+45 (35% / 56% / 77% of the excess against a matched no-restart arm; R+45 is still ×1.51). Inside the seed (lbr): two linear `min()` passes over all run heads per key plus a linear equality pass (`Iterator::reduce` + memcmp, ~25%), `antientropy::entry_digest` (~17%), and `group_of` → `RangeMap::replicas_for_key_while_rehoming` → `Ring::replicas_in_zone` (a ring walk per key, ~15%), plus `range_index` per key.

Candidates: a heap k-way merge as in 0416's `winner_count_page_prefetched` fix; caching the group/range for consecutive sorted keys within a range; then measure whether the seed's duty cycle should finish sooner instead of costing less per key.

Acceptance: preregistered L1 A/B with restart arms (bench/0416-late-shard-cpu), seed cycles per seeded key and the R+20/R+45 excess, kept or removed by the 0390 rules.

## Outcome

Kept by the preregistered L1 restart A/B (bench/0422-seed-merge/FINDINGS.md). seed_step_stream now merges run heads with a BinaryHeap plus a refill list (no per-key linear min/equality passes or key copy), caches the replica group per range of the placement map that prepare_zone_seed recorded (placement_ranges; off while a regroup is active) and the logical range index, and try_group_mut borrows the group. Window-read checkpoints no longer drop the step's folds from vals_ae_seed_fold_ops (BASE arm = HEAD + that counter fix). Results at the 8B-equivalent state: seed cycles per seeded key R+5 −51/−53%, R+20 −41/−42%; seed finished R+30.9 vs R+51.2 min (busy 257 vs 477 s; same per-second duty share); shard µs/write R+0→R+60 5.752 → 4.871 (−15.3%) vs 0.009 pre-restart difference. Microbench −55…−63% ns/key at 4–40 runs. New oracle test checks staged group leaves, logical counts and fold count against a per-key placement oracle. Key-filter gap (0421) untouched.

Commit: perf(0422): restart seed heap merge and per-range group/range cache, fold counter counts checkpointed steps; L1 restart A/B: seed cycles/key −41…−53%, seed done R+31 vs R+51 min, first-hour shard µs/write −15%

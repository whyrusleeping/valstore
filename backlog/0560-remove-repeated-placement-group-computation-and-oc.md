---
id: "0560"
title: Remove repeated placement-group computation and occupied-entry boxing from replica write accounting
status: done
priority: 3
created: "2026-11-02"
updated: "2026-11-10"
depends_on: []
spec_refs: []
---

## Description

0548: accepted writes at replica/write.rs:26 and213 call group_of -> lifecycle.rs:1285-1296 placement closure plus sort; replicas_in_zone/range placement computes a salted key/ring lookup and returns allocated Vec. version_index.rs:522-525 and557-559 then groups.entry(Box::from(group)) allocates even when group exists (seeded clean-bucket exact-old path). Seed path already caches sorted range placement and removed analogous boxing in0422; normal write path still does this. Historical bench/0422-seed-merge/FINDINGS.md measured group_of10–26% and try_group_mut3–9% of SEED cycles, NOT current write-path cost. No write throughput gain established here.

Suggested acceptance: profile normal write accounting first; cheap borrowed get_mut(group) before allocating on missing group; if justified cache group per immutable placement range with invalidation across generation/regroup/pending rehome, fallback for unsorted/nonrange cases. Test against per-key placement oracle under membership/range/rehome changes. Use existing L1/microbench/hardware A/B for adoption; do not assume historical seed gains transfer to writes.

## Outcome

Sorted (range-mapped) zones: placement closure memoizes each range's replicas per installed placement (RangeMap::replicas_of_range_while_rehoming by index), so group_of drops 122->48 ns (64 ranges) and 204->83 ns (4096 ranges); in-process seeded rf3 apply_blind_at overwrite 947-977->834-876 ns and 1083-1092->932-944 ns. VersionIndex apply/retire leaf XOR looks the group up borrowed before boxing (34.5->18.3 ns). Default/unsorted zones keep per-key ring placement (only boxing gain, below L1 resolution, L1 not run). New oracle test checks group_of against per-key placement across pending/commit/membership/split changes (fails with a broken memo index). vals-cluster/vals-client lib tests and clippy pass; no NVMe hardware A/B run. Evidence: bench/0560 (local).

Commit: replica write accounting (0560): memoize range-mapped placement per range index inside each placement closure; leaf-group XOR uses borrowed get_mut before boxing a new group

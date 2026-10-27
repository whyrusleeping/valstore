---
id: "0499"
title: 'Expired tombstones are never collected on RF-3 SPDK replicas (0194 soak: 0 of 37,500 after 24 h)'
status: done
priority: 2
created: "2026-10-22"
updated: "2026-10-22"
depends_on: []
spec_refs: []
---

## Description

Found by the 0194 24 h mixed-profile soak (bench/0194-profile-soak/FINDINGS.md, evidence/24h-20261021T080223Z). With gc_grace = 900s, current per-shard GC watermarks, and about 22 k merges per node (node 1 alone: 2,766 `lonegc` merges in 3 h), not one of 37,500 winning tombstones that were more than 900 s past grace was collected on any replica. The 0194 gate requires collection, so it cannot pass until this is fixed.

Hypothesis (unproven): replica/write.rs only rejects *live* writes older than the admission cutoff, and accepts a tombstone of any age. So after one replica GCs an expired tombstone, anti-entropy from a peer that has not GC'd it yet (a peer with several runs, so no lone-run GC) copies it straight back. The repeated `lonegc` merges that keep becoming due fit this.

Acceptance criteria:
- Reproduce locally (3-node RF 3 file backend, short grace) and identify the actual cause.
- Fix it so that expired tombstones converge to collected on all replicas. Deletes inside grace must still never resurrect.
- Add a regression test, and show that the lonegc merges stop repeating once the tombstones are collected.
- Then rerun the 0194 gate (`bench/0194-profile-soak/run.sh 24h`).

## Outcome

Expired tombstones now converge to collected on every replica, and deletes inside grace still never resurrect. Found three causes on a local RF-3 file cluster: GC ran only on a lone run, anti-entropy copied collected tombstones back, and expired tombstones in the unflushed tail were never collected. All three are fixed. Locally, 0 of 384 tombstones were collected before the fix. After it, all tombstones older than the data clock minus grace are collected on all replicas, and lonegc merges stop once they are. Regression tests cover each cause, plus run sets over the input cap and the fallback when a due ordinary merge has no plan. 0500 was split out for over-cap run sets and then folded back in. The 24 h hardware gate rerun moves to 0194, because it needs this commit deployed to lab.

Commit: fix(gc): collect expired tombstones on every replica: GC across all runs, no AE re-copy, flush expired tail tombstones (0499)

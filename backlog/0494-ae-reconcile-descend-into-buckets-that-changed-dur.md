---
id: "0494"
title: 'AE reconcile: descend into buckets that changed during the tree round trip instead of retrying the level (0467 fix)'
status: done
priority: 4
created: "2026-10-21"
updated: "2026-10-26"
depends_on: []
spec_refs: []
---

## Description
Follow-up to 0467 (bench/0467-ae-token-busy/FINDINGS.md). Needs the owner's go-ahead: on real nodes the outcome is rare.

`reconcile_with` gives up with Busy after 3 tree round trips at one level if the local tree scope token was invalidated during each one. `vals_ae_tree_token_exhaustions` counts it. On the lab SPDK cluster (3 nodes × 8 shards, sweep 5 s) it cost one hot store 0.9% / 6.4% / 9.1% of its reconcile slots at 1k / 2k / 4k single writes/s. A 64-way presplit zone lost 0.84% at 2k writes/s and 0.31% at 50k msgs/s in batches of 64. In process it was 57–100%. A lost slot is retried on the next pass.

Candidate fix: after the peer `tree` reply, compute which in-scope buckets changed generation. Treat their level ids (`slot % level`) as differing and descend into them, instead of retrying. Keep the retry/Busy path only for invalidations that are not per bucket: placement generation, pending regroup, pending RT fold. Buckets that did not change compare exactly as today. Changed buckets are a superset that the next level or the digest phase checks again. So convergence should be unchanged, and the extra cost should be proportional to the writes landing during round trips.

Acceptance: `vals_ae_tree_token_exhaustions` stays near 0 under sustained writes. The failure fraction at 1k–2k writes/s per zone is measured before and after the fix, on the in-process rtqueue and on lab (bench/0467-ae-token-busy/run.sh). Kill-the-fix tests show convergence is unchanged, and the extra digest-phase work (`ae_digests_examined`) is reported.

## Acceptance criteria

## Work log

- 2026-10-26: closed as not worth fixing (owner accepted triage). On SPDK hardware tree-token exhaustion costs 1–9% of reconcile slots per hot store, and a lost slot is retried on the next pass. Reopen only if AE convergence lag becomes a measured problem.

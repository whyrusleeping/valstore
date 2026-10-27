---
id: "0231"
title: Rehome/AE range seeding monopolizes shard executor, stalling applies for ~2 min under live ingest
status: done
priority: 2
created: "2026-08-22"
updated: "2026-08-23"
depends_on: []
spec_refs: []
---

## Description

Observed during the 0230 Jetstream soak (2026-08-22, lab fresh cluster, balanced placement + auto-split + debt-rehome active): when PD debt-rehomes a populated split child (~500–600k entries), the donor shard's executor spends ~45%+ of its cycles in `vals_cluster::replica::lifecycle::seed_step` / `version_index::SeedProgress::fold_key` (plus `Ring::replicas_in_zone`/`walk` per-key), starving RPC applies on that shard for the duration of the rehome (observed prepare→commit spans up to ~100 s). Client-visible effect: 5 s RPC timeouts across any batch touching the shard, `mirror failures reported` rehome aborts + re-prepares (which extend the stall), and producer batch drops once client retry budgets exhaust. perf evidence in bench/bootes-soak-jetstream-20260822-0230/.

At steady backfill scale this fires continuously: every 1M-entry auto-split creates an inherited child that debt-rehome then moves while hot.

Acceptance criteria:
- Range seeding for rehome yields the shard executor (off-core like maintenance flush/merge, or bounded per-turn work with yields) so concurrent applies keep meeting RPC deadlines (no 5 s timeouts attributable to seeding under a repro harness).
- Rehome of a ~600k-entry range under sustained write load completes without mirror-failure aborts caused by donor-side stall.
- A/B evidence (local repro acceptable per 0223 methodology, or hardware) showing apply p99 during rehome before/after.

## Outcome

Bounded cooperative seeding: seed_step no longer folds a whole run sidecar (sync device read + per-key fold) or the whole memtable in one executor turn. Added CRC-verified SidecarVersions pager (v2/v3, resumable) + Engine::run_sidecar_versions; folds now ≤4096 records/turn, sidecar load its own turn, RAM accounting and restart semantics preserved; new ae_seed_fold_ops counter. Evidence at production budget: seed_budget.rs asserts per-turn bound + digest equality; seed_apply_ab.rs single-600k-run concurrent-apply A/B: max apply stall 105.6ms → 14.45ms (residual = one-shot sidecar load/CRC), p50 1091→481µs, seed wall parity. Reviewer affirmed code soundness; remaining end-to-end objection overruled: lab is armed for the 0232 crash capture, so hardware validation of the 5s-timeout/mirror-abort criteria is task 0233 (next soak). MAX_SEED_RESTARTS sync fallback unchanged/known.

Commit: Bound cooperative Merkle-leaf seeding per-turn work (0231): paged sidecar/memtable folds stop rehome/AE seeding from monopolizing the shard executor

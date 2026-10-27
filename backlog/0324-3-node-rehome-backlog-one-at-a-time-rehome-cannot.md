---
id: "0324"
title: '3-node rehome backlog: one-at-a-time rehome cannot clear split-created placement debt; repeated copy_stall aborts'
status: done
priority: 2
created: "2026-09-28"
updated: "2026-09-28"
depends_on: []
spec_refs: []
---

## Description

## Description
## Evidence (SOAK0321-F4 settle + re-verify, 3-node RF2, ~1.4B keys/node)

- **Placement debt at end of write.** The PD counted **3,275 of 3,390 ranges with placement debt** (`vals_pd_unrehomed_ranges`), one per inherited auto-split.
- **Rehome runs strictly one range at a time.** Each move of about 1M entries takes about 1.5–2.5 min. In 30 min of settle, 15 started, 9 committed and 5 aborted as `copy_stall` ("pending owner node N shard S reported no copy progress within 60s").
- **One range stalls repeatedly.** `df6abc32…` (zone 2) aborted three times, on target shards n1s2, n1s1 and n3s1.
- **Rehome never gets a slot during writes.** Every poll splits, so all the debt is left for after ingest. Clearing it at this rate would take days.
- **Early-run rehomes cost throughput.** Mirror Busy (`vals_rehome_mirror_busy`: 8.5k/1.9k/6.3k in F3) produces the worst 10 s windows (106–375k at t = 90–180 s).

## Questions to answer before fixing

1. **Is the debt meaningful on this topology?** Children inherit a balanced parent placement, so check the per-node/shard balance with and without rehome.
2. **Why does copy stall on `df6abc32…`?** Is it copy progress reporting, a large key or value, or contention?
3. **Should rehome throughput scale?** Options: concurrent rehomes (`VALS_PD_MAX_CONCURRENT_REHOMES`) or faster copy. Or should the debt policy change, so inherited splits of a balanced parent create no debt?

## Acceptance

Pick one of these through a measured diagnosis. With it, a 3-node D-scale or full-cache run's post-drain debt should converge, or be shown to be harmless and dropped from the gate, with no write-throughput regression.

## Acceptance criteria

## Diagnosis (offline, from retained SOAK0321-F4 evidence at /data/dataset/task0297-results/SOAK0321-F4: observer.jsonl, pd.stderr)

Q1, whether the debt is meaningful: mostly not.
- `vals_pd_node_load_skew_pct` was 101 from t = 150 s through the end of settle, while `pd_unrehomed_ranges` grew 0 → 3,282.
- Node live keys at the end were 1.358/1.418/1.424B. The ideal is 1.393B per node, so every node is within ±2.5 %.
- Inherited children keep the parent's balanced token. Their replica set and engine are unchanged, so splitting moves no load.
- `pd_unrehomed_ranges` counts ranges; it does not measure load. The only load residual seen is engine skew: `pd_balance_before_engine_pct` = 124 (one planner sample at t ≈ 4,150 s, settle) against `balance_high_pct` = 120.
- Clearing every debt range would copy about 3.3B entries, one range at a time, about 2 min each.

Q2, why copy stalls on df6abc32: the source shard's own AE reseed starves the copy scans.
- Every rehome prepare bumped `ae_seed_placement_invalidations` and `ae_seed_starts` by 1 on the source owner nodes (nodes > RF, 0322 semantics). That started a streaming reseed of the source store.
- While that reseed ran, the source's scan latency climbed: 0.04 s → 0.4 → 0.9 → 1.6 → 2.8 → 3.5 → 5.5 s … up to 12–80 s.
  - Past the copier's 5 s `NODE_PEER_RPC_DEADLINE`, each tick re-issued the same page, so there was no progress.
  - The PD then aborted with `copy_stall` after 60 s. Each abort and re-prepare invalidated the seed again.
- Once the seed finished, scans dropped at once to 0.01–0.05 s and the copy ran at about 100 pages per 10 s.
- df6abc32 (999,909 entries) aborted 5 times (targets n3s0, n1s1, n1s2, n1s1, n3s1). It was simply the largest debt candidate, re-picked each time its backoff expired.
- The same climbing phase opens nearly every settle rehome, and most of them outlast it.

Q3, what to do: change the debt policy rather than scale rehome throughput.
- Debt rehome should run only while measured load is imbalanced.
- Scaling concurrency or copy speed would spend about 3.3B entry copies to fix a range-count metric.

## Change (one mechanism): skew-gated, hot-first debt rehome (pd/range_policy.rs, rehome block)

1. In `range_stats_poll`, compute current loads with `project_current_loads(snap, &agg, &local_hash_entries, &ring, Some(&topology))`.
   - Derive the topology from `targets` (node → addrs.len() as u32), the same shard mapping `rehome_owner_shard` uses.
   - Derive node and engine skew percentages with the existing `hottest_node` / `hottest_engine` and `LoadRatio::pct`. Make the helpers `pub(super)` as needed.
   - Publish a new gauge `pd_engine_load_skew_pct`. Node skew already has `pd_node_load_skew_pct`.
2. Start a debt rehome only if node or engine skew is above `snap.balance_high_pct` (default 120).
   - Otherwise start none this poll and increment a new counter `pd_rehome_debt_deferred_balanced`.
   - If any sorted range lacks a current-epoch entry count in `agg`, loads are unmeasured. Defer that poll as well and count it the same way. Simplest acceptable: one counter.
3. When a rehome is allowed, consider only debt ranges whose current replicas or engines include the hottest over-threshold node or engine, largest entries first. If none touch either hot target, defer and count it.
   - Keep the backoff, the per-zone actioned rules, the max-concurrent rules and the target-token selection unchanged.
4. Debt bits, map formats, routing and the rehome protocol are unchanged. `pd_unrehomed_ranges` stays as a structural count.
5. Tests in `pd/tests.rs`:
   - A balanced multi-range map with inherited debt issues no `PrepareRehome` and increments the counter.
   - A skewed map (e.g. a single parent split on one engine) still rehomes, and picks the debt range on the hot engine over a larger one elsewhere.
   - Adjust existing rehome tests only where they relied on rehoming a balanced layout; e.g. make their layout skewed, rather than weakening their assertions.
   - Run the full vals-cluster suite in debug.
6. Docs:
   - `docs/zones.md`, the automatic re-home paragraph: debt rehome is gated on load skew above `balance_high_pct`, hot-first; the new metrics; `pd_unrehomed_ranges` is not a convergence gate.
   - A short "0324 diagnosis" section in `bench/0321-long-soak/FINDINGS.md` with the numbers above.

## Follow-ups (new tasks, not in this change)
- Hardware confirmation on a 3-node D-scale run: no settle reshuffle when skew ≤ high; engine skew converges; write throughput not below the reference.
- Proposed: a rehome prepare must not starve its own copy source. Candidates: the seed yields to rehome scans, a range-local reprojection, or no source-store invalidation on prepare.

### Starting points
- crates/vals-cluster/src/pd/range_policy.rs:456-504 rehome candidate block; :128-159 agg/ring/publish_range_metrics/balance_dry_run
- crates/vals-cluster/src/pd/balance.rs:111 project_current_loads, :343 hottest_node, :350 hottest_engine, :62-100 LoadRatio, :1302 node_load_skew_pct, :1316 publish_range_metrics
- crates/vals-cluster/src/pd/range_policy.rs:3 rehome_owner_shard(targets,node,token) = shard mapping from targets
- crates/vals-client/src/observe.rs pd_node_load_skew_pct / pd_rehomes_started for adding gauge+counter
- docs/zones.md ~line 234 'Automatic re-home is controlled by VALS_PD_REHOME_ENABLED'

## Outcome

Diagnosis from the retained SOAK0321-F4 data: - **Inherited debt is not load.** Node skew stayed at 101% while debt grew to 3,282 ranges; engine skew was 124% at settle. - **Copy stalls:** each rehome prepare on nodes > RF invalidates the source store's AE seed. The reseed that follows pushes source scans past the copier's 5 s peer deadline, so the copy makes no progress and the PD aborts it after 60 s with copy_stall. Fix: debt rehome runs only when load is measured and node or engine skew exceeds `balance_high_pct`. It moves only debt ranges touching the hottest node or engine; otherwise it defers, counted by `pd_rehome_debt_deferred_balanced`. New `pd_engine_load_skew_pct` gauge; `pd_unrehomed_ranges` is documented as structural. Tests pass; reviewer accepted. Hardware confirmation is 0332; the copy-livelock fix is 0333 (proposed).

Commit: pd(0324): skew-gated, hot-first placement-debt rehome; engine skew gauge + deferral counter; SOAK0321-F4 debt/copy-stall diagnosis

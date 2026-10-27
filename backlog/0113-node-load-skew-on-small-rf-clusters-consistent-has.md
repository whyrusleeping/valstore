---
id: "0113"
title: Node load skew on small RF clusters (consistent-hash imbalance + vnodes default 64 vs 128)
status: done
priority: 3
created: "2026-08-01"
updated: "2026-08-01"
depends_on: []
spec_refs: []
---

## Description
## Context

Observed during the 2026-08-01 bootes soak on the 3-node lab SPDK cluster (RF 2): node 1 carries ~40% more data than nodes 2/3 and is the RAM/capacity leader. At ~115M records cluster-wide: node1 `records_written`=114.7M / `used_segments`=100k / RAM 11.1G, vs node2 82.9M/75k/10.1G and node3 80.7M/69k/8.6G — a persistent ~1.4× ratio. Early in the run it was ~1.7× and settled toward ~1.4× as ranges multiplied (175 ranges, auto-split healthy).

## This is NOT a correctness bug
Placement is deterministic consistent hashing (`ring.rs::walk` — clockwise from the token hash, first `rf` distinct nodes; ring points from `ln_pos(node,i)` = hash of node-id+vnode-index). No node-id bias. Every key lands on its correct rf=2 distinct replicas; quorum reads/writes are correct. `pd_range_skew_pct` (max_range/mean_range within a zone) reflects split *churn*, not node load.

## The actual issue: structural node load imbalance
node1's 64 vnodes happen to cover ~40% more of the token space (as primary-or-first-replica under RF2) than nodes 2/3 — the classic consistent-hashing imbalance at **small node count**, amplified by **RF2 over just 3 nodes** and a **modest vnode count**. Key property: this converges to the ring's fixed arc-length imbalance and does **not** smooth out with more ranges (range-count balance ≠ node-load balance). It matters because on a small RF cluster the usable capacity is bounded by the hottest node — node1 will hit the RAM/disk ceiling first (this is adjacent to the 0107 OOM: worst-case a hot node fills while others have headroom).

## Concrete finding worth fixing regardless
**Defaults mismatch:** `PdConfig`/`NodeConfig` default `vnodes = 64` (crates/vals-server/src/config.rs ~L563), but the ring library `DEFAULT_VNODES = 128` (crates/vals-client/src/ring.rs:32). Deployed clusters therefore run 64. More vnodes → smoother arcs → lower skew; reconcile the two (and consider that 64 is low for small clusters where imbalance hurts most).

## Questions to work through
- Quantify: for N=3, RF=2, what node-load imbalance should we expect at vnodes ∈ {64,128,256,512}? Is bumping the default enough, or is the RF2/N=3 amplification the dominant term?
- Should the ring be capacity/assignment-aware (bounded-load / consistent-hashing-with-bounded-loads, or explicit range→replica-set assignment that equalizes owned entries) rather than pure hash walk?
- Interaction with auto-split: when a hot range splits, do children get placed to *relieve* the hot node, or do they inherit near-parent ring positions and keep the skew? (Determines whether splitting can be made balance-aware.)
- Operational: expose a per-node owned-entries / capacity metric and a skew alert; decide an acceptable skew SLO for small clusters.

## Outcome
A decision + doc note on load-balance posture for small RF clusters, at minimum reconciling the vnodes default; larger rebalancing/bounded-load work filed as follow-ups if warranted.

## Acceptance
- [ ] vnodes default mismatch reconciled (or explicitly documented why they differ)
- [ ] Expected node-load imbalance vs vnodes/N/RF quantified and written up
- [ ] Decision on whether to pursue balance-aware placement/rebalancing (filed as follow-up task(s) if yes)
- [ ] A per-node load/skew observability surface identified or added

## Acceptance criteria

## Plan

Deliver the decision + doc note + the concrete vnodes-default reconcile; file the larger balance-aware and observability work as follow-ups. Scope is deliberately tight (one commit): a config default change + docs + a written quantification.

## 1. Reconcile the vnodes default (code)
The ring library (`ring.rs:32 DEFAULT_VNODES=128`), `RingConfig::default` (pd.rs:51) and docs/algorithms.md already say **128**; only the PD file-config parser defaults to 64 (`config.rs:633`), so file-configured clusters silently ran 64. Align the parser to 128:
- `crates/vals-server/src/config.rs:633`: change `None => 64` to use the ring constant (e.g. `vals_cluster::ring::DEFAULT_VNODES` — pick whatever import path config.rs already uses for ring types; if awkward, use the literal `128` with a comment pointing at `ring::DEFAULT_VNODES`). Keeping it tied to the constant is preferred so they can never drift again.
- Update the doc-comment `config.rs:563` `(\`rf = 3\`, \`vnodes = 64\`)` → `vnodes = 128`.
- Leave the parser TEST at config.rs:831/845 (explicit `vnodes = 32`) unchanged.

Update the two docs that show 64 as the default/example:
- `docs/deploy.md:137` `vnodes    = 64` → `128`.
- `docs/getting-started.md:169` `RingConfig { vnodes: 64, rf: 3 }` → `128`.

## 2. Write up the quantification + decision (docs/algorithms.md)
Add a new subsection **"### Load balance on small clusters"** under §1 Placement (after the "Zone salting" note, before "## 2. Placement — sorted-zone range tokens"). Content, grounded in a Monte-Carlo simulation over the *exact production hash* (FNV-1a + SplitMix64), RF=2, N=3, averaged over random cluster node-id draws, ~4–8k keys/trial, ~120–200 trials:

Per-node **owned-entries** skew (hottest/coldest = max/min ratio):

| vnodes | mean | p90 | worst (of 200) |
|---|---|---|---|
| 64  | 1.16 | 1.27 | 1.43 |
| 128 | 1.11 | 1.21 | 1.32 |
| 256 | 1.09 | 1.16 | 1.25 |
| 512 | 1.06 | 1.11 | 1.18 |

Key points to state:
- The observed lab soak (~1.4× hottest/coldest at vnodes=64) is a ~p95+ unlucky ring draw, fully consistent with theory — **not** a bug. Placement stays deterministic, every key on its correct RF distinct replicas.
- This is the ring's **fixed arc-length imbalance**: it does **not** smooth out with more keys or more ranges (range-count balance ≠ node-load balance). More keys only reduce sampling noise around the structural ratio.
- The only pure-hash lever is vnode count: load CoV ∝ **1/√vnodes**, so doubling 64→128 cuts excess skew ~30% (√2); halving it needs ×4 vnodes (256). Cost is trivial — the ring vector is N×vnodes entries (tens of thousands even for hundreds of nodes), rebuilt on membership change only.
- RF>1 does not amplify store skew — averaging a key over its RF replica arcs slightly *reduces* per-node store CoV vs primary-only. The dominant term is small **N** (few samples → fatter tails); at vnodes=64 the mean hottest/coldest grows 1.16 (N=3) → 1.29 (N=8) in the max/min tail. So bumping vnodes is genuinely effective and the "RF2/N3 amplification" is really small-sample tail risk.
- **Operational levers today:** (a) the bumped 128 default; (b) capacity **weights** already available (a node advertises `weight`, gets `weight×vnodes` points — see multi-drive.md) to hand-correct a persistently hot node; (c) raise `vnodes` at *cluster bootstrap* only — note explicitly that changing vnodes on a live cluster reshuffles all placement (it is PD-authoritative + epoch-checked), so it is a bootstrap-time knob, not a live retune.
- **Decision:** reconcile to 128 now (free, ~30% skew cut). Do **not** pursue bounded-load / explicit balance-aware assignment yet — it trades away pure-hash determinism and minimal-disruption for assignment state + migration machinery; deferred to a follow-up to revisit if a small-cluster deployment actually hits the hottest-node capacity ceiling. Recommended observability: a PD-side `pd_node_load_skew_pct` (max/mean of per-node owned entries, derivable from the per-range entry estimates the PD already aggregates × the ring's range→replica-set mapping) — filed as a follow-up.

Keep the algorithms.md:321 defaults table (already 128) consistent.

## 3. File follow-ups (create_task)
- **todo** (accepted split-off): "PD per-node load-skew gauge (`pd_node_load_skew_pct`): map aggregated per-(zone,range) entry estimates through the ring to per-node owned entries; publish max/mean skew + optional warn." Reference the aggregation in `pd.rs` (~L940 publish path, `agg`/`RangeMap`) and observe gauges.
- **proposed** (speculative redesign): "Balance-aware / bounded-load placement for small RF clusters — evaluate consistent-hashing-with-bounded-loads or explicit range→replica-set assignment that equalizes owned entries; interaction with auto-split (do hot-range children relieve the hot node?)."

## 4. Verify
`cargo build -p vals-server` and run the config parser test (`cargo test -p vals-server config`) to confirm the default change compiles and the explicit-vnodes test still passes. No behavior change for clusters that set `vnodes` explicitly.

### Starting points
- crates/vals-server/src/config.rs:633 (vnodes default 64)
- crates/vals-server/src/config.rs:563 (doc-comment)
- crates/vals-client/src/ring.rs:32 (DEFAULT_VNODES=128)
- crates/vals-cluster/src/pd.rs:51 (RingConfig::default uses DEFAULT_VNODES)
- crates/vals-cluster/src/lib.rs:106 (pub use vals_client::ring)
- docs/algorithms.md §1 Placement (~L38-82) and defaults table ~L321
- docs/deploy.md:137
- docs/getting-started.md:169

## Work log
- 2026-08-01 plan: Deliver the decision + doc note + the concrete vnodes-default reconcile; file the larger balance-aware and observability work as follow-ups. Scope is deliberately tight (one commit): a config default 
…[truncated]
- 2026-08-01 context hints: 8 recorded with plan
- 2026-08-01 implementer report: Implemented task 0113’s code and documentation scope.  Changes: - Reconciled the PD file-config default with `vals_cluster::ring::DEFAULT_VNODES` (128), eliminating future drift. - Updated the `PdCo
…[truncated]
- 2026-08-01 review tier: single-opus — reviewers: sol
- 2026-08-01 review (sol): revise — The vnode default is correctly tied to `DEFAULT_VNODES`, the documentation/examples are updated, and the load-skew quantification and placement decision are written up as requested. However, the imple
…[truncated]
- 2026-08-01 revision: Added the requested upgrade caveat to `docs/algorithms.md`: existing clusters whose PD config omits `vnodes` will switch from the old implicit 64 to 128 on the next upgraded PD boot, reshuffling place
…[truncated]
- 2026-08-01 review (sol): accept — The revision resolves the prior completeness issue by adding the requested todo observability task (0119) and proposed balance-aware placement task (0120). The implementation now reconciles the file-c
…[truncated]
- 2026-08-01 decision: accept — commit: Reconcile vnodes default (64→128) and document small-cluster load skew  The PD file-config parser defaulted vnodes to 64 while the ring library (DEFAULT_VNODES=128), RingConfig::default, and docs al
…[truncated]
- 2026-08-01 usage: 1,562,651 tok (in 399,402, out 26,097, cache_r 1,952,515, cache_w 28,569) · cost n/a (unpriced)
  reviewer:sol: 1,156,229 tok (in 264,065, out 13,060, cache_r 879,104, cache_w 0) · cost n/a (unpriced)
  implementer: 397,353 tok (in 135,313, out 3,992, cache_r 258,048, cache_w 0) · cost n/a (unpriced)
  coordinator: 9,069 tok (in 24, out 9,045, cache_r 815,363, cache_w 28,569) · cost n/a (unpriced)

---
id: "0029"
title: 'Sorted zones are served single-core per node: shard sorted-zone serving by range token'
status: done
priority: 3
created: "2026-07-02"
updated: "2026-07-04"
depends_on: []
spec_refs: []
---

## Description
A sorted zone is served unsharded on each node (one engine, one serving core) — the range map is the zone's *cross-node* scale-out axis, but within a node every range it owns funnels through one core (`zones.md` "sorted zones are served unsharded"). The 2026-07 scan pass showed a scan-saturated serving core; point-read and write throughput for sorted zones has the same per-node ceiling, while the unsorted default zone already scales with SHARDS.

The client transport already routes scans/writes for a sorted zone to "the shard its range token hashes to" (`NetTransport::scan_shard`), so the wire/client side anticipates this. The server side needs: per-shard sorted engines keyed by range token (or a range→shard map versioned with the range map), shard-local flush/compaction, and AE that understands the split. Alternatively: keep one engine but run multiple serving cores over disjoint ranges of it (needs engine-level concurrency — much bigger).

Acceptance: an SPDK node with SHARDS=k serves a sorted zone's scans/point ops at ~k× the single-core rate on disjoint ranges; distributed scan_in spreads across shards.

## Acceptance criteria

## Plan

## Goal

Serve **sorted zones on the sharded (multidrive/JBOD) path**, sharded intra-node by range token — removing the `refuse_sorted` restriction — with per-shard sorted engines, shard-local flush/compaction (already per-shard by construction), and anti-entropy/cleanup that understands the split so a runtime range-map split migrates keys to the newly-owning shard instead of stranding them.

Hardware (~k× throughput on the SPDK box) acceptance cannot be run in this session; deliver the mechanism + off-hardware tests, and file a follow-on box-validation task (same convention as tasks 0041/0042).

## Why this design is correct

- Shard assignment for a sorted zone is `hash64(range_token(key)) % shard_count` — exactly the client's `NetTransport::data_shard`/`scan_shard` routing, and shard count is uniform cluster-wide. Under a stable range map, shard `s` on every node holds the same key slice, so the existing per-shard AE (shard `s` ↔ peer shard `s`) stays valid, and a scan (clipped to one range by the coordinator) is complete on the one shard its token routes to.
- A runtime split of `[s0,s1)` at `m` leaves `[s0,m)` in place and gives `[m,s1)` a new token `m` → those keys may now belong to a different shard (and possibly different ring nodes). No peer's shard `t` has the data, so pull-based reconcile can't heal it; migration must be **push-based via the existing over-replication cleanup**, made shard-aware: a sorted key is "still mine" only if `owners.contains(local_node) && hash64(token) % shard_count == my_shard_index`; otherwise it is handed off (confirm-then-forget, existing safety) via a transport that routes `apply` to the key's correct shard — including this node's own sibling shard (self as a loopback peer).
- Reconcile's `keep` predicate gains the same shard filter for sorted zones so a shard never re-adopts keys that now belong to a sibling shard from a laggard peer (avoids adopt/cleanup churn). The AE Merkle leaf *grouping* stays shard-agnostic — both sides of an `s↔s` pair group identically, so trees stay symmetric mid-migration and converge.
- A `SetRangeMaps` push must actually trigger a cleanup pass + AE leaf re-seed: fold `range_map_epoch` into the sweep's `cleanup_cfg`/placement fingerprint (today it only kicks an eager reconcile — a latent gap even on the single-engine path, since a split changes placement).

## Changes (all in vals-cluster unless noted)

1. **`net/server.rs` (`ServerShared`)**: remove `refuse_sorted` + `refuse_sorted_zones()` and the refusal in zone materialization; add shard identity `(shard_index, shard_count)` (default `(0,1)`), builder e.g. `with_shard(index, count)`, accessor for the sweep. Sorted zones now materialize like any other zone on all paths.
2. **`node.rs` serve paths**: `serve_multidrive_zoned{,_with_liveness}` call `.with_shard(s, shards.len())` instead of `.refuse_sorted_zones()`. Thread what the sweep needs through `spawn_shard_at` → `sweep_loop`: the node's **own shard addresses** (from the listeners' `local_addr()`) and each peer's **full shard address list** (`peer_shards` is already full; today it's reduced to the same-index addr).
3. **`node.rs` `sweep_loop`**:
   - Keep the existing same-index-shard transport for reconcile/watermark (unchanged).
   - Build a second **handoff transport** (`NetTransport::connect_sharded`) with full per-peer shard lists plus, when sharded, self's shard addrs under the node's own id; give it the RPC deadline; each pass: `set_route_epoch`, `set_sorted_ranges(shared.range_maps clone)`, and add newly-learned members with their full address-book lists (`peer_addrs_of`). Use it for the `cleanup_scoped{,_zone}` calls (routing is equivalent for default/unsorted zones: `hash(key)%k` lands on the same shard index as today).
   - Fold `shared.range_map_epoch()` into `cleanup_cfg` (4-tuple) so splits trigger cleanup + leaf re-seed.
   - Pass shard identity into the sorted-zone reconcile/cleanup calls.
4. **`antientropy.rs`**: `reconcile_scoped_zone` and `cleanup_scoped_zone` gain shard identity (applied only when `range_map` is `Some` and count > 1): reconcile `keep` requires the token's shard == local shard; `cleanup_with` keep-predicate generalized so "not mine" includes "wrong shard" (handoff then routes via the transport's `data_shard`, which needs the sorted ranges installed — hence 3). Use the exact same `hash64` as the client (`vals_client::ring::hash64`, re-exported as `crate::ring::hash64`).
5. **`vals-server/src/run_node.rs`**: update comments + log lines (sorted zones now served on the JBOD path; keep the single-drive `serve_spdk_zoned` fast path as-is).
6. **Docs**: `docs/zones.md` ("sorted zones are served unsharded" simplification note → describe range-token sharding), `docs/multi-drive.md`, doc comments on `serve_multidrive_zoned`, ARCHITECTURE.md/BACKLOG.md mentions if they state the restriction.
7. **Tests** (mem-backed, no hardware):
   - Rewrite the node.rs multidrive test that currently asserts the sorted-zone *refusal* (~line 3619): sorted zone now serves on a k-shard `serve_multidrive_zoned` node — writes via a sharded coordinator across ranges (with a split map installed), reads back, distributed `scan_in` complete + ordered, per-shard raw scans show token-based placement, restart/reopen recovers.
   - **Split-migration test**: k-shard node (MemDriveSet), write keys under the initial map, push `SetRangeMaps` with a split whose new token hashes to a different shard, kick/await the sweep, assert: the moved keys are served/scannable at the new shard, the old shard released them (raw per-shard scan), coordinator reads/scans stay complete throughout, nothing lost.
   - Unit test in antientropy.rs for the shard-filtered `keep` (no sibling-shard adoption) and shard-aware cleanup ownership.
   - `cargo build && cargo test` workspace green; `cargo check --features spdk` type-check.

## Out of scope / follow-ons
- SPDK-box performance validation (~k× scaling) — new blocked backlog task.
- Per-shard AE for anything node-level today (watermark gossip stays shard-level as-is).

### Starting points
- crates/vals-cluster/src/net/server.rs: ServerShared { refuse_sorted, range_maps, range_map_epoch, peer_addrs_of }, refuse_sorted_zones() ~l.509, materialize refusal ~l.958, SetRangeMaps handler ~l.2632
- crates/vals-cluster/src/node.rs: serve_multidrive_zoned{,_with_liveness} ~l.523/583, spawn_shard_at ~l.757, sweep_loop ~l.1837 (cleanup_cfg ~l.1890, cleanup calls ~l.2188), multidrive sorted-refusal test ~l.3540-3670
- crates/vals-cluster/src/antientropy.rs: reconcile_scoped_zone ~l.307, cleanup_with ~l.576, cleanup_scoped_zone ~l.712
- crates/vals-client/src/net/client.rs: data_shard ~l.643, scan_shard ~l.660, connect_sharded ~l.521, Transport::set_sorted_ranges ~l.806, apply routes via data_shard ~l.821
- client shard fn: (hash64(range_token(key)) % shards as u64) as usize — server must match exactly (crate::ring::hash64 re-exports vals_client::ring::hash64)
- tests: crates/vals-cluster/src/net/tests.rs spawn_sorted_shards ~l.1715 (client-routing tests), node.rs MemDriveSet/SharedDs/bind_n/md_geom helpers used by the multidrive tests
- docs/zones.md l.16 ('sorted zones are served unsharded'), docs/multi-drive.md, crates/vals-server/src/run_node.rs l.153-260 (JBOD path comments/logs)

## Work log
- 2026-07-04 plan: ## Goal  Serve **sorted zones on the sharded (multidrive/JBOD) path**, sharded intra-node by range token — removing the `refuse_sorted` restriction — with per-shard sorted engines, shard-local flu
…[truncated]
- 2026-07-04 context hints: 7 recorded with plan
- 2026-07-04 context hints: crates/vals-cluster/src/net/server.rs: ServerShared { refuse_sorted, range_maps, range_map_epoch, peer_addrs_of }, refuse_sorted_zones() ~l.509, materialize refusal ~l.958, SetRangeMaps handler ~l.263
…[truncated]
- 2026-07-04 implementer report: ## Summary  Implemented **sorted zones served on the sharded (multi-drive/JBOD) path, sharded intra-node by range token** — removing the `refuse_sorted` restriction — with a shard-aware anti-entro
…[truncated]
- 2026-07-04 review tier: high-powered — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change correctly and completely implements sorted-zone serving on the sharded (multi-drive/JBOD) path, sharded intra-node by range token. It removes the refuse_sorted restriction, adds a (shard_in
…[truncated]
- 2026-07-04 decision: accept — commit: feat(cluster): serve sorted zones on the sharded (multi-drive/JBOD) path, sharded intra-node by range token — shard-aware AE reconcile/cleanup (self-loopback sibling-shard handoff, range-map epoch i
…[truncated]
- 2026-07-04 usage: 165,358 tok (in 588, out 164,770, cache_r 42,852,513, cache_w 472,790) · cost n/a (unpriced)
  implementer: 145,444 tok (in 504, out 144,940, cache_r 39,938,791, cache_w 273,939) · cost n/a (unpriced)
  coordinator: 11,787 tok (in 36, out 11,751, cache_r 2,200,145, cache_w 149,353) · cost n/a (unpriced)
  reviewer:Claude: 8,127 tok (in 48, out 8,079, cache_r 713,577, cache_w 49,498) · cost n/a (unpriced)

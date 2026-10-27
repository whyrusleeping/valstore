---
id: "0080"
title: AE/handoff/cleanup reachability for shard>0 vs fewer-sharded peers (asymmetric shard counts)
status: done
priority: 3
created: "2026-07-06"
updated: "2026-07-06"
depends_on:
    - "0079"
spec_refs:
    - docs/multi-drive.md
---

## Description
Split off from task 0079 (JBOD shard>0 born-fence fix). The 0079 fix gave shard>0 a **watermark-only** fallback route to fewer-sharded peers; anti-entropy / handoff / cleanup for shard>0 remain **unreachable** against peers with fewer shards (same index-aligned addressing), deliberately — see analysis below.

## Analysis (from 0079)
- The sweep's reconcile transport talks same-index (shard s ↔ peer shard s). A shard>0 with a fewer-sharded peer has no counterpart address, so AE/handoff/cleanup against that peer fail `Unavailable` every pass (`ae_sync_errors` accrues; heal happens only via other same-or-more-sharded replicas, if any).
- A naive address fallback (e.g. peer's shard-0) is **wrong for AE**: reconciling shard>0's store against a fewer-sharded peer's *whole* store via Merkle tree-diff would surface groups whose keys belong on *sibling* shards of this node and pull them into the wrong shard (mis-sharding: wasted space, AE churn, possible cleanup fights).
- Concrete consequence today: a **wiped** shard>0 (beyond-grace rejoin) whose only peers are fewer-sharded can never complete its post-wipe refill (`refill_ok` never proves a pull), so it stays fenced indefinitely — safe but unavailable.
- Design question is placement-level: should AE between asymmetric shard counts scope tree-diff/pulls by the *local* shard's key ownership (hash/range-token filter on the peer serve or the puller), or should asymmetric-shard topologies be handled at placement (e.g. peer shard sets in the ring / range map) instead of transport addressing?

## Acceptance criteria
- A design decision recorded (docs/multi-drive.md or a design note) for AE/handoff/cleanup across asymmetric shard counts.
- Shard>0 of a multi-shard node can heal (AE pull) from and hand off to fewer-sharded peers without mis-sharding keys onto wrong sibling shards, OR the topology is explicitly rejected/unsupported with a loud, documented refusal.
- Off-hardware test: asymmetric topology (2-shard node + 1-shard peer) where shard-1 data is deleted/lost on the JBOD node → AE refills it from the single-shard peer onto the correct shard (and no sibling-shard keys are pulled).
- The 0079 regression test and existing liveness/AE tests stay green.

## Plan

## Design decision (to be recorded in docs/multi-drive.md)

**Support asymmetric shard counts at the transport/sweep level via congruence routing + puller-side shard scoping; no placement-level change, no AE wire-protocol change.**

Rationale & mechanism:
- A node's intra-node shard for a key is `hash64(shard_key) % k` where `shard_key` = key (default/unsorted zones) or `range_token(key)` (sorted zones) — exactly the client's `shard_of`/`data_shard`.
- If our node has `k` shards and a peer has `k'`, a key of our shard `s` can live only on peer shards `i` with `i ≡ s (mod gcd(k, k'))` (CRT: `h ≡ s (mod k)` and `i = h mod k'` imply `i ≡ s (mod g)`, and every such `i` is realizable). So reconciling shard `s` against exactly that congruent subset of the peer's shard addresses is **complete** (covers every key of ours the peer can hold, for every (k,k') pair — divisibility not required) and **minimal** (symmetric k=k' degenerates to today's same-index `i = s`; k'=1 gives `i = 0`).
- Correctness against mis-sharding does NOT rely on the peer: the puller's `keep` filter must also enforce local shard ownership, so digests/values for sibling-shard keys are dropped value-free (the `keep` check in `reconcile_with` runs before the values fetch).
- Cost note (document it): an asymmetric pair's Merkle trees never match at the root (the peer's fold includes sibling-slice keys), so a *converged* asymmetric pair still walks budgeted digest pages each pass (`AE_RECONCILE_MAX_DIGESTS_PER_PASS` + resume cursor) instead of matching at the root. Symmetric pairs keep the O(root) fast path. That is the accepted price of heterogeneous shard counts.
- This subsumes the task-0079 watermark-only fallback: the reconcile transport now has a real (congruent) route to fewer-sharded peers, so the `wm_fallback` transport is removed and watermark gossip uses the main transport (any shard answers `GetWatermark`).
- Handoff/cleanup already route by the *peer's own* shard count (`connect_sharded` + `data_shard` on the full address list), so shard>0 → fewer-sharded-peer handoff is already reachable; the addition is the shard-ownership component in cleanup's `is_mine` for unsorted zones (mirroring the existing sorted-zone check) so a mis-sharded key is handed to the right shard (incl. loopback sibling) and forgotten, instead of kept.

## Implementation steps

1. **vals-client `NetPeer` shard pinning** (`crates/vals-client/src/net/client.rs`): `NetPeer` currently hardcodes `rpc(node, 0, ...)`. Add a shard field + constructor (e.g. `NetTransport::peer_shard(node, shard)`; keep `peer(node)` = shard 0) and a public way to ask how many addresses a node's entry has (there is a private `shard_count`). No wire change.

2. **Congruent transport install in the sweep** (`crates/vals-cluster/src/node.rs`, `sweep_loop`):
   - Add a small `gcd` helper; compute, per peer with full address list `addrs` (from the shared address book `peer_addrs_of` or static `peer_shards`), the congruent subset `[addrs[i] for i in 0..k' if i % g == s % g]`, `g = gcd(k, k')`, and install THAT as the peer's entry in the reconcile transport — both for the statically-known peers (today `peers_s` filters `peer_shards` to index-aligned only, so a fewer-sharded static peer is absent for shard>0; build the reconcile transport's entries from `peer_shards` congruently instead) and for members learned later in the install loop (which today falls back to the watermark-only route).
   - Delete the `wm_fallback` transport and its install/use; the watermark loop uses `transport.watermark(n)` (entry's first shard). Update the 0079 comment block to describe the congruent route.
   - Reconcile loop: iterate every shard of each peer's entry (`peer_shard(n, i)`), with the AE resume cursor keyed by `(zone_id, peer, i)` instead of `(zone_id, peer)`. `zone_pulled` semantics unchanged (any successful reconcile counts — so a wiped shard>0's post-wipe refill now completes and unfences).

3. **Puller-side shard `keep` filter** (`crates/vals-cluster/src/antientropy.rs`):
   - `reconcile_scoped` (default zone): take the `shard: (usize, usize)` param (callers on unsharded paths pass `(0,1)`) and add `shard_count > 1 → hash64(key) % shard_count == shard_index` to the keep closure.
   - `reconcile_scoped_zone`: extend the existing shard check (today gated on `range_map.is_some()`) to hash zones: sorted uses `hash64(range_token)`, unsorted uses `hash64(key)`.
   - `cleanup_scoped` / `cleanup_scoped_zone`: mirror the same shard-ownership component in `is_mine` for the default/unsorted cases (sorted already has it), so a wrong-shard key is handed off (transport already routes to the correct shard, incl. this node's loopback sibling) and forgotten rather than kept. Update doc comments.

4. **Docs** (`docs/multi-drive.md`): replace the "AE/handoff/cleanup reachability ... tracked as a separate follow-up" text (~lines 30–39) with the recorded decision above (congruence routing, puller filter, subsumed 0079 fallback, cost note, and the note that the reverse direction — a fewer-sharded node healing from a more-sharded peer — is covered by the same congruent set).

5. **Tests**:
   - Unit test for the congruent-subset computation over (k,k') ∈ {(2,1),(4,2),(3,2),(2,2),(1,2),(1,1)}.
   - In-process reconcile test mirroring `sorted_zone_reconcile_does_not_adopt_sibling_shard_keys`: default-zone reconcile with `shard=(1,2)` against a peer holding both slices adopts ONLY `hash64(key)%2==1` keys.
   - The acceptance integration test (model on `multidrive_liveness_shard_gt0_unfences_against_single_shard_peers`, node.rs ~5229): 2-shard JBOD node + single-shard peer, RF=2, liveness on; write keys landing on both shards with quorum 2; restart/boot node 1 with a **stale** (beyond-grace) watermark so it wipes and must refill via AE from the single-shard peer; assert (a) both shards unfence and serve their keys (shard-1 read at `addrs1[1]` succeeds — pre-fix this can never happen), (b) no mis-sharding: a shard-0 key read directly at shard 1's address returns None.
   - Keep green: the 0079 regression test, symmetric multidrive tests, existing AE/cleanup tests (`cargo test -p vals-cluster`, plus a full `cargo test` sanity pass).

## Out of scope (noted, not implemented here)
- The unsharded serve path's cleanup/handoff transport uses a single address per peer (`peers` → `vec![a]`), so a 1-shard node's *handoff* to a more-sharded peer can land on the peer's shard 0; if not trivially fixable by building the handoff transport from `peer_shards` unconditionally, file a follow-on task instead of expanding this change.

### Starting points
- crates/vals-cluster/src/node.rs:2588-2900 — sweep_loop: transports, wm_fallback (task 0079), member address install, netpeers, reconcile+cleanup loop, ae_cursors
- crates/vals-cluster/src/node.rs:607-960 — serve_sharded paths build peers_s by index-aligned filter of peer_shards
- crates/vals-cluster/src/antientropy.rs:273-367 — reconcile_scoped / reconcile_scoped_zone keep closures (sorted-zone shard filter at ~355)
- crates/vals-cluster/src/antientropy.rs:820-900 — cleanup_scoped / cleanup_scoped_zone is_mine (sorted shard check ~886)
- crates/vals-cluster/src/antientropy.rs:451-550 — reconcile_with: keep applied before value fetch (sibling digests dropped value-free)
- crates/vals-client/src/net/client.rs:724-900 — shard_of/data_shard/shard_count, peer(), NetPeer rpc(node, 0, ...)
- crates/vals-cluster/src/node.rs:5229 — 0079 regression test to model the new integration test on (uses MemDriveSet, FileLivenessLog, serve_multidrive_with_liveness)
- docs/multi-drive.md:20-40 — 0079 paragraph to replace with the design decision
- shard routing and AE bucketing both use hash64: bucket_of=hash64(key)%B (antientropy.rs:144), shard=hash64(key or range_token)%k (client.rs:727,748)

## Work log
- 2026-07-06 plan: ## Design decision (to be recorded in docs/multi-drive.md)  **Support asymmetric shard counts at the transport/sweep level via congruence routing + puller-side shard scoping; no placement-level change
…[truncated]
- 2026-07-06 context hints: 9 recorded with plan
- 2026-07-06 context hints: crates/vals-cluster/src/node.rs:2588-2900 — sweep_loop: transports, wm_fallback (task 0079), member address install loop, netpeers, watermark gossip, reconcile+cleanup loop, ae_cursors; crates/vals-
…[truncated]
- 2026-07-06 implementer report: Implemented congruence routing + puller-side shard scoping so shard>0 of a multi-shard node can AE/handoff/cleanup against fewer-sharded peers without mis-sharding.  ## Changes  **crates/vals-client/s
…[truncated]
- 2026-07-06 review tier: high-powered — reviewers: Claude
- 2026-07-06 review (Claude): accept — The change implements congruence routing (peer shards i ≡ s mod gcd(k,k')) plus a puller-side shard `keep` filter and mirrored cleanup `is_mine` shard-ownership, letting a JBOD shard>0 heal from / h
…[truncated]
- 2026-07-06 decision: accept — commit: cluster: AE/handoff/cleanup across asymmetric shard counts via congruence routing + puller-side shard scoping — shard s of k reconciles against peer shards i ≡ s (mod gcd(k,k')), keep/is_mine enfo
…[truncated]
- 2026-07-06 usage: 92,841 tok (in 306, out 92,535, cache_r 13,294,999, cache_w 311,495) · cost n/a (unpriced)
  implementer: 70,186 tok (in 222, out 69,964, cache_r 11,303,539, cache_w 155,296) · cost n/a (unpriced)
  reviewer:Claude: 12,351 tok (in 62, out 12,289, cache_r 1,064,297, cache_w 50,765) · cost n/a (unpriced)
  coordinator: 10,304 tok (in 22, out 10,282, cache_r 927,163, cache_w 105,434) · cost n/a (unpriced)

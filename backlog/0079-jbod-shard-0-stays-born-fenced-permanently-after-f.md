---
id: "0079"
title: JBOD shard>0 stays born-fenced permanently after format=false restart vs single-drive peers (no shard-0 fallback for watermark gossip)
status: done
priority: 2
created: "2026-07-06"
updated: "2026-07-06"
depends_on: []
spec_refs:
    - docs/multi-drive.md
---

## Description
Found on hardware during task 0047's SPDK multi-drive (JBOD) named-zone box verification (oldbox, 2026-07-07) — full evidence in `bench/zoned-multidrive-box-notes.md` item 7.

## Bug
On a `format=false` restart of a multi-drive (JBOD) node whose partitions carry **non-zero** liveness watermarks, every shard with index > 0 stays **born-fenced permanently** when the cluster's peers are single-drive (one-shard) nodes. Observed: `born fenced` ×2 but `unfenced` ×1 (`vals_unfences 1`), the stuck shard fenced across >60 s of sweeps and fresh cluster writes; single-drive peers unfence normally.

## Root cause (confirmed in code + on-box debug logs)
`crates/vals-cluster/src/node.rs` sweep loop (~L2704-2727): each shard's sweep/liveness transport installs each peer's **shard-index-aligned** address — `peer_addrs_of(n).and_then(|a| a.get(shard_index))`. Single-drive peers have no shard-1 address, so the JBOD node's shard 1 installs no peers at all; its watermark gossip reaches nobody → `peer_wms` stays empty → the sweep falls to the `refence_if_stale` else-branch and never `record_contact()`s → the born-fence never clears. Smoking gun (VALS_LOG=debug, every pass):
`node 1: member 2 has no known shard-1 address yet; sweep can't reach it this pass`.
A pristine (watermark-0) shard unfences via the pristine/lone path without peer gossip, which is why symmetric-shard off-hardware tests and the earlier Scenario-B.2 run (task 0041, second partition pristine) missed it. Confirmed on-box: reformatting (`format=true`, both watermarks 0) unfences **both** partitions.

## Impact (high)
After any `format=false` restart in a JBOD-with-asymmetric-peers topology, one drive's worth of the JBOD node's data is fenced off (serves no reads, acts as no AE source) indefinitely — silently reduced effective replication. The same shard-index-aligned addressing likely also blocks anti-entropy / handoff / cleanup for shard>0 against single-drive peers even when unfenced.

## Fix direction (design call needed — deliberately not hot-patched on box)
Give shard>0 a fallback route for the *node-level* liveness watermark gossip (watermark is per-node per `NetTransport::watermark` docs — "asks shard 0"), e.g. fall back to the peer's shard-0 (or last-available) address when the index-aligned one doesn't exist; decide whether AE/handoff for shard>0 vs fewer-sharded peers should use the same fallback or whether asymmetric shard counts should be handled at placement level. Touches the safety-critical fence/sweep peer-addressing, so it needs its own off-hardware regression coverage.

## Acceptance criteria
- Off-hardware regression test: a multi-shard node with non-zero watermarks on all shards, peers with fewer shards → every shard unfences after restart (test must FAIL on current code).
- The fix does not weaken the born-fence safety property (task 0070's gates stay intact; existing liveness tests green).
- AE/handoff/cleanup reachability for shard>0 vs fewer-sharded peers is either fixed by the same change or explicitly analyzed/covered.
- On-box re-verify of 0047 item 7 (JBOD `format=false` restart with non-zero watermarks on both partitions → `unfenced` ×2, both shards serving) — may be folded into the next box session.

## Plan

## Design decision
Scope the fix to the **node-level liveness watermark gossip only**. Do NOT give the AE/reconcile transport a fallback address: reconciling shard>0's store against a fewer-sharded peer's whole store via Merkle tree-diff would pull keys that belong on sibling shards (mis-sharding). AE/handoff/cleanup reachability for asymmetric shard counts is a placement-level design question — capture it as a follow-up task with the analysis (satisfies the "explicitly analyzed/covered" criterion).

## Fix (crates/vals-cluster/src/node.rs, sweep_loop)
1. Add a dedicated **watermark-fallback transport**: a second `NetTransport` (same `NODE_PEER_RPC_DEADLINE`), used exclusively for `watermark()` RPCs.
2. In the per-pass peer-install loop (~L2710-2728): resolve each member's full shard-address list from `shared.peer_addrs_of(n)` **or, failing that, the startup `peer_shards` parameter** (the address book is only populated by membership pushes; static-peer deployments/tests never fill it). If the index-aligned `addrs.get(shard_index)` exists → install into the main transport as today. Else, if any address exists → install the **last-available** address into the fallback transport (debug-log the fallback). Keep the existing debug log for no-address-at-all.
3. In the liveness gossip block (~L2762): for each netpeer, query `transport.watermark(n)` when `transport.has_peer(n)`, else `wm_fallback.watermark(n)` when the fallback has it. Everything downstream (rejoin_decision, record_contact, refence_if_stale) unchanged.
4. Comment clearly why the fallback is watermark-only (mis-sharding risk for AE), referencing task 0079.

Safety argument (no weakening of task 0070's born-fence gates): watermarks are node-level progress evidence; any shard of a peer is a legitimate responder (GetWatermark answers even while fenced — "control ops still answer"). The rejoin decision logic is untouched; only reachability improves. A beyond-grace shard>0 still wipes and stays fenced pending refill (refill via AE remains unreachable vs fewer-sharded peers — documented in the follow-up task).

## Regression test (must FAIL on current code)
New test in node.rs tests near the multidrive liveness tests, e.g. `multidrive_liveness_shard_gt0_unfences_against_single_shard_peers`:
- Ring `Ring::from_members(2, 64, [1, 2])`.
- Node 1: `serve_multidrive_with_liveness` — 2 mem drives (`MemDriveSet::uniform(256, 4096, 2)`), 2 listeners (2 shards), `FileLivenessLog` per shard **prestored with a recent non-zero watermark** (e.g. now-1000, grace 60_000 → recover-in-place, no wipe), create=true, peer_shards = [(1, addrs1), (2, vec![a2])].
- Node 2: single-shard `serve_with_liveness(2, ...)` with a recent non-zero watermark, peers = [(1, addrs1[0]), (2, a2)].
- Write keys via a sharded coordinator (`NetTransport::connect_sharded([(1, addrs1), (2, vec![a2])])`, quorum 1); pick at least one key per shard of node 1 (shard = `ring::hash64(key) % 2`).
- Loop with deadline: `read_from(addrs1[0], k_on_shard0)` AND `read_from(addrs1[1], k_on_shard1)` both return the value (reads only answer once that shard unfences). Current code: shard 1 never unfences → deadline fires → test fails ✓.
- Verify the test fails before the fix, passes after.

## Docs
Update the bug paragraph in docs/multi-drive.md (~L22-30): mark it fixed under task 0079 (watermark gossip falls back to the peer's last-available shard address, watermark-only), note the AE/handoff asymmetric-shard question is tracked separately.

## Follow-up task (I'll create it)
"AE/handoff/cleanup reachability for shard>0 vs fewer-sharded peers" — with the mis-sharding analysis and the wiped-shard-can't-refill limitation.

## Verification
- New test fails on current code, passes with fix.
- `cargo test -p vals-cluster` green (existing liveness tests incl. task 0070's born-fence gates).
- On-box re-verify (0047 item 7) is hardware-only: note in the work log that it folds into the next box session.

### Starting points
- crates/vals-cluster/src/node.rs sweep_loop L2580-2831: peer-install loop L2710-2728, watermark gossip block L2759-2831; sweep_loop already receives `peer_shards: Vec<(NodeId, Vec<SocketAddr>)>`
- NetTransport::has_peer/add_peer/watermark: crates/vals-client/src/net/client.rs L662-826; watermark() sends rpc(node, 0) = index 0 of the installed conn list, so a single installed fallback addr is what gets asked
- ServerShared::peer_addrs_of / merge_peer_addrs: crates/vals-cluster/src/net/server.rs L297-317 — book only filled by membership pushes
- GetWatermark answers even while fenced: net/server.rs L1801, L3077
- test harness: node.rs L5073 multidrive_with_liveness_unfences_and_persists_per_shard_watermarks (serve_multidrive_with_liveness + FileLivenessLog), L3889 two-node serve_with_liveness pattern, read_from(addr,key) helper L3222, Node::bind_n L471
- docs/multi-drive.md L22-30: bug paragraph to update

## Work log
- 2026-07-06 plan: ## Design decision Scope the fix to the **node-level liveness watermark gossip only**. Do NOT give the AE/reconcile transport a fallback address: reconciling shard>0's store against a fewer-sharded pe
…[truncated]
- 2026-07-06 context hints: 6 recorded with plan
- 2026-07-06 context hints: crates/vals-cluster/src/node.rs sweep_loop L2580-2831: transport built L2590, peer-install loop L2710-2728, watermark gossip block L2759-2831; sweep_loop already receives `peer_shards: Vec<(NodeId, Ve
…[truncated]
- 2026-07-06 implementer report: Fixed task 0079: JBOD shard>0 staying born-fenced permanently after a `format=false` restart against single-drive (fewer-sharded) peers.  ## Root cause confirmed & fixed In `crates/vals-cluster/src/no
…[truncated]
- 2026-07-06 review tier: high-powered — reviewers: Claude
- 2026-07-06 review (Claude): accept — The fix correctly addresses the root cause: sweep_loop now resolves each peer's full shard-address list from the shared address book OR the static startup `peer_shards` (address book is only filled by
…[truncated]
- 2026-07-06 decision: accept — commit: fix(cluster): unfence JBOD shard>0 against fewer-sharded peers via watermark-only fallback route (task 0079)  A multi-shard node's shard>0 had no index-aligned address for single-shard peers, so its l
…[truncated]
- 2026-07-06 usage: 66,881 tok (in 226, out 66,655, cache_r 5,328,189, cache_w 258,899) · cost n/a (unpriced)
  coordinator: 29,886 tok (in 72, out 29,814, cache_r 1,584,637, cache_w 138,496) · cost n/a (unpriced)
  implementer: 29,170 tok (in 114, out 29,056, cache_r 3,329,769, cache_w 85,531) · cost n/a (unpriced)
  reviewer:Claude: 7,825 tok (in 40, out 7,785, cache_r 413,783, cache_w 34,872) · cost n/a (unpriced)

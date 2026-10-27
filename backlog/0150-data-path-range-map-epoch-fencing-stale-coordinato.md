---
id: "0150"
title: 'Data-path range-map epoch fencing: stale coordinators must not be silently acked by former owners after a re-home cutover'
status: done
priority: 2
created: "2026-08-07"
updated: "2026-08-08"
depends_on:
    - "0142"
spec_refs: []
---

## Description
Follow-on to 0142 (confirmed re-home). After PD commits a cutover, a coordinator that has not yet refreshed its range map keeps routing writes for the moved range to the *former* owners, which accept and ack them (no ownership check on the data path). Those writes converge only via the slow confirmed-handoff cleanup, so fresh coordinators can read stale/not-found for data acked to a stale coordinator. Today the window is bounded by the coordinator map-refresh cadence and is documented in docs/zones.md, and re-home ships default-off — but before auto-re-home is default-on this should be closed properly.

Design space (decide in-task):
- carry the range-map epoch on data requests and have nodes reject/redirect writes for ranges they no longer own at a newer epoch (fencing), or
- confirmed forwarding from former owners for a grace period post-cutover, or
- a hybrid (accept + synchronous forward + flag in the reply telling the coordinator to refresh).
Mind cross-client parity (valsgo/valsq/valsmix — see task 0144): whatever fences must not break out-of-tree clients that never send an epoch.

Acceptance criteria:
- An in-process test where a coordinator with the pre-cutover map keeps writing across a cutover: no write may be acked and then be unreadable by a fresh-map coordinator beyond a tightly bounded window (or writes are rejected with a retryable refresh signal).
- Docs updated (zones.md protocol section) to replace the "stale coordinator" caveat with the fencing semantics.

## Acceptance criteria

## Plan

Goal: after a PD re-home cutover, a former owner must not silently ack writes routed by a stale coordinator. Close the gap with server-side ownership fencing on the write path, using only state nodes already have. No wire change.

DESIGN (decided in-task, per the task's design space — this is the "fencing" option):
- Fence rule (writes only — Apply and BatchApply; reads stay unfenced and are documented as bounded-staleness): for a sorted zone that has a mirrored range map on the node, each write key is resolved against the NODE'S OWN map: token = map.token_of(map.range_index(key)). The write is allowed iff this (node_id, shard) is a committed owner (ring.replicas_in_zone(zone, token, cfg.rf, cfg.scope) contains node_id AND hash64(token) % shard_count == shard_index) OR the range has a pending re-home token for which the same two-part test passes (pending target). Otherwise reject the whole request with Response::StaleEpoch(max(range_map_epoch, ring_epoch)) — an existing retryable refresh signal that the Rust coordinator (auto refresh + retry) and valsgo (respStaleEpoch on Apply/Batch/Read/Scan; payload ignored) already handle. This predicate is exactly the one mirror_rehome_writes and rehome_copy_loop already use — reuse/mirror that code.
- NO epoch comparison against the request's carried epoch: clients carry ring epoch only, and the shared PD clock means split-driven map epochs dwarf it; a carried-epoch guard would be always-true noise. Pure ownership check. Do NOT touch any set_route_epoch call site.
- Why pending targets are never spuriously fenced: PD only commits a cutover after the target reported copy-complete at ≥ the prepare epoch (pd.rs ~746-759), which requires the target's own mirror to already include the prepare. Mirror writes and coordinator dual-writes to pending targets therefore pass the fence.
- Known bounded blips (document, don't fix): (1) writes racing a prepare/cutover/split push can be fenced for the sub-second PD→node push-fanout window, surfacing as a retryable StaleRing/mirror-failed error; (2) a node with a stale ring during membership-change propagation can transiently fence a correctly-routed write. Both self-heal on push arrival; both are retryable. Add kill switch VALS_RANGE_FENCE=0 (default ON), same pattern as staged_batch_apply_enabled() (server.rs:2282).
- Skip the fence entirely when: zone_config(zone_id) is None, ordering != Sorted, or shared.range_maps has no entry for the zone (DEFAULT_ZONE / unsorted zones / hints replay are all unaffected).

IMPLEMENTATION:
1. crates/vals-cluster/src/net/server.rs — add a helper, e.g. fn rehome_write_fence<D>(shared: &ServerShared<D>, zone_id, keys: impl Iterator<Item=&[u8]>) -> Option<Response>. Memoize verdict per range index (batches usually hit 1-2 ranges) to avoid a ring walk per key. Increment a new counter on rejection.
   Hook it in dispatch_frame before staging work:
   - Apply arm (~line 1369): before apply_stage.
   - staged BatchApply arm (~line 1470): before spawning/acquiring the gate; fence over all write keys; any miss rejects the whole batch with StaleEpoch (valsgo and the Rust client both handle batch-level StaleEpoch).
   And in serve_request's Apply (~2980) and BatchApply (~3046) arms so the legacy/non-staged path and direct callers are covered (double-checking on the legacy dispatch path is harmless — the check is read-only).
2. crates/vals-client/src/observe.rs — add counter range_fence_rejections (near busy_rejections, line ~156) with Prometheus HELP text ("writes rejected because this node/shard no longer owns the key's range at its current range-map; retryable refresh signal").
3. Kill switch: VALS_RANGE_FENCE ("0" disables), read the same way staged_batch_apply_enabled does.
4. Tests (crates/vals-cluster/src/net/tests.rs; model on rehome_real_transport_copy_mirror_cutover_is_zero_gap at line 1895, reuse push_range_map_full / push_members_full / range_stats helpers):
   a. Acceptance test "stale coordinator across cutover": 3-node in-process cluster, RF=1 sorted zone, seed keys, prepare (pending map, epoch 10) pushed to all nodes, wait copy/mirror complete, push committed map (epoch 11) to NODES only. Then: a stale coordinator holding the PRE-PREPARE map (split, no pending — routes to the former owner) built with .with_manual_refresh(): its write must return Err (CoordError::StaleRing) — assert the value is NOT observable anywhere (fresh-map coordinator read returns the old value). Then an auto-refresh stale coordinator: same write succeeds transparently and a fresh-map coordinator reads it back. This is the acceptance criterion: no write acked-then-unreadable; rejection is a retryable refresh signal.
   b. Narrow fence-semantics tests (single node or small cluster, via serve_request or the client): non-owner write → StaleEpoch + counter bump; owner write → accepted; pending-target write → accepted; unsorted zone / missing map → never fenced; BatchApply containing one non-owned key → whole batch StaleEpoch.
   c. Run the full vals-cluster suite (~424 tests). Existing rehome/cleanup/handoff tests should pass because internal senders (mirror, cleanup push) target owners computed from their own maps and receivers agree in-tests; if a test seeds "unowned" keys over the network and now gets fenced, seed via the local store handle instead (the fence rejecting network writes of unowned keys is the intended new semantic) — but do not weaken the fence to accommodate a test without flagging it.
5. docs/zones.md — replace the stale-coordinator caveat paragraph (~line 152-156, "Cutover consistency assumes...") with the fencing semantics: nodes reject writes for ranges they no longer own per their own mirrored map (neither committed owner nor pending target on the receiving shard) with StaleEpoch, a retryable refresh signal all current clients already handle; reads remain best-effort within the map-refresh window; note the push-propagation blip and VALS_RANGE_FENCE=0 kill switch; update the "two known edges" sentence (~line 170-178) so 0150/fencing is landed and 0155 is the remaining gate before default-on.
6. No comments in code beyond Prometheus HELP (project rule). Hand-format new code; do NOT run cargo fmt.

VERIFY: cargo test -p vals-cluster (full), cargo check --workspace. Confirm the new acceptance test fails if the fence is disabled (quick sanity: run it once with VALS_RANGE_FENCE=0 expecting the old silent-ack behavior, if the test harness allows env control; otherwise reason via the narrow tests).

### Starting points
- crates/vals-cluster/src/net/server.rs:1326-1560 dispatch_frame write arms; :2282 staged_batch_apply_enabled (kill-switch pattern); :2669-2755 mirror_rehome_writes (exact ownership predicate to reuse: current.contains(node_id) && shard_index == hash64(token)%shard_count); :2980,:3046 serve_request Apply/BatchApply arms
- crates/vals-cluster/src/net/server.rs:38-100 ServerShared fields: range_maps, range_map_epoch, ring, catalog(zone_config), shard(), node_id
- crates/vals-client/src/partition.rs RangeMap: range_index/token_of/pending_of/replicas_in_zone via ring
- crates/vals-cluster/src/net/tests.rs:1895 rehome_real_transport_copy_mirror_cutover_is_zero_gap — model for the acceptance test; helpers push_range_map_full, push_members_full, range_stats
- crates/vals-client/src/coord/mod.rs:1087-1105 retry_stale/refresh_after_stale (StaleEpoch -> refresh ring+catalog+maps -> retry once); :723 with_manual_refresh
- crates/vals-client/src/observe.rs:~156 busy_rejections counter block + Prometheus HELP registration
- docs/zones.md:152-178 stale-coordinator caveat + 'two known edges' paragraph to update
- pd.rs:746-759 cutover gating on copy-complete reports >= prepare epoch (why pending targets always know the prepare)
- valsgo already handles respStaleEpoch on all ops, payload ignored — no client changes needed
- Project rules: no explanatory comments in code; tree is not rustfmt-clean — never cargo fmt

## Work log
- 2026-08-08 plan: Goal: after a PD re-home cutover, a former owner must not silently ack writes routed by a stale coordinator. Close the gap with server-side ownership fencing on the write path, using only state nodes 
…[truncated]
- 2026-08-08 context hints: 10 recorded with plan
- 2026-08-08 context hints: server.rs mirror_rehome_writes ~2716-2740: the exact ownership predicate (current.contains(&shared.node_id) && shard_index == (hash64(map.token_of(index)) % shard_count as u64) as usize); ServerShared
…[truncated]
- 2026-08-08 preload: 4 file(s), ~29 KiB seeded into implementer context
- 2026-08-08 implementer report: Implemented task 0150 data-path range-map ownership fencing.  Changes: - Added `rehome_write_fence` in `server.rs`, default-on with `VALS_RANGE_FENCE=0` kill switch. It fences sorted-zone `Apply`/`Bat
…[truncated]
- 2026-08-08 revision: Addressed both hot-path review findings: - Removed the per-request `RangeMap` clone. `rehome_write_fence` now holds the synchronous `range_maps` `Ref` and uses the map by reference for the duration of
…[truncated]
- 2026-08-08 review tier: single-opus — reviewers: sol
- 2026-08-08 review (sol): revise — The ownership predicate, retry signal, metrics, documentation, and stale-after-cutover integration test are generally well implemented, and the current 449-test vals-cluster binary passes (446 passed,
…[truncated]
- 2026-08-08 revision: Closed the in-flight cutover/ack race on every asynchronous write path.  Changes: - Added ACK-time ownership revalidation via `fence_write_response`. A post-cutover `StaleEpoch` now overrides the woul
…[truncated]
- 2026-08-08 review (sol): accept — The revision closes the in-flight cutover race by routing every asynchronous Apply/BatchApply success through ACK-time ownership revalidation, so a cutover installed after admission now converts the w
…[truncated]
- 2026-08-08 decision: accept — commit: 0150: data-path range fencing — former owners no longer silently ack stale-coordinator writes after a re-home cutover. Sorted-zone Apply/BatchApply are now admission-fenced against the node's own mi
…[truncated]
- 2026-08-08 usage: 8,717,669 tok (in 2,370,054, out 89,695, cache_r 10,720,568, cache_w 543,906) · cost n/a (unpriced)
  implementer: 6,378,706 tok (in 1,500,393, out 23,017, cache_r 4,855,296, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 2,284,065 tok (in 869,559, out 11,882, cache_r 1,402,624, cache_w 0) · cost n/a (unpriced)
  coordinator: 54,898 tok (in 102, out 54,796, cache_r 4,462,648, cache_w 543,906) · cost n/a (unpriced)

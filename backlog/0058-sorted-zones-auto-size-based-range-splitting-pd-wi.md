---
id: "0058"
title: 'Sorted zones: auto/size-based range splitting + PD-wired range merge'
status: done
priority: 4
created: "2026-07-05"
updated: "2026-07-05"
depends_on: []
spec_refs: []
---

## Description
Split from grooming umbrella 0025 (BACKLOG.md §Deferred follow-ons, "Sorted zones" bullet; token-based intra-node sharding already landed in task 0029).

Today range **splits** are an explicit admin op (`SetRangeMaps` push), and range **merge** exists in `RangeMap` but is not wired to a PD op. Two scoped extensions:

1. **Auto / size-based range splitting**: when a sorted-zone range grows past a size threshold (keys or bytes — pick and justify), PD (or the node reporting to PD) triggers a split at a sensible midpoint and pushes the updated range map. Must ride the existing split-migration machinery (shard-aware AE hand-off from 0029; a split is folded into the cleanup fingerprint so it triggers a rebalance pass). Threshold tunable via config; off by default is acceptable if justified.
2. **Range merge as a PD op**: expose `RangeMap` merge through a PD admin op (mirror of the split push), with the same safety story — merged-away token's keys must remain served (they hash to a possibly-different shard/node after merge, so the same orphan hand-off path must cover merges).

## Acceptance criteria
- A sorted zone under sustained ingest into one range auto-splits past the threshold and keys migrate correctly (no loss, reads/scans stay complete) — covered by an off-hardware test.
- An admin merge op joins two adjacent ranges; keys from the retired range remain readable and AE re-homes them; no data loss across restart.
- Both paths covered by tests at least at the MemDriveSet/mem-node level; BACKLOG.md deferred-follow-ons bullet updated.

## Acceptance criteria

## Plan

Two scoped extensions to sorted-zone range management, riding the existing split-migration machinery (range_map_epoch is already folded into the node's cleanup fingerprint, so ANY range-map change — split or merge — triggers a rebalance pass + orphan hand-off; no new node-side migration code is expected).

## Part A — Range merge as a PD op (mirror of split)
1. `PlacementDriver::merge_zone(zone_id, at) -> Result<bool, PartitionError>` in crates/vals-cluster/src/pd.rs, mirroring `split_zone` (~line 368): read committed map, `map.merge_at(at)`; return Ok(false) without proposing if `at` is not a boundary; else `set_range_map`.
2. `PdCmd`: generalize `SplitZone(ZoneId, Vec<u8>)` into a range-op carrying a kind (Split|Merge) — or add `MergeZone`. In BOTH drive loops (run_single ~line 882/890 and run_multi ~line 1109/1223) generalize the `pending_splits` queue to `(zone, at, kind)`, keeping the sequential one-in-flight read-modify-write discipline; "reflected" = boundary present (split) / boundary absent (merge).
3. `PdDaemon::merge_zone(zone_id, at)` public method mirroring `PdDaemon::split_zone` (~line 1370).
4. Admin op in crates/vals-server/src/admin.rs: `merge-zone <id|name> <key>` (+ module doc list + unknown-command usage string).
5. Tests:
   - `PlacementDriver::merge_zone` unit test (mirror the split test at pd.rs ~2710: removes a boundary, no-op when absent).
   - Extend the PD stub-push test (~2996): split, then merge; the pushed range map reflects the removed boundary.
   - Extend node.rs `multidrive_zoned_serves_sorted_zone_and_migrates_a_split` (~3782) with a merge phase (after the existing restart phase, epoch bump): push the merged (empty-splits) map; poll-until-settled that the retired range's keys re-home to the shard owning the merged token, with the union-across-shards no-loss assertion; coordinator reads + full scan complete and ordered. This pins "merged-away token's keys remain served / AE re-homes them / no data loss across restart" without a new node lifecycle.

## Part B — Auto / size-based range splitting
Metric decision: approximate **live entry count per range**, estimated RAM-only from each run's in-memory block index + the memtable index. Justification (put in code docs): entry counts are cheaply and uniformly estimable with zero device reads (run block indexes are fence-keyed and entry-proportional); per-range *byte* accounting would need an O(n) pass over vlog-resident value lengths. Threshold default **off** (justify: auto-resharding moves data; opt-in until box-validated, consistent with the repo's default-off rollout style).

1. vals-store Engine: `range_estimate(lo: Option<&[u8]>, hi: Option<&[u8]>) -> (u64 approx_entries, Option<Vec<u8>> midpoint)` (name/shape at implementer's discretion). Per device run: locate the block window overlapping [lo,hi) via the in-RAM index (cf. `locate_block`/`straddling_start_block`, run.rs ~1331/1399); approx entries += run entry_count × window/total blocks. Plus memtable in-range count via the front-coded index (index.rs, BTreeMap of blocks — bounded, RAM-only). Midpoint: a fence key near the middle of the in-range window of the largest run (fallback: memtable middle block's first key), returned only if non-empty, strictly > lo (when Some) and strictly < hi (when Some); else None. MUST be bounded, RAM-only, no device IO (it runs on the serving core).
2. Wire (crates/vals-client/src/wire.rs): read-only `Request::RangeStats` (no auth, like `RangeMaps`) and `Response::RangeStats { epoch, ranges }` where ranges = per (zone_id, token): approx entries + optional midpoint. Encode/decode + OwnedRequest; extend wire_fuzz coverage per that test's pattern.
3. Node serve (crates/vals-cluster/src/net/server.rs serve_request): for each ALREADY-OPEN sorted-zone store on this shard (do not lazily materialize zones), for each range of the adopted range map (`shared.range_map(zone)`, default single()) with a nonzero estimate, report (zone, token, entries, midpoint); stamp the reply with the shard's adopted `range_map_epoch`.
4. PD side (pd.rs pusher): give `spawn_pusher` a `mpsc::Sender<PdCmd>` clone. Leader-only, and only when threshold > 0, poll all push targets every poll interval (blocking TCP w/ PUSH_TIMEOUT, same style as the beacon): skip replies whose epoch < the PD's committed range_map_epoch (stale map adoption — prevents mis-attributed counts mid-migration); per sorted zone take the per-token MAX entries across replies; if a range exceeds the threshold and its reported midpoint is valid (non-empty, strictly inside that range's bounds per the PD's committed map, not an existing boundary), enqueue `PdCmd::SplitZone(zone, midpoint)` — at most one per zone per poll (the drive loop's sequential queue + `split()` idempotence make duplicates harmless). Config: `VALS_PD_RANGE_SPLIT_THRESHOLD` (entries; unset/0 = disabled, the default) and `VALS_PD_RANGE_SPLIT_POLL_MS` (default 5000), read at spawn; plus a `PdDaemon::set_auto_split(threshold, poll)` (or similar) that writes fields riding `PushState` so tests don't race on env vars — verify `publish_push_state` keeps setting fields individually so it doesn't clobber them.
5. Tests:
   - vals-store: estimator unit tests (uniform keys across memtable+flushed runs: whole-range estimate within a generous tolerance; midpoint lands mid-range, e.g. 25th–75th percentile; sub-range bounds respected; empty range → 0/None).
   - node-level wire test: RangeStats reply reflects per-range counts on a mem node.
   - E2E auto-split (node.rs or pd.rs, mem only): 1 node (2 shards, rf 1) + `PdDaemon::spawn_single`, sorted zone created via PD, auto-split enabled via the setter (low threshold, ~100ms poll); coordinator writes N keys into the single range; poll until the PD's committed/pushed range map gains ≥1 split; then poll-until-settled with the union no-loss assertion (mirroring the existing split test) and assert coordinator reads + full ordered scan of all N keys.

## Part C — docs
Update BACKLOG.md §Deferred follow-ons "Sorted zones" bullet (~line 943): auto-split + PD-wired merge landed; note the default-off `VALS_PD_RANGE_SPLIT_THRESHOLD` knob and the box-validation follow-up. Update admin.rs doc header command list.

## Verification
`cargo fmt --check`, `cargo build --workspace`, `cargo clippy` if configured, `cargo test -p vals-store -p vals-client -p vals-cluster -p vals-server` (full workspace test if time allows). No behavior change when the knob is unset (default off) — assert existing tests pass unmodified.

### Starting points
- crates/vals-client/src/partition.rs — RangeMap (split/merge_at already exist)
- crates/vals-cluster/src/pd.rs:368 PlacementDriver::split_zone; :578 PdCmd; :629 pusher_loop; :836+882 run_single pending_splits; :1085+1223 run_multi pending_splits; :1370 PdDaemon::split_zone; :1866 push helper; tests ~2710 (split unit), ~2914 (stub node), ~2996 (push test)
- crates/vals-cluster/src/node.rs:2044-2099 cleanup fingerprint already folds range_map_epoch (splits AND merges trigger rebalance); :3782 multidrive_zoned_serves_sorted_zone_and_migrates_a_split (template for merge phase); :1279 env-tunable pattern (VALS_RUN_COMPACT_THRESHOLD)
- crates/vals-cluster/src/net/server.rs:2706 serve_request; :2696 range_maps_reply; shared.range_map_epoch()/range_maps/catalog
- crates/vals-client/src/wire.rs:154 Request enum (RangeMaps read-op pattern); OwnedRequest at :380; crates/vals-cluster/tests/wire_fuzz.rs
- crates/vals-store/src/run.rs:1220 DeviceRun { index: Vec<BlockRef>, entry_count }; :1331 locate_block; :1399 straddling_start_block
- crates/vals-store/src/engine.rs:2373 run_handles, :1546 len, :3089 memtable_bytes; index.rs front-coded memtable index (BTreeMap of blocks)
- crates/vals-server/src/admin.rs:159 split-zone admin op (mirror for merge-zone)
- BACKLOG.md:943 deferred-follow-ons bullet to update
- crates/vals-cluster/src/testutil.rs running()/running_pd() RAII harnesses

## Work log
- 2026-07-05 plan: Two scoped extensions to sorted-zone range management, riding the existing split-migration machinery (range_map_epoch is already folded into the node's cleanup fingerprint, so ANY range-map change —
…[truncated]
- 2026-07-05 context hints: 10 recorded with plan
- 2026-07-05 context hints: crates/vals-client/src/partition.rs — RangeMap (split/merge_at already exist); crates/vals-cluster/src/pd.rs:368 PlacementDriver::split_zone; :578 PdCmd; :629 pusher_loop; :836+882 run_single pendin
…[truncated]
- 2026-07-05 implementer report: Implemented task 0058 — sorted-zone auto/size-based range splitting + PD-wired range merge — across all three parts. Everything builds, `cargo fmt --check` is clean, `cargo clippy` (the 4 crates) 
…[truncated]
- 2026-07-05 review tier: single-opus — reviewers: Claude
- 2026-07-05 review (Claude): accept — The change fully implements task 0058 across all three planned parts: PD-wired range merge (mirror of split, admin op, unit + stub-push + node-restart tests), auto/size-based range splitting keyed on 
…[truncated]
- 2026-07-05 decision: accept — commit: sorted zones: auto/size-based range splitting + PD-wired range merge (task 0058)  - PlacementDriver/PdDaemon merge_zone (mirror of split) + admin merge-zone op;   both drive loops share one sequential
…[truncated]
- 2026-07-05 usage: 139,656 tok (in 562, out 139,094, cache_r 37,097,342, cache_w 526,844) · cost n/a (unpriced)
  implementer: 100,840 tok (in 410, out 100,430, cache_r 32,953,688, cache_w 256,877) · cost n/a (unpriced)
  coordinator: 32,171 tok (in 90, out 32,081, cache_r 3,244,815, cache_w 222,493) · cost n/a (unpriced)
  reviewer:Claude: 6,645 tok (in 62, out 6,583, cache_r 898,839, cache_w 47,474) · cost n/a (unpriced)

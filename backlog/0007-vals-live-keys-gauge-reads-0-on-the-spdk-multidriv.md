---
id: "0007"
title: vals_live_keys gauge reads 0 on the SPDK/multidrive serve path
status: done
priority: 2
created: "2026-07-02"
updated: "2026-07-04"
depends_on: []
spec_refs: []
---

## Description
BACKLOG.md §Productization: the gauge isn't wired through `serve_multidrive`, so per-node key-count / per-key-RAM tracking — the metric the scaling work needs most — is blind exactly on the production backend (`records_written`/`records_read` are accurate). Small, contained observability fix.

## Acceptance criteria
- `vals_live_keys` reflects the real live key count on the SPDK/multidrive path (and stays correct across flush/compaction with the LSM — keys in runs count).
- Covered by a test on the software backend equivalent of the multidrive path; scrape-endpoint exposition verified.

## Acceptance criteria

## Plan

Root causes (two, both in the shared serve path — `sample_gauges` IS already called on the multidrive path via `periodic_loop`, but its inputs/publishing are wrong):

1. **LSM blindness**: `sample_gauges` sums `engine().len()` = memtable index length only. After a flush, keys move into on-device runs and leave the in-RAM index, so the gauge collapses toward 0 exactly on the production (SPDK/multidrive) backend, where flushes happen. This is the "reads 0" symptom.
2. **Multi-shard stomping**: each shard's periodic loop `set()`s the process-global gauge from *its own* zones, so on a P-shard multidrive node the gauge shows one shard's subtotal (last writer wins) — acknowledged as a follow-up in the existing NB comment.

Fix plan:

A. **Exact resident-key counter at the VersionedStore level** (`crates/vals-cluster/src/replica.rs`). The Merkle leaf accumulator (`VersionIndex`) is already maintained exactly in lock-step with every mutation, with the pre-write version (`old`, read for LWW) in hand: `apply_leaf(key, old, new)` on every winning write (old=None ⇔ brand-new key), `retire_leaf` on compaction-GC'd tombstones and `forget`, `clear()` on wipe, and `seed()` folds each recovered key exactly once (memtable + run sidecars). Add a `live: u64` count to `VersionIndex` maintained at exactly those points: +1 in `apply_leaf` when `old.is_none()`, −1 in `retire_leaf`, 0 in `clear`; seeding thus counts every recovered key. Expose `VersionedStore::live_keys() -> u64`. Semantics: distinct resident keys (live puts + not-yet-GC'd cluster tombstones), across memtable AND runs — matching today's file-backend semantics (index.len() also counts tombstones) but correct across flush/compaction (flush and run-merge are key-set-transparent; compaction GC decrements via retire_leaf).

B. **Per-shard delta publishing** (`crates/vals-cluster/src/net/server.rs` + `observe.rs`). Add `Gauge::adjust(old, new)` (wrapping `fetch_add(new - old)`). In `ServerShared`, keep `published_live_keys`/`published_used_segments` Cells; `sample_gauges` computes this shard's totals (using `store.live_keys()` for live keys) and publishes the delta vs. its last-published value, so N shards' contributions sum instead of stomping. Zero out this shard's contribution in `finalize_shutdown` (so a drained node doesn't leave stale counts — also keeps tests deterministic). Update the NB comment and the `Gauges::live_keys` doc/help text.

C. **Tests**:
   - Unit tests in replica.rs: `live_keys()` exact across new puts / overwrites / delete (tombstone-put keeps the key resident) / flush + writes over run-resident keys / compact GC of aged tombstones (decrements) / forget (decrements) / clear (0) / reopen-seed (matches).
   - New dedicated integration test binary `crates/vals-cluster/tests/live_keys_gauge.rs` (own process ⇒ the process-global gauge is deterministic): `serve_multidrive` over `MemDriveSet` (2 drives, P=2 shards, short sweep), `VALS_FLUSH_TAIL_EPOCHS=0` so every snapshot tick flushes; write N keys via the coordinator; poll `observe::gauges().live_keys` until == N (proves multi-shard summation); sleep past several flush ticks and assert it *stays* N (the regression test for the reads-0 symptom); optionally shut the node down and assert the contribution returns to 0.
   - Scrape exposition: extend `crates/vals-server/tests/daemon.rs` to eventually-assert `vals_live_keys` on node 1's scrape reflects the keys the e2e wrote (rf=3 ⇒ node 1 holds all keys); the existing metrics.rs unit test already covers rendering.

D. Mark the BACKLOG.md §Productization checkbox for this item fixed with a one-line note.

Verification: `cargo test -p vals-cluster -p vals-server` (plus targeted new tests), `cargo build` clean.

### Starting points
- crates/vals-cluster/src/net/server.rs: sample_gauges() (~line 807) — uses engine().len(), per-shard set() stomping; finalize_shutdown (~line 395)
- crates/vals-cluster/src/replica.rs: VersionIndex (line ~250), apply_leaf/retire_leaf/clear, seed() (~442), store() funnel (~1326), compact() retire loop (~753), forget() (~770)
- crates/vals-cluster/src/observe.rs: Gauge (line ~82, set/get only), Gauges::live_keys doc (~319), gauges_named()
- crates/vals-cluster/src/node.rs: periodic_loop calls sample_gauges (~536); flush_tail_epochs_threshold reads VALS_FLUSH_TAIL_EPOCHS (~558); snapshot_loop flush trigger (~1095)
- multidrive software-backend test template: crates/vals-cluster/src/node.rs test multidrive_node_serves_partitions_across_drives (~2600) — MemDriveSet::uniform, Node::bind_n, Coordinator + NetTransport::connect_sharded
- crates/vals-server/tests/daemon.rs: metrics scrape assertions (~line 177), eventually() helper
- crates/vals-server/src/metrics.rs test renders vals_live_keys (sets gauge to 42)
- BACKLOG.md line ~628: the checkbox to mark fixed

## Work log
- 2026-07-04 plan: Root causes (two, both in the shared serve path — `sample_gauges` IS already called on the multidrive path via `periodic_loop`, but its inputs/publishing are wrong):  1. **LSM blindness**: `sample_g
…[truncated]
- 2026-07-04 context hints: 8 recorded with plan
- 2026-07-04 context hints: crates/vals-cluster/src/net/server.rs: sample_gauges ~807, finalize_shutdown ~395, ServerShared constructors ~194 (single_zone_on) and the zoned one ~768; crates/vals-cluster/src/replica.rs: VersionIn
…[truncated]
- 2026-07-04 implementer report: Fixed task 0007: `vals_live_keys` reads 0 on the SPDK/multidrive serve path.  ## Root causes (both in the shared serve path; `sample_gauges` was already called per shard) 1. **LSM blindness**: `Server
…[truncated]
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change correctly fixes both root causes of the `vals_live_keys` gauge reading 0 on the SPDK/multidrive path. (1) LSM blindness: a new exact resident-key counter `VersionIndex.live` is maintained i
…[truncated]
- 2026-07-04 decision: accept — commit: Fix vals_live_keys reading 0 on the SPDK/multidrive path: exact resident-key count + per-shard delta gauges (task 0007)
- 2026-07-04 usage: 46,242 tok (in 278, out 45,964, cache_r 6,593,619, cache_w 236,579) · cost n/a (unpriced)
  implementer: 28,353 tok (in 178, out 28,175, cache_r 4,350,846, cache_w 80,136) · cost n/a (unpriced)
  reviewer:Claude: 11,851 tok (in 74, out 11,777, cache_r 1,011,832, cache_w 46,072) · cost n/a (unpriced)
  coordinator: 6,038 tok (in 26, out 6,012, cache_r 1,230,941, cache_w 110,371) · cost n/a (unpriced)

---
id: "0322"
title: 'Rehome on nodes>RF: only stores holding keys in the changed span refold (store-local placement reuse)'
status: done
priority: 1
created: "2026-09-27"
updated: "2026-09-28"
depends_on: []
spec_refs: []
---

## Description
## Evidence (SOAK0321-F2, 3-node RF2, 2026-09-27)

- **Every rehome refolds every zone store on every node.** On nodes > RF, a rehome changes a range's replica *set*, so each prepare and commit invalidates the AE seed of every shard store of the zone on every node. 0317 only covered order-only changes.
- **Early in the write.** Two rehomes at about 80–140M keys/node gave 127–135M seed folds per node. ACK/s dipped to 97k for about a minute, and RSS jumped from 3 to 10 GB and stayed there.
- **After drain.** Splits stopped, which freed the rehome slot, and a third rehome started. Each node restarted about 200 full seeds at 1.4B keys, with folds climbing to 620–826M. RSS rose from 20 GB to 36–45 GB, past the 36 GiB budget, while seeds shed and restarted (171–186 RAM sheds). The harness safety-stopped the run when lab-3's MemAvailable fell below 4 GiB. Verify never ran.

## Change (one mechanism)

- `prepare_zone_seed` computes the spans whose sorted replica set changed.
- If this store holds no key in any of them, it installs the new placement closure and keeps its leaves. "Holds no key" is exact: one `winner_count_page(lo, hi, 1)` per span over the memtable and runs, tombstones included.
- Leaves depend only on `group_of` of stored keys, so they stay exact; keys that arrive later use the new closure. Stores that do hold keys invalidate as before.
- Result: on 3-node RF2, a rehome refolds only the one owner shard store per node.
- New counter: `ae_seed_placement_local_reuses`.

## Acceptance

- Unit tests, including a node-set change on a range the store does not hold, a leaf/recompute check, and a node-set change on a range it does hold, which still invalidates. The full vals-cluster suite passes.
- Hardware: an F2-profile rerun (SOAK0321-F3). Per rehome, seed starts and folds should be about 1/8 of F2's, and there should be no post-drain RSS blowup.

## Acceptance criteria

## Work log

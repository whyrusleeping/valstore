---
id: "0479"
title: 'PD: a dropped zone''s range map (with a pending re-home) stays in PD state and blocks every balance/re-home cluster-wide'
status: done
priority: 2
created: "2026-10-16"
updated: "2026-10-17"
depends_on: []
spec_refs: []
---

## Description

## Description
## Found by
0478 hardware rerun (2026-10-16): after restart, `vals_pd_balance_noop_active_rehome` climbs every poll, `vals_pd_balance_blocked_ranges` = all ranges, `vals_pd_rehomes_started` stays 0 while zone `ring3` auto-splits 1,183 times in 20 min.

## Cause
`pd.stderr` on lab-1: `rehome prepare zone 18 range 711e33aaece480c4 (~0 entries)` at 03:30:14, never committed; zone 18 was then dropped. `PdDriver::drop_zone` (pd/driver.rs) deletes only the zone config key, so the zone's range-map records (meta + per-range records, including the pending token) stay in PD state. `range_maps_of` returns maps for every zone id that has map keys, and the global gates `snap.range_maps.values().any(|m| m.active_rehomes() > 0)` (balance.rs ~1061, state.rs ~630, range_policy.rs ~473, daemon.rs ~1403/2158) therefore see a re-home that can never finish.

## Acceptance
- Dropping a zone removes (or makes ignored) its range map, including pending re-home state, and orphan maps already persisted in PD state (like zone 18/19 on lab) no longer block balancing.
- A test: prepare a re-home, drop the zone, show balancing/re-homes of another zone proceed.
- Check whether nodes also keep/receive the orphan map and whether that matters.

## Acceptance criteria

## Outcome

drop_zone deletes the zone key and all its range-map records (incl. pending re-home) in one batch. range_maps_of ignores maps without a zone key (lab's zone 18/19 orphans no longer trip the global active_rehomes gates); the PD leader collects orphan records each settled poll; set_range_map_checked refuses missing zones and checks the zone key at apply; queued map ops of a dropped zone are discarded at the queue head, except PrepareBalance, whose validation publishes the rejection the executor waits for (review finding). Nodes replace the whole map set on SetRangeMaps, so they drop the orphan on the next push (it only left a dead mirror stat there). Driver, daemon and queue-head tests; kill-the-fix fails for discard and filter. vals-cluster lib + integration + vals-server pass. lab still runs patch build 67ab97ce.

Commit: fix(0479): dropping a zone deletes its PD range map incl. pending re-home; orphan maps are ignored and collected, map writes need a live zone, and dropped zones' queued map ops are discarded, so they no longer block re-homes

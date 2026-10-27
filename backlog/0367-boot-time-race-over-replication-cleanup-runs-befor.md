---
id: "0367"
title: 'Boot-time race: over-replication cleanup runs before sorted-zone range maps are adopted and plants hash-routed stray copies on non-owner shards'
status: done
priority: 1
created: "2026-10-02"
updated: "2026-10-02"
depends_on: []
spec_refs: []
---

## Description
## Evidence
`bench/0363-compact-values/FULL4-FINDINGS.md`, "Diagnosis B (AE)"; raw data in `/data/dataset/task0363-results/FULL4-KF2/B-AE-DIAG/` (`diagnosis-b.json`); read-only tool `bench/0363-compact-values/aediag/`.

Two reopens of the FULL4-KF2 corpus (master d4be3026, lab-1+3) each left exactly one stray on node1. Each stray is a byte-identical second copy of a real records-zone entry, on a shard that does not own the key's range.
- **Where the strays sit:** each is on `hash64(key) % 8` (shard 0 / shard 1), while the owning shards from the range token are 5 / 4. Each key is the first zone-1 key of its owning shard.
- **What it breaks:** AE reconcile correctly refuses wrong-shard keys, leaving a persistent one-leaf AE difference.
- **What it does not break:** no data loss, and reads stay correct.

## Mechanism (from code reading, consistent with all observations; confirm with a repro)
At boot each shard adopts the zone catalog and immediately triggers a sweep. The range maps arrive 0.4–1.3 s later. During that gap:
1. **Fallback map:** the sweep's over-replication cleanup falls back to a single-range map. So every shard except the one owning the single range decides its keys are not its own.
2. **Hash routing:** it hands its first key to the owners through a transport that has no range maps, so the transport routes by key hash.
3. **Unfenced accept:** the receiving shard's write fence accepts, because that shard has no range map yet either.
4. **Stall:** the other owner does not confirm, so nothing is forgotten, and the cleanup pass stalls after one key.

Hence at most one stray per shard per lost race.

## Fix (all three, each with a test)
- **Wait for maps:** do not run over-replication cleanup or AE reconcile on a sorted zone until its range map is adopted. No single-range fallback for sorted zones.
- **Transport:** the handoff/cleanup transport must not route sorted-zone keys by hash. It uses range maps, or refuses until they are available.
- **Fence:** the write fence must reject sorted-zone writes on a shard with no adopted range map, with a retriable error.

## Acceptance
- **Local repro** (multi-node test harness): delay range-map adoption after catalog adoption, and show a stray created before the fix and none after. Add a knockout test.
- **Existing strays:** document what removes ones already on disk. Prefer the existing wrong-shard cleanup path once 0368 is fixed; add a test that a pre-existing stray is cleaned up and AE converges.
- **Suites:** vals-cluster/vals-store suites pass in debug.
- **Hardware (optional):** reopen the preserved FULL4-KF2 corpus (lab-1/3, kept for this). It must show no new strays, the two existing strays removed, and AE digests reaching 0 with a non-frozen sweep.

## Acceptance criteria

## Work log

### 2026-10-02: fixed (uncommitted → committed with the 0367/0368 commit); independent review OK to merge; hardware check on the FULL4-KF2 corpus running
- **Root cause confirmed:** the `RangeMap::single()` fallback, hash routing via `range_maps_clone()` omitting unmapped sorted zones, and `rehome_write_fence*` accepting with no map.
  - **Data loss:** with the fixes knocked out, the race loses the only copy of a key at RF=1.
- **Fix:**
  - a shard holds its sorted zones from catalog knowledge until its first SetRangeMaps adoption;
  - the PD pushes SetRangeMaps even at epoch 0 once the catalog is published;
  - the sweep skips reconcile and cleanup for held zones (`vals_sweep_sorted_zones_held`);
  - handoff uses `sorted_routes()` and never hash-routes a sorted zone;
  - the fence returns StaleEpoch for sorted writes on a held shard.
  - deploy.md has the upgrade hazard: upgrade the PD first.
- **Existing strays** are removed by over-replication cleanup (test: planted stray handed off, forgotten, AE converges).
- **Review:** OK to merge. Minors, filed as 0369:
  - `beacon_repush` restart detection by ring epoch only, so a node can stay held if a membership bump lands first;
  - opaque maps without `VALS_PD_OPAQUE_RANGE_MAPS` leave nodes held forever (deploy.md line);
  - a held node replies StaleEpoch(0) → QuorumFailed instead of StaleRing (cosmetic);
  - runtime create-zone presplit gap (no loss);
  - `range_maps_acked` `unwrap_or(0)`;
  - test gaps: process-wide counters in parallel tests, the hold-only knockout heals within 300 ms.

### 2026-10-02: hardware-confirmed on the FULL4-KF2 corpus (lab-1+3; bench pin 4de2293c = master 5a6b13f8 content; valsd 5fed375f…)
- **No new strays across 3 reopens.** `vals_sweep_sorted_zones_held` went 0→16 per node, and the hold released within 0.1–0.7 s of the first range-map push.
- **Both pre-existing strays removed by over-replication cleanup** (37 and 68 min; slow, see 0369 item 10). All 24 (zone, shard) SyncTree pairs are equal and AE digests went to 0.
- **Sweep passes advanced every 5 s on idle clusters.** `vals_sweep_pass_age_seconds` max was 5 s while idle.
- **Correctness passed:** cursor, samplecheck (376,199 keys), exact per-zone counts.
- Evidence: `bench/0363-compact-values/FULL4-FINDINGS.md` "Fix check (0367/0368)".


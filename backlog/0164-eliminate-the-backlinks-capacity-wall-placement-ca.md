---
id: "0164"
title: 'Eliminate the backlinks capacity wall (placement/capacity levers): presplit depth, region sizing, and/or gated re-home — unblocks 0130''s multi-hour bar'
status: done
priority: 1
created: "2026-08-09"
updated: "2026-08-09"
depends_on:
    - "0161"
spec_refs:
    - docs/zones.md
    - plans/bootes-soak-runbook.md
---

## Description
The 0161 hardware arm re-diagnosed the ~60-min bootes soak wall (`bench/bootes-soak-lab/run-20260809T163755Z-0161-shadow/FINDINGS.md`): post-0160, the hot backlinks engine meets its reserve floor holding ~19.5 GiB of **99.8% unique live** backlink records — a genuine capacity/placement wall. Backlink keys (`ref\0source\0path`) are write-once; the ref distribution concentrates live data onto one range pair; presplit=8 is insufficient; canonical splits cannot move populated ranges; re-home defaults OFF. No reclaim can fix this. It is the direct blocker for 0130's multi-hour soak bar.

Scope: make a full-rate bootes soak survive multi-hour on the current fleet by capacity/placement levers, choosing among (measure first):
- raise backlinks presplit (e.g. 32–64) so per-pair live load at the multi-hour scale fits the 32 GiB region;
- grow backlinks `region_mib` for the fleet's drive size (capacity math: live backlinks bytes/hour ≈ 19.5 GiB per hot pair per hour at full rate);
- enable confirmed re-home (`VALS_PD_REHOME_ENABLED=1`) once 0155 (churn robustness) is judged ready, so hot populated ranges migrate off the full pair.

Acceptance criteria:
- A fresh full-rate soak arm runs well past 60 min with min_zone_free_segments on every pair bounded away from the floor (or demonstrably balanced decline consistent with total-capacity limits rather than one hot pair).
- The chosen lever(s) documented in the soak runbook with capacity math.
- Findings under bench/bootes-soak-lab; 0130's remaining bar (upstream-PDS-limited) re-attempted or explicitly re-scoped afterwards.

## Acceptance criteria

## Plan

## Diagnosis (from 0161 findings + code)

The wall: backlink keys `{ref}\x00{source}\x00{path}` share prefixes (`at://did:plc:`, `did:plc:`, `did:web:`) so numeric `presplit=8` (which cuts the full byte-space uniformly) put essentially ALL backlinks into one range → one replica pair → its 32 GiB zone slab fills with ~19.5 GiB of 99.8%-live data in ~60 min. `pd.rs` itself warns: "numeric K only spreads first-byte-uniform keys; presplit with real-key boundaries".

Code facts verified:
- `create-zone <z> <rf> sorted presplit=key,key,...` accepts up to 255 explicit strictly-increasing boundary keys (admin.rs parse_presplit); empty-zone presplit installs canonical tokens that spread across replica sets.
- One engine per (zone × shard-partition): ranges route to intra-node shards by hash64(token)%4 and SHARE the zone's slab there — deep presplit does NOT multiply slab consumption.
- Zone slabs are fixed up-front allocations (`table.alloc`) of `zone_region_mib` per zone per 256 GiB partition. 3 zones exist (default + records + backlinks) → at 96 GiB/zone 3×96=288 > 256 GiB region would fail alloc; **80 GiB (3×80=240 ≤ 256) is the safe max**.

## Levers chosen (measure-first math)

1. **Explicit real-key presplit for backlinks (~48 ranges)**: boundaries aligned to the actual key distribution — `at://did:plc:` + each base32-sortable char (31 boundaries → 32 uniform URI-ref ranges; plc ids are base32(sha256), uniform), `did:plc:` + every 2nd char (15 boundaries → 16 DID-ref ranges), plus `did:web:`. 47 boundaries → 48 ranges, matching the ~60/40 URI/DID backlink volume split at ~2%/range. Spread over 12 engines (3 nodes × 4 shards, RF2) instead of 1 pair.
2. **zone_region_mib 32768 → 81920 (80 GiB/zone slab)** on all three node.confs: 2.5× per-engine capacity; fits 3 zones in the 256 GiB partition region.
3. Re-home stays OFF (`VALS_PD_REHOME_ENABLED` unset): 0155 churn-robustness still open; with spreading from first write, populated-range migration shouldn't be needed.

Capacity math for the runbook: ~44 GiB/h raw backlinks cluster-wide at full rate (RF2) ÷ 12 engines ≈ 3.7 GiB/engine/h → 32 GiB slab ≈ 7–8 h, 80 GiB ≈ 19 h to the device floor if spreading holds. RAM (40 GiB budget/node ≈ 120M keys @ ~350 B/key, ~50M replica-keys/node/h) becomes the next ceiling at ~2.5–3 h — that sheds gracefully (ram_busy) per 0134/0153, not a wedge.

## Execution

1. Prep repo artifacts: new run dir `bench/bootes-soak-lab/run-<TS>-0164-presplit/` with adapted `collect.sh` (fix the row bug: computed `$SHE$SHF$SHD` shadow columns were never echoed — header/data misalignment that lost them last run).
2. Fleet prep per runbook §0/§0.5: health sanity, SPDK bindings, rsync HEAD (9827811) + parallel release builds on all 3 boxes; `sed` zone_region_mib=81920 into each node.conf (format=true reformats anyway).
3. Fresh cluster per runbook §1–2 (PD raft wipe; nodes with VALS_RAM_BUDGET_BYTES=40 GiB).
4. Zones per §3, but backlinks created with the explicit boundary list; records presplit unchanged. Verify PD `zones` + range counts.
5. Preflight §4: records parity AND a backlinks-zone batch-many/check-many round-trip (`at://did:plc:…` prefix) to confirm client routing parity on the deep presplit.
6. Launch bootes 300/64 per §5; start the collector (SAMPLES≈300 × 60 s) in the run dir; watcher in background.
7. Monitor ~4 h: min_zone_free_segments per node bounded away from floor (or balanced decline), busy/ram_busy, RSS bounded, per-zone detail from half-hourly snapshots; grab a mid-run PD skew read.
8. Bounded stop per §7; final metrics/logs into the run dir; FINDINGS.md with per-engine balance analysis + wall verdict.
9. Docs: runbook §3 create-zone command replaced with the boundary form + capacity math; wall gotcha updated; note the collect.sh fix. Re-scope note on 0130 (re-attempt now unblocked if this passes).
10. Review: repo changes are bench artifacts + runbook/docs only (no engine code) → simple tier self-review against acceptance criteria, then commit (memory: implementer/sol backend 400s persist; coordinator-run per recorded precedent).

Fallbacks: if one pair still runs hot (skewed ref popularity), evidence lands in FINDINGS and the residual becomes a follow-on (bounded-load placement 0120 / load splits 0118); if RAM shed dominates before multi-hour, that's the documented next ceiling, still satisfying "balanced decline consistent with total-capacity limits".

### Starting points
- crates/vals-server/src/admin.rs:125-215 parse_presplit (explicit boundaries)
- crates/vals-cluster/src/zonestore.rs:895-915 table.alloc — up-front zone slabs
- plans/bootes-soak-runbook.md §3 zone creation
- bench/bootes-soak-lab/run-20260809T163755Z-0161-shadow/collect.sh line 72 (missing $SHE$SHF$SHD)
- boxes: ~/vals/run/node.conf zone_region_mib=32768 → 81920

## Work log
- 2026-08-09 plan: ## Diagnosis (from 0161 findings + code)  The wall: backlink keys `{ref}\x00{source}\x00{path}` share prefixes (`at://did:plc:`, `did:plc:`, `did:web:`) so numeric `presplit=8` (which cuts the full by
…[truncated]
- 2026-08-09 context hints: 5 recorded with plan
- 2026-08-09 review tier: simple (coordinator self-review)
- 2026-08-09 decision: accept — commit: 0164: backlinks capacity wall ELIMINATED — distribution-aligned presplit + 80 GiB zone slabs carry a full-rate bootes soak 3h44m with zero backpressure (was: producer stalled at ~62 min in every pri
…[truncated]
- 2026-08-09 usage: 39,858 tok (in 140, out 39,718, cache_r 6,796,427, cache_w 590,833) · cost n/a (unpriced)

---
id: "0158"
title: Attribute the soak's 1.57× segment-space amplification and the wedge-time used_segments accounting gap
status: done
priority: 3
created: "2026-08-09"
updated: "2026-08-10"
depends_on:
    - "0157"
spec_refs: []
---

## Description
Open question inherited from task 0157 / the 0130 gate-A/B soak (`bench/bootes-soak-lab/run-20260808T223525Z-gate-ab/FINDINGS.md`): node-wide segment-space amplification measured 1.57× (n3 ~34 GiB raw key+value vs 53.4 GiB in segments), and at wedge time n2's node-wide `used_segments` (~40.6k) did not close against the fully-consumed 32,768-segment zone-2 region plus ~19.2 GB of records-zone values.

The 0157 local repro established that allocator accounting closes exactly (`used + free == total`) and bounded amplification (<4× on a small file-device engine), and that the wedge itself was a reserve-collapse bug (now fixed), but it did NOT settle where the soak's 1.57× amplification comes from (run-index overhead vs vlog dead space awaiting merge vs segment-granularity rounding of small backlinks values) nor confirm the gauge-staleness explanation for the accounting gap (gauges sample on the sweep cadence and can freeze on a wedged node).

Acceptance criteria:
- On the next soak (task 0130 re-run, which now exports `free_segments`/`min_zone_free_segments`), collect per-zone used/free plus raw ingested bytes and attribute the amplification factor to its components (run indexes, dead vlog, segment rounding), or
- A targeted local experiment reproduces ≥1.5× amplification with soak-like value sizes and explains the mechanism.
- The FINDINGS.md accounting anomaly is explained (gauge staleness confirmed or refuted).

## Acceptance criteria

## Plan

Close the attribution task by consolidating the already-landed 0159/0160 local repro and the 0161 hardware dump rather than rerunning a redundant long soak.

1. Add a focused task-0158 findings artifact under `bench/` that reconstructs the original 1.57× observation and separates the components supported by evidence: the dominant pre-0160 runtime orphan leak from index-only merges (1663/2048 segments in the rotating-overwrite repro; roughly the soak's missing ~30 GiB), ordinary record framing/key bytes, run-index segments, and bounded segment-tail rounding. Cite the exact existing tests, task findings, and hardware artifacts.
2. Use the post-0160 0161 live dump as the hardware cross-check: 19,052 referenced segments held 38.20M backlink records with 5.77 GB keys, 12.25 GB values, and ~1.76 GB record headers, only 0.2% duplicate keys. Explain that this near-closing result rules out severe small-value segment rounding/run-index amplification after orphan reclaim and confirms the old excess was runtime-orphan space.
3. Resolve the original `used_segments` anomaly explicitly in the original gate-arm `FINDINGS.md`: refute gauge staleness using the unchanged final scrape after two wedged hours, the exact local allocator invariants (`used + free == total`, and after 0160 `used == referenced`), and later soak snapshots where `used_segments + free_segments` equals configured per-engine capacity summed node-wide. Correct the arithmetic error: a fully allocated 32,768-segment zone is already included in the node-wide used gauge, while the cited ~19.2 GB was an unmeasured/inferred payload estimate, not an additional allocation count or a per-zone dump.
4. Keep this as evidence/documentation only unless a genuine inconsistency is found; the underlying leak and regression tests already landed in 0160. Run focused tests that pin orphan reclamation and space closure, plus markdown/link sanity checks. Do not touch unrelated dirty backlog files.

### Starting points
- bench/bootes-soak-lab/run-20260808T223525Z-gate-ab/FINDINGS.md §Space accounting doesn't close
- bench/bootes-soak-lab/run-20260809T063307Z-post0157/FINDINGS.md §Space anomaly
- bench/bootes-soak-lab/run-20260809T163755Z-0161-shadow/FINDINGS.md §The decisive measurement
- backlog/0160-reclaim-orphaned-vlog-segments-index-only-merges-l.md root cause and acceptance evidence
- crates/vals-store/src/engine.rs tests `pressure_floor_overwrite_churn_stays_live_without_zero_gain_merges` and orphan reclaim tests near lines 11500/12400

## Work log
- 2026-08-10 plan: Close the attribution task by consolidating the already-landed 0159/0160 local repro and the 0161 hardware dump rather than rerunning a redundant long soak.  1. Add a focused task-0158 findings artifa
…[truncated]
- 2026-08-10 context hints: 5 recorded with plan
- 2026-08-10 context hints: Add a concise `bench/space-amplification-0158/FINDINGS.md` or equivalently clear task-specific artifact.; Original final metrics: node 2 `vals_used_segments 40653`; broken collector timeseries doubled
…[truncated]
- 2026-08-10 preload: 6 file(s), ~20 KiB seeded into implementer context
- 2026-08-10 implementer report: Implemented task 0158 as evidence/documentation only.  Changes: - Added `bench/space-amplification-0158/FINDINGS.md` with the full attribution and source links. - Reconstructed the original n3 1.57× 
…[truncated]
- 2026-08-10 review tier: high-powered — reviewers: sol, claude
- 2026-08-10 review (sol): accept — The documentation-only change satisfies task 0158. It reconstructs the original ratio, uses the post-0160 hardware evidence to separate framing, vlog packing/tails, run-index/metadata cost, and duplic
…[truncated]
- 2026-08-10 review (claude): accept — Documentation/evidence-only close-out of task 0158. The new bench/space-amplification-0158/FINDINGS.md and the amended gate-ab FINDINGS correctly attribute the 1.57× amplification (ordinary framing +
…[truncated]
- 2026-08-10 revision: Updated the original gate-arm `FINDINGS.md` to remove the stale claim that the refuted arithmetic proved the regions should not have been full. It now records that the 0159/0160 local repro showed the
…[truncated]
- 2026-08-10 review (sol): accept — The revision addresses the prior finding. The gate-arm findings no longer rely on the refuted arithmetic to characterize the earlier corpus replay; they now accurately distinguish the proven orphan-le
…[truncated]
- 2026-08-10 review (claude): accept — The revision correctly fixes the one remaining internal inconsistency: the gate-ab FINDINGS paragraph on the 2026-08-05 corpus replay no longer relies on the refuted 'regions should not have been full
…[truncated]
- 2026-08-10 decision: accept — commit: 0158: attribute segment-space amplification
- 2026-08-10 usage: 7,988,779 tok (in 2,050,424, out 41,651, cache_r 6,224,790, cache_w 37,608) · cost n/a (unpriced)
  coordinator: 3,814,908 tok (in 996,425, out 8,371, cache_r 2,810,112, cache_w 0) · cost n/a (unpriced)
  implementer: 3,127,642 tok (in 602,295, out 18,339, cache_r 2,507,008, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 1,038,209 tok (in 451,678, out 6,947, cache_r 579,584, cache_w 0) · cost n/a (unpriced)
  reviewer:claude: 8,020 tok (in 26, out 7,994, cache_r 328,086, cache_w 37,608) · cost n/a (unpriced)

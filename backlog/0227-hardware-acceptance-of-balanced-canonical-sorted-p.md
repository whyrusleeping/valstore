---
id: "0227"
title: Hardware acceptance of balanced canonical sorted placement on lab (0226 gates, 0225 shapes)
status: done
priority: 2
created: "2026-08-20"
updated: "2026-08-20"
depends_on:
    - "0226"
spec_refs: []
---

## Description

Task 0226 shipped the mechanism: PD assigns balanced opaque tokens (replica slots, primary roles, per-shard/engine assignment) at canonical placement events — new-zone presplits, empty-range canonical splits on V4 maps, and debt re-homes on V4 maps — gated fail-closed on `VALS_PD_OPAQUE_RANGE_MAPS=1` + all-node FEATURE_OPAQUE_RANGE_MAP capability certification (exact epoch-tuple bound). Local tests prove 16/16/16 replica slots, 8/8/8 primaries, per-engine max−min ≤ 1 on the 24-range/3-node/RF2 fixture. This task runs 0226's hardware acceptance gates on lab.

Setup notes:
- Set `VALS_PD_OPAQUE_RANGE_MAPS=1` on PD; nodes need no config change (feature bit is negotiated).
- The preserved 0225/0224 corpus's `smix` zone has legacy canonical placement (18/18/12) and will NOT converge by itself — either recreate the sorted zone fresh with presplit (exercises the primary new path; 20M × 256 B regeneration is quick relative to the mandatory warm-up) or use the opt-in balance executor to converge it (0181/0182 territory).
- Follow the 0224/0225 warm-up discipline: after reopening a large corpus, warm under load until a quiescent probe is flat (~60–90 min) before acceptance numbers. Compare whole-arm distributions, alternate arms back-to-back.
- Placement attribution: `pd_canonical_placements_balanced` / `pd_canonical_placements_fallback` counters + per-node replica-write counters + per-shard /proc CPU deltas (0225 capture scripts in bench/lab-sorted-gaps-20260820-0225/).

## Outcome

All four 0226 gates met on lab (bench/lab-canonical-balance-20260820-0227/FINDINGS.md): replica-slot balance exact (write share 33.3/33.3/33.3, hot/cold 1.000× vs legacy 1.50×; balanced=24/fallback=0); writers=8 scalar sorted 1.22–1.40× of unsorted at matched churn (vs 2.3–3.3× in 0225) with equal p99 and shard CPU max/mean ≤1.22; batch-under-churn no longer nonlinear (1.27×); no quiescent or writers=2 regression (sorted best 3.00M vs 2.89M keys/s; w2 sorted ahead). Zero consistency errors. Recommendation: default VALS_PD_OPAQUE_RANGE_MAPS on (fail-closed); flip deferred to user. Primary drop+recreate path exposed a real bug — live drop-zone wedges teardown (Rc leak) and zone-id reuse silently loses half the replica writes — filed as 0228; session used fresh format + symmetric maturation instead. Residual ~1.2–1.4× sorted w8 gap filed as proposed 0229. Cluster torn down clean, format=false.

Commit: bench: hardware-accept 0226 balanced canonical sorted placement on lab; file 0228 drop-zone teardown wedge (0227)

---
id: "0383"
title: 'Overwrite/delete garbage in frozen runs: only an uncapped all-or-nothing shadow fold reclaims it; tombstone GC stalls once frozen runs exist (0381 follow-up)'
status: done
priority: 2
created: "2026-10-06"
updated: "2026-10-10"
depends_on:
    - "0381"
spec_refs: []
---

## Description

From the 0381 review (2026-10-06). With frozen runs (≥ cap, never recurring or forced inputs), garbage from overwrites and deletes inside frozen runs has only one reclaim path:
- **Pressure merges** take a capped suffix and stop at the first frozen run.
- **Aged vlog GC** counts shadowed values as live.
- **Shadow folds** are disabled at the floor (`space.rs` ~554).
- **The only remaining path** is a shadow fold over `t..n` (`space.rs` ~566-569), from the oldest run with garbage. It is uncapped, unpaced and gated on gain ≥ churn/4. At scale that is all or nothing: nothing, or a rewrite of most of the engine. That is the same class of huge merge 0381 removes.
- **Tombstone GC effectively stops** once any frozen run exists: recurring merges never cover the whole run list, and lone-run GC needs every run under the cap. The 0381 reviewer's model test saw 0 tombstones dropped.

The existing reclaim test (`frozen_runs_keep_overwrite_and_delete_garbage_bounded`) uses a 1 MiB cap and ~1 frozen run, and does not assert that deleted keys stay absent.

**Work:**
1. **A gate:** an overwrite/delete workload at realistic frozen-run counts (≥ 50 per engine). Measure space amplification over time, tombstone retention, reclaim merge sizes and pacing, and check that deleted keys stay absent (resurrection).
2. **A design for capped, paced reclaim of frozen-run garbage.** Options:
   - per-run garbage estimates with bounded windowed folds that preserve version order (windows need not be suffixes; newest-wins is by version — see the 0381 review);
   - tombstone GC on windows whose older runs provably contain no older versions of the dropped keys (key-filter or bloom checks against older runs);
   - WiscKey-style vlog liveness checks against the winning version.
3. **Implement and A/B.**

**Priority:** below the creates-only ingest work, but required before claiming overwrite/delete workloads are supported at multi-billion-key scale with 0381.

## Outcome

L1 gate (97 frozen runs, 1.5M overwrite/delete ops, full resurrection audit): the old uncapped fold made 31–39 MiB merges (60–79× cap), collapsed the frozen tier and wedged at 3.76× space; the estimator was inflated by bloom FP compounding. Implemented capped shadow folds (≤cap window in place, sources streamed as keys/versions on the maintenance device, reader tie-break drop rule, tombstone GC only with complete sources, bounded paced turns), FP-corrected estimator, pre-floor gainless pressure → recurring fallback. A/B: uniform ≤2.70×, 152k tombstones collected; hot 1.0 vs 8.6 GiB merge writes (max 2.98× vs 2.55×); 0 stale/lost/resurrected keys; cost 15–32 GiB streamed source bytes. Regressions added. No hardware arm; follow-ups proposed in 0413.

Commit: fix(0383): capped shadow folds stream other runs read-only instead of uncapped suffix folds; FP-corrected shadow estimate; L1 frozen-garbage gate A/B (old wedged at 3.76x, new <=2.70x)

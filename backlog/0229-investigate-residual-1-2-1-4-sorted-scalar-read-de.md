---
id: "0229"
title: Investigate residual ~1.2–1.4× sorted scalar read deficit at writers=8 after balanced placement (0227 observation)
status: done
priority: 3
created: "2026-08-20"
updated: "2026-08-21"
depends_on: []
spec_refs: []
---

## Description
Observed during 0227 hardware acceptance (bench/lab-canonical-balance-20260820-0227/FINDINGS.md): with canonical sorted placement fully balanced (physical write share exactly 33.3/33.3/33.3, shard CPU max/mean ≤1.22, fleet shard CPU mean only ~30%), writers=8 scalar sorted reads still trail unsorted by 1.22–1.40× (three matched-churn A/Bs at ~100–113k bg wr/s: 192k/166k/169k vs 234k/232k/214k rd/s). Latency-shaped: sorted p50 1–2 ms vs unsorted 1 ms at equal p99 (32.8 ms). Ruled out by arm counters: placement skew, CPU saturation, scan-validation (all vals_scan_* deltas 0), merge load (run_merges comparable), busy rejections / quiesce timeouts (0). Batch-read churn shows a smaller version (1.27× at width 256, 1.13× at width 64). Candidate suspects: ordered-path apply/read costs under high write concurrency (range-serialized apply, ordered memtable/run structures), winner-directory interaction with churn on sorted zones.

## Acceptance criteria
- Mechanism identified with counter/profile evidence (local repro or targeted hardware A/B).
- Either a fix that brings writers=8 scalar sorted within ~1.1× of unsorted at matched churn, or a documented decision that the residual cost is inherent to the ordered path.

## Work log

- 2026-08-21 result: Closed on lab. Mechanism was write-side work delaying the same shard executor that reaps read completions: stable sorted applies deep-cloned the 24-range map after the apply timer, scalar winner-directory digests redundantly searched the mutable index twice, and single-key fencing cloned config + allocated a batch-dedup map. Those fixes gave sorted +11.4% reads at identical post-restart churn with an unsorted +0.5% control. The mature corpus also reproduced task 0201's previously-missing generic planner wall (`victim_lives_ascending` 21–30% sorted / 36–37% unsorted hottest-shard cycles); restored the semantics-equivalent linear aggregation under its differential test, and final profiles remove the symbol. Final strict writers=8 matched-churn arms: sorted 684k/656k/603k reads/s at 152k/145k/132k writes/s versus unsorted 628k/563k at 141k/121k writes/s — sorted 4.4–8.9% ahead while accepting 3.3–9.2% more churn, all error gates zero. Full store+cluster tests pass. Artifacts: `bench/lab-sorted-breakthrough-20260820-0229/FINDINGS.md`.

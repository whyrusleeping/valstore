---
id: "0196"
title: 'Winner-directory control-byte probing: eliminate random full-key misses'
status: done
priority: 1
created: "2026-08-12"
updated: "2026-08-12"
depends_on: []
spec_refs:
    - docs/out-of-core-index.md#Optional winner-directory performance tier
    - bench/lab-breakthrough-20260812-0195/FINDINGS.md
---

## Description
Optimize the current latency profile's top steady write-path cost. Add a compact SwissTable-style control/fingerprint sidecar to WinnerDirectory so unsuccessful open-addressing probes reject without dereferencing random PackedWinner/key-arena locations; retain full-key comparison as the only collision authority.

Acceptance criteria:
- Measure probe lengths, fingerprint matches, full-key comparisons, memory bytes/key, and serving instructions/write before changing representation.
- Preserve collision safety, all winner-directory lifecycle/recovery semantics, and capacity profile behavior.
- Add adversarial collision and resize/remove/rebuild tests.
- Matched mature hardware A/B at 16x512 and 32x512 records throughput, p50/p99, PMU, RAM, and maintenance state; keep only for a repeatable material gain with an explicit bytes/key cost.

## Acceptance criteria

## Plan

1. Preserve the current dirty-tree baseline and inspect WinnerDirectory plus the existing lab task-0195 benchmark/run artifacts; do not absorb unrelated work. Before changing the representation, add or use zero-production-cost test/benchmark instrumentation to measure successful/unsuccessful probe lengths, candidate-bucket/full-key comparisons, current allocated bytes/key, and collect a matched latency-profile mature-hardware baseline at 16x512 and 32x512 including throughput, p50/p99, instructions/write, RSS/directory bytes, and flush/merge/dirty maintenance state. Retain the raw evidence under a task-0196 bench directory.
2. Add a compact per-bucket control-byte/fingerprint sidecar to WinnerDirectory. Derive a deterministic fingerprint from the already-computed full hash; probe controls first and dereference PackedWinner/key-arena data only on matching fingerprints. Preserve full-key equality as the sole hit authority, tombstone semantics, load factor, growth/shrink, arena compaction, rebuild/finalization, and capacity-profile disabled behavior. Include sidecar capacity in directory byte accounting and avoid new write-path hashing or production telemetry overhead.
3. Add focused tests for same-home and same-fingerprint adversarial collisions (including unequal full keys), successful and unsuccessful probes, deletion/tombstone reuse, moved-last-entry repair, resize/shrink, clear, rebuild/recovery, argmax, and byte accounting. Report instrumented post-change probe/fingerprint/full-key counts against the exact baseline workload.
4. Run targeted vals-store tests and the full debug vals-store and vals-cluster library suites without rustfmt-ing unrelated code. Then build and deploy the candidate to the same mature lab corpus/config, repeat matched 16x512 and 32x512 latency-profile passes, and capture the same throughput/latency/PMU/RAM/maintenance fields. Use multiple synchronized passes or explicitly reject maintenance-overlap outliers rather than comparing a single lucky pass.
5. Write FINDINGS.md documenting baseline/candidate representation measurements, correctness checks, exact bytes/key cost, raw artifacts, and matched hardware A/B. Keep the optimization only if the gain is repeatable and material; otherwise revert the representation while retaining the evidence and conclusion. Update the winner-directory design/deployment docs only for the retained behavior and measured cost.

## Work log
- 2026-08-12 plan: 1. Preserve the current dirty-tree baseline and inspect WinnerDirectory plus the existing lab task-0195 benchmark/run artifacts; do not absorb unrelated work. Before changing the representation, ad
…[truncated]
- 2026-08-12 context hints: Core representation: crates/vals-store/src/engine.rs:30-271 (WinnerDirectory).; Existing focused tests start near crates/vals-store/src/engine.rs:11495; lifecycle/recovery tests continue through ~1179
…[truncated]
- 2026-08-12 preload: 5 file(s), ~41 KiB seeded into implementer context
- 2026-08-12 implementer report: Implemented task 0196’s control-byte winner-directory representation in `crates/vals-store/src/engine.rs`: added a parallel 7-bit fingerprint/control vector, control-first probing, synchronized empt
…[truncated]
- 2026-08-12 revision: Addressed the review by completing a fresh matched mature-hardware A/B on devbox → lab-1/2/3 and replacing the provisional FINDINGS with the measured decision.  Hardware work: - Preserved the
…[truncated]
- 2026-08-12 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-12 review (sol): revise — The control-byte implementation itself is coherent and collision-safe, and the focused lifecycle tests and byte accounting are well designed. However, the retained performance decision is not supporte
…[truncated]
- 2026-08-12 review (sol#2/sol): revise — The control-byte implementation itself is clean and appears correctness-preserving: controls remain synchronized through insert/remove/rebuild/clear, full-key equality remains authoritative, byte acco
…[truncated]
- 2026-08-12 revision: Fixed both rejected evidence areas.  1. Replaced the independently seeded hardware comparison with a true same-corpus alternating gate: - Backed up all node configs, changed `format=true` to `format=f
…[truncated]
- 2026-08-12 review (sol): accept — The revision addresses both prior findings. It adds representative test-only hit/miss probe distributions with concrete baseline-equivalent and control-byte comparison counts, while preserving zero pr
…[truncated]
- 2026-08-12 review (sol#2/sol): accept — The revision addresses the prior blocking concern with a credible same-device, format-disabled B/A/B/A hardware gate. The retained runs cover both required load shapes with all passes, PMU/RAM/mainten
…[truncated]
- 2026-08-12 revision: Completed the accepted-review evidence hygiene cleanup:  - Removed all three identical ~9.3 MB candidate executable blobs from `raw/controlled/binaries/`; no baseline executable was added. Binary iden
…[truncated]
- 2026-08-12 decision: accept — commit: store: add winner directory control bytes
- 2026-08-12 usage: 20,919,795 tok (in 4,391,967, out 100,308, cache_r 16,427,520, cache_w 0) · cost n/a (unpriced)
  implementer: 12,727,682 tok (in 1,125,391, out 43,123, cache_r 11,559,168, cache_w 0) · cost n/a (unpriced)
  coordinator: 3,408,591 tok (in 1,294,578, out 13,021, cache_r 2,100,992, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 2,601,098 tok (in 986,460, out 25,646, cache_r 1,588,992, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 2,182,424 tok (in 985,538, out 18,518, cache_r 1,178,368, cache_w 0) · cost n/a (unpriced)

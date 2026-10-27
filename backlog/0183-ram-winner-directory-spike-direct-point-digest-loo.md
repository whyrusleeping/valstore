---
id: "0183"
title: 'RAM winner directory spike: direct point/digest lookup and read-free authoritative LWW'
status: done
priority: 1
created: "2026-08-11"
updated: "2026-08-11"
depends_on: []
spec_refs:
    - docs/out-of-core-index.md
    - docs/blind-append-lww.md
    - ARCHITECTURE.md#Status
---

## Description
Prototype and hardware-gate an optional per-engine collision-safe RAM directory mapping each key to its maximum Version and winning ValuePtr. The purpose is to restore the intended mature-LSM read shape: a value owner performs one direct value read and a digest owner performs zero device reads, while unconditional writes retain read-free winner resolution and exact Merkle updates.

Acceptance criteria:
- First build a deliberately simple feature-gated prototype with no on-disk format commitment; it must be easy to disable and must not replace the capacity-oriented out-of-core path.
- Directory state remains correct across append, stale/lower candidates, tombstones, flush, index-only merge, value co-compaction/pointer rewrite, reclaim, recovery/reopen, truncate, and generation races. Key identity is collision-safe; fingerprints alone are never correctness authority.
- Point Read and Digest use the directory to avoid run-probe block reads. Reads still validate the referenced record/key/version and fail closed on stale directory generations or I/O errors.
- Feature-enabled unconditional Apply/BatchApply can determine the authoritative winner without a run read, preserve blind-append acknowledgement/CAS semantics, and update Merkle state exactly rather than creating unknown-old dirty work.
- Focused correctness/crash tests plus full debug vals-store/vals-cluster suites pass.
- A mature flushed-corpus A/B on the current four-shard RF=2 lab cluster records logical throughput, p50/p99, client/server CPU and instructions/op, run/value device reads, probe counters, errors, and incremental bytes/key. Continue to production compaction only if the crude prototype improves strong-read throughput by at least 1.8x with probe blocks near zero and no material p99/write regression; otherwise document and stop.
- If the spike passes, replace crude storage with a measured compact representation and document the shipped performance-tier memory cost and recovery semantics.

## Acceptance criteria

## Plan

Resume the existing crude, default-off winner-directory spike rather than redoing it. First audit the current implementation and its focused tests against every correctness transition in the acceptance criteria, preserving the capacity-oriented path when the feature is disabled. Replace the per-key `HashMap<Box<[u8]>, IndexEntry>` with a measured compact collision-safe representation built from dense key storage plus packed lookup/winner metadata; full-key comparison remains authoritative, all mutations/rebuilds preserve maximum-version semantics, and async point reads continue to validate key/version and generation before returning data. Extend focused tests for collisions, lower/stale candidates, tombstones, flush/merge/co-compaction/reclaim/reopen/truncate/forget and staged-read generation races, and retain exact Merkle plus blind-append semantics in cluster paths. Update the design/performance docs with recovery behavior, feature gating, measured memory accounting, and the first hardware-gate result. Run the focused tests and full debug vals-store/vals-cluster suites. Repeat the mature lab A/B using the existing task-0183 harness, recording compact bytes/key, throughput/latency/CPU/I/O/errors and write behavior; keep the feature default-off unless the acceptance gates are met. Review the complete task-0183 diff separately from pre-existing task-0181/staged changes, revise as needed, then close and commit only when all acceptance criteria are supported by code, tests, and retained evidence.

### Starting points
- crates/vals-store/src/engine.rs: current `WinnerDirectory` crude HashMap and staged read paths
- crates/vals-store/src/vlog.rs: `ReadPlan::read_value_checked`
- crates/vals-cluster/src/net/server.rs and replica.rs: directory generation/Apply integration
- bench/lab-winner-directory-20260811-0183/FINDINGS.md: first hardware gate and required compact follow-up
- docs/out-of-core-index.md#Optional winner-directory performance tier

## Work log
- 2026-08-11 plan: Resume the existing crude, default-off winner-directory spike rather than redoing it. First audit the current implementation and its focused tests against every correctness transition in the acceptanc
…[truncated]
- 2026-08-11 context hints: 5 recorded with plan
- 2026-08-11 context hints: Current crude implementation is unstaged in engine.rs/index.rs/vlog.rs plus cluster server/replica/client-observe integration.; bench/lab-winner-directory-20260811-0183/FINDINGS.md lines 95-150 ide
…[truncated]
- 2026-08-11 preload: 11 file(s), ~64 KiB seeded into implementer context
- 2026-08-11 preliminary implementation: Replaced the crude boxed-key HashMap with a collision-safe dense-key arena, 48-byte packed winner slots, and an open-addressed u32 lookup table; the first compact A/B passed throughput and memory gates but review required additional synchronous validation, recovery-capacity, race, PMU, and device-I/O evidence.
- 2026-08-11 implementer report: Implemented and hardware-gated task 0183.  Code: - Replaced the crude `HashMap<Box<[u8]>, IndexEntry>` with a compact collision-safe directory: dense key arena, 48-byte packed winner slots, and an ope
…[truncated]
- 2026-08-11 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-11 review (sol): revise — The compact directory design, lifecycle hooks, exact-token read path, LWW/Merkle integration, focused tests, and retained throughput/memory evidence are substantial. However, one point-read path still
…[truncated]
- 2026-08-11 review (sol#2/sol): revise — The winner-directory read/write integration is generally coherent: full-key collision checks are used, exact versions/pointers are packed, network point reads validate key/version and revalidate autho
…[truncated]
- 2026-08-11 review closure: Added fail-closed full-key/exact-version validation to synchronous directory reads and focused wrong-key/version tests; changed recovery rebuild to dynamic unique-winner growth plus final shrink and proved capacity stays proportional across 12 overlapping runs; added same-key and storage-generation staged direct-read retry tests. Documented startup fail-closed versus maintenance fallback. Added explicit Prometheus value-device read/block counters with HELP/tests, then rebuilt and reran matched reviewed control/directory arms with external-client server PMU, dedicated-client PMU, run/index and value-device I/O, p50/p99, throughput, errors, RSS/bytes-key, fresh inserts, and overwrite passes. Final tuned reads were 2.320M/s (2.51x), zero run probes, one value read, 7.22 ms p99, 62.9 incremental RSS B/physical key, and 1.93x mature overwrites. Full debug suites passed (vals-store 301/7 ignored; vals-cluster 523/3 ignored). Feature remains default-off for its RAM and synchronous-open costs.
- 2026-08-11 revision: Addressed all review findings and re-closed task 0183.  Correctness/read validation: - Added synchronous `ValueLog::read_checked` and changed directory-backed `Engine::get` to validate the decoded rec
…[truncated]
- 2026-08-11 review (sol): accept — The revision addresses the prior findings. Directory-backed synchronous and asynchronous point reads now validate the full key and exact version, with focused corruption and generation-race coverage;
…[truncated]
- 2026-08-11 review (sol#2/sol): accept — The revision addresses the prior correctness and evidence gaps. Directory rebuild now grows with observed unique winners and compacts before publication; checked synchronous and asynchronous point rea
…[truncated]
- 2026-08-11 revision: Addressed the remaining batched point-read validation issue.  Changes: - Extended batched resolution metadata to retain each directory-selected exact version alongside its output slot. - Added `ValueL
…[truncated]
- 2026-08-11 review (sol): accept — The additional revision correctly closes the batched point-read validation gap. Batched resolution now preserves each selected winner version, and both synchronous and asynchronous multi-get paths val
…[truncated]
- 2026-08-11 review (sol#2/sol): accept — The remaining batched-read gap is correctly closed. Directory-backed `multi_get` and `multi_get_async` now retain each resolved version, pair it with the original key, and decode every returned record
…[truncated]
- 2026-08-11 decision: accept — commit: store: add compact winner directory tier
- 2026-08-11 usage: 63,309,592 tok (in 7,764,503, out 98,561, cache_r 55,446,528, cache_w 0) · cost n/a (unpriced)
  implementer: 42,506,802 tok (in 2,945,992, out 57,962, cache_r 39,502,848, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 10,411,766 tok (in 1,943,840, out 15,830, cache_r 8,452,096, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 8,415,835 tok (in 1,747,105, out 17,338, cache_r 6,651,392, cache_w 0) · cost n/a (unpriced)
  coordinator: 1,975,189 tok (in 1,127,566, out 7,431, cache_r 840,192, cache_w 0) · cost n/a (unpriced)

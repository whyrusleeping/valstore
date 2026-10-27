---
id: "0184"
title: Mature-corpus 4/8/12/16-shard scaling sweep on lab
status: done
priority: 2
created: "2026-08-11"
updated: "2026-08-12"
depends_on:
    - "0183"
spec_refs:
    - ARCHITECTURE.md#Status
    - bench/lab-shard-scaling-20260810/notes.md
---

## Description
Measure how far the three 24-core lab nodes scale when serving width is increased beyond four shards per node, using the winning task-0183 configuration if its gate passes and the baseline otherwise.

Acceptance criteria:
- Fresh, comparable 4/8/12/16-shard arms keep corpus maturity/run state comparable per shard and use enough independent external generation to avoid a client-host ceiling.
- Record repeated strong-read and overwrite distributions, latency, per-serving-thread CPU/instructions, host CPU, NVMe qpair/read/write utilization, NIC bytes/packets, errors, run state, RAM, and maintenance activity.
- Identify the first server, NIC, device, RAM, qpair, or load-generation knee; do not infer linear scaling from one arm.
- Preserve and restore box configuration, capture exact source/binary/config/commands, and publish capacity guidance for the best operational shard count.

## Acceptance criteria

## Plan

Build a dedicated, reproducible task-0184 harness from the accepted task-0183 winner-directory configuration rather than reusing the old out-of-core 0146 ceiling. Preserve the inactive fleet's standard four-shard node/PD configs and exact source/binary/environment identity, then generate fresh 4/8/12/16-shard-per-node configs with one listening endpoint and PD member per shard. Keep storage maturity comparable by loading 4M deterministic 100-byte logical keys per shard (16M/32M/48M/64M per arm), verify every arm has flushed mature runs and full winner-directory coverage, and reject/reload any non-comparable arm. For each topology, sweep independent external client process/coordinator/in-flight shapes to prove the generator is not the ceiling; retain all sweep points and select a stable knee. At that shape collect at least three strong-read and three mature-overwrite repetitions with sampled latency and correctness, synchronized per-node/per-serving-thread perf stats, /proc and host CPU snapshots, NIC bytes/packets, SPDK/NVMe qpair and device counters, Prometheus run/value I/O, RAM/swap, run state, errors, and maintenance deltas. Use full raw captures plus a parser-generated summary to identify the first measured server/NIC/device/RAM/qpair/generator knee without extrapolating from one arm. Publish exact commands, provenance, limitations, distributions, and operational shard-count/capacity guidance in a new bench artifact and ARCHITECTURE status entry. Stop services, restore the original node/PD configs and process state, remove temporary state, and explicitly verify the fleet is clean. Review the evidence and documentation against every acceptance criterion; do not change product behavior for this measurement task.

### Starting points
- bench/lab-winner-directory-20260811-0183/{run_arm.sh,COMPACT_PROVENANCE.md,FINDINGS.md}: accepted winner-directory baseline and capture shapes
- bench/lab-shard-scaling-20260810/{commands.md,corrective-*-capture.sh,notes.md}: prior topology generation/capture patterns and generator-limit trap
- Use 4M logical keys per shard: 16M/32M/48M/64M across 4/8/12/16-shard arms
- Existing staged task-0181 changes are unrelated concurrent work; do not modify, reset, stage, or commit them

## Work log
- 2026-08-11 plan: Build a dedicated, reproducible task-0184 harness from the accepted task-0183 winner-directory configuration rather than reusing the old out-of-core 0146 ceiling. Preserve the inactive fleet's standar
…[truncated]
- 2026-08-11 context hints: 4 recorded with plan
- 2026-08-11 context hints: bench/lab-winner-directory-20260811-0183/{run_arm.sh,COMPACT_PROVENANCE.md,FINDINGS.md}: accepted winner-directory baseline and capture shapes; bench/lab-shard-scaling-20260810/{commands.md,corr
…[truncated]
- 2026-08-11 preload: 10 file(s), ~64 KiB seeded into implementer context
- 2026-08-11 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-11 review (sol): revise — The artifact is unusually thorough on provenance, teardown, raw evidence, maturity gating, load-shape sweeps, and the decisive 15-qpair/14-slot operational limits. The conclusion to retain four shards
…[truncated]
- 2026-08-11 review (sol#2/sol): revise — The sweep is carefully documented and the retained evidence convincingly identifies the eight-shard qpair-allocation wall, with good provenance, repeated read/overwrite throughput, CPU, NIC, RAM, run-
…[truncated]
- 2026-08-11 revision: Corrected hybrid PMU accounting to combine scaled atom/core events by recorded running fraction (23.4k instructions/read); added sampled end-to-end overwrite acknowledgements on a fresh valid four-shard arm; captured a separate `--no-scale`, SPDK-API-operation-triggered overwrite-only resource window with wrapper operation/byte/completion and explicit async-poll counters; made analysis and teardown fail closed; regenerated findings, provenance, summary, and architecture guidance. Final cleanup verified inactive services, zero processes/temp paths, and unchanged node/PD configs.
- 2026-08-11 re-review: Anchored clean reconstruction to immutable base `3582787`, checked in the corrected four-file measurement patch, and removed the dirty-HEAD dependency from `build-review-fix.sh`. Renamed overclaiming write-command/qpair-poll metrics to Rust SPDK API operation and explicit async-poll semantics, documented excluded C/synchronous polling and synchronous completion-error undercount, updated analyzer labels/tests, and validated reconstruction locally without touching the clean fleet.
- 2026-08-11 provenance closure: Recovered the exact executed hardware patch from dangling source blobs and checked it separately as `measurement-executed.patch` (`24ba8290…`, matching raw provenance). Added an isolated historical build path that reproduced `b2dc1ace…` locally and would fail closed on `c401d7fd…` in explicit remote mode; corrected committed instrumentation remains a separate artifact/build path. Analyzer now requires both exact patch identities.
- 2026-08-11 revision: Addressed all task-0184 review findings without modifying the unrelated staged task-0181 implementation.  Changed: - Corrected hybrid-PMU accounting in `analyze.py`: original scaled atom/core counts a
…[truncated]
- 2026-08-11 review (sol): accept — The revision addresses the prior blockers. Hybrid-PMU counts are now converted to residency counts (or captured with `--no-scale`), the fresh valid four-shard rerun provides three sampled end-to-end o
…[truncated]
- 2026-08-11 review (sol#2/sol): revise — The revision substantially closes the prior gaps: the fresh valid arm now has repeated end-to-end overwrite-ack latency, an uncontaminated overwrite-only profile, direct write-byte/activity evidence, 
…[truncated]
- 2026-08-11 revision: Addressed the remaining task-0184 re-review findings only; unrelated staged work and prior raw evidence were preserved.  Changes: - Made `build-review-fix.sh` post-commit reproducible:   - hard-anchor
…[truncated]
- 2026-08-11 review (sol): revise — The metric names and HELP now accurately describe the Rust API boundary and explicitly document the synchronous-error/internal-poll exclusions, resolving the prior observability concern. However, the 
…[truncated]
- 2026-08-11 review (sol#2/sol): revise — The metric semantics are now accurately renamed and qualified, and the committed-result reconstruction path itself is sound. The analysis still reproduces cleanly and the benchmark conclusions remain 
…[truncated]
- 2026-08-12 revision: Closed the task-0184 provenance gap without contacting the fleet or altering unrelated staged work.  Changes: - Recovered the exact executed hardware source from Git objects, not prose:   - executed b
…[truncated]
- 2026-08-12 review (sol): accept — The provenance gap is resolved. The exact four-file patch that produced the retained hardware binaries is now preserved as `measurement-executed.patch`, its SHA-256 exactly matches the original raw pr
…[truncated]
- 2026-08-12 review (sol#2/sol): accept — The revision now preserves and validates both source variants correctly: `measurement-executed.patch` exactly matches the recorded `24ba8290…` hardware patch and reconstructs the documented source b
…[truncated]
- 2026-08-12 decision: accept — commit: bench: gate lab shard scaling
- 2026-08-12 usage: 94,245,802 tok (in 8,973,476, out 189,446, cache_r 85,082,880, cache_w 0) · cost n/a (unpriced)
  implementer: 62,816,774 tok (in 3,519,352, out 98,958, cache_r 59,198,464, cache_w 0) · cost n/a (unpriced)
  coordinator: 15,424,923 tok (in 2,143,500, out 49,807, cache_r 13,231,616, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 9,779,323 tok (in 1,743,848, out 22,931, cache_r 8,012,544, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 6,224,782 tok (in 1,566,776, out 17,750, cache_r 4,640,256, cache_w 0) · cost n/a (unpriced)

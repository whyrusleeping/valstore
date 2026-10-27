---
id: "0186"
title: Blind BatchApply width sweep and bounded transparent write combining
status: done
priority: 2
created: "2026-08-11"
updated: "2026-08-12"
depends_on:
    - "0183"
spec_refs:
    - docs/blind-append-lww.md
    - docs/batch-writes.md
    - ARCHITECTURE.md#Status
---

## Description
Determine whether per-operation wire/scheduling overhead hides a multi-million-op blind writer, then add bounded transparent combining only if the existing BatchApply path proves the capacity.

Acceptance criteria:
- Add or reuse a synthetic benchmark that compares blind Apply/BatchApply widths 1/8/32/128/512 at fixed logical concurrency for both buffered W=2 and requested-persistent W=2 writes.
- Record logical and physical writes/s, payload/network rate, client and server CPU/instructions, frame/syscall counts, append/sync latency, group-commit size, errors, probes, run/space state, and p50/p99.
- If batching produces at least a 1.5x server-capacity win, implement a bounded per-destination-shard combiner with configurable count/byte caps and a 25–100 us dwell sweep. Preserve per-entry versions/results, fencing, admission, sync-before-ack, re-home mirroring, retries, partial failures, backpressure, and cancellation.
- Do not overstate a one-host 10G payload ceiling as a server ceiling; use 100 B and 256 B values plus distributed generation where needed.
- Hardware A/B and correctness suites determine the default; retain a kill switch and document latency/throughput tradeoffs.

## Acceptance criteria

## Plan

Extend the existing synthetic/network write benchmark into a reproducible blind-write width sweep at fixed logical concurrency for widths 1/8/32/128/512, both 100 B and 256 B values, and both buffered W=2 and requested-device-persistent W=2. Reuse the distributed lab/box harness where practical and retain raw artifacts plus a concise findings table covering logical and replica/physical writes per second, payload/network rate, client/server CPU and instructions, frame/syscall counts, append/sync latency, group-commit size, errors, probes, run/space state, and p50/p99; explicitly separate one-host 10G generator limits from server capacity. First run the explicit BatchApply gate. Only if width batching demonstrates at least a 1.5x server-capacity gain, add a default-off/kill-switchable transparent client-side combiner keyed by destination shard and compatible request semantics, bounded by configurable entry and encoded-byte caps and a configurable dwell; sweep 25/50/100 us on hardware. Keep request entries and replies individually indexed so versions/results, retry and partial-failure behavior remain exact; do not combine incompatible epoch/zone/persistence destinations, and preserve fencing/stale-ring handling, admission/backpressure, sync-before-ack, cancellation, and re-home mirroring by feeding ordinary BatchApply semantics rather than bypassing server paths. Add focused tests for cap/dwell flushing, mixed destinations and persistence, per-entry outcomes/duplicates, cancellation, retry/stale epoch, failure fan-out, and bounded queues, then run full debug vals-client/vals-cluster (and affected store) suites. Use correctness plus matched hardware A/B to choose and document the default and latency/throughput tradeoff; if the 1.5x gate fails, document the result and stop without implementing the combiner.

### Starting points
- crates/vals-bench/src/main.rs: workload_distbatch and network benchmark commands
- crates/vals-client/src/net/client.rs: request encoding/transport path
- crates/vals-cluster/src/coord/: write_batch grouping and per-entry outcome scatter
- docs/batch-writes.md and docs/blind-append-lww.md
- plans/ and bench/lab-winner-directory-20260811-0183/: current box deployment/profiling patterns

## Work log
- 2026-08-14 hardware closure: The final serial persistent sweep and a paced fixed-logical-concurrency-512 comparison completed all requested widths with zero errors, correctness mismatches, or gate-Busy responses; synced writes covered every physical write. Together with the 12-arm combiner A/B, hardware selects default OFF: combining reduced requests and p50 but did not improve throughput and roughly doubled p99.
- 2026-08-14 persistent shape correction: Width 1 validated sync, but one call from each of 12 coordinators still collided on destination shards for widths 8+. Changed the pending matched durability-only rerun to COORDS=1 and one global API call at a time; default remains OFF.
- 2026-08-14 combiner A/B: All 12 fresh-format arms completed. Combining reduced request count and p50 but produced no throughput win and materially worsened p99, so default remains OFF. Corrected the pending persistent durability-shape harness from eight to one API call per coordinator after all first-attempt arms overloaded the shard gate.
- 2026-08-13 implementation gate: The retained fresh-format three-repetition gate cleared the conditional threshold for buffered 100-byte values (width 32 median 4.18x width 1), while buffered 256-byte batching did not improve and the fixed-concurrency device matrix was invalid overload evidence due to gate Busy shedding. Implemented a default-off bounded per-destination transport combiner with count/byte/queue caps, 25/50/100 us dwell, scalar fallback, indexed result scatter, and cancellation-safe transport-owned in-flight batches. Default selection remains pending hardware dwell A/B and a lower-load persistent arm.
- 2026-08-12 implementation: Added `vals-bench netbatchwrite`, an explicit width-1/8/32/128/512 fixed-concurrency network sweep with buffered/device persistence, fixed-count or fixed-duration generation, per-entry failure accounting, and sampled p50/p99 acknowledgement latency. Added the fail-closed three-node hardware harness/analyzer under `bench/task-0186-blind-batchapply/` for 100 B/256 B values and W=2, including physical writes, payload/NIC, client/server PMU, syscall/frame, append/sync/group-commit, probes/errors, and run/space captures. Review revisions added sudo-readable exact node-process blind launch proof/counter validation, correctness validation, per-host reset checks, per-coordinator fixed-duration written-range verification, mandatory fresh-format relaunch before every primary arm, repetition-parameterized analysis, strict latency-optional auxiliary parsing, and a synchronized multi-host primary generator mode with per-generator evidence and analyzer/preflight tests. Hardware evidence is not yet accepted, so the 1.5x gate remains unresolved and no transparent combiner was implemented or enabled.
- 2026-08-12 plan: Extend the existing synthetic/network write benchmark into a reproducible blind-write width sweep at fixed logical concurrency for widths 1/8/32/128/512, both 100 B and 256 B values, and both buffered
…[truncated]
- 2026-08-12 context hints: 5 recorded with plan
- 2026-08-12 context hints: crates/vals-bench/src/main.rs: workload_distbatch and network benchmark commands; crates/vals-client/src/net/client.rs: request encoding/transport path; crates/vals-cluster/src/coord/: write_batch gro
…[truncated]
- 2026-08-12 preload: 5 file(s), ~42 KiB seeded into implementer context
- 2026-08-12 implementer report: Implemented the task-0186 explicit BatchApply hardware gate; did not implement transparent combining because the required 1.5× hardware gate remains unresolved (the lab hosts were reachable, but n
…[truncated]
- 2026-08-12 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-12 review (sol): revise — The change adds a useful explicit-width benchmark and a substantial capture/analyzer scaffold, but the hardware gate is not yet reliable enough to determine the conditional combiner decision. In parti
…[truncated]
- 2026-08-12 review (sol#2/sol): revise — The benchmark code and capture matrix are a useful start, and the new Rust path is reasonably clean, but the central hardware gate remains unexecuted and the current analyzer is not actually fail-clos
…[truncated]
- 2026-08-12 revision: Revised the task-0186 gate to address both reviews and the first-run failure.  Changes: - Fixed the `set -u` failure by separating `name` and dependent `dir` assignments; audited the harness for same-
…[truncated]
- 2026-08-12 review (sol): revise — The revision substantially improves provenance, blind-path validation, correctness checking, reset detection, and distributed-generation support, and its analyzer unit tests pass. However, the gate st
…[truncated]
- 2026-08-12 review (sol#2/sol): revise — The revision successfully addresses the two analyzer correctness defects: correctness output is now required and validated, counter/NIC resets are checked per host, and focused analyzer tests pass. Th
…[truncated]
- 2026-08-12 revision: Applied the second gate revision while keeping task 0186 in progress and the combiner decision pending.  Changes: - Added `node-provenance.sh` to identify exactly one `valsd node -c/--config` process
…[truncated]
- 2026-08-12 revision: Made only the requested focused harness corrections:  - Removed the lab remote `rg` dependency:   - server preflight now requires portable `grep`, not `rg`;   - remote config validation uses `grep
…[truncated]
- 2026-08-12 revision: Made the requested smoke-run corrections only:  - Reset stop is now drain-tolerant and fail-closed:   - added configurable `STOP_COMMAND_TIMEOUT` (default 15s) and `SHUTDOWN_TIMEOUT` (default 180s);
…[truncated]
- 2026-08-12 revision: Fixed single-Apply physical-write observability without weakening the gate analyzer.  Changes: - Added shared single-Apply accounting used by both synchronous and conn-driver paths. - A successful `Ap
…[truncated]
- 2026-08-12 revision: Implemented the conditional default-off transparent write combiner after the retained buffered 100-byte gate cleared 1.5×.  Implementation: - Added a client `NetTransport` combiner for ordinary singl
…[truncated]
- 2026-08-12 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-12 review (sol): revise — The explicit-width benchmark and retained harness are substantial, and the buffered 100-byte evidence justifies experimenting with a default-off combiner. However, the task is not complete: the requir
…[truncated]
- 2026-08-12 review (sol#2/sol): revise — The explicit-width benchmark and retained captures are substantial, and the combiner is default-off with sensible queue/cap separation. However, the combiner's claimed cancellation safety is not corre
…[truncated]
- 2026-08-12 revision: Addressed the post-flush cancellation/lifetime issue and expanded correctness coverage.  Combiner lifetime fix: - Started combined BatchApply RPCs are now registered in an autonomous thread-local tran
…[truncated]
- 2026-08-12 revision: Added two focused, execution-ready hardware harnesses without changing server gate behavior or the default-off combiner setting.  Transparent combiner A/B: - Added `run-combiner-sweep.sh` using the pr
…[truncated]
- 2026-08-12 revision: Fixed the hardware liveness regression by replacing opportunistic noop-waker flight polling with reactor-scheduled detached tasks.  Core fix: - Added `vals_rt::spawn_local` / `poll_local_tasks`, a thr
…[truncated]
- 2026-08-12 revision: Diagnosed and fixed the second combiner liveness failure at the executor boundary.  Root cause confirmed: - The autonomous MuxRpc was spawned into a separate thread-local task set. - `workload_netload
…[truncated]
- 2026-08-12 revision: Reworked the combiner around one autonomous destination worker per active destination, eliminating caller-owned dwell/start logic.  Changes: - First successful enqueue atomically marks the destination
…[truncated]
- 2026-08-12 revision: Added the exact end-to-end regression first, then replaced caller-owned flush initiation with one destination-owned worker.  Regression tests added: - `local_executor_tcp_combiner_emits_batchapply_and
…[truncated]
- 2026-08-12 revision: Updated the hardware decision and corrected the pending persistent harness.  Combiner A/B findings and default: - Documented all 12 retained fresh-format C=512/30s arms in `FINDINGS.md`:   - off: 193,
…[truncated]
- 2026-08-12 review (sol): revise — The revision successfully adds the matched dwell A/B, establishes an OFF default, completes a valid serial persistence/durability check, and replaces caller-owned in-flight RPCs with autonomous destin
…[truncated]
- 2026-08-12 review (sol#2/sol): revise — The revision adds the missing hardware A/B evidence and an autonomous worker that fixes the original post-flush lifetime design in LocalExecutor-driven workloads. Two substantive gaps remain: enabling
…[truncated]
- 2026-08-12 review (sol): revise — The explicit-width summary is now populated and reproducible, and plain `block_on` without an executor safely falls back to scalar Apply. However, the runtime’s notion of an “active” executor is
…[truncated]
- 2026-08-12 review (sol#2/sol): revise — The paced fixed-concurrency persistence artifacts and block-on scalar fallback address the prior evidence and direct panic cases. The autonomous combiner still has one concrete executor-liveness bug,
…[truncated]
- 2026-08-12 review (sol): revise — The dormant-executor issue is fixed: `CURRENT` is now installed only while `run`/`run_until` actively drives the executor, so other contexts correctly use scalar fallback. The retained analyses also r
…[truncated]
- 2026-08-12 review (sol#2/sol): revise — The executor-boundary fallback and fixed-concurrency paced persistence evidence are now integrated correctly, and the retained analyzers reproduce their summaries. One substantive combiner correctness
…[truncated]
- 2026-08-12 review (sol): revise — The cap lost-wake is now addressed with a persistent flush-request marker and double-checked waker registration, and the dormant-executor fallback remains correct. The retained evidence still contains
…[truncated]
- 2026-08-12 review (sol#2/sol): revise — The persisted cap request fixes the lost-notification race, and the prior executor/persistence issues remain addressed. One production hang remains in the dwell worker's timer-error path; the remainin
…[truncated]
- 2026-08-12 review (sol): accept — The revision now satisfies the task. It includes reproducible explicit-width buffered/persistent benchmarks and retained all-metrics summaries, a default-off bounded per-destination combiner with cap-
…[truncated]
- 2026-08-12 review (sol#2/sol): accept — The correctness blockers are resolved: autonomous workers are only used under an actively running LocalExecutor, cap flush requests persist across worker startup, timer creation failure flushes immedi
…[truncated]
- 2026-08-12 decision: accept — commit: client: add gated write combining
- 2026-08-12 usage: 183,912,373 tok (in 22,267,682, out 357,523, cache_r 161,287,168, cache_w 0) · cost n/a (unpriced)
  implementer: 84,257,704 tok (in 7,312,430, out 120,442, cache_r 76,824,832, cache_w 0) · cost n/a (unpriced)
  coordinator: 59,874,810 tok (in 6,458,183, out 52,403, cache_r 53,364,224, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 21,288,989 tok (in 4,408,545, out 97,596, cache_r 16,782,848, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 18,490,870 tok (in 4,088,524, out 87,082, cache_r 14,315,264, cache_w 0) · cost n/a (unpriced)

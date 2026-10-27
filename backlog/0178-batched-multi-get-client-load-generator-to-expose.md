---
id: "0178"
title: Batched multi-get client/load generator to expose the sharded-node read ceiling
status: done
priority: 4
created: "2026-08-11"
updated: "2026-08-11"
depends_on: []
spec_refs:
    - ARCHITECTURE.md#Status
---

## Description
Follow-up surfaced by tasks 0014/0022. Build a batched multi-get client/load-generation path that can drive the sharded real-NIC cluster from fewer client hosts, so the true node read ceiling and past-knee behavior can be measured. Keep this separate from server admission/send-path tuning.

## Acceptance criteria
- A client-facing batched multi-get/load-generator mode issues multiple independent gets per syscall/round trip while preserving per-key result/error attribution and value verification.
- Cross-box A/B against the existing `vals-bench netread` protocol records client CPU/syscall cost, node CPU, throughput, errors, and latency across coordinator/in-flight sweeps.
- The run either reaches a repeatable saturated-node knee or quantifies the remaining load-generation limit; results are documented with reproduction commands.

## Plan

1. Confirm the existing batched-read plumbing (`Coordinator::read_many` / `Transport::read_values` / sharded `SyncValues`) and define a benchmark-only `netmultiread` mode that preserves the current RF=2 strong-read semantics while batching independent generated keys.
2. Extend `vals-bench` with configurable batch width (environment variable, leaving the existing positional coordinator/in-flight arguments comparable), issue each batch through `read_many`, account every key independently as hit/miss/error/mismatch, sample batch and per-key-equivalent latency, and print enough counters to distinguish requested keys from round trips. Add focused tests for batch selection/accounting/latency behavior and update user-facing benchmark documentation.
3. Build and run targeted client/benchmark tests plus the relevant workspace suites without formatting unrelated legacy code.
4. On an idle lab cluster, rebuild/deploy the exact tested revision and run cross-box A/B sweeps of legacy `netread` versus batched mode across coordinator/in-flight and batch-width settings. Capture commands, client CPU/task-clock and syscall counts, node CPU, throughput, errors/misses/mismatches, latency, and LSM/run state in a task-specific benchmark directory. Prefer short repeated steady-state arms and restore the fleet afterward.
5. Analyze whether the batched generator reaches a repeatable node-saturation knee; otherwise quantify the remaining client/load-source limit. Document raw artifact provenance, reproduction commands, comparison caveats, and conclusions, then inspect the final diff against all acceptance criteria.

### Starting points
- crates/vals-bench/src/main.rs: workload_netload/netload_thread and netread stats/tests
- crates/vals-client/src/coord/mod.rs: Coordinator::read_many/read_many_in
- crates/vals-client/src/net/client.rs: NetTransport::read_values batches SyncValues by node/shard
- bench/lab-shard-scaling-20260810/commands.md and plans/box-provisioning-runbook.md for prior cross-box methodology
- Do not disturb non-idle boxes; use timeout-wrapped ssh and restore standard four-shard services/configs.

## Work log
- 2026-08-11 plan: 1. Confirm the existing batched-read plumbing (`Coordinator::read_many` / `Transport::read_values` / sharded `SyncValues`) and define a benchmark-only `netmultiread` mode that preserves the current RF
…[truncated]
- 2026-08-11 context hints: 5 recorded with plan
- 2026-08-11 context hints: Use existing Coordinator::read_many rather than creating a new wire operation unless inspection proves it insufficient.; Keep this client/load-generator scoped; do not tune server admission or send pa
…[truncated]
- 2026-08-11 preload: 6 file(s), ~39 KiB seeded into implementer context
- 2026-08-11 implementer report: Implemented task 0178 end to end.  Changes: - Added `vals-bench x netmultiread`, using existing `Coordinator::read_many`/sharded `SyncValues` plumbing with `VALS_NETMULTIREAD_BATCH` (default 32), whil
…[truncated]
- 2026-08-11 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-11 review (sol): accept — The change satisfies task 0178: `netmultiread` uses the existing sharded `read_many`/`SyncValues` path, preserves RF=2/R=2 quorum semantics, verifies and classifies each requested key, distinguishes k
…[truncated]
- 2026-08-11 review (sol#2/sol): accept — The change correctly adds a configurable `netmultiread` mode over `Coordinator::read_many`, preserves strong-read behavior, accounts and verifies each requested key, and provides focused tests plus re
…[truncated]
- 2026-08-11 revision: Addressed both reviewer findings.  Changes: - Restored the legacy `netread` width-1 hot path inside `netload_thread`: it now uses the original scalar index selection, directly generates one key, and c
…[truncated]
- 2026-08-11 review (sol): accept — The revision cleanly addresses the review feedback. It restores the original allocation-free scalar `netread` hot path so the legacy control is not regressed, while keeping the batched implementation
…[truncated]
- 2026-08-11 review (sol#2/sol): accept — The revision cleanly addresses both prior findings. The legacy width-1 path again selects and reads scalar keys directly without the temporary batch vectors, while the measurement documentation and an
…[truncated]
- 2026-08-11 revision: Updated the Real NIC / multi-machine status in `ARCHITECTURE.md`: - Kept the 2026-07-30 client-wall finding as historical context, changing the batched multi-get wording to the “next lever at the ti
…[truncated]
- 2026-08-11 review (sol): accept — The latest revision completes the integration documentation appropriately: ARCHITECTURE.md now preserves the earlier client-wall result as historical context and records the shipped `netmultiread` mod
…[truncated]
- 2026-08-11 review (sol#2/sol): accept — The latest revision appropriately integrates task 0178’s shipped behavior and measured result into `ARCHITECTURE.md`, while retaining the previously accepted implementation, benchmark caveats, and a
…[truncated]
- 2026-08-11 decision: accept — commit: bench: add batched multi-get load generator
- 2026-08-11 usage: 15,823,674 tok (in 4,508,908, out 65,614, cache_r 11,249,152, cache_w 0) · cost n/a (unpriced)
  implementer: 9,134,776 tok (in 1,563,781, out 42,035, cache_r 7,528,960, cache_w 0) · cost n/a (unpriced)
  coordinator: 3,057,833 tok (in 1,213,017, out 6,992, cache_r 1,837,824, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 2,199,182 tok (in 971,079, out 8,263, cache_r 1,219,840, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 1,431,883 tok (in 761,031, out 8,324, cache_r 662,528, cache_w 0) · cost n/a (unpriced)

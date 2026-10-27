# Task 0287 — client-budget SPDK adoption gate

Prepared during task 0279 after the 2026-09-08 review of 0278. This is the historical execution runbook, not a current fleet procedure: task 0287 ran on 2026-09-09 and returned **no-go** for performance adoption (see [`FINDINGS.md`](../bench/0287-hardware-20260909/FINDINGS.md)). Do not rerun these commands unchanged. See `docs/backpressure.md`, `bench/0279-client-budget-support/README.md`, and the canonical backlog tasks for the contract.

## Sequence and topology

Use a separate short all-latency RF=2 gate on **three** physical nodes, before the fresh RF=3 mixed-profile 0194 soak. Keep server profile, source, W/R and persistence identical between finite-default and explicit-unbounded arms. A three-node RF=2 default zone supplies a real third-node sloppy fallback and keys owned solely by the two healthy nodes; RF=3 on three hosts cannot demonstrate those conditions. Use a separate sorted RF=2 zone for scans. The single PD is an explicitly non-HA test exception and must live on a non-faulted host.

The owner authorized replacement of the test corpus/PD state for a fresh soak, with selective retention. Preserve unresolved corruption cores with matching binaries, the sealed replay cache, and compact source/config/results provenance. Before any offline provisioning, verify stopped daemons and live PCI+serial identity. Restore lab-1's post-reboot hugepages/SPDK binding/core pattern first. No hardware mutation or test was performed during this preparation.

Capture an immutable source snapshot (or commit) of accepted 0278 before building. Do not reuse the 0277 client, which predates these client limits. Rebuild release binaries from the pinned source on each rsynced host and retain source manifest/patch, lock/toolchain identity and binary hashes. Use current guarded offline `valsd provision`; never `format=true` or historical `/dev/nvmeN` identities. After the short gate, stop everything, retain a compact result, and create the separate fresh RF=3 corpus for 0194.

## Benchmark support prepared by task 0279

The reviewed source now wires finite-default versus explicit-unbounded selection through every netload worker and its separate verification transport, prints actual limits and per-coordinator terminal logical/transport/connection/reconnect receipts, captures raw read/write latency samples, and selects Digest/Quorum/All read fanout. The artifact-local Rust probe covers nonvacuous concurrent scans with clean finite/unbounded selection, mixed-read fanout and scalar fallback, destination admission, partial/wholly-unsent cancellation, deadline-derived logical failure, explicit incoming response-byte pressure with healthy-destination isolation and reconnect receipts, route selection, and actual sloppy fallback/heal. Fault probes reject unbounded mode. Use the exact interfaces and required preconditions in `bench/0279-client-budget-support/README.md`; do not infer commands from this summary.

This readiness is software evidence only. No lab operation or SPDK adoption measurement was performed by task 0279. The 0277 harness remains a provenance/systemd/metrics/perf template only: it is hard-coded for two nodes and a server comparison, so do not execute it unchanged. Verify the pinned binary's CLI/environment receipts before freezing task 0287 commands.

## Short measurement matrix

Run at least three alternating whole-arm generations (U/F, F/U, U/F), initially ~12 seconds per clean arm, with a fixed read corpus and disjoint write prefixes. Include scalar writes, width-32 batches, scalar strong reads, width-32 Digest/Quorum/All reads and concurrent sorted scans. Preserve the same persistence choice within each pair; label RF2/W2/R2 as all-replica agreement, not RF3/W2 behavior.

Capture client CPU/perf separately from the primary throughput distributions, especially batch writes, non-digest mixed reads and scans. Compare per-generation paired deltas and complete latency distributions rather than only aggregate means. Do not invent a numeric tolerance: a repeatable material regression requires investigation. Record costs called out by review: per-RPC credits, double encoding after an admission wait, send-buffer reclamation under partial flush, cancellation splicing, and reply-pressure reconnects.

## Finite-only fault sequence

Never point an explicitly unbounded client at a deliberately stalled destination without an external hard stop.

1. Saturate node 1's per-destination operation/byte admission while remaining below global limits. Require visible blocking/refusal and eventual zero outstanding credits/waiters.
2. In parallel, use `replicas_for` to select keys owned by nodes 2 and 3 only; require useful progress during node-1 pressure, not merely after recovery.
3. With a cleanup trap guaranteed to send CONT, pause node 1 and demonstrate a genuinely partially transmitted valid frame via queue statistics. Cancel it; require physical release and local `Overloaded` collateral, not hints or FD failures. Separately cancel a wholly unsent frame and require same-connection sibling survival.
4. Exercise digest hedge losers and response-heavy scans/reads. Use the explicit oversized incoming-response probe to create actual reader/reply byte-admission pressure; completed caller outputs do not retain wire credits. Require deterministic local overload, simultaneous healthy-node isolation, terminal retained-byte baseline, and measured reconnect behavior.
5. Exercise the existing mixed-batch capability fallback with exact results and bounded accounting.
6. Test actual unavailability separately: stop node 1, select a default-zone key with homes 1/2, require the expected fallback to node 3 and hint, restart/heal, and fully verify. These expected hints must not be confused with forbidden hints from local overload/cancellation.

## Verification and receipts

Use source-pinned `kvchurn` before/after faults on the default and sorted zones, checking full value bytes. Scalar `netread` hot-loop presence checks are not exhaustive byte verification; batch reads and the scan probe must validate every returned value and ordering.

Retain exact commands/configs/source/binaries, whole-arm samples and raw latency distributions, client profiles, logical/transport credit and reconnect receipts, FD/hint/fallback evidence, full-value verification, metrics and safe postflight. Keep the normal arms free of errors/misses/mismatches/timeouts; fault arms must show expected classification and recovery. Stop nodes/PD, wait for units and `pgrep -x valsd` to clear, and document any forced stop before the subsequent fresh layout.

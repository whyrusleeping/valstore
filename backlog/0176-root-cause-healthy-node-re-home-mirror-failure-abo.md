---
id: "0176"
title: Root-cause healthy-node re-home mirror-failure abort storm before default-on
status: done
priority: 3
created: "2026-08-10"
updated: "2026-08-11"
depends_on:
    - "0168"
spec_refs:
    - docs/zones.md#Confirmed re-home
---

## Description
Task 0168's fresh lab churn run found `VALS_PD_REHOME_ENABLED` still not ready for default-on: outside the forced outage, 23 of 32 prepares aborted on `mirror_failure` during sustained writes, while quiet burndown had zero aborts. Determine which mirror RPC/error classes poison healthy moves, distinguish transient serving saturation from true target failure, and fix or safely retry the condition without weakening the invariant that an acknowledged mutation reached every pending owner.

Acceptance criteria:
- A focused repro attributes healthy-node mirror failures by concrete error/timeout class and identifies the causal path.
- The protocol/implementation change preserves ack-implies-mirrored and existing cutover/fencing safety tests.
- In-process churn tests cover the failure and stay green.
- Repeat the task 0168 hardware churn arm (or an explicitly justified equivalent A/B) and show ordinary-churn aborts are near zero and limited to genuine outages before reconsidering default-on.
- Update `docs/zones.md` and benchmark findings with the result and default decision.

## Acceptance criteria

## Plan

Root-cause and remove the healthy-load re-home mirror abort storm without relaxing acknowledged-write safety, then re-run the measured churn gate.

1. Trace the mirror path end to end (`mirror_rehome_writes` → `NetTransport::apply_batch`/RPC → target fence/apply → range-stat aggregation → PD abort) and build a focused in-process reproduction of the ordinary healthy-load failure. Add enough concrete classification/observability to separate applied, Busy/backpressure, stale-epoch, RPC deadline/transport, remote error, and true unreachable-target outcomes; retain the causal result in tests and findings rather than inferring from the aggregate PD counter.
2. Change retry/accounting at the narrowest safe layer so transient saturation does not permanently poison a move. Preserve these invariants: a successful client response is emitted only after every pending owner accepted the version; an unresolved transport/target failure cannot permit cutover; stale routing/fencing continues to force refresh/retry; and genuine outages still produce a retryable client result plus an abort/report signal. Prefer bounded, LWW-idempotent retry or resolution-aware accounting over treating failures as success.
3. Add deterministic unit/in-process churn coverage for the reproduced healthy-load class, successful retry/no-poison behavior, exhausted/genuine outage poisoning, apply and batch paths, and ack-implies-mirrored/cutover-abort safety. Run focused cluster/net tests and the existing cutover, fencing, mirror failure, and churn suites in debug mode, then broader relevant workspace tests as practical.
4. Rebuild and run an explicitly comparable task-0168 hardware A/B on lab (freshly sync/rebuild every node, same RF=2 sorted-range sustained-write/churn shape, cause/error-class metrics and oracle). Include a genuine-outage phase if needed to prove failures still abort. Preserve commands, configs, logs, samples, counts, source/build provenance, and compare healthy-load prepares/aborts directly with 0168's 23/32 blocker.
5. Write a new `bench/lab-rehome-*/FINDINGS.md` identifying the concrete causal class/path, implementation effect, healthy-load and outage results, exact-count/consistency evidence, and artifact index. Update `docs/zones.md` with retry/error semantics and make an evidence-based `VALS_PD_REHOME_ENABLED` default decision (change the code/architecture docs only if the gate is truly met).

### Starting points
- crates/vals-cluster/src/net/server.rs:3332-3574 — mirror classification, permanent per-range failure accounting, apply/batch response gate
- crates/vals-client/src/net/client.rs:837-897 — apply_batch currently collapses most RPC failures to BatchAck::Unreachable
- crates/vals-cluster/src/net/tests.rs:2353 — genuine transport failure safety coverage
- bench/lab-rehome-20260810/FINDINGS.md and docs/zones.md:121-228 — blocker baseline and protocol/default gate
- plans/bootes-soak-runbook.md and project memory — lab sync/build/launch constraints; use debug for local cluster tests

## Work log
- 2026-08-11 plan: Root-cause and remove the healthy-load re-home mirror abort storm without relaxing acknowledged-write safety, then re-run the measured churn gate.  1. Trace the mirror path end to end (`mirror_rehome_
…[truncated]
- 2026-08-11 context hints: 5 recorded with plan
- 2026-08-11 context hints: crates/vals-cluster/src/net/server.rs:3332-3574 — mirror classification, permanent per-range failure accounting, apply/batch response gate; crates/vals-client/src/net/client.rs:837-897 — apply_bat
…[truncated]
- 2026-08-11 preload: 6 file(s), ~60 KiB seeded into implementer context
- 2026-08-11 implementer report: Implemented task 0176 end to end.  Code/protocol: - Preserved concrete `BatchAck` classes for applied, Busy, stale, timeout, remote/protocol error, and unreachable transport instead of collapsing fail
…[truncated]
- 2026-08-11 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-11 review (sol): accept — The change correctly identifies healthy mirror failures as staged-batch queueing/RPC-deadline events, preserves concrete transport outcomes, sheds saturated target work as Busy, and adds bounded same-
…[truncated]
- 2026-08-11 review (sol#2/sol): accept — The implementation is correctness-preserving and clean overall: it distinguishes timeout/remote/unreachable mirror outcomes, sheds saturated staged batch work as a known non-accepting Busy response, r
…[truncated]
- 2026-08-11 revision: Addressed all review items.  1. Timeout uncertainty is now fail-closed: - `apply_mirror_with_timeout_retries` returns a resolved `MirrorOutcome` and remembers whether any attempt timed out. - After a 
…[truncated]
- 2026-08-11 review (sol): accept — The revision addresses the prior review findings. The benchmark directory now includes the exact cluster/build/load/outage/restart/count commands and checked-in PD/node configs, making the hardware ga
…[truncated]
- 2026-08-11 review (sol#2/sol): accept — The revision addresses the prior findings cleanly: ARCHITECTURE.md now records the shipped default-on behavior, and the obsolete gate-wait metric/instrumentation has been replaced by the Busy-shed cou
…[truncated]
- 2026-08-11 decision: accept — commit: cluster: resolve re-home mirror timeout aborts
- 2026-08-11 usage: 34,956,761 tok (in 6,294,958, out 76,075, cache_r 28,585,728, cache_w 0) · cost n/a (unpriced)
  implementer: 27,546,456 tok (in 2,891,065, out 51,487, cache_r 24,603,904, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 2,958,409 tok (in 1,221,890, out 10,055, cache_r 1,726,464, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 2,468,825 tok (in 997,554, out 7,975, cache_r 1,463,296, cache_w 0) · cost n/a (unpriced)
  coordinator: 1,983,071 tok (in 1,184,449, out 6,558, cache_r 792,064, cache_w 0) · cost n/a (unpriced)

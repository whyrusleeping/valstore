---
id: "0264"
title: Add finite client deadlines and operation, byte, and per-node budgets
status: done
priority: 2
created: "2026-09-07"
updated: "2026-09-08"
depends_on: []
spec_refs:
    - docs/production-readiness-review-2026-08.md#R9 — Client liveness and memory protection are opt-in (P1)
---

## Description

Accepted immediate slice split from 0242 during owner-approved triage. Introduce validated client configuration with finite connect/RPC deadlines and operation/byte/per-node pressure limits. Keep write-concern/persistence semantics and naming decisions separate in 0242.

Acceptance:
- Define finite defaults and explicit benchmark opt-outs; coordinate migration with current consumers rather than silently imposing undocumented limits.
- Black-holed/unresponsive peers cannot leave failed quorum operations pending forever.
- Bound outstanding operations and bytes; release credits on completion, cancellation, timeout, and partial failures; avoid starving healthy destinations behind one stalled node.
- Add deterministic failure/cancellation/accounting tests and document deadline/retry/partial-write semantics.
- Preserve an explicit high-throughput path; measure request-path overhead according to project gate policy.

## Acceptance criteria

Scope split on 2026-09-08: this task delivers the Rust software correctness slice: finite network connect/RPC defaults and strict coordinator write/delete/batch operation, key/value byte, and home-node credits, explicit benchmark opt-outs, migration documentation, deterministic lifecycle tests, and preliminary local overhead measurement. Remaining non-write/direct-transport/retained-memory and actual fallback-destination accounting is accepted task 0278. Task 0279 prepares the required benchmark/probe support; the SPDK performance/adoption gate is accepted task 0287 (after protocol-v2 gate 0271). Completing 0264 does not claim production performance adoption or full client-memory protection. Independent Go-client semantics remain unchanged.

## Outcome

Completed Rust software slice: validated 5s connect/hello and RPC defaults; coordinator write limits of 32768 ops/512 MiB globally and 8192 ops/128 MiB per home node. Strict key+value admission, exact per-node batch subsets, lifecycle-safe release, stale-route reacquisition, explicit benchmark opt-outs and migration/partial-write docs. Both reviewers accepted; documentation nits corrected. Independently passed client/cluster/benchmark tests, workspace all-targets check (existing PD-test warnings), and diff check. Local three-generation A/B is preliminary only. Accepted remaining scope: 0278 covers read/direct-transport/retained-memory/fallback-destination accounting; 0279 prepares gate tooling and 0287 covers required SPDK adoption after 0271. Completion does not claim full client-memory protection or hardware performance adoption. No hardware touched.

Commit: feat(client): default to finite deadlines and strict write budgets

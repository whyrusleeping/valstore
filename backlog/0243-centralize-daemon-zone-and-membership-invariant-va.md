---
id: "0243"
title: Centralize daemon, zone, and membership invariant validation
status: done
priority: 2
created: "2026-08-23"
updated: "2026-09-08"
depends_on: []
spec_refs:
    - docs/production-readiness-review-2026-08.md#R10 — Configuration and control-plane invariants are not validated centrally (P1)
    - docs/production-readiness-review-2026-08.md#R16 — Move correctness-affecting environment switches into typed startup config (P1/P2)
---

## Description

Accepted bounded validation slice: add typed validated constructors/configuration and a strict `valsd node|pd --check-config`. Reject zero/overflowing geometry, invalid cadence, malformed/duplicate zones, RF 0 or RF above eligible scope/membership, lossy wire conversions, and unsafe membership changes; make any degraded under-replication mode explicit. Preserve intentionally supported disabled modes only through explicit validated semantics rather than accidentally disarmed timers.

The broader supported-environment-switch migration is separately proposed as 0265; do not expand this task into wholesale configuration/product-surface redesign.

## Acceptance criteria

- Validate arithmetic and geometry before allocation/division or serving; reject invalid values with actionable errors.
- Enforce zone/membership invariants in the authoritative PD path, not only CLI parsing; test bypass attempts.
- Config-check mode validates without starting serving processes or formatting devices, and reports effective settings and relevant restart-only actions.
- Tests cover zero/overflow, duplicate/malformed zones, invalid RF/scope, lossy conversions, and unsafe membership changes.
- Preserve explicit supported disable/degraded modes with documented semantics; coordinate format behavior with 0240.

## Outcome

Implemented checked geometry/cadence and zone/member invariants, side-effect-free strict node|pd --check-config, atomic RF-safe membership bootstrap, leader/caught-up administrative validation with synchronous results, and safe leader-side zone ID allocation. Documented disabled modes, provisioning and persisted-state upgrade preflight. Both independent reviewers accepted after regression fixes. Final verification: 72 PD tests and complete vals-server suite passed; implementer also passed full vals-cluster integration/doc suites, client/raft tests and workspace check. Earlier full workspace serial suite passed after an isolated parallel handshake timing flake. Nonblocking embedding docs/unused-Result test cleanup split to accepted follow-up 0275.

Commit: Validate daemon configuration and authoritative PD mutations

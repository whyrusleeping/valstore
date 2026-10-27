---
id: "0241"
title: Define and implement authenticated encrypted transport for external deployments
status: done
priority: 4
created: "2026-08-23"
updated: "2026-10-12"
depends_on: []
spec_refs:
    - docs/production-readiness-review-2026-08.md#R5 — The network trust model is not suitable for an external production claim (P0 security)
---

## Description
Define and implement the external threat model: mTLS (or equivalent mutual identity) for data, control, and PD peer RPCs; authorization boundaries; fail-closed non-loopback daemon defaults; and bounded authentication handshakes. Preserve an explicit insecure loopback/development mode.

Owner-approved triage separated immediately useful transport resource bounds into accepted task 0263. This security project is retained as accepted conditional work, but blocked on an explicit external-deployment/threat-model decision; spec.md currently defers mTLS until external users. The 2026-09-29 backlog acceptance is not that product decision. Coordinate handshake accounting with 0263 rather than duplicating general connection/request budgets.

## Acceptance criteria

## Work log

- 2026-10-12 (owner decision): out of scope. Deployments put vals on a private network or an overlay such as Tailscale/WireGuard rather than vals shipping transport encryption/authentication. spec.md, docs/deploy.md, docs/getting-started.md and ARCHITECTURE.md updated. Closed as declined; resource bounds already shipped in 0263.

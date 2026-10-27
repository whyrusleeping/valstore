---
id: "0010"
title: mTLS on the wire
status: done
priority: 3
created: "2026-07-02"
updated: "2026-08-07"
depends_on: []
spec_refs: []
---

## Description

BACKLOG.md §Productization: the cluster secret stops stray/unprivileged senders only; data ops are unauthenticated and the control token is cleartext (an on-path observer can replay it) — mTLS is the documented "real answer" (ARCHITECTURE.md §Control plane).

**Needs an owner decision before planning**: adopting a TLS implementation (rustls + rustls-pemfile, most likely) cuts against the repo's dependency-light philosophy and touches the hand-rolled reactor/mux hot path (TLS record layer inside the readiness-driven ConnDriver/MuxConn). Alternatives: defer until there are external users; or scope to control-plane connections only (PD pushes + admin) where perf doesn't matter.

## Acceptance criteria

- Decision recorded (scope: data path vs control-plane-only vs deferred).
- If built: mutual auth on covered connections, hostile-peer test coverage, measured hot-path cost on the box.

## Acceptance criteria

## Plan

DECISION (owner, 2026-06-27): defer until there are external users. The cluster secret + no-secret-open model is sufficient for the current single-operator deployments; adopting rustls and touching the readiness-driven hot-path transport is not justified yet. Re-open when a client outside the trusted network exists. If scoped later, control-plane-only (PD pushes + admin) is the cheaper first cut.

Reconfirmed by owner 2026-07-05: still parked — nothing changed, no external users yet.

## Work log

- 2026-07-02 plan: DECISION (owner, 2026-06-27): defer until there are external users. The cluster secret + no-secret-open model is sufficient for the current single-operator deployments; adopting rustls and touching th
  …[truncated]

- 2026-08-07 CLOSED (won't-do, owner decision): parked indefinitely — no external users, the cluster-secret model stands. Re-file a fresh task if a client outside the trusted network ever appears; the parked reasoning above is the starting point.

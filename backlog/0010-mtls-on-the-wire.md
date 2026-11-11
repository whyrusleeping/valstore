---
id: "0010"
title: mTLS on the wire
status: done
priority: 3
created: "2026-07-02"
updated: "2026-11-11"
depends_on: []
spec_refs: []
---

## Description

**Outcome:** closed as won't-do on 2026-08-07 by owner decision (session s_d55350951c283799, event 41: "Close 0010/0017/0026 as won't-do"). The decision-needed note below is historical.

BACKLOG.md (the free-form backlog that preceded `backlog/`; its open items became tasks on 2026-07-02 and it was deleted 2026-08-01, so it is not in the public history) §Productization: the cluster secret stops stray/unprivileged senders only; data ops are unauthenticated and the control token is cleartext (an on-path observer can replay it) — mTLS is the documented "real answer" (ARCHITECTURE.md §Control plane).

**Needs an owner decision before planning**: adopting a TLS implementation (rustls + rustls-pemfile, most likely) cuts against the repo's dependency-light philosophy and touches the hand-rolled reactor/mux hot path (TLS record layer inside the readiness-driven ConnDriver/MuxConn). Alternatives: defer until there are external users; or scope to control-plane connections only (PD pushes + admin) where perf doesn't matter.

## Acceptance criteria

- Decision recorded (scope: data path vs control-plane-only vs deferred).
- If built: mutual auth on covered connections, hostile-peer test coverage, measured hot-path cost on the box.

## Plan

DECISION (owner, 2026-06-27): defer until there are external users. The cluster secret + no-secret-open model is sufficient for the current single-operator deployments; adopting rustls and touching the readiness-driven hot-path transport is not justified yet. Re-open when a client outside the trusted network exists. If scoped later, control-plane-only (PD pushes + admin) is the cheaper first cut.

Reconfirmed by owner 2026-07-05: still parked — nothing changed, no external users yet.

*(Groom 2026-11-11: no source found for the 2026-06-27 decision, so treat it as the authoring agent's record. The 2026-07-05 reconfirmation has no session log (the logs start 2026-07-15); its only source is the agent-written pre-release commit `ea0e9875` "record owner decisions" in the private pre-release history. The 2026-08-07 closure is sourced, see the Description.)*

## Work log

- 2026-07-02 plan: recorded in the Plan section above.

- 2026-08-07 CLOSED (won't-do, owner decision; source: session s_d55350951c283799, event 41): parked indefinitely — no external users, the cluster-secret model stands. Re-file a fresh task if a client outside the trusted network ever appears; the parked reasoning above is the starting point.

- 2026-11-11 groom: added an outcome line citing the session event for the 2026-08-07 won't-do closure; marked the 2026-06-27 decision as the authoring agent's record (no source found) and cited pre-release commit ea0e9875 as the only source of the 2026-07-05 reconfirmation; glossed BACKLOG.md; removed a duplicate empty Acceptance criteria heading; replaced the cut-off plan entry with a pointer to the Plan section.

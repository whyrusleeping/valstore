---
id: "0236"
title: Make writer identity collision-resistant and prove equal-time write convergence
status: done
priority: 1
created: "2026-08-23"
updated: "2026-09-07"
depends_on: []
spec_refs:
    - docs/production-readiness-review-2026-08.md#R1 — Colliding coordinator IDs can permanently diverge replicas (P0 correctness)
---

## Description

Replace caller-chosen u32 coordinator identity as the sole LWW tie-break with a collision-resistant, durable writer identity (and persisted HLC state or another complete total-order design). Equal logical versions must not be possible for different values. Add a deterministic regression that gives two writers the same physical clock, delivers conflicting writes in opposite orders, and proves reads plus anti-entropy converge.

## Outcome

Implemented CSPRNG 64-bit writer-incarnation identities with clone-shared atomic HLC state and durable full-width version propagation. Added deterministic equal-clock opposite-order local/quorum/AE convergence, recovery/hint/codec compatibility, digest-budget and handshake-readiness regressions. Protocol v2 explicitly rejects v1 before request transmission; legacy packed records and hint logs remain readable. Both comprehensive reviewers accepted; final cargo test --workspace, workspace build/all-target check and diff checks passed. Deployment integration and hardware gates split to 0270/0271; pre-existing daemon test race proposed as 0272. No hardware touched; pre-existing backlog/docs edits preserved.

Commit: fix: use collision-resistant writer incarnations for LWW versions

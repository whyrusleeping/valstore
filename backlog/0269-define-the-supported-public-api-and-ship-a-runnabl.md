---
id: "0269"
title: Define the supported public API and ship a runnable client demo
status: blocked
priority: 4
created: "2026-09-07"
updated: "2026-09-29"
depends_on:
    - "0242"
    - "0266"
spec_refs:
    - docs/production-readiness-review-2026-08.md#R15 — Define and shrink the supported product/API surface (P1/P2)
    - docs/production-readiness-review-2026-08.md#R17 — Repair the first-user path and status documentation (P1/P2)
---

## Description
Accepted conditional API/demo work (2026-09-29), blocked on selecting an external-user scope and the contract/publication decisions in 0242/0266. The redundant umbrella 0244 was removed. Decide supported daemon/client/embedding artifacts and seal or explicitly mark internals; document cancellation, retry, persistence, and thread-safety contracts. Ship a versioned runnable TCP put/get/delete example and a repeatable local demo without sibling-directory assumptions. Repair status/compatibility/config references without claiming unresolved blockers are fixed. Coordinate API decisions with 0242/0264 and publication intent with 0266; no public API freeze or release is implied by this proposal.

## Acceptance criteria

## Work log

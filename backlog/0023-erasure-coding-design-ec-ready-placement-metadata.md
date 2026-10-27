---
id: "0023"
title: 'Erasure coding: design EC-ready placement metadata or downgrade the claim'
status: done
priority: 3
created: "2026-07-02"
updated: "2026-07-04"
depends_on: []
spec_refs: []
---

## Description
BACKLOG.md §Open design (M5): placement is whole-value "next RF nodes," which is **not** EC-ready (EC needs per-shard-of-a-value placement). Either design the EC-ready placement metadata first, or downgrade the spec's EC-ready claim in ARCHITECTURE.md.

**Needs owner decision** — this is a fork: a real design effort vs a one-paragraph docs change.

## Acceptance criteria
- Decision recorded. Either a design doc for EC-ready placement metadata exists (no implementation required yet), or ARCHITECTURE.md/docs no longer claim EC-readiness.

## Acceptance criteria

## Plan

DECISION (owner, 2026-06-27): downgrade the claim — docs-only change, no EC metadata design. Edit ARCHITECTURE.md (§Planned subsystems "EC-ready placement metadata" and any EC-ready aspiration wording) and BACKLOG.md §Open design pointer to state plainly: placement is whole-value next-RF-nodes and erasure coding is out of scope unless placement is redesigned; remove/qualify "EC-ready" language. Small task: verify with a doc grep for "erasure|EC-ready" across README/ARCHITECTURE/docs.

## Work log
- 2026-07-02 plan: DECISION (owner, 2026-06-27): downgrade the claim — docs-only change, no EC metadata design. Edit ARCHITECTURE.md (§Planned subsystems "EC-ready placement metadata" and any EC-ready aspiration word
…[truncated]
- 2026-07-04 context hints: ARCHITECTURE.md:601-656 — §Planned subsystems intro + 'EC-ready placement metadata' bullet; BACKLOG.md:1013-1017 — §Open design 'Erasure coding (M5)' entry; spec.md:105 — decision already reco
…[truncated]
- 2026-07-04 implementer report: Docs-only downgrade of the EC-ready claim per the owner decision (2026-06-27, task 0023). No code changes.  Changes: 1. ARCHITECTURE.md §Planned subsystems intro (~line 603): reworded so erasure codi
…[truncated]
- 2026-07-04 review tier: simple (coordinator self-review)
- 2026-07-04 decision: accept — commit: docs: downgrade the EC-ready placement claim — erasure coding out of scope (task 0023)
- 2026-07-04 usage: 5,695 tok (in 26, out 5,669, cache_r 208,526, cache_w 22,202) · cost n/a (unpriced)
  coordinator: 3,492 tok (in 16, out 3,476, cache_r 178,727, cache_w 12,432) · cost n/a (unpriced)
  implementer: 2,203 tok (in 10, out 2,193, cache_r 29,799, cache_w 9,770) · cost n/a (unpriced)

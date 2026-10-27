---
id: "0187"
title: Survey optional and weakly validated features for cleanup
status: done
priority: 2
created: "2026-08-12"
updated: "2026-08-12"
depends_on: []
spec_refs:
    - spec.md#Constraints / conventions
    - ARCHITECTURE.md
---

## Description
Audit the current tree and recent shipped work for feature gates, environment-variable toggles, compatibility/legacy branches, optional subsystems, and experiments that were never validated or did not demonstrate a benefit. Produce an evidence-based cleanup inventory grouped into keep/default/remove/retest, identify dependencies and risk, and propose a staged cleanup plan. Do not remove behavior until the owner reviews the survey.

Acceptance criteria:
- Inventory optional runtime/build-time features and major legacy/experimental branches.
- Connect each candidate to available benchmark/validation evidence from backlog, plans, docs, and git history.
- Recommend keep, remove, consolidate, or short A/B validation with rationale and cleanup risk.
- Record agreed cleanup work as follow-up backlog tasks after owner review.

## Acceptance criteria

## Work log

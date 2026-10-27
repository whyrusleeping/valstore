---
id: "0407"
title: 'Local-first validation policy: validation tiers, per-task tier, triage of the open backlog'
status: done
priority: 2
created: "2026-10-09"
updated: "2026-10-09"
depends_on: []
spec_refs: []
---

## Description
## Why
Hardware validation is the main bottleneck on backlog throughput. Many tasks ask for a full or near-full hardware replay by default, even when their acceptance could be established locally, from existing data, or with a much smaller hardware step.

## Scope
- Write docs/validation.md. It defines the validation tiers:
  - L0: local tests and analysis of existing data;
  - L1: local scale model;
  - L2a: retained corpus (0404);
  - L2b: scaled-down profile (0405);
  - L2c: short targeted A/B;
  - L3: the full replay release gate.

  The policy is local-first. Hardware is used for confirmation, as the smallest step that can answer the question, and full replays are batched rather than run once per fix.
- Link it from spec.md "Constraints / conventions" and docs/ci.md.
- Triage the current open tasks by cheapest valid tier, and record the result in docs/validation.md so later tasks can follow the pattern.

## Acceptance
- docs/validation.md exists and is linked from the places above.
- The triage table is recorded in it.
- New tasks state their intended validation tier.

## Acceptance criteria

## Work log

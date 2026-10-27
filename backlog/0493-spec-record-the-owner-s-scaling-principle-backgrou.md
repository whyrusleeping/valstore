---
id: "0493"
title: 'spec: record the owner''s scaling principle (background/per-shard cost must not grow with total cluster data or range count)'
status: done
priority: 4
created: "2026-10-21"
updated: "2026-10-26"
depends_on: []
spec_refs: []
---

## Description
Promotion candidate from project memory note m-dma3v1fvvwbx (user guidance, 2026-10-20):

> "Owner 2026-10-20: vals must handle massive datasets; per-shard or background costs that grow with total cluster data or range count (not the shard's own share or what changed) are scaling bugs worth fixing."

spec.md has no general statement of this principle; it exists only as task-local context in 0469 ("Aim for background range work that scales with what changed, not with the total range count").

## Acceptance
- With owner approval, spec.md (constraints/principles section) states the principle in one or two sentences and links 0469 as the current known instance.
- Memory note m-dma3v1fvvwbx is retired once the spec carries it.

## Acceptance criteria

## Work log

- 2026-10-26: owner accepted. spec.md Constraints / conventions now states the principle ("Scale with what changed") and names 0469, 0502 and 0503 as examples; memory note m-dma3v1fvvwbx retired.

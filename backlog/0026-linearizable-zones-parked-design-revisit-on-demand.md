---
id: "0026"
title: Linearizable zones (parked design — revisit on demand)
status: done
priority: 5
created: "2026-07-02"
updated: "2026-08-07"
depends_on: []
spec_refs: []
---

## Description
**Blocked on a fresh owner decision** — parked by design; do not pick up without an explicit go-ahead.

Parked by design: true linearizable CAS / in-partition multi-key atomicity needs a per-key consensus path — out of the default leaderless data path. The captured design is an opt-in per-zone mode routed through per-range consensus groups: `docs/future-linearizable-zones.md`. Owner decision 2026-06-27: keep parked but tracked as a low-priority design task (no implementation without a fresh decision). Reconfirmed by owner 2026-07-05: still parked — no workload needs linearizable CAS.

## Acceptance criteria
- Only actionable if/when a workload needs linearizable CAS: refresh the parked design against the current codebase (zones, range maps, PD) and produce a build/no-build recommendation.

## Acceptance criteria

## Work log

- 2026-08-07 CLOSED (won't-do, owner decision): no workload needs linearizable CAS. The design stays captured in `docs/future-linearizable-zones.md`; re-file only when a workload demands it.

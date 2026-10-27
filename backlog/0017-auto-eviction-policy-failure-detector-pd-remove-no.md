---
id: "0017"
title: Auto-eviction policy (failure detector → PD remove_node)
status: done
priority: 4
created: "2026-07-02"
updated: "2026-08-07"
depends_on: []
spec_refs: []
---

## Description
BACKLOG.md §Operability: the failure detector is built and routing-only; optionally let a *durably* down node (tripped ≫ cooldown) feed `PdDaemon::remove_node` so the ring reshapes. **Deliberately deferred** — the seam is `remove_node`; the hard part is a conservative timeout so a transient blip doesn't trigger data movement (data movement is expensive and wrong-eviction is dangerous).

**Needs owner decision**: is auto-eviction wanted at all, or does membership stay an explicit operator action (current stance)?

**DECISION (owner, 2026-07-05): keep parked as a someday-maybe.** Membership stays an explicit operator action for now; do not build without a fresh go-ahead.

## Acceptance criteria
- Decision recorded. If built: conservative multi-signal policy (detector + lease/watermark evidence), opt-in, with a simulation/integration test showing a transient blip does NOT evict.

## Acceptance criteria

## Work log

- 2026-08-07 CLOSED (won't-do, owner decision): membership stays an explicit operator action. Re-file if that changes; the seam is still `PdDaemon::remove_node`.

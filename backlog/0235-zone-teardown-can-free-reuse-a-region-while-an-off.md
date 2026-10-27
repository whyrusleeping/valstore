---
id: "0235"
title: Zone teardown can free/reuse a region while an off-core maint job still writes to it
status: done
priority: 2
created: "2026-08-23"
updated: "2026-09-05"
depends_on: []
spec_refs: []
---

## Description

Found during the 0232 audit. `ServerShared::drop_zone` moves a zone into `pending_drops` and `complete_pending_drops` releases the zone's storage as soon as `Rc::strong_count(&p.store) == 1` (net/server/shared.rs ~982-1016). An in-flight off-core flush or compaction submitted to the maint worker holds only the `FlushPlan`/`CompactPlan` (reserved segments + `Arc<SubDev>` device handle), NOT the store `Rc`, so teardown can proceed while the maint thread is still writing to the plan's reserved segments. `release_zone_storage` → `RegionZoneStore::drop_zone` frees the region in the region table; under rehome churn the region can be immediately reallocated to a NEW zone, whose data the still-running maint job then scribbles (cross-zone on-disk corruption). The `MaintDone::Flushed` "zone is gone; dropping" handler only runs after the write completed.

Likely fix direction: make pending-drop completion also wait for zone-scoped in-flight maint work (e.g. consult `flush_builds`/a per-zone in-flight maint count, or have plans hold a teardown guard), and/or have the maint worker check a zone-generation fence before writing.

Acceptance criteria:
- A pending zone drop does not release region storage while an off-core flush/compact for that zone is still running (test demonstrating the old interleaving is fenced).
- No regression in prompt teardown (0228): `vals_zone_drops_deferred` still drains once maint work finishes.

## Outcome

Added a per-zone in-flight maintenance fence covering off-core flush, compaction, and cleanup compaction; pending drops cannot release/reuse storage until jobs finish and retry teardown promptly on completion. Real-worker flush/compaction regressions plus existing teardown/recreate tests pass; cargo check and full debug library suite passed (571 passed, 3 ignored). Two independent reviewers accepted. Also recorded 0233's hardware blocker (lab-2 rebooted, ingest stalled) without altering fleet experiments. Unrelated live driver metrics capture excluded from commit.

Commit: fix(cluster): defer zone storage release until maintenance jobs finish

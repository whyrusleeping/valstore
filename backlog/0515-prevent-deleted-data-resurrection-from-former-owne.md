---
id: "0515"
title: Prevent deleted data resurrection from former owners after tombstone GC
status: done
priority: 1
created: "2026-10-28"
updated: "2026-10-28"
depends_on: []
spec_refs:
    - docs/backup-restore.md#Consistency contract
    - ARCHITECTURE.md#Deletes and tombstones
---

## Description

Accepted correctness follow-up to adopter review 0507, finding 5. The backup guide documents that a stale put on a former owner can reappear after current owners GC a newer tombstone, and says anti-entropy has the same exposure. Isolate and fix this scenario rather than treating a larger backup drill as sufficient.

Acceptance criteria:
- Build a deterministic local reproduction involving topology/ownership change, stale former-owner data, and collected point/range tombstones as applicable.
- Establish how current placement, pending rehomes, cleanup and GC/liveness watermarks should fence stale candidates in backup merge and anti-entropy/read repair without omitting valid acknowledged records.
- Implement the smallest correct mitigation and add regression tests that fail without it, including interrupted/pending rehomes and backup/restore deletion checks.
- Verify any hot-path change on appropriate hardware; if the resolution is a supported restriction rather than implementation, make that restriction enforceable and explicit, not only a rarity claim.
- Update the known-limitation documentation only when the corresponding regression proves the exposure closed.

## Outcome

Former-owner copies whose newer delete has been garbage-collected can no longer come back: - Backup create fences puts held only by non-owners if they sit below some owner's GcFloor (new op 24). It fails by default; `--stale-copies` overrides. - Anti-entropy pulls only keys both peers own. - A re-home copy is held while its target has an old value the source lacks. - With tombstone GC on, `add-node` and `remove-node` refuse until every member that stays has a clean cleanup lap under its current placement (reported by the new CleanupStatus op 25), no re-home is pending or held, and a joining node is empty. `--force` skips this check. Also fixed: sorted-zone cleanup no longer stalls on a copy it cannot hand off. Regressions use real tombstone GC and fail without the fix. The docs include 0505's uncommitted edits to the same files. No hardware run, since no client path changed. This covers proposed 0518; 0517 is still open.

Commit: 0515: fence former-owner copies whose deletes were collected (backup merge, AE scope, re-home hold, PD membership-change cleanup gate)

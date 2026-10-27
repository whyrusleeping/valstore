---
id: "0345"
title: Point docs/deploy.md and spec.md PD admin sections at docs/pd-admin.md (after 0338 lands)
status: done
priority: 4
created: "2026-09-29"
updated: "2026-10-01"
depends_on:
    - "0282"
spec_refs: []
---

## Description

Split from 0282. 0282 added leader-aware, commit-aware PD administration (`status`, `ranges`, `ok committed` / `ok queued` / `err:` / `unknown:` reply contract, exit codes 0/1/3, authoritative vs non-authoritative inspection) documented in docs/pd-admin.md. docs/deploy.md "Administering a running cluster" (~line 506-538) and spec.md still describe the old behaviour ("reply `ok proposed`", "no leader discovery"). Both files carry the owner's uncommitted 0338 edits, so they were not edited in 0282.

Acceptance: once docs/deploy.md and spec.md are clean in git, replace their stale admin-semantics prose with a concise pointer to docs/pd-admin.md (keep the command table in deploy.md, add `status` and `ranges <zone>`); remove the "update those references" sentence at the end of docs/pd-admin.md; do not duplicate the reply contract or imply cross-group fencing (0283).

## Outcome

docs/deploy.md: command table gains `status` and `ranges <zone>`; the stale "no leader discovery / reply `ok proposed`" paragraph and the PD-unavailability table row now point to docs/pd-admin.md for the leader-location, reply-contract/exit-code, and authoritative-inspection workflow (contract not duplicated, no cross-group fencing implied). docs/pd-admin.md's "update those references" sentence removed. spec.md needed no change: after 0338 it no longer carries PD admin prose.

Commit: docs(0345): deploy.md PD admin section points to pd-admin.md; add status/ranges to command table; drop stale ok-proposed prose

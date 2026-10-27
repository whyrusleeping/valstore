---
id: "0266"
title: Define external-release licensing, notices, and crate publication intent
status: done
priority: 4
created: "2026-09-07"
updated: "2026-10-12"
depends_on: []
spec_refs:
    - docs/production-readiness-review-2026-08.md#R7 — The repository is not legally/package/release ready (P0 distribution)
---

## Description
External-release prerequisite, not an external-release authorization. Accepted as conditional work in the 2026-09-29 triage; blocked until the owner selects publication/licensing intent. The redundant release umbrella 0244 was removed; this task owns the actual obligation. Confirm licensing/publication intent with the owner; supply appropriate license/notices, real repository metadata, and explicit publishability for each crate. Publishable crates need versioned path dependencies and package metadata. Validate package dry-runs and third-party notice coverage. Do not silently choose a new license or publish artifacts.

## Acceptance criteria

## Work log

- 2026-10-12 (owner decision): closed source for now; nothing is published. Workspace metadata now says `license = "LicenseRef-Proprietary"` and `publish = false` (inherited by every crate), replacing the incorrect Apache-2.0 declaration; recorded in spec.md. Whether to sell vals as a product is an open business decision, not backlog work. Closed; file a new task if a release/product path is chosen.

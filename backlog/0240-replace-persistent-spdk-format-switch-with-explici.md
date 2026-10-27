---
id: "0240"
title: Replace persistent SPDK format switch with explicit offline provisioning
status: done
priority: 1
created: "2026-08-23"
updated: "2026-09-08"
depends_on: []
spec_refs:
    - docs/production-readiness-review-2026-08.md#R6 — `format = true` is a persistent restart-time data-destruction switch (P0 operational safety)
---

## Description

Remove `format = true` from normal daemon startup. Add a separate offline destructive format/provision command that verifies device identity/serial and requires explicit confirmation; normal node startup must only open an existing compatible layout and fail closed otherwise. Add restart tests proving ordinary daemon restarts cannot format data.

## Outcome

Removed NodeConfig.format capability: legacy true is rejected, false inert; both daemon SPDK topologies reopen only and refuse blank/incompatible or missing-default-zone layouts. Added offline valsd provision with exact PCI/attached serial checks, explicit node confirmation, exclusive device locks shared by node/dump, and all-device geometry preflight before writes. Updated deployment guidance and active runbooks. Two independent safety reviewers accepted. Verification: cluster lib suite 588 passed/4 ignored; server tests passed except known timing-sensitive PD assertion that passed isolated; coordinator reran 16 daemon unit tests and focused provisioning tests; SPDK feature check, clippy and diff checks passed. No disks touched; C shim linked-build/device validation remains proposed task 0274.

Commit: Replace SPDK startup formatting with explicit offline provisioning

---
id: "0265"
title: Move supported correctness-affecting environment switches into typed config
status: done
priority: 2
created: "2026-09-07"
updated: "2026-09-29"
depends_on:
    - "0243"
spec_refs:
    - docs/production-readiness-review-2026-08.md#R16 — Move correctness-affecting environment switches into typed startup config (P1/P2)
---

## Description

Separate broader migration proposal from the accepted bounded validation slice in 0243. Inventory correctness-affecting VALS_* switches, decide which belong to the supported product, and migrate those into parsed startup configuration. Do not mechanically productize every experimental A/B knob.

Acceptance before implementation: record supported-versus-experimental dispositions and precedence/migration policy. Implementation must validate malformed values and cross-node capability requirements, export/log effective settings, and preserve placement/rehome safety under mixed versions. The owner accepted this cleanup work alongside 0338; a broad migration must still begin with those concrete dispositions rather than mechanically promoting all environment variables to public config keys.

## Acceptance criteria

- First bounded slice: identify the supported PD placement/rehome and node maintenance/resource settings, distinguish diagnostic/A/B controls, and record defaults and migration precedence from current code and deployment use.
- Pass typed settings into the actual implementation, resolving legacy environment input once at startup; reject malformed or conflicting inputs with actionable errors and report effective settings.
- Preserve existing default behavior, placement/capability safety, and on-disk compatibility. Do not remove the baseline seed implementation ahead of 0330's oracles or incident instrumentation needed by blocked hardware tasks.
- Replace subprocess environment tests with per-instance configuration tests as the affected components gain injectable settings; retain subprocess tests for startup/environment parsing itself.
- Run debug suites and any behavior/performance hardware gates required by the affected settings. No fleet reprovisioning without a separate retention decision.

## Outcome

Slice 1 accepted (standard review, accept). All 16 PD placement/rehome switches are now optional typed PD config keys resolved once at startup (config authoritative; legacy env accepted when absent with deprecation log; differing key+env or malformed/out-of-range values fail startup; --check-config uses same resolution); pusher gets per-instance settings; public spawn* keep signatures with defaults, settings variants used by vals-server and vals-bench cluster_live. Defaults, V4 capability/balance-execute gating and on-disk formats unchanged. docs/configuration.md records supported/pending/diagnostic/incident/test dispositions, defaults, precedence and rollback for every VALS_* switch; zones.md/ARCHITECTURE.md name config keys. Verified: workspace build, vals-server tests, vals-cluster lib debug suite (709 pass), clippy, reviewer rebuild. Node-side migration (dirty 0338 files) split to 0347.

Commit: config(0265): typed PD placement/rehome settings with strict legacy env resolution; docs/configuration.md dispositions; node-side migration split to 0347

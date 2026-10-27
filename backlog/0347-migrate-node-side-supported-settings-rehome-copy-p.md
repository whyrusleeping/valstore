---
id: "0347"
title: Migrate node-side supported settings (rehome copy paging, maintenance, RAM, seed permits, transport, scan inflight) into typed node config (after 0338 lands)
status: done
priority: 2
created: "2026-09-29"
updated: "2026-09-30"
depends_on:
    - "0265"
spec_refs:
    - docs/production-readiness-review-2026-08.md#R16 — Move correctness-affecting environment switches into typed startup config (P1/P2)
---

## Description

Split from 0265 (owner-accepted scope). 0265 slice 1 migrated the 16 PD placement/rehome switches into typed PD config (`PdPlacementSettings`, strict legacy-env resolution, conflict errors, effective-setting logs) and recorded dispositions for ALL `VALS_*` switches in docs/configuration.md. The node-side migration was deferred because the affected files carry the owner's uncommitted 0338 work (crates/vals-cluster/src/lib.rs, maint.rs, node/maintenance.rs, net/tests.rs, node/tests.rs, testutil.rs, replica/seed_permit.rs, docs/deploy.md) and the commit tooling cannot attribute edits to dirty paths.

Start only once those 0338 changes are committed or otherwise resolved.

Acceptance:
- Migrate the settings docs/configuration.md marks "supported, pending migration" (node rehome copy paging/hold, flush/compact maintenance cadence and thresholds, RAM budget and broker reserves, seed-permit concurrency/cooldown, transport limits, scan max inflight, cleanup confirms, as dispositioned there) into typed node config, using the same precedence policy as 0265 (config key authoritative; legacy env accepted when absent with deprecation log; differing key+env ⇒ startup error; malformed/out-of-range ⇒ actionable error). Resolve once at startup; log effective settings; `--check-config` exercises the same resolution.
- Pass settings per instance into the implementation; replace subprocess environment tests (e.g. net/tests.rs `VALS_REHOME_COPY_PAGE_ENTRIES` isolated_env_test cases, maint.rs `VALS_MERGE_TIER_MIN_K` if dispositioned supported) with per-instance configuration tests where components gain injectable settings; keep subprocess tests only for env parsing itself.
- Preserve defaults, placement/capability safety and on-disk compatibility. Do not migrate diagnostic/A-B controls or incident instrumentation; do not remove the baseline seed path (that is 0346).
- Link docs/configuration.md from docs/deploy.md and update docs/backpressure.md env references to name config keys.
- Run debug workspace suites; hardware gates only if a setting's behaviour (not just plumbing) changes. No fleet reprovisioning.

## Outcome

All 25 pending-migration node settings are typed valsd node config keys (env name minus VALS_, lowercased), resolved once at startup with 0265 precedence (key wins; alias fallback warns; conflict/malformed/out-of-range => error); --check-config shares resolution and prints effective values. Per-node settings injected via *_with_settings serve variants; process-wide resources configured set-once; components no longer read these env vars. VALS_LOG strict vs log_level; PD transport stays strict env. Env-mutating tests replaced with per-instance settings; docs updated. Defaults/wire/disk unchanged. Debug cluster/store/server suites pass; comprehensive review 2/2 accept.

Commit: feat(0347): typed node config for supported node settings with strict key/legacy-alias precedence, check-config parity and env-free components; docs

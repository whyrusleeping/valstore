---
id: "0338"
title: 'Close cleanup audit: trustworthy tooling, test seams, supported paths and stale guidance'
status: done
priority: 2
created: "2026-09-29"
updated: "2026-09-29"
depends_on: []
spec_refs: []
---

## Description
Owner accepted implementing the cleanup audit in conversation. Scope: fix netread-fleet false success and seed-mode mislabel; remove destructive pre-provision discard from current runbook; repair seed-budget coverage and use one seed-permit implementation in tests/production; eliminate identified process-environment test leakage; isolate maintenance test scaffolding and remove blanket dead-code suppression; retire obsolete harness entry points without deleting evidence; remove stale frontier/default/status claims; repair backlog discovery; narrow experimental configuration and move server observability out of the SDK where bounded and safe. Preserve pre-existing dirty/untracked work, retained incident evidence, on-disk compatibility and progress fallbacks. No destructive hardware operations or blanket feature deletion. Coordinate remaining typed-config scope with 0265 and seed baseline removal with 0330 rather than bypassing their evidence gates.

Acceptance: targeted regression tests for tooling failure paths and test seams; debug workspace build/tests and clippy where feasible; documented precise remaining scope for hardware-gated or larger follow-ups. Changes remain uncommitted unless requested.

## Acceptance criteria

- [x] Tooling fails closed on failed/missing/malformed benchmark results and retains failure logs; obsolete destructive entry points are retired with regression tests.
- [x] Seed limiter tests execute the real atomic implementation; seed-budget baseline and stream arms are explicit; benchmark labels use resolved mode.
- [x] Identified environment-sensitive tests are isolated from the parallel harness; maintenance-only test scaffolding is absent from production.
- [x] Server observability lives outside the SDK without duplicate client counter instances or changed exported metric schema.
- [x] Retired scan selectors and stale current-status claims are removed; historical evidence and pre-existing work are preserved.
- [x] Debug workspace build/tests, targeted regressions, clippy, and diff checks run; larger migration and hardware gates remain explicit.

## Work log

- 2026-09-29: netread-fleet now waits for each child, requires exactly one valid result, rejects client errors/misses/mismatches/zero work and retains logs on failure. Added nine local tests (including mixed child outcomes, valid output followed by failure, and malformed/duplicate output).
- Retired host-setup `--blkdiscard` and removed destructive pre-provision recipe steps; canonical offline identity/retention checks precede provisioning. Corrected source/binary provenance guidance and rsync scope. No hardware commands were executed.
- Retired direct hardware CLI execution in RANGEMAP01, SCALE01 and task0304 ancestors; retained imports/local analysis and documented the LONG01 lineage in bench/HARNESSES.md. Added three retirement/import/help tests. No benchmark artifacts removed.
- SeedPermit now uses one atomic Limiter for production and test instances, with cap contention, cross-thread release, cooldown and nested-state regressions. seed_budget explicitly selects both modes; capacity output reads the actual resolver.
- Environment-dependent cluster/store tests run in exact-name child processes using Command-configured settings, requiring successful exit and a one-passed-test summary. Dynamic store A/B changes remain confined to those children. live_keys_gauge already has one test per integration process. The staged-batch degradation test had an obsolete subprocess filter that ran zero tests; switched it to the checked helper and verified the actual test passes.
- Removed maintenance blanket dead_code suppression and unused reexports; Echo/Echoed, submit and spawn are test-only. Removed VALS_STAGED_SCAN and VALS_SCAN_PREFETCH environment selectors. Kept synchronous scan progress fallback and the explicit client set_scan_prefetch API.
- SDK observability reduced from the server-wide registry to ten client counters and primitives. Cluster owns 202 server counters and server utilities; client counter identity is shared through both paths. Schema comparison verified all 212 counter names/HELP unchanged; gauge/histogram registry and renderer unchanged. Server-only standalone SDK observe APIs now use vals_cluster::observe; documented migration.
- Removed stale README/spec/SPDK frontier claims and corrected client defaults. Marked old cleanup survey and 0294 phase notebook as historical; 0294's existing dirty contents are byte-identical below the added banner. Replaced stale backlog status table with a canonical-directory/tools hub.
- Full-suite verification exposed two test-fixture races: liveness used a released ephemeral peer port, and control_auth queued boot membership only on PD1 despite either voter being eligible for leadership. Reserved a nonresponsive peer listener and supplied identical boot intent to both voters; no production liveness/PD code changed.
- Verification: cargo build --workspace passed; final cargo test --workspace --no-fail-fast passed (including 699 cluster unit tests and 328 store unit tests, plus other crates/integrations/doctests). Earlier runs' liveness/control_auth failures and isolated passes led to the fixture repairs above, not ignored failures. The final staged-batch subprocess correction passed its exact test and cluster library rerun. cargo clippy --workspace --all-targets completed with warnings in existing code; no blanket formatting applied. All 12 Python tooling tests, shell syntax checks and git diff --check passed. Independent focused source reviews of Rust isolation/limiter and tooling/docs found no blocking issues.
- Remaining accepted work: task0265 owns the broader typed-startup configuration migration and supported-versus-diagnostic dispositions; it is now todo, not claimed completed here. Baseline seed removal still requires 0330's correctness oracles. Incident instrumentation and retained SHUT0326-F2-R2 drives remain untouched. Changes are uncommitted.

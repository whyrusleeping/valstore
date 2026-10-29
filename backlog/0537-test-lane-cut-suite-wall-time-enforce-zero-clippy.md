---
id: "0537"
title: 'Test lane: cut suite wall time, enforce zero Clippy warnings, cache and doc-check in CI'
status: in_progress
priority: 2
created: "2026-10-29"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description
From the 2026-10-29 project review; owner asked to fix the identified test issues, especially test time.

1. **Suite wall time.** Measured on 2026-10-28 debug binaries: `vals_store` lib 564 tests = 130 s, set by `engine::tests::keyfilter::property_global_filter_covers_every_covered_run_and_matches_per_run_probes` (130 s), `keyfilter::rebuilt_filter_has_scaled_resident_and_compound_fpr_bounds` (95 s) and `index::tests::front_coded_matches_reference_with_remove_if_eq` (48 s); vals-cluster integration `churn_reclaim` 76 s. Scale these to a default-lane size that keeps their coverage meaningful, with the full size behind an env var (documented in plans/testing-notes.md); each still fails when the property it guards is broken.
2. **Clippy.** Warnings drifted 90 → 144 lines. Fix them, including the two production `await_holding_refcell_ref` cases (net/server/placement.rs ~422, net/server/range_delete.rs ~165; check they cannot panic), allow only genuinely noisy style lints in a workspace `[workspace.lints.clippy]` table, and make `scripts/ci.sh` fail on Clippy warnings (`-D warnings`).
3. **CI.** Add a cargo build cache to .github/workflows/ci.yml and a `cargo doc --workspace --no-deps` step with `RUSTDOCFLAGS=-D warnings` in scripts/ci.sh (docs point users to `cargo doc`); update docs/ci.md.

Acceptance: `scripts/ci.sh` passes with zero Clippy warnings and the doc step; the vals-store lib test binary's wall time is no longer dominated by those tests (report before/after); docs/ci.md describes the lane exactly.

Timing-assertion flakes are tracked separately in 0526.

## Acceptance criteria

## Work log

- 2026-10-29 (uncommitted, in the main worktree):
  - Test time: vals-store lib binary 111 s → 29 s; keyfilter property 109 → 7 s, rebuilt_filter 72 → 5 s, index remove_if_eq 37 → 3 s (plus three more same-cause tests), churn_reclaim 65 → 12 s. Full sizes behind `VALS_TEST_FULL=1` (plans/testing-notes.md). Each default lane still fails the knockouts tried (keyfilter merge/flush/cover/probe knockouts, index integrity-update bugs, reclaim disabled / frees nothing, filter leak / undersized rebuild / one-hash FPR). The rebuilt_filter RSS check now measures `RssAnon` (VmRSS included ~4.4 MB of debug code pages).
  - Clippy: `[workspace.lints.clippy]` allows only type_complexity, too_many_arguments, large_enum_variant; every crate opts in; all other warnings fixed. The two production `await_holding_refcell_ref` reports (placement.rs mirror_rehome_writes, range_delete.rs mirror_rehome_range_delete) were false positives (explicit `drop` before the await); restructured into lexical scopes. ci.sh runs Clippy with `-D warnings` and a `cargo doc` step with `RUSTDOCFLAGS=-D warnings`; ci.yml uses Swatinem/rust-cache@v2; docs/ci.md and CONTRIBUTING updated.
  - `VALS_CI_LOG_DIR=/tmp/ci-0537 scripts/ci.sh`: build, all workspace tests (172 s), Python and doc steps pass; Clippy fails only on three pre-existing lints in files 0523's live session has dirty: net/server/staged_apply.rs ~3201–3202 (`int_plus_one`, `>= x + 1`) and net/server/connection.rs ~489 (`nonminimal_bool`). Fix those once 0523 lands, rerun ci.sh, then close.

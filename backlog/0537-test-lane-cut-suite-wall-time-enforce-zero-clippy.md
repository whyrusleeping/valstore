---
id: "0537"
title: 'Test lane: cut suite wall time, enforce zero Clippy warnings, cache and doc-check in CI'
status: done
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

## Outcome

Implementation committed as 8d84ac5 (62 files, split from the shared worktree's 0526/0538 work): long tests scaled (vals-store lib 111 s → 28.6 s; full sizes via VALS_TEST_FULL=1, documented in plans/testing-notes.md), workspace Clippy lint table and zero warnings enforced with -D warnings, cargo doc step with RUSTDOCFLAGS=-D warnings, Swatinem/rust-cache in CI, docs/ci.md updated. scripts/ci.sh on that tree alone exits 0 on every step.

Commit: 0537: close test-lane task (implementation in 8d84ac5): work log and CI evidence

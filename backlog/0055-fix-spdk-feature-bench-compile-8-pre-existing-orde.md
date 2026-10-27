---
id: "0055"
title: 'Fix spdk-feature bench compile: 8 pre-existing Ordering type mismatches'
status: done
priority: 4
created: "2026-07-05"
updated: "2026-07-05"
depends_on: []
spec_refs: []
---

## Description
`cargo check -p vals-bench --features spdk` fails on master with 8 pre-existing type mismatches: `vals_store::Ordering` vs `vals_cluster::Ordering` (`Ordering::Unsorted`/`Ordering::Sorted` passed where the other crate's enum is expected) in the SPDK-gated workloads `distmd`, `distmdmove`, `distmdfail`, `distzone`, `distsorted`. Found during task 0020 (clean SPDK teardown): the spdk-featured bench does not currently compile, which blocks any on-box bench run (including the teardown box validation). Likely drift from a re-export change; fix is to convert/use the expected enum at each call site (or re-unify the re-export).

## Acceptance criteria
- `cargo check -p vals-bench --features spdk` exits 0 (VALS_SPDK_LINK unset, type-check only).
- No behavior change to the default (non-spdk) build; workspace tests stay green; clippy zero warnings.

## Acceptance criteria

## Plan

Fix the 8 pre-existing type mismatches in the spdk-featured vals-bench build by using the expected enum at each call site, without touching the crates' re-exports (lowest-risk, no behavior change to non-spdk builds).

1. `cargo check -p vals-bench --features spdk` currently fails with 8 E0308s in crates/vals-bench/src/main.rs at lines 1063, 1166, 1286, 1858, 2343, 2372, 2571, 2603 (SPDK-gated workloads distmd, distmdmove, distmdfail, distzone, distsorted). At each site a `vals_store::Ordering` value is passed where `vals_cluster::Ordering` (re-export of `vals_client::zone::Ordering`) is expected — either as `PartitionGeom { ordering: ... }` or as the second arg to `ZoneStore::open(...)`.
2. At each of the 8 sites, replace the bare `Ordering::Unsorted` / `Ordering::Sorted` with `vals_cluster::Ordering::Unsorted` / `vals_cluster::Ordering::Sorted` (keep the top-level `use vals_store::{... Ordering ...}` import as-is, since the rest of the file legitimately uses the store enum for `bench_config`/`Engine`).
3. Do NOT re-unify the enums or change re-exports in vals-store/vals-cluster/vals-client — out of scope and riskier.
4. Verify:
   - `cargo check -p vals-bench --features spdk` exits 0 (VALS_SPDK_LINK unset).
   - Default build unaffected: `cargo build --workspace` and `cargo test --workspace` green.
   - `cargo clippy --workspace --all-targets` zero warnings, and clippy with `--features spdk` on vals-bench also clean if it runs without SPDK libs.
   - `cargo fmt --check` on the touched file introduces no new drift (note: task 0053 says there is pre-existing fmt drift elsewhere; don't fix that here).

### Starting points
- crates/vals-bench/src/main.rs lines 1063, 1166, 1286, 1858, 2343, 2372, 2571, 2603 — the 8 mismatch sites (PartitionGeom { ordering: ... } and ZoneStore::open(DEFAULT_ZONE, ...))
- top-of-file import: `use vals_store::{Config, Engine, Ordering, RunBuildOpts};` — keep it; qualify the cluster enum at call sites
- vals_cluster::Ordering is a re-export of vals_client::zone::Ordering (crates/vals-client/src/zone.rs:27)
- verify with: cargo check -p vals-bench --features spdk (VALS_SPDK_LINK unset, type-check only)

## Work log
- 2026-07-05 plan: Fix the 8 pre-existing type mismatches in the spdk-featured vals-bench build by using the expected enum at each call site, without touching the crates' re-exports (lowest-risk, no behavior change to n
…[truncated]
- 2026-07-05 context hints: 4 recorded with plan
- 2026-07-05 context hints: crates/vals-bench/src/main.rs lines 1063, 1166, 1286, 1858, 2343, 2372, 2571, 2603 — the 8 E0308 sites; expected type is vals_cluster::Ordering (re-export of vals_client::zone::Ordering, crates/vals
…[truncated]
- 2026-07-05 implementer report: Fixed the 8 pre-existing type mismatches in the spdk-featured vals-bench build.  ## Change In `crates/vals-bench/src/main.rs`, at the 8 SPDK-gated call sites (distmd/distmdmove/distmdfail/distzone/dis
…[truncated]
- 2026-07-05 review tier: simple (coordinator self-review)
- 2026-07-05 decision: accept — commit: fix(bench): use vals_cluster::Ordering at the 8 spdk-gated call sites (task 0055)  The spdk-featured vals-bench build failed with 8 E0308s: vals_store::Ordering (the file's top-level import) was passe
…[truncated]
- 2026-07-05 usage: 8,138 tok (in 46, out 8,092, cache_r 331,265, cache_w 26,638) · cost n/a (unpriced)
  coordinator: 5,045 tok (in 26, out 5,019, cache_r 246,325, cache_w 14,611) · cost n/a (unpriced)
  implementer: 3,093 tok (in 20, out 3,073, cache_r 84,940, cache_w 12,027) · cost n/a (unpriced)

---
id: "0053"
title: Fix pre-existing cargo fmt drift (17 sites) under pinned 1.96 toolchain
status: done
priority: 5
created: "2026-07-04"
updated: "2026-07-05"
depends_on: []
spec_refs: []
---

## Description
`cargo fmt --check` under the pinned 1.96.0 toolchain reports 17 pre-existing diffs in files untouched by task 0037 (verified via `git stash` against the clean tree): vals-cluster/src/{hints.rs:297, net/server.rs:1611, node.rs:1456, replica.rs:934/949/4266, zonestore.rs:427/452/476/1430/1820/1831}, vals-cluster/tests/netsim.rs:265/480/1012, vals-server/src/config.rs:847, vals-server/tests/daemon.rs:416. These predate 0037 (likely fmt drift from edits made under a different toolchain, or manual formatting).

Run `cargo fmt` (pinned toolchain), review the diff is whitespace/line-wrap only, and land it.

## Acceptance criteria
- `cargo fmt --check` exits 0 under the pinned 1.96.0 toolchain.
- Diff is formatting-only (no token changes); `cargo clippy --workspace --all-targets` still zero warnings; workspace tests green.

## Acceptance criteria

## Plan

1. Confirm drift with `cargo fmt --check` under the pinned 1.96.0 toolchain (done — drift present at the expected sites).
2. Run `cargo fmt` across the workspace.
3. Verify the resulting diff is formatting-only: inspect `git diff` and confirm no token changes (whitespace/line-wrap only), e.g. by comparing token streams before/after (`git diff --word-diff` review or rustfmt idempotence + build).
4. Verify: `cargo fmt --check` exits 0; `cargo clippy --workspace --all-targets` zero warnings; `cargo test --workspace` green.
5. This is a mechanical, tool-generated change — self-review (simple tier), then commit.

## Work log
- 2026-07-05 plan: 1. Confirm drift with `cargo fmt --check` under the pinned 1.96.0 toolchain (done — drift present at the expected sites). 2. Run `cargo fmt` across the workspace. 3. Verify the resulting diff is for
…[truncated]
- 2026-07-05 review tier: simple (coordinator self-review)
- 2026-07-05 decision: accept — commit: style: fix pre-existing cargo fmt drift under pinned 1.96 toolchain (task 0053)  Mechanical `cargo fmt` across the workspace: line rewraps and rustfmt trailing commas only, no semantic changes. fmt --
…[truncated]
- 2026-07-05 usage: 3,809 tok (in 26, out 3,783, cache_r 220,787, cache_w 11,388) · cost n/a (unpriced)

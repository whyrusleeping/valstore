---
id: "0505"
title: 'Public-release docs pass: CONTRIBUTING/SECURITY, docs index, deploy.md and ARCHITECTURE cleanup, crate rustdoc'
status: in_review
priority: 2
created: "2026-10-27"
updated: "2026-10-27"
depends_on: []
spec_refs: []
---

## Description
Owner (2026-10-27) asked for a README-style pass over the rest of the docs for the first open-source release.

Acceptance criteria:
- CONTRIBUTING.md (conventions: no cargo fmt, comment policy, scripts/ci.sh, backlog/AI workflow, PR stance) and SECURITY.md (reporting, threat model: unauthenticated/unencrypted data plane).
- docs/README.md indexes every doc, split into user docs vs design notes/history; no stale status claims.
- docs/deploy.md restructured around operator tasks; old-fleet format/upgrade history moved out; task numbers removed from user-facing text.
- ARCHITECTURE.md Status section trimmed to an as-built summary.
- No dead bench/ links, unexplained "ycc", or bare task numbers in user-facing docs; all relative links and anchors resolve.
- Crate-level rustdoc for vals-store, vals-spdk, vals-rt, vals-raft, vals-server.
- getting-started.md fixes are tracked with 0269.

## Acceptance criteria

## Work log

### 2026-10-28 — note from 0509 (growth guide)
0509 added `docs/growing.md` (one machine to HA: minimal single-node setup, zone sizing, add-node, backup/restore migration to three PDs) and linked it in the working tree from README.md (HA paragraph and Documentation list), docs/README.md (Guides) and docs/getting-started.md ("Run the daemon"), plus deploy.md and backup-restore.md. 0509 could not commit: README.md, docs/README.md and getting-started.md also carry this task's and 0516/0269's uncommitted edits, which reference untracked files (the `kv`/`tokio_bridge` examples with their Cargo.toml/Cargo.lock changes, docs/embedding.md, SECURITY.md, CONTRIBUTING.md, docs/format-history.md). Committing them alone would publish a quick start HEAD cannot build, and HEAD's getting-started still says "raft-replicate later". The 0509 link lines in those three files are intentional; land them (and `docs/growing.md`) together with this onboarding stack, or let 0509 commit right after it.

### 2026-10-28 — note from 0269 (API contract)
Commit 0f02d04 (0269) landed `crates/vals-client/examples/kv.rs`, `tokio_bridge.rs` and the matching vals-client Cargo.toml/Cargo.lock, so the README/getting-started quick start (`cargo build -p vals-client --example kv`) now builds at HEAD. It also landed this task's crate-level rustdoc for vals-store and vals-rt (extended with "Supported API" sections); vals-raft, vals-spdk and vals-server crate docs are still uncommitted here. The vals-cluster crate doc now mentions `docs/embedding.md`, which lands with this stack. Remaining onboarding files (README, docs/README, getting-started, embedding.md, CONTRIBUTING/SECURITY, format-history, 0509's growing.md) are unchanged by 0269.

### 2026-10-28 — note from 0510 (file-backed benchmarks)
0510 committed `docs/benchmarks.md`, `scripts/file-bench.py` and a headline bullet in ARCHITECTURE.md §Headline performance. It also added link lines that exist only in the working tree, in three files that hold this stack's uncommitted edits: the README Performance paragraph ("For what to expect from the default file-backed `valsd` …"), the docs/README.md Guides entry for `benchmarks.md`, and a sentence in docs/SPDK.md §Measured throughput. HEAD's SPDK.md has no Measured throughput section. These lines are intentional; land them together with this stack.

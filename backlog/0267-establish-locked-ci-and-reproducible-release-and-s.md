---
id: "0267"
title: Establish a pinned locked software CI lane
status: done
priority: 1
created: "2026-09-07"
updated: "2026-09-29"
depends_on: []
spec_refs:
    - docs/production-readiness-review-2026-08.md#R18 — Establish release and evidence hygiene (P2)
---

## Description
Make the existing software verification repeatable on a clean checkout and run it in CI. Use the pinned Rust toolchain and Cargo.lock. Start with one debug workspace lane rather than a release-engineering program.

## Acceptance criteria

- Run `cargo build --locked --workspace`, `cargo test --locked --workspace`, and the Python script regression tests added in 0338. Run clippy with an explicit policy for existing warnings; do not hide test failures behind retries or summary parsing.
- Give developers the same documented local command(s), retain useful failure logs, and verify the lane on a clean source checkout without SPDK hardware or sibling repositories.
- Record the tested revision and toolchain. Separate software CI success from SPDK-linked/hardware validation.
- Do not wholesale-format the tree or expand into release publishing, SBOMs, licensing audits, artifact archival, or a new benchmark framework. Release automation can be reconsidered when an external release is actually selected.

## Work log

- 2026-09-29: owner accepted backlog triage. Narrowed the former CI/reproducible-release/SPDK umbrella to the immediately useful software lane; highest local engineering priority. Historical hardware provenance and retention requirements remain in their runbooks and 0258, not erased by this scope reduction.
- 2026-09-29 implementer report: Created executable `scripts/ci.sh`, `.github/workflows/ci.yml`, and `docs/ci.md` only. The script runs pinned/locked debug build, tests, Python regressions, and Clippy; records provenance, per-step lo
…[truncated]
- 2026-09-29 BLOCKED (coordinator): lane implemented but left UNCOMMITTED in the worktree (`scripts/ci.sh`, `.github/workflows/ci.yml`, `docs/ci.md`). It passed on the workspace and on a clean detached snapshot of HEAD ef9bb766 plus the pending uncommitted 0338 source, with no target/, third_party/ or sibling repos, run under `taskset -c 0-3`: every step exited 0; 1,351 Rust tests passed (14 ignored); 12 Python tests passed; 90 Clippy warning lines (informational). Toolchain was rustc 1.96.0. The standard review returned revise with one major finding: the 0338 Python regression tests (`scripts/test_netread_fleet.py`, `scripts/test_retired_entrypoints.py`) and the bench/scripts sources they import exist only as uncommitted 0338 work. A clean checkout of any committed revision therefore fails the lane's Python step. The lane is correct to fail closed on zero tests, so it should not be weakened. Committing this lane alone would therefore commit a CI that goes red. Under 0338 the owner chose to keep that work uncommitted unless asked. To unblock, the owner either commits the 0338 working-tree changes, or explicitly authorizes including them with this task. After that, rerun `scripts/ci.sh` on a clean checkout of the resulting revision, then commit these three files.
- 2026-09-30 (done): owner asked to finish the uncommitted work; 0338 committed (5af3c40c) and 0305 cherry-picked (b7f223c3). `scripts/ci.sh` on a clean detached worktree of b7f223c3 plus only the three lane files (no target/, third_party/ or sibling repos; `taskset -c 0-31`): every step exit 0; fresh debug build, 1,366 Rust tests passed (0 failed, 14 ignored), 12 Python tests, 89 Clippy warning lines (informational); rustc 1.96.0 matched the pinned channel. Lane files committed.

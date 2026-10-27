---
id: "0031"
title: Test suite SIGABRT under rustc 1.96 (pre-existing, ordering-dependent) + toolchain lint/fmt drift
status: done
priority: 2
created: "2026-07-02"
updated: "2026-07-04"
depends_on: []
spec_refs: []
---

## Description
`cargo test --release -p vals-cluster --lib` aborts (SIGABRT) with rustc 1.96.0 when the full suite runs in sequence — reproduced at `--test-threads=1`, deterministic at HEAD **before** the 2026-07 scan changes (verified via `git stash` on both the dev box and oldbox). The aborting point in one sequenced run: after `pd::tests::pd_daemon_propagates_membership_to_live_nodes ... ok`... i.e. some earlier test's detached thread panics/aborts later (cross-test pollution), or a Drop aborts. Each failing test passes in isolation; `pd::tests` alone passes. Under parallel threads, various pd/node tests flake instead.

Also pre-existing with the newer toolchain: `cargo clippy` now denies `clippy::uninit_vec` on `Scratch::heap_take` (vals-spdk lib.rs ~404) and vlog's `io_buf` pattern, and `cargo fmt --check` has ~125 diffs — the pinned-toolchain assumptions have drifted.

Acceptance: full workspace suite green in sequence and parallel on rustc 1.96; clippy/fmt clean or a pinned toolchain documented (rust-toolchain.toml).

## Acceptance criteria

## Plan

Root cause (diagnosed, reproduced on this box, rustc 1.96.0, ulimit -n 1024):
- `cargo test --release -p vals-cluster --lib -- --test-threads=1` SIGABRTs after `pd::tests::pd_daemon_propagates_membership_to_live_nodes` because the process runs out of file descriptors: `thread 'vals-shard' panicked at crates/vals-cluster/src/reactor.rs:357: eventfd: Too many open files (os error 24)`, and `observe::spawn_abort_on_panic` (by design) turns any shard-thread panic into a process abort.
- The fds leak cumulatively across tests: most vals-cluster tests spawn nodes via `Node::serve*` and drop the returned `NodeHandle` without calling `.shutdown()`. Dropping a NodeHandle deliberately does NOT stop the node (production semantics), so each test leaks its shard threads + listener + eventfds/timerfds/epoll fds for the life of the process. Measured: fd count climbs monotonically (~10–60 fds per node/antientropy test) to the 1024 soft limit. Under parallel threads the same exhaustion hits arbitrary tests → the observed flakes. Not a rustc 1.96 bug — the toolchain mention was a red herring.

Plan:
1. Fix the fd leaks in vals-cluster tests: every test that spawns nodes (Node::serve / serve_with_liveness / serve_sharded / serve_zoned* / serve_multidrive) or PD daemons must stop them at test end. Prefer a small test-local RAII guard (e.g. `struct Running(Option<NodeHandle>)` whose Drop calls `shutdown()`, plus a tiny helper to wrap `Node::serve*` calls) so cleanup also happens on assertion panics; explicit `handle.shutdown()` at the end is fine where a guard is awkward. Sweep ALL test modules in the crate (antientropy, node, pd, and any others that spawn reactors — find with `rg 'Node::serve|daemon|spawn_shard' crates/vals-cluster/src`), not just node::tests. Do not change production semantics of NodeHandle drop.
2. Verify with an fd-sampling loop (run the test binary directly, sample `ls /proc/$PID/fd | wc -l`) that the fd count stays roughly flat across the sequenced run instead of climbing to ~1024.
3. Verify: `cargo test --release -p vals-cluster --lib -- --test-threads=1` green end-to-end, and the default parallel run green; then the rest of the workspace suite (`cargo test --release --workspace`, chunk per-crate if slow).
4. Toolchain drift: add `rust-toolchain.toml` pinning `channel = "1.96.0"` with components rustfmt+clippy (this box runs 1.96.0). Then make the pinned toolchain clean:
   - clippy denies `clippy::uninit_vec` on the two deliberate uninit-read-buffer patterns (`Scratch::heap_take` in crates/vals-spdk/src/lib.rs ~399 and `io_buf` in crates/vals-store/src/vlog.rs ~56). Both are documented perf wins (skip memset; device read fills all bytes before decode) — keep the behavior and add targeted `#[allow(clippy::uninit_vec)]` on those functions, keeping/extending the existing SAFETY comments. Do not rewrite to zero-fill (documented ~7% serving-core cost).
   - `cargo fmt` the workspace (~138 mechanical diffs); confirm `cargo fmt --check` and `cargo clippy --workspace --all-targets` are clean.
5. No production-code behavior changes expected anywhere except the two `#[allow]` attributes and test-only cleanup.

### Starting points
- crates/vals-cluster/src/node.rs:44-126 — NodeHandle + shutdown(); drop does NOT stop the node
- crates/vals-cluster/src/observe.rs:487 spawn_abort_on_panic — why a shard panic aborts the whole test process
- crates/vals-cluster/src/reactor.rs:357 — the eventfd EMFILE assert that fires
- test call sites: rg -n 'Node::serve' crates/vals-cluster/src/node.rs (lines ~1795-3383); also antientropy::tests, pd::tests
- clippy failures: crates/vals-spdk/src/lib.rs:399 heap_take; crates/vals-store/src/vlog.rs:56 io_buf
- fd sampling: run target/release/deps/vals_cluster-* --test-threads=1 in background, watch ls /proc/$PID/fd | wc -l
- ulimit -n is 1024 (soft) on this box; do not raise it as the 'fix'

## Work log
- 2026-07-04 plan: Root cause (diagnosed, reproduced on this box, rustc 1.96.0, ulimit -n 1024): - `cargo test --release -p vals-cluster --lib -- --test-threads=1` SIGABRTs after `pd::tests::pd_daemon_propagates_members
…[truncated]
- 2026-07-04 context hints: 7 recorded with plan
- 2026-07-04 context hints: crates/vals-cluster/src/node.rs:44-126 — NodeHandle + shutdown(); drop does NOT stop the node (keep it that way); crates/vals-cluster/src/node.rs tests start at line 1627; serve call sites ~1795-338
…[truncated]
- 2026-07-04 implementer report: Task 0031 complete. The SIGABRT was file-descriptor exhaustion (not a rustc 1.96 bug): tests spawn nodes via `Node::serve*`/`PdDaemon::spawn*` and drop the handle, which by production contract does NO
…[truncated]
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change correctly identifies the SIGABRT as file-descriptor exhaustion from tests leaking spawned nodes/daemons (dropping a handle deliberately does not stop the server), not a rustc 1.96 bug. It f
…[truncated]
- 2026-07-04 decision: accept — commit: Fix test-suite SIGABRT (fd exhaustion from leaked test nodes) + pin toolchain 1.96, fmt/clippy clean (task 0031)
- 2026-07-04 usage: 110,442 tok (in 374, out 110,068, cache_r 12,392,967, cache_w 273,046) · cost n/a (unpriced)
  implementer: 84,144 tok (in 254, out 83,890, cache_r 10,790,752, cache_w 150,819) · cost n/a (unpriced)
  coordinator: 15,546 tok (in 60, out 15,486, cache_r 875,046, cache_w 80,096) · cost n/a (unpriced)
  reviewer:Claude: 10,752 tok (in 60, out 10,692, cache_r 727,169, cache_w 42,131) · cost n/a (unpriced)

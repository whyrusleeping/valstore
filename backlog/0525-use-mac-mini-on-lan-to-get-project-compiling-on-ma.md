---
id: "0525"
title: Use mac mini on LAN to get project compiling on macOS
status: done
priority: 3
created: "2026-10-29"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description

mac mini is (i think) at 192.168.1.99

Reported in https://github.com/whyrusleeping/valstore/issues/1 ("BIG: protobuf
issue blocks builds on Apple Silicon", opened 2026-10-29 by folkengine). Summary:

1. **protoc / raft-proto.** `cargo build --release -p vals-server` fails in
   `raft-proto 0.7.0`'s build script. The chain is `vals-raft` → `raft 0.7.0` →
   `raft-proto 0.7.0` → `protobuf-build 0.14.1`. That crate only accepts
   `protoc` with major version 3, so Homebrew's `libprotoc 36.2` is rejected.
   It then falls back to a bundled `protoc`, which has no macOS/aarch64 build,
   and panics. Homebrew's `protobuf@21` (the last 3.x) was disabled in Jan 2026.
   `protobuf-build 0.15.1` fixes both problems: it checks `>= 3.1` and builds
   `protoc` from source via `protobuf-src` on non-Windows. Upstream raft-rs
   switched to it in tikv/raft-rs#510 (commit `9d360a3b0cdb`, July 2023), but
   no crates.io release has it.
   - Fix A: `[patch.crates-io]` raft and raft-proto to git rev `9d360a3b0cdb`
     (3 commits after 0.7.0). Every clean build, Linux included, then compiles
     protoc from C++ (needs cmake and a C++ compiler, and is slower), and it
     adds a git dependency. The reporter confirmed it gets raft-proto building
     on Apple Silicon.
   - Fix B: document `PROTOC=<3.x protoc>` in docs/getting-started.md, e.g.
     protoc 21.12 osx-aarch_64 from the GitHub release (it reports 3.21.12), or
     the bundled `protoc-osx-x86_64` under Rosetta 2.
2. **epoll in vals-client.** Once protoc is fixed, `vals-client` fails with 27
   errors (`epoll_create1` not in `libc`, crates/vals-client/src/reactor.rs:30).
   The reactor uses Linux epoll directly, so it needs a kqueue backend or a
   portable crate (`polling`/`mio`). More Linux-only code may turn up after
   that. The README currently says Linux only.
3. **CI.** GitHub Actions `macos-14`+ runners are Apple Silicon. A
   `runs-on: macos-14` job running `cargo build -p vals-server` would reproduce
   the failure and verify a fix (CI is ubuntu-latest only today).

## Outcome

On an Apple M1 the workspace builds all targets and passes clippy -D warnings. The vals-rt/store/raft/client tests pass, and the README quick start works. raft-proto 0.7.0 is vendored with its pre-generated code via [patch.crates-io], so no build needs protoc. The reactor has a kqueue backend on macOS, and the Linux epoll path is unchanged in behaviour. On non-Linux the client waits for connect to finish before writing (macOS gives ENOTCONN). Linux-only APIs and tests are gated, and two test races are fixed. A macos-14 CI job was added, and the docs are updated. Linux scripts/ci.sh passes. On macOS the vals-cluster and vals-server tests fail only on timing flakes and on the PD read-index race (proposed task 0542).

Commit: 0525: build on macOS (Apple Silicon): vendor raft-proto 0.7.0 with pre-generated protobuf code (no protoc), kqueue reactor backend, Linux-only APIs gated, connect-in-progress handling on non-Linux, macos-14 CI job; docs

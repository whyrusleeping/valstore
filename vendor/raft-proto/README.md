# raft-proto 0.7.0, pre-generated

This is [raft-proto 0.7.0](https://crates.io/crates/raft-proto/0.7.0) from
[raft-rs](https://github.com/tikv/raft-rs) (Apache-2.0, see `LICENSE`), used in
place of the crates.io release through `[patch.crates-io]` in the workspace
`Cargo.toml`.

The crates.io release generates its Rust code at build time with
`protobuf-build 0.14`. That build script accepts only a `protoc` whose major
version is 3, and otherwise falls back to a bundled `protoc` that has no
macOS/aarch64 build, so the workspace did not build on Apple Silicon. Here the
generated code is checked in and there is no build script, so no `protoc` is
needed on any platform.

Changes from the release:

- `src/protos/eraftpb.rs` is the file the release's build script writes to
  `OUT_DIR/protos/eraftpb.rs` (rust-protobuf 2.28.0, `protobuf-codec`
  feature), copied unchanged from a Linux x86-64 build.
- `src/lib.rs` declares `pub mod eraftpb;` instead of including the generated
  `mod.rs` from `OUT_DIR`.
- `src/lib.rs` also allows all warnings. Cargo silences a registry crate's
  warnings with `--cap-lints allow` but shows a path dependency's, and the
  2023 code warns under the pinned toolchain.
- `Cargo.toml` drops the `protobuf-build` build dependency and the
  `prost-codec` feature, which vals does not use.

`src/confchange.rs`, `src/confstate.rs` and `proto/eraftpb.proto` are the
release's files unchanged.

To regenerate `src/protos/eraftpb.rs`, for example after moving to a newer
raft-proto, build the upstream crate on Linux x86-64 with the `protobuf-codec`
feature and copy `target/*/build/raft-proto-*/out/protos/eraftpb.rs` here.

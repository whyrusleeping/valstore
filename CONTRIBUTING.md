# Contributing to vals

Thanks for your interest. vals is a young project with one maintainer, so
please open an issue before starting anything large; it saves both of us
from a pull request that doesn't fit. Bug reports with a reproduction are
especially welcome. For security problems, see [`SECURITY.md`](SECURITY.md)
instead of filing a public issue.

## Building and testing

You need Linux, [rustup](https://rustup.rs/), and Python 3.10 or newer.
`rust-toolchain.toml` pins the Rust version, and CI checks it. The workspace
also builds on macOS (Apple Silicon); a separate CI job checks that and runs
part of the tests there ([`docs/ci.md`](docs/ci.md#macos)), but
`scripts/ci.sh` and the hardware work are Linux only.

```sh
cargo build --workspace
cargo test --workspace
scripts/ci.sh        # the full CI lane: locked build, tests, clippy, Python tests
```

`scripts/ci.sh` is exactly what GitHub CI runs (see [`docs/ci.md`](docs/ci.md)).
Everything except the SPDK backend runs on the in-memory and file-backed
block devices, so no hardware is needed. Run the cluster and store tests in
debug builds; several timing-sensitive tests are flaky under `--release` or on
a heavily loaded machine. If a test fails in a full parallel run but passes
on its own, mention it in your PR rather than changing the test.

Changes to the hot path (storage engine, replica serving, transport) also
need measurements on real NVMe before they are adopted. If you don't have
suitable hardware, say so in the PR and the maintainer will run them.
[`docs/validation.md`](docs/validation.md) describes the tiers, from local
tests up to full-scale hardware runs.

## Code conventions

- **Don't run `cargo fmt` on the tree.** The code is not rustfmt-clean, and
  reformatting would bury your change in an unrelated diff. Format the code
  you write by hand to match its surroundings.
- **No explanatory code comments.** The only comments in Rust code are
  `// SAFETY:` notes on `unsafe` blocks, doc comments (`///`, `//!`, including
  doctests), and Prometheus `HELP` strings. Put the reasoning in the commit
  message, in a test name, or in the design docs under `docs/`.
- **Few dependencies.** The wire codec, config parser and metrics endpoint are
  hand-written on purpose; there is no serde, gRPC or tokio on the hot path. A
  new dependency needs a strong case.
- **Regression tests should fail without the fix.** When you fix a bug, check
  that the new test fails with the fix reverted.
- **Clippy** runs in CI with `-- -D warnings`; warnings fail the lane.

## Design docs and project records

Design lives in [`ARCHITECTURE.md`](ARCHITECTURE.md) and [`docs/`](docs/README.md).
If you change behaviour, a wire or on-disk format, or a config key, update
the relevant doc in the same PR. Byte layouts are in
[`docs/formats.md`](docs/formats.md) and cross-node algorithms are in
[`docs/algorithms.md`](docs/algorithms.md); both must stay exact.

Most development happens with AI coding agents. `spec.md`, `plans/`, and the
task tracker in `backlog/` (indexed in `backlog.md`) are their working files.
You can read them for context on why something is the way it is, but you
don't need to edit them in a PR.

## License

By contributing, you agree that your contributions are licensed under the
[MIT License](LICENSE).

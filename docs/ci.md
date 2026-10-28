# Software CI

From the repository root, run `scripts/ci.sh`. It needs rustup with the
toolchain pinned in `rust-toolchain.toml` and Python 3.10 or newer, and nothing
else: no SPDK, no hardware, no other repositories. GitHub Actions runs the same
script on push, pull request and manual dispatch with the pinned toolchain.

The script runs, in order:

1. `cargo build --locked --workspace`
2. `cargo test --locked --workspace --no-fail-fast`
3. `python3 -m unittest discover -s scripts -p 'test_*.py' -v` (a missing test
   file fails the step)
4. `cargo clippy --locked --workspace --all-targets`

There are no retries; each command's exit code is its result. Clippy uses the
default lint levels: deny-level lints and compile errors fail, while warn-level
warnings are printed and counted but do not fail. `cargo fmt --check` is
deliberately not run: the tree is not rustfmt-clean, and this lane does not
reformat it.

Each run recreates `${VALS_CI_LOG_DIR:-target/ci-logs}`. The full console
output of each step is saved in a numbered log; `summary.txt` records exit
codes, durations and the Clippy warning count; `provenance.txt` records the
tested Git revision, the dirty path count, the pinned and active toolchains,
command versions, host and UTC time. The script fails if the active `rustc`
version differs from the pinned channel. It unsets `VALS_SPDK_LINK` and
`VALS_SPDK_DIR`, sets `RUST_BACKTRACE=1` and `PYTHONDONTWRITEBYTECODE=1`, and
logs the names (not the values) of any other `VALS_*` variables left in the
environment. CI uploads the logs even when a step fails.

## Hardware is a separate gate

A green software CI run covers the in-memory and file backends. It says
nothing about SPDK-linked builds or behaviour on real NVMe; those are checked
by hand on hardware, as described in [`SPDK.md`](SPDK.md).
[`validation.md`](validation.md) describes the validation tiers, from local
tests up to full-scale hardware runs. Use the cheapest tier that can answer the
question; hardware is for confirmation, and the full replay is a release gate.

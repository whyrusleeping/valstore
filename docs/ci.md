# Software CI

From the repository root, run `scripts/ci.sh` (requires rustup with the pinned
`rust-toolchain.toml` toolchain and Python 3.10 or newer). It needs neither SPDK
nor hardware nor sibling repositories such as `../bootes`. CI runs the same
script on push, pull request, and manual dispatch using the pinned toolchain.

The debug lane runs `cargo build --locked --workspace`,
`cargo test --locked --workspace --no-fail-fast`, Python's
`unittest discover -s scripts -p 'test_*.py' -v` (a missing test file fails),
and `cargo clippy --locked --workspace --all-targets`. There are no retries;
each command's exit code determines its result. Clippy uses default lint
levels: deny-level lints and compile errors fail, while warn-level warnings
are printed and counted but do not fail. `cargo fmt --check` is intentionally
excluded: the tree is not rustfmt-clean, and this lane does not reformat it.

Each run recreates `${VALS_CI_LOG_DIR:-target/ci-logs}`. Full console output
is also saved in numbered step logs; `summary.txt` records exit codes,
durations, and Clippy warning lines. `provenance.txt` records the tested Git
revision, dirty path count, pinned and active toolchains, command versions,
host, and UTC time. The script fails if the active rustc version differs from
the pinned channel. It unsets `VALS_SPDK_LINK` and `VALS_SPDK_DIR`, sets
`RUST_BACKTRACE=1` and `PYTHONDONTWRITEBYTECODE=1`, and logs the names (not
values) of any other `VALS_*` variables left in the environment. CI uploads
these logs even on failure.

## Hardware is a separate gate

A green software CI run covers software mem/file backends, **not** SPDK-linked
builds or real NVMe/hardware behavior. Hardware/SPDK-linked validation remains
in the `plans/` runbooks and `docs/SPDK.md`; do not treat this lane as evidence
for those gates. [`validation.md`](validation.md) defines the validation tiers.
Use the cheapest tier that can establish a task's acceptance criteria; hardware
is for confirmation, and the full replay is a release gate.

#!/usr/bin/env bash
set -euo pipefail

# Run from the source root, independent of the caller's working directory.
cd "$(dirname "${BASH_SOURCE[0]}")/.."
log_dir=${VALS_CI_LOG_DIR:-target/ci-logs}
rm -rf -- "$log_dir"
mkdir -p -- "$log_dir"
log_dir=$(realpath "$log_dir")
summary="$log_dir/summary.txt"
printf 'Step\tExit code\tDuration (s)\n' > "$summary"
failed=0

# Never link local SPDK, even if the caller has an SPDK environment.
unset VALS_SPDK_LINK VALS_SPDK_DIR
export RUST_BACKTRACE=1 PYTHONDONTWRITEBYTECODE=1

provenance() {
    local revision dirty channel version remaining
    revision=$(git rev-parse HEAD 2>/dev/null) || revision=unknown
    dirty=$(git status --porcelain 2>/dev/null | wc -l) || dirty=unknown
    channel=$(sed -n 's/^channel = "\([^"]*\)".*/\1/p' rust-toolchain.toml)
    printf 'Revision: %s\nDirty paths: %s\nPinned channel: %s\n' "$revision" "$dirty" "$channel"
    printf 'Active toolchain: '; rustup show active-toolchain || return $?
    rustc -Vv || return $?
    cargo -V || return $?
    cargo clippy -V || return $?
    python3 -V || return $?
    uname -a || return $?
    date -u '+UTC: %Y-%m-%dT%H:%M:%SZ' || return $?
    printf '%s\n' 'Scope: software CI only (mem/file backends, SPDK not linked, no hardware). Hardware/SPDK-linked validation is separate (plans/, docs/SPDK.md).'
    printf '%s\n' 'VALS_SPDK_LINK and VALS_SPDK_DIR unset. Other VALS_* variable names (values not logged):'
    remaining=$(env | cut -d= -f1 | grep '^VALS_' || true)
    printf '%s\n' "${remaining:-none}"
    version=$(rustc --version | awk '{print $2}')
    if [[ -z "$channel" || "$version" != "$channel" ]]; then
        printf 'ERROR: rustc version %s does not match pinned channel %s\n' "$version" "$channel" >&2
        return 1
    fi
}

python_tests() {
    if ! compgen -G 'scripts/test_*.py' > /dev/null; then
        echo 'ERROR: no scripts/test_*.py files; refusing zero-test success' >&2
        return 1
    fi
    python3 -m unittest discover -s scripts -p 'test_*.py' -v
}

run_step() {
    local name=$1 start end code
    shift
    start=$(date +%s)
    printf '\n== %s ==\n' "$name"
    if "$@" 2>&1 | tee "$log_dir/$name.log"; then
        code=0
    else
        code=$?
        failed=1
    fi
    end=$(date +%s)
    printf '%s\t%s\t%s\n' "$name" "$code" "$((end - start))" >> "$summary"
    printf '== %s: exit %s, %ss ==\n' "$name" "$code" "$((end - start))"
}

run_step 00-provenance provenance
cp "$log_dir/00-provenance.log" "$log_dir/provenance.txt"
run_step 01-build cargo build --locked --workspace
run_step 02-test cargo test --locked --workspace --no-fail-fast
run_step 03-python python_tests
run_step 04-clippy cargo clippy --locked --workspace --all-targets
# Informational only: default warning-level lints do not fail the lane.
warning_count=$(grep -c '^warning' "$log_dir/04-clippy.log" || true)
printf 'Clippy warning lines (informational): %s\n' "$warning_count" | tee -a "$log_dir/04-clippy.log" "$summary"
printf '\nSummary (%s):\n' "$summary"
cat "$summary"
exit "$failed"

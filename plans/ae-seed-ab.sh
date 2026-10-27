#!/bin/sh
set -eu

artifact_dir=${1:-bench/0289-ae-seed-ab}
bin=${VALS_CAPACITY_BIN:-target/release/vals-bench}
sizes=${VALS_CAPACITY_SIZES:-"100000 250000 500000 1000000"}
cpuset8=${VALS_CAPACITY_CPUSET_8:-0-7}
cpuset4=${VALS_CAPACITY_CPUSET_4:-0-3}
scratch_parent=${VALS_CAPACITY_SCRATCH_PARENT:-${TMPDIR:-/tmp}}
mkdir -p "$artifact_dir"
[ -x "$bin" ] || { echo "build $bin first" >&2; exit 2; }
[ -d "$scratch_parent" ] || { echo "scratch directory missing: $scratch_parent" >&2; exit 2; }
avail=$(df -B1 --output=avail "$scratch_parent" | tail -1 | tr -d ' ')
[ "$avail" -ge 1073741824 ] || { echo "need 1 GiB free scratch" >&2; exit 2; }
scratch_dir=$(mktemp -d "$scratch_parent/vals-seed-0289.XXXXXX")
scratch=$scratch_dir/capacity.img
cleanup() { rm -f "$scratch"; rmdir "$scratch_dir" 2>/dev/null || true; }
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
{
    echo "task=0289"
    echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "git_head=$(git rev-parse HEAD)"
    echo "git_describe=$(git describe --always --dirty)"
    echo "rustc=$(rustc --version)"
    echo "kernel=$(uname -srmo)"
    echo "filesystem=$(df -T "$scratch_parent" | tail -1)"
    echo "binary_sha256=$(sha256sum "$bin" | cut -d' ' -f1)"
    echo "run_source_sha256=$(sha256sum crates/vals-store/src/run.rs | cut -d' ' -f1)"
    echo "seed_source_sha256=$(sha256sum crates/vals-cluster/src/replica/lifecycle.rs | cut -d' ' -f1)"
    echo "bench_source_sha256=$(sha256sum crates/vals-bench/src/capacity.rs | cut -d' ' -f1)"
    echo "sizes=$sizes cpuset8=$cpuset8 cpuset4=$cpuset4"
    echo "scope=local file-backed A/B, no NVMe or fleet"
} > "$artifact_dir/provenance.txt"
for mode in baseline stream; do
    for cpus in "8c:$cpuset8" "4c:$cpuset4"; do
        label=${cpus%%:*}
        set=${cpus#*:}
        for n in $sizes; do
            rm -f "$scratch"
            VALS_AE_SEED_MODE=$mode /usr/bin/time -v -o "$artifact_dir/time-$mode-$label-$n.txt" \
                taskset -c "$set" "$bin" file capacity "$n" 64 "$scratch" 0 \
                > "$artifact_dir/$mode-$label-$n.txt" 2>&1
        done
    done
    rm -f "$scratch"
    VALS_AE_SEED_MODE=$mode VALS_CAPACITY_SEED_INTERFERENCE=1 \
        /usr/bin/time -v -o "$artifact_dir/time-$mode-interference.txt" \
        taskset -c "$cpuset8" "$bin" file capacity 250000 64 "$scratch" 0 \
        > "$artifact_dir/$mode-interference.txt" 2>&1
done
echo "wrote $artifact_dir"

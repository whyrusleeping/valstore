#!/bin/sh
set -eu

artifact_dir=${1:-bench/0285-capacity-local}
bin=${VALS_CAPACITY_BIN:-target/release/vals-bench}
sizes=${VALS_CAPACITY_SIZES:-"100000 250000 500000"}
value_bytes=${VALS_CAPACITY_VALUE_BYTES:-64}
probes=${VALS_CAPACITY_PROBES:-2000}
offered=${VALS_CAPACITY_OFFERED_OPS:-20000}
late_us=${VALS_CAPACITY_LATE_US:-10000}
cpuset8=${VALS_CAPACITY_CPUSET_8:-0-7}
cpuset4=${VALS_CAPACITY_CPUSET_4:-0-3}
scratch_parent=${VALS_CAPACITY_SCRATCH_PARENT:-${TMPDIR:-/tmp}}

mkdir -p "$artifact_dir"
if [ ! -x "$bin" ]; then
    echo "missing $bin; run: cargo build --release -p vals-bench" >&2
    exit 2
fi
if [ ! -d "$scratch_parent" ]; then
    echo "capacity scratch parent does not exist: $scratch_parent" >&2
    exit 2
fi
avail=$(df -B1 --output=avail "$scratch_parent" | tail -1 | tr -d ' ')
if [ "$avail" -lt 1073741824 ]; then
    echo "capacity baseline requires at least 1 GiB free at $scratch_parent; have $avail" >&2
    exit 2
fi
scratch_dir=$(mktemp -d "$scratch_parent/vals-capacity-0285.XXXXXX")
scratch=$scratch_dir/capacity.img
cleanup() {
    rm -f "$scratch"
    rmdir "$scratch_dir" 2>/dev/null || true
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

{
    echo "task=0285"
    echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "git_head=$(git rev-parse HEAD)"
    echo "git_describe=$(git describe --always --dirty)"
    echo "rustc=$(rustc --version)"
    echo "kernel=$(uname -srmo)"
    echo "filesystem=$(df -T "$(dirname "$scratch")" | tail -1)"
    echo "binary=$bin"
    echo "binary_sha256=$(sha256sum "$bin" | cut -d' ' -f1)"
    echo "capacity_source_sha256=$(sha256sum crates/vals-bench/src/capacity.rs | cut -d' ' -f1)"
    echo "sizes=$sizes"
    echo "value_bytes=$value_bytes"
    echo "probes=$probes"
    echo "offered_ops_s=$offered"
    echo "late_us=$late_us"
    echo "cpuset8=$cpuset8"
    echo "cpuset4=$cpuset4"
    echo "scratch_parent=$scratch_parent"
    echo "scratch=$scratch"
    echo "scope=file-backed local mechanism measurement; not NVMe or distributed throughput"
} > "$artifact_dir/provenance.txt"

for n in $sizes; do
    rm -f "$scratch"
    VALS_CAPACITY_OFFERED_OPS=$offered VALS_CAPACITY_LATE_US=$late_us \
        /usr/bin/time -v -o "$artifact_dir/time-8c-$n.txt" \
        taskset -c "$cpuset8" "$bin" file capacity "$n" "$value_bytes" "$scratch" "$probes" \
        > "$artifact_dir/baseline-8c-$n.txt" 2>&1
    rm -f "$scratch"
done

largest=$(printf '%s\n' $sizes | tail -1)
VALS_CAPACITY_OFFERED_OPS=$offered VALS_CAPACITY_LATE_US=$late_us \
    /usr/bin/time -v -o "$artifact_dir/time-4c-$largest.txt" \
    taskset -c "$cpuset4" "$bin" file capacity "$largest" "$value_bytes" "$scratch" "$probes" \
    > "$artifact_dir/baseline-4c-$largest.txt" 2>&1
rm -f "$scratch"

echo "wrote $artifact_dir"

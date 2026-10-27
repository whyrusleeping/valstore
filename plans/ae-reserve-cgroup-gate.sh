#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
ARM=${1:?usage: $0 arm cap-bytes cpuset keys stream-or-baseline artifact-dir}
CAP=${2:?}
CPUS=${3:?}
KEYS=${4:?}
MODE=${5:?}
OUT=${6:?}
VALSD=${VALSD:-$ROOT/target/release/valsd}
BENCH=${BENCH:-$ROOT/target/release/vals-bench}
SCRATCH_PARENT=${SCRATCH_PARENT:-/tmp}
BUDGET=$(( CAP * 85 / 100 ))
[ "$MODE" = stream ] || [ "$MODE" = baseline ] || exit 2
[ -x "$VALSD" ] && [ -x "$BENCH" ] || { echo 'release binaries missing' >&2; exit 2; }
[ -d "$SCRATCH_PARENT" ] || exit 2
mkdir -p "$OUT"
OUT=$(CDPATH= cd -- "$OUT" && pwd)
WORK=$(mktemp -d "$SCRATCH_PARENT/vals-0316-${ARM}.XXXXXX")
PREFIX="vals0316-${ARM}-$$"
printf 'reserve-gate-local-secret\n' > "$WORK/secret"
python3 - "$WORK/ports" <<'PY'
import socket, sys
socks = [socket.socket() for _ in range(6)]
for sock in socks: sock.bind(('127.0.0.1', 0))
with open(sys.argv[1], 'w') as out:
    out.write(' '.join(str(sock.getsockname()[1]) for sock in socks) + '\n')
PY
read P1 P2 P3 M1 M2 M3 < "$WORK/ports"
for node in 1 2 3; do
    eval port=\$P$node metrics=\$M$node
    other1=$(( node % 3 + 1 )); other2=$(( (node + 1) % 3 + 1 ))
    eval peer1=\$P$other1 peer2=\$P$other2
    cat > "$WORK/node$node.conf" <<EOF
node_id = $node
profile = capacity
listen = 127.0.0.1:$port
metrics = 127.0.0.1:$metrics
backend = file
data_dir = $WORK/data-node$node
peer = $other1 @ 127.0.0.1:$peer1
peer = $other2 @ 127.0.0.1:$peer2
rf = 2
sweep = 1s
region_mib = 4096
secret_file = $WORK/secret
EOF
done
cat > "$WORK/pd.conf" <<EOF
pd_id = 1
data_dir = $WORK/data-pd
data_node = 1 @ 127.0.0.1:$P1
data_node = 2 @ 127.0.0.1:$P2
data_node = 3 @ 127.0.0.1:$P3
rf = 2
vnodes = 64
zone = reserve-gate 2 sorted
admin_socket = $WORK/pd.sock
secret_file = $WORK/secret
EOF
cp "$WORK"/*.conf "$OUT/"
{
    echo "arm=$ARM cap_bytes=$CAP budget_bytes=$BUDGET cpus=$CPUS keys_per_pass=$KEYS mode=$MODE"
    echo "started=$(date -u +%FT%TZ) scratch=$WORK"
    echo "git_describe=$(git -C "$ROOT" describe --always --dirty)"
    echo "valsd_sha256=$(sha256sum "$VALSD" | cut -d' ' -f1)"
    echo "bench_sha256=$(sha256sum "$BENCH" | cut -d' ' -f1)"
    echo "gate_sha256=$(sha256sum "$ROOT/plans/ae-reserve-cgroup-gate.sh" | cut -d' ' -f1)"
    echo "kernel=$(uname -r)"
} > "$OUT/provenance.txt"
printf 'starting\n' > "$OUT/phase"
cleanup() {
    rc=$?
    trap - EXIT INT TERM HUP
    touch "$OUT/stop-sampling"
    [ -z "${SAMPLE_PID:-}" ] || wait "$SAMPLE_PID" 2>/dev/null || true
    for node in 1 2 3; do
        systemctl --user stop "$PREFIX-node$node.scope" 2>/dev/null || true
    done
    if [ -n "${PD_PID:-}" ]; then kill "$PD_PID" 2>/dev/null || true; wait "$PD_PID" 2>/dev/null || true; fi
    rm -rf -- "$WORK"
    echo "finished=$(date -u +%FT%TZ) exit=$rc" >> "$OUT/provenance.txt"
    exit "$rc"
}
trap cleanup EXIT INT TERM HUP
start_node() {
    node=$1
    env VALS_RAM_BUDGET_BYTES="$BUDGET" VALS_AE_SEED_MODE="$MODE" \
        systemd-run --user --scope --quiet --unit "$PREFIX-node$node" \
        -p "MemoryMax=$CAP" -p MemorySwapMax=0 \
        taskset -c "$CPUS" "$VALSD" node -c "$WORK/node$node.conf" \
        >> "$OUT/node$node.log" 2>&1 &
    echo $! > "$WORK/node$node.runner"
}
wait_port() {
    python3 - "$1" <<'PY'
import socket, sys, time
end = time.monotonic() + 90
while time.monotonic() < end:
    try:
        with socket.create_connection(('127.0.0.1', int(sys.argv[1])), .3):
            sys.exit(0)
    except OSError:
        time.sleep(.2)
sys.exit('port did not become ready: ' + sys.argv[1])
PY
}
run_write() {
    label=$1
    printf '%s\n' "$label" > "$OUT/phase"
    start=$(date +%s)
    case "$label" in continued-*) key_prefix=post-restart ;; *) key_prefix=$label ;; esac
    VALS_NETLOAD_SKIP_READ_PHASE=1 VALS_BENCH_BOUNDED_CLIENT=1 VALS_NETLOAD_KEY_PREFIX="$key_prefix" \
        timeout 420 taskset -c "$CPUS" "$BENCH" x netbatchwrite "$KEYS" 256 \
        "1@127.0.0.1:$P1;2@127.0.0.1:$P2;3@127.0.0.1:$P3" 64 2 4 \
        > "$OUT/$label.log" 2>&1 || echo "driver_exit=$?" >> "$OUT/$label.log"
    echo "$label $start $(date +%s)" >> "$OUT/phases.txt"
}
start_node 1; start_node 2; start_node 3
( "$VALSD" pd -c "$WORK/pd.conf" > "$OUT/pd.log" 2>&1 ) & PD_PID=$!
wait_port "$P1"; wait_port "$P2"; wait_port "$P3"
wait_port "$M1"; wait_port "$M2"; wait_port "$M3"
python3 "$ROOT/plans/ae-reserve-sample.py" "$PREFIX" "$OUT" "$M1" "$M2" "$M3" & SAMPLE_PID=$!
sleep 3
run_write initial
printf 'degraded\n' > "$OUT/phase"
systemctl --user stop "$PREFIX-node3.scope"
wait "$(cat "$WORK/node3.runner")" 2>/dev/null || true
run_write degraded
printf 'rebuild\n' > "$OUT/phase"
rm -rf -- "$WORK/data-node3"
REBUILD_START=$(date +%s)
echo "rebuild_start=$REBUILD_START" >> "$OUT/provenance.txt"
start_node 3
wait_port "$P3"; wait_port "$M3"
"$VALSD" admin -s "$WORK/pd.sock" split-zone reserve-gate initial-001000000 canonical > "$OUT/split.txt" 2>&1 || true
run_write post-restart
pass=1
while [ "$pass" -le "${EXTRA_PASSES:-3}" ]; do
    run_write "continued-$pass"
    pass=$((pass + 1))
done
python3 - "$BENCH" "$P1" "$P2" "$P3" "$M3" "$REBUILD_START" "${REBUILD_WAIT_SECS:-360}" "$OUT" <<'PY'
import json, pathlib, subprocess, sys, time, urllib.request
bench, p1, p2, p3, metrics, restart, timeout, out = sys.argv[1:]
restart, timeout = int(restart), int(timeout)
out = pathlib.Path(out)
start, last_change, last = time.monotonic(), time.monotonic(), 0
first_exact, exact_details = None, None
peers = f'1@127.0.0.1:{p1};2@127.0.0.1:{p2};3@127.0.0.1:{p3}'
with (out / 'digest-polls.jsonl').open('w', buffering=1) as polls:
    while time.monotonic() - start < timeout:
        try:
            body = urllib.request.urlopen(f'http://127.0.0.1:{metrics}/metrics', timeout=2).read().decode()
            applied = int(next(line.split()[1] for line in body.splitlines() if line.startswith('vals_ae_entries_applied ')))
            if applied != last:
                last, last_change = applied, time.monotonic()
        except (OSError, ValueError, StopIteration):
            pass
        if first_exact is None:
            try:
                result = subprocess.run([bench, 'x', 'netseedcheck', '0', '0', peers],
                                        capture_output=True, text=True, timeout=25)
                passed = result.returncode == 0 and 'SEED_DIGEST_CHECK status=pass' in result.stdout
                detail = (result.stdout if passed else result.stderr).strip().splitlines()[-1][:240]
            except (subprocess.TimeoutExpired, IndexError):
                passed, detail = False, 'probe timeout or empty output'
            polls.write(json.dumps({'time': round(time.time(), 2), 'pass': passed, 'detail': detail}) + '\n')
            if passed:
                first_exact = round(time.time() - restart, 1)
                exact_details = detail
        if first_exact is not None and last > 0 and time.monotonic() - last_change >= 45:
            break
        time.sleep(2)
with (out / 'repair-wait.json').open('w') as receipt:
    json.dump({'ae_entries_applied': last, 'idle_seconds': round(time.monotonic() - last_change, 1),
               'wait_seconds': round(time.monotonic() - start, 1),
               'settled_proxy': last > 0 and time.monotonic() - last_change >= 45,
               'exact_completed_seconds': first_exact, 'exact_details': exact_details}, receipt)
PY
VALS_NETLOAD_KEY_PREFIX=initial VALS_BENCH_BOUNDED_CLIENT=1 \
    timeout 60 taskset -c "$CPUS" "$BENCH" x netread 1000 256 \
    "1@127.0.0.1:$P1;2@127.0.0.1:$P2;3@127.0.0.1:$P3" 16 2 2 \
    > "$OUT/rebuild-read.log" 2>&1 || echo "read_exit=$?" >> "$OUT/rebuild-read.log"
"$VALSD" admin -s "$WORK/pd.sock" zones > "$OUT/zones.txt" 2>&1 || true
printf 'done\n' > "$OUT/phase"
sleep 2

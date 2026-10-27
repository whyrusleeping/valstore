#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
BENCH=${BENCH:-$ROOT/target/debug/vals-bench}
PROBE_SECS=${PROBE_SECS:-8}
CONNECT_SECS=${CONNECT_SECS:-3}
RPC_SECS=${RPC_SECS:-4}
OUT=${OUT:-}
[ -x "$BENCH" ] || { echo "build vals-bench first" >&2; exit 1; }
TMP=${TMPDIR:-/tmp}/profile-soak-probe-test.$$
mkdir -p "$TMP"
blackhole=
cleanup() {
  rc=$?
  [ -z "$blackhole" ] || kill "$blackhole" 2>/dev/null || true
  rm -rf "$TMP"
  exit "$rc"
}
trap cleanup EXIT INT TERM HUP

python3 - "$TMP/port" <<'PY' &
import socket, sys, time
srv = socket.socket()
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", 0))
srv.listen(16)
with open(sys.argv[1], "w") as f:
    f.write(str(srv.getsockname()[1]))
held = []
while True:
    conn, _ = srv.accept()
    held.append(conn)
    time.sleep(0.01)
PY
blackhole=$!

deadline=$(( $(date +%s) + 15 ))
while [ ! -s "$TMP/port" ]; do
  [ "$(date +%s)" -lt "$deadline" ] || { echo "blackhole listener did not start" >&2; exit 1; }
  sleep 0.2
done
PORT=$(cat "$TMP/port")

run_probe() {
  label=$1
  peers=$2
  limit=$((PROBE_SECS + 20))
  started=$(date +%s)
  set +e
  timeout "${limit}s" env \
    VALS_PROFILECHECK_ZONE=1 VALS_PROFILECHECK_PROBE_NODE=1 VALS_PROFILECHECK_PROBE_SECS="$PROBE_SECS" \
    VALS_PROFILECHECK_CONNECT_DEADLINE_SECS="$CONNECT_SECS" VALS_PROFILECHECK_RPC_DEADLINE_SECS="$RPC_SECS" \
    "$BENCH" x profilecheck 0 1 "$peers" > "$TMP/$label.log" 2>&1
  rc=$?
  set -e
  elapsed=$(( $(date +%s) - started ))
  printf '%s rc=%s elapsed_s=%s limit_s=%s\n' "$label" "$rc" "$elapsed" "$limit"
  [ "$rc" -ne 0 ] || { echo "$label unexpectedly succeeded against a dead endpoint" >&2; exit 1; }
  [ "$rc" -ne 124 ] || { echo "$label hung until the external ${limit}s kill: the driver is not self-bounded" >&2; exit 1; }
  [ "$elapsed" -lt "$limit" ] || { echo "$label did not finish inside its own bound" >&2; exit 1; }
}

run_probe half-open-blackhole "1@127.0.0.1:$PORT"
run_probe unroutable-endpoint "1@203.0.113.7:7001"
run_probe refused-endpoint "1@127.0.0.1:1"

if [ -n "$OUT" ]; then
  mkdir -p "$OUT"
  cp "$TMP"/*.log "$OUT/" 2>/dev/null || true
fi
echo "profile-soak probe bound test PASS (half-open, unroutable and refused endpoints all fail inside the driver's own deadlines)"

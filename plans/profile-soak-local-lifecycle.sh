#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
VALSD=${VALSD:-$ROOT/target/debug/valsd}
BENCH=${BENCH:-$ROOT/target/debug/vals-bench}
OUT=${OUT:-$ROOT/bench/task-0194-harness-lifecycle-$(date -u +%Y%m%dT%H%M%SZ)}
SEED_KEYS=${SEED_KEYS:-50000}
WORKLOAD_KEYS=${WORKLOAD_KEYS:-256}
DURATION=${DURATION:-70}
GC_GRACE=${GC_GRACE:-900s}
REQUIRE_COLLECTED_TOMBSTONES=${REQUIRE_COLLECTED_TOMBSTONES:-0}
RUNTIME=${TMPDIR:-/tmp}/task0194-lifecycle.$$
HOOK=$ROOT/plans/profile-soak-local-hook.sh
mkdir -p "$OUT/configs" "$OUT/logs" "$RUNTIME"
[ -x "$VALSD" ] && [ -x "$BENCH" ] && [ -x "$HOOK" ] || { echo "build binaries and chmod hooks first" >&2; exit 1; }
[ "$SEED_KEYS" -ge 50000 ] || { echo "local lifecycle requires at least 50000 seed keys" >&2; exit 2; }
echo local-profile-lifecycle-secret > "$RUNTIME/secret"

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}
P1=$(free_port); P2=$(free_port); P3=$(free_port)
M1=$(free_port); M2=$(free_port); M3=$(free_port)

write_node_config() {
  node=$1; port=$2; metrics=$3; profile=$4; path=$5
  other1=$((node % 3 + 1)); other2=$(( (node + 1) % 3 + 1))
  eval other1_port=\$P$other1
  eval other2_port=\$P$other2
  cat > "$path" <<EOF
node_id = $node
profile = $profile
listen = 127.0.0.1:$port
metrics = 127.0.0.1:$metrics
backend = file
data_dir = $RUNTIME/data-node$node
peer = $other1 @ 127.0.0.1:$other1_port
peer = $other2 @ 127.0.0.1:$other2_port
rf = 3
sweep = 200ms
gc_grace = $GC_GRACE
secret_file = $RUNTIME/secret
EOF
}
write_node_config 1 "$P1" "$M1" capacity "$RUNTIME/node1.conf"
write_node_config 2 "$P2" "$M2" latency "$RUNTIME/node2.conf"
write_node_config 3 "$P3" "$M3" capacity "$RUNTIME/node3.capacity.conf"
write_node_config 3 "$P3" "$M3" latency "$RUNTIME/node3.latency.conf"
cp "$RUNTIME/node3.capacity.conf" "$RUNTIME/node3.conf"
cat > "$RUNTIME/pd.conf" <<EOF
pd_id = 1
data_dir = $RUNTIME/data-pd
data_node = 1 @ 127.0.0.1:$P1
data_node = 2 @ 127.0.0.1:$P2
data_node = 3 @ 127.0.0.1:$P3
rf = 3
vnodes = 64
zone = profile-soak 3 sorted
admin_socket = $RUNTIME/pd.sock
secret_file = $RUNTIME/secret
EOF
cp "$RUNTIME"/*.conf "$OUT/configs/"

stop_process() {
  name=$1
  pid_file=$RUNTIME/$name.pid
  [ -f "$pid_file" ] || return 0
  pid=$(cat "$pid_file")
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    deadline=$(( $(date +%s) + 20 ))
    while kill -0 "$pid" 2>/dev/null; do
      state=$(awk '{print $3}' "/proc/$pid/stat" 2>/dev/null || echo X)
      [ "$state" = Z ] && break
      if [ "$(date +%s)" -ge "$deadline" ]; then kill -KILL "$pid" 2>/dev/null || true; break; fi
      sleep 1
    done
  fi
  wait "$pid" 2>/dev/null || true
  rm -f "$pid_file"
}

cleanup() {
  rc=$?
  trap - EXIT INT TERM HUP
  stop_process pd
  stop_process node3
  stop_process node2
  stop_process node1
  rm -rf "$RUNTIME"
  exit "$rc"
}
trap cleanup EXIT INT TERM HUP

start_node() {
  node=$1
  (cd "$RUNTIME"; "$VALSD" node -c "node$node.conf" >> "$OUT/logs/node$node.log" 2>&1) &
  echo $! > "$RUNTIME/node$node.pid"
}
start_pd() {
  (cd "$RUNTIME"; "$VALSD" pd -c pd.conf >> "$OUT/logs/pd.log" 2>&1) &
  echo $! > "$RUNTIME/pd.pid"
}
wait_port() {
  port=$1
  python3 - "$port" <<'PY'
import socket, sys, time
port=int(sys.argv[1]); end=time.time()+30
while time.time() < end:
    try:
        with socket.create_connection(("127.0.0.1",port), timeout=.2):
            raise SystemExit(0)
    except OSError:
        time.sleep(.1)
raise SystemExit("port did not become ready: %d" % port)
PY
}

start_node 1; start_node 2; start_node 3; start_pd
wait_port "$P1"; wait_port "$P2"; wait_port "$P3"
wait_port "$M1"; wait_port "$M2"; wait_port "$M3"
deadline=$(( $(date +%s) + 30 ))
while [ ! -S "$RUNTIME/pd.sock" ]; do
  [ "$(date +%s)" -lt "$deadline" ] || { echo "PD admin socket not ready" >&2; exit 1; }
  sleep 1
done
sleep 2

export LOCAL_WORK=$RUNTIME LOCAL_VALSD=$VALSD LOCAL_LOG_DIR=$OUT/logs
export LOCAL_PORT_1=$P1 LOCAL_PORT_2=$P2 LOCAL_PORT_3=$P3 LOCAL_DOWNTIME=5
export LOCAL_METRICS_1=$M1 LOCAL_METRICS_2=$M2 LOCAL_METRICS_3=$M3 LOCAL_READY_TIMEOUT=60
printf 'LOCAL LIFECYCLE PREFLIGHT; NOT 24h; NOT hardware\n' > "$OUT/LABEL.txt"
printf 'started=%s\nsource=%s\nruntime_removed_on_exit=%s\nseed_keys=%s\nworkload_keys=%s\nduration=%s\ngc_grace=%s\nrf=3\ndriver_quorum=2\n' \
  "$(date -u +%FT%TZ)" "$(git -C "$ROOT" rev-parse HEAD)" "$RUNTIME" "$SEED_KEYS" "$WORKLOAD_KEYS" "$DURATION" "$GC_GRACE" > "$OUT/local-provenance.txt"
sha256sum "$VALSD" "$BENCH" > "$OUT/local-binaries.sha256"
sha256sum "$ROOT/plans/profile-soak-local-lifecycle.sh" "$ROOT/plans/profile-soak-local-hook.sh" > "$OUT/local-harness.sha256"

PROFILE_SOAK_ALLOW_TEST_MODE=1 \
PEERS="1@127.0.0.1:$P1;2@127.0.0.1:$P2;3@127.0.0.1:$P3" \
METRICS="http://127.0.0.1:$M1/metrics,http://127.0.0.1:$M2/metrics,http://127.0.0.1:$M3/metrics" \
OUT="$OUT/wrapper" ZONE=1 BIN="$BENCH" DURATION="$DURATION" ROUNDS=1 SEED_KEYS="$SEED_KEYS" \
WORKLOAD_KEYS="$WORKLOAD_KEYS" SCRAPE_INTERVAL=5 RESTART_INTERVAL=25 EXPECTED_RESTARTS=2 \
DIRECT_RETRY_SECS=1 COORD_RETRY_SECS=30 PRESSURE_TIMEOUT=20 NODE_RESTART_TIMEOUT=40 HOOK_TIMEOUT=60 \
FINAL_HOOK_TIMEOUT=180 CURL_TIMEOUT=3 AE_TIMEOUT=60 AE_POLL_INTERVAL=1 VERIFY_RETRY_SECS=30 \
VERIFY_CONVERGE_SECS=120 VERIFY_POLL_SECS=5 MAX_NODE_OUTAGE_SECS="${MAX_NODE_OUTAGE_SECS:-60}" \
NODE_CONVERGE_SECS="${NODE_CONVERGE_SECS:-60}" \
MAX_MODEL_RSS_MIB=2048 REQUIRE_COLLECTED_TOMBSTONES="$REQUIRE_COLLECTED_TOMBSTONES" \
MAX_AE_SYNC_ERRORS_DELTA=5000 MAX_SERVER_ERRORS_DELTA=10 REQUIRE_OMITTED_WRITES=1 PROTOCOL_VERSION=2 \
REQUIRE_HEALED_OMISSIONS="${REQUIRE_HEALED_OMISSIONS:-0}" PROBE_SECS=15 \
EXPECTED_VALSD_SHA256="$(sha256sum "$VALSD" | awk '{print $1}')" \
CONFIG_FILES="$RUNTIME/node1.conf,$RUNTIME/node2.conf,$RUNTIME/node3.capacity.conf" \
EXTRA_CONFIG_FILES="$RUNTIME/node3.latency.conf" \
NODE_STATUS_CMD="$HOOK" ZONE_STATUS_CMD="$HOOK" RESTART_CMD="$HOOK" \
  "$ROOT/plans/profile-soak.sh" lifecycle-test > "$OUT/wrapper.log" 2>&1

omitted=$(awk '/^PROFILECHECK PASS / {for(i=1;i<=NF;i++) if($i ~ /^omitted_replica_writes=/){split($i,a,"="); v=a[2]}} END{print v+0}' "$OUT/wrapper/profilecheck.log")
[ "$omitted" -gt 0 ]
rg_pattern='PROFILECHECK VERIFY PASS'
awk -v pattern="$rg_pattern" 'index($0, pattern) {ok=1} END {exit !ok}' "$OUT/wrapper/final-verify.log"
collected=$(awk '/^PROFILECHECK VERIFY PASS / {for(i=1;i<=NF;i++){split($i,a,"="); if(a[1]=="collected_tombstones") v=a[2]}} END{print v+0}' "$OUT/wrapper/final-verify.log")
tombstones=$(awk '/^PROFILECHECK PASS / {for(i=1;i<=NF;i++){split($i,a,"="); if(a[1]=="winner_tombstones") v=a[2]}} END{print v+0}' "$OUT/wrapper/profilecheck.log")
resurrections=$(awk '/^PROFILECHECK PASS / {for(i=1;i<=NF;i++){split($i,a,"="); if(a[1]=="resurrections") v=a[2]}} END{print v+0}' "$OUT/wrapper/profilecheck.log")
rss=$(awk '/^PROFILECHECK PASS / {for(i=1;i<=NF;i++){split($i,a,"="); if(a[1]=="rss_kib") v=a[2]}} END{print v+0}' "$OUT/wrapper/profilecheck.log")
printf 'finished=%s\nomitted_replica_writes=%s\nwinner_tombstones=%s\nresurrections=%s\ncollected_tombstones=%s\ndriver_rss_kib=%s\nresult=PASS_NOT_24H_NOT_HARDWARE\n' \
  "$(date -u +%FT%TZ)" "$omitted" "$tombstones" "$resurrections" "$collected" "$rss" >> "$OUT/local-provenance.txt"

stop_process node3; stop_process node2; stop_process node1; stop_process pd
rm -rf "$RUNTIME/data-node1" "$RUNTIME/data-node2" "$RUNTIME/data-node3" "$RUNTIME/data-pd"
echo "local wrapper lifecycle PASS (NOT 24h; NOT hardware) artifacts=$OUT"

#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
BIN_DIR=${BIN_DIR:-$ROOT/target/debug}
VALSD=$BIN_DIR/valsd
BENCH=$BIN_DIR/vals-bench
OUT=${OUT:-$ROOT/bench/task-0194-local-preflight-$(date -u +%Y%m%dT%H%M%SZ)}
SEED_KEYS=${SEED_KEYS:-512}
WORKLOAD_KEYS=${WORKLOAD_KEYS:-64}
ROUNDS=${ROUNDS:-96}
ZONE=1
WORK=$OUT/work
mkdir -p "$WORK" "$OUT/logs" "$OUT/metrics"
[ -x "$VALSD" ] && [ -x "$BENCH" ] || { echo "build valsd and vals-bench first" >&2; exit 1; }

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}
P1=$(free_port); P2=$(free_port); P3=$(free_port)
M1=$(free_port); M2=$(free_port); M3=$(free_port)
ADMIN=$WORK/pd.sock
printf 'local durable file-backed harness preflight; NOT 24h; NOT hardware\n' > "$OUT/LABEL.txt"
printf 'source=%s\n' "$(git -C "$ROOT" rev-parse HEAD)" > "$OUT/provenance.txt"
printf 'git_status_sha256=%s\n' "$(git -C "$ROOT" status --porcelain=v1 | sha256sum | awk '{print $1}')" >> "$OUT/provenance.txt"
printf 'started=%s\nports=%s,%s,%s\nmetrics=%s,%s,%s\nzone=%s\ngc_grace=900s\nrf=3\nw=2\nr=2\n' "$(date -u +%FT%TZ)" "$P1" "$P2" "$P3" "$M1" "$M2" "$M3" "$ZONE" >> "$OUT/provenance.txt"
sha256sum "$VALSD" "$BENCH" > "$OUT/binaries.sha256"
sha256sum "$ROOT/crates/vals-bench/src/profilecheck.rs" "$ROOT/plans/profile-soak.sh" \
  "$ROOT/plans/profile-soak-local-preflight.sh" "$ROOT/plans/profile-soak-selftest.sh" \
  "$ROOT/plans/mixed-profile-soak-runbook.md" > "$OUT/harness.sha256"
echo local-profile-secret > "$WORK/secret"

write_node_config() {
  node=$1; port=$2; metrics=$3; profile=$4; path=$5
  eval other1_port=\$P$((node % 3 + 1))
  other1=$((node % 3 + 1))
  other2=$(( (node + 1) % 3 + 1))
  eval other2_port=\$P$other2
  cat > "$path" <<EOF
node_id = $node
profile = $profile
listen = 127.0.0.1:$port
metrics = 127.0.0.1:$metrics
backend = file
data_dir = node$node
peer = $other1 @ 127.0.0.1:$other1_port
peer = $other2 @ 127.0.0.1:$other2_port
rf = 3
sweep = 200ms
gc_grace = 900s
secret_file = secret
EOF
}
write_node_config 1 "$P1" "$M1" capacity "$WORK/node1.conf"
write_node_config 2 "$P2" "$M2" latency "$WORK/node2.conf"
write_node_config 3 "$P3" "$M3" capacity "$WORK/node3.capacity.conf"
write_node_config 3 "$P3" "$M3" latency "$WORK/node3.latency.conf"
cp "$WORK/node3.capacity.conf" "$WORK/node3.conf"
cat > "$WORK/pd.conf" <<EOF
pd_id = 1
data_dir = pd
data_node = 1 @ 127.0.0.1:$P1
data_node = 2 @ 127.0.0.1:$P2
data_node = 3 @ 127.0.0.1:$P3
rf = 3
vnodes = 64
zone = profile-soak 3 sorted
admin_socket = pd.sock
secret_file = secret
EOF
sha256sum "$WORK"/*.conf > "$OUT/config.sha256"

stop_pid() {
  name=$1
  file=$WORK/$name.pid
  [ -f "$file" ] || return 0
  pid=$(cat "$file")
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
  rm -f "$file"
}

cleanup() {
  rc=$?
  trap - EXIT INT TERM HUP
  stop_pid pd
  stop_pid node3
  stop_pid node2
  stop_pid node1
  exit "$rc"
}
trap cleanup EXIT INT TERM HUP

start_node() {
  node=$1
  (cd "$WORK" && exec "$VALSD" node -c "node$node.conf") >> "$OUT/logs/node$node.log" 2>&1 &
  echo $! > "$WORK/node$node.pid"
}
start_pd() {
  (cd "$WORK" && exec "$VALSD" pd -c pd.conf) >> "$OUT/logs/pd.log" 2>&1 &
  echo $! > "$WORK/pd.pid"
}
wait_port() {
  port=$1
  python3 - "$port" <<'PY'
import socket, sys, time
port=int(sys.argv[1]); end=time.time()+30
while time.time() < end:
    try:
        with socket.create_connection(("127.0.0.1",port), timeout=.2):
            sys.exit(0)
    except OSError:
        time.sleep(.1)
raise SystemExit("port did not become ready: %d" % port)
PY
}
wait_clean() {
  deadline=$(( $(date +%s) + 30 ))
  while :; do
    clean=1
    for metrics in "$M1" "$M2" "$M3"; do
      value=$(curl -fsS --max-time 2 "http://127.0.0.1:$metrics/metrics" | awk '$1 == "vals_ae_dirty_buckets" {print $2; found=1} END {if (!found) print "missing"}') || value=missing
      [ "$value" = 0 ] || clean=0
    done
    [ "$clean" -eq 1 ] && return
    [ "$(date +%s)" -lt "$deadline" ] || { echo "AE did not converge" >&2; return 1; }
    sleep 1
  done
}
PEERS_ARG="1@127.0.0.1:$P1;2@127.0.0.1:$P2;3@127.0.0.1:$P3"

run_driver() {
  phase=$1; rounds=$2
  dirty_env=
  if [ "${ALLOW_DIRTY_RESUME+x}" = x ]; then
    dirty_env=VALS_PROFILECHECK_ALLOW_DIRTY_RESUME=$ALLOW_DIRTY_RESUME
  fi
  env $dirty_env \
  VALS_PROFILECHECK_ZONE=$ZONE VALS_PROFILECHECK_STATE="$OUT/reference.state" VALS_PROFILECHECK_SEED_KEYS=$SEED_KEYS \
  VALS_PROFILECHECK_WORKLOAD_KEYS=$WORKLOAD_KEYS VALS_PROFILECHECK_GC_GRACE_SECS=900 \
  VALS_PROFILECHECK_MAX_RECONCILES=${MAX_RECONCILES:-65536} \
  VALS_PROFILECHECK_DIRECT_RETRY_SECS=2 VALS_PROFILECHECK_COORD_RETRY_SECS=30 \
    "$BENCH" x profilecheck "$rounds" 1 "$PEERS_ARG" > "$OUT/$phase.log" 2>&1
}

crash_before_checkpoint() {
  phase=$1
  VALS_PROFILECHECK_ZONE=$ZONE VALS_PROFILECHECK_STATE="$OUT/reference.state" VALS_PROFILECHECK_SEED_KEYS=$SEED_KEYS \
  VALS_PROFILECHECK_WORKLOAD_KEYS=$WORKLOAD_KEYS VALS_PROFILECHECK_GC_GRACE_SECS=900 \
  VALS_PROFILECHECK_CHECKPOINT_SECS=900 VALS_PROFILECHECK_SAMPLE_EVERY=1 \
  VALS_PROFILECHECK_DIRECT_RETRY_SECS=2 VALS_PROFILECHECK_COORD_RETRY_SECS=30 \
    "$BENCH" x profilecheck 1000000 1 "$PEERS_ARG" 600 > "$OUT/$phase.log" 2>&1 &
  crash_pid=$!
  deadline=$(( $(date +%s) + 30 ))
  while ! grep -q '^PROFILECHECK FIRST_MUTATION ' "$OUT/$phase.log"; do
    kill -0 "$crash_pid" 2>/dev/null || { echo "driver exited before its first mutation" >&2; exit 1; }
    [ "$(date +%s)" -lt "$deadline" ] || { echo "driver made no mutation before timeout" >&2; exit 1; }
    sleep 0.1
  done
  kill -9 "$crash_pid" 2>/dev/null || true
  wait "$crash_pid" 2>/dev/null || true
  if grep -q '^PROFILECHECK CHECKPOINT ' "$OUT/$phase.log"; then
    echo "crashed phase unexpectedly reached a checkpoint" >&2
    exit 1
  fi
}

state_clean_flag() {
  python3 - "$OUT/reference.state" <<'PY'
import struct, sys
with open(sys.argv[1], "rb") as state:
    data = state.read(72)
if len(data) < 72 or data[:8] != b"VPCSTATE":
    raise SystemExit("invalid profilecheck state header")
print(struct.unpack_from("<Q", data, 64)[0])
PY
}
restart_node3_as() {
  profile=$1
  stop_pid node3
  cp "$WORK/node3.$profile.conf" "$WORK/node3.conf"
  start_node 3
  wait_port "$P3"
  printf '%s node3 profile=%s restarted\n' "$(date -u +%FT%TZ)" "$profile" >> "$OUT/restarts.log"
}

start_node 1; start_node 2; start_node 3; start_pd
wait_port "$P1"; wait_port "$P2"; wait_port "$P3"
for metrics in "$M1" "$M2" "$M3"; do wait_port "$metrics"; done
sleep 2
"$VALSD" admin -s "$ADMIN" zones > "$OUT/zones.txt"
awk '$1 == 1 && $2 == "profile-soak" && $3 == 3 && $4 == "sorted" {ok=1} END {exit !ok}' "$OUT/zones.txt"

run_driver phase-capacity "$ROUNDS"

if [ "${RESUME_TEST:-1}" = 1 ]; then
  [ "$(state_clean_flag)" = 1 ] || { echo "completed state was not clean before crash test" >&2; exit 1; }
  crash_before_checkpoint phase-crash
  [ "$(state_clean_flag)" = 0 ] || { echo "pre-mutation dirty marker was not durable after SIGKILL" >&2; exit 1; }
  if run_driver phase-refused 16; then
    echo "resuming a crashed (dirty) reference was not refused" >&2
    exit 1
  fi
  grep -q "was not cleanly finalized" "$OUT/phase-refused.log" || {
    echo "dirty resume failed for an unexpected reason" >&2
    tail -5 "$OUT/phase-refused.log" >&2
    exit 1
  }
  if ALLOW_DIRTY_RESUME=0 run_driver phase-env0-refused 16; then
    echo "VALS_PROFILECHECK_ALLOW_DIRTY_RESUME=0 enabled dirty recovery" >&2
    exit 1
  fi
  grep -q "was not cleanly finalized" "$OUT/phase-env0-refused.log" || {
    echo "dirty resume with environment value zero failed for an unexpected reason" >&2
    tail -5 "$OUT/phase-env0-refused.log" >&2
    exit 1
  }
  ALLOW_DIRTY_RESUME=1 MAX_RECONCILES=64 run_driver phase-resume 128
  grep -q '^PROFILECHECK RESUME .*clean=0' "$OUT/phase-resume.log" || {
    echo "the reconciling resume did not report a dirty predecessor" >&2
    exit 1
  }
  awk '/^PROFILECHECK PASS / { for (i=1; i<=NF; i++) if ($i ~ /^reconciled_winners=/) { split($i,a,"="); if (a[2] <= 64) ok=1 } } END { exit !ok }' "$OUT/phase-resume.log" || {
    echo "the dirty resume did not finish within its reconciliation bound" >&2
    exit 1
  }
  printf '%s crash_before_checkpoint=1 dirty_marker=1 refused_absent=1 refused_env0=1 bounded_dirty_resume=1\n' "$(date -u +%FT%TZ)" >> "$OUT/restarts.log"
  unset ALLOW_DIRTY_RESUME MAX_RECONCILES
fi

restart_node3_as latency
run_driver phase-latency 64
restart_node3_as capacity
run_driver phase-returned 64

for node in 1 2 3; do
  stop_pid "node$node"
  start_node "$node"
  eval port=\$P$node
  wait_port "$port"
  printf '%s node=%s final_restart=1\n' "$(date -u +%FT%TZ)" "$node" >> "$OUT/restarts.log"
done
wait_clean
node=1
for metrics in "$M1" "$M2" "$M3"; do
  curl -fsS --max-time 5 "http://127.0.0.1:$metrics/metrics" > "$OUT/metrics/final.node$node.prom"
  node=$((node + 1))
done
VALS_PROFILECHECK_ZONE=$ZONE VALS_PROFILECHECK_STATE="$OUT/reference.state" VALS_PROFILECHECK_SEED_KEYS=$SEED_KEYS \
VALS_PROFILECHECK_WORKLOAD_KEYS=$WORKLOAD_KEYS VALS_PROFILECHECK_GC_GRACE_SECS=900 \
VALS_PROFILECHECK_VERIFY_ONLY=1 VALS_PROFILECHECK_VERIFY_RETRY_SECS=30 VALS_PROFILECHECK_VERIFY_CONVERGE_SECS=120 \
  "$BENCH" x profilecheck 0 1 "1@127.0.0.1:$P1;2@127.0.0.1:$P2;3@127.0.0.1:$P3" > "$OUT/final-verify.log" 2>&1
awk '/^PROFILECHECK VERIFY PASS / {ok=1} END {exit !ok}' "$OUT/final-verify.log"
[ "$(state_clean_flag)" = 1 ] || { echo "verify-only changed the clean state marker" >&2; exit 1; }
printf 'finished=%s\nresult=PASS_NOT_24H_NOT_HARDWARE\n' "$(date -u +%FT%TZ)" >> "$OUT/provenance.txt"
stop_pid node3; stop_pid node2; stop_pid node1; stop_pid pd
rm -rf "$WORK/node1" "$WORK/node2" "$WORK/node3" "$WORK/pd"
echo "local durable profile preflight PASS (NOT 24h; NOT hardware) artifacts=$OUT"

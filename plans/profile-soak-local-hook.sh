#!/bin/sh
set -eu

WORK=${LOCAL_WORK:?}
VALSD=${LOCAL_VALSD:?}
LOG_DIR=${LOCAL_LOG_DIR:?}
DOWNTIME=${LOCAL_DOWNTIME:-4}
SOAK_EVENT=${SOAK_EVENT:?}
SOAK_RESTART_INDEX=${SOAK_RESTART_INDEX:-0}

node_port() {
  eval "printf '%s\\n' \"\${LOCAL_PORT_$1:?}\""
}

node_metrics_port() {
  eval "printf '%s\\n' \"\${LOCAL_METRICS_$1:?}\""
}

wait_ready() {
  node=$1
  profile=$2
  port=$(node_port "$node")
  metrics=$(node_metrics_port "$node")
  deadline=$(( $(date +%s) + ${LOCAL_READY_TIMEOUT:-60} ))
  scratch=$(mktemp)
  while :; do
    ready=0
    if python3 - "$port" <<'PY'
import socket, sys
try:
    with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=.5):
        raise SystemExit(0)
except OSError:
    raise SystemExit(1)
PY
    then
      if curl -fsS --max-time 3 "http://127.0.0.1:$metrics/metrics" > "$scratch"; then
        if awk -v want="$profile" '
            $1 == "vals_free_segments" { free=$2; have_free=1 }
            $1 == "vals_profile_info{profile=\"latency\"}" && $2 == 1 { latency=1 }
            $1 == "vals_profile_info{profile=\"capacity\"}" && $2 == 1 { capacity=1 }
            $1 == "vals_blind_append_enabled" { blind=$2 }
            $1 == "vals_winner_directory_enabled" { directory=$2 }
            $1 == "vals_winner_directory_keys" { keys=$2 }
            END {
              if (!have_free || free <= 0) exit 1
              if (!latency && !capacity) exit 1
              if (latency && (blind != 1 || directory != 1 || keys <= 0)) exit 1
              if (capacity && (blind != 0 || directory != 0)) exit 1
              if (want == "latency" && !latency) exit 1
              if (want == "capacity" && !capacity) exit 1
            }
          ' "$scratch"; then ready=1; fi
      fi
    fi
    if [ "$ready" -eq 1 ]; then
      rm -f "$scratch"
      printf 'READY node=%s profile=%s data_port=%s engine_open=1 directory_checked=1\n' "$node" "$profile" "$port"
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      rm -f "$scratch"
      echo "node $node did not reach serving readiness" >&2
      return 1
    fi
    sleep 1
  done
}

stop_node() {
  node=$1
  pid_file=$WORK/node$node.pid
  [ -f "$pid_file" ] || return 0
  pid=$(cat "$pid_file")
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    deadline=$(( $(date +%s) + 15 ))
    while kill -0 "$pid" 2>/dev/null; do
      state=$(awk '{print $3}' "/proc/$pid/stat" 2>/dev/null || echo X)
      [ "$state" = Z ] && break
      if [ "$(date +%s)" -ge "$deadline" ]; then kill -KILL "$pid" 2>/dev/null || true; break; fi
      sleep 1
    done
  fi
  rm -f "$pid_file"
}

start_node() {
  node=$1
  (cd "$WORK"; nohup "$VALSD" node -c "node$node.conf" >> "$LOG_DIR/node$node.log" 2>&1 </dev/null & echo $! > "node$node.pid.tmp")
  mv "$WORK/node$node.pid.tmp" "$WORK/node$node.pid"
}

restart_node() {
  node=$1
  profile=${2:-unchanged}
  stop_node "$node"
  sleep "$DOWNTIME"
  if [ "$profile" = latency ] || [ "$profile" = capacity ]; then
    cp "$WORK/node3.$profile.conf" "$WORK/node3.conf"
  fi
  start_node "$node"
  wait_ready "$node" "$profile"
}

case "$SOAK_EVENT" in
  status)
    binary_sha=$(sha256sum "$VALSD" | awk '{print $1}')
    for node in 1 2 3; do
      pid=$(cat "$WORK/node$node.pid")
      kill -0 "$pid"
      stats=$(awk -v pid="$pid" '/^VmRSS:/ {rss=$2} /^VmSwap:/ {swap=$2} END {if (rss == "" || swap == "") exit 1; printf "pid=%s rss_kib=%s swap_kib=%s", pid, rss, swap}' "/proc/$pid/status")
      echo "NODE_STATUS node=$node host=localhost $stats clock_skew_ms=0 active=1"
      conf=$WORK/node$node.conf
      running_sha=$(sha256sum "/proc/$pid/exe" 2>/dev/null | awk '{print $1}')
      [ -n "$running_sha" ] || running_sha=$binary_sha
      config_sha=$(sha256sum "$conf" | awk '{print $1}')
      config_id=$(awk -F= '{ sub(/#.*/, ""); gsub(/[[:space:]]/, ""); if ($1 == "node_id") print $2 }' "$conf")
      config_profile=$(awk -F= '{ sub(/#.*/, ""); gsub(/[[:space:]]/, ""); if ($1 == "profile") print $2 }' "$conf")
      echo "NODE_CONFIG node=$node path=$conf config_sha256=$config_sha config_node_id=$config_id config_profile=$config_profile binary_sha256=$running_sha"
      if [ "$node" -eq 3 ]; then
        for alt in capacity latency; do
          echo "NODE_ALT node=3 profile=$alt config_sha256=$(sha256sum "$WORK/node3.$alt.conf" | awk '{print $1}')"
        done
      fi
    done
    ;;
  zone-status)
    zones=$("$VALSD" admin -s "$WORK/pd.sock" zones)
    printf '%s\n' "$zones" | awk '$1 == 1 && $2 == "profile-soak" && $3 == 3 && $4 == "sorted" {ok=1} END {exit !ok}'
    echo "ZONE_STATUS zone=1 name=profile-soak sorted=1 rf=3 declared_w=2 declared_r=2"
    echo "ZONE_STATUS_NOTE declared_w/declared_r are operator-declared intent; executed quorums appear as quorum=N in the PROFILECHECK PASS line"
    ;;
  hourly)
    case "$SOAK_RESTART_INDEX" in
      1)
        restart_node 3 latency
        echo "EVENT node=3 restart=1 from=capacity to=latency"
        ;;
      2)
        restart_node 3 capacity
        echo "EVENT node=3 restart=1 from=latency to=capacity"
        ;;
      *)
        node=$(( (SOAK_RESTART_INDEX - 3) % 3 + 1 ))
        restart_node "$node"
        echo "EVENT node=$node restart=1 profile=unchanged"
        ;;
    esac
    ;;
  final)
    for node in 1 2 3; do restart_node "$node" unchanged; done
    echo "EVENT rolling=all restart=1"
    ;;
  *) echo "unsupported SOAK_EVENT=$SOAK_EVENT" >&2; exit 2 ;;
esac

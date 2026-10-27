#!/bin/sh
set -eu

HOSTS=${HOSTS:?set HOSTS to node1-host,node2-host,node3-host}
SSH_USER=${SSH_USER:-why}
NODE_UNIT=${NODE_UNIT:-vals-node}
METRICS_PORT=${METRICS_PORT:-9464}
DATA_PORT=${DATA_PORT:-7001}
SSH_TIMEOUT=${SSH_TIMEOUT:-20}
NODE_RESTART_TIMEOUT=${NODE_RESTART_TIMEOUT:-240}
HEALTH_TIMEOUT=${HEALTH_TIMEOUT:-$((NODE_RESTART_TIMEOUT - SSH_TIMEOUT - 20))}
NODE_CONFIG_PATH=${NODE_CONFIG_PATH:-/home/user/vals/run/node.conf}
SOAK_EVENT=${SOAK_EVENT:?invoked by profile-soak.sh}
SOAK_RESTART_INDEX=${SOAK_RESTART_INDEX:-0}

[ "$HEALTH_TIMEOUT" -gt 0 ] || { echo "HEALTH_TIMEOUT must be positive" >&2; exit 2; }
[ "$((HEALTH_TIMEOUT + SSH_TIMEOUT + 10))" -le "$NODE_RESTART_TIMEOUT" ] || {
  echo "HEALTH_TIMEOUT+SSH_TIMEOUT must fit inside NODE_RESTART_TIMEOUT=$NODE_RESTART_TIMEOUT" >&2
  exit 2
}

host_for_node() {
  wanted=$1
  oldifs=$IFS; IFS=,; n=0
  for host in $HOSTS; do
    n=$((n + 1))
    if [ "$n" -eq "$wanted" ]; then IFS=$oldifs; printf '%s\n' "$host"; return; fi
  done
  IFS=$oldifs
  return 1
}

restart_node() {
  node=$1
  profile=${2:-unchanged}
  host=$(host_for_node "$node")
  install_src=none
  if [ "$profile" = latency ]; then
    [ "$node" -eq 3 ] || exit 2
    : "${NODE3_LATENCY_CONFIG:?set remote prevalidated latency config path}"
    install_src=$NODE3_LATENCY_CONFIG
  elif [ "$profile" = capacity ]; then
    [ "$node" -eq 3 ] || exit 2
    : "${NODE3_CAPACITY_CONFIG:?set remote prevalidated capacity config path}"
    install_src=$NODE3_CAPACITY_CONFIG
  fi
  timeout "${NODE_RESTART_TIMEOUT}s" ssh -o BatchMode=yes -o ConnectTimeout="$SSH_TIMEOUT" "$SSH_USER@$host" \
    sh -s "$NODE_UNIT" "$METRICS_PORT" "$DATA_PORT" "$HEALTH_TIMEOUT" "$profile" "$install_src" \
      "${NODE3_ACTIVE_CONFIG:-$NODE_CONFIG_PATH}" "$node" <<'REMOTE'
set -eu
unit=$1; metrics_port=$2; data_port=$3; health_timeout=$4; profile=$5; install_src=$6; active_conf=$7; node=$8

config_facts() {
  file=$1
  sha=$(sha256sum "$file" | awk '{print $1}')
  prof=$(awk -F= '{ sub(/#.*/, ""); gsub(/[[:space:]]/, ""); if ($1 == "profile") print $2 }' "$file")
  printf '%s %s\n' "$sha" "$prof"
}

before=$(config_facts "$active_conf")
printf 'CONFIG_BEFORE node=%s config_sha256=%s config_profile=%s\n' "$node" "${before% *}" "${before#* }"
if [ "$install_src" != none ]; then
  sudo -n install -m 0644 "$install_src" "$active_conf"
  expected=$(config_facts "$install_src")
  after=$(config_facts "$active_conf")
  [ "$after" = "$expected" ] || { echo "deployed config does not match the prevalidated alternate" >&2; exit 1; }
fi
sudo -n systemctl restart "$unit"

scratch=$(mktemp)
trap 'rm -f "$scratch"' EXIT

data_port_ready() {
  if command -v nc >/dev/null 2>&1; then
    nc -z 127.0.0.1 "$data_port"
  elif command -v bash >/dev/null 2>&1; then
    bash -c "exec 3<>/dev/tcp/127.0.0.1/$data_port"
  else
    ss -ltnH 2>/dev/null | grep -q ":$data_port "
  fi
}

end=$(($(date +%s) + health_timeout))
while :; do
  ready=0
  if [ "$(systemctl is-active "$unit")" = active ] \
    && curl -fsS --max-time 5 "http://127.0.0.1:$metrics_port/metrics" > "$scratch" \
    && data_port_ready; then
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
  [ "$ready" -eq 1 ] && break
  [ "$(date +%s)" -lt "$end" ] || { echo "node did not reach serving readiness" >&2; exit 1; }
  sleep 2
done

after=$(config_facts "$active_conf")
printf 'CONFIG_AFTER node=%s config_sha256=%s config_profile=%s\n' "$node" "${after% *}" "${after#* }"
printf 'READY node=%s profile=%s data_port=%s engine_open=1 directory_checked=1\n' "$node" "$profile" "$data_port"
REMOTE
}

if [ "$SOAK_EVENT" = final ]; then
  for node in 1 2 3; do restart_node "$node"; done
  echo "EVENT rolling=all restart=1"
  exit 0
fi

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

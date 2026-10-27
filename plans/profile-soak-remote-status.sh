#!/bin/sh
# NODE_STATUS_CMD for profile-soak.sh: per node, the running daemon's RSS, binary, deployed config
# and node-3 alternates, with remote clock skew.
set -eu

HOSTS=${HOSTS:?set HOSTS to node1-host,node2-host,node3-host}
SSH_USER=${SSH_USER:-why}
NODE_UNIT=${NODE_UNIT:-vals-node}
SSH_TIMEOUT=${SSH_TIMEOUT:-15}
MAX_CLOCK_SKEW_MS=${MAX_CLOCK_SKEW_MS:-500}
NODE_CONFIG_PATH=${NODE_CONFIG_PATH:-/home/user/vals/run/node.conf}
NODE3_CAPACITY_CONFIG=${NODE3_CAPACITY_CONFIG:-}
NODE3_LATENCY_CONFIG=${NODE3_LATENCY_CONFIG:-}

oldifs=$IFS
IFS=,
node=0
for host in $HOSTS; do
  node=$((node + 1))
  alternates=
  if [ "$node" -eq 3 ]; then
    alternates="$NODE3_CAPACITY_CONFIG $NODE3_LATENCY_CONFIG"
  fi
  before=$(($(date +%s%N) / 1000000))
  out=$(timeout "${SSH_TIMEOUT}s" ssh -o BatchMode=yes -o ConnectTimeout="$SSH_TIMEOUT" "$SSH_USER@$host" "
    set -eu
    test \"\$(systemctl is-active '$NODE_UNIT')\" = active
    pid=\$(systemctl show -p MainPID --value '$NODE_UNIT')
    test \"\$pid\" -gt 1
    printf 'remote_ms=%s ' \"\$((\$(date +%s%N) / 1000000))\"
    awk -v pid=\"\$pid\" '
      /^VmRSS:/ { rss=\$2 }
      /^VmSwap:/ { swap=\$2 }
      END { if (rss == \"\" || swap == \"\") exit 1; printf \"pid=%s rss_kib=%s swap_kib=%s\", pid, rss, swap }
    ' /proc/\$pid/status
    printf '\n'
    binary_sha=\$(sudo -n sha256sum /proc/\$pid/exe 2>/dev/null | awk '{print \$1}')
    if [ -z \"\$binary_sha\" ]; then binary_sha=\$(sha256sum /proc/\$pid/exe 2>/dev/null | awk '{print \$1}'); fi
    if [ -z \"\$binary_sha\" ]; then echo 'cannot hash the running daemon executable' >&2; exit 1; fi
    conf='$NODE_CONFIG_PATH'
    test -r \"\$conf\"
    config_sha=\$(sha256sum \"\$conf\" | awk '{print \$1}')
    config_id=\$(awk -F= '{ sub(/#.*/, \"\"); gsub(/[[:space:]]/, \"\"); if (\$1 == \"node_id\") print \$2 }' \"\$conf\")
    config_profile=\$(awk -F= '{ sub(/#.*/, \"\"); gsub(/[[:space:]]/, \"\"); if (\$1 == \"profile\") print \$2 }' \"\$conf\")
    config_rf=\$(awk -F= '{ sub(/#.*/, \"\"); gsub(/[[:space:]]/, \"\"); if (\$1 == \"rf\") print \$2 }' \"\$conf\")
    config_grace=\$(awk -F= '{ sub(/#.*/, \"\"); gsub(/[[:space:]]/, \"\"); if (\$1 == \"gc_grace\") print \$2 }' \"\$conf\")
    printf 'CONFIG path=%s config_sha256=%s config_node_id=%s config_profile=%s config_rf=%s config_gc_grace=%s binary_sha256=%s\n' \\
      \"\$conf\" \"\$config_sha\" \"\$config_id\" \"\$config_profile\" \"\$config_rf\" \"\$config_grace\" \"\$binary_sha\"
    for alt in $alternates; do
      test -r \"\$alt\"
      alt_profile=\$(awk -F= '{ sub(/#.*/, \"\"); gsub(/[[:space:]]/, \"\"); if (\$1 == \"profile\") print \$2 }' \"\$alt\")
      printf 'ALT profile=%s config_sha256=%s path=%s\n' \"\$alt_profile\" \"\$(sha256sum \"\$alt\" | awk '{print \$1}')\" \"\$alt\"
    done
  ")
  after=$(($(date +%s%N) / 1000000))
  line=$(printf '%s\n' "$out" | awk 'NR == 1')
  config_line=$(printf '%s\n' "$out" | awk '/^CONFIG /')
  [ -n "$config_line" ] || { echo "node $node did not report its deployed config" >&2; exit 1; }
  remote_ms=$(printf '%s\n' "$line" | awk '{ for (i = 1; i <= NF; i++) { split($i, a, "="); if (a[1] == "remote_ms") print a[2] } }')
  [ -n "$remote_ms" ] || { echo "node $node did not report its clock" >&2; exit 1; }
  skew=$(awk -v remote="$remote_ms" -v before="$before" -v after="$after" 'BEGIN {
    mid = (before + after) / 2
    d = remote - mid
    if (d < 0) d = -d
    printf "%d", d
  }')
  [ "$skew" -le "$MAX_CLOCK_SKEW_MS" ] || {
    echo "node $node clock skew ${skew}ms exceeds ${MAX_CLOCK_SKEW_MS}ms (keep NTP disciplined before the gate)" >&2
    exit 1
  }
  printf 'NODE_STATUS node=%s host=%s %s clock_skew_ms=%s active=1\n' "$node" "$host" "$line" "$skew"
  printf 'NODE_CONFIG node=%s host=%s %s\n' "$node" "$host" "$(printf '%s\n' "$config_line" | cut -d' ' -f2-)"
  printf '%s\n' "$out" | awk -v node="$node" '/^ALT / { printf "NODE_ALT node=%s %s\n", node, substr($0, 5) }'
done
IFS=$oldifs
[ "$node" -eq 3 ]

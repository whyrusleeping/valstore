#!/bin/sh
# DIAG_CMD for profile-soak.sh: per node, writes into $SOAK_OUT/diag/<tag>.node<N>.*
#   SOAK_EVENT=diag          a PERF_SECS perf profile of the running node daemon (self time by
#                            symbol) and its WARN/ERROR/panic log lines since the run started
#   diag-final|diag-failure  the node's whole log since the run started (gzip) plus the same extract
set -u

HOSTS=${HOSTS:?set HOSTS to node1-host,node2-host,node3-host}
SSH_USER=${SSH_USER:-why}
SSH_TIMEOUT=${SSH_TIMEOUT:-20}
NODE_UNIT=${NODE_UNIT:-vals-node}
NODE_LOG=${NODE_LOG:?set NODE_LOG to the node daemon log path on each host}
PERF_SECS=${PERF_SECS:-15}
SOAK_OUT=${SOAK_OUT:?invoked by profile-soak.sh}
SOAK_EVENT=${SOAK_EVENT:?invoked by profile-soak.sh}
SOAK_DIAG_TAG=${SOAK_DIAG_TAG:?invoked by profile-soak.sh}
SOAK_STARTED=${SOAK_STARTED:?invoked by profile-soak.sh}
since="[${SOAK_STARTED%Z}"
mkdir -p "$SOAK_OUT/diag"

remote() {
  host=$1
  limit=$2
  shift 2
  timeout "${limit}s" ssh -o BatchMode=yes -o ConnectTimeout="$SSH_TIMEOUT" "$SSH_USER@$host" "$@"
}

window="awk -v since='$since' 'on || (substr(\$1, 1, 1) == \"[\" && \$1 >= since) { on=1; print }' '$NODE_LOG'"

node=0
for host in $(printf '%s\n' "$HOSTS" | tr ',' ' '); do
  node=$((node + 1))
  dest=$SOAK_OUT/diag/$SOAK_DIAG_TAG.node$node
  (
    remote "$host" 120 "$window | grep -E ' (WARN|ERROR) |panic|integrity|corrupt' | tail -n 2000" > "$dest.warn.txt" 2>&1
    case "$SOAK_EVENT" in
      diag)
        [ "$PERF_SECS" -gt 0 ] || exit 0
        remote "$host" "$((PERF_SECS + 240))" "
          set -u
          pid=\$(systemctl show -p MainPID --value '$NODE_UNIT')
          [ \"\$pid\" -gt 1 ] || { echo 'no running node daemon'; exit 1; }
          ev=cycles
          [ -d /sys/bus/event_source/devices/cpu_core ] && ev=cpu_core/cycles/
          tmp=\$(mktemp -d /tmp/profile-soak-perf.XXXXXX)
          data=\$tmp/perf.data
          sudo -n perf record -e \"\$ev\" -F 999 -p \"\$pid\" -o \"\$data\" -- sleep $PERF_SECS >/dev/null 2>&1
          sudo -n perf report -f -i \"\$data\" --stdio --no-children --sort comm,symbol --percent-limit 0.3 2>/dev/null \
            | grep -v '^#' | grep -v '^\$' | head -200
          sudo -n rm -rf \"\$tmp\"
        " > "$dest.perf.txt" 2>&1
        ;;
      diag-final|diag-failure)
        remote "$host" 600 "$window | gzip -c" > "$dest.log.gz" 2> "$dest.log.err"
        ;;
    esac
  ) &
done
wait
ls -l "$SOAK_OUT/diag" | grep "$SOAK_DIAG_TAG"

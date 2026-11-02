#!/usr/bin/env bash
#
# netbench.sh — network baseline between hosts with iperf3 / netperf / ping.
# No root needed (ping falls back to a 0.2 s interval when unprivileged).
#
# Usage:
#   netbench.sh server                         start iperf3 -s on ports 5201-5204 and
#                                              netserver (if present); pids -> $ROOT/run/netbench.pids
#   netbench.sh stop                           kill exactly those pids
#   netbench.sh client --peer IP --name NAME [--streams N] [--secs S] [--port P] [--procs K] [--reverse]
#                                              iperf3 -c IP -P N -t S -J -> out/net/NAME.json; prints Gbit/s
#                                              --procs K runs K iperf3 clients on ports P..P+K-1 concurrently
#                                              (iperf3 is single-threaded; one process tops out ~20-30 Gbit/s)
#                                              and writes NAME-pK.json + NAME-summary.json
#   netbench.sh rr --peer IP --name NAME [--secs S]
#                                              netperf TCP_RR latency -> out/net/NAME-rr.txt
#   netbench.sh ping --peer IP --name NAME [--count N]
#                                              ping -c N -i 0.01 -q (-i 0.2 unprivileged) -> out/net/NAME-ping.txt
#   [--root DIR]  override $VALS_CLOUD_ROOT (default $HOME/vals)
#
# Prints one machine-readable line at the end:  RESULT {...json...}
set -euo pipefail

log()  { printf '[netbench] %s\n' "$*"; }
warn() { printf '[netbench] WARN: %s\n' "$*" >&2; }
die()  { [ -z "${OUT:-}" ] || fix_owner "$OUT"; printf '[netbench] ERROR: %s\n' "$*" >&2; echo "RESULT {\"ok\":false,\"error\":$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$*")}"; exit 1; }

default_root() {
  if [ -n "${VALS_CLOUD_ROOT:-}" ]; then echo "$VALS_CLOUD_ROOT"; return; fi
  if [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != root ]; then
    local h; h="$(getent passwd "$SUDO_USER" | cut -d: -f6)"
    [ -n "$h" ] && { echo "$h/vals"; return; }
  fi
  echo "$HOME/vals"
}
fix_owner() {
  [ "$(id -u)" -eq 0 ] || return 0
  [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != root ] || return 0
  local p
  for p in "$@"; do [ -e "$p" ] && chown -R "$SUDO_USER:" "$p" 2>/dev/null || true; done
}
json_obj() {
  python3 - "$@" <<'PY'
import json, sys
o = {}
for a in sys.argv[1:]:
    k, _, v = a.partition('=')
    if k.endswith(':'):
        k = k[:-1]
        try:
            v = json.loads(v)
        except ValueError:
            pass
    o[k] = v
print(json.dumps(o, separators=(',', ':')))
PY
}

MODE=""; PEER=""; NAME=""; STREAMS=8; SECS=10; PORT=5201; PROCS=1; REVERSE=0; COUNT=200; ROOT=""
while [ $# -gt 0 ]; do
  case "$1" in
    server|stop|client|rr|ping) MODE="$1"; shift;;
    --peer) PEER="$2"; shift 2;;
    --name) NAME="$2"; shift 2;;
    --streams) STREAMS="$2"; shift 2;;
    --secs) SECS="$2"; shift 2;;
    --port) PORT="$2"; shift 2;;
    --procs) PROCS="$2"; shift 2;;
    --count) COUNT="$2"; shift 2;;
    --reverse) REVERSE=1; shift;;
    --root) ROOT="$2"; shift 2;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
    *) die "unknown argument: $1";;
  esac
done
[ -n "$MODE" ] || die "usage: netbench.sh server|stop|client|rr|ping ... (see --help)"
[ -n "$ROOT" ] || ROOT="$(default_root)"
OUT="$ROOT/out"
NET="$OUT/net"
RUN="$ROOT/run"
PIDFILE="$RUN/netbench.pids"
mkdir -p "$NET" "$RUN"
PORTS=(5201 5202 5203 5204)

need_peer_name() {
  [ -n "$PEER" ] || die "$MODE needs --peer IP"
  [ -n "$NAME" ] || die "$MODE needs --name NAME"
  [[ "$NAME" =~ ^[A-Za-z0-9._-]+$ ]] || die "NAME must be [A-Za-z0-9._-]+"
}

port_listening() { ss -Hltn "sport = :$1" 2>/dev/null | grep -q . ; }

case "$MODE" in
  # ---------------------------------------------------------------------------------
  server)
    command -v iperf3 >/dev/null 2>&1 || die "iperf3 not installed"
    started=()
    # Idempotent: re-use servers we started earlier that are still alive.
    declare -A known=()
    if [ -s "$PIDFILE" ]; then
      while read -r p what port; do
        [ -n "$p" ] && [ -d "/proc/$p" ] && [ "$(cat "/proc/$p/comm" 2>/dev/null)" = "$what" ] && known["$what:$port"]="$p"
      done < "$PIDFILE"
    fi
    : > "$PIDFILE.new"
    for port in "${PORTS[@]}"; do
      if [ -n "${known[iperf3:$port]:-}" ]; then
        echo "${known[iperf3:$port]} iperf3 $port" >> "$PIDFILE.new"; log "port $port already served by our pid ${known[iperf3:$port]}"
        continue
      fi
      if port_listening "$port"; then
        owner="$(ss -Hltnp "sport = :$port" 2>/dev/null | grep -oE 'pid=[0-9]+' | head -1 | cut -d= -f2 || true)"
        if [ -n "$owner" ] && [ "$(cat "/proc/$owner/comm" 2>/dev/null)" = iperf3 ]; then
          echo "$owner iperf3 $port" >> "$PIDFILE.new"; log "port $port already served by iperf3 pid $owner"
        else
          warn "port $port is in use by another process; skipping"
        fi
        continue
      fi
      pidf="$RUN/iperf3-$port.pid"
      rm -f "$pidf"
      if iperf3 -s -p "$port" -D -I "$pidf" >/dev/null 2>&1; then
        for _ in 1 2 3 4 5 6 7 8 9 10; do [ -s "$pidf" ] && break; sleep 0.1; done
        p="$(cat "$pidf" 2>/dev/null || true)"
        [ -n "$p" ] || p="$(pgrep -n -f "iperf3 -s -p $port" || true)"
        if [ -n "$p" ]; then echo "$p iperf3 $port" >> "$PIDFILE.new"; started+=("iperf3:$port"); else warn "iperf3 on $port started but pid unknown"; fi
      else
        warn "iperf3 -s -p $port failed to start"
      fi
    done
    if command -v netserver >/dev/null 2>&1; then
      if [ -n "${known[netserver:12865]:-}" ]; then
        echo "${known[netserver:12865]} netserver 12865" >> "$PIDFILE.new"; log "netserver already running (our pid ${known[netserver:12865]})"
      elif port_listening 12865; then
        log "netserver already listening on 12865"
        owner="$(pgrep -x netserver | head -1 || true)"; [ -n "$owner" ] && echo "$owner netserver 12865" >> "$PIDFILE.new"
      else
        before="$(pgrep -x netserver | sort | tr '\n' ' ' || true)"
        if netserver -p 12865 >/dev/null 2>&1; then
          sleep 0.3
          for p in $(pgrep -x netserver || true); do
            case " $before " in *" $p "*) ;; *) echo "$p netserver 12865" >> "$PIDFILE.new"; started+=("netserver:12865");; esac
          done
        else
          warn "netserver failed to start"
        fi
      fi
    else
      warn "netperf/netserver not installed; rr tests will be unavailable"
    fi
    mv "$PIDFILE.new" "$PIDFILE"
    fix_owner "$RUN"
    log "servers: $(tr '\n' ';' < "$PIDFILE")"
    echo "RESULT $(json_obj ok:=true mode=server started:="$(printf '%s\n' "${started[@]:-}" | python3 -c 'import json,sys; print(json.dumps([l for l in sys.stdin.read().split() if l]))')" pids="$(tr '\n' ';' < "$PIDFILE")" pidfile="$PIDFILE")"
    ;;
  # ---------------------------------------------------------------------------------
  stop)
    killed=()
    if [ -s "$PIDFILE" ]; then
      while read -r p what port; do
        [ -n "$p" ] && [ -d "/proc/$p" ] || continue
        # Only kill what we recorded: verify the comm still matches.
        comm="$(cat "/proc/$p/comm" 2>/dev/null || true)"
        if [ "$comm" != "$what" ]; then warn "pid $p is now '$comm', not $what; left alone"
        elif kill "$p" 2>/dev/null; then killed+=("$p:$what:$port")
        else warn "kill $p failed"; fi
      done < "$PIDFILE"
      rm -f "$PIDFILE"
    else
      log "no $PIDFILE; nothing to stop"
    fi
    rm -f "$RUN"/iperf3-*.pid
    echo "RESULT $(json_obj ok:=true mode=stop killed:="$(printf '%s\n' "${killed[@]:-}" | python3 -c 'import json,sys; print(json.dumps([l for l in sys.stdin.read().split() if l]))')")"
    ;;
  # ---------------------------------------------------------------------------------
  client)
    need_peer_name
    command -v iperf3 >/dev/null 2>&1 || die "iperf3 not installed"
    rev=(); [ "$REVERSE" -eq 1 ] && rev=(-R)
    pids=(); files=()
    for i in $(seq 0 $((PROCS - 1))); do
      port=$((PORT + i))
      if [ "$PROCS" -eq 1 ]; then f="$NET/$NAME.json"; else f="$NET/$NAME-p$((i + 1)).json"; fi
      files+=("$f")
      iperf3 -c "$PEER" -p "$port" -P "$STREAMS" -t "$SECS" -J "${rev[@]}" > "$f" 2>"$f.err" &
      pids+=($!)
    done
    rc=0
    for p in "${pids[@]}"; do wait "$p" || rc=$?; done
    SUMMARY="$(python3 - "$NAME" "$PEER" "$STREAMS" "$SECS" "$REVERSE" "${files[@]}" <<'PY'
import json, sys
name, peer, streams, secs, reverse = sys.argv[1:6]
files = sys.argv[6:]
total, per, errors = 0.0, [], []
for f in files:
    try:
        d = json.load(open(f))
    except (OSError, ValueError) as e:
        errors.append('%s: %s' % (f, e)); continue
    if d.get('error'):
        errors.append('%s: %s' % (f, d['error'])); continue
    end = d.get('end', {})
    s = end.get('sum_received') or end.get('sum_sent') or {}
    bps = s.get('bits_per_second', 0.0)
    total += bps
    per.append({'file': f, 'gbps': round(bps / 1e9, 3),
                'retransmits': end.get('sum_sent', {}).get('retransmits'),
                'sender_cpu_pct': end.get('cpu_utilization_percent', {}).get('host_total'),
                'receiver_cpu_pct': end.get('cpu_utilization_percent', {}).get('remote_total')})
out = {'name': name, 'peer': peer, 'streams': int(streams), 'secs': int(secs), 'reverse': reverse == '1',
       'procs': len(files), 'gbps': round(total / 1e9, 3), 'per_proc': per, 'errors': errors}
if len(files) > 1:
    import os
    with open(os.path.join(os.path.dirname(files[0]), name + '-summary.json'), 'w') as fh:
        json.dump(out, fh, indent=2); fh.write('\n')
print(json.dumps(out, separators=(',', ':')))
PY
)"
    for f in "${files[@]}"; do [ -s "$f.err" ] || rm -f "$f.err"; done
    fix_owner "$NET"
    gbps="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["gbps"])' "$SUMMARY")"
    nerr="$(python3 -c 'import json,sys; print(len(json.loads(sys.argv[1])["errors"]))' "$SUMMARY")"
    log "$NAME: $gbps Gbit/s ($PROCS proc x $STREAMS streams, ${SECS}s$([ "$REVERSE" -eq 1 ] && echo ', reverse'))"
    ok=true; { [ "$rc" -eq 0 ] && [ "$nerr" -eq 0 ]; } || ok=false
    echo "RESULT $(json_obj ok:="$ok" mode=client summary:="$SUMMARY")"
    [ "$ok" = true ]
    ;;
  # ---------------------------------------------------------------------------------
  rr)
    need_peer_name
    out="$NET/$NAME-rr.txt"
    if ! command -v netperf >/dev/null 2>&1; then
      warn "netperf not installed; TCP_RR skipped"
      echo "netperf not installed" > "$out"
      echo "RESULT $(json_obj ok:=true mode=rr skipped:=true reason="netperf not installed" file="$out")"
      exit 0
    fi
    rc=0
    netperf -H "$PEER" -t TCP_RR -l "$SECS" -- -o min_latency,mean_latency,p50_latency,p90_latency,p99_latency,max_latency,transaction_rate \
      > "$out" 2>&1 || rc=$?
    fix_owner "$NET"
    if [ "$rc" -ne 0 ]; then
      warn "netperf failed (rc=$rc): $(tail -1 "$out")"
      echo "RESULT $(json_obj ok:=false mode=rr rc:="$rc" file="$out")"
      exit 1
    fi
    # Last line is the CSV values row (header row precedes it).
    STATS="$(python3 - "$out" <<'PY'
import json, sys
lines = [l.strip() for l in open(sys.argv[1]) if l.strip()]
keys = ['min_latency_us', 'mean_latency_us', 'p50_latency_us', 'p90_latency_us', 'p99_latency_us', 'max_latency_us', 'transactions_per_sec']
vals = lines[-1].split(',') if lines else []
out = {}
for k, v in zip(keys, vals):
    try:
        out[k] = float(v)
    except ValueError:
        out[k] = v
print(json.dumps(out, separators=(',', ':')))
PY
)"
    log "$NAME TCP_RR: $STATS"
    echo "RESULT $(json_obj ok:=true mode=rr peer="$PEER" secs:="$SECS" stats:="$STATS" file="$out")"
    ;;
  # ---------------------------------------------------------------------------------
  ping)
    need_peer_name
    out="$NET/$NAME-ping.txt"
    interval=0.01
    if ! ping -c "$COUNT" -i "$interval" -q "$PEER" > "$out" 2>&1; then
      if grep -qi 'minimal interval\|cannot flood\|Operation not permitted' "$out"; then
        interval=0.2
        warn "sub-200ms ping interval needs root; using -i 0.2"
        ping -c "$COUNT" -i "$interval" -q "$PEER" > "$out" 2>&1 || { fix_owner "$NET"; die "ping failed: $(tail -1 "$out")"; }
      else
        fix_owner "$NET"; die "ping failed: $(tail -1 "$out")"
      fi
    fi
    fix_owner "$NET"
    STATS="$(python3 - "$out" "$interval" <<'PY'
import json, re, sys
t = open(sys.argv[1]).read()
o = {'interval_s': float(sys.argv[2])}
m = re.search(r'(\d+) packets transmitted, (\d+) received.*?([\d.]+)% packet loss', t)
if m:
    o.update(transmitted=int(m.group(1)), received=int(m.group(2)), loss_pct=float(m.group(3)))
m = re.search(r'= ([\d.]+)/([\d.]+)/([\d.]+)/([\d.]+) ms', t)
if m:
    o.update(min_ms=float(m.group(1)), avg_ms=float(m.group(2)), max_ms=float(m.group(3)), mdev_ms=float(m.group(4)))
print(json.dumps(o, separators=(',', ':')))
PY
)"
    log "$NAME ping: $STATS"
    echo "RESULT $(json_obj ok:=true mode=ping peer="$PEER" stats:="$STATS" file="$out")"
    ;;
esac

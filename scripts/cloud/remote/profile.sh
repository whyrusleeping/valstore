#!/usr/bin/env bash
#
# profile.sh — capture a SECS-second CPU profile of one process (default the
# local `valsd node`) or of the whole system, into $ROOT/out/prof/NAME/.
#
# Usage (root):
#   sudo -E env VALS_CLOUD_ROOT=/home/ubuntu/vals bash $ROOT/cloud/remote/profile.sh NAME SECS [PROC_REGEX]
#   ... profile.sh NAME SECS --system          # perf record -a -g (frame pointers, 199 Hz)
#   [--root DIR]  override $VALS_CLOUD_ROOT (default $HOME/vals)
#
# PROC_REGEX (default 'valsd node') is matched against full command lines
# (pgrep -f); the newest matching process wins; this script and its ancestors
# are excluded.
#
# Captured concurrently over the window:
#   perfstat-system.txt   perf stat -a (cycles, instructions, cache, branch,
#                         ctx switches, migrations, page faults; unsupported
#                         events dropped, default set as last resort)
#   perfstat-proc.txt     perf stat -p PID (same events)            [process mode]
#   perf record           -F 499 -g --call-graph dwarf,16384 -p PID [process mode]
#                         -F 199 -a -g                               [--system]
#   top-self.txt          perf report --no-children (>=0.5%)
#   top-children.txt      perf report --children (>=2%)
#   by-thread.txt         perf report --sort comm,dso
#   stacks.folded + flamegraph.svg   via $ROOT/FlameGraph (if present)
#   threads.tsv           per-thread cores_used over the window + last cpu [process mode]
#   numa.txt              /proc/PID/numa_maps bytes per node, numastat -p   [process mode]
#   irq-delta.txt         top 30 /proc/interrupts deltas
#   meta.json             pid, cmdline, window, sample count, report time
# perf.data is deleted after reporting unless KEEP_PERF_DATA=1.
#
# Knobs: PERF_FREQ (499), PERF_DWARF_SIZE (16384), PERF_MAX_MB (4096: the
# sample frequency is lowered to keep the estimated perf.data under this),
# PERF_REPORT_TIMEOUT (900 s per perf report/script invocation).
#
# Prints one machine-readable line at the end:  RESULT {...json...}
set -euo pipefail

log()  { printf '[profile] %s\n' "$*"; }
warn() { printf '[profile] WARN: %s\n' "$*" >&2; WARNINGS=$((WARNINGS + 1)); }
die()  { [ -z "${OUT:-}" ] || fix_owner "$OUT"; printf '[profile] ERROR: %s\n' "$*" >&2; echo "RESULT {\"ok\":false,\"error\":$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$*")}"; exit 1; }
WARNINGS=0

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

NAME=""; SECS=""; REGEX="valsd node"; SYSTEM=0; ROOT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --system) SYSTEM=1; shift;;
    --root) ROOT="$2"; shift 2;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
    *) if [ -z "$NAME" ]; then NAME="$1"; elif [ -z "$SECS" ]; then SECS="$1"; else REGEX="$1"; fi; shift;;
  esac
done
[ -n "$NAME" ] && [ -n "$SECS" ] || die "usage: profile.sh NAME SECS [PROC_REGEX|--system]"
[[ "$SECS" =~ ^[0-9]+$ ]] && [ "$SECS" -gt 0 ] || die "SECS must be a positive integer"
[[ "$NAME" =~ ^[A-Za-z0-9._-]+$ ]] || die "NAME must be [A-Za-z0-9._-]+"
[ -n "$ROOT" ] || ROOT="$(default_root)"
OUT="$ROOT/out"
DIR="$OUT/prof/$NAME"
FG="$ROOT/FlameGraph"
FREQ="${PERF_FREQ:-499}"
DWARF="${PERF_DWARF_SIZE:-16384}"
MAX_MB="${PERF_MAX_MB:-4096}"
RTIMEOUT="${PERF_REPORT_TIMEOUT:-900}"
[ "$(id -u)" -eq 0 ] || die "profile.sh needs root (perf)"
command -v perf >/dev/null 2>&1 || die "perf not installed (linux-tools-$(uname -r))"
perf stat -e cycles -- true >/dev/null 2>&1 || warn "perf stat cycles probe failed: perf may be a stub for another kernel"
mkdir -p "$DIR"
if [ "$SYSTEM" -eq 1 ] && [ "$FREQ" = 499 ]; then FREQ=199; fi

# ---- PID resolution --------------------------------------------------------------
PID=""; CMDLINE=""
if [ "$SYSTEM" -eq 0 ]; then
  ancestors=" $$ $PPID "
  p=$PPID
  for _ in 1 2 3 4 5 6; do
    p="$(awk '{print $4}' "/proc/$p/stat" 2>/dev/null || echo 1)"; [ "$p" -gt 1 ] 2>/dev/null || break
    ancestors="$ancestors$p "
  done
  best=""; best_start=0
  for c in $(pgrep -f -- "$REGEX" 2>/dev/null || true); do
    case "$ancestors" in *" $c "*) continue;; esac
    cl="$(tr '\0' ' ' < "/proc/$c/cmdline" 2>/dev/null || true)"
    case "$cl" in *profile.sh*|*"pgrep "*) continue;; esac
    st="$(awk '{print $22}' "/proc/$c/stat" 2>/dev/null || echo 0)"
    if [ "$st" -ge "$best_start" ]; then best="$c"; best_start="$st"; fi
  done
  [ -n "$best" ] || die "no process matches /$REGEX/"
  PID="$best"
  CMDLINE="$(tr '\0' ' ' < "/proc/$PID/cmdline" | sed 's/ $//')"
  log "pid=$PID ($CMDLINE)"
fi

# ---- perf events -----------------------------------------------------------------
EVENTS=""
for e in cycles instructions cache-misses cache-references branch-misses context-switches cpu-migrations page-faults; do
  if perf stat -e "$e" -x, -- true >/dev/null 2>&1; then EVENTS="${EVENTS:+$EVENTS,}$e"; else warn "perf event $e unsupported; dropped"; fi
done
EV_ARGS=()
if [ -n "$EVENTS" ]; then EV_ARGS=(-e "$EVENTS"); else warn "no requested perf events supported; using perf stat defaults"; fi

# ---- size bound: lower the sample rate if the dwarf dump would be huge ------
busy_est() { # cores used by PID over 0.5 s
  python3 - "$PID" <<'PY'
import glob, os, sys, time
pid = sys.argv[1]
def ticks():
    t = 0
    for s in glob.glob('/proc/%s/task/*/stat' % pid):
        try:
            f = open(s).read().rsplit(')', 1)[1].split()
            t += int(f[11]) + int(f[12])
        except (OSError, IndexError, ValueError):
            pass
    return t
a = ticks(); time.sleep(0.5); b = ticks()
print('%.2f' % ((b - a) / os.sysconf('SC_CLK_TCK') / 0.5))
PY
}
if [ "$SYSTEM" -eq 0 ]; then
  busy="$(busy_est)"
  est_mb="$(python3 -c 'import sys; b,f,d,s=map(float,sys.argv[1:]); print(int(b*f*(d+256)*s/1e6))' "$busy" "$FREQ" "$DWARF" "$SECS")"
  if [ "$est_mb" -gt "$MAX_MB" ]; then
    newf="$(python3 -c 'import sys; f,e,m=map(float,sys.argv[1:]); print(max(29,int(f*m/e)))' "$FREQ" "$est_mb" "$MAX_MB")"
    warn "estimated perf.data ${est_mb} MB at ${FREQ} Hz (${busy} busy cores, dwarf ${DWARF}); lowering to ${newf} Hz (PERF_MAX_MB=$MAX_MB)"
    FREQ="$newf"
  fi
  log "perf record -F $FREQ --call-graph dwarf,$DWARF (est. ${est_mb} MB, ${busy} busy cores)"
fi

# ---- snapshots before ----------------------------------------------------------------
snap_threads() { # snap_threads <file>
  local t
  for t in /proc/"$PID"/task/*/stat; do cat "$t" 2>/dev/null || true; done > "$1"
}
cat /proc/interrupts > "$DIR/interrupts.before" 2>/dev/null || true
if [ "$SYSTEM" -eq 0 ]; then snap_threads "$DIR/threads.before"; fi
T0="$(date +%s.%N)"

# ---- concurrent capture --------------------------------------------------------------
PIDS=()
perf stat -a "${EV_ARGS[@]}" -o "$DIR/perfstat-system.txt" -- sleep "$SECS" >/dev/null 2>&1 &
PIDS+=($!)
if [ "$SYSTEM" -eq 0 ]; then
  perf stat -p "$PID" "${EV_ARGS[@]}" -o "$DIR/perfstat-proc.txt" -- sleep "$SECS" >/dev/null 2>&1 &
  PIDS+=($!)
  perf record -F "$FREQ" -g --call-graph "dwarf,$DWARF" -p "$PID" -o "$DIR/perf.data" -- sleep "$SECS" >"$DIR/perf-record.log" 2>&1 &
  PIDS+=($!)
else
  perf record -F "$FREQ" -a -g -o "$DIR/perf.data" -- sleep "$SECS" >"$DIR/perf-record.log" 2>&1 &
  PIDS+=($!)
fi
rc_record=0
for p in "${PIDS[@]}"; do wait "$p" || rc_record=$?; done
[ "$rc_record" -eq 0 ] || warn "a perf capture exited non-zero (see $DIR/perf-record.log)"
T1="$(date +%s.%N)"

# ---- snapshots after ---------------------------------------------------------------------
cat /proc/interrupts > "$DIR/interrupts.after" 2>/dev/null || true
if [ "$SYSTEM" -eq 0 ] && [ -d "/proc/$PID" ]; then
  snap_threads "$DIR/threads.after"
  cat "/proc/$PID/numa_maps" > "$DIR/numa_maps.raw" 2>/dev/null || true
fi

python3 - "$DIR" "$PID" "$T0" "$T1" <<'PY'
import os, subprocess, sys
d, pid, t0, t1 = sys.argv[1], sys.argv[2], float(sys.argv[3]), float(sys.argv[4])
win = max(t1 - t0, 0.001)
clk = os.sysconf('SC_CLK_TCK')

def parse_threads(path):
    out = {}
    try:
        for line in open(path):
            try:
                head, tail = line.rsplit(')', 1)
                tid = int(head.split('(', 1)[0])
                comm = head.split('(', 1)[1]
                f = tail.split()
                out[tid] = (comm, int(f[11]) + int(f[12]), int(f[36]))
            except (ValueError, IndexError):
                pass
    except OSError:
        pass
    return out

if pid:
    a, b = parse_threads(d + '/threads.before'), parse_threads(d + '/threads.after')
    rows = []
    for tid, (comm, t, cpu) in b.items():
        ta = a.get(tid, (comm, 0, cpu))[1]
        rows.append((tid, comm, (t - ta) / clk / win, cpu))
    rows.sort(key=lambda r: -r[2])
    with open(d + '/threads.tsv', 'w') as f:
        f.write('tid\tthread\tcores_used\tlast_cpu\n')
        for tid, comm, cores, cpu in rows:
            f.write('%d\t%s\t%.3f\t%d\n' % (tid, comm, cores, cpu))
        f.write('# window=%.2fs total_cores=%.2f threads=%d\n' % (win, sum(r[2] for r in rows), len(rows)))
    # numa_maps: N<node>=<pages> per mapping, kernelpagesize_kB per mapping
    per_node = {}
    try:
        for line in open(d + '/numa_maps.raw'):
            f = line.split()
            kb = 4
            for tok in f:
                if tok.startswith('kernelpagesize_kB='):
                    kb = int(tok.split('=')[1])
            for tok in f:
                if tok.startswith('N') and '=' in tok and tok[1:].split('=')[0].isdigit():
                    n, pages = tok[1:].split('=')
                    per_node[int(n)] = per_node.get(int(n), 0) + int(pages) * kb * 1024
    except OSError:
        pass
    with open(d + '/numa.txt', 'w') as f:
        f.write('# bytes resident per NUMA node (from /proc/%s/numa_maps)\n' % pid)
        for n in sorted(per_node):
            f.write('node%d\t%d\t%.1f MiB\n' % (n, per_node[n], per_node[n] / 2**20))
        try:
            r = subprocess.run(['numastat', '-p', pid], capture_output=True, text=True, timeout=30)
            f.write('\n# numastat -p %s\n%s' % (pid, r.stdout))
        except (OSError, subprocess.TimeoutExpired):
            f.write('\n# numastat not available\n')
    try:
        os.remove(d + '/numa_maps.raw')
    except OSError:
        pass

def parse_irqs(path):
    out = {}
    try:
        lines = open(path).read().splitlines()
    except OSError:
        return out
    ncpu = len(lines[0].split()) if lines else 0
    for line in lines[1:]:
        f = line.split()
        if not f or not f[0].endswith(':'):
            continue
        name = f[0][:-1]
        counts = []
        for tok in f[1:1 + ncpu]:
            if tok.isdigit():
                counts.append(int(tok))
            else:
                break
        desc = ' '.join(f[1 + len(counts):])
        out[name] = (sum(counts), desc)
    return out

a, b = parse_irqs(d + '/interrupts.before'), parse_irqs(d + '/interrupts.after')
rows = []
for k, (tot, desc) in b.items():
    delta = tot - a.get(k, (0, ''))[0]
    if delta > 0:
        rows.append((delta, k, desc))
rows.sort(reverse=True)
with open(d + '/irq-delta.txt', 'w') as f:
    f.write('# top 30 interrupt deltas over %.2fs (sum over cpus)\n' % win)
    f.write('%-12s %-8s %s\n' % ('delta', 'irq', 'description'))
    for delta, k, desc in rows[:30]:
        f.write('%-12d %-8s %s\n' % (delta, k, desc))
PY

# ---- reports ------------------------------------------------------------------------------
SAMPLES=0; REPORT_SECS=0; FLAME=false
if [ -s "$DIR/perf.data" ]; then
  R0="$(date +%s)"
  rep() { # rep <outfile> <perf report args...>
    local out="$1"; shift
    timeout "$RTIMEOUT" perf report -i "$DIR/perf.data" --stdio "$@" > "$out" 2>"$out.err" || warn "perf report for $(basename "$out") failed/timed out"
    [ -s "$out.err" ] || rm -f "$out.err"
  }
  rep "$DIR/top-self.txt" --no-children -g none --percent-limit 0.5 --sort dso,sym
  rep "$DIR/top-children.txt" --children -g none --percent-limit 2 --sort sym
  rep "$DIR/by-thread.txt" --no-children -g none --sort comm,dso
  SAMPLES="$(grep -m1 -oE '^# Samples: [0-9.]+[KMG]? ' "$DIR/top-self.txt" 2>/dev/null | awk '{print $3}' || true)"
  if [ -x "$FG/stackcollapse-perf.pl" ] && [ -x "$FG/flamegraph.pl" ]; then
    if timeout "$RTIMEOUT" perf script -i "$DIR/perf.data" 2>"$DIR/perf-script.err" | "$FG/stackcollapse-perf.pl" > "$DIR/stacks.folded" 2>/dev/null && [ -s "$DIR/stacks.folded" ]; then
      if "$FG/flamegraph.pl" --title "$NAME ${CMDLINE:-system} ${SECS}s" "$DIR/stacks.folded" > "$DIR/flamegraph.svg" 2>/dev/null; then FLAME=true; else warn "flamegraph.pl failed"; fi
    else
      warn "perf script/stackcollapse failed (no folded stacks)"
    fi
    [ -s "$DIR/perf-script.err" ] || rm -f "$DIR/perf-script.err"
  else
    warn "FlameGraph not found at $FG; no folded stacks/svg"
  fi
  REPORT_SECS=$(( $(date +%s) - R0 ))
  [ "$REPORT_SECS" -le 60 ] || warn "perf reporting took ${REPORT_SECS}s"
  if [ "${KEEP_PERF_DATA:-0}" = 1 ]; then log "keeping $DIR/perf.data ($(du -h "$DIR/perf.data" | cut -f1))"; else rm -f "$DIR/perf.data"; fi
else
  warn "no perf.data produced"
fi

# ---- console summary -----------------------------------------------------------------------
echo "[profile] === perf stat (${PID:+pid $PID}${PID:-system-wide}) ==="
grep -E 'insn per cycle|instructions|cycles|cache-misses|context-switches|task-clock' "$DIR/perfstat-proc.txt" "$DIR/perfstat-system.txt" 2>/dev/null | sed 's/^/  /' | head -16 || true
if [ -s "$DIR/threads.tsv" ]; then echo "[profile] === busiest threads ==="; head -8 "$DIR/threads.tsv" | sed 's/^/  /'; fi
echo "[profile] === top self ==="
grep -vE '^#|^$' "$DIR/top-self.txt" 2>/dev/null | head -12 | sed 's/^/  /' || true

json_obj ok:=true name="$NAME" pid:="${PID:-null}" cmdline="$CMDLINE" system:="$([ "$SYSTEM" -eq 1 ] && echo true || echo false)" \
  secs:="$SECS" window_secs:="$(python3 -c 'import sys; print(round(float(sys.argv[2])-float(sys.argv[1]),2))' "$T0" "$T1")" \
  freq:="$FREQ" dwarf:="$DWARF" events="$EVENTS" samples="$SAMPLES" report_secs:="$REPORT_SECS" flamegraph:="$FLAME" \
  warnings:="$WARNINGS" dir="$DIR" | python3 -m json.tool > "$DIR/meta.json"
fix_owner "$ROOT/out/prof"
echo "RESULT $(json_obj ok:=true name="$NAME" pid:="${PID:-null}" samples="$SAMPLES" report_secs:="$REPORT_SECS" flamegraph:="$FLAME" warnings:="$WARNINGS" dir="$DIR")"

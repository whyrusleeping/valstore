#!/usr/bin/env bash
#
# profile-node.sh — profile the local `valsd node` while a load runs, on an
# lab box. Captures a perf call-graph (→ folded + flamegraph SVG), perf stat
# counters (IPC / cache / context-switches), and a /metrics delta over the
# window. Run it ON the node box (e.g. via ssh) concurrently with a load driver
# (vals-bench netread/netload) pointed at the cluster.
#
#   ./scripts/profile-node.sh [duration_s] [tag] [metrics_port]
#
# Prereqs (set up once): perf (linux-tools), ~/FlameGraph clone, and relaxed
# sysctls — `sudo sysctl -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0`.
# valsd must be built un-stripped (default). Output lands in ~/vals/prof/<tag>.*
set -euo pipefail

DUR="${1:-12}"
TAG="${2:-prof}"
PORT="${3:-9464}"
FREQ="${PROFILE_FREQ:-999}"
OUT="$HOME/vals/prof"
FG="$HOME/FlameGraph"
mkdir -p "$OUT"

# The node PID (not the PD, which shares the valsd binary on node 1).
PID=""
for p in $(pgrep -x valsd); do
  if tr '\0' ' ' < "/proc/$p/cmdline" | grep -q 'valsd node -c'; then PID=$p; break; fi
done
[ -n "$PID" ] || { echo "no 'valsd node' process found" >&2; exit 1; }
echo "[profile] node pid=$PID  dur=${DUR}s  freq=${FREQ}Hz  tag=$TAG"

metrics() { curl -s --max-time 5 "http://127.0.0.1:$PORT/metrics" 2>/dev/null; }
metrics > "$OUT/$TAG.metrics.before"

# perf stat (counters) alongside the call-graph record, both on the node pid.
sudo perf stat -p "$PID" -o "$OUT/$TAG.perfstat" -- sleep "$DUR" 2>/dev/null &
STAT=$!
sudo perf record -F "$FREQ" --call-graph dwarf -p "$PID" -o "$OUT/$TAG.data" -- sleep "$DUR" 2>/dev/null
wait "$STAT" 2>/dev/null || true
metrics > "$OUT/$TAG.metrics.after"

# Top self-time symbols (perf.data stays root-owned; process under sudo).
sudo perf report -i "$OUT/$TAG.data" --stdio --percent-limit 1 --no-children 2>/dev/null \
  | grep -vE '^#|^$' | head -30 > "$OUT/$TAG.top.txt" || true

# Folded stacks + flamegraph SVG.
if [ -x "$FG/stackcollapse-perf.pl" ]; then
  sudo perf script -i "$OUT/$TAG.data" 2>/dev/null \
    | "$FG/stackcollapse-perf.pl" > "$OUT/$TAG.folded" || true
  [ -s "$OUT/$TAG.folded" ] && "$FG/flamegraph.pl" --title "valsd node — $TAG" \
    "$OUT/$TAG.folded" > "$OUT/$TAG.svg" || true
  sudo chown "$USER" "$OUT/$TAG.folded" 2>/dev/null || true
fi

# Compact console summary: metric deltas over the window + top self-time.
echo "[profile] === /metrics delta over window ==="
join -j1 \
  <(awk '/^vals_/{print $1, $2}' "$OUT/$TAG.metrics.before" | sort) \
  <(awk '/^vals_/{print $1, $2}' "$OUT/$TAG.metrics.after"  | sort) 2>/dev/null \
  | awk '{d=$3-$2; if (d!=0) printf "  %-40s +%s\n", $1, d}' | head -40
echo "[profile] === perf stat ==="
grep -E 'instructions|cycles|insn per|context-switches|cache-misses|task-clock|branch-misses' \
  "$OUT/$TAG.perfstat" 2>/dev/null | sed 's/^/  /' || true
echo "[profile] === top self-time symbols ==="
sed 's/^/  /' "$OUT/$TAG.top.txt" 2>/dev/null | head -18
echo "[profile] artifacts in $OUT/$TAG.{svg,folded,top.txt,perfstat,data}"

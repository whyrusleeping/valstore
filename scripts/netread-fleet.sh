#!/usr/bin/env bash
# Drive prepopulated peers with concurrent netread clients and sum valid results.
# PEERS='1@ip:port;2@ip:port' ./scripts/netread-fleet.sh <nprocs> <coords/proc> <secs> [n] [vsize] [conc] [rf]
set -euo pipefail
NPROC="${1:-4}"; COORDS="${2:-12}"; SECS="${3:-12}"
N="${4:-2000000}"; VSIZE="${5:-100}"; CONC="${6:-64}"; RF="${7:-2}"
BIN="${BIN:-./target/release/vals-bench}"
: "${PEERS:?set PEERS to the sharded peer string}"
for value in "$NPROC" "$COORDS" "$SECS" "$N" "$VSIZE" "$CONC" "$RF"; do
  if [[ ! "$value" =~ ^[1-9][0-9]{0,8}$ ]]; then
    echo "[fleet] arguments must be positive integers: $value" >&2
    exit 2
  fi
done

TMP="$(mktemp -d)"
pids=()
cleanup() {
  local rc=$?
  trap - EXIT
  if (( rc == 0 )); then
    rm -rf "$TMP"
  else
    echo "[fleet] FAILED; client logs retained at $TMP" >&2
  fi
}
interrupt() {
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait || true
  exit 130
}
trap cleanup EXIT
trap interrupt INT TERM

echo "[fleet] $NPROC procs x $COORDS coords, ${SECS}s sustained reads, n=$N vsize=$VSIZE conc=$CONC rf=$RF"
for ((i=1; i<=NPROC; i++)); do
  VALS_NETREAD_SECS="$SECS" "$BIN" x netread "$N" "$VSIZE" "$PEERS" "$CONC" "$RF" "$COORDS" \
    > "$TMP/p$i.log" 2>&1 &
  pids+=("$!")
done
failed=0
for ((i=0; i<NPROC; i++)); do
  if wait "${pids[i]}"; then
    :
  else
    rc=$?
    echo "[fleet] proc$((i+1)) exited $rc" >&2
    failed=1
  fi
done
pids=()

total=0; errs=0; misses=0; mismatches=0
pattern='strong[[:space:]]*:[[:space:]]*([0-9]{1,18}) (ops|keys)/s.*,[[:space:]]*([0-9]{1,18}) errors,[[:space:]]*([0-9]{1,18}) misses,[[:space:]]*([0-9]{1,18}) mismatches\)'
for ((i=1; i<=NPROC; i++)); do
  mapfile -t lines < <(grep -E 'read  R=' "$TMP/p$i.log" || true)
  if (( ${#lines[@]} != 1 )) || [[ ! "${lines[0]}" =~ $pattern ]]; then
    echo "[fleet] proc$i: missing, duplicate, or malformed result" >&2
    failed=1
    continue
  fi
  ops=$((10#${BASH_REMATCH[1]})); e=$((10#${BASH_REMATCH[3]}))
  m=$((10#${BASH_REMATCH[4]})); bad=$((10#${BASH_REMATCH[5]}))
  echo "  proc$i: $ops ops/s  ($e errors, $m misses, $bad mismatches)"
  total=$((total + ops)); errs=$((errs + e))
  misses=$((misses + m)); mismatches=$((mismatches + bad))
  if (( ops == 0 || e != 0 || m != 0 || bad != 0 )); then failed=1; fi
done
if (( failed )); then exit 1; fi
echo "[fleet] TOTAL: $total ops/s across $NPROC procs  ($errs client errors, $misses misses, $mismatches mismatches)"

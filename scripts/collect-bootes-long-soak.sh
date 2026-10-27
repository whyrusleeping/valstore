#!/bin/sh
set -eu
RUN=${1:?run directory required}
START=$(cat "$RUN/start.epoch")
PID=$(cat "$RUN/BOOTES_PID")
get_metric() {
  name=$1 file=$2
  awk -v n="$name" '$1 == n {print $2; found=1; exit} END {if (!found) print 0}' "$file"
}

# A half-dead host accepts the TCP connection but never completes the SSH banner
# exchange, so `ConnectTimeout` (which only bounds the *connect*) does not fire and
# the client hangs forever. On 2026-08-05 that stalled this collector for ~10h
# against a kernel-wedged lab-2 (task 0131) — it stopped sampling instead of
# reporting the outage. Bound the whole command, not just the connect.
SSH="timeout -k 5 20 ssh -o BatchMode=yes -o ConnectTimeout=5"

# Stall watchdog: a node whose vals_records_written stops advancing while the
# producer is still running is the earliest cluster-side symptom of exactly that
# failure. Shout on stderr (the collector log) and drop a marker file so a human
# or a wrapper can notice without reading the timeseries.
prev_total=""; stall=0
alert() {
  printf '%s ALERT: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >&2
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >> "$RUN/ALERTS"
}

while kill -0 "$PID" 2>/dev/null; do
  now=$(date -u +%s)
  stamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  bm=$(mktemp)
  if ! curl -sf --max-time 10 http://127.0.0.1:9091/metrics > "$bm"; then rm -f "$bm"; sleep 30; continue; fi
  for pair in '114 10.0.0.114' '18 10.0.0.18' '200 10.0.0.200'; do
    set -- $pair
    if ! curl -sf --max-time 10 "http://$2:9464/metrics" > "$RUN/.node-$1.metrics"; then
      alert "node $1 ($2): /metrics scrape failed"
    fi
  done
  rss=$(awk '/VmRSS:/ {print $2*1024}' /proc/$PID/status 2>/dev/null || echo 0)
  spill_files=$(find /tmp -maxdepth 1 -name 'indigo-stream-blocks-*' 2>/dev/null | wc -l)
  spill_bytes=$(du -cb /tmp/indigo-stream-blocks-* 2>/dev/null | awk 'END {print $1+0}')
  line="$stamp,$((now-START)),$rss,$(get_metric process_cpu_seconds_total "$bm"),$(get_metric go_goroutines "$bm"),$(get_metric go_memstats_heap_alloc_bytes "$bm"),$(awk '$1 ~ /^ingest_backfill_records_total/ {print $2; found=1} END {if(!found) print 0}' "$bm"),$(awk '$1 ~ /^backfill_bytes_processed_total/ {print $2; found=1} END {if(!found) print 0}' "$bm"),$(awk '$1 ~ /^backfill_jobs_processed_total/ {print $2; found=1} END {if(!found) print 0}' "$bm"),$spill_files,$spill_bytes"
  for h in 114 18 200; do line="$line,$(get_metric vals_records_written "$RUN/.node-$h.metrics")"; done
  for h in 114 18 200; do line="$line,$(get_metric vals_busy_rejections "$RUN/.node-$h.metrics")"; done
  for h in 114 18 200; do line="$line,$(get_metric vals_server_errors "$RUN/.node-$h.metrics")"; done
  for h in 114 18 200; do
    bytes=$($SSH user@10.0.0.$h 'systemctl show vals-node -p MemoryCurrent --value' 2>/dev/null || echo 0)
    case "$bytes" in ''|*[!0-9]*) alert "node $h: MemoryCurrent probe failed/timed out"; bytes=0;; esac
    line="$line,$bytes"
  done
  printf '%s\n' "$line" >> "$RUN/timeseries.csv"

  # Cluster-side liveness: total records written across all three nodes.
  total=0
  for h in 114 18 200; do
    total=$((total + $(get_metric vals_records_written "$RUN/.node-$h.metrics" | cut -d. -f1)))
  done
  if [ -n "$prev_total" ] && [ "$total" -le "$prev_total" ]; then
    stall=$((stall + 1))
    [ "$stall" -ge 3 ] && alert "cluster writes STALLED for ${stall}m at $total records while bootes (pid $PID) is still running — check each node's :9464 and 'journalctl -k'"
  else
    stall=0
  fi
  prev_total=$total

  cp "$bm" "$RUN/bootes-latest.metrics.txt"
  for h in 114 18 200; do cp "$RUN/.node-$h.metrics" "$RUN/node-$h-latest.metrics.txt"; done
  rm -f "$bm"
  sleep 60
done

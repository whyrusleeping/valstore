#!/bin/sh
# Sample the lab cluster while a `valsdump replay` run drives it.
#
# usage: collect-replay.sh <run-dir>
#   <run-dir>/REPLAY_PID  — pid of the valsdump process (collector exits with it)
#   <run-dir>/start.epoch — unix start time
#
# Writes one CSV line per sample to <run-dir>/timeseries.csv and keeps the latest
# raw /metrics scrape per node. Same hardening as the bootes collector: every ssh
# is wrapped in `timeout` (a half-dead host hangs after the TCP connect, task
# 0131) and a stall watchdog shouts when cluster writes stop advancing while the
# replayer is still alive.
set -eu
RUN=${1:?run directory required}
START=$(cat "$RUN/start.epoch")
PID=$(cat "$RUN/REPLAY_PID")
INTERVAL=${INTERVAL:-30}

SSH="timeout -k 5 20 ssh -o BatchMode=yes -o ConnectTimeout=5"

get() { awk -v n="$1" '$1 == n {print $2; found=1; exit} END {if (!found) print 0}' "$2"; }

alert() {
  printf '%s ALERT: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >&2
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >> "$RUN/ALERTS"
}

if [ ! -s "$RUN/timeseries.csv" ]; then
  hdr='ts,elapsed_s,replay_rss,replay_cpu_s'
  for m in records_written record_bytes_written server_errors busy_rejections \
           batch_apply_us_sum batch_apply_count batch_apply_gate_waits \
           pd_range_splits run_merges segments_freed mem_bytes; do
    for h in 114 18 200; do hdr="$hdr,${m}_$h"; done
  done
  printf '%s\n' "$hdr" >> "$RUN/timeseries.csv"
fi

prev_total=''; stall=0
while kill -0 "$PID" 2>/dev/null; do
  now=$(date -u +%s)
  stamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  for pair in '114 10.0.0.114' '18 10.0.0.18' '200 10.0.0.200'; do
    set -- $pair
    # Scrape to a temp file and only publish it as the "last good" copy on
    # success: a failed scrape must not clobber the postmortem evidence. The CSV
    # still records zeros for the failed sample, so node loss stays visible.
    if curl -sf --max-time 10 "http://$2:9464/metrics" > "$RUN/.node-$1.metrics.tmp"; then
      mv -f "$RUN/.node-$1.metrics.tmp" "$RUN/.node-$1.metrics"
      cp "$RUN/.node-$1.metrics" "$RUN/node-$1-latest.metrics.txt"
    else
      rm -f "$RUN/.node-$1.metrics.tmp"
      : > "$RUN/.node-$1.metrics"
      alert "node $1 ($2): /metrics scrape failed"
    fi
  done
  rss=$(awk '/VmRSS:/ {print $2*1024}' "/proc/$PID/status" 2>/dev/null || echo 0)
  cpu=$(awk '{print ($14+$15)/100}' "/proc/$PID/stat" 2>/dev/null || echo 0)
  line="$stamp,$((now-START)),$rss,$cpu"
  for m in vals_records_written vals_record_bytes_written vals_server_errors \
           vals_busy_rejections vals_batch_apply_latency_us_sum \
           vals_batch_apply_latency_us_count vals_batch_apply_gate_waits \
           vals_pd_range_splits vals_run_merges vals_segments_freed; do
    for h in 114 18 200; do line="$line,$(get "$m" "$RUN/.node-$h.metrics")"; done
  done
  for h in 114 18 200; do
    bytes=$($SSH "user@10.0.0.$h" 'systemctl show vals-node -p MemoryCurrent --value' 2>/dev/null || echo 0)
    case "$bytes" in ''|*[!0-9]*) alert "node $h: MemoryCurrent probe failed/timed out"; bytes=0;; esac
    line="$line,$bytes"
  done
  printf '%s\n' "$line" >> "$RUN/timeseries.csv"

  total=0
  for h in 114 18 200; do
    total=$((total + $(get vals_records_written "$RUN/.node-$h.metrics" | cut -d. -f1)))
  done
  if [ -n "$prev_total" ] && [ "$total" -le "$prev_total" ]; then
    stall=$((stall + 1))
    [ "$stall" -ge 3 ] && alert "cluster writes STALLED for $stall samples at $total records while valsdump (pid $PID) is still running"
  else
    stall=0
  fi
  prev_total=$total

  sleep "$INTERVAL"
done

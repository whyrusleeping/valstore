#!/bin/sh
set -eu

write_metrics() {
  dir=${MOCK_METRICS_DIR:?}
  counter_file=$dir/counter
  counter=$(cat "$counter_file" 2>/dev/null || echo 10)
  merges=$(cat "$dir/merges" 2>/dev/null || echo 10)
  node3=$(cat "$dir/node3-profile" 2>/dev/null || echo capacity)
  dirty=${MOCK_DIRTY:-0}
  server_errors=$(cat "$dir/server-errors" 2>/dev/null || echo 0)
  node=1
  while [ "$node" -le 3 ]; do
    profile=capacity
    [ "$node" -eq 2 ] && profile=latency
    [ "$node" -eq 3 ] && profile=$node3
    if [ "${MOCK_PROFILE_BINDING_WRONG:-0}" = 1 ] && [ "$node" -eq 2 ]; then profile=capacity; fi
    blind=0; directory=0
    if [ "$profile" = latency ]; then blind=1; directory=1; fi
    if [ "${MOCK_DEGRADED:-0}" = 1 ] && [ "$node" -eq 2 ]; then blind=0; fi
    {
      echo "vals_ae_dirty_buckets $dirty"
      echo "vals_ae_dirty_oldest_ms $dirty"
      echo "vals_ae_sync_errors 0"
      echo "vals_ae_dirty_rebuild_retries 0"
      echo "vals_ae_dirty_rebuild_errors 0"
      echo "vals_used_segments 20"
      echo "vals_free_segments 100"
      echo "vals_resident_runs 4"
      echo "vals_run_merges $merges"
      echo "vals_winner_directory_keys 100"
      echo "vals_winner_directory_bytes 6400"
      echo "vals_profile_info{profile=\"$profile\"} 1"
      echo "vals_blind_append_enabled $blind"
      echo "vals_winner_directory_enabled $directory"
      [ "${MOCK_MISSING_METRIC:-0}" = 1 ] || echo "vals_records_written $counter"
      echo "vals_region_exhausted_rejections $(cat "$dir/region-exhausted" 2>/dev/null || echo 0)"
      if [ "${MOCK_NONNUMERIC_COUNTER:-0}" = 1 ]; then
        echo "vals_server_errors unavailable"
      else
        echo "vals_server_errors $server_errors"
      fi
      echo "vals_apply_run_reads 0"
      echo "vals_batch_apply_run_reads 0"
      echo "vals_apply_probe_runs 0"
      echo "vals_apply_probe_blocks 0"
    } > "$dir/node$node.prom"
    node=$((node + 1))
  done
}

case "${SOAK_EVENT:-}" in
  init)
    write_metrics
    exit 0
    ;;
  status)
    status_count_file=${MOCK_METRICS_DIR:?}/status-count
    status_count=$(cat "$status_count_file" 2>/dev/null || echo 0)
    status_count=$((status_count + 1))
    echo "$status_count" > "$status_count_file"
    [ "${MOCK_STATUS_FAIL:-0}" = 0 ] || exit 1
    if [ "${MOCK_STATUS_FAIL_AFTER:-0}" -gt 0 ] && [ "$status_count" -gt "$MOCK_STATUS_FAIL_AFTER" ]; then exit 1; fi
    echo "NODE_STATUS node=1 host=mock1 pid=101 rss_kib=10 swap_kib=0 clock_skew_ms=3 active=1"
    echo "NODE_STATUS node=2 host=mock2 pid=102 rss_kib=10 swap_kib=0 clock_skew_ms=4 active=1"
    echo "NODE_STATUS node=3 host=mock3 pid=103 rss_kib=10 swap_kib=0 clock_skew_ms=5 active=1"
    node3=$(cat "${MOCK_METRICS_DIR:?}/node3-profile" 2>/dev/null || echo capacity)
    binary_sha=${EXPECTED_VALSD_SHA256:-mockbinary}
    [ "${MOCK_WRONG_BINARY:-0}" = 0 ] || binary_sha=deadbeef
    node=1
    while [ "$node" -le 3 ]; do
      profile=capacity
      [ "$node" -eq 2 ] && profile=latency
      [ "$node" -eq 3 ] && profile=$node3
      conf=${MOCK_CONFIG_DIR:?}/node$node.conf
      [ "$node" -ne 3 ] || conf=${MOCK_CONFIG_DIR:?}/node3.$profile.conf
      if [ "${MOCK_STALE_DEPLOYED_CONFIG:-0}" = 1 ] && [ "$node" -eq 3 ]; then
        conf=${MOCK_CONFIG_DIR:?}/node3.capacity.conf
      fi
      config_sha=$(sha256sum "$conf" | awk '{print $1}')
      config_id=$(awk -F= '{ sub(/#.*/, ""); gsub(/[[:space:]]/, ""); if ($1 == "node_id") print $2 }' "$conf")
      config_profile=$(awk -F= '{ sub(/#.*/, ""); gsub(/[[:space:]]/, ""); if ($1 == "profile") print $2 }' "$conf")
      echo "NODE_CONFIG node=$node host=mock$node path=$conf config_sha256=$config_sha config_node_id=$config_id config_profile=$config_profile binary_sha256=$binary_sha"
      node=$((node + 1))
    done
    for profile in capacity latency; do
      alt_sha=$(sha256sum "${MOCK_CONFIG_DIR:?}/node3.$profile.conf" | awk '{print $1}')
      if [ "${MOCK_WRONG_ALTERNATE:-0}" = 1 ] && [ "$profile" = latency ]; then alt_sha=deadbeef; fi
      echo "NODE_ALT node=3 profile=$profile config_sha256=$alt_sha path=${MOCK_CONFIG_DIR:?}/node3.$profile.conf"
    done
    exit 0
    ;;
  zone-status)
    echo "ZONE_STATUS zone=${ZONE:-19} name=profile-test sorted=1 rf=3 declared_w=2 declared_r=2"
    echo "ZONE_STATUS_NOTE declared quorums are operator intent only"
    exit 0
    ;;
  hourly)
    counter_file=${MOCK_METRICS_DIR:?}/counter
    counter=$(cat "$counter_file" 2>/dev/null || echo 10)
    echo $((counter + 10)) > "$counter_file"
    if [ "${MOCK_ERROR_THEN_RESET:-0}" = 1 ]; then
      if [ "$SOAK_RESTART_INDEX" -eq 1 ]; then echo 1 > "$MOCK_METRICS_DIR/server-errors"; else echo 0 > "$MOCK_METRICS_DIR/server-errors"; fi
    fi
    [ "${MOCK_REGION_EXHAUSTED_AT_RESTART:-0}" != "$SOAK_RESTART_INDEX" ] || echo 1 > "$MOCK_METRICS_DIR/region-exhausted"
    if [ "${MOCK_MERGE_RESET_NO_PROGRESS:-0}" = 1 ]; then
      echo 0 > "$MOCK_METRICS_DIR/merges"
    elif [ "${MOCK_MERGE_RESET_WITH_PROGRESS:-0}" = 1 ]; then
      if [ "$SOAK_RESTART_INDEX" -eq 1 ]; then echo 1 > "$MOCK_METRICS_DIR/merges"; else
        merges=$(cat "$MOCK_METRICS_DIR/merges" 2>/dev/null || echo 1)
        echo $((merges + 3)) > "$MOCK_METRICS_DIR/merges"
      fi
    else
      merges=$(cat "$MOCK_METRICS_DIR/merges" 2>/dev/null || echo 10)
      echo $((merges + 1)) > "$MOCK_METRICS_DIR/merges"
    fi
    if [ "${MOCK_TRANSITIONS_MISSING:-0}" = 0 ]; then
      if [ "$SOAK_RESTART_INDEX" -eq 1 ]; then
        echo latency > "$MOCK_METRICS_DIR/node3-profile"
        event='from=capacity to=latency'
      elif [ "$SOAK_RESTART_INDEX" -eq 2 ]; then
        echo capacity > "$MOCK_METRICS_DIR/node3-profile"
        event='from=latency to=capacity'
      else
        event='profile=unchanged'
      fi
    else
      event='profile=unchanged'
    fi
    write_metrics
    if [ "${MOCK_RESTART_EVIDENCE_MISSING:-0}" = 0 ]; then
      echo "EVENT node=3 restart=1 $event"
    else
      echo "mock restart without evidence"
    fi
    [ "${MOCK_HOOK_FAIL_AFTER_EVENT:-0}" = 0 ] || exit 44
    exit 0
    ;;
  diag|diag-final|diag-failure)
    echo "$SOAK_EVENT ${SOAK_DIAG_TAG:?} started=${SOAK_STARTED:?}" >> "${MOCK_METRICS_DIR:?}/diag.log"
    exit 0
    ;;
  final)
    [ "${MOCK_DIRTY:-0}" = 1 ] || write_metrics
    [ "${MOCK_FINAL_HOOK_SECONDS:-0}" = 0 ] || sleep "$MOCK_FINAL_HOOK_SECONDS"
    echo "EVENT rolling=all restart=1"
    [ "${MOCK_FINAL_HOOK_FAIL_AFTER_EVENT:-0}" = 0 ] || exit 45
    exit 0
    ;;
esac

if [ -n "${VALS_PROFILECHECK_PROBE_NODE:-}" ]; then
  [ "${MOCK_PROBE_FAIL:-0}" = 0 ] || exit 43
  echo "PROFILECHECK PROBE OK node=${VALS_PROFILECHECK_PROBE_NODE} zone=${VALS_PROFILECHECK_ZONE:-0} scan_entries=1 elapsed_s=0.001"
  exit 0
fi

if [ "${VALS_PROFILECHECK_VERIFY_ONLY:-0}" = 1 ]; then
  [ "${MOCK_FINAL_FAIL:-0}" = 0 ] || exit 42
  nodes=3
  [ "${MOCK_VERIFY_NODE_MISSING:-0}" = 0 ] && nodes=3 || nodes=2
  node=1
  while [ "$node" -le "$nodes" ]; do
    echo "PROFILECHECK VERIFY NODE node=$node live=1 tombstones=1 collected=0 elapsed_s=0.001"
    node=$((node + 1))
  done
  echo "PROFILECHECK VERIFY PASS seed=mock nodes=3 start_round=1 keys=2 verified_live=1 verified_tombstones=1 collected_tombstones=${MOCK_COLLECTED_TOMBSTONES:-1} rss_kib=1000 elapsed_s=0.001"
  exit 0
fi
[ "${MOCK_DRIVER_FAIL:-0}" = 0 ] || exit 41
[ "${VALS_PROFILECHECK_ALLOW_DIRTY_RESUME:-}" = 0 ] || exit 46
: > "${VALS_PROFILECHECK_STATE:?}"
sleep "${MOCK_DRIVER_SECONDS:-4}"
echo "PROFILECHECK PASS seed=mock quorum=${MOCK_QUORUM:-2} start_round=0 rounds=1 keys=2 max_keys=100 operations=100 checkpoints=2 omitted_replica_writes=${MOCK_OMITTED_WRITES:-4} winner_tombstones=${MOCK_WINNER_TOMBSTONES:-4} resurrections=${MOCK_RESURRECTIONS:-2} cold_overwrites=3 retired_keys=2 omitted_live_cold_seed=${MOCK_OMITTED_COLD_SEED:-2} healed_live_cold_seed=${MOCK_HEALED_COLD_SEED:-2} deferred_cold_overwrites=1 pending_omissions=${MOCK_PENDING_OMISSIONS:-0} reconciled_winners=${MOCK_RECONCILED:-0} gc_grace_ms=${VALS_PROFILECHECK_GC_GRACE_SECS:-900}000 rss_kib=1000 elapsed_s=4"

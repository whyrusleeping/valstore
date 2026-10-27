#!/bin/sh
# Mixed-profile soak wrapper: plans/profile-soak.sh preflight|lifecycle-test|gate|24h
# (environment and verdict: plans/mixed-profile-soak-runbook.md).
set -eu

MODE=${1:-preflight}
PEERS=${PEERS:?set PEERS to '1@host:port;2@host:port;3@host:port'}
METRICS=${METRICS:?set METRICS to comma-separated metrics URLs}
OUT=${OUT:-bench/profile-soak-$(date -u +%Y%m%dT%H%M%SZ)}
ZONE=${ZONE:-0}
BIN=${BIN:-target/debug/vals-bench}
SCRAPE_INTERVAL=${SCRAPE_INTERVAL:-60}
NODE_RESTART_TIMEOUT=${NODE_RESTART_TIMEOUT:-240}
HOOK_TIMEOUT=${HOOK_TIMEOUT:-300}
FINAL_HOOK_TIMEOUT=${FINAL_HOOK_TIMEOUT:-$(( (NODE_RESTART_TIMEOUT + 60) * 3 ))}
CURL_TIMEOUT=${CURL_TIMEOUT:-10}
PROBE_SECS=${PROBE_SECS:-20}
REQUIRE_OMITTED_WRITES=${REQUIRE_OMITTED_WRITES:-0}
REQUIRE_COLLECTED_TOMBSTONES=${REQUIRE_COLLECTED_TOMBSTONES:-0}
REQUIRE_HEALED_OMISSIONS=${REQUIRE_HEALED_OMISSIONS:-0}
GATE=0
AE_TIMEOUT=${AE_TIMEOUT:-900}
AE_POLL_INTERVAL=${AE_POLL_INTERVAL:-10}
STATE=${STATE:-$OUT/reference.state}
LOG=$OUT/profilecheck.log
VERIFY_LOG=$OUT/final-verify.log
workload=
sleeper=
snapshot_sequence=0

case "$MODE" in
  preflight)
    DURATION=${DURATION:-8}
    ROUNDS=${ROUNDS:-96}
    SEED_KEYS=${SEED_KEYS:-256}
    WORKLOAD_KEYS=${WORKLOAD_KEYS:-64}
    LONG_GATE=0
    ;;
  24h|gate)
    if [ "$MODE" = 24h ]; then
      DURATION=86400
    else
      DURATION=${DURATION:-7200}
      RESTART_INTERVAL=${RESTART_INTERVAL:-600}
    fi
    ROUNDS=${ROUNDS:-1}
    SEED_KEYS=${SEED_KEYS:-1000000}
    MIN_SEED_KEYS=${MIN_SEED_KEYS:-1000000}
    WORKLOAD_KEYS=${WORKLOAD_KEYS:-25000}
    LONG_GATE=1
    GATE=1
    REQUIRE_OMITTED_WRITES=1
    REQUIRE_COLLECTED_TOMBSTONES=1
    REQUIRE_HEALED_OMISSIONS=1
    GC_PROBE_SECS=${GC_PROBE_SECS:-300}
    GC_MAX_HELD_PCT=${GC_MAX_HELD_PCT:-10}
    DIAG_INTERVAL=${DIAG_INTERVAL:-1800}
    ;;
  lifecycle-test)
    [ "${PROFILE_SOAK_ALLOW_TEST_MODE:-0}" = 1 ] || { echo "lifecycle-test requires PROFILE_SOAK_ALLOW_TEST_MODE=1" >&2; exit 2; }
    DURATION=${DURATION:-6}
    ROUNDS=${ROUNDS:-1}
    SEED_KEYS=${SEED_KEYS:-16}
    WORKLOAD_KEYS=${WORKLOAD_KEYS:-16}
    LONG_GATE=1
    ;;
  *) echo "usage: $0 preflight|gate|24h" >&2; exit 2 ;;
esac
RESTART_INTERVAL=${RESTART_INTERVAL:-3600}
if [ "$GATE" -eq 1 ]; then
  EXPECTED_RESTARTS=${EXPECTED_RESTARTS:-$(( DURATION / RESTART_INTERVAL - 1 ))}
else
  EXPECTED_RESTARTS=${EXPECTED_RESTARTS:-23}
fi
[ "$EXPECTED_RESTARTS" -ge 2 ] || [ "$GATE" -eq 0 ] \
  || { echo "the gate needs at least 2 restarts (both node-3 profile flips): raise DURATION or lower RESTART_INTERVAL" >&2; exit 2; }
GC_PROBE_SECS=${GC_PROBE_SECS:-0}
GC_MAX_HELD_PCT=${GC_MAX_HELD_PCT:-100}
DIAG_INTERVAL=${DIAG_INTERVAL:-0}
TREND_SAMPLES=${TREND_SAMPLES:-$(( DURATION / SCRAPE_INTERVAL / 2 < 120 ? DURATION / SCRAPE_INTERVAL / 2 : 120 ))}

if [ -n "${PROFILE_SOAK_SOURCE_ROOT:-}" ]; then
  [ "$MODE" = lifecycle-test ] && [ "${PROFILE_SOAK_ALLOW_TEST_MODE:-0}" = 1 ] \
    || { echo "PROFILE_SOAK_SOURCE_ROOT is restricted to lifecycle-test" >&2; exit 2; }
  SOURCE_ROOT=$PROFILE_SOAK_SOURCE_ROOT
else
  SOURCE_ROOT=.
fi
source_head=$(git -C "$SOURCE_ROOT" rev-parse HEAD) || { echo "cannot capture source HEAD" >&2; exit 1; }
git_dirty=$(git -C "$SOURCE_ROOT" status --porcelain=v1) || { echo "cannot capture source status" >&2; exit 1; }
git_untracked=$(git -C "$SOURCE_ROOT" ls-files --others --exclude-standard -- crates plans scripts tools docs Cargo.toml Cargo.lock rust-toolchain.toml) \
  || { echo "cannot capture untracked source list" >&2; exit 1; }
mkdir -p "$OUT/metrics" "$OUT/process" "$OUT/hooks"
: > "$OUT/coverage.log"
: > "$OUT/counter-samples.log"

run_diag() {
  [ -n "${DIAG_CMD:-}" ] || return 0
  rd_tag=$1
  rd_event=$2
  if ! timeout "${HOOK_TIMEOUT}s" env SOAK_EVENT="$rd_event" SOAK_DIAG_TAG="$rd_tag" SOAK_OUT="$OUT" \
      SOAK_STARTED="${run_started:-}" sh -c "$DIAG_CMD" > "$OUT/hooks/diag.$rd_tag.txt" 2>&1; then
    printf '%s diag_failed tag=%s\n' "$(date -u +%FT%TZ)" "$rd_tag" >> "$OUT/coverage.log"
  fi
  return 0
}

cleanup() {
  rc=$?
  trap - EXIT INT TERM HUP
  [ -z "$sleeper" ] || kill "$sleeper" 2>/dev/null || true
  if [ -n "$workload" ] && kill -0 "$workload" 2>/dev/null; then
    kill "$workload" 2>/dev/null || true
    wait "$workload" 2>/dev/null || true
  fi
  [ "$rc" -eq 0 ] || [ -z "${run_started:-}" ] || run_diag failure diag-failure
  exit "$rc"
}
trap cleanup EXIT INT TERM HUP

fail() {
  printf 'FAIL %s\n' "$*" >&2
  exit 1
}

bounded_hook() {
  name=$1
  command=$2
  event=$3
  index=$4
  limit=${5:-$HOOK_TIMEOUT}
  output=$OUT/hooks/$name.txt
  if ! timeout "${limit}s" env SOAK_EVENT="$event" SOAK_RESTART_INDEX="$index" SOAK_OUT="$OUT" \
      NODE_RESTART_TIMEOUT="$NODE_RESTART_TIMEOUT" sh -c "$command" > "$output" 2>&1; then
    cat "$output" >&2 || true
    fail "hook $name failed or exceeded ${limit}s"
  fi
  cat "$output"
}

snapshot() {
  tag=$1
  snapshot_sequence=$((snapshot_sequence + 1))
  oldifs=$IFS
  IFS=,
  node=0
  for url in $METRICS; do
    node=$((node + 1))
    dest=$OUT/metrics/$tag.node$node.prom
    attempt=1
    ok=0
    while [ "$attempt" -le 3 ]; do
      if timeout "${CURL_TIMEOUT}s" curl -fsS --max-time "$CURL_TIMEOUT" "$url" > "$dest.tmp" 2>> "$OUT/coverage.log"; then
        mv "$dest.tmp" "$dest"
        ok=1
        [ "$attempt" -eq 1 ] || printf '%s scrape_recovered tag=%s node=%s attempt=%s\n' "$(date -u +%FT%TZ)" "$tag" "$node" "$attempt" >> "$OUT/coverage.log"
        break
      fi
      printf '%s scrape_retry tag=%s node=%s attempt=%s url=%s\n' "$(date -u +%FT%TZ)" "$tag" "$node" "$attempt" "$url" >> "$OUT/coverage.log"
      rm -f "$dest.tmp"
      attempt=$((attempt + 1))
      sleep 1
    done
    [ "$ok" -eq 1 ] || {
      printf '%s scrape_gap tag=%s node=%s url=%s\n' "$(date -u +%FT%TZ)" "$tag" "$node" "$url" >> "$OUT/coverage.log"
      IFS=$oldifs
      return 1
    }
    if [ "$LONG_GATE" -eq 1 ]; then
      require_metric_file "$dest"
      for counter_metric in vals_records_written vals_run_merges vals_ae_sync_errors vals_ae_dirty_rebuild_retries vals_ae_dirty_rebuild_errors vals_region_exhausted_rejections vals_server_errors vals_apply_run_reads vals_batch_apply_run_reads vals_apply_probe_runs vals_apply_probe_blocks vals_ae_entries_applied vals_expired_tombstones_skipped 'vals_run_merges_by_kind_total{kind="lonegc"}' 'vals_run_merge_input_bytes_by_kind_total{kind="lonegc"}'; do
        counter_value=$(metric_value "$counter_metric" "$dest")
        printf 'COUNTER %s %s %s %s %s\n' "$snapshot_sequence" "$tag" "$node" "$counter_metric" "$counter_value" >> "$OUT/counter-samples.log"
      done
    fi
  done
  IFS=$oldifs
  if [ "$LONG_GATE" -eq 1 ]; then
    bounded_hook "status.$tag" "$NODE_STATUS_CMD" status 0 > "$OUT/process/$tag.txt"
    [ -s "$OUT/process/$tag.txt" ] || return 1
    status_nodes=$(awk '/^NODE_STATUS / { seen[$2]=1 } END { print length(seen) }' "$OUT/process/$tag.txt")
    [ "$status_nodes" -eq 3 ] || { echo "NODE_STATUS_CMD did not report exactly three nodes" >&2; return 1; }
  fi
}

metric_value() {
  metric=$1
  file=$2
  awk -v metric="$metric" '$1 == metric { print $2; found=1; exit } END { if (!found) print "MISSING" }' "$file"
}

counter_positive_delta() {
  wanted_metric=$1
  wanted_node=$2
  awk -v metric="$wanted_metric" -v node="$wanted_node" '
    $1 == "COUNTER" && $4 == node && $5 == metric {
      if ($6 !~ /^-?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$/) {
        printf "non-numeric %s sample %s for node %s\n", metric, $6, node > "/dev/stderr"
        exit 2
      }
      value=$6 + 0
      if (seen) {
        if (value >= previous) total += value - previous
        else total += value
      }
      previous=value
      seen=1
    }
    END {
      if (!seen) {
        printf "no %s counter samples for node %s\n", metric, node > "/dev/stderr"
        exit 1
      }
      print total + 0
    }
  ' "$OUT/counter-samples.log"
}

counter_delta() {
  cd_metric=$1
  cd_node=$2
  cd_value=$(counter_positive_delta "$cd_metric" "$cd_node") \
    || fail "restart-aware delta for $cd_metric on node $cd_node is unusable (missing or non-numeric samples)"
  printf '%s\n' "$cd_value"
}

config_values() {
  cv_file=$1
  cv_key=$2
  awk -v want="$cv_key" '
    {
      line=$0
      p=index(line, "#")
      if (p > 0) line=substr(line, 1, p - 1)
      e=index(line, "=")
      if (e == 0) next
      key=substr(line, 1, e - 1)
      value=substr(line, e + 1)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", key)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      if (key == want) { print value; found=1 }
    }
    END { exit !found }
  ' "$cv_file"
}

config_one() {
  co_file=$1
  co_key=$2
  co_value=$(config_values "$co_file" "$co_key") || fail "$co_file has no $co_key"
  [ "$(printf '%s\n' "$co_value" | wc -l)" -eq 1 ] || fail "$co_file repeats $co_key"
  printf '%s\n' "$co_value"
}

duration_secs() {
  printf '%s\n' "$1" | awk '
    /^[0-9]+ms$/ { sub(/ms$/, ""); printf "%d\n", $0 / 1000; found=1 }
    /^[0-9]+s$/ { sub(/s$/, ""); printf "%d\n", $0; found=1 }
    END { exit !found }
  '
}

port_of() {
  printf '%s\n' "$1" | awk -F: '{ print $NF }'
}

ports_of() {
  printf '%s\n' "$1" | tr ',' '\n' | awk -F: 'NF { print $NF }' | sort -n | paste -sd, -
}

require_metric_file() {
  file=$1
  required_oldifs=$IFS
  IFS=' '
  required='vals_ae_dirty_buckets vals_ae_dirty_oldest_ms vals_ae_sync_errors vals_ae_dirty_rebuild_retries vals_ae_dirty_rebuild_errors vals_used_segments vals_free_segments vals_resident_runs vals_run_merges vals_winner_directory_keys vals_winner_directory_bytes vals_profile_info vals_blind_append_enabled vals_winner_directory_enabled vals_records_written vals_region_exhausted_rejections vals_server_errors vals_apply_run_reads vals_batch_apply_run_reads vals_apply_probe_runs vals_apply_probe_blocks'
  for metric in $required; do
    awk -v metric="$metric" '$1 == metric || index($1, metric "{") == 1 { found=1 } END { exit !found }' "$file" || fail "$file missing $metric"
  done
  IFS=$required_oldifs
}

writes_advanced() {
  before=$1
  after=$2
  node=1
  while [ "$node" -le 3 ]; do
    b=$(metric_value vals_records_written "$OUT/metrics/$before.node$node.prom")
    a=$(metric_value vals_records_written "$OUT/metrics/$after.node$node.prom")
    if [ "$a" != MISSING ] && [ "$b" != MISSING ] && awk -v a="$a" -v b="$b" 'BEGIN { exit !(a > b) }'; then
      return 0
    fi
    node=$((node + 1))
  done
  return 1
}

check_counter_bounds() {
  cb_node=$1
  for cb_metric in vals_ae_sync_errors vals_ae_dirty_rebuild_errors vals_region_exhausted_rejections vals_server_errors vals_ae_dirty_rebuild_retries; do
    cb_delta=$(counter_delta "$cb_metric" "$cb_node")
    case "$cb_metric" in
      vals_ae_sync_errors) cb_max=${MAX_AE_SYNC_ERRORS_DELTA:-10000} ;;
      vals_ae_dirty_rebuild_errors) cb_max=${MAX_REBUILD_ERRORS_DELTA:-0} ;;
      vals_region_exhausted_rejections) cb_max=${MAX_REGION_EXHAUSTED_DELTA:-0} ;;
      vals_server_errors) cb_max=${MAX_SERVER_ERRORS_DELTA:-0} ;;
      vals_ae_dirty_rebuild_retries) cb_max=${MAX_REBUILD_RETRIES_DELTA:-10} ;;
    esac
    awk -v delta="$cb_delta" -v max="$cb_max" 'BEGIN { exit !(delta <= max) }' || fail "$cb_metric accumulated delta $cb_delta above bound $cb_max on node $cb_node"
  done
  if [ "$cb_node" -eq 2 ]; then
    for cb_metric in vals_apply_run_reads vals_batch_apply_run_reads vals_apply_probe_runs vals_apply_probe_blocks; do
      cb_delta=$(counter_delta "$cb_metric" 2)
      awk -v delta="$cb_delta" -v max="${MAX_LATENCY_PROBE_DELTA:-0}" 'BEGIN { exit !(delta <= max) }' || fail "$cb_metric accumulated delta $cb_delta beyond latency-profile allowance"
    done
  fi
}

# Fails when the newest TREND_SAMPLES minute samples of a node end in a run of more than half the
# window of consecutive free-segment drops, used-segment rises or resident-run rises. Returns 1 while
# fewer samples exist (unless the caller requires them).
check_space_trend() {
  st_node=$1
  st_require=$2
  st_files=$(ls "$OUT"/metrics/sample.*.node$st_node.prom 2>/dev/null | tail -n "$TREND_SAMPLES")
  st_count=$(printf '%s\n' "$st_files" | awk 'NF {n++} END {print n+0}')
  if [ "$st_count" -lt "$TREND_SAMPLES" ]; then
    [ "$st_require" -eq 0 ] || fail "node $st_node lacks the final $TREND_SAMPLES scrape samples"
    return 0
  fi
  awk -v half="$((TREND_SAMPLES / 2))" '
    $1 == "vals_free_segments" { if (have_free && $2 < free) free_bad++; else free_bad=0; free=$2; have_free=1 }
    $1 == "vals_used_segments" { if (have_used && $2 > used) used_bad++; else used_bad=0; used=$2; have_used=1 }
    $1 == "vals_resident_runs" { if (have_runs && $2 > runs) runs_bad++; else runs_bad=0; runs=$2; have_runs=1 }
    END { exit (free_bad > half || used_bad > half || runs_bad > half) }
  ' $st_files || fail "node $st_node has a persistent space/run deterioration trend over its last $TREND_SAMPLES samples"
}

# In-run checks after every minute sample of the long modes, so a failing gate stops early.
live_checks() {
  lc_tag=$1
  lc_node=1
  while [ "$lc_node" -le 3 ]; do
    lc_free=$(metric_value vals_free_segments "$OUT/metrics/$lc_tag.node$lc_node.prom")
    awk -v free="$lc_free" 'BEGIN { exit !(free > 0) }' || fail "node $lc_node has no free segments at $lc_tag"
    check_counter_bounds "$lc_node"
    [ "$GATE" -eq 0 ] || check_space_trend "$lc_node" 0
    lc_node=$((lc_node + 1))
  done
}

run_restart() {
  rr_index=$1
  rr_tag=restart.$(printf '%03d' "$rr_index")
  snapshot "$rr_tag.before" || fail "metrics/status coverage gap before restart $rr_index"
  validate_deployed_configs "$rr_tag.before"
  printf '%s restart index=%s started\n' "$(date -u +%FT%TZ)" "$rr_index" >> "$OUT/restarts.log"
  bounded_hook "$rr_tag" "$RESTART_CMD" hourly "$rr_index" >> "$OUT/restarts.log"
  cat "$OUT/hooks/$rr_tag.txt"
  printf '%s restart index=%s finished\n' "$(date -u +%FT%TZ)" "$rr_index" >> "$OUT/restarts.log"
  if awk '/^EVENT / && /node=3/ && /to=latency/ { found=1 } END { exit !found }' "$OUT/hooks/$rr_tag.txt"; then
    expected_node3_profile=latency
  elif awk '/^EVENT / && /node=3/ && /to=capacity/ { found=1 } END { exit !found }' "$OUT/hooks/$rr_tag.txt"; then
    expected_node3_profile=capacity
  fi
  snapshot "$rr_tag.after" || fail "metrics/status coverage gap after restart $rr_index"
  awk '/^EVENT / && /node=[123]/ && /restart=1/ { found=1 } END { exit !found }' "$OUT/hooks/$rr_tag.txt" || fail "restart hook $rr_index omitted EVENT node=N restart=1 evidence"
  probe_serving_nodes "$rr_tag"
  validate_deployed_configs "$rr_tag.after"
  rr_pressure_tag=$rr_tag.after
  rr_pressure_deadline=$(( $(date +%s) + ${PRESSURE_TIMEOUT:-30} ))
  while ! writes_advanced "$rr_tag.before" "$rr_pressure_tag"; do
    kill -0 "$workload" 2>/dev/null || fail "workload stopped before producing restart pressure evidence"
    [ "$(date +%s)" -lt "$rr_pressure_deadline" ] || fail "no vals_records_written increase on any serving node across restart $rr_index"
    sleep 2
    rr_pressure_tag=$rr_tag.pressure.$(date +%s)
    snapshot "$rr_pressure_tag" || fail "coverage gap while awaiting restart pressure evidence"
  done
  printf 'PRESSURE restart=%s writes_advanced=1 sample=%s\n' "$rr_index" "$rr_pressure_tag" >> "$OUT/restarts.log"
}

validate_config() {
  file=$1
  expect_node=${2:-}
  expect_profile=${3:-}
  [ -f "$file" ] || fail "missing config provenance file $file"
  cfg_node=$(config_one "$file" node_id)
  cfg_profile=$(config_one "$file" profile)
  cfg_listen=$(config_values "$file" listen | paste -sd, -)
  [ -n "$cfg_listen" ] || fail "$file has no listen"
  cfg_rf=$(config_one "$file" rf)
  cfg_grace_secs=$(duration_secs "$(config_one "$file" gc_grace)") \
    || fail "$file has an unparsable gc_grace (use an s/ms suffix)"
  [ "$cfg_grace_secs" -gt 0 ] || fail "$file must use a nonzero production gc_grace"
  [ "$cfg_rf" = 3 ] || fail "$file must set rf=3, got $cfg_rf"
  case "$cfg_profile" in
    capacity|latency) ;;
    *) fail "$file profile must be capacity or latency, got $cfg_profile" ;;
  esac
  peer_list=$(config_values "$file" peer) || fail "$file has no peer entries"
  [ "$(printf '%s\n' "$peer_list" | wc -l)" -ge 2 ] || fail "$file must name both peers"
  if cfg_format=$(config_values "$file" format); then
    case "$cfg_format" in
      *true*) fail "$file must never set format=true" ;;
    esac
  fi
  if [ -n "$expect_node" ]; then
    [ "$cfg_node" = "$expect_node" ] || fail "$file declares node_id=$cfg_node but is supplied as node $expect_node"
  fi
  if [ -n "$expect_profile" ]; then
    [ "$cfg_profile" = "$expect_profile" ] || fail "$file declares profile=$cfg_profile but the gate matrix binds node $expect_node to $expect_profile"
  fi
  cfg_metrics=$(config_values "$file" metrics || true)
  printf 'CONFIG file=%s node_id=%s profile=%s listen=%s metrics=%s gc_grace_s=%s\n' \
    "$file" "$cfg_node" "$cfg_profile" "$cfg_listen" "${cfg_metrics:-none}" "$cfg_grace_secs" >> "$OUT/config-binding.txt"
}

peer_field() {
  printf '%s\n' "$PEERS" | tr ';' '\n' | awk -v want="$1" -F@ '
    { id=$1; gsub(/[[:space:]]/, "", id); if (id == want) { print $2; found=1 } }
    END { exit !found }
  '
}

metrics_field() {
  printf '%s\n' "$METRICS" | tr ',' '\n' | awk -v want="$1" 'NR == want { print; found=1 } END { exit !found }'
}

validate_topology_binding() {
  : > "$OUT/config-binding.txt"
  : > "$OUT/expected-configs.sha256"
  : > "$OUT/expected-configs.index"
  node=1
  configs=0
  gc_grace_secs=
  oldifs=$IFS; IFS=,
  for file in $CONFIG_FILES; do
    IFS=$oldifs
    case "$node" in
      1) want_profile=capacity ;;
      2) want_profile=latency ;;
      3) want_profile=capacity ;;
    esac
    validate_config "$file" "$node" "$want_profile"
    sha256sum "$file" >> "$OUT/config.sha256"
    sha256sum "$file" >> "$OUT/expected-configs.sha256"
    printf '%s  %s node=%s profile=%s\n' "$(sha256sum "$file" | awk '{print $1}')" "$file" "$cfg_node" "$cfg_profile" >> "$OUT/expected-configs.index"
    peer_addr=$(peer_field "$node") || fail "PEERS has no entry for node $node"
    [ "$(ports_of "$peer_addr")" = "$(ports_of "$cfg_listen")" ] \
      || fail "node $node config listens on $cfg_listen but PEERS routes writes to $peer_addr"
    metrics_url=$(metrics_field "$node") || fail "METRICS has no entry $node"
    if [ -n "$cfg_metrics" ]; then
      [ "$(port_of "$cfg_metrics")" = "$(printf '%s\n' "$metrics_url" | awk -F[:/] '{ print $5 }')" ] \
        || fail "node $node config exposes metrics on $cfg_metrics but METRICS scrapes $metrics_url"
    fi
    if [ -z "$gc_grace_secs" ]; then
      gc_grace_secs=$cfg_grace_secs
    else
      [ "$cfg_grace_secs" = "$gc_grace_secs" ] \
        || fail "node configs disagree on gc_grace ($gc_grace_secs vs $cfg_grace_secs); the correctness model needs one grace"
    fi
    configs=$((configs + 1))
    node=$((node + 1))
    IFS=,
  done
  IFS=$oldifs
  [ "$configs" -eq 3 ] || fail "CONFIG_FILES must contain exactly three active node configs in node order 1,2,3"
  oldifs=$IFS; IFS=,
  for file in ${EXTRA_CONFIG_FILES:-}; do
    [ -n "$file" ] || continue
    IFS=$oldifs
    validate_config "$file" 3 latency
    [ "$cfg_grace_secs" = "$gc_grace_secs" ] || fail "$file uses a different gc_grace than the active configs"
    sha256sum "$file" >> "$OUT/config.sha256"
    sha256sum "$file" >> "$OUT/expected-configs.sha256"
    printf '%s  %s node=%s profile=%s\n' "$(sha256sum "$file" | awk '{print $1}')" "$file" "$cfg_node" "$cfg_profile" >> "$OUT/expected-configs.index"
    IFS=,
  done
  IFS=$oldifs
  GC_GRACE_SECS=$gc_grace_secs
}

probe_serving_nodes() {
  ps_tag=$1
  ps_node=1
  while [ "$ps_node" -le 3 ]; do
    ps_log=$OUT/hooks/probe.$ps_tag.node$ps_node.txt
    if ! timeout "$((PROBE_SECS + 10))s" env \
        VALS_PROFILECHECK_ZONE="$ZONE" VALS_PROFILECHECK_PROBE_NODE="$ps_node" \
        VALS_PROFILECHECK_PROBE_SECS="$PROBE_SECS" \
        VALS_PROFILECHECK_CONNECT_DEADLINE_SECS="${CONNECT_DEADLINE_SECS:-5}" \
        VALS_PROFILECHECK_RPC_DEADLINE_SECS="${RPC_DEADLINE_SECS:-15}" \
        "$BIN" x profilecheck 0 1 "$PEERS" > "$ps_log" 2>&1; then
      cat "$ps_log" >&2 || true
      fail "node $ps_node did not answer a bounded data RPC on zone $ZONE after $ps_tag"
    fi
    awk '/^PROFILECHECK PROBE OK / { ok=1 } END { exit !ok }' "$ps_log" \
      || fail "node $ps_node readiness probe after $ps_tag produced no PROBE OK evidence"
    ps_node=$((ps_node + 1))
  done
}

expected_node3_profile=capacity

validate_deployed_configs() {
  vd_tag=$1
  vd_file=$OUT/process/$vd_tag.txt
  vd_node=1
  while [ "$vd_node" -le 3 ]; do
    case "$vd_node" in
      1) vd_profile=capacity ;;
      2) vd_profile=latency ;;
      3) vd_profile=$expected_node3_profile ;;
    esac
    vd_line=$(awk -v node="node=$vd_node" '/^NODE_CONFIG / && $2 == node { print; found=1 } END { exit !found }' "$vd_file") \
      || fail "status hook reported no deployed config for node $vd_node at $vd_tag"
    vd_sha=$(printf '%s\n' "$vd_line" | awk '{ for (i = 1; i <= NF; i++) { split($i, a, "="); if (a[1] == "config_sha256") print a[2] } }')
    vd_id=$(printf '%s\n' "$vd_line" | awk '{ for (i = 1; i <= NF; i++) { split($i, a, "="); if (a[1] == "config_node_id") print a[2] } }')
    vd_prof=$(printf '%s\n' "$vd_line" | awk '{ for (i = 1; i <= NF; i++) { split($i, a, "="); if (a[1] == "config_profile") print a[2] } }')
    vd_bin=$(printf '%s\n' "$vd_line" | awk '{ for (i = 1; i <= NF; i++) { split($i, a, "="); if (a[1] == "binary_sha256") print a[2] } }')
    [ "$vd_id" = "$vd_node" ] || fail "node $vd_node runs a config declaring node_id=$vd_id at $vd_tag"
    [ "$vd_prof" = "$vd_profile" ] || fail "node $vd_node runs profile=$vd_prof but the gate expects $vd_profile at $vd_tag"
    grep -q "^$vd_sha  *" "$OUT/expected-configs.sha256" \
      || fail "node $vd_node deployed config $vd_sha at $vd_tag is not one of the validated local configs"
    grep -q "^$vd_sha  *.*node=$vd_node profile=$vd_prof\$" "$OUT/expected-configs.index" \
      || fail "node $vd_node deployed config $vd_sha does not match a validated $vd_prof config at $vd_tag"
    if [ -n "${EXPECTED_VALSD_SHA256:-}" ]; then
      [ "$vd_bin" = "$EXPECTED_VALSD_SHA256" ] \
        || fail "node $vd_node runs daemon $vd_bin, not the source-pinned $EXPECTED_VALSD_SHA256, at $vd_tag"
    fi
    printf 'DEPLOYED tag=%s node=%s profile=%s config=%s binary=%s\n' "$vd_tag" "$vd_node" "$vd_prof" "$vd_sha" "$vd_bin" >> "$OUT/deployed-configs.log"
    vd_node=$((vd_node + 1))
  done
  for vd_alt_profile in capacity latency; do
    vd_alt_count=$(awk -v profile="profile=$vd_alt_profile" '/^NODE_ALT / && $2 == "node=3" && $3 == profile { count++ } END { print count+0 }' "$vd_file")
    [ "$vd_alt_count" -eq 1 ] \
      || fail "status hook must report exactly one node-3 $vd_alt_profile alternate at $vd_tag (found $vd_alt_count)"
    vd_alt_sha=$(awk -v profile="profile=$vd_alt_profile" '/^NODE_ALT / && $2 == "node=3" && $3 == profile { for (i=1; i<=NF; i++) { split($i, a, "="); if (a[1] == "config_sha256") print a[2] } }' "$vd_file")
    grep -q "^$vd_alt_sha  *.*node=3 profile=$vd_alt_profile\$" "$OUT/expected-configs.index" \
      || fail "remote node-3 $vd_alt_profile alternate $vd_alt_sha does not match the retained validated config at $vd_tag"
  done
}

validate_running_profiles() {
  tag=$1
  node=1
  while [ "$node" -le 3 ]; do
    case "$node" in
      1) want_profile=capacity ;;
      2) want_profile=latency ;;
      3) want_profile=capacity ;;
    esac
    file=$OUT/metrics/$tag.node$node.prom
    awk -v want="$want_profile" '
      $1 == "vals_profile_info{profile=\"" want "\"}" && $2 == 1 { ok=1 }
      END { exit !ok }
    ' "$file" || fail "metrics endpoint $node does not report the bound startup profile $want_profile"
    node=$((node + 1))
  done
  awk '/^NODE_STATUS / { print $2 }' "$OUT/process/$tag.txt" | sort > "$OUT/process/$tag.nodes"
  printf 'node=1\nnode=2\nnode=3\n' | cmp -s - "$OUT/process/$tag.nodes" \
    || fail "NODE_STATUS_CMD did not report exactly nodes 1,2,3 in the expected identity binding"
}

validate_long_preconditions() {
  [ -n "${RESTART_CMD:-}" ] || fail "$MODE requires RESTART_CMD"
  [ -n "${NODE_STATUS_CMD:-}" ] || fail "$MODE requires remote NODE_STATUS_CMD"
  [ -n "${ZONE_STATUS_CMD:-}" ] || fail "$MODE requires ZONE_STATUS_CMD"
  [ -n "${CONFIG_FILES:-}" ] || fail "$MODE requires comma-separated CONFIG_FILES"
  [ "${PROTOCOL_VERSION:-}" = 2 ] || fail "$MODE is protocol-v2 only (set PROTOCOL_VERSION=2 after provenance check)"
  if [ "$GATE" -eq 1 ]; then
    [ -n "${MAX_AE_SYNC_ERRORS_DELTA:-}" ] || fail "$MODE requires an explicit MAX_AE_SYNC_ERRORS_DELTA"
    [ -n "${EXPECTED_VALSD_SHA256:-}" ] || fail "$MODE requires EXPECTED_VALSD_SHA256 pinned to the built daemon"
    [ "$REQUIRE_OMITTED_WRITES" = 1 ] || fail "$MODE cannot disable omitted-write (AE healing) coverage"
    [ "$REQUIRE_COLLECTED_TOMBSTONES" = 1 ] || fail "$MODE cannot disable tombstone-collection coverage"
    [ "$REQUIRE_HEALED_OMISSIONS" = 1 ] || fail "$MODE cannot disable omitted-winner healing coverage"
  fi
  [ "$ZONE" -ne 0 ] || fail "$MODE requires a dedicated non-default sorted test zone"
  [ "$MODE" = lifecycle-test ] || [ "$SEED_KEYS" -ge "$MIN_SEED_KEYS" ] || fail "$MODE requires at least $MIN_SEED_KEYS seed keys"
  [ "$WORKLOAD_KEYS" -ge 1 ] || fail "$MODE requires a positive bounded workload keyspace"
  [ "$HOOK_TIMEOUT" -ge "$((NODE_RESTART_TIMEOUT + 5))" ] \
    || fail "HOOK_TIMEOUT ($HOOK_TIMEOUT s) cannot cover one node restart bounded at ${NODE_RESTART_TIMEOUT}s"
  [ "$FINAL_HOOK_TIMEOUT" -ge "$((NODE_RESTART_TIMEOUT * 3 + 15))" ] \
    || fail "FINAL_HOOK_TIMEOUT ($FINAL_HOOK_TIMEOUT s) cannot cover three sequential node restarts bounded at ${NODE_RESTART_TIMEOUT}s each"
  validate_topology_binding
  if [ "$GATE" -eq 1 ]; then
    [ "$DURATION" -gt "$((GC_GRACE_SECS + RESTART_INTERVAL))" ] \
      || fail "DURATION ${DURATION}s must exceed gc_grace (${GC_GRACE_SECS}s) plus one restart interval, or no tombstone can be collected"
  fi
  bounded_hook zone-status "$ZONE_STATUS_CMD" zone-status 0 > "$OUT/zone-status.txt"
  awk -v zone="$ZONE" '/^ZONE_STATUS / && $0 ~ "zone=" zone && /sorted=1/ && /rf=3/ && /declared_w=2/ && /declared_r=2/ { found=1 } END { exit !found }' "$OUT/zone-status.txt" \
    || fail "ZONE_STATUS_CMD did not prove a dedicated sorted RF=3 zone with operator-declared W=2/R=2"
}

[ -x "$BIN" ] || fail "missing executable BIN=$BIN"
sha256sum "$BIN" > "$OUT/binary.sha256"
sha256sum "$0" > "$OUT/harness.sha256"
for source in crates/vals-bench/src/profilecheck.rs plans/mixed-profile-soak-runbook.md plans/profile-soak-remote-status.sh plans/profile-soak-remote-restart.sh plans/profile-soak-remote-diag.sh plans/profile-soak-zone-status.sh; do
  [ -f "$source" ] && sha256sum "$source" >> "$OUT/harness.sha256"
done
if [ -n "$git_dirty" ]; then
  source_state=dirty
  printf '%s\n' "$git_dirty" > "$OUT/git-status.txt"
  git -C "$SOURCE_ROOT" diff "$source_head" > "$OUT/source.patch" \
    || fail "could not archive tracked source changes from captured HEAD $source_head"
  if [ -n "$git_untracked" ]; then
    printf '%s\n' "$git_untracked" > "$OUT/untracked-files.txt"
  else
    : > "$OUT/untracked-files.txt"
  fi
  if [ -s "$OUT/untracked-files.txt" ]; then
    archive_tar=${PROFILE_SOAK_TAR:-tar}
    if ! "$archive_tar" -C "$SOURCE_ROOT" -czf "$OUT/untracked-source.tar.gz" \
        --verbatim-files-from -T "$OUT/untracked-files.txt"; then
      fail "could not archive every untracked source file listed in $OUT/untracked-files.txt"
    fi
    "$archive_tar" -tzf "$OUT/untracked-source.tar.gz" | sort > "$OUT/untracked-archive-files.txt" \
      || fail "could not read back the untracked source archive manifest"
    sort "$OUT/untracked-files.txt" > "$OUT/untracked-files.sorted"
    cmp -s "$OUT/untracked-files.sorted" "$OUT/untracked-archive-files.txt" \
      || fail "untracked source archive file list does not match the captured manifest"
    : > "$OUT/untracked-content.sha256"
    while IFS= read -r file; do
      expected=$(sha256sum "$SOURCE_ROOT/$file" | awk '{print $1}') \
        || fail "could not hash untracked source file $file"
      if ! "$archive_tar" -xOf "$OUT/untracked-source.tar.gz" "$file" > "$OUT/untracked-verify.tmp"; then
        rm -f "$OUT/untracked-verify.tmp"
        fail "could not read untracked source file $file back from the archive"
      fi
      actual=$(sha256sum "$OUT/untracked-verify.tmp" | awk '{print $1}')
      rm -f "$OUT/untracked-verify.tmp"
      [ "$actual" = "$expected" ] || fail "archived content hash mismatch for untracked source file $file"
      printf '%s  %s\n' "$expected" "$file" >> "$OUT/untracked-content.sha256"
    done < "$OUT/untracked-files.txt"
    sha256sum "$OUT/untracked-source.tar.gz" "$OUT/untracked-archive-files.txt" \
      "$OUT/untracked-content.sha256" >> "$OUT/harness.sha256"
  fi
  sha256sum "$OUT/source.patch" >> "$OUT/harness.sha256"
  if [ "$GATE" -eq 1 ] && [ "${ALLOW_DIRTY_SOURCE:-0}" != 1 ]; then
    fail "$MODE requires a clean pinned source tree (commit the harness/scripts, or set ALLOW_DIRTY_SOURCE=1 to run from the archived patch/untracked snapshot in $OUT)"
  fi
else
  source_state=clean
  : > "$OUT/git-status.txt"
fi

printf '%s\n' "mode=$MODE" "not_24h=$([ "$MODE" = 24h ] && echo 0 || echo 1)" "gate=$GATE" "restart_interval=$RESTART_INTERVAL" "expected_restarts=$EXPECTED_RESTARTS" "trend_samples=$TREND_SAMPLES" "peers=$PEERS" "metrics=$METRICS" "zone=$ZONE" \
  "source_state=$source_state" "allow_dirty_source=${ALLOW_DIRTY_SOURCE:-0}" "expected_valsd_sha256=${EXPECTED_VALSD_SHA256:-unpinned}" \
  "duration=$DURATION" "rounds=$ROUNDS" "seed_keys=$SEED_KEYS" "workload_keys=$WORKLOAD_KEYS" \
  "hook_timeout=$HOOK_TIMEOUT" "final_hook_timeout=$FINAL_HOOK_TIMEOUT" "node_restart_timeout=$NODE_RESTART_TIMEOUT" \
  "protocol_version=${PROTOCOL_VERSION:-unverified}" \
  "restart_cmd=${RESTART_CMD:-none}" "node_status_cmd=${NODE_STATUS_CMD:-none}" "zone_status_cmd=${ZONE_STATUS_CMD:-none}" \
  "git=$source_head" "git_status_sha256=$(printf '%s\n' "$git_dirty" | sha256sum | awk '{print $1}')" > "$OUT/provenance.txt"
run_started=$(date -u +%FT%TZ)
printf 'started=%s\n' "$run_started" >> "$OUT/provenance.txt"

GC_GRACE_SECS=${GC_GRACE_SECS:-900}
if [ "$LONG_GATE" -eq 1 ]; then
  validate_long_preconditions
fi
snapshot before || fail "initial metrics/status coverage gap"
if [ "$LONG_GATE" -eq 1 ]; then
  for file in "$OUT"/metrics/before.node*.prom; do require_metric_file "$file"; done
  validate_running_profiles before
  validate_deployed_configs before
  probe_serving_nodes before
  printf 'gc_grace_s=%s\n' "$GC_GRACE_SECS" >> "$OUT/provenance.txt"
fi

[ -z "${GC_OVERDUE_SECS:-}" ] || export VALS_PROFILECHECK_GC_OVERDUE_SECS="$GC_OVERDUE_SECS"
VALS_PROFILECHECK_ZONE=$ZONE VALS_PROFILECHECK_STATE=$STATE VALS_PROFILECHECK_SEED_KEYS=$SEED_KEYS \
VALS_PROFILECHECK_WORKLOAD_KEYS=$WORKLOAD_KEYS VALS_PROFILECHECK_GC_GRACE_SECS=$GC_GRACE_SECS \
VALS_PROFILECHECK_ALLOW_DIRTY_RESUME=0 \
VALS_PROFILECHECK_MAX_NODE_OUTAGE_SECS=${MAX_NODE_OUTAGE_SECS:-900} \
VALS_PROFILECHECK_NODE_CONVERGE_SECS=${NODE_CONVERGE_SECS:-900} \
VALS_PROFILECHECK_MAX_MODEL_RSS_MIB=${MAX_MODEL_RSS_MIB:-4096} \
VALS_PROFILECHECK_DIRECT_RETRY_SECS=${DIRECT_RETRY_SECS:-5} \
VALS_PROFILECHECK_COORD_RETRY_SECS=${COORD_RETRY_SECS:-300} \
VALS_PROFILECHECK_GC_PROBE_SECS=$GC_PROBE_SECS VALS_PROFILECHECK_GC_MAX_HELD_PCT=$GC_MAX_HELD_PCT \
  "$BIN" x profilecheck "$ROUNDS" 1 "$PEERS" "$DURATION" > "$LOG" 2>&1 &
workload=$!
start_epoch=$(date +%s)
next_scrape=$start_epoch
next_restart=$((start_epoch + RESTART_INTERVAL))
next_diag=$((start_epoch + DIAG_INTERVAL))
restart_count=0
supervisor_failed=0

while kill -0 "$workload" 2>/dev/null; do
  now=$(date +%s)
  if [ "$now" -ge "$next_scrape" ]; then
    if ! snapshot "sample.$now"; then
      supervisor_failed=1
      break
    fi
    [ "$LONG_GATE" -eq 0 ] || live_checks "sample.$now"
    next_scrape=$((next_scrape + SCRAPE_INTERVAL))
    while [ "$next_scrape" -le "$now" ]; do next_scrape=$((next_scrape + SCRAPE_INTERVAL)); done
  fi
  if [ "$LONG_GATE" -eq 1 ] && [ "$restart_count" -lt "$EXPECTED_RESTARTS" ] && [ "$now" -ge "$next_restart" ]; then
    restart_count=$((restart_count + 1))
    if ! run_restart "$restart_count"; then
      supervisor_failed=1
      break
    fi
    next_restart=$((next_restart + RESTART_INTERVAL))
    while [ "$next_restart" -le "$now" ]; do next_restart=$((next_restart + RESTART_INTERVAL)); done
  fi
  if [ "$DIAG_INTERVAL" -gt 0 ] && [ "$now" -ge "$next_diag" ]; then
    run_diag "t$((now - start_epoch))" diag
    next_diag=$((next_diag + DIAG_INTERVAL))
  fi
  sleep 1 & sleeper=$!
  wait "$sleeper" || true
  sleeper=
done

if [ "$supervisor_failed" -ne 0 ]; then
  kill "$workload" 2>/dev/null || true
fi
set +e
wait "$workload"
workload_rc=$?
set -e
workload=
cat "$LOG"
[ "$supervisor_failed" -eq 0 ] || fail "supervisor failed"
[ "$workload_rc" -eq 0 ] || fail "profilecheck workload exited $workload_rc"
awk '/^PROFILECHECK PASS / { pass=1 } END { exit !pass }' "$LOG" || fail "profilecheck PASS marker missing"
pass_field() {
  awk -v want="$1" '/^PROFILECHECK PASS / { for (i = 1; i <= NF; i++) { split($i, a, "="); if (a[1] == want) value=a[2] } } END { print value }' "$LOG"
}
if [ "$LONG_GATE" -eq 1 ]; then
  [ "$(pass_field quorum)" = 2 ] || fail "driver did not run with the three-node majority quorum (quorum=$(pass_field quorum))"
  [ "$(pass_field gc_grace_ms)" = "$((GC_GRACE_SECS * 1000))" ] \
    || fail "driver correctness model used gc_grace_ms=$(pass_field gc_grace_ms) instead of the configured ${GC_GRACE_SECS}s"
  for field in winner_tombstones resurrections cold_overwrites; do
    value=$(pass_field "$field")
    [ -n "$value" ] && [ "$value" -gt 0 ] || fail "driver produced no $field coverage"
  done
  keys=$(pass_field keys)
  max_keys=$(pass_field max_keys)
  [ -n "$keys" ] && [ -n "$max_keys" ] && [ "$keys" -le "$max_keys" ] \
    || fail "driver reference keyspace $keys is not bounded by $max_keys"
  reconciled=$(pass_field reconciled_winners)
  [ "$reconciled" = 0 ] || fail "$MODE cannot pass after reconciling unknown winners ($reconciled)"
  printf 'driver_keys=%s\ndriver_max_keys=%s\ndriver_rss_kib=%s\ndriver_reconciled_winners=%s\n' \
    "$keys" "$max_keys" "$(pass_field rss_kib)" "$reconciled" >> "$OUT/provenance.txt"
fi
if [ "$REQUIRE_OMITTED_WRITES" = 1 ]; then
  omitted=$(pass_field omitted_replica_writes)
  [ "${omitted:-0}" -gt 0 ] || fail "the run required restart-overlap omitted replica writes as AE-healing coverage"
fi
if [ "$REQUIRE_HEALED_OMISSIONS" = 1 ]; then
  healed=$(pass_field healed_live_cold_seed)
  [ "${healed:-0}" -gt 0 ] \
    || fail "no omitted live cold-seed winner was identified and read back exactly on its missing replica"
fi
if [ "$GC_PROBE_SECS" -gt 0 ] && [ "$GATE" -eq 1 ]; then
  [ "$(pass_field gc_probes)" -gt 0 ] && [ "$(pass_field gc_collected)" -gt 0 ] \
    || fail "the in-run tombstone-collection probe never checked an overdue winning tombstone (gc_probes=$(pass_field gc_probes) gc_collected=$(pass_field gc_collected))"
fi
if [ "$LONG_GATE" -eq 1 ]; then
  [ "$(pass_field pending_omissions)" = 0 ] \
    || fail "the driver finished with unhealed omitted winners still pending ($(pass_field pending_omissions))"
fi
snapshot after-workload || fail "post-workload metrics/status coverage gap"

all_ae_clean() {
  tag=$1
  node=1
  while [ "$node" -le 3 ]; do
    file=$OUT/metrics/$tag.node$node.prom
    [ "$(metric_value vals_ae_dirty_buckets "$file")" = 0 ] || return 1
    [ "$(metric_value vals_ae_dirty_oldest_ms "$file")" = 0 ] || return 1
    free=$(metric_value vals_free_segments "$file")
    awk -v free="$free" 'BEGIN { exit !(free > 0) }' || return 1
    node=$((node + 1))
  done
}

converge_ae() {
  phase=$1
  began=$(date +%s)
  while :; do
    now=$(date +%s)
    tag=$phase.$now
    snapshot "$tag" || fail "coverage gap during $phase AE convergence"
    if all_ae_clean "$tag"; then
      printf '%s ae_converged phase=%s elapsed_s=%s\n' "$(date -u +%FT%TZ)" "$phase" "$((now - began))" >> "$OUT/convergence.log"
      FINAL_TAG=$tag
      return
    fi
    [ $((now - began)) -lt "$AE_TIMEOUT" ] || fail "$phase AE convergence exceeded ${AE_TIMEOUT}s"
    sleep "$AE_POLL_INTERVAL"
  done
}

if [ "$LONG_GATE" -eq 1 ]; then
  [ "$restart_count" -ge "$EXPECTED_RESTARTS" ] || fail "observed $restart_count restarts, expected at least $EXPECTED_RESTARTS"
  [ "$(awk '/^PRESSURE / { n++ } END { print n+0 }' "$OUT/restarts.log")" -eq "$restart_count" ] || fail "missing restart pressure evidence"
  awk '/^EVENT / && /node=3/ && /from=capacity/ && /to=latency/ { up=NR } /^EVENT / && /node=3/ && /from=latency/ && /to=capacity/ && up && NR > up { down=1 } END { exit !(up && down) }' "$OUT/restarts.log" || fail "missing ordered node3 capacity->latency->capacity hook evidence"

  converge_ae quiesce
  validate_deployed_configs "$FINAL_TAG"
  bounded_hook final-restart "$RESTART_CMD" final 0 "$FINAL_HOOK_TIMEOUT" >> "$OUT/restarts.log"
  cat "$OUT/hooks/final-restart.txt"
  awk '/^EVENT / && /rolling=all/ && /restart=1/ { found=1 } END { exit !found }' "$OUT/hooks/final-restart.txt" || fail "final hook did not prove rolling all-node restart"
  converge_ae final
  probe_serving_nodes final
  validate_running_profiles "$FINAL_TAG"
  validate_deployed_configs "$FINAL_TAG"

  VALS_PROFILECHECK_ZONE=$ZONE VALS_PROFILECHECK_STATE=$STATE VALS_PROFILECHECK_SEED_KEYS=$SEED_KEYS \
  VALS_PROFILECHECK_WORKLOAD_KEYS=$WORKLOAD_KEYS VALS_PROFILECHECK_GC_GRACE_SECS=$GC_GRACE_SECS \
  VALS_PROFILECHECK_ALLOW_DIRTY_RESUME=0 \
  VALS_PROFILECHECK_VERIFY_ONLY=1 VALS_PROFILECHECK_VERIFY_RETRY_SECS=${VERIFY_RETRY_SECS:-300} \
  VALS_PROFILECHECK_VERIFY_CONVERGE_SECS=${VERIFY_CONVERGE_SECS:-900} \
  VALS_PROFILECHECK_VERIFY_POLL_SECS=${VERIFY_POLL_SECS:-10} \
    "$BIN" x profilecheck 0 1 "$PEERS" > "$VERIFY_LOG" 2>&1 || { cat "$VERIFY_LOG" >&2; fail "final verify-only scan failed"; }
  cat "$VERIFY_LOG"
  awk '/^PROFILECHECK VERIFY PASS / { pass=1 } END { exit !pass }' "$VERIFY_LOG" || fail "final verify-only PASS marker missing"
  [ "$(awk '/^PROFILECHECK VERIFY NODE / { n++ } END { print n+0 }' "$VERIFY_LOG")" -eq 3 ] \
    || fail "final verification did not stream a converged full scan from all three replicas"
  if [ "${REQUIRE_COLLECTED_TOMBSTONES:-0}" = 1 ]; then
    collected=$(awk '/^PROFILECHECK VERIFY PASS / { for (i = 1; i <= NF; i++) { split($i, a, "="); if (a[1] == "collected_tombstones") value=a[2] } } END { print value+0 }' "$VERIFY_LOG")
    [ "$collected" -gt 0 ] || fail "short-grace run collected no winning tombstones"
  fi

  node=1
  while [ "$node" -le 3 ]; do
    file=$OUT/metrics/$FINAL_TAG.node$node.prom
    require_metric_file "$file"
    awk '
      $1 == "vals_profile_info{profile=\"latency\"}" && $2 == 1 { latency=1 }
      $1 == "vals_profile_info{profile=\"capacity\"}" && $2 == 1 { capacity=1 }
      $1 == "vals_blind_append_enabled" { blind=$2 }
      $1 == "vals_winner_directory_enabled" { directory=$2 }
      $1 == "vals_winner_directory_keys" { winner_keys=$2 }
      $1 == "vals_free_segments" && $2 <= 0 { bad_space=1 }
      END {
        if (latency && (blind != 1 || directory != 1 || winner_keys <= 0)) exit 1
        if (capacity && (blind != 0 || directory != 0)) exit 1
        if ((!latency && !capacity) || bad_space) exit 1
      }
    ' "$file" || fail "profile degradation or exhausted space in $file"
    check_counter_bounds "$node"
    node=$((node + 1))
  done
  merge_progress=0
  node=1
  while [ "$node" -le 3 ]; do
    delta=$(counter_delta vals_run_merges "$node")
    merge_progress=$(awk -v total="$merge_progress" -v delta="$delta" 'BEGIN { print total + delta }')
    node=$((node + 1))
  done
  if [ "$GATE" -eq 1 ] || [ "${REQUIRE_MERGE_PROGRESS:-0}" = 1 ]; then
    awk -v progress="$merge_progress" 'BEGIN { exit !(progress > 0) }' || fail "no restart-aware merge progress"
  fi
  if [ "$GATE" -eq 1 ]; then
    node=1
    while [ "$node" -le 3 ]; do
      check_space_trend "$node" 1
      node=$((node + 1))
    done
  fi

  up=${TRANSITION_UP_RESTART:-1}
  down=${TRANSITION_DOWN_RESTART:-2}
  up=$(printf '%03d' "$up")
  down=$(printf '%03d' "$down")
  awk '$1 == "vals_profile_info{profile=\"capacity\"}" && $2 == 1 { ok=1 } END { exit !ok }' "$OUT/metrics/restart.$up.before.node3.prom" || fail "node3 was not capacity before transition"
  awk '$1 == "vals_profile_info{profile=\"latency\"}" && $2 == 1 { ok=1 } END { exit !ok }' "$OUT/metrics/restart.$up.after.node3.prom" || fail "node3 did not become latency"
  awk '$1 == "vals_profile_info{profile=\"capacity\"}" && $2 == 1 { ok=1 } END { exit !ok }' "$OUT/metrics/restart.$down.after.node3.prom" || fail "node3 did not return to capacity"

  awk '/scrape_gap/ { gap=1 } END { exit gap }' "$OUT/coverage.log" || fail "metrics coverage contains an unrecovered gap"
fi

date -u +finished=%FT%TZ >> "$OUT/provenance.txt"
run_diag final diag-final
trap - EXIT INT TERM HUP
if [ "$GATE" -eq 1 ]; then
  echo "profile soak $MODE PASS duration_s=$DURATION restarts=$restart_count seed_keys=$SEED_KEYS gc_grace_s=$GC_GRACE_SECS source=$source_state artifacts=$OUT"
else
  echo "profile soak $MODE PASS (NOT 24h; does not satisfy hardware gate) source=$source_state artifacts=$OUT"
fi

#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
TMP=${TMPDIR:-/tmp}/profile-soak-selftest.$$
trap 'rm -rf "$TMP"' EXIT INT TERM HUP
mkdir -p "$TMP"

for node in 1 2 3; do
  peers=
  other=1
  while [ "$other" -le 3 ]; do
    if [ "$other" -ne "$node" ]; then peers="$peers
peer = $other @ 127.0.0.1:$((7100 + other))"; fi
    other=$((other + 1))
  done
  profile=capacity
  [ "$node" -eq 2 ] && profile=latency
  cat > "$TMP/node$node.conf" <<EOF
node_id=$node
profile=$profile
listen=127.0.0.1:$((7100 + node))
backend=file
data_dir=node$node
rf=3
gc_grace=900s   # compact key=value syntax with a trailing comment
$peers
EOF
done
cp "$TMP/node3.conf" "$TMP/node3.capacity.conf"
sed 's/^profile=capacity$/profile=latency/' "$TMP/node3.conf" > "$TMP/node3.latency.conf"
CONFIG_FILES="$TMP/node1.conf,$TMP/node2.conf,$TMP/node3.capacity.conf"
EXTRA_CONFIG_FILES="$TMP/node3.latency.conf"
MOCK=$ROOT/plans/profile-soak-selftest-mock.sh
HARNESS=$ROOT/plans/profile-soak.sh
PEERS='1@127.0.0.1:7101;2@127.0.0.1:7102;3@127.0.0.1:7103'
passes=0

run_case() {
  expected=$1
  name=$2
  shift 2
  dir=$TMP/$name
  metrics=$dir/live
  mkdir -p "$metrics"
  echo 10 > "$metrics/counter"
  echo capacity > "$metrics/node3-profile"
  env MOCK_METRICS_DIR="$metrics" MOCK_CONFIG_DIR="$TMP" "$@" SOAK_EVENT=init "$MOCK"
  set +e
  env PROFILE_SOAK_ALLOW_TEST_MODE=1 MOCK_METRICS_DIR="$metrics" \
    PEERS="$PEERS" METRICS="file://$metrics/node1.prom,file://$metrics/node2.prom,file://$metrics/node3.prom" \
    OUT="$dir/out" ZONE=19 BIN="$MOCK" DURATION=4 MOCK_DRIVER_SECONDS=4 SCRAPE_INTERVAL=1 \
    RESTART_INTERVAL=1 EXPECTED_RESTARTS=2 REQUIRE_MERGE_PROGRESS=1 NODE_RESTART_TIMEOUT=1 HOOK_TIMEOUT=6 \
    FINAL_HOOK_TIMEOUT=20 CURL_TIMEOUT=2 AE_TIMEOUT=2 AE_POLL_INTERVAL=1 \
    PROTOCOL_VERSION=2 CONFIG_FILES="$CONFIG_FILES" EXTRA_CONFIG_FILES="$EXTRA_CONFIG_FILES" \
    MOCK_CONFIG_DIR="$TMP" EXPECTED_VALSD_SHA256=mockbinary \
    NODE_STATUS_CMD="$MOCK" ZONE_STATUS_CMD="$MOCK" RESTART_CMD="$MOCK" "$@" \
    "$HARNESS" lifecycle-test > "$dir/result.log" 2>&1
  rc=$?
  set -e
  if [ "$expected" = pass ] && [ "$rc" -ne 0 ]; then
    cat "$dir/result.log" >&2
    echo "selftest $name unexpectedly failed" >&2
    exit 1
  fi
  if [ "$expected" = fail ] && [ "$rc" -eq 0 ]; then
    cat "$dir/result.log" >&2
    echo "selftest $name unexpectedly passed" >&2
    exit 1
  fi
  passes=$((passes + 1))
  echo "ok $passes - $name ($expected)"
}

make_git_fixture() {
  fixture=$1
  mkdir -p "$fixture"
  git -C "$fixture" init -q
  git -C "$fixture" config user.email profile-soak@example.invalid
  git -C "$fixture" config user.name profile-soak-selftest
  printf 'fixture\n' > "$fixture/README"
  git -C "$fixture" add README
  git -C "$fixture" commit -qm fixture
}

CLEAN_SOURCE=$TMP/clean-source
make_git_fixture "$CLEAN_SOURCE"
DIRTY_SOURCE=$TMP/dirty-source
make_git_fixture "$DIRTY_SOURCE"
mkdir -p "$DIRTY_SOURCE/plans"
printf 'untracked source\n' > "$DIRTY_SOURCE/plans/untracked.txt"

run_case pass success
run_case pass clean-source-capture-before-out PROFILE_SOAK_SOURCE_ROOT="$CLEAN_SOURCE" OUT="$CLEAN_SOURCE/out"
grep -q '^source_state=clean$' "$CLEAN_SOURCE/out/provenance.txt" || {
  echo "clean source fixture was dirtied by creating OUT inside its checkout" >&2
  exit 1
}
run_case fail untracked-source-archive-failure PROFILE_SOAK_SOURCE_ROOT="$DIRTY_SOURCE" PROFILE_SOAK_TAR=false
grep -q 'could not archive every untracked source file' "$TMP/untracked-source-archive-failure/result.log" || {
  echo "archive failure was not diagnostic" >&2
  exit 1
}
run_case pass ambient-dirty-resume-is-forced-off VALS_PROFILECHECK_ALLOW_DIRTY_RESUME=1
run_case pass restart-reset-keeps-merge-progress MOCK_MERGE_RESET_WITH_PROGRESS=1
run_case pass final-hook-outlives-hourly-timeout MOCK_FINAL_HOOK_SECONDS=10
run_case fail final-hook-exceeds-final-timeout MOCK_FINAL_HOOK_SECONDS=25
run_case fail driver-failure MOCK_DRIVER_FAIL=1
run_case fail supervisor-failure MOCK_STATUS_FAIL_AFTER=1
run_case fail missing-metrics MOCK_MISSING_METRIC=1
run_case fail non-numeric-counter MOCK_NONNUMERIC_COUNTER=1
run_case fail missing-restart-evidence MOCK_RESTART_EVIDENCE_MISSING=1
run_case fail hook-fails-after-event MOCK_HOOK_FAIL_AFTER_EVENT=1
run_case fail final-hook-fails-after-event MOCK_FINAL_HOOK_FAIL_AFTER_EVENT=1
run_case fail missing-profile-transitions MOCK_TRANSITIONS_MISSING=1
run_case fail profile-binding-mismatch MOCK_PROFILE_BINDING_WRONG=1
run_case fail restart-reset-hides-no-merge-progress MOCK_MERGE_RESET_NO_PROGRESS=1
run_case fail restart-reset-hides-error MOCK_ERROR_THEN_RESET=1
run_case fail dirty-timeout MOCK_DIRTY=1
run_case fail degraded-profile MOCK_DEGRADED=1
run_case fail final-verification MOCK_FINAL_FAIL=1
run_case fail final-verification-missing-node MOCK_VERIFY_NODE_MISSING=1
run_case fail driver-without-tombstone-coverage MOCK_WINNER_TOMBSTONES=0
run_case fail driver-without-majority-quorum MOCK_QUORUM=1
run_case fail reconciled-winner-cannot-pass-gate MOCK_RECONCILED=1
run_case fail short-grace-without-collected-tombstones REQUIRE_COLLECTED_TOMBSTONES=1 MOCK_COLLECTED_TOMBSTONES=0
run_case fail readiness-probe-fails MOCK_PROBE_FAIL=1
run_case fail deployed-binary-not-source-pinned MOCK_WRONG_BINARY=1
run_case fail deployed-config-does-not-follow-the-flip MOCK_STALE_DEPLOYED_CONFIG=1
run_case fail wrong-remote-alternate-before-restart MOCK_WRONG_ALTERNATE=1
if grep -q '^EVENT ' "$TMP/wrong-remote-alternate-before-restart/result.log"; then
  echo "wrong remote alternate was not rejected before restart events" >&2
  exit 1
fi
run_case fail no-omitted-write-coverage REQUIRE_OMITTED_WRITES=1 MOCK_OMITTED_WRITES=0
run_case fail no-healed-omitted-winner REQUIRE_HEALED_OMISSIONS=1 MOCK_HEALED_COLD_SEED=0
run_case fail unhealed-omissions-left-pending MOCK_PENDING_OMISSIONS=2
run_case pass diagnostics-run-on-schedule-and-at-the-end DIAG_CMD="$MOCK" DIAG_INTERVAL=2
grep -q '^diag t' "$TMP/diagnostics-run-on-schedule-and-at-the-end/live/diag.log" \
  && grep -q '^diag-final final ' "$TMP/diagnostics-run-on-schedule-and-at-the-end/live/diag.log" || {
  echo "scheduled and final diagnostics did not run" >&2
  exit 1
}
began=$(date +%s)
run_case fail in-run-check-stops-early DURATION=60 MOCK_DRIVER_SECONDS=60 MOCK_REGION_EXHAUSTED_AT_RESTART=1 DIAG_CMD="$MOCK"
[ $(( $(date +%s) - began )) -lt 30 ] || { echo "an in-run counter failure did not stop the run early" >&2; exit 1; }
grep -q 'vals_region_exhausted_rejections accumulated delta 1' "$TMP/in-run-check-stops-early/result.log" \
  && grep -q '^diag-failure failure ' "$TMP/in-run-check-stops-early/live/diag.log" || {
  echo "the in-run failure was not diagnostic or skipped the failure diagnostics" >&2
  exit 1
}

echo "1..$passes"
echo "profile-soak lifecycle selftest PASS (mock only; NOT 24h and NOT hardware)"

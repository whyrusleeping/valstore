---
id: "0293"
title: Benchmark sustained realistic Jetstream ingestion with lifecycle metrics and CPU profiles
status: done
priority: 2
created: "2026-09-11"
updated: "2026-09-13"
depends_on: []
spec_refs: []
---

## Description
User accepted sustained realistic ingestion benchmark with profiling and bottleneck investigation. Execute a fresh source-pinned sealed Jetstream cache replay through current protocol-v2 bootes/valsgo into three-node RF2 SPDK vals, initially target 60 minutes of actual progressing workload (not idle liveness). Use existing bootes soak/cache-replay procedures; benchmark is distinct from blocked 0194 RF3 mixed-profile/HA gate and does not clear 0248 corruption. Inspect live fleet and workload/tooling before mutation; preserve unrelated 0279/0287 staged/worktree changes. Prior owner authorization allows replacement of test corpus/PD state only; preserve sealed cache and useful unresolved-corruption cores with matching binaries, compact provenance/results. Explicitly establish test PD topology and effective profile/persistence/client budgets before launch; no production HA claim. Rebuild/hash current source on every node and source-pin bootes/valsgo. Keep automatic balance execution default-off; normal auto-split/AE/flush/compaction remain active.

Acceptance: capture source/config/build/workload hashes, initial/final state, completed source events and logical/physical writes separately, interval throughput and client/server latency/retries/Busy/errors, per-shard CPU and periodic bounded perf callgraphs, RSS/DMA/headroom, disk/network activity, flush/compaction/AE/seed/range churn and backlog. Verify driver rate and maintenance progress; identify client vs serving vs maintenance bottlenecks. Use source-consistent full-value/ordered checks without contaminating baseline. On crash/stall/resource danger retain evidence and stop safely rather than silently restart or count idle exposure. Record whether sustained maintenance equilibrium was reached. Keep baseline fixed; optimization proposals/accepted scoped A/B follow after attribution, not mid-run. Safe teardown with clients then nodes/PD, record forced stops, preserve corpus/configs and forensics. One hour is an initial benchmark, not release/corruption clearance or proof at production scale.

## Acceptance criteria

Accepted as the corrected 60-minute measurement baseline, including its negative
result: throughput declined and maintenance equilibrium was not reached. The
historical verification/attribution gaps below are explicitly not satisfied by
this closure. Remaining client telemetry/concurrency work, full source-consistent
payload/version/tombstone verification, readiness/debt evidence, matched fixes,
and final sustained validation are owned by 0294. No release or corruption
clearance is implied.

## Work log

### 2026-09-13 — accepted baseline checkpoint

Owner requested committing the measured 0293/0294 gains and continuing performance
work. Close 0293 as the corrected baseline/attribution record, not an exhaustive
correctness gate. `CORRECTIONS.md` takes precedence over the original optimistic
summary; original raw receipts and analyses remain immutable. The bounded
sentinel and Scan/Get checks do not establish full-corpus correctness. 0294
records the subsequent short matched server gain and remains `in_progress` for
client lanes, full correctness, direct readiness/debt evidence, and the long gate.
Compact evidence/scripts and a path/size/SHA-256 retained-raw inventory are
checkpointed; no fleet operation or evidence deletion accompanies this closure.


### 2026-09-11 — harness, build and preflight (pre-mutation)

Created `bench/0293-sustained-ingest-20260911/` with `deploy.sh` (source-pinned
SPDK deploy/build), `run_ingest.py` (supervised harness) and `README.md`
(topology label, chosen limits, watchdog and measurement discipline).

- Source pinned at `5c27de53fdd2fa2d77c6d430d869dd56d2d3e961`; `crates/`,
  `Cargo.*` and `rust-toolchain.toml` verified clean against HEAD. Unrelated
  staged 0279/0287 evidence and the unstaged 0287 backlog edit were left exactly
  as found (no stash, reset, commit or `cargo fmt`).
- Rebuilt `vals-server --features spdk` on all three lab hosts from that
  source; the three binaries byte-match at
  `5f7be71a900532e86e0921a43217d6c0eee120f1c0c35d64e2c9d1c7be8b81d0` and are
  preserved as `/home/user/vals0293-artifacts/valsd-5c27de53` for future core
  decoding. Local source-pinned driver/probe binaries: `bootes-0293`
  (`2e3e7f42…`, `go test ./...` clean), `interop-0293` (`ccaceaea…`),
  `valsq-0293` (`7b1f46e0…`, built against this workspace).
- Read-only preflight passed: all three hosts reachable with no `valsd` and no
  `vals*` units, scratch devices are Samsung 9100 PRO on `vfio-pci`
  (`0000:81:00.0/SERIAL-REDACTED`, `0000:01:00.0/SERIAL-REDACTED`,
  `0000:01:00.0/SERIAL-REDACTED`), 4096 hugepages each,
  `core_pattern=/home/user/vals/run/core.%e.%p`, `panic_on_oops=1`, ~45-46 GiB
  MemAvailable, 33-40 GiB free on `/home`. Sealed cache manifest still
  `efa9777f…a22`. Existing cores inventoried and retained, including
  lab-3 `core.vals-shard.1702165` (4,129,230,848 bytes).

Planned arm: fresh offline `valsd provision --device PCI=SERIAL
--confirm-destroy node-N` (never `format = true`), single benchmark-only non-HA
PD on lab-1, RF2, four capacity-profile shards per node, 36 GiB RAM budget,
256 GiB region with 2/80/80 GiB default/records/backlinks slabs, balance
execution off, split/AE/flush/compaction on, 60-minute sealed-cache replay with
20 s metric scrapes, 60 s /proc host samples, and bounded 15 s 199 Hz
system-wide perf profiles at ~2/10/30/50 min plus stall/slowdown triggers.

### 2026-09-11 — executed 60-minute arm; complete, clean, loss-free

Attempt 1 (14:47Z) aborted safely before any workload: the harness checked shard
listeners immediately after the metrics endpoint bound, before SPDK region open
finished, so all nodes reported 0/4 listeners and the guaranteed teardown stopped
everything (all units inactive, no `valsd`). Receipts kept in
`run-attempt1-early-listener-check/`; fixed by polling listeners.

Attempt 2 ran 14:50:12Z → 15:50:23Z and stopped on its own `sixty-minute-bound`.

- Workload: 54,810,007 admitted source events (cursor 0 → 55,961,493) and
  234,309,565 physical replica applications (109.2 GB record bytes, 4.27
  applications per source event, RF2/both zones — not a logical rate).
- Throughput declined broadly: five-minute medians 38,462/s (0-5 min) →
  6,002/s (55-60 min), interval median 9,701/s, last-third median 6,112/s;
  physical peak 319,430/s → last-third median 26,559/s.
- Clean drain: `admitted_records = persisted_records = 54,810,069`,
  `admitted_backlinks = persisted_backlinks = 57,200,293`, 0 dropped/rejected/
  buffered/inflight, `deadline_expired=false`, cursor saved at 55,965,589.
  Zero dropped batches, zero quorum failures; 13 client Busy throttles and 2
  flush retries, matching `vals_range_fence_rejections` 34/34/36. One
  unexplained `vals_server_errors` increment on node 2 (no log line, no
  client-visible failure).
- Resource state: RSS 12.5-14.3 GB of the 36 GiB budget, `used_segments`
  77k-85k with ~580k free, zero Busy/ram-Busy/region-exhausted. Server apply
  p50 stayed <=5 ms while BatchApply operations per window fell ~4.5x and tails
  grew to 0.1-1 s.
- Attribution: no cause established. Continuous placement-debt re-homing
  (54->164 ranges, 110 splits, 53 re-homes started / 52 completed, 58 still
  un-rehomed; late prepare->commit 49-72 s back-to-back) is the strongest
  association - whole-run median 8,558/s with a move in flight versus 27,398/s
  quiet - but the partition is observational and confounded with corpus growth.
  Sustained maintenance equilibrium was NOT reached.
- Verification: 300 sentinel keys byte-exact after the run; bounded sorted scans
  of real ingested keys ordered with non-empty decoded values and matching point
  reads in `records`. No ground-truth comparison against the sealed cache was
  performed (and none is directly possible: bootes derives keys/values with
  dynamic timestamps).
- Teardown/retention: nodes stopped in 1-11 s and PD cleanly, all units
  inactive, no `valsd`, no forced stop, no new core; every pre-existing core
  (including lab-3 `core.vals-shard.1702165`) and the sealed cache
  (`efa9777f…a22`) are unchanged. AER storm continued (85,397/84,895/85,121
  lines) and is recorded as a confound only.

### 2026-09-11 — evidence correction and harness repair (no fleet work)

Post-run review found overstated claims in the first write-up and real defects
in the harness/analyzer. This entry is evidence and tooling only: **no fleet
workload, provision, restart or product change**, nothing committed. The fleet
was independently confirmed stopped beforehand (all three `vals0293-node`/`-pd`
units inactive/dead with MainPID 0, no `valsd` and no `valsd-5c27de53`
process); this session made no connection to any host. Raw receipts in
`bench/0293-sustained-ingest-20260911/run/` are byte-unchanged, and unrelated
staged 0279/0287 work was left exactly as found.

Claims withdrawn or weakened (full list with evidence in
`bench/0293-sustained-ingest-20260911/CORRECTIONS.md`):

- "Monotonic decay" - withdrawn: 102 of 179 interval steps are increases and
  late intervals still spike to 47,462/s; the honest statement is a ~6x decline
  in five-minute medians with large fluctuation, ending ~6,000/s against a
  hoped-for sustained ~25,000/s.
- "Nothing was CPU- or capacity-bound" and "2.1-2.3 of 24 cores" - withdrawn as
  an exoneration. Writes are served by four `vals-shard` threads per node; from
  /proc per-TID ticks the hottest thread ran at 0.63-0.69 cores median (peak
  0.94) and cluster process CPU ROSE 5.11 -> 7.0 cores while throughput fell
  4.3x, i.e. CPU per admitted event grew ~5.8x (174 -> 1,014 us). Poll loops
  count as busy CPU and no wait/off-CPU telemetry exists, so CPU-bound versus
  blocked is unresolved.
- "Client head-of-line blocking" as a measured second factor - withdrawn: the
  driver exports no client latency metric. Replaced by the explicit write
  contract read from source (one ordered <=1,000-entry chunk in flight per zone
  lane; valsgo fans out per (node, shard), requires W=2 of RF2 acks, sync=1
  group-commit device fsync per destination chunk; no Rust finite credit pool in
  this path) plus the statement that its throughput consequence is a hypothesis.
- System-wide `perf -a` percentages were being read as server-thread
  utilization; they include idle and unrelated processes and are now labelled as
  a symbol ranking only, never mixed with /proc per-TID numbers.
- "Maintenance kept up" - withdrawn: counts are volume, not equilibrium.
- Re-home association is now stated as correlation only, with the confound and
  small late quiet-sample counts explicit; task 0292's matched ~25% result is
  labelled a separate experiment. Also documented: automatic balance execution
  was off (`vals_pd_balance_exec_prepares=0`) while placement-debt re-homing is
  a *different* PD path that defaults ON (`VALS_PD_REHOME_ENABLED`,
  `VALS_PD_MAX_CONCURRENT_REHOMES=1`), so data movement ran anyway.
- Verification limits stated: the backlinks point-read check verified nothing
  (hex-rendered NUL keys fed back as literal keys), scans ignored probe exit
  status and would have accepted empty results, the comparison was a substring
  test, and node/PD logs were saved before the stop so no server shutdown lines
  exist for this run.
- `timeline.jsonl` `node_cpu_seconds`/`node_rss_bytes` are absent-metric zeros
  (valsd exports `vals_process_resident_bytes`, no process CPU counter), not
  measurements.

Harness/analyzer repairs (post-run; they did NOT execute during the arm):

- fail-closed exclusivity probe covering `valsd` AND `valsd-5c27de53`, unit
  state, MainPID and `/proc/PID/exe`, required from every host before provision;
- asynchronous perf recording with rendering deferred until after teardown, plus
  a `SafetyWatch` thread that keeps sampling resources through drain/teardown -
  safety never waits on profiling (measured instrument-induced gap in the arm
  was bounded: 22.8 s largest scrape gap against a 20 s cadence);
- unavailable metrics/host samples recorded as unavailable, with per-source
  scrape-failure and host-sample-failure watchdog conditions;
- node RSS from `vals_process_resident_bytes`, CPU only from /proc per-TID;
  analyzer reports per-thread utilization and CPU cost per event, and refuses to
  overwrite existing analysis files (`analysis-corrected.txt` written from the
  original receipts; `analysis.txt` untouched);
- disk-aware RSS danger limit `min(40 GiB, free(/home) - 8 GiB)` recorded per
  host, with an explicit warning that a node at its full RAM budget cannot be
  dumped into 33-40 GiB of free space; core capture documented as best-effort;
- explicit `final-status.txt` verdict from driver exit code, parsed shutdown
  summary (admitted==persisted, zero pending/dropped/rejected, no expired
  deadline), an independent `cursor/jetstream` read-back from the live cluster,
  stop-receipt inspection for forced-stop signatures, and post-stop log saves;
- strict payload sampling: non-zero probe exit or empty scan fails, point reads
  compare exact values, binary keys are labelled unverifiable instead of
  reported as mismatches.

`bench/0293-sustained-ingest-20260911/test_harness.py` adds 47 offline tests for
these fail-stop paths (mocked receipts; no fleet, network or `run/` access) -
all passing.

Remaining gaps (not closed here): no wait/off-CPU telemetry, so the bottleneck
is still unattributed; no client-side latency instrumentation; no payload
ground-truth oracle; no server shutdown logs for this arm; single uncontrolled
arm with no matched control. Task stays `in_progress` pending final review.

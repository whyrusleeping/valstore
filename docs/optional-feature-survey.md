# Optional feature and cleanup survey

Status: historical owner-reviewed survey (task 0187, 2026-08-17), with dispositions
recorded through 2026-08-18. This is not a current feature-removal plan: later
validation and consumer adoption can supersede its recommendations. Check the
[canonical tasks](../backlog.md) before acting; in particular, do not treat the
old balanced-placement or client-budget recommendations below as current evidence.
Owner decisions: file the complete low-risk deletion group; reserve hardware and run task 0182's
balanced-range gate; keep blind append and the compact winner directory and productize explicit
capacity/latency profiles.

## Final dispositions (2026-08-18)

- Phase A removals and hard-wiring landed in tasks 0188–0193.
- Task 0194 productized explicit `capacity` and `latency` deployment profiles.
- Tier-1 mechanical cleanup and the oversized-file module splits landed in tasks 0212/0213.
- Remaining decision forks are tracked by tasks 0217 (optional subsystems), 0182 (balanced-range
  hardware gate), and 0218 (bench-artifact retention).

## Scope and method

This survey inventories current product-side feature gates, compatibility fallbacks, tuning knobs,
and public opt-in subsystems. It cross-checks the implementation against canonical docs, completed
backlog decisions, retained hardware findings, current deployment runbooks, nearby downstream
consumers, and introducing commits.

At the pre-cleanup survey checkpoint, the tree read **67 named `VALS_*` runtime variables outside
tests and `vals-bench`**. This is a historical snapshot, not the post-0188–0193 count. Many were
legitimate capacity/tuning parameters, but too many correctness fixes still carried permanent
rollback switches, while several large default-off experiments remained in production code. At that
checkpoint none of the surveyed optional deployment features appeared in tracked runbook launch
commands; the soak runbook enabled only the RAM budget. Searches of the nearby consumers found no
calls to the surveyed opt-in client APIs and no deployment use of the optional feature variables.

Introducing-commit source deltas below are historical scale indicators, not deletion estimates:
subsequent fixes and shared prerequisites mean cleanup must be done from the current tree, not by
reverting commits.

## Executive recommendation

1. **Delete now:** transparent write combining, the disabled merge-concurrency permit, NVMe
   Deallocate plumbing, the legacy merge monolith, and old correctness kill switches whose enabled
   behavior is now a required invariant. Legacy on-core flush preparation was removed in task 0192.
2. **Retest briefly, then decide:** key-ordered vlog co-compaction, digest hedging, hot-range splitting,
   auto-merge, cleanup `min_confirms`, and the opt-in client availability/backpressure additions.
3. **Keep, but turn into explicit supported profiles:** blind append and the compact winner directory.
   They have large measured wins; their costs are real and documented. Environment switches are a poor
   long-term product interface, but deleting these paths would discard the strongest recent gains.
4. **Do not add more code before deciding its fate:** balanced sorted-range execution. It is a large,
   unvalidated default-off subsystem. Either run task 0182 promptly or remove executor/opaque-token
   machinery while retaining the useful read-only load-skew metric.
5. **Consolidate configuration:** after removals, distinguish supported deployment settings from hidden
   developer A/B overrides. Stop exposing every internal constant and every shipped fix as a permanent
   environment variable.

## Tier 1: removal candidates with evidence already sufficient

### Transparent single-write combining — removed (0188)

- Former gate: `VALS_WRITE_COMBINE` plus five tuning variables; default was off.
- Evidence: the final matched hardware sweep reduced request count and p50 but **did not improve
  throughput and materially worsened p99**. The docs explicitly say OFF was selected. See
  `docs/batch-writes.md` and task 0186.
- Cost: introducing commit `bb34c38` changed roughly +960/-45 product-source lines and required many
  review rounds around executor liveness, cancellation, timer failure, and lost wakes.
- Use: no tracked deployment or nearby consumer enables it.
- Disposition: task 0188 deleted the combiner queues/workers/configuration/metrics/tests and retained
  explicit `BatchApply`, which did prove useful.

### Per-node merge concurrency permit — removed (0189)

- Former gate: `VALS_MERGE_NODE_CONCURRENCY`; default was unlimited/off.
- Evidence: task 0166 measured limit=1 as **net-negative**. The useful result from that task was the
  default-on maintenance duty-cycle pacer, which reduced flush-window collapses and write errors.
- Disposition: task 0189 deleted the optional permit, its variable, and plan-policy branches. The
  `VALS_MAINT_DUTY_PCT` / `VALS_MAINT_FLUSH_BYTES` pacing behavior remains.

### NVMe Deallocate / TRIM — removed (0190)

- Former gate: `VALS_NVME_DEALLOC`; default was off.
- Evidence: the task-0169 whole-namespace and free-time TRIM A/B **neither helped nor hurt** the
  sustained-overwrite crawl; the root cause was synchronous apply reads, later addressed by tasks
  0172/0167.
- Disposition: task 0190 deleted allocator free recording, block-device and SPDK Deallocate APIs,
  format/slab/free-time calls, metrics, and tests. Formatting correctness remains based on explicit
  metadata and segment-header writes, not advisory deallocation.

### Legacy merge monolith — removed (0191)

- Former gate: `VALS_MERGE_LEGACY=1`; default was off.
- Evidence: retained only for task-0069/0097 one-binary A/B. The old merge-all + co-compact-all behavior
  was superseded because it repeatedly copied all live values and created the structural O(N²) merge
  problem.
- Disposition: task 0191 deleted the runtime plan branch and A/B-only tests/bench defaults. Manifest
  v1-v3 readers and conservative handling of metadata without liveness fields remain.

### Legacy on-core flush preparation — removed (0192)

- Former gate: `VALS_FLUSH_ONCORE_PREP=1`; default was off.
- Evidence: task 0091 moved preparation off the serving core; the old path was retained temporarily
  after migration. Later validation confirmed the deployed environment used the new path.
- Disposition: task 0192 deleted the old sync/sort execution path and knob. Flush snapshot and durability
  sub-phase histograms remain.

### Correctness and liveness invariants — hard-wired on

Task 0193 removed the production kill switches for net-gain-aware pressure planning, orphan
reclamation, terminal stall detection, sorted-range ownership fencing, and reserve-floor admission
shaping. These behaviors are unconditional because disabling them restored known compaction livelock,
space leakage, infinite Busy responses, stale-owner acknowledgements, or reserve-floor collapse.
`VALS_ADMIT_BAND_PCT` remains available to tune the admission band percentage.

Treat `VALS_VLOG_GC`, `VALS_SHADOW_EST`, and `VALS_SHADOW_FOLD` more cautiously: they are intertwined
with capacity diagnosis and should be removed only after a focused floor-stack audit.

## Tier 2: large or uncertain items needing a short decision gate

### Balanced sorted-range executor and opaque placement tokens — validate immediately or remove

- Gates: `VALS_PD_OPAQUE_RANGE_MAPS` and `VALS_PD_BALANCE_EXECUTE`; both default off.
- Status: planner local/debug gates passed, but task 0182's destructive hardware A/B has not run because
  the lab fleet was not explicitly reserved. Canonical docs explicitly say no hardware result is
  implied.
- Cost: commits `48e059a` and `caa76fc` introduced roughly 2,000 product-source lines; later executor
  integration in `26ca672` adds substantial state-machine and wire/test surface. The subsystem also
  complicates range-map versions, capability negotiation, statistics polling, Raft intents, and
  re-home serialization.
- Benefit: plausible mitigation for the measured placement-skew capacity wall, but still unproved.
- Recommendation: time-box the decision. If task 0182 can be authorized now, run one short control and
  execution gate plus the required outage/restart checks. If not, remove executor, opaque map upgrade,
  capability/wire branches, and balance-only metrics. Retain `pd_node_load_skew_pct`, which is useful
  observability independent of automatic movement. Do not leave this indefinitely in limbo.

### Key-ordered vlog co-compaction — one current workload A/B, then likely remove

- Gate: `VALS_VLOG_CO_COMPACT=1`; default off.
- Evidence: hardware showed 64× fewer scan device reads but only about **1.61× scan wall-time** because
  decode is CPU-bound. The implementation was demoted from recurring default to opt-in, at-most-once
  bottommost clustering. Current operational runbooks do not enable it.
- Cost: the original implementation was roughly +1,222/-130 product-source lines, although victim
  reclaim and liveness metadata are now shared with mandatory space management.
- Recommendation: run a short aged-corpus scan A/B on current code and the actual expected scan
  workload. If scans are not operationally important or the present benefit remains modest, remove
  `VlogRewrite::All`/co-clustering while preserving victim-targeted reclaim and `value_seg_bytes`.

### Digest hedging — validate demand, then keep or remove

- API: `Coordinator::with_digest_hedge`; default absent. Production includes `HedgeClock`, a service
  ticker, hedged reserve collection, and metrics.
- Evidence: under an induced 5 ms slow replica, hardware p99 improved about 35% and p99.9 about 27%; no
  saturated peak regression. An initial 10% latency-bound overhead was fixed in task 0082.
- Cost: initial implementation added roughly 819 product-source lines, plus the later waiter fix.
- Use: no nearby consumer enables it; only benchmarks do.
- Recommendation: ask whether replica-tail hedging is part of the supported client contract. If yes,
  keep and expose it through a real client profile/configuration. If no present consumer needs it,
  remove rather than carrying a second quorum collector and timer subsystem indefinitely.

### Hot-range split telemetry/policy — retest or remove policy, possibly retain telemetry

- Gate: `VALS_PD_RANGE_LOAD_SPLIT_OPS`; default 0.
- Evidence: task 0118 has deterministic compatibility/integration tests but no retained hardware result
  showing a hot, small range became faster after a split/re-home. A metadata split initially inherits
  placement, so benefit depends on the separately complex re-home path.
- Cost: commit `7b54138` added roughly 509 product-source lines, a new RangeStatsV2 shape, hot-path
  counters, PD policy, and compatibility fallback.
- Recommendation: run a 15-minute skewed point-load A/B. If no material per-core/cluster gain, remove
  the automatic load trigger. Retaining ops/s telemetry is optional only if operators actually use it.

### Auto-merge of under-full sorted ranges — likely remove policy

- Gate: `VALS_PD_RANGE_MERGE_THRESHOLD`; default 0.
- Evidence: correctness and anti-thrash tests exist, but no real shrinking-zone workload or hardware
  benefit is documented. It can undo intentional presplits and changes placement.
- Recommendation: unless a production workload deletes enough keys to create harmful range-map
  fragmentation, remove the automatic policy and retain the explicit admin `merge-zone` mechanism.

### Cleanup `min_confirms` — audit and probably remove the tunable

- Gate: `VALS_CLEANUP_MIN_CONFIRMS`; default requires all owners.
- Evidence: implemented as a deferred micro-follow-on; no deployment usage or hardware benefit appears
  in current artifacts. Lower values weaken release confirmation policy.
- Recommendation: keep the conservative all-owner default as fixed behavior unless a real topology
  requires quorum release; remove the environment variable. Do not remove the surrounding ReadGate
  quiesce/correctness work.

### Optional client availability/backpressure additions — consumer review, then prune

No nearby consumer uses the following public opt-ins:

- `with_node_credits` (per-node credit sub-limit),
- `with_digest_hedge`,
- `with_failure_detector`,
- `with_hint_store`,
- `write_sloppy` / `delete_sloppy`,
- `with_proactive_refresh`.

Several are coherent parts of the Dynamo design, so absence of use is not by itself proof of no value.
However, they enlarge the public API and maintain independent state machines. Decide which deployment
profile vals actually supports. At minimum, remove the per-node credit limit if no user needs it;
retain the basic bounded hint store and strict read/write path. Sloppy quorum should stay only if the
project still claims and tests the explicit availability mode.

### Live drive hot-swap — likely remove public online path

- API: `NodeHandle::rebalance_drives` / `NodeRebalancer`; no `valsd` admin/config integration and no
  non-test caller.
- Cost: introducing commit `e0b3d8d` added roughly 879 product-source lines, followed by durability
  hardening.
- Current scope: the non-zoned `serve_multidrive` library path shipped; the zoned/JBOD daemon path
  has no operator integration and remains offline-only.
- Recommendation: if hot-swap is not going to be exposed and operated, remove the online serving path
  while preserving offline crash-journaled relocate. Otherwise add the missing operator surface and
  box validation; do not keep a library-only production claim.

## Tier 3: high-value optional profiles to retain, but productize

### Blind append — keep, complete rollout decision

- Productized by task 0194 as daemon `profile = latency`; capacity remains the default.
- Evidence: mature overwrite median improved **1.57×**, mean **1.70×**, write-path probes fell to zero,
  read p99 did not regress, and short-run AE/space checks passed (task 0175, three-node SPDK A/B,
  2026-08-11).
- Caution: it changes unconditional `Applied::Won` semantics, and argmax reader/recovery/Merkle
  prerequisites cannot be rolled back after unresolved candidates exist.
- Decision: retained in the named latency profile rather than promoted by default. The
  mixed-profile SPDK soak gate (task 0194: 2 h, three PDs, three SPDK nodes × 8 shards, a restart
  every 10 min, every correctness check passing) passed on 2026-10-22.
  Legacy readers cannot be restored; only the resolving writer is optional.

### Compact winner directory — keep as an explicit memory/performance tier

- Productized by task 0194 as the coupled `profile = latency` index tier.
- Evidence: repeated mature hardware A/B reached **2.51× strong-read throughput**, zero run probes,
  much better p99, and **1.93× mature overwrite throughput**. Cost was 62.9 incremental RSS bytes per
  physical key, an 8.8% fresh-insert penalty, and synchronous O(dataset) open rebuild.
- Cost: introducing commit `3582787` added roughly 1,265 product-source lines and many lifecycle hooks.
- Recommendation: keep because it has proven value, but expose a named `capacity` versus `latency`
  deployment profile with RAM sizing/startup checks. Consider persisting or lazily rebuilding it before
  any default-on discussion.

### Staged scan and client page prefetch — keep, remove rollback branches later

- Gates: `VALS_STAGED_SCAN=0` and `VALS_SCAN_PREFETCH=0`; both default on.
- Evidence: hardware showed 2.09× concurrent-window scan throughput and 1.85× point-get throughput
  during a scan for staged selection; prefetch gave 1.12× full-scan throughput at RF=3 and was neutral
  at RF=1. Identity was value/fingerprint checked.
- Recommendation: keep the features. After a release/soak confidence window, delete the old synchronous
  scan fallback and both kill switches. The fallback itself, not the feature, is cleanup debt.

## Keep as operational tuning, but narrow and document

These are not feature candidates merely because they are configurable: RAM budget, flush byte/interval
limits, merge tier/input/read-ahead parameters, maintenance pacing, re-home copy pacing/deadline, range
split threshold/polling, and SPDK build/location settings. They do need consolidation:

- Mark a small supported set in `docs/deploy.md` and preferably parse it through daemon config.
- Move benchmark-only A/B overrides out of the public operational namespace or hide them in debug/test
  builds where practical.
- Parse process-stable choices once at startup; avoid per-request `std::env::var` (notably staged scan).
- Reject invalid values loudly instead of silently falling back when the setting affects behavior.

## Documentation and status inconsistencies found

Cleanup should fix these regardless of feature decisions:

- `spec.md` current-focus text is stale relative to completed tasks 0106–0111 and newer work.
- `ARCHITECTURE.md` and `docs/multi-drive.md` say live hot-swap remains, while tasks 0059/0065 say the
  library path shipped.
- `ARCHITECTURE.md` still says blind-append hardware validation is separate, but task 0175 contains the
  completed A/B.
- `docs/README.md` says formats are not versioned/frozen, contradicting `spec.md` and the versioned wire/
  on-disk work.
- Some older backlog text calls co-compaction default-on, while current code/docs correctly make it
  opt-in.

## Proposed cleanup sequence

### Phase A — low-risk deletion

Accepted as tasks 0188–0193:

1. Delete write combining and its six variables (0188).
2. Delete merge concurrency permit and `VALS_MERGE_NODE_CONCURRENCY` (completed in 0189).
3. Delete NVMe Deallocate plumbing (completed in 0190).
4. Delete the legacy merge branch (completed in 0191) and legacy on-core flush preparation (completed in 0192).
5. Hard-wire the five mature correctness/liveness fixes on; keep narrow test seams (0193).
6. Update stale docs and produce a supported-settings table as the affected tasks land.

Run full debug workspace tests after each independent removal group; run a short write/read/scan smoke on
SPDK after the phase. These removals should not require a performance A/B because they remove disabled
or known-inferior behavior.

### Phase B — short evidence gates

Use 10–15 minute matched arms, not multi-hour exploration:

1. Co-compaction on an aged sorted corpus: scan throughput/tail and maintenance bytes.
2. Hot-range split on a small skewed range: actual owner/shard movement and per-core throughput.
3. Auto-merge only if a shrinking workload can be named.
4. Digest hedge only if a real client wants the availability/tail profile.
5. Consumer/API audit for per-node credits, sloppy quorum, proactive refresh, and online hot-swap.

Delete anything that fails to show material benefit or lacks an intended user.

### Phase C — resolve the two strategic forks

1. Owner decision: reserve hardware and complete task 0182; balanced execution remains default-off
   until that evidence exists.
2. Owner decision: keep blind append and the winner directory. Task 0194 owns the long mixed-version
   soak and supported `capacity` / `latency` profiles, with the directory included only in the latter.

## Suggested acceptance bar for follow-up cleanup tasks

- Every removed path has its variable/API/docs/metrics/tests deleted together.
- On-disk and wire backward decoders are not removed merely because runtime legacy policy is removed.
- No correctness fix is made optional again through a renamed knob.
- Full debug workspace tests pass; SPDK smoke covers write, strong read, restart, scan, split/re-home,
  and metrics.
- `docs/deploy.md` lists every supported production setting; unlisted developer A/B knobs are gone or
  explicitly internal.

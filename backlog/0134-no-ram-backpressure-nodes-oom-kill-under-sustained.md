---
id: "0134"
title: 'No RAM backpressure: nodes OOM-kill under sustained write load instead of shedding load'
status: done
priority: 1
created: "2026-08-06"
updated: "2026-08-07"
depends_on: []
spec_refs: []
---

## Description
vals has admission control for **device** pressure (maintenance-reserve floor → retryable `Busy`, task 0092) but none for **RAM**. Under sustained write pressure a node's resident set grows from (a) the fully-resident per-key index (~240–400 B/key measured, `docs/out-of-core-index.md`) and (b) an un-budgeted flush/merge working set, until the kernel OOM-kills it.

Measured in task 0132 (two independent full-corpus replays, 2026-08-05/06): nodes 2 and 3 were OOM-killed at 47.1 GiB peak on 60 GiB boxes both times, ~50 minutes in. Setting `VALS_FLUSH_MAX_BYTES=512 MiB` (8× smaller memtable cap) changed neither the RAM trajectory nor the outcome. The node that survived did so only because a *device* region filled first, which pushed it into `Busy` and throttled its intake — RAM pressure got no such treatment. At the same key count (32.8M vs 33.6M `live_keys`) the throttled node held 7.4 GiB and the un-throttled one 17.7 GiB.

Wanted: a per-node RAM budget (configured, defaulting to a fraction of the box's memory) that is observable and that engages the existing retryable-`Busy` backpressure path before the OOM killer does — covering both the index residency term and in-flight maintenance buffers. A node approaching its RAM ceiling should shed load and report it, not die.

Acceptance criteria:
- A configured per-node RAM budget with a documented default; aggregate memtable/flush/merge working set and index residency accounted against it.
- Crossing the budget engages retryable backpressure (`Busy`) and a dedicated counter/gauge, before the kernel OOM killer can act.
- Metrics expose per-node RAM headroom and what is consuming it.
- Reproduction: replaying the 2026-08-05 corpus at RF=2 on 3 × 60 GiB lab boxes degrades into `Busy`/reduced throughput instead of OOM-killing nodes.
- Runbook guidance on sizing keys-per-node against box RAM (~300–400 B/key measured).

## Folded in from task 0133 (per-engine memtable cap), 2026-08-07

`FLUSH_MAX_BYTES_DEFAULT` (4 GiB, `crates/vals-cluster/src/node.rs`) caps the
memtable **per engine**, and its doc comment claims this bounds the flush-build
peak to "~3x = ~12 GiB/node" — true only for a single-engine node. A soak/replay
node serves 4 SPDK partitions x 2 sorted zones = **8 engines**, each with an
independent 4 GiB cap, so the true per-node worst case is ~8 x 12 GiB ~= 96 GiB.
A controlled re-run at `VALS_FLUSH_MAX_BYTES=512 MiB` (verified in
`/proc/<valsd>/environ`) changed neither the RAM trajectory nor the OOM outcome,
so this multiplication is **not** what killed the nodes — but it is a real hazard
and the natural first component of the node-wide budget this task introduces.

Sub-criteria (were task 0133's):
- Per-node aggregate memtable / flush-build RAM is bounded by a single configured
  budget regardless of shard x zone count (e.g. budget divided by live engine
  count, or a shared byte-denominated admission token pool).
- The documented default is derived from the node's engine count; `VALS_FLUSH_MAX_BYTES`
  (or its successor) keeps a clear **per-node** meaning.
- A gauge exposes aggregate memtable bytes and in-flight flush-build bytes per node.
- Unit/integration coverage for the shared budget with many engines.

Evidence: `bench/replay-lab/run-20260806T001507Z-tier-one-flushcap512m/FINDINGS.md`.

## Acceptance criteria

## Plan

Goal: a per-node (per-process) RAM budget that engages the existing retryable `Busy` backpressure before the kernel OOM killer, plus the folded-in 0133 per-node memtable/flush accounting, with observability. The lab replay reproduction criterion is hardware-bound and will be split into a follow-on box-validation task (same pattern as 0142→0151).

Design (session decisions):
1. RAM budget + sampler (new small module `crates/vals-cluster/src/ram.rs` or in observe):
   - `VALS_RAM_BUDGET_BYTES` env; explicit `0` disables. Default: 70% of MemTotal from /proc/meminfo (documented); if MemTotal is unreadable, disabled with a one-time warn log. Optionally min() with cgroup-v2 memory.max when present (implementer judgement; keep simple).
   - Process-global, thread-safe sampler: cached RSS (via existing `observe::process_resident_bytes()`) with a time-gated resample (~100 ms) using atomics — must NOT depend on the AE sweep cadence (gauges gotcha: with AE off, sweep-sampled gauges never update). The sampler also keeps the `process_resident_bytes` gauge fresh.
   - `ram_over_budget()` used at write admission. Pure helper `fn over_budget(rss, budget)` for unit tests + a test override hook that does not rely on mutating process env in parallel tests.
2. Enforcement: in `net/server.rs`, gate client write intake — the fast-path `OwnedRequest::Apply` (~line 1303) and both `BatchApply` arms (~1399/1444), plus the `serve_request` dispatch site arms (~2774 Apply, ~2840 BatchApply). Over budget → increment a new dedicated counter (e.g. `ram_busy_rejections`, distinct from `busy_rejections`) and return `Response::Busy`. Reads/scans/AE/sync/maintenance are NOT gated (maintenance must keep running so memory can drain; repair must not wedge). Add a throttled (≤1/60s) warn log when crossing ~90% of budget and when Busy engagement starts.
3. Per-node memtable/flush budget (folded 0133): `VALS_FLUSH_MAX_BYTES` (default 4 GiB) becomes explicitly a per-NODE aggregate memtable budget. The per-engine flush trigger passed to `zone_maint_due` (node.rs ~1662) becomes `flush_max_bytes_threshold() / max(1, live_engine_count)` where live engine count is a process-global atomic gauge (e.g. `zone_engines`) incremented when a zone store is opened/inserted (`ServerShared::zone_store` and boot-time insertion paths) and decremented on zone teardown — NOT sweep-sampled. Fix the misleading doc comment on `FLUSH_MAX_BYTES_DEFAULT` ("~3x = ~12 GiB/node" claim; node.rs ~1148).
4. Observability of what's consuming RAM (gauges, process-global with the existing published/adjust pattern where sampled, direct adjust where event-driven):
   - `memtable_bytes_total`: Σ engine.memtable_bytes() across zones (sample in `sample_gauges` like live_keys).
   - `flush_build_bytes`: in-flight flush working set — adjust up by the zone's memtable_bytes estimate when a flush is submitted off-core, down when its commit/failure completes (node.rs snapshot_loop flush submit ~1720+ and the off-core commit/fail paths ~1490–1512; one flush in flight per zone, so a per-zone recorded size works).
   - `ram_budget_bytes`: set once at first sampler use (0 when disabled).
   - If the engine's run indexes have a cheap exact/near-exact resident-bytes figure (run index blocks held in RAM), expose `run_index_resident_bytes`; if not cheap, skip — RSS minus the above tells the story. Do not build new bookkeeping machinery for it.
   - Prometheus HELP docs for every new counter/gauge (the only in-code doc prose allowed).
5. Docs: new "RAM budget" section in docs/backpressure.md (env var, default derivation, semantics: retryable Busy on client writes only, maintenance continues; sizing note ~300–400 B/key resident). Runbook guidance in plans/bootes-soak-runbook.md: size corpora at ≤ budget/(~350 B/key) keys/node and set VALS_RAM_BUDGET_BYTES explicitly on 60 GiB boxes (hugepage/MemTotal caveat).
6. Tests:
   - Unit: budget default/parse (parameterized pure fns, not env mutation); over_budget threshold; per-engine divide in zone_maint_due (pass engine count as a parameter or compute threshold at the call site and test the division fn); flush_build gauge adjust bookkeeping.
   - Integration (follow the existing reserve-floor Busy test pattern in net/server.rs tests ~3360+): with a tiny test-overridden budget, Apply and BatchApply answer Busy (and `ram_busy_rejections` increments) while Read still serves; with the override cleared, writes succeed again. Ensure no cross-test env leakage (use the override hook, not env).
   - `cargo test -p vals-cluster` and `cargo check --workspace` green. Hand-format new regions only (no cargo fmt).
7. Follow-on task (created by coordinator): box-validate on lab — replay the 2026-08-05 corpus at RF=2 on 3×60 GiB boxes and confirm degradation into Busy/reduced throughput instead of OOM, then tune the default fraction if needed.

Constraints: no explanatory comments in code (only SAFETY, doctests, Prometheus HELP). Writes must never silently drop; Busy is the only refusal. Maintenance paths (flush/merge/AE/hints/cleanup) must be unaffected by the gate.

### Starting points
- crates/vals-cluster/src/net/server.rs: write dispatch at ~1303 (Apply), ~1399/1444 (BatchApply), second dispatch site ~2774/~2840; sample_gauges at ~630; reserve-floor Busy tests ~3360-3475
- crates/vals-cluster/src/node.rs: FLUSH_MAX_BYTES_DEFAULT ~1148, zone_maint_due ~1219, snapshot_loop flush plan/submit ~1654-1800, off-core flush commit/fail ~1490-1512
- crates/vals-client/src/observe.rs: Counter/Gauge/gauges_named/process_resident_bytes; vals-cluster re-exports it as crate::observe (vals_client::observe)
- crates/vals-store/src/engine.rs: memtable_bytes() ~2817
- docs/backpressure.md, plans/bootes-soak-runbook.md
- gotcha: gauges sampled in sample_gauges follow the AE sweep cadence and read 0 with AE off - the RAM sampler must be self-timed at admission, not sweep-driven

## Work log
- 2026-08-07 plan: Goal: a per-node (per-process) RAM budget that engages the existing retryable `Busy` backpressure before the kernel OOM killer, plus the folded-in 0133 per-node memtable/flush accounting, with observa
…[truncated]
- 2026-08-07 context hints: 6 recorded with plan
- 2026-08-07 context hints: net/server.rs write dispatch: fast-path Apply ~1303, BatchApply ~1399/1444; serve_request arms ~2774/~2840; sample_gauges ~630; reserve-floor Busy tests ~3360-3475; node.rs: FLUSH_MAX_BYTES_DEFAULT ~1
…[truncated]
- 2026-08-07 preload: 3 file(s), ~13 KiB seeded into implementer context
- 2026-08-07 implementer report: Implemented task 0134’s per-process RAM backpressure and folded node-wide flush budgeting.  Changes: - Added `ram.rs` with `VALS_RAM_BUDGET_BYTES` handling (`0` disables; default 70% of `/proc/memin
…[truncated]
- 2026-08-07 review tier: single-opus — reviewers: sol
- 2026-08-07 review (sol): revise — The RAM sampler, budget derivation, metrics, node-wide flush-threshold division, documentation, and direct Apply/BatchApply Busy behavior are generally well implemented. However, the admission gate is
…[truncated]
- 2026-08-07 revision: Addressed both review items.  Changes: - Documented the deliberate wire-level gate breadth in `docs/backpressure.md`: all inbound `Apply`/`BatchApply` intake is shed regardless of origin, including AE
…[truncated]
- 2026-08-07 review (sol): revise — The revision correctly preserves retryable `Busy` through re-home mirror responses, so the prior non-retryable-error problem is fixed. It does not, however, address the other half of the prior major f
…[truncated]
- 2026-08-07 revision: Addressed the third review round.  Changes: - Kept wire-level hint replay and cleanup handoff gated, per the final design decision. - Changed re-home mirror accounting so `MirrorOutcome::Busy` returns
…[truncated]
- 2026-08-07 review (sol): accept — The revision now handles RAM pressure consistently across client and bounded repair/re-home intake, preserves retryable Busy end-to-end for mirror writes, and avoids treating transient RAM Busy as a h
…[truncated]
- 2026-08-07 decision: accept — commit: 0134: per-node RAM budget with retryable-Busy backpressure — VALS_RAM_BUDGET_BYTES (default 70% MemTotal, 0 disables; self-timed 100 ms RSS sampler) sheds ALL wire Apply/BatchApply record intake (cl
…[truncated]
- 2026-08-07 usage: 13,406,100 tok (in 1,626,373, out 83,087, cache_r 15,212,678, cache_w 424,904) · cost n/a (unpriced)
  implementer: 10,306,608 tok (in 947,019, out 31,717, cache_r 9,327,872, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 3,062,142 tok (in 679,270, out 14,104, cache_r 2,368,768, cache_w 0) · cost n/a (unpriced)
  coordinator: 37,350 tok (in 84, out 37,266, cache_r 3,516,038, cache_w 424,904) · cost n/a (unpriced)

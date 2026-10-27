---
id: "0091"
title: Move memtable-flush on-core work (sync + snapshot/serialize prep) off the serving core to stop 6× apply service-time inflation under maintenance
status: done
priority: 3
created: "2026-07-08"
updated: "2026-07-08"
depends_on: []
spec_refs: []
---

## Description
From task 0087's attribution (bench/t0087/notes.md): during write bursts, per-op `apply` service time on the single serving core inflates ~18 µs → ~120 µs when background maintenance (memtable flush prep — sync log, snapshot+serialize memtable, reserve segments — and vlog inflight drains) contends the core. With ~16 pipelined clients this yields the flat ~2 ms client latency / ~7k writes/s floor regime. Run-merge is already off-core (since 0088), the flush run-write is off-core (flush_plan/write_flush/flush_commit), but the flush *prep* and its log sync still run on the serving core.

## Acceptance criteria
- Profile/attribute which on-core maintenance component dominates the apply service-time inflation (flush prep serialize? log sync/drain? commit step?).
- Reduce the on-core share (e.g. incremental/off-core memtable serialization, bounded prep slices) without breaking the flush's durability ordering (log tail synced before the run/manifest commit).
- A/B on hardware: burst apply_mean under active flushes measurably improved vs baseline (bench/t0087/notes.md §2 tier=one table is the baseline); no regression in cluster tests.

## Plan

Two-part session: (A) code change moving the remaining on-core flush-prep work off the serving core plus fine-grained attribution instrumentation, then (B) hardware A/B on oldbox against the bench/t0087 tier=one baseline.

## Part A — code (implementer)

### A1. Attribution instrumentation (the acceptance criterion "profile/attribute which component dominates")
- `Engine::flush_plan` times its sub-phases with `Instant` and carries them on the returned `FlushPlan` (new pub fields or accessors): `sync_us` (the log drain+tail+flush portion) and `snapshot_us` (memtable clone [+sort when on-core]).
- New histograms in `vals-client/src/observe.rs` (auto-rendered on /metrics like sync_flush_us): `vals_flush_plan_us` (whole borrow-held flush_plan call), `vals_flush_plan_sync_us`, `vals_flush_plan_snapshot_us`, `vals_flush_commit_us`, `vals_compact_commit_us`. Observed from vals-cluster: node.rs `snapshot_loop` times the `store.borrow_mut().flush_plan()` call and feeds the plan's sub-phase fields; `maint_loop` times `flush_commit` and `compact_commit`.
- Vlog submit-stall attribution (the suspected device-contention term inside apply): in `ValueLog::submit_block`, when the inflight-full spin loop is actually entered (len >= MAX_INFLIGHT_WRITES), accumulate elapsed µs + count into process-wide `AtomicU64`s in vals-store (new tiny pub API, e.g. `vals_store::vlog_stall_totals() -> (count, us)`); take `Instant::now` only in the blocked branch so the hot path is untouched. Render as counters `vals_vlog_stalls` / `vals_vlog_stall_us_total` on the /metrics endpoint (server.rs).

### A2. Move flush prep off-core
- New `ValueLog` method (e.g. `drain_and_flush_tail`): `drain_inflight` + `flush_tail` WITHOUT the trailing `dev.flush()`. `sync()` stays unchanged for other callers.
- `Engine::flush_plan` default behavior:
  - calls `drain_and_flush_tail` instead of `log.sync` — the NVMe FLUSH moves off-core: the worker's existing final handle-B `flush()` in `build_and_write_flush` is namespace-wide and precedes `WrittenFlush`'s return, hence precedes `flush_commit`'s manifest write, so every covered log record (all completed at plan time via the drain; the partial tail written synchronously by `flush_tail`) is durable before the run is named. Document this durability argument at both ends (flush_plan + build_and_write_flush), incl. per-backend note (SPDK FLUSH is namespace-wide across qpairs; File `sibling` is a dup'd fd so fsync syncs the same file; Mem no-op).
  - snapshots the memtable UNSORTED (no `sort_unstable_by`, no min/max at plan); computes the segment reservation via `run_len_upper_bound_agg(n, key_bytes, max_key)` with the aggregates accumulated during the snapshot loop (byte-identical to `run_len_upper_bound` on the same entries).
  - `FlushPlan` drops plan-time `min_key`/`max_key` (entry_count stays); `build_and_write_flush` sorts the entries (same comparator, `a.key.cmp(&b.key)`) before `build_run` and computes min/max/entry_count into `WrittenFlush` from the sorted vec. `flush_commit` is unchanged (it already reads them from `WrittenFlush`; `value_liveness`/`tombstone_min_version` are order-independent).
- Legacy/baseline path behind env knob `VALS_FLUSH_ONCORE_PREP=1` (matching the codebase's other VALS_* knobs): plan does the full `log.sync` (with device FLUSH) + on-core sort, i.e. current behavior, so one binary can A/B both regimes and the sub-phase histograms attribute each side. Internally plumb the mode as a parameter so tests can exercise both paths without env races.

### A3. Tests
- vals-store/maint: a test proving the off-core flush round-trips with entries snapshotted in unsorted order (insert keys in non-sorted order, off-core flush, assert committed RunMeta min/max/entry_count correct and keys read back through the run). Exercise the legacy (on-core-prep) mode via the internal parameter as well.
- Full `cargo test -p vals-store -p vals-cluster` (plus fmt/clippy per repo norms) must pass; existing offcore flush/compact tests in maint.rs cover the handshake.

## Part B — box A/B (coordinator, after review)
- rsync to oldbox, `VALS_SPDK_LINK=1 cargo build --release -p vals-server --features spdk`; also build valsmix if needed.
- t0087 rig (`~/run/cluster/t0087`, sorted zone smix rf=1, 1 MiB segs, region_mib=1024, sweep=10s): fresh format + 100k preload + six 10s 100%-write bursts (16 thr × 64 pipeline), twice with the SAME binary: baseline `VALS_FLUSH_ONCORE_PREP=1` vs default off-core prep.
- Collect per burst: writes/s, client p50/p90, landed/busy, apply_mean, used_segs, plus the new attribution histograms (flush_plan/sync/snapshot, flush/compact commit, vlog stalls) on both sides. Compare against bench/t0087/notes.md §2 tier=one.
- Write bench/t0091/notes.md: attribution verdict (which on-core component dominated), A/B table, and honest conclusion. If attribution shows the dominant term is NOT flush prep (e.g. vlog submit stalls from device contention with handle-B maintenance I/O), record that and file a follow-on task for the real dominator.
- Leave the box clean (no valsd/valsmix procs, no dpdk/hugepage residue).

## Risks
- Durability-ordering change (removing plan-time FLUSH) is the risky bit → high-powered review tier.
- Memtable clone stays on-core (O(n) but memcpy-bound); if measurement shows it dominates, that's iteration 2 or a follow-on (incremental snapshot).

### Starting points
- crates/vals-store/src/engine.rs: Engine::flush_plan (~line 2522), flush_commit (~2624), FlushPlan/WrittenFlush structs (~146-350), build_and_write_flush (~260)
- crates/vals-store/src/vlog.rs: ValueLog::sync (~1392), drain_inflight (~1004), submit_block spin (~984-1000), MAX_INFLIGHT_WRITES=64 (line 47)
- crates/vals-store/src/run.rs: run_len_upper_bound (~567) and run_len_upper_bound_agg (~584) — order-independent, O(1) aggregate form exists
- crates/vals-cluster/src/node.rs: snapshot_loop flush arm (~2570-2612), maint_loop flush_commit (~2251-2300) and compact_commit (~2301)
- crates/vals-cluster/src/maint.rs: run_job Flush arm (~168) — worker's build_and_write_flush ends with handle-B flush() (the durability barrier)
- crates/vals-client/src/observe.rs: Histograms struct (~421-465), histograms_named (~484) — add new histograms here; server.rs /metrics renders them automatically
- crates/vals-cluster/src/net/server.rs: apply_latency_us observation (~3112) and metrics endpoint rendering (~3230s)
- env-knob precedent: run_compact_threshold() in node.rs (~1702)

## Work log
- 2026-07-08 plan: Two-part session: (A) code change moving the remaining on-core flush-prep work off the serving core plus fine-grained attribution instrumentation, then (B) hardware A/B on oldbox against the
…[truncated]
- 2026-07-08 context hints: 8 recorded with plan
- 2026-07-08 context hints: crates/vals-store/src/engine.rs: Engine::flush_plan (~2522), flush_commit (~2624), FlushPlan struct (~170), WrittenFlush (~213), build_and_write_flush (~260); crates/vals-store/src/vlog.rs: ValueLog::
…[truncated]
- 2026-07-08 review tier: high-powered — reviewers: Claude
- 2026-07-08 review (Claude): accept — Solid, correct implementation of Part A (code + attribution) of task 0091. The change moves the memtable-flush prep off the serving core: `flush_plan` now calls `drain_and_flush_tail` (deferring the N
…[truncated]
- 2026-07-08 decision: accept — commit: store+bench: off-core flush prep + on-core maintenance attribution (task 0091) — flush_plan's sort + NVMe FLUSH move to the maint worker (handle-B flush is the namespace-wide durability barrier befo
…[truncated]
- 2026-07-08 usage: 105,802 tok (in 354, out 105,448, cache_r 13,060,521, cache_w 515,564) · $20.1943
  coordinator: 64,550 tok (in 142, out 64,408, cache_r 7,748,029, cache_w 371,026) · $15.6077
  implementer: 31,160 tok (in 144, out 31,016, cache_r 4,214,711, cache_w 89,512) · $3.4429
  reviewer:Claude: 10,092 tok (in 68, out 10,024, cache_r 1,097,781, cache_w 55,026) · $1.1437

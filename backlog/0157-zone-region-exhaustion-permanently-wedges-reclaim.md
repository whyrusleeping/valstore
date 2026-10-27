---
id: "0157"
title: Zone-region exhaustion permanently wedges reclaim and blackholes a replica pair (backlinks soak wall at 65 min)
status: done
priority: 1
created: "2026-08-09"
updated: "2026-08-09"
depends_on: []
spec_refs: []
---

## Description
Found by the 0130 gate A/B soak arm (`bench/bootes-soak-lab/run-20260808T223525Z-gate-ab/FINDINGS.md`): 65 minutes into a fresh 300-way bootes soak (37.3M records / 156.3M applies), the backlinks (zone 2) vlog allocator hit `free_segments = 0` on **both replicas of the hot placement pair** (n2+n3, within 40 ms of each other). Terminal state, zero recovery over 2 h of observation:

- Client writes to that range: reserve-floor `Busy` from both replicas forever → W=2 quorum permanently unreachable → producer retry loop (238 attempts) and total pipeline stall.
- Reclaim deadlock: the run-compaction plan needs an index snapshot segment (`SnapshotError::DeviceFull`, `snapshot_io.rs:111` uses `Allocator::allocate` which only fails at 0 free) and retried every 1 s for 2 h; `segments_freed` frozen. The maintenance reserve floor (`Engine::maint_reserve_segs`) sizes the data-path reserve for flush (+ merge under dead-fraction pressure) but did NOT keep reclaim viable at the floor.
- Space accounting does not close: at wedge, n2 reported only ~40.6k used segments (~40 GiB) node-wide while records-zone values alone were ~19.2 GB — inconsistent with a fully consumed 32 GiB (32,768-segment) zone-2 region. Either `used_segments` undercounts/stales on a wedged engine, or the zone-2 allocator hit free=0 well below region capacity (sizing/leak bug), or small-value runs have severe segment-granularity space amplification (node-wide measured 1.57×). Note: this also casts doubt on the earlier "lab scratch device-capacity overflow" attribution from the 2026-08-05 replay runs — same signature.
- Blast radius is RF-wide by construction: both replicas of a range fill in lockstep, so this is the *norm* at capacity, not bad luck; with re-home default-OFF and populated-range splits inheriting placement there is no operator path back short of a wipe.
- Evidence: run dir artifacts + full node logs on the boxes (`~/vals/run/node.log`); on-device zone state preserved until the next `format=true` start.

Acceptance criteria:
- Root cause of free=0 established with a local repro (file-device engine, small-value unique-key ingest through frequent batch flushes): live-capacity vs run/snapshot segment-granularity amplification vs allocator sizing/gauge bug.
- Reclaim can always make progress at the floor: the reserve (or an equivalent mechanism) guarantees the compaction/snapshot path its segments, or compaction degrades gracefully instead of retry-looping at DeviceFull; the 1/s WARN spam loop is bounded/backoff'd.
- Per-zone (and ideally per-shard) used/free segment gauges exported so an approaching wall is visible in /metrics before writes stop.
- A defined terminal behavior for a genuinely-full zone region: a distinguishable error/status (not indistinguishable-from-transient Busy), documented operator guidance (grow region / re-home / split policy), so clients and operators can react.
- The 0130 soak reproduces past 65 min without wedging on this wall (or the wall is shown to be genuine capacity with correct accounting and clear signals).

## Acceptance criteria

## Plan

## Analysis (from code reading, to be confirmed by repro)

The wedge signature: `free_segments = 0` on an armed engine, then `compact_plan_*` fails forever with `SnapshotError::DeviceFull` ("no free segments for snapshot", from `run::allocate_run_segs` / `snapshot_io::write_chain`), retried 1/s by the sweep loop (node.rs ~1724), while puts return Busy forever. Candidate holes that let free reach 0 despite `arm_maint_reserve()`:

1. **Reserve collapse window**: `Engine::maint_reserve_segs()` returns only `flush_segs` when not "under pressure"; `flush_segs` is 0 when the memtable is empty. `flush_commit_step` calls `refresh_data_reserve()` right after the memtable drains → reserve drops to ~0 → data path (`vlog roll`, `allocate_data`) admits writes down to literally 0 free. Ratchets to free=0 over repeated flush cycles.
2. **Pressure gate can never engage**: `under_pressure` requires `lsm_dead_fraction() >= 0.10`; that function returns `None` if ANY run lacks `value_bytes` metadata (`or.meta.value_bytes?`) → merge reserve silently disabled. Also unique-key ingest keeps dead fraction low until late, and by the time it crosses the gate, free is already below the newly computed reserve (set_data_reserve sets a floor but can't un-allocate).
3. **Up-front whole-merge reservation**: `compact_plan_inner` reserves the FULL output bound (`sum of input run handle.len`, plus `2*value_bytes+2` segs for vlog rewrite) via unchecked `alloc.allocate()` before doing anything. At/near the floor this can never succeed and there is no smaller fallback → permanent retry loop, no graceful degradation, WARN spam 1/s.
4. **No observability**: gauges only export node-wide `used_segments`, sampled on the sweep cadence (can go stale when wedged — likely explains the "accounting doesn't close" anomaly in FINDINGS.md).

## Work plan (single session, vals-store + vals-cluster + vals-server + docs)

### 1. Repro test (root cause) — vals-store
Add engine test(s) on `MemBlockDevice` with a small region, `set_maint_reserve_enabled(true)`, mimicking the node maintenance policy (flush when memtable large, merge when run_count ≥ threshold, pressure plans at low free):
- Unique-key small-value ingest through repeated flush cycles must currently drive `free_segments()` to 0 (demonstrating the collapse-window ratchet), after which `compact_plan_pressure` returns `SnapshotError::DeviceFull` forever while `reclaimable_fraction() > 0` — the wedge. Write the test to characterize the wedge first (it will initially demonstrate the bug; after the fix it must assert the fixed behavior: free never reaches 0, reclaim stays plannable or a distinguishable terminal error is returned).
- Also cover hole 2: a run without `value_bytes` must not silently zero the merge reserve.

### 2. Fix — vals-store engine
- **Non-collapsing viability floor**: when `maint_reserve_enabled`, `maint_reserve_segs()` must never fall below a floor that keeps maintenance viable regardless of memtable emptiness or dead-fraction gate once free is at/below the pressure threshold — at minimum: manifest snapshot chain segs + smallest viable merge headroom (bounded merge output for a capped input suffix + minimal vlog victim rewrite). Drop/repair the `lsm_dead_fraction()==None` silent-disable (treat unknown conservatively for gating, not with the unbounded 2×live term). Size the merge term to the BOUNDED merge the sweeper actually schedules (inputs capped by `merge_input_cap_bytes`), not 2× total live bytes.
- **Graceful merge degradation**: in `compact_plan_inner`, when the full up-front reservation cannot be satisfied, shrink the input selection (smaller suffix of runs) so the output bound fits the available headroom (leaving manifest-chain slack); only if even the minimal 2-run (or 1-run pressure/GC) merge cannot reserve, return `EngineError::InsufficientHeadroom { needed, free }` (already exists and is handled as a "paused" state in node.rs) instead of `Snapshot(DeviceFull)` — making the terminal state distinguishable from a transient allocation race.
- Expose per-engine `free_segments()` (a public accessor probably already exists or is trivial).

### 3. Cluster layer — vals-cluster
- **Bounded WARN spam**: throttle the per-zone "compaction plan failed" / headroom-paused warnings to ~1/zone/60s with a counter (`compaction_plan_failures` or reuse `compaction_headroom_skips`), following the existing PD throttled-warn pattern.
- **Distinguishable terminal signal**: when a write fails DeviceFull/Backpressure AND the engine reports reclaim non-viable (e.g. new `Engine` predicate: free at/below floor and no plannable compaction), respond `Response::Err("zone region exhausted: …")` (wire variant already exists) instead of `Busy`, and increment a new counter (e.g. `region_exhausted_rejections`). Keep plain Busy for the ordinary transient floor. Be conservative: only divert to Err when genuinely terminal, so existing retry semantics stay intact for transient states.
- **Observability**: extend `sample_gauges` + `observe.rs` with `free_segments` (node-wide total) and `min_zone_free_segments` (min across zone engines) gauges so an approaching wall is visible in /metrics before writes stop. (Flat gauges — the /metrics renderer has no label support; per-zone breakdown via min+total is the pragmatic fit.)

### 4. Docs
Add an operator section (docs/backpressure.md or docs/zones.md, whichever fits the existing structure) describing: the reserve floor and reclaim guarantee, the region-exhausted terminal state and its signals (metric names, Response::Err text), and recovery guidance (raise `region_mib`, enable re-home, split policy caveats).

### Out of scope (existing/follow-on tasks)
- Hardware soak re-validation past 65 min = task 0130 (blocked on this landing).
- If the repro leaves the 1.57× segment-granularity space-amplification question open, file a follow-on task rather than expanding this one.

### Verification
`cargo test -p vals-store -p vals-cluster -p vals-server` (plus targeted new tests); hand-format only new regions (tree is not rustfmt-clean).

### Starting points
- crates/vals-store/src/engine.rs: maint_reserve_segs (~2920), refresh_data_reserve call sites, compact_plan_inner (~2382), compact_step_filtered (~3119), EngineError::InsufficientHeadroom (~900), existing maint-reserve tests (~7859-8046)
- crates/vals-store/src/alloc.rs: Allocator::allocate vs allocate_data + data_reserve floor
- crates/vals-store/src/run.rs:843 allocate_run_segs -> SnapshotError::DeviceFull
- crates/vals-store/src/vlog.rs:975 roll(): Backpressure vs DeviceFull at the floor
- crates/vals-cluster/src/node.rs: sweep/flush/compact scheduling ~1664-1789, compaction retry WARN ~1724, zone_maint_due ~1216, PRESSURE_FREE_FRACTION=0.50
- crates/vals-cluster/src/net/server.rs: sample_gauges ~644, is_backpressure/is_device_full -> Response::Busy mappings
- crates/vals-client/src/observe.rs: Gauges struct + gauges_named (flat, no labels)
- crates/vals-client/src/wire.rs:275 Response enum already has Err(String)
- bench/bootes-soak-lab/run-20260808T223525Z-gate-ab/FINDINGS.md: full failure evidence
- gotcha: lsm_dead_fraction returns None if any run lacks value_bytes -> under_pressure silently false
- user pref: no explanatory comments in code; Prometheus HELP text in observe.rs is the exception

## Work log
- 2026-08-09 plan: ## Analysis (from code reading, to be confirmed by repro)  The wedge signature: `free_segments = 0` on an armed engine, then `compact_plan_*` fails forever with `SnapshotError::DeviceFull` ("no free s
…[truncated]
- 2026-08-09 context hints: 11 recorded with plan
- 2026-08-09 context hints: engine.rs: maint_reserve_segs ~2920, refresh_data_reserve sites (1242,1506,1576,2151,2321,2786,3485), compact_plan_inner ~2382, compact_step_filtered ~3119 (only for run_count==0 zones), MAINT_RESERVE
…[truncated]
- 2026-08-09 preload: 3 file(s), ~20 KiB seeded into implementer context
- 2026-08-09 implementer report: Implemented task 0157 across vals-store, vals-cluster/vals-client observability, server behavior, and docs.  Root-cause/repro findings: - Added a small-region unique-key/frequent-flush/recurring-merge
…[truncated]
- 2026-08-09 review tier: single-opus — reviewers: sol
- 2026-08-09 review (sol): revise — The change adds useful observability, throttled logging, documentation, and a nonzero reserve, but the core reclaim guarantee is not yet established. The reserve can still become unsatisfiable across
…[truncated]
- 2026-08-09 revision: Addressed all four review findings.  Changes: - Projected reserve sizing now includes the pending memtable flush output in the capped post-flush merge suffix whenever an existing run set will gain the
…[truncated]
- 2026-08-09 review (sol): revise — The revision fixes the previously identified no-op pressure plan and improves terminal classification and file-device coverage. One reserve-collapse case remains, however: a first flush performed at t
…[truncated]
- 2026-08-09 revision: Addressed the remaining reserve-collapse case and whitespace check.  Changes: - `maint_reserve_segs()` now gates merge-index and minimal victim-rewrite headroom on the projected post-flush run set (`p
…[truncated]
- 2026-08-09 review (sol): accept — The revision addresses the remaining reserve-collapse path: reserve sizing now uses the projected post-flush run set even for the first flush, and a file-device regression verifies one-run victim recl
…[truncated]
- 2026-08-09 revision: Fixed the low-fill regression and verified the complete affected test surface.  Changes: - Capped the computed maintenance reserve at 25% of the engine’s segment count. The reserve still computes fl
…[truncated]
- 2026-08-09 review (sol): revise — The new 25% reserve cap reintroduces the core failure mode that the prior revision had eliminated. It makes the configured floor smaller than the minimal computed maintenance plan for large run indexe
…[truncated]
- 2026-08-09 revision: Implemented the half-region reserve model and terminal capped-state handling.  Changes: - Raised the principled reserve cap from 25% to 50% of engine segments: `computed.min(num_segments / 2)`. This k
…[truncated]
- 2026-08-09 review (sol): revise — Capped states are now surfaced to clients as terminal, which addresses the previous review. However, pressure selection can still enter a permanent Busy state when the configured merge-input cap selec
…[truncated]
- 2026-08-09 revision: Aligned floor-pressure planning with the reserve and terminal predicate.  Changes: - `PlanKind::Pressure` now expands a one-run capped suffix to the minimal two-run suffix whenever the engine is at th
…[truncated]
- 2026-08-09 review (sol): revise — The planner and reserve are now aligned for the no-victim input-cap case, but the unconditional two-run floor rule discards a valid smaller reclaim path. A multi-run zone can be declared terminal even
…[truncated]
- 2026-08-09 revision: Fixed pressure-plan ordering so viable one-run reclaim is never sacrificed for a two-run merge.  Changes: - Floor-pressure selection now starts with the normal capped suffix. - If the capped suffix is
…[truncated]
- 2026-08-09 review (sol): revise — The capped-one-run victim case and environment cleanup are fixed, but the same viable-victim path is still lost when the normal capped suffix initially contains two or more runs. In that state the pla
…[truncated]
- 2026-08-09 revision: Resolved the planner/predicate asymmetry in both directions.  Changes: - Floor-pressure planning now computes the viable newest-run victim alternative regardless of the initially capped suffix size. -
…[truncated]
- 2026-08-09 review (sol): accept — The revision resolves the planner/predicate asymmetry: floor-pressure planning now evaluates the newest-run victim alternative regardless of the initially selected suffix, permits shrinking to that on
…[truncated]
- 2026-08-09 decision: accept — commit: 0157: zone-region exhaustion no longer wedges reclaim or blackholes replica pairs. Root cause established by local file-device repro: the maintenance reserve collapsed to ~0 after every flush commit (
…[truncated]
- 2026-08-09 usage: 75,830,261 tok (in 7,528,523, out 143,018, cache_r 73,246,643, cache_w 849,553) · cost n/a (unpriced)
  implementer: 70,546,291 tok (in 5,548,505, out 93,082, cache_r 64,904,704, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 5,257,782 tok (in 1,979,944, out 23,822, cache_r 3,254,016, cache_w 0) · cost n/a (unpriced)
  coordinator: 26,188 tok (in 74, out 26,114, cache_r 5,087,923, cache_w 849,553) · cost n/a (unpriced)

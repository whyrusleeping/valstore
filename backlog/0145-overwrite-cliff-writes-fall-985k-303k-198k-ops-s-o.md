---
id: "0145"
title: 'Overwrite cliff: writes fall 985k → 303k → 198k ops/s over repeated overwrite passes as run count grows'
status: done
priority: 1
created: "2026-08-07"
updated: "2026-08-10"
depends_on: []
spec_refs: []
---

## Description
## The measurement

8M keys × 256 B, fresh 3-node lab cluster, the identical `vals-bench netload` run repeated three times (task 0138, `bench/lab-bench-20260806/F-insert-vs-overwrite.txt`):

| pass | write ops/s | node1 `used_segments` |
|---|---|---|
| insert (empty zone) | **985,828** | 1,946 |
| overwrite #1 (same 8M keys) | **302,988** | 4,128 |
| overwrite #2 (same 8M keys) | **197,532** | 6,044 |

3.3× slower on the first overwrite pass, then ~35% worse again on the next. The size sweep shows the same effect at scale: writing 20M keys into a zone that already held 8M ran at **54,087 ops/s** versus ~1M ops/s into an empty zone (`E-size-sweep.txt`).

This matters because every real workload is overwrite-heavy — the bootes soak, the `.vdump` replay, and any steady-state application — so the number that matters is the 200k-50k one, not the ~1M insert number. It also degrades over time, which is exactly the shape of a soak that slowly wedges.

## Why

A put must LWW-resolve the previous version, which searches the runs: the write profile (`prof/unsorted-write.selftime.txt`) shows `DeviceRun::locate_block` 5.9% + `__memcmp_avx2` 6.5% on the *write* path, alongside `HashIndex::put` 10.1% and `HashIndex::get` 4.8%. On an empty zone the blooms reject immediately and the resolve is nearly free; once the key exists, the resolve walks (and reads) real runs.

The cost therefore scales with **run count**, and run count is growing unchecked here: `used_segments` climbs monotonically (1,946 → 4,128 → 6,044) while `vals_run_merges` stayed in single digits across these runs. Recurring merges skip below the min fan-in (`VALS_MERGE_TIER_MIN_K`, default 4), so a workload that rewrites in place accumulates runs faster than the merge policy retires them.

## Directions to explore (measure before choosing)

1. **Make merges keep up.** Instrument runs-consulted-per-put and merge throughput vs ingest rate; then revisit the tiering policy / min fan-in / merge budget so run count stays bounded under sustained overwrite.
2. **Avoid the pre-write resolve.** A put already carries an HLC version; LWW could be settled at read/merge time instead of at apply time (blind append), turning the write path into an append plus index insert. Note `VersionIndex::apply_leaf` (2.8%) and the staged async apply work (tasks 0091/0093/0096) already move in this direction — this would finish the job.
3. **Short-circuit the search.** A per-shard "latest version" cache or a per-run key-range + cuckoo/bloom prefilter could skip most run probes for the common case of overwriting a recently written key.

## Acceptance criteria
- A metric for runs probed per put (and per read), so the amplification is visible in `/metrics` rather than inferred.
- Sustained overwrite throughput no longer degrades pass over pass on the box repeat above (flat, not 985k → 303k → 198k).
- The 20M-into-8M case is materially better than 54k ops/s.

## Artifacts
`bench/lab-bench-20260806/` — `F-insert-vs-overwrite.txt`, `E-size-sweep.txt`, `notes.md` (§ "Follow-up: what actually limits each path"), `prof/unsorted-write.*`.

## Plan

## Diagnosis (from code reading, to be confirmed by measurement)

The engine `put` is already a blind append; the LWW pre-write resolve lives in the cluster layer: `Replica::apply_stage` → `Engine::probe_version` collects a `RangeLocator` for EVERY bloom-positive run (newest-first), then `resolve_run_probe` reads run index blocks in order with early exit on first hit. So an overwrite of a flushed key pays: N× (bloom check + partition_point + locator materialization) + ≥1 device block read, where N = resident run count. Inserts pay ~0 (blooms reject → `Absent`).

Two structural gaps make this degrade pass-over-pass:
1. **Merge scheduling gap**: `zone_maint_due` (node.rs) only schedules a run-merge at `run_count >= RUN_COMPACT_THRESHOLD (8)` absent pressure, while the engine's tiered policy (`select_recurring_merge`, min_k=4) would happily merge earlier. Between 4 and 7 runs, nothing ever merges — resolves pay up to 7 run probes forever, and under sustained overwrite the run count saw-tooths high while merges arrive late and huge (input cap 2 GiB, co-compacting ever more dead vlog), lagging ingest.
2. **No visibility**: nothing exports run counts probed/blocks read per put or per read, nor a resident-run-count gauge (`used_segments` is vlog segments, not runs — the task conflated them for lack of a metric).

## Phases

**Phase 1 — instrumentation (implementer).**
- New counters in `crates/vals-client/src/observe.rs` (server-side shared registry):
  - `apply_probe_runs` — Σ run locators collected for LWW old-version probes (write path: single Apply + BatchApply staging, i.e. `StagedVersion::Runs{probes}` lengths).
  - `apply_probe_blocks` — run index blocks actually read to settle those probes (early-exit visible: blocks ≪ runs when the newest hit wins).
  - `read_probe_runs` / `read_probe_blocks` — same for point reads (`build_get_plan` / digest path).
  - Gauge `resident_runs` — Σ `run_count()` over local zone engines, published in the same loop as `used_segments` (server.rs `publish_gauges`-ish block ~line 677).
- `resolve_run_probe` (vals-store) must report blocks read: add a counted variant (e.g. return `(Option<SnapshotEntry>, usize)` internally or a `resolve_run_probe_counted`), keep existing callers compiling; wire counts at the vals-cluster layer only (vals-store must NOT depend on observe).
- Prometheus HELP text for each (that is the one sanctioned comment surface).

**Phase 2 — measure (coordinator).** Local 3-node file-backend cluster (docs/deploy.md five-minute recipe) + `vals-bench x netload` insert pass then 2+ identical overwrite passes (~2M×256B scaled down). Per pass capture: ops/s, `resident_runs`, probes/put, blocks/put, `run_merges`, merge input/output bytes. Attribute the cliff: run-count growth vs per-resolve block reads vs merge interference.

**Phase 3 — fix (implementer, gated on Phase 2 data).** Expected primary lever: continuous merge scheduling — expose an engine-side "recurring merge selectable" signal (run `select_recurring_merge` over current run sizes) into `MaintSignals`, and let `zone_maint_due` schedule compaction when the tiered policy actually has work (keeping `VALS_RUN_COMPACT_THRESHOLD` as an upper-bound override), so run count stays near min_k under sustained overwrite instead of saw-toothing to 8+. Secondary (only if Phase 2 shows probe-collection CPU dominating): defer locator materialization beyond the first probe. Kill-the-fix tests: with 4..7 runs and no pressure, maintenance now schedules a merge; run count stays bounded across simulated overwrite passes.

**Phase 4 — validate locally.** Repeat Phase 2 A/B (fix on/off): overwrite passes must be flat, not degrading. Full suites: vals-store, vals-cluster, workspace check.

**Phase 5 — hardware (coordinator).** Provision fresh lab 3-node SPDK cluster (plans/box-provisioning-runbook.md + bootes-soak-runbook §0.5–§2), rerun the F repeat (8M×256 insert + 2 overwrite passes) and the 20M-into-8M arm; acceptance: overwrite passes flat, 20M-into-8M materially >54k ops/s; artifacts under `bench/lab-bench-<date>-0145/`. If the session runs long, split Phase 5 into a follow-on task rather than rushing it.

## Constraints
- No explanatory comments in code (HELP strings and SAFETY only); never `cargo fmt` the tree.
- Don't regress write amp: the scheduling change must still respect tiered min-fan-in discipline (no eager pair merges) — the engine-side signal IS the tiered selector, just consulted earlier.

### Starting points
- crates/vals-client/src/observe.rs — counters!{} macro registry, Gauges struct + gauges_named(); apply_run_reads/batch_apply_run_reads already exist (cold entries, not blocks)
- crates/vals-cluster/src/net/server.rs:1950-2140 serve_apply/serve_batch_apply (probe staging + version_from_probe), ~1902 build_get_plan, ~677 gauge publish loop (used_segments)
- crates/vals-store/src/engine.rs:4564 resolve_run_probe (early-exit loop), 4605 probe_version, 4532 probe_get, 641 select_recurring_merge, 616 select_merge_suffix
- crates/vals-cluster/src/node.rs:1214 MaintDue/zone_maint_due (run_threshold=8 via RUN_COMPACT_THRESHOLD:1157), 1677-1725 snapshot_loop maint scheduling
- MaintSignals::sample — find in node.rs or replica.rs
- metrics endpoint: crates/vals-server/src/metrics.rs renders snapshot_named()/gauges_named()

## Work log
- 2026-08-10 plan: ## Diagnosis (from code reading, to be confirmed by measurement)  The engine `put` is already a blind append; the LWW pre-write resolve lives in the cluster layer: `Replica::apply_stage` → `Engine::
…[truncated]
- 2026-08-10 context hints: 6 recorded with plan
- 2026-08-10 context hints: crates/vals-store/src/engine.rs:4564 resolve_run_probe, 4591-4625 version_from_probe/probe_version, 4532 probe_get; crates/vals-cluster/src/net/server.rs:1950 serve_apply, 1999 serve_batch_apply, 1902
…[truncated]
- 2026-08-10 review tier: high-powered — reviewers: sol, claude
- 2026-08-10 review (sol): unknown — reviewer error: turn 1: API returned non-200 status code 400: {"detail":"Unsupported parameter: max_output_tokens"}
- 2026-08-10 review (claude): accept — The change delivers all three acceptance criteria: probe-amplification metrics per put and per read (apply_probe_runs/blocks, read_probe_runs/blocks, resident_runs gauge, all rendering in /metrics per
…[truncated]
- 2026-08-10 decision: accept — commit: 0145: overwrite cliff fixed — merge-when-selectable scheduling + resolve-amplification metrics; sustained overwrite no longer degrades pass over pass. Root cause (pinned by new metrics + local file-
…[truncated]
- 2026-08-10 usage: 103,086 tok (in 378, out 102,708, cache_r 25,096,080, cache_w 279,862) · cost n/a (unpriced)
  coordinator: 90,412 tok (in 342, out 90,070, cache_r 24,185,506, cache_w 210,994) · cost n/a (unpriced)
  reviewer:claude: 12,674 tok (in 36, out 12,638, cache_r 910,574, cache_w 68,868) · cost n/a (unpriced)

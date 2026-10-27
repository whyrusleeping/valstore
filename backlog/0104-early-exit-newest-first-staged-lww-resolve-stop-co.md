---
id: "0104"
title: 'Early-exit newest-first staged LWW resolve: stop cold-read probing at the first confirmed hit'
status: done
priority: 2
created: "2026-07-10"
updated: "2026-07-10"
depends_on: []
spec_refs:
    - scan-performance
---

## Description
t0097 replay profiling (500M-record mark, `prof/20260710T155714Z-replay_500m`): the serving cores are the throughput ceiling (73–92% CPU) and the cold LWW read-before-write path dominates them. Two compounding costs in the staged probe:

1. `Engine::probe_get`/`probe_version` collect a `RangeLocator` for **every** bloom-positive run (newest→oldest), and `resolve_run_probe` then reads **all** of those blocks — **sequentially awaited per key** — taking `argmax(version)`. On the ×3.25 rewrite-history corpus (~60% of records are re-puts, `vals_batch_apply_run_reads` ≈ 0.58/record) a key with V historical versions costs V device block reads instead of 1.
2. `DeviceRun::locate_block` (which inlines `Bloom::contains`: k≈7 *random cache line* probes per run) runs for every run per cold key → 32% of all cycles, plus malloc traffic for the probe Vecs (~6%).

Fix directions (validate the invariant first):
- **Early exit**: probes are ordered newest→oldest; under the put-path LWW read-modify-write invariant the newest run containing a key should hold the node's newest known version (flushes always write the resolved winner; merged runs install at their oldest input's position but only contain keys whose newer copies live in strictly newer runs). If that argument holds — including under anti-entropy/hint replay — stage 2 can stop at the **first confirmed hit** instead of reading every candidate block: expected reads/key drops to ~1 + bloom-FP rate. If AE can violate it, consider a per-run max-HLC watermark to bound the scan instead.
- Overlap (don't sequentially await) the residual multi-probe case within a key.
- SmallVec/reuse for the probe list.

Acceptance: A/B on the t0097 replay corpus (or a truncated t0101-style window): serving-core `locate_block`+resolve share drops materially, rec/s up, staged-vs-sync equivalence tests still green (hot/cold/absent/tombstone/losing/dup/skew), and a kill-the-fix test that an out-of-order older-version write still loses.

## Acceptance criteria

## Plan

## Goal
Cut the cold LWW resolve cost on the serving cores: stop the staged run-probe at the FIRST confirmed hit (newest→oldest) instead of reading every bloom-positive candidate block, plus reduce per-probe allocation traffic.

## Invariant validation (done by coordinator — document it in code)
The early exit is sound because every write path enforces store-if-newer, so for any key the newest run containing it holds its maximal run version:
1. All cluster writes funnel through `Replica::decide_and_store` (strict `version.wins_over(cur)` — a losing/equal version is never stored) or `stamp_and_store` (stamps a version dominating the current resolve). This covers Apply/BatchApply (staged: `apply_stage`/`apply_commit_staged`, with manifest-generation revalidation forcing a re-stage if the run set changed during the await), AE repair, hinted handoff replay, CAS, and local puts/deletes. So a key is admitted to the memtable only with a version strictly greater than every existing run copy.
2. Flush installs the memtable as the newest run ⇒ its copies strictly dominate all older-run copies of the same keys.
3. Merges always select a **contiguous newest-suffix** subset (`select_recurring_merge`; legacy/lone-GC/pressure are bottommost `start == 0`), emit per-key argmax over the inputs, and install at the oldest input's position (`compact_commit`), with survivor flushes landing strictly newer. Contiguity means no run can sit *between* merge inputs, so the dominance order among runs is preserved by every merge.
Note: the bare `Engine::put` is blind (no resolve) — the engine already assumes the store-if-newer discipline from the layer above for GC safety (see `compact_plan_lone_gc` docs); the early exit relies on the same documented assumption.

## Changes (crates/vals-store, small ripple into vals-cluster)
1. `resolve_run_probe` (engine.rs ~5778): return on the first probe whose block actually contains `key` (a block miss = bloom false positive ⇒ continue to the next probe). Update its docs and the `probe_get`/`probe_version`/`StagedGet::Runs` docs to state the dominance invariant and the required caller discipline (store-if-newer). This covers both staged gets and staged version resolves (`version_from_probe`), i.e. the hot Apply/BatchApply cold-resolve path.
2. Deliberately KEEP the sync `Engine::lookup_runs` as full argmax: it is off the hot path and serves as an independent oracle — the existing staged-vs-sync equivalence tests would surface any invariant violation as a divergence. Add a comment there explaining this.
3. Skip the "overlap the residual multi-probe case" idea: with early exit the expected reads/key is ~1 + bloom-FP rate, and sequential awaiting is exactly what lets us NOT read the remaining candidates. Note this in the code doc.
4. Allocation reduction: `RangeLocator.seg_lbas` is a per-candidate-run `Vec<u64>` clone of the run's full segment LBA list. Add the `smallvec` crate to vals-store and make `seg_lbas` a `SmallVec<[u64; 4]>`; in `locate_range`, store ONLY the segments the `[off, off+len)` range actually covers (adjust `off` to be relative to the first stored segment) so a block probe (≤2 segments) stays inline while `locate_sidecar`'s whole-section locator may spill (fine). Keep `RangeLocator::read`'s spanning math correct — add/extend a unit test that a locator spanning a segment boundary still reads correctly. Optionally also make `StagedGet::Runs { probes }` / `StagedVersion::Runs { probes }` a `SmallVec<[RangeLocator; 4]>` if the ripple through replica.rs stays small; skip if messy.
5. Tests:
   - vals-store: key present in TWO runs with different versions (put v1, flush, put v2, flush) — `probe_get` + `resolve_run_probe` over a read-counting device returns v2 with exactly ONE candidate-block read (assert probes.len() == 2 first, so the early exit is actually exercised).
   - vals-store: fallthrough — hand `resolve_run_probe` a probe list whose first locator points at a block NOT containing the key (locate a block via a different key from a run that lacks the target key) followed by a locator that does contain it ⇒ returns the hit, 2 reads (proves an FP doesn't terminate the scan / no false-absent).
   - vals-cluster (replica.rs tests, CountingDevice at ~4604 already exists): kill-the-fix test — write K at v_new, flush; staged-apply an OLDER version (goes through apply_stage → NeedsRunRead → version_from_probe → apply_commit_staged) ⇒ `Applied::Lost`, nothing stored; staged read of K still returns v_new (with the newest-run copy resolved from one block read even though K sits in ≥2 runs).
   - Run the full vals-store + vals-cluster test suites (existing staged-vs-sync equivalence tests: hot/cold/absent/tombstone/losing/dup/skew must stay green).
6. Doc touch: brief entry in docs/scan-performance.md's landed-work notes is optional; skip if it doesn't fit the doc's structure.

## Out of scope (deferred)
Box A/B on the t0097 replay corpus: task 0097 (rebaseline) depends on 0104+0105 and is the designated measurement run — note this in the work log rather than running a multi-hour box job here.

### Starting points
- crates/vals-store/src/engine.rs: probe_get ~5727, resolve_run_probe ~5778, probe_version ~5849, lookup_runs ~2529, compact_plan_inner ~3752 (contiguous newest-suffix selection), compact_commit ~3978 (install at oldest input position)
- crates/vals-store/src/run.rs: RangeLocator ~1633 (seg_lbas Vec), locate_range ~1950, locate_block ~1966, locate_sidecar ~2074
- crates/vals-cluster/src/replica.rs: decide_and_store ~2424, apply_stage ~2477, apply_commit_staged ~2526, stamp_and_store ~2560, CountingDevice test helper ~4607
- cargo test -p vals-store -p vals-cluster (mem devices, no SPDK needed)

## Work log
- 2026-07-10 plan: ## Goal Cut the cold LWW resolve cost on the serving cores: stop the staged run-probe at the FIRST confirmed hit (newest→oldest) instead of reading every bloom-positive candidate block, plus reduce 
…[truncated]
- 2026-07-10 context hints: 4 recorded with plan
- 2026-07-10 context hints: crates/vals-store/src/engine.rs: probe_get ~5727, resolve_run_probe ~5778, probe_version ~5849, version_from_probe ~5831, lookup_runs ~2529, compact_plan_inner ~3752 (contiguous newest-suffix selectio
…[truncated]
- 2026-07-10 implemented: `resolve_run_probe` now returns on the FIRST confirmed hit (newest→oldest; bloom-FP block miss falls through) — run-dominance invariant documented on the fn, `lookup_runs` kept as full-argmax oracle. `RangeLocator.seg_lbas` is a `SmallVec<[u64;4]>` trimmed to only the covered segments (off rebased), and `StagedGet::Runs`/`StagedVersion::Runs` probes are an inline `ProbeList` (`SmallVec<[RangeLocator;4]>`) — cold-key probe builds are allocation-free. New tests: early-exit 1-read (2 candidates), FP fallthrough 2-reads, locator trim/segment-boundary parity, and the replica kill-the-fix (older staged write ⇒ `Lost` after ONE block read; newest copy intact). Full vals-store+vals-cluster suites green. Box A/B deliberately NOT run here — task 0097 (rebaseline, depends on 0104+0105) is the designated measurement run.
- 2026-07-10 implementer report: Task 0104 implemented: early-exit newest-first staged LWW resolve + allocation-free probes.  **Changes** - `crates/vals-store/src/engine.rs`   - `resolve_run_probe` now returns on the **first probe wh
…[truncated]
- 2026-07-10 review tier: single-opus — reviewers: Claude
- 2026-07-10 review (Claude): accept — Excellent, complete implementation of the early-exit newest-first staged LWW resolve. `resolve_run_probe` now returns on the first confirmed hit (bloom-FP misses fall through), soundly justified by an
…[truncated]
- 2026-07-10 decision: accept — commit: 0104: early-exit newest-first staged LWW resolve + allocation-free run probes  resolve_run_probe stops at the first confirmed hit (newest→oldest), sound under the store-if-newer run-dominance invari
…[truncated]
- 2026-07-10 usage: 64,641 tok (in 180, out 64,461, cache_r 6,600,263, cache_w 252,293) · $12.4850
  implementer: 49,458 tok (in 126, out 49,332, cache_r 5,254,796, cache_w 116,613) · $9.1803
  coordinator: 10,613 tok (in 26, out 10,587, cache_r 1,032,860, cache_w 99,877) · $2.8109
  reviewer:Claude: 4,570 tok (in 28, out 4,542, cache_r 312,607, cache_w 35,803) · $0.4938

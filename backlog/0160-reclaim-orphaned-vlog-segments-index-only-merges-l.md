---
id: "0160"
title: 'Reclaim orphaned vlog segments: index-only merges leak fully-dereferenced segments forever (~80% of region in 0159 repro)'
status: done
priority: 2
created: "2026-08-09"
updated: "2026-08-09"
depends_on: []
spec_refs: []
---

## Description
Found while root-causing 0159 with a local file-device repro (see 0159 work log): after overwrite-heavy churn with default index-only merges, **~80% of the region (1663/2048 segments locally; ~30 GiB in the 0159 soak wedge) is orphaned** — vlog segments referenced by NO run's value_segs, no manifest, no open log segment, and no memtable pointer. Mechanism: an index-only merge (co_compacted=false, the default recurring tiered increment) drops superseded entries; when the last entry referencing a vlog segment is dropped, the segment vanishes from the output run's value_segs, but the co_compacted dead-sweep in `compact_commit` only runs for co-compacting merges and only over merged_inputs' value_segs — so fully-dereferenced segments are never freed by anything. They are also invisible to `select_victims` (which only scores run-referenced segments), so pressure reclaim can never touch them. This is the dominant space amplification driver behind 0158's 1.57×/30 GiB gap.

Acceptance criteria:
- An orphan reclaim path for sorted-zone engines: segments not referenced by any run (index or value_segs), the live manifest, the open log segment, memtable-pointed segments, or in-flight plan reservations get freed — either as a sweep at compact/flush commit (where the reference set is already computable) or a periodic bounded scan. Must be safe vs concurrent reads (ReadGate semantics: vlog segment frees don't bump manifest_generation — see 0156 span-restart gotcha) and vs runs with unknown value_segs metadata (skip when any run's metadata is incomplete).
- Repro test: the 0159 rotating-overwrite workload's orphan count stays bounded (near zero after maintenance) instead of growing to ~80% of the region.
- Space accounting closes: used ≈ referenced + bounded slack in the repro.
- `cargo test -p vals-store -p vals-cluster -p vals-server` green.

Relation to 0158: this supplies the attribution and the fix for the soak's unaccounted ~30 GiB; 0158 can validate on hardware after this lands.

## Acceptance criteria

## Plan

## Root cause (already pinned by 0159 repro)
`compact_commit`'s dead-sweep (engine.rs ~2947-2987) runs only for `written.co_compacted` merges and only over merged_inputs' value_segs. Default index-only merges drop the last reference to vlog segments without freeing them; orphans are also invisible to `select_victims`. Recovery (`open_inner` ~1138-1168) marks allocated exactly {live_manifest.segs, run handle.segs, run value_segs, log tail segs ≥ covered_epoch, open segment} — so orphans are reclaimed on reopen; the leak is runtime-only, and that recovery set is the authoritative runtime reference universe (+ in-flight plan reservations).

## Design: full-reference-set orphan sweep at commit/plan points
1. **Env switch** `fn orphan_reclaim_enabled()` next to `pressure_net_gain_enabled()` (~536): default true; `VALS_ORPHAN_RECLAIM=0` restores old behavior (house kill-switch pattern).
2. **`fn referenced_segments(&self) -> Option<HashSet<SegmentId>>`** (shared helper): returns None if ANY run's `meta.value_segs.is_none()` (incomplete metadata ⇒ never sweep). Otherwise the keep set = live_manifest segs (if Some) ∪ every run's handle.segs ∪ every run's value_segs ∪ `log.open_segment()` ∪ `log.tail_segs_through(u64::MAX)` (all tail segs; memtable ptr segs are ⊆ open ∪ tail by construction — same as recovery — so no O(memtable) index scan).
3. **`fn reclaim_orphan_segments(&mut self) -> usize`**: returns 0 unless `orphan_reclaim_enabled()` and `!self.flush_in_flight && !self.compact_in_flight` (staged plans hold allocations only via caller-held plan objects — must not sweep under them) and `referenced_segments()` is Some. Then for each seg in `self.alloc.used_list()` not in keep: `self.alloc.free(seg)` + count. If count > 0, `self.refresh_data_reserve()`. No device zeroing — matches the existing co_compacted sweep's crash-safety envelope (orphans have seq ≤ covered_seq so replay winners filter them; recovery re-marking an epoch-boundary seg is conservative-safe).
4. **Call sites**:
   a. End of `compact_commit`, right after `self.compact_in_flight = false;` (~2988): `segments_freed += self.reclaim_orphan_segments();` so frees flow into `CompactRunsStats.segments_freed` and the existing `segments_freed` Prometheus counter. Keep the existing co_compacted targeted sweep as-is (still covers the flush_in_flight-skip case).
   b. `compact_plan_inner`, `PlanKind::Pressure` only, immediately after the poisoned/in-flight/min-runs guard (~2396-2398): call `self.reclaim_orphan_segments()` before headroom/floor math — so a floor-pressure state with invisible garbage self-heals instead of classifying terminal (0159 interplay), and `at_maintenance_floor`/free_segments see the recovered space.
5. **Test helper** `orphan_segments_for_test(&self) -> Vec<SegmentId>` (same pattern as `last_run_value_segs_for_test` ~3211): allocated used_list − referenced_segments, for assertions.

## Safety notes to preserve
- ReadGate: vlog frees don't bump manifest_generation — established-safe per 0156 (staged span pages restart unconditionally after any yield); freeing MORE segs at the same commit points is the same class. No vals-cluster changes needed.
- 0157/0159 invariants: reclaim only FREES segments (never allocates), so the reserve floor cannot collapse; running it before pressure planning only increases headroom. Existing region_exhausted_*/floor tests must stay green.
- House style: no explanatory comments; hand-format; never cargo fmt.

## Tests (engine.rs mod tests)
1. `index_only_merge_orphans_reclaimed_at_commit`: co_compact_env(false); vlog-routed values, overwrite-heavy across ≥2 flushed runs so an index-only merge fully dereferences segs; after `compact_commit`, `orphan_segments_for_test()` is empty and free_segments reflects the reclaim. Kill-the-fix twin: same sequence under `VALS_ORPHAN_RECLAIM=0` leaves orphans non-empty (proves the assertion detects the defect). Use the existing env-mutex guard pattern (co_compact_env ~4714) for the new env var to avoid cross-test races.
2. Extend `pressure_floor_overwrite_churn_stays_live_without_zero_gain_merges` (~10955, the 0159 repro): after maintenance in each blocked round (or at loop end after a final merge), assert orphan count stays bounded (== 0 after a commit) and space accounting closes: used_segments == referenced set size (used ≈ referenced + zero slack once swept). Don't destabilize its existing assertions.
3. `pressure_plan_reclaims_orphans_at_floor`: fabricate orphans directly (mutate a run's `meta.value_segs` to drop segs while leaving them allocated — pattern of the test at ~8772); call `compact_plan_pressure` and assert free_segments rises by the orphan count via the plan-time hook even when the plan itself is None/InsufficientHeadroom.
4. Suites: `cargo test -p vals-store -p vals-cluster -p vals-server` green (note: `cleanup_quiesces_inflight_reads` release-mode stall failure is pre-existing on this box; debug green is the bar). Fix any tests whose exact used_segments expectations legitimately shift now that orphans free at commit — adjust only with justification.

### Starting points
- crates/vals-store/src/engine.rs ~2947-2987: existing co_compacted dead-sweep in compact_commit (keep/dead set construction to mirror)
- crates/vals-store/src/engine.rs ~1138-1168: open_inner recovery mark set = the authoritative reference universe
- crates/vals-store/src/engine.rs ~2385-2445: compact_plan_inner guards (place pressure-path reclaim after line ~2398)
- crates/vals-store/src/engine.rs ~536 pressure_net_gain_enabled: env-switch pattern; ~4714 co_compact_env: test env-mutex guard pattern
- crates/vals-store/src/alloc.rs: used_list() ~115, free() ~158
- crates/vals-store/src/vlog.rs ~708-734: open_segment / tail_segs_through(epoch<=) / prune_tail_segs_through
- crates/vals-store/src/engine.rs ~10955-11048: pressure_floor_overwrite_churn test (0159 repro) to extend
- crates/vals-store/src/engine.rs ~8760-8790: test mutating run meta.value_segs directly (orphan fabrication pattern)
- flush attaches ALL log tail segs through covered_epoch to the flush run's value_segs (engine.rs ~1805-1815) — so flush never creates orphans; only compact_commit does

## Work log
- 2026-08-09 plan: ## Root cause (already pinned by 0159 repro) `compact_commit`'s dead-sweep (engine.rs ~2947-2987) runs only for `written.co_compacted` merges and only over merged_inputs' value_segs. Default index-onl
…[truncated]
- 2026-08-09 context hints: 9 recorded with plan
- 2026-08-09 context hints: crates/vals-store/src/engine.rs ~2947-2987: existing co_compacted dead-sweep in compact_commit; crates/vals-store/src/engine.rs ~1138-1168: open_inner recovery mark set = authoritative reference unive
…[truncated]
- 2026-08-09 preload: 3 file(s), ~16 KiB seeded into implementer context
- 2026-08-09 review tier: high-powered — reviewers: sol, claude
- 2026-08-09 review (sol): unknown — reviewer error: turn 1: API returned non-200 status code 400: {"detail":"Unsupported parameter: max_output_tokens"}
- 2026-08-09 review (claude): accept — The change implements the planned full-reference-set orphan sweep faithfully and correctly: a kill-switchable `reclaim_orphan_segments` guarded against in-flight flush/compact plans, a conservative `r
…[truncated]
- 2026-08-09 decision: accept — commit: 0160: orphaned vlog segments are now reclaimed — index-only merges no longer leak fully-dereferenced segments forever (~80% of region in the 0159 repro, ~30 GiB in the soak wedge). Root cause: compa
…[truncated]
- 2026-08-09 usage: 69,341 tok (in 100, out 69,241, cache_r 3,664,014, cache_w 156,843) · cost n/a (unpriced)
  coordinator: 52,732 tok (in 76, out 52,656, cache_r 3,293,057, cache_w 106,416) · cost n/a (unpriced)
  reviewer:claude: 16,609 tok (in 24, out 16,585, cache_r 370,957, cache_w 50,427) · cost n/a (unpriced)

---
id: "0069"
title: 'Decouple the run-merge''s three jobs: index-only tiered merges, cluster values at most once, dead-fraction vlog reclaim'
status: done
priority: 2
created: "2026-07-06"
updated: "2026-07-09"
depends_on:
    - "0068"
    - "0088"
spec_refs: []
---

## Description

### Diagnosis (2026-07-09, user-confirmed framing)

A run-merge (`crates/vals-store/src/engine.rs` `compact_plan` → `compact_and_write` →
`compact_commit`) currently bundles **three distinct jobs** into one monolithic rewrite,
fired every `RUN_COMPACT_THRESHOLD` (8) flushes:

1. **Probe-count control** — fold the index runs (SnapshotEntry = key + ValuePtr +
   version, ~40–60 B/entry) so lookups probe few runs. Cheap: index bytes only.
2. **Scan clustering** — the key-ordered vlog co-compaction (`co_compact_values`)
   re-reads **every live value byte of every input run** and re-copies it into fresh
   key-ordered segments. This is ~95 % of merge input bytes at ~1 KiB values.
3. **Vlog space reclaim** — dead values are freed only as a *side effect* of job 2
   (live bytes copied forward → old segments freeable). Co-compaction is today the
   ONLY vlog reclaim mechanism.

Because the policy is "merge ALL resident runs into one" and job 2 rides along every
time, **each live value is re-copied on every merge cycle → total merge traffic is
O(N²/flush_size) over an ingest's life**. Box evidence (bench/bootes-live-1b/notes.md,
the 1B ingest): merges grew 10.5 GB/164 s → 31 GB/1,908 s, fell behind, run count piled
to 8–18, and apply throughput collapsed 170k → 43k rec/s. This is the top structural
perf defect for what the store wants to be (ingest throughput on raw NVMe).

**Measured value of clustering on NVMe (bench/cocompact-box-notes.md):** 64× fewer
device reads, but only **1.61× wall-time scan gain** — the scan is CPU-bound
(~147k keys/s/core decode ceiling); the post-co-compaction scan exactly matches a
from-scratch key-ordered load (1.00× control). Clustering on NVMe buys IOPS/CPU budget
(coalesced spans vs per-record 4 KiB reads under mixed load), NOT seek time. Paying it
O(N) times per value for ≤1.61× (today) is a bad trade. Caveat: if scan CPU is ever
parallelized (task 0029), the 64× device-read reduction could become wall-relevant —
so clustering is "worth at most once per value, opt-in", not "worthless".

### Design legs

**A. Take value co-compaction off the recurring merge path (the O(N²) fix — the big one).**
Recurring merges become **index-only** (entries keep their ValuePtrs; today's
`co_compact = false` fallback path is exactly this shape). Values get clustered **at
most once** — a graduation/bottom-level pass — and preferably only **opt-in** (per-zone
flag or an explicit "optimize zone for scans" maintenance op), since the measured
default benefit is 1.61×. Total traffic goes from values × O(N/flush) to values × ≤1,
plus index bytes × O(log N).

**B. Decoupled vlog reclaim (required once A lands).** With job 2 gone from the merge,
reclaim needs its own mechanism: dead-fraction-targeted segment GC — track per-segment
dead bytes (superseded/tombstoned), rewrite only mostly-dead segments (live survivors
re-appended, index repointed), free the rest. Must preserve the 0084/0085/0089
churn-liveness regime (pressure trigger, maintenance-headroom reserve, retryable Busy):
the pressure path currently relies on co-compaction to self-heal a nearly-full region,
so the replacement reclaim must be plannable from the reserve the same way.

**C. Tiered/leveled index-merge selection (from the 2026-07-09 note).** Merge only
similar-sized runs; the base run joins only when newer tiers' cumulative size ≈ its own
→ per-key index rewrites drop from O(N/flush) to O(log_k N). Fold k≈4–8 runs per
increment (per-merge WA ∝ 1/(k−1): ~5–15 % at k=4–8 vs 63 % at 2). Target ≤ ~2 GB
input / ~1.5 M entries ≈ 10 s worker occupancy per increment. Read-side cost of more
resident runs is largely pre-paid by 0093/0096 staged resolves; run-count-aware
admission is 0092's scope.

**D. Streaming merge writer.** Kill the `merged: Vec` (~224 B/entry RAM spike; 1.8 GB
at 8 M entries) — k-way iterator straight into the run writer, bloom/sidecar built
streaming. Mandatory at 100M+ keys/node. (Index-only merges shrink the spike ~30×
already, but the O(total-keys) materialization should die regardless.)

**E. Merge I/O throughput.** Merges ran at 16–64 MB/s on 3 GB/s drives on the 1B ingest
(QD1-ish completion polling ~50 % of the maint thread, crc::update_hw up to 20.8 %) —
pipeline segment reads at real QD + batch/cheapen CRC. Matters for whatever rewrite
volume remains after A–C.

**F. Write-path clustering (exploratory, user suggestion 2026-07-09).** Make arrival
order approximate key order so clustering is (partially) free:
- *Intra-batch sort*: sort each BatchApply's records by key before the vlog append
  (~1000 × 1 KiB ≈ one segment). Near-free. Big win only when a batch spans a narrow
  key range (dump replay, time-ordered keys — the backfill corpus is largely
  key-ordered already); ~no clustering win for uniform-random ingest (1000 random keys
  sorted still span the whole keyspace).
- *Bucketed vlog append streams*: N open segments, one per key-range bucket, so values
  land roughly clustered by construction — turns clustering from a rewrite problem into
  a placement problem. Costs: open-segment fan-out (group-commit/sync must cover
  multiple tails, recovery/epoch bookkeeping, partial-segment waste). Design-sketch
  only unless A+C leave a measured scan gap.
- *Trivial install for non-overlapping runs*: bulk ingest of sorted, non-overlapping
  input (dump replay/backfill) should install runs without merging at all (RocksDB
  "trivial move" analogue). Makes restore-from-dump nearly rewrite-free.

### Measured sizing inputs (task 0088, bench/t0088/notes.md)

Merge monolith runs at ~0.19 GB/s input under concurrent write load (~0.38 GB/s
quiescent), linear in input size (9.6 GB → 52 s); RAM spike ≈ 224 B per merged entry
(8 M entries → 1.8 GB RSS); merge WA ∝ 1/(run_count−1) — 8-run merges cost ~4.6 % WA vs
~63 % at the 2-run pressure floor, so increments should still fold ≥ several runs.
Liveness is NOT the binding constraint (0085 reserve + 0089 Busy held at every tested
size). NOTE: 0088's WA numbers are constant-live-set *churn*; the O(N²) bites on
*growth* workloads (bootes/backfill), which is what legs A+C fix.

A/B harness: the task-0097 full-corpus replay baseline (bench/t0097/).

## Acceptance criteria
- **No value bytes on the recurring merge path**: steady-state merges are index-only;
  value clustering happens at most once per value (graduation) or is opt-in — design
  choice recorded (spec/docs updated: run-merge, vlog reclaim, clustering lifecycle).
- Total merge traffic quasi-linear on a growth workload: on the 0097 replay corpus,
  cumulative `run_merge_input_bytes` grows ~O(N log N), not O(N²) (no
  monotonically-growing full re-merges in the node logs).
- Vlog reclaim works without co-compaction: churn suites (churn_reclaim,
  churn_reclaim_wedge) + the 0084/0085 pressure regime still hold (no region wedge,
  bounded Busy).
- Merge RAM bounded (no O(total-keys) `merged` Vec).
- Store + cluster suites green; merge stays read-transparent (no Merkle leaf changes).
- Replay A/B vs bench/t0097 baseline: ingest throughput no longer declines with
  dataset size the way the baseline does; note written up in bench/.

## Plan

Implement legs A–D in one coherent change (E and F split into follow-on tasks; the box replay A/B criterion is carried by task 0097, which depends on this one and A/Bs via kill switches on one binary).

## Design decisions (recorded per acceptance criterion 1)
- Clustering (key-ordered vlog co-compaction) becomes **opt-in, at most bottommost**: recurring merges are index-only; `VALS_VLOG_CO_COMPACT=1` opts a node into clustering on *bottommost* merges only; `=0` disables everywhere; default (unset) = index-only recurring merges + victim-targeted reclaim. Rationale: measured default benefit is only 1.61× wall (bench/cocompact-box-notes.md), so pay it at most once per value and only when asked.
- Vlog reclaim decouples from clustering via a **victim-segment rewrite mode** on the existing merge machinery: the merge stays the pointer-update vehicle (runs are immutable, so repointing requires an index rewrite anyway), but value bytes are copied only for entries pointing into chosen mostly-dead victim segments; victims are freed at commit. Pressure regime keeps its trigger/reserve/Busy contract.
- A full-legacy kill switch (`VALS_MERGE_LEGACY=1`) restores merge-all + co-compact-always for the 0097 box A/B.

## Phase 1 — engine merge modes (legs A + B)
1. Replace `co_compact: bool` in `CompactPlan`/`WrittenCompact` with an enum `VlogRewrite { None, All, Victims(set) }`. `compact_and_write`/`co_compact_values`: in `Victims` mode only entries with `ptr.segment ∈ victims` are re-read/re-appended (chunked as today); others keep their ptrs. `compact_commit` frees the victims (all their live entries were repointed) plus the existing All-mode dead-set logic; `None` leaves the vlog untouched (today's plain-merge path).
2. Per-segment vlog liveness: extend `value_liveness` to per-segment Σ ptr.len; `RunMeta` gains `value_seg_bytes: Option<Vec<u64>>` parallel to `value_segs` (manifest encode/decode, backward-compatible: legacy → None → victim reclaim unavailable, fall back to All exactly as today). Populate on flush and merge commit.
3. Victim selection at plan time (O(runs × segs) under the borrow): aggregate per-seg live bytes across **input** runs; a candidate must be referenced only by input runs (automatic for bottommost/all-run reclaim) and be below the covered watermark (memtable/tail never references covered segments — assert this invariant); dead fraction = 1 − live/seg_payload ≥ threshold const (e.g. 0.5, tunable); order by deadness and reserve vlog output = Σ victim live bytes (much smaller than today's all-live bound). Update `compute_maint_reserve` sizing consistently (keep the engine/node PRESSURE_* constant mirroring intact).
4. Policy wiring: recurring `compact_plan` → `None` (index-only); `compact_plan_pressure` → `Victims` (fall back to `All` when per-seg bytes are unavailable or victims can't reach a reclaim target, so a nearly-full region always self-heals — churn suites must hold); `compact_plan_lone_gc` → `None` (dropped values become dead bytes that victim GC later collects).

## Phase 2 — tiered/leveled selection (leg C)
5. `compact_plan_inner` selects a **contiguous newest-suffix subset** of runs instead of all: fold k≈4–8 similar-sized runs per increment with an input-size cap (~2 GB, env-tunable); the base (oldest) run joins only when the newer runs' cumulative size ≈ its own (graduation → bottommost merge). Only bottommost merges may GC tombstones (force `gc_collect_before = None` otherwise — update the merge_sources drop-winner safety docs), cluster (All), or reclaim (Victims).
6. `compact_commit`: install the merged run at the position of the **oldest input** (preserving recency order relative to un-merged older runs) instead of always front. LWW stays correct (lookup_runs is argmax(version)).
7. node.rs: run-count trigger unchanged; the plan picks the subset. Pressure keeps firing the bottommost reclaim plan.

## Phase 3 — streaming merge writer (leg D)
8. Kill the `merged: Vec<SnapshotEntry>`: turn `merge_sources` into a pull iterator (progress guard + drop_winner preserved), feed `build_run_into` from the iterator — bloom sized from the Σ input entry_counts upper bound and inserted incrementally, block index/sidecar accumulated per block as today, min/max/entry_count/tombstone_min tracked streaming. The Victims/All value rewrite happens chunk-wise on the stream (CO_COMPACT_CHUNK) before the run writer. Synchronous `compact_runs`/`merge_all_runs` may keep materializing (test/legacy path) if sharing is awkward.

## Phase 4 — docs + tests
9. Update spec.md + docs (algorithms.md, formats.md for the manifest field, scan-performance.md): run-merge lifecycle (tiered, index-only), decoupled victim reclaim, clustering opt-in-once, kill switches.
10. Tests: victim selection math; index-only merge preserves ptrs/liveness; victim rewrite repoints only victim entries and frees victims; subset-merge install order + lookup equivalence vs merge-all (incl. Merkle read-transparency); non-bottommost merge never drops tombstones (kill-the-fix); streaming-build parity with the old Vec path; manifest round-trip incl. legacy decode; churn_reclaim + churn_reclaim_wedge + 0084/0085 pressure tests stay green; full store + cluster suites.

Fallback: if Phase 3 turns out too risky mid-flight, deliver Phases 1–2 coherent and report — D splits into its own task.

### Starting points
- crates/vals-store/src/engine.rs: CompactPlan/WrittenCompact (~l.395–560), compact_and_write (~l.585), co_compact_values (~l.753), merge_sources (~l.901), compact_plan_inner (~l.3253), compact_commit (~l.3359, vlog dead-set free at ~l.3535), lsm_dead_fraction (~l.3946), compute_maint_reserve (~l.3900), MAINT_RESERVE_* consts (~l.733) — must mirror node.rs PRESSURE_*
- crates/vals-cluster/src/node.rs: run_compact_threshold (~l.1702), PRESSURE_FREE_FRACTION/PRESSURE_DEAD_MIN_FRACTION (~l.1768), zone_maint_due (~l.1852), merge scheduling (~l.2459–2548)
- crates/vals-store/src/run.rs: build_run_into (~l.464, bloom built after data blocks — size from upper bound for streaming), SegmentStreamSink, RunBuildOpts
- RunMeta/manifest encode-decode: rg 'value_segs' crates/vals-store/src — flush path also populates value liveness
- co_compact_enabled (engine.rs l.962): VALS_VLOG_CO_COMPACT kill switch, semantics change to opt-in
- Task 0097 needs a one-binary A/B: keep VALS_MERGE_LEGACY=1 restoring merge-all+co-compact-always
- bench/t0088/notes.md: sizing inputs (0.19 GB/s, 224 B/entry, WA ∝ 1/(k−1))
- memory.md gotcha: vlog reclaim tests must drive the OFF-CORE merge path (compact_plan→compact_and_write→compact_commit), not Engine::compact_runs

## Work log
- 2026-07-09 plan: Implement legs A–D in one coherent change (E and F split into follow-on tasks; the box replay A/B criterion is carried by task 0097, which depends on this one and A/Bs via kill switches on one binar
…[truncated]
- 2026-07-09 context hints: 8 recorded with plan
- 2026-07-09 context hints: crates/vals-store/src/engine.rs: CompactPlan/WrittenCompact (~l.395–560), compact_and_write (~l.585), co_compact_values (~l.753), merge_sources (~l.901, progress guard + drop_winner safety doc), com
…[truncated]
- 2026-07-09 implementer report: Implemented task 0069 legs A, B, C (Phases 1, 2, 4). Leg D (Phase 3, streaming merge writer) deferred to a follow-on per the task's stated fallback — justification below.  ## What I changed  **Manif
…[truncated]
- 2026-07-09 review tier: high-powered — reviewers: Claude
- 2026-07-09 review (Claude): accept — The change correctly implements task 0069 legs A (index-only recurring merges), B (decoupled dead-fraction victim vlog reclaim), and C (tiered newest-suffix selection), plus Phase 4 docs/tests. The en
…[truncated]
- 2026-07-09 revision: Addressed all three review minors (they had been applied during the prior interrupted attempt; I verified each is correctly in place and re-ran the suites).  1. **Orphan-tail union now unconditional.*
…[truncated]
- 2026-07-09 review (Claude): accept — The revision cleanly addresses both prior findings. Leg D is now tracked by a dedicated follow-on backlog task (0100), completing the task's fallback contract. The vlog_reclaim_possible() gating incon
…[truncated]
- 2026-07-09 decision: accept — commit: store: decouple the run-merge's three jobs (task 0069 legs A–C) — recurring merges are index-only tiered newest-suffix increments (select_merge_suffix: fan-in VALS_MERGE_TIER_K≈6, ~2 GiB input c
…[truncated]
- 2026-07-09 usage: 170,633 tok (in 540, out 170,093, cache_r 42,192,929, cache_w 914,251) · $33.7363
  implementer: 130,801 tok (in 436, out 130,365, cache_r 38,566,225, cache_w 520,506) · $25.7976
  reviewer:Claude: 20,929 tok (in 62, out 20,867, cache_r 1,874,641, cache_w 182,073) · $2.5973
  coordinator: 18,903 tok (in 42, out 18,861, cache_r 1,752,063, cache_w 211,672) · $5.3414

---
id: "0001"
title: 'Stage B1: prefix-compress (front-code) the in-RAM index'
status: done
priority: 1
created: "2026-07-02"
updated: "2026-11-11"
depends_on: []
spec_refs:
    - Current focus areas
---

## Description
**Outcome (done 2026-07-02):** `FrontCodedIndex` now backs sorted-zone memtables; measured 218.9→63.6 B/key (3.44×) at realistic fanout. The Arc-sharing blocker below turned out moot. The SPDK box A/B criterion moved to task 0027 (box A/B of this index, done 2026-07-05); the IndexEntry shrink is task 0021 (done). The text below is the original pre-implementation description.

The single-node RAM cap is the in-RAM index (~117 B/key, non-draining; live OOM on a 1.41B-record soak confirmed it). Front-coding measured (deterministic byte accounting, `bench/prefix-index/measure.rs`): **3.2× on `records` (149→47 B/key), 4.1× on `backlinks`** — validated 2026-06-26, not yet implemented. Full design in `docs/out-of-core-index.md` §Stage B.

**Known integration blocker**: `Index::put` returns an `Arc<[u8]>` that the cluster shares, so keys live once today; front-coding the engine index alone breaks the sharing → no net win. Either fold the version map into the front-coded store and drop the Arc-handle API (touches store-if-newer + AE digest serving — every hot-path lookup), or take the low-risk half-win of front-coding only the engine index. Note: the per-key in-RAM version index was **retired** in Stage C (the LSM / on-device-runs stage of `docs/out-of-core-index.md`; C.5a-3/4 are its steps that deleted the cluster's per-key version index) — re-verify how much of the blocker still applies post-LSM before choosing.

## Acceptance criteria
- Marginal per-key index RAM measured (memprof-style, bsky-shaped keys) shows the expected multi-× reduction on sorted zones.
- Point lookup + range scan correctness pinned by tests; full workspace suite green.
- Box A/B on the SPDK cluster: no read/write-CPU regression outside noise (front-coded block scan vs BTreeMap point lookup is the risk). *(Moved to task 0027, done; not met in this task.)*
- `docs/out-of-core-index.md` status updated (B1 implemented; B2 gate outcome recorded).

## Plan

Goal: front-code the resident per-key index RAM on sorted zones — validated 3.2–4.1× (149→47, 210→51 B/key) — attacking the ~117 B/key non-draining cap the 1.41B soak measured.

Steps:
1. RE-BASELINE THE BLOCKER FIRST. The doc's B1 section (written 2026-06-26) assumes the cluster `VersionedStore.versions` shares `Arc<[u8]>` keys with the engine index, but Stage C (C.5a-3/4) retired the per-key in-RAM version index. Establish on current HEAD exactly which structures hold resident per-key bytes: (a) the ordered memtable (`OrderedIndex`) for sorted zones, (b) per-run resident block-index first keys, (c) any remaining cluster-side key copies. Measure with the memprof bench to apportion the 117 B/key. This decides whether the doc's option-1/option-2 fork still exists or front-coding is closer to a drop-in Index impl now.
2. Implement front-coded storage where the bytes actually live (likely: a front-coded leaf encoding for the ordered memtable, and/or front-coded run block-index keys), block=32 per the measured accounting. Keep `Index` trait semantics; if the Arc-handle API must change, prefer the smallest surface that still nets the win.
3. Preserve contracts: point lookup, ordered range iteration (scan + AE digest pages need ordered seek), store-if-newer version check, Merkle leaf accumulators unchanged. Property-test the front-coded structure against a BTreeMap reference over randomized keysets (insert/overwrite/delete/seek/range).
4. Measure: `vals-bench memprof`-style marginal B/key on bsky-shaped 61–130 B sorted keys — target the validated 3.2–4.1× on stored key bytes (overall B/key floor ~40–50 due to IndexEntry, which is follow-on task 0021).
5. Box A/B on the SPDK cluster (oldbox recipe): read/write CPU regression check (front-coded block scan vs BTreeMap point lookup is the named risk), kvchurn value-verified, 0 errors.
6. Update `docs/out-of-core-index.md` (B1 implemented + B2 gate arithmetic with the banked win) and the spec/backlog status.

Full workspace suite + clippy + fmt green throughout; kill-the-fix tests for any correctness-sensitive seam touched.

### Starting points
- docs/out-of-core-index.md §Stage B — the validated design, measured byte accounting, and the Arc-sharing integration blocker (options 1/2)
- bench/prefix-index/measure.rs — the deterministic front-coding byte-accounting harness (block=32, first-key-full + (shared_len, suffix) varints)
- crates/vals-store/src/index.rs — HashIndex/OrderedIndex, Index::put returns Arc<[u8]>, Index::key_handle
- crates/vals-store/src/run.rs — DeviceRun: per-run resident bloom + block index (keys packed in blocks on device already)
- crates/vals-cluster/src/replica.rs — VersionedStore; note C.5a-3/4 retired the per-key in-RAM version index (the Arc-sharing blocker may be smaller than the doc's B1 section assumes — re-baseline first)
- Box validation recipe: rsweep.sh / kvchurn on oldbox (the older SPDK test box; see BACKLOG.md §AE digest serve item for the exact invocation — BACKLOG.md was the free-form backlog before `backlog/`, whose open items became tasks when this task landed; it was deleted 2026-08-01, so it is not in the public history; the invocation `rsweep.sh ... 5s 5000000 2 240` and its pitfalls are recorded in the local-only `bench/b1-box-ab-notes.md` from task 0027)

## Work log
- 2026-07-02 plan: recorded in the Plan section above.
- 2026-07-02 context hints: 6 recorded with plan
- 2026-07-02 IMPLEMENTED (code): `FrontCodedIndex` now backs `new_index(Ordering::Sorted)` (crates/vals-store/src/index.rs) — sorted front-coded block list (32/blk, split at 64), allocation-free `get` scratch, streaming block re-encode on insert/remove. Blocker was moot: Stage C (C.5a-3/4) already retired the cluster per-key version index, so `Index::put`→`()`, `key_handle` deleted, keys `Box<[u8]>`, `versions()`/`RecoveredVersions`→`Box<[u8]>`. Property test vs BTreeMap (20k ops) + edge tests; full store suite (130) + replica (53) + AE (19) green; clippy/fmt clean on touched files.
- 2026-07-02 MEASURED: live marginal index RSS (vals-bench memprof, file backend, 1M 61B keys) — realistic fanout (64 rec/DID) 218.9→66.1 B/key = 3.31×; worst-case unique-DID 195.9→85.7 = 2.29×. Deterministic accounting (bench/prefix-index/measure.rs) unchanged: records 3.19×, backlinks 4.08×. Floor now IndexEntry-dominated (→ task 0021).
- 2026-07-02 REMAINING (follow-on, out of this task's code scope): SPDK box A/B read/write-CPU regression check (front-coded block re-encode/scan vs BTreeMap; file-backend microbench showed insert CPU ~1.9M→0.32M keys/s with no device latency to amortize under — must be validated on the real device path). Docs (`docs/out-of-core-index.md`) + spec updated: B1 implemented, B2 gate recomputed against the banked win.
- 2026-07-02 implementer report: Implemented Stage B1: front-coded (prefix-compressed) the sorted-zone in-RAM index; `crates/vals-store/src/index.rs` replaced `OrderedIndex` (BTreeMap) with a new `FrontCodedIndex` (details in the IMPLEMENTED entry above).
- 2026-07-02 REVISION (reviewer): replaced the `FrontCodedIndex` top level `Vec<Block>` with `BTreeMap<Box<[u8]>, Block>` keyed by each block's first key — eliminates the O(#blocks) `Vec::insert`/`remove` memmove on split/block-drop (at the default multi-GiB memtable cap, ~1M blocks × random-order inserts → a Vec split memmoves tens of MB/insert on the serving core). Block internals + BlockBuilder + streaming re-encode unchanged; block re-key (position-0 insert/remove) = BTreeMap remove+reinsert under the new first key. Dropped the now-redundant `Block.first`/`Block::len`. Added a targeted re-key test; property test (20k ops) + full suite still green (store 131, replica 53, AE 19).
- 2026-07-02 RE-MEASURED after BTreeMap-top: realistic fanout 218.9→63.6 B/key = 3.44× (was 66.1 with Vec — slightly better, dropped first-key dup); worst-case unique-DID 195.8→86.4 = 2.27×. Scaling A/B (unique-DID, CPU-bound file backend): front-coded insert throughput 274k→245k keys/s as keys grow 2M→8M (block count ~60k→250k) — only ~10% drop, i.e. NO O(#blocks) memmove cliff (a Vec top would degrade ~linearly). Absolute insert CPU vs the plain-BTreeMap baseline: ~0.75M→0.27M keys/s @2M unique-DID (inherent O(block) re-encode; validated properly on the device path in the box A/B follow-on).
- 2026-07-02 item 3 (reviewer): chose the doc-note option — added an "O(index) materialization, not for per-page hot use" note to `Index::versions` (the `version_digest_page` per-AE-page whole-memtable walk is a pre-existing O(memtable) shape; a filtered variant would touch the engine, which the reviewer asked to leave). Flagging for a possible follow-on if the box shows it.
- 2026-07-02 revision: Addressed all three review items; the main one changed `FrontCodedIndex`'s top level from `Vec<Block>` to `BTreeMap<Box<[u8]>, Block>` to eliminate the O(#blocks) memmove (see the REVISION and item 3 entries above).
- 2026-07-02 review tier: single-opus — reviewers: Claude
- 2026-07-02 review (Claude): accept — The change correctly and completely implements Stage B1: a front-coded (prefix-compressed) FrontCodedIndex backs sorted zones, and the now-moot Arc-key-sharing blocker is cleanly removed (`put`→`()`, `key_handle` deleted).
- 2026-07-02 decision: accept — commit: perf(store): Stage B1 — front-code the sorted-zone in-RAM index (3.4× less resident index RAM). The commit also converted the legacy BACKLOG.md open items into `backlog/` tasks and recorded: SPDK box A/B tracked as task 0027; IndexEntry shrink is task 0021.
- 2026-07-02 usage: 172,391 tok (in 450, out 171,941, cache_r 30,370,225, cache_w 557,123) · cost n/a (unpriced)
  implementer: 139,785 tok (in 302, out 139,483, cache_r 24,480,315, cache_w 250,143) · cost n/a (unpriced)
  coordinator: 21,286 tok (in 82, out 21,204, cache_r 4,427,409, cache_w 243,904) · cost n/a (unpriced)
  reviewer:Claude: 11,320 tok (in 66, out 11,254, cache_r 1,462,501, cache_w 63,076) · cost n/a (unpriced)
- 2026-07-02 decision: accept — commit: docs(backlog): finalize task 0001 metadata; revert stale 0027 in_progress flip
- 2026-07-02 usage: 47,129 tok (in 94, out 47,035, cache_r 3,743,079, cache_w 187,550) · cost n/a (unpriced)
  implementer: 41,458 tok (in 72, out 41,386, cache_r 3,017,925, cache_w 112,338) · cost n/a (unpriced)
  coordinator: 5,671 tok (in 22, out 5,649, cache_r 725,154, cache_w 75,212) · cost n/a (unpriced)
- 2026-11-11 groom: added an outcome summary to the Description; glossed Stage C / C.5a-3/4, oldbox and BACKLOG.md; marked the box A/B criterion as moved to 0027; removed a duplicate empty Acceptance criteria heading; replaced cut-off work-log entries (plan and context hints are in the sections above; implementer/revision/review lines trimmed to complete text; decision line restored from the pre-release commit message).

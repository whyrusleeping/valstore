---
id: "0028"
title: 'Key-ordered value clustering: vlog co-compaction in key order (scan-sequential aged data)'
status: done
priority: 2
created: "2026-07-02"
updated: "2026-07-04"
depends_on: []
spec_refs:
    - Current focus areas
---

## Description
The 2026-07 scan-performance pass (see `docs/scan-performance.md`) made scan pages read their values through LBA-coalesced **spans** (`ValueLog::plan_read_spans`): records that sit in the same or adjacent vlog blocks are fetched in one device command. On key-clustered data (bulk-loaded in key order) an ordered scan collapses into sequential I/O and runs at millions of keys/s per core. But on *aged* data — written in arrival order (e.g. the firehose corpus) — values are scattered in the vlog, spans degenerate to ~1 record per read, and scans stay bound by ~random-4K-read rate per serving core.

**The architectural fix**: when run-merge compaction rewrites runs (`compact_runs`), co-compact the value log **in key order** — walk the merged key stream, append each live value to fresh vlog segments in that order, and point the merged run's entries at the new locations. After one co-compaction pass the bulk of the dataset's values are physically key-ordered, so the span coalescer turns any ordered scan over compacted data into large sequential reads (the memtable + young L0 tail stays scattered but is small). This also *is* the vlog space reclaim follow-up (task 0004's "vlog co-compaction") — same rewrite, one extra sort constraint — and it obsoletes nothing on-disk: records are self-describing, ptr updates live in the new run, formats unchanged.

Design sketch:
- Extend the off-core compaction (`maint.rs` run-merge path) to stream `(key, ptr)` winners, read each value (span-coalesced from the old segments), append to new segments via a maintenance-handle vlog writer, and emit the merged run with updated ptrs.
- Old segments freed after the manifest commits (same crash discipline as run-merge; the vlog records also need seq/epoch preserved or re-stamped consistently with recovery).
- Headroom accounting like `compact()` (old + new copies resident transiently).
- Tombstone/GC interaction: co-compaction naturally drops values superseded by newer runs — fold in tombstone GC over runs (task 0004) or keep separable.

Acceptance criteria:
- After flush + co-compaction of a shuffled-write corpus, `bench scanlsm`-style ordered scan reaches within ~2× of the bulk-loaded (key-ordered-write) scan rate on the same backend.
- Box validation: on an SPDK node holding an aged (arrival-order) sorted zone, a full-range scan after co-compaction is ≥5× the pre-compaction rate, value-verified.
- Crash-safety: kill mid-co-compaction at arbitrary points recovers to pre-compaction state (old runs + old segments intact).
- Space reclaim measured (superseded/tombstoned value bytes actually freed).

## Acceptance criteria

## Plan

Goal: when the off-core run-merge compaction rewrites a zone's runs, co-compact the value log in key order — walk the merged winner stream, copy each live value into fresh vlog segments in that order, point the merged run's entries at the new locations, and free the dead old vlog segments after the manifest commits. After one pass, ordered scans over compacted data become sequential I/O (span coalescer), and superseded/GC'd value bytes are actually reclaimed.

SCOPE NOTE: the box/SPDK validation criterion (≥5× scan on an aged sorted zone on hardware) is split into a follow-on box task; this session lands + verifies everything in-repo.

## Design (pinned invariants — implementer must re-verify each against code)

1) **Metadata (manifest format bump).** `RunMeta` gains:
   - `value_segs: Option<Vec<SegmentId>>` — the distinct vlog segments this run's entries point into (exact, collected from the entries' `ptr.segment` when the run is built; entries are in RAM at flush/merge build time).
   - `value_bytes: Option<u64>` — Σ `ptr.len` over the run's entries (the co-compaction output reservation bound).
   `None` = legacy/unknown (old manifests must still decode; version-gate the manifest encoding). Populate exactly on every path that builds a run: flush (`flush_plan`/sync flush), sync `merge_all_runs`, off-core `compact_and_write` output (new ptrs → the co-compaction output segs), `forget_keys`. A plain (non-co-compacting) merge output's value_segs = distinct `ptr.segment` over surviving entries (exact, from RAM).

2) **Co-compaction output segments are vlog segments with a "not-tail" marker.** Written with a proper segment header but a distinguishable header version (or flag) meaning: *never tail-replay this segment into the memtable*. Records inside: `kind=Put`, caller-tombstone flag preserved, `version` preserved verbatim, **original `seq` preserved**, record `epoch` = the output segment's (fresh) epoch so records stay self-describing for dump tooling. Fresh epochs are taken on the serving core at plan time (bump the ValueLog epoch counter by the reserved segment count) so epochs stay unique. NEVER re-stamp fresh seqs (a fresh higher seq could beat a genuinely newer memtable record on replay — resurrection).

3) **Recovery rule (only when every manifest run carries `value_segs = Some(..)`)**: allocated = manifest chain segs ∪ run segs ∪ all runs' value_segs ∪ ordinary log segments with epoch ≥ covered_epoch (the replayed tail). Everything else (header-valid or not) is left FREE — this is what makes reclaim survive reboot without header wiping, and reclaims a crashed pass's partial output (crash before commit → new segs unreferenced → free). Marked ("not-tail") segments are never replayed regardless of epoch. If ANY run lacks value_segs (legacy), fall back to today's conservative rule (mark every log segment allocated) — never mix rules unsafely. Watch for double-mark panics (`mark_allocated` expects once): a segment can appear in several runs' value_segs and in the tail rule — dedupe.

4) **Plan (`Engine::compact_plan`)**: in addition to today's run-output reservation, reserve vlog output segments: bound = Σ inputs' `value_bytes` (+ per-segment header overhead, rounded up). If any input's value_bytes is None (legacy) OR the reservation fails (DeviceFull / headroom), **fall back to a plain merge** (co_compact=false) — compaction must never wedge. Carry: vlog seg ids + LBAs, seg_bytes, block_size, the fresh epoch range, and a co_compact flag. Consider an env kill-switch (e.g. VALS_VLOG_CO_COMPACT=0) but default ON.

5) **Off-core (`compact_and_write`)**: after the k-way merge produces the winner vec, stream it in bounded chunks: plan span-coalesced reads of the chunk's values from the OLD segments (factor the span planner so it works off-core — today `ValueLog::plan_read_spans` takes &self only to drain inflight writes; off-core reads over handle-B target quiesced old segments, LBAs resolved via the detached read-only allocator), read, decode, then append re-encoded records to a new streaming vlog writer over the pre-reserved segments (segment header with the marker + assigned epoch; records CRC'd; bounded buffer — one block/segment buffered, like SegmentStreamSink), and update each entry's `ptr` in place. GC-dropped winners are never copied (that IS the reclaim). Then `build_run_into` as today. Flush the device before returning (durability barrier before commit). On any error return ALL reserved segs (run + vlog output) for `free_compact_segs` — extend that path to free both sets and clear the gate.

6) **Commit (`Engine::compact_commit`)**: manifest's merged RunMeta carries new ptr-derived value_segs (the used vlog output prefix) + value_bytes; free the unused vlog reservation tail. After the manifest is durable, free old dead vlog segments: (inputs' value_segs ∪) minus — survivors' value_segs, minus any segment referenced by the current memtable index (one O(memtable) walk collecting distinct `ptr.segment`), minus the ValueLog's currently-open segment, and conservatively minus any segment with epoch ≥ covered_epoch if cheap to know (the memtable walk + open-segment exclusion already covers the live-tail hazards; re-verify the boundary segment epoch==covered case: it can hold both run-referenced and memtable/tail records — the memtable walk protects live records; superseded-only tail records may be freed safely because seq-aware replay resolves winners from surviving copies, but re-check the max_seq/max_epoch resume logic never depends on a freed segment — the open segment is excluded). If value_segs of an input is None (legacy), free nothing from the vlog for that input (plain-merge fallback should have been chosen anyway).

7) **Crash-safety matrix to verify with tests**: crash after vlog+run write but before manifest commit → old manifest + old runs + old segments authoritative, new segments reclaimed at recovery (not replayed — the marker), state byte-identical to pre-compaction; crash after commit → new run + new value segments live, old dead segments left free by the recovery rule; recovered scan/get equals pre-crash expectations either way.

8) **Sync paths unchanged semantically**: `compact_runs` (sync) and `forget_keys` stay plain merges (no vlog rewrite) but must populate the new RunMeta fields exactly.

## Implementation order
a. Formats: RunMeta fields + manifest version bump + decode back-compat tests; seg-header marker + decode; populate value_segs/value_bytes on all run-build paths.
b. Recovery rule change (gated on all-Some) + tests (incl. legacy fallback, dedupe, marked-seg skip).
c. Off-core plan/write/commit co-compaction + fallback-to-plain + free-path extension.
d. Crash-injection tests (kill at arbitrary points: use the existing torn-manifest / free-on-error test patterns; a fault-injecting BlockDevice if one exists in testutil).
e. Space-reclaim test: overwrite-heavy corpus → flush ×N → co-compaction → assert used_segments drops ~to live size; reboot → still reclaimed.
f. Perf acceptance in-repo: a `bench scanlsm`-style or test-level measurement — shuffled-write corpus, flush + co-compact, ordered scan rate within ~2× of the same corpus bulk-loaded in key order (file backend). Use the existing scan bench plumbing in vals-bench if present.
g. Docs: update docs/out-of-core-index.md C.6 status + docs/scan-performance.md (vlog co-compaction now landed), note the trigger policy + kill-switch.

## Risks / judgement calls made
- Recovery liveness becomes manifest-driven (value_segs) instead of header-scan-driven — the highest-risk change; hence the strict all-Some gate + legacy fallback.
- Original seq preserved (never fresh) in copied records — required to avoid replay resurrection.
- Write-amp: co-compaction rides every off-core full merge (threshold-8 merge-all); acceptable first cut, kill-switch provided.
- valsd dump tooling must tolerate the new header marker (at minimum: still decodes records; document ordering/seq semantics).

### Starting points
- crates/vals-store/src/engine.rs — compact_plan/compact_and_write/compact_commit (~L2295-2551), merge_all_runs (~L2209), open_inner recovery + replay_winners (~L910-1240), free_compact_segs
- crates/vals-store/src/vlog.rs — ValueLog::append_tombstone/roll, plan_read_spans (~L1007), decode_seg_header/SEG_HEADER_LEN, encode_header, read_record
- crates/vals-store/src/run.rs — SegmentStreamSink, build_run_into, allocate_run_segs (pattern for the vlog stream writer)
- crates/vals-store/src/manifest.rs — Manifest/RunMeta encode/decode (version-gate here)
- crates/vals-store/src/snapshot_io.rs — write_manifest/load_manifest
- crates/vals-cluster/src/node.rs ~L1084-1260 — off-core maintenance driver (compact_plan → worker compact_and_write → compact_commit)
- crates/vals-cluster/src/replica.rs compact_plan/compact_commit wrappers (~L694-760)
- crates/vals-store/src/dump.rs scan_log_segments — recovery pass 1 + dump tooling share it
- existing crash-safety test patterns: engine.rs tests around torn manifest commit / flush_plan_reserves_segments_freeable_on_abandon
- docs/out-of-core-index.md §C.6 + docs/scan-performance.md — status text to update

## Work log
- 2026-07-04 plan:
- 2026-07-04 plan: Goal: when the off-core run-merge compaction rewrites a zone's runs, co-compact the value log in key order — walk the merged winner stream, copy each live value into fresh vlog segments in that orde
…[truncated]
- 2026-07-04 context hints: 10 recorded with plan
- 2026-07-04 context hints: crates/vals-store/src/engine.rs — compact_plan (~L2316), compact_and_write (~L429), compact_commit (~L2382), merge_all_runs (~L2209), open_inner recovery (~L910-1038), replay_winners (~L1213), free_
…[truncated]
- 2026-07-04 implementer report: Implemented **task 0028 — key-ordered value-log co-compaction** in-repo (box/SPDK validation split to task 0042). The off-core run-merge now co-compacts the value log in key order, so aged (arrival-
…[truncated]
- 2026-07-04 review tier: high-powered — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change implements key-ordered value-log co-compaction end to end: manifest v2 with back-compatible per-run value_segs/value_bytes, a not-tail segment-header marker, an off-core VlogWriter + detach
…[truncated]
- 2026-07-04 revision: Addressed the reviewer's hardening.   ## What I changed  **The fix (`Engine::open_inner` epoch resume):** the epoch resume no longer trusts only surviving segment headers. I extracted a `resume_next_e
…[truncated]
- 2026-07-04 review (Claude): accept — The revision cleanly addresses the review. It adds `resume_next_epoch`, folding the manifest watermark into the value-log epoch resume so `next_epoch > covered_epoch` unconditionally — closing a dat
…[truncated]
- 2026-07-04 decision: accept — commit: feat(store): key-ordered vlog co-compaction in the off-core run merge (task 0028)  The off-core run-merge compaction now co-compacts the value log in key order: merged winners' values are copied span-
…[truncated]
- 2026-07-04 usage: 203,200 tok (in 506, out 202,694, cache_r 38,339,311, cache_w 1,049,261) · cost n/a (unpriced)
  implementer: 134,914 tok (in 328, out 134,586, cache_r 32,877,878, cache_w 584,659) · cost n/a (unpriced)
  coordinator: 43,664 tok (in 82, out 43,582, cache_r 3,023,179, cache_w 308,441) · cost n/a (unpriced)
  reviewer:Claude: 24,622 tok (in 96, out 24,526, cache_r 2,438,254, cache_w 156,161) · cost n/a (unpriced)

---
id: "0038"
title: Tombstone GC over runs (kind-promotion) — unblocks F2 forget/cleanup over flushed keys
status: done
priority: 2
created: "2026-07-04"
updated: "2026-07-04"
depends_on: []
spec_refs:
    - docs/out-of-core-index.md#C.5/C.6 status
---

## Description
Split out of task 0004 (item 2). Stage C's run-merge compaction (`Engine::compact_runs` / off-core `compact_and_write`) merges by `argmax(version)` but cannot collect aged tombstones: run entries carry no `kind`, so a tombstone in a run is indistinguishable from a live value and is carried forward forever. Deleted keys' index entries + tombstone vlog records are never reclaimed once flushed, and the **F2 gap** stays open: `forget`/`compact_filtered` no-op while runs exist, so over-replication cleanup over flushed keys stalls (see `docs/out-of-core-index.md` §C.6 deferred item (b)).

Scope:
- **Kind-promotion**: carry the record `kind` (live vs tombstone) in run entries (format change or ptr-bit encoding — decide at plan time; runs are rebuilt on compaction so a versioned format bump is cheap).
- **Tombstone GC watermark** (obligation #3 in `docs/out-of-core-index.md`): a tombstone may only be dropped when the merge output is the bottommost level AND no older run/memtable copy of the key can exist — otherwise dropping it resurrects the older value.
- Apply the **resurrection-vector analysis from the C.5b review** (see BACKLOG.md / `docs/out-of-core-index.md` C.5b notes: a delete must stay shadowed by a higher-version entry until every older copy is gone).
- Unblock F2: make `forget`/over-replication cleanup effective over flushed keys (compact_filtered or the run-merge path drops handed-off keys safely).

Acceptance criteria:
- Aged tombstones are actually collected by run compaction once safe (index entry + sidecar space measured before/after).
- **Kill-the-fix tests**: deliberately weaken the watermark/bottommost guard and show the resurrection test catches it; delete→flush→compact→read stays deleted across recovery.
- No key resurrection: property/regression tests covering delete-then-compact interleavings with older runs present, and across crash/recovery.
- F2 verified: over-replication cleanup (forget) reclaims flushed keys after handoff, tested at cluster level.

## Acceptance criteria

## Plan

Goal: make run-merge compaction able to collect aged cluster tombstones (kind-promotion into run entries + GC watermark, obligation #3), and make `forget`/over-replication cleanup effective over flushed keys (F2).

Key existing facts (verified in code):
- A cluster tombstone is an engine-level `Put` of blob `KIND_TOMB ++ []`; the engine cannot see it (kind byte is inside the value). Run entries are `{key, ptr, seq, version}` — no kind.
- Cluster admission is strict store-if-newer everywhere (`Replica::apply_at` checks `current_version` = argmax over memtable∪runs, `?`-propagated non-ack on read failure; local writes stamp dominating HLC versions). Therefore: any memtable or later-flushed (survivor-run) copy of a key is STRICTLY NEWER than any copy in the current run set. This is the invariant that makes tombstone-drop safe.
- `compact_runs` (sync) and `compact_plan`/`compact_and_write`/`compact_commit` (off-core, used by node maintenance at `RUN_COMPACT_THRESHOLD`) merge ALL current runs into one (bottommost by construction); flushes during the off-core merge become "survivors" (newer).
- vlog record header has a reserved `flags` byte (always 0 today, CRC-covered); `pack_version` is hlc-high/node-low so u128 order == LWW `Version` order; Version.hlc = physical_ms<<16|counter.
- `VersionedStore::compact` (vlog path) GCs tombstones with `eff_now = min(wall_ms, high_water_ms)`, cutoff `eff_now - gc_grace_ms`, and retires the Merkle leaf per dropped key; `forget` uses `compact_filtered`, which no-ops while runs exist (the F2 gap).

Part 1 — kind-promotion (engine-level "caller tombstone" flag):
1. vlog: define `FLAG_TOMBSTONE = 0x01` in the record header's reserved `flags` byte. New `Engine::put_kind(key, value, version, tombstone: bool)` (existing `put` delegates with false). `Replica::store` passes `kind == KIND_TOMB`. Recovery replay + `read_record`/`replay_segment` surface the flag; `copy_live_records` preserves it when copying records forward. No FORMAT_VERSION bump needed (layout unchanged; old records decode flags=0 = live — conservative and safe: pre-existing tombstones just stay un-GC'd, read path unchanged since the cluster still decodes the blob's kind byte).
2. `IndexEntry` and `SnapshotEntry` gain `tombstone: bool` (both are plain in-RAM structs; FrontCodedIndex stores `Vec<IndexEntry>` so no codec change). Flush (`flush_plan` snapshot) and recovery propagate it.
3. Run block codec: encode the tombstone flag as the TOP BIT of the entry's `suffix_len` u32 (keys are far below 2^31; add an explicit bounds guard in `encode_block`). v1 runs always have the bit 0 → decode as live (conservative, safe). Bump `RUN_VERSION` to 2 and accept {1, 2} in `decode_footer`; decoding is identical for both. Sidecar/digests unchanged (kind not needed there). `block_lookup`, `decode_block`, `RunScan` reconstruct the flag into `SnapshotEntry.tombstone`.

Part 2 — tombstone GC in the run merge:
4. `Engine::compact_runs` and `Engine::compact_plan`/`compact_and_write`/`compact_commit` gain a GC cutoff `gc_collect_before: Option<u128>` (opaque packed-version threshold; engine compares u128s only). In `merge_sources`/callers: when the per-key argmax winner is a tombstone with `version < cutoff`, OMIT it from the merged output and record it in `dropped_tombstones: Vec<(Vec<u8>, u128)>` (carried through `WrittenCompact` to `compact_commit`; returned in stats from the sync path). Non-winner (superseded) tombstones drop as today with no reporting.
   Safety (document at the drop site + engine API contract): correct because (a) the merge consumes ALL runs captured at plan time (bottommost), and (b) callers uphold store-if-newer, so memtable/survivor copies are strictly newer — no older value can be exposed. debug_assert at commit: any memtable entry for a dropped key has version > dropped version.
5. Current-version filtering for the Merkle leaf: at `compact_commit`/`compact_runs` (sync, under the borrow, no await), a dropped tombstone was the key's CURRENT version iff the key is absent from the memtable index (RAM) and absent from every survivor run (sync `DeviceRun::lookup` — bloom-negative is RAM-only; survivors are usually zero). Return only current-dropped pairs to the caller; the cluster must retire exactly these (retiring a superseded drop would corrupt the leaf accumulator).
6. `VersionedStore::compact_runs`/`compact_plan` wrappers compute the cutoff when `gc_grace_ms > 0`: `eff_now = min(wall, high_water_ms)`, cutoff version = `pack_version(Version { hlc: (eff_now - grace) << 16, node: 0 })` (same clock-domain invariant as `VersionedStore::compact` — reuse/document it). `VersionedStore::compact_commit` (and the sync wrapper) calls `versions.retire_leaf(&k, unpack_version(v))` for each current-dropped tombstone (this also maintains the `live` counter). node.rs maintenance passes wall time where needed.
7. Obligation #3 admission floor (the GC watermark, kill-the-fix target): in `Replica::apply_at`, when `gc_grace_ms > 0` and the key has NO current version (`old == None`), reject (`Applied::Rejected`) an incoming version with `physical_ms() < min(wall_ms, high_water_ms()).saturating_sub(gc_grace_ms)`. This floor is monotonically ≥ every past GC cutoff (wall and watermark only advance), so it needs no persisted state, survives restarts, and closes the "GC emptied every bloom → straggling pre-delete write admitted as new" resurrection vector. Local stamped writes are unaffected (fresh HLC versions). Document the contract: with GC enabled, a brand-new write older than grace is refused — the same operational window `gc_grace` already imposes.

Part 3 — F2: forget over flushed keys:
8. New `Engine::forget_keys(keys: &HashSet<Vec<u8>>) -> Result<Option<ForgetRunsStats>, EngineError>` (or similar): returns None/deferred when `flush_in_flight || compact_in_flight` (caller retries next sweep — matches the existing `can_forget` deferral pattern; both gates are REQUIRED: a concurrent flush would re-expose the key via its committed run, a concurrent off-core compaction's commit would reinstall merged inputs containing it). Otherwise, synchronously:
   a. If runs exist: merge ALL runs into one (reuse the sync merge machinery), omitting EVERY entry (live or tombstone) whose key ∈ keys; commit the manifest new-before-old, free old runs' segments (same crash-safety as `compact_runs`; watermark preserved).
   b. Then for each key ∈ keys still in the memtable index: append a `RecordKind::Delete` record and remove the index entry (crash-safe: higher seq keeps it absent on tail replay; safe vs runs because step (a) already removed all run copies). This ordering has no exposure window (while the memtable copy exists it is the newest).
   c. Report per-key dropped current versions so the cluster can retire leaves.
   Note: flushed keys' vlog record BYTES are not reclaimed (vlog co-compaction is separately deferred, see BACKLOG) — the index entries, run space, sidecar digests, and AE visibility are, which is what F2 needs.
9. `VersionedStore::forget`: if `run_count() == 0`, keep the existing `compact_filtered` path unchanged; else call `forget_keys`, retire leaves for the dropped pairs, return the released count (0 when deferred). `cleanup_with` needs no structural change (it already re-checks versions synchronously just before the forget with no await).

Part 4 — tests (map to acceptance criteria):
- Codec: run entry tombstone flag round-trips (encode/decode/lookup/scan/device run); v1-footer runs still open and decode as all-live; `run_len_upper_bound` property still dominates.
- Engine: cluster-style tombstone-put → flush → run carries the flag; recovery across manifest+tail preserves it; aged tombstone winner is collected by `compact_runs` with a cutoff (index entry gone, entry_count shrinks, stays absent), NOT collected when cutoff doesn't cover it; superseded tombstone drop reports nothing; delete→flush→compact→read stays deleted ACROSS reopen/recovery; off-core plan/write/commit path equivalent (incl. dropped-tombstone reporting with a survivor run flushed mid-merge — survivor holds newer entry ⇒ not current-dropped, leaf untouched).
- Kill-the-fix: (a) resurrection test that FAILS if the admission floor is removed: put K, delete K, flush, age past grace, compact (tombstone GC'd), then `apply` a pre-delete-versioned straggler → must be Rejected and K stays absent; assert the inverse (floor disabled ⇒ K would resurrect) by testing the floor logic directly at a version just below/above the cutoff. (b) test that a NOT-yet-aged tombstone survives compaction (weakening the cutoff/bottommost guard flips this test).
- Property/regression: randomized interleavings of put/delete(cluster tombstone)/flush/compact(+GC)/crash-recover asserting no key resurrection and digest/leaf consistency (extend the existing engine property-test style).
- Cluster-level F2: keys written + flushed into runs, ownership handed off, `cleanup_scoped` → forget reclaims flushed keys (released count > 0, local get None, leaf accumulator consistent, owners unaffected); deferral when flush/compact in flight.
- Full workspace `cargo build` + `cargo test`; keep clippy clean for touched code.

Part 5 — docs: update `docs/out-of-core-index.md` (C.6 deferred item (b) closed: kind-promotion + tombstone GC landed; F2 closed; obligation #3 discharged via the admission floor — describe it) and any stale comments (`compact_runs` doc "tombstone GC is deferred", `Replica::forget` no-op note, BACKLOG.md if it lists this as open).

Out of scope (leave as-is / follow-on tasks): vlog space reclaim (co-compaction), GC-only compaction trigger for a single lone run (GC rides along on threshold-triggered merges; document), shrinking IndexEntry (task 0021).

### Starting points
- crates/vals-store/src/run.rs: encode_block/decode_block/block_lookup (entry codec, suffix_len u32), RUN_VERSION=1 in decode_footer, RunScan
- crates/vals-store/src/engine.rs: compact_runs ~L2033, compact_plan ~L2159, compact_and_write ~L400, merge_sources ~L558 (argmax core), compact_commit ~L2223, compact_filtered ~L2478 (no-op with runs/flush_in_flight), copy_live_records ~L2623, flush_plan/flush_commit ~L1746/1846, delete ~L1310 (flush_in_flight debug_assert)
- crates/vals-store/src/vlog.rs: HEADER_LEN=50, flags byte at h[5] (reserved, always 0), RecordKind, encode_header/validate_record
- crates/vals-store/src/index.rs L72: IndexEntry{ptr,seq,version} — FrontCodedIndex stores Vec<IndexEntry> in RAM, keys front-coded separately (no codec change for a new field)
- crates/vals-store/src/snapshot.rs: SnapshotEntry{key,ptr,seq,version}
- crates/vals-cluster/src/replica.rs: KIND_PUT/KIND_TOMB blob encode/decode ~L34-90, store ~L1375 (engine.put with pack_version), apply_at ~L1411 (store-if-newer; the old==None admission point for the floor), compact ~L756 (eff_now=min(wall,high_water), grace cutoff, retire_leaf pattern), forget ~L800, VersionedStore::compact_runs/compact_plan/compact_commit ~L673-706
- crates/vals-cluster/src/hlc.rs: Version{hlc,node}, physical_ms = hlc>>16; pack_version is hlc-high/node-low (replica.rs L54)
- crates/vals-cluster/src/antientropy.rs: cleanup_with ~L560-680 (can_forget gate, to_forget set, no await between recheck and forget), existing cleanup tests ~L1032-1265
- crates/vals-cluster/src/node.rs: RUN_COMPACT_THRESHOLD=8, compact_plan submit ~L1150-1200, compact_commit reconcile ~L992-1020
- docs/out-of-core-index.md: 'Correctness obligations' #3 (tombstone-GC watermark), C.6 status (deferred item b)

## Work log
- 2026-07-04 plan: Goal: make run-merge compaction able to collect aged cluster tombstones (kind-promotion into run entries + GC watermark, obligation #3), and make `forget`/over-replication cleanup effective over flush
…[truncated]
- 2026-07-04 context hints: 10 recorded with plan
- 2026-07-04 context hints: crates/vals-store/src/run.rs: encode_block/decode_block/block_lookup (entry codec, suffix_len u32), RUN_VERSION=1 in decode_footer, RunScan; crates/vals-store/src/engine.rs: compact_runs ~L2033, compa
…[truncated]
- 2026-07-04 implementer report: Implemented Task 0038: tombstone GC over runs (kind-promotion) + F2 forget/cleanup over flushed keys + obligation #3 admission floor. All acceptance criteria met; full workspace builds, 583 tests pass
…[truncated]
- 2026-07-04 review tier: high-powered — reviewers: Claude
- 2026-07-04 review (Claude): revise — The functional implementation is correct and complete against all acceptance criteria. Kind-promotion is threaded end-to-end: the vlog record header's reserved flags byte carries FLAG_TOMBSTONE (CRC-c
…[truncated]
- 2026-07-04 revision: Addressed both hygiene items from the review; no functional changes.  1. **`cargo fmt`**: ran `cargo fmt`; `cargo fmt --check` now passes clean. The reformatting touched only the files already modifie
…[truncated]
- 2026-07-04 review (Claude): accept — The revision addresses both prior findings. `cargo fmt --check` now passes across the whole workspace, and the previously-introduced clippy type_complexity warning on merge_all_runs was factored into 
…[truncated]
- 2026-07-04 decision: accept — commit: feat(store,cluster): tombstone GC over runs via kind-promotion + F2 forget over flushed keys (task 0038)  Run entries now carry the caller-tombstone flag (vlog header FLAG_TOMBSTONE -> IndexEntry/Snap
…[truncated]
- 2026-07-04 usage: 206,364 tok (in 642, out 205,722, cache_r 61,783,294, cache_w 731,248) · cost n/a (unpriced)
  implementer: 154,257 tok (in 514, out 153,743, cache_r 56,786,337, cache_w 343,105) · cost n/a (unpriced)
  coordinator: 39,387 tok (in 66, out 39,321, cache_r 3,352,290, cache_w 309,869) · cost n/a (unpriced)
  reviewer:Claude: 12,720 tok (in 62, out 12,658, cache_r 1,644,667, cache_w 78,274) · cost n/a (unpriced)

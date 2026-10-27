---
id: "0021"
title: Shrink IndexEntry/ValuePtr (post-B1 dominant per-key cost)
status: done
priority: 2
created: "2026-07-02"
updated: "2026-07-04"
depends_on:
    - "0001"
spec_refs: []
---

## Description
BACKLOG.md §Scaling avenue 3, promoted 2026-06-26: once keys front-code down to single-digit bytes, the 40 B `IndexEntry` ({ValuePtr 16, seq 8, version 16}) is ~85% of per-key cost — the new dominant term and B1's ~40–50 B/key floor. `seq u64` may be reducible; segment+offset could pack into fewer bytes given segment geometry. Small, low-risk, additive to B1.

## Acceptance criteria
- Measured per-key reduction on top of the front-coded index (memprof-style).
- No overflow hazards: packing bounds checked at construction (like the existing u32 segment-offset guard); suite green.

## Acceptance criteria

## Plan

Goal: cut the resident per-key cost of the index entry (currently `IndexEntry` = 48 B in RAM: ValuePtr{SegmentId u64, offset u32, len u32}=16 + seq u64=8 + version u128=16 + tombstone bool + 7 B padding from u128's 16-byte alignment) without changing any public API or on-disk format.

Approach — packed in-RAM storage, unchanged API types:
1. Add a private `PackedEntry` in `crates/vals-store/src/index.rs`: a `[u8; 36]` (align 1, Copy) little-endian layout:
   - segment: u32 (4 B) — checked at pack time that `segment.0 <= u32::MAX` (mirrors the existing u32 segment-offset guard in `ValueLog::new`).
   - offset: u32 (4 B), len: u32 (4 B) — already u32 in ValuePtr.
   - seq+tombstone: u64 (8 B) — top bit = tombstone flag, low 63 bits = seq; checked `seq < 1<<63` at pack time (unreachable in practice; seq is a monotonic write counter).
   - version: u128 (16 B) — opaque, carried verbatim.
   `fn pack(&IndexEntry) -> PackedEntry` (with descriptive asserts) and `fn unpack(&self) -> IndexEntry`. Compile-time/static assert size == 36, align == 1.
   Net: 48 → 36 B per resident entry (−12 B/key; ~25% of the entry, and the entry is ~85% of the post-B1 per-key floor).
2. Store `PackedEntry` instead of `IndexEntry` in the two resident structures:
   - `FrontCodedIndex`: `Block::entries: Vec<PackedEntry>`, `BlockBuilder::entries` — the sorted-zone per-key store the memprof floor is about.
   - `HashIndex`: `HashMap<Box<[u8]>, PackedEntry>` — same win for hash zones.
   Convert at the `Index` trait boundaries (get/put/remove/scan/scan_bounded/versions/decode_pairs etc.). Public `IndexEntry`/`ValuePtr` stay unchanged, so engine.rs/run.rs/snapshot.rs/cluster are untouched.
3. Startup-time geometry guard: assert in `Allocator::new` (or engine create/open) that `num_segments <= u32::MAX + 1` region — i.e. every valid SegmentId fits the packed u32 — so a violating geometry fails at open, not mid-write. Keep the pack-time assert as the hard construction check per the acceptance criteria.
4. Tests:
   - pack/unpack round-trip over representative + extreme values (segment = u32::MAX, offset/len = u32::MAX, seq = (1<<63)-1, version = 0 and u128::MAX, tombstone both ways).
   - #[should_panic] for segment > u32::MAX and seq >= 1<<63.
   - size/align static assertion.
   - Full existing suite green (`cargo test` workspace) — the FrontCodedIndex property test vs BTreeMap reference is the main behavioral net.
5. Measurement (acceptance criterion): run `cargo run --release -p vals-bench -- file memprof 1000000 100` on the unmodified tree first (baseline bytes/key), then after the change, same machine/params; record both numbers. Expect ≈12 B/key reduction (e.g. ~86 → ~74 B/key on the unique-DID memprof shape).
6. Doc touch-up: one short note in docs/out-of-core-index.md where task 0021 is named (the "IndexEntry-dominated floor" paragraphs) recording the packed-entry follow-on and measured delta.

Out of scope: shrinking the public ValuePtr/SegmentId types (on-disk/manifest ripple), reducing version (opaque), spilling cold keys.

### Starting points
- crates/vals-store/src/index.rs — IndexEntry (line ~72), HashIndex (~187), BlockBuilder/Block (~320-443), FrontCodedIndex (~463+), property tests at bottom
- crates/vals-store/src/vlog.rs:179 ValuePtr; ValueLog::new (~line 630) has the existing u32 segment-offset assert to mirror
- crates/vals-store/src/alloc.rs — SegmentId(pub u64), Allocator::new computes num_segments
- bench: crates/vals-bench/src/main.rs workload_memprof (~line 3820); run: cargo run --release -p vals-bench -- file memprof 1000000 100
- docs/out-of-core-index.md ~lines 214-263 reference task 0021 (IndexEntry-dominated floor)

## Work log
- 2026-07-04 plan: Goal: cut the resident per-key cost of the index entry (currently `IndexEntry` = 48 B in RAM: ValuePtr{SegmentId u64, offset u32, len u32}=16 + seq u64=8 + version u128=16 + tombstone bool + 7 B paddi
…[truncated]
- 2026-07-04 context hints: 5 recorded with plan
- 2026-07-04 context hints: crates/vals-store/src/index.rs — IndexEntry (~72), HashIndex (~187), BlockBuilder (~320), Block (~376), FrontCodedIndex (~463), property tests at bottom of file; crates/vals-store/src/vlog.rs:179 Va
…[truncated]
- 2026-07-04 implementer report: Implemented task 0021: shrank the resident index entry from 48 B to 36 B by storing a packed byte-array form in RAM while leaving the public API and on-disk format untouched.  ## Changes - **crates/va
…[truncated]
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change shrinks the resident index entry from 48 B to 36 B by introducing a private PackedEntry([u8;36], align 1) stored in both HashIndex and FrontCodedIndex, while leaving the public IndexEntry/V
…[truncated]
- 2026-07-04 decision: accept — commit: perf(store): pack resident index entry to 36 B (task 0021)  Store IndexEntry packed in RAM in both HashIndex and FrontCodedIndex blocks: u32 segment + u32 offset + u32 len + 63-bit seq with tombstone 
…[truncated]
- 2026-07-04 usage: 36,924 tok (in 158, out 36,766, cache_r 3,049,336, cache_w 178,156) · cost n/a (unpriced)
  implementer: 18,565 tok (in 86, out 18,479, cache_r 2,031,385, cache_w 62,708) · cost n/a (unpriced)
  coordinator: 15,087 tok (in 50, out 15,037, cache_r 895,348, cache_w 97,398) · cost n/a (unpriced)
  reviewer:Claude: 3,272 tok (in 22, out 3,250, cache_r 122,603, cache_w 18,050) · cost n/a (unpriced)

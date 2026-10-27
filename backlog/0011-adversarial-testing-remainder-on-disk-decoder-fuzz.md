---
id: "0011"
title: 'Adversarial testing remainder: on-disk decoder fuzz, loom, network simulation'
status: done
priority: 3
created: "2026-07-02"
updated: "2026-07-04"
depends_on: []
spec_refs: []
---

## Description
BACKLOG.md §Adversarial test infrastructure — the wire-codec fuzz/property harness landed (`wire_fuzz.rs`); remaining gaps:
1. **No `loom` on the unsafe runtime core** (hand-rolled Rc waker vtables, SPDK completion handshake, thread confinement).
2. **No deterministic network simulation** (fault injection is a `set_down` bool).
3. **No fuzz on the on-disk record decoder** (`vals-store` record/segment replay — length/CRC-guarded per audit, but no systematic coverage).

Scope each as a sub-decision at plan time; the record-decoder fuzz is the cheapest (reuse the seeded SplitMix64 pattern from wire_fuzz, no new deps).

## Acceptance criteria
- On-disk record/segment replay has a no-panic + roundtrip fuzz harness in-tree.
- Decision recorded on loom (dependency-light tension) and network simulation, with tasks split out if pursued.

## Acceptance criteria

## Plan

Scope decisions (made at plan time, per the task):

1. **On-disk record/segment fuzz — DO NOW** (the acceptance criterion). Add `crates/vals-store/tests/record_fuzz.rs`, modeled directly on `crates/vals-cluster/tests/wire_fuzz.rs` (same seeded SplitMix64 PRNG, no new deps; same module-doc style explaining WHY). Targets are the public recovery-path decoders in `vals_store::vlog`: `decode_record`, `decode_record_value`, `decode_seg_header`, `replay_segment`. Properties:
   a. **Roundtrip property**: generate random batches of records (keys 0..~32 B incl. empty, values 0..~600 B so records span blocks and segments roll, random `RecordKind` (Put/Free — whatever variants exist), random tombstone flag via `append_tombstone` vs `append`, random version u128); write them through the REAL `ValueLog::append*` against a `vals_spdk::MemBlockDevice` + `Allocator` (mirror the setup in vlog.rs unit tests: small block size, few blocks per segment), `sync`, read each used segment's raw bytes back, `replay_segment` each, and assert the union of replayed records equals what was appended — key, value, seq, epoch, kind, tombstone, version, and that each returned offset actually re-decodes via `decode_record` to the same record. Run over many seeds.
   b. **No-panic fuzz on arbitrary bytes**: purely random buffers of varied length (0..~2×block) through all four decoders inside `catch_unwind`; also every single-byte and two-byte prefix, and empty input. Assert Ok/Err, never unwind (report seed + hex bytes on failure, like wire_fuzz).
   c. **No-panic + prefix property on mutated valid segments**: take a valid segment image, mutate it (byte flips, truncation to random length, appending random trailing bytes, overwriting a 4-byte window with u32::MAX — reuse wire_fuzz's `mutate` shape), then `replay_segment`. Assert: no panic; and if it returns Ok, the returned records must be a *prefix* of the original record list (CRC + epoch guard means a mutation can only truncate the replay, never alter or extend it — except mutations that only touch zero padding, which leave the full list; "prefix" covers both). If the mutation corrupts the seg header, Err is fine.
   Keep iteration counts sized so the whole test file runs in a few seconds (wire_fuzz uses 4000–6000 iterations; segment builds are heavier, so ~200–500 roundtrip seeds and a few thousand mutation cases is fine).

2. **loom — NOT PURSUED (decision recorded, no task)**. Rationale to record in BACKLOG.md §Adversarial test infrastructure: the unsafe runtime core's hand-rolled Rc waker vtables are single-threaded by design (thread-per-core, single-owner) — loom checks cross-thread interleavings and adds nothing there (Miri would be the matching tool); the one real cross-thread surface (SPDK completion handshake / WriteHandle atomics) is small, and exercising it under loom would require cfg-gated loom shims for every sync primitive across vals-rt/vals-spdk plus a new dev-dependency in a deliberately dependency-light workspace — high intrusion, low residual risk. Revisit if the runtime grows new cross-thread state.

3. **Deterministic network simulation — PURSUED LATER (task split out)**. It is a real gap (fault injection is a `set_down` bool) but a standalone project: a deterministic in-process transport with seeded fault schedules (drop/delay/duplicate/partition), virtual time, and cluster-level invariant checks. Coordinator creates a new backlog task capturing that scope; this task only records the decision.

4. **BACKLOG.md**: update the §Adversarial test infrastructure bullet — note the record-decoder fuzz harness now exists (file path, properties), record the loom decision (not pursued + rationale), and point the network-simulation remainder at the new backlog task.

Verification: `cargo test -p vals-store --test record_fuzz` passes; `cargo test -p vals-store` still green; `cargo clippy -p vals-store --tests` introduces no new warnings.

### Starting points
- crates/vals-cluster/tests/wire_fuzz.rs — the pattern to mirror (SplitMix64 Rng, mutate(), assert_no_panic, module doc)
- crates/vals-store/src/vlog.rs — targets: decode_record, decode_record_value, decode_seg_header, replay_segment; constants HEADER_LEN/SEG_HEADER_LEN/KEY_OFF; unit tests at bottom show MemBlockDevice+Allocator+ValueLog setup (BS=256, SEG_BLOCKS=4) and append signature
- vals_spdk::MemBlockDevice — in-RAM device backend, already used by vlog unit tests
- BACKLOG.md line ~670 — §Adversarial test infrastructure bullet to update
- check which of HEADER_LEN/KEY_OFF etc. are pub; integration test can only use pub items — replay offsets + decode_record on slices avoid needing private encoders

## Work log
- 2026-07-04 plan: Scope decisions (made at plan time, per the task):  1. **On-disk record/segment fuzz — DO NOW** (the acceptance criterion). Add `crates/vals-store/tests/record_fuzz.rs`, modeled directly on `crates/
…[truncated]
- 2026-07-04 context hints: 5 recorded with plan
- 2026-07-04 context hints: crates/vals-cluster/tests/wire_fuzz.rs — the pattern to mirror (SplitMix64 Rng, mutate(), assert_no_panic, module-doc style); crates/vals-store/src/vlog.rs — targets: decode_record, decode_record_
…[truncated]
- 2026-07-04 implementer report: Implemented task 0011 (adversarial-testing remainder).  ## A. New fuzz/property harness — `crates/vals-store/tests/record_fuzz.rs` Modeled directly on `vals-cluster/tests/wire_fuzz.rs`: copied-in se
…[truncated]
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change adds a well-crafted on-disk record/segment fuzz + property harness (crates/vals-store/tests/record_fuzz.rs) faithfully modeled on wire_fuzz.rs: seeded SplitMix64, no new deps, a roundtrip p
…[truncated]
- 2026-07-04 decision: accept — commit: test: fuzz/property harness for on-disk record/segment replay; record loom + net-sim decisions (task 0011)
- 2026-07-04 usage: 31,545 tok (in 100, out 31,445, cache_r 2,117,622, cache_w 145,975) · cost n/a (unpriced)
  implementer: 15,449 tok (in 30, out 15,419, cache_r 869,027, cache_w 71,295) · cost n/a (unpriced)
  coordinator: 11,798 tok (in 48, out 11,750, cache_r 1,100,978, cache_w 52,560) · cost n/a (unpriced)
  reviewer:Claude: 4,298 tok (in 22, out 4,276, cache_r 147,617, cache_w 22,120) · cost n/a (unpriced)

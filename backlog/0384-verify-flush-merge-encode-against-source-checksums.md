---
id: "0384"
title: Verify flush/merge encode against source checksums and key order before CRC (a RAM flip between sort and encode persisted a mis-ordered run on lab-2)
status: done
priority: 1
created: "2026-10-06"
updated: "2026-10-06"
depends_on: []
spec_refs: []
---

## Description
## Evidence
SOAK5B-V4-R2 (`bench/0363-compact-values/SOAK5B-V4-R2-FINDINGS.md` "Bad run diagnosis").
- **What the read-only runverify found:** an lab-2 zone-2 memtable flush output (891,777 entries) holds one key with a single flipped bit (byte 5, XOR 0x04: 'd' → 0x60). That is the same bit as lab-2's exit-75 memtable flips.
- **Why it persisted:** the flip happened after the memtable was sorted (the 0370 guard passed) and before the block was encoded and CRC'd. So the stored CRC is valid, and the run is permanently mis-ordered.
- **Effect:** every merge taking it as input fails with `merge: run cursor not strictly ascending`, and the run stopped under the retry rule. The replica (lab-3) holds the correct key with the same version.
- **Why nothing caught it:** flush output gets no readback, and 0375 build verify covers only merge output and only CRCs. The 0365 version-stream walker checks order, but only at read time.

## Fix
Make the encode path verify what it encodes against what the source structure attested, before computing block/index CRCs:
1. **Flush:** the 0370 memtable blocks carry an additive FxHash over (key, packed entry). While encoding the run blocks from a memtable block, recompute the same hash over the entries **as encoded** (from the encode buffer, after front coding, or over the exact bytes fed to the encoder) and compare with the memtable block checksum. Also assert strictly ascending keys during encode.
2. **Merge output:** assert strictly ascending keys during encode. Consider an end-to-end checksum: combine per-input-run running hashes of the surviving entries vs the output's hash.
3. **On mismatch:** return a retryable error. The flush or merge aborts without publishing, segments are freed, and it retries from the source (memtable or inputs). Count it (`vals_encode_integrity_failures_total`) and log the evidence (block, entry, XOR). A persistent repeat on a flush goes through the 0370 fail-fast path (exit 75, restart, replay from the vlog).
4. **Optionally:** extend build verify (0375) to check key order while it re-reads, and add flush readback.

## Acceptance
- Unit tests: inject a bit flip between memtable sort and encode (test seam) → the flush fails retryably and no run is published; the retry succeeds. Same for merge-output encode. Knockout tests.
- Cost: microbench of the extra hash/order check per entry (expected negligible against encode).
- Hardware: the next soak shows any `vals_encode_integrity_failures_total` events instead of persisted mis-ordered runs.

## Acceptance criteria

## Work log
- 2026-10-06 (implementation, uncommitted): encode verification before the block CRC.
  - **Attestation:** the 0370 guard now hashes the cloned key it hands out; `Index::flush_page_attested` returns per-memtable-block-piece `SourceGroup {entries, sum}` (hash memtables: groups of 64 copies). Flush snapshot/plan carry the groups; flushes no longer re-sort.
  - **Check point:** `run/encode_check.rs` `EncodeCheck::block`: encode body → CRC → decode the same buffer: strict key ascent (also across blocks), 0370 entry hash per decoded entry vs attested group sums (flush) or per-entry push-time hashes (streamed merge; `push_owned` avoids the clone; sync merge/test builds hash inline). Bloom, index first keys, min/max come from decoded bytes; index verified after its CRC (first-key hashes, ascent, contiguous offsets to bloom_off).
  - **Failure:** `EngineError::EncodeIntegrity` (via `SnapshotError::EncodeIntegrity` for the streamed builder), segments freed by existing flush/merge failure paths, `vals_encode_integrity_failures_total{path}`, ERROR with check/block/entry/source first key and single-bit diagnosis (stage=encode diff or stage=source search). Off-core flush failures feed the 0370 fail-fast streak; streak now cleared on flush commit (not plan Ready).
  - **Tests:** run/encode_check (buffer flip, attested source flip, order, index, stream entry flips, knockouts ORDER/SOURCE_HASH/INDEX), engine flush (source flip ×{hash,order}×{off-core,sync}, encode buffer flip, knockouts publish), engine merge (after-hash, before-hash order, buffer flips + knockouts), index paged attestation, cluster fail-fast persistent/transient.
  - **Cost (release microbench `encode_check_cost_microbench`, 400k ~150 B keys):** flush encode +~42 ns/entry (~14%); merge-output encode +~51 ns/entry with push_owned (~16%), +~79 ns with clone (~24%).
  - **Residual:** merge stream decode→push window (order only), v4 sidecar from source, bloom bits after insert, key-filter sink, post-CRC copies (CRC error at read; flush has no readback). Hardware acceptance (next soak) open.

### 2026-10-06: review (agent_2: mergeable) and commit; SOAK5B-V4-R3 early stop on lab-2
- **Review:** coverage confirmed for off-core flush, sync flush and streamed merge (sync merge from its output Vec). No realistic false positives: snapshots are always byte-ascending, duplicates can't reach the encoder, tombstones are covered. Cleanup correct; no spurious fail-fast from the streak change.
- **Minors, follow-ups:**
  1. a stale streak window after non-integrity build failures;
  2. order/decode failures key the streak on the run block's first key (unstable under load), so it should be keyed on the source group;
  3. the merge source-to-push window is not hashed, and flush output has no device readback (post-CRC);
  4. a failed streamed merge keeps reading all inputs (no early stop);
  5. hash-zone snapshot slices add one FxHash per entry on the shard;
  6. diagnosis cost grows with key length², so cap it per key;
  7. nits.
- **Cost (reviewer's microbench):** flush encode +42 ns/entry (+13%); merge output +31–34 ns/entry with `push_owned`. Runs on maintenance threads.
- **SOAK5B-V4-R3** (pin 5da8dee8 = this exact diff, sha-verified) stopped at 1.55B ACK under the amended same-zone build-verify rule. lab-2 zone 2 had an off-core merge read `crc mismatch` (10:52:34Z), then a build-verify failure (10:54:04Z); 0 crashes; nothing published. lab-2 also failed in V4 and V4-R2, while lab-1/3 ran clean in all three.
- **Hypothesis:** a bad physical memory region on lab-2 inside the SPDK hugepage (DMA) pool, which the 2 h memtest did not cover. To be tested.

### 2026-10-06: in_review (backlog cleanup, owner-approved)
- **Owner:** lab-2's RAM is known bad. Do not test it further (no hugepage memtest), and no DIMM replacement (budget). Treat its integrity events as expected hardware faults that the checks must catch before publication.
- **Code:** committed and reviewed. The review minors (1–7 above) are filed as a follow-up task.
- **Hardware acceptance:** V4-R3 already showed the intended behaviour (faults caught, nothing published). Confirm `vals_encode_integrity_failures_total` events versus persisted bad runs on the full-replay run, then close.

### 2026-10-06: second independent review (agent_3, astra): CLOSE WITH FOLLOW-UPS; closed
- **No new defects:**
  - No false positive on legitimate input: hash-zone snapshots are byte-ordered, attestations are owned copies, and page splits are safe.
  - Every production run-block builder is checked (both flushes, sync merge, streamed compaction including forget/pressure/shadow/GC).
  - `EncodeIntegrity` cleanup happens before publication and frees reservations exactly once.
  - The streak change is sound.
- Debug suites pass on 36e57156.
- **Correction to the note above:** V4-R3 did NOT show this detector firing. Both encode-integrity counters stayed 0; the faults there were caught by other mechanisms (merge input-read CRC, build verify). The hardware criterion is carried to 0386, not passed.
- **Cost note, added to 0387 item 5:** sorted-zone snapshots also allocate a `Vec<u64>` per memtable block (`checked_pairs`, index.rs ~791-813) and store group attestations on the shard. These are not covered by the encode microbench.


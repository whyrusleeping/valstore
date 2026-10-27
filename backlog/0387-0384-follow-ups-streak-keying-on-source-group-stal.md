---
id: "0387"
title: '0384 follow-ups: streak keying on source group, stale streak window, merge push-window hash, early stop on failed streamed merge, shard hash cost, bounded diagnosis'
status: done
priority: 3
created: "2026-10-06"
updated: "2026-10-12"
depends_on: []
spec_refs: []
---

## Description
Review minors from agent_2's review of 0384 (2026-10-06), none a correctness blocker:
1. A stale streak window after non-integrity build failures.
2. Order/decode integrity failures key the 0370 fail-fast streak on the run block's first key, which is unstable under load. Key them on the source group instead.
3. The merge source-to-push window is not hashed (order only), and flush output has no device readback after the CRC.
4. A failed streamed merge keeps reading all its inputs (no early stop).
5. Hash-zone snapshot slices add one FxHash per entry on the shard thread. Measure it; this is a shard cost, so it matters for 0385. Also measure, on sorted zones, the per-memtable-block `Vec<u64>` that `checked_pairs` allocates (index.rs ~791-813) and the stored group attestations (second review, agent_3).
6. The single-bit diagnosis cost grows with key length². Cap it per key (cf. 0372 item 2).
7. Nits from the review.

Acceptance: fix, or justify not fixing, each item, with tests for 1, 2 and 4, and microbench numbers for 5.

## Acceptance criteria

## Work log

- 2026-10-10: consolidated (owner-approved). **Items 2 and 6 moved to 0372**, which shares the same streak-keying and diagnosis-cost code; don't implement them here. Remaining scope: items 1, 3, 4, 5, 7.
- 2026-10-10: **item 1 (stale streak window) also moved to 0372**. The 0372 review found that the new (zone, engine) streak keying makes the stale window worse, so it is fixed in the same change. Remaining scope here: items 3, 4, 5 and 7. Start after 0372 lands (shared files).
- 2026-10-10: 0372 (which includes items 1, 2 and 6) is integrated in the main tree. Items 3, 4, 5 and 7 can start once it is committed.
- 2026-10-11: **item 5 split to 0430 and done there** (bench/0430-attest-cost/FINDINGS.md). It needed no edits to the shared files. Decision: no fix. Hash-zone attestation costs 20–25 ns per entry (0.6–1.0% of shard µs/write). On sorted zones, a build with 0384's increment removed (hashes vector, zip and group sums gone; 0370 check kept) measured 2–6 ns per entry less (0.06–0.24%). These are point estimates, no larger than the ±6.5 ns that the control's pooled medians move between builds. The scratch-buffer variant measured neutral. Stored groups take 0.25–0.36 B per entry. **Remaining scope here: items 3, 4 and 7.** They still edit run.rs, encode_check.rs and index.rs, so start them after 0372 is committed.

### 2026-10-11: items 3, 4 and 7 implemented; integrated in the main tree, uncommitted (run.rs is shared with 0371/0372)
- **Item 4 (fixed):** `RunStreamBuilder::take_failure` (run.rs); the streamed merge loop (`engine/merge.rs`, clean file) returns `EngineError::EncodeIntegrity` at the end of the chunk (≤8192 entries) in which a block fails, before any further input read, vlog-writer finish or tail write. Same cleanup as the other in-loop errors (the caller frees all reserved segments). Test `engine::tests::merge::failed_streamed_merge_stops_reading_its_inputs` (readahead off, 2×30000 interleaved keys, flip at entry 700, counts device read bytes): the failed merge reads 426 KB of 3.05 MB input (14%); with the check knocked out it reads 3.01 MB and the test fails; the retried clean merge reads 6.06 MB and publishes. vals-store lib suite (debug) 478 passed.
- **Item 3 (not fixed, justified):**
  - *Merge decode → push window.* Hashing it end to end means computing the 0370 hash at block decode and carrying it through `MergeStream`, the shadow probe and the co-compact pointer rewrite (which changes the hashed entry and would need a verified re-hash). That is roughly another 0384 increment per input entry (+31–51 ns/entry measured for the output check), and input entries outnumber output entries when merges drop shadowed versions. The exposure it closes is small: entries are decoded from CRC-checked blocks (a flip in prefetched raw bytes fails the block CRC) and stay unhashed only for one chunk (≤8192 entries, ~1–2 MB per running merge, milliseconds), against the memtable's GiBs held for tens of seconds that motivated 0384. Order-breaking key flips in the window are still caught by the encode order check. Left listed under "Not covered" in ARCHITECTURE.md.
  - *Flush output readback.* A flip after the block CRC is not silent: it fails the CRC at the first read (GET, merge input, AE seed), unlike 0384's pre-CRC flip. Readback would move detection before publication at the cost of re-reading every flushed byte on the ingest path (write throughput is the owner's top priority). Merges already get the full readback (0375); flushes keep the footer/filter/index CRC check (deploy.md, 0371 wording). Revisit only if a soak shows persistent CRC mismatches on flush-output runs.
- **Item 7 (nothing to do):** the agent_2 review's nits were not recorded beyond "nits", and no review artifact was kept; the second review (agent_3) found no defects. ARCHITECTURE.md's 0384 paragraph is updated for the 0372 streak keying ((zone, engine), not the block first key) and the early stop.
- **Review:** sol (standard tier), round 1: accept, no blocker or major. The reviewer reran the merge, run and cluster integrity suites (pass).
- **Pre-existing flake, not 0387:** `engine::tests::stress::concurrent_in_place_forced_windows_never_corrupt_merge_outputs` intermittently fails its `pressure` merge-coverage assertion in the current dirty tree: 2/13 runs with this change, 2/12 with the early-stop check knocked out. Any merge failure panics that harness, so the new branch never runs there.
- **Commit order:** run.rs holds 0371/0372's uncommitted hunks, so commit 0387 with or after them. The run.rs hunk here is only the 3-line `take_failure` accessor, so a 0371/0372 commit that sweeps it builds on its own; merge.rs, tests/merge.rs and ARCHITECTURE.md carry only 0387 changes.

## Outcome

Item 4: a streamed merge whose output fails the encode integrity check now stops reading its inputs at the end of the failing chunk (≤8192 entries) via Run::take_failure; test failed_streamed_merge_stops_reading_its_inputs reads 426 KB of 3.05 MB (3.01 MB with the check removed). Item 3 not fixed, with reasons in the work log (per-entry hashing cost on the merge path; flush readback would re-read every flushed byte). Item 5 was 0430. Item 7: ARCHITECTURE.md 0384 paragraph updated. vals-store lib suite 478 passed; one sol reviewer accepted. Stress-test flake tracked as proposed 0431.

Commit: fix(0387): failed streamed merge stops reading inputs at the failing chunk (<=8192 entries); item 3 justified not fixed; ARCHITECTURE 0384 paragraph updated

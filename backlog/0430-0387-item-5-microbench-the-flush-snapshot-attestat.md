---
id: "0430"
title: '0387 item 5: microbench the flush-snapshot attestation shard cost (hash-zone FxHash per entry, sorted checked_pairs Vec<u64>, stored group attestations)'
status: done
priority: 3
created: "2026-10-11"
updated: "2026-10-11"
depends_on: []
spec_refs: []
---

## Description

Split from 0387 (item 5) so it can proceed while 0387 items 3/4/7 wait for 0372's commit (they edit run.rs/encode_check.rs/index.rs, which carry 0372's uncommitted in-review hunks). This slice edits none of those files.

Scope: measure, on the shard-thread flush-snapshot path (`Index::flush_page_attested` in `FLUSH_SLICE_ENTRIES` pages, as `Engine::flush_plan_step` calls it):
- hash zones (HashIndex): the per-entry FxHash attestation vs the unattested `maintenance_page`;
- sorted zones (FrontCodedIndex): `checked_pairs` + hash vs unattested decode, and specifically the per-block `Vec<u64>` hashes allocation (A/B against a scratch-buffer variant in a separate worktree);
- the stored `SourceGroup` attestations (bytes per entry).
Relate the numbers to shard µs/write (0390/0416) and R3's hardware profile (SUMMARY-0391 node1 `flush_page_attested` self kcycles/write).

Acceptance: an ignored microbench kept under bench/0387-attest-cost/ with a FINDINGS.md giving ns/entry per arm (interleaved repeats), the derived µs/write share, and a fix-or-justify decision for item 5; 0387's work log updated so its remaining scope is items 3, 4 and 7.

## Outcome

Ignored microbench, variant patches, raw log, analyzer and FINDINGS in bench/0430-attest-cost/ (1M-entry shuffled indexes, 4096-entry pages, 3 builds × 4 rounds × 7 trials). - Hash zones: 20–25 ns/entry (0.6–1.0% of 2.5–3.5 µs/write). - Sorted zones: 0384's increment over the 0370 check, measured against a pre0384 build, is 2–6 ns/entry (0.06–0.24%). Point estimates, no larger than the ±6.5 ns that control medians move between builds. - Per-block Vec<u64> scratch reuse: neutral, not adopted. - Stored groups: 0.25–0.36 B/entry. No fix. The review's request to narrow the claim was applied and accepted. 0387 keeps items 3, 4 and 7 (gated on 0372's commit).

Commit: bench(0430): 0387 item 5 flush-snapshot attestation cost — hash zones 20–25 ns/entry (0.6–1.0% shard µs/write), sorted 0384 increment 2–6 ns/entry, Vec<u64> scratch neutral; no fix; 0387 keeps items 3/4/7

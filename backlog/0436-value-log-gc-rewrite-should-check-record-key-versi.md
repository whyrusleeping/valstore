---
id: "0436"
title: Value-log GC rewrite should check record key/version against the merge entry (wrong-record pointer laundering)
status: done
priority: 3
created: "2026-10-13"
updated: "2026-10-14"
depends_on: []
spec_refs: []
---

## Description

Follow-up from 0339. `engine/merge.rs::rewrite_chunk_values` reads victim records through `plan_read_spans_detached(..).execute_sync` (length/CRC only) and re-appends them with the *merge entry's* key and version plus the *record's* value. A pointer that lands on a different valid record of the same length (the 0339 wrong-record case) is therefore rewritten into a self-consistent record holding another key's value, after which no read-time check (0339) can detect it.

Proposal: use a checked span decode (`SpanReadPlan` already has the slot→(key, version) expectation machinery for `execute_span_checked`) in the GC rewrite, so the merge fails exactly as it already does on a bad length. Decide the failure policy first: today a bad length fails the merge on every retry (merge stall); confirm that is acceptable for key/version mismatches too, or route it to the integrity fail-fast/watch path.

Acceptance: a local test with a wrong-record pointer in a GC victim segment fails the merge instead of publishing a rewritten record; no persisted-format change.

## Outcome

GC/co-compaction rewrites read victims via new SpanReadPlan::execute_sync_checked with the entry's (key, version); any corrupt record (key/version/length/CRC) fails the merge with EngineError::CorruptValueRecord before publication — nothing re-appended or dropped, no exit 75. The vals-cluster maint loop (normal + cleanup compactions) calls integrity::corrupt_merge_record: existing vals_store_corrupt_reads_total kinds, 0339 rate-limited ERROR line with path "merge rewrite", and Engine::exclude_from_value_gc(segment) (in RAM, logged once) so victim selection, aged-GC candidate accounting, the exhaustion estimate and All-mode rewrites skip it and other maintenance continues. Documented in deploy.md §Corrupt value reads. New store test (key + version cases, fails without the fix) and cluster test; full vals-store and vals-cluster lib suites pass. No format change.

Commit: fix(0436): GC rewrites check record key/version against the merge entry; a mismatch fails the merge unpublished, is counted and logged, and the segment is excluded from GC until restart

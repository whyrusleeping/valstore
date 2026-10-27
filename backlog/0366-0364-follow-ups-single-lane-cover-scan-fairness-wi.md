---
id: "0366"
title: '0364 follow-ups: single-lane cover-scan fairness, winner-directory flap, merge-sink reservation, planner allocation'
status: done
priority: 4
created: "2026-10-02"
updated: "2026-10-12"
depends_on:
    - "0364"
spec_refs: []
---

## Description
Second-review (agent_5, 2026-10-02) minor items left after the 0364 commit d4be3026. None is a correctness bug.

1. **One compaction lane.** With `maint_compact_lanes=1`, the single lane takes every cover-scan chunk before normal merges (`maint.rs:370` always sets scan_lanes=1, and there is no linger with one lane). With many engines, recurring/forced merges wait for the whole post-restart coverage pass. Let one normal job through after every N scan chunks; test it.
2. **Two lanes (default).** Lane 0 refuses normal merges while scan chunks keep finishing, so merge concurrency halves until coverage completes. Measure the merge backlog and run count during coverage in the FULL4-KF2 restart check, and tune (e.g. a lane-0 duty share) if it hurts.
3. **Winner-directory flapping.** After a degrade, `rebuild_winner_directory` is still attempted on merges. Alternating failure/success drops the filter and triggers full re-scans repeatedly.
   - Skip the rebuild once degraded, or rate-limit it.
   - Add a test for degrade → None → re-cover.
4. **Merge-sink reservation.** When the sink's sub-filter is newer than the one holding the covered inputs' keys, re-inserting those keys overfills it beyond the reservation (higher FPR, earlier `stale()`). Reserve all input entries in that case.
5. **Per-tick allocation.** `cover_scan_merge_soon_start` allocates a sizes Vec every tick per zone even when every run is covered. Check for an uncovered run first.

## Validation amendment (0407, owner-approved 2026-10-08)
Tier L0 for items 1, 3, 4 and 5; see docs/validation.md. Item 2 is L1: measure it locally by reopening a file-backed engine with a planted merge backlog during a cover-scan, then assert merge concurrency (≥2 lanes). A hardware restart is optional.

## Acceptance criteria

## Work log

### 2026-10-03
FULL5-V5 restart check: coverage at +397–467 s, but with 0 merges in flight. A test reopen must happen right after the drain without settle (merge backlog present) to exercise item 2.

### 2026-10-03: FULL6-NOVERIFY restart immediately after the drain (no settle)
`bench/0363-compact-values/FULL6-FINDINGS.md`.
- **Before the restart:** 2 merges in flight per node.
- **After the reopen:** ~200 uncovered runs (210 GB) per node; at most ONE merge ran at a time (vs 2 before); the last merge was in flight at +142–172 s; the first cover commit at +112 s; fully covered at +438–482 s (from the new process), with 0 merges in flight.
- **Confirms item 2:** merge concurrency halves while cover-scans run.
- **Coverage time:** well within 20 min, but this workload's post-drain backlog is too small to test the "under merges" condition. Testing it needs a mid-ingest restart.

### 2026-10-10: implemented, reviewed, and integrated into the main tree (uncommitted)
- **Item 1:** with 1 lane, the reserved scan lane admits one normal job after every 4 scan chunks (`COVER_SCAN_DUTY_CHUNKS`). Tests cover saturation, lanes ≥ scan_lanes and linger. The knockout fails.
- **Item 2:** resolved as measured. Concurrency does halve during coverage. A 2-lane duty share gave earlier merges, but in the realistic small-chunk proxy it made coverage 6.5× slower and drain 3–4% worse. Per the owner's rule it was not adopted, and the 2-lane default is unchanged. The 0407 "assert ≥2 lanes" check is not adopted, for this reason. Numbers are in bench/0366-cover-scan-followups/FINDINGS.md.
- **Item 3:** no automatic winner-directory rebuild after a process-wide degrade. The fan-out to all zones is documented and follows the existing fail-closed design.
- **Item 4:** already fixed by 8178aac1 (0396); the test was strengthened.
- **Item 5:** the covered-zone planner returns early. An allocation-counting test passes; the knockout fails.
- Review: opus (agent_7). Its major findings (the 2-lane share and a flaky knockout) were fixed; the minors are addressed or documented.
- 2026-10-10 (integration): the combined main-tree run found that item 3 broke `seed_pin_tests::failed_post_commit_directory_rebuilds_still_journal_retired_winners`. Its second commit expected a post-commit rebuild after a process-wide degrade, and that rebuild no longer happens by design. The coordinator split it into two isolated tests, one per first-failing path (`merge_all_runs`, off-core `compact_commit`). The merge-first test now also asserts that no rebuild is attempted after the degrade. Results: vals-store lib 472/0; vals-cluster lib 807/0, apart from the known 0380 item 7 flake; vals-server bin and tests all green.

## Outcome

Items 1, 3, 4 and 5 fixed with tests. Item 2's proposed tuning measured worse locally (coverage 6.5× slower, drain 3–4% worse), so it was not adopted and the 2-lane default is unchanged; this drops the 2026-10-08 'assert ≥2 merge lanes' check (bench/0366-cover-scan-followups/FINDINGS.md). Integration split a seed-pin test into one isolated test per failure path (landed with 0377's seed_pin_tests.rs). Integrated in s_8666773a (2026-10-11); committed 2026-10-12.

Commit: fix(0366): 0364 cover-scan follow-ups — items 1/3/4/5 with tests; item 2 tuning measured worse (coverage 6.5x slower, drain 3–4% worse), 2-lane default kept

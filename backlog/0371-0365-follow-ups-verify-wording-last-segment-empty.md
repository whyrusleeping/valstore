---
id: "0371"
title: '0365 follow-ups: verify wording/last segment, empty VALS_RUN_WRITE_VERSION, digest plan after-trap, test gaps, downgrade check'
status: done
priority: 3
created: "2026-10-02"
updated: "2026-10-12"
depends_on:
    - "0365"
spec_refs: []
---

## Description
Minor items from the agent_5 review of 0365. The review found no blocker or major issue and mutation-tested the equivalence property.

1. **Verify can't see the last segment.** `read_logical` (`run.rs` ~1262-1282) serves the final not-yet-written segment from the sink's RAM buffer, so the build verify cannot check that segment's device write; the old v4 readback had the same gap. Fix the wording of the `vals_run_build_verify_bytes_total` help text and the deploy.md text ("re-read" overstates it). State explicitly that the sync `merge_all_runs` path (`merge.rs` ~957) and flushes are not verified. Optionally verify the last segment after its write completes.
2. **Empty value selects v5.** `VALS_RUN_WRITE_VERSION` set but empty (`Some("")`, `run.rs` ~501) selects v5, while configuration.md says other values write v4, so a templated unset variable silently closes the rollback window. Map empty to 4 (with an error log) or document it.
3. **Plan/execute `after` trap.** `version_source(after)` fixes the start block at planning time, while `version_digest_page_from_plan*` takes `after` separately. A future caller passing an earlier `after` at execution would silently skip keys. Store `after` in `DigestPagePlan` (or debug_assert equality).
4. **Test gaps:**
   - add a block larger than the 256 KiB window (oversized-block branch);
   - re-add v4 coverage to `run_len_upper_bound_dominates_build_run_over_randomized_shapes` and tighten its slack (num_buckets up to 65535 adds up to 256 KB);
   - run `tests/seed_apply_ab.rs` on both formats (planned, not done);
   - replace the vacuous "v5 with sidecar selector" knockout with a meaningful one;
   - encode the tombstone-drop knockout as a permanent test.
5. **Downgrade check.** Confirm what a pre-0365 binary does on "unsupported run version" at open: it should fail closed without side effects (no recovery writes or manifest changes before the error). Document the result in deploy.md.

## Acceptance criteria

## Work log

- 2026-10-10: implemented, reviewed (sol, agent_9: no blocker or major issue), and integrated into the main tree, uncommitted.
  - **Item 1:** already fixed by 0375 (90a3db95). The streamed build verify re-reads the last segment from the device after `sink.finish()`. deploy.md now states that flushes and `merge_all_runs` don't re-read data blocks.
  - **Item 2:** an empty or whitespace `VALS_RUN_WRITE_VERSION` now writes v4 and logs an error; docs updated. The reviewer grepped the harnesses: nothing relies on empty meaning v5.
  - **Item 3:** `DigestPagePlan` stores `after`, with a debug_assert at execution. The stored key is charged in `resident_bound_bytes` (review minor, fixed by the coordinator).
  - **Item 4:** the bound test covers v4 and v5 with uniform-key shapes; new permanent tests for the start block and tombstones; oversized blocks were already covered by 0378; `seed_apply_ab` was run on both formats. The knockouts fail as expected.
  - **Item 5:** a pre-0365 valsd fails closed with `unsupported run version`: 0 writes and a byte-identical data dir, independently reproduced by the reviewer. Recorded in deploy.md.
  - **Follow-up:** proposed 0419 (legacy header slack in the run bound).

## Outcome

All items done. An empty VALS_RUN_WRITE_VERSION (unexpanded template) now logs an error and writes v4; a pre-0365 binary refuses v5 data without writing anything (the reviewer reproduced this). Reviewed and integrated in chat session s_8666773a (2026-10-11); committed 2026-10-12.

Commit: fix(0371): 0365 follow-ups — v5 run-format verify wording, last-segment-empty, empty VALS_RUN_WRITE_VERSION logs and writes v4; pre-0365 binaries refuse v5 data without writing

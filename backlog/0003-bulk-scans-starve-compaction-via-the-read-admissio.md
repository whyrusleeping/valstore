---
id: "0003"
title: Bulk scans starve compaction via the read-admission quiesce gate
status: done
priority: 2
created: "2026-07-02"
updated: "2026-07-04"
depends_on: []
spec_refs: []
---

## Description
Sub-bug of the bulk-export finding (BACKLOG.md §Efficiency/healing): compaction waits for in-flight reads to drain (100 ms, `COMPACT_QUIESCE_WAIT`); a sustained bulk scan keeps a read in flight continuously, so the node spams `compaction skipped — 1 in-flight read did not drain within 100ms` and the LSM never compacts down while being scanned → read-amp stays high exactly when compaction would help. A bulk reader must not be able to indefinitely block compaction.

Candidate fixes: a low-priority / yielding bulk-read admission class, or a periodic forced-compaction tick the gate can't starve.

## Acceptance criteria
- Kill-the-fix test: under a sustained scan load that holds a read in flight at every gate check, compaction still runs within a bounded number of ticks.
- No correctness regression: the gate's original invariant (no read mid device-await against a segment the pass frees) still holds, pinned by existing tests.

## Acceptance criteria

## Plan

## Problem

The background compactor (`compact_loop`, crates/vals-cluster/src/node.rs) quiesces reads before a pass: it pauses new read admission via the `ReadGate` and waits a fixed `COMPACT_QUIESCE_WAIT` (100 ms) for in-flight reads to drain. A sustained bulk scan holds each `InflightGuard` across a whole page's device reads (up to `SCAN_REPLY_BUDGET` = 8 MiB of span reads on a pegged core), which routinely exceeds 100 ms — so every tick's drain times out, the pass is skipped, and compaction starves indefinitely while being scanned (exactly when it would help).

## Key insight that picks the fix

Every `InflightGuard` holder (`serve_read_into`, `serve_digest`, `serve_bucket_digests`, `serve_scan`, `serve_sync_values`) drops the guard before any network send — hold time is bounded by device-read completion only. Since a pause blocks *new* admissions, waiting longer is **guaranteed to drain** (absent a wedged device). So the minimal, safe fix is: **escalate the drain bound after consecutive timeouts** (the "forced-compaction tick the gate can't starve" candidate from the task), rather than the far more invasive yielding bulk-read admission class (which would require invalidating/rebuilding a page's ReadPlan mid-flight, since the plan pins vlog segments).

## Changes (crates/vals-cluster/src/node.rs, small accessor in net/server.rs)

1. Add `COMPACT_QUIESCE_WAIT_MAX: Duration = Duration::from_secs(2)` with a doc comment explaining: why escalation is safe (no guard held across a network send; pause blocks new admissions, so drain is bounded by one in-flight read's device time), and why it is capped (a genuinely wedged device read must not pause read admission unboundedly — serving reads still outranks reclamation in the pathological case).
2. Extract a small pure helper, e.g. `fn quiesce_wait(consecutive_timeouts: u32) -> Duration`, that doubles `COMPACT_QUIESCE_WAIT` per consecutive timeout and clamps to `COMPACT_QUIESCE_WAIT_MAX` (use saturating/checked arithmetic; no overflow at large counts).
3. `drain_reads` takes the wait `Duration` as a parameter instead of reading the const.
4. `compact_loop` keeps a `consecutive quiesce timeout` counter across ticks: reset to 0 on `Drain::Drained`, saturating-increment on `Drain::TimedOut`. The timeout warn log should include the bound actually used this tick (so the escalation is visible in logs). The existing `compaction_quiesce_timeouts` counter keeps counting each skip.
5. Update the `compact_loop` doc comment (the "Read safety" paragraph) to describe the escalation.

## Tests (in node.rs test module, modeled on `compaction_quiesces_inflight_reads_instead_of_starving`)

6. Unit test the escalation helper: base at 0, doubling at 1..n, clamped at the cap, no overflow at u32::MAX.
7. Kill-the-fix integration test `compaction_escalates_quiesce_bound_under_sustained_bulk_reads`:
   - Store with reclaimable garbage (same setup as the existing quiesce test).
   - A reader task that models a bulk scan whose page read outlives the base bound *deterministically*: acquire the guard, then poll a short (~5 ms) timer until it observes the gate paused, then keep holding for a fixed 150 ms *after observing the pause*, then drop and re-acquire **in the same poll** (drop + immediate acquire, as the existing test does, so `inflight` is never 0 across an await point). This guarantees the base 100 ms drain times out at every gate check, while the escalated 200 ms bound drains — old code fails this test (never compacts before the deadline), new code passes within a few ticks.
   - Needs a tiny `pub(crate) fn paused(&self) -> bool` accessor on `ReadGate` (net/server.rs) for the reader to observe the pause; keep it `pub(crate)`.
   - Assert `used_segments` drops below the pre-test count within a generous deadline (~10 s), then clean shutdown as the existing test does.
   - Actually verify kill-the-fix: temporarily pin the wait to the base const and confirm the new test fails, then restore (mention the check in the report; don't commit the reversion).
8. Existing gate tests (`read_gate_pause_parks_admission_until_resumed`, `read_gate_signals_drained_only_when_paused_and_empty`, `compaction_quiesces_inflight_reads_instead_of_starving`) must still pass unchanged — they pin the original invariant.

## Docs

9. BACKLOG.md: mark sub-bug 1 under the "No fast sequential bulk-read / export path" finding as resolved (escalating quiesce bound), matching the file's existing "(Resolved so far: ...)" conventions — a brief note is enough.

## Verification

- `cargo build` + `cargo test -p vals-cluster` (note: the suite has a known pre-existing ordering-dependent SIGABRT under rustc 1.96 — task 0031; if hit, run the relevant test targets individually and note it).
- `cargo fmt --check` / clippy if configured.

### Starting points
- crates/vals-cluster/src/node.rs:640-860 — compact_loop + COMPACT_QUIESCE_WAIT + drain_reads (lines ~1152-1201)
- crates/vals-cluster/src/net/server.rs:2032-2155 — ReadGate / InflightGuard / ReadPause
- crates/vals-cluster/src/node.rs:2269-2348 — existing test compaction_quiesces_inflight_reads_instead_of_starving (pattern to model the new test on)
- crates/vals-cluster/src/observe.rs:222 — compaction_quiesce_timeouts counter
- BACKLOG.md lines ~745-775 — the bulk-export finding, sub-bug 1 is this task

## Work log
- 2026-07-04 plan: ## Problem  The background compactor (`compact_loop`, crates/vals-cluster/src/node.rs) quiesces reads before a pass: it pauses new read admission via the `ReadGate` and waits a fixed `COMPACT_QUIESCE_
…[truncated]
- 2026-07-04 context hints: 5 recorded with plan
- 2026-07-04 context hints: crates/vals-cluster/src/node.rs:640-860 — compact_loop + COMPACT_QUIESCE_WAIT; drain_reads at ~1152-1201; crates/vals-cluster/src/net/server.rs:2032-2155 — ReadGate / InflightGuard / ReadPause; cr
…[truncated]
- 2026-07-04 implementer report: Implemented Task 0003: bulk scans no longer starve compaction via the read-admission quiesce gate. The fix escalates the compactor's drain bound after consecutive quiesce timeouts (the "forced tick th
…[truncated]
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change implements the "escalating quiesce drain bound" fix exactly as planned: a pure `quiesce_wait(consecutive_timeouts)` helper doubles the 100ms base per consecutive timeout, clamped to a 2s ca
…[truncated]
- 2026-07-04 decision: accept — commit: Fix compaction starvation under sustained bulk scans: escalate the quiesce drain bound (task 0003)  The compactor's read-quiesce waited a fixed 100ms for in-flight reads to drain; a bulk scan page's d
…[truncated]
- 2026-07-04 usage: 66,765 tok (in 202, out 66,563, cache_r 3,989,852, cache_w 219,879) · cost n/a (unpriced)
  implementer: 44,029 tok (in 112, out 43,917, cache_r 2,576,946, cache_w 75,245) · cost n/a (unpriced)
  coordinator: 17,253 tok (in 58, out 17,195, cache_r 1,161,297, cache_w 120,596) · cost n/a (unpriced)
  reviewer:Claude: 5,483 tok (in 32, out 5,451, cache_r 251,609, cache_w 24,038) · cost n/a (unpriced)

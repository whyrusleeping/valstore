---
id: "0155"
title: 'Re-home robustness under churn: progress-aware report deadline, epoch-stable copy resume, PD rehome event logging + per-cause abort counters'
status: done
priority: 3
created: "2026-08-08"
updated: "2026-08-10"
depends_on: []
spec_refs: []
---

## Description
Box validation of 0142 on lab (task 0151, bench/lab-rehome-20260808/FINDINGS.md) found the protocol safe and convergent but abort-heavy under sustained write churn (~50% of prepares aborted), traced to three compounding design choices:

1. **Completion-based report deadline.** `VALS_PD_REHOME_REPORT_DEADLINE_MS` (60 s) fires unless every required owner has *completed* (copy-complete / zero-failure mirror report at the prepare epoch). A pending owner making steady copy progress reports nothing until done, so any cross-node copy slower than 60 s aborts. Fix: have pending owners report copy progress (cursor position or entries-copied) in `OwnedRangeStat` and reset the PD's `last_report` on progress, aborting only on *stalled* copies; or scale the deadline with range size.
2. **Epoch-keyed resume cursor.** `rehome_copy_loop` keys its scan cursor by `(zone, lower, epoch)` (crates/vals-cluster/src/node.rs:1895,1953). Every abort/re-prepare bumps the epoch, so the retry re-scans the whole range from `lo` even though prior pages were durably applied. Keying by `(zone, lower, pending_token)` with epoch validation would make retries incremental.
3. **Copy pace.** One `REHOME_COPY_PAGE_ENTRIES=4096` page per 100 ms tick per range ≈ 41k entries/s ceiling; consider adaptive paging when the serving path is idle.

Also an observability gap: the PD logs auto-splits but no rehome prepare/commit/abort events, and `pd_rehomes_aborted` does not distinguish report-gap from mirror-failure aborts. Add log lines (zone, range lower bound, reason) and split the abort counter (or a reason label).

Acceptance criteria:
- Under a sustained-churn in-process test, a re-home whose copy takes several deadline periods completes without abort while genuine stalls (dead pending owner) still abort within the deadline.
- Aborted-then-retried copies resume rather than restart (test observes bounded re-scan work).
- PD logs prepare/commit/abort with reasons; abort counters attribute cause.
- Existing 0142 safety tests (mirror-failure abort, zero-gap cutover, interruption) stay green.

## Acceptance criteria

## Plan

Goal: make re-home robust under sustained write churn — (1) progress-aware report deadline, (2) safe epoch-stable copy resume, (3) PD rehome event logging + per-cause abort counters, (4) copy-pace env levers. All acceptance criteria are local in-process tests; `VALS_PD_REHOME_ENABLED` default stays OFF.

Background (from bench/lab-rehome-20260808/FINDINGS.md + code read): the PD's 60 s report deadline (`rehome_report_gap_due`, pd.rs:626) aborts on any required (node,shard) whose RangeStats report hasn't arrived; under churn reports get dropped (whole-reply `epoch < snap.range_map_epoch` filter at pd.rs:662, or blocking_request timeouts on busy shards), so healthy slow copies abort ~50% of prepares. Each abort/re-prepare bumps the epoch, and `rehome_copy_loop` (node.rs:1937) keys its cursor by `(zone, lower, epoch)`, so retries re-scan from range start.

## A. Progress-aware report deadline (pd.rs + node.rs + server.rs + wire)

1. Node side: `RehomeMirrorStat` (net/server.rs:109) gains `copy_progress: u64` — a monotonic count of entries applied by `rehome_copy_loop` for that (zone, lower). Bump it after each successfully applied page (node.rs ~2016-2027). `range_stats_reply` exports it as new field `rehome_copy_progress` on `wire::OwnedRangeStat` (encode+decode in vals-client/src/wire.rs; internal-only — valsgo never decodes RangeStats, verified; update wire tests + vals-cluster/tests/wire_fuzz.rs).
2. PD side (range_stats_poll): stale-epoch replies must still feed liveness/progress tracking. Restructure the collection at pd.rs:653-680 so reports with `epoch < snap.range_map_epoch` are EXCLUDED from `agg` (split/merge/candidate decisions unchanged) but INCLUDED in the per-range `reports` used for deadline tracking. Commit conditions keep their per-field epoch checks (`rehome_mirror_epoch >= snap.range_map_epoch`, `rehome_copy_complete_epoch >= ...`) so cutover safety is unchanged.
3. New deadline semantics in `RehomeReportState`: per required (node,shard) keep `{ last_refresh: Instant, last_progress: Option<u64> }`. Refresh rules on report arrival: current-only owners refresh on any report (liveness); pending-target owners refresh when copy_complete_epoch >= snap.range_map_epoch OR progress value changed since last seen (first sighting counts as change). Abort reasons: `mirror_failure` (existing), `report_gap` (required owner silent past deadline), `copy_stall` (target reporting but no progress past deadline while incomplete). Keep `rehome_report_gap_due`-style pure helper(s) with unit tests (existing test at pd.rs:2254 updated/extended).

## B. Epoch-stable, SAFE copy resume (node.rs + server.rs + wire + transport)

Naive resume across epochs is unsafe: between abort and re-prepare, acked writes are NOT mirrored to targets, and docs/zones.md is explicit that AE cannot repair a former-owner-only key — a resumed copy that skips the already-scanned prefix would strand acked mutations written in the gap. So resume must be validated by the source:

1. Source-side write mark: each shard server maintains `range_write_marks: RefCell<FastMap<(ZoneId, Vec<u8> /*lower*/), u64>>`, bumped for every applied sorted-zone write whose range currently has NO pending token — do the bump inside `mirror_rehome_writes` (net/server.rs ~2860+), which already iterates every write and resolves its range/pending state (bump exactly the writes it declines to mirror). Marks are frozen while pending is set (those writes are mirrored), so they advance only during unmirrored windows.
2. New internal RPC: `Request::RangeWriteMark { zone_id, lower }` → reply with existing `Response::Watermark(u64)` (no response-enum change). New wire tag, encode/decode, server dispatch, `NetTransport` method. Update wire_fuzz.
3. Copier: replace `cursors: HashMap<(zone, lower, epoch), Vec<u8>>` with `HashMap<(ZoneId, Vec<u8>), CopyCursor { pending: Vec<u8>, epoch: u64, source: NodeId, mark: u64, cursor: Vec<u8> }`. Same epoch + same pending token → use cursor directly (today's behavior). Epoch advanced but pending token unchanged → RESUME iff same source is still a current owner AND `RangeWriteMark` returns the stored mark (no unmirrored writes touched the range since the copy began); otherwise restart from `lo`. Sample the mark from the source when creating the cursor entry (before/at first page). On restart or token change, replace the entry. Remove entries on completion (existing) and prune entries whose range no longer has pending.
4. Node-side observability counters: `rehome_copy_pages` (pages applied), `rehome_copy_resumes`, `rehome_copy_restarts` — these also let tests observe bounded re-scan work.

## C. PD rehome event logging + abort attribution (pd.rs + observe.rs)

- log::info at prepare send (zone, range lower, entries estimate), at commit (zone, lower), and log::warn at abort (zone, lower, reason: mirror_failure | report_gap w/ which (node,shard) | copy_stall).
- Counters: keep `pd_rehomes_aborted` as the total; add `pd_rehomes_aborted_report_gap`, `pd_rehomes_aborted_mirror_failure`, `pd_rehomes_aborted_copy_stall` (observe.rs counter list + HELP strings following existing pd_ counter conventions).

## D. Copy pace levers (small)

- `REHOME_COPY_PAGE_ENTRIES` becomes env-overridable `VALS_REHOME_COPY_PAGE_ENTRIES` (default 4096), and add `VALS_REHOME_COPY_PAGES_PER_TICK` (default 1) — a plain operator lever; no adaptive logic this task.

## Tests (vals-cluster; debug profile — release is timing-flaky per project memory)

1. Slow-copy survives multiple deadline periods: in-process cluster with rehome enabled, tiny `VALS_PD_REHOME_REPORT_DEADLINE_MS` (e.g. 500 ms) + small copy page (via new env) so the copy spans several deadlines; assert the move commits with 0 aborts (or at least completes and pd_rehomes_aborted stays 0) — uses existing net/tests.rs rehome harness (~lines 1874-2400).
2. Dead pending owner still aborts within the deadline (may already exist — extend existing kill test to assert the report_gap-attributed counter/log).
3. Resume: force an abort mid-copy with the copier alive (e.g. block/stall a required owner's reports or kill a non-source current owner), let PD retry, assert `rehome_copy_resumes >= 1` and total `rehome_copy_pages` bounded (< ~1.5× range/page_size across both attempts) and the move completes with exact key count.
4. Kill-the-fix safety test: same abort/retry but with a write to the already-scanned prefix during the gap → assert `rehome_copy_restarts >= 1` (mark validation forces rescan) and final data includes the gap write after cutover.
5. Pure-fn unit tests for the new deadline/refresh logic; wire round-trip tests for the new OwnedRangeStat field and RangeWriteMark request.
6. Existing 0142 safety tests (mirror-failure abort, zero-gap cutover, interruption, antientropy.rs rehome_copy_survives_sweeps_and_cutover) stay green: `cargo test -p vals-cluster` + `cargo check --workspace`.

## Docs

- docs/zones.md: rewrite the "remaining gate" paragraph (~lines 176-180) to describe the progress-aware deadline, validated resume, abort attribution; keep the default-off status wording accurate.

Out of scope (do NOT fold in): 0165's RangeMap clone caching in rehome_copy_loop (separate task; avoid gratuitous restructuring of that loop beyond the cursor logic), adaptive idle-based pacing, flipping the rehome default.

### Starting points
- crates/vals-cluster/src/pd.rs:621-772 — RehomeReportState, rehome_report_gap_due, range_stats_poll (abort/commit decisions at 762-770; prepare candidates at 819-848)
- crates/vals-cluster/src/pd.rs:662 — whole-reply stale-epoch drop that starves deadline tracking under churn
- crates/vals-cluster/src/node.rs:1934-2040 — rehome_copy_loop, REHOME_COPY_PAGE_ENTRIES, epoch-keyed cursors
- crates/vals-cluster/src/net/server.rs:109-114 RehomeMirrorStat; 2840-2904 mirror_rehome_writes (per-write pending resolution — bump write marks here); 2953-3002 range_stats_reply
- crates/vals-client/src/wire.rs:91-102 OwnedRangeStat (+ encode/decode ~1184, tests ~1567); Response::Watermark already exists — reuse for RangeWriteMark reply
- crates/vals-cluster/src/net/tests.rs:1874-2400 — existing rehome integration harness (range_stats helper, kill tests)
- crates/vals-cluster/src/antientropy.rs:1770 rehome_copy_survives_sweeps_and_cutover
- observe.rs counter list: rg -n 'pd_rehomes' — follow existing counter+HELP conventions
- gotchas: no explanatory comments in code; tree not rustfmt-clean (hand-format new regions only); run suites in debug, not --release
- safety argument for mark-validated resume: docs/zones.md:117-150 — ack-implies-mirrored + 'AE is not a fallback' means a skipped prefix with unmirrored gap writes permanently strands acked mutations

## Work log
- 2026-08-10 plan: Goal: make re-home robust under sustained write churn — (1) progress-aware report deadline, (2) safe epoch-stable copy resume, (3) PD rehome event logging + per-cause abort counters, (4) copy-pace e
…[truncated]
- 2026-08-10 context hints: 10 recorded with plan
- 2026-08-10 context hints: pd.rs:621-772 RehomeReportState / rehome_report_gap_due / range_stats_poll; 819-848 prepare candidates; 2254 existing pure-fn test; node.rs:1934-2040 rehome_copy_loop + cursors keyed (zone,lower,epoch
…[truncated]
- 2026-08-10 preload: 3 file(s), ~31 KiB seeded into implementer context
- 2026-08-10 review tier: high-powered — reviewers: sol, claude
- 2026-08-10 review (sol): unknown — reviewer error: turn 1: API returned non-200 status code 400: {"detail":"Unsupported parameter: max_output_tokens"}
- 2026-08-10 review (claude): revise — Strong, well-structured implementation that covers all four acceptance areas: progress-aware deadline with per-cause abort attribution and logging (PD restructure keeps cutover epoch checks intact and
…[truncated]
- 2026-08-10 review (sol): unknown — reviewer error: turn 1: API returned non-200 status code 400: {"detail":"Unsupported parameter: max_output_tokens"}
- 2026-08-10 review (claude): accept — The revision resolves the blocking safety issue: mark bumps for unmirrored writes now happen inside mirror_rehome_writes against the same range-map snapshot as the decline decision (with the active_re
…[truncated]
- 2026-08-10 decision: accept — commit: 0155: re-home robustness under churn — progress-aware report deadline, safe epoch-stable copy resume, abort attribution + rehome event logging, copy-pace levers. PD deadline is now progress-based: p
…[truncated]
- 2026-08-10 usage: 231,987 tok (in 362, out 231,625, cache_r 35,580,656, cache_w 849,011) · cost n/a (unpriced)
  coordinator: 189,220 tok (in 306, out 188,914, cache_r 33,332,009, cache_w 619,783) · cost n/a (unpriced)
  reviewer:claude: 42,767 tok (in 56, out 42,711, cache_r 2,248,647, cache_w 229,228) · cost n/a (unpriced)

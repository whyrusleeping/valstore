---
id: "0049"
title: 'Box validation: scan-pipeline follow-ons on SPDK (staged run-block cursors; streaming/prefetch)'
status: done
priority: 3
created: "2026-07-04"
updated: "2026-07-06"
depends_on:
    - "0030"
spec_refs: []
---

## Description
Hardware-bound validation of the scan-pipeline follow-ons (split from task 0030, same precedent as 0028 → 0042). On the SPDK cluster with the firehose corpus:

- Measure scan keys/s before/after the staged (borrow-free, async run-block cursor) scan page selection from task 0030, perf-profiled so the improvement is attributable (serving core no longer serialized on sync QD1 block reads under the borrow; other requests interleave during page selection).
- Measure the streaming/prefetched page change (follow-on task) the same way once it lands.
- 0 errors, value-verified results (counts byte-identical to the pre-change path on pinned windows).
- Confirm the `VALS_STAGED_SCAN=0` kill switch restores the old path.

**UNBLOCKED (2026-07-05)**: owner approved tearing down the live 4-node valsd cluster on oldbox; drives free to claim. Box access verified: `ssh oldbox` (repo at `~/vals`, passwordless sudo). Gracefully stop the old cluster (valsd pd + node1–4, root PIDs up since Jul 03) before claiming the drives.

## Acceptance criteria
- Box-measured scan keys/s improvement attributable to each change (perf-profiled), 0 errors, value-verified.

## Plan

## Scope

Hardware validation (oldbox, SPDK) of the two scan-pipeline follow-ons that have both landed:
1. **Staged scan page selection** (task 0030) — server-side, kill switch `VALS_STAGED_SCAN=0` (read per request in `serve_scan`).
2. **Client-side scan-page prefetch** (task 0048) — coordinator-side, kill switch `VALS_SCAN_PREFETCH=0` (read once at `Coordinator::new`; `set_scan_prefetch` A/B hook exists).

Acceptance: box-measured scan keys/s improvement attributable to each change (perf-profiled), 0 errors, value-verified; confirm `VALS_STAGED_SCAN=0` restores the old path.

## Step 1 — bench wiring (implementer): new `scanab` vals-bench workload

A server-level A/B harness in `crates/vals-bench/src/main.rs` (`mem | file | spdk` backends, following the `cocompact` precedent for corpus + verdict style and `distsorted` for the wire/coordinator shape):

- **Topology**: 1 node, 1 drive/region, RF=1, R=1, one **sorted** zone served by a zoned `ReplicaServer` (so scans go through the real `serve_scan` path over the real wire), driven by `Coordinator::scan_in`.
- **Corpus (firehose/aged)**: n bsky-shaped keys with deterministic values (FNV-1a from key bytes, like `cocompact_expected`, so every record value-verifies without holding the corpus), written through the coordinator in **Fisher-Yates-shuffled order**, plus a deterministic ~1/4 overwrite subset at higher versions. The engine state at scan time must have **multiple overlapping runs** (that's what makes staged page selection matter: R runs × B blocks of sync QD1 reads under the borrow in the old path). Print whatever run/segment stats are reachable so the corpus shape is recorded. If background maintenance would collapse the runs before measurement, control for it (the bench may need to disable/limit auto-compaction for the measurement window — implementer's call, but the corpus must still be firehose-shaped, i.e. values scattered vs key order).
- **Phases** (each prints keys/s or ops/s + p50/p99 where relevant, error count, verified count):
  1. **FULL A/B** — paged full-keyspace scan via `scan_in`, value-verifying EVERY record, run with staged ON then OFF (`std::env::set_var("VALS_STAGED_SCAN", ...)` between phases — it's read per request server-side; same-process env toggling is the established pattern in server.rs tests). Assert the ON and OFF result streams are **identical** (kill-switch criterion).
  2. **WINDOW A/B** — many bounded window scans (distsorted RANGE-SCAN shape) staged ON/OFF; scans/s + keys/s.
  3. **GET-DURING-SCAN A/B** — one coordinator runs a continuous paged-scan loop while a second coordinator issues random gets; report get ops/s + p50/p99 with staged ON vs OFF. This is the direct attribution of the staged change ("serving core no longer serialized on sync QD1 block reads under the borrow; other requests interleave during page selection").
  4. **PREFETCH A/B** — full paged scan with prefetch ON vs OFF (fresh coordinator per arm or `set_scan_prefetch`), staged held ON; keys/s. Result-identity asserted as well.
- **Verdict line** `SCANAB: OK/CHECK`: 0 scan/get errors, 0 value mismatches, ON/OFF result identity holds. Ratios are printed informationally (no hard threshold — mem/file hide device costs; the box numbers are the deliverable).
- **Off-hardware smoke** (must be green before the box): `mem scanab` and `file scanab` at modest n; plus `cargo build --workspace && cargo test` staying green and clippy-clean under the pinned toolchain.

## Step 2 — box runs (coordinator, via ssh)

- Rsync HEAD to `oldbox:~/vals`, `VALS_SPDK_LINK=1 cargo build --release -p vals-bench --features spdk`.
- Verify no residue/processes (runbook prereqs), then run `spdk scanab` on one drive (`0000:05:00.0`), n=20M vsize=128 (cocompact-scale; ~2.5 GiB values), detached via setsid + log file.
- **perf attribution**: `perf record`/`perf stat` sampling the bench during the FULL scan phase in each arm (staged ON vs OFF) — expect the OFF profile to show sync run-block loads under the borrow on the serving core and the ON profile to shift that to async device reads; keep the reports.
- A smaller probe run (n=2M) first to sanity-check phase wiring on hardware.
- Leave the box clean per the runbook (no dpdk/hugepage residue, drives still vfio-bound).

## Step 3 — record + close

- Write `bench/scanab-box-notes.md` (cocompact-notes style): box/build info, corpus shape, per-phase numbers, perf attribution summary, headline finding, leave-clean check.
- Note expectations honestly: per the cocompact session, a full scan is CPU-bound at ~147k keys/s/core on this hardware, so staged ON/OFF **wall-time** may be near-1× while the attributable win shows up in get-latency-during-scan (and prefetch's win is the per-page RTT/merge overlap, bounded by the client-merge share of page time). The task's acceptance is "improvement attributable to each change, perf-profiled, 0 errors, value-verified" — attribution + correctness are the bar, and the notes must name which metric shows each change's effect.
- Update BACKLOG.md if it tracks these focus areas, work-log the task, commit.

## Risks / notes
- If background compaction on the zoned server collapses the firehose runs before the scan phases, the staged path has nothing to show; the bench must pin the corpus shape (this is the main wiring subtlety).
- The GET-DURING-SCAN phase needs the scan to actually occupy the serving core with page selection in the OFF arm — window scans over many runs are the pressure case; sizes may need tuning on the box.

### Starting points
- crates/vals-bench/src/main.rs: workload_distsorted (~3443) — wire/coordinator + zoned ReplicaServer shape, raw_shard_count paging, RANGE-SCAN phase; workload_cocompact (~5226+) — firehose corpus (shuffled writes, FNV values, overwrite subset), verdict-line style
- crates/vals-cluster/src/net/server.rs:2481 staged_scan_enabled() — VALS_STAGED_SCAN read per request; ~3374 tests toggle it via std::env::set_var in-process
- crates/vals-client/src/coord/mod.rs:1194,1247 — VALS_SCAN_PREFETCH read at Coordinator::new; set_scan_prefetch A/B hook
- crates/vals-cluster/src/zonestore.rs — MemZoneStore / FileZoneStore / SpdkZoneStore for the mem|file|spdk backends
- bench/cocompact-box-notes.md — notes format + box recipe (drive 0000:05:00.0, detached setsid run, LD_LIBRARY_PATH)
- plans/distnode-box-bench.md — box prereqs / leave-clean checklist
- crates/vals-cluster/src/maint.rs — background maintenance that could collapse runs; check how/whether the zoned server auto-compacts

## Work log
- 2026-07-06 plan: ## Scope  Hardware validation (oldbox, SPDK) of the two scan-pipeline follow-ons that have both landed: 1. **Staged scan page selection** (task 0030) — server-side, kill switch `VALS_STAGE
…[truncated]
- 2026-07-06 context hints: 7 recorded with plan
- 2026-07-06 context hints: crates/vals-bench/src/main.rs: workload_distsorted (~3443) — wire/coordinator + zoned ReplicaServer shape and RANGE-SCAN phase; workload_cocompact (~5226+) — firehose corpus (shuffled writes, FNV-
…[truncated]
- 2026-07-06 implementer report: Implemented **Step 1** of task 0049: a new `scanab` server-level scan A/B workload in `crates/vals-bench/src/main.rs` (533 insertions, additive-only diff; no other files touched).  ## What was built `
…[truncated]
- 2026-07-06 revision: Addressed both review gaps in `crates/vals-bench/src/main.rs` (diff contained to that file).  ## 1. Phase-2 error classification + tunable concurrency - Added an `ErrTally` helper + `coord_err_variant
…[truncated]
- 2026-07-06 review tier: single-opus — reviewers: Claude
- 2026-07-06 review (Claude): accept — Task 0049 is a hardware-validation task; the implementer delivered a new additive `scanab` A/B bench in crates/vals-bench/src/main.rs (FULL / WINDOW / GET-DURING-SCAN / PREFETCH phases), plus bench/sc
…[truncated]
- 2026-07-06 decision: accept — commit: bench: SPDK box validation of the scan-pipeline follow-ons — staged page selection 2.1× concurrent-scan throughput (perf: borrow-held QD1 spin-poll 27%→5% of serving core), client prefetch 1.12×
…[truncated]
- 2026-07-06 usage: 172,103 tok (in 528, out 171,575, cache_r 31,406,351, cache_w 706,342) · cost n/a (unpriced)
  implementer: 112,582 tok (in 296, out 112,286, cache_r 17,751,545, cache_w 367,089) · cost n/a (unpriced)
  coordinator: 54,615 tok (in 210, out 54,405, cache_r 13,357,556, cache_w 297,602) · cost n/a (unpriced)
  reviewer:Claude: 4,906 tok (in 22, out 4,884, cache_r 297,250, cache_w 41,651) · cost n/a (unpriced)

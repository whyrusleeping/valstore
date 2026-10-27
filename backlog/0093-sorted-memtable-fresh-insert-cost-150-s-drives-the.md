---
id: "0093"
title: Sorted memtable fresh-insert cost (~150 µs) drives the post-flush write floor — attribute and optimize (real cause behind 0087's "6× apply inflation")
status: done
priority: 3
created: "2026-07-08"
updated: "2026-07-08"
depends_on:
    - "0091"
spec_refs: []
---

## Description
Task 0091's box trace (bench/t0091/notes.md §3) identified the REAL driver of the write-burst apply_mean gyration that task 0087 mis-attributed to on-core maintenance contention: per-op `apply` is ~148 µs immediately after a flush drains the memtable and decays monotonically to ~25–30 µs as the memtable refills — i.e. a **fresh insert** into the front-coded sorted memtable index costs ~150 µs while an in-place overwrite of a resident key costs ~25 µs, and every flush resets the churn workload to the insert-heavy regime (coupon-collector curve over the key space). This is the binding constraint of the ~7k/s flat-2ms floor regime (16 clients ÷ ~150 µs). Relates to task 0066's finding that front-coded sorted insert is slow on hardware.

## Acceptance criteria
- Reproduce/attribute the ~150 µs fresh-insert cost off-hardware or on-box (e.g. microbenchmark insert-into-large-front-coded-index vs overwrite; confirm with an unsorted-zone comparator run — hash index should show a flat apply_mean if the theory holds).
- Identify and implement the optimization (e.g. cheaper insert path into the front-coded leaves, batched leaf splits, an unsorted staging buffer merged on flush, or index structure change) — design choice to be justified by the attribution.
- Box A/B: burst apply_mean immediately after a flush measurably reduced from ~148 µs, and the post-flush throughput trough (~5–7k/s) raised, without read/scan regression (sorted zones must keep ordered iteration).
- No regression in vals-store/vals-cluster suites.

## Plan

## Diagnosis (supersedes the task's "index insert" hypothesis — verified in code)

The ~150 µs post-flush apply cost is NOT the front-coded index insert (0066's splice fix already landed). It is the **LWW read-before-write**: `Replica::apply_at` → `current_version(key)` → `Engine::version(key)`, which for a memtable-absent, bloom-positive key does a **synchronous QD1 run-block device read on the serving core, under the store borrow** (replica.rs:2298 even flags it: "the cold-churn read-before-write the out-of-core design flags as the #1 perf risk"). After a flush drains the memtable, every churned key takes this path (~NVMe read ≈ 100 µs + base ≈ 25 µs ≈ the observed 148 µs), decaying as keys become memtable-resident again. The read cannot be skipped (the LWW compare and the Merkle leaf accumulator's XOR-out both need the exact old version), but it can be made **async + pipelined**, which docs/out-of-core-index.md obligation #5 already prescribes: re-validate immediately before the store with no await between, retrying on change.

## Fix: staged (async) apply for the live server's Apply path

Reuse the existing staged-read machinery (probe_version / version_from_probe / ReadGate / spawned serve tasks — all built for reads in C.5a-5):

1. vals-store: ensure a monotone run-set revalidation token. Reuse `manifest_generation()`; audit every run-set mutation bumps it (flush_commit/compact_commit/sync flush/compact do; **truncate does NOT** per engine.rs:4735 — make truncate bump it; kill-the-fix test).
2. replica.rs staged apply API (no awaits inside; plain &mut self):
   - `apply_stage(key, value, version, wall_ms) -> StagedApply`: skew-cap / note_version / hlc.observe as today; staged version lookup — memtable hit or all-blooms-negative resolves `old` with zero device I/O → run the existing decide+store tail synchronously → `Done(Applied)`; bloom-positive → `NeedsRunRead { probe: StagedVersion, gen: manifest_generation() }`.
   - `apply_commit_staged(key, value, version, run_old, gen, wall_ms) -> Applied | Retry`: `Retry` iff generation changed; else `old` = memtable version if now present (beats the stale probe) else `run_old`; then the same decide+store tail. Share ONE implementation of the decide+store tail with apply_at (behavior byte-identical for the sync path).
3. server.rs: dedicated live-dispatch arm for `OwnedRequest::Apply` (before the serve_request fallthrough):
   - Inline fast attempt: borrow_mut().apply_stage — `Done` replies inline exactly as today (apply_latency_us, Busy mapping preserved). No gate, no spawn for hot keys.
   - `NeedsRunRead`: spawn a task (reads_inflight accounting like Read). Bounded loop (~8): acquire read_gate; re-borrow + fresh apply_stage under the gate (pre-gate probe is discarded — probes must be built after admission, mirroring serve_read_into's invariant); drop borrow; `version_from_probe(probe, &dev).await` (device error → Response::Err — obligation #4, never "absent"); re-borrow + apply_commit_staged; Retry → loop. Retries exhausted → final fallback: synchronous `apply()` under one borrow (guaranteed progress). New counter `apply_run_reads`; apply_latency_us observed end-to-end (doc note: cold-key applies now include await time). Document that spawned applies may complete out of order per connection (LWW-safe).
   - Scope: single Apply only. BatchApply, the in-process serve_request arm, AE apply, hint replay, CAS, local put stay synchronous (CAS keeps obligation-#5 local atomicity; note in docs). Follow-on task for staged BatchApply if the box shows it matters.
4. Tests: staged == sync equivalence across memtable-resident / flushed / absent / tombstone / Won / Lost / skew-Rejected / gc-floor-Rejected (mirror async_staged_serve_matches_sync); race tests (gen change → Retry; key inserted between stage and commit → memtable version wins, exact leaf accumulator); truncate-bumps-generation; leaf-accumulator exactness across staged applies; full vals-store + vals-cluster + vals-server suites (churn_reclaim* especially).

## Verification (coordinator, after review)
- Box A/B on the t0091 rig: six tier=one bursts + the 1 Hz trace, HEAD vs fix. Expect: post-flush apply_mean spike (~148 µs) flattened, post-flush throughput trough (~5–7k/s) raised, `apply_run_reads` tracking the cold-churn rate; read/scan sanity via a read burst. Write bench/t0093/notes.md; box left clean.

## Risks
- Correctness-critical LWW + Merkle-accumulator path → high-powered review; the revalidation (memtable-first, then generation token) is the crux — any run-set change between probe and commit must force Retry, and no await may sit between the final validation and the store.

### Starting points
- crates/vals-cluster/src/replica.rs: apply_at (~2371), current_version (~2300, the doc-flagged cold-churn read), store() (~2316), probe_version (~1997) + version_from_probe (~2009), test async_staged_serve_matches_sync_flushed_and_mixed (~2887)
- crates/vals-cluster/src/net/server.rs: live dispatch arms ~1843-2035 (Read spawn + reads_inflight pattern), inline serve_request Apply arm (~3100), serve_digest (~2259: gate.acquire().await then brief borrow, 'no await between admission and the borrow' invariant), ReadGate (~98)
- crates/vals-store/src/engine.rs: manifest_generation() (~4794), truncate gen comment (~4735: gen NOT bumped by truncate today), StagedVersion/StagedGet/resolve_run_probe (~5136-5180)
- crates/vals-client/src/observe.rs: Counters macro (~171), Histograms (~421)
- bench/t0091/notes.md §3 — the post-flush 148µs→25µs decay curve this must flatten

## Work log
- 2026-07-08 plan: ## Diagnosis (supersedes the task's "index insert" hypothesis — verified in code)  The ~150 µs post-flush apply cost is NOT the front-coded index insert (0066's splice fix already landed). It is th
…[truncated]
- 2026-07-08 context hints: 5 recorded with plan
- 2026-07-08 context hints: crates/vals-cluster/src/replica.rs: apply_at (~2371), current_version (~2300), store() (~2316), probe_version (~1997), version_from_probe (~2009), test async_staged_serve_matches_sync_flushed_and_mixe
…[truncated]
- 2026-07-08 implementer report: Implemented task 0093: staged (async, pipelined) LWW apply for the live server's single-`Apply` path, moving the post-flush cold-churn read-before-write off the serving core.  ## Diagnosis confirmed i
…[truncated]
- 2026-07-08 review tier: high-powered — reviewers: Claude
- 2026-07-08 review (Claude): accept — Solid, correctness-critical implementation of task 0093. The staged async apply moves the post-flush cold-churn LWW read-before-write off the serving core while preserving byte-identical LWW/accumulat
…[truncated]
- 2026-07-08 decision: accept — commit: cluster: staged async LWW apply — the post-flush write floor was the cold-churn read-before-write, not index inserts (task 0093). apply_stage resolves hot keys inline (zero device I/O); a flushed bl
…[truncated]
- 2026-07-08 usage: 109,763 tok (in 360, out 109,403, cache_r 18,865,283, cache_w 496,892) · $22.9899
  implementer: 57,895 tok (in 216, out 57,679, cache_r 8,412,431, cache_w 130,699) · $6.4661
  coordinator: 39,317 tok (in 84, out 39,233, cache_r 9,528,585, cache_w 315,085) · $15.4296
  reviewer:Claude: 12,551 tok (in 60, out 12,491, cache_r 924,267, cache_w 51,108) · $1.0941

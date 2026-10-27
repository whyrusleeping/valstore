---
id: "0154"
title: AE bucket-digest serving materializes O(keys×runs) per page on the shard thread — third non-intake RAM growth mode; OOM-killed budgeted nodes in 0152 run 2
status: done
priority: 1
created: "2026-08-07"
updated: "2026-08-08"
depends_on: []
spec_refs: []
---

## Description
Found during 0152 run 2 (`bench/replay-lab/run-20260807T215938Z-rambudget40g-0153/`), which FAILED despite 0153: node 3 OOM-killed at 22:46:16 UTC (anon-rss 48.97 GiB + full 8 GiB swap vs a 40 GiB budget); node 1 ran the same burst (33→38 GiB in ~3 min) and survived only because the requesting peer died.

**Attribution is now proven** (this was the open question after run 1 — 0153's new gauges plus a live perf capture nailed it):
- During the kill bursts `merge_in_flight`=0, `ae_seed_staged_bytes`=0, memtable flat, all activity counters frozen — 0153's two bounded growth modes are innocent.
- One `vals-shard` thread pinned at 100% CPU (~36 min utime). `perf` (both fp and dwarf call graphs, saved as `node114-perf-burst*.txt` + `node114-burst.perf` in the bench dir) shows the time in `ConnDriver::dispatch_frame` closures / `LocalExecutor::drain_ready` → `vals_store::engine::merge_digest_sources`, `_int_malloc`/`_int_free`, `memmove`, sip-hashing of keys (a fake return address spells ASCII ".feed.li" — feed.like digest keys).
- Node 1's burst crested shortly after node 3 (its AE counterpart) was OOM-killed, then released ~6 GiB → the responder side of AE digest serving owns the bytes (causal ordering inferred from 30 s samples, not instrumented).

**Mechanism** (`crates/vals-cluster/src/net/server.rs` `serve_bucket_digests` → `replica.rs plan_bucket_digest_page`/`bucket_digest_page_from_plan` → `engine.rs digest_page_from_plan`/`merge_digest_sources`):
1. The AE requester (`antientropy.rs` ~line 305) walks the merkle tree and pages `bucket_digests` over the differing leaf-bucket set. When a peer is far behind (e.g. mid-replay with constant churn), essentially ALL buckets differ → `ids` covers the whole keyspace.
2. Per page, the responder fully materializes `Vec<(key, u128)>` digest vectors for the wanted buckets from the memtable AND EVERY run sidecar, merges them, then truncates the OUTPUT to `SCAN_REPLY_BUDGET` — inputs are unbounded (O(keys-in-want × runs)).
3. Each `after`-page REPEATS the full materialization → sustained alloc/free churn on the shard executor (measured RSS growth of several GiB/min); glibc arenas ratchet RSS; the shard thread is pinned (freezing that shard's other work).
4. It is a read-side path, deliberately admitted while over the RAM budget (reads must keep serving), so no 0153 bound applies and no gauge attributes it.

Fix directions (implementer to judge):
- Bound the per-page input materialization: stream/iterate run sidecar digests with early cutoff once the page budget (+ merge lookahead) is met, instead of collecting whole buckets per run; and/or cap the wanted-bucket count handled per request, forcing the requester to sub-page.
- Consider deferring digest SERVING (not reads generally) while over the RAM budget, matching 0153's treatment of local AE-heal intake — with care not to deadlock mutual healing.
- Add an attribution gauge (e.g. `ae_digest_serve_bytes`) so this working set is visible.
- Keep requester paging semantics compatible.

Acceptance criteria:
- Digest page serving allocates O(page budget) rather than O(keys×runs) per page (unit test with many runs × many keys asserting bounded materialization, in the style of 0153's resident measurements).
- Shard thread no longer monopolized: digest paging yields between bounded chunks.
- Existing AE convergence tests pass.
- 0152 run-2 artifacts referenced; 0152 (still open) is the hardware re-validation criterion once this lands.

## Acceptance criteria

## Plan

Goal: make AE bucket-digest page serving allocate O(page budget) instead of O(keys×runs) per page, make it yield on the shard executor, and add an attribution gauge. No wire/requester changes.

Mechanism recap (confirmed in code):
- Async hot path: `server.rs serve_bucket_digests` → `replica.rs plan_bucket_digest_page` / `bucket_digest_page_from_plan` → `engine.rs plan_version_digest_page` / `version_digest_page_from_plan`. Per page it (a) collects the whole filtered memtable, (b) reads EVERY run's ENTIRE sidecar into RAM (`RangeLocator::read`) and materializes a full `Vec<(Vec<u8>,u128)>` per run, then (c) merges and truncates only the OUTPUT to SCAN_REPLY_BUDGET (8 MiB). Each `after` page repeats it all.
- Key enabler: v3+ sidecars (run format ≥3; current v4) store records in run-entry order = sorted by key, so they can be stream-merged with early cutoff. v1/v2 sidecars are bucket-grouped and not globally sorted.

Changes:
1. Streaming sidecar cursor (vals-store run.rs): a per-run cursor that reads the sidecar in bounded chunks (~256KiB–1MiB windows, reusing the RangeLocator seg_lbas/block math for ranged reads), incrementally parses v3 records (u32 klen, key, 16-byte version) with a carry buffer for records spanning chunk boundaries, skips keys ≤ `after` and non-`want` buckets, and yields matching entries in key order. Device reads stop as soon as the merge no longer needs the cursor (lazy pull).
   - CRC: the sidecar only has a whole-body CRC; keep a rolling CRC and verify it only when a cursor consumes the sidecar to the end (early-cutoff pages can't verify — document that trade-off in a doc comment). Structural parse errors still surface as Corrupt.
   - v1/v2 runs: fall back to the existing full `parse_want_bucket_digests` materialization for that run only (legacy compatibility).
2. Rework `version_digest_page_from_plan` into a lazy k-way merge over (bounded memtable source + streaming run cursors): pull min-key, resolve LWW max version across sources holding that key, stop at max_bytes/max_entries. `vals_rt::yield_now().await` between chunk-read windows so the shard thread is not monopolized. Always admit at least one entry so a truncated page is never empty (requester at antientropy.rs ~314 errors on empty+more).
3. Bound the memtable source in `plan_version_digest_page` (must stay sync under the store borrow): filter, sort, and truncate to one page's worth (max_bytes of key+overhead), recording the truncation cutoff key K; the merge must then cap output at keys ≤ K and set more=true. (Run cursors need no cap — they are lazy.)
4. Sync path `Engine::version_digest_page` (SyncBucketDigests, sync BlockDevice): apply the same bounding by sharing the record parser with sync ranged reads if it factors cleanly; at minimum bound its per-run materialization the same way. Not the hot box path but don't leave it O(keys×runs) if cheap.
5. Attribution gauge: add `ae_digest_serve_bytes` Gauge to vals-client observe.rs (with Prometheus HELP text per project convention) tracking bytes currently held by in-flight digest-page serving (chunk buffers + collected output), updated via an RAII guard in serve_bucket_digests / the from_plan path.
6. Explicitly NOT doing: deferring digest serving while over RAM budget — after this fix serving is O(page), and deferral risks mutual-heal deadlock.

Tests:
- Engine unit test, many runs × many keys (all buckets wanted, worst case): assert per-page device bytes read + materialized bytes are bounded (O(page + chunk×runs)), far below total sidecar bytes — e.g. via a byte-counting device wrapper, in the spirit of the existing `pressure_merge_resident_measurement` test.
- Paging-equivalence test: paging to exhaustion via the new path over a multi-run corpus with overlapping keys (LWW winners in different sources, keys straddling chunk boundaries, `after` exactly equal to a key) yields exactly the same key→version set as the old full-materialization merge.
- Existing AE convergence tests pass: `cargo build --workspace && cargo test -p vals-store -p vals-cluster`.

Constraints: no cargo fmt (tree not fmt-clean); no explanatory comments (SAFETY/doctests/Prometheus HELP only); requester paging semantics and wire format unchanged.

0152 (box re-validation) remains the hardware acceptance gate once this lands.

### Starting points
- crates/vals-store/src/engine.rs:3770-3910 — plan_version_digest_page / version_digest_page_from_plan / merge_digest_sources (async hot path)
- crates/vals-store/src/engine.rs:1275-1330 — sync version_digest_page (same pattern)
- crates/vals-store/src/run.rs:1309-1394 — RangeLocator::read + parse_want_bucket_digests (v3 records are key-sorted; v1/v2 bucket-grouped)
- crates/vals-store/src/run.rs:510-547 — sidecar encoding (records in sorted entry order, whole-body CRC trailer)
- crates/vals-cluster/src/net/server.rs:2100-2129 — serve_bucket_digests (gauge guard goes here)
- crates/vals-cluster/src/replica.rs:1390-1434 — plan/from_plan wrappers, DIGEST_WIRE_OVERHEAD=16
- crates/vals-client/src/observe.rs:330-430 — existing attribution gauges (merge_in_flight_input_bytes, ae_seed_staged_bytes) to mirror for ae_digest_serve_bytes
- vals_rt::yield_now — crates/vals-rt/src/lib.rs:440
- engine.rs:8559 pressure_merge_resident_measurement — style template for the bounded-materialization test
- requester: crates/vals-cluster/src/antientropy.rs:305-320 (errors on empty truncated page — never return empty+more)

## Work log
- 2026-08-07 plan: Goal: make AE bucket-digest page serving allocate O(page budget) instead of O(keys×runs) per page, make it yield on the shard executor, and add an attribution gauge. No wire/requester changes.  Mecha
…[truncated]
- 2026-08-07 context hints: 10 recorded with plan
- 2026-08-07 context hints: crates/vals-store/src/engine.rs:3770-3910 — plan_version_digest_page / version_digest_page_from_plan / merge_digest_sources; crates/vals-store/src/engine.rs:1275-1330 — sync version_digest_page; c
…[truncated]
- 2026-08-07 preload: 6 file(s), ~17 KiB seeded into implementer context
- 2026-08-07 implementer report: Implemented Task 0154.  Changes: - Added a lazy v3+ run-sidecar cursor with 256 KiB ranged reads, record carry-over across windows, exclusive `after` filtering, bucket filtering, rolling CRC verificat
…[truncated]
- 2026-08-08 review tier: single-opus — reviewers: sol
- 2026-08-08 review (sol): accept — The change correctly replaces the production AE digest-serving hot path with bounded, lazy v3+ sidecar cursors, performs a lazy LWW k-way merge with exclusive paging semantics, yields after bounded re
…[truncated]
- 2026-08-08 decision: accept — commit: 0154: bound AE bucket-digest serving to O(page) — streaming v3+ sidecar cursors (256 KiB ranged windows, carry-over parsing, rolling CRC verified at EOF) feed a lazy k-way LWW merge in version_diges
…[truncated]
- 2026-08-08 usage: 5,795,383 tok (in 1,077,053, out 40,186, cache_r 5,730,644, cache_w 374,836) · cost n/a (unpriced)
  implementer: 3,855,051 tok (in 498,307, out 21,320, cache_r 3,335,424, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 1,929,461 tok (in 578,706, out 8,035, cache_r 1,342,720, cache_w 0) · cost n/a (unpriced)
  coordinator: 10,871 tok (in 40, out 10,831, cache_r 1,052,500, cache_w 374,836) · cost n/a (unpriced)

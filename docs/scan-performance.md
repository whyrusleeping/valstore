# Ordered-scan performance (2026-07 pass)

Sorted zones exist for **ordered iteration over a keyspace**, but until this
pass the scan path ran ~50× slower than the point-read path: the 2026-06-26
corpus capture measured **~76 k rec/s cluster-wide** (all serving cores pegged)
against ~4 M/s point reads. This document records the diagnosis, what changed,
the measured results, and the remaining roadmap to make *every* ordered scan
run at device-sequential rates.

## Reverse scans (0484; foundations 0487–0491)

`Engine::scan_winners_page_rev(lo, hi, max_entries, max_bytes)` returns descending
LWW index winners over the same half-open `[lo, hi)` interval as the ascending
method. Point tombstones remain in the winner stream; range-covered winners are
filtered out. The counted twin, `scan_winners_page_rev_counted`, also returns
`RtScanStats`. Both require sorted ordering. Empty or inverted windows are empty.

The result is `(entries, more)`. Resume a truncated page with **`hi = last_key`**,
without appending a zero byte. Budgets match ascending winner pages: byte cost is
key length plus the value-record pointer's length, and the first entry is allowed
to exceed the byte budget to make progress. A zero entry budget returns no entries
(and `more` if a visible winner exists), so a caller must use a positive entry
budget to paginate.

Reverse memtable pages walk the block map backwards, decoding one front-coded
block at a time; the engine refills at most 4096 entries at a time. Each reverse
run cursor binary-searches the last fence strictly below `hi`, decodes that block,
and pops entries backwards before stepping to the preceding block. Its
`next_picking` selector takes/returns an **exclusive block end** (`0` means done),
not the ascending cursor's next-block index. Covered-block skipping uses the
existing max-HLC tree in reverse, conservatively retaining boundary blocks and
blocks whose maximum HLC equals the covering HLC (writer-ID ties still matter).
No new resident index or write-time bookkeeping is added.

### Staged reverse pages (0488)

`Engine::stage_scan_page_rev(lo, hi, max_entries, max_bytes, covered_budget)`
selects the same winners without borrowing the engine across device reads.
It returns `StagedScanRevPage::Done(StagedScanDone)` for uncovered memtable-only
or empty windows, otherwise `Runs(StagedScanRevMerge)`. Range-tombstone filtering
is always enabled; use `SCAN_COVERED_BUDGET` for the ordinary covered-work cap.
The staged merger owns at most 4096 memtable entries and one decoded block plus
64 locators per overlapping run, alongside its budgeted output. It seeds each
run from the last fence below `hi`, primes first reads concurrently, and reads
subsequent blocks asynchronously in descending order. Locator windows are not
read eagerly. Fully covered blocks use the same conservative reverse skip as
the synchronous path.

Drive `merge.step(dev).await` **off the engine borrow**. On `NeedMem { from }`,
reborrow the engine and call `scan_supply_mem_rev(&mut merge, &from)` (`from` is
an exclusive upper bound). On `NeedBlocks { cursor }`, call
`scan_supply_blocks_rev(&mut merge, cursor)`. After `Done`, `merge.finish()`
returns the common `StagedScanDone`; `plan_staged_scan` can materialize its values
using the existing LBA-coalesced span reader, preserving descending slot order.
`meter_server_scan()` opts into the existing server cursor/block/byte counters;
repeated calls do not double-count cursors.

Like the ascending staged primitive, this is **not a snapshot or a self-guarding
cursor**. Callers must compare the captured `generation()` with the engine's
storage generation after each await and before every refill and value plan,
and discard/restart stale attempts (including stale-attempt read errors).
`mutable_generation()` and `aggregate_generation()` expose the captured mutable
state for the serving layer's existing finalize/revalidation protocol. A
concurrent RT change must likewise be accounted for before publishing a page.
The engine primitive alone does not provide quorum or concurrent-scan guarantees.

A covered-work cap may end a page with **no visible entries**. In that case
`done.resume()` is the last filtered key and `more` is true: the page covers
`[resume, original_hi)`, and the next request uses **`hi = resume`**. Never append
a zero byte. If `resume()` is absent and `more` is true, resume from the last
winner instead. Save `resume()` before consuming `done` via `into_winners()` or
`plan_staged_scan`, which do not carry it. As with synchronous pages, use a
positive entry budget to paginate. Covered block budgets count blocks contributing
a filtered winner, not all reads: priming, visible winners and one-entry lookahead
can add reads, and a single merged winner can touch multiple runs. RT statistics
report actual block reads separately from skipped blocks and filtered entries.

### Internal reverse serving (0489)

`net::server::scan::reverse::serve_scan_rev` serves one descending `[lo, hi)` page,
returning the existing internal `Response::Scan` plus RT counters. It accepts the
same entry cap (`0` means unlimited) and range-tombstone capability as ascending
serving, plus keys-only mode. Since 0491 it is reachable through negotiated
reverse MultiScan RPCs, including scalar reverse calls.

The server drives staged reverse selection off-borrow, checking storage generation
before accepting read results (including errors), after gate checkpoints and before
refills/value planning. After three storage restarts it falls back to synchronous
reverse selection (`Engine::scan_plan_rev` / `VersionedStore::plan_scan_rev`), with
asynchronous coalesced value reads. Three failed value-read/finalization attempts
return Busy. Reverse entries are temporarily reversed for the existing ascending
mutable-winner finalizer, then restored to descending order. Ordinary concurrent
writes patch selected winners; RT application changes storage generation and forces
a restart. Keys-only pages materialize current mutable versions without value I/O.
These are the same non-snapshot semantics as ascending serving, not a new isolation
level.

RT fragments remain in **ascending fragment order**, clipped to the page's covered
suffix: `[resume, hi)` if an explicit resume exists, otherwise `[last_key, hi)` for
a truncated nonempty page, or the whole `[lo, hi)` for a completed page. Reply-budget
fitting visits fragments by descending **exclusive upper endpoint** alongside the
descending entries. If a fragment cannot fit, coverage stops at its upper endpoint.
If an entry `k` cannot fit, coverage stops at `k || 0x00`, so `k` is excluded from
this page but included by the next request. This is an internal cut calculation:
the caller always resumes with **`hi = resume` unchanged**, or `hi = last_key` when
no explicit resume exists. Already-selected fragments are clipped to the new lower
bound; fragments ending at that bound are discarded rather than emitted empty.
The first item, and items needed to move strictly below the request's upper
bound, may exceed the soft byte budget to guarantee progress. A peer unable to
consume fragments/resume keys gets an error rather than incomplete coverage.

### Reverse coordinator (0490)

`Coordinator::scan_in_rev` and `scan_many_in_rev` now implement descending slice
traversal and independent per-query quorum pagination, via new reverse-only
Transport hooks. The max-coverage lower-bound rule, exclusive cursor and API
contract are in [algorithms.md §6](algorithms.md#descending-scans-04900491).
An in-process transport connects the coordinator to this reverse server for tests
of mixed runs/memtable, overwrites, point/range tombstones, keys-only mode, limits,
small reply/covered-work budgets, majority reads with one replica down, and
committed-token authority during pending re-homes. A dense/sparse replica fixture
fails if the lower coverage bound is changed from maximum to minimum.

0491 adds negotiated reverse MultiScan (hello bits 4+5, request flag bit 1),
NetTransport hooks, and TCP tests for split/shard routing, limits, keys-only mode,
RT quorum pages, small budgets, re-home fences and old-peer rejection. Scalar
reverse uses a one-query MultiScan; legacy ascending opcode 11 is unchanged.
The batched transport/serving loops specialize by direction. The
[0484 aged-graph comparison](../bench/0484-reverse/FINDINGS.md) verifies natural-time
latest-N with exact values on file and SPDK backends. Versus inverted-time
ascending scans, reverse latest-N costs 1.89× local / 1.67× SPDK median latency
on this many-overlapping-run shape. Matched pre-reverse/candidate arms show no
material ascending/write regression within the measured variability; these are
bounded closed-loop workloads, not saturation tests. Reverse scalar calls use the
batched coordinator loop, without ascending scalar's range concurrency, page
prefetch or first-quorum cancellation; do not infer performance parity from the
correctness tests.

## Where the time went (measured, oldbox + local profiles)

A scan page (`Coordinator::scan_in` → `Scan` RPC → `serve_scan` →
`Engine::scan_winners_page` + value materialization) paid, per page:

1. **O(remaining-range) memtable re-materialization, per page.** With on-disk
   runs present, `scan_winners_page` materialized the memtable's *entire*
   `[cursor, hi)` window on every page (`Index::scan`), making a paged scan of
   a dense range quadratic over the whole scan. Dominant CPU in the local
   reproduction (`bench scanlsm`).
2. **One random ~4 KiB device read per record.** Values live in the value log
   in *arrival* order; the scan's budgeted page issued one `ReadPlan` per
   record (QD-windowed `join_all`), each with its own scratch buffer, NVMe
   submit/poll, CRC, and decode. The serving core spent ~40 % of its cycles in
   SPDK submit/completion plumbing at an effective device QD of ~6.
3. **Serial ranges.** `scan_in` walked range-map slices one at a time — on a
   4-node RF=1 cluster, 3 of 4 serving cores idled during a full-zone scan.
4. **Coordinator merge overhead.** Every page was folded through a
   `BTreeMap` LWW merge even with a single responder (R=1).

## What changed

All engine/server/coordinator changes are format- and wire-compatible (no
on-disk or protocol change).

- **Lazy memtable paging** (`Engine::scan_winners_page`): the memtable is now
  consumed through a budgeted, refillable `Index::scan_page` cursor
  (4096-entry chunks, refilled past the last consumed key) — O(page) per page,
  never O(range). Pinned by
  `paged_scan_refills_the_memtable_cursor_past_the_chunk_budget`.
- **LBA-coalesced span reads** (`ValueLog::plan_read_spans` →
  `SpanReadPlan`): a scan page's value records are sorted by device LBA and
  merged into contiguous **spans** (gap ≤ 4 blocks, span ≤ 128 KiB — one NVMe
  command, safely under MDTS and equal to the shim's own sync chunk). Records
  sharing or adjoining blocks are read **once**; on key-clustered values a
  whole page collapses into a few large sequential reads. Both the sync scan
  (`scan_bounded*`) and the borrow-free server path (`plan_scan` →
  `execute_span_page`, spans windowed at QD 64) use it. The per-record
  `ReadPlan` path remains for point reads and `plan_values`.
- **Parallel range scans** (`Coordinator::scan_in`): an unlimited scan drives
  up to 8 range-map slices concurrently (disjoint ranges → different replica
  sets), concatenating per-slice results in slice order — a full-zone scan now
  spreads across the cluster's serving cores. `limit`ed scans stay serial
  (they must stop at the limit). Single-responder (R=1) pages skip the
  `BTreeMap` merge entirely.
- **Boot fix (side finding, big):** `Engine::recovered_versions` — the boot /
  maintenance fold every zone open runs — called `DeviceRun::bucket_digests`
  **per bucket**, and each call read + CRC'd the **entire sidecar**. At the
  production 4096 buckets a restarted node re-read every run's sidecar 4096×:
  box-observed nodes pinned at 100 % for **10+ hours** without serving (this
  is why the corpus cluster's nodes had "never been restarted"). Replaced by
  one streaming sidecar pass per run (`DeviceRun::for_each_bucket_digest`; v5
  runs, task 0365, stream their data blocks instead) and
  a presized fold map (rehashing was ~60 % of the remaining CPU). A full node
  boot on the 2.1 B-record corpus now completes in ~15 min (still O(dataset) —
  task 0005 remains the real fix).

## Measured results

`bench scanlsm` (single core, 2 M bsky-shaped keys, 100 B values, 4 runs +
memtable, paged full-keyspace scan, page 8192 / 8 MiB budget):

| configuration | before | after |
|---|---|---|
| mem backend, shuffled (arrival-order) values | 510 k keys/s | **1.42 M keys/s** |
| file backend, shuffled values | — | **0.92 M keys/s** |
| file backend, **key-ordered** values | — | **5.9 M keys/s** |

The sorted-vs-shuffled gap is the span coalescer at work: key-clustered values
scan at ~6 M keys/s **per core** — the rate task 0028 (key-ordered vlog
co-compaction) unlocks for aged data.

Box (oldbox, live 2.1 B-record firehose corpus, 4 × 1-drive SPDK
nodes, RF=1, R=1, arrival-order values — i.e. worst-case span coalescing;
post-restart AE sweep churning in the background throughout):

| workload | before | after |
|---|---|---|
| `valsdump export` (8 threads, 34 ranges, records zone) | ~76 k rec/s, 9/103 ranges lost | **~441 k rec/s sustained** (86 M records / 49.6 GiB in 195 s), 0 ranges lost |
| single-range window count (851 k keys) | 8.6 s (98 k/s) | 5.8 s (146 k/s) under light sweep load |

Counts were byte-identical before/after (851,429 / 1,314,336 on the two pinned
windows) — the fast path returns exactly the same data.

### Co-compaction on aged data (task 0042, box: oldbox, 1 SPDK drive)

`bench cocompact` ages an arrival-order (shuffled-write) sorted zone, runs the
production off-core co-compaction (`compact_plan` → worker `compact_and_write` over
a sibling qpair → `compact_commit`), and value-verifies a full scan before/after.
At **20 M keys / 128 B**, the co-compaction cut the full-scan **device reads
19,995,344 → 312,500 = 64× fewer** (0 mismatches, scans driven through the server's
QD-64 staged page path), reclaimed space durably (`used_segments` 9,327 → 7,572,
persisting across a store reopen), and kept serving reads throughout the 166 s
off-core merge (5.7k → 5.0k get/s; p99 257 µs → 737 µs).

The **wall-time** scan rate rose only ~1.6× (91.5k → 147k keys/s) — NOT ≥5×. A
key-ordered-write control (same corpus written key-clustered into one run, no
compaction) scanned at **147k keys/s / 312,500 device reads** — i.e. the
post-co-compaction scan is at the key-ordered ceiling (ratio ~1.00×, identical device
reads and on-device layout). So co-compaction achieved everything key-clustering can
on this hardware; the residual gap is the **per-core scan CPU ceiling** (~150k
keys/s/core at vsize=128: per-record decode + LWW merge + span planning), not
ordering. A single-core full scan is CPU-bound — the drive already absorbs scattered
reads at depth, so cutting device reads 64× barely moves the wall clock; the ~6×
file/page-cache figure above is the CPU-bound rate with *zero* device-read cost and
does not hold as a wall-time ratio on NVMe. Raising the wall-clock rate is the domain
of the scan-pipeline follow-ons (task 0049) and sharded sorted-zone serving (task
0050, more cores across a range). Full numbers + serving-impact detail:
`bench/cocompact-box-notes.md`. *(Post-script, task 0049: the scan-pipeline
follow-ons were box-validated — they buy concurrent-scan throughput (2.09×) and
serving-core availability, and prefetch buys 1.12× at R=2, but a single scan's
per-core ceiling stands; per-range sharding (0050) remains the wall-clock lever.
See `bench/scanab-box-notes.md`.)*

## Remaining roadmap (backlog)

- **0028 (LANDED)** — key-ordered vlog co-compaction: the off-core run-merge can
  co-compact the value log **in key order**, so *aged* (arrival-order) data becomes
  key-clustered and the span coalescer gives every ordered scan the sequential
  (~6 M keys/s/core) rate, not just bulk-loaded zones. Same rewrite reclaims dead
  value bytes (task 0004's vlog space reclaim, with one sort constraint). Recovery
  liveness is manifest-driven (`RunMeta.value_segs`); output segments carry a
  not-tail header marker so they never replay into the memtable; original record
  `seq` is preserved to avoid tail-replay resurrection. In-repo A/B: a 600-key
  shuffled corpus dropped from 184 to 10 device spans over 64-key scan pages (~18×
  fewer reads). Box/SPDK validation on an aged sorted zone (task **0042**): **64×
  fewer scan device reads** at 20 M keys, durable space reclaim, serving continuity —
  see the co-compaction section above and `bench/cocompact-box-notes.md`.
  - **Superseded as the *recurring* default by task 0069** (below): co-compaction's
    measured wall-time benefit is only ~1.61× (device-read reduction ≠ scan speedup on
    the CPU-bound decode path), so paying it O(N) times per value on every merge was
    the top structural perf defect. Clustering is now **opt-in, at most once per
    value** (a bottommost graduation merge with `VALS_VLOG_CO_COMPACT=1`); steady-state
    merges are index-only.
- **0069 (LANDED)** — decoupled run-merge (the three-jobs split): a run-merge used to
  bundle (1) index folding, (2) value clustering, and (3) space reclaim into one
  monolith fired every `RUN_COMPACT_THRESHOLD` flushes, re-copying every live value
  each cycle → O(N²) merge traffic on a growth workload. Now:
  - **Recurring merges are index-only** (`VlogRewrite::None`): entries keep their
    `ValuePtr`s, no value bytes move. Value clustering is **opt-in at most once per
    value** — `VALS_VLOG_CO_COMPACT=1` clusters on *bottommost* (graduation) merges
    only; default (unset) never clusters.
  - **Tiered/leveled selection**: a recurring merge folds a contiguous **newest-suffix
    subset** of ~`VALS_MERGE_TIER_K` (default 6) similar-sized runs (input capped at
    `VALS_MERGE_INPUT_CAP_BYTES`, default 2 GiB); the base run joins only once the
    newer tiers reach ≈ its own size (graduation → a bottommost merge). Per-key index
    rewrites drop from O(N/flush) to O(log_k N). Runs that can no longer fit a capped
    merge are frozen (0381), so scans k-way over ~data ÷ cap runs.
  - **Decoupled vlog reclaim** (`VlogRewrite::Victims`): the region-pressure merge
    rewrites only the entries pointing into chosen **mostly-dead** value segments
    (dead fraction ≥ `VALS_VICTIM_DEAD_FRACTION`, default 0.5), repointing them and
    freeing the victims — copying far less than a full co-compaction while still
    self-healing a nearly-full region (falls back to cluster-all when victims can't be
    sized, e.g. a legacy manifest). Per-segment liveness is tracked in
    `RunMeta.value_seg_bytes` (manifest `v4`).
  - Merge stays read-transparent: no key's current version changes and Merkle leaves
    remain untouched.
- **0099 (DESIGN GATE CLOSED)** — write-path clustering by intra-batch sorting,
  bucketed vlog streams, or trivial non-overlapping-run install is not implemented:
  the completed 0132 replay showed bounded merges and no placement-driven ingest or
  space pressure, while it did not measure scans. Re-open criteria and the required
  placement/scan A/B are recorded in [Write-path clustering](write-path-clustering.md).
- **0029 (LANDED)** — sorted-zone data and scans shard by
  `hash64(range_token) % shards`; each range co-locates on one shard while
  different ranges spread across a node's partitions/cores.
- **0048 (LANDED)** — client-side scan page **prefetch**: `Coordinator::scan_slice`
  now overlaps the client-side LWW merge/drain of page N with the server's build of
  page N+1. Once a round's resume bound is final it primes the next page's replica
  fan-out (`vals_rt::prime` polls the fan-out future once so every replica's request
  frame is queued *now*) then eagerly flushes the transport
  (`Transport::flush_requests` → `flush_dirty`), so the request hits the wire *before*
  the CPU-bound merge instead of at the next pre-park hook. Depth-1 pipeline, no wire
  change (wire-compatible, no new opcode). Applies to **unlimited** scans only — a
  `limit`ed scan's per-page cap depends on results so far, so it stays strictly
  serial. Stale-epoch refusal / quorum failure on a prefetched page surface exactly
  as an inline round would (retry_stale restarts the whole scan; the prefetched page
  is dropped, its RPC abandoned like a quorum-cancelled responder). Prefetch defaults
  on; applications can use `Coordinator::set_scan_prefetch(false)` when needed.
  The redundant `VALS_SCAN_PREFETCH` environment switch is retired. Equivalence is
  test-pinned (results byte-identical off vs on, including the stale-retry and
  quorum-failure paths); an in-repo loopback measurement
  (`scan_page_prefetch_overlaps_the_client_merge_over_tcp`, RF=3/R=2, ~30 MiB paged
  several times past the 8 MiB reply budget) shows ~0.86× wall time (off≈198 ms,
  on≈170 ms locally). Box validation (task **0049**, `scanab` bench): **1.12×
  full-scan keys/s at RF=3/R=2** on SPDK (144,996 vs 128,903 at 10 M keys — the
  un-overlapped two-page LWW merge is pure serial time without prefetch), neutral
  (~0.98×) at RF=1/R=1 where the client merge is trivial; results byte-identical
  on vs off in both topologies. See `bench/scanab-box-notes.md`. A `ScanStream`
  push op (server pushes successive budgeted pages behind a PROTO feature bit —
  option (a), which would also remove the per-page request decode) remains a
  possible future step.
- **0030 (LANDED, box-validated)** — **staged (async, borrow-free) scan page
  selection**: page selection's run-block loads previously were sync QD1 ~56 µs
  device reads *under the engine borrow* on the serving core; the staged path
  (stage → async merge via windowed `RangeLocator`s → generation-guarded restart,
  sync progress fallback at the restart cap) reads them off-borrow. Staged selection
  is unconditional; the former `VALS_STAGED_SCAN` switch is retired. Box
  validation (task **0049**, `scanab` bench, firehose 5-run corpus): **2.09×
  concurrent window-scan throughput** (449,904 vs 215,571 keys/s, 20 M keys,
  8 in-flight, 0 errors, value-verified) and **1.85× point-get throughput during
  a full scan** (up to 4.4× lower get p99 at 2 M); perf shows the serving core's
  ~25–27% of cycles spent spin-polling NVMe completions under the borrow drop to
  ~1–5% staged. Kill-switch identity is fingerprint-pinned on-box. A single
  full-zone scan stays at the ~150k keys/s/core CPU ceiling (0.97×–1.00× wall) —
  the win is concurrency, not one scan's wall clock. Server-push `ScanStream`
  remains open (see 0048 above).
- **0005/0045 (LANDED)** — run-backed Merkle leaves seed lazily and
  cooperatively when anti-entropy first needs them, with bounded steps instead
  of blocking startup on a one-shot leaf rebuild.
- **0015 (LANDED)** — anti-entropy caches leaf groups by replica set and compares
  only groups shared by both peers under RF<N, so legitimate ownership
  differences no longer keep converged nodes grinding.

## Reproduction

```sh
# single-core LSM scan (mem|file), page 8192, 4 runs, shuffled|sorted values:
cargo run --release -p vals-bench -- file scanlsm 2000000 100 /tmp/scan.img 8192 4 shuffled
cargo run --release -p vals-bench -- file scanlsm 2000000 100 /tmp/scan.img 8192 4 sorted

# box: valsq count on pinned windows / valsdump export with a splits file
# (the original scan path served this at ~76 k rec/s before this pass).
```

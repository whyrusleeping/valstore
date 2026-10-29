# Write-path clustering: measurement gate

This note closes the exploratory write-path-clustering gate from task 0099. It
considers three ways to make values arrive approximately in key order instead of
paying for a later key-ordered value-log rewrite:

1. stable key sorting within `BatchApply`,
2. multiple key-range-bucketed value-log append streams, and
3. trivial installation of sorted, non-overlapping runs.

**Decision (2026-08-11): implement none of them now.** The successor to the lost
0097 benchmark, the completed 0132 replay, does not show an ingest bottleneck
that value placement would fix. It also did not measure scans, so it cannot
establish a scan-placement gap. The mechanisms remain candidates with the
measurement gates below; this is not a conclusion that clustering never helps.

## Evidence used for the gate

Task 0097's original machine and corpus were lost, so its exact A/B cannot be
run. Task 0132 is the applicable post-0069 successor: it replayed 155,158,024
records (82 GiB raw) in batches of 1000 into a fresh three-node SPDK cluster.

The device-persistent `--tier one` arm completed in 6,170.3 s at 25,146 logical
records/s, with zero write errors and zero Busy, RAM-Busy, region-exhausted, or
compaction-plan failures. Its early physical-apply peak was 223k/s. Resident
runs remained bounded while the nodes performed 96/35/105 run merges. Even the
hottest observed zone retained at least 23,708 of its 81,920 segments. The
completed arm therefore shows neither a post-0069 merge backlog nor
placement-driven space pressure.

The throughput curve does decline, but its measured causes are elsewhere:

- cold last-writer-wins resolution grows with the on-disk index: run reads per
  apply rise from about 0.15 to about 0.6;
- after most replay threads finish, the final roughly 40 minutes are driven by
  only the two or three largest input files, a client-side fan-out tail; and
- recurring index merges keep pace rather than accumulating an ever-growing
  merge monolith.

Sorting or bucketing **value-log records** does not remove the run-index block
reads needed to determine the previous LWW winner. A direct bulk-ingest API
could avoid those reads only under a stronger non-overlap proof; that is a
separate property from physical value placement.

The `--tier nosync` arm reinforces the bottleneck split. It completed in
5,815.1 s at 26,682 logical records/s, only 1.061x faster end to end. With batch
size 1000, removing the durability wait saves little because one shard sync is
already amortized over a large batch. Its placement, live-key counts, free-space
minima, and regime changes were near-identical to the persistent arm.

Both arms ran on 2026-08-10 (task 0132's `--tier one` and `--tier nosync`
replays); the numbers above are taken from their findings.

### What the replay did not measure

Task 0132 did not run an ordered-scan A/B, count value-read spans, or report scan
CPU and device-read utilization. It therefore says nothing direct about how
well replay arrival order clustered values, and it cannot prove that a
write-placement change would or would not improve scans.

The separate aged-data co-compaction benchmark bounds that opportunity on the
measured hardware. At 20 million keys with 128-byte values, key-ordering the
values cut scan device reads from 19,995,344 to 312,500 (64x), but wall-clock
scan rate rose only from 91.5k to 147k keys/s (about 1.61x). A key-ordered-load
control reached the same 147k keys/s and device-read count, showing that the
remaining limit was per-core decode/merge/planning CPU rather than value
placement. Clustering can still be valuable for device IOPS and mixed-workload
serving budget, but the observed wall-time return is too small to justify a
large write/recovery redesign without a workload-specific A/B. See
[Ordered-scan performance](scan-performance.md), whose co-compaction section
carries the box measurements (task 0042).

## Mechanism 1: stable key sorting within `BatchApply`

### Proposed shape

At each replica, create a stable permutation of the replica-local sub-batch by
key and process writes in that order. Keep the wire request and
`Response::Batch` aligned with the original input indexes: results must be
scattered back through the permutation because duplicate keys and partial
per-write outcomes are part of the API contract. Stability preserves request
order among equal keys.

A server-side implementation is preferable to relying on callers to sort: the
coordinator first partitions a client batch by destination node, so the physical
append unit is the replica-local sub-batch, not necessarily the original batch
of 1000.

The existing staged LWW path prevents this from being just one `sort` call.
RAM-resolved/absent keys can append during the first staging pass, while keys
that need run reads append after asynchronous resolution. Iterating each pass in
key order would produce two approximately sorted bands, not one fully sorted
batch. Guaranteeing one band requires resolving the whole sub-batch before
appending its winners, retaining the key/value/permutation state across the
read phase and revalidating the storage generation. The blind-append mode is
simpler, but it still must preserve response indexing and equal-key behavior.

### Expected benefit

This is the cheapest candidate when each sub-batch covers a narrow, mostly
monotonic key interval. Successive batches then lay adjacent key ranges next to
each other in the value log, allowing ordered scan pages to collapse into fewer
LBA spans. Dump restore and time-ordered keys are plausible workloads.

It gives little clustering for uniform-random batches: sorting 1000 random keys
still spreads each batch across almost the whole keyspace, and successive
batches overlap. It also cannot repair placement across batch boundaries,
across replica-local partitions of one client batch, or across the warm/cold
staged-apply split. It does not address 0132's measured cold LWW index reads.

### Costs and correctness obligations

- Preserve one response per input entry in input order, including duplicate
  keys, stale versions, skew rejection, retries, and partial failures.
- Preserve LWW/Merkle outcomes when several versions of one key occur in a
  batch. Physical sequence is a recovery tie-breaker for equal versions, so the
  equal-key ordering and append/result semantics must be test-pinned.
- Account for sort/permutation allocation and key comparisons on the serving
  core. “Near-free” is a hypothesis, especially for large keys or small values.
- Decide whether approximate two-band ordering is enough; do not lengthen the
  staged-apply gate and retain all values until every cold probe completes
  without measuring its latency and RAM effect.

**Gate result:** do not implement. The available replay has no scan-span datum,
and its ingest decline is not append order. This is the first candidate to
reopen because it has the smallest implementation and operational surface.

## Mechanism 2: key-range-bucketed value-log streams

### Proposed shape

Hash or range-partition keys into a fixed number of ordered buckets and retain
one open append segment per bucket. Records in a bucket would cover a narrower
key interval than the entire zone even when client batches are random. Ordered
scans would then read from fewer, more locally contiguous segment regions.
Range buckets, rather than ordinary hash buckets, are required for ordered-scan
locality.

The bucket mapping must be stable for the life of records or recorded in
metadata. Quantile-derived boundaries could reduce hot buckets, but changing
boundaries creates generations of placement and complicates both operations and
measurement; fixed prefix/range boundaries are simpler but can skew badly.

### Interaction with current semantics

The current `ValueLog` has one current segment, one partial-block tail, one
current epoch, one global sequence allocator, one in-flight write queue, and one
tail overlay used by reads before the partial block is durable. `sync()` drains
that queue, writes the one dirty tail block, and issues one device flush.
Recovery scans ordinary log segments by epoch, replays segments at/after the
manifest watermark, and resolves winners by version (then sequence for an equal
version). A flush records one covered sequence/epoch watermark and protects the
ordinary tail segments through that watermark in run metadata.

Bucketed streams therefore require a real multi-tail design, not an array around
`append()`:

- allocate global sequence and epoch identities without collisions while
  routing records to independent tails;
- drain every stream's in-flight writes and persist every dirty partial block
  before the single device flush releases a group-commit cohort;
- make point reads and span plans overlay the correct one of several dirty tail
  blocks;
- carry all covered and post-watermark tail segments through flush, manifest
  liveness, victim reclaim, crash recovery, and allocator reconstruction; and
- define segment closing/rebalancing for cold, hot, and remapped buckets.

One device flush could still durably cover all streams after their dirty blocks
have been submitted, so N streams need not mean N flush commands. The expensive
part is correctly establishing the multi-tail pre-flush boundary and recovery
contract.

### Expected benefit and costs

Unlike intra-batch sorting, bucketing can cluster a uniform-random stream. It is
worth considering when ordered scans are demonstrably constrained by scattered
value I/O, or when scan traffic consumes enough device IOPS to harm foreground
reads/writes despite a CPU-bound single scan.

Costs are substantial:

- N open data segments and N partial blocks per engine/zone increase reserve
  consumption; cold buckets leave partially filled segments resident longer.
- Hot buckets can roll rapidly while cold buckets strand capacity, requiring a
  skew policy that does not undermine the simple recovery mapping.
- More placement generations can reduce coalescing until old data ages out or is
  rewritten, so changing N/boundaries is not instantaneously effective.
- Segment-level victim reclaim and per-run `value_seg_bytes` remain necessary;
  placement does not reclaim overwritten bytes.
- Routing, multi-tail overlays, group commit, flush snapshots, recovery, format
  compatibility, and crash tests all expand together.

**Gate result:** do not implement. Nothing in 0132 shows device-I/O pressure or
space pressure from placement, and the only scan A/B shows a 1.61x wall ceiling
after a 64x read reduction. The implementation risk is disproportionate to the
unmeasured benefit.

## Mechanism 3: trivial install for sorted, non-overlapping input

“Trivial install” has two possible scopes and they should not be conflated.

1. **Keep an existing non-overlapping flush run without folding it into adjacent
   runs.** Flush already writes a key-sorted immutable run and atomically adds it
   to the manifest. Point reads already reject runs whose `min_key..max_key`
   cannot contain the key. The recurring merge selector, however, is size- and
   run-count-driven rather than overlap-aware, so it may later rewrite the run's
   index.
2. **Build and install one external/bulk run directly.** This could avoid the
   normal per-record memtable path, LWW old-version probes, many small flushes,
   and their later index merges. It is the larger potential ingest win, but it
   is a new bulk-ingest protocol and commit path, not merely a merge-selection
   optimization.

### Proof and correctness requirements

A safe fast path needs a conservative proof that the incoming key interval is
sorted, unique or internally LWW-folded, and disjoint from all live run and
memtable keys for the target engine. `RunMeta.min_key/max_key` can prove interval
disjointness from runs; it cannot prove that a broad interval with holes is
logically disjoint. Concurrent writes and range-map movement must be excluded or
revalidated at commit.

A direct installer must also preserve the work currently performed per write:

- zone/range ownership and stale-epoch fencing;
- RF placement and the requested acknowledgement/persistence contract;
- version/skew and tombstone-admission rules;
- Merkle leaf accounting and recovery-visible version winners;
- value-log segment liveness (`value_segs`/`value_seg_bytes`) and manifest
  new-before-old crash safety; and
- input-to-result/error accounting, or an explicitly different bulk API.

Keeping many disjoint flush runs forever is not free. Although point probes can
skip out-of-range runs, a full-zone scan spans all of them, and every run retains
bloom/block-index metadata. Run-count bounds still need an overlap-aware
concatenation or promotion policy. A true bulk builder should instead produce a
small number of large runs and key-ordered values in one pass.

### Expected benefit

This can pay for a certified initial load whose files map to disjoint key
intervals, with writes quiesced for those intervals. It is not applicable to a
normal replay containing overwrites/tombstones interleaved with existing keys.
The 0132 corpus had 155.16 million records but only about 109 million distinct
live keys and exercised LWW run reads, so the evidence does not establish the
required non-overlap workload. Its merges stayed bounded and were not the
throughput limiter.

**Gate result:** do not implement. Reopen only for an explicit sorted bulk-load
contract and corpus that proves substantial avoidable merge/probe cost. Do not
silently infer the property from a dump file's current ordering.

## Reopen criteria and required A/B

Use a fixed corpus, placement, binary, and cluster recipe. Verify byte-identical
logical results and report confidence across repeated arms rather than comparing
single noisy passes.

### Intra-batch stable sort

Reopen when a production replay/export pair shows narrow replica-local batch key
spans and scan pages frequently read values written by the same or adjacent
batches. A kill-switched A/B must record:

- ingest records/s versus progress, batch-apply latency, serving CPU, allocation,
  cold LWW run reads, and merge state;
- replica-local batch key-span/locality distributions, not just client batch
  size;
- post-load scan keys/s, value device reads, coalesced span count/bytes, scan CPU,
  and foreground read/write interference; and
- duplicate-key and recovery equivalence with sorting off/on.

Implement only if the scan/IO-budget gain is material and ingest/tail latency is
neutral within run-to-run noise.

### Bucketed append streams

Reopen only if the preceding A/B leaves a material scattered-read problem and
profiles show device I/O or mixed-load interference, rather than the known
per-core scan CPU ceiling. Sweep N and boundary policies. In addition to the
scan/ingest metrics above, measure open/partially-filled segments per zone,
space amplification, bucket skew, group-commit latency, recovery time, and crash
behavior across flush and remapping boundaries.

### Trivial install

Reopen for a named bulk-load source that can certify sorted, non-overlapping
intervals before dispatch. Compare ordinary replay with bulk build/install on
logical records/s, cold LWW probes, flush/run count, cumulative merge input
bytes, bytes written, RSS, restart time, and post-load point/scan performance.
Include concurrent-overlap refusal, stale range epoch, torn install, replication
failure, tombstone/version, and Merkle-equivalence tests. The fast path pays only
if run/probe elimination is a measured bottleneck and the proof remains cheap.

Until one of these gates fires, task 0069's current policy remains the better
trade: recurring merges are index-only, value clustering is opt-in and at most
bottommost, and victim-targeted rewriting handles reclaim without making value
placement part of every merge.

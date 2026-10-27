# Range deletes for sorted zones (versioned range tombstones)

Status: **built** (task 0439, 2026-10-15). This note was the design (slice 0443)
and is kept as the as-built reference. The slices, in order: fail-loud decoding of
newer formats (0450), run format v6 (0451), the fragment set (0455), the engine
core and cover-aware engine reads (0452, 0453), compaction filtering and RT
collection (0454), replica apply semantics (0456), the accounted set and the RT
fold (0457, 0458), the wire op and server handler (0459), the replica skip-scan
(0460), the coordinator (0461), the anti-entropy RT exchange (0462), re-home copy
and mirror (0463), the cleanup handoff (0464), the AE `Busy` measurement with the
inline fold turn (0465), backup VREC v2 and `VDUMP\x02` (0448), and the
validation and docs slice (0449). Where the build differs from the text below,
the text says so. Measurements: the head-of-queue counters and the zero-RT
hot-path A/B in `bench/0449-rt-hot-path/FINDINGS.md`, AE `Busy` under
continuous deletes in `bench/0465-rt-ae-busy/FINDINGS.md`. Operator notes are in
`deploy.md` §Range deletes, the client API in `getting-started.md`, and byte
layouts in `formats.md`.

## Why

A sorted zone can delete only one key at a time. Deleting a span costs one point
tombstone per key, and every replica `Scan` page returns those tombstones until
`gc_grace` collects them (never, at the default `gc_grace = 0`). A queue, log or
time-bucket workload that consumes and deletes in key order therefore pays scan
cost proportional to its deleted history. A range delete records one versioned
object per span instead, and replicas skip what it covers.

Prior art: RocksDB `DeleteRange` and Cassandra range tombstones. Both orders
things by sequence number or timestamp per SSTable. vals cannot: its runs are not
version-ordered (anti-entropy pulls, read-repair and blind appends write old
versions into new runs), so every rule below compares **versions**, never run
recency.

## Semantics

```rust
Coordinator::delete_range_in(
    zone,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
    options: impl Into<WriteOptions>, // a bare WriteConcern is buffered
) -> Result<Version, CoordError>
```

- **Sorted zones only.** Unsorted zones and the default zone return
  `CoordError::NotSorted`, as `scan_in` does. `lo`/`hi` use `scan_in`'s bounds:
  `lo` inclusive, `hi` exclusive, `None` unbounded. An empty span (`lo >= hi`) is
  refused (`CoordError::InvalidRange`) without stamping a version.
- The coordinator stamps **one** `Version` V, splits `[lo, hi)` by its range map
  (`RangeMap::ranges_overlapping`, as scans do) and sends each range's slice to
  that range's replicas. Each slice needs W acknowledgements under the write concern, exactly
  as a single write needs them. Slices to the same replica shard travel in one
  request (up to 4096 slices), with bounded request concurrency.
- **Not atomic across ranges**, like a batch. On failure the call returns an error
  that carries V and names the slices that missed W (`RangeDeleteIncomplete`).
  Slices that reached W stay deleted. Retrying the whole call is always safe: a
  later delete gets a greater version and covers the same keys.
- No sloppy variant and no hinted handoff: a replica that misses a range
  tombstone (RT) gets it from anti-entropy (§4), which exchanges RT sets every
  sweep pass.
- A coordinator issues range deletes only on connections that negotiated hello
  feature bit 3 (`RANGE_TOMBSTONES`, §8). If any target replica lacks it, the call
  fails with `CoordError::Unsupported` before stamping. Range deletes are therefore
  available once every node is upgraded.

**Cover.** A replica holds a set F of RT fragments. For a key k,

```
cover(k) = max { f.version : f ∈ F, f.lo <= k < f.hi }        (none if no fragment holds k)
```

The logical winner of k is the argmax over the physical point candidates for k
**plus a synthetic tombstone at cover(k)**. A point candidate with
`version <= cover(k)` is **covered**: it can never win. Equality only arises for a
point tombstone at exactly V, which read-repair writes (§3); treating it as covered
lets compaction drop it. Versions are otherwise unique per write, so the tie rule
changes nothing else.

Consequences, all of which fall out of the cover rule:

- A key in `[lo, hi)` whose winner is older than V reads as absent. A write newer
  than V wins as usual, including one that lands before the RT on some replica.
- A straggling write older than V that arrives after the RT loses on every apply
  path (§3), including for keys the replica has never seen. Nothing is stored.
- CAS sees a covered key exactly as it would see a point tombstone at V.

## 1. Engine representation

### In-RAM fragment set

Each sorted-zone engine keeps F as a canonical set of disjoint, non-empty
fragments `(lo, hi, version)` in a `BTreeMap` keyed by `lo` (an unbounded `lo` is
the empty key; built in 0455, `range_tombstone::FragmentSet`):

- **Overlay** (applying an RT): the new cover is the pointwise max of the old cover
  and the new RT's version. Fragments split at the new RT's bounds where needed.
  Adjacent fragments with equal versions coalesce. Fragments carry no `seq`: a
  per-fragment "max seq when coalescing" would depend on application order once a
  later RT splits a coalesced fragment, so seqs stay on the RT records, from which
  the accounted and filter sets are built. Overlay is a join: commutative, associative and idempotent. So replicas that
  apply the same RTs in any order hold the same canonical set, and re-applying an
  RT (retry, AE, copy) changes nothing.
- The overlay returns its **delta**, a list of `(span, old cover, new cover)` where
  the cover rose. The cluster layer uses it for accounting (§4).
- **Lookup:** `cover(k)` is one `range(..=k).next_back()` probe, O(log |F|). Every
  caller first tests `F.is_empty()`, so an engine with no range tombstones pays one
  predictable branch per operation. Key-ordered consumers (scans, seeds, folds,
  flush and merge streams) walk F with a cursor alongside their key stream, so
  their cost per entry is O(1) amortised.
- **Cap:** `range_tombstone_max_fragments` per engine (default 65,536, roughly
  6 MiB at 100-byte bounds). A slice whose overlay would exceed it is refused with
  a per-slice `Err` (not `Busy`: it does not clear on its own) and counted.
  Cumulative deletes `[base, cursor)` with rising V collapse to **one** fragment.
  Deletes of disjoint spans accumulate one fragment each until `gc_grace`
  collects them (§7). With `gc_grace = 0` they are kept forever, as point
  tombstones are.

### Persistence

- **Value log.** An RT is appended as a record of a new kind `2 = RangeDelete`.
  Its key is empty, and its value is
  `[u8 flags (bit 0 lo present, bit 1 hi present)][u32 len][lo][u32 len][hi]`.
  The record version is the packed V. It joins the shard/zone group-commit barrier
  like any write, so `Persistence::Device` waits for its sync. An overlay that
  changes nothing appends nothing. Replay of the uncovered tail (records with
  `seq > covered_seq`) overlays RT records into F; they are not memtable entries.
- **Runs (format v6).** A flush writes into its output run the RT records whose
  `seq <= covered_seq` of that flush. They go in a new CRC-checked **RT block**:
  `[u32 count] × ([u8 flags][bytes lo][bytes hi][u128 version][u64 seq])`.
  - The block holds **records** (applied RTs), not F's canonical fragments. Each
    record has one seq, and a record is taken by exactly one flush. Every run
    rewrite (merge, fold, value-log GC, forget plan) moves its inputs' records into
    its output, minus those collected or dominated (§7) or released by cleanup
    (§5). A flush also leaves out dominated tail records (§7).
  - So **every persisted RT record lives in exactly one run at a time**, and
    removing one is part of the same manifest commit as the rewrite of that run.
  - F is the overlay of the records of every run plus the replayed tail. Removing
    a record recomputes F over that record's span from the records that remain.
  - A flush with RTs but no point entries writes an **RT-only run**. Its manifest
    entry has `entry_count = 0` and `min_key = max_key = ""`. The bounds of an
    unbounded fragment cannot be expressed in `min_key`/`max_key`, and the run
    holds no point key to bound. The manifest's `min_key`/`max_key` describe point
    entries only, for every run.
  - Every key-range consumer (lookups, merge planning, shadow and dead-fraction
    estimators, fold and seed sources, bucket and sidecar paths) must treat
    `entry_count = 0` as "holds no point key". Slice 1 audits each of them.
  - Once its run commits, the RT's value-log record is dead, and value-log GC
    reclaims it like any superseded record.
- **Why runs and not the manifest.** Today's `load_manifest` maps any manifest
  decode error, including an unknown version, to "no manifest". A previous-release
  binary would then boot an engine with a v5 manifest as if it had no runs. It
  opens runs **before** replaying the tail, and an unknown run version fails the
  open loudly (`decode_footer`: "unsupported run version"). Keeping the manifest
  at v4 and putting RTs in v6 runs turns a downgrade into a loud open failure.

### Downgrade fence

- New runs are written as v6 by default. `VALS_RUN_WRITE_VERSION=5` (or `4`, both
  still accepted) keeps writing older runs for a rollback window, as for v5 (0365).
  A node in either mode **refuses range deletes** (per-slice `Err`).
- **Invariant:** once an engine has accepted an RT, every manifest it commits
  references at least one v6 run. A previous-release binary therefore fails at
  run open, before it can reach a tail that holds an RT record. That binary stops
  walking a segment at an unknown record kind without an error, so it would
  otherwise silently drop the rest of the segment.
  - *Establishing it.* Before the first RT record is appended, an engine whose
    manifest references no v6 run commits an empty RT-only v6 marker run, and only
    then appends the record (0452). The record reaches a run with the next flush
    like any tail record. Its seq is always above the `covered_seq` of every
    committed manifest (a fresh engine skips seq 0), so tail replay never mistakes
    it for a persisted record.
  - *Keeping it.* Every later manifest commit preserves it. If a merge, fold or
    forget plan would leave the manifest with no v6 run, it writes an empty
    RT-only v6 marker run in the same commit. That case arises only when runs are
    written as v5/v4 and range deletes are therefore refused, or when every run
    has been forgotten.
- The same slice makes this release fail loud on formats newer than itself, so
  later format bumps are fenced too. A CRC-valid manifest chain with an unknown
  version, a CRC-valid segment header with an unknown format version (today
  `scan_log_segments` treats it as free space) and a CRC-valid record with an
  unknown kind all become open errors instead of being skipped.

### Block version bound (run format v6)

Each block-index entry gains `max_hlc: u64`, the largest HLC among the block's
entries. If `max_hlc < V.hlc`, every entry in the block is older than V. This is
the first sound run-level version bound the read path has (`blind-append-lww.md`
notes none existed). Only the RT skip (§2) uses it, so no existing read changes
behaviour.

The per-block field is resident with the fence index: 8 B per ~32 KiB block,
about 0.04 B/entry next to the fence's ~1.1 B/entry. While F is non-empty, every
v6 run of the engine also has a resident **max tree** over those values. It is an
implicit segment tree that answers "first block at or after i with
`max_hlc >= h`" in O(log blocks), at 16 B more per block (it stores only the
internal nodes, padded to a power of two; its leaves are the resident per-block
values).

**The scan path never builds a tree.** Trees are built in three places:

- *Run install.* A run installed while F is non-empty (flush, merge, fold or
  value-log GC output) builds its tree from the block maxima its build just
  computed. That adds O(blocks) to a build that is already O(entries).
- *Open.* An engine that opens with a non-empty F builds trees for all its v6
  runs, in the same pass that loads their fence indexes.
- *First RT.* When F goes from empty to non-empty, the first RT's fold (§4)
  builds the trees of every existing v6 run before it completes.

An engine drops its trees when F becomes empty, so engines with no range
tombstones pay only the 8 B per block. A scan that meets a v6 run without a
tree can do so only before that first fold completes. It then filters the run
entry by entry, the documented pre-fold case of §2.
`vals_rt_max_tree_built_blocks_total` counts construction work.

v5 runs have no bound and are never skipped. Merges rewrite them as v6, and
frozen v5 runs are rewritten when space reclaim picks them (§7).

## 2. Scans

### Replica side

A replica scan merges the memtable and run cursors as today, with two additions
when F intersects the window:

1. **Filter.** An entry with `version <= cover(k)` is dropped: it is not
   returned, not counted toward `max`, and not charged to the reply byte budget.
2. **Skip.** Block i of a run spans `[first(i), first(i+1))`; the last block
   spans `[first(i), run max_key]`. A block may be skipped without reading it only
   if **its whole span lies inside one fragment f** and `max_hlc(i) < f.version.hlc`.
   That means `first(i) >= f.lo`, or the cursor is already inside f at or after
   `first(i)`, and `first(i+1) <= f.hi` (for the last block: `max_key < f.hi`).
   - A run cursor inside f locates the block holding `f.hi` (`j`) by binary search
     on the fence index.
   - It uses the max tree (§1) to find the first block in `[i, j)` with
     `max_hlc >= f.version.hlc`, and skips everything before it.
   - The block it lands on, and block `j` itself (which straddles `f.hi`), are
     read and filtered entry by entry. So are the blocks straddling `f.lo`. Live
     keys at or above `f.hi` that share a block with the covered tail are always
     returned.
   - The memtable cursor filters in RAM. The RT fold (§4) removes covered memtable
     entries, so the memtable holds none once that RT's fold has completed.

A page also stops after a covered-work budget (`SCAN_COVERED_BUDGET`: 65,536
filtered entries, or 64 run blocks read that held a filtered winner, per page,
whichever comes first). Blocks whose entries are all returned do not count, so
the budget never shortens a page that meets no covered entry, while live keys
interleaved with covered ones do not reset it. The budget is checked when an
entry is filtered.
If it cuts the page, the page returns `more = 1` and a **resume key** just above
the last filtered key, so strictly above the request's `lo`, and it may carry no
entries. Today's rule that a truncated page is never empty still holds for peers
without feature bit 3. They never receive a cut page: a cut always lands on a
filtered key, so the window meets F and the old-client rule below applies.

Only the `Scan` serve path uses the cover-aware staged scan. Cleanup's key
enumeration keeps the plain staged scan, because it must also see covered keys to
forget them. The synchronous fallback (`plan_scan`, after three staged restarts)
filters and skips the same way but has no covered-work budget.

Each page carries the fragments of F that overlap its **window**
`[lo, coverage)`, clipped to it. `coverage` is the resume key if present, else
`last_key ++ 0x00` if `more`, else `hi`. Fragments count toward the reply byte
budget: the replica walks the page's entries and fragments in key order and, at
the first item that would overflow the budget, ends the window at that item's
start, which becomes the page's resume key (items starting at `lo` always fit).
Fragments are the only way a lagging peer's covered
entries get hidden, so a page **must** report every fragment it holds over its
window, including spans it skipped.

### Coordinator side (`algorithms.md` §6)

- `coverage(page)` becomes the resume key when present. The min-coverage `bound`
  is unchanged otherwise.
- Within `[cursor, bound)` the coordinator overlays the fragments from **all**
  responders into a window cover, then LWW-merges point entries as today. It
  emits a key only if its winner is a live value with `version > cover(k)`. It
  applies this on single-responder rounds too: a page whose scan straddled an
  RT's arrival may hold an entry the replica examined before the RT was applied,
  while its fragment list, captured at the end of the page, covers it.
- An empty page is valid coverage when it carries a resume key. A `more = 1` page
  with neither entries nor a resume key is still a protocol violation.

**Head-of-queue cost.** Take a scan that starts below a span of N range-deleted
entries, on replicas that hold the RT, whose fold has completed, and whose runs
are v6. Per run, its replica work over the span is two O(log) searches (fence
index and max tree) plus at most two boundary blocks read and filtered. The
memtable holds no covered entries. Device reads, filtered entries, reply bytes
and page rounds therefore do not depend on N, before any merge and after. The
acceptance test measures this with the counters in §8.

Two documented exceptions:

- **Before the RT's fold completes** (normally milliseconds), covered memtable
  entries are filtered one by one. For an engine's first RT, so are runs whose
  max tree is not built yet. The memtable byte cap bounds the memtable part, and
  the covered-work budget bounds both per page.
- **v5 runs** have no block bound. Their covered entries are filtered one by one
  until a merge or space reclaim rewrites them.

**Lagging replica.** A replica that missed the RT still returns its covered
entries, and the coordinator hides them using the other responders' fragments.
That is correct but costs rounds proportional to N, until anti-entropy delivers
the RT (one sweep pass of the lagging node). v1 adds no scan-time RT repair.

**Old clients.** A `Scan` request on a connection without feature bit 3 whose
window intersects F is answered `Err("range tombstones present: client lacks
RANGE_TOMBSTONES")`. An old client cannot hide covered keys itself, so an answer
would be wrong. Windows with no fragment are served unchanged, so `valsgo` and
`bootes` builds from before 0468 keep working on zones that never use range
deletes.

## 3. Point reads, digests, read-repair, writes

- **Reads.** A covered key reads as a **tombstone at cover(k)**. This holds even
  when the replica has no physical entry for it. If the replica answered "absent",
  a quorum read whose other responder still held the old value would pick that
  value. `Read`, `Digest`, `MixedBatchRead` (both roles) and `SyncValues` return
  `[version V][None]` / `[version V]`. No reply format changes. The engine
  returns a synthetic tombstone entry with no value pointer, and readers must not
  plan a value read for it. The cover probe runs in RAM before the device read,
  and the normal lookup still runs because a newer point write may exist. The
  winner directory (`latency` profile) and the key-filter negative path apply the
  same check.
- **Read-repair** needs no change. It writes a point tombstone at V to responders
  behind it. That is one record per key actually read. The RT itself reaches them
  through anti-entropy, after which the repaired tombstone (version = V) is covered
  and compaction drops it.
- **Store-if-newer.** Every `VersionedStore` apply path tests `cover(k)` after the
  skew check, the admission floor and `note_version`, and before any probe. A
  candidate with `version <= cover(k)` returns `Applied::Lost(cover)` with no
  append and no run read. Coordinators already count `Lost` as an ack. The paths
  are: `apply_at`, `apply_stage`, `apply_blind_at`, `apply_commit_staged_snapshot`
  (which re-tests, because an RT can land during the probe await), the batch and
  re-home copy paths built on them, and local `put`/`delete`/`cas`. Local stamping
  observes `max(point, cover)`, so a local write after an RT gets a greater
  version. The blind path's test is RAM-only, so it stays read-free.
- **Apply an RT** (`VersionedStore::apply_range_delete(lo, hi, V, wall)`):
  - Skew check (`Rejected` beyond ε). No admission floor: like a point tombstone,
    an RT cannot resurrect anything.
  - `note_version`, HLC observe, then engine overlay (§1) and append.
  - The reply is `Won` (applied or already implied), `Rejected` (skew) or `Err`
    (cap, or a v5-only node).
  - The overlay delta goes to the accounting fold (§4).

## 4. Anti-entropy

### Logical state excludes covered keys

Merkle leaves (`algorithms.md` §5), `BucketDigests` pages, seeds, dirty-bucket
rebuilds, regroups and split recounts all describe the set of **uncovered point
winners**: live values and point tombstones newer than any covering RT. Covered
keys contribute nothing, whatever physical copies a replica still has. So two
replicas with the same F and the same uncovered winners have equal leaves however
far each has compacted. The rejected alternative (digesting covered entries) is
discussed at the end.

### The accounted set and the RT fold

F changes when an RT arrives, and the keys it newly covers must leave the leaves
and range counts. That costs one enumeration of the newly covered winners. Each
`VersionedStore` therefore keeps an **accounted set** A: the cover its leaves and
counts currently reflect. A is always the overlay of every RT whose local record
seq is at most an **accounted seq** `S_A`. Serving and LWW decisions always use F.

- **Queue.** Each applied RT whose overlay changed F joins a queue in record seq
  order. The shard accounting task folds the queue one **fold** at a time.
- **Immutable target.** A fold starts by fixing its target `T = A ⊕ (queued RTs
  with seq <= S_T)`, where `S_T` is the highest queued seq at that moment. T does
  not change during the fold. RTs that arrive later wait for the next fold. The
  fold's spans are where T is above A.
- **The fold.** It runs in bounded turns: 1,024 winners or 1 ms, under the seed
  duty budget, prefetched under the read gate and validated by storage
  generation, exactly like split recounts and regroups.
  - It enumerates the point winners in its spans that are visible under A, using
    the §2 skip with A, so history already covered under A is not re-read.
  - For each winner covered under **T** (not F), it XORs `entry_digest(k, v)` out
    of its group leaf and decrements its range count.
  - It removes the covered memtable entries it meets, using the remove-if-equal
    primitive the flush commit uses. This is RAM only. Their records stay in the
    value log, and replay finds them covered. A run that an in-flight flush is
    building may still hold them; the RT covers them there too. (Point `delete`
    refuses during a flush because no RT would cover a snapshotted copy.)
  - It keeps a cursor. In its spans, keys below the cursor are accounted under T,
    keys at or above it under A. When the fold completes, `A := T` and
    `S_A := S_T`.
- **Writes.** Leaf and count maintenance treat key k's old winner as present in
  the leaves iff it is not covered by `acct(k)`. `acct(k)` is `T(k)` if k is in
  the fold's spans below its cursor, else `A(k)`. This is the rule regroup uses
  for its old placement. A write that F accepts has a version above
  `F(k) >= T(k) >= A(k)`, so it always enters the leaves.

  Take the overlapping case. RT1 (V1) is queued, then RT2 (V2 > V1) arrives over
  a point winner W with V1 < W < V2.
  - A fold that targets only RT1 leaves W in place, and the next fold removes it.
  - A fold that targets both removes W once.
  - A newer write to k in between (necessarily above V2) XORs W out under
    `acct(k)`, and the fold then finds an uncovered winner.

  In every order, W leaves the leaves exactly once.
- **Busy while pending.** While a fold is pending, tree and reconcile requests for
  that store answer `Busy`, and `RangeStatsV3` reports accounting not ready.
  Regroup has the same rule, for the same reason.
- **Inline turn.** A replica serving `RangeDelete` runs one fold turn (one page of
  the turn bound above, block reads awaited under the read gate) after applying
  the slices and before it replies. It kicks the accounting task only if a fold
  is still pending. A queue-sized fold completes inside the request, so AE
  requests never see it pending. The fold's target overlays only the RT records
  with seq in `(S_A, S_T]`.
- **Serialisation.** Seeds, recounts, regroups and folds run on the one
  accounting task and never interleave inside a page. A full seed filters with F
  as of its start, at seq `S`. When it publishes, `A := F_S`, `S_A := S`, and
  queued RTs with seq `<= S` leave the queue.
- **Compaction filters with A, never with F** (§7). It uses only RTs with
  `seq <= S_A`, and ignores the in-flight fold. Physical drops therefore only ever
  remove entries the leaves already exclude, and no merge has to report covered
  drops to the accounting layer. The engine learns `S_A` from
  `Engine::advance_range_tombstone_accounted_seq` (monotone, 0 after open, so an
  engine whose replica layer never sets it filters nothing).

Queue workloads issue many small RTs, so each fold is small: only the entries
consumed since the previous delete. A delete of a large live span folds once,
O(span) sequential reads in the background, like a split recount.

### RT set reconciliation

Range tombstones need their own exchange. Leaves exclude covered keys, so they
cannot tell two replicas apart if one is missing an RT that covers keys neither
of them holds.

- New op `SyncRangeTombstones`. The request is
  `(zone, requester, digest, after)`. The responder clips its F to the ranges
  whose replica sets (current or pending, the cleanup `keep` predicate) contain
  both requester and responder.
- The digest is a `hash64` fold over the clipped canonical fragments
  `(lo, hi, version)`. If both digests match, the reply is "same". Otherwise the
  reply is a page of fragments after `after`.
- The requester applies each fragment through `apply_range_delete`. That gives the
  same skew check, overlay and fold as a client RT.
- It runs once per (zone, peer) per sweep pass, **before** the Merkle compare, so
  a pass first adopts the peer's RTs and then compares leaves.
- An implementation may piggyback the digest on the level-0 `SyncTree` reply
  (gated by bit 3) to save the RPC when the digests are equal.

**Convergence.** F converges because overlay is a join and every pass exchanges
it. Given equal F, uncovered winners converge exactly as today. The only extra
pull is replica A pulling a bucket in which a peer still holds a covered entry
because that peer has not yet adopted the RT. The pull's apply returns `Lost`,
and it stops once the peer's own pass pulls the RT from A, so a stale peer causes
at most one sweep pass of wasted pulls. No resurrection is possible: a covered
pull never stores.

## 5. Range lifecycle

- **Inherited splits, opaque token assignment.** No data moves and F is keyed by
  key span, so nothing changes. Split recounts filter with A.
- **Slices and shards.** The coordinator clips per range, so each stored fragment
  starts inside one range. A replica accepts a slice only if every range of its
  own map overlapping the slice is owned by it (committed or pending re-home
  target) and routes to the receiving shard. Otherwise the whole request gets
  `StaleEpoch`, as `BatchApply` does, and the coordinator refreshes its map and
  retries with the same V (idempotent). A later inherited split may leave a
  fragment spanning two ranges with the same owners and shard, which is harmless.
- **Confirmed re-home.**
  - *Copy.* The pending owner pulls `Scan` pages from a current owner. It applies
    each page's fragments before the page's entries, so covered entries are never
    copied as live. A page cut by the covered-work or reply byte budget may
    carry fragments and no entries; the copy continues from its resume key. A
    fragment the target rejects (clock skew) or cannot apply fails the page, which
    is retried on a later tick. When the copy completes, the target's F contains
    the source's F over the span.
  - *Mirror.* A current owner mirrors each RangeDelete slice that overlaps a
    pending range to every pending owner, clipped to that range. It uses the same
    fail-closed protocol as writes: timeouts retried twice; failures and
    unresolved timeouts poison the move.
  - Cutover needs no new condition.
- **Covered merges** (0477). The PD merges adjacent ranges that hold no uncovered
  entries and that every owner reports wholly covered by F (docs/zones.md
  "Covered merges"). A sibling from an inherited split keeps its placement, so
  nothing moves. Otherwise only F and covered entries change owner, through the
  cleanup handoff below.
- **Cleanup handoff** (after cutover, auto-merge, membership change, wrong-shard
  strays). For a released span S, cleanup first hands F ∩ S to the owners and
  needs the same confirmations as keys. It then hands off uncovered winners as
  today. Covered entries are not handed off; they become forgettable once the
  fragments are confirmed. Forgetting S also clips the RT records over S out of
  RAM and, in the runs that the forget plan rewrites, out of their RT blocks. A
  restart before a run holding such a record is rewritten brings back a fragment
  over a span the node no longer owns. That is harmless: it covers only
  forgotten entries. If the node later owns S again, any owner that
  has collected the fragment (§7) holds no entry older than it, so the leftover
  hides nothing. In detail (0464):
  - *Pieces.* Each cleanup page covers the window from its resume key to the next
    page's start (to the lap end on the last page). Cleanup clips F to the window
    and splits it by range. A piece whose range the node no longer owns (the
    cleanup `is_mine` test, current or pending owner and shard) goes to that
    range's owners as `RangeDelete` slices: one request per (owner, version), not
    synced, like key handoffs. They may use at most half of the page's handoff RPC
    cap. If the pieces need more, the window ends at the first piece that does not
    fit, and the next page starts there. The key loop keeps the rest of the cap and
    its own wall budget, so every page makes progress on keys too.
  - *Release.* A piece is released when `Won`/`Lost` acks reach the key
    confirmation threshold and the accounted set A already covers the whole piece
    at its version (no seed in flight). Then no entry under the piece is in the
    leaves or counts, and dropping it changes neither. Any other piece leaves the
    lap dirty and is retried. Released pieces join the cleanup forget batch with
    the released keys.
  - *Covered keys.* A covered key reads as a tombstone at its cover, which differs
    from its listed physical version, so the key loop never hands it off. The
    span drop below forgets it. (A read-repair tombstone at exactly V is handed off
    like any key, and an owner that holds the fragment answers `Lost`.)
  - *Forget plan.* The engine keeps only pieces that F still covers entirely at
    exactly the piece version. So a piece whose span a newer RT has raised, or one
    already clipped by an earlier forget, is not released. The forget rewrite takes every run and drops every entry under
    a released piece, as well as the released keys. A released key also loses
    its older run versions, since the rewrite drops versions up to the confirmed
    one. The rewrite clips the pieces out of its output's RT block. At plan
    time the engine also removes covered memtable entries under the pieces (RAM
    only). Their records replay covered, because the RT record that covers them
    is newer and so is in the tail too. No flush is in flight at plan time, and no
    covered write can enter later, so no other run can pick up an entry under a
    piece.
  - *Commit.* The commit clips the pieces out of the tail records and recomputes
    F. The replica then recomputes A and an in-flight fold's T from the records.
    A clip removes only the parts of a record whose version is at or below the
    piece version, so an RT applied while the rewrite ran survives. Runs other
    than the rewrite's output keep their records, so a run's in-RAM records always
    match its RT block. Such a run can only be one flushed after the plan, from
    tail records. The commit also leaves the tail alone while a flush is in
    flight, because that flush has already captured it. Either way the result is
    a fragment over S that covers nothing, and the next lap releases it. A
    clipped record's pieces keep its seq and stay together in one run or in the
    tail.
- **Zone drop and liveness wipe.** Both discard the engine, F with it. A wiped node
  refills F through the RT exchange during its fenced refill.

## 6. Accounting

Range counts (`RangeStatsV3`, auto-split, auto-merge, balance weights) count
**uncovered point winners**, the same set as the leaves. Retained point tombstones
still count until GC, as today. An RT is not a counted entry. Covered keys leave
the count when the fold passes them. Until then the store reports accounting
not ready for its pending spans, so a balance move never uses a count the fold has
not reached. The lag is the fold's runtime, normally milliseconds for queue-sized
deletes.

## 7. GC and compaction

- **Flush** drops memtable entries (values and point tombstones) covered by the
  RTs with `seq <= min(S_A, covered_seq of that flush)`. The rule exists for a
  crash case. Suppose a covered entry were dropped on the strength of an RT whose
  record is still unsynced. A crash could then lose the RT while keeping the
  synced run without the entry.
- **Merges and folds** (recurring, forced, pressure, shadow folds, value-log GC
  rewrites) drop input entries covered by the RTs with `seq <= S_A` that are
  persisted in runs, as captured at plan time. A covered entry may be dropped from any run in
  any merge, not only bottommost ones. While the RT exists every copy of the key
  older than V is also covered, so reads cannot change. This is how covered value
  bytes become dead and reclaimable.
- **Space reclaim.** The shadow estimator counts sampled entries covered by A as
  garbage: index share plus value bytes, like shadowed entries. Frozen runs full
  of covered data are therefore chosen by capped shadow folds. Merges drop covered
  entries but report no "current dropped" entries, because covered keys are
  already absent from the leaves.
- **RT collection** works per persisted RT record r (§1), and only in a run
  rewrite whose inputs include the run holding r. r is collected when all of
  these hold:
  - `gc_grace != 0` and `r.version` is below the durable GC cutoff
    (`gc_collect_before`, which point tombstones also use).
  - `r.seq <= S_A` and `r.seq <= covered_seq` of the last committed flush. So
    the memtable no longer holds any entry that r covers: entries written before
    r were flushed, and every later write under r was rejected at apply. A flush
    that ran before r was accounted wrote its covered entries into a run whose
    `rt_seq_floor` is below `r.seq`. The next condition catches that run.
    "Committed" means the flush has also finished removing its snapshot from the
    memtable (its last `flush_commit_step`), not just published its manifest.
    Tail replay at open skips point records covered by any RT record it replays,
    including records at or below `covered_seq` that are only filters, so a
    covered entry whose segment is replayed after r was collected stays gone.
  - Every run outside the rewrite whose point key range intersects r's span has
    `rt_seq_floor >= r.seq`. The rewrite's own output filters with r (the
    previous condition), so it qualifies too. Together this means no run can
    still hold an entry r covers.

    `rt_seq_floor` is a new v6 field (index trailer): every RT with
    `seq <= rt_seq_floor` was in the filter set the build used. That is the
    minimum of `S_A` and the highest persisted RT seq at plan time. v5 runs count
    as floor 0.

  The rewrite's output omits r. r lives in no other run, so its removal is part
  of that one manifest commit and a reopen cannot bring it back. At commit, F and
  A (and an in-flight fold's T, keeping its cursor) are recomputed over r's span
  from the remaining records. Leaves and counts do
  not change, because nothing r covered is left. If the commit fails or
  conflicts, r stays. The commit re-checks the third condition against the runs
  then outside the rewrite and conflicts if a run committed since the plan breaks
  it. The engine recomputes F; `CompactRunsStats::collected_range_tombstones`
  hands the collected records to the replica layer for A, and
  `CompactRunsStats::covered_dropped` counts the covered entries a rewrite
  dropped.

  Collection is safe for the same reasons as point-tombstone GC. The admission
  floor rejects live writes below the cutoff, so nothing older than r can be
  stored later. And the liveness lease wipes any replica that was away long
  enough to miss both r and its collection.
- **Dominated records** (0475). A queue that deletes `[base, cursor)` with a
  rising cursor leaves one record per delete, and every empty range behind its
  head gets a new same-span slice per delete. F holds one fragment per range,
  but without `gc_grace` the records are kept forever, in RAM, in every flush and
  in every merge. So a flush or run rewrite also drops a record r from its
  output when another record r' **of the same output** has:
  - `r'.version > r.version` and a span that contains r's span, and
  - `r.seq <= S_A` and `r'.seq <= S_A` (S_A as of the plan; it only grows).

  Nothing observable changes:
  - *Durability.* The commit that removes r also persists r', in the same run.
    A crash before it keeps both (in the inputs or the replayed tail). After it,
    r's value-log record can only replay at or below `covered_seq`, where it is
    a replay filter (above) whose cover lies inside r''s.
  - *F.* r' covers r's span at a higher version, so the overlay without r is the
    same canonical set and F is not recomputed. A dropped r' is itself contained
    in a kept record of higher version, so every dropped record is.
  - *A, T and filter sets.* Both seqs are at or below S_A. So the overlay of the
    records through any seq `>= S_A` is unchanged: A, seeds
    (`range_tombstones_through`), fold targets (which read only records above
    `S_A`) and the flush and merge filter sets. `rt_seq_floor` keeps its meaning
    for the collection test above, because a later build's filter set holds r',
    which filters everything r would.
  - *Later removal of r'.* A collection of r' needs `r'.version` below the
    cutoff, so r was collectible by version too and covered only entries r'
    covers, of which no run holds any. A cleanup clip removes only a piece that
    F covers at exactly the piece version, `>= r'.version > r.version`, so it
    would have clipped r's part as well.
  - *Replicas and tools.* The RT exchange, backups and the leaves depend on F
    only, and `valsd dump` overlays the remaining records. Two replicas that
    dropped different records still hold the same F.

  The rule is kept narrow. The unflushed tail is never pruned, and records in
  other runs never count as dominators: the run holding them may be rewritten
  later without them. Equal versions never dominate each other (a clipped
  record's pieces share seq and version but are disjoint). The check costs
  O(n log n) per output for its n records: an antichain of maximal spans in
  descending version order. Async flushes and merges run it in the off-core
  builder. Each flush then keeps one record per delete series per range (the
  latest), merges collapse the survivors across runs, and
  `vals_rt_dominated_dropped_total` counts the drops.
- **Winner directory and blind append** (`latency` profile). Directory lookups
  apply the cover test, and the blind path tests cover before appending (§3).
  Merges that drop covered entries refresh the directory through the existing
  post-commit path.

## 8. Formats

**Wire** (`formats.md` §1):

| Item | Encoding |
|---|---|
| Hello feature bit 3 `RANGE_TOMBSTONES` | Advertised by RT-capable peers. It gates every item below. |
| Request `RangeDelete` = 21 | `[u64 epoch][u32 zone][u8 sync][version][u32 count]` then `([opt<bytes> lo][opt<bytes> hi]) × count`, with `count <= 4096`. Reply `Batch` (one `applied` per slice) or a whole-request `StaleEpoch`/`Busy`/`Err`. |
| Request `SyncRangeTombstones` = 22 | `[u32 zone][opt<u32> requester][u64 digest][opt<bytes> after]` |
| Response `RangeTombstones` = 20 | `[u8 same][u8 more][u32 count]` then `rt-entry × count` |
| `rt-entry` | `[opt<bytes> lo][opt<bytes> hi][version]` |
| `Scan` reply trailer | Written only when bit 3 is negotiated: `[u32 count] rt-entry × count` then `[opt<bytes> resume]`. Decoders treat a missing trailer as no fragments and no resume key. |

**Engine:**

- Value-log record kind `2 = RangeDelete` (§1).
- Run format v6: block-index `max_hlc`, an index trailer with `rt_seq_floor` and
  the RT block's offset/length (the 80-byte footer keeps its v5 layout and only
  its version changes, so every reader still finds it at the same place), and the
  RT block itself. The block is CRC-checked and verified after encoding like 0384
  data blocks. Built in 0451 (`formats.md` §2.2a).
- The manifest stays v4.

**Tools:**

- *Backup* `zone-<id>.vrec` goes to version 2 with record tag
  `3 = range tombstone [opt<bytes> lo][opt<bytes> hi][version]`.
  - Tag-3 records follow all point records, ordered by `lo` and non-overlapping
    (the merged canonical set).
  - `backup create` fetches every replica's F with `SyncRangeTombstones`
    (requester `None`: unscoped) and overlays them. It omits point records
    covered by the merged set.
  - `backup load` applies the tag-3 records once the whole file has been read
    and its trailer CRC checked, after the point records. A damaged file then
    never deletes data, and the order changes nothing: no point record in the
    file is covered, and the cover rule compares versions, not arrival order.
    `compare` overlays the target's sets the same way and checks them.
  - Version 1 files still load.
- *`valsd dump`* goes to `VDUMP\x02` when the zone holds range tombstones. A
  zone without them keeps the `VDUMP\x01` layout, so existing replayers still
  read it.
  - Before streaming, the dump reads the manifest's runs' RT blocks and the
    uncovered tail's RT records. That set is small and the tail is memtable-sized.
  - It emits that set first, as range records
    `[u32 lo_len|0xFFFF_FFFF][lo][u32 0xFFFF_FFFE][u32 hi_len|0xFFFF_FFFF][hi][u128 version]`.
  - It then skips every value-log record that the set covers. Value-log GC can
    relocate a covered value past its RT, so in-place emission would replay
    wrongly.
  - The remaining records then replay to the same final state in log order.

**Metrics:**

- Fragment state: `vals_range_deletes_applied_total`,
  `vals_range_delete_refused_total{reason}`. As built, there is no
  `vals_range_tombstone_fragments` gauge.
- Fold: the counters `vals_rt_fold_winners_total`, `vals_rt_fold_pages`,
  `vals_rt_fold_retired`, `vals_rt_fold_memtable_removed`,
  `vals_rt_fold_memtable_deferred`, `vals_rt_fold_completions`,
  `vals_rt_fold_prefetch_discards` and `vals_rt_fold_not_ready_busy`. As built,
  the `vals_rt_fold_pending` gauge is not exported (per store,
  `VersionedStore::rt_fold_pending()`).
- Scan: `vals_scan_rt_filtered_entries_total`, `vals_scan_rt_skipped_blocks_total`,
  `vals_scan_rt_block_reads_total`, `vals_scan_rt_index_probes_total` (fence and
  max-tree steps), `vals_scan_rt_resume_pages_total`,
  `vals_rt_max_tree_built_blocks_total` (construction at install, open and first
  fold; never on the scan path).
- Compaction: `vals_rt_covered_dropped_total`, `vals_rt_collected_total`,
  `vals_rt_dominated_dropped_total` (§7, 0475).
- Records: the gauge `vals_engine_rt_records{engine="shard/zone"}`, records held
  in runs and the unflushed tail.
- Anti-entropy: `vals_ae_rt_exchanges_total{result=same|pulled|error}`.

## Validation plan

Kill-the-fix tests, L0, file or memory backend. Each must fail with its fix
reverted:

| 0439 criterion | Test |
|---|---|
| Older hidden, newer wins | `VersionedStore`: put, RT, then read, digest, scan and CAS. Also a newer put inside the span, landing both before and after the RT. Repeated on the coordinator over a local cluster. |
| Straggler loses | Older apply after the RT through `apply_at`, staged apply (RT inserted during the probe await), blind apply, batch and copy. Includes a key the replica never held. Expect `Lost` and no append. |
| Skip boundaries | RT bounds inside run blocks, with older live keys just below `lo` and just at or above `hi` sharing blocks with covered keys, and the last block of a run straddling `hi`. Every live key is returned. Fails if the skip trusts `max_hlc` without the whole-block-inside rule. |
| R > 1 scan, one replica missed the RT | RF 3, the delete reaches two replicas, R = 2 pairs the lagging replica with a holder. No covered key is returned, over many pages (multi-page window). |
| Fold accounting | Overlapping RTs (V1 < V2) arriving before and during a fold, with point winners between V1 and V2 and newer writes interleaved at random fold turns. After the folds, leaves and counts equal a from-scratch seed. Fails if the fold targets F instead of its immutable T. |
| AE converges | Replicas with different compaction states and one that missed the RT. After k passes leaves and F are equal, and pulls go to zero (counted). No covered key is stored anywhere. |
| Crash / reopen | RT in the unsynced tail (lost together with nothing it covered), in the synced tail, in a run, after a merge. Each reopens to the same reads, and to the same leaves after reseed. |
| Split, merge, re-home across an RT boundary | Inherited split inside a fragment, auto-merge of ranges on both sides of an RT, a re-home whose copy and mirror both see RTs (mirror failure poisons the move), cleanup of a released span. |
| `gc_grace` collection | Records and covered entries collected only when all three conditions hold. Includes a partial merge that collects a record while other runs carry other RTs, followed by a reopen: the collected record stays gone and nothing resurrects. A straggler below the cutoff is rejected. |
| Dump / restore round trip | `backup create` → `load` → `compare`, and `valsd dump` replay, on a zone with RTs, overwrites and stragglers. |

**Head-of-queue cost.** One node RF 1 and three nodes RF 3 with R = 2. N ∈ {10k,
200k} entries are written and range-deleted. Once the RT's fold has completed, a
scan with `limit = 10` starts at the span's `lo`. It is measured twice:

- *Before compaction*: entries flushed into v6 runs, merges disabled.
- *After compaction*: forced compaction.

Both N must give the same coordinator page rounds,
`vals_scan_rt_block_reads_total`, `vals_scan_rt_filtered_entries_total` and reply
bytes. `vals_scan_rt_index_probes_total` may grow only logarithmically: at most
a constant number of probes more for 20× the entries. Disabling the block skip
or the max tree must make the N = 200k case fail.

The test runs **cold**: the engine is reopened after the delete, so every tree
comes from open or from the fold, never from an earlier scan. It asserts that
`vals_rt_max_tree_built_blocks_total` does not move during the measured scan,
which the scan path cannot do anyway (§1). That rules out passing by warming
trees with a scan. A second cold case writes and deletes without a reopen and
measures right after the first RT's fold completes.

A separate case covers the moment between the RT and its fold. Covered memtable
entries are filtered, and the covered-work budget caps them per page.

*Result* (`coord::range_delete_scan_cost_tests`, recorded in
`bench/0449-rt-hot-path/FINDINGS.md` §3): after the fold, cold, and after
compaction, N = 10k and N = 200k give the same rounds (one per responder), block
reads, filtered entries (500 per replica before compaction, 0 after) and reply
bytes, with 5 more index probes per replica at 20× the entries. Before the fold
the 200k span is filtered entry by entry over 5 budget-cut pages per replica.

**Space reclaim.** After RT plus compaction, the covered entries' run bytes and
value-log segments are freed. Run index bytes and the allocator's free segments
show it.

**Hot-path neutrality (zero RTs).**

- Local tier first (`validation.md`): the write-path A/B
  (`bench/0420-rangemap-borrow/ab.py`) and the read A/B
  (`bench/0339-read-identity-check/ab.py`), ABBA pairs, comparing shard µs/write
  and µs/read.
- The apply, read and scan paths change, so a box confirmation follows on lab-1
  (`bench/0435-range-residual/ab.py`), one ABBA pair. Pass bar: no regression
  beyond noise (±1% shard µs/op).
- v6 runs keep `max_hlc` resident (8 B per block). `vals_run_fence_resident_bytes`
  must grow by no more than that. Engines with no RTs build no max tree.

*Result* (`bench/0449-rt-hot-path/FINDINGS.md`, pass): lab-1 write +0.23% shard
µs/write (idle-subtracted 0.0%); devbox point reads +0.75%, scans −1.1%, writes
inside the shared box's noise; fence bytes +8 B per block exactly; no max tree built.

## Implementation slices

Each slice is a backlog task, landing in order, and 0439 depends on all of them.
Slice 1 is this note (0443).

1. **Engine core** (0444).
   - Fragment set, record kind 2, replay.
   - Run v6 (`max_hlc`, max tree, RT block, `rt_seq_floor`) and the v6-run
     invariant.
   - Decoder hardening.
   - Cover-aware engine reads and probes, flush and merge filtering with a
     caller-supplied filter set, RT collection, shadow estimator.
   - Engine tests (reopen, skip boundaries, GC, space).
2. **Replica semantics** (0445).
   - `apply_range_delete`, cover tests on every apply path, synthetic tombstones
     on reads and digests.
   - Accounted set and RT fold, cover-filtered seeds, rebuilds, regroups and
     recounts.
   - Replica tests, including fold/write interleavings.
3. **Wire, server and coordinator** (0446).
   - Feature bit 3, `RangeDelete` (fence, mirror, persistence).
   - `Scan` trailer, replica skip-scan and covered-work budget.
   - `delete_range_in*`, coordinator fragment merge.
   - End-to-end tests and head-of-queue counters.
4. **Anti-entropy and lifecycle** (0447). `SyncRangeTombstones` in the sweep,
   re-home copy and mirror, cleanup handoff, and the split, merge, re-home and AE
   tests.
5. **Tools** (0448). Backup VREC v2, `VDUMP\x02`, round-trip tests.
6. **Validation and docs** (0449). Head-of-queue and hot-path A/Bs (local, then
   box), updates to `formats.md`, `algorithms.md`, `zones.md`,
   `backup-restore.md`, `deploy.md` (downgrade) and `getting-started.md`, plus
   the Go client follow-up task (`valsgo`).

## Rejected alternatives

- **Expand into point tombstones** (scan, then delete each key). O(N) writes, and
  the scan cost this task removes stays.
- **RT set in the manifest (VMAN v5).** A previous-release binary would boot such
  an engine without its runs (§1).
- **Digest covered entries, or plant point tombstones for them.** Two replicas
  that compacted different subsets would differ forever, or re-pull covered data
  every pass. The leaves must describe logical state.
- **Hinted handoff for RTs.** It would need a new hint record format. The
  per-pass RT exchange is cheap and complete, which hints are not (bounded store,
  default zone only).
- **An entry kind inside `BatchApply`.** That would add RT parsing and a different
  fence (span ownership) and mirror to the batch hot path. A separate op keeps the
  batch path untouched.
- **Per-run max version only.** A run flushed after a delete mixes old covered keys
  with newer writes elsewhere, so whole-run bounds rarely allow a skip. The bound
  has to be per block.
- **Removing covered memtable entries synchronously at RT apply.** That puts
  O(memtable) work on the apply path. The fold removes them instead, in bounded
  turns. With the RT in place this is safe even during a flush (§4), unlike point
  `delete`'s memtable removal (`FlushInFlight`).
- **Compaction filtering with F instead of A.** Merges could then drop entries
  still present in leaves before the fold reaches them, and the fold could never
  XOR them out.

## Risks to measure

- **AE availability under continuous deletes** (measured in 0465). A store answers AE
  `Busy` while a fold is pending, and a queue issuing frequent RTs keeps short
  folds coming. The `vals-bench rtqueue` recipe runs 3 local nodes, a sorted zone
  with RF 3 and a cumulative delete after every append at 200 appends/s. There,
  ~39% of the zone's reconcile slots were lost to a pending fold, against 0% without
  deletes. Each fold cloned every live RT record, about 323 µs per fold. The peers'
  tree requests also ran right after the `RangeDelete` handler, before the
  accounting task did. The inline turn of §4 and the seq-bounded target brought
  fold `Busy` to 0. The 4–13% of slots still lost in that arm come from write
  churn: any leaf change during a tree round trip invalidates the level's scope
  token, and a zone without deletes loses 57–78% of its slots at 1,000 writes/s.
  That limit predates range deletes (proposed 0467).
- **Fragment growth.** With `gc_grace = 0`, deletes of disjoint spans grow F
  without bound up to the cap. `getting-started.md` will recommend cumulative
  `[base, cursor)` deletes.
- **Lagging-replica scans** cost O(N) rounds until the next AE pass. A
  scan-time RT repair is the follow-up if that matters.
- **v5 runs** are never skipped until merges rewrite them as v6. Frozen runs
  rewrite only under space reclaim, which the covered-garbage estimate (§7)
  triggers.

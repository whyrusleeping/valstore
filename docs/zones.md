# Zones

A **zone** is the unit of tenancy and configuration in `vals`: a namespace with
its own replication factor, key ordering, and (future) feature flags. This
document is the design for how zones integrate into the leaderless data path.

> **Status: built — unsorted/hash *and* sorted zones.** The catalog (PD +
> `SetZones` push), per-zone engines (lazy `ZoneStore` providers: Mem/File, and
> SPDK with a device-region allocator + crash-safe superblock), the zone-tagged
> data path (`replicas_for`, `write_in`/`read_in`/…), and per-zone
> anti-entropy are all implemented. The **sorted-zone range partitioner** (the
> `RangeMap` + distributed range scans + PD-managed splits) is built — see
> [Placement](#placement-a-zone-selects-a-partitioner) below and `partition.rs`.
> One v1 simplification: hinted handoff is default-zone only (the hint store has
> no zone tag) — non-default zones heal via read-repair + anti-entropy. Sorted
> zones are served on the sharded (multi-drive / JBOD) path too, sharded
> intra-node by **range token** (key `k` on shard `hash64(range_token(k)) %
> shards`) so a sorted zone scales out per-node like the unsorted default zone;
> the range map remains the *cross-node* scale-out axis. See
> [`ARCHITECTURE.md`](../ARCHITECTURE.md) for the
> as-built system.

## What a zone carries

A zone catalog entry:

```
zone {
  name:      string
  rf:        replication factor
  ordering:  unsorted | sorted
  scope:     placement scope (set of nodes; default = all members)
  flags:     reserved for future per-zone features
}
```

A zone bundles two separable concerns — **storage** and **placement** — and the
clean design comes from treating them separately.

## Storage: a zone is a per-node engine

A `vals_store::Engine` is opened with **one** ordering mode and is a single
value-log + index. A node holding both a sorted and an unsorted zone therefore
cannot share one engine between them. So:

> **Each zone is its own `VersionedStore` (engine) on every node that replicates
> it.** Its own value log, its own index of the zone's ordering type.

Consequences:

- **`drop_zone`** is a clean engine teardown (free its segments); no scanning a
  shared store for a zone's keys.
- **Compaction / GC** (including tombstone GC under `gc_grace`) is naturally
  per-zone and isolated — one zone's GC churn doesn't touch another's.
- **Recovery** enumerates the node's zones and opens each engine.
- **Lazy creation.** A node creates a zone's engine on the *first* apply or sweep
  that touches the zone, using the ordering mode from the catalog — not eagerly
  for every zone it could place. This avoids near-empty engines on nodes that own
  vnodes for a zone but haven't received data yet (see Scalability).

The **HLC is node-level and shared across all of a node's zone engines.** The
clock orders writes; LWW only ever compares versions of the same key (same zone),
so a shared monotonic per-node clock is both correct and desirable.

## Placement: a zone selects a partitioner

Placement is governed by the zone's **partitioner**, chosen by its ordering mode.
This is the abstraction that also resolves the "sorted zones don't fit a hash
ring" question — it's contained inside the zone rather than left orthogonal.

```
zone → partitioner → replicas(zone, key)
  unsorted → hash ring:  ring.replicas_in_zone(hash(zone, key), zone.rf)   [built]
  sorted   → range map:  ring.replicas_in_zone(hash(zone, range_token(key)), zone.rf)  [built]
```

The **sorted** partitioner (`partition::RangeMap`) divides the zone's keyspace
into contiguous ranges by a set of **split keys**, and gives each range an
explicit **placement token**. Without the opaque-map capability gate, a newly
created layout uses the inclusive lower bound (`b""` for the first range) as each
token. With PD config `opaque_range_maps=true` (legacy alias
`VALS_PD_OPAQUE_RANGE_MAPS=1`) and every node negotiating V4 support,
the PD searches deterministic opaque tokens for new canonical ranges to balance
replica slots, primary roles, and reported per-node shards. A populated split
still inherits its parent's token. Placement hashes the stored token onto the
*same* global ring as hash zones — only *what* is hashed changes (the range
token, not the key). So every key in a range shares a replica set (co-location →
a range scan touches few nodes), and the partitioner reuses **all** of the ring's
machinery (membership, weighting, scope, per-zone RF, and the consistent-hash
minimal-disruption rebalance) with no second placement structure.

This realizes the original "PD-managed range → replica map" as *range → token →
ring* rather than the PD pinning each range to specific nodes. Membership
changes rebalance ranges automatically through the ring (the scoped
anti-entropy / rebalance path is unchanged — it just hashes the token), with no
PD machinery to reassign ranges on every join/leave. Current boundary-derived
tokens cannot deliberately target an arbitrary replica set. RangeMap V4 now
supports a fixed 21-byte `VOPT` version-1 opaque token and explicit per-range
placement debt, while preserving this same request-time lookup and ring walk.
The PD computes deterministic opaque-token proposals when its capability and
input gates are satisfied. New-zone presplits, explicit canonical splits on V4
maps, and placement-debt re-homes on V4 maps use searched balanced tokens without
the balance-executor gate. The independent PD config `balance_execute=true`
(legacy alias `VALS_PD_BALANCE_EXECUTE=1`) opt-in may
also move existing ranges by load through confirmed re-home, one move globally. See
[the placement decision and rollout](algorithms.md#decision-pd-side-assignment-for-sorted-ranges).
`scope` continues to cover tenancy isolation.

**Growth = metadata-only range splits.** `PlacementDriver::split_zone(zone, at)`
adds a boundary (a raft-replicated, committed read-modify-write of the range map
under an `R` key prefix), and the PD daemon pushes the new map to the data nodes
(`SetRangeMaps`, retry-until-acked). Splitting `[a, c)` at `m` gives both
`[a, m)` and `[m, c)` the parent's token. Its explicit placement-debt state is
preserved on the left and the new right child is marked indebted; a canonical
empty-range split instead creates a debt-free right child. On a V4 map, that child
gets a searched opaque token balanced against the zone's other effective tokens;
a legacy map keeps the lower-bound behavior. The normal inherited split changes no
key's replica set or intra-node partition, so committed data stays readable
immediately and no handoff is required. Coordinators learn boundaries, tokens, and
V4 state via `refresh_range_maps` (folded into `refresh_catalog`) and route point ops + range
scans by them.

This replaces the earlier design that made the new upper range's lower bound
its token. That design depended on the sweep handing off split-orphaned keys,
but AE pull only compares keys that both peers currently own and was therefore
blind to keys held solely by the old owner. The cleanup push was the remaining
path, and its whole-store cursor, 8,192-key pass limit, 256-handoff-RPC limit,
250 ms wall budget, and five-second cadence reduced a large move to a trickle.
Repeated splits could consequently make committed keys appear absent for hours.
Metadata-only splits are safe by construction. An explicit canonical split can
spread an empty range because it has no data to move; use `admin split-zone
<zone> <key> canonical` only before writing that range. The PD refuses the
operation when its latest estimate for the existing parent range reports live
entries. Estimates remain applicable only while membership, zone, and range-map
epochs and topology are unchanged, and expire after three configured stats-poll
intervals. Missing or stale estimates remain permissive so new-zone setup does
not depend on stats delivery; this guardrail is not proof that a range is empty
and does not replace confirmed re-home.

**Confirmed re-home.** A populated inherited range moves in six resumable steps.
First the PD prepares it by setting `pending_token` to a searched balanced opaque
token on V4 maps, or to the range lower bound on legacy maps, and pushes that
epoch until every data node acknowledges it. Current owners then
serve all reads as before and mirror every versioned put, delete, CAS, and batch
write to each pending owner; client routing and write-tier evaluation remain
based on the current set. A successful client acknowledgement implies that every
pending owner also accepted the version. Pending-owner serving saturation is
shed as `Busy` before its bounded staged-apply gate becomes an RPC-deadline queue;
an initial `Busy` or stale-map refusal returns a retryable client result without
poisoning the move because it positively did not accept the mutation. An RPC
timeout is uncertain, so the current owner retries the same LWW-idempotent version
twice. Only a later positive apply acknowledgement resolves that uncertainty; a
later `Busy` or stale refusal says nothing about whether the timed-out attempt
applied, so it remains fail-closed. Retry exhaustion, any non-applied result after
a timeout, unreachable transport, remote error, or version rejection returns a
retryable write error and poisons the re-home for PD abort. Thus no unresolved
target failure permits cutover and cutover can never strand an acknowledged mutation.
Pending owners independently pull the exact `[lo, hi)`
span from a current owner, preserving versions and a resume cursor. Cutover is
allowed only after every node acknowledged prepare, every pending owner reported
copy completion at that epoch or newer, and every current owner positively
reported zero mirror failures since prepare. The PD then commits the pending
token and clears it. Existing confirmed-handoff cleanup releases the old copies;
its pace affects only space reclamation after cutover. Because the new owners
already hold every copied version, the former owner confirms them with one
batched digest probe per owner and page rather than one apply per key and owner
(0478, [algorithms](algorithms.md)), so it releases up to a full 8,192-key page
per sweep pass.

A mirror error, unreachable owner, or missing required-owner report past
PD config `rehome_report_deadline_ms` (legacy `VALS_PD_REHOME_REPORT_DEADLINE_MS`,
default 60,000 ms) aborts the move rather
than weakening write availability: the PD clears `pending_token` and
keeps the old set authoritative. Partial pending copies are harmless
LWW-idempotent over-replication. While pending is set, sweep placement and
cleanup treat the current and pending replica sets (and both intra-node token
shards) as owners, so cleanup cannot erode the candidate copy. After abort or
cutover, ordinary confirmed-handoff cleanup releases the obsolete copies. Reads therefore
never have a gap: they use the complete old set until all prerequisites are
confirmed, then the complete new set. While any range in a map is moving, every
split, canonical split, and merge on that map is refused so no structural change
can alter copied spans or range indexes under the worker.

This dedicated range copy does not inherit the store-wide cleanup cursor, key/RPC
limits, 250 ms wall budget, or five-second sweep cadence. Anti-entropy is not a
fallback for failed placement moves: `VersionedStore::scoped_leaf_digests`
includes only keys both peers currently own, and the pull skips every key in a
differing bucket that the peer does not own too (0515), so a key retained by a
former owner is invisible to every AE pull. Only push-side mirroring/copy can make that key
visible at the new placement.

The write path fences stale coordinators after cutover. For sorted zones, a node
accepts `Apply` and `BatchApply` only when its own mirrored map says the receiving
node and shard are either a committed owner or a pending re-home target for every
key. Otherwise it replies `StaleEpoch`, a retryable refresh signal handled by the
current Rust and Go clients. Reads are fenced the same way (0476): a sorted-zone
`Read`, `Digest` or `MixedBatchRead` whose keys, or a `Scan` whose span, meet a
range the receiving node and shard neither own nor are a pending target for gets
`StaleEpoch`. Without it a coordinator that only reads never refreshed its map
and kept reading a former owner's leftover copy, which misses later writes and
range deletes, until cleanup forgot it. A sorted zone with no explicit range map
is fenced, for reads and writes, as the single range of `RangeMap::single()`, the
map coordinators route it by; only a replica that has adopted no membership at all
(a standalone server) skips the ownership check. A read stamped with the inspection epoch
`u64::MAX` (`wire::INSPECT_EPOCH`) skips this fence, so tools and tests can list
a shard's physical contents, strays included. A write racing the sub-second prepare, cutover, split, or ring
push fanout can bounce once until the receiving node adopts the push; refresh and
retry self-heals it. The ownership fence is unconditional. A batch is checked
again after its apply and re-home mirror step. If a range-map push, ring push or
catalog push landed in between, the full fence runs again. If none did, the
pre-apply verdict stands. Each shard caches per-range ownership until the next
such push.

Opaque placement has a separate compatibility gate. A fresh endpoint
handshake/beacon must acknowledge feature bit 0, the current range-map epoch must
be acknowledged everywhere, and the operator must set `VALS_PD_OPAQUE_RANGE_MAPS=1`
only after all stateless coordinators are upgraded. Readiness is certified against
one exact membership, catalog, and range-map epoch tuple, so any control-plane
advance fails closed until the next preflight. Presplit token selection waits for
prior control mutations to settle and for published and committed membership to
match. The setting defaults off. Once ready, new canonical presplits emit V4 maps,
and canonical splits or automatic debt re-homes on V4 maps choose balanced opaque
tokens. A queued opaque choice downgrades
to legacy placement if readiness is lost before proposal; a presplit also downgrades
if its membership epoch changes after token selection. A second gate,
`VALS_PD_BALANCE_EXECUTE` (also default off), permits the PD to upgrade an existing
legacy sorted map to V4 after the full endpoint preflight (a routing-neutral metadata
change), wait for its acknowledged statistics epoch, and prepare the load planner's
exact opaque token. Existing legacy maps keep lower-bound placement until that
executor upgrades and converges them; old/mixed nodes continue receiving only V1–V3
canonical/inherited maps.

Balance weights are exact current logical winner-key counts. Retained tombstones count
until GC retires them; overwrite, flush, run, and compaction history do not add weight.
Each sorted store projects a volatile count vector from the existing cooperative winner
seed and then maintains exact absent-to-present and retirement transitions. Unsorted
and default-zone counts are unavailable while any anti-entropy bucket is dirty. Reopen
and every split or re-home map epoch discard the vector and reproject it in bounded seed
turns. With `ae_seed_isolation=1` (0374) a seed already in flight on a sorted store is not
restarted by a split/merge generation bump: it keeps counting against the map it started
with, and at publication its exact counts are re-partitioned onto the current map through
the range-bounded split recount (or the exactly tracked live counts are kept when
present). `ae_seed_generation_carries` counts such seeds and `ae_seed_generation_resets`
the remaining from-scratch restarts. Merges and forgets do not restart it either (0379):
the seed pins its starting runs ([`docs/backpressure.md`](backpressure.md)).

A re-home that changes a range's replica set changes the leaf group of every key in that
range, including on a store whose node keeps the range and only sees its peer change.
Leaves are XORs per (sorted replica set, bucket), so moving a range from {A, B} to {A, C}
moves A's keys from group {A, B} to group {A, C}; keeping A's leaves unchanged would leave
them in the old group, and A would disagree with both B and C. A store that holds no key
in any changed span keeps its leaves (0322). Since 0377 (`ae_span_regroup=1`) a seeded
store that holds keys there keeps its leaves too and starts a span regroup instead of a
whole-engine refold: bucket, live and range counts do not depend on the group, so only
each changed-span winner's digest moves, `leaves[old][b] ^= d; leaves[new][b] ^= d`. The
shard's accounting task scans the changed spans in bounded winner-version pages (1024
winners or 1 ms per turn, prefetched under the read gate and validated by storage
generation like split recounts) under the seed duty budget. A prefetched page whose block
reads fail is re-read synchronously; only a synchronous page error invalidates the leaves.
Until a span's cursor passes a key, writes, retires and forgets of that key still update
the old placement's group, so each page moves exactly the winners present when it is read.
While a regroup is in flight, tree requests and reconciles of that store answer Busy when
`ae_seed_isolation=1` and the shard's seed driver is attached (otherwise the caller drives
the regroup inline before answering), and the synchronous control-plane SyncTree answers
Busy. Dirty-bucket rebuilds wait for the regroup: a rebuild reads pages across awaits, and
the regroup cursor could advance between a rebuild page and its publication, leaving that
page's keys in the group they were routed to when read. Dirty buckets are skipped by the
regroup and rebuilt afterwards with the final placement.

A placement change that arrives while a regroup is in flight composes with it: the
regroup's current mixed routing (old placement for keys not yet scanned, current placement
otherwise) is frozen as the new old placement, the unscanned parts of its spans are unioned
with the newly changed spans, and the cursors restart. Keys outside that union already sit
in the group that both the frozen routing and the new placement give them. Up to 8
compositions chain; the next falls back to the whole-engine invalidation
(`ae_regroup_depth_fallbacks`). A change that is placement-equivalent or touches only spans
the store holds no key in leaves the in-flight regroup as it is. A change during a full
seed, a ring/catalog/config change, a synchronous page error, `ae_span_regroup=0`, or any
other leaf invalidation still falls back to the whole-engine invalidation
(`ae_regroup_fallbacks`). `ae_seed_placement_regroups` (including compositions,
`ae_regroup_compositions`), `ae_regroup_completions` and the unchanged
`ae_seed_placement_invalidations` / `ae_seed_starts` make a rehome's peer reseeds observable. Since 0379 the refold completes
under merge churn. An unknown-old append can also invalidate an exact vector after the prior sweep
has quiesced; an unready `RangeStatsV3` poll therefore schedules another sweep, with
notifications coalesced by the sweep driver. The poll itself never performs a serving
scan. `logical_accounting_reseed_requests` records this repair edge; seed fold,
projection, journal replay, cleanup, RAM-shed, and dirty-bucket metrics distinguish the
work that follows. Persistence is intentionally omitted: it would need atomic
data/map/GC updates, would create a second recovery authority, and would not remove the
winner seed already required by anti-entropy.

`RangeStatsV3` identifies the accounting epoch and readiness. Only V3 endpoints whose
response epoch and ready accounting epoch both equal the current range-map epoch may
enter the balance planner. V1/V2 fallback remains usable for monitoring and split or
re-home progress, but cannot authorize a balance move. The planner still requires every
endpoint, range, current owner, and shard, exact owner agreement, and one topology and
epoch snapshot; `VALS_PD_BALANCE_EXECUTE` remains default-off.

Automatic re-home is controlled by PD config `rehome_enabled` (legacy alias
`VALS_PD_REHOME_ENABLED`) and ships **on**; set it to `false` for
emergency/operator rollback. `max_concurrent_rehomes` (legacy alias
`VALS_PD_MAX_CONCURRENT_REHOMES`) defaults to 1.
See [configuration and migration policy](configuration.md) for all PD keys,
strict validation, environment precedence and mixed-version rollback. Remaining
`VALS_PD_*` references below name the deprecated aliases for those keys. Debt re-home only starts when all sorted ranges have current-epoch
entry counts and the measured node or engine load skew exceeds `balance_high_pct`
(default 120). Since 0485 that load counts, per range, only reports from its current
owner shards, and only `RangeStatsV3` reports whose accounting is ready at the
response epoch (V1/V2 reports carry no readiness and still count). A former owner's
not-yet-forgotten copy is not load on the current owner, and an unready shard's
physical estimate still includes range-deleted entries awaiting a fold: on the 0477
ring arm these transient estimates, not owner load, triggered most debt re-homes
(task 0485's analysis of 2026-10-19: unready replies were 2,223 of 17,256 endpoint
replies and exceeded the lowest replica of the same shard by more than 0.5M entries
in 1,651 cases, so a different engine looked hot about every 10 s). It only moves debt ranges touching the hottest over-threshold
node or engine, largest first, with at most one active move per zone.
`pd_node_load_skew_pct` and `pd_engine_load_skew_pct` show current skew;
`pd_rehome_debt_deferred_balanced` counts polls with eligible debt and a free
slot where skew is below the threshold, range counts are incomplete, or no
debt range touches the hottest over-threshold node or engine.
`pd_unrehomed_ranges` counts inherited placement tokens structurally, not load
debt or a convergence gate. `pd_rehomes_active` and the `pd_rehomes_started`,
`pd_rehomes_completed`, and `pd_rehomes_aborted` counters show moves, with
prepare/commit/abort events logged and aborts attributed by cause
(`pd_rehomes_aborted_report_gap`, `pd_rehomes_aborted_mirror_failure`,
`pd_rehomes_aborted_copy_stall`). Auto-split creates useful structure without
itself redistributing load.

**Recovery hold (0374).** The leader PD stops *starting* re-homes while a data
node recovers from a restart.

- **What is held:** new debt re-homes and balance proposals.
- **What is never held:** copy-complete commits, aborts and splits.

Each range-stats poll counts a node as recovering in any of three cases:

- **Unreachable.** At least one of its shard endpoints answered no
  range-stats request version.
- **Recovery seed.** A shard's `RangeStatsV3` reply ends with the optional
  recovery-seed byte set to 1.
  - The byte is 1 while the shard has an engine of a configured zone that was
    reopened with runs at process start (or lazy reopen) and has not yet
    published its first anti-entropy leaf seed.
  - Reseeds from placement changes are not reported. This covers a re-home's
    own prepare or commit, and split-recount fallbacks. If they were reported,
    every re-home would hold itself.
  - Older decoders ignore the trailing byte. A reply without it never holds.
- **Restarting (0425).** The PD beacon saw the node restart (its ring or
  zone-catalog epoch went backwards). The node counts as recovering until a
  poll has seen its recovery seed and a later poll no longer reports it, or
  until max(30 s, 2 range-stats polls) pass with no recovery seed reported.
  A poll that finds the node unreachable restarts that grace. This covers the
  seconds after a restarted node serves again but before it reports seeding,
  when it is neither down nor seeding. A new leader starts with no restart
  watches.

The hold is bounded per *episode*:

- An episode starts at the first recovering poll.
- It ends only after `RECOVERY_CLEAN_INTERVAL` (60 s) of consecutive clean
  polls.
- Within an episode, the time spent holding accumulates. When it reaches
  `rehome_recovery_defer_ms` (default 1,800,000 ms), starts resume even if the
  node is still recovering, until the episode ends.
- A flapping node therefore cannot extend the hold.
- `rehome_recovery_defer_ms=0` disables the hold.
- A PD that loses leadership drops its episode.
- **Release lag.** The hold lifts at the first range-stats poll that finds no
  node recovering. It therefore trails the end of the last recovery seed by up
  to one poll interval (`range_split_poll_ms`, default 5 s) plus the poll's own
  fan-out. FULL11 saw about 12 s (≈2 polls) between the node's
  `vals_ae_seed_active` reaching 0 and `pd_rehome_recovery_hold` reaching 0,
  part of it scrape resolution. The lag only ever holds longer, never shorter.

Why hold starts at all: a re-home changes placement, and each placement change
makes the owners of the zone's engines reseed. In RECOV01, a commit during a
node's post-crash seeding made both healthy peers reseed for about 5.5 minutes.

Metrics:

- `pd_recovery_down_nodes`, `pd_recovery_seeding_nodes`,
  `pd_recovery_restarting_nodes` (restart watches that are neither down nor
  seeding) and `pd_rehome_recovery_hold` show the state. A hold with no down
  and no seeding node is attributed by `pd_recovery_restarting_nodes`.
- `pd_rehome_starts_deferred_recovery` counts held polls.

The report deadline (`VALS_PD_REHOME_REPORT_DEADLINE_MS`, default 60,000 ms) is
progress-aware: pending owners export a monotonic copied-entry count in their
range stats, and the PD resets an owner's deadline only when that count
advances (or the owner reports copy completion at the prepare epoch), while
current owners need only keep reporting. Report and copy deadlines do not begin
until every required owner shard has confirmed the pending map epoch. If a
beacon detects a restart and invalidates that confirmation, deadlines pause and
restart only after the map is delivered again; `pd_rehomes_delivery_stalled`
distinguishes this state from a genuine copy stall. A copy that spans many
deadline periods therefore never aborts while it moves; a silent required owner
aborts as a report gap and a pending owner that reports without progressing
aborts as a copy stall. Mirror-failure aborts remain immediate, and stale-epoch
stats replies still feed liveness and progress tracking; map acknowledgement,
mirror, copy-completion, and cutover preconditions keep their epoch checks.
There is deliberately no delivery timeout: an unreachable required endpoint can
leave the move pending indefinitely. Restore that shard (with V4 support when the
map is V4), or, when the whole node is permanently being decommissioned and the
zone's RF/scope remains satisfiable, use the existing
`valsd admin -s <socket> remove-node <id>` membership operation so the PD derives
a new required-owner set. There is no manual `abort-rehome` admin command.

With `gc_grace > 0` the copier also checks, page by page, the target's own
entries in the page's window. A live value that the source lacks, and that is
older than half the grace, may be a former owner's leftover whose delete the
source already collected. Cutting over would make the target serve it, so the
page is not counted as copied and the copy stays incomplete until the key is
gone (`vals_rehome_copy_target_only_blocks`). Younger target-only values are
in-flight mirrors and pass. The check is keys-only: it reads the target's index,
not its values. With RF > 1, an old acknowledged write that the source owner
missed also holds the copy, until anti-entropy repairs the source. Only a copy
that no current owner holds holds it for good. A held copy is rechecked every
5 s. Ring changes (`add-node`, `remove-node`) are checked separately, before
the PD commits them ([`backup-restore.md`](backup-restore.md#consistency-contract)).

Aborted-then-retried copies resume instead of re-scanning. The copy cursor is
keyed by the range and pending token and survives the abort's epoch bump; on
re-prepare the copier revalidates it against its pinned source's
unmirrored-write mark — a per-range value each owner advances for exactly the
writes its mirror path declines to mirror because the range has no pending
token, recorded against the same range-map snapshot as the decline and before
the write can be acknowledged; anti-entropy heals applied by an owner also
invalidate its marks. An unchanged mark proves no acked write went
unmirrored during the gap, so skipping the already-copied prefix cannot strand
a mutation; a moved mark (or a changed source, or any local cleanup forget in
the zone since the cursor was cut) forces a restart from the range start. The
target also registers a copy hold (`VALS_REHOME_COPY_HOLD_MS`, default 60 s
past the last tick the range was pending) so its own over-replication cleanup
does not eat the partial copy while an abort is being retried; expired holds
release normally and a forget-generation check invalidates any cursor the
cleanup raced. Copy pace remains one `VALS_REHOME_COPY_PAGE_ENTRIES` (4096)
page per 100 ms tick per range, tunable via that variable and
`VALS_REHOME_COPY_PAGES_PER_TICK`; `rehome_copy_pages`, `rehome_copy_resumes`,
and `rehome_copy_restarts` count the copier's work. In a sorted zone with range
tombstones each page's fragments are applied before its entries, and the next page
starts at the page's resume key when it has one (`docs/range-deletes.md` §5).

The protocol was validated on hardware on 2026-08-08 (task 0151: 3-node RF=2
SPDK cluster, 20M-key
sorted zone grown from a single range: 46 inherited ranges converged to zero
unrehomed with no operator intervention, exact key counts and zero consistency
errors through a SIGKILL'd owner mid-copy and a PD restart mid-move). The
client-parity gate is satisfied
by task 0144: the current Go client decodes map-v2/v3 replies and routes by committed
range tokens; older Go clients must be upgraded. Data-path fencing for stale
coordinators landed in task 0150.

Task 0168 re-ran that hardware churn protocol on 2026-08-10, after the task
0155 progress-aware deadline, validated-resume, and abort-attribution changes.
Safety and convergence passed again:
35 ranges reached zero unrehomed, the count was exactly 20M, and the churn oracle
saw no consistency errors through a 164-second node outage and PD restart. The
new counters showed 0 report-gap and 0 copy-stall aborts, but exposed a remaining
**default-on blocker**: healthy sustained writes caused mirror-failure aborts on
23 of 32 prepares (39 of 73 overall including the forced outage), versus 32 of
77 unattributed aborts in the earlier run. Retry copies did resume 16 times but
conservatively restarted 47 times. At the task-0168 checkpoint,
`VALS_PD_REHOME_ENABLED` therefore remained default-off pending root cause and a
repeat churn arm that confined aborts to
genuine outages. The task 0120 balance planner was likewise gated on that result:
it would reuse this protocol and could not execute while ordinary healthy writes
still poisoned most moves.

Task 0176 isolated and removed that blocker (hardware arm of 2026-08-11). The
old mirror transport
collapsed its two-second RPC deadline with unreachable/remote failures, and the
target queued `BatchApply` behind a full eight-entry staged-read gate; the first
deadline permanently poisoned the range even if a later retry accepted the same
version. The target now sheds a full gate as explicit retryable `Busy`, mirror
acks preserve concrete applied/busy/stale/timeout/remote/unreachable/rejected
classes, and uncertain deadlines receive two bounded identical-version retries.
Only a positive apply ack resolves a timeout; exhaustion and genuine target
failure remain fail-closed. In the comparable 20M-key hardware arm, healthy
ordinary-churn aborts fell from 23/32 to **0/11** while all 7 observed mirror
deadlines resolved positively; a subsequent 120-second SIGKILL outage produced
10/10 mirror-failure aborts, zero consistency errors, exact 20M count, and quiet
convergence to zero unrehomed ranges. Automatic re-home therefore defaults on,
with `VALS_PD_REHOME_ENABLED=0` retained as rollback.

Post-cutover cleanup cooperatively
checkpoints long digest and staged-scan reads at bounded device-read windows
before forgetting obsolete copies. A
checkpoint temporarily leaves the shard read gate, lets the forget quiesce
drain, then re-enters and rejects staged work if its run-set generation changed.
Cleanup-specific quiesce attempts cap at 250 ms and failed attempts back off for
eight times their wait, rather than repeatedly imposing the former 0.1–2 s
read stalls. The fix still needs a hardware tail-latency and release-rate
re-check in the next lab session.

**Auto-split is on by default (task 0108).** A fresh sorted zone starts as a
single range whose token `""` pins it to one RF-scoped replica set and one
intra-node partition — O(1) placement regardless of cluster size. That silently
concentrates load/space at scale (the single-range footgun). So the PD leader now
runs a size-triggered auto-split loop by default: it polls each node's per-range
live-entry estimate (`Request::RangeStats`, a cheap RAM read) and, when a range
exceeds `VALS_PD_RANGE_SPLIT_THRESHOLD` (default ~1M entries), enqueues a
`split_zone` at a node-reported midpoint — refining its structural range map
without moving existing data. Both populated children inherit the parent token,
so this split alone cannot relieve a hot replica set. The re-home loop waits for
separate child estimates and then moves one indebted child, using balanced opaque
placement when the map is V4; see
[the assignment design](algorithms.md#load-aware-assignment-algorithm).
Each poll splits every over-threshold range of a zone, largest first, up to
`VALS_PD_RANGE_SPLITS_PER_POLL` (default 16) per zone (task 0319). Distinct ranges
split at their own validated midpoints, so they are independent. Splitting only one
range per zone per poll capped a two-zone cluster at about 0.4 splits/s. At
about 1 M KV/s, LONG01 needs 1–1.5 splits/s, so ranges grew to about 8 M entries and
never converged after load. The loop stays anti-thrash guarded (stale-epoch replies
skipped, idempotent enqueue, split/merge hysteresis) and fully reversible: set
the threshold to `0` to disable. Cadence is `VALS_PD_RANGE_SPLIT_POLL_MS`
(default 5000).

The same poll can split a small but hot range. Set
`VALS_PD_RANGE_LOAD_SPLIT_OPS` to a per-replica read+write operations/second
threshold (`0`, the default, disables the load trigger). Data nodes count point
reads, point writes, each write in a batch, and scans at their routed start in
memory; `RangeStatsV2` samples those counters over the interval since the prior
V2 poll without device I/O. The PD conservatively uses the highest replica
report for a range and applies the same midpoint validation and per-poll split cap
as size splitting. Mixed-version clusters fall back to legacy
statistics, which report no load and therefore retain size-only splitting until
the data node is upgraded.

Two default-on **guardrails** back this up and make the remaining manual cases
loud rather than silent:

- **Presplit at creation:** `create-zone <name> <rf> sorted presplit=K` (K in
  2..=256) installs K canonical ranges while the zone is empty. Numeric presplit
  uses single-byte boundaries spread over `0x00..0xFF`, so it only distributes a
  keyspace that is uniform in its first byte (for example, hashes). It is
  effectively a no-op for normal prefixed keyspaces such as
  `app.bsky.feed.post/…` or `did:plc:…`: all keys still land in one range. For a
  real keyspace, use `presplit=key1,key2,...` with 1..=255 non-empty, strictly
  increasing real-key boundaries chosen for the expected distribution. These
  explicit boundaries are also canonical and spread writes immediately. With the
  opaque capability ready, their replica slots, primaries, and known shard choices
  are balanced by searched tokens; otherwise their tokens remain lower bounds.
  `pd_canonical_placements_balanced` and `pd_canonical_placements_fallback` count
  those choices. A purely numeric boundary cannot use this shorthand because it is interpreted as
  K; add that boundary with `split-zone <zone> <key> canonical` while the range is
  empty. Never canonical-split a populated range without the confirmed re-home
  protocol.
- **Loud concentration warning + fill/skew metrics:** the same PD poll (active
  whenever auto-split *or* the warn guardrail is enabled) logs throttled warnings
  for any sorted zone still on one range whose entries exceed
  `VALS_PD_SINGLE_RANGE_WARN_ENTRIES` (default ~1M; `0` disables), and for a
  multi-range zone when its largest range exceeds that entry floor and holds at
  least `VALS_PD_RANGE_CONCENTRATION_WARN_PCT` of all zone entries (default 80;
  `0` disables the concentration check). It publishes PD gauges —
  `pd_sorted_ranges`, `pd_single_range_sorted_zones`, `pd_max_range_entries`,
  `pd_oversized_single_range_zones`, `pd_concentrated_range_zones`,
  `pd_range_skew_pct` (worst multi-range zone's max/mean fill ×100), and
  `pd_unrehomed_ranges` — plus the `pd_single_range_warnings` and
  `pd_range_concentration_warnings` counters. The concentration gauge catches
  degenerate multi-range maps whose empty ranges make the range-count guardrail
  look healthy.

**Capacity balance planner and opt-in executor.** On each fresh range-stat poll the
PD evaluates one global sorted-range move. Planning alone does not mutate control
state. It includes every sorted
replica and routed shard plus fixed default/unsorted shard load; advertised member
weights set node targets and each node target is divided equally among its freshly,
consistently reported shards. V4 map support, no active re-home, exact RF/scope,
all-current-owner estimates, complete shard topology, the operator gate, all endpoint
feature acknowledgements, and all range-map acknowledgements are required. Planner
input comes only from successful V2 replies whose epoch exactly equals the committed
snapshot epoch; every expected endpoint and shard, every applicable sorted range
(including zero-entry ranges), and zero-load hash shards must be represented coherently.
Scoped-zone keys are required from every endpoint of each in-scope member but are
legitimately absent on out-of-scope members. Range weights use the same maximum across
these complete fresh V2 reports as `pd_node_load_skew_pct`; every current owner must
agree with that maximum. Fallback, older, newer, missing, inconsistent, or legacy input
is blocked rather than estimated.

Sorted-range `entries` are currently storage estimates, not logical consistency proofs.
The store skips runs whose persisted key extrema are disjoint from the range, then
proportionally allocates each intersecting run's entries by its selected index-block
fraction and adds exact in-range memtable entries. Runs that span sparsely populated key
islands can therefore give different estimates for replicas with equal logical winners
but different flush or compaction layouts. Exact owner equality deliberately turns that
case into blocked planner input; there is no tolerance or substitution rule. Correcting
this remaining limitation requires separately designed, map-epoch-scoped exact
accounting rather than per-poll scans or weaker planner validation.

The memtable part sums entry counts of whole in-range index blocks, searches only the
two boundary blocks and decodes keys only in the median's block, so a poll no longer
materializes the in-range keys and allocates only the returned midpoint (0415).
When exact logical counts are ready, a range reports them and computes only the
midpoint, which reads the memtable only if no run has two index blocks in the range.

PD configuration (all ratios are load/target ×100, where 100 is exactly target):

- `balance_high_pct=120` (legacy `VALS_PD_BALANCE_HIGH_PCT`): do not propose
  until either hottest node or hottest engine is above this ratio.
- `balance_low_pct=105` (legacy `VALS_PD_BALANCE_LOW_PCT`): hysteresis
  low-water/cap; must be >=100 and <= high (invalid values are rejected, not
  clamped). Any candidate bringing both hottest ratios to/below it is preferred
  over every constrained candidate; only when none exists may an above-cap
  fallback be emitted, and then only after exact minimum-improvement comparison.
- `balance_min_improvement_pct=5` (legacy `VALS_PD_BALANCE_MIN_IMPROVEMENT_PCT`):
  required reduction, in ratio percentage points, in an overloaded dimension.
- `balance_token_search_limit=4096` (legacy `VALS_PD_BALANCE_TOKEN_SEARCH_LIMIT`):
  global increasing-nonce budget per
  range. Each token is routed once and classified by its exact node/shard result;
  target sets unseen within the budget are exhausted. RF, node distinctness, and
  scope are never relaxed.

`pd_balance_{before,after}_{node,engine}_pct` publish projected normalized ratios;
after is zero if no candidate was evaluated and may retain the best suppressed
candidate for below-improvement diagnosis. `pd_balance_proposed_entries`,
`pd_balance_proposed_copy_entries`, `pd_balance_movable_ranges`,
`pd_balance_blocked_ranges`, `pd_balance_proposal`, and
`pd_balance_token_exhaustions` are poll snapshots and reset on every poll. The
`pd_balance_noop_*` and `pd_balance_token_search_exhausted` counters are cumulative,
with no-ops attributed to capability, blocked input, active re-home, below-high,
below-improvement, no-feasible-plan, or split-first. Proposal logs contain only zone
id, range ordinal/stable digest, node/shard ids, ratios, and volumes—never boundary
or key bytes.

`VALS_PD_BALANCE_EXECUTE=1` enables the separate mutation layer. It is **off by
default**. The layer carries the dry-run's exact fixed-size token and immutable
proposal identity; no target-set assignment database is added. Before prepare, both
the poll and authoritative drive thread require unchanged membership/weights and
member epoch, ring configuration, catalog/RF/scope and zone epoch, selected range-map
epoch and exact map/bounds/current token/debt/pending state, opaque feature/ack
readiness, complete endpoint statistics, and shard topology. The drive thread first
waits until every preceding Raft log entry is applied, then compares the proposal to
the newest published complete endpoint-statistics fingerprint and
shard topology, not merely to proposal-carried fields. That authority generation
continues advancing on every complete poll until the pending map is reflected; an
unresolved prepare result remains in flight rather than being treated as rejection.
Any newer poll, pending control mutation, or exact intent/map mismatch rejects the
durable intent without changing the map. Raft enqueue is not acceptance: a reflected
single-key intent is revalidated before a separate idempotent exact-map conditional
write. The drive thread holds later control commands behind those two phases, and the
executor remains globally blocked until that pending token is committed and reflected. Every
phase changes one durable key; restart safely resumes a reflected pending map or
conditionally clears an orphan intent. One move is allowed
globally (therefore also one per zone),
and ordinary automatic debt re-home is capped to the same global serialization while
the execution gate is active. Commit, abort, PD restart/resume, mirror/copy fencing,
and cleanup are the existing confirmed protocol. Abort clears only pending state and
leaves the old token authoritative.

A split or merge and balance prepare cannot action the same zone in one poll. A
populated split always gives the new child the parent's token and marks inherited
debt; the executor waits for map visibility and a complete child-stat poll before a
later re-home. After commit, abort, or stale rejection it consumes one complete fresh
statistics poll before another prepare. The high/low ratios above remain the
convergence hysteresis.

Execution counters are `pd_balance_exec_prepares`,
`pd_balance_exec_copy_entries` (planned copy entries),
`pd_balance_exec_{commits,aborts,cooldown_polls}`, and
`pd_balance_exec_stale*`, with stale causes split across identity, gate, capability,
live poll generation, membership, catalog, range-map, and topology. Existing `rehome_copy_pages` records
actual copier work and `pd_rehomes_aborted_*` retains protocol abort attribution.
Logs expose only proposal digest, zone/range ordinal, target node/shard ids, and
volumes, never boundary/key bytes.

A deterministic five-node, RF=2 test workload with range weights
900/300/200/100, 250 fixed hash entries, and equal capacities produces 269 for both
the planner's before-node ratio and the independently exported
`pd_node_load_skew_pct`; report order and a simulated planner restart select the
same token and node/shard target. Task 0181's implementation and local debug-mode
gates passed.

Balance execution remains **default-off** after task 0182's destructive lab
hardware A/B. Its reproducible 12-range uneven RF=2 corpus had a measured hottest
node ratio of 132.5% and hottest engine ratio of 207.5%, but ordinary successful
loading left small persistent differences between some current-owner V2 range
estimates (for example 400,018 versus 400,000 entries). The fail-closed planner
therefore reported all 12 ranges blocked, zero movable ranges, no before/after
prediction, and no prepare/copy/commit/abort even with execution opted in. Matched
blocked-executor samples showed no obvious request-path regression, and exhaustive
consistency checks survived a node-service outage and PD restart, but movement,
move-time failure recovery, and hysteresis convergence were necessarily unrun.
Do not interpret zero copy or abort counters as a safety pass. Resolve the
current-owner estimate-coherence blocker and rerun the complete gate before any
default-on reconsideration. That is the whole of the evidence from the
2026-09-07 gate; hash/default-zone placement and
`vnodes` behavior were unchanged.

**Auto-merge (shrink-reclaim) is opt-in (`0` = off by default).** The inverse of
auto-split: when a zone *shrinks*, its range map can stay needlessly fragmented
(many under-full ranges, each pinned to its own replica set). Set
`VALS_PD_RANGE_MERGE_THRESHOLD` to a combined per-pair live-entry count and the
same PD poll will, for each sorted zone, merge the *smallest* adjacent range pair
whose combined entries fall below it — retiring one boundary via `merge_zone`.
Unlike a split, a merge re-points the right range to the left token and therefore
moves data through the cleanup handoff. That path scans the whole store and is
capped at 8,192 keys, 256 handoff RPCs, and 250 ms per pass on the five-second
sweep cadence, so auto-merge does not yet use confirmed re-home and retains this
slow-handoff exposure for a large populated range. Auto-merge is default-**off**
and can also collapse deliberately presplit-empty ranges (`presplit=K`) and
scope/weight-tuned layouts (task 0071), so an operator opts in for a zone they
know has shrunk.
Anti-thrash: one merge per
zone per poll, a zone that split in a poll does not also merge in it, and
the merge ceiling is **clamped to `≤ VALS_PD_RANGE_SPLIT_THRESHOLD / 2`**. That
hysteresis guarantees a freshly-merged pair (combined `< ceiling ≤ split/2`) is
never an immediate re-split candidate — the merged range must roughly double before
it re-splits. When load splitting is enabled, a pair must additionally be below
`VALS_PD_RANGE_LOAD_SPLIT_OPS / 2` combined ops/s before it may merge. Thus neither
size-triggered nor load-triggered split↔merge can oscillate. Each merge
bumps the `pd_range_merges` counter (and each auto-split `pd_range_splits`).

**Covered merges are on by default (`VALS_PD_RANGE_MERGE_COVERED`, 0477).** A
queue or ring buffer writes at a moving tail and range-deletes behind a moving
head. Size splits fire inside the live window, and the deletes then empty every
range behind the head. Without merges these ranges pile up (the 0471 ring arm
reached 8,550 ranges after 2 h), and every cumulative head delete is sliced across
all of them, so range-delete work per trim grows without bound. So the PD merges
an adjacent pair of ranges, independent of `range_merge_threshold`, when both
ranges are *covered empty*:

- the current-epoch statistics report zero requests/s for the range, and
- every current owner (each replica node's owning shard) lists the range in its
  `RangeStatsV3` covered-empty trailer. This optional list follows the optional
  recovery-seeding byte; a sender must include that byte to send the list. Older
  decoders ignore the trailer, and absent lists cannot authorize covered merges.
  A shard lists a range only when its exact
  logical count is zero and its range tombstones cover the range's whole span.
  Entry counts from other shards are ignored: a former owner that has not yet
  forgotten its copy of a re-homed range (cleanup lag, 0480) still counts those
  entries as live, because the deletes after the move went only to the new owners.

Size splits normally use the largest count any shard reports, stray copies
included. Covered-empty ranges are excluded from automatic splits as well as
re-homes: splitting a range whose owners hold no uncovered entries only creates
more debris. Covered merges therefore need no size ceiling. Applying the normal
threshold-merge ceiling here would make former-owner copies block reclamation
until cleanup catches up, even though every current owner reports the range
empty. The all-owner report and zero-request checks are recomputed each poll;
a range that receives new live writes can split normally again.

So an empty range that nothing deleted is never merged by this rule:
presplit-empty ranges of a new zone, or a drained queue partition's head range,
whose span above the head is not covered. A merge must also not move live data,
so a boundary is merged only when no range other than the right one takes its
placement from the boundary (in legacy maps, an inherited split places its
children by an ancestor's lower bound). The right range then either keeps its
placement (an inherited sibling, nothing moves) or re-points to its left
neighbour's owners. In that case it holds only fragments and covered entries, and
the cleanup handoff moves the fragments (`docs/range-deletes.md` §5) and forgets
the rest. All covered merges of a zone in one poll go into a single range-map
update, so they keep pace with any split rate (a ring re-homes a range for about
30 s out of every 45, and neither splits nor merges apply during a re-home). They
run in the same poll as splits of other ranges, because the two never touch the
same ranges. A batch that reaches the queue head while the zone has an active
re-home is dropped, and the next poll recomputes it. A zone with
covered merges takes no threshold merge or balance prepare in that poll. Covered
empty ranges are never re-home candidates, since moving data that is already
deleted is wasted work. Each covered merge bumps `pd_range_covered_merges`. An
operator can still truncate a whole presplit zone with one range delete. Its
ranges then merge, and the zone re-splits by size or load as it refills.

**Range scans** (`Coordinator::scan_in(zone, lo, hi, r, limit)`): the coordinator
asks the range map which ranges `[lo, hi)` overlaps, reads `r` replicas of each
(the `Scan` wire op returns versioned entries *including tombstones*), merges by
last-writer-wins (a tombstone winner hides the key), and concatenates the
per-range key-sorted results in range order — globally sorted, and with a read
quorum (`W + R > RF`) every key reflects the latest committed write.

Replica scan replies are **paged**: a replica fills each reply up to its byte
budget (and an optional requested entry cap) and flags truncation, so a dense
range — one whose whole window would exceed the frame cap — is served in many
bounded replies. The coordinator pages through each range, merging only the
window every queried replica has fully covered per round (the **min-coverage
rule**, see `docs/algorithms.md` §Distributed range scan), which keeps the
paged merge exactly as strong as the unpaged one. A `limit`ed scan passes its
remaining need as the requested cap, so a small `limit` over a huge range pulls
a few entries, not the range.

**Descending scans** (`Coordinator::scan_in_rev(zone, lo, hi, r, limit)`) use
exactly the same half-open `[lo, hi)` bounds and quorum/LWW/point- and
range-tombstone visibility, but return keys largest first. Use natural
big-endian timestamp keys for newest-N without an inverted-time index; use
`limit = Some(1)` to recover the last live key under a prefix. `None` bounds
mean the respective end of the zone. An unsorted zone returns `NotSorted`.

Reverse scans visit range slices newest first and merge only the suffix all
queried replicas covered: the **maximum** page lower-coverage bound, inclusive.
A truncated reply resumes with `hi = resume` (exclusive), without appending a
zero byte. This also advances across empty range-tombstone-covered pages.
`scan_many_in_rev(zone, scans, r, keys_only)` batches independent reverse
queries; each retains its own bounds, limit and coverage cursor. Both APIs use
MultiScan with negotiated `MULTI_SCAN` and `REVERSE_SCAN` hello features. A peer
missing either returns an explicit error, never an ascending emulation. Existing
ascending Scan/MultiScan encodings and stored key/run formats are unchanged.
See [the algorithm](algorithms.md#6-distributed-range-scan--paging-and-the-min-coverage-merge) and
[scan implementation details](scan-performance.md) for paging and concurrency.

- **Per-zone RF is nearly free.** The ring already computes "next N distinct
  nodes" (`Ring::replicas_n`); placement passes `N = zone.rf` instead of a fixed
  ring RF. The ring *layout* (vnode → node) is a single global structure shared
  by all hash zones.
- **Hash the (zone, key) pair**, not just the key, so the same key in two zones
  spreads to independent node sets (avoids correlated hotspots). Costs nothing.
- **Placement scope.** `replicas` is computed over the zone's `scope` (a subset
  of members), defaulting to all members. Scoping a zone restricts it to a node
  subset — used for tenancy isolation *and* to bound how widely a small zone
  spreads (see Scalability). The scope is growable: expand it as the zone grows.

Anti-entropy and rebalancing are unaffected in structure: both already ask "is
this node a replica for this (zone, key)?" via a scoped predicate
(`reconcile_scoped(keep_pred)`), which simply becomes partitioner-aware. They do
not care whether the partitioner is hash or range.

## The catalog: PD-replicated, pushed to data nodes

The zone catalog lives in the **same PD raft group as membership** — one
consensus group, so membership and catalog changes are totally ordered together
and there is no second group to operate.

The catalog must reach **data nodes**, not just the PD: a node cannot open a
zone's engine without its ordering mode. So:

- The PD's existing `SetMembers` push extends to also carry the catalog (a
  `SetZones`/catalog op over the same channel). `create_zone` / `drop_zone` /
  `get_zone` are PD admin operations. `drop_zone` deletes the zone's range map
  (and any pending re-home) from PD state in the same proposal; a range-map write
  applies only while its zone exists, queued range ops of a dropped zone are
  discarded, and map records left without a zone (pre-0479 drops) are ignored and
  collected by the PD leader, so they never gate balancing or re-homes (0479).
- A node applies catalog changes the way it applies membership: update the cached
  catalog; create engines lazily, tear them down on `drop_zone`. Teardown is
  **deferred-quiesced**: the zone becomes unroutable immediately, but the storage
  region is freed only once nothing in flight (an anti-entropy pass, a borrow-free
  read mid device-await) still holds the engine — tracked by the engine `Rc`'s
  reference count — so a freed region can never be re-allocated under an op that
  can still touch it. Re-creating the same zone id waits out a pending teardown.
- **Coordinators** fetch a combined cluster view (members + catalog) and cache it.
  A request carries `(zone, key)`; the coordinator looks up the zone to get
  `rf` + partitioner + scope, computes `replicas`, and tags the zone in the wire
  op. An unknown zone triggers a catalog refresh, then an error if still unknown.

## Wire / API impact

- `apply` / `read` / anti-entropy ops gain a **zone id** field.
- A **`default` zone** (id 0, unsorted, the cluster's configured RF) preserves
  today's zero-zone behavior; the existing single-zone code path is "zone 0".
- Admin surface: `create_zone(name, {rf, ordering, scope})`, `drop_zone(name)`,
  `get_zone(name)`.
- **Range deletes** (sorted zones only, 0439): `delete_range_in(zone, lo, hi, tier)`
  stamps one version and stores one range tombstone per range slice of `[lo, hi)`
  on that range's replicas (wire `RangeDelete`, hello feature bit 3). Covered keys
  read as absent, older stragglers lose, and replica scans skip the covered span.
  Unsorted zones and the default zone return `NotSorted`. A range tombstone lives in
  the zone's own engine (value-log record kind 2, run format v6 RT block), so
  `drop_zone` discards it with the engine. Inherited splits need nothing; re-home
  copy and mirror, cleanup handoff and anti-entropy each carry the fragments over
  the span they move (`docs/range-deletes.md` §4–§5).

## Scalability / growth on demand

This is a primary requirement: zones must grow without manual resharding.

- **Hash zones grow for free.** Adding a node reassigns some vnodes to it, so it
  becomes a replica for ~1/N of every hash zone's keys; the scoped-sweep /
  rebalance path fills its per-zone engines — **eagerly** (a `SetMembers`/`SetZones`
  handler kicks the anti-entropy sweep via an eventfd, so a node materializes +
  heals a newly-owned zone immediately, not at the next tick). A zone's data spreads
  across the *whole* cluster even though each key has only `rf` copies — so a zone
  scales out to the full cluster automatically.
- **Vnode count must be generous/adaptive.** The default (128) is too few once a
  cluster reaches hundreds of nodes (uneven spread, some nodes get zero vnodes).
  The global ring's vnode count should scale with cluster size.
- **Sorted-zone growth = range splits** — **built + automatic, but structural
  only**: range maps are raft-replicated PD state and `PlacementDriver::split_zone` /
  `PdDaemon::split_zone` adds a boundary and auto-pushes the new range map
  (retry-until-acked). A split is **metadata-only**: the new upper range inherits its
  parent's placement token, so no key changes replica set or intra-node shard and no
  committed key can become unreadable. It does not affect the hash path. As of task
  0108 the PD **auto-splits by default** (size-triggered, at a node-reported
  midpoint) and ships two default-on guardrails — `presplit=K` at creation and a loud
  single-range warning + fill/skew metrics. Runtime splitting therefore *prepares*
  boundaries but does **not by itself** redistribute load. Presplit-at-creation and
  explicit `canonical` splits of empty ranges spread immediately. Confirmed
  re-home is built and default-on for populated inherited ranges after task 0176
  cleared the measured healthy-write mirror-failure gate described above. It uses
  balanced opaque targets for V4 maps and canonical lower-bound targets for legacy
  maps; load/capacity-aware movement remains the separate opt-in balance executor.
  PD config keys (legacy aliases in parentheses):
  `range_split_threshold` (`VALS_PD_RANGE_SPLIT_THRESHOLD`; `0` disables auto-split),
  `range_load_split_ops` (`VALS_PD_RANGE_LOAD_SPLIT_OPS`; `0` disables load splits),
  `range_split_poll_ms` (`VALS_PD_RANGE_SPLIT_POLL_MS`),
  `range_splits_per_poll` (`VALS_PD_RANGE_SPLITS_PER_POLL`; default 16),
  `single_range_warn_entries` (`VALS_PD_SINGLE_RANGE_WARN_ENTRIES`; `0` disables
  warning guardrails), `range_concentration_warn_pct`
  (`VALS_PD_RANGE_CONCENTRATION_WARN_PCT`; default 80; `0` disables concentration
  warnings), `range_merge_threshold` (`VALS_PD_RANGE_MERGE_THRESHOLD`;
  opt-in shrink-reclaim, `0` = off), `range_merge_covered`
  (`VALS_PD_RANGE_MERGE_COVERED`; default on, see "Covered merges").
  See "Growth = range splits" above.

### Known boundary: per-zone-engine overhead at high zone count

Because each zone is a separate engine per replicating node, a cluster with very
many zones (thousands) × many nodes accumulates many small engines, each with
fixed overhead (open segments, in-memory index). Mitigations, in order:

1. **Lazy engine creation** (above) — a node materializes a zone's engine only
   when it actually receives data for it.
2. **Placement scope** — bound a small zone to a node subset so it doesn't spread
   a handful of keys across the entire cluster. This is the primary knob, and the
   reason `scope` is a day-one catalog field even though v1 defaults it to "all
   members."

This is a scaling boundary to monitor, not a blocker: at tens-to-hundreds of
zones it is a non-issue, and the scope knob handles the small-zone case.

## Build order

1. **Zone catalog in the PD** — `create_zone` / `drop_zone` / `get_zone`,
   replicated in the membership raft group; a `default` zone for back-compat.
2. **Per-zone engines on the Node** — `zone → VersionedStore` map (lazy create
   from catalog, teardown on drop, enumerate-and-open on recovery); zone-tagged
   wire ops; coordinator RF/partitioner lookup from the cached catalog.
3. **Per-(zone, shard) anti-entropy** — the sweep iterates a node's zones.

Steps 1–3 shipped unsorted/hash first. The **sorted-zone range partitioner**
(step 4) then slotted in without disturbing the coordinator or anti-entropy,
exactly as the partitioner abstraction intended:

4. **Sorted-zone range partitioner** — `partition::RangeMap` (range-token
   placement on the same ring) + a version-aware `VersionedStore::scan`; the
   `Scan` wire op + `Coordinator::scan_in` distributed range scan (LWW merge,
   in-range-order concatenation); sorted-zone placement consistency (the
   node-side AE/cleanup sweep scopes by range token, agreeing with the
   coordinator); range-map propagation (`SetRangeMaps`/`RangeMaps` + coordinator
   `refresh_range_maps`); and PD ownership + growth (raft-replicated range maps
   under an `R` prefix, `split_zone`, daemon auto-push). v1 first served sorted
   zones unsharded; they are now served on the sharded (multi-drive) path too,
   sharded intra-node by range token (key `k` on shard `hash64(range_token(k)) %
   shards`, matching the client's `data_shard`/`scan_shard` routing). A range's
   keys co-locate on one shard (a range scan stays cheap) while different ranges
   spread across the node's shards/drives, so a sorted zone scales out per-node like
   the unsorted default zone. A runtime range-map **split** never moves keys: the new
   range inherits its parent's token, so its keys keep the same shard and ring nodes
   (see "Growth = metadata-only range splits" above for why the earlier
   move-and-hand-off design stranded data). Auto/size-based splitting shipped in task
   0108. Confirmed re-home now safely moves populated inherited ranges to new
   tokens by default after task 0176 cleared the healthy-write mirror-failure gate.
   Task 0226 added searched balanced opaque targets for V4 maps while legacy maps
   retain canonical lower-bound targets; load/capacity-aware movement remains the
   separate opt-in balance executor.

   On a **multi-drive (JBOD) `backend = spdk`** node (which serves per-partition
   *sharded* engines), the catalog's **unsorted** named zones are served across the
   partitions by `hash(key) % shards`, and **sorted** zones by `hash(range_token) %
   shards` (each partition sub-allocates its region into per-zone engines — see
   [`multi-drive.md`](multi-drive.md)); single-drive spdk (and file/mem) nodes serve
   the same full catalog with one engine per zone.

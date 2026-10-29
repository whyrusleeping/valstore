# vals agreement algorithms

The algorithms whose output **must be bit-identical across nodes** (and across any
second implementation) for the cluster to be correct. [`formats.md`](formats.md)
pins *what the bytes are*; this file pins *how to compute the values those bytes
carry* — placement, versioning, the anti-entropy digest, and integrity. A node that
hashes a key differently, buckets a Merkle leaf differently, or orders two versions
differently will silently compute a **disjoint** view (wrong replica set, false
"in sync", wrong last-writer) — exactly the class of bug the ring/catalog epoch
agreement guards against at the *config* level but cannot catch at the *algorithm*
level. [`ARCHITECTURE.md`](../ARCHITECTURE.md) explains *why* each exists; this is
the recipe.

Every algorithm here is extracted from the cited code — keep them in lock-step.
Same stability caveat as `formats.md`: these are the current contract, not a frozen
versioned one.

## 0. The base hash — `hash64`

Everything placement- and digest-related derives from one explicit, stable,
dependency-free 64-bit hash (`ring.rs:38`). **Never** the toolchain's
`DefaultHasher` — that varies across builds and would break agreement.

```
hash64(bytes):
    h = 0xcbf29ce484222325                      # FNV-1a 64-bit offset basis
    for b in bytes:                             # FNV-1a
        h = (h XOR b) * 0x100000001b3   (mod 2^64)
    h = (h XOR (h >> 30)) * 0xbf58476d1ce4e5b9   (mod 2^64)   # SplitMix64 finalizer
    h = (h XOR (h >> 27)) * 0x94d049bb133111eb   (mod 2^64)
    return h XOR (h >> 31)
```

The FNV-1a pass alone has weak avalanche (sequential keys / small node ids cluster
on the ring); the SplitMix64 finalizer scatters them. All multiplications wrap mod
2⁶⁴.

## 1. Placement — the consistent-hash ring

Defined in [`ring.rs`](../crates/vals-client/src/ring.rs). The ring is a pure
function of `(weighted members, vnodes)`; the PD is the single authority for those
three (`reconfigure`), so every node derives the identical ring.

**Vnode positions.** A member `node` with weight `w` occupies `w * vnodes` points
(default `vnodes = 128`). The `i`-th point (`ring.rs:52`):

```
vnode_pos(node, i) = hash64( node:u32-LE (4B) ++ i:u32-LE (4B) )        # i in 0 .. w*vnodes
```

The ring is the list of `(vnode_pos, node)` for every member's every point, **sorted
by `(position, node_id)`** (the node-id tie-break makes a position collision
deterministic) (`ring.rs:236`).

**Preference list.** For `key`, walk clockwise from `hash64(key)` and collect the
first `RF` **distinct physical nodes** (`ring.rs:262,304`):

```
replicas(key):
    pos   = hash64(key)
    start = first ring index whose position >= pos   (wrap to 0 if none)
    walk entries start, start+1, … (mod len), appending each node not already taken,
      until RF distinct nodes (or the ring is exhausted)
```

The first is the *primary* (no special authority — all RF are equal). `replicas_n`
takes an arbitrary count `n` (sloppy quorum extends the list past RF this way).

**Zone salting.** A non-default zone hashes the key **salted with the zone id**, so
the same key in two zones lands on independent node sets, and optionally restricts
the walk to a node subset `scope` (`ring.rs:287`):

```
replicas_in_zone(zone_id, key, rf, scope):
    pos = hash64( zone_id:u32-LE (4B) ++ key )
    walk as above, skipping nodes not in `scope` (when Some), until rf distinct
```

> The default zone (id 0) deliberately uses the **unsalted** `replicas(key)`, *not*
> `replicas_in_zone(0, …)`, so its placement is unchanged from before zones existed.
> The two are different layouts for the same key — don't mix them for zone 0.

### Load balance on small clusters

Pure consistent hashing has a fixed arc-length imbalance for each ring draw. An
initial Monte-Carlo using the production `hash64`, RF 2, three randomly drawn
node ids, 4–8k keys per trial, and 200 trials measured these hottest/coldest
owned-entry ratios:

| vnodes | mean | p90 | worst (of 200) |
|------:|-----:|----:|---------------:|
| 64  | 1.16 | 1.27 | 1.43 |
| 128 | 1.11 | 1.21 | 1.32 |
| 256 | 1.09 | 1.16 | 1.25 |
| 512 | 1.06 | 1.11 | 1.18 |

The pure-hash lever is more vnodes. Load coefficient of variation scales as
`1/sqrt(vnodes)`, so doubling 64 → 128 cuts excess skew by about 30% (`sqrt(2)`);
halving it requires four times as many vnodes (256). The ring has only
`N * vnodes` entries—tens of thousands even for hundreds of nodes—and is rebuilt
on membership change, so this cost is negligible. RF greater than one does not
amplify store skew: averaging over a key's replica arcs slightly reduces
per-node store coefficient of variation compared with primary-only placement.
More keys or ranges only reduce sampling noise around the ring's structural arc
ratio; they do not remove it.

Task 0120 repeated the simulation for the capacity metric and the small-cluster
sizes of interest. It created one Python 3.10.12
`random.Random(0x01202026)` (MT19937) and kept that single stream across
`N = 3, 4, 5`, without reseeding. At each N it ran 1,000 trials in order. Each
trial called `rng.sample(range(2**32), N)` once for the unique node ids, then
called `rng.randbytes(16)` exactly 8,192 times in range order for the tokens. Each
trial therefore has 8,192 equal-entry ranges; each range contributes one owned
entry to both RF=2 replicas. The ring used vnodes 128 and equal node weights, and
salted each token with zone id 1 before the production `hash64` and distinct-node
ring walk. `p90` is the nearest-rank trial percentile:

| nodes | hottest/mean mean | p90 | worst | hottest/coldest mean | p90 | worst |
|------:|------------------:|----:|------:|---------------------:|----:|------:|
| 3 | 1.052 | 1.091 | 1.156 | 1.113 | 1.196 | 1.332 |
| 4 | 1.064 | 1.108 | 1.203 | 1.139 | 1.228 | 1.435 |
| 5 | 1.077 | 1.122 | 1.237 | 1.165 | 1.250 | 1.472 |

For an upper-bound comparison, the same 8,192 equal ranges were assigned across
all RF-distinct node pairs, largest first, minimizing the resulting maximum node
load, then squared deviation, then node-id order. Every pair appeared as a
preference set in every simulated 128-vnode ring; the future bounded seed search
still makes representability an explicit execution check. The resulting owner
counts and ratios were:

| nodes | per-node owned ranges | hottest/mean | hottest/coldest |
|------:|----------------------:|-------------:|----------------:|
| 3 | 5,462 / 5,461 / 5,461 | 1.00012 | 1.00018 |
| 4 | 4,096 / 4,096 / 4,096 / 4,096 | 1.00000 | 1.00000 |
| 5 | 3,277 / 3,277 / 3,277 / 3,277 / 3,276 | 1.00006 | 1.00031 |

Both tables measure node ownership; the planner below must additionally satisfy
per-engine placement because a range token also selects its shard. The second
table is an equal-range granularity limit, not a promise for a real
zone. For total logical range weight `W`, largest indivisible range `m`, `N`
equal-capacity nodes, and replication factor `RF`, even an unconstrained
assignment has

```
hottest / mean >= max(1, N * m / (RF * W))
```

and replica-set constraints can make the bound higher. For example, one range
holding half of an N=5, RF=2 zone alone imposes hottest/mean >= 1.25. Entry
estimates can also lag or miss unequal value sizes. A range larger than a node's
fair share must be split first; placement cannot divide one range across two
alternative replica sets. These results show that a sufficiently fine range map
has enough assignment granularity to remove most ring-draw skew, not that every
workload can reach 1.00.

This is now a measured capacity issue, not a speculative tail. The
task 0130 sustained-ceiling run
ended at 6 h 25 min when one records engine filled; its node received 1.79× the
applies of the cold node while the other engines retained space. Hundreds of
automatic splits did not help because populated split children correctly
inherited their parent's placement. `pd_node_load_skew_pct` now exposes
hottest/mean owned-entry skew directly; `pd_range_skew_pct` remains a different
metric for within-zone range fill.

Operationally, new clusters still use vnodes 128. Capacity
[`weight`](multi-drive.md) remains available: a node contributes
`weight * vnodes` points, allowing operators to hand-correct a persistently hot
node by adjusting relative advertised capacities. Do not change `vnodes` as a
live retune: it reshuffles all placement, is PD-authoritative and epoch-checked,
and therefore requires the corresponding data migration. On upgrade, an
existing PD config that omits `vnodes` changes from the old implicit 64 to 128 at
the next PD boot and reshuffles placement. Pin `vnodes = 64` before upgrading to
preserve the layout, or schedule and accept the one-time reshuffle.

#### Decision: PD-side assignment for sorted ranges

Request-time consistent-hashing-with-bounded-load cascades are rejected. A
coordinator would need a current load cap and cascade decision for each key, or
an unbounded per-key assignment table; either adds work and state to every write
and makes ownership load-dependent. That violates the requirement that this
feature add **zero per-write cost**. The pure ring therefore remains the only
placement mechanism for the default and unsorted zones.

For sorted zones, assignment-time balancing runs in the PD. A coordinator
already does a cached range-map lookup and hashes that range's token through the
ring. Assignment changes only which token the PD stores: it does not
add a lookup, branch, cascade, or RPC to the request path. The token becomes an
opaque, fixed-size PD-selected seed, not necessarily a range boundary, and the
existing `range_token` lookup plus `replicas_in_zone` walk remains bit-for-bit
the routing shape. Confirmed re-home supplies the incremental migration path, so
existing ranges stay where they are until individually selected; there is no
full-keyspace reshuffle.

RangeMap V4 supplies representation, validation, and compatibility support for
opaque tokens. New canonical placement uses a deterministic bounded nonce search
once `VALS_PD_OPAQUE_RANGE_MAPS=1` and the node capability preflight are ready.
The load planner below remains a gated dry-run and, behind the independent
default-off `VALS_PD_BALANCE_EXECUTE` gate, may submit its exact deterministic
token to confirmed re-home. The executor first upgrades legacy sorted maps to V4;
that metadata upgrade changes no placement. Ordinary debt re-home chooses a balanced
opaque target for V4 maps and the canonical lower-bound target for legacy maps.

Canonical placement greedily assigns ranges in lower-bound order. For each range,
it counts replica slots, primary roles, and—when a fresh topology snapshot exists—
per-node engine slots from every other effective token (pending target if present,
otherwise committed token). It searches the domain-separated token sequence and
minimizes max-minus-min spread for primary roles, replica slots, and engine slots,
then their squared deviations, with nonce as the final tie-break. The same chooser
handles initial presplits, a new empty child on a
V4 canonical split, and a debt re-home target. If the capability, RF/scope members,
or a valid bounded-search candidate is unavailable, the operation retains legacy
lower-bound/inherited behavior.

#### Load-aware assignment algorithm

The PD planner operates on a committed catalog/range-map/ring snapshot:

1. Build per-node owned-entry loads from the same sorted-range estimates,
   all-replica projection, and local hash-zone totals used by
   `pd_node_load_skew_pct`. Also retain per-(node, shard) loads from range stats:
   a sorted token selects one engine on every owner, and engine exhaustion—not
   merely node total—ended task 0130. Hash/unsorted zones and ranges not being
   moved are fixed load. Advertised node capacity weights define node targets:
   for total projected replica load `L` and authoritative member capacity
   `C = sum(c_i)`, node `i` targets `T_i = L*c_i/C`; divide that target equally
   across the node's reported shard count for engine targets.
2. A range of estimated weight `e` is indivisible. For each feasible RF-distinct
   target set in the zone's scope and each seed-realizable target shard, adjust
   node load for owners entering/leaving the set; independently move `e` from
   every current `(node, shard)` pair to every changed target pair, including a
   retained node whose token selects a different shard. Rank plans by lowest
   maximum engine load/target, then lowest maximum node `load_i/T_i`,
   then lowest squared normalized deviation, greatest current-owner overlap
   (least copy), and lexicographically sorted node ids and shard ids. Range
   candidates rank by predicted reduction in the hottest normalized engine and
   node loads, then larger `e`, zone id, and raw lower-bound bytes. All arithmetic
   and tie-breaks are deterministic.
3. Use bounded-load hysteresis rather than chasing every estimate change. Plan
   only when the engine or node maximum exceeds its configurable `1 + epsilon`
   high-water ratio, require a minimum predicted improvement, and stop below
   separate lower ratios. If no move can meet the cap, retain the minimum-maximum
   plan only when it clears the improvement threshold; otherwise report the
   indivisible/constrained range and wait for a split or capacity change.
4. Search one domain-separated fixed-size seed sequence per range, in increasing
   nonce order from `(planner version, ring epoch, zone id, range lower bound)`.
   Route each token exactly once through the production zone-salted ring walk and
   `hash64(token) % shard_count`, classify its replica set independent of primary
   order, and retain each set's best realized engine placement by the same objective
   with nonce as the final tie-break. The global per-range nonce bound makes unseen
   node sets observable exhaustion; they are skipped rather than weakening RF/scope
   rules. The chosen seed—not the
   target set—is the Raft-replicated pending token, preserving one routing
   representation. A separate per-range placement-debt bit distinguishes an
   intentionally assigned opaque token from a token merely inherited by a split;
   routing never reads that bit.
5. Schedule at most one move per zone and initially one globally. Wait for the
   previous prepare/copy/commit and a fresh statistics poll before replanning;
   do not split or merge that zone in the same poll. A populated split remains
   metadata-only and both children inherit the old token; the left child retains
   its prior debt state and the new right child is marked as inherited placement
   debt. After child estimates arrive, move at most one child through confirmed
   re-home. Presplits created transactionally before a new zone becomes writable
   may receive balanced seeds without copying. Thus a hot indivisible range is
   handled in the safe order **split, observe, then re-home**, never by changing
   placement as part of the split.

Prepare, copy, mirroring, fencing, cutover, and cleanup remain those in
[`zones.md`](zones.md). A failed mirror/copy or missing report
aborts and clears the pending token, leaving the complete old replica set
authoritative; a successful cutover commits the opaque token. Planner state is
therefore advisory and reconstructible, while the current and pending tokens
and epochs remain PD/Raft authority. Membership, weight, RF, scope, or ring-epoch
changes invalidate the plan and force recomputation after the new snapshot is
acked. Once committed, an opaque token follows the same ring membership-change
semantics as every current range token.

#### Compatibility and rollout

Range-map V4 is the shipped representation capability. It carries current and
pending tokens plus one explicit placement-debt bit per range, and accepts the
fixed 21-byte `VOPT` version-1 token defined in [`formats.md`](formats.md#33-rangemap-codec).
Legacy maps retain their bytes and validation and derive debt as
`token != lower bound`; an intentional V4 commit clears debt, inherited split
marks the new right child, and abort preserves it. `pd_unrehomed_ranges` and automatic
canonical re-home candidate selection now use debt rather than token shape.
Routing, fencing, scans, and shard selection continue to hash the committed or
pending token as appropriate and never inspect debt.

Handshake feature bit 0 advertises V4 support. Opaque-capable map emission is
blocked unless the operator has set `VALS_PD_OPAQUE_RANGE_MAPS=1` (default off)
and every current data-node endpoint has confirmed the current map through a V4
connection. Delivery of an existing V4 map preflights and retries each unconfirmed
endpoint independently, so one slow handshake does not prevent healthy endpoints
from receiving the map. Each actual push connection checks its own negotiated bit
before writing any V4 request bytes. Capability observations are process-local and
are invalidated on leadership loss, a failed beacon, or a beacon-detected endpoint
restart; an endpoint must freshly preflight and then negotiate V4 on the sending
connection before a post-restart push. A binary downgrade therefore blocks V4
redelivery rather than trusting an old observation. The operator gate asserts that
stateless coordinators have been upgraded. Old software rejects V4, and
mixed-version clusters continue emitting only canonical/inherited V1–V3 maps. The
gate establishes rollout readiness for opaque canonical placement: new
presplit maps may be V4 immediately, and already-V4 canonical split or debt-rehome
operations may choose opaque tokens. Only the separate
`VALS_PD_BALANCE_EXECUTE=1` gate permits a metadata-only upgrade of an existing
legacy map and load-planner execution.

Rollout is deliberately phased:

1. Task 0176 resolved the healthy-node mirror-failure abort storm and passed the
   repeated churn gate; ordinary automatic re-home now defaults on. Opaque-token
   balance execution remains separately gated on the capability work below.
2. Task 0179 added RangeMap V4, explicit placement debt, Rust/Go codec and
   routing parity, handshake feature negotiation, and the default-off two-part
   rollout gate. No existing map is rewritten and no opaque placement is selected.
3. Task 0180 shipped the read-only deterministic planner. It publishes projected
   before/after hottest node and engine ratios, movable/blocked range counts,
   proposed copy volume, bounded-search exhaustion, and attributed no-op reasons.
   The deterministic five-node RF=2 fixture documented in `zones.md` reports 269
   from both its before-node projection and `pd_node_load_skew_pct`. It emits no
   command and cannot mutate a map.
4. Task 0181 shipped opt-in, one-at-a-time prepare through the existing re-home
   protocol. Proposals are bound to membership/weights/ring config and epoch,
   catalog/RF/scope and epoch, the selected map/bounds/current token/pending state
   and epoch, endpoint capability, complete fresh statistics, and shard topology;
   the drive thread revalidates against the current published poll generation and
   live Raft-backed state after the control log is fully applied and before appending
   a conditional prepare. Complete poll authority continues advancing while the
   result is unresolved, and the reflected intent is revalidated against the newest
   fingerprint before map prepare; later control commands remain queued across both
   single-key phases. Enqueue is
   not acceptance: a crash-replay-safe single-key conditional intent phase serializes
   delayed quorum outcomes, followed after reflection and fresh-poll revalidation by
   a separate idempotent exact-map conditional write. Each phase mutates one durable
   record only; restart either resumes from the pending map or conditionally rejects
   and clears an orphan intent. A completed, aborted, or stale-rejected move consumes
   one complete fresh poll before
   another prepare. Split/merge arbitration remains per-zone and populated split stays
   inherited and metadata-only.
5. Task 0182's destructive uneven-RF=2 hardware A/B reached only the fail-closed
   V2 estimator-coherence fence and therefore left automatic balance execution
   **default-off**. Tasks 0284 and 0290 replaced planner weights with exact,
   epoch-scoped V3 logical-winner accounting and adopted that accounting on SPDK;
   this accounting adoption did not validate unattended movement.
6. Task 0262's first two coherent-V3 movement runs exposed an online accounting
   forward-progress gap: an unready poll notified the general sweep, but a sweep
   already occupied before map adoption could not advance the current epoch. V3 polls
   install the current logical range map, remain fail-closed, and signal both a
   dedicated local-accounting task and the general sweep. The dedicated task advances
   cooperative seeding independently of peer reconciliation, checks shutdown between
   steps, and never enters the one-shot synchronous seed fallback; request handling
   never loads run version streams (sidecars or v5 data blocks). Since 0374 it is the
   only seed driver of its shard's stores: AE handlers answer Busy for an unfinished
   seed instead of stepping it, and the task holds seeding to `ae_seed_duty_pct` of
   shard time while foreground work is in flight (see `docs/backpressure.md`). A source-pinned SPDK rerun observed seed/projection
   progress to all-endpoint readiness while general sweep-pass counters were flat,
   then passed node-outage abort and old-token authority, exact values, persisted-PD
   resume, prediction/actual ownership, capacity, and convergence without hysteresis
   oscillation. Logical node skew improved from 120% to 100.63% and engine skew from
   230% to 162.5%; steady writes were 99.0% of control. Clean active-move writes,
   however, were only 65.6% of control. The executor remains **default-off**; task 0292
   owns copy/mirror admission and throughput remediation. Unsorted/default zones,
   `vnodes`, and the ring walk remain unchanged.
7. Task 0226 added balanced canonical assignment for new presplits, empty-range
   canonical splits on V4 maps, and automatic debt re-home on V4 maps. It reuses the
   capability gate but not the load-executor gate; defaults remain unchanged and
   existing legacy maps still require executor-driven upgrade and convergence.

### Epoch-only seed reuse (0294, enabled)

Range authority and seed projection identity are distinct. The enabled 0294
implementation removes whole-zone work caused solely by **strictly increasing,
previously nonzero global range epochs** when the zone's inputs are identical.
Reuse applies automatically when these structural checks pass; there is no enable
flag. Independent software review and two short matched SPDK pairs with required
maintenance active support this checkpoint, not completion of the sustained-ingest
or full-correctness gate.

- Logical accounting compares the complete `RangeMap`, not just split boundaries.
  Equal maps rebase the receipt epoch without changing counts, readiness, or logical
  generation. Unready counts stay unready. In-flight fold/projection/journal state
  retains its generation and publishes against the new authority. A map change,
  lower epoch, or transition from unknown epoch zero takes the invalidation path.
- AE preparation retains one bounded metadata snapshot **inside each store instance**:
  zone ID, complete zone config (RF, scope, ordering, name), complete range map
  (including tokens, pending targets, debt and opaque mode), ring RF, vnode count,
  and ordered node/weight membership. Equality is structural, not a hash collision
  assumption. Manual placement replacement clears this snapshot. Reopened/recreated
  stores start without it; no zone-ID-only cache survives teardown.
- Ring and catalog epochs intentionally remain part of this snapshot: even equal
  content with changed ring/catalog authority still invalidates in this
  implementation. Full map equality similarly retains conservative invalidation on
  debt/opaque-mode changes. The optimization targets unrelated-zone **range** epoch
  churn only. There is no new key-sized state or work on the apply path.
- Reuse does not mark leaves clean, reverse unknown-old invalidation, restore
  RAM-shed state, or bypass run-removal, bucket, source, or projection-generation
  fences. Real split/prepare/abort/commit transitions still invalidate/reproject;
  exact maximum-version winners and tombstones remain the accounting source.
  Store/shard identity is fixed for the lifetime of these local caches.
- Global routing/adoption, V3 `epoch`/`accounting_epoch`, mirror/copy receipts,
  stale-response checks and planner fences remain epoch-sensitive. Rebased local
  counts do not authorize old receipts or old placement decisions.
- Cleanup (0478) uses epochs only to schedule laps: any ring, catalog or range-map
  epoch change makes every zone run one more full lap, but nothing is discarded.
  Its progress is scoped to the zone's placement: ring members/weights/RF, the
  zone's RF/scope and, per key span, the range token and pending token. Inherited
  splits and other zones' changes leave it intact. A lap in progress continues
  and repeats. The accumulated forget batch drops only the keys and released
  fragments in spans whose placement changed. An in-flight forget rewrite is
  aborted at commit only if one of its keys or pieces lies in such a span, as
  re-checked against the map current at commit. A ring or zone-config change
  still discards everything.
- Cleanup handoff confirms in bulk first (0478). Before pushing keys one by one, a
  cleanup page sends each owner a batched digest probe (`MixedBatchRead`, digest
  role) for its not-owned keys. The digest is the owner's logical version, the
  newer of its entry and its range-tombstone cover. A key whose every owner
  reports at least the listed version is released with no value read and no
  apply, the same outcome as every owner answering `Lost`. All other keys take the
  per-key push path with its 256-RPC cap, in key order. Probes go out lazily as
  the page loop reaches them, in windows that start at one not-owned key, double
  up to 1,024 while a window takes under 1/8 of the 250 ms page budget and halve
  when one takes longer. A window that confirms no key ends probing for the page,
  so a page the owners lack (auto-merge, membership change) costs one digest
  before the push path. No window starts once the budget is spent, and an
  in-flight window is dropped (its keys count as unconfirmed) at the first wakeup
  after the budget runs out or a stop is requested, so a slow peer or one on the
  scalar digest fallback holds a page for at most about one RPC past either, as a
  per-key apply already could. After a
  confirmed re-home the owners already hold every copied version, so the former
  owner releases a whole 8,192-key page per pass instead of 85 keys at RF 3.
  `vals_cleanup_probe_released_keys` counts these releases. Owners count probed
  keys in their per-range op rates, as they already counted cleanup applies.

New fixed-name counters (Prometheus `vals_` prefix) count transitions, not keys:
`ae_seed_epoch_suppressions` and `logical_range_epoch_rebases` count accepted epoch
advances; `ae_seed_placement_invalidations` and `logical_range_map_invalidations`
include first installation and conservative authority rejection; they do **not**
count full folds because invalidations can coalesce. `logical_range_unknown_invalidations`
counts published-count invalidation from unknown old winners. `ae_seed_starts`
counts cooperative source snapshots, including restart snapshots, not publications
or synchronous fallback. Compare their deltas with fold/project-winner counters,
restarts, RAM sheds, and acknowledged KV operations at matched cursor progress;
none alone estimates time saved. Two 900-second pairs, including reversed order,
measured pooled acknowledged KV throughput 57.613→72.872k/s (+26.5%) and source
throughput 28.465→35.949k/s (+26.3%), with common-progress fold/projection work
about 56% lower. RF2/W2 device-sync and rehome/split/AE/compaction stayed active;
balance execution stayed off. Evidence and per-arm tail/debt caveats are in
`bench/0294-ingest-optimization/phase6/20260913T052004Z/FINDINGS.md`.
Late slowdown remains; 200 Scan/Get samples per arm are not a full payload,
version, or tombstone oracle. Direct readiness/debt proof and the final sustained
one-hour gate remain in 0294; this checkpoint does not clear 0248.

### Placement-equivalent range-map reuse (0297, enabled)

An inherited auto-split moves a sorted zone through three range epochs (split,
rehome prepare, rehome commit). The 0294 rule above compares complete maps, so
each of those epochs used to refold every local zone store three times per split.
Two structural extensions, no flag:

- **Boundary-preserving rebase.** When the epoch strictly advances from nonzero and
  the new map has identical split boundaries (only tokens, pending targets or
  placement debt differ), logical accounting keeps its counts, readiness and
  generation and adopts the map/epoch in place (`logical_range_boundary_rebases`).
  Counts are indexed by boundary, so nothing they measure changed. Unready or
  in-flight state stays as it is and publishes against the new authority.
- **Replica-set-preserving placement reuse.** AE preparation keeps its placement
  closure, generation and leaves when the only input change is the range map and
  both maps yield the same `replicas_for_key_while_rehoming` replica *sets* at every
  boundary of either map plus the empty key (the piecewise-constant probe set), so
  the replica function is pointwise identical as a set (`ae_seed_placement_reuses`).
  Leaves key on the sorted set (`group_of`), so ring-walk order is irrelevant: a
  rehome commit that only reorders replicas, which is every rehome when nodes equal
  RF, keeps the seed (0317; before it, each such commit refolded every store of the
  zone and cut LONG01 ACK/s to 50–300k for about 250 s). When replica sets do
  change (nodes > RF), a store that holds no key in any changed span keeps its
  leaves and only installs the new placement (0322). "Holds no key" is exact:
  one winner-count page per span over the memtable and runs, tombstones included.
  Leaves key on `group_of` of stored keys, so they are unchanged. Keys that arrive
  later are grouped by the new placement. On a 3-node RF2 cluster a rehome
  therefore touches only the one owner shard store per node instead of every
  store of the zone, and since 0377 that store regroups only the changed spans'
  winners between replica groups instead of refolding (see `docs/zones.md`). Any
  ring/catalog/config/RF/scope/membership change or a lower or zero epoch still
  invalidates, as does a changed replica set over a span the store holds keys in
  while a seed is in flight, past 8 regroup compositions, or with `ae_span_regroup=0`.
- A boundary change no longer refolds the store when counts are published at a
  strictly newer nonzero epoch; see the next subsection. Other boundary changes
  still invalidate counts and refold once.

Measured on the two-node RF2/W2 device-sync realistic fixture
(`bench/0297-ingest-1m/rangemap01/FINDINGS.md`): with the accepted one-lane client
the change removed the work (seed starts 24→16, folds −37%) at +0.3% throughput,
because shard threads were mostly idle; with a depth-4 pipelined client it gave
+8.3% and +14.9% whole-write ACK KV/s in two matched pairs (post-first-split +18%
and +23%, stall seconds 4→1 and 18→12), with exact counts, full-value samples,
cursor and clean stops in every arm and no seed restarts, RAM sheds or
accumulated staged bytes.

### Range-bounded split recount (0310)

Before 0310, any boundary change cleared logical counts and the Merkle leaves, so
every auto-split cost one full O(store) cooperative seed on every shard store of the
zone. The leaves never depended on boundaries, and 0297 already keeps placement for
inherited splits. Since 0310, counts are rebuilt from the changed ranges only
(`replica/lifecycle.rs`):

- **Transition.** A map change with different boundaries is handled here only under
  these conditions: a strictly newer, previously nonzero epoch; published counts of
  the right length; and a sorted engine. Any combination of splits and merges is
  accepted.
  - **Groups.** One merge walk over the old and new boundary lists finds the
    boundaries common to both maps that are not strictly inside a pending span.
    These cut the keyspace into groups, and each group is a union of whole old
    ranges and of whole new ranges. It also contains every pending span it touches.
    A group's exact `total` is the sum of the counts of its non-pending old ranges
    plus the totals of its spans.
  - **Unchanged groups** copy their count, or their pending span, as is.
  - **A group that becomes one range** (a merge, including a merge at or across a
    pending cut or span edge) gets exactly `total`. A zero-total group gives exact
    zeros.
  - **Otherwise each new child is classified.** A child is known when it is a union
    of old ranges whose counts are exact: non-pending ranges and finished children
    of a span. A child that equals an old partially scanned child keeps its scan
    cursor. A child made of one old derived child plus known pieces is derivable.
    Anything else, such as a sub-piece of a split pending child, is fresh.
    - If at most one child is unknown, it gets `total - known` at once, and no span
      is created.
    - Otherwise the group becomes a pending `SplitRecount { lo, hi, cuts, children,
      total }` span. Exactly one unknown child is `Derived`: a carried derived child
      if there is one, otherwise the one with the most remaining work by
      `Engine::range_size_hint`. That hint is the run block-window estimate plus a
      memtable page capped at 1024 entries. It performs no run I/O and does not
      materialize the whole memtable range. The other unknown children are scanned.
  - **Nested splits.** A nested split of a pending child, two cuts in one range, or
    a merge at a span edge therefore re-partitions the span with the same exact
    total (`logical_range_split_recount_repartitions`). It never falls back.
  - **Fallbacks.** Only these reach the full invalidation path and bump
    `logical_range_split_recount_fallbacks`: wrong-length counts, an arithmetic
    mismatch, an unknown old winner while a span is pending, and an engine error or
    finalize mismatch during a recount. First installs, epoch-0 boot installs, stale
    or equal-epoch reinstalls, and changes while counts are unready still
    invalidate, but they are not counted.
  - **On success**, the logical generation advances (an in-flight seed restarts only
    its RAM projection), and the leaves, placement and seed state are left alone.
- **Invariants.**
  - Spans are disjoint, aligned to map boundaries, and keyed by keys, so splits and
    merges elsewhere need no fix-ups. Boundary-preserving rehome rebases keep them.
  - `total` is the exact number of winners in the span, including tombstones and
    keys this node does not own.
  - A scanned child's `covered` is the exact number of its winners with
    `key <= after`, or all of its winners once it is done.
  - The existing apply/retire hooks keep both exact, and they are the only
    winner-set mutation points. Covered paths: store, blind apply with a winner
    directory, forget and forget plans, GC tombstone drops in run and vlog
    compaction (including cooperative plan/commit), and staged or copy/mirror
    applies. When nothing is pending, the hook pays only one `is_empty()` check.
  - An apply whose old winner is unknown still invalidates, and it also drops the
    spans.
- **Recount.** The accounting loop runs `logical_recount_step` after its seed loop.
  - Each turn counts one page of the first unfinished scanned child with
    `Engine::winner_count_page`, which uses the same memtable-plus-run merge as
    scans. A page is at most 1024 distinct max-version winners and stops early
    once a 1 ms turn deadline passes; the deadline is checked every 32 winners.
    The per-run block cursor setup of a page is not covered by the deadline. A page
    runs synchronously inside a single borrow, so no mutation interleaves with it
    and no source snapshot or run fence is held across turns.
  - **Run-block prefetch (0318).** The accounting loop drives each turn through
    `logical_recount_turn`, so run blocks are not read synchronously on the shard
    thread. Under the shard read gate, a short borrow plans the page from the same
    `page_lo`. For each overlapping run, the plan takes `straddling_start_block`
    plus a window of W = 16 blocks. The **horizon** is the smallest first key, over
    all runs, of the block just past its window. Each run's locators are then cut
    at `min(hi, horizon)`, so small runs read only the one or two blocks the page
    needs.
    - The reads are issued concurrently and awaited with no `RefCell` borrow held.
      Then the gate is released.
    - `Engine::winner_count_page_prefetched` then merges synchronously in one
      borrow: only the prefetched blocks plus the memtable, read at merge time,
      with keys limited to `[page_lo, horizon)`. The page stops at the horizon, at
      1024 winners, or at the turn deadline. It returns the same
      `(n, last, more)` as `winner_count_page`, so `covered`/`after` and the
      apply/retire hooks are unchanged. Memtable writes while the reads are in
      flight need no special handling. Run heads are kept in a min-heap, so each
      winner costs O(log R) key comparisons over R overlapping runs rather than a
      scan of every head (0416: −59% merge time per key at 24 runs).
    - Every key below the horizon has all of its run entries fetched. With W ≥ 2
      the run that sets the horizon has a whole fetched block inside the page, so
      every page yields at least one winner. A page that would still return no
      winner with `more` set is treated as a discard.
    - The prefetch is discarded, before any decode, if the engine
      `storage_generation`, the page bounds, or the recount span/child changed.
      A stale read or decode error is therefore never treated as corruption.
    - After four consecutive discards, the turn falls back to the synchronous
      `logical_recount_step` to guarantee progress.
    - Counters: `logical_range_split_recount_prefetch_pages`,
      `logical_range_split_recount_prefetch_blocks`,
      `logical_range_split_recount_prefetch_discards` and
      `logical_range_split_recount_sync_fallbacks`. The turn histogram counts plan
      and merge time, but not the awaited reads.
  - When every scanned child is done, the scanned children are set to their
    `covered` and the derived child to `total - Σcovered`. If the scanned children
    exceed the total, the step records `logical_range_split_recount_mismatches`,
    abandons, and the loop reseeds in the same kick.
  - Under RAM pressure the step returns `Paused`. The loop then waits on a 100 ms
    timer (or shutdown) and retries without needing an external kick.
- **Readiness and reporting.**
  - `logical_range_counts` reports nothing, so V3 reports `accounting_ready=false`,
    until every span is finished. Unready V3 polls, and sweeps that leave a span
    pending, kick the accounting loop.
  - While counts are published at the current epoch but spans are pending, V3 still
    reports per-range entries:
    - ranges outside every span, and finished children, report their exact count;
    - other span children report `range_estimate` clamped to exact bounds
      (`[covered, total - Σcovered of the others]`).
  - So PD split/merge policy, which ignores readiness, no longer sees rewrite-inflated
    estimates for ranges that are not being recounted. A split of a pending child
    that PD still makes is re-partitioned without a reseed.
  - A full seed that publishes, or `clear()`, supersedes pending spans. Spans are not
    persisted, so a restart reseeds as before.

**Cost per split.** The owner shard does O(winners in all but the largest unknown
child) of run-block reads, spread over pages. Other shards do O(1) work. There is no
fold, digest or placement projection and no staged memory. A canonical split still
reseeds its leaves through the placement path, and that publication also completes
its counts.

**Counters and histogram.** Counters: `logical_range_split_recounts`,
`logical_range_split_empty`, `logical_range_split_recount_pages`,
`logical_range_split_recount_keys`, `logical_range_split_recount_done`,
`logical_range_split_recount_repartitions`, `logical_range_split_recount_fallbacks`,
`logical_range_split_recount_mismatches`, `logical_range_split_recount_ram_pauses`,
`logical_range_split_recount_prefetch_pages`,
`logical_range_split_recount_prefetch_blocks`,
`logical_range_split_recount_prefetch_discards`,
`logical_range_split_recount_sync_fallbacks` and `logical_range_merge_sums`.
Histogram: `logical_range_split_recount_turn_us`, which records serving-core time
per page turn.

## 2. Placement — sorted-zone range tokens

A sorted zone divides its keyspace into contiguous ranges by a sorted list of split
keys, then places each *range* (not each key) on the ring
([`partition.rs`](../crates/vals-client/src/partition.rs)). Each range carries an
explicit placement token; the codec is in `formats.md §3.3`.

```
range_index(key) = number of split keys <= key            # partition_point
range_token(key) = tokens[range_index(key)]
placement(key)   = replicas_in_zone(zone_id, range_token(key), rf, scope)
```

All keys in a range share the token ⇒ share a replica set ⇒ a range scan touches
few nodes. Legacy canonical maps use each range's lower bound as its token. When the
opaque capability is ready, new-zone presplits and explicit `split-zone … canonical`
operations on empty ranges in V4 maps use balanced opaque tokens instead. A normal
runtime or automatic split gives the new right range its parent's token, so placement
is unchanged. Canonical splitting is unsafe once the range contains data; populated
ranges need the [confirmed re-home protocol](zones.md). Opaque canonical assignment
and the opt-in load executor alter only the stored token and do not alter this
request-time range-token algorithm. Splits compare with raw byte ordering (`<=` on
`&[u8]`). A map with more than 16 splits answers `range_index` from a lazily built
16-way search tree of 8-byte key heads, which every split or merge drops. It returns
the same count, and at R3's range counts it cuts the cost of the write path's two
per-key searches by 58–72% (`bench/0442-range-index`).

## 3. Per-node shard routing

Within a node, a key routes to one of `N` shards (one engine per core):

- **Hash (unsorted) zone & the default zone:** `shard = hash64(key) % N`.
- **Sorted zone:** `shard = hash64(range_token(key)) % N`, so a range co-locates on
  one shard and a range scan hits one shard per node (see `formats.md`/`zones.md`
  and the routing note in the source). A scan clips to one range, so
  `range_token(lo)` recovers that range's shard.

Node-level operations (membership, anti-entropy control) use shard 0.

## 4. Versioning — HLC and last-writer-wins

Defined in [`hlc.rs`](../crates/vals-client/src/hlc.rs). A `Version` is
`{ hlc: u64, node: u64 }`; `node` is the legacy field name for an opaque writer-incarnation
identity, not the caller's placement node ID. The packed clock is
`hlc = (physical_ms << 16) | counter16` (48 ms bits + 16-bit intra-ms counter).

**Local stamp** (`now`, `hlc.rs`): if wall `phys_ms` is strictly ahead of the last
clock's physical part, take `phys_ms << 16` (counter 0); otherwise `last + 1`
(advance the counter, carrying into the physical part if it overflows 16 bits).

**Merge an observed remote clock** (`observe`):
```
1. Skew clamp: if max_skew_ms != 0 and (remote >> 16) > phys_ms + max_skew_ms,
   replace remote with ((phys_ms + max_skew_ms) << 16)   # clamped physical, counter 0
2. max_phys = max(last_phys, remote_phys, phys_ms)
3. counter = (max_phys == last_phys == remote_phys) -> max(last_ctr, remote_ctr) + 1
            (max_phys == last_phys)                 -> last_ctr   + 1
            (max_phys == remote_phys)               -> remote_ctr + 1
            (else, max_phys == phys_ms ahead of both) -> 0
   hlc = (max_phys << 16) | (counter & 0xffff)
```
The clamp protects the local clock; storing an over-ε write is refused separately in
`VersionedStore::apply` (returns `Rejected`). `max_skew_ms` defaults to **500 ms**.

**Last-writer-wins order.** `Version` compares by `hlc` first, then the 64-bit
writer identity in the legacy-named `node` field. Every `Hlc::new` obtains a fresh
identity from the OS CSPRNG and rejects the legacy u32 range, keeping generated
identities disjoint from legacy node IDs while retaining effectively 64 bits of
entropy. Clones share one atomic HLC state, so they cannot issue the same clock
value. A process restart creates a new writer incarnation; clock rollback therefore
cannot reproduce a prior version unless the random 64-bit identity collides. The identity is carried in every wire, hint, AE, and
engine-record version. This is a probabilistic collision-resistance contract, not
a durable per-write clock journal.

The tie-break makes the total order identical on every node, so all replicas pick
the same winner regardless of delivery order. In the `capacity` profile, the
resolving writer stores a candidate only if it **strictly** beats the current
version (`wins_over` = `>`); equal versions are the same write. The `latency`
profile's blind-append semantics may retain physical losing candidates, but this
does not change logical state: every point read, digest, scan, merge, and recovery
path resolves argmax(`Version`).

## 5. Anti-entropy — the Merkle digest

Defined in [`antientropy.rs`](../crates/vals-cluster/src/antientropy.rs). Two
replicas reconcile by comparing a Merkle tree over their logical-winner
`(key, version)` sets — **value bytes never enter the digest**. `VersionIndex`
keeps cached leaf groups, per-bucket generation/count/dirty metadata, and a
mutation journal, not a per-key version map. A dirty bucket is rebuilt exactly
from engine logical-winner digest pages (run version streams — v3/v4 sidecars or
v5 data blocks — plus mutable state) before
it is served, and that rebuild may perform device reads.

```
DEFAULT_BUCKETS = 4096
bucket_of(key)        = hash64(key) % num_buckets                      (antientropy.rs:38)
entry_digest(key, ver)= hash64( key ++ ver.hlc:u64-LE ++ ver.node:u64-LE )  (antientropy.rs)
leaf[b]               = XOR of entry_digest(k, v) over all (k,v) with bucket_of(k)==b  (antientropy.rs:55)
```

XOR makes a leaf **order-independent** and **incrementally updatable**; two replicas'
leaf `b` are equal iff they hold the same set of `(key, version)` hashing into `b`.

**Root** (`antientropy.rs`): a bottom-up pairwise fold of the 4096 leaves —
`mix2(a, b) = hash64(a:u64-LE ++ b:u64-LE)` on each adjacent pair, repeated until one
value remains (an odd element carries up unchanged). Equal roots ⇒ in sync.

**Reconcile protocol** (`antientropy.rs`), pull-based and placement-scoped:
```
1. mine = my leaf digests;  peer_leaves = SyncLeaves(zone, num_buckets)
2. if root(mine) == root(peer_leaves): done (identical)
3. diff = { b : mine[b] != peer_leaves[b] }
4. after = None
   loop:
     (entries, more) = SyncBuckets(zone, num_buckets, diff, after)
         # one PAGE of the peer's (key, version, value) in those buckets:
         # ascending key order, keys strictly > after, truncated at the peer's
         # reply budget (formats.md §1.5); a truncated page is never empty
     for each entry whose key this node should hold (placement scope `keep`):
         apply store-if-newer (older/equal = no-op)
     if !more: done
     after = last entry's key
```
Both peers must agree on `num_buckets`, `bucket_of`, `entry_digest`, and the root
fold or step 2/3 misfires. Scoping (`keep`) is essential — an unscoped pull would
over-replicate and break RF. Applying each page *before* pulling the next is safe
because store-if-newer is idempotent and order-independent.

`VersionIndex` uses `bucket_of(key, INDEX_BUCKETS)` for its cached leaves and dirty
metadata (`INDEX_BUCKETS == DEFAULT_BUCKETS`,
[`replica/version_index.rs`](../crates/vals-cluster/src/replica/version_index.rs)).
For step 4 (`SyncBuckets`), the engine produces logical-winner pages for the requested
buckets, collapsing every physical candidate by argmax(`Version`); the responder
then reads values only for those page winners. There is no resident per-bucket map
of keys and versions to iterate.

**Shared-keyspace scoping under RF<N** (task 0015). With `RF < N` each node holds a
*different* subset of the keyspace, so two peers' full-leaf roots **permanently differ**
on the keys only one of them replicates — the descent never converges those buckets and
every sweep re-examines them (measured ~68k digests/s at steady state with zero churn).
The fix groups each store's leaf accumulators **by the key's replica set**
(`leaves[replica-set][bucket]`, [`replica/`](../crates/vals-cluster/src/replica)) and
scopes the tree exchange to the keyspace *both* peers replicate: when node L reconciles
with peer P, each side folds only the groups whose replica set contains **both** L and P.
So a converged RF<N pair matches at the root round and examines ~0 digests, while a genuine
divergence *within* the shared keyspace still heals with the full hierarchical-descent
localization. Keys a node holds but the peer does not (sloppy-quorum fallback, post-membership
leftovers) are **not** AE's job — they are healed by hinted handoff and the over-replication
cleanup pass (which pushes them to the rightful owners before forgetting); comparing them in
AE was pure waste. The value pull applies the same rule per key: digest pages list every key
of a differing bucket, and a key the peer does not own (current or pending set) is skipped
even when the local node owns it, so a former owner's leftover never enters through AE. The grouping fn is the ring's placement (`ring.replicas` / `replicas_in_zone`,
agreeing exactly with the reconcile/cleanup `keep` predicate); a placement change (ring/catalog
epoch) re-seeds the grouped accumulators from the engine's current keyset under the new
replica sets.

**Covered keys and the accounted set** (sorted zones, 0457/0458;
docs/range-deletes.md §4). A key whose winner has `version <= cover(k)` under the
store's range tombstones F is **covered** and contributes nothing to the leaves or
the range counts, so two replicas with the same F and the same uncovered winners
have equal leaves however far each has compacted the covered history. Each store
keeps an **accounted set** A, the overlay of every range tombstone whose record seq
is at most `S_A`: leaves and counts describe the winners A does not cover. A full
seed filters with F as of its start and publishes `A := F_S`. A range tombstone that
raises F joins a queue, and the shard's accounting task **folds** it in bounded
turns (1,024 winners or 1 ms): the fold fixes its target T (A plus the queued
tombstones up to `S_T`), walks the winners of the spans where T is above A (with the
block skip of §6, under A), XORs every winner T covers out of its leaf, decrements
its range count, removes covered memtable entries, and finally sets `A := T`. Write
maintenance in between uses `acct(k)`, T below the fold's cursor and A elsewhere, so
each winner leaves the leaves exactly once. While a fold is pending, tree and
digest requests for the store answer `Busy` and `RangeStatsV3` reports accounting
not ready; the `RangeDelete` handler runs one fold turn before it replies, so a
queue-sized fold is normally done before any peer asks. Compaction drops covered
entries only under A (never F), so it never removes an entry the leaves still hold.

**Range-tombstone exchange** (sorted zones, 0462; docs/range-deletes.md §4). The
leaves describe uncovered winners only, so they cannot show that one replica is
missing a range tombstone over keys neither side holds. Each sweep pass therefore
first exchanges range-tombstone sets with every peer shard
([`rt_sync.rs`](../crates/vals-cluster/src/rt_sync.rs)), before any Merkle compare
of the zone: both sides clip their fragment set to the ranges whose current or
pending replica sets contain both nodes and that route to their own shard, fold it
into a `hash64` digest, and on a mismatch the requester pages the responder's
clipped fragments (`SyncRangeTombstones`, formats.md §1.3) and applies each piece
that falls in its own clipped scope through `apply_range_delete`. Overlay is a join,
so every pass converges F; the pulled tombstones' fold then moves the newly covered
winners out of the leaves, and the Merkle compare of a later pass sees equal
leaves. Peers without hello bit 3 are skipped. A liveness-wiped node's sorted zone
counts as refilled only after a successful exchange as well as a successful
Merkle pull, in a pass where no received fragment was refused beyond the
clock-skew bound. An exchange pages until the responder reports no more
fragments (each page advances `after`), so it never stops partway. With unequal shard counts on the two nodes the clipped scopes differ,
so the digests never match and each pass pages the fragments again (correct, but not
cheap); equal shard counts pair shard *i* with shard *i* and compare equal sets.

## 6. Distributed range scan — paging and the min-coverage merge

Defined in [`coord.rs`](../crates/vals-client/src/coord/mod.rs) (`scan_in`). A scan
of `[lo, hi)` in a sorted zone visits the overlapping ranges in order; within a
range it reads `r` replicas and merges by LWW. Because replica replies are
**paged** (a reply carries at most the replica's byte budget / the requested
`max` entries — `formats.md` §1.5), the coordinator must merge *pages* without
weakening the quorum guarantee. The hazard: replicas truncate at different keys,
so naively merging everything received would emit a key some responder has a
newer version of but hasn't shipped yet (and emit it again next round).

Per range (replica set from the range token, `needed = min(r, |replicas|)`):

```
cursor = range.lo                       # None = open start
loop:
  ask every replica: Scan(zone, cursor, range.hi, max = remaining client limit, or 0)
  wait for the first `needed` pages (cancel the rest)
      # a page with more=1, no entries and no resume key is a protocol
      # violation, and so is a resume key <= cursor — treat that replica as
      # failed, not as coverage
  coverage(page) = page.resume        if present       # range-delete trailer
                 = last_key ++ 0x00   if page.more     # smallest key past the page
                 = range.hi (whole range)  otherwise
  bound = min over the `needed` pages' coverage        # None if all complete
  merge by LWW only entries with key < bound            # the min-coverage rule
  cover(k) = max version of the fragments holding k, over every responder's
      fragment trailer (range-deletes.md §2), single-responder rounds included
  emit merged live winners with version > cover(k) (a tombstone winner or a
      covering fragment hides the key), in key order, stopping at the client limit
  if bound == None or bound >= range.hi: next range
  cursor = bound                                       # strictly > old cursor ⇒ progress
```

Below `bound`, every one of the `needed` responders has contributed its full
window, so the paged merge observes exactly what a single-reply merge would —
`W + R > RF` strength is preserved across page boundaries. Entries at/past
`bound` are discarded and re-requested next round (bounded waste: at most one
page per replica per round). Progress is guaranteed because a truncated page is
non-empty or carries a resume key above the cursor, so `bound` strictly exceeds
`cursor`. Only peers that negotiated hello feature bit 3 send fragments and
resume keys; a replica that missed a range tombstone still returns the keys it
covers, and the other responders' fragments hide them.

On the replica side (0460), a page drops entries with `version <= cover(k)`
without charging them to `max` or the byte budget, and skips a run block unread
when the block's whole span lies inside one fragment f and the block's `max_hlc`
(run format v6) is below `f.version.hlc`; a per-run max tree finds the first block
in f that may hold a newer entry in O(log blocks). A page stops after
`SCAN_COVERED_BUDGET` (65,536 filtered entries or 64 run blocks read that held a
filtered winner) with a resume key just above the last filtered key. Once the
range tombstone's fold has completed, a scan that starts at the head of a deleted
queue costs the same whatever the deleted span's length; before it, covered
entries are filtered one by one in budget-cut pages
(bench/0449-rt-hot-path/FINDINGS.md §3).

A client `limit` flows into the per-page `max` (the *remaining* need each
round), so a small limit over a huge range pulls a few entries rather than the
range; tombstones count toward a page but not toward the limit, so the loop
keeps paging until it has `limit` live winners or the window is exhausted.

### Batched scans (0482)

`Coordinator::scan_many_in(zone, queries, r, keys_only)` accepts borrowed
`(lo, hi, Option<usize> limit)` triples and returns live `(key, Stored)` lists in
input order. Full mode retains values and versions; keys-only mode uses
`Some(empty)` for a live key, retaining its version. A zero limit or empty span
returns an empty list. It is not a cross-query snapshot.

Each query has its **own** range slice, remaining live-entry limit, cursor and
minimum coverage. In each round the coordinator groups those current slices by
replica; the transport groups by shard and sends at most 256 sub-scans per RPC.
The batch transport completes each node/shard's paged reply before returning;
the coordinator uses up to R successful pages independently for each query
(R is at least one, capped by the replica count). It does not cancel outstanding
batch replies when one query reaches quorum. For each query it runs the same
minimum-coverage/LWW/fragment merge above and observes the maximum entry/fragment
HLC. Exhausting one range advances only that query to its next slice; neither
another query's coverage nor the outer RPC `more` is a key-space bound.

The outer reply cursor is a sub-scan position. A returned sub-scan page can itself
be incomplete: its resume key/`more` belongs to the inner merge, not the outer
cursor. Peers without bit 4 receive scalar `Scan` requests, with values discarded
locally in keys-only mode (fallback therefore still performs value-log reads).
On a cold connection, a value-free `GetWatermark` probe completes hello before
choosing the opcode, so capable peers never receive a full-value scan merely
because their feature bits were not yet known.

### Descending scans (0490–0491)

`Coordinator::scan_in_rev(zone, lo, hi, r, limit)` returns live `(key, value)`
entries in descending order over the same half-open `[lo, hi)` window.
`scan_many_in_rev(zone, queries, r, keys_only)` returns descending `(key, Stored)`
lists in query order, with the same keys-only representation as `scan_many_in`.
Both use the reverse batch loop: at most 256 queries per group, one active slice
per query, visited from highest to lowest. Limits count live winners, not page
entries or tombstones; zero limits and empty/inverted windows send no scans.
There is no ascending emulation. The default `Transport::scan_rev` returns an
explicit unsupported error, and `scan_many_rev` defaults to serial reverse calls.
NetTransport implements both hooks via MultiScan with the reverse flag, gated
by hello bits 4 and 5. Scalar reverse uses a one-query vector. Like ascending
MultiScan it groups by shard using each slice's fixed lower bound, supports outer
reply cursors and keys-only mode, and probes cold connections before choosing the
encoding. Missing either feature returns an explicit unsupported error, not a
scalar ascending fallback. The server applies the same epoch, ownership, admission
and read-gate checks in either direction.

For each query's current slice:

```
cursor = range.hi                        # None = open end
loop:
  ask every replica: reverse Scan(range.lo, cursor, remaining limit or 0)
  collect up to R successful pages         # R >= 1, capped by replica count
  coverage(page) = page.resume             if present
                 = last_key               if page.more
                 = range.lo (whole slice) otherwise
  bound = max over the pages' lower coverage bounds  # None if all complete
  merge by LWW only entries with key >= bound
  apply all responders' fragment covers, emit live winners in descending order
  stop at the live-entry limit
  if bound == None or bound <= range.lo: next lower slice
  cursor = bound                          # exclusive upper bound, no ++0x00
```

The selected responders all cover `[bound, old_cursor)`, so `W + R > RF`
retains the ascending scan's strength. Taking the **minimum** instead would
publish stale versions or skip keys not yet returned by the denser responder.
Complete pages impose no additional lower bound. A truncated empty page needs
an explicit resume key. Every truncated coverage key must be at least the
slice's lower bound and strictly below a finite request upper bound; malformed
or non-advancing pages count as failed replicas, not quorum coverage. A bound
equal to the slice's lower bound completes that slice after merging its inclusive
boundary key. The empty key is a valid lower bound/key, not an absent cursor.

Each query maintains independent coverage, limit and cursor. Like ascending
batched scans, reverse calls finish the outstanding node batches rather than
cancelling at quorum; scalar `scan_in_rev` is a one-query batch. There is no
cross-page or cross-query snapshot. Current committed range tokens remain read
authority during a pending re-home. Response retention, HLC observation, failure
details, overload handling and stale-epoch retry follow the ascending batch path.
The ascending scan and write method bodies are unchanged.

## 7. Integrity — CRC-32C

All on-disk records (vlog record/segment, zone superblock, drive layout) and the
torn-write detection use **standard CRC-32C (Castagnoli)**
([`crc.rs`](../crates/vals-store/src/crc.rs)):

- reflected polynomial **`0x82F63B78`** (the one the SSE4.2 `crc32` instruction
  uses), init **`0xFFFFFFFF`**, final XOR **`0xFFFFFFFF`**, reflected in/out;
- hardware (SSE4.2) and scalar slicing-by-8 paths produce the identical value, so
  the on-disk format is independent of which ran;
- check value: `crc32c("123456789") == 0xE3069283`.

---

## 8. Single-node algorithms (deterministic, not cross-node contracts)

These don't have to agree *between* nodes — each node computes them locally — but
they are load-bearing and worth recording. Full rationale is in `ARCHITECTURE.md`.

- **Log recovery / replay** (`vals-store`): at `open`, load the manifest and runs,
  then replay the uncovered ordinary tail; each segment walks records until the
  first bad/short magic. For a repeated key the greatest `version` wins, with `seq`
  only breaking an equal-version tie. The allocator bitmap is reconstructed from
  the manifest's live structures and replayable tail; it is not persisted.
- **Compaction / GC** (`engine/merge.rs`, `ARCHITECTURE.md` consistency section):
  merge inputs by argmax(`version`) and publish freshly synced run/manifest state
  before freeing old structures. Tombstone collection is restricted to covered
  bottommost input and the durable GC cutoff, preventing an older value from
  resurfacing after the winning tombstone is removed.
- **Run-merge (LSM) — decoupled three jobs** (`engine.rs` `compact_plan_inner` →
  `compact_and_write` → `compact_commit`, task 0069): a run-merge separates its three
  jobs instead of bundling them. (1) **Index fold** is the always-present job: a
  recurring merge folds a contiguous **newest-suffix subset** of similar-sized runs
  (tiered selection, fan-in `VALS_MERGE_TIER_K`≈6, input cap `VALS_MERGE_INPUT_CAP_BYTES`;
  the oldest "base" run joins only when the newer tiers reach ≈ its size — graduation
  → a *bottommost* merge). Entries keep their `ValuePtr`s (**index-only**,
  `VlogRewrite::None`) — no value bytes move, so recurring merge traffic is
  quasi-linear, not O(N²). (2) **Value clustering** (key-ordered vlog co-compaction,
  `VlogRewrite::All`) is **opt-in and paid at most once per value**: only a bottommost
  graduation merge with `VALS_VLOG_CO_COMPACT=1` clusters. (3) **Space reclaim**
  (`VlogRewrite::Victims`) is decoupled onto the region-pressure path: the plan
  aggregates per-segment live bytes (`RunMeta.value_seg_bytes`) across the input runs,
  picks the **mostly-dead** segments (dead fraction ≥ `VALS_VICTIM_DEAD_FRACTION`,
  default 0.5), rewrites only *their* live survivors forward, and frees the victims
  (falling back to cluster-all when victims can't be sized so a nearly-full region
  always self-heals). Tombstone GC / winner-dropping is permitted only on a bottommost
  merge (an older un-merged run may shadow a live copy). The merged run installs at the
  position of its **oldest input**, preserving recency order for un-merged older runs;
  `lookup_runs` resolves by `argmax(version)`, so a subset merge stays LWW-correct and
  read-transparent (no leaf changes).
- **Bounded merges and the frozen top tier** (`engine/merge.rs`
  `select_recurring_merge`, task 0381). The rules:
  - **Cap.** No recurring or forced merge reads more than `VALS_MERGE_INPUT_CAP_BYTES`
    of run input.
  - **Frozen runs.** A run is frozen when it is at or above `VALS_MERGE_FREEZE_BYTES`
    (default: the cap), or too large for any size-ratio balanced window under the cap
    (size × (1 + 1/r) > cap; 1.2 GiB at the defaults). Frozen runs are never recurring
    or forced inputs.
  - **Active tail.** Tiered selection works only on the runs newer than the newest
    frozen run.
  - **Valve.** It fires when the active tail holds `VALS_MERGE_FORCE_RUNS` (6) runs that
    tiering could not fold. It takes the cheapest adjacent window of 2..K tail runs
    under the cap: fewest input bytes per eliminated run, newest and widest on ties. It
    prefers balanced windows (largest input ≤ r × the rest) and otherwise takes any
    capped window. When none fits it does nothing and the run count grows.
  - **Adjacency.** Windows are contiguous in recency order. The output takes the
    window's place, so the "later runs are newer" order that the shadow estimator and
    shadow folds rely on (`entry_shadowed` probes only runs after *i*) holds. Tombstone
    GC and cluster-all co-compaction need the window to cover every run.
  - **Pacing.** Every recurring merge, forced ones included, is paced. Pressure merges,
    shadow folds and aged value-log GC are not.

  **Why it is bounded.** A balanced merge grows its largest input by at least
  1 + 1/r. Merge outputs never exceed the cap. So each byte is rewritten at most
  log_{1+1/r}(cap/flush) + 1 times, and run count is about data ÷ (0.6–1 × cap).

  **Space reclaim.** Frozen runs are rewritten only by space-reclaim plans: pressure
  (capped suffix, or the minimal two-run suffix at the floor), capped shadow folds,
  aged value-log GC (one run with victims), tombstone-GC merges and cleanup forget.
  Before the floor, a pressure plan that has nothing to reclaim (no gainful victims)
  falls back to the recurring policy instead of rewriting the newest runs' values.
- **Tombstone-GC merges** (`replica/maintenance.rs` `tombstone_gc_due`, task 0499). Point
  tombstones are dropped only by a bottommost merge, and tiered merges rarely include the
  base run, so a separate GC merge (kind `lonegc`) rewrites every run of an engine when the
  whole run set fits the merge input cap, or is one run. Over the cap, each GC merge
  instead rewrites one capped window of adjacent runs, rotating through the engine, with
  every other run streamed as shadow sources (the shadow-fold probe). An entry shadowed by
  a newer version elsewhere is dropped, and an expired tombstone is dropped only when no
  other run holds its key. So one rotation drops the older values under expired
  tombstones, and the next drops the tombstones. It is due when any run
  holds a tombstone below the durable GC cutoff, at most once per `min(gc_grace, 1 h)`
  of wall time per engine (counting attempts that planned nothing), so each expired
  tombstone is collected within one more such interval and a run is not rewritten every
  time another tombstone expires. It also runs when an ordinary merge is due but has no
  plan, such as a base run plus a small tail at the run-count threshold. When the
  unflushed tail holds an expired tombstone (the engine tracks the minimum tail
  tombstone version and the last tombstone's seq), the same gate schedules a flush first.
  The cutoff follows the data-progress watermark, so tombstones written in the last
  `gc_grace` before writes stop stay until data advances again. Collection on one replica
  must not be undone by repair: an incoming tombstone below the GC admission floor for a
  key with no local version is acknowledged without being stored
  (`vals_expired_tombstones_skipped`). Otherwise anti-entropy from a replica that has not
  collected yet copies it straight back, and the replicas never all drop it. Blind applies
  can skip it only with the winner directory; without it they store it as before.
  **Former-owner copies** (0515). A node that no longer owns a key (cleanup has not
  forgotten it yet, an aborted re-home target, a sloppy fallback) may hold a put whose newer
  tombstone every owner has collected. The owners' admission floor refuses that put on every
  path that writes it to them (cleanup handoff, read repair). AE does not pull it at all.
  **Copies from owners** (0517) skip the floor: AE pulls from the peer shard that owns the key
  under the committed placement (pulls from a pending re-home target or another shard keep the
  floor), re-home
  copies from the current owner and `valsd backup load` (`BatchApply` flag bit 1, `fill`)
  apply with `LiveFloor::Waive`, so a put older than the grace still reaches a new replica. That rests on
  the liveness invariant: an owner received every delete within the grace, so an old put it
  still holds has no newer collected tombstone. A re-home copy counts only `Won` and `Lost` as
  copied; a refused entry keeps the range from completing.
  **Trusted handoffs and refused copies** (0518). A cleanup handoff applies with `Waive`
  (`BatchApply` with `fill`) when the sender owned the key, as a committed owner, under the
  placement just before the latest change and saw that change less than half the grace ago,
  counted from its last cluster contact before the sweep pass that saw it. If a delete reaches
  every owner within half the grace, a delete the sender missed as an owner is younger than
  the change, so nobody collects it before the window ends. A put that every owner refuses
  (cleanup keeps asking the owners while every answer so far is a refusal), older than half
  the grace, and that no owner holds at that version or newer (a digest read per owner after
  the refusals, since the floor check comes before the key read), goes into an in-memory refused set per zone and shard (65,536 keys; an entry is
  pruned at a lap end only when a read shows the copy gone or changed). Cleanup stops
  offering it, and when a placement adoption (`SetMembers`, `SetZones` or `SetRangeMaps`,
  before the reply; a sweep pass; a refusal noted after one) makes the node an owner (or
  pending re-home target) of the key, the node applies a tombstone at `(hlc + 1, own node id)`,
  so reads and AE under the new placement see the delete. If that write fails, the shard
  answers reads and AE requests like a fenced node until a sweep pass succeeds.
  `valsd backup create` reads every replica directly, so its merge applies the floor itself.
  A put held only by non-owners (or only by a pending re-home target) is merged only if its
  physical time is at or above the `GcFloor` of every owner shard, read after that owner's stream
  passed the key. If that holds, no owner had collected a newer tombstone when it was read, so
  the owner's copy of any delete wins the merge. Otherwise the copy is ambiguous: its delete
  may have been collected, or it was never copied to the current owners. `create` then fails
  unless the operator picks `--stale-copies omit` or `keep` (docs/backup-restore.md).
  Cleanup counts every copy of a non-owned key that a lap moves past without releasing,
  and reports per zone whether a lap under the current ring, started after the latest
  `CleanupStatus` check, released them all. `add-node` and `remove-node` refuse to run
  until every member that stays reports that, so a membership change cannot make such a
  holder an owner, unless the operator passes `--force`.
- **Capped shadow folds** (`engine/merge.rs`, `Engine::shadow_fold_window`, task 0383).
  Overwrite and delete garbage in frozen runs is shadowed by versions in *other* runs,
  so no capped suffix merge sees it. A fold rewrites one **window** of adjacent runs
  (input ≤ `VALS_MERGE_INPUT_CAP_BYTES`, or a single larger run) in place and streams
  the other runs read-only, keys and versions only (`RunVersionStream`):
  - **Sources.** Every run newer than the window. When a GC cutoff is set and the
    window holds tombstones, every older run too, so the source set is complete.
  - **Drop rule.** A window winner is dropped when a newer source holds the key at
    a version ≥ its own, or an older source at a version > its own: the same
    argmax(`version`) / newest-run tie-break every reader uses, so reads cannot change.
  - **Tombstones.** With a complete source set, a winning tombstone below the cutoff
    is collected only when no source holds the key at all. If any older copy
    survives elsewhere, the tombstone stays until that copy is folded away. The
    commit re-checks the memtable and runs flushed during the fold (sources are
    known absent), and a fold whose sources changed is a `CompactionConflict`.
  - **Selection.** The sampled shadow estimate picks the window with the largest
    estimated garbage, starting at a run with at least one segment of it. Garbage is
    run-shadowed value bytes plus the same share of index bytes, plus the index
    share of sampled tombstones below the GC cutoff (so a tombstone-only run is
    selectable). Memtable-shadowed bytes feed the pressure gate but not fold
    selection, since a fold does not read the memtable. Selection requires
    gain ≥ max(`VALS_SHADOW_FOLD_MIN_GAIN_SEGS`, half the window's index segments) and
    headroom for the output. Folds run at the floor too (unpaced there, paced
    otherwise). Victim value segments of the window are relocated as in pressure
    merges; partially dead segments become visible to aged value-log GC.
  - **Execution.** Sources are bound to the maintenance device when the fold runs
    (never the serving device), and each loop turn is bounded by examined window
    entries plus source entries advanced, so a fold that drops everything still
    yields, checks cancellation and charges the pacer. A fold whose realized drop
    in run index plus live value bytes is under half the prediction backs off.
  - **Estimator.** Bloom matches against ~100 newer runs are mostly false positives
    (97 frozen runs with zero overwrites read as 37 MiB of garbage). The sample now
    subtracts the expected false-positive mass: with `p` = 1 − ∏(1 − fpr_j) over newer
    runs (fpr from each run filter's fill), shadowed ≈ (matched − p·sampled)/(1 − p).
    Memtable shadows stay exact.

  **Measured (L1 gate, `frozen_garbage_gate_l1`):** 400k keys, 160 B values,
  0.5 MiB cap, 97 frozen runs after load, 1.5M uniform ops with 20% deletes. The old
  uncapped fold reached 31 MiB inputs (60× the cap), collapsed the frozen tier to 2
  runs and wedged at the floor after 533k ops (3.76× space, 1,001 rejected writes).
  The capped fold ran all 1.5M ops at ≤ 2.70× space with 45–100 frozen runs, every
  fold ≤ the cap and paced, and 152k tombstones collected. With 80% of ops on 10% of
  keys the old path survived only through 30–39 MiB merges (8.6 GiB merge writes,
  max 2.55×); the capped fold wrote 1.0 GiB (max 2.98×). The cost is source reads:
  15–32 GiB of source index-block/sidecar bytes streamed per 1.5M ops. No arm read a
  stale, lost or resurrected key.

  **Measured.** Micro-simulation (`engine/tests/policy.rs`, ~200 MiB flushes, 2 GiB
  cap): merge input per flushed byte is 1.45–1.53× from 10 to 160 GB/engine, with
  101 runs at 160 GB. The pre-0381 rule reaches 307× and 134 GiB merges, holding
  11 runs.
- **Drive layout `plan`** (`drive.rs`, `multi-drive.md`): a node spreads its `P`
  partitions across its `M` drives by capacity-weighted consistent hashing of
  `partition → drive`. It is per-node and deterministic from the node's drive config
  (recovery recomputes it), guarded by the layout fingerprint (`formats.md §3.2`) —
  so it is **not** a cross-node agreement contract like the ring is.
- **Write acknowledgement and group commit** (`coord/mod.rs`, `net/server.rs`,
  `vlog.rs`): `WriteOptions { concern, persistence }`. The `WriteConcern` is
  replica-memory breadth on both single and batch paths: `Unacknowledged`=W0,
  `One`=W1, `Majority`=W(RF/2+1), `Quorum(w)`=W(w), and `All`=W(RF); a `Quorum(0)`
  or a W above the key's replica count is refused with `InvalidWriteOptions` before
  any replica is contacted. `Persistence::Buffered` (the default, also for a bare
  `WriteConcern`) permits each replica acknowledgement after the write is installed
  in memory; `Persistence::Device` (`WriteOptions::device_durable`) delays it until
  device sync. Full vlog blocks submit
  asynchronously (queue depth > 1), reads serve the partial tail from memory, and
  concurrent device-persistent single/batch requests on one shard and zone join one
  barrier after installing their writes. The leader issues one engine sync, then
  releases all joined requests with the same success/failure result, so N in-flight
  requests require O(1) device syncs. `Unacknowledged` waits for no acknowledgements,
  so pairing it with `Persistence::Device` is refused as `InvalidWriteOptions`.

---

## 9. Parameters & defaults

The constants that must agree cluster-wide (top group) and the operational
defaults (bottom group).

| Parameter | Default | Must agree cluster-wide? | Source |
|---|---|---|---|
| `vnodes` (ring points/node) | 128 | **Yes** — PD-authoritative, epoch-checked | `ring.rs:31` |
| `num_buckets` (Merkle) | 4096 | **Yes** — both peers, per reconcile | `antientropy.rs:16` |
| default-zone `rf` | (deployment) | **Yes** — PD-authoritative | — |
| `max_skew_ms` (HLC ε) | 500 | Should match (per-replica check) | `hlc.rs:23` |
| `gc_grace_ms` | 0 (GC off) | Should match (delete/wipe safety) | — |
| `MAX_FRAME` | 64 MiB | protocol bound (both ends) | `net.rs:857` |
| `MAX_CONN_SEND_BUF` | 256 MiB | per-conn blast-radius bound | `net.rs` |
| `PUSH_TIMEOUT` (PD push) | 1 s | operational | `pd.rs:900` |
| connect / per-RPC deadline | 5 s each (`NetTransport`) | operational | `vals-client/src/net/client/transport.rs` |
| client credits | 32,768 ops / 512 MiB global; 8,192 ops / 128 MiB per node (`NetTransport`) | per client | `vals-client/src/net/client/transport.rs` |
| `CLEANUP_MIN_CONFIRMS` | all owners | operational | `node.rs` |
| `SLOPPY_FALLBACK_SLACK` | 2 | operational | `coord.rs` |

"Must agree" means a mismatch silently breaks correctness (disjoint rings, false
Merkle (in)equality, wrong LWW); "should match" means a mismatch degrades a safety
margin (skew tolerance, delete durability) rather than corrupting agreement.

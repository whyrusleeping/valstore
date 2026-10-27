# Multi-drive nodes

Real infrastructure gives a node many independent NVMe drives. This is the model
for how a node uses them. The approach is **JBOD**: each drive is an independent
store, a node distributes its data across its drives, and a single drive failure
loses only that drive's slice (re-replicated from peers) rather than the whole
node.

The **device-level aggregation is built and measured**: `SpdkController::open_shard`
hands each core its own qpair over a disjoint LBA region, and the `mdwrite`/`mdread`
benches attach K controllers and spread partitions across them — 8 drives reach
~8.0M read IOPS / ~29 GiB/s write, ~8× the single-drive ceilings (near-linear). The
*plumbing* also already supports per-shard stores — `Node::serve_sharded` takes
`Vec<(TcpListener, VersionedStore<D>)>`, i.e. each shard supplies its own engine
over its own `BlockDevice`.

**Status: built; named-zone SPDK JBOD path box-verified 2026-07-07 (task 0047,
`bench/zoned-multidrive-box-notes.md`)** — format/boot, `zone_region_mib` sizing +
config validation, unsorted/sorted/default zones, both-drive shard spread, cross-node
AE heal, device-driven zone reopen on `format=false` restart, zone drop→region
reuse→no-resurrection, blank/pre-zoned reopen refusal, and per-partition watermark
persistence all PASS on hardware. **One reproducible bug found (FIXED, tasks 0079
+ 0080):** on a `format=false` restart, a JBOD node's higher-index shard (shard>0)
with a **non-zero** watermark stayed born-fenced permanently against **single-drive
peers**, and — even once it could unfence — could not anti-entropy / hand off /
clean up against a fewer-sharded peer at all. Both stem from the same root cause:
the sweep's reconcile transport talked **same-index** (shard `s` ↔ peer shard `s`),
and a fewer-sharded peer has no shard-`s` address, so shard>0 had no route to it.

**Fix (tasks 0079 + 0080): congruence routing + puller-side shard scoping.** The
sweep no longer talks same-index; our shard `s` of `k` shards reconciles against
the **congruent subset** of a peer's `k'` shard addresses — peer shards `i` with
`i ≡ s (mod gcd(k, k'))` (`congruent_shard_addrs` in the sweep loop). This is
**complete** and **minimal** for every `(k, k')` pair (divisibility not required):

- A key of our shard `s` has `h = hash64(shard_key)` with `h ≡ s (mod k)`, and on
  the peer lives on shard `i = h mod k'`. By CRT `i ≡ s (mod gcd(k, k'))`, and
  every such `i` is realizable — so the congruent subset covers **exactly** the
  peer shards that can hold any of our shard's keys.
- Symmetric `k = k'` degenerates to today's single index-aligned address
  (`i = s`); a single-shard peer (`k' = 1`) gives the lone shard 0 (so a shard>0
  finally has a real AE/watermark route to it); the reverse direction — a
  fewer-sharded node healing from a *more*-sharded peer — is covered by the same
  set (`k = 1` ⇒ every peer shard is congruent, which is right: a 1-shard node
  holds the whole slice the peer spreads across its shards).

Correctness against **mis-sharding does not rely on the peer.** An asymmetric
peer's store folds sibling-slice keys into one Merkle tree, so a tree-diff against
it surfaces keys that belong on *our* sibling shards. The puller's `keep` filter
therefore enforces local shard ownership too — `shard_count > 1 ⇒ hash64(shard_key)
% shard_count == shard_index` (`shard_key` = the key for default/unsorted zones,
the range token for sorted zones, matching the client's `shard_of` / `data_shard`).
The `keep` check runs **before** the phase-2 values fetch, so a sibling-shard
digest is dropped value-free — never adopted onto the wrong shard, and no wasted
device read. Over-replication **cleanup** mirrors the same shard-ownership
component in `is_mine` (unsorted zones now, sorted already did): a key that lands
on a sibling shard of this node is handed off (the handoff transport routes the
`apply` to the key's correct shard, including this node's own sibling shard as a
loopback peer) and then forgotten, instead of kept and churned.

**Cost note.** An asymmetric pair's Merkle trees never match at the root (the
peer's fold includes sibling-slice keys), so a *converged* asymmetric pair still
walks budgeted digest pages each pass (`AE_RECONCILE_MAX_DIGESTS_PER_PASS` + a
resume cursor keyed by `(zone, peer, peer-shard)`) instead of matching at the root.
Symmetric pairs keep the O(root) fast path. That is the accepted price of
heterogeneous shard counts. This **subsumes** the task-0079 watermark-only fallback
transport (now removed): the reconcile transport has a real congruent route to
fewer-sharded peers, so watermark gossip uses it (`GetWatermark` answers on any
shard, fenced or not). Concrete win: a **wiped** shard>0 (beyond-grace rejoin)
whose only peers are fewer-sharded now completes its post-wipe refill from them and
unfences, instead of staying fenced forever. Regressions:
`multidrive_liveness_shard_gt0_unfences_against_single_shard_peers` (0079) and
`multidrive_shard_gt0_refills_from_single_shard_peer_without_missharding` (0080).

**Handoff routes by the peer's real shard count (was a known gap).** Both sweep
transports — reconcile *and* handoff/cleanup — are refreshed every pass from the
membership-borne address book (filled by `SetMembers` pushes), and `add_peer` is
change-detecting, so a coarse static seed (the *unsharded* serve path wires a single
address per peer, `peers → vec![a]`) is **upgraded** to the peer's full shard-address
list once membership is known. A 1-shard node's over-replication cleanup **handoff**
`apply` to a more-sharded peer therefore routes over the real shard list to
`hash64(shard_key) % k'` — the key's correct sibling shard — rather than
unconditionally landing on the peer's shard 0. (Its reconcile transport already routed
congruently: a 1-shard node reconciles against all of a peer's shards.) Regression:
`unsharded_handoff_routes_to_correct_shard_of_more_sharded_peer` (0081). Residual: a
purely static deployment that never pushes membership and only ever supplies one
address per peer still has that lone route — but PD deployments always push the full
per-peer shard lists.

The cluster *policy* is implemented in
[`vals_cluster::drive`](../crates/vals-cluster/src/drive.rs):

- **Layout policy** — [`drive::plan`] spreads `P` partitions across a node's `M`
  drives, each partition an engine over a drive LBA **region**, with
  capacity-weighted **consistent hashing** of partition→drive (a bigger drive
  hosts proportionally more partitions; adding/removing a drive moves only a
  bounded slice). [`drive::DriveSet`] is the backend seam (`MemDriveSet` for
  tests, `SpdkDriveSet` = one controller per drive, `open_shard` per region).
  [`Node::serve_multidrive`] builds the per-partition engines and serves them.
- **Ring capacity weighting** — a node advertises a ring weight ∝ its drive count
  ([`drive::node_weight`]); the [`Ring`](../crates/vals-client/src/ring.rs) places
  `weight × vnodes` points per node, and the weight propagates through the PD
  membership log and the `SetMembers` wire op so every node and coordinator builds
  the identical weighted ring (heterogeneous nodes balance).
- **Intra-node rebalance / fault story** — [`drive::replan`] + [`drive::relocate`]
  move the partitions that changed drives when a drive is added, removed, or fails
  (a failed drive's partitions reappear empty and anti-entropy refills them from
  peers). `relocate` is **crash-safe and streaming**: each moved partition is
  copied one key at a time (bounded memory, no full-partition RAM buffering), and a
  per-move **crash journal** (persisted via the `DriveSet` seam before any
  destructive write, re-persisted after each move durably lands) makes a crash at
  *any* point recoverable — re-running `relocate` with the same old/new layouts
  resumes from the journal. It never buffers a not-yet-copied source in RAM, so a
  crash cannot lose data even at RF=1; a destination region is formatted only after
  the move sourcing from it has durably finished (copy-then-commit ordering).
  In-place slot-swap **cycles** are broken by staging one member into a spare
  scratch slot (or refused with `NoSpareSlot` if none exists), and
  [`drive::verify_layout`] refuses to reopen (`RelocateInProgress`) while a journal
  is present, forcing the operator to finish the move first. Layout is a **pure
  deterministic function** of `(drive caps, region size, P, cores)`, so recovery
  recomputes the identical map (no separate on-disk map needed; a byte codec is
  provided for snapshot/inspection). A **layout fingerprint** (crc of the encoded
  layout) is persisted on first format and re-checked on reopen —
  [`drive::verify_layout`] refuses with `LayoutMismatch` if a
  capacity/geometry change recomputes a different map, catching the foot-gun of a
  capacity-changing cold swap restarted without `replan`+`relocate` (the SPDK
  backend's on-device fingerprint **and** durable relocate journal are a follow-on;
  the seam is inert there for now).
- **Named zones on JBOD** — a multi-drive `backend = spdk` node serves the
  PD-pushed zone **catalog** (not just the default zone). Each partition
  sub-allocates its LBA region into per-zone engines: a small reserved,
  double-buffered, crc-checked **superblock** at the front, then contiguous zone
  sub-regions handed out by the same region allocator the single-drive path uses.
  `zone_region_mib` is the fallback size and repeated per-zone-ID overrides can
  select different sizes for new allocations ([`zonestore::RegionZoneStore`],
  with each zone's engine
  on a [`zonestore::SubDev`] offset view of the shared partition device — no
  per-zone qpair). [`Node::serve_multidrive_zoned`] wires it: the default zone is
  opened up front, named zones materialize lazily from the catalog, boot-reopen
  before serving (the region table persists each zone's ordering, crash-safe:
  a reopen recovers every zone with the right ordering), and anti-entropy heals
  them. **Sorted zones too** on the sharded path, sharded intra-node by range
  token — see below.

Live hot-swap is scoped by serving path. The **non-zoned**
`serve_multidrive` library path ships online add/retire through
`NodeHandle::rebalance_drives` / `NodeRebalancer`; relocation is crash-journaled
and task 0065 closed the post-swap/pre-`Done` durability window. The zoned/JBOD
`valsd` path has no operator/admin integration and remains offline-only: stop,
`replan` + `relocate`, then restart. See
[`ARCHITECTURE.md`](../ARCHITECTURE.md).

## Named zones on the sharded (JBOD) path

A single-drive spdk node already serves the **full** catalog — named *and* sorted
zones, range scans included — via `SpdkZoneStore` over one namespace. The
multi-drive / multi-shard node now serves the full catalog too, each partition
holding its own per-zone engines:

- **Per-partition superblock (format change).** The reserved superblock at the
  front of each partition region means the zoned layout is **not** byte-compatible
  with a pre-zone default-zone-only multidrive node. Reopening one
  (`create = false`) with no valid superblock fails **loud** and directs the
  operator to offline `valsd provision`, rather than silently reading engine data
  as a fresh namespace and clobbering it. Existing default-zone-only multidrive
  nodes must be **offline provisioned** to adopt named zones; absent an override,
  the default zone's capacity is now
  `zone_region_mib` per partition, not the whole partition region.
- **Sorted zones shard by range token.** The client routes a sorted zone's data
  ops and range scans by `hash64(range_token) % shards` (its
  `data_shard`/`scan_shard`), so a range's keys co-locate on one shard (a range
  scan hits one shard, cheaply) while different ranges spread across the node's
  shards/drives — a sorted zone scales out per-node like the unsorted default zone.
  The server matches that routing exactly, and the anti-entropy sweep knows each
  node's shard identity: a key is "still mine" on shard `s` only if this node is a
  ring owner **and** `hash64(range_token) % shards == s`. A runtime split gives the
  new right range its parent's explicit token, so no key moves to another shard or
  ring node. Canonical presplits installed while a zone is empty use lower-bound
  tokens and spread ranges immediately. An operator can likewise run `split-zone
  <zone> <key> canonical` on an empty range; never use that mode on populated data.
  Redistributing a grown zone requires the confirmed re-home protocol tracked by
  task 0142. Unsorted named zones shard by
  `hash(key) % shards` as before.

### Variable zone-region geometry

On the sharded/JBOD SPDK path, `zone_region_mib` remains the required fallback for
zones without an explicit entry. An override is repeated once per stable numeric
catalog ID:

```ini
region_mib = 262144
zone_region_mib = 81920
zone_region_mib_override = 0:1024
zone_region_mib_override = 7:98304
zone_region_mib_override = 8:143360
```

The value is `<zone-id>:<MiB>`. Obtain and record IDs from the committed catalog;
names are not persisted in partition region records. Duplicate IDs, zero,
undersized or segment-misaligned values, arithmetic overflow, and an individual
extent that cannot fit beside the partition superblock are configuration errors.
Overrides are rejected on the single-drive SPDK and file/memory paths rather than
being silently ignored.

Allocation is bounded and contiguous. A new zone uses its override or the
fallback, then takes the first sufficiently large free gap; otherwise it takes
space from the unallocated tail. Adjacent dropped extents coalesce. If no one
extent is large enough, opening the new zone fails loudly with namespace
exhaustion even when smaller fragmented gaps remain. Extent bases and lengths are
segment-aligned relative to the end of the small partition superblock. Recovery
rejects duplicate IDs, overlap, undersized or misaligned extents, out-of-range
ends, and overflow before any engine is opened.

The crc-checked v2 region table already persists each zone's exact `base` and
`len`. On an ordinary restart, those persisted values are authoritative: changing
an override does not resize, move, or truncate an existing zone. The new value
only applies if that ID is allocated after the change (including after a committed
drop). To change existing geometry, stop writers, retain a replayable copy, run
the offline `valsd provision` workflow in `deploy.md`, recreate the catalog in the
same intended form, and replay. Provisioning destroys the old partition contents.

Do not roll a variable layout back to software that only accepts equal fixed
regions. Old readers fail geometry validation when lengths differ; they cannot be
used as an in-place rollback. Rollback is removal of overrides followed by a
fresh format and replay with the old equal-slab geometry. A mixed-version cluster
may replicate at the protocol level, but every process that directly opens a
variable-layout device must run variable-extent-aware code.

Capacity plans must still leave 20–30% maintenance/free headroom in each target
zone and include observed worst-shard placement skew. Overrides remove reserved
empty-slab tax; they do not permit one live engine to grow into a neighbor.

The fresh three-node task-0202 hardware gate measured 39,729 physical writes/s
with equal slabs and 40,101/s with variable extents over the same 3,291-second
real-corpus window (+0.94%), with zero replay-window errors. A changed-config
reopen preserved all persisted lengths and byte-identical target-zone scans; an
oversized fourth zone failed without materializing or affecting existing scans.
See `bench/task-0202-zone-geometry/FINDINGS.md` and its retained raw evidence.

For direct per-engine measurements, the node admin socket accepts `zone-stats`.
It returns CSV rows with `shard,zone_id,used_segments,free_segments,live_keys` and
`resident_runs`. Run it through `sudo valsd admin` when the SPDK node process
created a root-owned socket. Capture all nodes before load, after load, and after
the ordinary reopen; aggregate process gauges are not a substitute for
worst-zone/worst-shard sizing.

## JBOD, not striping

With raw NVMe and no RAID layer beneath us, the choice is: stripe each shard
across all drives (RAID-0 style, one logical space over M devices), or treat
drives as independent (JBOD). **JBOD wins on fault blast radius.**

- *Striping:* one drive failure corrupts every shard (they all stripe across all
  drives) → a single drive failure is as catastrophic as losing the whole node,
  recovered only by re-replicating everything.
- *JBOD:* a drive failure loses only that drive's slice (~1/M of the node),
  re-replicated from peers. This is what Cassandra (`data_file_directories`),
  HDFS DataNodes, etc. do.

The usual striping argument — aggregate bandwidth — we get anyway: a node has
many shards spread across all drives, so the real workload (independent
single-key writes) uses every drive. Only a pathological single-shard hotspot is
capped at one drive's bandwidth, which is the same hotspot ceiling the spec
already accepts (a hot key/shard is one core's worth of throughput).

## The model

### 1. The cluster ring stays node-level

Placement is key → RF **distinct nodes** (unchanged). RF is about *node* fault
tolerance, so a node holds **at most one copy** of any key — on one of its drives.
Drives are entirely node-internal; the cluster layer never sees them, and there is
no new cluster-level state. Fault model:

- Node failure → lose one copy of the keys placed there.
- Drive failure → lose a *fraction* (~1/M) of that one copy → re-replicate just
  that fraction from peers.

### 2. Heterogeneous nodes via capacity weighting

A node with more drives advertises proportionally more **vnodes** on the ring
(an 8-drive node ~2× the vnodes of a 4-drive node), so the ring routes it more
keys. The cluster ring weight is the node's **drive count** (`drive::node_weight`),
not its summed capacity — individual drives never appear on the global ring.
Per-drive *capacity* matters one level down, weighting the intra-node
partition→drive placement (`drive::plan`'s capacity-weighted consistent hash), not
the cluster ring. (Weighting the global ring by summed capacity, so a node with
larger drives takes proportionally more keys, is a possible refinement.)

### 3. A node = M independent drive-engines (JBOD)

One value log + index + allocator per drive (or per drive region). Each is a
`vals_store::Engine` over that drive's `BlockDevice` (the `SpdkBlockDevice` binds
one NVMe namespace). Engines recover independently via log replay.

### 4. Partition = engine over a drive region, pinned to one core

This **subsumes today's `hash % N` server sharding**: a shard is now backed by a
physical drive. "Partition" unifies the three axes the spec kept separate —
**storage region + core + shard**:

- `#partitions ≈ #cores`, distributed evenly across the M drives.
- If `#cores > #drives`, split a drive into multiple LBA-disjoint partitions, each
  its own engine on its own core (its own SPDK per-core qpair to that namespace —
  still shared-nothing, disjoint LBA regions, no lock sharing).
- Each partition is owned by exactly one core; that core serves all requests for
  the keys on it.

### 5. Intra-node routing: key → partition → (drive, core)

A stable hash maps a key to one of the node's partitions (which fixes its drive
and owning core). Use the ring's hashing intra-node so that adding/removing a
drive only remaps a bounded slice of keys rather than reshuffling everything.

## Lifecycle

- **Drive failure** → mark its partitions down; the node serves the rest; the lost
  keys are re-replicated from peers via anti-entropy / rebalance. Cluster capacity
  weight for the node drops.
- **Drive added** → assign it partitions, rebalance the node's keyspace onto it
  (intra-node), raise the node's vnode weight.
- These are rare hardware events; an intra-node rebalance on change is acceptable,
  and stable intra-node hashing keeps the moved set bounded.

## Interactions

- **Zones × drives compounds the small-engine concern.** "Each zone is its own
  engine per node" ([`zones.md`](zones.md)) becomes per *(zone, partition)* — a
  node with Z zones and P partitions has up to Z×P engines. The same mitigations
  apply (lazy engine creation; placement `scope` to bound a small zone's spread),
  but the multiplier is real — monitor it at high zone × partition counts.
- **Anti-entropy** is per-(zone, partition); the existing per-shard sweep
  generalizes directly, since shard = partition.
- **The coordinator stays drive-agnostic** — it still addresses `node:shard`
  exactly as today; the shard just happens to be backed by a specific drive.

## What changed (built)

- `serve_sharded` accepts a per-shard `VersionedStore`; [`drive::build_stores`]
  is the **policy** that builds those stores — one engine per drive region,
  partitions distributed across drives (capacity-weighted consistent hashing),
  one core each. [`Node::serve_multidrive`] ties it together.
- **Capacity weighting** in the ring (vnodes ∝ drive count) via the per-node
  weight in [`Ring`], propagated cluster-wide through the PD + `SetMembers` wire.
- **Intra-node rebalance** on drive add/remove/failure: [`drive::replan`] +
  [`drive::relocate`] (copy the moved partitions; a failed drive's partitions come
  back empty for anti-entropy to refill).
- The **drive → partition → core** mapping is a *deterministic function* of node
  config `(drive caps, region size, P, cores)`, so recovery recomputes the
  identical layout — no separate persisted map (a [`Layout::encode`] byte codec is
  provided for snapshot/inspection).

**Remaining:** expose and validate hot-swap in the zoned/JBOD daemon/operator
surface; that path is still offline-only. Online add/retire is already available
for the non-zoned `serve_multidrive` library path. The zone × partition engine
multiplier at high zone counts also remains.

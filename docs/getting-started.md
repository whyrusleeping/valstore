# Getting started

`vals` is consumed two ways: as a deployable daemon — **`valsd`**, configured
with a file and administered over a socket (see [`deploy.md`](deploy.md)) —
or as a Rust library, where your application embeds `vals-cluster` on the
server side (a `Node` serving a storage engine). The client side is the same
either way: a `Coordinator` over a TCP transport. A **client-only application**
depends on the slim **`vals-client`** crate — the coordinator, the transport,
and the wire types, with none of the server/PD/storage-engine code — rather than
`vals-cluster` (which re-exports it, so `vals_cluster::…` paths also work). This
guide walks the library path from an in-process toy cluster to a real multi-node
deployment; if you just want to run servers, start with `deploy.md` and skip to
[Clients](#clients).

For the design behind everything here, see [`../ARCHITECTURE.md`](../ARCHITECTURE.md);
for byte-level formats and agreement algorithms, see [`formats.md`](formats.md)
and [`algorithms.md`](algorithms.md).

## The model in one minute

- **Placement.** Keys are placed on a consistent-hash **ring**: the replicas of
  a key are the next `RF` distinct nodes clockwise from its hash. There is no
  leader — any coordinator can write any key.
- **Writes.** Each write is stamped with one **version** (hybrid logical clock +
  writer id) and fanned out to all `RF` replicas; it succeeds when `W` of them
  ack, where `W` is the **write concern** you chose (`Unacknowledged`=0, `One`=1,
  `Majority`, `All`=RF). An acknowledgement means the replica installed the write
  in memory unless you ask for device durability (see
  [Write options](#reading-and-writing)). Replicas apply store-if-newer
  (last-writer-wins), so replicas converge in any delivery order.
- **Reads.** A read asks `R` replicas and returns the newest version. Pick
  `R` so that `W + R > RF` and a read always overlaps the latest committed
  write ("strong" read); `R = 1` (`read_any`) is fastest and eventually
  consistent.
- **Healing.** Read-repair, hinted handoff, and a periodic Merkle anti-entropy
  sweep converge replicas in the background; you don't manage any of it.
- **Zones.** Namespaces with their own replication factor, placement scope,
  and key ordering. Zone 0 (`default`) always exists, is unordered (hash
  index), and is what the un-suffixed API methods use. **Sorted** zones
  support globally-ordered range scans.
- **Control plane.** A raft-replicated **PD** (placement driver) owns
  membership, the zone catalog, and sorted-zone range maps, and pushes them to
  every data node (retried until confirmed, re-pushed after node restarts).
  Nothing on the data path waits for consensus.

## Ten minutes, in-process

The fastest way to touch the API is `LocalCluster` — the in-process `Transport`
implementation backed by in-memory block devices. No threads, no sockets. This
exact example is the crate-level doctest of `vals-cluster` (it runs under
`cargo test --doc`):

```rust
use vals_cluster::{Coordinator, LocalCluster, Ring, VersionedStore, WriteConcern};
use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine};

// Three replica stores, as a real cluster would hold — one per node.
let mut cluster = LocalCluster::new();
for node in 1..=3u32 {
    let engine = Engine::create(MemBlockDevice::new(4096, 4096), Config::default()).unwrap();
    cluster.insert(node, VersionedStore::new(engine, node).unwrap());
}

// The placement ring: RF = 3, all three nodes, equal weight.
let mut ring = Ring::new(3);
ring.set_members_weighted([(1, 1), (2, 1), (3, 1)]);

// A coordinator using routing/client label 100, strong reads at R = 2.
// Its LWW writer-incarnation identity is generated independently.
let coord = Coordinator::new(100, ring, 2, cluster);

// The coordinator API is async; drive it with the bundled executor.
vals_rt::block_on(
    async {
        // Write acknowledged by a majority (W = 2), read it strongly back (R = 2):
        // W + R = 4 > RF = 3, so the read must see the write.
        let v = coord
            .write_with(b"hello", b"world", WriteConcern::Majority)
            .await
            .unwrap();
        let got = coord.read(b"hello").await.unwrap().expect("present");
        assert_eq!(got.value.as_deref(), Some(&b"world"[..]));
        assert_eq!(got.version, v);

        // Deletes are versioned tombstones — an older write can't resurrect.
        coord.delete(b"hello", WriteConcern::Majority).await.unwrap();
        assert_eq!(coord.read(b"hello").await.unwrap(), None);
    },
    || {}, // poll_io hook: nothing to poll in-process
);
```

Notes on what you just used:

- `Engine` is the node-local log-structured store; `MemBlockDevice` is the
  RAM-backed `BlockDevice`. Swap in `FileBlockDevice` for a file, or
  `SpdkBlockDevice` (the `spdk` feature) for raw NVMe — every layer above is
  identical.
- `VersionedStore::new(engine, node_id)` wraps the engine with versioned
  last-writer-wins semantics. It does not build a resident key→version map:
  `VersionIndex` caches logical Merkle leaf groups plus dirty-bucket metadata and
  a journal, and seeds run-backed leaves cooperatively when anti-entropy needs
  them. Current versions resolve through the engine (`profile = capacity`) or the
  winner directory (`profile = latency`), always by argmax(`Version`).
- `vals_rt::block_on(future, poll_io)` is the executor entry point. The second
  argument is the I/O poll hook; for real networking pass
  `vals_cluster::reactor::poll_io` (below).

## A real cluster over TCP

A deployment has three roles, usually three groups of processes: **data
nodes**, the **PD**, and **clients**. One process can host several roles
(the PD and a data node co-locate fine).

A `valsd` data node also selects `profile = capacity|latency`. `capacity` is the
default: resolving unconditional writes and the lower-RAM out-of-core point
index. `latency` couples blind unconditional writes with the compact winner
directory: accepted candidates optimistically reply `Won`, and startup performs
an O(dataset) synchronous directory rebuild before serving. The retained mature
corpus measured **62.9 additional RSS bytes per physical key**, so budget RAM and
restart time explicitly. Formats are compatible and profiles may be mixed for a
rolling rollout, but replica-level clients must count both `Won` and `Lost` as
unconditional-write acknowledgements. See [the deployment profile
reference](deploy.md#data-node-deployment-profiles) before selecting latency or
rolling back.

### Data nodes

Each node serves a `VersionedStore` over TCP and runs its own healing loops:

```rust
use std::time::Duration;
use vals_cluster::{Node, Ring, VersionedStore};

vals_cluster::observe::init_stderr_logger();          // logs, level via VALS_LOG
vals_cluster::init_cluster_secret(b"...shared...");   // gate control ops (see Operating)

// The boot ring only needs to be roughly right — the PD pushes the
// authoritative (epoch'd) ring config to every node shortly after start.
let mut ring = Ring::new(3);
ring.set_members_weighted([(1, 1), (2, 1), (3, 1)]);

let (listener, addr) = Node::bind()?; // or bind a specific SocketAddr yourself
let handle = Node::serve(
    1,                         // this node's id
    listener,
    store,                     // VersionedStore over your BlockDevice
    ring,
    vec![(2, addr2), (3, addr3)], // boot peers (the PD grows this set later)
    Duration::from_secs(5),    // anti-entropy sweep cadence
);

// ... on SIGTERM:
handle.shutdown(); // drain reads, flush conns, sync engines, persist watermark
```

`Node::serve` is the single-zone baseline. The variants compose the same way
and all return a `NodeHandle`:

- `Node::serve_zoned(...)` — multi-zone node: takes a `ZoneStore` provider
  (`MemZoneStore` / `FileZoneStore` / `SpdkZoneStore`) + a boot `Catalog`;
  zone engines materialize lazily when the PD creates zones.
- `Node::serve_sharded(...)` — thread-per-core: N independent shards
  (listener + store each); the client transport routes keys to shards.
- `Node::serve_multidrive(...)` — JBOD: spreads P partitions across the
  node's drives, weights the ring by capacity (see
  [`multi-drive.md`](multi-drive.md)).
- `Node::serve_with_liveness(...)` — adds the wipe-before-rejoin lease so a
  node that was down longer than the tombstone-GC grace re-syncs instead of
  resurrecting deletes. Use this whenever you enable tombstone GC
  (`with_gc_grace_ms` on the store).

### The PD (control plane)

One PD daemon — or three for fault tolerance — owns membership and the
catalog. Production PDs should use the **durable** constructors (raft state
survives restarts):

```rust
use std::time::Duration;
use vals_cluster::pd::{PdDaemon, RingConfig};

// Single-PD (durable): fine to start with; raft-replicate later.
let pd = PdDaemon::spawn_single_durable(
    1,
    pd_engine,                          // its own small Engine (any backend)
    RingConfig { vnodes: 128, rf: 3 },  // the authoritative ring shape
    vec![(1, vec![addr1]), (2, vec![addr2]), (3, vec![addr3])],
    "/var/lib/vals/pd",                 // raft state directory
)?;

// Bootstrap an empty RF=3 deployment atomically; adding the first member alone
// would violate RF. This boot intent is retried through leader election.
pd.bootstrap_members(vec![
    (1, 1, vec![addr1]),
    (2, 1, vec![addr2]),
    (3, 1, vec![addr3]),
]);
while pd.members() != [1, 2, 3] {
    std::thread::sleep(Duration::from_millis(10));
}

// Administrative mutations are leader-only and return validation/proposal errors.
// Success means proposed, not committed; poll the committed view when ordering
// subsequent external actions depends on the change.
pd.add_node_addressed(4, 1, vec![addr4])?;
while !pd.members().contains(&4) {
    std::thread::sleep(Duration::from_millis(10));
}
pd.remove_node(2)?;
while pd.members().contains(&2) {
    std::thread::sleep(Duration::from_millis(10));
}
let zone_name = zone_config.name.clone();
pd.create_zone(zone_config)?;
while pd.catalog().by_name(&zone_name).is_none() {
    std::thread::sleep(Duration::from_millis(10));
}
pd.shutdown(); // on exit
```

A 3-replica PD group uses `PdDaemon::spawn_durable(id, listener, pd_peers,
data_nodes, engine, ring, campaign, dir)` — same API, raft over TCP between
the PD replicas. Send administrative mutations to the leader; a follower returns
`PartitionError::NotLeader`, and a newly elected leader may briefly return
`PartitionError::NotReady` while it catches up. A successful administrative call
means Raft accepted the proposal, not that it is committed or pushed, so observe
`members()`, `catalog()`, or `range_map()` when the distinction matters. The
startup-only `bootstrap_members` call is different: it queues one atomic initial
membership intent and retries it until the PD can propose it.

Membership changes carry each node's shard addresses, so a node added with
`add_node_addressed` becomes reachable to everyone — config push,
anti-entropy, handoff — with no restarts anywhere.

### Clients

A client is a `Coordinator` over a `NetTransport`. A client-only application
depends on **`vals-client`** (`use vals_client::…`); a server binary already has
these re-exported through `vals-cluster` (`use vals_cluster::…`). Either import
path works — the example uses `vals_client`:

```rust
use vals_client::{Coordinator, NetTransport, Ring};
use vals_client::reactor;

// Installs the finite production deadline and write-admission defaults.
let transport = NetTransport::connect(&[(1, addr1), (2, addr2), (3, addr3)]);

let mut ring = Ring::new(3);
ring.set_members_weighted([(1, 1), (2, 1), (3, 1)]); // boot guess
let coord = Coordinator::new(100, ring, 2, transport);

vals_rt::block_on(
    async {
        // Adopt the PD's authoritative ring + zone catalog from any live node.
        coord.refresh_ring(1).await?;
        coord.refresh_catalog(1).await?;

        let v = coord.write(b"k", b"v").await?;
        let got = coord.read(b"k").await?;
        // ...
        Ok::<_, vals_client::CoordError>(())
    },
    reactor::poll_io, // the epoll-backed network poll hook
)?;
```

Things to know:

- **Finite client defaults.** `NetTransport::connect` uses a 5 second
  connect/hello deadline and a fresh 5 second deadline for each RPC after the
  hello completes. A coordinator built over that transport also limits
  outstanding writes to 32,768 operations / 512 MiB globally and 8,192
  operations / 128 MiB per destination node. `ClientConfig` customizes these
  values; `connect_with_config` and `connect_sharded_with_config` validate that
  deadlines and budgets are positive and per-node budgets do not exceed the
  global ones. Newly learned peers inherit the same setting. The compatibility
  builder `with_rpc_deadline(d)` sets both connect/hello and RPC deadlines to `d`;
  use `ClientConfig` to configure them independently.
- **Stale routing self-heals.** Every data op carries the coordinator's ring
  epoch; a replica running under a PD config refuses an older-epoch op (an
  unstamped epoch-0 op included), so a stale or unrefreshed client can't route
  onto the wrong replica set. By default the coordinator handles a refusal
  itself: it adopts the current config from a live member and retries the op
  once, so a reconfiguration under you looks like one slightly slower op, not
  an error. `Coordinator::with_manual_refresh()` opts out — the op then fails
  with `CoordError::StaleRing` (carrying the epoch you need) for you to handle.
- **Still refresh at startup.** `refresh_ring`/`refresh_catalog` adopt the PD's
  current config (and learn new members' addresses). Calling them once up front
  means the first ops route correctly instead of paying a refusal + retry, and
  it is how a coordinator built from a boot-guess ring converges before
  traffic. (Automatic *periodic* refresh is on the backlog.)
- Coordinator methods take `&self` and pipeline: issue many writes
  concurrently (`vals_rt::join_all`) from one coordinator, or run several
  coordinators (one per core) against the same cluster — they are independent
  by design.
- Use multiple coordinators for scale-out; a single one is eventually
  CPU-bound around ~100–200k ops/s.

## Reading and writing

**Write options.** Every write, delete, batch and range delete takes
`WriteOptions { concern, persistence }`, or a bare `WriteConcern`, which means
buffered persistence. `write(k, v)` is `WriteOptions::default()`, i.e.
`WriteOptions::buffered(WriteConcern::All)`.

The `WriteConcern` picks `W`, how many replicas must acknowledge:

| `WriteConcern` | W | Meaning |
|---|---|---|
| `WriteConcern::Unacknowledged` | 0 | fire-and-forget (fastest, no ack) |
| `WriteConcern::One` | 1 | one replica acknowledgement |
| `WriteConcern::Majority` | ⌊RF/2⌋+1 | strict majority — pairs with a majority read for `W+R>RF` at every RF |
| `WriteConcern::All` | RF | every replica acknowledges |
| `WriteConcern::Quorum(n)` | n | explicit W, `1 <= n <= RF` (prefer `Majority`) |

`Persistence` says what an acknowledgement means:

| `Persistence` | Constructor | A replica acknowledges after |
|---|---|---|
| `Buffered` (default) | `WriteOptions::buffered(c)` or a bare `c` | installing the write in memory |
| `Device` | `WriteOptions::device_durable(c)` | the shard's coalesced device sync that covers the write |

The default is the high-throughput buffered path: an acknowledged write survives
the loss of any replica process or machine as long as one acknowledging replica
stays up, but a correlated power loss or crash of every acknowledging replica can
lose it (and a lost delete can let an older durable value return). Ask for
`device_durable` when an acknowledgement must survive that.

Options that cannot be met are refused with `CoordError::InvalidWriteOptions
{ options, replicas }` before anything is sent: `Quorum(0)`, `Quorum(n)` above
the key's replica count, and `Unacknowledged` with `Device` (there is no
acknowledgement to wait for). W is never silently clamped. In a batch, the
affected entries fail with that error and `acks: 0`; the others are written. See
[Batch writes](batch-writes.md) for the exact guarantee matrix.

```rust
use vals_client::{WriteConcern, WriteOptions};

coord.write(b"k", b"v").await?; // All replicas, buffered
coord.write_with(b"k", b"v", WriteConcern::Majority).await?; // majority, buffered
coord
    .write_in(zone, b"k", b"v", WriteOptions::device_durable(WriteConcern::Majority))
    .await?; // majority, each ack after device sync
```

**Migrating from `Durability` (task 0242).** Behaviour is unchanged except for
the rejected options above; only names moved:

| Before | After |
|---|---|
| `Durability` | `WriteConcern` |
| `NoSync` / `OneNode` / `Replicated` | `Unacknowledged` / `One` / `All` |
| `Majority`, `Quorum(n)` | unchanged, but `Quorum(n)` with `n` = 0 or above the replica count is now an error instead of `min(n, RF)` |
| `write_tier(k, v, t)` / `write_tier_in(z, k, v, t)` / `try_write_tier` | `write_with(k, v, t)` / `write_in(z, k, v, t)` / `try_write` |
| `write_tier[_in]_with_persistence`, `delete[_in]_with_persistence`, `write_batch[_in]_with_persistence`, `delete_range_in_with_persistence` with `(.., t, Persistence::Device)` | `write_with`/`write_in`, `delete`/`delete_in`, `write_batch`/`write_batch_in`, `delete_range_in` with `WriteOptions::device_durable(t)` |
| `*_with_persistence(.., t, p)` with a variable `p` | `WriteOptions { concern: t, persistence: p }` |

The Go client (`valsgo`, in `vals-examples`) and its callers bootes and
real-client are unchanged by this rename and keep their current contract: `W`
comes from `Config.W` (0 selects a majority of the zone's RF; a `W` above the
replica count fails rather than being reduced). `Put`/`Delete` acknowledge from
replica memory (buffered); `PutBatch`, the path bootes and real-client write
through, always requests device-synced acknowledgements (`BatchApply sync=1`). A
Go caller that needs a durable single write uses a one-entry `PutBatch`.

**Reads**, weakest to strongest:

- `read_any(key)` — one replica, fastest, eventually consistent.
- `read(key)` / `read_in(zone, key)` — the coordinator's configured `R`.
- `read_quorum(key, r)` — explicit `R` per call.

Strong reads default to **digest mode** (`ReadFanout::Digest`): one replica
returns the value, the rest return tiny version digests served from memory —
~1 device read per strong read instead of `R`. `with_read_fanout` selects
`All`/`Quorum` if you want value fan-out for tail latency instead.

**Versions and CAS.** Every write returns the `Version` it committed at;
`Stored` carries the version a read observed. `cas(key, value,
expected: Option<Version>)` does a majority read-check-write —
**best-effort optimistic concurrency, not linearizable** (two racing CAS ops
can both pass the check; see `ARCHITECTURE.md` §Consistency model). Use it
for low-contention coordination, not for a counter.

**Batches.** `write_batch(&[(k, v)], options)` /
`write_batch_in(zone, writes, options)` group writes by destination node. With
the default `Buffered` persistence they perform no forced fsync;
`WriteOptions::device_durable(concern)` joins a coalesced
per-shard/zone barrier, so concurrent batches share device syncs instead of each
batch forcing its own. Per-write outcomes come back 1:1 with the input.

**Sloppy quorum** (`write_sloppy`/`delete_sloppy`) is the opt-in
availability-over-consistency mode: if home replicas can't make `W`, the
write spills to fallback nodes and hands hints to the homes. The returned
`SloppyAck` tells you exactly what happened (`home_acks`/`fallback_acks`/
`hinted`, `is_strict()`); a sloppy ack is *eventual*, not strong, until the
hints drain home.

**Backpressure.** Network coordinators inherit the finite defaults above.
Credits count each write operation and its key plus value bytes; deletes count
the key. Per-node charges follow every home destination, including batches.
Temporary exhaustion waits fairly, except that work for a healthy destination
may bypass an earlier waiter blocked only by another node. A request larger
than any applicable limit and `try_write` under current exhaustion return
`Overloaded` immediately. See [`backpressure.md`](backpressure.md).

## Zones and range scans

```rust
use vals_cluster::{Ordering, ZoneConfig};

// Admin side: validate and propose through the PD. Success is not yet a commit.
pd.create_zone(ZoneConfig {
    name: "events".into(),
    rf: 3,
    ordering: Ordering::Sorted,
    scope: None, // place on all members
})?;

// First observe the zone in the PD's committed catalog.
let zone = loop {
    if let Some(zone) = pd.catalog().by_name("events") {
        break zone;
    }
    std::thread::sleep(std::time::Duration::from_millis(10));
};

// Delivery to data nodes follows commit. Retry the refresh until this node
// serves the new catalog before using the zone through the client.
loop {
    coord.refresh_catalog(1).await?;
    if coord.catalog().by_name("events") == Some(zone) {
        break;
    }
    std::thread::sleep(std::time::Duration::from_millis(10));
}

coord.write_in(zone, b"2026-06-11/42", b"...", WriteConcern::Majority).await?;

// Sorted zones scan: globally ordered, strong when r pairs with W (W+R>RF),
// `limit` caps live results.
let rows = coord
    .scan_in(zone, Some(b"2026-06-01/"), Some(b"2026-07-01/"), 2, Some(1000))
    .await?;
```

A sorted zone's keyspace is divided into contiguous **ranges**, each placed on
the ring by its lower-bound token, so keys in a range co-locate (cheap scans)
while different ranges spread across nodes and drives. Ranges start as one and
grow by explicit splits: `pd.split_zone(zone, b"2026-06-15/".to_vec())?`. Scans
of any size work — replica replies are paged under a server byte budget and the
coordinator reassembles them (a `limit` also caps what is pulled, not just what
is returned). See [`zones.md`](zones.md) for the full design.

**Range deletes** (sorted zones only) remove every key in `[lo, hi)` with one
versioned range tombstone per range instead of one tombstone per key:

```rust
// Consumed everything below the cursor: delete the whole prefix [base, cursor).
let v = coord
    .delete_range_in(zone, Some(b"queue/"), Some(b"queue/0000001234"), WriteConcern::Majority)
    .await?;
```

- `lo` is inclusive, `hi` exclusive, `None` unbounded, as for `scan_in`; an empty
  span is `InvalidRange`, an unsorted zone `NotSorted`. The call stamps one version
  V. Keys whose newest write is older than V read as absent and drop out of scans;
  a write newer than V wins as usual; a late write older than V is discarded.
- Replicas skip covered spans, so once a replica has folded the delete (normally
  milliseconds; the replica does one fold turn before it acknowledges), a scan that
  starts at the head of a consumed queue costs the same however much has been
  deleted: a `limit = 10` scan after 200k deleted entries pages exactly as it does
  after 10k. Before the fold, covered entries are filtered one by one, in pages
  bounded by a covered-work budget.
- Like a batch it is not atomic across ranges: `RangeDeleteIncomplete { version,
  missed }` names the range slices that missed `W`. Retrying the whole call is
  safe. `WriteOptions::device_durable(concern)` waits for the device sync, as for
  writes.
- Every replica must run a build with range deletes (hello feature bit 3);
  otherwise the call fails with `Unsupported` before anything is written. Clients
  without bit 3 (the Go client before 0468) get an error from a scan whose window
  meets a range tombstone. The Go client issues range deletes with
  `DeleteRange(zone, lo, hi, durable)`, with the same rules and `W` from
  `Config.W`.
- **Prefer cumulative deletes.** Deleting `[base, cursor)` with a rising cursor
  keeps a single fragment per zone engine. Deleting many disjoint spans keeps one
  fragment each, kept until `gc_grace` collects them (forever at the default
  `gc_grace = 0`, as for point tombstones), up to 65,536 per zone engine, after
  which further disjoint deletes are refused.

Design: [`range-deletes.md`](range-deletes.md).

## Operating it

- **Auth.** Call `vals_cluster::init_cluster_secret(secret)` once at startup
  in **every** process (nodes, PDs, clients that issue control ops). It gates
  the mutating control ops (`SetMembers`/`SetZones`/`SetRangeMaps`) and the PD
  raft port. Unset = open, for tests. The token is cleartext on the wire —
  it stops stray/unprivileged senders, not an on-path attacker; run on a
  private network or an overlay such as Tailscale/WireGuard; vals does not
  encrypt its transport. Data ops are not authenticated.
- **Logging & metrics.** `observe::init_stderr_logger()` (level via
  `VALS_LOG=info|debug|...`), or install any `log`-facade logger you like.
  `observe::snapshot()` returns the process-wide counters (writes/reads,
  read-repairs, hints, wipes, denials, compactions, PD pushes...) — export
  them however you export metrics.
- **Deadline, retry, and partial-write semantics.** Connect/hello and RPC
  deadlines are on by default. A timeout is a failed replica response, so a
  quorum operation can still succeed from enough other replicas; otherwise it
  returns the existing quorum failure. The client cannot know whether a timed
  out write reached that replica, so retry only operations whose LWW/idempotency
  contract permits it. Automatic stale-ring handling retries once after a
  successful refresh; timeout and ordinary transport failures are not retried
  automatically. Dropping a coordinator future cancels local waiting and
  releases its credits, but cannot retract bytes already sent or a write already
  applied remotely. `CoordinatorService::submit_write_batch` is detached once
  submitted, so dropping its handle does not cancel that batch. Pair deadlines
  with `Coordinator::with_failure_detector` to route around repeatedly failing
  nodes.
- **Benchmark opt-out.** `NetTransport::connect_unbounded` and
  `connect_sharded_unbounded` explicitly restore the old no-deadline,
  no-coordinator-budget behavior for controlled benchmarks. In-tree
  `vals-bench` call sites use these names so their historical load shape is not
  silently changed. External Rust consumers should either adopt the finite
  defaults or pass a reviewed `ClientConfig`; this does not alter independent
  Go-client behavior.
- **Tombstone GC + liveness.** Deletes leave versioned tombstones. To reclaim
  them, set a grace (`with_gc_grace_ms`) — and then serve with the liveness
  lease (`serve_with_liveness`), which wipes-and-resyncs a node that rejoins
  after being unreachable longer than the grace (otherwise it could resurrect
  GC'd deletes). The lease's grace is stamped onto every engine by
  construction; the serve parameter is the single source of truth.
- **Shutdown.** `NodeHandle::shutdown()` and `PdDaemon::shutdown()` drain and
  sync; a rolling restart is then distinguishable from a crash (the PD's
  restart beacon re-pushes config to a restarted node within ~1s either way;
  it compares both the ring and the zone-catalog epoch, so a membership push
  that reaches the node first does not hide the restart).
- **Version-gated formats.** Every data connection negotiates the version/features
  from its `PROTO_VERSION` hello. On-disk containers carry `[magic][u16 version]`
  envelopes and unknown versions fail loud. [`formats.md`](formats.md) is the
  current-contract snapshot; versioning makes compatibility decisions explicit,
  but is not an unlimited backward-compatibility promise. Check release notes and
  use rolling mixed-version deployment only where that combination is supported.

## Storage backends

| Backend | Construct | Use for |
|---|---|---|
| `MemBlockDevice` | `MemBlockDevice::new(block_size, num_blocks)` | tests, toys |
| `FileBlockDevice` | file-backed | durable single-box dev |
| `SpdkBlockDevice` | `spdk` feature, see [`SPDK.md`](SPDK.md) | raw NVMe, the real target |

The whole distributed layer is generic over `BlockDevice`, so everything in
this guide behaves identically on all three — develop against mem/file, deploy
on SPDK.

## What's deliberately not here

- **Multi-key transactions / cross-key ordering** — given up in the leaderless
  pivot (see [`../ARCHITECTURE.md`](../ARCHITECTURE.md)). An opt-in linearizable-zone design is
  parked in [`future-linearizable-zones.md`](future-linearizable-zones.md).
- **Linearizable CAS** — `cas` is best-effort (above).
- **Auto-eviction** — the failure detector only routes around a down node;
  removing it from the ring is an explicit `pd.remove_node` (auto-eviction is
  a backlog item).
- **Non-Rust clients** — the wire contract is version-negotiated, but no unlimited
  backward-compatibility promise or packaged non-Rust client is provided. Client
  packaging is tracked in the ycc backlog ([`../backlog.md`](../backlog.md)). A
  server binary exists: `valsd`, see [`deploy.md`](deploy.md).

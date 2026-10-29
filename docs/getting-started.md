# Getting started

Start by running **`valsd`** as a database service, then connect your
application. This guide covers that path and the distributed data model used
by both daemon deployments and embedded nodes.

| What you want | Start here |
|---|---|
| Run a database service | [Local daemon deployment](#run-the-daemon), then [`deploy.md`](deploy.md) |
| Connect an application to that service | [`vals-client` over TCP](#clients), or the [Tokio bridge](#tokio-applications) |
| Store data inside your own Rust process | [Embedding guide](embedding.md): local engine or distributed node |

Using a Rust client library is **not** embedding the database: `valsd` owns
the storage and server lifecycle, and your application only sends requests.
The separate [embedding guide](embedding.md) covers `vals-store` and hosting
`vals-cluster` in your own service. You do not need their constructors to run
or use the daemon.

## Run the daemon

Use the [README quick start](../README.md#run-it-a-local-three-node-cluster)
to build `valsd` and the `kv` example, start three local file-backed data nodes
and one PD, and send traffic. No SPDK or dedicated drives are needed. The
PD (placement driver) manages membership and namespaces; the data nodes hold
your records. File-backed records can survive restarts, but write
acknowledgements are buffered unless the client requests
[device durability](#reading-and-writing).

This one-host setup is for evaluation, not high availability. A production
HA layout uses three voting PDs on independent hosts or failure domains, usually
alongside the data nodes. Growing the demo's single PD into a three-voter
group in place is unsupported. To run a small deployment and grow it later,
follow [growing a small deployment](growing.md): a minimal one-node setup,
zone sizing, adding data nodes, and the backup/restore migration to three PDs.

For configuration, persistence and sizing, profiles, systemd, monitoring,
backups and failure recovery, use the [operator guide](deploy.md). Keep data
ports inside an enforced trusted network boundary: the cluster secret does
not authenticate data reads or writes ([security model](../SECURITY.md)).

## Clients

A remote client uses `vals-client` for quorum coordination, transport and wire
types, plus `vals-rt` to drive it. It needs no storage-engine or server crate.
Clients of an embedded networked node use this same API. The crates are not
on crates.io; use git dependencies:

```toml
[dependencies]
vals-client = { git = "https://github.com/whyrusleeping/valstore" }
vals-rt     = { git = "https://github.com/whyrusleeping/valstore" }
```

For storage in the application process instead, see the [embedding guide](embedding.md).

**Try it from the command line.** `vals-client` ships a small runnable
client, the `kv` example. With the local cluster from the
[README quick start](../README.md#run-it-a-local-three-node-cluster) listening on
127.0.0.1:7001-7003, run it from the repository root:

```sh
cargo run -p vals-client --example kv -- put hello world
cargo run -p vals-client --example kv -- get hello
cargo run -p vals-client --example kv -- delete hello
cargo run -p vals-client --example kv -- --zone events put 2026-06-11/42 click
cargo run -p vals-client --example kv -- --zone events scan 2026-06-01/ 2026-07-01/
```

```text
$ cargo run -q -p vals-client --example kv -- put hello world
ok, version 1793141744239.0 writer 50e809714d38d9a1
$ cargo run -q -p vals-client --example kv -- get hello
world
version 1793141744239.0 writer 50e809714d38d9a1
```

The `kv` example writes and deletes with **buffered majority** acknowledgements,
not device syncs, and reads and scans from a majority. A quorum read sees an
acknowledged candidate's version or a greater LWW version, not necessarily the
last request to finish. See [the shared contract](#the-model-in-one-minute).
For another cluster, pass `--nodes ID@HOST:PORT,...`; one live seed node is
enough to discover the membership and zone catalog, but operations still need
their replica quorums. The client needs no secret: the cluster secret gates
only control operations (see [Operating it](#operating-it)). `--help` lists
the options, and [`kv.rs`](../crates/vals-client/examples/kv.rs) is a complete
client program to copy from.

**In your own code**, a client is a `Coordinator` over a `NetTransport`.
This complete program talks to the README's RF3 cluster, requests
**device-durable majority** writes, and derives the read majority from the
discovered default-zone RF. The initial ring is only a boot guess;
`refresh_ring` and `refresh_catalog` adopt the current configuration.
For seed fallback and fuller error handling, copy `kv.rs` or the
[Tokio example](#tokio-applications) instead.

```rust
use vals_client::{
    reactor, Coordinator, NetTransport, Ring, WriteConcern, WriteOptions, DEFAULT_ZONE,
};

fn main() {
    let nodes = [
        (1, "127.0.0.1:7001".parse().unwrap()),
        (2, "127.0.0.1:7002".parse().unwrap()),
        (3, "127.0.0.1:7003".parse().unwrap()),
    ];
    let transport = NetTransport::connect(&nodes);
    let mut ring = Ring::new(3);
    ring.set_members_weighted([(1, 1), (2, 1), (3, 1)]);
    let coord = Coordinator::new(100, ring, 2, transport);

    vals_rt::block_on(
        async {
            coord.refresh_ring(1).await?;
            coord.refresh_catalog(1).await?;
            let majority = coord.ring().rf() / 2 + 1;
            let options = WriteOptions::device_durable(WriteConcern::Majority);
            let version = coord.write_in(DEFAULT_ZONE, b"hello", b"world", options).await?;
            let got = coord.read_quorum(b"hello", majority).await?.expect("present");
            assert_eq!(got.value.as_deref(), Some(&b"world"[..]));
            assert_eq!(got.version, version);
            Ok::<_, vals_client::CoordError>(())
        },
        reactor::poll_io,
    ).unwrap();
}
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
  traffic. For periodic refresh, host the coordinator in a
  `CoordinatorService` built `with_proactive_refresh(period)`; the refresh
  runs while you drive operations through `CoordinatorService::run`.
- Coordinator methods take `&self` and pipeline: issue many writes
  concurrently (`vals_rt::join_all`) from one coordinator, or run several
  coordinators (one per core) against the same cluster — they are independent
  by design.
- Use multiple coordinators for scale-out; a single one is eventually
  CPU-bound around ~100–200k ops/s.

### Tokio applications

The network coordinator is thread-local (`!Send` and `!Sync`), and its sockets
need the vals reactor to make progress. Do not put a coordinator in shared
Tokio application state or call `vals_rt::block_on` on a Tokio worker thread.
Instead, create and drive it on a dedicated OS thread, and share a channel
handle with your application tasks.

[`examples/tokio_bridge.rs`](../crates/vals-client/examples/tokio_bridge.rs)
is a complete example of that boundary. With the local cluster above running:

```sh
cargo run -p vals-client --example tokio_bridge
# Or seed discovery from another cluster:
cargo run -p vals-client --example tokio_bridge -- --nodes 1@127.0.0.1:7001,2@127.0.0.1:7002,3@127.0.0.1:7003
```

The example discovers membership and the zone catalog, then demonstrates
put/get/delete from Tokio tasks using a cloneable, `Send` handle. It uses the
default zone, derives the read majority from its replication factor, and
requests device-durable majority acknowledgements for writes and deletes.
Reads select the greatest LWW version, not the last request to finish; neither
the bridge nor device durability makes CAS or a group of operations atomic.
Tokio is an example-only development dependency, not a production dependency
of `vals-client`.

The bridge is an integration pattern to copy, not a new supported client API
or a throughput benchmark:

- A bounded request channel applies backpressure before the worker accepts
  more work. It bounds the number of queued requests, not their total bytes
  or the number of application tasks waiting to send; bound payload sizes and
  application concurrency too.
- One worker executes one operation at a time. A private current-thread
  Tokio runtime parks its idle inbox; the vals reactor drives each operation
  on that same dedicated thread. Slow replicas do not block an application
  Tokio worker, but they delay every request queued behind the current one.
  Queueing, connect/hello, RPC and any stale-ring refresh/retry all contribute
  to operation time; an application deadline must cover the whole wait.
- Dropping a caller skips its request if the worker has not started it yet,
  but cannot retract an operation already started. A timeout, write error or
  lost worker reply is not proof that a put or delete failed to take effect;
  automatic application retries must respect the LWW/idempotency contract.
- A long-idle connection may have been closed by the server (120 seconds by
  default). The first write after that can fail even when the cluster is
  healthy. The transport retries an idempotent read once on a fresh connection,
  but never automatically resends a write. Handle that failure explicitly.
- The coordinator's automatic stale-ring refresh/retry remains enabled. The
  example's numeric read majority is computed at startup, however; re-create
  the bridge if the default zone's replication factor changes.
- Shut down the bridge explicitly and await its worker join before ending the
  application. Stop request producers first. Startup, operation and worker
  failures are returned to the application rather than hidden in a detached
  background task.

For a web service, construct the bridge once at startup, put clones of its
channel handle in request-handler state, and keep its worker owner for
application shutdown. To scale beyond this serial example, use the
coordinator's pipelining/batch APIs on the vals thread or multiple independent
workers; do not move a live coordinator between threads.

## The model in one minute

This is the distributed contract, whether replicas run in `valsd` or inside
an application. The [local engine](embedding.md) alone does not provide
replication or quorum semantics.

- **Placement.** Keys are placed on a consistent-hash **ring**: the replicas of
  a key are the next `RF` distinct nodes clockwise from its hash. There is no
  data leader — any coordinator can write any key.
- **Writes.** Each write is stamped with a **version** (hybrid logical clock +
  writer id) and sent to all `RF` replicas. It succeeds when `W` acknowledge,
  according to the chosen write concern. Unless you request device durability,
  an acknowledgement means the candidate is installed in memory. The greatest
  LWW version wins, not necessarily the last request to complete.
- **Reads.** A read asks `R` replicas and chooses the greatest version. With
  strict home-replica writes, `W + R > RF` makes the read overlap an
  acknowledged write and observe its version or a greater one. This is not
  linearizability or an atomic read-modify-write guarantee. `R = 1` reads are
  eventually consistent when they do not meet that overlap condition.
- **Healing.** Read-repair, hinted handoff and periodic Merkle anti-entropy
  help replicas converge in the background. Operators still need to monitor
  repair progress and failures.
- **Zones.** Namespaces with their own replication factor, placement scope
  and key ordering. Zone 0 (`default`) always exists and is unordered. Sorted
  zones support globally ordered range scans.
- **Control plane.** A Raft-replicated **PD** owns membership, zone catalogs
  and sorted-zone range maps, and pushes them to data nodes. It is not on the
  steady-state data path; see [PD availability and recovery](deploy.md#pd-availability-and-recovery)
  for what stops working during a control-plane outage.

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

Create namespaces through the PD's admin socket, not by constructing a PD
inside your client. The quick start already creates the RF3 sorted `events`
zone. For example, to create another sorted zone named `archive`, run on the
current PD leader's host:

```sh
valsd admin -s /path/to/pd.sock create-zone archive 3 sorted
valsd admin -s /path/to/pd.sock zones
```

For `create-zone`, an `ok committed` reply confirms the catalog mutation
committed; delivery to data nodes follows. Other commands can report only
queued intent, so check the reply rather than treating every successful exit
as a commit. After creating a zone, refresh the client's catalog from a live
seed until it appears, retrying with backoff if the push is still pending.
See [`pd-admin.md`](pd-admin.md) for leader discovery and ambiguous outcomes.

Within the async client block above, the existing `events` zone can be used
like this. The RF3 majority is 2; choose R per zone if RFs differ. The result
limit bounds live rows, not a cross-key snapshot:

```rust
coord.refresh_catalog(1).await?;
let zone = coord.catalog().by_name("events").expect("events catalog is ready");
coord.write_in(
    zone,
    b"2026-06-11/42",
    b"click",
    WriteOptions::device_durable(WriteConcern::Majority),
).await?;
let rows = coord
    .scan_in(zone, Some(b"2026-06-01/"), Some(b"2026-07-01/"), 2, Some(1000))
    .await?;
```

A sorted zone's keyspace is divided into contiguous **ranges**, each placed on
the ring by its lower-bound token, so keys in a range co-locate (cheap scans)
while different ranges spread across nodes and drives. The PD can auto-split
ranges as they grow; for a manual split, run on its leader's host:

```sh
valsd admin -s /path/to/pd.sock split-zone events 2026-06-15/
valsd admin -s /path/to/pd.sock ranges events
```

`split-zone` replies with queued intent, not a committed split. Confirm the
expected boundary in `ranges` on the authoritative leader before depending
on it, polling with a deadline if it is not present yet.

Scans of any size work — replica replies are paged under a server byte budget
and the coordinator reassembles them (a `limit` also caps what is pulled, not
just what is returned). See [`zones.md`](zones.md) for placement and rehoming;
a split alone does not necessarily move data to different owners.

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
  otherwise the call fails with `Unsupported` before anything is written. A
  client that does not advertise bit 3 gets an error from a scan whose window
  meets a range tombstone.
- **Prefer cumulative deletes.** Deleting `[base, cursor)` with a rising cursor
  keeps a single fragment per zone engine. Deleting many disjoint spans keeps one
  fragment each, kept until `gc_grace` collects them (forever at the default
  `gc_grace = 0`, as for point tombstones), up to 65,536 per zone engine, after
  which further disjoint deletes are refused.

Design: [`range-deletes.md`](range-deletes.md).

## Operating it

The full lifecycle instructions are in [the operator guide](deploy.md).
For application-owned servers, use the [embedding guide](embedding.md) instead.

- **Auth.** Set the same `secret_file` on every node and PD. It gates
  mutating control operations and the PD Raft port; unset means open. The
  token is cleartext, and data reads/writes and client discovery are never
  authenticated. Enforce network isolation or use a trusted overlay such as
  WireGuard/Tailscale; see [`SECURITY.md`](../SECURITY.md).
- **Logging & metrics.** Daemons log to stderr; set `log_level` in the config.
  Set `metrics = <private-address>:<port>` to export Prometheus counters.
  The endpoint has no authentication. Use [config validation](deploy.md#configuration-basics)
  before restarting a service.
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
  `connect_sharded_unbounded` turn off the deadlines and the coordinator write
  budgets, for controlled benchmarks only; the in-tree `vals-bench` harnesses
  use them. Other clients should keep the finite defaults or pass a reviewed
  `ClientConfig`.
- **Tombstone GC + liveness.** Deletes leave versioned tombstones forever
  at the default `gc_grace = 0s`. A nonzero value on every data node enables
  collection and the wipe-before-rejoin lease: a sufficiently stale node
  wipes and re-syncs instead of bringing back collected deletes. Use the same
  setting everywhere and account for its resync cost. Read the [node config
  notes](deploy.md#node-config-reference) and the [known backup/repair exposure](backup-restore.md#consistency-contract)
  before enabling it; a grace period alone is not a deletion guarantee.
- **Shutdown and recovery.** Send SIGTERM/SIGINT and wait for daemon exit;
  shutdown drains and syncs engines and persists PD state. A durable
  acknowledgement is distinct from a buffered write rescued by a clean stop.
  See [systemd](deploy.md#systemd), [PD recovery](deploy.md#pd-availability-and-recovery)
  and [backup/restore](backup-restore.md) before relying on restarts or replication
  as your recovery plan.
- **Version-gated formats.** Every data connection negotiates the version/features
  from its `PROTO_VERSION` hello. On-disk containers carry `[magic][u16 version]`
  envelopes and unknown versions fail loud. [`formats.md`](formats.md) is the
  current-contract snapshot; versioning makes compatibility decisions explicit,
  but is not an unlimited backward-compatibility promise. Run mixed versions
  during a rolling upgrade only when the docs for that change say it is safe.

## Storage backends

| Daemon setting | Backend | What to expect |
|---|---|---|
| `backend = file` (default) | File-backed block device | Persistent local evaluation without dedicated hardware; size each zone with `region_mib`. |
| `backend = mem` | In-memory block device | Tests and throwaway deployments; data is lost on exit, even if device durability is requested. |
| `backend = spdk` | Raw NVMe through SPDK | Dedicated drives, explicit offline provisioning, hugepages and an SPDK-linked build; see [`SPDK.md`](SPDK.md). |

The distributed algorithms share the same `BlockDevice` abstraction, but
performance, provisioning and device durability depend on the backend. The
file-backed demo is not evidence of SPDK performance or hardware recovery.
For block-device constructors and engine ownership in your own process, see
the [embedding guide](embedding.md).

## What's deliberately not here

- **Multi-key transactions / cross-key ordering**, given up for the leaderless
  design (see [`../ARCHITECTURE.md`](../ARCHITECTURE.md)). An opt-in
  linearizable-zone design is parked in
  [`future-linearizable-zones.md`](future-linearizable-zones.md).
- **Linearizable CAS** — `cas` is best-effort (above).
- **Auto-eviction** is not yet supported. The failure detector only routes
  around a down node; removing it from the ring is an explicit
  `valsd admin -s pd.sock remove-node ID` (see
  [`deploy.md`](deploy.md)).
- **Non-Rust clients.** There is no packaged non-Rust client. The wire
  protocol is version-negotiated and documented in [`formats.md`](formats.md),
  but there is no unlimited backward-compatibility promise. A server binary
  exists: `valsd`, see [`deploy.md`](deploy.md).

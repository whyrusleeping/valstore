# Embedding vals in a Rust application

Running [`valsd`](deploy.md) is the shortest path to a service that applications
can share. Embedding is also a first-class way to use vals: choose the layer
that matches what your application needs, rather than building a cluster just
to store local data.

| Application need | Crate / entry point | What runs in your process |
|---|---|---|
| Talk to a running daemon cluster | `vals-client`: `Coordinator`, `NetTransport` | Client routing and network I/O, **not an embedded database** |
| Own a local key/value engine | `vals-store`: `Engine<D>` | Storage engine over a `BlockDevice`; no PD, sockets, or replicas required |
| Host distributed storage or its control plane | `vals-cluster`: `VersionedStore`, `Node`, `PdDaemon` | Replica serving/healing or PD consensus, with your application supplying server integration |

`vals-cluster` re-exports the client types, but does not re-export the engine,
block-device crate, or executor. The crates are not published on crates.io;
use git dependencies and keep their revisions aligned. For a local engine:

```toml
[dependencies]
vals-store = { git = "https://github.com/whyrusleeping/valstore" }
vals-spdk  = { git = "https://github.com/whyrusleeping/valstore" }
```

For the distributed examples below, also add:

```toml
vals-cluster = { git = "https://github.com/whyrusleeping/valstore" }
vals-rt      = { git = "https://github.com/whyrusleeping/valstore" }
```

Despite its name, `vals-spdk` also supplies the memory and file block devices;
those do not require SPDK or its feature flag. API and format compatibility
must be evaluated for the revision you deploy; these examples do not establish
an API freeze. See [formats](formats.md) for the current on-disk/wire contract.

## A local, file-backed engine

This complete `src/main.rs` creates a 64 MiB backing file in a **new** directory,
writes and reads a value, explicitly syncs, drops the engine, and reopens it
to assert recovery. It also has a separate `reopen`
mode that verifies the previous run before making another update.

Creation and recovery are deliberately separate:

- `FileBlockDevice::create` **truncates an existing file**. `Engine::create`
  formats the device. Neither is a recovery operation.
- `FileBlockDevice::open` followed by `Engine::open` recovers an existing store.
  Preserve the block size and engine configuration. Propagate recovery errors;
  never fall back to formatting when opening fails.
- The example uses `create_dir`, not `create_dir_all`, to refuse an existing
  destination before it can reach the truncating constructor. Use a trusted
  parent directory and give this process exclusive ownership of the store;
  this is not a locking protocol against other processes or hostile path changes.

```rust
use std::error::Error;
use std::fs::{self, File};
use std::path::PathBuf;
use vals_spdk::FileBlockDevice;
use vals_store::{Config, Engine};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 2 || (args[0] != "new" && args[0] != "reopen") {
        return Err("usage: local-engine <new|reopen> DIRECTORY".into());
    }
    let dir = std::env::current_dir()?.join(PathBuf::from(&args[1]));
    let path = dir.join("engine.blocks");
    let cfg = Config::default();
    let block_size = 4096;

    let mut engine = if args[0] == "new" {
        fs::create_dir(&dir)?;
        let dev = FileBlockDevice::create(&path, block_size, 16_384)?;
        let mut engine = Engine::create(dev, cfg)?;
        engine.sync()?;
        File::open(&dir)?.sync_all()?;
        File::open(dir.parent().ok_or("directory needs a parent")?)?.sync_all()?;
        engine
    } else {
        let dev = FileBlockDevice::open(&path, block_size)?;
        let engine = Engine::open(dev, cfg)?;
        assert_eq!(engine.get(b"hello")?.as_deref(), Some(&b"world"[..]));
        engine
    };

    let version = engine.version(b"hello")?.unwrap_or(0)
        .checked_add(1).ok_or("version exhausted")?;
    engine.put(b"hello", b"world", version)?;
    assert_eq!(engine.get(b"hello")?.as_deref(), Some(&b"world"[..]));

    engine.sync()?;
    drop(engine);

    let dev = FileBlockDevice::open(&path, block_size)?;
    let engine = Engine::open(dev, cfg)?;
    assert_eq!(engine.get(b"hello")?.as_deref(), Some(&b"world"[..]));
    assert_eq!(engine.version(b"hello")?, Some(version));
    drop(engine);
    println!("recovery verified: {}", path.display());
    Ok(())
}
```

In a Cargo binary project with the two local-engine dependencies above:

```sh
cargo run -- new ./local-engine-data
cargo run -- reopen ./local-engine-data
```

Running `new` again against that directory must fail without overwriting it.
The `reopen` mode expects a store written by this example, not an arbitrary
application database. The example intentionally leaves its data behind.

### Local semantics and durability responsibilities

An `Engine` call is **not a replicated operation**: it has no write concern,
quorum read, HLC writer identity, read repair, or cluster-wide LWW guarantee.
The raw API accepts a `u128` version supplied by the caller and uses it in
storage resolution; supplying a version does not turn the engine into a
`VersionedStore`. Here the version is simply a single-owner counter for one
key. Design version allocation and mutation ordering for your own wrapper.

The raw `Engine::delete` is a low-level removal from the mutable index, not the
replica layer's versioned logical tombstone operation. Do not assume it masks
an older value in an immutable run. This example intentionally demonstrates
only puts and reads; use the `VersionedStore`/coordinator layer for the
versioned put/delete semantics described in
[Reading and writing](getting-started.md#reading-and-writing).

`put` returning successfully is not a device-durability barrier. Call
`Engine::sync()` and check its result before promising durable completion.
For `FileBlockDevice`, the flush reaches `File::sync_data` (`fdatasync`); the filesystem,
operating system, device, and power-loss behavior still determine the actual
persistence guarantee. The example also syncs the new directory entry and its
parent. Your application owns provisioning, directory durability, exclusive
access, error handling, and backup policy. Do not rely on `drop` as a substitute
for a checked sync. A memory device cannot become persistent by calling sync.

This is a small recovery demonstration, not a long-running service template or
a power-failure qualification. An embedded engine does not start the node's
maintenance loops: budget capacity and memory and arrange the appropriate
flush/snapshot/compaction work for a sustained workload. Inspect the
[engine API](../crates/vals-store/src/engine/) and the [node maintenance integration](../crates/vals-cluster/src/maint/)
before building that lifecycle yourself.

## Runtime and thread ownership

The synchronous local file example needs no executor. File I/O and recovery
can block; do not run them directly on an async application's worker thread.

The distributed layer uses the vals executor and reactor, not Tokio's socket
driver. A network `Coordinator`/`NetTransport` is thread-local (`!Send`, `!Sync`):
construct it and drive its futures on the same dedicated OS thread with
`vals_rt::block_on(future, vals_cluster::reactor::poll_io)` (or the
`vals_client::reactor` re-export). An empty poll hook works for the in-process
toy below, not for TCP. A generic executor polling these futures without the
vals reactor will not provide the required socket progress.

`Node::serve*` and `PdDaemon::spawn*` start their own server threads and drive
their internal reactors. You retain the returned handles; you do not put their
serving loops on a Tokio worker. For a client in the same process, use a
separate thread/channel boundary as described in
[Tokio applications](getting-started.md#tokio-applications). Keep live transports
and shard state on their owning threads. SPDK additionally has shard-thread
queue-pair affinity and device initialization requirements; follow
[SPDK setup](SPDK.md), not a mechanical substitution into the file example.

## Embedded distributed data nodes

Embedding the server changes who owns its lifecycle, not the data contract.
Use the shared descriptions of
[placement and LWW](getting-started.md#the-model-in-one-minute),
[quorums and persistence](getting-started.md#reading-and-writing), and
[zones and scans](getting-started.md#zones-and-range-scans).
A remote client uses the same `vals-client` API whether the server is `valsd`
or your own binary.

The following is a **partial server integration sketch**, not a complete
cluster launcher. It assumes node 1's persistent engine was already provisioned
in an exclusively owned directory, that the other addresses match your
configuration, and that your application supplies `wait_for_shutdown`.
It uses the single-zone baseline with tombstone GC disabled.

```rust
use std::net::TcpListener;
use std::time::Duration;
use vals_cluster::{Node, Ring, VersionedStore};
use vals_spdk::FileBlockDevice;
use vals_store::Config;

vals_cluster::observe::init_stderr_logger();
let secret = std::fs::read("/etc/vals/cluster.secret")?;
vals_cluster::init_cluster_secret(&secret);

let dev = FileBlockDevice::open("/var/lib/vals/node-1/engine.blocks", 4096)?;
let store = VersionedStore::open(dev, Config::default(), 1)?;
let mut ring = Ring::new(3);
ring.set_members_weighted([(1, 1), (2, 1), (3, 1)]);
let listener = TcpListener::bind("10.0.0.1:7001")?;
listener.set_nonblocking(true)?;
let handle = Node::serve(
    1,
    listener,
    store,
    ring,
    vec![(2, "10.0.0.2:7001".parse()?), (3, "10.0.0.3:7001".parse()?)],
    Duration::from_secs(5),
);

wait_for_shutdown();
handle.shutdown();
```

For a **fresh** store, use `VersionedStore::new(Engine::create(dev, cfg)?,
node_id)?` after safely provisioning a new device. For recovery use
`VersionedStore::open(dev, cfg, node_id)?`, as above, preserving the node ID
and geometry. Do not format an existing replica or expose the same backing
file through two independently writable engines.

Bind a nonblocking listener on the address peers and clients actually dial;
register the same address with the PD. `Node::bind()` is a test/demo helper
that binds loopback on an ephemeral port. The boot ring and peers seed startup;
the PD pushes authoritative, epoch-stamped membership and addresses. Do not
admit application traffic until the required cluster configuration is ready.

Retain `NodeHandle` and explicitly call its consuming `shutdown()` method on
orderly exit. It signals and joins the shards, whose shutdown path attempts a bounded drain
of in-flight work, syncs stores, and persists liveness state when configured,
then joins maintenance. Stop application producers first and allow time for this
work. The method returns `()`, not a durable-write receipt: inspect errors in
logs and use checked device-durable operations for application acknowledgements.

### Zones, maintenance, and liveness

Select the node constructor and persistence providers deliberately:

- `Node::serve_zoned` takes zone 0's store, a `ZoneStore` provider, a boot
  `Catalog`, and an optional persistent `CatalogStore`. Additional zone engines
  materialize lazily from the catalog. Use persistent catalog metadata for a
  restartable server rather than relying only on an in-memory boot catalog.
- `MemZoneStore` is volatile. `FileZoneStore::new(node_id, dir, geom)` manages
  file-backed zone engines. `SpdkZoneStore` manages zone regions on its device;
  see [zones](zones.md) and [SPDK](SPDK.md) for allocation and capacity concerns.
  Keep the provider's geometry and ordering consistent across restarts and
  preserve its metadata, not just value-log files.
- `Node::serve_sharded` hosts independent shards; advertise all shard addresses.
  `Node::serve_multidrive` and the multidrive-zoned variants spread partitions
  across drives. See [multi-drive](multi-drive.md). The `_with_settings`
  variants expose resource settings rather than automatically adopting a
  `valsd` deployment profile.
- Tombstones are retained indefinitely by default (`gc_grace_ms = 0`). To
  enable collection, use the appropriate `*_with_liveness` constructor with
  a consistent nonzero grace and a persistent liveness log, such as
  `FileLivenessLog`, for every persistent shard. That constructor configures
  the grace for existing and future zone engines; do not independently turn
  on `VersionedStore::with_gc_grace_ms` without the corresponding rejoin safety.
- Liveness compares durable progress watermarks and can wipe and resync a
  stale replica before it rejoins. GC eligibility follows applied data
  progress, not just elapsed wall time. Preserve the liveness metadata and
  arrange peers and maintenance so recovery can complete. Choose grace and
  repair policy for your failure window; GC is not permission to retain an
  old disconnected copy and later serve it unchecked. Membership removal,
  restore, and re-enrolment also need the safeguards in
  [Operating it](getting-started.md#operating-it) and
  [backup and restore](backup-restore.md).

## Embedded PD (control plane)

Use durable PD constructors for a restartable control plane. PD state is
separate from every data-node engine; retain both its engine and its Raft
state directory. A single durable PD has **no PD fault tolerance**.
Increasing the number of PD voters in place is not supported: if you need a
three-voter group, start with three, or plan an explicit migration to a new
group. Do not assume a single PD can be “Raft-replicated later” by adding peers.
Adding **data nodes** is a separate, supported membership operation.

This is a **partial integration sketch** for a deliberately single-PD system.
`pd_engine` must be a separately provisioned or recovered `Engine`; the
addresses, zone configuration, bounded readiness/retry policy, administration
endpoint, and process shutdown integration belong to your application.

```rust
use vals_cluster::pd::{PdDaemon, RingConfig};

let pd = PdDaemon::spawn_single_durable(
    1,
    pd_engine,
    RingConfig { vnodes: 128, rf: 3 },
    vec![(1, vec![addr1]), (2, vec![addr2]), (3, vec![addr3])],
    "/var/lib/vals/pd-1/raft",
)?;
pd.bootstrap_members(vec![
    (1, 1, vec![addr1]),
    (2, 1, vec![addr2]),
    (3, 1, vec![addr3]),
]);

wait_for_committed_bootstrap(&pd)?;
pd.add_node_addressed(4, 1, vec![addr4])?;
wait_for_committed_member(&pd, 4)?;
wait_for_shutdown();
pd.shutdown();
```

The startup-only `bootstrap_members` queues one atomic initial membership
intent, retried through leader election; adding only the first member of an
empty RF=3 deployment would violate RF. Observe `members()` for the committed
view instead of treating the bootstrap call as a readiness barrier.

For three PDs, each process uses
`PdDaemon::spawn_durable(id, listener, pd_peers, data_nodes, engine, ring,
campaign, dir)`. Supply the same complete voter/address list, distinct IDs,
and separate persistent state for each replica. The PD listener is for Raft
traffic, not the data-client protocol. See [PD administration](pd-admin.md)
and [deployment](deploy.md) for operational behavior.

Administrative calls such as `add_node_addressed`, `remove_node`, and
`create_zone` validate and propose changes. Followers return `NotLeader`;
a new leader can return `NotReady`. Success means proposed, **not committed
or delivered**. Observe `members()`, `catalog()`, or `range_map()` as appropriate,
then allow config push to reach data nodes before dependent client actions.
Give waits deadlines and surface failures. Addressed data-node additions let
config push, anti-entropy, and handoff discover the node without restarting
all peers. Explicitly call `PdDaemon::shutdown()` at process exit.

## An in-memory distributed API demo

`LocalCluster` is a **nonpersistent, non-HA demo**, not a required step on the
way to running daemons. All three replicas share one process and failure
domain; there are no sockets or independent server processes. This complete
program exercises coordinator semantics without a network reactor:

```rust
use vals_cluster::{Coordinator, LocalCluster, Ring, VersionedStore, WriteConcern};
use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine};

fn main() {
    let mut cluster = LocalCluster::new();
    for node in 1..=3u32 {
        let engine = Engine::create(MemBlockDevice::new(4096, 4096), Config::default())
            .unwrap();
        cluster.insert(node, VersionedStore::new(engine, node).unwrap());
    }
    let mut ring = Ring::new(3);
    ring.set_members_weighted([(1, 1), (2, 1), (3, 1)]);
    let coord = Coordinator::new(100, ring, 2, cluster);
    vals_rt::block_on(
        async {
            let version = coord.write_with(b"hello", b"world", WriteConcern::Majority)
                .await.unwrap();
            let got = coord.read(b"hello").await.unwrap().expect("present");
            assert_eq!(got.value.as_deref(), Some(&b"world"[..]));
            assert_eq!(got.version, version);
            coord.delete(b"hello", WriteConcern::Majority).await.unwrap();
            assert_eq!(coord.read(b"hello").await.unwrap(), None);
        },
        || {},
    );
}
```

`VersionedStore` supplies replica LWW behavior; the coordinator uses RF=3,
W=2, R=2 and routing label 100 (the writer identity is generated separately).
Its delete is a versioned tombstone, unlike the
raw engine removal above. For the contract and limitations, refer to
[Reading and writing](getting-started.md#reading-and-writing).

## What your application inherits from valsd

Embedding is more than replacing a daemon's `main`. Use
[`vals-server`](../crates/vals-server/src/) as the integration reference and
review the [configuration reference](configuration.md). Your binary owns:

- **Configuration and resources:** stable IDs, advertised addresses, placement,
  backend geometry, directory ownership, shard/core layout, memory and disk
  budgets, background maintenance, admission limits, and finite client
  deadlines. Daemon `capacity`/`latency` profiles are not automatically
  installed by constructing an engine or node.
- **Security:** initialize the same cluster secret in every embedded node/PD
  with `init_cluster_secret` before starting network roles; protect the secret
  and administration endpoints. The secret gates control mutations and PD
  Raft traffic, not data reads/writes, and is sent in cleartext. It is not TLS
  or application authorization. Use private networking or an encrypted overlay
  and implement the access boundaries your service needs. See
  [security policy](../SECURITY.md).
- **Logging and metrics:** install a `log` logger once (or
  `observe::init_stderr_logger`, with level controlled by `VALS_LOG`), collect
  `observe::snapshot()` counters, expose metrics through your monitoring
  system, and monitor disk/capacity, recovery, replication, and sync failures.
  Constructing a `Node` does not install the daemon's admin/metrics listeners.
- **Supervision and lifecycle:** startup validation and readiness, signals,
  worker failures, bounded queues, shutdown ordering, joins, restart policy,
  backups, upgrades, and restore drills. Do not detach server handles and
  assume the surrounding application will keep them healthy. Co-locating a
  PD and data node is possible but shares their process failure domain.

For a daemon-first application, none of this server integration is necessary:
run `valsd` and use the [client guide](getting-started.md#clients). Choose
embedding when owning those responsibilities is part of your application design.

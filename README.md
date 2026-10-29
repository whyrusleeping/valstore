# vals

`vals` is a distributed key/value store written in Rust and built to get as
close as possible to what NVMe drives can deliver. Each node runs one thread
per core with no state shared between them, and can drive its drives directly
from userspace through [SPDK](https://spdk.io/), bypassing the kernel. The
design favours throughput for very large numbers of independent single-key
reads and writes, and it gives up multi-key transactions to get there.

The data path follows Dynamo: there is no leader, clients pick per-request
write and read quorums, and replicas repair each other in the background.
Raft is used only by a small control plane (the PD, or placement driver) that
manages membership and namespaces. You can run vals as a server (`valsd`) or
embed it in an application as a Rust library. Without SPDK it runs on
in-memory or file-backed storage.

## Status

vals is young. The version is 0.0.1, it is not on crates.io, and its APIs may
still change. It runs on Linux; it also builds and runs on macOS (Apple
Silicon) for development, without SPDK. A shared secret can protect control
operations, but it is off unless you set `secret_file`, and without it
control operations are open too. Data traffic is never authenticated or
encrypted, so run vals on a private network; [`SECURITY.md`](SECURITY.md)
has the details. Its main real-world test is replaying about 5
billion Bluesky firehose events on a three-machine SPDK cluster. Which
revision to evaluate, what it was tested on and its known limits are in
[`docs/evaluation.md`](docs/evaluation.md).

What to expect if you're used to a SQL database or etcd:

- Choose `W + R > RF` for a read that sees an acknowledged write's version
  or a greater LWW version. "Latest" means version order, not request completion
  order; this is not linearizability. `R = 1` alone does not generally give
  that overlap.
- There are no multi-key transactions. Concurrent writes to one key resolve
  by last writer wins, and `cas` is best-effort, not linearizable.
- By default, an acknowledged write is held in replica memory, not on disk.
  Ask for device durability per write when you need it.

## Performance

All numbers are with replication factor 3 (unless stated otherwise),
verified values, and zero errors:

- **One machine with 8 PM9A3 drives**, running four node processes (two
  drives each) over loopback TCP, with the background anti-entropy sweep
  off: about **3.5–4M replicated writes/s**, **~4.4M strong reads/s**, and
  **~6.5M `R = 1` reads/s**. The single-node engine on its own reaches the
  drives' limits (~8M read IOPS, ~29 GiB/s writes).
- **Three machines on a real network** (one drive and four shards per node,
  RF 2, with the non-default `profile = latency`): **~5M strong 100-byte
  reads/s**.

These are benchmark results from specific runs, not guarantees. The
hardware and setup behind each number are in [`docs/SPDK.md`](docs/SPDK.md),
and the [Status section of `ARCHITECTURE.md`](ARCHITECTURE.md#status)
summarizes what has been validated. For what to expect from the default
file-backed `valsd` on ordinary machines (about 78k buffered overwrites/s and
100–125k reads/s on 24-core hosts with a consumer NVMe drive), with a recipe
to measure your own, see [`docs/benchmarks.md`](docs/benchmarks.md).

## Quick start

You need Linux or macOS and [rustup](https://rustup.rs/). The repository
pins its Rust toolchain in `rust-toolchain.toml`, and rustup uses that
version automatically.

```sh
git clone https://github.com/whyrusleeping/valstore
cd valstore
```

Start with `valsd` below to run vals as a database service. Rust applications
can also embed the [local engine or distributed node](docs/embedding.md);
embedding is a separate path, not a prerequisite for using the daemon.

### Run it: a local three-node cluster

This starts three data nodes and one PD on localhost, with file-backed
storage. A single PD is fine for trying vals out, but it is not highly
available; real deployments run three.

```sh
cargo build --release -p vals-server
cargo build --release -p vals-client --example kv
VALSD=$PWD/target/release/valsd
KV=$PWD/target/release/examples/kv

mkdir -p /tmp/vals && cd /tmp/vals
echo "demo-secret" > secret

for i in 1 2 3; do
  cat > node$i.conf <<EOF
node_id  = $i
listen   = 127.0.0.1:700$i
data_dir = node$i
$(for p in 1 2 3; do [ $p != $i ] && echo "peer = $p @ 127.0.0.1:700$p"; done)
secret_file = secret
EOF
done

cat > pd.conf <<EOF
pd_id     = 1
data_dir  = pd
data_node = 1 @ 127.0.0.1:7001
data_node = 2 @ 127.0.0.1:7002
data_node = 3 @ 127.0.0.1:7003
rf        = 3
zone      = events 3 sorted
admin_socket = pd.sock
secret_file  = secret
EOF

for i in 1 2 3; do $VALSD node -c node$i.conf 2> node$i.log & done
$VALSD pd -c pd.conf 2> pd.log &
sleep 2

$VALSD admin -s pd.sock members     # the three data nodes
$VALSD admin -s pd.sock zones       # the sorted `events` zone
```

To send traffic, use `kv`, a small example client from `vals-client`. It
connects to 127.0.0.1:7001-7003 by default, and `$KV --help` lists its
options:

```sh
$KV put hello world
$KV get hello                       # prints `world` and its version
$KV delete hello
$KV --zone events put 2026-06-11/42 click
$KV --zone events scan              # key<TAB>value lines in key order, then a count
```

Each daemon logs to its `*.log` file. Stop the demo with `pkill -x valsd`
(only on a machine with no other valsd deployments), which sends SIGTERM for
a clean shutdown. These examples use buffered majority writes: an
acknowledgement is not a device sync. The client guide explains how to request
[device-durable writes](docs/getting-started.md#reading-and-writing).

### Connect your application

A Rust application talking to `valsd` uses `vals-client` and `vals-rt`; it
**does not embed the database**. See the [client walkthrough](docs/getting-started.md#clients)
for dependencies and a TCP example, or the [Tokio bridge](docs/getting-started.md#tokio-applications)
for an existing async application. There is no packaged non-Rust client yet.

Next, read the [consistency and durability contract](docs/getting-started.md#the-model-in-one-minute)
and the [operator guide](docs/deploy.md), which covers configuration,
three-PD availability, systemd, sizing, backup and restore. The local demo
is not highly available: growing its single PD into a three-voter group in
place is not supported. Choose three PDs from the outset for HA, or follow
[growing a small deployment](docs/growing.md), which covers a minimal
one-node setup, zone sizing, adding nodes and the backup/restore migration
to three PDs.

### Embed it in Rust

Embedding is also a first-class way to use vals:

- **Local storage inside your process:** `vals-store` provides the persistent
  engine, without networking, replication or a PD.
- **A distributed node inside your service:** `vals-cluster` adds versioning,
  replica serving and healing; your application owns its runtime and lifecycle.

The [embedding guide](docs/embedding.md) covers dependencies, file-backed
create/reopen/sync, an in-process replicated demo, and hosting networked nodes
and a durable PD. It also explains which maintenance, security and shutdown
responsibilities your application takes on instead of `valsd`. An embedded
`vals-cluster` node and `valsd` share the same [quorum and LWW contract](docs/getting-started.md#the-model-in-one-minute);
the local engine alone has no replication or quorum semantics.

## Building and testing

```sh
cargo build --workspace     # mem/file storage backends; SPDK not needed
cargo test --workspace      # no hardware needed
scripts/ci.sh               # exactly what CI runs: build, test, clippy, Python tests
```

Every layer above the block device (`MemBlockDevice`, `FileBlockDevice`, and
`SpdkBlockDevice`) is generic over the same `BlockDevice` trait, so
everything except the NVMe backend itself is tested without hardware. The
code is not rustfmt-clean, so please don't run `cargo fmt` on the whole tree.

For raw NVMe you need SPDK v24.09, dedicated drives, and hugepages. See
[`docs/SPDK.md`](docs/SPDK.md), then run:

```sh
VALS_SPDK_LINK=1 cargo build --release -p vals-server --features spdk
```

## Repository layout

| Crate | What it is |
|---|---|
| [`vals-client`](crates/vals-client) | The client: the `Coordinator` (quorum reads/writes, durability options, CAS, batches, scans), the TCP transport, and the wire types. It does not depend on any server code. Clients also need `vals-rt` to drive it. |
| [`vals-cluster`](crates/vals-cluster) | The distributed layer: versioning, the ring, replicas, healing, zones, and the PD. It re-exports `vals-client`; a server that embeds it also uses `vals-store`, `vals-spdk` and `vals-rt` directly. |
| [`vals-server`](crates/vals-server) | `valsd`, the daemon: the `node`, `pd`, `admin`, `backup`, `dump`, `keyscan`, `provision`, and `reenrol` subcommands. |
| [`vals-store`](crates/vals-store) | The single-node storage engine. It is log-structured and has an out-of-core index, crash recovery, and compaction. |
| [`vals-spdk`](crates/vals-spdk) | The `BlockDevice` trait and its backends: in-memory, file, and SPDK NVMe (with the `spdk` feature). |
| [`vals-rt`](crates/vals-rt) | A small thread-per-core async executor, shared by the storage and network event loops. |
| [`vals-raft`](crates/vals-raft) | A wrapper around `raft-rs` with durable storage. Only the PD uses it. |
| [`vals-bench`](crates/vals-bench) | Benchmark and correctness harnesses for both the software and real-NVMe paths. |

Outside `crates/`, `docs/` holds the design and reference docs and `scripts/`
holds CI and provisioning helpers. The task tracker lives in `backlog/`, with
an index in `backlog.md`, and planning notes live in `spec.md` and `plans/`.
Those are kept by the AI-assisted workflow the project is developed with.
They are public because they record why things are the way they are, but
they are not user documentation. Some of them cite `bench/` experiment
write-ups that are not part of this repository.

## Documentation

- [`docs/getting-started.md`](docs/getting-started.md): **start here** to run
  vals and connect an application. It covers the client API, the data model,
  durability options, CAS, batches, zones and scans.
- [`docs/evaluation.md`](docs/evaluation.md): the revision to evaluate, the
  evidence behind it, upgrade expectations and known limitations.
- [`docs/deploy.md`](docs/deploy.md): the `valsd` operator guide
  (configuration, the PD, admin, systemd, backup and restore).
- [`docs/growing.md`](docs/growing.md): from one machine to a highly
  available cluster, and what each step needs.
- [`docs/embedding.md`](docs/embedding.md): use the local engine or host a
  distributed node in Rust, including persistence, runtime ownership,
  maintenance and shutdown.
- [`ARCHITECTURE.md`](ARCHITECTURE.md): the design, with a map of what is
  built, and a hub that links the deep-dive docs.
- [`docs/README.md`](docs/README.md): an index of every doc. That includes
  the guides, the reference docs (wire and on-disk formats, the algorithms
  every node must agree on), and the design notes (zones, multiple drives,
  batch writes, backpressure, SPDK).
- API docs: `cargo doc --workspace --no-deps --open`.

## Contributing and security

See [`CONTRIBUTING.md`](CONTRIBUTING.md) before sending a change; the code
conventions are unusual. Report vulnerabilities privately as described in
[`SECURITY.md`](SECURITY.md), which also spells out what vals does and
doesn't protect against.

## License

MIT; see [`LICENSE`](LICENSE).

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
still change. It runs on Linux only. A shared secret protects control
operations, but data traffic is neither authenticated nor encrypted, so run
it on a private network. Its main real-world test is replaying about 5
billion Bluesky firehose events on a three-machine SPDK cluster.

What to expect if you're used to a SQL database or etcd:

- Choose `W + R > RF` for reads that see the latest write. `R = 1` reads are
  only eventually consistent.
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
  **~6.5M `R = 1` reads/s**. The last figure is essentially the raw read IOPS
  of the 8 drives. The single-node engine on
  its own reaches the drives' limits (~8M read IOPS, ~29 GiB/s writes).
- **Three machines on a real network** (one drive per node, RF 2): **~5M
  strong 100-byte reads/s**.

These are benchmark results from specific runs, not guarantees. Methodology
and the full history are in [`docs/SPDK.md`](docs/SPDK.md) and the Status
section of [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Quick start

You need Linux and [rustup](https://rustup.rs/). The repository pins its Rust
toolchain in `rust-toolchain.toml`, and rustup uses that version
automatically.

```sh
git clone https://github.com/whyrusleeping/valstore
cd valstore
```

### Embed it: a cluster in one process

This is the smallest working program. It runs three in-memory replicas
inside a single process and talks to them through a coordinator, with no
threads or sockets. A fuller version of the same program runs as a doctest
of `vals-cluster`.

```rust
use vals_cluster::{Coordinator, LocalCluster, Ring, VersionedStore, WriteConcern};
use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine};

fn main() {
    // Three replicas, each a storage engine on an in-memory block device.
    let mut cluster = LocalCluster::new();
    for node in 1..=3u32 {
        let engine = Engine::create(MemBlockDevice::new(4096, 4096), Config::default()).unwrap();
        cluster.insert(node, VersionedStore::new(engine, node).unwrap());
    }

    // A consistent-hash ring with replication factor 3 over those nodes.
    let mut ring = Ring::new(3);
    ring.set_members_weighted([(1, 1), (2, 1), (3, 1)]);

    // A client-side coordinator (id 100) whose reads ask R = 2 replicas.
    let coord = Coordinator::new(100, ring, 2, cluster);

    vals_rt::block_on(
        async {
            // W = 2 (majority) + R = 2 > RF = 3, so the read must see the write.
            coord.write_with(b"hello", b"world", WriteConcern::Majority).await.unwrap();
            let got = coord.read(b"hello").await.unwrap().expect("present");
            assert_eq!(got.value.as_deref(), Some(&b"world"[..]));
        },
        || {}, // I/O poll hook; nothing to poll in-process
    );
}
```

The crates are not on crates.io, so depend on them from git, e.g.
`vals-cluster = { git = "https://github.com/whyrusleeping/valstore" }`.
Use the smaller `vals-client` crate instead if your application is only a
client.

### Run it: a local three-node cluster

This starts three data nodes and one PD on localhost, with file-backed
storage. A single PD is fine for trying vals out, but it is not highly
available; real deployments run three.

```sh
cargo build --release -p vals-server
VALSD=$PWD/target/release/valsd

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

Each daemon logs to its `*.log` file. Stop them with `pkill -x valsd`,
which sends SIGTERM, and they shut down cleanly. There is no command-line
get/put tool yet; to send traffic, use a `Coordinator` over TCP as shown in
[`docs/getting-started.md`](docs/getting-started.md#clients). The operator
guide, with every config option, the three-PD setup, systemd units, and
backups, is [`docs/deploy.md`](docs/deploy.md).

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
| [`vals-client`](crates/vals-client) | The client: the `Coordinator` (quorum reads/writes, durability options, CAS, batches, scans), the TCP transport, and the wire types. It does not depend on any server code. |
| [`vals-cluster`](crates/vals-cluster) | The distributed layer: versioning, the ring, replicas, healing, zones, and the PD. It re-exports `vals-client`, so a server only needs this crate. |
| [`vals-server`](crates/vals-server) | `valsd`, the daemon: the `node`, `pd`, `admin`, `backup`, and `dump` subcommands. |
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

- [`docs/getting-started.md`](docs/getting-started.md): **start here** to use
  vals. It covers the data model, the client API, zones and scans,
  durability options, CAS, batches, and operations.
- [`docs/deploy.md`](docs/deploy.md): how to run `valsd` in production
  (configuration, the PD, admin, systemd, backup and restore).
- [`ARCHITECTURE.md`](ARCHITECTURE.md): the design, with a map of what is
  built, and a hub that links the deep-dive docs.
- [`docs/README.md`](docs/README.md): an index of the reference docs (wire
  and on-disk formats, the algorithms every node must agree on) and of the
  subsystem docs (zones, multiple drives, batch writes, backpressure, SPDK).
- API docs: `cargo doc --workspace --no-deps --open`.

## License

MIT; see [`LICENSE`](LICENSE).

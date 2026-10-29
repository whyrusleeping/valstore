# vals docs

There are three kinds of documents here. Guides tell you how to use and run
vals. Reference docs pin down exact formats and algorithms. Design notes
explain how a subsystem works and why it was built that way. The API
reference is generated from the code: `cargo doc --workspace --no-deps --open`.

New to vals? Start with the [daemon quick start](../README.md#run-it-a-local-three-node-cluster),
then [connect your application](getting-started.md#clients). For storage inside
your own Rust process, take the separate [embedding path](embedding.md).
Using `vals-client` to talk to a daemon is not embedding the database.
If you want to work on vals itself, read [`../ARCHITECTURE.md`](../ARCHITECTURE.md)
and [`../CONTRIBUTING.md`](../CONTRIBUTING.md).

## Guides

- [`getting-started.md`](getting-started.md): the daemon/client walkthrough.
  Start a local deployment, send CLI traffic, connect a Rust or Tokio
  application, then learn the shared consistency/durability contract, client
  API, zones and scans.
- [`embedding.md`](embedding.md): the Rust embedding walkthrough. Choose a
  local persistent engine or an embedded distributed node; covers create/reopen,
  sync, runtime ownership, maintenance, recovery and shutdown.
- [`evaluation.md`](evaluation.md): the revision to evaluate, what was tested
  on which hardware, supported artifacts and platforms, compatibility and
  upgrade expectations, and known limitations.
- [`deploy.md`](deploy.md): running `valsd` in production. It covers node and
  PD configuration, three-PD high availability and recovery, deployment
  profiles, SPDK provisioning, the admin socket, systemd, the security
  checklist, and operational notes.
- [`growing.md`](growing.md): growing a deployment from one machine to a
  highly available cluster: a minimal single-node setup, zone sizing, adding
  data nodes, and the backup/restore migration to three PDs.
- [`configuration.md`](configuration.md): which configuration keys and
  environment variables are supported, and which are diagnostic only.
- [`pd-admin.md`](pd-admin.md): administering the PD through its admin socket,
  including how to handle a mutation whose outcome is unclear.
- [`backup-restore.md`](backup-restore.md): taking, verifying, and restoring a
  logical backup (`valsd backup`).
- [`SPDK.md`](SPDK.md): building and running the raw-NVMe backend, plus the
  measured device and cluster throughput.
- [`benchmarks.md`](benchmarks.md): measured throughput, latency and resource
  use of file-backed `valsd` with default settings on ordinary machines, and
  the script to repeat the runs on yours.
- [`ci.md`](ci.md): what the software CI lane runs.

## Reference

These are exact. A change to either one is a compatibility change.

- [`formats.md`](formats.md): byte-level wire and on-disk layouts. This
  includes the RPC protocol, value-log records and segments, record
  versions, the zone superblock, the drive-layout map, the hint and liveness
  files, and a table of magic numbers.
- [`algorithms.md`](algorithms.md): the algorithms every node must compute
  identically. These are the placement hash, ring and range-token placement,
  shard routing, HLC last-writer-wins order, the Merkle anti-entropy digest,
  and the CRC-32C variant, plus a table of parameters and defaults.
- [`format-history.md`](format-history.md): how the on-disk formats changed
  before the first public release.

All integers are little-endian, and checksums are CRC-32C (Castagnoli). The
wire protocol negotiates a `PROTO_VERSION` in its hello. On-disk containers
start with a `[magic][u16 version]` envelope, and an unknown version is
refused rather than skipped. These version checks make upgrades explicit,
but they do not promise unlimited backward compatibility.

## Design notes

- [`zones.md`](zones.md): zones (per-zone replication factor, placement
  scope, and key ordering), the sorted-zone range partitioner, range maps,
  distributed scans, splits, and re-homing.
- [`range-deletes.md`](range-deletes.md): versioned range tombstones in
  sorted zones. It covers what a range delete hides, engine persistence,
  skip-scans, anti-entropy, and garbage collection.
- [`multi-drive.md`](multi-drive.md): spreading one node over several drives,
  ring weighting by drive count, and moving partitions between drives.
- [`batch-writes.md`](batch-writes.md): the batch write API, per-node group
  commit, and per-write outcomes.
- [`backpressure.md`](backpressure.md): client credit pools and the
  per-connection send-buffer limit.
- [`out-of-core-index.md`](out-of-core-index.md): how the storage engine
  keeps its index on disk instead of in RAM.
- [`blind-append-lww.md`](blind-append-lww.md): the write path behind the
  `latency` deployment profile, where replicas accept writes without first
  reading the current version.
- [`scan-performance.md`](scan-performance.md): how ordered range scans were
  made fast, with measurements.
- [`future-linearizable-zones.md`](future-linearizable-zones.md): a parked
  design for an opt-in zone mode with linearizable CAS. It is not built.

## History and decisions

These record investigations and reviews. They explain past decisions, but
some details are out of date.

Design notes and history docs often cite four-digit numbers such as `0439`.
Those are task ids in [`../backlog/`](../backlog.md), the maintainer's
internal planning notes. The measurements those docs rely on are stated
inline (the number, the date and the setup); the raw experiment write-ups
behind them live in a local `bench/` tree that is not part of this
repository, so a task id is a pointer to the maintainer's records, not to a
file you can open here. [`validation.md`](validation.md) is likewise the
maintainer's hardware-validation reference, and some of the lab scripts it
names exist only on the lab machines.

- [`validation.md`](validation.md): how the maintainer validates changes,
  from local tests up to full-scale hardware runs on the lab cluster.
- [`production-readiness-review-2026-08.md`](production-readiness-review-2026-08.md):
  an external-style review from August 2026. Most of its findings have since
  been fixed, and some were declined (transport encryption, for example). Its
  first section gives the current disposition of each finding.
- [`optional-feature-survey.md`](optional-feature-survey.md): a survey of
  optional features and which ones were kept or removed.
- [`write-path-clustering.md`](write-path-clustering.md): why writes are not
  sorted by key on arrival.
- [`io-uring-transport.md`](io-uring-transport.md): why the network
  transport does not use io_uring. The idea was measured and rejected.

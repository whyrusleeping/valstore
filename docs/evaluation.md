# Evaluating vals

This page answers the questions to settle before you put vals in front of
real data: which revision to evaluate, what was tested and on what hardware,
what you can build and depend on, what to expect from upgrades, and what is
still open. vals is pre-release software. Nothing on this page is a production
qualification; the [last section](#before-you-rely-on-it) lists what you have
to establish yourself.

## The evaluation revision

Evaluate commit `ba4694afcbe560ecb3aedddb6587406703c1e69f` (`ba4694a`, 28
October 2026) on `master`. Its workspace version is 0.0.1. There are no release
tags, packages, prebuilt binaries or crates.io releases. Build it from source:

```sh
git clone https://github.com/whyrusleeping/valstore
cd valstore
git checkout ba4694afcbe560ecb3aedddb6587406703c1e69f
scripts/ci.sh                          # the software test lane (docs/ci.md)
cargo build --release -p vals-server   # target/release/valsd
```

A Rust application pins the same revision for every vals crate it uses:

```toml
vals-client = { git = "https://github.com/whyrusleeping/valstore", rev = "ba4694afcbe560ecb3aedddb6587406703c1e69f" }
```

Later commits may fix or break things, and the evidence below was not
repeated on them. Commits that change only documentation do not change what
you are evaluating. `valsd` has no `--version` flag, so record the commit you
built.

## What you can build and depend on

| Artifact | Status at this revision |
|---|---|
| `valsd` | The supported server: `node`, `pd`, `admin`, `backup`, `provision` (SPDK only) and `dump`. The file backend is the default; `mem` is for tests. The SPDK backend needs `--features spdk` and an SPDK build ([`SPDK.md`](SPDK.md)). |
| `vals-client` | The supported Rust client. Its crate docs list the supported items and their threading, cancellation, retry, persistence and consistency contracts. |
| `vals-store` | The local storage engine, for embedding. Supported items are listed in its crate docs. |
| `vals-cluster` | Hosting data nodes or the PD inside your own process. Supported items are listed in its crate docs. |
| `vals-rt` | The single-threaded executor that drives `vals-client`; `block_on`, `LocalExecutor`, `join_all` and `race` are supported. |
| `vals-spdk` | The block devices engines run on (memory, file, SPDK), used as the `vals-store` and `vals-cluster` docs show. |
| `vals-raft`, `vals-bench`, everything else | Internal. They may change in any commit. |

Every crate has `publish = false` and there is no API freeze, so keep all vals
crates, and the servers your clients talk to, on one revision. The code is
MIT licensed (`LICENSE`).

**Platforms.** Linux only. The software lane runs on x86-64 (GitHub's
`ubuntu-latest` and the developer machine below) with the Rust toolchain
pinned in `rust-toolchain.toml`. The file backend was measured on ext4. The
SPDK backend was tested with SPDK v24.09 built by `scripts/build-spdk.sh`, run
as root on hosts with an IOMMU, on Samsung PM9A3, 990 PRO and 9100 PRO
drives. Other architectures, filesystems, SPDK versions and drives are
untested. Client libraries for other languages are not published.

## Evidence

Each row says which source the run used. "Before the public history" means a
development build from before the first public commit (27 October 2026): the
code has changed since, and those runs were not repeated.

| What | Source | Setup | Result |
|---|---|---|---|
| Software lane: `scripts/ci.sh` (locked build, every workspace test, script tests, Clippy) | `ba4694a`, clean checkout | one x86-64 Linux host; memory and file backends only, no SPDK | build, script tests and Clippy passed; 2 of 1,978 tests failed as parallel-run flakes and passed on rerun ([details](#software-lane)) |
| File-backed `valsd` throughput and latency ([`benchmarks.md`](benchmarks.md)) | `2f21514`; the daemon is unchanged at `ba4694a` | 3 nodes and a PD on one host, or one node per host on three hosts; consumer 990 PRO drives; RF 3, majority writes and reads | about 78k buffered overwrites/s, 100–125k warm reads/s, 1.4–5k device-durable writes/s; no errors, every value checked |
| Client progress while one of three hosts is black-holed for 30 s | the source of `2f21514` | three SPDK hosts, client on a fourth machine, 2,000 operations/s | with the recommended client setup (failure detector and a 50 ms hedge) no throughput dip; without the failure detector, about 17 operations/s until the host returned |
| Backup and restore ([details](backup-restore.md#hardware-qualification)) | the source of `0b546f5` | three hosts, each with one voting PD and one SPDK node; `gc_grace = 0s`; 4.85 million records | every record and range tombstone verified after restore, the old PD group fenced, 341 s from the first restore step to a passing compare |
| Full replay of about 5 billion Bluesky firehose events (10.4 billion key writes) | before the public history | three SPDK nodes | ingest finished in 5 h 20 min; at the end, per-segment record counts were exact, a 10.4-million-key sample read back correctly and the replicas' digests matched; 8 node crashes during the run were recovered by restart, 3 of them heap-corruption aborts (see [below](#what-the-evidence-does-not-show)) |
| Mixed-profile soak | before the public history | three SPDK nodes, tombstone GC on (`gc_grace = 300s`) | 2 h with 11 node restarts, including profile changes; exact scans matched on all three replicas, overdue tombstones were collected |
| PD failover drills ([details](deploy.md#three-pd-reference-deployment)) | before the public history | three voting PDs, three SPDK nodes, 2,000 writes/s | leader failover in 1.1–1.8 s; quorum loss refuses reconfiguration and leaves the data path running |
| Peak SPDK throughput ([headline numbers](../ARCHITECTURE.md#headline-performance)) | before the public history | one 8-drive machine, or three machines with one drive each | benchmark results for specific configurations, not what the default `valsd` setup delivers |

Backup and restore is a separate qualification from the ingest runs. The
backup drill measured backup, restore and their effect on a light probe load;
it says nothing about ingest throughput, and the ingest runs say nothing about
recovery from a backup.

The three lab hosts are desktop-class machines (Intel Core Ultra 9 285HX, 24
cores, 61 GiB of RAM without ECC) linked by 10 GbE.

### Software lane

`scripts/ci.sh` at `ba4694a` on a clean checkout, run on 2026-10-28 on an
x86-64 Linux host with the pinned Rust 1.96.0. The locked build, the 18
script tests and Clippy passed (Clippy printed warnings, which the lane
reports but does not fail on). The test step ran 1,978 tests and failed 2,
both in the `vals-cluster` library: one test lost a race for a local port
(`Address already in use`), and one counter assertion saw a process-wide
counter move because other tests ran at the same time. Both passed when run
alone, and a second run of the whole `vals-cluster` library passed (1,004
passed, 7 ignored). Tests that share ports and process-wide counters can
fail like this when the suite runs in parallel on a busy machine; rerun a
failure before treating it as a regression.

### What the evidence does not show

- **Production use.** Nobody has run vals in production. The longest
  continuous runs above last hours, not months, and every run used
  synthetic or replayed load from one application.
- **Behaviour on server-class hardware.** The lab hosts have no ECC memory,
  and memory tests found bit errors on one of them. Long SPDK runs on these
  hosts ended in heap-corruption aborts (glibc `double free` and
  `free(): invalid next size`) and in checksum mismatches that the integrity
  checks caught before the data was published. None of the aborts was traced
  to a cause. They were not pursued further because the memory is known to be
  faulty, so a software cause has not been ruled out. If you see such an
  abort on ECC hardware, please report it.
- **Backup and restore at scale.** It was qualified once, with tombstone GC
  off and 4.85 million records. With `gc_grace > 0`, restore has the limit
  described under [Known limitations](#known-limitations).
- **Upgrades.** No upgrade from one revision to a later one has been
  exercised as a release procedure.
- **Security.** There is no authentication or encryption of data traffic;
  see the [security checklist](deploy.md#security-checklist).

## Compatibility and upgrades

Until a first tagged release there is no compatibility promise. APIs, config
keys, wire messages and on-disk formats can change in any commit, and
[`formats.md`](formats.md) describes the encodings as a snapshot, not a
frozen contract. What the code does today:

- **Wire protocol.** Every data connection starts with a version handshake.
  This revision speaks protocol version 2 only and refuses version 1 peers, so
  clients and servers must come from compatible revisions.
- **On-disk formats.** A newer binary reads the older formats it knows; an
  older binary refuses a newer format when it opens it, before writing
  anything. Once a newer format has been written, the only way back to an
  older binary is to rebuild that node from its peers
  ([details](deploy.md#on-disk-format-and-compatibility)).
- **PD state.** Do not run an older PD against a directory a newer PD has
  written to. A PD can go back to the old binary only until the new one
  writes its first PD mutation ([configuration
  basics](deploy.md#configuration-basics)).
- **Upgrading in place.** Check every config with the new binary's
  `--check-config`, then restart one process at a time: data nodes one at a
  time, PD followers first
  and the leader last, checking health between steps
  ([three-PD deployment](deploy.md#three-pd-reference-deployment)).
- **Backups.** Restore a backup with a build at least as new as the one that
  wrote it. Restoring into a new cluster is also the way to move between
  deployments that cannot be upgraded in place
  ([`backup-restore.md`](backup-restore.md)).

## Known limitations

These hold at the evaluation revision. The [Known limitations in
`ARCHITECTURE.md`](../ARCHITECTURE.md#known-limitations-and-open-problems)
cover the design-level ones.

- **Security.** Data reads and writes are not authenticated, nothing on the
  wire is encrypted, and the cluster secret that guards control operations is
  sent in cleartext. Run vals on a private network or an overlay and follow
  the [security checklist](deploy.md#security-checklist).
- **Consistency.** Concurrent writes resolve by last writer wins. There are no
  multi-key transactions, `cas` is best-effort, and quorum reads are not
  linearizable.
- **Durability.** A write is acknowledged from replica memory unless you ask
  for device durability per write ([recommended
  policy](getting-started.md#reading-and-writing)).
- **File backend.** Each node serves from one thread, which caps it at about
  the rates above. Device-durable writes sync on that thread, so reads mixed
  with them wait behind the sync (median read latency 15–29 ms in that
  benchmark). After a burst of writes, anti-entropy catch-up can stall reads
  for about 1.2 s every sweep for seconds to minutes
  ([`benchmarks.md`](benchmarks.md)).
- **Tombstone GC (`gc_grace > 0`).** Every node refuses a put older than its
  newest data minus the grace. Values older than that therefore never reach a
  new replica: not by anti-entropy after `add-node`, not by a range re-home,
  and not by `backup load`. Keep `gc_grace = 0s` if you will grow the cluster
  or restore backups of old data. With GC on, keep clocks synchronized: a
  rejoining node whose peers report watermarks more than `gc_grace` ahead of
  its own discards its data and resyncs from them, and a peer clock running
  ahead can trigger that.
- **Membership.** `remove-node` at RF 1 makes the removed node's keys
  unreachable, because nothing drains them first. There is no supported way
  to raise a zone's RF in place, or to change the number of PD voters in
  place. Dead nodes are not evicted automatically.
- **SPDK.** `scripts/build-spdk.sh` fetches SPDK by the tag `v24.09`, not by
  commit. Multi-drive nodes cannot add or retire drives online.

## Before you rely on it

1. Run the software lane at the revision you deploy, and record the commit.
2. Qualify on your own hardware (ECC memory), with your workload and at your
   size: a soak with kill and restart drills, and a backup and restore drill
   that verifies the restored data.
3. Deploy three PDs on independent hosts ([PD config
   reference](deploy.md#pd-config-reference)) and work through the
   [security checklist](deploy.md#security-checklist).
4. Use device-durable majority writes with majority reads where losing an
   acknowledged write matters ([recommended
   policy](getting-started.md#reading-and-writing)), and turn on the client
   failure detector (the "Replica outages" section of the `vals-client` crate
   docs).
5. Scrape the metrics and alert on node restarts; a data node exits with code
   75 when its integrity checks fail ([exit codes](deploy.md#exit-codes)).

The historical [production-readiness review](production-readiness-review-2026-08.md)
lists the original release blockers and their current disposition.

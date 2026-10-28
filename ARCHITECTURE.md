# vals architecture

`vals` is a distributed key/value store designed to run directly on raw NVMe
(userspace, kernel-bypass) and squeeze maximum IOPS out of the device. This
document is the design reference for people who want to understand how vals
works or change it. It describes the **target architecture** — the system we are
building toward — and marks each piece **✅ built** or **◻ planned** so it doubles
as a status map; the [Status](#status) section at the end summarizes what is
built, how it was validated, and what is still open.

If you want to *use* vals rather than work on it, start with the
[README](README.md) and [`docs/getting-started.md`](docs/getting-started.md).
[`docs/README.md`](docs/README.md) indexes the reference docs (wire and on-disk
formats, the algorithms every node must agree on) and the subsystem deep dives
that this document links to. For the planning hub and the rationale behind the
big decisions, see [`spec.md`](spec.md); open work is tracked in the backlog
(`backlog/`, indexed in `backlog.md`), and the shipped-work history lives in git.

The end-state vision in one breath: a thread-per-core, shared-nothing store where
each node drives many raw NVMe drives directly (JBOD); data is organized into
**zones** (per-zone replication factor, key ordering, placement scope); the data
path is **leaderless** (consistent-hash placement, tunable W/R quorum, HLC
last-writer-wins, read-repair + anti-entropy + hinted handoff healing); a raft
**PD** owns membership, the zone catalog, and the ring; clients write through a
tier-aware **batch API** with credit-pool backpressure; and the whole thing runs
on a unified per-core event loop that overlaps device and network I/O.

> **Data-path model: leaderless (Dynamo-style).** An early design picked Raft
> per partition on the data path. We pivoted: the workload is independent
> single-key writes at high volume plus follower-scalable reads, so there is **no
> leader and no consensus on the write path**. Raft is used **only in the control
> plane** (membership + the placement ring). The trade is explicit: we gave up
> multi-key atomicity and cross-key total order to get availability and linear
> write scaling.

---

## Crates

| Crate | Responsibility |
|---|---|
| `vals-spdk` | `BlockDevice` trait + backends: `MemBlockDevice`, `FileBlockDevice`, and the real userspace-NVMe `SpdkBlockDevice` (behind the `spdk` feature). Async read surface (`AsyncRead::read_async`; `SpdkBlockDevice` adds inherent `read_at_async`/`write_at_async`), synchronous batched `read_batch`, per-core qpair sharding (`open_shard`), `poll_device` for the unified loop, DMA pool, 4 KiB-logical-over-512 B blocks. |
| `vals-store` | Single-node log-structured storage engine: `alloc` (segment allocator), `vlog` (append-only value log + CRC + group commit), `index` (pluggable hash / ordered), `engine` (`put`/`get`/`delete`/`scan`/`sync`, log-replay recovery, `compact` GC, `multi_get`/async batched reads). |
| `vals-rt` | Thread-per-core async executor: `block_on`, `join_all`, `quorum_ok`/`quorum_collect` (first-k completion), spawner. Hand-rolled `Rc` wakers; integrates a `poll_io` hook so SPDK and the network reactor share one event loop. |
| `vals-raft` | `raft-rs` wrapper (`Partition`): drive / `take_outgoing` / `step`, protobuf message codec, **durable storage** (`PdStorage`: HardState/log/snapshots persisted, engine-dump snapshots + log compaction, real snapshot-apply state transfer). **Control-plane only** — it backs the PD, not the data path. |
| `vals-client` | The slim client-facing crate: the leaderless `Coordinator` (W/R quorum writes/reads, durability tiers, digest reads, read-repair, CAS, batches, scans), the pluggable `Transport` seam with the TCP `NetTransport`, and the wire value types — with **none** of the server/PD/storage-engine code in its dependency graph (only `vals-rt` + `libc`/`socket2`/`log`). A client-only app depends on this; `vals-cluster` re-exports it. |
| `vals-cluster` | The distributed layer (see below): HLC/versioning, ring, coordinator, replica store, transport, healing, PD. Depends on `vals-client` and re-exports its surface, so a server embeds this one crate. |
| `vals-server` | `valsd`, the server daemon: `node` / `pd` subcommands over a hand-rolled `key = value` config (see `docs/deploy.md`), boot-state seeding (members/zones proposed once, idempotently), a local unix-socket PD admin channel (`valsd admin`), and SIGTERM/SIGINT → graceful shutdown. |
| `vals-bench` | Benchmark + correctness harness: single-node engine workloads; SPDK device/multi-drive (`mdwrite`/`mdread`); distributed (`dist`/`distmc`/`distshard`/`distmcp`); real-NVMe cluster (`distspdk`/`distspdkmc`); multi-drive × multi-node scaling (`distmdmc`); and off-hardware discrete-event mocks (`pipewrite`/`mcread`) that model server-side queue depth without a device. |

### `vals-cluster` modules

- `hlc` — shared atomic hybrid logical clock + `Version`; a CSPRNG-generated
  64-bit writer incarnation breaks equal-HLC ties into a last-writer-wins total order.
- `replica` — `VersionedStore`: store-if-newer LWW `apply`, versioned tombstones, blind `put` + single-node `cas` (locally atomic; distributed CAS is best-effort — see Consistency model).
- `ring` — consistent-hash ring: vnodes, `replicas(key)` = next RF distinct nodes clockwise.
- `coord` — `Coordinator<T: Transport>`: W/R quorum fan-out, durability tiers, read-repair, hint capture.
- `wire` — binary frame codec (length-prefixed, opcode/kind-tagged; hand-rolled LE, no serde).
- `net` — `NetTransport` (async TCP, one **multiplexed** conn per node-shard, request-id demux), `ReplicaServer`/`ShardedServer` (async `ConnDriver` per connection).
- `reactor` — epoll-based readiness driver (per-fd waker, `EPOLLONESHOT`); the network half of `poll_io`.
- `antientropy` — Merkle tree (256 buckets) + scoped pull reconcile.
- `rt_sync` — range-tombstone set exchange per sweep pass (scoped clip, digest, paging; sorted zones).
- `hints` — `HintStore` trait + `MemHintStore` / `FileHintStore` for hinted handoff.
- `node` — `Node`: one executor thread = replica server + peer transport + periodic anti-entropy sweep.
- `pd` — `PdDaemon`: raft-replicated membership, zone catalog and range maps; pushes
  `SetMembers`, `SetZones` and `SetRangeMaps` to data nodes.
- `observe` — the observability floor: the standard `log` facade (a built-in stderr logger, level via `VALS_LOG`; raft's slog is bridged onto it) + process-wide striped counters (`observe::snapshot()`). Every state-changing transition — liveness wipe (plus a durable pre-wipe marker via the liveness log), fence/unfence, detector trip/recover, ring/catalog/range-map adoption, zone teardown, compaction/sweep outcomes, PD push/drive/propose failures, hint + credit saturation — logs at the site and bumps a counter.

---

## Data path (leaderless)

The cross-node agreement algorithms below (the placement hash, HLC/LWW order, the
Merkle digest, CRC) are specified bit-for-bit in [`docs/algorithms.md`](docs/algorithms.md);
their serialized forms are in [`docs/formats.md`](docs/formats.md).

### Placement — two levels

1. **Key → node(s).** A consistent-hash ring maps `hash(key)` to a point; the
   key's **preference list** is the next `RF` distinct nodes clockwise. The first
   is the "primary" but carries no special authority — all RF replicas are equal.
   The ring is a pure function of `(members, vnodes)`, so every node and
   coordinator computes the same layout from the same membership.
2. **Node → shard/core.** Within a node, a key routes to shard `hash(key) % N`,
   where each shard is an independent `ReplicaServer` on its own core
   (thread-per-core, shared-nothing). Membership/anti-entropy operations are
   node-level (shard 0). A node with multiple NVMe drives backs each shard with a
   physical drive region (JBOD): the `vals_cluster::drive` layout policy spreads
   the node's `P` partitions across its `M` drives (capacity-weighted), and the
   node weights its **ring presence by drive count** so heterogeneous nodes
   balance. Partition count `P` is uniform cluster-wide (drives sit *below*
   partitions), so `hash % P` routing and per-shard anti-entropy are unchanged;
   `Node::serve_multidrive` builds and serves it and the weight propagates through
   the PD + `SetMembers`. See [`docs/multi-drive.md`](docs/multi-drive.md). The
   **non-zoned library path** supports crash-journaled online add/retire through
   `NodeHandle::rebalance_drives` / `NodeRebalancer`; the zoned/JBOD daemon path has
   no operator/admin integration and still requires an offline rebalance.

### Write path

1. The coordinator computes `replicas(key)` from its cached ring.
2. It stamps **one** `Version` (HLC, see below) and fans the write out to all RF
   replicas in parallel (`vals_rt::join_all`).
3. It returns as soon as **W** replicas ack (`quorum_ok`); the `WriteConcern`
   sets W, the write's **replication breadth** (how many replicas have the write):

   | `WriteConcern` | W = replicas that must ack |
   |---|---|
   | `Unacknowledged` | 0 (fire-and-forget) |
   | `One` | 1 |
   | `Majority` | `floor(RF/2) + 1` (a strict majority) |
   | `Quorum(W)` | exactly `W`, `1 <= W <= RF` (caller's responsibility to keep `W + R > RF`) |
   | `All` | RF |

   An impossible concern (`Quorum(0)`, `Quorum(W)` above the key's replica count,
   or `Unacknowledged` with device persistence) fails with `InvalidWriteOptions`
   before anything is sent; it is never clamped.

   `Majority` is the concern to reach for when a strong read must always see the
   write: paired with a majority read quorum (`R = floor(RF/2) + 1`), `W + R > RF`
   holds at **every** RF, odd and even (odd → `RF + 1`, even → `RF + 2`).
   `Quorum(W)` is the escape hatch for a hand-picked `W`; note `Quorum(RF/2)` is
   the classic even-RF off-by-one — `Quorum(2)` at RF=4 with R=2 gives `W + R = RF`,
   *not* `> RF`, so prefer `Majority` unless you have a specific reason.

4. Each replica applies the write via `VersionedStore::apply` (store-if-newer).
   A replica that was unreachable produces a **hint** held by the coordinator and
   replayed when the replica returns (hinted handoff).

> **Write guarantees have two independent axes: breadth (W) × persistence.**
> `WriteOptions { concern, persistence }` selects them; the concern sets only W,
> identically for single and batch writes. The default (`write(k, v)`, and every
> method given a bare `WriteConcern`) is `Persistence::Buffered`: a replica acks
> after installing the write in its in-memory vlog tail. This survives
> uncorrelated replica/process loss while an acknowledging machine remains alive,
> but not a correlated power event. `WriteOptions::device_durable(concern)` selects
> `Persistence::Device`: each counted replica ack waits for the
> shard/zone group-commit device sync. Concurrent single and batch requests share
> the same barrier, so one sync releases all requests whose writes were installed
> before it. `Unacknowledged` waits for W=0, so it is rejected with `Device` and
> provides no persistence
> guarantee. A completed device write is durable only to the
> drive's flush/FUA contract (or its power-loss protection); `sync` forces that
> software-visible barrier. The device flush is asynchronous: the shard keeps
> serving while it runs. Shards on one NVMe namespace share a node-wide flush
> domain with at most one flush in flight (see
> `docs/batch-writes.md`).

Two write modes: **versioned** (`cas`, optional `if-version` precondition;
version returned) and **blind/fast** (`put`, no read-back; the server still stamps
a cheap HLC so replicas converge). The `if-version` precondition is **best-effort
optimistic concurrency, not a linearizable compare-and-swap** — see the
Consistency model for the explicit non-guarantee. The distributed
`Coordinator::cas(key, value, expected)` is a coordinator-side read-modify-write:
a **majority read** for the current version → precondition check → a **majority
write** of the new value (so `W + R > RF`), returning `CasResult::Applied(version)`
or `CasResult::Mismatch { current }`. Because vals is leaderless the read and the
write are two separate quorum ops — a writer landing **between** them can be lost
(classic lost update) and two racing CAS calls can both pass the same precondition.
It *narrows*, but does not close, the lost-update window; atomicity needs a per-key
consensus path (the opt-in linearizable-zone mode, `docs/future-linearizable-zones.md`).

### Read path

Reads request **R** responses and pick the max `Version` (LWW). `R=1` reads any
single replica (cheap, possibly stale — follower read); higher R compares
replicas and **read-repairs** laggards inline. A tombstone winner reads as
absent. With strict quorum, `W + R > RF` guarantees the read observes the
acked write's version or a greater one (see Consistency model).

A `ReadConcern` sets R from each key's replica count, the way `WriteConcern`
sets W:

| `ReadConcern` | R = replicas that must answer |
|---|---|
| `One` | 1 |
| `Majority` | `floor(RF/2) + 1` |
| `Quorum(R)` | `R`, clamped to `1..=RF` |
| `All` | RF |

Unlike a write concern, a read concern is never refused: `Quorum(0)` reads one
replica and `Quorum(R)` above RF waits for every replica. Point reads, batched
reads and scans all take `impl Into<ReadConcern>`, and a plain `usize` means
`Quorum(n)`, so a fixed count is applied unchanged in every zone. That is the
mixed-RF trap: `R = 2` overlaps majority writes at RF 3 but not at RF 5
(`W = 3`, `3 + 2 = 5`). `ReadConcern::Majority` is computed per key (per range
for scans), so majority writes and majority reads overlap in every zone
whatever its RF. The client's recommended policy for data that must not be
lost is device-durable majority writes plus majority reads; the `write()`
default stays buffered `All` for compatibility (see the `vals-client` crate
documentation, "Choosing a policy").

### Healing

Three layers, weakest-but-cheapest to strongest-but-slowest:

- **Read-repair** (inline): on an `R>1` read, replicas behind the winning version
  are updated from the responders.
- **Hinted handoff**: a write that couldn't reach a replica is stored as a hint
  and replayed on the replica's return (also auto-drained on a later write that
  touches a now-reachable replica).
- **Anti-entropy** (backstop): each node periodically runs a placement-scoped
  Merkle compare against peers and pulls (store-if-newer) any keys it owns and is
  behind on. Scoping is essential — an unscoped pull would over-replicate and
  break RF. `VersionIndex` caches logical Merkle leaf groups plus per-bucket
  generation/count/dirty metadata and a mutation journal; it is **not** a resident
  per-key version map. Dirty buckets are rebuilt exactly from engine
  logical-winner digest pages (run version streams plus mutable state) before their tree
  can be served. In the `capacity` profile, store-if-newer and digest version
  probes resolve through the engine and may read run index blocks. Each probed
  key is hashed once and every run's filter line is prefetched before testing.
  This cut probe CPU by 77% at 10 runs/engine and gave +10.9% ingest ACK/s;
  the `latency` profile's winner directory provides the resident winner lookup.
  **Engine-wide key filter:** the write-path `probe_version` first tests a
  per-engine, in-RAM scalable Bloom filter covering every run marked *covered*. On
  a miss it skips all covered runs and probes only uncovered ones, so the common
  new-key write costs one filter probe instead of one per run. Flush and merge
  builds insert their keys; a run recovered at open stays uncovered (probed
  individually) until a bounded maintenance cover-scan streams its keys (cover-scan
  chunks get a preferred compaction lane ahead of ordinary merges). Reads that resolve
  through `lookup_runs` use the same filter. It is off under the winner directory.
  Nothing is persisted. See [`docs/out-of-core-index.md`](docs/out-of-core-index.md#engine-wide-key-filter-0364).
  **Run format v5:** new runs no longer write the per-run sidecar (a second
  copy of every `(key, version)`); AE seeds, digest pages, `recovered_versions` and
  cover-scans stream the same entries (tombstones included, key order, footer bucket
  modulus) from the data blocks. v1–v4 runs keep their sidecar paths (except v4
  digest-page continuations, which seek through the block index) and merges
  converge to v5. Downgrade to a pre-v5 binary fails closed once a v5 run exists;
  `VALS_RUN_WRITE_VERSION=4` keeps writing sidecars for a rollback window
  ([`docs/deploy.md`](docs/deploy.md#on-disk-format-and-compatibility)).
  **Run format v6:** the block index carries each block's largest HLC
  (`max_hlc`, 8 B/block resident) and an index trailer locates an optional
  range-tombstone record block and its `rt_seq_floor`, groundwork for range deletes
  ([`docs/range-deletes.md`](docs/range-deletes.md), layout in `formats.md` §2.2a).
  Merges write at least their newest non-sidecar input's version; pre-v6 binaries
  refuse v6 runs, and `VALS_RUN_WRITE_VERSION=5` keeps a rollback window.
  **Range-tombstone engine core:** a sorted engine keeps a
  canonical fragment set (`range_tombstone::FragmentSet`), appends RTs as record kind
  2, replays unflushed ones from the tail, and moves RT records into flush and rewrite
  outputs (each record in exactly one run); the first RT commits a v6 marker run so
  older binaries fail at run open. Unflushed RT records keep their log segments
  protected like memtable values.
  **Fail-loud newer formats:** an intact unknown manifest/chain version,
  segment format version or record kind fails `Engine::open` instead of being skipped.
  **Resumable AE digest pages:** a sparse differing-bucket set can need a scan of
  every run, which took far longer than the 5 s peer RPC deadline, so abandoned scans
  kept the digest-serving cap busy. With hello feature bit 2, a `BucketDigests` page
  stops about 1 s in at a run-window checkpoint and returns a resume key; the merge
  advances the lagging run first so the key always makes progress. In-node AE follows
  resume keys and saves its cursor after at most 16 pages per pass
  ([`docs/formats.md`](docs/formats.md#15-reply-paging-scan--multiscan--bucketdigests--values--mixedbatchread)).
  **Run blocks and filter bits:** new runs close data blocks at ~32 KiB of
  estimated entry bytes (was 4 KiB; `VALS_RUN_BLOCK_TARGET_BYTES`), so the resident
  fence index (one first-key copy per block) drops from ~8.5 to ~1.1 B/entry for
  148 B keys. Run blooms are 10 bits/entry (was 12) and the key filter uses a
  constant 12 bits/key with 2× sub-filter growth (was 13→18 bits at 1.5×; ~1.75%
  compound FPR and ~1.7 B/key modelled at 440M keys/engine). Block length and bloom
  geometry are stored per run, so mixed old/new runs need no format change. A run
  probe now reads and CRC-checks one ~20 KiB block; `block_lookup` compares
  front-coded keys in place without allocating per entry, and scan cursors seek to
  their lower bound in place and decode entries lazily (`run::BlockCursor`). `vals_run_{fence,filter}_resident_bytes` and
  `vals_run_entries` export the per-entry split
  ([`docs/deploy.md`](docs/deploy.md#memory-sizing)).
  **Bounded merges and a frozen top tier:** no recurring or forced merge
  reads more than `VALS_MERGE_INPUT_CAP_BYTES` (2 GiB). Runs at or above
  `VALS_MERGE_FREEZE_BYTES` (default: the cap), or too large for any size-ratio
  balanced window under the cap (> cap × r/(1+r), 1.2 GiB at r = 1.5), are frozen.
  Only space reclaim (pressure, shadow folds, aged value-log GC, tombstone GC,
  forget) rewrites them, so run count grows ~linearly with data ÷ cap. The run-count
  valve (`VALS_MERGE_FORCE_RUNS`, 6) now counts only the active tail newer than the
  newest frozen run. It merges the cheapest capped adjacent window there, and it is
  paced like every recurring merge. The old valve forced the two newest runs past the
  cap: a multi-billion-event soak saw merge output per write rise 195 → 1,026 B, and a
  micro-simulation shows rewrite amplification growing without bound (308× at
  160 GB/engine). The new valve holds it at ~1.5× merge input per flushed byte.
  ([`docs/algorithms.md`](docs/algorithms.md), [`docs/configuration.md`](docs/configuration.md)).
  **Run-count costs:** shadow estimation caches fixed, run-id-seeded
  block samples. Flushes read only new runs and probe only newly added newer-run
  blooms; removing a newer run re-evaluates surviving cached samples without I/O.
  Memtable generations invalidate the estimate without resampling. Staged GET
  uses the key-filter negative to skip covered runs, and range fences precede
  bloom prefetches. Merge policy is captured at engine open; cover scans avoid
  exactly the next recurring/forced window, including inner windows. Inner-window
  merge counts advance only at commit. `vals_stranded_runs` and
  `vals_engine_stranded_runs` report non-frozen runs older than the newest frozen
  run, which recurring merges cannot take.
  **Capped shadow folds:** overwrite/delete garbage in frozen runs used to
  have one reclaim path, an uncapped fold of everything from the oldest garbage run
  to the newest, which at ~100 runs either never fired or rewrote most of the
  engine and collapsed the frozen tier. A fold now rewrites one window of adjacent
  runs (≤ the merge input cap, or one larger run) in place, streams the other runs'
  keys and versions read-only, and drops window entries a newer copy shadows. With
  a complete source set it also collects tombstones no other run holds. Folds are
  paced and allowed at the floor. The shadow estimator subtracts the bloom
  false-positive mass of all newer filters. Pre-floor pressure with nothing gainful
  falls back to recurring merges. On the L1 gate (97 frozen runs, 1.5M overwrite/
  delete ops) the old path wedged at 3.76× space while the new one held ≤ 2.70×
  with every fold ≤ the cap; the price is streamed source reads
  ([`docs/algorithms.md`](docs/algorithms.md)).
  All readers choose argmax(`Version`), independent of physical history.
  **Structural epoch reuse is enabled automatically:** a strictly newer,
  previously nonzero range authority with the same complete per-store placement
  inputs retains AE seed generation and rebases identical-map logical counts.
  Unready counts stay unready; real map/topology changes and unknown/lower
  authority still invalidate. Routing, mirror receipts, and cleanup fences remain
  epoch-sensitive. This avoids unrelated-zone epoch-only whole-zone reseeding
  without changing durability or adding key-sized state; see
  [`docs/algorithms.md`](docs/algorithms.md#epoch-only-seed-reuse-0294-enabled).
  **Range-bounded split recount:** a boundary change (split, merge, or
  nested change on a pending span) no longer clears AE leaves or refolds the
  store. It re-partitions exact per-range counts and recounts only the unknown
  children, in bounded turns of 1024 winners or 1 ms. One child is derived as the
  total minus the counted ones. Readiness stays false until every span finishes.
  At 95M keys/node this took whole-write ingest from 465k to 816k ACK KV/s
  (1.755×) with zero seed folds.
  Recount pages prefetch their run blocks with concurrent async reads under the
  shard read gate, then merge synchronously up to a horizon (15× less shard
  time per key). A rehome commit that only reorders a range's replicas keeps every
  store's seed, because leaves key on the sorted set. The PD auto-split loop
  splits every over-threshold range per zone per poll, up to 16. With all
  three, a 400.7M-key ingest run on two nodes keeps every range near the 1M target
  while writing, settles in 21–91 s, and sustains 1.15M ACK KV/s with production
  defaults.
  **Seed isolation (`ae_seed_isolation`):** after a restart, seeding no longer
  starves the apply path. The shard accounting task is the only seed driver (AE
  handlers answer Busy until seeded) and pauses to hold seeding to
  `ae_seed_duty_pct` (25%) of shard time while foreground work is recent or in flight. The
  budget is windowed and covers split recounts, and run-churn escalation stops at 50%. An
  in-flight seed survives split/merge generation bumps (its counts are remapped
  through the split recount at publication). The seed journal is charged to the
  RAM broker and capped node-wide, spilling to direct staged deltas
  ([`docs/backpressure.md`](docs/backpressure.md)). The PD holds new re-home starts
  (never commits) for a bounded time per episode while a node is down or reseeding after a
  restart ([`docs/zones.md`](docs/zones.md)). An in-flight seed also survives merges:
  it pins its starting run set, whose segments stay allocated until it publishes or
  resets, instead of restarting on every run removal (a long soak saw 161 seed
  restarts, no seed completing under ingest, and 23% lower ack throughput). Pinned
  bytes are capped node-wide
  (`ae_seed_pin_max_bytes`) and revoked under region pressure.
- **Over-replication cleanup** (the inverse of anti-entropy): a node *releases*
  keys it no longer owns (after a join/leave shifts ownership, or a sloppy
  fallback write) by handing them off to the rightful owners and then physically
  forgetting them — **no tombstone**, so the owners' copies aren't deleted, and
  only after the handoff is confirmed so the last copy is never dropped. A key is
  forgotten only if its **local version still matches the one handed off**: a
  request handler can interleave a *newer* write on the node's executor during the
  handoff awaits, and that write was never handed off — re-checking the version
  before the (tombstone-less) `forget` keeps it instead of silently dropping it (a
  later pass hands the new version off first).
  **Range tombstones.** In a sorted zone, each cleanup page first hands the
  fragments of F that lie in released ranges to their owners as `RangeDelete`
  slices. A piece is released once enough owners confirm it and the local
  accounted set A already covers it. Covered entries are never handed off. The
  forget rewrite drops every entry under a released piece and clips the
  piece out of the RT records (`docs/range-deletes.md` §5).
  **Sorted zones wait for placement.** A shard that learns a sorted zone
  from a PD catalog push (or a persisted boot catalog) holds that zone until it
  adopts its first `SetRangeMaps` push: the sweep skips its reconcile and cleanup
  (`vals_sweep_sorted_zones_held`), and the write fence answers sorted writes with
  retriable `StaleEpoch`. Before this, the boot gap between catalog and range maps
  made cleanup use a single-range fallback and hand keys to a hash-routed transport,
  planting wrong-shard strays (or, at RF=1, forgetting the only copy). After the
  first push a mapless sorted zone is single-range, and the handoff transport routes
  every catalog sorted zone by its map or the single map, never by key hash.
  Existing wrong-shard strays are handed to the token shard and forgotten by this
  cleanup once the owners confirm.
  The hold is per shard, not per zone, and ends at the first push. A sorted zone
  created later (`create-zone ... presplit=`) is therefore served single-range in
  the gap (normally milliseconds) between its `SetZones` and its initial map's
  `SetRangeMaps`. This is
  deliberate: a node cannot tell a coming initial map from none (a zone
  without presplit never gets one, and the PD can drop an unauthorized one —
  `CommittedMapDropped`), so a per-zone hold could hold that zone forever. The
  worst case is strays, not loss: the zone is empty at creation; a write admitted
  in the gap lands on the single-range owners; after the map arrives the changed
  placement fingerprint restarts cleanup, which hands each such key to its real
  owners and forgets it only once they confirm. A read in that window can miss such
  a key, which is why `create-zone ... presplit=` tells the operator to confirm
  the layout with `ranges <zone>` before using the zone
  ([`docs/pd-admin.md`](docs/pd-admin.md)).
  **Cleanup pacing.** Each traversal starts at a key sampled from the
  zone's run blocks, runs to the end of the keyspace, then laps from the start
  back to that key, so restarts that interrupt traversals do not starve the keys
  late in the keyspace. A sweep pass runs one 8,192-key page per zone, and keeps
  running further pages while the shard sees no foreground request, until the pass
  has spent min(`cleanup_idle_burst_ms`, half the sweep interval) in cleanup across
  its zones. File-backed
  release L1 (one stray behind 1M keys, 1 s sweep): 2.0 s / 496k keys/s with the
  burst, 124 s / 8.1k keys/s with it off.
  `vals_cleanup_scanned_keys`, `vals_cleanup_traversals` and
  `vals_cleanup_idle_burst_pages` show the speed;
  `vals_cleanup_probe_released_keys` counts not-owned keys confirmed by the
  batched owner digest probe ([`docs/algorithms.md`](docs/algorithms.md)).

### Failure detection

An **opt-in, coordinator-side circuit breaker** (`failure::FailureDetector`)
observes RPC outcomes per node: after a few consecutive transport errors
(timeouts included) a node trips open and the coordinator **fast-fails** RPCs to
it — no connect attempt — so the write path hints it. Reads order each key's
replicas healthy first, so a tripped node is only asked when its peers cannot
make the read concern. It auto-recovers by admitting a single probe after a
cooldown; a read sends a due probe in addition to the replicas it needs, so the
probe never holds the read. With digest hedging, a replica whose reply a read
gave up on after the hedge fired also counts as a failure, so a silent node trips
after a few hedged reads rather than after RPC deadlines. It is request-driven (no
heartbeat thread) and informs **routing only**: it never changes membership. The
cluster rides a failure out via hints + sloppy quorum and recovers when the node
returns; eviction stays an explicit PD decision. Hints are replayed only to a node
that has just acknowledged a write, in windows of 64 that stop at the first
failure, so replay never makes a write wait on an unreachable node. The client
guidance (recommended detector and hedge settings, measured outage behaviour) is
in the `vals-client` crate documentation, "Replica outages".

---

## Control plane (PD)

A small raft group (`raft-rs` via `vals-raft`) holds the **membership**, the
**ring shape** (`RingConfig`: vnodes + default-zone RF), the **zone catalog**, and
the per-zone **range maps**, from which every node derives an identical ring and
placement. The `PdDaemon`:

- represents membership as a replicated log (join = put, leave = delete); each
  member carries a **ring weight** (∝ its drive count) so heterogeneous nodes
  balance, plus its **shard addresses** (`add_node_addressed` — one address per
  server shard, index-aligned with the node's shard layout), and the leader
  stamps each push with a monotonic **config epoch** (= the raft applied
  index) — a node ignores a lower-epoch (stale/reordered) push. Addresses are
  raft state deliberately: they must be agreed and durable with the membership
  they belong to, travel in every `SetMembers` (and the `Members` reply), and
  are what make a **grown-in node reachable** — nodes merge them into a peer
  address book and the anti-entropy sweep adds unknown members to its transport
  (same-index shard) before each pass, so a node that joined after everyone
  else started still gets anti-entropy, handoff, and cleanup traffic. The
  pusher likewise merges committed member addresses into its target list, so a
  node absent from the PD's boot-time `data_nodes` is itself pushed the
  ring/catalog/range maps;
- on each committed change, the leader pushes the relevant state to every shard
  address of every live data node over TCP — `SetMembers` (members + `(epoch,
  vnodes, rf)`), `SetZones` (catalog), `SetRangeMaps` (sorted-zone ranges; sent
  even at range-map epoch 0 with no maps once the catalog is published, so nodes
  can release their sorted-zone hold) — and
  **retries any node not yet confirmed at the current epoch until it acks** (the
  push reply carries the node's adopted epoch), so a node unreachable at push time
  is healed on recovery rather than left holding a stale ring/catalog indefinitely.
  V4 range-map capability preflights and pushes retry per endpoint, so one slow
  shard cannot globally block delivery to healthy shards; the sending connection
  negotiates V4 again before any opaque map bytes are sent. Per-endpoint attempts,
  blocks, failures, and last-confirmed age are exported for delivery diagnosis.
  Pushes (and the restart beacon) run on a **dedicated pusher thread**: the drive
  loop — which also ticks raft — publishes each push kind's committed payload
  pinned at its config epoch plus current leadership, and the pusher delivers
  against that snapshot (clearing its per-shard confirmed maps while not leader,
  so a newly-elected leader re-pushes to everyone). A slow or black-holed data
  node therefore costs only push latency, never raft heartbeats/elections;
- runs a **restart beacon**: every second the leader probes each *confirmed*
  shard's adopted ring epoch (read-only `Members` query, 250 ms bound); a shard
  reporting an epoch below what it once confirmed has restarted (epoch 0, boot
  ring, empty catalog), so its confirmed entries are cleared across all three
  push kinds and the current config is re-pushed — a rolling restart heals in
  ~1 s instead of waiting for the next unrelated config change. Nodes complement
  this by re-opening their persisted zones at boot (`ZoneStore::existing()` ∩
  boot catalog, placement scope ignored so leftover data from a moved-away zone
  stays visible for over-replication cleanup) and by re-seeding the
  data-progress watermark from recovered versions;
- tracks pending intents (membership / zone / split changes not yet committed)
  as **ordered per-key queues**: only the oldest not-yet-reflected intent per
  node / zone id is live, re-proposed each round until committed state reflects
  it (guards against a command proposed before the leader is known being
  silently dropped), then popped so its successor goes live — an
  add-then-remove or create-then-drop sequence executes both ops in order
  instead of the inverse op being mistaken for already-applied and lost;
- is **restart-durable** when spawned over a storage directory
  (`spawn_single_durable` / `spawn_durable` → `vals_raft::PdStorage`): HardState
  (term/vote — a restarted replica can't double-vote in its old term), the log,
  and snapshots are persisted (atomic write → fsync → rename per file,
  CRC-checked), the log is compacted behind periodic **engine-dump snapshots**
  (a real state transfer — a lagging or restarted member's state machine is
  rebuilt from snapshot data, then the committed suffix replays), and the config
  epoch **resumes** after a restart. This matters because the epoch guard cuts
  both ways: a memory-only PD restart resets the epoch to ~0, so every
  subsequent push would be silently ignored by the data nodes while the PD's
  confirm logic counts it delivered — reconfiguration bricked cluster-wide.

**Control-op authentication (cluster secret).** The mutating control ops
(`SetMembers`/`SetZones`/`SetRangeMaps`) arrive on the same listener as the data
path, and placement is cluster-destroying in the wrong hands — one stray frame
with `epoch: u64::MAX` would tear down every non-default zone or brick the node
against all future legitimate pushes (every real epoch is then lower). So each
`Set*` request carries an **auth token**, checked (constant-time) against a
process-wide **cluster secret** (`auth::init_cluster_secret`, set once at startup
on every node and PD — "libraries verify, applications init", like `observe`);
a mismatch is refused with a distinct `Response::Denied` (warn log + a
`control_denied` counter; the PD surfaces a denial as a named misconfiguration
rather than a retry-forever). With no secret configured the gate is open (tests,
single-process benches). The **PD raft peer port** is gated by the same secret:
an inbound peer connection must open with an auth **hello frame** (magic +
token, size-capped pre-allocation) before any raft message is decoded —
otherwise a forged high-term message from a stray connection could depose the
leader or fabricate committed membership, which the PD would then push *with a
valid secret*. Threat model: this stops stray/unprivileged-but-reachable
senders; the token is cleartext on the wire, so an *on-path* observer can replay
it — transport security is delegated to the network (a private network or an
overlay such as Tailscale/WireGuard; in-process mTLS is out of scope), and the
read-only control queries (`Members`/`Zones`/`RangeMaps`), like the data ops,
stay open.

Coordinators close the loop the other way: `Transport::members()` +
`Coordinator::refresh_ring()` let a coordinator poll a node's membership view and
update its own ring, so client placement tracks reconfiguration — including the
members' shard addresses, which the refresh feeds to `Transport::add_peer` so the
coordinator can route to a node that joined after its transport was constructed.
(Refresh is on demand by default — and automatic on a stale-ring refusal, see
Transport below; `CoordinatorService::with_proactive_refresh(period)` adds a
periodic refresh.)

The PD is **not on the data hot path** — it changes only on membership events.

---

## Transport

Custom binary protocol over TCP (no gRPC/serde on the hot path):

- **`wire`** — length-prefixed frames `[u32 len][payload]`, opcode/kind-tagged.
  Tombstone (`None`) vs empty value (`Some(&[])`) are distinct on the wire. Protocol
  v2 carries the 16-byte `[u64 HLC][u64 writer]` version; each side sends only its
  hello until it accepts its peer's hello, then releases queued requests. V1 peers
  are rejected rather than receiving a request they would decode with a truncated
  writer. The byte-level opcode/field reference is in
  [`docs/formats.md`](docs/formats.md).
- **`NetTransport` (multiplexed)** — async `Transport` impl with **one TCP
  connection per node-shard** (`MuxConn`). Every concurrent request shares that
  socket, tagged with a `u64` request id; replies are demultiplexed back to their
  `MuxRpc` futures by id. This replaced an earlier pool of
  one-request-per-socket connections that opened/tore down a socket per concurrent
  request and, at high fan-out, melted the kernel's socket/fd path (a "connection
  storm" — profiling found ~80% of CPU in the net stack and ~100% quorum failures
  past 4 coordinators). With mux it's one socket per peer carrying all
  concurrency. A **cancelled request just drops its demux slot** — the late reply
  is discarded by id; no poison, no reconnect. Connect is non-blocking (`socket2`;
  the write loop parks on writable=connected). Frames: `[u32 len][u64 reqid][payload]`.
- **`reactor`** — epoll with `EPOLLONESHOT` and a per-fd waker map; `poll_io`
  wakes only ready fds. `poll_io_nonblocking` + `BlockDevice::poll_device` are the
  seams that let one loop drive both the SPDK qpair (busy-poll when device I/O is
  in flight) and the sockets (park when idle). "In flight" is gated reads, device
  waits/flushes, **or any submitted-but-uncompleted SPDK async I/O on the thread**
  (`vals_spdk::async_io_pending`), so ungated background reads (sweep cleanup
  scan, AE dirty-tree rebuild) complete on an idle shard instead of freezing the
  sweep; `vals_sweep_pass_age_seconds` exposes a frozen sweep.
- **server** — `ReplicaServer` is one `LocalExecutor` per thread with a
  non-blocking accept loop and a per-connection `ConnDriver`; the store is local
  to that thread behind `Rc<RefCell>` (no locks). The driver serves many requests
  on its connection **concurrently** — every device-reading op (point `Read`, range
  `Scan`, and the anti-entropy `SyncBuckets` pull) runs on a spawned task so its
  device I/O overlaps across connections, while control/apply ops run inline. The
  read ops are **borrow-free**: each builds its read plan(s) under a brief store
  borrow, drops the borrow, then executes the device reads off a shared device
  handle (a multi-key `Scan`/`SyncBuckets` page issues all its reads at once, queue
  depth = page size). This keeps the single serving core from busy-blocking at QD1
  inside a sweep's bucket pull — the failure that once made anti-entropy the
  cluster's throughput bottleneck on SPDK (see [Status](#status)). An `Apply` is
  staged: it completes inline when the memtable or winner directory can decide
  it, and is spawned onto a task when it needs a run read
  (`StagedApply::NeedsRunRead`) or a re-home mirror. `ShardedServer` runs N of
  these.
- **Safety bounds.** Hard caps bound a desynced/hostile/slow peer's blast radius:
  a 64 MiB **`MAX_FRAME`** fails the conn before buffering an over-cap length
  (remote-OOM guard, both readers); a 256 MiB per-conn **`MAX_CONN_SEND_BUF`**
  high-water mark fails/closes a conn whose queued-but-unsent bytes back up behind a
  peer that won't drain; and finite default connect/hello and per-RPC **deadlines**
  (both 5 seconds, checked by the reactor flush hook) fail a request whose connection or
  reply never completes with `TransportError::Timeout`, so a silent **black-hole**
  peer is observable to the failure detector instead of hanging forever. A
  connection that receives nothing for a whole RPC deadline (the connect deadline
  during the handshake) while it awaits a reply is closed, even if every caller has
  since given up on its request, so the next request dials afresh instead of
  waiting out TCP retransmission or SYN backoff to a host that has come back. A batched
  read whose RPC times out or finds the node unavailable fails its keys on that
  node at once instead of retrying them one by one. The default
  network coordinator also limits caller write key/value pressure globally and per
  destination node; these logical credits are not a byte-for-byte transport heap cap. The two replies that can legitimately outgrow a frame — a range
  **scan** and an anti-entropy **bucket pull** — are **paged** under a server
  reply budget (`SCAN_REPLY_BUDGET`, 8 MiB) with resume cursors, so `MAX_FRAME`
  never has to be the answer to "how big can a range get" (see
  `docs/formats.md` §1.5 and `docs/algorithms.md` §Distributed range scan).
- **Stale-ring enforcement.** Each data RPC (`Apply`/`Read`/`Digest`/`Scan`/
  `BatchApply`) is stamped with the coordinator's ring epoch; a replica **refuses**
  an op stamped older than its own ring (`Response::StaleEpoch`), surfaced as
  `CoordError::StaleRing` (on the single-write, read, scan, *and* batch paths), so
  a coordinator that adopted a config and then went stale can't silently route onto
  a disjoint replica set (the strict half of [ring agreement](#control-plane-pd)).
  A `0` stamp is **not** exempt: once a replica has adopted a PD config
  (epoch > 0), a coordinator that never called `refresh_ring` is refused too — it
  is routing on operator guesswork, and the refusal carries the epoch it needs.
  Pre-PD setups (both sides epoch 0) are unaffected. Nodes stamp their own
  *adopted* epoch on the anti-entropy sweep's handoff applies, so cleanup is a
  first-class participant under enforcement. A stale-refusing replica is
  **healthy**: it is never hinted (a hint for it could evict hints held for
  genuinely-down nodes from the bounded store) and a refusal proves liveness to
  the failure detector. By default a refusal **self-heals**: the coordinator
  adopts the current config (ring + catalog/range maps) from a live member and
  retries the op once (`stale_ring_refreshes` counter), so a reconfiguration
  costs one slower op instead of an error; `with_manual_refresh()` opts out and
  surfaces `StaleRing` to the caller.

Quorum latency tracks the **W-th fastest** replica, not the sum of RTTs:
fan-out is concurrent (`join_all`), the coordinator returns at the W-th ack
(`quorum_ok`), and a straggler request is simply cancelled — its mux slot is
dropped and the eventual reply discarded by id.

---

## Storage engine (single node)

Each replica is a `vals_store::Engine` over a `BlockDevice` (the on-disk record /
segment / superblock byte layouts are in [`docs/formats.md`](docs/formats.md)):

- **Value log** — append-only segments of raw blocks; each record is
  `header | key | value` with a **CRC-32C** (hardware SSE4.2, scalar fallback).
  Each record carries (CRC'd) the **epoch of its segment incarnation**, and replay
  rejects a record whose epoch differs from the segment header's — so a reused
  segment's leftover prior-generation records (freeing wipes only the header
  block) can never be resurrected, even when new data ends exactly at a block
  boundary. Group commit batches the durability barrier; the header is built on
  the stack and the value is checksummed + streamed to the device without a
  per-record copy. **Writes are pipelined** (queue depth > 1): full blocks submit
  asynchronously and are drained at `sync`, so the write path isn't QD1-bound.
- **Index** — pluggable: hash (unsorted zones, with a fast non-crypto hasher) or
  ordered/front-coded (sorted zones). The front-coded memtable does one
  map descent per put/remove, searches a block by tracking the common prefix
  from the stored shared lengths (no key materialisation), splices inserts and
  removals in place with amortised block capacity, and flush commit removes
  each still-captured snapshot slice block-at-a-time (`remove_if_eq_rev`). This
  cut shard CPU per ingested KV by 31% and gave +25.9% ingest ACK/s. On `open`, recovery loads manifest runs and
  replays the uncovered ordinary tail. Duplicate records resolve by greatest
  `version`, with `seq` only breaking an equal-version tie, so segment scan order
  is not authoritative.
- **Memtable integrity guard and fail-fast** — every front-coded
  block keeps an additive FxHash checksum over `(key, packed entry)` computed
  from the caller's key at write time. Before any flush emits a block, it is
  decoded with checked varints/bounds, its first key is compared with the
  block-map key, and the checksum is recomputed. A mismatch aborts the whole
  flush snapshot (no run, no manifest; `vals_memtable_integrity_failures_total`)
  and retries on the next tick. The error carries a diagnosis: a single-bit
  search over the block's encoded keys, length varints, packed entries, map key
  and checksum word. It reports the region, key index, offset, actual/expected
  byte, XOR, candidate count and a window of up to 17 bytes, or `unexplained`.
  Multi-bit damage, software bugs and blocks over 4 KiB that no longer parse
  come out as `unexplained`. A mismatch first seen at a block split is reported
  with `detected_at=split`, with the diagnosis taken before the split. The
  search runs on the shard thread only after a failed check; it measured
  2–18 ms in release for ~290-byte keys, but grows roughly with entries ×
  key length² and is not yet capped. Because the value log holds the caller's original bytes, a
  persistent mismatch is an in-RAM fault: the same zone engine failing on the
  same block (first key) for `integrity_fail_fast_attempts` consecutive flush
  attempts (default 3), or for `integrity_fail_fast_secs` (default 30 s, at
  least two attempts), makes the node fail fast. It logs the evidence, counts
  `vals_integrity_fail_fast_total`, runs the normal graceful shutdown (which
  never flushes memtables: it syncs the value log and persists the watermark)
  and exits with code 75. A 30 s watchdog `_exit(75)`s if that shutdown hangs.
  The supervisor restarts the node, and recovery rebuilds the memtable from the
  intact value log. A flush that commits (or finds nothing to flush) clears the
  streak. On-disk signals keep their own handling and never fail fast:
  run/merge/cover-scan CRC errors and recovery failures. Library embedders must
  poll `vals_cluster::integrity::fail_fast_requested()`; only `valsd` exits.
- **Encode verification before the block CRC** — closes the window
  between the source structure and the CRC: a long soak persisted a flush run
  with a valid CRC and one key flipped (XOR 0x04) after the memtable guard passed,
  which every later merge refused. The flush guard now hashes the **copied**
  key it hands to the flush (not its scratch buffer), so the per-block sums it
  verifies attest the exact snapshot entries. `flush_page_attested` returns
  one `SourceGroup` (entry count, additive FxHash sum) per emitted memtable
  block piece; unsorted (hash) memtables, which have no checksum, attest
  groups of 64 copied entries. The flush no longer re-sorts its already
  ascending snapshot. Every run build (`build_run_into_checked` for flushes and
  sync merges, `RunStreamBuilder` for streamed merges) encodes a block body,
  computes its CRC, and only **then** decodes that same buffer: each decoded
  key must be strictly above the previous one (across blocks), and each decoded
  `(key, entry)` is hashed with the memtable guard's entry hash. Flushes compare the
  running sums with the attested groups; merges compare per entry with a hash
  taken when the merge stream's entry is pushed (merges now move the entry in,
  `push_owned`, with no clone). Because the check reads the buffer after the
  CRC, a flip before the CRC is seen by the check, and a flip after it breaks
  the CRC. The bloom filter, the index first keys, `min_key`/`max_key` and the
  first-key hashes come from the decoded (verified) bytes. The index is verified
  after its CRC too: first-key hashes, strict ascent, and contiguous block
  offsets/lengths ending at the bloom offset. A mismatch aborts the build
  without publishing (`EngineError::EncodeIntegrity`; segments freed through
  the normal flush/merge failure paths) and counts
  `vals_encode_integrity_failures_total{path}`. The ERROR names the check
  (`order`, `source-hash`, `decode`, `index`), block, entry, the source group's
  first key and, for hash mismatches, the stage (`encode`: the encoded entry
  differs from its source, diffed byte-wise; `source`/`source-copy`: the
  source entry changed after attestation, located by a capped single-bit
  search) with the memtable guard's `diagnosis=single-bit …` format. Off-core flush
  failures feed the same fail-fast streak, keyed on (zone, engine); a
  streamed merge stops reading its inputs at the end of the chunk (≤8192
  entries) in which a block fails, and merge failures just retry from the
  on-disk inputs. Cost (release microbench,
  ~150-byte keys): +~42 ns/entry on flush encode (~14%), +~51 ns/entry on
  merge-output encode (~16%; +~79 ns with a cloned push). Not covered: the
  merge stream's decode → push window (order check only), the v4 sidecar,
  bloom bits after insertion, the key-filter sink, and post-CRC copies (they
  surface as CRC errors at read time; flush output still has no device
  readback).
- **Allocator** — segment bitmap over the raw LBA space. When device
  deallocation is enabled (off by default), freed segments are
  tracked for advisory NVMe deallocation after a subsequent durable manifest;
  reuse cancels pending deallocation. Bounded batches run on shard maintenance
  ticks (provisioning and kill-switch details in [`docs/deploy.md`](docs/deploy.md)).
- **GC** — `compact()` copies live records forward and frees superseded segments;
  `compact_filtered(drop_live)` additionally drops records a higher layer marks
  collectible (the seam `VersionedStore` uses to GC aged tombstones — crash-safe via
  a tombstone written as the atomic commit point), and `truncate()` is the full
  logical wipe behind the liveness-lease re-sync. A **failed pass rolls back**
  (the partial new log is quiesced, durably invalidated, and freed; seq/epoch
  numbering advances past it so nothing it handed out is ever re-issued); a
  headroom precondition refuses a pass that can't hold old + new live data
  simultaneously, and only if the rollback's invalidation writes themselves fail
  is the engine **poisoned** (mutations refused, reads fine, reopen recovers).
- **Backends** — `MemBlockDevice` / `FileBlockDevice` for development; the
  `SpdkBlockDevice` (userspace poll-mode NVMe) for production (see
  [`docs/SPDK.md`](docs/SPDK.md)). Batched + async read/write paths drive the
  device at high queue depth, and `SpdkBlockDevice` presents a 4 KiB logical
  block over 512 B namespaces (one device write per value, not eight).

On real NVMe the engine reaches the device's raw ceilings: a single core does
~360k random-read IOPS and ~3.7 GiB/s of large-value writes, and
**thread-per-core sharding** (`SpdkController::open_shard` — one qpair per core
over a disjoint LBA region) scales reads to the drive's ~1.0M IOPS ceiling at
16 cores. A **JBOD multi-drive** layout
(one engine per drive) aggregates near-linearly (~8 drives → ~8M read IOPS, ~29
GiB/s write). See [`docs/multi-drive.md`](docs/multi-drive.md) and
[`docs/SPDK.md`](docs/SPDK.md).

The `VersionedStore` wraps the engine and prepends a version header
(`hlc u64 | node u32 | kind u8`) to every value, so the LWW version survives a
reopen.

---

## Consistency model

The data path is **leaderless, last-writer-wins, tunable per-operation
consistency**. The contract:

- **Convergence is unconditional.** `Version` (`hlc`, then `node` as tiebreak) is
  a deterministic total order, so any set of replicas that have seen the same
  writes agree on the same winner regardless of delivery order, retries, or clock
  skew. Replicas are eventually consistent always.

- **Strict quorum (default) — `W + R > RF` ⇒ read sees the latest acked write.**
  Because the write and read replica sets must overlap on the home preference
  list, a quorum read observes the acked write's candidate or a greater LWW
  version. Here "latest" means greatest `Version`, not
  request-arrival order; an acknowledged lower-version candidate cannot displace
  a previously stored greater version. So a write can be acknowledged and never
  become visible: if a coordinator whose clock runs ahead wrote the same key
  just before, a request that starts after that write finished can still carry
  the smaller version and lose. "Strong" or "latest" reads elsewhere in these
  docs mean exactly this greatest-version guarantee, not linearizability.

- **Sloppy quorum (opt-in availability mode) is eventual-only.** When home
  replicas are down, a sloppy write is accepted on fallback nodes (with a hint to
  the rightful owner) to preserve write availability. Reads consult only home
  replicas, so `W + R > RF` **no longer** guarantees the read sees that write
  until hints drain. Sloppy mode trades the read-latest guarantee for
  availability — by design, and only when explicitly enabled. Built as
  `Coordinator::write_sloppy` / `delete_sloppy`: a write whose home preference
  list can't make `W` spills onto the next nodes clockwise past the list to reach
  the quorum, and the down home owners get hints so the data migrates home on
  recovery. With healthy home replicas it is identical to a strict write.
  Default-zone only in v1 (hints carry no zone tag). The outcome is a
  `SloppyAck` (not a bare version): it surfaces **ack provenance** —
  `home_acks` (overlap a strict read) vs `fallback_acks` (do not) and `hinted`
  (home owners whose hints must drain) — so a caller can tell whether the write
  is already strict-readable (`is_strict()`) or is riding on fallbacks. A bare
  `Ok(version)` hid this: an `Ok` write that reached `W` mostly off-home, whose
  home hints the bounded store later dropped, was invisible to strict reads
  indefinitely with no signal. Fallback acks are **not** silently counted as
  home quorum.

### Blind-append LWW reply semantics (latency deployment profile)

An unconditional replicated put or delete is an LWW **candidate**. The accepted
design removes the cold run-index lookup before append: after route,
admission, skew, and the read-free `gc_grace` floor checks, a replica may append
and acknowledge the candidate without proving it beats an already stored
version. The initial compatible wire encoding reuses `Applied::Won` to mean
"accepted" on this path; callers must not interpret it as an authoritative LWW
outcome or expect `Applied::Lost` to disclose the current winner. Quorum success
means `W` replicas admitted the candidate (and synced it when requested), not
that request-arrival order overrides a greater `Version`. The daemon's
restart-only `profile = latency` couples the blind writer with the compact winner
directory; `profile = capacity` retains the resolving writer and out-of-core
index and is the explicit default. Mixed profiles remain wire-compatible.

This moves the resolution obligation rather than weakening LWW: point reads,
digests, scans, recovery, and compaction must select `argmax(Version)` across the
mutable candidate and every live run. Deletes are blind tombstone candidates;
tombstone GC may not expose an older physical value. CAS retains an authoritative
precondition read (while remaining non-linearizable across the leaderless
replica set). Merkle leaves cache **logical winners**, so an unresolved cold
write dirties its bucket; tree equality is unavailable until an exact
max-version run-stream rebuild, including after flush/merge races. A crash discards
the cache and seeds it exactly before serving anti-entropy. Stale live candidates
below `min(wall, high_water) - gc_grace` are conservatively rejected without a
key read, while tombstones remain admissible.

The complete prerequisites and correctness design are in
[`docs/blind-append-lww.md`](docs/blind-append-lww.md); supported profile
operations and rollback are in [`docs/deploy.md`](docs/deploy.md#data-node-deployment-profiles).
Latency-profile open synchronously rebuilds the directory over the dataset
before serving. Its retained mature-corpus cost is 62.9 additional RSS bytes per
physical key (with allocated capacity exposed by winner-directory gauges), so it
is not the default. A runtime directory rebuild error process-stably degrades
latency fail closed: the failed directory is dropped and every subsequently
decoded unconditional apply—including requests on existing multiplexed
connections—resolves through the out-of-core max-version path until restart.
Effective blind/directory gauges fall to zero while selected-profile
info remains latency, providing an alert condition; another engine cannot clear
the degradation. Current capacity readers safely resolve unresolved blind
history; an old-binary downgrade is unsafe without verified canonicalization or
peer rebuild.

### Compare-and-swap is best-effort, not linearizable

`cas` / `if-version` is **optimistic-concurrency *detection*, not a safety
mechanism**. Because the data path is leaderless — no leader, no consensus —
**there is no point at which read-check-then-write is atomic across the replica
set**, so CAS cannot be made mutually exclusive:

- **Lost updates under contention.** Two concurrent `cas` calls with the same
  `if-version` can each pass their precondition (against a different replica
  subset, or both against a coordinator read taken before either writes) and both
  apply. LWW then keeps one and **silently discards the other — which may already
  have received an `Applied` ack**.
- **No enforcement point rescues it.** A coordinator that does read-quorum →
  check → write still has a time-of-check/time-of-use gap (nothing locks the key
  between the read and the write). Per-replica enforcement diverges instead:
  the precondition can pass on some replicas and fail on others, leaving a split
  state that anti-entropy/LWW reconciles later — with an ack that reflects no
  single global decision.
- **What it _does_ give:** under uncontended, no-partition conditions it reliably
  catches a stale `if-version` (someone wrote since you read), which is useful for
  benign optimistic-concurrency loops. It is simply not a guarantee you can lean
  on under concurrent writers or partitions.

Application patterns that are **unsafe** on it, because two acknowledged CAS
calls can both succeed and LWW keeps only one:

- counters and balances (`read n`, `cas n+1`): concurrent increments are lost;
- locks, leases and leader election (`cas None → me`): two holders can both
  believe they won;
- unique-id or sequence allocation: two callers can receive the same id;
- check-then-act invariants across keys (there is no multi-key atomicity).

Use it for low-contention updates where losing a concurrent update is
acceptable or detectable later, such as a single writer guarding against its
own stale cache.

This is the same reason Cassandra bolts on Paxos (LWT) for its CAS path. True
linearizable CAS requires a **per-key consensus path**, which is **out of the
default leaderless data path** by design. The captured (parked, not committed)
way to offer it is an opt-in **linearizable zone mode** — writes for that zone
routed through per-range consensus groups, happy-path single round-trip — which
also restores in-partition multi-key atomicity for those zones. See
[`docs/future-linearizable-zones.md`](docs/future-linearizable-zones.md). The
default path stays best-effort and says so.

### Clock-skew bound (HLC)

LWW resolves conflicts by `(HLC, writer incarnation)`. The HLC's physical component
is wall-clock derived, while the collision-resistant 64-bit writer identity breaks
HLC ties. Thus "the wall-clock-latest write wins" holds **only within a bounded
clock skew ε**. Beyond ε, the system still *converges* (a deterministic winner is
always chosen) but may not pick the wall-clock-latest write.

- Nodes are required to run NTP/PTP; ε is a configured max offset (target
  ~250–500 ms).
- A node **rejects** any write (local or observed) whose physical time exceeds its
  local wall clock by more than ε, so a runaway clock cannot pin the HLC
  arbitrarily far into the future. ✅ Built: `Hlc` carries `max_skew_ms` (ε,
  default 500 ms); `Hlc::observe` clamps an over-ε remote time (the clock can't be
  pinned) and `VersionedStore::apply` returns `Applied::Rejected` for an over-ε
  write — nothing stored, HLC not advanced. The coordinator does **not** count
  `Rejected` as an ack, so a runaway-clock write (rejected by *every* replica)
  fails quorum instead of poisoning a key with a far-future version.
- The skew check is **per replica** (against each replica's *local* wall clock), so
  a write can be `Won` on in-skew replicas and `Rejected` on lagging ones. If fewer
  than `W` accept, the write durably committed on the accepters (a **partial
  commit**) while missing quorum — *not* a clean "nothing landed" failure. The
  coordinator surfaces this distinctly as `CoordError::SkewRejected { needed, got,
  rejected }` (rather than a bare `QuorumFailed`): `got` replicas hold the write and
  the `rejected` replicas are healthy-but-clock-lagging, so they converge via
  read-repair / anti-entropy (retrying the same write won't help until their clocks
  advance — they are not hinted, the replica is up). ✅ Built.

### Deletes and tombstones

A delete is a **versioned tombstone** (the version is retained so a straggling
older write still loses LWW and the key cannot resurrect). Tombstones are garbage
collected under a **grace period (`gc_grace`)**:

- A tombstone is GC-eligible only once `eff_now - tombstone.physical_ms >
  gc_grace`, where `eff_now = min(wall_now, high_water_ms)` — the eligibility
  clock is **bounded by the data-progress watermark**, the same clock the
  liveness lease compares (below). Wall clock alone would let GC outrun what
  `rejoin_decision` can detect: during a write-idle window every watermark
  freezes, a wall-clock GC still collects the tombstone, and a replica that
  crashed just before the delete rejoins with no peer measurably "ahead" — its
  stale pre-delete value then resurrects via AE/read-repair. Bounding by
  `high_water_ms` means tombstones only age when the cluster makes data
  progress, which is exactly when a lagging rejoiner is detectable. GC happens
  in `VersionedStore` during compaction (the engine sees opaque bytes; only
  `VersionedStore` understands the tombstone kind + version).
- `gc_grace` must exceed the longest window in which a lagging replica could still
  hold the pre-delete value (max tolerated downtime + an anti-entropy cycle).
- **Invariant — now enforced:** a node offline (or partitioned) longer than
  `gc_grace` must be wiped and full-resynced before rejoining — otherwise it still
  holds a pre-delete value whose tombstone has been GC'd everywhere, and **either**
  anti-entropy (a peer pulling it) **or read-repair** (a strong read picking it as
  the LWW winner and pushing it to the `None` responders) resurrects the key. This
  is enforced node-side by a **liveness lease** (`liveness` module), *not* left to
  the operator:
  - Each node persists a **data-progress watermark** — the highest write
    `physical_ms` it has observed (`VersionedStore::high_water_ms`, advanced on
    every apply/local write). It is *data*-derived, not wall-clock, so a node taking
    no writes does not advance it.
  - On each anti-entropy sweep a node **gossips watermarks** with reachable peers
    (the `GetWatermark` RPC) and runs `rejoin_decision`: a peer more than `gc_grace`
    ahead proves the cluster GC'd deletes this node missed → it **wipes**
    (`VersionedStore::clear`) and re-syncs from peers rather than letting a stale
    value win or repair.
  - A node is **born fenced** — it refuses to serve as a read / anti-entropy
    *source* (`Read`/`SyncLeaves`/`SyncBuckets` error out, so peers can't pull its
    data and coordinators route around it) until that first verification clears it,
    and **re-fences** if it loses cluster contact past the grace (the live-partition
    case). A node that *wiped* additionally stays fenced **through its refill**:
    the fence clears only at the end of a sweep pass in which every zone
    reconciled from at least one peer (unfencing on first contact alone would
    serve `None` for the whole keyspace — visible to digest / get-any reads —
    until anti-entropy lands).
  - The lease's `gc_grace_ms` **is** the engines' tombstone-GC grace by
    construction: `serve_with_liveness` stamps it onto every installed (and
    later-materialized) engine, overriding (with a warning) any mismatched
    store-level setting — a store grace below the lease's would silently reopen
    the resurrection hole, one above it would wipe too eagerly.
  - **Whole-cluster restart is safe by construction:** the decision compares a node
    against its *peers*, not against wall-clock `now`. If the whole cluster was down
    longer than the grace, every watermark froze together at shutdown, no peer is
    ahead, and all nodes **recover untouched** — nothing was deleted/GC'd while
    everything was down. (A naive `now − last_alive > grace ⇒ wipe` would instead
    wipe every node, and with RF < N could wipe both replicas of a key → data loss.)
  - ✅ Built (opt-in): `Node::serve_with_liveness(.., gc_grace_ms, LivenessLog)`
    and the sharded / zoned / multidrive `serve_*_with_liveness` variants, over
    `MemLivenessLog`, the fsync-durable `FileLivenessLog`, or the
    `SuperblockLivenessLog` that keeps the watermark on the SPDK superblock.
    `gc_grace == 0` (the default — GC disabled) makes the whole mechanism inert.
- **Former owners.** A node that stops owning a key keeps its copy until
  over-replication cleanup hands it off. Owners refuse a put below their GC
  floor, so a copy whose newer delete they collected can never be handed off,
  and an ownership change would make its holder serve it. `valsd backup create`
  fences such copies, a re-home copy is held while its target has one, and,
  unless every node runs with `gc_grace = 0`, the PD refuses
  `add-node`/`remove-node` until every member that stays has a clean
  cleanup lap and a joining node is empty (`--force` overrides;
  [`docs/backup-restore.md`](docs/backup-restore.md#consistency-contract)).
- ✅ Built: `VersionedStore::with_gc_grace_ms` + `VersionedStore::compact(wall_ms)`,
  over a new `Engine::compact_filtered(drop_live)` seam (a versioned tombstone is an
  opaque live `Put` to the engine, so only the cluster layer can decide it is
  collectible). `gc_grace == 0` (default) disables tombstone GC but still reclaims
  superseded value-log segments. ✅ **Scheduled on the Node:** a `compact_loop`
  (its own timerfd, on a slow cadence — a multiple of the sweep interval) compacts
  each materialized zone, so dead records and aged tombstones are actually reclaimed
  instead of growing the log unbounded. Compaction frees + reuses on-device segments,
  which would invalidate an outstanding borrow-free `ReadPlan` (the server reads the
  device with no store borrow held, across an await) — so the compactor **quiesces**
  reads through the node's `ReadGate`: it pauses new read admissions (reads park on
  the gate before planning), waits — bounded (100 ms, backing off to 2 s after
  repeated timeouts), racing shutdown — for the in-flight device-awaits to drain
  to zero, runs one **bounded step** (`compact_step`, at most
  `compact_step_segments` segments, default 4), resumes (parked reads wake), and
  yields before the next step; a zone gets a capped number of steps per tick.
  A drain that doesn't finish in time releases the pause and skips the step
  (counted + logged): serving reads outranks reclaiming space. Merely
  *sampling* `inflight == 0` per tick would starve under sustained read load — the
  count is almost never zero at any instant on a read-busy node. A zone is only
  stepped when its reclaimable fraction is at least 20% (garbage-ratio trigger),
  and this on-core path applies to engines with no out-of-core runs; engines
  with runs reclaim through merges, shadow folds and value-log GC on the
  node's off-core maintenance lanes.
- ✅ **Range deletes** (sorted zones): one versioned range tombstone per
  span instead of one tombstone per key. Covered keys read as a tombstone at the
  range tombstone's version, older stragglers lose, and replicas skip covered
  spans in scans. Design and as-built notes in
  [`docs/range-deletes.md`](docs/range-deletes.md): engine (record kind 2, run
  format v6 RT block and per-block `max_hlc`), replica cover tests and the
  accounted-set fold, wire `RangeDelete`/`SyncRangeTombstones` and the `Scan`
  trailer, the coordinator's `delete_range_in`, the anti-entropy RT exchange
  (`rt_sync`), re-home and cleanup lifecycle, backup VREC v2 and `VDUMP\x02`.
  With no range tombstones the write, read and scan paths cost the same as before
  (measured on hardware). A client that does not negotiate feature bit 3 keeps
  working on zones without range tombstones and gets an `Err` from a scan whose
  window meets one.

---

## Planned subsystems

Subsystems that were part of the target architecture beyond the core data
path. Most are now **✅ built** and link to their design docs; the ones that are
not say so plainly. **Erasure coding is out of scope** under the current
placement model (see the bullet below). Open items are tracked in the backlog
(`backlog.md`).

- **Zones** — ✅ built for unsorted/hash **and sorted** zones, see
  [`docs/zones.md`](docs/zones.md). A zone is a per-node engine (storage) + a
  partitioner-driven placement with per-zone RF and `scope` (placement). The PD
  owns the zone catalog (in the membership raft group) and pushes it over
  `SetZones`; nodes materialize a zone's engine lazily via a `ZoneStore` provider
  (Mem/File, or SPDK with a device-region allocator + crash-safe superblock so
  many zone engines share one NVMe namespace); the coordinator tags each op with
  its zone and resolves placement from the cached catalog (`replicas_for`,
  `write_in`/`read_in`/…). The `default` zone (id 0) preserves the original
  single-zone behavior. A zone that leaves the catalog is torn down with a
  **deferred-quiesced free**: unroutable immediately, but its storage region is
  released only once no in-flight sweep pass / borrow-free read still holds the
  engine (tracked by the engine `Rc`'s reference count), so the region can't be
  re-allocated to another zone under an op that can still touch it.
- **Batched prefix/range scans** — ✅ `Coordinator::scan_many_in`
  groups independent sorted scans by replica and shard, preserving input order
  and per-query quorum, minimum coverage, limit, range cursor and RT trailers.
  Negotiated `MultiScan` (hello bit 4) carries up to 256 spans and pages the
  contiguous reply prefix under 8 MiB; older peers use scalar `Scan`. The server
  fences every span and overlaps four staged scans on the shared read gate;
  run reads and value spans overlap within each scan, with value-span coalescing
  within (not between) sub-scans. Keys-only replies use the CRC-checked index
  entries' key/version/tombstone and mutable-entry revalidation, without reading
  the value log. This does not verify omitted value bytes or provide a snapshot.
  No persistent format or apply-path changes. See `docs/algorithms.md` §6 and
  `docs/formats.md` §1.3–1.5.
- **Descending sorted scans** — ✅ reverse engine cursors, staged serving and
  quorum merging are implemented and validated locally and on SPDK.
  `Coordinator::scan_in_rev` / `scan_many_in_rev` reach them over negotiated
  `MultiScan` (hello bits 4+5, request direction flag). They retain `[lo, hi)`,
  LWW/RT visibility and quorum intersection, visiting slices newest-first and
  merging at or above the maximum lower coverage bound. No ascending emulation
  for old peers. Natural-time latest-N is value-verified on the aged graph workload;
  its SPDK p50 is 6.55 ms versus 3.92 ms for inverted-time ascending scans. Matched
  pre-reverse/candidate arms show no material ascending/write regression at this
  workload's resolution.
- **Sorted zones / range scans** — ✅ built (`docs/zones.md`). The `sorted`
  partitioner (`partition::RangeMap`) divides a zone's keyspace into contiguous
  ranges by split keys and places each range by hashing its explicit placement
  **token** onto the *same* global ring
  (`ring.replicas_in_zone(zone, token, rf, scope)`) —
  so keys in a range co-locate (a range scan touches few nodes) and the
  partitioner reuses all of the ring's membership/weighting/scope/minimal-
  disruption machinery. `Coordinator::scan_in(zone, lo, hi, r, limit)` does a
  distributed range scan (reads `r` replicas per overlapping range, merges by LWW
  with tombstones hidden, concatenates in range order → globally sorted, strong
  under `W+R>RF`). The node-side anti-entropy / cleanup sweep scopes a sorted
  zone by the same range token, so placement agrees end-to-end. Growth is
  **range splits**: range maps are raft-replicated PD state (`R` key prefix),
  `PlacementDriver::split_zone` / `PdDaemon::split_zone` adds a boundary and the
  daemon auto-pushes the new map (`SetRangeMaps`, retry-until-acked); the new
  right range inherits its parent's token, so a split is metadata-only and moves
  no data. Presplits on an empty zone and explicit `split-zone … canonical`
  operations on empty ranges use lower-bound tokens and spread immediately;
  populated ranges require confirmed re-home. v1 first served sorted zones
  unsharded; they are now served on the sharded (multi-drive) path too, sharded
  intra-node by **range token** (key `k` on shard
  `hash64(range_token(k)) % shards`, matching the client's routing) so a sorted
  zone scales out per-node like the unsorted default zone. Auto/size-based
  splitting is default-on and safely refines boundaries without redistributing
  existing data. Confirmed automatic canonical re-home is built and default-on;
  set PD config `rehome_enabled=false` (legacy
  `VALS_PD_REHOME_ENABLED=0`) for operator rollback. RangeMap V4 opaque tokens also
  support a deterministic capacity planner. Its independent `balance_execute`
  mutation gate (legacy `VALS_PD_BALANCE_EXECUTE`) is default-off; when explicitly
  enabled after `opaque_range_maps` (legacy `VALS_PD_OPAQUE_RANGE_MAPS`), the PD
  revalidates an immutable proposal and
  executes at most one exact-token move globally through the same confirmed protocol,
  consuming a fresh statistics poll between moves. PD placement/rehome policy is
  typed startup configuration, with strict legacy-environment resolution and
  effective-setting logs; see [configuration policy](docs/configuration.md).
  No request-time placement branch
  or assignment database was added, populated splits remain metadata-only, and
  default/hash zones plus `vnodes` are unchanged. The planner's implementation and
  local debug-mode gates passed, but the executor remains default-off: the
  destructive uneven-RF=2 hardware A/B that would justify turning it on by default
  has not run.
  Protocol, retry, fencing, planner, and validation details are in
  [`docs/zones.md`](docs/zones.md).
- **Batch write API** — ✅ built (`Coordinator::write_batch`), see
  [`docs/batch-writes.md`](docs/batch-writes.md). Replaces the spec's general
  per-write durability *barrier* (a poor fit for the logless leaderless path). A
  batch is N independent (non-atomic) writes, stamped and **grouped by destination
  node**. Buffered batches do not force sync; device-persistent batches join the
  same per-shard/zone coalesced group-commit barrier as device-persistent single
  writes. Acks scatter back to **per-write outcomes** aligned 1:1 with the
  input, so a client detects exactly which writes failed and whether a failure
  still reached quorum. The fire-now / `await_tier`-later handle is deferred (the
  tier is chosen at submit time today); the general streaming barrier stays deferred.
- **Erasure coding — out of scope.** Placement is whole-value "next RF nodes,"
  which is *not* erasure-coding-ready: EC needs per-shard-of-a-value placement.
  The spec's earlier EC-ready aspiration is **withdrawn** (decision 2026-06-27);
  EC stays out of scope unless placement is first redesigned around
  per-shard metadata.
- **Auto-eviction policy (detector → PD).** The failure detector itself is **built**
  and routing-only (see [Failure detection](#failure-detection) above). What stays
  open is letting a *durably* down node feed `PdDaemon::remove_node` so the ring
  reshapes automatically — deliberately deferred (it needs a conservative timeout so
  a transient blip doesn't trigger data movement); membership is explicit join/leave
  through the PD until then.
- **Client backpressure and liveness** — ✅ built (`ClientConfig`,
  `Coordinator::with_credits`, `Coordinator::with_node_credits`), see
  [`docs/backpressure.md`](docs/backpressure.md). A default network coordinator
  bounds in-flight writes by operations and key-plus-value bytes, both globally and
  per destination node. Temporary global exhaustion is FIFO; a waiter blocked only
  by one node does not gate disjoint healthy destinations. Impossible requests and
  `try_write_*` overload return `Overloaded`. Completion, failure, timeout, and
  cancellation release the guard; stale-ring retries reacquire against the refreshed
  destinations. Five-second connect/hello and RPC defaults bound failed quorum
  liveness. `connect[_sharded]_unbounded` preserves an explicit benchmark path, and
  `credit_stats()` exposes saturation. An SPDK RF=2 hardware gate
  validated finite-client fault behavior but did not establish performance adoption:
  finite defaults cost 4.59%/6.78% median width-32 Digest/Quorum read throughput
  versus explicit-unbounded, and hardware hedge-loser cancellation remains unproven.

---

## Status

An as-built summary; benchmark methodology is in [`docs/SPDK.md`](docs/SPDK.md).

### What is built, and how it was validated

Everything above the `BlockDevice` trait runs unchanged on the in-memory and
file backends, so `cargo test --workspace` (no hardware) covers the engine, the
cluster layer, the PD, and `valsd` end to end, including crash recovery, node
loss, restart, rejoin, and reconfiguration. Hardware validation is a second
layer: SPDK benchmarks that count every op error and verify read values, fault
drills on real clusters, and a multi-day application soak.

| Area | State | Hardware validation |
|---|---|---|
| Storage engine: value log, out-of-core hash/sorted index, recovery, bounded merges, compaction, memtable and encode integrity guards | ✅ | single-drive and 8-drive JBOD benches at the drives' ceilings |
| Leaderless data path: ring placement, W/R quorums, HLC LWW, durability tiers, read-repair, hinted handoff, Merkle anti-entropy, over-replication cleanup, liveness lease and tombstone GC | ✅ | SPDK clusters on one box and across three machines; node-down and restart drills |
| PD control plane: raft membership, zone catalog, range maps, retry-until-acked pushes, restart beacon, admin socket, control-op auth | ✅ | three-PD `valsd` deployments on the lab cluster |
| Zones: hash and sorted, range splits, confirmed re-home, range deletes, ascending/descending/batched scans | ✅ | SPDK runs with value verification |
| Multi-drive JBOD nodes, server sharding, multiplexed TCP transport, epoll reactor, process-wide transport bounds | ✅ | 8-drive single box; 3-machine cluster with external clients |
| Client: batch writes, credit backpressure, digest reads, batched reads, stale-ring self-heal, failure detector | ✅ | matched hardware A/B gates |
| `valsd`: config, graceful shutdown, panic → abort, logging + counters + Prometheus metrics, backup/restore, dump | ✅ | multi-day soak replaying the Bluesky firehose (about 5 billion events) |
| Capacity planner / balance executor | built, default-off | local gates only; the hardware A/B that would justify default-on has not run |
| Auto-eviction (detector → PD), online drive add/retire on the zoned daemon path, linearizable zones | ◻ not built | — |

### Headline performance

Results from specific benchmark runs with verified values and zero errors; not
guarantees. 100-byte values unless stated.

- **Single-node engine, one PM9A3:** ~360k random-read IOPS on one core,
  scaling to the drive's ~1.0M IOPS ceiling at 16 cores; ~3.7 GiB/s of
  large-value writes. Across 8 drives (JBOD): ~8M read IOPS, ~29 GiB/s writes.
- **One machine, 8 PM9A3 drives** as 4 node processes × 2 drives × 16
  partitions (64 server cores), loopback TCP, RF 3, in-process benchmark with
  the anti-entropy sweep off: **~3.5–4M replicated writes/s**, **~4.4M strong
  (digest) reads/s**, **~6.5M `R = 1` reads/s**. Large-value writes reach
  ~21 GiB/s, about 70% of the 29.9 GiB/s raw JBOD write ceiling. About 65% of
  the write path's CPU on this box is loopback kernel networking, so these
  numbers are conservative for a real network.
- **Three machines** (24-core Ultra 9 285HX, one Samsung 9100 PRO each, 10G
  links), `valsd` with four shards per node, RF 2, R 2, `profile = latency`
  (not the default; its winner directory costs ~63 B of RAM per key), external
  clients, anti-entropy sweep on: **~5M strong 100-byte reads/s** with batched
  digest reads (every serving shard thread saturated, ~7.4 Gbit/s of server
  traffic) and **~2.0–2.7M quorum writes/s** (the higher figure at the
  saturation knee, p99 ~25 ms).

### Lessons that shaped the design

- **Coordinator writes pipeline.** A `&mut self` write path allowed one write
  per quorum round trip per coordinator; interior-mutable HLC and hint state
  let writes scale with coordinators instead of collapsing.
- **One multiplexed connection per peer shard.** A one-request-per-socket pool
  melted the kernel's socket path past ~4 coordinators (~80% of CPU in the net
  stack, ~100% quorum failures). One socket per peer with request-id demux
  fixed it; a cancelled request just drops its slot.
- **Borrow-free, overlapped device reads on the server.** A synchronous QD1
  device read on the serving thread capped reads at ~30k/s per shard; planning
  under a brief store borrow and executing off a shared device handle lifted
  that ~14×. The same shape on `Scan` and the anti-entropy bucket pull removed
  the strong-read collapse under a running sweep.
- **Server cores set the ceiling, not coordinators.** Spreading drives over
  many partitions (one engine per core) took the cluster from ~1.5M to ~4M
  writes/s; on a real NIC a node is serving-core bound, not drive- or NIC-bound.
- **Digest reads.** A strong read needs R versions but only one value. One
  value read plus version-only digests (a hedged R-quorum) cut device
  amplification to about one read per strong read and beat full fan-out at
  every measured configuration, more so as the drives saturate.
- **Persistent edge-triggered epoll.** `EPOLLONESHOT` re-arm cost an
  `epoll_ctl` per park (~5% of CPU); one registration per fd lifetime fixed it.
- **Node-scoped maintenance lanes.** An NVMe controller's I/O qpair budget (15
  on the lab drives) cannot fund a maintenance qpair per shard at eight shards,
  so all shards share one flush lane and one compaction lane per node.
- **Integrity guards are there for a reason.** A long soak persisted a run with
  a valid CRC and one flipped key bit; the memtable and encode-verification
  guards now catch such faults before publication.

### Known limitations and open problems

- No multi-key transactions; CAS is best-effort (see [Consistency model](#consistency-model)).
  Linearizable zones are designed but not built.
- Data traffic is neither authenticated nor encrypted, and the control-op token
  is cleartext on the wire. Run on a private network or overlay.
- The one-machine headline numbers come from an in-process benchmark with the
  anti-entropy sweep off (`valsd` does not allow that). With the sweep on,
  throughput is lower; the reconcile apply side and AE rate limiting have not
  been brought to the sweep-off ceiling.
- Backpressure is per coordinator. There is no cluster-wide admission control,
  so past the throughput peak adding coordinators degrades throughput gently
  rather than refusing load.
- Membership changes are explicit PD operations; a durably dead node is routed
  around but not evicted automatically. Sloppy quorum is default-zone only.
- On-core compaction steps briefly pause read admission on the shard (bounded
  quiesce per step), and the zoned/JBOD daemon path has no online drive
  add/retire.
- The `latency` profile's winner directory costs ~63 bytes of RSS per key, so
  `capacity` is the default; the memtable integrity diagnosis search is not
  capped and can take tens of milliseconds on the shard thread.

The prioritized remaining work is in the backlog (`backlog/`, `backlog.md`).

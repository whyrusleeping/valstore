# vals architecture

`vals` is a distributed key/value store designed to run directly on raw NVMe
(userspace, kernel-bypass) and squeeze maximum IOPS out of the device. This
document describes the **target architecture** — the system we are building
toward — and marks each piece **✅ built** or **◻ planned** so it doubles as a
status map. Planned subsystems link to their design docs under `docs/`. For the
planning hub and the rationale behind the big decisions, see [`spec.md`](spec.md);
for open work see the ycc backlog (`backlog.md` / `backlog/`), and the shipped-work
history lives in git. If you want to *use* vals rather than work
on it, start with [`docs/getting-started.md`](docs/getting-started.md).

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
- `pd` — `PdDaemon`: raft-replicated membership; pushes `SetMembers` to data nodes.
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
> domain with at most one flush in flight (task 0303; see
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
absent. With strict quorum, `W + R > RF` guarantees the read observes the latest
acked write (see Consistency model).

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
  key is hashed once and every run's filter line is prefetched before testing
  (0312). This cut probe CPU by 77% at 10 runs/engine and gave +10.9% ingest ACK/s;
  the `latency` profile's winner directory provides the resident winner lookup.
  **Engine-wide key filter (0364):** the write-path `probe_version` first tests a
  per-engine, in-RAM scalable Bloom filter covering every run marked *covered*. On
  a miss it skips all covered runs and probes only uncovered ones, so the common
  new-key write costs one filter probe instead of one per run. Flush and merge
  builds insert their keys; a run recovered at open stays uncovered (probed
  individually) until a bounded maintenance cover-scan streams its keys (cover-scan
  chunks get a preferred compaction lane ahead of ordinary merges). Reads that resolve
  through `lookup_runs` use the same filter. It is off under the winner directory.
  Nothing is persisted. See [`docs/out-of-core-index.md`](docs/out-of-core-index.md#engine-wide-key-filter-0364).
  **Run format v5 (0365):** new runs no longer write the per-run sidecar (a second
  copy of every `(key, version)`); AE seeds, digest pages, `recovered_versions` and
  cover-scans stream the same entries (tombstones included, key order, footer bucket
  modulus) from the data blocks. v1–v4 runs keep their sidecar paths (except v4
  digest-page continuations, which seek through the block index, 0411) and merges
  converge to v5. Downgrade to a pre-0365 binary fails closed once a v5 run exists;
  `VALS_RUN_WRITE_VERSION=4` keeps writing sidecars for a rollback window
  ([`docs/deploy.md`](docs/deploy.md#run-format-v5-task-0365)).
  **Run format v6 (0451):** the block index carries each block's largest HLC
  (`max_hlc`, 8 B/block resident) and an index trailer locates an optional
  range-tombstone record block and its `rt_seq_floor`, groundwork for range deletes
  ([`docs/range-deletes.md`](docs/range-deletes.md), layout in `formats.md` §2.2a).
  Merges write at least their newest non-sidecar input's version; pre-0451 binaries
  refuse v6 runs, and `VALS_RUN_WRITE_VERSION=5` keeps a rollback window.
  **Range-tombstone engine core (0452, not yet exposed):** a sorted engine keeps a
  canonical fragment set (`range_tombstone::FragmentSet`), appends RTs as record kind
  2, replays unflushed ones from the tail, and moves RT records into flush and rewrite
  outputs (each record in exactly one run); the first RT commits a v6 marker run so
  older binaries fail at run open. Unflushed RT records keep their log segments
  protected like memtable values.
  **Fail-loud newer formats (0450):** an intact unknown manifest/chain version,
  segment format version or record kind fails `Engine::open` instead of being skipped.
  **Resumable AE digest pages (0411):** a sparse differing-bucket set can need a scan of
  every run, which took far longer than the 5 s peer RPC deadline, so abandoned scans
  kept the 0401 digest-serving cap busy. With hello feature bit 2, a `BucketDigests` page
  stops about 1 s in at a run-window checkpoint and returns a resume key; the merge
  advances the lagging run first so the key always makes progress. In-node AE follows
  resume keys and saves its cursor after at most 16 pages per pass
  ([`docs/formats.md`](docs/formats.md#15-reply-paging-scan--bucketdigests--values--mixedbatchread)).
  **Run blocks and filter bits (0378):** new runs close data blocks at ~32 KiB of
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
  ([`docs/deploy.md`](docs/deploy.md#run-blocks-run-blooms-and-key-filter-bits-task-0378)).
  **Bounded merges and a frozen top tier (0381):** no recurring or forced merge
  reads more than `VALS_MERGE_INPUT_CAP_BYTES` (2 GiB). Runs at or above
  `VALS_MERGE_FREEZE_BYTES` (default: the cap), or too large for any size-ratio
  balanced window under the cap (> cap × r/(1+r), 1.2 GiB at r = 1.5), are frozen.
  Only space reclaim (pressure, shadow folds, aged value-log GC, tombstone GC,
  forget) rewrites them, so run count grows ~linearly with data ÷ cap. The run-count
  valve (`VALS_MERGE_FORCE_RUNS`, 6) now counts only the active tail newer than the
  newest frozen run. It merges the cheapest capped adjacent window there, and it is
  paced like every recurring merge. The old valve forced the two newest runs past the
  cap: SOAK5B-V3-R2 saw merge output per write rise 195 → 1,026 B, and a
  micro-simulation shows rewrite amplification growing without bound (308× at
  160 GB/engine). The new valve holds it at ~1.5× merge input per flushed byte.
  ([`docs/algorithms.md`](docs/algorithms.md), [`docs/configuration.md`](docs/configuration.md)).
  **Run-count costs (0382):** shadow estimation caches fixed, run-id-seeded
  block samples. Flushes read only new runs and probe only newly added newer-run
  blooms; removing a newer run re-evaluates surviving cached samples without I/O.
  Memtable generations invalidate the estimate without resampling. Staged GET
  uses the key-filter negative to skip covered runs, and range fences precede
  bloom prefetches. Merge policy is captured at engine open; cover scans avoid
  exactly the next recurring/forced window, including inner windows. Inner-window
  merge counts advance only at commit. `vals_stranded_runs` and
  `vals_engine_stranded_runs` report non-frozen runs older than the newest frozen
  run, which recurring merges cannot take.
  **Capped shadow folds (0383):** overwrite/delete garbage in frozen runs used to
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
  **Structural epoch reuse (0294) is enabled automatically:** a strictly newer,
  previously nonzero range authority with the same complete per-store placement
  inputs retains AE seed generation and rebases identical-map logical counts.
  Unready counts stay unready; real map/topology changes and unknown/lower
  authority still invalidate. Routing, mirror receipts, and cleanup fences remain
  epoch-sensitive. This avoids unrelated-zone epoch-only whole-zone reseeding
  without changing durability or adding key-sized state; see
  [`docs/algorithms.md`](docs/algorithms.md#epoch-only-seed-reuse-0294-enabled).
  **Range-bounded split recount (0310):** a boundary change (split, merge, or
  nested change on a pending span) no longer clears AE leaves or refolds the
  store. It re-partitions exact per-range counts and recounts only the unknown
  children, in bounded turns of 1024 winners or 1 ms. One child is derived as the
  total minus the counted ones. Readiness stays false until every span finishes.
  At 95M keys/node this took whole-write ingest from 465k to 816k ACK KV/s
  (1.755×) with zero seed folds
  (`bench/0297-ingest-1m/long01/FINDINGS.md`).
  Recount pages prefetch their run blocks with concurrent async reads under the
  shard read gate, then merge synchronously up to a horizon (0318, 15× less shard
  time per key). A rehome commit that only reorders a range's replicas keeps every
  store's seed, because leaves key on the sorted set (0317). The PD auto-split loop
  splits every over-threshold range per zone per poll, up to 16 (0319). With all
  three, the 400.7M-KV LONG01-D run on two nodes keeps every range near the 1M target
  while writing, settles in 21–91 s, and sustains 1.15M ACK KV/s with production
  defaults.
  **Seed isolation (0374, `ae_seed_isolation`):** after a restart, seeding no longer
  starves the apply path. The shard accounting task is the only seed driver (AE
  handlers answer Busy until seeded) and pauses to hold seeding to
  `ae_seed_duty_pct` (25%) of shard time while foreground work is recent or in flight. The
  budget is windowed and covers split recounts, and run-churn escalation stops at 50%. An
  in-flight seed survives split/merge generation bumps (its counts are remapped
  through the split recount at publication). The seed journal is charged to the
  RAM broker and capped node-wide, spilling to direct staged deltas
  ([`docs/backpressure.md`](docs/backpressure.md)). The PD holds new re-home starts
  (never commits) for a bounded time per episode while a node is down or reseeding after a
  restart ([`docs/zones.md`](docs/zones.md)). RECOV01: t80 70 s, R600 0.64; RECOV02 (round
  two): t80 76/72 s, R600 0.81/0.82. Since 0379 an in-flight seed also survives merges:
  it pins its starting run set, whose segments stay allocated until it publishes or
  resets, instead of restarting on every run removal (SOAK5B: 161 restarts, no seed
  completed under ingest, ACK −23%). Pinned bytes are capped node-wide
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
  **Range tombstones (0464).** In a sorted zone, each cleanup page first hands the
  fragments of F that lie in released ranges to their owners as `RangeDelete`
  slices. A piece is released once enough owners confirm it and the local
  accounted set A already covers it. Covered entries are never handed off. The
  forget rewrite drops every entry under a released piece and clips the
  piece out of the RT records (`docs/range-deletes.md` §5).
  **Sorted zones wait for placement (0367).** A shard that learns a sorted zone
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
  deliberate (0426): a node cannot tell a coming initial map from none (a zone
  without presplit never gets one, and the PD can drop an unauthorized one —
  `CommittedMapDropped`), so a per-zone hold could hold that zone forever. The
  worst case is strays, not loss: the zone is empty at creation; a write admitted
  in the gap lands on the single-range owners; after the map arrives the changed
  placement fingerprint restarts cleanup, which hands each such key to its real
  owners and forgets it only once they confirm. A read in that window can miss such
  a key, which is why `create-zone ... presplit=` tells the operator to confirm
  the layout with `ranges <zone>` before using the zone
  ([`docs/pd-admin.md`](docs/pd-admin.md)).
  **Cleanup pacing (0426).** Each traversal starts at a key sampled from the
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
  batched owner digest probe (0478, [`docs/algorithms.md`](docs/algorithms.md)).

### Failure detection

An **opt-in, coordinator-side circuit breaker** (`failure::FailureDetector`)
observes RPC outcomes per node: after a few consecutive transport errors a node
trips open and the coordinator **fast-fails** RPCs to it — no connect attempt —
so the write path hints it and reads route around it (sloppy fallback / fewer
responders). It auto-recovers by admitting a single probe after a cooldown. It is
request-driven (no heartbeat thread) and informs **routing only**: it never
changes membership. The cluster rides a failure out via hints + sloppy quorum and
recovers when the node returns; eviction stays an explicit PD decision.

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
(Today this is on-demand; periodic/auto refresh is a backlog item.)

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
  (`vals_spdk::async_io_pending`, 0368), so ungated background reads (sweep cleanup
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
  inside a sweep's bucket pull — the failure that made anti-entropy the cluster's
  throughput bottleneck on SPDK (see Status). `ShardedServer` runs N of these.
  (✅ read + scan + AE-bucket paths on the unified loop; the write apply's full
  overlap is the remaining open optimization — see Status.)
- **Safety bounds.** Hard caps bound a desynced/hostile/slow peer's blast radius:
  a 64 MiB **`MAX_FRAME`** fails the conn before buffering an over-cap length
  (remote-OOM guard, both readers); a 256 MiB per-conn **`MAX_CONN_SEND_BUF`**
  high-water mark fails/closes a conn whose queued-but-unsent bytes back up behind a
  peer that won't drain; and finite default connect/hello and per-RPC **deadlines**
  (both 5 seconds, checked by the reactor flush hook) fail a request whose connection or
  reply never completes with `TransportError::Timeout`, so a silent **black-hole**
  peer is observable to the failure detector instead of hanging forever. The default
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
  ordered/front-coded (sorted zones). The front-coded memtable (0313) does one
  map descent per put/remove, searches a block by tracking the common prefix
  from the stored shared lengths (no key materialisation), splices inserts and
  removals in place with amortised block capacity, and flush commit removes
  each still-captured snapshot slice block-at-a-time (`remove_if_eq_rev`). This
  cut shard CPU per ingested KV by 31% and gave +25.9% LONG01 ACK/s. On `open`, recovery loads manifest runs and
  replays the uncovered ordinary tail. Duplicate records resolve by greatest
  `version`, with `seq` only breaking an equal-version tie, so segment scan order
  is not authoritative.
- **Memtable integrity guard and fail-fast** (0361, 0370) — every front-coded
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
  key length² and is not yet capped (0372). Because the value log holds the caller's original bytes, a
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
- **Encode verification before the block CRC** (0384) — closes the window
  between the source structure and the CRC: SOAK5B-V4-R2 persisted a flush run
  with a valid CRC and one key flipped (XOR 0x04) after the 0370 guard passed,
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
  `(key, entry)` is hashed with the 0370 entry hash. Flushes compare the
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
  search) with the 0370 `diagnosis=single-bit …` format. Off-core flush
  failures feed the 0370 fail-fast streak, keyed on (zone, engine); a
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
  deallocation is enabled (off by default since 0356), freed segments are
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

On real NVMe (single core) the engine reaches the device's raw ceilings — random
reads ~1.0M IOPS, writes ~3.7 GiB/s. **Thread-per-core sharding**
(`SpdkController::open_shard` — one qpair per core over a disjoint LBA region)
scales reads to the device ceiling across cores, and a **JBOD multi-drive** layout
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
  a previously stored greater version.

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
0167 design removes the cold run-index lookup before append: after route,
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
    (`MemLivenessLog` / fsync-durable `FileLivenessLog`). `gc_grace == 0` (the
    default — GC disabled) makes the whole mechanism inert. *(Follow-ons: persist the
    watermark on the SPDK superblock; the sharded / zoned / multidrive serve
    variants.)*
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
  the gate before planning), waits — bounded (100 ms), racing shutdown — for the
  in-flight device-awaits to drain to zero, runs the synchronous pass, and resumes
  (parked reads wake). A drain that doesn't finish in time releases the pause and
  skips the pass (counted + logged): serving reads outranks reclaiming space. Merely
  *sampling* `inflight == 0` per tick would starve under sustained read load — the
  count is almost never zero at any instant on a read-busy node. *(v1
  limitation: a synchronous stop-the-world pass per zone on a fixed cadence;
  incremental compaction + a garbage-ratio trigger are future refinements.)*
- ✅ **Range deletes** (sorted zones, task 0439): one versioned range tombstone per
  span instead of one tombstone per key. Covered keys read as a tombstone at the
  range tombstone's version, older stragglers lose, and replicas skip covered
  spans in scans. Design and as-built notes in
  [`docs/range-deletes.md`](docs/range-deletes.md): engine (record kind 2, run
  format v6 RT block and per-block `max_hlc`), replica cover tests and the
  accounted-set fold, wire `RangeDelete`/`SyncRangeTombstones` and the `Scan`
  trailer, the coordinator's `delete_range_in`, the anti-entropy RT exchange
  (`rt_sync`), re-home and cleanup lifecycle, backup VREC v2 and `VDUMP\x02`.
  With no range tombstones the write, read and scan paths cost the same as before
  (`bench/0449-rt-hot-path/FINDINGS.md`). The Go client (`valsgo`) does not speak
  feature bit 3 yet: it keeps working on zones without range tombstones and gets an
  `Err` from a scan whose window meets one.

---

## Planned subsystems

Part of the target architecture but **not yet built**. Most now have a locked
design (linked). **Erasure coding is out of scope** under the current placement
model (see the bullet below). All are tracked in the ycc backlog (`backlog.md`).

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
- **Batched prefix/range scans (0482)** — ✅ `Coordinator::scan_many_in`
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
- **Descending sorted scans (0484; local/SPDK validated)** — reverse engine
  cursors, staged serving and quorum merging are implemented (0487–0490).
  `Coordinator::scan_in_rev` / `scan_many_in_rev` reach them over negotiated
  `MultiScan` (hello bits 4+5, request direction flag; 0491). They retain `[lo, hi)`,
  LWW/RT visibility and quorum intersection, visiting slices newest-first and
  merging at or above the maximum lower coverage bound. No ascending emulation
  for old peers. Natural-time latest-N is value-verified on the aged graph workload;
  its SPDK p50 is 6.55 ms versus 3.92 ms for inverted-time ascending scans. Matched
  pre-reverse/candidate arms show no material ascending/write regression at this
  workload's resolution; see `bench/0484-reverse/FINDINGS.md` for limits and receipts.
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
  existing data. Confirmed automatic canonical re-home is built and default-on after
  task 0176; set PD config `rehome_enabled=false` (legacy
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
  default/hash zones plus `vnodes` are unchanged. Task 0181's implementation and
  local debug-mode gates passed, but the executor remains default-off: follow-on
  task 0182 owns the destructive uneven-RF=2 hardware A/B, benchmark findings, and
  any later default-on reconsideration, and that hardware gate has not run.
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
  The spec's earlier EC-ready aspiration is **withdrawn** (decision 2026-06-27,
  task 0023); EC stays out of scope unless placement is first redesigned around
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
  `credit_stats()` exposes saturation. The [0287 SPDK RF2 gate](bench/0287-hardware-20260909/FINDINGS.md)
  validated finite-client fault behavior but did not establish performance adoption:
  finite defaults cost 4.59%/6.78% median width-32 Digest/Quorum read throughput
  versus explicit-unbounded, and hardware hedge-loser cancellation remains unproven.

---

## Status

**2026-09-13 measured-ingest checkpoint (0293/0294).** Structural epoch reuse is
accepted and enabled, with no flag to flip. Two matched 900s BASE/CAND SPDK pairs
(including reversed order) with rehome, split, AE and compaction active, RF2/W2
and device-sync unchanged, measured pooled acknowledged KV throughput
57.613→72.872k/s (+26.5%) and source events 28.465→35.949k/s (+26.3%).
Common-progress fold/projection work fell about 56%. The corrected 0293 baseline
is complete as measurement/attribution evidence, not full-corpus verification.
Late slowdown, mixed tail behavior, and readiness/maintenance-debt uncertainty
remain: 0294 still owns client-lane evaluation (default one), a full
payload/version/tombstone oracle, and the final sustained one-hour gate. No claim
of collapse elimination or 0248 clearance. Source pins, per-arm distributions,
build-path failure proof, and retained-raw mappings are under
[`bench/0294-ingest-optimization/`](bench/0294-ingest-optimization/README.md),
particularly `phase6/20260913T052004Z/FINDINGS.md`.

The leaderless data path is feature-complete and benchmarked end-to-end over the
async TCP transport (placement → versioned quorum writes → tunable reads →
read-repair → hinted handoff → Merkle anti-entropy → raft-replicated PD), with
server sharding, per-shard anti-entropy, an epoll reactor, non-blocking connect,
a **multiplexed transport** (one conn per peer-shard), and a **unified server
read loop** that overlaps device I/O across connections.

The engine runs on **real NVMe via SPDK**, optimized to the device's raw ceilings
single-core and scaled across cores (thread-per-core sharding) and across drives
(JBOD), and the **distributed path is validated on real drives**: a 3-node × 2-drive
cluster (RF=3, loopback) does quorum writes/reads, replication, node-down
availability, and hinted handoff correctly on SPDK engines. Since 2026-07-30 it
also runs **across real machines**: a 3-box SPDK cluster (lab-1/2/3, 10G NICs,
RF=2) driven by external clients — see step 10 below and
[`plans/box-provisioning-runbook.md`](plans/box-provisioning-runbook.md).

**Performance note (honest accounting).** Single-node and multi-drive throughput
hit the hardware ceilings (per above). *Clustered* throughput was long bound by
the coordination layer, but with enough server cores it now reaches **millions of
ops/s** and is read-saturated on the drives; the remaining gap is the loopback
network stack (a single-box artifact). The bottleneck climbed a clear staircase:

1. **Coordinator pipelining (fixed).** `write_tier` was `&mut self` (it stamps the
   HLC and records hints), so a coordinator had only one write in flight at a time
   — writes were serialized at `1/quorum-latency` while reads (`&self`) pipelined.
   Making the write path `&self` (interior-mutable HLC + hint store) lets a
   coordinator pipeline writes; on the 3-node × 2-drive SPDK cluster writes now
   **scale with coordinators (85k → 350k @ 8)** instead of collapsing (~100k peak
   before). Validated in isolation with an in-process discrete-event mock
   (bench `pipewrite`): pipelining scales linearly (8/32/64/256× at matching
   concurrency).
2. **Connection multiplexing (fixed).** A pool of one-request-per-socket
   connections opened/tore down a socket per concurrent request (RF × depth) and
   melted the kernel net stack past ~4 coordinators (~100% quorum failures, hidden
   until benches counted op results). One `MuxConn` per peer-shard with request-id
   demux now carries all concurrency on a single socket; mem `distmcp` went from
   ~100% errors to 0 at every coordinator count.
3. **Server read path — unified loop (fixed).** The replica server used to run its
   epoll network reactor and a busy-poll device as **split loops**, so a read was a
   synchronous QD1 device read on the server thread (reads flat at ~30k). The
   server now serves reads through a **borrow-free async path** (`plan_get` snapshots
   the dirty tail under a brief borrow; `execute_read_plan` runs the device read
   with no store borrow across the await; the engine holds its device behind
   `Arc<D>`) on a **unified poll loop** (`server_poll` busy-polls the qpair when
   reads are in flight, parks on the reactor when idle). Reads now scale with
   coordinators — **58k → 426k @ 8** on the 3-node × 2-drive SPDK cluster
   (`distspdkmc`), ~14× the old ceiling. Built off-hardware first against a
   discrete-event mock (`mcread`).
4. **Server cores — multi-drive × multi-node (fixed, the headline).** The remaining
   cap was the *number* of server cores: `distspdkmc` ran one server shard per
   drive (6 cores), and 8 coordinators saturated them at ~1.5M writes / ~0.55M
   reads. The JBOD/partition design already answers this — a node spreads its drives
   over `P` partitions, each an engine on its own core. `bench distmdmc` drives it:
   8 PM9A3 as **4 nodes × 2 drives × P partitions** (RF=3). At **64 server cores**,
   writes reach **~4.0M**, strong reads **~2.0M**, get-any **~3.8M** ops/s — all
   0-error (the bench sums op-errors *and verifies read values* cluster-wide).
   Large-value (4 KiB) writes hit **~21 GiB/s device = ~70 %** of the 29.9 GiB/s raw
   JBOD ceiling; get-any reads hit **~83 %** of the 6.6 M-IOPS raw read ceiling.
5. **Read fan-out RF → R (fixed).** A strong read fanned to all RF replicas and
   returned at the first R, so the drives did `RF/R`× the reads the answer needed.
   `Coordinator::with_read_fanout(ReadFanout::Quorum)` issues only R and escalates
   on a shortfall — same `W+R>RF` overlap, same read-repair coverage, at the cost of
   tail latency when a chosen replica is slow. It lifts strong reads **+42–52 %
   (1.9M → 2.85M)**, at which point strong reads are device-bound (~86 % of the raw
   read-IOPS ceiling) — i.e. **bound by the quorum-read device amplification itself**.
6. **Digest reads — one value read per strong read (fixed; the current default).**
   The amplification in (5) is intrinsic to fetching the *value* from R/RF replicas,
   but a strong read only needs R *versions* to pick the LWW winner — and one *value*,
   the winner's. `ReadFanout::Digest` fetches the value from *one* replica and a
   version-only **digest** from the others, then materializes the winner's value from
   the value reply (steady state) or one fallback value read on a digest mismatch
   (the value replica was stale/down). In the `latency` profile the winner directory
   answers version probes without run reads; the lower-RAM `capacity` profile resolves
   argmax through the engine and can read run index blocks. Either way only one reply
   carries value bytes, cutting value traffic ~R× while preserving `W+R>RF` overlap
   and read-repair. The digest is exactly the argmax version a value read would report,
   so the quorum-intersection argument is unchanged. It is a **hedged R-quorum**: only `R`
   probes (1 value + R-1 digests) are issued up front — matching `ReadFanout::Quorum`'s
   request count — and a held-back digest is pulled in *only* if one of the R fails
   (the `quorum_collect_reserve` primitive: the reserve future is never polled, so
   never sent, unless a failure drops the viable count below R; this avoids the
   early-exit a plain `R-of-R` quorum suffers when the value primary fails fast).
   **Hardware-measured** (`distmdmc`, 4 nodes × 2 drives × 16 partitions = 64 server
   cores, RF=3, real NVMe, 0-error): digest beats *every* prior mode, and the margin
   grows with load and value size as the single device read pulls away from the
   drive-bound R/RF reads —

   | config | `All` | `Quorum` | `Digest` |
   |---|---|---|---|
   | 16 coord, 100 B | 1.04M | 1.48M | **1.56M** |
   | 16 coord, 4 KiB | 0.86M | 1.14M | **1.30M** |
   | 48 coord, 100 B | 2.23M | 3.08M | **3.85M** |

   At 48 coordinators `Quorum` has hit the drive-saturation knee (2 device reads/read)
   and is declining, while `Digest` keeps climbing on its single device read.
7. **Loopback network stack (was the dominant cost) + oversaturation (open).** A
   write-path profile at 4M ops/s is **~65 % kernel** — loopback TCP send/recv,
   skb alloc, AppArmor socket hooks — with no single userspace hog (the in-flight
   demux map's SipHash, since replaced with the fast `FxHasher`, was ~2.5 %). On
   loopback the client and server CPU share one box and pay every byte's memcpy
   twice; a **real NIC / multi-machine run** moves that off the critical cores and
   is the real test — **now run, see step 10**. Past the write peak throughput
   still declines mildly with
   coordinator count (per-coordinator credit pools can't bound *aggregate* load —
   cross-coordinator admission is the open lever); the multiplexed transport already
   turned the old hard collapse into a gentle, variance-dominated slope. A read also
   still drains in-flight writes, so mixed read/write serializes (accepted for now).
8. **Per-op coordination cost — persistent edge-triggered reactor + fast version
   index (fixed).** A fresh profile at the step-6 peak was 67 % kernel / 22 % app,
   flat, with three addressable clusters: the `EPOLLONESHOT` re-arm (an
   `epoll_ctl` per park, whose in-kernel `sock_poll`/`tcp_poll` readiness probe
   summed to ~5 %+ across client and server), the version index's default SipHash
   on the apply hot path (~3–4 %), and `read_tsc` (~3 %, which call-graph
   attribution showed is the kernel TCP transmit path's own packet timestamping —
   loopback tax, not HLC; left alone). Fixes: reactor registrations are now
   **persistent + edge-triggered** (one `epoll_ctl ADD` per fd lifetime, re-park =
   userspace waker swap; deregister-before-close is now load-bearing and
   `MuxConn`/`Timer`/`Notify` deregister on `Drop`), and the version index uses
   the engine's `FxHasher`. A third candidate — gating the mux client's
   opportunistic per-poll drains to once per conn per turn — was built, measured,
   and **rejected**: it won under-saturation (+35–40 % writes at 16–32
   coordinators) but collapsed peak strong reads −33 %; near saturation those
   "redundant" drains deliver other tasks' replies mid-turn, exactly what digest
   reads' R-of-R barrier is most sensitive to (rationale recorded in
   `net::drain_reads`). Result (`distmdmc` 4×2×16, RF=3, 0-error, value-verified,
   n=30M): writes 3.25M → **3.51M**, strong reads 4.27M → **4.45M**, get-any
   6.11M → **6.46M** ops/s — short-run get-any peaks at **6.58M ≈ the raw 8-drive
   read-IOPS ceiling**, so eventual reads now saturate the hardware and strong
   reads are within ~70 % of it on a single box.
9. **Anti-entropy serve path — async borrow-free (fixed); the steady-state
   healing-on cost (partly open).** The benches in (1)–(8) drive *passive* servers
   with no anti-entropy sweep. With the **sweep on** (the live `Node`), a 3-node
   SPDK RF=2 cluster under `kvchurn` collapsed: client strong reads failed with
   `quorum failed: needed 2, got 1`. Root cause: only the point-`Read` serve path
   had been moved to the async borrow-free loop (step 3); the **`Scan` and
   anti-entropy `SyncBuckets` serve paths were still synchronous**, reading every
   page record back at QD1 while holding the store borrow — so serving one
   divergent bucket pull busy-blocked the single serving core and starved the async
   client reads past their deadline. Fix: `Scan`/`SyncBuckets` now serve through the
   same **borrow-free overlapped** path as `Read` (`plan_scan` /
   `plan_bucket_entries` build the page's read plans under a brief borrow; the
   spawned `serve_scan` / `serve_sync_buckets` execute them off the shared device
   handle at queue depth = page size). This removed the read-path collapse (**6 133
   → 0** `quorum failed: needed 2, got 1` errors over a 200 k-key + churn run), but a
   5 M write-ingest load still collapsed — `perf` traced that to a *second* cause:
   `execute_entry_plans` issued one `join_all` over the whole bucket page, and
   `vals_rt::join_all` re-polls every pending child per wake, so a ~tens-of-thousands
   -record `SyncBuckets` page was **O(N²)** and burned ~38 % of the serving core in
   the poll loop. Fixes: the page now reads in `PAGE_READ_CONCURRENCY`-wide windows
   (O(N·W), same QDN overlap without the quadratic re-poll), and a reconcile pass is
   bounded (`AE_RECONCILE_MAX_ENTRIES_PER_PASS`, with a rotating resume cursor) so
   AE's per-sweep read volume is capped. **Measured (3-node SPDK RF=2, sweep=5 s,
   5 M load):** ingest went from stuck at ~3–5 k writes/s (never completing) to
   **completing in ~45 s at ~126–638 k writes/s**; strong reads under churn rose from
   ~340/s to ~14–35 k/s. Still open: reads under sweep don't yet reach the sweep-off
   ceiling (the reconcile *apply* side + AE rate-limiting), and a *separate*
   **index-resize stall** (an unbounded O(n) `HashMap`/`BTreeMap` rehash on the
   serving core at hundreds of millions of keys/node) remains the next write-scaling
   blocker. All tracked in the ycc backlog (`backlog.md`).
10. **Real NIC / multi-machine (answered 2026-07-30) — the wall moved to the
   *client*.** Steps (1)–(8) were all measured on one box, where ~65 % of the
   serving core was loopback kernel networking; the honest test was always a
   cross-box run. Hardware now exists (lab-1/2/3, 24-core Ultra 9 285HX, one
   scratch NVMe each, **all links 10G**, <0.25 ms RTT, clients external). The
   in-process `distmdmc` staircase can't span machines, so the cross-box analogue
   is `vals-bench netload` / `netread` against a live `valsd` SPDK cluster (RF=2,
   PD-driven membership, 0 errors / 0 mismatches, live keys = RF×n in every run).
   Results, in the order the levers were applied:

   | cluster layout | writes | strong reads | node CPU |
   |---|---|---|---|
   | 3 nodes × 1 shard (16–32 coords) | **1.8M** ops/s | ~420k/node (1.0 CPU) | serving-core bound |
   | 3 nodes × 4 shards (1 client) | — | **~820k** | 2.7 CPU |
   | 3 nodes × 4 shards (4 client hosts) | — | **≥2.02M** | **~15 % of 24 cores** |

   Findings: (a) a node's read throughput is **serving-core bound, not
   drive- or NIC-bound** — one shard ≈ 420k reads/s/core, and sharding a
   single-drive node into 4 partitions scales it ~1.9×; (b) at ≥2M strong
   reads/s the node boxes are only ~15 % busy, so **the cluster ceiling was never
   reached — aggregate client load-gen capacity is the wall** (a single `netread`
   process plateaus ~0.9M and burns ~27 µs CPU/read, ~93 % of it kernel socket
   plumbing across ~4 tiny socket ops per key; batched multi-get was the
   obvious next lever at the time); (c) enabling the IOMMU for SPDK's vfio costs
   **~14 % of the serving core** in per-packet NIC DMA mapping — `iommu=pt`
   reclaims ~9 % of the core and is now the fleet default. So the loopback tax
   of step 7 is real and does come off the critical cores on a real NIC; what
   replaces it as the limiter is serving-core count per node (a config knob)
   and client-side syscall cost, not the network. Details: [`plans/box-provisioning-runbook.md`](plans/box-provisioning-runbook.md)
   §First real multi-machine SPDK cluster run and `bench/lab-read-prof/notes.md`.

   **Batched follow-up (task 0178, 2026-08-11).** `vals-bench netmultiread` now
   batches through `Coordinator::read_many` and sharded `SyncValues`. The tested
   batch-32 clean knee was **~0.61M keys/s**, versus **~1.19M keys/s** for legacy
   `netread`. Batching reduced client CPU/syscall pressure but exposed a lower
   batched server-path ceiling, so it did not reveal extra node throughput. See
   [`bench/task-0178-netmultiread/README.md`](bench/task-0178-netmultiread/README.md)
   for methodology and denominator caveats.

   **Vectorized digest quorum (task 0185).** `Coordinator::read_many` now uses the
   same one-value-plus-version-only shape as scalar strong reads: one value owner
   and R−1 digest owners per key, grouped into one bounded mixed-read RPC per
   node/shard. Replies are position-stable and carry per-key failures; a digest
   winner triggers one grouped fallback value fetch, after which the coordinator
   performs the usual tombstone-aware LWW repair. The server overlaps staged run
   probes and checked value reads across keys, answers winner-directory digests
   without device I/O, and pages contiguous result prefixes under the 8 MiB reply
   budget from conservative resolved-plan sizes. Feature negotiation falls back to scalar
   `Read`/`Digest` against older peers. `VALS_BATCH_DIGEST_READS=0` forces that scalar
   compatibility path for matched A/B measurement. The mature RF=2 lab gate
   passed: versus scalar digest, batched width 8/32/128 delivered **1.28× / 3.88× /
   5.63×** median keys/s (0 errors/misses/mismatches, zero digest probe blocks,
   ~1.00001–1.00006 value reads/key), while maximum sampled p99 improved from
   0.940/0.905/1.031 ms to 0.591/0.257/0.195 ms. Width 1 was neutral at 1.02×.
   Full PMU/network denominators and raw captures:
   [`bench/lab-batched-digest-20260812-0185/FINDINGS.md`](bench/lab-batched-digest-20260812-0185/FINDINGS.md).

   **Batched winner probing (task 0204, 2026-08-14).** Hot-shard profiling found
   `WinnerDirectory::find` at 13.6–25.3% self: every mixed-batch key independently
   traversed the large random-access directory, value reads also probed the mutable
   index, and post-I/O validation repeated the directory walk. The mixed server now
   hashes a whole batch, prefetches control/table → packed-entry → key-arena dependency
   stages, falls back to the scalar collision-safe probe when the home slot does not
   match, and carries an exact slot/key/entry token through checked value I/O. Repeated
   mature RF=2 reads at width 256 and 12 coordinators × 2 in flight rose from a
   **4.068M/s median to 4.960M/s (1.219×)**, with zero errors/misses/mismatches; batch
   p50 improved from 1.424–1.448 ms to 1.144–1.154 ms and p99 from 2.637–2.789 ms to
   2.198–2.349 ms. Server task-clock fell from 2.75 to 2.29 µs/key while the unchanged
   consistency/I/O shape remained one checked value read, one in-RAM quorum digest,
   zero run probes, ~184 B of server NIC traffic/key, and unchanged RSS. A 512
   MiB/engine block-cache experiment was rejected as RAM-expensive and neutral. Evidence:
   [`bench/lab-read-breakthrough-20260814-0204/FINDINGS.md`](bench/lab-read-breakthrough-20260814-0204/FINDINGS.md).

   **Post-0204 ceiling (task 0205, 2026-08-14).** Exact-source reruns reproduced
   **4.95–5.00M/s** at the supported shape. Every one of the twelve serving shard
   threads was 99–100% busy while the hosts retained idle cores; fresh hot-shard
   profiles moved the first actionable cost from winner lookup to mixed-batch result
   construction and memory movement (`memmove` 12.4% self on a representative shard,
   plus allocation and response work). Core top-down was about 59% backend-bound and
   43–44% memory-bound. Two independent clients remained at 4.951M/s combined and
   server traffic was only 7.3–7.4 Gbit/s, excluding a single client and the 10 GbE
   link. A corrected stopped-store control used the actual four consecutive 256 GiB
   production regions and reached 1.493–1.500M 4 KiB reads/s at QD128–256. That
   establishes the populated-region depth knee, but its absolute IOPS are not directly
   comparable with live Rust API-operation counters and do not establish NVMe as the
   next wall; re-measure it after the bounded response-workspace A/B. Evidence:
   [`bench/lab-read-ceiling-20260816-0205/FINDINGS.md`](bench/lab-read-ceiling-20260816-0205/FINDINGS.md).

   **Winner-directory shard gate (tasks 0183/0184, 2026-08-11).** The optional
   compact winner directory removes mature run probes and restores one checked
   value read per strong read. On matched fresh four-shard/node corpora, repeated
   strong reads reached **2.506–2.528M/s** and mature overwrites
   **0.996–1.211M/s**. A clean fresh-format review rerun measured end-to-end
   overwrite-ack p50 **0.590–0.619 ms** and p99 **3.987–4.428 ms**; its synchronized
   write-only window observed 1.072M accepted/submitted Rust SPDK write API
   operations carrying 6.147 GB of API payload over ten seconds, with zero
   asynchronous completion
   errors. These wrapper counters are not controller-command or physical-media
   telemetry. That run found the old per-shard maintenance design inoperable at
   eight shards: the controller's 15-I/O-qpair ceiling admitted eight serving plus
   only seven maintenance siblings, so one shard/node never flushed. Maintenance is
   now node-scoped: all shards route through one fair flush lane and one compaction
   lane, and each lane owns at most one thread-local qpair per controller. Thus an
   eight-shard, one-controller node has a steady budget of eight serving plus two
   maintenance qpairs; `(owner, zone)` caches retain completion and range isolation.
   The fresh node-scoped hardware gate then made every owner flush and merge under
   continuous reads with exactly 8+2 qpairs/node and no maintenance, qpair, snapshot,
   or SPDK completion errors. A separate no-run arm then made every owner reclaim
   value-log segments at 20.0–20.4% garbage under continuous individual reads: 29–31
   compactions and 14,848–15,872 segments freed/node, zero flushes/merges/runs and
   only the eight serving qpairs. Three matched mature RF=2 repetitions improved median
   strong-read throughput from **4.403M to 5.177M keys/s (1.176x)** with clean client
   correctness and tails. Eight shards cost about 2x daemon CPU/RAM and 1.78x
   task-clock/key, so use **eight shards/node for maximum read throughput on the
   Samsung 9100 PRO latency profile**, and four for CPU/RAM efficiency. Do not
   extrapolate the old invalid wider arms or this controller-specific qpair budget.
   Current evidence:
   [`bench/lab-maint-qpair-gate-20260817-0208/FINDINGS.md`](bench/lab-maint-qpair-gate-20260817-0208/FINDINGS.md).
   Historical failed-arm evidence:
   [`bench/lab-shard-scaling-20260811-0184/FINDINGS.md`](bench/lab-shard-scaling-20260811-0184/FINDINGS.md).

   **Write-path CPU breakthrough (task 0195, 2026-08-12).** Hardware profiling
   found that blind append's already-dirty Merkle buckets republished unchanged
   process-wide gauges on every candidate, consuming **41–44% of serving CPU**.
   Publishing only on the clean-to-dirty transition, caching restart-only
   admission/pacer configuration, and removing a redundant exact-winner hash
   lookup raised matched width-1 RF=2 quorum writes from **1.790M/s to 2.010M/s**
   at 16 coordinators × 512 in flight (**1.12×**), with zero errors/mismatches and
   improved sampled p50/main p99. A client-shape sweep then established the real
   throughput knee at 32 × 512: three confirmations delivered **2.677–2.684M/s**
   (**1.50×** the matched baseline) with zero errors and 1,000/1,000 sampled
   values correct. That saturation profile has higher p99 (23.7–26.1 ms); use
   16 × 512 for the ~2.0M/s lower-tail point. The current named latency profile
   additionally fuses winner-directory compare/update; its synchronized profile
   reached 2.006M/s at p50 3.62 ms / p99 9.79 ms and removed the previous second
   directory insertion probe. Evidence and invalid overload arms:
   [`bench/lab-breakthrough-20260812-0195/FINDINGS.md`](bench/lab-breakthrough-20260812-0195/FINDINGS.md).

   The same fleet now carries the recurring multi-day application soak (bootes
   firehose+backfill through the Go client, `plans/bootes-soak-runbook.md`), which
   is where the current write-path limits (per-engine memtable caps, RAM
   backpressure) are being found. The 24B-entry ATProto sizing pass found a
   separate mature-batch hotspot: pressure planning's victim-live sorter rescanned
   every run's segment metadata once per victim (O(victims × references)), taking
   23% of core-cycle self samples during one clean 1,000-entry persistent batch
   arm. That one-off hotspot motivated a controlled hardware A/B, but its matrix
   did not reproduce a material repeatable benefit or cross a merge interval, so
   the one-pass candidate was initially reverted (task 0201). Task 0229 later
   reproduced the mature hotspot repeatedly (21–30% sorted, 36–37% unsorted)
   and retained the linear `victim_lives_ascending` implementation; the final
   profiles no longer show that wall. `smallest_victim` remains the legacy scan.
   See `bench/lab-sorted-breakthrough-20260820-0229/FINDINGS.md`. The sizing
   pass records why full-network deployment uses the capacity profile and
   requires roughly 40–43 TiB live RF=2 allocation before headroom and skew;
   task 0202 supersedes its equal-slab geometry with variable extents and a
   conservative worst-shard sizing model. See
   `bench/task-0200-atproto-backfill/FINDINGS.md` and
   `bench/task-0202-zone-geometry/FINDINGS.md`. Final production-scale validation
   remains separate from these measured conclusions.

**Operability.** The observability floor is in (2026-06-09): every
state-changing transition (liveness wipe + durable pre-wipe marker,
fence/unfence, failure-detector trip/recover, ring/catalog adoption, zone
teardown, compaction/sweep outcomes, PD push/drive failures) logs through the
standard `log` facade (stderr logger built in, `VALS_LOG` level; raft's slog
bridged) and bumps a striped process-wide counter (`observe::snapshot()`).
Panic containment is in too (same day): every long-lived thread (shard
executors, PD drive loops, raft readers) runs under
`observe::spawn_abort_on_panic` — a panic aborts the process, so a dead shard
is a dead node rather than an invisible one-core degradation. Logging-failure
hardening (0254) makes stderr writes best-effort: I/O errors such as ENOSPC do
not panic the logging worker. A caught worker panic goes directly to abort,
without calling the logger, acquiring logging locks, or dropping its panic
payload. Original diagnostics remain the responsibility of the existing Rust
panic hook (and can be lost if stderr is unavailable); no secondary fatal log
is attempted. A blocked or non-returning panic hook can still prevent control
from reaching the catch branch; this does not make stderr writes nonblocking.
Subprocess regressions exercise `/dev/full`, a panicking logger,
and a panicking payload destructor, checking SIGABRT rather than merely a
nonzero exit. This does not add supervision of unexpected normal thread exits
or change executor/reactor teardown. Control-op
authentication is in (2026-06-10): the mutating `Set*` ops require the cluster
secret (`auth::init_cluster_secret`; see [Control plane](#control-plane-pd)) —
transport encryption is out of scope (use a private network or overlay). Process-wide transport bounds are adopted (tasks
0263/0277, 2026-09-08): all data/control/PD listeners share finite connection,
request, and retained-byte credits plus initial/frame/idle/write deadlines; the
wire protocol is unchanged and these controls are not authentication. The matched
reachable RF=2 SPDK gate measured write/read throughput at -1.08%/+1.61%, directly
profiled response allocation/copy, output release/reallocation, and shared atomic
costs as subdominant, passed full-value kvchurn, and demonstrated healthy progress
under low-limit abusive load. Exact tails, limits, profiles, and the lack of
three-node coverage are in
[`bench/0277-hardware-20260908/FINDINGS.md`](bench/0277-hardware-20260908/FINDINGS.md).
Graceful shutdown is in too (2026-06-10):
`Node::serve*` return a `NodeHandle` whose `shutdown()` drains the node — stop
accepting, exit the background loops, bounded wait for in-flight reads, flush +
close connections, group-commit every zone engine, persist the liveness
watermark, join (storage drop = clean SPDK detach) — and `PdDaemon::shutdown()`
stops + joins the control plane, so a rolling restart no longer looks like a
crash. With that, **every production blocker from the 2026-06-09 deep review is
resolved**; what remains is the correctness-hardening and operability list in
the ycc backlog (`backlog.md`).

See the ycc backlog (`backlog.md`) for the prioritized remaining work.

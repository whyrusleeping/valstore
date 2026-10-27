# Batch writes

The goal is to issue many writes at high throughput while amortizing protocol and,
when requested, device-sync cost. This is the **batch write API**. It replaces the
originally sketched general per-write durability barrier, which is a poor fit for
the leaderless data path (see "Why not a general barrier" below).

**Status: built** as `Coordinator::write_batch` (the `BatchApply` wire op and
per-write `WriteOutcome`s). The `WriteOptions` are chosen at submit time and the
resolved `BatchResult` is returned directly. `write_batch_in` supports zones.
Persistence is an independent choice inside `WriteOptions`: a bare `WriteConcern`
requests buffered replica-memory acknowledgements, and
`WriteOptions::device_durable(concern)` requests synced ones.

## Not atomic — bundling + joint durability, not a transaction

A batch is **N independent single-key writes** submitted together for efficiency
and a joint durability wait. It is **not** a transaction:

- Each write is independently HLC-versioned and resolved last-writer-wins.
- A subset can fail (e.g. lose quorum) while the rest succeed. There is no
  rollback of the successes.
- Multi-key atomicity was given up in the leaderless pivot; this API does not
  reintroduce it.

Because partial success is a real outcome, **the client must be able to tell
exactly which writes failed** — that is a first-class part of the API, below.

## API shape

```rust
let buffered: BatchResult = coord
    .write_batch(&writes, WriteConcern::Majority)
    .await?;

let device_persistent: BatchResult = coord
    .write_batch(&writes, WriteOptions::device_durable(WriteConcern::Majority))
    .await?;

let zoned: BatchResult = coord
    .write_batch_in(zone_id, &writes, WriteConcern::All)
    .await?;
```

`WriteConcern` sets only **replication breadth**: `Unacknowledged` (W=0), `One`
(W=1), `Majority` (W=RF/2+1), `Quorum(w)` (exactly W=w, `1 <= w <= RF`), or `All`
(W=RF). `Persistence` independently sets what each counted acknowledgement means:

| `Persistence` | Replica may acknowledge after | Power-loss guarantee |
|---|---|---|
| `Buffered` | installing the write in replica memory | survives process/network failures while an acknowledging replica remains alive; does not promise survival of correlated power loss |
| `Device` | the shard group-commit containing the write completes its device sync | survives restart/power loss on every acknowledging replica, subject to the device's flush contract |

Every method given a bare `WriteConcern` (and `write(k, v)`, which is `All`) uses
`Persistence::Buffered`. Pass `WriteOptions::device_durable(concern)` to the same
single, batch or range-delete method for synced acknowledgements. Thus the same
`WriteConcern` value has exactly the same replica-count meaning on both paths.
`Unacknowledged` waits for zero acknowledgements, so `Device` with
`Unacknowledged` is refused as `InvalidWriteOptions`; use at least `One` when a
durable acknowledgement is required. An entry whose W is impossible for its replica
set (`Quorum(0)`, or `Quorum(w)` above its replica count) gets a `Failed`
`InvalidWriteOptions` outcome with `acks: 0` and is not sent; W is never clamped.
A device-persistent single write is encoded as a
one-entry `BatchApply(sync=true)`; the protocol-v2 `Apply` layout remains the
buffered single-write operation.

## Detecting partial failure (the load-bearing part)

Each batch call resolves to a result whose per-write outcomes are **aligned 1:1
with the input writes, in input order** — so even duplicate keys are unambiguous:

```rust
pub struct BatchResult {
    /// One entry per input write, same index/order as `writes`.
    pub outcomes: Vec<WriteOutcome>,
}

pub enum WriteOutcome {
    /// Reached the requested write concern.
    Ok { version: Version, acks: usize },
    /// Did not reach the requested write concern. `acks` = how many replicas did ack,
    /// so the client can see it got e.g. quorum but not full RF.
    Failed { error: CoordError, acks: usize },
}

impl BatchResult {
    pub fn all_ok(&self) -> bool;
    /// (input index, error) for every write that did not reach the write concern.
    pub fn failures(&self) -> impl Iterator<Item = (usize, &CoordError)>;
}
```

So the client reacts precisely:

```rust
for (i, err) in result.failures() {
    // writes[i] did not reach the write concern; retry / compensate / surface it.
}
```

Carrying `acks` on both arms means a `Failed` at `All` still tells the
client whether the write reached majority breadth or reached nothing. Whether
those acknowledgements are buffered or device-persistent is determined separately
by the selected `Persistence`.

## Execution

1. **Stamp** each write with a `Version` from the node's HLC (monotonic; each
   write independently versioned).
2. **Group by destination node.** Each write's replicas are `replicas(zone, key)`;
   collect, per node, the writes that node must store, and send it **one** message
   carrying its sub-batch.
3. **Optional, coalesced sync per shard.** For `Buffered`, the shard acknowledges
   after applying its sub-batch. For `Device`, the request joins the shard/zone
   group-commit barrier. All concurrent single or batch requests that installed
   writes before that barrier share one engine sync and are released with the same
   sync result. One sync therefore serves N in-flight durable requests rather than
   each request issuing its own flush.
4. **Decide per write.** Quorum is still per-write: a write reaches its write concern when
   `W` of *its own* RF replicas ack at the required durability. Acks from the
   per-node sub-batch responses are scattered back to per-write outcomes.
5. **Failure provenance, exactly like the single-write path.** A per-node reply
   distinguishes three non-ack outcomes: a **stale-ring refusal**
   (`Response::StaleEpoch` for the whole sub-batch — the replica is *healthy*;
   the per-write failure surfaces as `CoordError::StaleRing` so the caller
   refreshes, and the refusing node is **not** hinted), a **clock-skew refusal**
   (`Applied::Rejected` — healthy, not hinted, surfaced as `SkewRejected`), and
   an **unreachable** node/shard — the only case that records a hint for a
   committed write. Hinting a healthy refuser would evict hints held for
   genuinely-down nodes from the bounded hint store.

The coordinator fans out with `vals_rt::join_until`: the batch resolves as soon as
every write has reached its `W`, and the sub-batch requests still outstanding are
cancelled, exactly as `quorum_ok` cancels the slowest replica of a single write.
So with `W < RF`, one black-holed replica does not hold the batch for the RPC
deadline (0496). An `Unacknowledged` batch, or one with a write that never
reaches `W`, waits for every reply, so a `Failed` outcome still counts every
replica that acked; an `Ok` outcome's `acks` counts the replies that had arrived
when the batch resolved (at least `W`). A cancelled replica that never received its
sub-batch is repaired by anti-entropy, as for single writes. Range deletes resolve
the same way: they keep up to eight node requests in flight (at most four per node,
so a black-holed node cannot hold every slot), skip a queued request whose slices
have all reached `W`, and resolve once every slice has. Their range-tombstone
support check uses the features last negotiated with a node, so a reconnecting
replica is not probed before every delete. The replica's sync-and-ack path confirms persistence past the
coalesced group-commit barrier. A sync failure is returned to every request joined
to that barrier; none of those device-persistent acknowledgements escapes early.

### Asynchronous group commit and the node-wide flush domain (0303)

The group-commit leader no longer spins its shard's qpair through the device
flush. It snapshots the joined waiters, drains and persists the tails
(`sync_prepare`), submits an asynchronous flush, and yields; the shard keeps
serving while the flush is outstanding. Requests that join after the snapshot
wait for the next round.

Shards whose zones share one NVMe controller namespace share a **flush domain**.
The domain keeps three monotonic counters: `claimed`, `completed` and `failed`.
It allows at most one device flush in flight. The rule rests on the NVMe Flush
contract: a Flush covers every command for the namespace that the controller
*completed* before the Flush was submitted, on any queue. A leader first
completes its own writes (`sync_prepare`), then reads `need = claimed + 1`. Any
flush numbered `need` or higher is claimed after that read and submitted after
its claim, so it covers the leader's writes. The leader either claims and
submits the next flush itself or waits for `completed >= need`. If
`failed >= need`, every covered waiter gets the error. Only the owning shard
publishes completion. If an owner abandons a claim (its state is dropped, or the
flush could not be submitted), the in-flight marker is released, `completed`
never advances, and a waiter claims a newer flush. A live but stalled owner is
bounded by a 2 s fallback to the leader's own blocking flush
(`vals_domain_flush_stalls`).

A cancelled leader, for example after a client disconnect, never fails the
other waiters. Its batch is re-queued, and one of the waiters leads a fresh
round (new prepare and a new flush claim). Orphaned in-flight flushes are
tracked and reaped by the shard. `finalize_shutdown` drains them, bounded to
5 s, before the devices are closed.

Measured (task 0303 AB01, two-node RF2/W2 device-sync on a server-bound dual-link
client): about 2.7× fewer device flushes for the same group-commit count,
response-ready 25–27 → 19–22 ms, and +14.2 % ACK KV/s (1.35 M vs 1.19 M).
Evidence is in `bench/0303-flush-domain-ab/`.

The SPDK regression arm pins breadth at `All` and reports buffered versus
device persistence for single writes and batch sizes 8/32/128:

```text
cargo run --release -p vals-bench --features spdk -- \
  spdk distbatch 4000000 100 <pci0,pci1,pci2,pci3,pci4,pci5> 8
```

The final argument is coordinator count. Run it only on six disposable namespaces:
the benchmark creates stores on the supplied devices.

The network width gate is `vals-bench netbatchwrite`; the reproducible width/value/persistence
matrix, captures, and fail-closed analyzer are in
`bench/task-0186-blind-batchapply/`. Width 1 uses the ordinary single-write API and widths
8/32/128/512 use explicit `BatchApply`, all at the same logical concurrency. The gate uses both
100-byte and 256-byte values and buffered/requested-device persistence at W=2. A one-host 10 Gb/s
payload ceiling is not treated as a server ceiling; server-capacity claims require distributed
client generation when the local link is limiting.

The retained task-0186 gate also evaluated transparent single-write combining. The matched hardware
off/25/50/100-us sweep found that combining reduced request count and median latency, but did not
improve throughput and materially worsened p99. The default-off experiment and its transport queues,
workers, and configuration were therefore removed. The historical harness and findings remain in
`bench/task-0186-blind-batchapply/` as decision evidence, not as a supported interface.

The 256-byte explicit matrix also did not improve. The fixed-concurrency and first attempted
"lower-load" device-persistent matrices overloaded the server gate; see
`bench/task-0186-blind-batchapply/FINDINGS.md`. The final serial `COORDS=1`, one-global-call sweep
completed widths 1/8/32/128/512 with zero errors or Busy responses and synced writes covering every
physical write. A second paced matrix held logical concurrency at 512 and offered 100 logical
writes/s at every width, also without errors or Busy. These validate sync-before-ack and group-commit
shape under a matched offered load, not the device path's saturation capacity. Buffered single
writes use scalar `Apply`; device-persistent single writes retain the validated one-entry
`BatchApply(sync=true)` path.

## Why not a general barrier

The original spec sketched `barrier(handles..., tier)` over arbitrarily many
independently-issued fire-and-forget writes. In the Raft design that was cheap —
wait until a partition's monotonic commit watermark passes the batch's max log
index. The **leaderless** path has no log and no watermark: each write goes to RF
replicas independently. A general barrier would force the coordinator to retain a
handle for every fire-and-forget write, track each write's landing position
across its replicas, and add a queryable/awaitable per-replica durability
watermark — an open-ended tracking protocol with its own GC and failure handling.

The batch handle tracks **one batch**, not every write ever issued, which removes
all of that. A general barrier can be layered on later **if** a true streaming
workload (continuous fire-and-forget with occasional checkpoints) actually
emerges; until then it is deferred, not built.

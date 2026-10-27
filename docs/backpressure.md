# Backpressure

Light durability tiers let a client issue writes faster than replicas can durably
absorb them. In the extreme (`W=0` fire-and-forget) the coordinator returns
without waiting, so without a bound the in-flight set grows unboundedly →
out-of-memory, latency blowup, or pressure to silently drop writes. This is the
flow-control design that prevents that.

**Built** (`ClientConfig`, `Coordinator::with_credits`, and
`Coordinator::with_node_credits`); this is the design it implements. See
[`ARCHITECTURE.md`](../ARCHITECTURE.md) for the as-built coordinator. A
coordinator over the default `NetTransport` receives finite global and per-node
logical-write limits, and the transport independently applies the same configured
ceilings to wire operations and retained wire memory. Coordinators over another
`Transport` remain unbounded unless their builders install limits. `W=0`
releases its logical credit at "sent to live replicas".

## Three queues, two ownership bounds

"The write queue" is really three stacked layers. The coordinator and transport
now each bound the allocations they own; TCP and the hint cap remain backstops.

1. **Transport (TCP)** — *implicit, already present.* If a replica's apply loop
   falls behind it stops reading its socket; TCP flow control fills the send
   buffer and the coordinator's *send* future parks. Transport-level throttle,
   mostly no code — plus an explicit per-conn **`MAX_CONN_SEND_BUF`** (256 MiB)
   high-water mark that fails/closes a conn whose queued-but-unsent bytes back up
   behind a peer that won't drain, so a wedged peer is a bounded blast radius rather
   than unbounded coordinator/server memory. The credit pool limits caller-owned
   key/value bytes per node; this transport cap separately bounds encoded unsent
   frames.
2. **Hint store (failure path)** — *bounded, already present.* Writes to a down
   replica become hints; `MemHintStore` has a per-node cap and evicts oldest, so
   a dead node cannot grow memory without limit.
3. **Coordinator issue path.** The in-flight writes that have been issued but
   have not yet reached their durability tier. Bounded by the logical credit
   pool below. Reads, scans, control RPCs, anti-entropy peer calls, and calls made
   directly through `NetTransport` are instead admitted by the transport pool.

The two pools account different ownership and therefore do not double-charge one
allocation. The coordinator pool charges borrowed caller key/value bytes held for
a logical write. The transport pool charges encoded request frames, outstanding
wire operations, response frame storage, and decoded-response handoff. Each pool
has the configured global and per-node ceilings, so capacity planning must allow
up to both byte ceilings plus fixed connection/socket overhead. TCP and the hint
cap compose underneath them.

## The credit pool

A coordinator holds a pool of in-flight **credits**, bounded on **both**:

- **ops** — max number of in-flight writes; and
- **bytes** — sum of in-flight key plus value sizes (a delete charges its key).

Bytes is the real memory guard (a few multi-MB values blow a generous op count);
bound both, hit either limit and you're throttled.

### Acquire / release

- **Acquire at issue.** Each write (and each write in a batch) acquires a credit
  before it is dispatched. If the pool is temporarily exhausted, the `write()`
  future **awaits** a free credit — *this is the backpressure*: the producer
  slows to the rate replicas drain. An earlier waiter blocked by the global
  pool gates later work. A waiter blocked only by one destination does not gate
  later work for disjoint healthy destinations.
- **Reject impossible requests.** A single request whose operation or byte cost
  exceeds either its global or destination-node limit returns `Overloaded`
  without entering the wait queue. Finite limits are never exceeded just
  because a pool or node happens to be idle.
- **Release when the operation future ends.** Success, quorum failure, timeout,
  cancellation, stale-ring retry completion, and partial replica failure all
  drop the same credit guard. Below `All`, unfinished replica futures are
  cancelled locally after the W-th acknowledgement; at `All` (W=RF), a
  slow replica holds the credit until it replies or reaches its finite RPC
  deadline. Cancellation cannot retract a write already sent or applied.

### Transport admission and allocation ownership

Every finite `NetTransport` RPC acquires its actual destination's transport
credits. A scalar RPC charges one operation; batch apply, mixed read, and values
RPCs charge one operation per logical entry carried by that frame. Request bytes
are the encoded payload plus framing. Encoding is performed once to measure the
exact charge; if admission cannot happen immediately that temporary buffer is
dropped before the future joins the wait queue and is encoded again only after
admission. Thus a waiting transport future retains borrowed request arguments and
small routing state, but no encoded request allocation.

Once admitted, separate operation and encoded-request credits move into the mux
waiter. The request-byte credit is released after its frame watermark is sent,
while the operation credit remains through reply arrival. Cancelling or timing
out a wholly unsent request synchronously splices its frame from the connection
queue, compacts retained vector capacity, adjusts later frame watermarks, and only
then releases its credits. Unrelated waiters on that connection continue normally,
including while the socket is waiting for its handshake. A partially sent frame
cannot be retracted safely, so cancellation tears down that connection after
physically releasing queued storage; collateral waiters receive local
`Overloaded`, which is neutral to failure detection and hint creation. A fully
sent cancelled request can drop its waiter and operation credit directly.
Connection failure likewise clears the output and pending-response maps before
releasing their credits. Reader or reply allocation refusal resets the connection
but completes every affected waiter as local `Overloaded`, so it does not count as
a node failure or create a hint. Reader reset always drops partial-frame storage,
including unaccounted connections whose byte counter is zero.

Inbound frame-buffer capacity is charged as it grows and remains charged while
the connection retains that capacity. The decoded reply is built from charged
wire bytes before acquiring its own retained-object charge, so instantaneous
transient decoded expansion can exceed the pool until admission refusal drops it.
The copied mux response is charged during wire decode, then replaced by a charge
for decoded value-bearing vector capacity, key capacity, and value capacity.
Multi-shard, multi-page, and scalar-fallback
batch reads reserve their result-vector capacity before fanout and retain decoded
value guards (including duplicate-value clones) until the transport operation
hands its output to the caller. Coordinator read and scan paths immediately
replace that handoff guard with an actual-destination response-retention charge
and keep it through quorum aggregation, read repair, page merging, and the public
coordinator handoff. Decoded accounting covers all heap-owning wire replies,
including anti-entropy trees/digests, membership/catalog/range metadata, batches,
and remote error strings; expanded values results also charge cloned values and
error strings until handoff. Completed RPC operation credits are released before
sibling/page admission, so low operation limits cannot self-deadlock. Every
finite-pool admission wait expires with `Overloaded` at the configured RPC
deadline (or the default RPC deadline when wire deadlines were disabled), so a
page or read repair that cannot fit beside output accumulated by its own operation
cannot wait on itself forever. A typed result returned directly
from a transport method becomes caller-owned at that handoff and is then outside
transport credits; a coordinator-owned result stays charged until the coordinator
hands it to its caller. If an incoming or decoded allocation cannot fit, the connection or RPC
fails deterministically rather than retaining unaccounted bytes. Returned
caller-owned values, caller-owned input buffers, fixed per-connection read
scratch, kernel socket buffers, and catalog/routing metadata are outside
transport credits. `NetTransport::transport_credit_stats` reports this pool
separately from `Coordinator::credit_stats`.

Transport per-node charges always name the node actually called, including sloppy
fallbacks. The logical write pool continues to describe the original write and
home placement; it is not reused as wire accounting. The wait discipline permits
a later request for a disjoint node past an earlier waiter blocked only by a
saturated destination, while an earlier globally blocked waiter still gates later
work.

### Never drop

Exhaustion **blocks**, it does not drop — even a fire-and-forget write should not
silently vanish when the client believes it is queued. For callers that would
rather shed load than slow down, a `try_write` returns `Overloaded` instead of
awaiting. An individual direct transport request larger than either transport
limit returns `TransportError::Overloaded`; temporary exhaustion awaits only
until the finite admission deadline. (Default is to await for at most five
seconds.)

## `W=0` (fire-and-forget) credit semantics

`W=0` returns without waiting for acks, so "reached its tier" needs a definition.
**Chosen (and built): release the credit once the write has been _sent to its live
replicas_** — the transport write toward every currently-live owner completed, and
hints are recorded for any unreachable ones. Rationale: this gives **end-to-end**
backpressure — dispatch rate is gated by TCP (layer 1), which is gated by the
replicas' absorb rate — so a producer that outruns slow replicas is actually
throttled, and a dead replica never stalls the producer (it goes straight to a
hint). The cost: `W=0` is not "return the instant I'm called," it is "return once
the bytes are on their way."

**As built.** `put_once` special-cases `needed == 0`: it fans out `Transport::apply_send`
(gated by the failure detector, so an FD-down owner fails fast to a hint) to every
home replica and awaits **all** of them before the caller's credit drops. A
successful send is *not* fed to the failure detector — a local flush proves nothing
about the peer — but a definitive send error is. `apply_send` has a default that
delegates to `apply` and discards the ack (correct-and-stronger; exact for the
in-process `LocalCluster`, which completes synchronously). `NetTransport` overrides
it with true sent semantics: each connection tracks a monotone `sent_total`
byte counter and a send-waiter resolves once that counter passes its frame-end
watermark (failed on conn reset / the per-RPC deadline sweep, so a black-holed peer
can't hang `W=0` forever).

The alternative — release the moment the write is *handed to the transport* (queued
into the conn's out buffer) — bounds only coordinator memory and lets a producer
outrun the network. It is simpler but weaker; the difference is local to the
credit-release point.

## Scope and observability

The standalone SDK's `vals_client::observe` contains client counters and metric
primitives only. Server/PD registries and utilities (process memory sampling, logger
initialization, and abort-on-panic thread spawning) live in `vals_cluster::observe`.
Code that previously imported those server facilities through the client module must
use the cluster path. Cluster snapshots and the daemon exporter include the same
shared client counters; Prometheus names, HELP text, and types are unchanged.

- **Global pools + per-node sub-limits.** The coordinator's logical pool is the
  aggregate guard for caller write input, and the transport pool is the aggregate
  guard for actual wire work. Their per-node sub-limits isolate a slow/dead
  replica so it cannot starve requests routed to disjoint healthy nodes. A
  logical scalar write counts its full `(ops, bytes)` against every home, while
  transport frames charge the actual destination. A logical batch computes each
  home's exact subset; each transport shard frame charges the entries it carries.
- **Accounting scope.** Logical credits cover caller-owned keys/values for
  coordinator write, delete, sloppy-write, and batch paths until those operation
  futures end. Transport credits cover encoded requests and response-side
  allocations for read, scan, write, control, anti-entropy, and direct transport
  calls. These are pressure/admission limits, not a byte-for-byte process heap
  cap: returned caller-owned results, routing metadata, hints, and kernel socket
  buffers remain outside. The transport also retains the independent per-conn
  `MAX_CONN_SEND_BUF` failsafe.
- **Expose metrics** — in-flight ops, in-flight bytes, credits available, and
  time spent awaiting a credit — so operators can see saturation (this also feeds
  the broader observability gap noted in `ARCHITECTURE.md`).

## Defaults and migration

`NetTransport::connect` and `connect_sharded` install 32,768 global operations,
512 MiB global bytes, 8,192 operations per destination node, and 128 MiB per
node in each of the logical-write and transport ownership pools. The same
`ClientConfig` also installs 5 second connect/hello and per-RPC deadlines.
`connect_with_config` validates custom settings. Rust consumers migrating from
0264 must budget for transport operations as well as coordinator writes and for
up to two independently bounded ownership pools. Response headroom must include
connection frame capacity, the copied mux payload, and decoded typed capacity;
the default 128 MiB per-node transport budget already equals only two 64 MiB
maximum frames, so a near-maximum valid value-bearing response can close its
connection when those allocations overlap. Smaller custom limits encounter that
boundary earlier. `connect_unbounded` and
`connect_sharded_unbounded` are explicit benchmark/development opt-outs that
remove deadlines and both pools. `vals-bench netload` now selects production
finite defaults when `VALS_BENCH_BOUNDED_CLIENT` is present and retains the
historical explicit-unbounded shape when it is absent; it prints the effective
mode and per-coordinator receipts. Other benchmark constructors keep their
explicitly chosen mode. External Rust consumers must deliberately adopt the
finite defaults, pass reviewed custom values, or opt out. Independent Go
clients are unchanged and do not acquire these Rust limits. Task 0279 supplies
the reviewed benchmark/probe prerequisite. The task 0287 SPDK RF2 hardware gate
([findings](../bench/0287-hardware-20260909/FINDINGS.md), "Disposition") is an
executed no-go for performance adoption: it validated finite-client fault
behavior, but finite defaults cost 4.59%/6.78% median width-32 Digest/Quorum
read throughput versus explicit-unbounded, and hardware hedge-loser
cancellation remains unproven. The finite defaults are retained as
resource-safety bounds, not as a performance-adopted configuration.

## Interaction with the batch API

A batch ([`batch-writes.md`](batch-writes.md)) acquires global credits for its
constituent writes and exact per-node credits for the entries routed to each
home; `write_batch` awaits when a temporary limit is full, exactly like a single
write. `CoordinatorService::submit_write_batch` performs the same admission
before reporting the submission admitted. An impossible submitted batch resolves
its handle with `Overloaded` rather than parking forever. Dropping that handle
does not cancel its already detached service task.

## Replica RAM budget

Each replica process has server-side RAM admission control in addition to the
coordinator credit pool and device-space reserve. `ram_budget_bytes`
(legacy alias `VALS_RAM_BUDGET_BYTES`) sets a
per-process RSS-plus-swap byte ceiling. If it is unset, vals uses 70% of Linux `MemTotal`; an
explicit value of `0` disables RAM admission control. If `MemTotal` cannot be
read and no explicit budget is supplied, admission control is disabled and the
node logs a warning.

The admission sampler reads process RSS and anonymous `VmSwap` independently at
most once every 100 ms; it does not depend on anti-entropy or maintenance sweep
cadence. The enforced footprint and `ram_headroom_bytes` are RSS plus swap, so
swapping anonymous allocations out cannot manufacture admission headroom. At the ceiling,
wire-level `Apply` and `BatchApply` record intake receives retryable `Busy`,
regardless of origin. This deliberately includes client writes, hint replay,
cleanup handoff, and re-home mirror writes. Local anti-entropy heal applies and
re-home range-copy intake also check the budget and defer their apply phase when
over budget. Those internal producers run bounded, best-effort loops and back
off and retry; during RAM pressure the node stops accepting repair and re-home
traffic by design. A Busy-refused mirror issues no ack and does not count as a
mirror failure, so it does not poison the re-home; a re-home whose copy is
deferred by RAM pressure simply stays pending (the target keeps reporting, so
`rehome_report_deadline_ms` (legacy alias `VALS_PD_REHOME_REPORT_DEADLINE_MS`) does not fire) and resumes when pressure
clears. Reads, scans, digests, control
operations, and the node's own local flush/merge/compaction drain remain
admitted, so pressure can clear without a wedge or lost writes. The
`ram_busy_rejections`, `ram_budget_bytes`, `ram_headroom_bytes`,
`process_resident_bytes`, `process_swap_bytes`, `memtable_bytes_total`,
`flush_build_bytes`, and `zone_engines` metrics show engagement and the
accounted working sets. `merge_in_flight` and `merge_in_flight_input_bytes`
identify active off-core merge working sets. `ae_seed_staged_bytes` estimates
memtable snapshots, run version-stream windows (v3/v4 sidecars, v5 data blocks), merge heads, and placement digest groups
retained by cooperative seeding;
`ae_seed_ram_sheds` counts unpublished seeds discarded at the ceiling.
A process-wide byte broker also reserves AE seed and off-core flush/merge working
sets before scheduling them. `ram_maint_reserve_bytes`
(legacy alias `VALS_RAM_MAINT_RESERVE_BYTES`) caps the shared
maintenance pool (default 19% of the RAM budget); available capacity is the
smaller of that cap and budget minus charged serving memtables, resident run
metadata and configured `ram_dma_scratch_bytes`
(legacy alias `VALS_RAM_DMA_SCRATCH_BYTES`, default 0). There is no broker deferral when the
RAM budget is disabled. Recurring jobs defer at the pool limit or RSS ceiling.
Since 0381 that includes recurring and forced run merges, whatever the run count.
They used to overdraw at `run_compact_threshold` runs, and with a frozen tier that
threshold is always crossed at scale. A deferred plan returns its reserved segments.
Pressure flushes, pressure/shadow-fold/value-log-GC/tombstone-GC/forget merges and
the synchronous seed fallback proceed with recorded debt to keep drain live. Merge reservations estimate
40 bytes per input entry plus 2 MiB for bloom, dropped tombstones and run parts;
they are estimates, not measured RSS. `ram_charged_bytes`,
`ram_reserved_ae_seed_bytes`, `ram_reserved_flush_bytes`,
`ram_reserved_merge_bytes`, `ram_reserve_capacity_bytes`,
`ram_reserve_debt_bytes`, `ram_reserve_deferrals` and
`ram_reserve_oldest_wait_ms` expose broker accounting separately from
`process_resident_bytes`. `run_metadata_resident_bytes` estimates charged
resident serving filters, fences, indices, run parts and manifest copies from
engine-owned capacity; it is not measured RSS. SPDK hugepages may sit outside RSS.
Streaming exact AE seed is the only cooperative seed path. Legacy v2 sidecar
runs are materialized and sorted per run before joining the streaming merge;
that memory is charged to the seed reservation.
The SOAK0321-F4 retained-data run observed 19.3 GB versus 43.6 GB RSS,
zero seed sheds and an exact census PASS; this is not a fixed-envelope gate.
Cooperative full anti-entropy seeds share a process-wide cap of four concurrent
stores (`ae_seed_concurrency`, legacy alias `VALS_AE_SEED_CONCURRENCY`; `0` disables the cap). A store without a
permit defers until a later sweep rather than spinning; a RAM shed blocks new
seed starts node-wide for ten seconds (`ae_seed_shed_cooldown_ms`,
legacy alias `VALS_AE_SEED_SHED_COOLDOWN_MS`, `0`
disables the cooldown). Existing seeds can continue or shed independently.
One-shot synchronous fallback never waits for a permit. `ae_seed_active` counts
held permits and `ae_seed_permit_deferrals` counts skipped starts. On glibc,
completed or shed seeds schedule an off-core `malloc_trim(0)` after seed
activity quiets, at most once per thirty seconds; `VALS_AE_SEED_TRIM=0`
disables it. A file-backed mass-reseed A/B is recorded in
`bench/0323-seed-permit/report.md`.

Seed isolation (0374, `ae_seed_isolation=1` by default) keeps post-restart
seeding off the apply path. Each shard's logical-accounting task is the only
driver of its stores' seeds; it attaches to every zone store it serves
(`ae_seed_drivers`). Peer `SyncTree` requests, local tree readiness, eager
sweep rebuilds and AE reconcile no longer step an unfinished seed: they answer
retryable Busy (`ae_seed_not_ready_busy`) or skip the zone until a later sweep
(`ae_seed_reconcile_skips`), so a peer's request cannot keep a seed spinning
after its RPC deadline. Stores without a driver task (library and test use)
still seed on demand; a driver that exits at shutdown leaves its stores
driver-owned, so connection drain cannot restart on-demand seeding.

The driver treats the shard as busy while any Apply, BatchApply, read or scan
request was dispatched in the last 200 ms, or while the BatchApply, read or
scan gate has work in flight. (RECOV01 showed the in-flight check alone misses
sustained ingest: on a single-threaded shard most batches start and finish
between two seed steps, so the gates looked idle and seeding took 84% of busy
plus pause time against a 25% target.) While busy, seed steps and post-carry
split-recount turns draw from one budget that earns `ae_seed_duty_pct` (25)
percent of elapsed wall time, holds at most 20 ms of credit, and once the debt
reaches 1 ms pauses the driver until it is repaid (at most 1 s per pause). Debt
below 1 ms carries over to later steps, so the share stays exact over the
window without a timer per step, and many short steps cannot add up to more
than the target. An idle shard seeds at full speed. Each
run-churn restart of the same seed doubles its share so merges cannot starve
it, but only up to 50% (or the configured share if higher). `ae_seed_busy_us`
(seed steps and recount turns), `ae_seed_duty_pauses` and
`ae_seed_duty_pause_us` show the duty.

Because the duty fixes the seed's share of a busy shard, a cheaper seed key
shortens the seed rather than lowering its per-second cost. The streaming seed
merges run heads with a binary heap, reuses a key's replica group and logical
range while consecutive keys stay inside one range of the placement map, and
counts folds (`ae_seed_fold_ops`) at every step end, including window-read
checkpoints. In the one-shard L1 restart A/B (`bench/0422-seed-merge/FINDINGS.md`)
this halved seed cycles per key and finished the post-restart seed at R+31
instead of R+51 minutes.

Under isolation each shard runs at most one seed at a time, so driver seeds do
not take an `ae_seed_concurrency` permit; they still count in `ae_seed_active`
and honour the RAM-shed cooldown. With 8 shards and 4 permits, RECOV01's
restarted node seeded four shards at a time while each held its permit through
its duty pauses, which held seeding to about one core. Seed memory stays
bounded without the cap: staged leaves, memtable snapshots and cursor windows
are reserved from the RAM broker, which defers or sheds a seed that does not
fit, and the journal has a node-wide cap.

Writes into an engine whose seed is in flight are journaled. The journal's
bytes are charged to the RAM broker like memtables. The gauge
`ae_seed_journal_bytes` is node-wide and is an estimate: each store
publishes in 256 KiB steps.

`ae_seed_journal_max_bytes` (256 MiB) is enforced on reservations, not on the
gauge:

- Before a journal grows its vector capacity or key bytes, it reserves the
  bytes from one process-wide counter by compare-and-swap, so it can never pass
  the cap.
- Reservations are made in 64 KiB chunks and released when the seed publishes
  or resets. A journal's actual bytes never exceed its reservation.
- The sum of reservations is therefore an exact bound. The only exception is a
  re-entrant journal write made while the same store's seed step holds its
  progress. It must not be dropped, so it overdraws and is counted. This path
  is not reached in practice.

(In RECOV01 the cap was per seed and checked after `Vec` doubling, and the
gauge reached 373 MB with four 64 MiB seeds. In RECOV02 a lagging node-wide
check reached 279 MB against 256 MiB.) A write that cannot reserve room is applied
directly to its seed's staged accumulator as a commutative delta (leaf XORs;
signed bucket, live and range-count deltas; an unknown old winner dirties its
bucket). That seed then stays in spill mode until it publishes. This is
counted by `ae_seed_journal_spills` and `ae_seed_journal_spill_ops`.
`ae_seed_isolation=0` restores the multi-driver, unbudgeted, uncapped
behaviour for A/B.

A seed survives merges (0379). Before, every run removal (a merge or forget
commit bumping `runs_removed_epoch`) restarted an in-flight seed from scratch.
With two merges in flight under ingest a seed never finished: in SOAK5B one node
restarted 161 times, the stuck seed held its shard near 95% CPU and ACK fell
23%. Now a seed pins its input snapshot, the runs present when it started:

- When a merge or forget removes a pinned run, the engine keeps the run's
  segments allocated (they count as protected, so orphan reclaim and staged
  frees skip them) instead of freeing them. The seed keeps reading its
  snapshot, and `ae_seed_merge_carries` counts each removal commit it rides
  through (since 0427 the epoch advance, not the steps that saw one). Runs
  written after the seed started are never held.
- This is exact. The seed folds the winners of its snapshot (memtable plus
  pinned runs) and then replays its journal. A merge does not change logical
  winners except through retirements that are journaled anyway. Dropped
  shadowed versions were never winners. A GC-dropped tombstone or forgotten
  entry that was the current winner is reported (`current_dropped_tombstones`
  or forget's dropped list) and journaled as a retire, so the snapshot's copy
  is folded in and then retired. The commit refuses a drop that a survivor
  would resurrect. Leaves and counts include tombstones as winners on both
  sides, so the published state equals a from-scratch seed.
- The held bytes are at most the seed's starting run set. They are released
  when the seed publishes or resets, at the next reap (the shard's seed driver
  after each step, and every maintenance tick). Reaped segments are counted by
  `ae_seed_pin_reaped_segments`, not `segments_freed`. A truncate or store drop
  discards them; they are in-memory state only, so a restart never leaks them.
  `ae_seed_pinned_bytes` is the node-wide gauge. A held v5 run also keeps its
  block index (fence) resident through the seed's stream source, about 0.2% of
  its bytes. Until the seed releases it, that index is counted in
  `run_metadata_resident_bytes`, so the RAM broker charges it (0380).
  The engine-local commit check compares only bytes still held by a live pin
  against the seed's share; an earlier seed's released, not-yet-reaped runs do
  not count.
- **Cap.** `ae_seed_pin_max_bytes` (64 GiB, node-wide) is a budget shared by
  the in-flight pinned seeds in pin order (0380). Each pinned seed gets a
  ticket when it pins. The oldest pinned seed is never restarted by the cap.
  Every younger seed may hold the cap minus the bytes held by all older pinned
  seeds. Once the node exceeds the cap the bound drops to 85% of the cap
  (`PIN_SHED_TARGET_PCT`) until the node is back at or below 85%, and no new
  seed pins while shedding or at the cap. A younger seed over its share
  restarts (`ae_seed_pin_cap_fallbacks`). The share is checked at each seed
  step and also by the engine at the merge or forget commit that would retire
  the seed's runs: if holding them would exceed the share, the commit frees
  them and revokes the pin. A younger seed's share is fixed at its last step,
  so overshoot is bounded by the oldest seed's holdings plus what older seeds
  gained since then, not by a whole merge of every engine. `ae_seed_pin_cap_exempt_steps` counts steps where the
  oldest seed held its runs while the node was over the cap. A seed that
  starts without a pin restarts on its engine's next run removal
  (`ae_seed_merge_restarts`). A restarted seed's own not-yet-reaped bytes do
  not count against its new pin.
- **Why one seed always completes.** Seeds are shed youngest first, and a
  restarted seed re-enters with a fresh, youngest ticket. The oldest seed rides
  every merge and finishes in a bounded number of steps unless region pressure
  or a placement/RAM reset intervenes. When it publishes, the next-oldest
  becomes exempt. Seeds therefore complete at least one at a time in pin
  order. The pre-0380 rule restarted every holder while the node was over the
  cap, so seeds that together needed more than the cap restarted forever (the
  SOAK5B mode).
- **Sizing.** The cap stays a fixed 64 GiB rather than a share of free space.
  Space safety is the engine-local pressure revocation below, which is already
  relative to each engine's region. The cap only bounds how much space
  concurrent seeds hold together. At the 5B scale (~2.8 TB/node across ~24
  engines, ~100+ GB of runs per engine) 64 GiB is ~2% of the node's data. That
  is several typical tiered-merge retirements per seed, since SOAK5B-V2's eight
  concurrent recovery seeds peaked at 7.09 GB. A full-set merge of one large
  engine can exceed the cap, but only for the oldest seed, which is the one the
  policy lets finish.
- **Pressure.** If the engine's region nears its floor (the admission band,
  the maintenance floor, or held segments at least equal to free segments), the
  engine revokes its pins at the next merge commit or reap and frees the runs.
  The oldest seed is no exception. The seed restarts
  (`ae_seed_pin_revocations`).
- `ae_seed_pin_max_bytes=0` restores the old restart-on-merge rule
  (`ae_seed_merge_restarts`).

`ae_seed_completions` counts published seeds and `ae_seed_duration_ms` is the
first-step-to-publication histogram (bounds in milliseconds, sum in seconds).
Every started seed ends exactly once (0427), so at any scrape `ae_seed_starts`
equals completions + `ae_seed_restarts` + `ae_seed_placement_resets` +
`ae_seed_generation_resets` + `ae_seed_ram_sheds` + `ae_seed_corrupt_sidecars` +
`ae_seed_error_resets` (other read errors; the driver logs them at warn, at most
once a minute per zone) + `ae_seed_dropped` (truncate, leaf invalidation, store
drop) + seeds in flight. A synchronous fallback counts one restart and then its
own start and completion. A seed in flight at a fail-fast exit, which skips the
store drop, is not counted.

## Staged BatchApply gate

Each shard admits at most `batch_apply_gate` (default 64 since 0373, previously 32; bounds 1..=256; legacy
alias `VALS_BATCH_APPLY_GATE`) concurrent staged `BatchApply` requests; the permit is taken before the segment-pinning read gate.
A request arriving at a full gate is not queued: it gets retryable `Busy`
immediately and increments `batch_apply_gate_busy`. This covers client writes and
re-home mirror writes alike (task 0176: queueing behind a full gate, not shedding,
had pushed mirror RPCs past their two-second deadline). The limit was raised from
8 to 32 in task 0301: every pipelined client `PutBatch` fans out to every shard, so
the old gate capped the whole cluster at about 8 in-flight `PutBatch`es and shed
beyond (PRESSURE01-K8 collapse). With 32, client depths 8 and 16 run without sheds,
and master plus this change alone reached 930k ACK KV/s on the realistic two-node
RF2/W2 device-sync fixture (`bench/0297-ingest-1m/scale01/FINDINGS.md`, attempt
SCALE01-Y5). Admitted staged work stays bounded at about 32 sub-batches per shard
(roughly 100 ms of serving-core work at 1,000 entries each), well below the 2 s
mirror and 5 s client RPC deadlines. Depth 24 still shed against 32 (SCALE01-Y2).

The gate is a startup-only typed node setting (see [configuration.md](configuration.md)).
bootes's `--vals-server-gate` must match the server's `batch_apply_gate`; raising
`--vals-depth` requires raising this gate as well, with room for fanout and mirror
work. A larger limit also increases staged memory and tail latency; it does not
change the one-Flush-per-controller rule.

## Off-core maintenance I/O pacing

Off-core maintenance — flush run builds and run-merge folds — competes with
the serving path for device bandwidth. Under sustained overwrite every apply
can perform one cold LWW run-block read. Device-persistent requests additionally
join a per-shard/zone value-log sync barrier; concurrent requests coalesce behind
one barrier, while buffered requests do not force one. A synchronized maintenance
burst (all shard engines flushing together under uniform load) inflates apply
latency from hundreds of microseconds to several
milliseconds and collapses intake for seconds at a time while the node sits
otherwise idle (the task 0166 attribution,
`bench/lab-bench-20260810-0166/notes.md`). Three mechanisms bound that
interference. A process-wide duty-cycle pacer spreads maintenance device work
over wall-clock time: each paced job charges its measured device work against
a shared debt and sleeps the complementary share, so each maint worker (lane)
holds roughly `VALS_MAINT_DUTY_PCT` percent (default 33) of its wall time and
the process total scales with the lane count; whichever lane charges next may
sleep off debt another lane accrued. Accumulated debt is capped at 500 ms and any
single sleep at 100 ms, so pacing can slow but never wedge a job. Pacing is
backlog-aware (0314): the pacer only sleeps while no other maintenance job is
queued behind the running one (the flush and compaction queues are counted at submit and
at worker start), so it spreads isolated bursts but never lets flush or merge
work pile up behind a sleeping lane; a paced merge picks readahead from the
same signal, and `maint_pace_backlog_unpaced_ms` exports the work run unpaced
for backlog. Under sustained ingest this cut resident runs roughly in half and
gave +11.6 % ACK/s over 158 M KV at about 1.3× device write bytes per KV.
Run merges execute on `maint_compact_lanes` (legacy alias `VALS_MAINT_COMPACT_LANES`)
compaction lanes per node (default 2, valid 1..=4; flushes keep their single lane). The lanes pull from one
owner-fair queue, each lane opens its own maintenance qpair per controller on its
own thread, and each engine still has at most one merge in flight
(`compact_in_flight`), so lanes only add parallelism across engines; the
per-engine maintenance reserve and headroom accounting are unchanged. Paced run
builds interleave a device flush every `VALS_MAINT_FLUSH_BYTES` (default
16 MiB) so a serving-path device-persistence barrier never pays a 100+ MB
dirty-cache bill at once. Paced merges read their inputs with the sync scan instead of
the QD-8 prefetch (`VALS_MERGE_READAHEAD` still governs unpaced merges).
Liveness-critical work runs unpaced: pressure flushes and pressure or
aged-vlog-GC merges, and any flush whose memtable has already overrun twice its
per-engine budget. Key-filter cover scans of active-uncovered runs (after a
reopen every apply probes those runs individually) also run unpaced (0421);
shadow-rebuild scans stay paced. Since 0381, recurring merges chosen by the run-count valve
(`VALS_MERGE_FORCE_RUNS`) are paced like other recurring merges. They are
bounded by the input cap, and a larger run count costs reads, not liveness.
Pacing still yields to a queued backlog.
Pacing's measured effect is on the latency tail, not the mean: flush windows
serve at full rate with sub-millisecond applies instead of collapsing, and
client-visible timeout errors drop roughly 8x, at unchanged write
amplification. `maint_pace_charged_ms` and `maint_pace_slept_ms` export the
observed work and the imposed sleep; `VALS_MAINT_DUTY_PCT=0` (or any value at
or above 100) disables pacing.

## Zone-region capacity and maintenance reserve

Each armed zone engine computes a segment reserve for the current memtable
flush, a bounded run-merge output, manifest snapshot installation, and a
minimal value-log victim rewrite. The reserve is additionally raised to the
planner-aligned cost of the cheapest admissible pressure plan (its index
output, commit headroom, and victim value-log staging) plus a small margin,
recomputed as runs grow at flush and merge commits, so intake can never
ratchet free space below the one plan that reclaims it. The enforced floor is
capped at half the
region, so it covers the computed maintenance requirement when that requirement
fits within half the region without making a small, nearly-empty zone
unwritable. The floor remains present while the memtable is empty and does not
depend on complete run accounting metadata. Compaction planning shrinks a
recurring or pressure merge to the smallest viable suffix when necessary. If
even that minimal plan exceeds the available floor, the engine reports terminal
region exhaustion. At the floor, pressure planning may cross
`VALS_MERGE_INPUT_CAP_BYTES` to include the minimal two-run suffix; the cap is a
throughput bound, not a reclaim-liveness boundary. Planning reports
`InsufficientHeadroom` rather than repeatedly failing an index snapshot with
`DeviceFull`. The corresponding
plan-failure counter is
`compaction_plan_failures`; warning logs are limited to one per zone per minute.

Overwrite-heavy zones accumulate cross-run LWW-shadowed garbage that per-run
accounting cannot see: an old version stays "live" in the run that references
it until a merge folds that run with its shadower, so `lsm_dead_fraction` and
per-run `value_seg_bytes` under-report dead space in multi-run engines. Once
merges have folded the shadow chains, an **aged-vlog
GC** rides pressure sweeps: it rewrites the run with the largest visible dead
value bytes (once that dead at least matches the run's own index size; at the
reserve floor any net gain qualifies), carrying the largest affordable
gainful victim set and freeing the victims at commit. At the floor this keeps
the engine live and retryable rather than terminally wedged, but its staging
budget there is small, so recovery from a mostly-dead region is slow.

Reclaiming this garbage is driven by a sampled **shadow estimator** plus a
**capped shadow fold** (task 0383; details in [algorithms.md](algorithms.md)).
When free space falls under a soft threshold, each run-set generation samples a
few index blocks per run and probes every newer run's resident bloom filter and
the memtable for the sampled keys. Bloom matches are corrected for the expected
false-positive mass of all newer filters (at ~100 runs most raw matches are false),
and the value-length-weighted shadowed fraction scaled by the run's live value bytes
estimates its cross-run garbage. The estimate feeds the same free-plus-dead
pressure gate as visible dead. Pressure planning then picks the window of adjacent
runs (at most `VALS_MERGE_INPUT_CAP_BYTES`, or one larger run) with the most
estimated garbage, when that gain clears both `VALS_SHADOW_FOLD_MIN_GAIN_SEGS` and
half the window's index size. The fold rewrites only the window, streaming every
newer run (and, for tombstone GC, every older run) read-only to drop window
entries that another run shadows; it never grows a run or reads more than the cap
of merge input, and it is paced except at the floor. Wholly dead value segments
become orphans reclaimed by the plan-time sweep, and partially dead ones become
visible to the aged-vlog GC. Each fold's realized reclaim (the drop in summed
per-run live value bytes) is compared against its prediction: a shortfall below
half disarms the trigger with exponential backoff, and even successful folds are
spaced by at least one output's worth of intake, so a wrong or stale estimate can
never become a per-sweep merge storm (the 2026-08-09 arm-4 failure mode). Before
the floor, a pressure plan with no gainful victims falls back to the recurring
merge policy rather than rewriting the newest runs' values for no gain.
`VALS_SHADOW_EST=0` disables estimation (the gate sees only visible
dead), `VALS_SHADOW_FOLD=0` keeps the estimator and its
`shadowed_value_bytes_est` gauge live for observe-only arms while pinning
the gate to visible dead and pressure suffix selection to the capped shape,
and `VALS_VLOG_GC=0` disables the GC retarget
and restores the pre-GC planner/predicate shapes. The estimator also
distinguishes reclaimable from legitimately full: the 2026-08-09 hardware
arm's write-once backlinks engine met its floor 99.8% unique-keyed (a
capacity wall), the gauge correctly read only the ~0.2% duplicate residue,
and zero shadow folds fired — a low-free state with no shadow garbage must
be answered with capacity or re-home (below), never with reclaim churn.

`free_segments` exports total free segments across local zone engines, while
`min_zone_free_segments` exposes the closest individual zone to its floor.
`used_segments` remains the corresponding node-wide used total. Alert on a
falling minimum before intake reaches the reserve. Ordinary reserve-floor
backpressure remains retryable `Response::Busy` and increments
`busy_rejections`. Pending flushes and in-flight maintenance remain Busy.

Reserve-floor backpressure is additionally *shaped* before the floor bites
(task 0092). The hard floor alone lets an unthrottled burst overshoot region
fill (the t0087 `tier=nosync` arm reached ~1018/1024 segments) and then
over-corrects into an all-Busy collapse oscillating with the reclaim cadence.
Each armed zone engine therefore sheds a growing fraction of record intake
across a pre-floor band: the band spans from the reserve floor up to floor
plus `admit_band_pct` (legacy alias `VALS_ADMIT_BAND_PCT`) percent of the region (default 6),
and the shed probability ramps quadratically from 0 at the top of the band to
1 at the floor, so admission degrades smoothly toward the reclaim rate
instead of collapsing. A shed is one Bernoulli draw per wire `Apply` or
`BatchApply` — a batch draws once, like a single write, so batch admission is
not inflated by its record count — answered with the same retryable
`Response::Busy`, counted in both `admit_shed_rejections` and
`busy_rejections`. Internal producers (anti-entropy heal applies, re-home
range-copy intake) are not shaped; they already back off on hard-floor
Backpressure. Shaping never allocates or holds anything and leaves the
maintenance reserve, pressure planning, and terminal region-exhaustion
semantics untouched — it only issues the same retryable rejection earlier —
so the 0085/0089 liveness guarantees are preserved. A floor-bound engine is
terminal when its minimal pressure plan cannot fit; a
single complete run whose best victim rewrite would free no net segments
(including the no-victim case) is also terminal. Floor-pressure planning is
net-gain aware: it rewrites the largest affordable set of mostly-dead value-log
segments and refuses a rewrite whose projected output consumes at least as many
segments as it frees, instead of re-running the same zero-net-gain plan
against the device. Unknown run
metadata remains Busy unless the independently computable minimal multi-run
plan itself cannot fit. Terminal writes receive
`Response::Err("zone region exhausted: grow region_mib, enable re-home, or reduce load")`
and increment `region_exhausted_rejections`; clients should not spin on this as
transient Busy.

A capacity-stalled engine whose memtable is *not* empty (writes stopped before
a flush could drain it, and the flush itself can no longer fit) reaches the
same terminal signal instead of looping Busy forever. The non-empty-memtable
path requires every retryable escape to be provably closed: at least three
consecutive infeasible pressure plans — each attempt having already run the
plan-time orphan sweep and the aged-vlog GC check against current state —
visible dead below the reserve gate, and a fresh shadow estimate (refreshed
every maintenance tick) under one segment, so the memtable demonstrably holds
no overwrites or tombstones a flush-plus-fold could reclaim. Any maintenance
progress (a plan, a commit, an orphan reclaim, or an aborted job returning its
segments) resets the failure streak. With `VALS_SHADOW_EST=0` the estimator
cannot discriminate reclaimable from live, so the non-empty path never fires
and the engine stays Busy.

Recovery requires adding capacity or moving the populated range: increase the
zone `region_mib` for a newly provisioned layout, or use confirmed automatic
re-home (default-on; ensure `rehome_enabled` (legacy alias `VALS_PD_REHOME_ENABLED`) was not set to false). A
metadata-only split of a populated
range inherits its placement and therefore does not escape a full replica pair;
canonical splits spread placement only while ranges are empty. See
[zones.md](zones.md) before changing split or re-home policy. If neither resize
nor re-home is available, stop intake and restore/migrate onto a larger region;
restarting the same full layout is not a recovery action.

`flush_max_bytes` (legacy alias `VALS_FLUSH_MAX_BYTES`) is a per-node aggregate memtable budget (default 4 GiB),
not a per-zone-engine limit. The node divides it by the live engine count when
selecting each engine's byte-triggered flush threshold, so adding shards or
zones does not multiply the configured cap.

Capacity planning should allow about 300–400 bytes per resident key (about 350
bytes/key as a starting estimate), plus memtables, maintenance work, and runtime
overhead. Linux `MemTotal` includes memory reserved later for SPDK hugepages,
while those hugepages are outside `/proc/self/statm` RSS. Therefore operators
using SPDK or other large reservations should set `ram_budget_bytes`
(legacy alias `VALS_RAM_BUDGET_BYTES`) explicitly below the actually available process-memory ceiling rather than rely
on the 70% default.

The 0152 replay exposed two non-intake growth modes. Local file-device pressure
merge measurements (`pressure_merge_resident_measurement`) show that the merge
working set scales with input entries: the output bloom is allocated for the
sum of input entry counts, GC retains every dropped tombstone key until commit,
and both the streaming builder index and `read_run_parts` output scale with the
output block count. At 10k/40k/160k keys the measured bloom storage was
30,016/120,000/480,000 bytes, retained dropped-tombstone storage was
305,000/1,220,000/4,880,000 bytes, and reopened run-part storage was
33,565/133,725/534,482 bytes. One-millisecond sampled peak RSS deltas were
2,703,360/4,866,048/2,162,688 bytes; the largest-scale delta is muted by arena
reuse from the earlier arms, while each owning component and its record count
grows exactly 4x per scale step. Pressure plans had selected every run and
therefore made all of these terms unbounded. Recurring merges now use only the
tiered/index-only policy: they select a similar-sized newest suffix under
`VALS_MERGE_INPUT_CAP_BYTES`. Since 0381 the run-count valve is bounded the same way.
It merges the cheapest adjacent window under the cap in the active tail, and never
takes a frozen run (see [algorithms.md](algorithms.md)). Before 0381 it forced the
two newest runs past the cap, and SOAK5B-V3-R2 saw 15.7 GB of in-flight merge input
for 2 merges. Pressure and lone-GC selection use the same cap;
lone GC defers unless the capped input reaches the bottommost run. Atomic runs
are never pulled in past the cap, so low-overlap run-count folding can stall at
the cap boundary by design. `VALS_MERGE_INPUT_CAP_BYTES` is the
memory-versus-reclaim-depth dial. Under pressure the engine first sweeps orphan
value-log segments, then may plan aged-vlog victim GC; a one-run rewrite proceeds
only when victim selection finds reclaimable space. Otherwise pressure planning
defers instead of repeatedly rewriting the same run. Explicit cleanup `forget`
planning remains bottommost and uncapped because skipping older runs could leave
requested keys behind.

Streaming cooperative seeding merges sorted version sources directly into
placement digest groups, without a per-engine-key winner accumulator. Its working
set includes a memtable snapshot, bounded version-stream windows (v3/v4 sidecar
windows, or v5 windows of whole data blocks ≤ 256 KiB unless one block is larger,
decoded one entry at a time) and merge heads, and materialized legacy sidecar runs. Placement-generation changes restart
the merge rather than retaining a canonical winner fold. An unpublished seed is
discarded when RSS plus swap reaches the budget and restarted after headroom
returns. Publication releases seed staging immediately; there is no separate
published-winner disposal phase. The historical hardware evidence that motivated
the RAM bounds is in
`bench/replay-lab/run-20260807T195353Z-rambudget40g/FINDINGS.md`.

The responder side of anti-entropy bucket-digest paging is bounded to
O(page) (task 0154): v3/v4 run sidecars are read through streaming cursors
(256 KiB ranged windows, CRC verified at cursor EOF) and v5 runs through
block cursors (task 0365: whole-block windows ≤ 256 KiB starting at the block
holding the resume key, each block CRC-checked before use; plan bound
2 × max(256 KiB, largest block) + largest block per run) feeding a lazy k-way
LWW merge, the memtable source is page-bounded at plan time, and the shard
thread yields between read windows. The `ae_digest_serve_bytes` gauge
attributes the serving working set. Legacy v1/v2 sidecars still fully
materialize. Before 0154 this path materialized O(keys × runs) per page,
stayed admitted over the RAM budget (read-side), and OOM-killed a budgeted
node in the 0152 run-2 replay
(`bench/replay-lab/run-20260807T215938Z-rambudget40g-0153/FINDINGS.md`);
the 0152 run-3 hardware validation confirmed the bound (digest serving peaked
at tens of MB through replay and far-behind-peer healing, zero OOM kills:
`bench/replay-lab/run-20260808T010817Z-rambudget40g-0154/FINDINGS.md`).

## Server-side availability under resource exhaustion

Backpressure bounds *client* memory; the server side has a matching guarantee
that a **resource pinch never turns into lost availability**. Two failure modes,
both isolated so a squeeze degrades gracefully instead of going node-wide:

- **A full zone region is retryable, not fatal** (task 0107). A zone whose
  region fills with live data returns the retryable `Busy` to writers of *that*
  zone; reads on it, and all ops on other zones/shards, and the metrics
  endpoint keep serving. Fault isolation is per-zone/per-shard, not per-process
  (see [`zones.md`](zones.md)).
- **A transient `accept(2)` error never kills a listener** (task 0109). Under fd
  or kernel-memory pressure (`EMFILE`/`ENFILE`/`ENOMEM`/`ENOBUFS`), an aborted
  half-open connection (`ECONNABORTED`), or an interrupted syscall (`EINTR`), a
  shard's accept loop logs (rate-limited), backs off briefly (bounded; a
  time-based sleep, since a timerfd/eventfd backoff would itself need an fd
  `EMFILE` denies), and **retries** — the listener stays up and self-heals once
  the pressure clears. Only a genuinely fatal error (e.g. `EBADF`, the listener
  gone) exits the loop. The `vals_accept_transient_errors` counter surfaces a
  pinch; a sustained climb says raise `RLIMIT_NOFILE` / investigate. Historically
  *any* accept error permanently exited the loop, dropping the owned listener
  socket so the port went to connection-refused for the process lifetime — the
  "husk" state (all shard listeners + the metrics endpoint refusing) the
  2026-07-31 soak autopsied. The metrics endpoint's (separate, blocking) accept
  loop gets the same bounded-backoff treatment.

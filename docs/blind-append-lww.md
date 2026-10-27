# Blind-append LWW

Status (2026-08-17): **supported in the latency deployment profile**. The daemon's
`profile = latency` couples task 0175's blind writer and task 0183's collision-safe compact winner
directory. `profile = capacity` retains resolving `Apply`/`BatchApply` plus the out-of-core index and
is the explicit default. The directory can prove the exact old winner without a run read, so a
dominated candidate can return optimistic `Won` without appending and a new winner updates Merkle
state exactly. It passed its mature-corpus gate: tuned strong reads were 2.51× matched control with
zero run probes, p99 improved, and mature overwrites were 1.93× control. Latency remains opt-in
because the measured 62.9 RSS bytes/physical key and synchronous O(dataset) open rebuild are
explicit operator costs. See `deploy.md` for profile operations and rollback.

## Decision and scope

An unconditional replicated write is an LWW **candidate**, not a conditional update. Once a
replica has passed admission, clock-skew, and tombstone-GC-horizon checks, it may append that
candidate without first reading a run block to discover the current winner. The replica may
acknowledge the append even when a higher `Version` already stored on that replica will eventually
beat it.

Consequently, the reply to an unconditional put or delete no longer proves that the candidate won
LWW. Reads, scans, anti-entropy, recovery, flush, and compaction must all agree that the logical
value of a key is the candidate with maximum `Version` over every live physical source. CAS keeps
an authoritative precondition read; it is not converted into blind compare-and-swap. As before,
leaderless cluster CAS is best-effort rather than linearizable.

Reader, recovery, and anti-entropy prerequisites remain enabled in both supported profiles. This
design does not authorize a shortcut in which stale reads are tolerated until compaction.

## Existing path inventory

The distinction that matters is whether the current winner is resident in the mutable index or
only in an immutable run.

| Path | Current behavior | Old-winner dependency |
| --- | --- | --- |
| Single replica `Apply` | In `profile = latency`, `VersionedStore::apply_blind_at` checks skew and the live-value floor, observes progress, appends without constructing a run probe, and returns optimistic `Won`. Capacity uses `apply_stage`/`serve_apply` and resolves the old winner. | The blind path has no old-winner dependency. A capacity-profile bloom-positive flushed key still incurs an asynchronous run-block read and can return authoritative `Lost`. |
| `BatchApply` | In latency, `serve_batch_apply` appends entries in request order without run probes and then performs the existing optional persistence barrier. Capacity stages every entry, overlaps up to 64 cold probes, and revalidates generations. | Blind entries are independently accepted, including repeated keys. In both modes, earlier entries can have landed if a later append or final sync fails. |
| Direct local `put` / `delete` | `stamp_and_store` calls `current_version`, observes it in the HLC, generates a new version, and updates the leaf digest with old and new versions. | It resolves a cold winner both to stamp above it and to update the Merkle leaf. These APIs are not the coordinator's normal replicated-write path, because replicated requests already carry a version. |
| Local `cas` | `cas_at` resolves the current version, compares it with `expected`, stamps, and stores without an intervening await. Cluster `Coordinator::cas` separately performs a quorum read followed by an ordinary majority write. | The precondition is inherently read-dependent. Local no-await compare/store atomicity must remain. Cluster CAS remains subject to its documented read/write race. |
| Point read / digest | `Engine::resolve` returns a mutable-index entry immediately, otherwise `lookup_runs` chooses maximum version. The staged network read similarly returns a mutable entry immediately; its run resolver currently exits on the first matching run. | Immediate mutable/newest-run precedence is valid only because the current writer refuses to insert known losers. It is not valid after blind append. |
| Scan | `scan_winners_page` merges mutable and run cursors by key and chooses maximum version. `VersionedStore::scan_page` decodes the winning put/tombstone. | Already has the required cross-source LWW rule, subject to taking a consistent source snapshot. |
| Digest pages / anti-entropy values | `version_digest_page` merges mutable and run version streams (v3/v4 sidecars; v5 data blocks, task 0365) by key using maximum version. `SyncValues` and read repair use ordinary logical reads/applies. | The page merge is already winner-oriented. Point digest and value reads need the point-read correction. Repair applies currently use authoritative `Won`/`Lost`, though both are treated as successful transport acks. |
| `VersionIndex` Merkle leaves | The cache XORs out `old` and XORs in `new` in `apply_leaf`. Startup seeding scans mutable/run version metadata and takes maximum version per key. Concurrent writes are replayed through a seed journal containing both old and new. | An unresolved cold write cannot safely mutate the cached XOR because its old logical winner is unknown. Guessing either absent or the new candidate can create false Merkle equality. |
| Flush | The mutable index contains one entry per key. Flush writes those entries to a new L0 run, publishes the manifest, then removes the corresponding mutable entries. | It assumes the mutable entry is the global winner. With blind append it is only the mutable-source winner and can legitimately lose to another run. |
| Recurring merge / compaction | `MergeStream` groups equal keys from selected runs and emits maximum `version`; loser copies are discarded. Tombstone GC can drop an eligible selected winner. | Version-based selection is already correct within the selected inputs. GC additionally needs proof that no unselected older value can surface after the tombstone is removed. |
| Recovery | Manifest runs are opened and the value-log tail is replayed into the mutable index. Tail collisions currently choose greatest physical `seq`; recovered Merkle seeding later takes maximum version across mutable and runs. | `seq` is not LWW order. Blind append requires tail collisions and tail-vs-run reads to use `Version`, while retaining `seq` only for log/watermark bookkeeping. |
| `gc_grace` admission | If no current version exists, `decide_and_store` rejects a live candidate below `min(wall_ms, high_water_ms) - gc_grace_ms`. Tombstones are admitted, except that one below the same floor for a key with no local version is acknowledged without being stored (0499); a blind apply does this only when the winner directory proves the key absent. | The current code reads the winner to decide whether the key is absent. Blind append needs a conservative read-free form of the same resurrection barrier. |

Read repair, hinted handoff, mirror/re-home writes, and anti-entropy all eventually enter the same
unconditional `Apply` contract. They are not exceptions that may restore an authoritative
`Won`/`Lost` result.

## Reply and API contract

### Unconditional replicated writes

For a feature-enabled `Apply` or each feature-enabled `BatchApply` entry:

1. Validate route/range ownership and ordinary RAM/space admission.
2. Reject a version beyond the local clock-skew bound without advancing the HLC or appending it.
3. Apply the read-free live-value GC floor described below. Tombstones bypass that floor.
4. Observe an admitted version in the HLC/data-progress watermark.
5. Append the candidate and update the mutable-source winner. A persistence-requested operation is
   not acknowledged until its existing sync barrier succeeds.
6. Return an **accepted** result. This means the replica owns a candidate sufficient for LWW
   convergence; it does not mean that candidate is the replica's logical winner.

The initial protocol preserves the existing response encoding. `Applied::Won` is reused to mean
"accepted" on the blind path, `Applied::Rejected` remains a non-ack, and `Applied::Lost(version)`
is emitted only by the legacy resolving path. Coordinators already count both `Won` and `Lost` as
acks, so no quorum-counting wire change is needed. The semantic change is nevertheless visible to
direct transport users: after enabling the feature they must not interpret `Won` as proof of the
winner or expect `Lost` to disclose it. A future negotiated protocol may rename this result to
`Accepted`; adding a new wire tag is not required for the first rollout.

A quorum success therefore means that the candidate was admitted by `W` replicas (and persisted
there when durable persistence was requested). It does not promise that a lower-version candidate
overrides an already present higher-version write. The normal LWW meaning of "latest" remains
maximum `Version`, not last request arrival or last acknowledgement.

A safe optimization may avoid writing an exact duplicate, or a candidate already dominated by a
proven mutable-source winner. Task 0183's optional full winner directory extends that proof to
immutable runs without a device read. It still returns the optimistic accepted result because the
retained candidate dominates it. Implementations must not use an unresolved run as an assumed
absence.

### Deletes, retries, and batches

Deletes are blind tombstone candidates and use the same accepted semantics. A lower-version delete
may lose to a higher-version value; a higher-version tombstone hides every lower version on all
read surfaces. Tombstones are never rejected by the live-value GC floor.

Retries reuse the same version and payload. Duplicate physical records are harmless and may be
coalesced in memory or at merge. The protocol invariant is that `(key, Version)` identifies one
immutable mutation, including put versus tombstone and the value bytes. Reusing one version for
different mutations is invalid; the storage engine is not required to choose between conflicting
payloads with an equal version.

A batch preserves its current per-entry results and persistence behavior. Every admitted entry is
an independent candidate, including repeated keys in one batch; the mutable winner for such a key
is their maximum version. A batch-level error can leave earlier appends present, just as today, but
no failed or un-synced entry is counted as a durable ack. Read, device, and rebuild failures are
never translated to "absent" or to a successful append.

### Local writes and CAS

Direct `VersionedStore::put` and `delete` can become blind only after the store's HLC has been
initialized above the maximum recovered HLC. Lazy-open stores must either complete that one-time
version seed before their first direct local mutation or retain the old resolved stamping path.
Once initialized, a direct put/delete can call `Hlc::now`, append without a per-key read, and return
the generated candidate version; that returned version is not proof it won. This is not on the
critical replicated `Apply` rollout path and must not delay it.

Local `cas_at` continues to resolve the logical maximum over mutable state and every possible run,
compare with `expected`, and revalidate immediately before append with no await in between. If an
asynchronous device read is used, manifest/mutable generations are rechecked and the operation
retries before the no-await compare/store section. `CasResult::Mismatch` remains authoritative for
the local snapshot it checked. `CasResult::Applied` means the checked local append occurred.

The coordinator CAS contract is unchanged: its quorum precondition read remains authoritative for
that read, but the subsequent leaderless write is not atomic with the read and can lose to a
concurrent greater version. Blind append must not weaken or overstate that existing contract.

### Failure behavior

Skew rejection, GC-floor rejection, admission backpressure, append failure, and requested-sync
failure are non-acks. An append error may leave a CRC-valid record only if the value-log API already
defines it as committed; recovery must treat any such record using the same LWW rule. A run-read
failure no longer blocks an unconditional blind append, because that read is absent, but it still
fails point reads, CAS, dirty-Merkle rebuilds, and any operation requiring an authoritative winner.

## Storage and read invariants

The implementation must preserve these invariants at every manifest generation:

1. The logical state of key `k` is `argmax Version` over the mutable winner and every live run entry
   for `k`. Physical recency, run level, run-list order, and value-log `seq` never break a tie in
   place of `Version`.
2. Every point value read, point digest, scan, anti-entropy digest page, recovery seed, and
   compaction observes the same winner set. A winning tombstone is returned internally as a
   versioned absence and hidden only at the public value boundary.
3. The mutable index retains maximum `Version` among candidates represented by the current tail,
   not the last appended `seq`. A put below the mutable winner's version is not logged (0466): a
   dead record past the flush watermark could outlive the winner's log segment and resurface on
   replay after the winner is forgotten or collected.
4. Operations use one consistent snapshot of mutable tables plus the run manifest. A new run is
   visible before its flushed mutable source is removed. If a staged device read crosses a manifest
   or mutable generation change, it retries rather than combining incompatible source sets.
5. Equal versions denote identical mutations. This makes retry deduplication and arbitrary
   equal-version source selection safe.
6. A loser may be discarded only after another retained candidate for the same key is known to
   dominate it. Removing a winning tombstone under `gc_grace` additionally removes or proves absent
   every older physical candidate that could otherwise resurface.

### Point reads and digests

A point probe must return its mutable candidate and all bloom/range-positive run locators rather
than treating either as final. Run blocks may be read concurrently, but every matching entry is
considered and the greatest version wins. The value-log read is planned only for that winning
entry. "Newest run containing the key" and early exit on first hit are not valid optimizations
unless run metadata proves that all remaining runs have a lower version bound; no such proof exists
today.

The same resolver serves value reads, version-only digests, read repair, and the authoritative part
of CAS. It may avoid run I/O only on a true range/bloom-negative result or with a future sound
per-run version bound. A block error is propagated as an error, never converted to absence.

### Scans and digest pages

Sorted scans already merge equal keys by maximum version and should retain that rule. Async and
paged scans must apply it across page/refill boundaries and all overlapping runs. Page truncation is
computed after duplicate-key resolution so one logical key cannot appear twice or resume at a
loser.

Version digest pages similarly stream mutable and run version streams in key order and emit one maximum
version per key. They must not XOR or return every physical version. These metadata paths need no
value-log read.

### Flush

Flush snapshots the mutable-source winner for every included key. It may write a run entry that is
older than a candidate in another run; this is expected and safe because readers compare all
sources. Manifest publication remains new-before-old, and the flushed mutable entry is removed only
if it is the exact entry captured by the snapshot. A concurrent newer candidate stays mutable, and a
paged flush captures only entries at or below its `covered_seq` watermark (0466).

Superseded tail records not selected by the mutable index become reclaimable only after the flush
run and manifest safely represent the mutable winner and the covered tail epoch. Segment-liveness
accounting must continue to protect every value pointer retained by any run.

### Merge and compaction

For every key represented in a merge's selected inputs, the output is their maximum version. It is
safe to drop lower selected candidates even if an unselected run has a still-higher candidate: the
selected maximum remains dominated but preserves dominance over everything discarded. Merge output
must retain the original LWW version even if rewriting assigns a fresh physical sequence number.

Tombstone GC is stricter. An aged tombstone may be removed only when it is the global winner and the
compaction covers every run/mutable overlap that can contain an older candidate for that key, or
when equivalent metadata proves those candidates absent. The commit removes the winner tombstone
and all covered older values atomically in the new manifest. A partial tier merge must retain an
eligible tombstone if an unselected older value might remain. Losing tombstones can be discarded as
ordinary losers once a retained greater candidate is proven.

### Recovery

Recovery opens one valid manifest generation and replays the uncovered value-log tail. Tail
collisions are selected by maximum `Version`, not maximum `seq`; `seq` remains the physical replay
and manifest-watermark order only. Reads then compare that mutable tail winner with all manifest
runs. Recovery must not materialize a tail candidate as an unconditional global winner.

Before serving anti-entropy tree equality after restart, the node seeds logical winners by maximum
version across recovered mutable metadata and every run version stream. This seed also supplies the
maximum recovered HLC needed by direct local version generation. A crash loses only the Merkle
cache's dirty bookkeeping, not correctness: the cache is rebuilt from physical candidates before
it can claim equality.

## Anti-entropy Merkle strategy

`VersionIndex` represents logical `(key, winning Version)` pairs, not append history. XORing the
new candidate into a clean leaf without XORing the actual old winner is incorrect: two nodes can
then report equal tree nodes while holding different logical winners. Blind append therefore adds
explicit per-leaf cleanliness.

### Clean and dirty leaves

Each finest-granularity bucket has a state containing a mutation generation and either:

- **clean**: exact XOR digest and logical-key count for that bucket, partitioned by placement group;
  or
- **dirty**: no digest may be used to establish equality.

An append increments the bucket mutation generation. If there are no possible run candidates, the
old mutable winner is exact and the cache can apply an ordinary old/new delta. A candidate known to
be no greater than an existing mutable candidate cannot change the global winner, even if runs are
possible, and also leaves a clean digest unchanged. Every other write with a possible cold
candidate marks the bucket dirty. Dirty state is monotone until an exact rebuild publishes; later
writes do not attempt guessed XOR deltas.

Startup seeding and its concurrent-write journal must understand dirty operations. A journal entry
that lacks an exact old winner dirties its bucket instead of replaying `old = None`. Placement-map
changes continue to invalidate/reproject affected group caches.

### Exact rebuild and publication

Before `SyncTree` or a scoped leaf request uses a dirty bucket, the node rebuilds it from a
consistent mutable-plus-manifest snapshot:

1. Stream all mutable and run version-stream entries in that bucket in key order.
2. Collapse equal keys by maximum version.
3. Recompute the bucket's XOR and key count, routing each logical key to its current placement
   group.
4. Publish only if both the bucket mutation generation and manifest/placement generations still
   match the captured snapshot. Otherwise discard and retry.

Rebuild is cooperative, page-bounded, and may be shared by concurrent requests. If it cannot
finish because of I/O, cancellation, RAM shedding, or continuous generation churn, the tree RPC
returns a retriable error/Busy response. It must never return the previous digest, an all-zero
stand-in, or a common "dirty" sentinel: any of those can create false equality. The usual digest
hash collision probability is accepted; dirty bookkeeping must not introduce a deterministic
false-equality case.

`SyncBucketDigests` remains exact because it streams and max-merges per-key run version streams.
`SyncValues` uses the corrected point resolver. Thus a mismatch can descend and repair while dirty
leaf rebuild is pending, but no tree comparison can skip a divergent bucket due to stale cached
state.

Flush never clears dirtiness merely because a candidate moved into L0. Merge/compaction commits may
schedule or accelerate rebuilds with the winners they exposed, but may mark a bucket clean only if
they covered every physical source needed to reconstruct that complete bucket. In the general
case, the post-commit run-stream rebuild is the authority. Tombstone retirement updates a clean leaf
only if the retired version is its proven logical winner; otherwise the leaf remains/becomes dirty.

The dirty map itself need not be persisted. On crash the entire in-memory `VersionIndex` is
unseeded, and recovery performs an exact max-version seed before serving tree digests. This rule is
what makes a crash between candidate append and dirty-leaf reconciliation safe.

## Read-free `gc_grace` resurrection floor

Blind append cannot ask whether a key is absent before admitting a stale live candidate. With
`gc_grace_ms > 0`, it therefore applies the existing monotone horizon conservatively to **every
unconditional live candidate**:

```text
effective_now = min(local_wall_ms, data_high_water_ms)
reject live candidate when candidate.physical_ms < effective_now - gc_grace_ms
```

The implementation retains the greatest established cutoff in RAM if either clock input can
regress. Before a compaction commit collects tombstones at a new greatest cutoff, that cutoff (or
the data high-water mark from which it is derived) must be synchronously recorded in the node's
durable liveness metadata. Open initializes the floor from the maximum of recovered data and that
metadata; an unreadable/missing floor on a node that previously enabled GC is a fence-and-resync
condition, not permission to reset it. This puts persistence on the comparatively rare GC-cutoff
advance, not on every write, and requires no per-key tombstone memory or write-path device read.
Standalone stores that enable GC need an equivalent durable scalar before they may collect a
tombstone.

Rejecting an old live retry that is already present is conservative but safe: the replica returns a
non-ack even though its logical state may already contain that version. This behavior is necessary
because the read-free path cannot distinguish that case from resurrection after every tombstone
copy was collected. Coordinators and repair loops may retry after clocks/data progress change, but
must not reinterpret the rejection as absence. Tombstone candidates remain admissible at every
age because they cannot resurrect a value.

The floor complements, rather than replaces, compaction coverage. Compaction must remove all older
physical values when collecting a winning tombstone; the floor prevents a delayed external copy
from being appended later. The existing liveness fence/wipe rule for replicas outside the grace
window remains unchanged.

## Implementation and rollout status

1. **Completed — universal version-based resolution.** Mutable insertion, synchronous and staged
   reads/digests, scans, replay, recovery, and merges resolve `argmax(Version)` with generation
   checks where awaits cross mutable or manifest changes.
2. **Completed — tombstone compaction hardening.** Full-overlap collection and the durable scalar
   live-value floor prevent older physical values or delayed live retries from resurrecting data.
3. **Completed — dirty Merkle leaves.** Per-bucket generations/counts, exact run-stream rebuild,
   seed-journal operations, and fail-closed tree serving shipped with the required lifecycle tests.
4. **Completed — blind unconditional apply.** `profile = latency` selects blind single and batch
   apply; wire `Won` means accepted, while skew/admission/append/sync failures retain their non-ack
   boundaries and CAS remains authoritative.
5. **Completed — direct local operations remain resolved.** They do not weaken the blind replicated
   path's HLC, LWW, or tombstone rules.
6. **Retired — default-on flag promotion.** Task 0194 instead productized two supported daemon
   profiles: default `capacity` keeps the lower-RAM resolving writer, while `latency` couples blind
   apply with the winner directory. The mixed-profile SPDK soak gate passed on 2026-10-22
   (`bench/0194-profile-soak/FINDINGS.md`).

No on-disk run or value-log format change is required: both already store `Version`. Wire bytes are
also unchanged initially. The semantic compatibility implication is real, however. Clients that
inspect replica-level `Applied::Won/Lost` must opt in knowingly; cluster coordinators that only
count either as an ack are compatible. Mixed replicas converge because both modes store/read by
LWW, although legacy replicas still pay the old probe and can return `Lost`.

Disabling the feature stops creating new unresolved losers but does not remove existing ones.
Corrected argmax readers and recovery must remain enabled. Downgrading to an older binary that uses
mutable/newest-run precedence is unsafe until an offline or verified full compaction has
canonicalized one winner per key and no uncovered tail remains. Rollback procedures must state this
explicitly.

## Observability

Retain `apply_run_reads`, `batch_apply_run_reads`, `apply_probe_runs`, and `apply_probe_blocks`.
They should be zero for feature-enabled unconditional writes and nonzero only for legacy/CAS
resolution. Add counters/gauges for:

- blind candidates appended, exact duplicates, and mutable-dominated candidates;
- blind candidates rejected by skew, GC floor, admission, append, and sync stage;
- dirty Merkle buckets, dirties avoided by a proven mutable winner, rebuild requests/retries/errors,
  rebuild metadata bytes/blocks, and oldest dirty age;
- point-read probe runs/blocks and the number of physical candidates collapsed;
- merge loser candidates dropped, tail replay collisions resolved by version, and tombstone-GC
  deferrals caused by incomplete overlap coverage.

Track end-to-end write latency and throughput separately for inserts and flushed-key overwrites.
Moving I/O to reads/AE is not free, so also track point-read p50/p99, AE sweep duration and bytes,
dirty backlog, run count, write amplification, and value-log space amplification.

## Validation

### Unit and property tests

- Place permutations of older/newer puts and tombstones in mutable state, several overlapping runs,
  and the replay tail. Point read, digest, scan, digest page, and recovery must return the same
  maximum version independent of append, flush, run, and merge order.
- Append a lower-version candidate over a flushed higher-version put and tombstone. It is
  optimistically accepted, incurs zero apply probe blocks, and never becomes visible.
- Append a higher-version candidate over a flushed value. It is immediately visible before flush,
  after flush, after partial merges, and after restart.
- Exercise repeated keys and out-of-order versions inside one batch, exact retries, and invalid
  equal-version/different-payload detection where feasible.
- Property-test a reference map keyed by `max Version` against random put/delete, flush, partial
  merge, value-log rewrite, GC, crash/reopen, and read/scan sequences.
- Inject manifest and mutable generation changes during staged point reads and CAS. Reads retry;
  CAS never stores after a stale precondition snapshot.

### Anti-entropy and GC tests

- A cold unresolved write dirties its bucket before any tree response. Two divergent nodes with
  dirty buckets can never compare equal without an exact rebuild.
- Race rebuild with append, flush commit, compaction commit, placement change, cancellation, and
  crash. Stale generations never publish and errors fail closed.
- Restart after a synced blind append but before rebuild; startup seed includes the candidate and
  serves the correct logical digest.
- Reconcile nodes containing the same winner but different loser histories; logical trees compare
  equal. Reconcile nodes with different winners; they mismatch and repair to the maximum.
- Advance the data watermark past grace, collect a winning tombstone with older values spread over
  selected and unselected runs, and prove no value resurfaces. A below-floor live apply is rejected
  without a run read; an equally old tombstone remains admissible.

### Integration, compatibility, and performance

- Run single and batch network tests with capacity/latency profiles mixed across replicas. Quorum, skew
  partial-commit reporting, hinted handoff, read repair, anti-entropy, re-home mirroring, sync, and
  restart durability retain their existing success/failure boundaries.
- Verify an old client can decode the optimistic `Won`, while documenting that it cannot infer an
  authoritative winner. Audit all in-tree consumers for assumptions about `Lost(version)`.
- Crash at value-log append, sync, flush-manifest publication, merge-manifest publication, and
  Merkle rebuild publication boundaries.
- Re-run the 0145 overwrite benchmark. For feature-enabled unconditional writes,
  `apply_run_reads / records_written`, `batch_apply_run_reads / records_written`, and
  `apply_probe_blocks / records_written` must approach zero for flushed overwrites. Report insert
  and overwrite throughput distributions, not one pass; the target is to remove the attributable
  insert/overwrite gap without regressions in read p99, AE convergence, or space stability.

Task 0167's write path and reader/recovery prerequisites are shipped. Task 0194 exposes it only
through `profile = latency`; its mixed-profile SPDK soak gate passed on 2026-10-22
(`bench/0194-profile-soak/FINDINGS.md`).

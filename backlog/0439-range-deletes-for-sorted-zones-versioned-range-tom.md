---
id: "0439"
title: Range deletes for sorted zones (versioned range tombstones)
status: done
priority: 3
created: "2026-10-14"
updated: "2026-10-15"
depends_on:
    - "0443"
    - "0444"
    - "0445"
    - "0446"
    - "0447"
    - "0448"
    - "0449"
spec_refs:
    - docs/range-deletes.md
---

## Description

## Description
## Why
Sorted zones can only delete one key at a time. Deleting a prefix or span costs one tombstone per key, and those tombstones stay visible to every scan: replica `Scan` pages return them and only the coordinator's LWW merge hides them (docs/algorithms.md §6: "tombstones count toward a page but not toward the limit"). With `gc_grace = 0` (the default) they are never collected. So any workload that consumes and deletes in key order (queues, logs, time-bucket retention) pays scan cost proportional to deleted history, not to live data. Range deletes are also the general tool for prefix or tenant teardown, time-bucket expiry, and dropping derived data before a rebuild without a zone per bucket (which hits the per-zone engine overhead in docs/zones.md "Known boundary").

Prior art: RocksDB `DeleteRange`, Cassandra range tombstones. Both have a history of read-path and compaction pitfalls, so design first.

## Proposed semantics (to confirm in the design note)
- `Coordinator::delete_range_in(zone, lo, hi, tier)` (plus a `_with_persistence` variant), sorted zones only; hash zones return `NotSorted`. The coordinator stamps one `Version` V and fans out to the replicas of every range overlapping `[lo, hi)`, clipped per range as scans are.
- A key k in `[lo, hi)` whose winning version is older than V reads as absent. Writes newer than V win as usual. A straggling write older than V that arrives after the range tombstone loses: the apply path's store-if-newer must consult covering range tombstones, including keys the replica has never seen.
- A range tombstone is a durable, versioned object with the same LWW and `gc_grace` rules as a point tombstone. Covered entries may be physically dropped only while the range tombstone that covers them is retained, or under the same resurrection fence point tombstones use.

## Design questions the note must settle
1. **Engine representation.** Where range tombstones live (memtable, runs, manifest, or a small per-engine interval set), how they survive reopen and index-snapshot recovery, and how point lookups check them cheaply. The no-range-tombstone path must stay effectively free.
2. **Scans.** A replica should seek past fully covered spans rather than return covered entries; that is the main payoff. For R > 1 correctness, scan pages must carry the range tombstones overlapping the page window, so the coordinator merge hides keys a lagging replica still holds. This interacts with the min-coverage paging rule.
3. **Point reads, digests, read-repair.** How a read reply expresses "deleted by range tombstone V", and what read-repair writes: propagate the range tombstone, or a point tombstone.
4. **Anti-entropy.** The Merkle tree is per key. Define the digest over logical state (covered entries excluded), or apply the covering check on pull, so replicas that have and have not yet compacted covered entries don't diverge forever or re-pull dead data. Range tombstones themselves need reconciliation (a separate small set, digested or exchanged whole). Also check how VersionIndex leaf groups and seeding are affected.
5. **Range lifecycle.** Splits, merges and rehome copy/mirror must clip and carry range tombstones for the moved span; cleanup handoff likewise.
6. **Accounting.** Logical live-entry counts per range (they drive auto-split and PD stats) must reflect covered entries, exactly or with a documented lag; the exact-accounting invariants (0291 etc.) are a known hard area.
7. **GC and compaction.** Merges and vlog reclaim drop covered entries. Range-tombstone GC under `gc_grace` and the liveness lease. The latency profile's winner directory and the read-free blind-append path (docs/blind-append-lww.md) must not report a covered key as live.
8. **Formats.** Wire op or `BatchApply` entry kind with capability/version negotiation, the on-disk record/run format version bump, and the `valsd dump` record type, all documented in docs/formats.md.

## Acceptance
- A design note (docs/range-deletes.md, linked from ARCHITECTURE.md), reviewed before implementation, covering the questions above.
- Rust client API and wire support. The Go client (vals-examples/valsgo) gets a follow-up task.
- Kill-the-fix tests:
  - older writes hidden and newer writes win;
  - a straggling older write after the range delete loses;
  - an R > 1 scan where one replica missed the range delete still hides the keys;
  - anti-entropy converges without resurrection and without endless re-pull;
  - crash/reopen and index-snapshot recovery preserve it;
  - split, merge and rehome across a range-tombstone boundary;
  - `gc_grace` collection of the range tombstone and its covered entries;
  - dump/restore round trip.
- **Head-of-queue scan cost:** a scan starting below a range-deleted span of N entries takes a bounded number of page rounds and replica work independent of N, shown by counters both before and after compaction.
- Covered entries' space is reclaimed by compaction.
- **Hot-path neutrality:** an A/B with zero range tombstones shows no write or point-read regression (local tier first per docs/validation.md; box confirmation if the shard apply or read path changed).


## Acceptance criteria

## Outcome

Range deletes complete via slices 0443–0449. Acceptance re-checked: design note docs/range-deletes.md (0443); Rust API and wire (0446), Go and valsdump in follow-up 0468; kill-the-fix tests for older-hidden/newer-wins, stragglers, R>1 lagging-replica scans, AE convergence, crash/reopen and manifest+tail recovery, split/merge/re-home/cleanup across RTs, gc_grace collection (now also asserts a live straggler below the cutoff is Rejected after collection), dump/restore. Head-of-queue cost is independent of N before and after compaction. Covered space is reclaimed. The zero-RT hot-path A/B passes (0449). The vals-store, vals-client, vals-cluster and vals-server suites passed twice at HEAD.

Commit: test+chore(0439): close range-delete umbrella after acceptance re-check; assert a live straggler below the GC cutoff is rejected after RT collection

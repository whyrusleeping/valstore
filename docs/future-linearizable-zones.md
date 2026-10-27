# Future work: linearizable zones

**Status: future work — a captured design sketch, not committed and not built.**
The default data path is leaderless LWW (see [`ARCHITECTURE.md`](../ARCHITECTURE.md)),
where compare-and-swap is best-effort only (it can detect contention but cannot
provide mutual exclusion — see the CAS section of the consistency model). This
document records *how* we would offer real linearizable CAS for the minority of
keys that need it, so the idea isn't lost. It is parked, not planned.

## What it provides

A **linearizable zone**: an opt-in zone mode whose writes go through a per-key-range
**consensus group** instead of the leaderless quorum path. In such a zone you get:

- **Real linearizable compare-and-swap** — locks, counters, unique-key
  reservations, the use cases the LWW path can't serve safely.
- **Multi-key atomicity within a partition** — the thing the leaderless pivot gave
  up, back for these zones (a partition's leader orders a batch as one entry).

LWW zones are unchanged and remain the high-volume default.

## Why it has to be a per-zone mode (not a per-op flag)

A consensus protocol owns its register: phase 1 reads the current value and phase 2
commits the chosen one. A blind fire-and-forget `put` from the leaderless path
would clobber that value out from under the protocol and break the invariant it
relies on. So a CAS-managed key **cannot also take LWW writes** — linearizability
must govern a key's *entire* write path, which makes it a property of the zone, not
of an individual operation. It slots into the zones design as a feature flag
(`docs/zones.md`): a zone is either `lww` (leaderless, default) or `linearizable`.

## Shape

- A linearizable zone is partitioned into key-ranges; each range is a **consensus
  group across its RF replica nodes** (reusing `vals-raft`, already in the tree for
  the PD). This is essentially the **pre-pivot "Raft per partition"** design,
  scoped to opt-in zones.
- The leader serializes writes, so CAS is trivially correct — the leader does
  read-check-write atomically in its state machine and replicates the result.
- **Happy path = one round-trip** (a stable leader appends + replicates to a
  quorum, same order of cost as a quorum write). The two-phase prepare/election
  only runs when leadership is established or contested; contention adds rounds +
  backoff to avoid dueling proposers.

## Consistency with the pivot

This does **not** undo the leaderless pivot. The pivot's claim was that the
*default* path for the high-volume single-key workload is leaderless — not that
consensus is banned everywhere. A linearizable zone mode is the right tool for the
keys that genuinely need ordering, coexisting with LWW zones because the two are
separated at the zone boundary.

## Honest costs (why it's opt-in, not default)

- The happy path is still ~1 RTT — not free.
- Per-range leadership + election machinery to operate and reason about.
- **Reads must go through the leader/quorum** to be linearizable — no cheap `R=1`
  follower reads in these zones.
- CAS keys store consensus state (ballots/Raft log) rather than just an HLC
  version — different per-key metadata and storage format.
- Running consensus groups is real operational weight.

## If/when this is picked up

Open questions to resolve at that time: range partitioning + split/merge for
linearizable zones (overlaps the sorted-zone range partitioner in `docs/zones.md`);
how a linearizable zone's ranges map onto the existing node/shard/drive layout;
linearizable read implementation (leader lease vs read-index); and the client API
surface for in-partition multi-key transactions.

# Backup and restore

`vals` ships a **logical, cluster-wide backup**: an authoritative copy of the PD
Raft state, plus one key-sorted file per zone holding every key's newest
version (value or tombstone), merged across replicas. `valsd backup create`
takes one and `valsd backup verify` checks one offline. `valsd backup
restore-pd`, `load` and `compare` rebuild it as a **new, separately fenced
cluster** and check the copy exactly ([Restoring a backup](#restoring-a-backup)).

Byte layouts are in [`formats.md`](formats.md) §3.7. The raw per-node vlog
export, `valsd dump` ([`deploy.md`](deploy.md#bulk-vlog-export-valsd-dump)), is
a different tool. It copies replica-local history, not deduplicated logical
state, and it is not a backup.

## Taking a backup

```
valsd backup create -s /run/vals/pd-admin.sock --out /backups/vals-2026-10-14
valsd backup verify /backups/vals-2026-10-14
```

Run `create` on the host of the **current PD leader**, pointing `-s` at that
replica's admin socket (find it with `status`, see [`pd-admin.md`](pd-admin.md)).
The output directory must not exist yet; its parent must. The PD process
itself writes the PD-state captures into that directory (as transient
`.pd-capture-N` files), so the PD's user must be able to write there. Every
data node in the PD membership must be reachable from the backup host on its
data address(es).

`create`:

1. Asks the PD for `export-state`. It succeeds only on a leader confirmed by a
   ReadIndex quorum, and returns the complete PD state machine at its applied
   index: membership with addresses, catalog, range maps, the cluster
   incarnation and the remaining PD keys. A PD that has not minted a cluster
   incarnation yet is refused.
2. For the default zone (id 0) and every catalog zone, it opens one connection
   to **every replica shard** that can hold the zone: each listen address of
   every member, or only the members in a scoped zone's scope. It pages through
   each replica's live index over the data protocol: `SyncBucketDigests` over
   all buckets in key order, then `SyncValues` for those keys. These are the
   reads anti-entropy uses, so they include memtable contents and tombstones
   with their versions. The streams are merged by key, and the **highest
   version wins** (the cluster's LWW order). For a sorted zone it also pages
   each replica's range tombstones (`SyncRangeTombstones`, unscoped), overlays
   them into one canonical set, writes that set after the point records, and
   leaves out every point record the set covers (`version <= cover(key)`).
   Replicas without hello feature bit 3 hold no range tombstones and are
   skipped for this step.
3. Asks the PD for `export-state` again and compares the two captures (see
   below). If they differ, the backup is discarded and the output directory
   removed.
4. Writes `MANIFEST` last: sizes, CRC-32C and record counts of every file. A
   directory without a `MANIFEST` is an incomplete backup.

`--stale-copies fail|omit|keep` (default `fail`) chooses what happens to puts
held only by nodes that are not current owners of the key when an owner refuses
them (see
[Consistency contract](#consistency-contract)).

Exit codes: 0 success (`ok backup <id> -> <dir>: ...`), 1 failure (`err: ...`,
nothing usable left behind), 2 usage error.

`verify` reads only the backup directory. It checks the manifest version and
fields, then each file's size and CRC-32C against the manifest, and each file's
own trailer CRC and record count. It checks that keys are strictly increasing,
that the PD state decodes with the manifest's incarnation and index, and that
the zone files match the PD-state catalog one for one, with the same name,
ordering and RF.

## What a backup contains

| Item | Treatment |
|---|---|
| PD state | Every key/value of the PD Raft state machine at index `pd_applied`, from a confirmed leader. |
| Zones | Default zone 0 plus every zone in that catalog. Data of dropped zones still on disk is not read. |
| Versions | Every record keeps its 16-byte HLC version, so a restore can reproduce exact LWW order. |
| Tombstones | Kept, with versions, when they are a key's newest version on any replica. Tombstones already garbage-collected on every replica are absent, which is the same logical state. |
| Range tombstones (sorted zones, [`range-deletes.md`](range-deletes.md)) | The overlay of every replica's fragment set, with versions, after the point records. Keys they cover are not written: a covered key is absent, which is the same logical state. Fragments already collected under `gc_grace` on every replica are absent. |
| Replica duplicates | Merged: one record per key, holding the highest version seen on any replica (puts held only by non-owners are fenced, see the consistency contract). Two replicas holding **different** contents (value, or put vs tombstone) at the **identical** version are corruption: the backup fails and names both replicas. |
| Range maps, split points, placement tokens | In the PD state, as committed. |

Not captured:

- **Hinted handoff backlogs.** These live in the writing client's hint store,
  not in the cluster. A sloppy-quorum write whose hint has not been delivered
  is in the backup only if some replica already holds it.
- **Node-local state:** incarnation binding, liveness watermarks,
  superblocks, layouts and config files. A restore re-provisions these.
- **PD and node config files** (addresses, secrets, tuning). Keep them in
  config management.
- **Nodes outside the PD membership.**

## Consistency contract

- **PD state** is a single authoritative snapshot at index `pd_applied`.
- **Data is per key, not a cross-key point in time.** For each key, the
  backup holds the newest version present on any of the key's replicas at
  the moment that replica's stream reached the key. Every write acknowledged
  before `create` started is either in the backup or superseded by a newer
  version, because all replicas are read and an acknowledged write is on at
  least one. A write concurrent with the backup may or may not be included,
  independently for each key. There is no multi-key atomicity, matching the
  leaderless data path.
- **Placement must not move data during the backup.** Between the two PD
  captures, membership (ids, weights, addresses), the catalog and the
  incarnation must be identical. Every range map must be
  placement-equivalent: each key keeps the same placement token and pending
  re-home target, and the opaque-token mode is unchanged. Splits that inherit
  their parent's token (the default `split-zone`, auto-splits of populated
  ranges) and merges of ranges that share a token are allowed. A re-home
  commit or abort, a new re-home or balance move, a canonical split or merge
  that changes a key's token, or a membership or catalog change discards the
  backup. Retry when topology is quiet: pause manual
  re-homes/balancing, or set `rehome_enabled=false`, for the duration.
  A re-home that is pending at both captures is fine, because both its source
  and its target are read.

- **Copies on nodes that do not own the key are fenced.** A node can still hold a
  key it no longer owns: a former owner before over-replication cleanup removes
  it, a re-home target after an abort, or a sloppy-quorum fallback. The merge
  computes each key's owners from the captured placement (ring, catalog, range
  map, and pending re-home targets). Records held by a current owner, and every
  tombstone, are always merged. A **put** held only by non-owners, or only by a
  pending re-home target (whose legitimate copies the current owner also
  holds), is merged only if its version is at or above the tombstone-GC floor
  of **every** owner shard, pending targets included. That floor is the one below which the owner
  refuses a put (`GcFloor`, [`formats.md`](formats.md) §1.3). It is read only
  when such a copy would win, after the owner's stream has passed the key, so a
  tombstone the owner collected before or while it was read cannot be missed.
  With `gc_grace = 0s` every floor is 0, so nothing is fenced.

  Such a put below some owner's floor is **ambiguous**. Its delete may have
  been collected, or the value may never have reached the current owners. The
  cluster cannot tell which, and that owner will never accept it, so cleanup
  cannot hand it off either. `create` therefore fails by
  default, names up to 20 such keys and the nodes that hold them, and leaves
  nothing behind. Rerun with `--stale-copies omit` to back up what the owners
  serve (the omitted keys are listed in the progress output), or with
  `--stale-copies keep` to include every copy as before. Use `keep` only if
  none of those keys was deleted.

  Inside the cluster, the same floor keeps cleanup handoff and read repair from
  bringing such a copy back, and anti-entropy pulls only keys both peers own.
  Remaining assumptions:
  - Every owner that was an owner when the delete was written received the
    tombstone within `gc_grace`
    ([`../ARCHITECTURE.md`](../ARCHITECTURE.md#deletes-and-tombstones)).
  - An owner that has seen no data progress yet (an empty node that just
    became an owner or a pending re-home target) has floor 0. Until its first
    write it would accept such a copy from a cleanup handoff.
  - A re-home to a node that still holds such a copy does not complete. For
    each page, the target checks its own entries in the page's window. A value
    the source lacks, and older than half the grace, holds the copy, so the PD
    aborts the move rather than cut over to it
    (`vals_rehome_copy_target_only_blocks`, with a warning naming the key).
    Delete the key on the target, or let the move stay aborted.
  - A membership change (`add-node`, `remove-node`, a weight change) can make
    a node that holds such a copy an owner again, and an owner serves what it
    holds. The PD therefore refuses the change unless every member that stays
    answers `CleanupStatus`, has adopted the current membership, and has
    finished an over-replication cleanup lap of every zone, started after the
    check began, that released every copy of a key it does not own. The check
    passes without that only when every member, including a departing node,
    answers that tombstone GC is off (`gc_grace = 0s`); a member that does not
    answer or has no data address fails it. A copy that not every owner
    confirms keeps the zone unsettled (a refusing owner, as for a put under a
    collected point or range delete, or an unreachable one), and the refusal
    names the node, the zone and up to four sample keys. To resolve it,
    delete a key that should stay deleted (the owner then holds a newer delete,
    so cleanup releases the copy), or rerun the command with `--force`, which
    lets the node serve its copies (the right call only if those values were
    never deleted). A node that `add-node` brings in must hold no data unless
    tombstone GC is off on every member: the PD asks it at the addresses given
    to `add-node` and refuses one that holds records (wipe a removed node before
    it rejoins). A pending re-home also refuses the change, both in the PD
    state and on any member, and so does a re-home copy hold that outlives an
    abort (`rehome_copy_hold_ms`): cleanup treats a pending target's or a held
    range's copies as owned, so it cannot vouch for them, and a lap that ran
    under such a hold or under a placement that has since changed does not
    count. A check reuses a cleanup lap that started within
    `min(gc_grace / 2, 10 min)`. The PD waits up to 4 s for laps to finish;
    rerun the command if a large store needs longer.

  Regression tests: `backup::tests::merge_*`, `node::former_owner_tests` (including
  a membership change after a real tombstone GC, the return of a removed
  stale holder, and a pending then aborted re-home whose target holds the
  stale copy), `pd::cleanup_gate::tests`,
  `net::tests::rehome_copy_holds_while_the_target_keeps_an_old_leftover_the_source_lacks`,
  and the `valsd` drill
  `a_former_owner_copy_whose_delete_aged_out_is_neither_backed_up_nor_restored`.
  `compare` applies no fence: a non-owner copy on the target shows up as a
  difference.

## Limits and evidence

- Validated by local tests only (file backend, loopback, small data). There
  has been no SPDK or hardware run, and backup throughput is unmeasured.
- The tool pulls replicas one request at a time from a single thread, with
  two round trips per page per replica shard, and reads every replica (RF×
  read amplification). It has no rate limit of its own. The node-side digest
  serving is bounded (an overloaded node answers `Busy`, which the tool retries
  for up to 120 s per request), but a large backup adds read load for its
  whole duration.
- Memory: for each replica shard the tool holds one digest page of keys (at
  most 8 MiB) plus the values of at most 256 keys.
- Every member must answer. There is no partial backup with a node down. When
  a put held only by non-owners would win, the owners must also answer
  `GcFloor`. If a node runs a build that predates that request, `create` fails
  with an upgrade message.
- No incremental or continuous backup and no point-in-time recovery: each
  backup is a full copy.

## Restoring a backup

A restore never touches the source cluster and never reuses its identity. It
builds a **new cluster** from the backup: new PD data directories, empty
freshly provisioned data nodes, and a **new cluster incarnation**
([`deploy.md`](deploy.md#cluster-incarnation-id)). The restored PD records its
lineage (the source incarnation, the backup id and the source PD index), so
the copy is traceable and a surviving or restarted old PD stays fenced out:
target nodes bind to the new incarnation on the first push and refuse
everything the old PD sends them. Restoring an older backup works the same
way.

### Runbook

1. **Check the backup:** `valsd backup verify <dir>`.
2. **Fence the source.** Stop the old PD processes (or isolate them from the
   target nodes) if they are still alive. The incarnation check protects
   target nodes once they are bound, but it cannot stop an old PD from
   pushing to a node that has never been bound (step 4 starts the new PD
   before the nodes, so the new PD binds them first).
3. **Write the target configs.** The node ids and ring weights must be the
   backup's (`data_node` lines, with the target hosts' addresses); `rf` and
   `vnodes` must equal the backup's `ring` line; every boot `zone` line must
   name a zone of the backup with the same RF and ordering; a backup with
   opaque-token range maps needs `opaque_range_maps = true`. The number of
   shards per node may differ. Provision every target node empty (`valsd
   provision` for SPDK, an empty `data_dir` for the file backend).
4. **Seed the PD, then start the cluster:**

   ```
   valsd backup restore-pd -c /etc/vals/pd.conf /backups/vals-2026-10-14
   ```

   The PD `data_dir` must be empty or absent; restore-pd never overwrites a
   PD. It writes the PD Raft storage under `<data_dir>/raft` as one snapshot
   at the backup's PD index (format: [`formats.md`](formats.md) §3.7), and
   prints the new incarnation and a state digest. For a three-PD target, run
   it on every voter with the same `--incarnation <32 hex digits>` (restore-pd
   refuses a multi-voter config without one and suggests a fresh value) and
   identical `data_node` lines, check that every voter printed the same digest,
   and only then start any voter: voters seeded differently would diverge
   silently. Start the PD(s), then the data nodes.
5. **Load the data:**

   ```
   valsd backup load -s /run/vals/pd-admin.sock /backups/vals-2026-10-14
   ```

   `-s` is the **target** PD leader's admin socket; like `create`, run it on
   that host (the PD writes its state captures into a private scratch
   directory under `$TMPDIR`). `load` refuses a PD whose lineage does not name
   this backup, then waits up to 120 s until every data node serves the
   restored membership, catalog and range maps. It writes every record,
   tombstones included, **at its original version** to **every** replica of
   its key, in batches of at most 256 records or 8 MiB with device sync, and
   needs every replica to acknowledge. A replica that already holds a newer version of a
   key fails the load: the target was not empty. Busy, unreachable and
   stale-placement replies are retried for up to 120 s per batch. A load is
   idempotent; rerun it after any failure. `load` checks each zone file's
   checksums only when it reaches the file's end, after its records are
   written, so run `verify` (step 1) first. A zone's range tombstones are
   applied only after its whole file has been read and checked, at their
   original versions, as `RangeDelete` slices to every replica of each range
   they overlap, so a damaged file never deletes data.
6. **Check the copy:**

   ```
   valsd backup compare -s /run/vals/pd-admin.sock /backups/vals-2026-10-14
   ```

   `compare` re-reads every replica of every zone exactly as `create` does,
   merges them the same way and compares the result with the backup, record
   for record. A backup tombstone that the target has already garbage-collected
   counts as equal (the same logical state); any other missing, extra or
   different record is a difference. Range tombstones are compared as merged
   sets: a fragment held on one side only is a difference, and target records
   the backup's set covers are skipped. It prints up to 20 examples and exits 1
   on any difference. It also checks the lineage, membership ids and weights,
   catalog, ring and range placement. Run it before the restored cluster takes
   writes: afterwards it reports their effects as differences.
7. **Serve.** Point clients at the target nodes.

Exit codes for all three: 0 success (`ok ...`), 1 failure (`err: ...`), 2 usage
error.

### What a restore changes

| Item | Restored as |
|---|---|
| Records | Every backup record at its original version on every replica, so LWW order is unchanged. |
| Membership | Same node ids and weights; addresses from the target `data_node` lines. |
| Catalog, zone ids, next zone id, range splits and tokens | Unchanged. |
| Pending re-homes, balance lock | Cleared. The source copy of a moving range is its owner in the restored map; the data of both owners was merged into the backup. |
| Cluster incarnation | New. The source incarnation and the backup id are kept as the restore lineage. |
| PD Raft index and term | The backup's PD index and term, so placement epochs do not restart from zero. |

### Limits and evidence

- Validated by local tests only (file backend, loopback, one PD, three
  nodes, small data), including a drill test that backs up a cluster, stops
  it, restores it into empty nodes on new addresses, loads, compares, reads
  every key through a client, restarts the old PD with its push targets
  pointed at the target nodes and checks that they refuse it. There has been
  no SPDK, three-PD or hardware drill, and load and compare throughput are
  unmeasured.
- `load` runs one batch at a time from a single thread with no rate limit,
  and writes RF× the backup's records. `compare` costs the same as `create`.
- Every target node must be reachable and must stay so for the whole load.
- A target node that is not empty is detected only where it holds a newer
  version of a backed-up key (load fails) or a key the backup lacks (compare
  reports it). Provision targets empty.
- The new PD's placement policy runs during the load: with `rehome_enabled` or
  balancing on, a range move during the load fails it (rerun it). Keep them off
  until compare passes.
- Hint backlogs, node-local files and configs are not restored (they are not
  in the backup).
- **Format compatibility.** Backups are written as zone-file version 2 (which
  carries range tombstones) whether or not a zone holds any. Builds that
  predate version 2 refuse such files in `verify`, `load` and `compare`, so
  restore a backup with a build at least as new as the one that wrote it.
  Current builds still load version 1 files.

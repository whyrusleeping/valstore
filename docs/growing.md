# Growing a small deployment

This guide is the path from vals on one machine to a highly available
cluster. It says which changes work on a running cluster, which need a new
cluster built from a backup, and which have no supported path yet.

The transitions below were checked with local smoke tests: file-backed nodes
and PDs on one machine, `gc_grace = 0s` (the default) and small data sets. That
shows the commands and their order work. It does not qualify them on hardware
or at large scale. The backup and restore tools themselves were also
qualified once on hardware ([`backup-restore.md`](backup-restore.md#hardware-qualification)).

| Change | How | Downtime |
|---|---|---|
| Larger regions for **new** zones | Raise `region_mib` and restart the node | One node restart |
| Larger regions for **existing** zones | [Backup and restore](#moving-to-three-pds-new-hosts-or-larger-regions) into nodes with a larger `region_mib` | Writes stop until clients use the new cluster |
| More data nodes | [`add-node`](#adding-data-nodes) on the running cluster | None |
| One PD to three PD voters | [Backup and restore](#moving-to-three-pds-new-hosts-or-larger-regions) into a new three-voter cluster | Writes stop until clients use the new cluster |
| Data nodes on new hosts or addresses | The same backup and restore, with the new addresses | Writes stop until clients use the new cluster |
| Higher RF for data you already have | [Not supported](#replication-factor): create a new zone and copy the data in your application | Depends on your copy |
| Removing a data node at RF 1 | [Not supported](#adding-data-nodes): `remove-node` refuses; with `--force` its keys become unreachable | — |

If you already know you will need high availability, start with the
[three-PD reference deployment](deploy.md#three-pd-reference-deployment) and
RF 3 zones. That avoids the backup and restore step and the copy into new
zones that this guide describes.

## A minimal single-node deployment

One data node and one PD, both file-backed, on one host. The PD owns
membership and the zone catalog, so even the smallest deployment runs both
processes. Both configs need `rf = 1`: RF can never exceed the number of data
nodes, and the default is 3.

```ini
# /etc/vals/node1.conf
node_id     = 1
listen      = 10.0.0.1:7001          # a private address your application can reach
data_dir    = /var/lib/vals/node1
rf          = 1
region_mib  = 16384                  # per-zone file size, fixed when a zone is created (below)
secret_file = /etc/vals/secret       # mode 0600, shared with the PD
metrics     = 10.0.0.1:9464          # optional, unauthenticated: keep it private
```

```ini
# /etc/vals/pd.conf
pd_id        = 1
data_dir     = /var/lib/vals/pd
data_node    = 1 @ 10.0.0.1:7001
rf           = 1
zone         = events 1 sorted       # optional boot zones: <name> <rf> <sorted|unsorted>
admin_socket = /run/vals/pd.sock
secret_file  = /etc/vals/secret
```

Check both files, then start the node and the PD (under systemd, use the
[units in `deploy.md`](deploy.md#systemd)):

```sh
valsd node -c /etc/vals/node1.conf --check-config
valsd pd -c /etc/vals/pd.conf --check-config
valsd admin -s /run/vals/pd.sock status     # role=leader, an incarnation id, voters=1
valsd admin -s /run/vals/pd.sock members    # 1 1 10.0.0.1:7001
valsd admin -s /run/vals/pd.sock zones      # the boot zones
```

From the repository, the `kv` example client confirms that data survives a
restart:

```sh
cargo build --release -p vals-client --example kv
KV=target/release/examples/kv
$KV --nodes 1@10.0.0.1:7001 put hello world
$KV --nodes 1@10.0.0.1:7001 --zone events put 2026-06-11/42 click
# restart both services, then:
$KV --nodes 1@10.0.0.1:7001 get hello                 # world
$KV --nodes 1@10.0.0.1:7001 --zone events scan        # 2026-06-11/42  click
```

Know what this deployment does not give you:

- **One copy of everything.** Losing the disk or the host loses every record
  written since your last backup, and the PD state with it. Nothing rebuilds
  the PD from the data node ([PD recovery, case 4](deploy.md#pd-availability-and-recovery)).
- **Backups are your only protection.** Take them with
  `valsd backup create` and check them with `valsd backup verify`, and copy
  them off the host ([`backup-restore.md`](backup-restore.md)). A backup can
  be restored only into a cluster with the same data-node ids and weights and
  the same RF, as described [below](#moving-to-three-pds-new-hosts-or-larger-regions).
- **Acknowledged is not synced.** By default a write is acknowledged once it
  is in the node's memory, so a crash or power loss can lose acknowledged
  writes. Request [device-durable writes](getting-started.md#reading-and-writing)
  for data that must survive that. A clean `SIGTERM` stop syncs everything.
- **No availability during restarts.** While the node restarts, every read
  and write fails. While the PD is down, the node keeps serving, but no zone
  or membership change can be made.
- **Security.** The secret gates only control operations; reads and writes
  are never authenticated or encrypted. Bind the data port to a private
  network ([operational notes](deploy.md#operational-notes)).
- **Keep `gc_grace = 0s`** (the default) if you plan to migrate with a
  backup: a restore into nodes with tombstone GC on is not qualified
  ([`backup-restore.md`](backup-restore.md#limits-and-evidence-1)).

## Sizing zones on the file backend

Every file-backed data node keeps each zone in its own file,
`<data_dir>/zones/zone-<id>.vlog`. The node creates that file the first time
it opens the zone (zone 0, `default`, at its first start) with a size of
`region_mib`. The file is sparse, so it takes disk space only as data is
written, but the filesystem must have room for every zone file to fill:
plan for `region_mib` × the number of zones on each node, including the
default zone.

**`region_mib` applies only to zone files created after the change.** A zone
file keeps the size it was created with. In the smoke test, a node restarted
with `region_mib` raised from 64 to 128 kept its two existing 64 MiB zone
files and created the next new zone at 128 MiB. Nothing resizes an existing
zone file in place; do not extend one by hand. Choose `region_mib` before you
create your zones, or move existing zones into larger files with a
[backup and restore](#moving-to-three-pds-new-hosts-or-larger-regions).

A zone holds less than its region. The engine keeps a reserve for flushes and
merges, up to half of a small region, and overwritten values take space until
compaction reclaims them. In a 16 MiB test region, writes of 120 KB values
were refused after about 7.8 MB. Watch `vals_min_zone_free_segments` (the zone
closest to full) and `vals_busy_rejections`, and add capacity before a zone
gets close ([`backpressure.md`](backpressure.md#zone-region-capacity-and-maintenance-reserve)).

**When a zone fills**, the node refuses writes to that zone with a retryable
`Busy`, which the Rust client reports as a `backpressure` error. That
includes deletes, because a delete writes a tombstone, so you cannot free
space by deleting. Reads of the full zone, and every operation on other
zones, keep working. Restarting the node does not help. Some states end in
the terminal error `zone region exhausted`, which also does not clear on its
own. In the smoke test the zone stayed full after a second data node took
over about half of its keys, so do not count on `add-node` to rescue a full
zone. What worked was a backup, which only reads, and a restore into nodes
with a larger `region_mib`.

## Adding data nodes

Adding a data node is an online, in-place change; the PD keeps its state and
clients keep working. On the new host, write a node config with a new
`node_id` (and `rf = 1` if the cluster has fewer than three nodes, since a
node checks `rf` against the nodes it knows at boot), start it with an empty
`data_dir`, then on the PD leader's host run:

```sh
valsd admin -s /run/vals/pd.sock add-node 2 1 10.0.0.2:7001
valsd admin -s /run/vals/pd.sock members    # now lists node 2
```

The reply must be `ok committed ...` ([`pd-admin.md`](pd-admin.md)). The
second number is the ring weight: give the new node a weight in proportion
to its capacity. Then add a matching `data_node = 2 @ 10.0.0.2:7001` line to
every PD config, so the configs keep matching the membership; a restore needs
them to (see [below](#moving-to-three-pds-new-hosts-or-larger-regions)).
With `gc_grace` above zero, the new node must be empty and the PD first
checks that cleanup has settled ([`deploy.md`](deploy.md#administering-a-running-cluster)).

What adding nodes does and does not change:

- **Keys move to the new node.** Some keys now belong to the new node, and
  the old owner hands them over in the background. At RF 1 a moved key reads
  as not found until it arrives: in the smoke test, 1 of 200 keys read as not
  found in the first pass after `add-node`, and all of them read back about
  two seconds later. At RF 2 or more, the old replicas that keep the key
  still serve it while anti-entropy copies it; this guide's smoke tests did
  not cover that case.
- **RF stays the same.** At RF 1 there is still one copy of each key; the
  copies are just spread over more nodes. See [Replication factor](#replication-factor).
- **The PD stays single.** A single PD cannot be grown into a three-voter
  group; see the next section.
- **Do not remove a data node at RF 1.** `remove-node` does not move a
  removed node's keys anywhere first. In the smoke test, 105 of 200 keys read
  as not found right after `remove-node`, even with the removed node still
  running. With `gc_grace = 0s`, starting that node again and running
  `add-node` with its old id brought them back. So `remove-node` refuses with
  `err: membership change refused, nothing applied: removing node N would
  leave keys with no surviving replica ...` when the ring RF is 1 or a zone
  that may place keys on the node has RF 1, and names those zones; `--force`
  removes the node anyway and accepts losing access to its keys. At RF 1,
  move nodes with backup and restore instead.

## Moving to three PDs, new hosts or larger regions

A single PD cannot become a three-voter group in place, and there is no
supported way to change a data node's address or the size of an existing
zone file. All three changes use the same procedure: back up the cluster,
then restore the backup into a **new cluster** that has three PD voters, the
new addresses and the new `region_mib`. The restore never modifies the source
cluster, so it stays available as a fallback until you switch over.

**What the new cluster must keep:** the data-node ids and ring weights, the
PD `rf` and `vnodes`, and each zone's RF and ordering. `restore-pd`, `load`
and `compare` refuse a cluster that differs. Add data nodes after the
restore with `add-node`. **What may change:** addresses, the number of PDs,
`region_mib`, data directories and the number of shards per node.

**Before you start:**

- **Downtime.** A backup contains every write acknowledged before
  `backup create` starts; a write after that may or may not be in it. So stop
  your application's writes before the backup and keep them stopped until
  your clients use the new cluster. Reads can keep using the old cluster
  until then. To estimate the downtime, run `valsd backup create` once
  against the live cluster beforehand: it is online and only reads, and
  `load` and `compare` take about as long again. For scale, on SPDK hardware
  a backup ran at about 15,600 records/s and a load at about 51,000
  records/s ([hardware numbers](backup-restore.md#hardware-qualification)).
- **Capacity.** The backup needs free space about the logical size of the
  newest version of every key. Each new node needs at least the capacity of
  the node with the same id; with the file backend that means a large enough
  `region_mib`. The new PDs and nodes need empty data directories. If they
  run on the old hosts, use different directories and ports, since the old
  cluster must stay intact until you switch.
- **Quiet placement.** Do not add or remove nodes, or create or drop zones,
  during the backup; `create` fails if the topology changes.
- **`gc_grace = 0s`** on the new nodes, as noted above.

**Steps.** These use three hosts, one PD and one data node on each, and a
source with one data node; adjust the `data_node` lines to your node ids.

1. Stop application writes, then back up and verify on the source PD's host:

   ```sh
   valsd backup create -s /run/vals/pd.sock --out /backups/vals-migrate
   valsd backup verify /backups/vals-migrate
   ```

2. Stop the source PD (`SIGTERM`), so that it cannot push its configuration
   to the new nodes before they bind to the new cluster
   ([restore runbook](backup-restore.md#runbook), step 2). The source data
   nodes keep serving reads on their last configuration; stop them once
   clients have switched. Keep every source directory until the new cluster
   has passed `compare` and your own checks.
3. Write the new configs. Each node keeps its `node_id`; set the new
   `listen` address and `region_mib`, and an empty `data_dir`. Each PD voter
   gets the same `data_node` lines (old ids, new addresses), `rf`, `vnodes`
   and secret, and the [three-PD settings](deploy.md#pd-config-reference):

   ```ini
   # /etc/vals/pd.conf on host 1 (pd_id 2 and 3 on hosts 2 and 3, without `campaign`)
   pd_id        = 1
   data_dir     = /var/lib/vals/pd
   data_node    = 1 @ 10.0.0.1:7001
   rf           = 1
   admin_socket = /run/vals/pd.sock
   secret_file  = /etc/vals/secret
   listen       = 0.0.0.0:7100
   pd_peer      = 1 @ 10.0.0.1:7100
   pd_peer      = 2 @ 10.0.0.2:7100
   pd_peer      = 3 @ 10.0.0.3:7100
   campaign     = true
   ```

4. Copy the backup to every PD host and seed each voter with the same new
   incarnation id. Every voter must print the same state digest:

   ```sh
   INC=$(od -An -tx1 -N16 /dev/urandom | tr -d ' \n')   # once; use the same value on every host
   valsd backup restore-pd -c /etc/vals/pd.conf --incarnation $INC /backups/vals-migrate
   ```

5. Start the three PDs, then the data nodes. On every PD host, `status` must
   show the same `leader`, `voters=1,2,3` and the new incarnation:

   ```sh
   valsd admin -s /run/vals/pd.sock status
   ```

6. On the leader's host, load the backup and compare the result with it:

   ```sh
   valsd backup load -s /run/vals/pd.sock /backups/vals-migrate
   valsd backup compare -s /run/vals/pd.sock /backups/vals-migrate
   ```

   `compare` must end with `ok target matches`. Both commands can be rerun
   after a failure ([restore runbook](backup-restore.md#runbook)).
7. Check some of your own keys with a client pointed at the new addresses,
   then switch your application's seed nodes to them and resume writes.
8. Grow the new cluster in place: [add data nodes](#adding-data-nodes) and
   add their `data_node` lines to all three PD configs.

In the smoke test, a single-PD cluster whose only zone was full (16 MiB
region) was restored this way into three PD voters and a node with a 64 MiB
region. `compare` matched every record, writes to the zone succeeded again,
and two more data nodes were then added with `add-node`. A second rehearsal
of exactly these steps, with a default and a sorted zone and a deleted key,
read every key back with its value after the restore and after adding two
nodes, and the deleted key stayed deleted.

**Rollback.** If anything fails before you switch clients, stop the new
cluster and start the old one again: the restore never changed the old PD
or nodes (in the rehearsal, the old cluster served every key afterwards).
The two clusters have different
[incarnation ids](deploy.md#cluster-incarnation-id), so a node bound to one
refuses control pushes from the other's PD.

## Replication factor

A zone's RF is fixed when the zone is created. No admin command changes it,
and a restore keeps it. The default zone's RF is the PD's `rf` key, and the
PD records it in its committed state the first time it leads. A PD whose state
records one `rf` refuses to start with another in its config: it exits with
an error that names both values, and starts again once `rf` is set back.
There is no in-place RF change for an existing cluster.

To keep data at RF 3 once you have at least three data nodes, create new
zones with RF 3 and copy your data into them in your application, by
scanning or reading the old zone and writing each key to the new one. Then
switch your application to the new zones and drop the old ones with
`drop-zone`:

```sh
valsd admin -s /run/vals/pd.sock create-zone events3 3 sorted
```

The copy writes each key with a new version, so it would overwrite any newer
write made to the new zone meanwhile. Stop writes to the old zone while you
copy it, and switch them to the new zone only when the copy is done. The
default zone is not in the catalog and cannot be dropped; move its data into
a named zone the same way.

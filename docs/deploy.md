# Running vals with `valsd`

This is the operator guide for `valsd`, the vals server daemon. It covers
building it, a local test cluster, every config key, the PD (control plane)
and how to keep it available, storage profiles and sizing, SPDK provisioning,
day-to-day administration, systemd, and the failure modes an operator sees.

Which revision to run, what it was tested on and its known limits are in
[`evaluation.md`](evaluation.md); before exposing any port, work through the
[security checklist](#security-checklist).

For the data model and the client API, read
[`getting-started.md`](getting-started.md) first. The full list of tuning
keys and their legacy environment aliases is in
[`configuration.md`](configuration.md).

## What `valsd` is

`valsd` is one binary with several subcommands:

| Command | What it does |
|---|---|
| `valsd node -c <config> [--check-config]` | Serve a data node. |
| `valsd pd -c <config> [--check-config]` | Serve one replica of the placement driver (PD), the small Raft group that owns membership, zones and placement. |
| `valsd admin -s <socket> <command...>` | Send one command to a running PD's (or node's) admin socket. |
| `valsd provision -c <config> --device <pci>=<serial>... --confirm-destroy node-<id>` | Create a fresh on-device layout for an SPDK node. Destroys existing data. |
| `valsd reenrol -c <config> (--incarnation <id> \| --clear)` | Move a stopped node to a different PD group. |
| `valsd backup create\|verify\|restore-pd\|load\|compare ...` | Cluster-wide backup and restore; see [`backup-restore.md`](backup-restore.md). |
| `valsd dump -c <config> (--zone <name> \| --zone-id <id>) --out <path> [--skip-tombstones] [--parallel <N>]` | Export one zone's value log from a stopped (or file-backed) node. |
| `valsd keyscan -c <config> --zone-id <id> --out-dir <dir> [--want-ids <file>] [--partition <N>]` | Offline diagnostic key listing; SPDK multi-drive layouts only. |

Running `valsd` with no arguments prints the exact usage of each.

**Signals.** `SIGTERM` and `SIGINT` shut a daemon down gracefully: it drains
reads, flushes connections, syncs every engine, and persists the liveness
watermark and Raft state, so a rolling restart is safe. `SIGHUP` re-reads the
config file and applies the reloadable keys (see [Config reload
(SIGHUP)](#config-reload-sighup)).

**Logs** go to stderr. Set the level with `log_level` in the config
(reloadable). The older `VALS_LOG` environment variable still works but logs a
deprecation warning, and startup fails if both are set to different values.

## Building

```sh
cargo build --release -p vals-server            # file and mem backends
VALS_SPDK_LINK=1 cargo build --release -p vals-server --features spdk   # raw NVMe
```

The SPDK build needs SPDK v24.09, hugepages and dedicated drives; see
[`SPDK.md`](SPDK.md).

## A three-node cluster in five minutes

This starts three data nodes and one PD on one machine with file-backed
storage, the same shape the end-to-end test deploys. It is a development and
test setup only: a single PD is not highly available, and changing the
addresses to spread the data nodes over machines does not change that. Real
deployments run three PDs ([PD config reference](#pd-config-reference)).

```sh
cargo build --release -p vals-server
VALSD=$PWD/target/release/valsd

mkdir -p /tmp/vals && cd /tmp/vals
echo "demo-secret" > secret

for i in 1 2 3; do
  cat > node$i.conf <<EOF
node_id  = $i
listen   = 127.0.0.1:700$i
data_dir = node$i
$(for p in 1 2 3; do [ $p != $i ] && echo "peer = $p @ 127.0.0.1:700$p"; done)
secret_file = secret
EOF
done

cat > pd.conf <<EOF
pd_id     = 1
data_dir  = pd
data_node = 1 @ 127.0.0.1:7001
data_node = 2 @ 127.0.0.1:7002
data_node = 3 @ 127.0.0.1:7003
rf        = 3
zone      = events 3 sorted
admin_socket = pd.sock
secret_file  = secret
EOF

for i in 1 2 3; do $VALSD node -c node$i.conf 2> node$i.log & done
$VALSD pd -c pd.conf 2> pd.log &
sleep 2

$VALSD admin -s pd.sock ping
$VALSD admin -s pd.sock members     # the three data nodes
$VALSD admin -s pd.sock zones       # the sorted `events` zone
```

Stop everything with `pkill -x valsd` (SIGTERM). Clients connect with a
`Coordinator` over `NetTransport`, as shown in
[`getting-started.md`](getting-started.md#clients).

On first start the PD commits the complete boot membership (the `data_node`
entries) in one Raft entry, then the boot zones, and then pushes the ring and
zone catalog to every node, retrying until each node confirms.

**Ring weights.** A `data_node` entry may carry a ring weight:
`data_node = <id>[*<weight>] @ <addr>[,<addr>...]`. The weight defaults to 1
and must be at least 1. Keys are placed in proportion to weight, so give a
multi-drive SPDK node its drive count as weight (the node logs its computed
weight at startup):

```ini
data_node = 1        @ 127.0.0.1:7001   # weight 1 (default)
data_node = 2*4      @ 127.0.0.1:7002   # weight 4 (a 4-drive node)
```

## Configuration basics

Both daemons read a flat `key = value` file. `#` starts a comment, blank lines
are ignored, and keys marked repeatable may appear more than once; any other
key given twice, or any unknown key, is an error with its line number. Relative
paths (`data_dir`, `secret_file`, `admin_socket`) are resolved against the
directory that holds the config file.

**Check a config before restarting.** `valsd node -c node1.conf --check-config`
and `valsd pd -c pd.conf --check-config` parse the file, resolve settings with
the real environment, and print the effective values and which keys need a
restart. They bind no sockets, create no directories, start no elections, and
never attach or format a device. A failed check exits 2.

PD check mode validates only the file; it cannot certify the Raft state an
existing PD will load. Before an upgrade, use the running release's `members`
and `zones` commands to find and fix state that would break the membership,
RF, scope or unique-name rules in [Administering a running
cluster](#administering-a-running-cluster). Keep the old binary until the new
one has started, and use it only before the new release writes its first PD
mutation; never downgrade a PD after that.

**Environment variables.** Typed config keys are the supported interface. Many
keys still accept a legacy `VALS_*` environment alias; a key always wins, and a
key and alias that disagree are a startup error
([`configuration.md`](configuration.md) has the precedence rules). Other
`VALS_*` variables found in the source or in old benchmark commands are
diagnostic controls, not supported settings. Do not copy a benchmark's
environment into a production unit: some of those switches change correctness,
resource bounds or placement. Record any overrides you do use.

## Node config reference

```ini
node_id   = 1                  # required: this node's cluster id
profile   = capacity           # capacity (default) | latency; restart-only
listen    = 0.0.0.0:7000       # required; repeatable: one per shard (spdk only)
backend   = file               # file (default) | mem | spdk
data_dir  = /var/lib/vals/n1   # required for file: zones + liveness log
peer      = 2 @ host:7000      # repeatable: boot peers (the PD extends them at runtime)
rf        = 3                  # boot-ring RF, default 3 (the PD's pushed config overrides)
sweep     = 5s                 # anti-entropy cadence, default 5s (reloadable)
gc_grace  = 0s                 # default 0s; > 0 enables tombstone GC + the liveness lease
secret_file = /etc/vals/secret # or: secret = <literal> (not both)
metrics   = 127.0.0.1:9464     # optional: Prometheus scrape endpoint
admin_socket = /run/vals-node/admin.sock # optional: node admin socket (ping, zone-stats, dump)
log_level = info               # optional: error|warn|info|debug|trace|off (reloadable)

# engine geometry
block_size = 4096              # default 4096; spdk requires 4096
seg_blocks = 256               # blocks per segment, default 256
region_mib = 1024              # default 1024: per-zone region cap (file/mem),
                               #   partition region (spdk multi-drive)

# spdk (backend = spdk; needs the --features spdk build)
pci    = 0000:a1:00.0          # required for spdk; repeatable: one per drive
zone_region_mib = 128          # multi-drive only: default zone size, default region_mib / 8
zone_region_mib_override = 7:98304   # multi-drive only; repeatable: <zone-id>:<MiB>
spdk_shm_id = 1                # SPDK shared-memory id, default node_id
spdk_mem_mb = 0                # SPDK hugepage memory in MiB, 0 = SPDK default
spdk_core   = -1               # SPDK main core, -1 = SPDK default
```

The node also accepts the typed resource and maintenance keys listed in
[`configuration.md`](configuration.md#node-resource-and-maintenance-settings): RAM
budget, flush and compaction cadence, anti-entropy seeding, transport limits
([Transport resource bounds](#transport-resource-bounds)), scan and batch
admission, device deallocation, and the integrity fail-fast thresholds. All
of them are restart-only.

Rules and notes:

- **file and mem** nodes serve every zone the PD creates (each materializes
  on first use) on exactly one `listen` address. `mem` loses everything on
  exit; it is for tests and throwaway clusters.
- **`rf` cannot exceed the node's boot membership** (the node itself plus its
  `peer` entries), even when a PD later manages membership. This stops a node
  from starting with an under-replicated ring before PD state arrives.
- **`sweep` must be greater than zero.** Anti-entropy cannot be switched off
  by setting its timer to zero.
- **`gc_grace`.** `0s` (the default) disables tombstone GC and the liveness
  lease, so deletes keep their tombstones forever. A value above zero turns on
  tombstone GC *and* the wipe-before-rejoin liveness lease (see the consistency
  section of [`ARCHITECTURE.md`](../ARCHITECTURE.md)): a node that rejoins after
  being down longer than the grace wipes its data and re-syncs from peers, so
  it cannot bring back deletes that were already collected. Use the same value
  on every node. It works on all backends; SPDK nodes persist the watermark on
  the device (on each zone or partition superblock), so they need no
  `data_dir`.
- **spdk, one drive** (`pci` and `listen` given once) serves the full zone
  catalog, named and sorted zones alike, over one NVMe namespace, like the file
  backend.
- **spdk, several drives** serves one partition per `listen` address, spread
  over the `pci` drives, and serves the full catalog in each partition (see
  [`multi-drive.md`](multi-drive.md), which also explains
  `zone_region_mib_override`). Either way the node logs its computed ring
  weight at startup; put it in `data_node = <id>*<weight> @ ...` or pass it to
  `add-node` so placement matches capacity.
- **The daemon never formats an SPDK device.** It opens an existing compatible
  layout and refuses a blank, incompatible or partly provisioned one. Before
  any engine writes, it checks the persisted topology, the `listen` count
  (partition count), node id, partition extents, and segment and superblock
  geometry. Changing any of these means reprovisioning offline with `valsd
  provision`, which destroys the old data. A leftover `format = false` line is
  ignored; `format = true` is rejected.

## PD config reference

Production deployments run **three voting PD replicas on three independent
hosts or failure domains**. Running one lightweight PD next to each of three
data nodes is the usual layout; dedicated control hosts also work. Each
replica needs its own durable `data_dir`. Three PD processes on one host do not
survive the loss of that host. A single PD is only for development, CI and
controlled performance tests.

```ini
pd_id     = 1                    # required: raft id of this PD replica
data_dir  = /var/lib/vals/pd1    # required: durable raft state
data_node = 1 @ host:7000        # repeatable: boot membership + push targets
                                 #   (comma-list the addrs of a sharded node;
                                 #   optional ring weight: `1*4 @ ...`)
rf        = 3                    # default 3: authoritative ring RF, pushed to every node
vnodes    = 128                  # default 128: virtual nodes per weight unit on the ring
state_capacity_mib = 256         # default 256: bound on live PD state; restart to change
zone      = events 3 sorted      # repeatable: boot zones (<name> <rf> <sorted|unsorted>)
admin_socket = /run/vals-pd/admin.sock # default: <data_dir>/admin.sock
secret_file  = /etc/vals/secret  # or: secret = <literal> (not both)
metrics      = 127.0.0.1:9465    # optional: Prometheus scrape endpoint
log_level    = info              # optional (reloadable)

# multi-PD (a raft group of 3): identical full voter list on every replica
listen    = 0.0.0.0:7100         # local raft bind address (required with pd_peer)
pd_peer   = 1 @ host1:7100       # advertised address of replica 1 (itself here)
pd_peer   = 2 @ host2:7100       # repeatable; exactly one address each
pd_peer   = 3 @ host3:7100
campaign  = true                 # exactly one replica: campaigns when it bootstraps
                                 #   an empty data_dir (a restart with state does not)
join      = false                # true: an EMPTY data_dir joins an existing group
                                 #   as a new learner (replacing a lost voter)
```

The PD also accepts optional placement and rehome keys (split and merge
thresholds, rehome limits, `opaque_range_maps`, balancing); they are listed
with defaults in [`configuration.md`](configuration.md#pd-placement-and-rehome-settings).
Set them identically on every replica so a leader change keeps the same policy.

**Peers.** Every replica uses the same complete `pd_peer` list, including an
entry for its own `pd_id` at its advertised address. `listen` is the local bind
address and is separate: a wildcard bind such as `0.0.0.0:7100` is never used
as a peer destination. The `pd_peer` list seeds the voter set only when a
replica starts on an empty `data_dir`; after that the committed Raft membership
is authoritative and the list only supplies addresses. A replica whose
`pd_peer` ids differ from the committed voters logs it at `warn` (at `info`
while a learner is being added). A member missing from the list is reached at
the address committed when it was added ([Replacing a lost PD
voter](#replacing-a-lost-pd-voter)). `join = true` requires `pd_peer` and
excludes `campaign`, and like `campaign` it only acts on an empty `data_dir`.
Restarting the campaigning replica with its state intact does not force an
election; it waits for the leader's heartbeat like any follower.

**Boot state is idempotent.** On every start the PD waits briefly for its
recovered state, then proposes only the `data_node` members and `zone` names
that are not committed yet. A restart never duplicates zones or bumps the
config epoch.

### Elections and timing

Raft timing is fixed. The drive loop ticks every 100 ms, the leader sends a
heartbeat every 3 ticks (300 ms), and a follower campaigns after 10–20 ticks
(1–2 s) without one. A leader stall shorter than about a second, such as the
placement search of a 64-range presplit `create-zone`, does not cause an
election.

Elections use Raft pre-vote and check-quorum. A replica that times out first
asks for pre-votes without raising its term, and a voter that heard from a
leader within the last election timeout ignores both pre-votes and votes. So a
replica that cannot hear the leader (an isolated one, or a rejoining one whose
peers still hold stale connections to it) cannot depose a healthy leader, and a
replica without a quorum keeps its term (`role=pre-candidate`
`leader=unknown`). A leader that hears from no quorum for an election timeout
steps down.

Each peer gets its own sender thread with a bounded queue, so a slow or
unreachable peer never stalls the drive loop. Messages that do not fit, or
whose connect or write fails, are dropped (`vals_pd_raft_send_drops`) and Raft
retransmits them. Peer connections set `TCP_USER_TIMEOUT` to 1 s, so a
connection to a host that vanished is replaced about a second later.

### PD state capacity

`state_capacity_mib` bounds the in-memory key/value state that the PD rebuilds
from its Raft snapshot and log; it is not a disk allocation. It must be
nonzero. Proposal admission reserves worst-case growth for queued entries and
refuses a mutation before used plus reserved bytes would cross the bound. A
committed entry that still does not fit on a follower stops that follower's
drive loop, and it needs a restart (with a larger bound) rather than continuing
with diverged state. Monitor `vals_pd_state_capacity_bytes`,
`vals_pd_state_used_bytes`, `vals_pd_state_reserved_bytes` and
`vals_pd_state_headroom_bytes`.

### Sorted zones and range-map pushes

A data node holds a sorted zone from the moment it receives the zone catalog
until its first `SetRangeMaps` push: no sweep reconcile or cleanup runs for
that zone, and its writes get a retryable `StaleEpoch`. The PD sends that push
even when no zone has a range map yet.

A node that boots without reaching a PD leader is held the same way. A
file-backed node reloads its last adopted catalog and holds its sorted zones
until the first `SetRangeMaps` push. An SPDK node keeps no catalog on disk, so
it refuses writes, range deletes and fenced reads to every re-opened sorted
zone with a retryable `StaleEpoch` until it has received both a catalog push
and a range-map push; it logs `holding re-opened sorted zones` at boot.

**`opaque_range_maps`.** Once the PD has committed any opaque-token (V4) range
map, it must keep running with `opaque_range_maps = true` (or the legacy
`VALS_PD_OPAQUE_RANGE_MAPS=1`). Without it the PD withholds every range-map
push, so a data node that restarts stays held, with its sorted-zone sweep
paused and its sorted writes refused, until the PD is restarted with the
setting. The PD logs this once at error level (`every range-map push is
withheld`).

### PD availability and recovery

The PD's Raft state is the authority for data-node membership, zone identity
and configuration, placement epochs and sorted-zone range maps. Each replica
stores its snapshot, log, hard state and Raft membership under
`<data_dir>/raft`, with checksummed, synced file replacement. That is local
durability, not a backup: in a three-PD deployment the other voters are the
independent copies. A failure to persist or apply state stops the PD; it must
be restarted and does not keep serving or pushing.

The PD is not on the read/write path. **Already running** nodes and clients
that share the last pushed membership, catalog and range maps can keep serving
while the PD is down. That is conditional, not a promise that every operation
stays available or that an outage is safe for any length of time. A data-node
failure during the outage can remove a read/write quorum. A data node that
restarts may have only its boot config and local metadata rather than the last
full push. A client that restarts must rediscover routing from a reachable
node. And stale placement cannot react to failures, hot spots or full disks.

While no caught-up PD leader has a Raft quorum:

| Control-plane function | When it stalls and what that means |
|---|---|
| Config push and restart healing | Immediately. Running nodes keep their config, but a restarted, lagging or new node is not brought to the current epochs. |
| `add-node` / `remove-node` | Immediately. Capacity cannot be added and a failed node cannot be routed around; affected ranges run with reduced redundancy and can lose their data-path quorum as soon as enough owners fail. |
| Create/drop/split/merge zone | Immediately. Existing zones keep working on their current config; new zones and manual range changes wait. |
| Automatic split/merge | At the next policy poll or threshold. Merges are cleanup; a blocked split hurts once an unsplit range carries enough load or data to overload its owners. |
| Range rehome and rebalancing | No new plan or commit can complete. Owners keep the last committed map, so skew and reduced redundancy persist; do not assume an in-flight multi-step move will finish. |
| Failure-detector eviction | Automatic eviction does not exist yet (any future version would also need quorum), and manual `remove-node` is unavailable, so the impact depends on the failed node's ranges and each zone's quorum. |
| Admin inspection and mutation | `ping` only proves the process is alive. A follower rejects mutations, and without quorum nothing can commit, so no `ok committed` reply is possible. Inspection without quorum is non-authoritative and may lag (see [`pd-admin.md`](pd-admin.md)). |

Recovery depends on what survived:

1. **Process restart, or moving intact state.** Restart the same `pd_id` with
   the same peer set and its intact `data_dir`; it replays its snapshot and log
   to the committed state before serving. Moving that directory to another
   host is the same operation, but only after the old process is stopped or
   fenced off the network, and only if the replica keeps its advertised
   `pd_peer` address (move the DNS name or IP with it), since the other voters
   address it by that entry.
2. **One replica lost in a three-PD group.** The other two keep quorum and
   elect a leader (about 1–1.8 s in the lab drills below; an observation, not a
   guarantee). Keep both surviving directories. Restore redundancy by
   [replacing the lost voter](#replacing-a-lost-pd-voter): a new replica with
   a **new** `pd_id` joins as a learner, catches up from a snapshot, and takes
   the lost voter's place. The group keeps its log, its state and its cluster
   incarnation, so data nodes need nothing. Never restart a lost voter's
   `pd_id` on an empty `data_dir`, never bootstrap a new group, and never wipe a
   survivor.
3. **Quorum lost, original state still present.** Bring back enough original
   replicas, with their original ids and directories, to recover the same
   group. Do not bootstrap a second group just because the first cannot elect a
   leader right now.
4. **All PD state lost.** The copies on data nodes are serving caches, not a
   complete and consistent PD snapshot, and no tool rebuilds the catalog and
   range maps from them. A fresh `data_dir` bootstraps a *new* authority from
   config; it does not restore the old one. The supported recovery is a
   restore from a backup made earlier with `valsd backup create`: `valsd backup
   restore-pd` seeds new PD directories, `valsd backup load` writes the records
   into empty, freshly provisioned data nodes, and `valsd backup compare`
   checks the result record by record ([`backup-restore.md`](backup-restore.md#restoring-a-backup)).
   The result is a new cluster with a new incarnation holding the data as of
   the backup; later writes are lost. Without a backup there is no supported
   recovery: make no topology changes, and keep every surviving PD directory and
   node image. Losing the host of a single PD, with its only `data_dir`, is this
   case.

Fence the old process before any relocation or disaster recovery. Within one
healthy three-voter group, an isolated minority cannot become leader, and only
the current leader pushes config or accepts mutations. Across groups, the
[cluster incarnation id](#cluster-incarnation-id) fences data nodes.

#### Replacing a lost PD voter

This replaces **one** voter of a three-voter group whose `data_dir` is gone
(or whose host will not come back), while the other two voters still form a
quorum with a caught-up leader. Every step goes through that leader, so
nothing commits without the surviving quorum. It cannot repair a lost quorum
(cases 3 and 4 above).

1. **Fence the lost replica.** Make sure its process cannot run again: power
   off or reprovision the host, or block its Raft port. Once it is removed
   (step 4) the group ignores it while its log is behind, but a surviving copy
   of its old directory still holds a stale view; do not start it.
2. **Start the replacement** on an empty `data_dir` with a **new `pd_id`** the
   group has never used, a copy of a survivor's config (same `data_node`, `rf`,
   `vnodes`, `zone` and secret), and:
   ```ini
   pd_id   = 4
   listen  = 0.0.0.0:7100
   pd_peer = 1 @ host1:7100          # the surviving voters ...
   pd_peer = 2 @ host2:7100
   pd_peer = 4 @ host4:7100          # ... and itself
   join    = true                    # no `campaign`
   ```
   It waits as a non-member (`status` shows `voters=- learners=-`), neither
   campaigning nor answering vote requests. Its first state must arrive as a
   snapshot that lists it as a learner. If the group already tracks that id as
   a member (for example a wiped member restarted under its old id with
   `join = true`), it logs `cannot be recreated`, installs nothing and stops.
3. **Add it as a learner** on the leader:
   `valsd admin -s <leader pd.sock> pd-add-learner 4 host4:7100`. The address
   is committed with the change, so every voter can reach the learner without a
   restart or config edit. Each replica takes a snapshot when it applies a
   membership change, and the leader sends that snapshot to the learner. A
   learner receives the log but does not vote, so the quorum is unchanged.
4. **Replace the voter:** `valsd admin -s <leader pd.sock> pd-replace 3 4`. The
   leader refuses until learner 4 has installed the group's state and is within
   16 entries of the commit index. It then commits two single-voter changes:
   remove voter 3 (the group runs on voters 1 and 2, the quorum it already
   needed), then promote 4. The reply is `ok pd 3 replaced by pd 4 (...);
   voters=1,2,4 learners=-`. The command is safe to rerun: after an
   interruption, a timeout or a leader restart between the two changes it
   continues from the committed state, and once finished it replies `already
   done`. `status` on every replica then shows `voters=1,2,4` and an unchanged
   `incarnation`.
5. **Update every replica's config:** replace `pd_peer = 3 @ ...` with
   `pd_peer = 4 @ host4:7100` on the survivors, and drop `join` on the new
   replica. Running replicas need no restart; the edit keeps later restarts
   correct and silences the `pd_peer` mismatch warning.

Guards: the leader refuses a learner whose id is or was a member (ids are
never reused; the group records each removed id), replacing itself, a second
replacement while one is half done (fewer than three voters), and any change
while another membership change is still being applied. `pd-remove-learner
<id>` drops a learner that will not catch up (for example one with a wrong
address); add a new id instead. A learner already chosen by a half-done
replacement (voter 3 removed, 4 not yet promoted) cannot be removed: either
finish with `pd-replace 3 4`, or add another learner and run `pd-replace 3 <its
id>`, which promotes that one instead, then remove 4. Admin replies follow
[`pd-admin.md`](pd-admin.md); after an `unknown:` reply, check `status` on the
leader and rerun the same command.

Not supported (there is no tool; do not improvise one):

- **Changing the number of voters**, including growing a single PD into a
  group. A single PD refuses these commands.
- **Changing a live voter's address in place.** Keep the address when moving
  intact state, or replace the voter with a new id at the new address.
- **Reusing a removed or lost id**, or restarting a lost voter on an empty
  directory under its old id. A voter's vote and log are part of Raft's
  safety; an empty copy can elect a second leader in an old term.
- **Repairing a lost quorum** by forcing a smaller voter set. Restore enough
  original replicas (case 3) or restore from a backup (case 4).

A snapshot travels in one Raft frame, so the PD state must fit the
transport's queued-byte budget (`VALS_TRANSPORT_MAX_QUEUED_BYTES` on a PD,
512 MiB by default). The procedure has been tested with three PDs and three
data nodes on one machine; it has not yet been exercised across separate hosts.

#### Cluster incarnation id

The first caught-up leader of a PD group picks a random non-zero 128-bit id and
commits it to the PD state (`valsd admin -s <pd.sock> status` shows it as
`incarnation=`). Every control push (`SetMembers`, `SetZones`, `SetRangeMaps`)
carries it, and a leader pushes nothing until the id is committed and applied.
Each data-node shard adopts the id from its first push and persists it (in the
zone superblock, or `<data_dir>/zones/incarnation` for the file backend) before
applying that push. From then on it refuses every control push that carries a
different id **or none**, whatever its epoch. A refused push changes nothing:
the node replies `incarnation mismatch`, the PD logs a failed push and retries,
and the node counts it in `vals_control_incarnation_rejected` and logs a
rate-limited ERROR. `vals_control_incarnation_adoptions` counts adoptions (one
per bound shard, process-wide).

| Event | Incarnation | Operator step |
|---|---|---|
| Bootstrap of a new group | minted once by its first leader | none |
| Leader failover, PD restart, intact-state relocation | kept (it is replicated Raft state) | none |
| Empty-state PD voter replacement | kept: same group | none |
| Data-node restart | kept (persisted per shard) | none |
| Fresh `valsd provision` of a node | cleared; the node adopts the id of its first push | none |
| Restore into a new cluster (`valsd backup restore-pd`) | **replaced**: restore-pd writes a new id | freshly provision every target node empty |

A restored cluster is a new lineage. It must not inherit the old id, or a
surviving or partitioned old authority would be indistinguishable from it. To
move a node to a different group, stop the node and run

```
valsd reenrol -c /etc/vals/node1.conf --incarnation <32 hex digits from the new PD's status>
```

which rewrites the persisted id of every shard (file and SPDK backends; for
SPDK it takes the same exclusive device locks as `valsd node`) and prints the
old and new values. `--clear` instead returns the node to the unbound state, so
it adopts the id of whichever group pushes to it first. There is no online way
to switch a running node to a different id.

What the id does **not** fence, so other fencing is still needed:

- **Two live copies of one PD replica** (same group and `pd_id`, for example a
  moved `data_dir` whose old process still runs) share the id. Stop or
  network-fence the old process before starting the copy.
- **Unbound nodes** accept whichever group pushes first: a freshly provisioned
  or `--clear`ed node. A node is bound once `vals_control_incarnation_adoptions`
  equals its shard count; that counter resets on restart, so after a restart
  look for the `bound to cluster incarnation` log line instead.
- **Other traffic.** Only the three control pushes are fenced. Client
  requests, anti-entropy between nodes and the PD's read-only polls are not.

**Minting blocks pushes.** If a new leader cannot commit the id (for example
because the PD state is at its capacity bound), it logs `cluster incarnation
proposal failed` every 10 s and pushes no config at all. Free PD state capacity
to recover.

### Three-PD reference deployment

The reference layout is one voting PD on each of three data hosts. It was
validated on three lab hosts with 3 SPDK data nodes × 8 shards, RF 3, and 2,000
writes/s of sorted-zone load from a separate client host.

**Generate and check configs.** `scripts/pd_topology.py` renders the replica
configs, either `three` (the reference) or `single-perf` (a single non-HA PD
for performance tests, marked by a comment line). Its `check_pd_confs`
rejects a set unless every replica has the identical full `pd_peer` list
(including itself at its advertised host address), a `listen` that is a
wildcard or that same address and port, distinct hosts, exactly one
`campaign = true`, no `join`, and identical shared settings (`data_node`,
`rf`, `vnodes`, `zone`, `secret_file`). Also run `valsd pd --check-config` on
each host.

**Secret and ports.** PDs and data nodes share one `secret_file` (mode 0600).
The PD Raft hello and every control push carry it; a mismatch shows up as
`DENIED raft peer connection` or `control_denied`. Each PD host needs its Raft
port (`listen`, 7100 here) reachable from the other two PD hosts, every data
node's ports reachable from every PD (for pushes), and its admin socket local
only. Keep each advertised `pd_peer` address stable (a static address or a DHCP
reservation), because changing a live voter's address is not supported.

**Health checks.** On all three replicas, `valsd admin -s <pd.sock> status`
must show the same `leader`, `term` and `incarnation`, `voters=1,2,3`, and
`applied` equal to `commit` when idle (only the leader reports
`caught_up=true`). Each data node must log `bound to cluster incarnation <id>`
once per shard and report `vals_control_incarnation_rejected` 0. A replica
stuck at `role=pre-candidate` cannot reach a quorum.

**Upgrades and rolling restarts.** Restart one replica at a time, followers
first and the leader last, checking `status` on all three between steps.
Follower restarts keep the term; the leader restart costs one election.

**Measured behaviour** (single runs per drill, times from the daemons' logs):

| Event | Control plane | Data path (client writes) |
|---|---|---|
| Follower PD stopped 20 s, restarted | no election; caught up 0.5 s after start | unaffected |
| Leader PD SIGKILLed (systemd restarts it after 10 s) | new leader after 1.09–1.78 s (median 1.25 s over 8 kills, each one election round); a zone created on the new leader reached all 24 shards in 0.35–0.38 s; the old leader rejoined as a follower | unaffected, except one write p99 of 180–200 ms while the new leader re-pushed every config |
| Two PDs stopped (quorum lost) for 48 s | the survivor stays `role=pre-candidate` with `leader=unknown` and an unchanged term; `create-zone` is refused with `not leader` and inspection is `non-authoritative`; with one voter back, a leader after 1.1 s | unaffected |
| Data node restarted while quorum lost | no ring, catalog or range-map push until a leader returned; then all 8 shards were pushed 1.7 s later | re-opened sorted zones are held until those pushes ([above](#sorted-zones-and-range-map-pushes)) |
| All three PDs stopped and restarted | leader within 1.8 s of the start commands; same incarnation, catalog and members; commit +1 (the election entry) | unaffected |
| Leader host rebooted (PD and data node) | new leader after 1.3 s; when the host came back, its PD heard the leader 0.3 s after starting and followed it without an election | client writes nearly stopped while the host was unreachable (see below) |

Lessons from these drills:

- **A black-holed host hurts more than a crashed one.** In these drills a Rust
  client with 5 s RPC deadlines nearly stopped writing while one of three hosts was
  unreachable without sending resets: the surviving nodes saw about 13 applies/s
  instead of 2,000 until the host answered again. A data-node process restart,
  which refuses connections immediately, cost only 1–2 s. Later clients keep
  writing at W 2 through such an outage, but a read still waits a full RPC
  deadline for a silent replica unless the client's failure detector is on. In
  a later drill that black-holed one of three hosts for 30 s, a client with the
  failure detector and a 50 ms hedge showed no throughput dip, and one without
  the detector managed about 17 operations/s. The recommended client setup is
  in the "Replica outages" section of the `vals-client` crate docs.
- **Control pushes to a dead host.** Each pushed address has its own thread, and
  the restart probe and range-stats poll run off the push loop, so a host that
  drops off the network delays only its own pushes. (Before this was changed,
  one unreachable host delayed a new zone reaching the healthy nodes by 23 s.)
- **A rejoining PD** whose peers still hold stale connections to it loses their
  first messages until those connections are replaced, which `TCP_USER_TIMEOUT`
  does within about a second. Pre-vote with check-quorum keeps the current
  leader in place meanwhile.

## Data-node deployment profiles

`profile` selects a write and index policy for the whole process. It is
restart-only and logged before any zone engine opens. `vals_profile_info` shows
the selected profile; `vals_blind_append_enabled` and
`vals_winner_directory_enabled` show what is actually in effect.

| Profile | Unconditional Apply/BatchApply | Point index | Open/recovery |
|---|---|---|---|
| `capacity` (default) | Resolves the current maximum version before acknowledging; a flushed positive may read a run block. | Out of core. | Replays the log tail and opens run metadata without building an in-RAM directory of the data set. |
| `latency` | Blind-appends an admitted last-writer-wins candidate and returns optimistic `Won`; clock-skew, GC-floor, admission, append and requested-sync checks are unchanged. | Compact collision-safe winner directory in RAM. | Scans all recovered runs and the replayed memtable before serving; a failure aborts engine open. |

The latency profile couples both features; there is no half profile. Its
directory costs RAM: on a large hardware data set the process RSS grew by
**62.9 bytes per physical key** (a physical key is one replica's copy, so RF 2
doubles it). Size from that plus the rest of the process budget, key growth,
zones, shards, caches and allocator slack. `vals_winner_directory_keys` and
`vals_winner_directory_bytes` show logical coverage and allocated directory
capacity (the latter is not a full RSS measure). Opening an engine rebuilds the
directory by reading the whole data set, so budget that time into restarts and
rolling maintenance.

Both profiles use the same wire and on-disk formats, and replicas may mix
profiles during a rollout. Replica-level clients must count both `Applied::Won`
and `Applied::Lost(_)` as acknowledgements of an unconditional write. In
latency mode `Won` means "candidate accepted", not "proven winner"; only a
maximum-version read decides the winner. CAS always resolves.

Switching a node from latency back to capacity is a restart; no on-disk
rewrite is needed. Moving a node that ran the latency profile to an older
binary that predates it is unsafe, because that binary's run precedence can
expose a losing write; rebuild the node from peers instead (or first run a
full compaction to one version per key with no uncovered log tail). Roll
latency out one replica at a time and watch these through a restart before
continuing:

- write probes: `vals_apply_run_reads`, `vals_batch_apply_run_reads`,
  `vals_apply_probe_runs` and `vals_apply_probe_blocks` (close to zero for
  latency-profile unconditional writes);
- directory size: `vals_winner_directory_keys` and
  `vals_winner_directory_bytes`;
- repair: `vals_ae_dirty_buckets`, oldest dirty age, rebuild
  requests/retries/errors, AE sweeps and errors;
- space and recovery: used and free segments, resident runs, merges, admission
  errors, process RSS and swap, and restart time.

If a winner-directory rebuild fails at runtime, the whole process degrades
safely: the failing engine drops its directory, every engine and every open
connection switches to resolving writes on its next request, and both
effective gauges drop to `0` while `vals_profile_info{profile="latency"}` stays
`1`. This lasts until the process restarts. Out-of-core maximum-version reads
remain correct. Alert when a latency process shows either effective gauge at
zero, fix the device or rebuild error, and restart; engine open must rebuild
every directory successfully before serving.

Capacity stays the default because of the latency profile's RAM and
startup cost. Use capacity in particular for a large bulk load of mostly unique
keys unless a measured overwrite or read workload justifies latency: at RF 2,
24 billion logical keys are 48 billion physical keys, so the latency directory
alone would need about 3.0 TB of RAM across the cluster, and every restart would
rebuild it. Capacity's write path needs no run read for a new key whose blooms
say "absent"; confirm that with the write-probe metrics above. See
[`blind-append-lww.md`](blind-append-lww.md) and
[`out-of-core-index.md`](out-of-core-index.md).

## On-disk format and compatibility

Byte layouts are in [`formats.md`](formats.md); the history of format changes
before the first public release is in [`format-history.md`](format-history.md).
What matters operationally:

- **Runs.** New flushes and merges write run format v6, which carries a
  per-block version bound and range-tombstone records. The engine also reads
  older run formats (v1–v5), and merges rewrite them as v6. Block size
  and bloom parameters are stored per run, so changing them affects only new
  runs and needs no rewrite.
- **Newer reads older; older refuses newer.** A binary that meets a format it
  does not know refuses to open it (for runs, `unsupported run version`, exit 1)
  before writing anything, so a failed downgrade leaves the data untouched. Once
  a newer format has been written, the way back to an older binary is to
  rebuild the node from its peers. The same holds for the zone superblock and
  for PD Raft state: do not run an older PD against a directory a newer PD has
  written to.
- **`VALS_RUN_WRITE_VERSION`** (read once at startup; `vals_run_write_version`
  shows the active value) can keep a node writing the older v5 (`5`) or v4
  (`4`) run formats as a rollback aid. Leave it unset. While runs are written
  below v6 the node refuses range deletes, and any value other than `4`, `5` or
  `6`, including an empty string, logs an error and writes v4.
- **SPDK layout geometry is fixed at provisioning** (see the [node config
  notes](#node-config-reference)). Provisioning, and every other path that
  formats a region, zeroes every segment header first, so records left on a
  reused drive by an earlier layout can never be replayed as live data.

## Memory sizing

The capacity profile keeps the point index out of core, but each zone engine
still holds per-run metadata and an engine-wide key filter in RAM. With the
default settings:

- **Run fences** (one first-key copy per 32 KiB run block): about 1.1 bytes per
  entry for 148-byte keys and 0.4 bytes for 70-byte keys, plus 8 bytes per block
  for the range-delete version bound.
- **Run blooms** (10 bits per key): about 1.25 bytes per entry.
- **Key filter** (12 bits per key in each sub-filter, doubling capacity per
  growth step): starting from empty with a 16M-key first sub-filter, about
  1.69 bytes per inserted key and a 1.75% compound false-positive rate at 440M
  keys per engine (5 sub-filters). Each further full sub-filter adds about
  0.4% (2.1% at 880M keys per engine, 2.5% with 7 sub-filters). Right after a
  growth step the new, doubled sub-filter is allocated and touched quickly, so
  RAM per key is about 3.1–4.5 bytes just after a step and falls toward 1.5
  bytes as keys double (about 2.1 on average). **Plan RAM for the peak.**
  After a restart the first sub-filter is sized from the engine's key count
  (×1.25), so a single filter gives about 0.1–0.4% false positives.
- **Latency profile:** add its winner directory ([Data-node deployment
  profiles](#data-node-deployment-profiles)).

The RAM admission budget defaults to 70% of `MemTotal`
(`ram_budget_bytes`, see [`configuration.md`](configuration.md#node-resource-and-maintenance-settings)).

**Read and overwrite cost of 32 KiB blocks.** A run probe reads one whole
block, about 19–24 KiB from the device, and spends about 2 µs of CPU per
probed block, mostly on the block CRC. Range scans decode only the entries a
page returns, but a page still reads each overlapping run's whole block.
Overwrite-heavy workloads pay most, because every write to a key no longer in
the memtable resolves its old version from a run block: with 20k hot keys,
32 KiB blocks were 15–20% slower than 4 KiB blocks. Such deployments can trade
RAM for speed with `VALS_RUN_BLOCK_TARGET_BYTES` of 8192–16384 (default 32768,
clamped to 4096–262144, new runs only; it is listed with the diagnostic
controls in [`configuration.md`](configuration.md#diagnostic--a-b-controls-do-not-promote-to-public-config)).
`vals_read_probe_bytes` and `vals_apply_probe_bytes`, divided by
`vals_read_probe_blocks` and `vals_apply_probe_blocks`, give bytes per probed
block.

**Key filter rebuilds.** Engines rebuild their key filter in the background
when its chain of sub-filters grows. Permits go to the highest active
false-positive rate first (ties to the longest wait; waiters not refreshed for
60 s are skipped). `VALS_KEY_FILTER_REBUILD_CONCURRENCY` (default 1) caps
simultaneous rebuilds. Engines at or above
`VALS_KEY_FILTER_REBUILD_URGENT_FPR_PPM` (default 25000, i.e. 2.5%; 0 disables;
other values are clamped to 1000–500000) may use up to
`VALS_KEY_FILTER_REBUILD_URGENT_EXTRA` extra permits (default 1, at most 8).
These bound concurrent rebuilds, not chain length, which can keep growing while
an engine waits. One extra rebuild of a 440M-key engine needs about 0.73 GiB
(440M × 1.1 × 12 bits); check measured peak RAM and the worst false-positive
rate before relying on them.

**Memory metrics.** These are estimates of specific structures, not RSS; do
not add overlapping ones together.

| Metric | Meaning |
|---|---|
| `vals_run_fence_resident_bytes` / `vals_run_fence_payload_bytes` | Allocated vs. payload bytes of open runs' block indexes. |
| `vals_run_filter_resident_bytes` / `vals_run_filter_payload_bytes` | Allocated vs. payload (bloom words only) bytes of run blooms; the difference is capacity slack. |
| `vals_run_metadata_resident_bytes` | All open-run metadata, including the two rows above. Seed-held indexes and merge-worker copies are not included. |
| `vals_memtable_bytes_total` | Logical key-plus-entry estimate that drives flush thresholds. |
| `vals_memtable_allocated_bytes_total` | Estimated allocated mutable-index bytes (overlaps the previous row; do not add them). |
| `vals_flush_protection_bytes` | Allocated capacity of the flush-protected segment sets. |
| `vals_merge_worker_metadata_bytes` | Live fences and blooms held by background merges, through commit or cancellation. |
| `vals_key_filter_bytes` / `vals_key_filter_resident_bytes` | Allocated and estimated touched key-filter bytes, including rebuild shadows and retired sub-filters (reported separately by `vals_key_filter_rebuild_*` and `vals_key_filter_retired_*`). |
| `vals_key_filter_capacity_keys`, `vals_key_filter_inserted_keys` | Active-generation capacity and inserted keys, to tell growth slack from stale bits. |
| `vals_key_filter_max_sub_filters`, `vals_key_filter_max_est_fpr_ppm`, `vals_key_filter_max_uncovered_runs` | Maxima over local engines (not sums, and not necessarily the same engine). Runs not covered by the filter are always probed individually. |
| `vals_key_filter_rebuilding`, `vals_key_filter_rebuilds_{started,completed,abandoned}`, `vals_key_filter_rebuild_peak_bytes`, `vals_key_filter_rebuild_{wait,open}_seconds_max`, `vals_key_filter_rebuild_wait_seconds_total`, `vals_key_filter_rebuild_urgent_grants` | Rebuild progress, waits and urgent grants. The peak-bytes gauge sums per-engine high-water marks and is not a node-wide simultaneous peak; track `vals_key_filter_bytes` over time for that. |
| `vals_allocator_in_use_bytes` / `vals_allocator_free_bytes` | glibc `mallinfo2` in-use (`uordblks + hblkhd`) and free (`fordblks`) bytes for the whole process, sampled at most every 4 s off the shard threads. Neither is RSS, and in-use overlaps every other heap gauge. |

Most of these gauges aggregate the node's engines on the sweep cadence.

**Returning freed memory to the OS.** On Linux/glibc nodes, periodic
`malloc_trim(0)` is off by default. A RAM-constrained host can set
`VALS_MALLOC_TRIM_SECS` (restart-only, nonnegative seconds; `0` or unset
disables it; an invalid value fails startup and `--check-config`). A positive
value starts a `vals-malloc-trim` thread at nice 19 that waits that long after
each call; it never runs on serving threads, and `valsd pd` does not start it.
It returns free arena pages, not live metadata. In one hardware comparison
trimming cost 3–6% of acknowledged write throughput, and each call took
290–420 ms; it takes the allocator lock, so it can still slow other threads.
Watch RAM and acknowledgement latency when enabling it.
`vals_allocator_trim_calls_total`, `vals_allocator_trim_ns_total`,
`vals_allocator_trim_max_ns` and `vals_allocator_trim_rss_returned_bytes_total`
(an estimate from RSS drops) report its work.

## Range deletes

Sorted zones support range deletes (`delete_range_in`; design in
[`range-deletes.md`](range-deletes.md), API in
[`getting-started.md`](getting-started.md)). Zones that never use them pay
nothing measurable beyond the 8-byte per-block version bound counted above.

- **Every replica must support them.** A coordinator refuses a range delete
  with `Unsupported`, before stamping a version, while any target replica lacks
  the range-tombstone hello feature (bit 3). A client that does not advertise
  that bit keeps working on zones without range tombstones; its scan whose
  window meets one fails with `Err("range tombstones present: client lacks
  RANGE_TOMBSTONES")` rather than returning deleted keys.
- **Fragments are bounded.** With `gc_grace = 0` (the default) range-tombstone
  fragments are kept forever, like point tombstones, up to a fixed 65,536 per
  zone engine; beyond that a slice is refused
  (`vals_range_delete_refused_total{reason="cap"}`). Deleting a growing prefix
  `[base, cursor)`, as a queue does, collapses into one fragment; deleting
  disjoint spans adds one each. There is no fragment-count gauge yet.
- **Run format.** A node writing runs below v6 (`VALS_RUN_WRITE_VERSION`)
  refuses range deletes with a per-slice error, counted in
  `vals_range_delete_refused_total`.
- **Metrics.** `vals_range_deletes_applied_total`,
  `vals_range_delete_refused_total`, the fold counters `vals_rt_fold_*`, the
  scan counters `vals_scan_rt_*`, `vals_rt_max_tree_built_blocks_total`,
  `vals_rt_covered_dropped_total`, `vals_rt_collected_total`,
  `vals_rt_dominated_dropped_total`, `vals_ae_rt_exchanges_total` and the
  per-engine record gauge `vals_engine_rt_records` (range-deletes.md §8).

Backups and dumps of zones that hold range tombstones use their own record
types; see [`backup-restore.md`](backup-restore.md) and [the dump
format](#dump-file-format-vdump).

## Offline SPDK provisioning

> **`valsd provision` destroys all data on every configured drive.** Run it
> only on a new or deliberately disposable node, with its daemon stopped.

Record each controller's serial number independently (from its label or your
inventory), then name every configured PCI address with its serial:

```sh
valsd provision -c /etc/vals/node1.conf \
  --device '0000:a1:00.0=SERIAL_FROM_CONTROLLER_LABEL_OR_INVENTORY' \
  --confirm-destroy node-1
```

Give `--device` once for every `pci` line; the set must match the config
exactly. The command attaches and holds every controller before writing, reads
each controller's serial through SPDK, and compares it byte for byte. A
missing or extra device, a serial mismatch, the wrong `--confirm-destroy` node,
an attach failure or an invalid layout aborts before anything is written. The
SPDK controller claim gives the command exclusive ownership while it runs; if
the daemon or another SPDK process owns any configured controller,
provisioning fails. On success it writes the on-device layout and default zone,
without binding listeners or starting node services.

Then remove any `format` line from the node config and start the node
normally. Reprovisioning uses the same offline workflow; there is no
format-at-startup switch.

### Device deallocation

`device_dealloc_segments_per_tick` (default **0**) controls NVMe deallocation
(DSM). At 0 nothing is deallocated. A nonzero value is **experimental and not
for production**: in hardware tests it gave no throughput gain, and the only
long run with runtime deallocation failed correctness checks for a reason not
yet found. When enabled:

- after verifying every serial, provisioning deallocates the **whole namespace
  of every configured drive**, including space outside vals regions, with
  bounded DSM commands before writing the layout. A deallocation error aborts
  provisioning; a device that does not support it logs a warning and
  provisioning continues. The file backend never deallocates.
- at runtime, a freed value-log or run segment becomes eligible only after a
  later manifest is durably written, and reuse removes it from the pending set.
  Up to that many segments per zone are deallocated per flush/snapshot tick, in
  bounded batches on the maintenance tick, not on the write path; errors are
  advisory. The pending set is in memory only, so segments freed shortly before
  a restart are not deallocated until they are reused and freed again.

`vals_device_dealloc_*` metrics report commands, ranges, bytes, errors,
unsupported attempts and total latency.

## Config reload (SIGHUP)

`SIGHUP` makes a running node or PD re-read its config file (the path given
with `-c`) and apply the reloadable keys live, with no restart and no dropped
connections:

- **`log_level`** (node and PD) retunes the logger, for example to switch a
  misbehaving node to `debug` and back.
- **`sweep`** (node only) retunes the anti-entropy cadence, and with it the
  compaction and flush cadences derived from it when `compact_interval_ms` and
  `flush_interval_ms` are unset. Each background loop picks up the change on
  its next tick, so the old period bounds the delay.

Every other key is restart-only. If it changed, the daemon logs a warning
(`` `<key>` changed but is not reloadable; keeping the running value (restart
to apply)``) and keeps the running value. That includes `profile`, `backend`,
`data_dir`, `listen`, `node_id`/`pd_id`, peers, `rf`, `gc_grace`, the secret,
the geometry keys, `pci` and `spdk_*`, `metrics`, `admin_socket`, the typed
node settings and PD placement settings, and every other PD key.

A reload that fails to parse or validate is logged (`config reload failed …;
keeping the running config`) and changes nothing. Each reload ends with one
summary line, for example:

```
node 1: config reloaded (applied: ["log_level", "sweep"]; ignored non-reloadable: ["region_mib"])
```

Signal the process with `kill -HUP <pid>`, or under systemd add
`ExecReload=/bin/kill -HUP $MAINPID` to the unit and run `systemctl reload
vals-node`.

## Administering a running cluster

`valsd admin -s <socket> <command>` talks to one PD replica's admin socket,
which is local to that PD's host. File permissions on the socket are the only
access control; mutations still go through Raft.

| Command | Effect |
|---|---|
| `ping` | liveness check |
| `status` | this replica's view: `pd`, `role`, `term`, `leader`, `commit`, `applied`, `caught_up`, `incarnation`, `voters`, `learners` |
| `members` | committed membership: `id weight addrs` per line |
| `zones` | committed catalog: `id name rf ordering` per line |
| `ranges <zone>` | committed range boundaries of a zone, one `hex=<bytes>` key per line |
| `add-node <id> <weight> [<addr>,...] [--force]` | grow the cluster (the addresses make the node reachable cluster-wide, with no restarts anywhere) |
| `remove-node <id> [--force]` | shrink the ring (data re-replicates through anti-entropy) |
| `create-zone <name> <rf> <sorted\|unsorted> [presplit=K \| presplit=key,key,...]` | create a zone and push it to all nodes; a sorted zone can be presplit into K first-byte ranges or at explicit canonical key boundaries |
| `drop-zone <id\|name>` | tear a zone down everywhere |
| `split-zone <id\|name> <key> [canonical]` | split a sorted zone's range at `key`; `canonical` spreads an empty range only (populated ranges need a rehome) |
| `merge-zone <id\|name> <key>` | merge the sorted range that begins at `key` into its predecessor |
| `export-state <absolute-path>` | write the complete PD state (confirmed leader only) to a new file on the PD host; used by `valsd backup` |
| `pd-add-learner <pd_id> <addr>` | add a new, empty PD replica (started with `join = true`) as a non-voting learner |
| `pd-replace <old_pd_id> <new_pd_id>` | replace a lost PD voter with a caught-up learner (safe to rerun) |
| `pd-remove-learner <pd_id>` | drop a learner that was never promoted |

Unless every node reports `gc_grace = 0s`, `add-node` and `remove-node` first
check that every member that stays answers, has finished over-replication
cleanup and has handed off every copy of a key it no longer owns. Otherwise
the change could make a node the owner of a copy whose delete was already
collected. A node that `add-node` brings in must hold no data, so give its
addresses to `add-node` and wipe a removed node before it rejoins. If the
check fails, the command fails with `err: membership change
refused, nothing applied`, naming the node, the zone and sample keys, or the
member that did not answer. `--force` skips the check
([`backup-restore.md`](backup-restore.md#consistency-contract)).

Each PD replica has its own socket, and only the current, caught-up leader
accepts mutations. How to find the leader, what each reply and exit code means
(`ok` 0, `err:` 1, `unknown:` 3), authoritative versus non-authoritative
inspection, and how to handle an ambiguous outcome are in
[`pd-admin.md`](pd-admin.md). Boot membership and boot zones from the config
are the exception: startup keeps retrying them until a leader commits them.

The PD validates every mutation again when it proposes it. Zone names must be
unique, nonempty and contain no whitespace. A zone's RF must be at least 1 and
no more than the current eligible membership (or its explicit scope). A member
cannot be removed if that would take membership below the global RF, break any
zone's scope, or leave a zone under-replicated. There is no implicit degraded
mode: if you intend to reduce durability, change or drop the affected zones
first.

### Node admin socket

A data node with `admin_socket` set answers `ping`, `zone-stats` (one CSV line
per shard and zone: `shard,zone_id,used_segments,free_segments,live_keys,resident_runs`)
and `dump` (below). The socket is off unless `admin_socket` is set, and it is
separate from the PD's; do not point both at the same path. It serves
connections concurrently, so a `ping` during a long dump answers at once.

## Backup and restore

For a cluster-wide, deduplicated, versioned backup use `valsd backup create`,
and restore with `valsd backup restore-pd` and `valsd backup load`; see
[`backup-restore.md`](backup-restore.md). The per-node dumps below are raw
value-log exports, not backups.

## Bulk vlog export (`valsd dump`)

`valsd dump -c <node-config> (--zone <name> | --zone-id <id>) --out <path>
[--skip-tombstones] [--parallel <N>]` exports one zone's value log to a flat
`.vdump` file at sequential device speed. It uses the same sequential scan as
boot recovery (no merge, RPC or quorum) through its own handle on the zone's
`.vlog`, so it is safe to run against a live file-backed node. It is a
point-in-time capture: a log tail written since the last sync may be missing.

- `--zone <name>` resolves the name through the node's persisted catalog
  (`<data_dir>/catalog`); `--zone-id <id>` skips that lookup. A missing zone is
  an error that lists the zone ids on disk.
- `--parallel <N>` (default 1, at most 64) scans with `N` per-segment workers,
  each with its own device handle. Output is still emitted in log order, so a
  parallel dump is **byte-identical** to a serial one. To capture a whole
  cluster, run one `valsd dump` per node at the same time; nodes and zones are
  independent, so the total time is about one node's.
- **How much parallelism.** On one NVMe drive with about 302 GiB (2.53 billion
  records): `1` = 0.67 GiB/s (7.5 min), `4` = 1.07 GiB/s, `8` = 2.07 GiB/s,
  `16` = 2.98 GiB/s (1.7 min). Even serial is about 74× faster than reading the
  same records through scans. For an **idle** drive use `--parallel 8` to `16`.
  The default stays 1 because the same option drives the live dump below, where
  more parallelism takes device bandwidth from serving.
- `backend = mem` has nothing on disk to dump. `backend = spdk` requires
  `--zone-id` (zone names are not stored on the device), a `--features spdk`
  build, and an idle drive that no daemon is serving, because the command
  attaches the controller itself. To dump a **running** node (SPDK or file),
  use the node admin `dump` below.

### Live-node dump (node admin socket)

A running node with `admin_socket` set answers a `dump` command that dumps a
zone through the daemon's own device, so it works for both file and SPDK
backends. The scan runs on a dedicated thread over a sibling of the serving
handle (a second SPDK queue pair or a duplicated file descriptor) and never
touches the serving handle.

```
valsd admin -s /run/vals-node/admin.sock ping
valsd admin -s /run/vals-node/admin.sock dump <zone-id-or-name> <out-path> \
    [skip-tombstones] [parallel=N] [rate_mib=N] [shard=S]
```

- `<zone-id-or-name>` is a numeric zone id or a catalog name, and `<out-path>`
  is on the **node's host**. `parallel=N` sets per-segment workers
  (byte-identical output, at most 64); `rate_mib=N` caps the total device read
  rate in MiB/s (a positive integer), shared by all workers and covering the
  whole scan; `shard=S` (default 0) picks the shard of a sharded node. Without
  `rate_mib=` reads are unlimited. The reply is `ok <summary>` or `err:
  <reason>`.
- The same point-in-time caveats apply: records not yet synced to the device
  are missing. A concurrent compaction can race the scan; the dump then fails
  with `err:` and can simply be retried. A graceful shutdown waits for an
  in-flight dump before detaching the device.
- **Effect on serving.** Dumping a node under a sustained write load kept it
  serving correctly with no client errors. The cost grows with dump duration
  times its share of device bandwidth: a multi-minute 302 GiB dump at
  `parallel=4` cut serving throughput by about 25% (from device bandwidth, not
  CPU), while short dumps (about 15 s, 10 GiB) showed no measurable dip at
  `parallel=1` or `4`. A recommended `rate_mib=` value has not been measured
  yet. Until then keep live dumps at `parallel=1` or `2`, and set `rate_mib=`
  conservatively when a bounded read rate matters more than finishing fast. The
  higher parallelism recommended for idle drives does **not** apply to a
  serving node. Concurrent dumps to different output paths run at the same
  time, each on its own thread.

### Dump file format (`.vdump`)

A dump is **raw**, not deduplicated: every record appears in log order, so a
replayer that applies records in file order (the later record wins for each
key) rebuilds the zone's final state, overwrites and deletes included.

The file starts with the magic `b"VDUMP\x01"` and `[u8 name_len][zone name]`,
followed by records until EOF. A put is `[u32-le key_len][key][u32-le
val_len][value]`. A **tombstone** is `[u32-le key_len][key][0xFFFF_FFFF]`
with no value bytes (a value length no real value can have). Give a put-only
replayer that does not understand this sentinel a dump made with
`--skip-tombstones`, which leaves tombstones out.

**Range tombstones** ([`range-deletes.md`](range-deletes.md)): a zone that
holds range tombstones dumps with the magic `b"VDUMP\x02"`; a zone without them
keeps the `\x01` layout byte for byte, and a `\x01`-only replayer refuses a
`\x02` file on its magic. Before streaming, the dump reads the range tombstones
of the zone's runs and of its log tail and writes their canonical fragment set
first, one **range record** per fragment: `[u32-le lo_len | 0xFFFF_FFFF][lo]
[0xFFFF_FFFE][u32-le hi_len | 0xFFFF_FFFF][hi][u128-le version]`
(`0xFFFF_FFFF` means unbounded, and `0xFFFF_FFFE` in the value-length position
marks a range record). `version` is the packed engine version
(`hlc << 64 | writer`). The dump then **skips every log record a fragment
covers** (`record version <= fragment version`), because value-log GC can
move a covered value after its range tombstone and replaying it in place would
bring it back. So a replayer applies the range records first (for example as
range deletes, after the wall clock has passed their versions), then the point
records in file order; every point record is newer than any range that holds
its key. The summary line counts the range tombstones and the skipped covered
records. On a live node, a range delete applied after the dump has read the
fragment set is missing from the dump, like any record written after the scan
passed it.

## systemd

File-backed data nodes and PDs run as an unprivileged user that owns their
data. Create it once per host, with the config directory and the secret
readable only by root and that user. Generate the secret on the first host
and copy that file to every other node and PD host with the same owner and
mode:

```sh
sudo useradd --system --home-dir /var/lib/vals --shell /usr/sbin/nologin vals
sudo install -d -o root -g vals -m 0750 /etc/vals
openssl rand -hex 32 | sudo install -o vals -g vals -m 0600 /dev/stdin /etc/vals/secret
sudo install -o root -g vals -m 0640 node.conf pd.conf /etc/vals/
```

```ini
# /etc/systemd/system/vals-node.service
[Unit]
Description=vals data node
Wants=network-online.target
After=network-online.target
# Restart-storm guard: at most 4 starts per 30 min, then the unit stays failed.
StartLimitIntervalSec=1800
StartLimitBurst=4

[Service]
User=vals
Group=vals
# Files and sockets valsd creates are owner-only: data, dumps, PD state
# exports, and the admin socket (srwx------), so only this user and root can
# send admin commands.
UMask=0077
# /var/lib/vals, mode 0700, owned by User: put data_dir under it.
StateDirectory=vals
StateDirectoryMode=0700
# /run/vals-node, mode 0700, owned by User: admin_socket = /run/vals-node/admin.sock
RuntimeDirectory=vals-node
RuntimeDirectoryMode=0700
ExecStart=/usr/local/bin/valsd node -c /etc/vals/node.conf
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=10s
# Exit 2 is a config/usage error and exit 1 a startup/recovery failure (e.g.
# on-disk corruption): restarting cannot fix either. Exit 75 (integrity
# fail-fast) and crashes restart.
RestartPreventExitStatus=1 2
# SIGTERM drains and shuts down gracefully. A stop has taken over 30 s while
# a node was seeding a replica; systemd sends SIGKILL after this timeout.
TimeoutStopSec=300

[Install]
WantedBy=multi-user.target
```

```ini
# /etc/systemd/system/vals-pd.service
[Unit]
Description=vals PD
Wants=network-online.target
After=network-online.target
StartLimitIntervalSec=1800
StartLimitBurst=4

[Service]
User=vals
Group=vals
UMask=0077
StateDirectory=vals
StateDirectoryMode=0700
# admin_socket = /run/vals-pd/admin.sock
RuntimeDirectory=vals-pd
RuntimeDirectoryMode=0700
ExecStart=/usr/local/bin/valsd pd -c /etc/vals/pd.conf
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=10s
RestartPreventExitStatus=1 2
TimeoutStopSec=300

[Install]
WantedBy=multi-user.target
```

Each unit has its own runtime directory, because systemd removes a unit's
runtime directory when the unit stops. A host with more than one data node
uses a template unit instead (`vals-node@.service` with
`RuntimeDirectory=vals-node-%i` and `-c /etc/vals/node-%i.conf`). An older
deployment that ran as root needs `chown -R vals:vals` on its data
directories before it switches to `User=vals`.

With these units, run admin commands as the daemon user or as root, for
example `sudo -u vals valsd admin -s /run/vals-pd/admin.sock status`. Run
`valsd backup create` the same way, with an output directory that user can
write: the PD writes its state export into that directory, owner-only, and
the backup tool reads it from there.

**SPDK data nodes** run as root (non-root vfio needs the extra setup in
[`SPDK.md`](SPDK.md)), so leave out `User=`, `Group=` and `StateDirectory=`
and keep `UMask=0077` and the runtime directory. Do not add sandboxing that
hides devices or shared lock files from the node: `PrivateDevices=`,
`DevicePolicy=`, `PrivateTmp=`, `ProtectSystem=strict` or a reduced
`CapabilityBoundingSet=`. The node needs `/dev/vfio` and `/dev/hugepages`,
and its exclusive device locks (`/run/lock/valsd-spdk-*.lock` and SPDK's
`/var/tmp/spdk_pci_lock_*`) must be the same files that `valsd provision` and
`valsd dump` see when you run them from a shell; a private `/tmp` and
`/var/tmp` would hide SPDK's lock from them. Because the node runs as root,
its admin socket is root-only, and its `dump` command can write any path:
leave `admin_socket` unset on SPDK nodes unless you need live dumps.

Set the log level with `log_level` in the config file rather than
`VALS_LOG` in the unit. For SPDK nodes, add hugepage and vfio setup (see
[`SPDK.md`](SPDK.md)) as `ExecStartPre` steps or a separate oneshot unit. The
SPDK shim unlinks its hugepage map files as soon as it creates them, so the
kernel frees every hugepage when the process exits, however it exits, and a
restart needs no cleanup step.

On hosts with unattended-upgrades and needrestart, a library upgrade can
restart vals services at an arbitrary time. Exclude them with
`/etc/needrestart/conf.d/90-vals.conf` containing
`$nrconf{override_rc}{qr(^vals)} = 0;` (this matches every unit whose name
starts with `vals`). Keep unattended-upgrades on and schedule vals restarts
yourself.

### Exit codes

| Code | Meaning | Restart? |
|---|---|---|
| 0 | Clean shutdown after `SIGTERM`/`SIGINT`, or a passing `--check-config`. | no |
| 1 | Startup or recovery failure, for example on-disk corruption, a format the binary does not know, or an SPDK device already owned by another process. | no: it fails again until fixed |
| 2 | Config or usage error (including a failed `--check-config`). | no |
| 75 | Integrity fail-fast (`EX_TEMPFAIL`, data nodes only), below. | yes |

Crashes (signals) should restart. `valsd admin` exits 0 for `ok`, 1 for `err:`
(or when it cannot reach the socket) and 3 for `unknown:`; see
[`pd-admin.md`](pd-admin.md).

## Integrity checks

A node checks its own data at several points and reacts differently to damage
in RAM (fail fast and restart) and damage on disk (fail the operation, keep the
evidence, keep serving everything else).

### Integrity fail-fast (exit 75)

`valsd node` exits with code **75** when an in-RAM integrity check keeps
failing, so that a supervisor restart can replay the intact value log. This
relies on the unit's `Restart=on-failure`. Without it, a node whose RAM
corrupted a memtable would retry the same flush forever and stall ingest.

The trigger is the front-coded memtable guard refusing to flush the same block
of the same zone engine for `integrity_fail_fast_attempts` consecutive flush
attempts (default 3) or for `integrity_fail_fast_secs` (default 30 s, at least
two attempts), whichever comes first (see
[`configuration.md`](configuration.md#integrity-fail-fast)). A second in-RAM
check feeds the same streak: an off-core flush whose encoded run blocks do not
decode back to the entries the memtable guard attested (or are not strictly
ascending) fails before publication, and the same memtable block failing on
consecutive attempts counts toward the streak. Merge-output encode failures
never fail fast; the merge retries from its on-disk inputs.

What happens and what operators see:

- **Before the exit**, every failed attempt logs an ERROR
  `memtable flush aborted owner=… front-coded index integrity mismatch
  block_first_key=[…] entries=N packed_bytes=M diagnosis=…`. A single-bit
  diagnosis looks like
  `diagnosis=single-bit region=key key_index=0 offset=19 actual=0x7f
  expected=0x6f xor=0x10 candidates=1 window@11=[…]`. `region` is one of `key`,
  `key_len`, `entry.segment`, `entry.offset`, `entry.len`, `entry.seq`,
  `entry.version`, `block_key` or `checksum`. `diagnosis=unexplained` means no
  single-bit flip explains the mismatch, which points to multi-bit damage or a
  software bug. `vals_memtable_integrity_failures_total` and
  `vals_snapshot_errors` rise.
- **Encode verification** logs an ERROR `run encode integrity mismatch
  path=flush|merge check=… block=B entry=E source_first_key=[…] …`, followed by
  the usual `off-core flush write failed (retrying next tick)` or compaction
  WARN. `check` is `order` (the encoded key is not strictly above the previous
  one; both keys are logged in hex), `source-hash` (the decoded entries do not
  hash to the source), `decode` (the block does not parse) or `index`. For
  `source-hash`, `stage=encode` means the encoded bytes differ from the source
  entry (with a byte-wise diff, and `diagnosis=single-bit region=key|entry.segment|
  entry.offset|entry.len|entry.seq|entry.version|entry.tombstone …` when one bit
  differs), and `stage=source`/`source-copy` means the source copy itself
  changed after it was attested (a single-bit search, with offsets as in the
  memtable diagnosis). `vals_encode_integrity_failures_total{path="flush"|"merge"}`
  counts every aborted build, including retries, and must stay 0. Nothing is
  published: the run's segments are freed, the memtable is untouched, and the
  next tick retries.
- **The trigger** logs one ERROR, `persistent in-RAM memtable integrity
  failure … failed its checksum or its flush encode verification … failing
  fast …`, with the attempt count, the elapsed time, the single-bit signature
  and the evidence line. It increments `vals_integrity_fail_fast_total`, but a
  scrape catches that only during the short shutdown, so the log and the exit
  status are authoritative. valsd then logs `fail-fast: graceful shutdown
  without a final memtable flush, then exit 75`.
- **The shutdown** is the normal SIGTERM drain, except that memtables are not
  flushed: it syncs the value log (the client bytes as written, not the corrupt
  in-RAM copy) and persists the liveness watermark. No run or manifest derived
  from the corrupt memtable is written **on this node**, because the guard
  refuses that block on every attempt. The guarantee is local: during the
  detection window, anti-entropy digests, over-replication cleanup and rehome
  still read the memtable unchecked, so a corrupt key can reach a peer and be
  stored there (where AE and cleanup treat it as a wrong-shard or extra key).
  Client reads and scans of the corrupt key fail instead, because the record's
  key no longer matches ([Corrupt value reads](#corrupt-value-reads)). If the
  drain takes longer than 30 s, a watchdog calls `_exit(75)`. That has the same
  durability as `kill -9`: writes acknowledged as durable are already synced
  and are replayed. There is no panic and no core dump either way.
- **The restart.** systemd logs `status=75` and restarts the unit after
  `RestartSec`. Recovery replays the value-log tail into a fresh memtable and
  the next flush passes the guard (verified on hardware). The kernel releases
  the SPDK device lock with the process on every exit path, and hugepages are
  freed (see [systemd](#systemd)); in a hardware test, 8 restarts after exit 75,
  SIGSEGV and SIGABRT left no map files and all hugepages free.
- **Restart storms.** A deterministic software bug, or a persistent memory
  fault on the host, fails again after every restart. systemd's
  `StartLimitIntervalSec`/`StartLimitBurst` bound that (above: 4 starts per 30
  min, then `start-limit-hit` and the unit stays failed); valsd keeps no restart
  count of its own. Manual `systemctl restart`s count toward the same limit, so
  after rolling restarts run `systemctl reset-failed vals-node`. Alert on unit
  failure or repeated `status=75`. A node that stays down is like any node
  outage: peers keep serving and anti-entropy heals it when it returns. Run
  `memtester` and compare the logged XOR and region across incidents before
  putting the host back into service.
- **Opting out.** Set both thresholds to 0. The node then retries a corrupt
  flush forever.

Not covered: a mismatch that a later retry clears resets the streak; on-disk
signals (run index, merge-read and cover-scan CRC errors) and recovery failures
keep their own handling. Applications that embed `vals-cluster` as a library
only get the request flag (`vals_cluster::integrity::fail_fast_requested()`);
`valsd` performs the exit.

### Corrupt value reads

Every value read checks the record it decodes against the index entry that
pointed to it: magic, kind, length, CRC, and the record's key and version. This
covers gets, scans, mixed batch reads and anti-entropy value sync (key only). A
pointer that lands on a different valid record of the same length, or on an
older record of the same key, fails the read instead of returning the wrong
value.

- The client gets an error reply such as `corrupt record: length mismatch` or
  `corrupt record: record key mismatch`; a quorum read can still succeed from
  another replica.
- `vals_store_corrupt_reads_total{kind="magic"|"kind"|"length"|"crc"|"key"|"version"|"other"}`
  counts each failed reply (a read that raced with maintenance and was retried
  is not counted). It must stay 0.
- An ERROR line names the replica and the pointer, at most one line per 10 s per
  shard thread (later lines report how many were suppressed):
  `corrupt value read: zone Z shard S get|scan|mixed|sync-values key <hex,
  first 48 bytes>[…] (N bytes) segment G offset O len L: <reason>`.
- The damage stays until the key is rewritten. A flipped bit in a persisted
  value-pointer length shows up as `kind="length"`. Anti-entropy does not
  repair it, because digests cover only (key, version).

Merges read values only when value-log GC rewrites them, and that rewrite runs
the same checks, so a wrong-record pointer can never be rewritten as a
self-consistent record holding another key's value:

- The merge fails before publication. The pointer, the record and the input
  runs stay as they were (keeping the evidence), and reads of that key keep
  failing as above. The node does not exit: this is persisted damage that a
  restart cannot fix, so it is not an exit 75 case.
- It is counted in the same `vals_store_corrupt_reads_total{kind=...}` and
  logged under the same rate limit with path `merge rewrite`: `corrupt value
  read: zone Z shard S merge rewrite key <hex> (N bytes) segment G offset O
  len L: <reason>; failing the merge unpublished; the segment is excluded from
  value-log GC until restart`.
- The segment holding the bad record is then left out of GC and later
  rewrites until restart (one `engine: value segment G holds a corrupt record
  ...` ERROR per segment), so the zone's other merges and space reclaim go on,
  but that segment's space is not reclaimed. After a restart, the first GC merge
  that reaches it fails once and excludes it again.

### Merge output verification

Before a streamed merge output run is published, the whole run is read back
from the device after its last write and checked: every data block's CRC, plus
the footer, filter and index CRCs and a byte compare against what the builder
wrote (which catches lost or stale tail writes). Flushes and full merges check
the footer, filter and index from the device before the manifest write.

- The full readback is on by default. `VALS_RUN_BUILD_VERIFY=0` turns it off;
  the footer, filter and index CRC checks always run.
- It sees what the device returns right after the write, so it does not catch
  media corruption that appears later, or a bit flip in an entry before it was
  encoded.
- `vals_run_build_verify_bytes_total` counts the bytes checked, and
  `vals_run_build_verify_failures_total` must stay 0. A failure aborts the
  merge without publishing anything: the reserved segments are released, the
  inputs stay live, and the merge is retried on the next maintenance tick.

## Operational notes

- **Set the secret everywhere.** Use the same `secret_file` on every node and
  PD; it gates control operations and the PD Raft port. Without it, any process
  that can reach the ports can reconfigure the cluster, and valsd logs a
  warning at startup. The secret travels in cleartext, and vals does not
  encrypt or authenticate data traffic, so run it on a private network or an
  overlay such as WireGuard or Tailscale.
- **Run three durable PD replicas**, one per independent host or failure
  domain ([PD availability and recovery](#pd-availability-and-recovery)). A PD
  quorum outage leaves an already-running data path on its last pushed config,
  but blocks all reconfiguration and restart healing.
- **Restarts heal themselves.** The PD probes nodes every second and re-pushes
  config to any that restarted, and nodes reopen their persisted zones at boot.
- **Capacity-weighted rings.** File nodes default to weight 1. SPDK multi-drive
  nodes log their drive-count weight at startup; set it in the PD config
  (`data_node = <id>*<weight> @ ...`) or pass it to `add-node`.
- **Metrics.** Set `metrics = <addr>` in a node or PD config to serve the
  process-wide counters as Prometheus text over HTTP; every counter is exported
  as `vals_<name>` with its description as `# HELP`. The endpoint has no
  authentication, so bind it to a private address. Inside a process,
  `observe::snapshot()` gives the same counters programmatically.

## Security checklist

vals trusts its network. Data reads, writes and deletes are not
authenticated, nothing on the wire is encrypted, and the cluster secret that
guards control operations travels as a plain token. Anyone who can reach a
data port can read and change every key; anyone who can also see the traffic
can learn the secret and reconfigure the cluster. Work through this list on
every host before you load real data. The examples use the ports and paths
from this guide (data `7000`–`7099`, PD Raft `7100`, metrics `9464`/`9465`,
the [systemd units](#systemd)); substitute your own.

- [ ] **Isolate the data and Raft ports with a firewall, not only by
  addressing.** Data nodes need their data ports reachable from clients, the
  other data nodes and every PD host; a PD's Raft port only from the other PD
  hosts; metrics only from your scraper. Data nodes never connect to a PD.
  An `nftables` example (`nft -f` it, then make it persistent with your
  distribution's mechanism):

  ```
  table inet vals {
      set cluster {
          type ipv4_addr
          elements = { 10.0.0.1, 10.0.0.2, 10.0.0.3 }
      }
      set clients {
          type ipv4_addr
          flags interval
          elements = { 10.0.1.0/24 }
      }
      set monitoring {
          type ipv4_addr
          elements = { 10.0.2.10 }
      }
      chain input {
          type filter hook input priority filter; policy accept;
          iif "lo" accept
          tcp dport 7100 ip saddr @cluster accept
          tcp dport 7000-7099 ip saddr @cluster accept
          tcp dport 7000-7099 ip saddr @clients accept
          tcp dport { 9464, 9465 } ip saddr @monitoring accept
          tcp dport { 7000-7100, 9464, 9465 } drop
      }
  }
  ```

  Check from a host outside those sets; neither command may print `OPEN`:
  `nc -z -w 2 10.0.0.1 7000 && echo OPEN` and
  `nc -z -w 2 10.0.0.1 7100 && echo OPEN`.
- [ ] **Use an overlay for any untrusted path.** If vals traffic crosses a
  network you do not control (shared or cloud networks, links between
  sites), carry all of it over WireGuard or Tailscale, and set `listen`,
  `pd_peer`, `data_node` and `peer` to overlay addresses instead of
  `0.0.0.0` or public addresses. The secret is cleartext on every
  control push and every PD Raft connection; vals has no TLS option.
- [ ] **Set the same secret on every node and PD.** Use `secret_file`, mode
  `0600`, owned by the daemon user (`stat -c '%a %U' /etc/vals/secret` prints
  `600 vals`), with identical contents everywhere (`sha256sum` matches across
  hosts). Prefer it to `secret = <literal>`, which puts the secret in the
  config file. Without a secret, control operations are open to anyone who
  can reach the ports: `journalctl -u vals-node -u vals-pd | grep 'control
  ops are OPEN'` must print nothing.
- [ ] **Restrict the admin sockets.** Anyone who can connect to the PD socket
  can change membership and zones and export the PD state; anyone who can
  connect to a node socket can make the node write a dump to any path the
  daemon user can write. With `UMask=0077` and a mode-0700 runtime
  directory, `stat -c '%A %U' /run/vals-pd/admin.sock` prints `srwx------
  vals` and `ls -ld /run/vals-pd` shows `drwx------`. Leave the node's
  `admin_socket` unset unless you need live dumps, especially on SPDK nodes,
  which run as root.
- [ ] **Bind metrics to a private address.** The endpoint is unauthenticated
  HTTP. Use `metrics = 127.0.0.1:9464` with a local scraper, or a private
  address covered by the firewall above; `ss -ltn | grep -E ':946[45]'` shows
  what is bound.
- [ ] **Protect backups and dumps like the data.** They are complete,
  unencrypted copies. Write them into a directory only the daemon user can
  read (`install -d -o vals -g vals -m 0700 /backups`) and keep copies off
  the host just as restricted.
- [ ] **Run file-backed daemons unprivileged.** Use the `vals` user from the
  [systemd units](#systemd); only SPDK nodes need root.

These steps limit who can reach vals. They do not add authentication or
encryption to vals itself, so every host and client inside the firewall sets
is trusted with all the data.

## Transport resource bounds

Every daemon has one process-wide transport budget covering all data shards and
listeners:

| Limit | Default | Node config key | PD (environment only) |
|---|---|---|---|
| Accepted inbound connections | 4,096 | `transport_max_connections` | `VALS_TRANSPORT_MAX_CONNECTIONS` |
| Decoded or queued requests | 32,768 | `transport_max_requests` | `VALS_TRANSPORT_MAX_REQUESTS` |
| Charged transport bytes | 512 MiB | `transport_max_queued_bytes` | `VALS_TRANSPORT_MAX_QUEUED_BYTES` |
| Time to complete the first protocol frame | 5 s | `transport_initial_timeout_ms` | `VALS_TRANSPORT_INITIAL_TIMEOUT_MS` |
| Time to finish a partly received frame | 30 s | `transport_frame_timeout_ms` | `VALS_TRANSPORT_FRAME_TIMEOUT_MS` |
| Idle connection with no executing request | 120 s | `transport_idle_timeout_ms` | `VALS_TRANSPORT_IDLE_TIMEOUT_MS` |
| Stalled output making no progress | 30 s | `transport_write_timeout_ms` | `VALS_TRANSPORT_WRITE_TIMEOUT_MS` |

Values must be positive integers. A node also accepts the `VALS_TRANSPORT_*`
names as legacy aliases of its config keys. The limits are fixed for the
process lifetime and logged at startup. `VALS_TRANSPORT_UNBOUNDED=1` disables
all seven, for development and benchmarks only; on a node it conflicts with any
`transport_*` config key.

What is charged: data input buffers (by allocated capacity), executing requests
at their encoded frame size, retained output buffers, admin commands and
output, metrics output, and encoded PD inbox messages. Credits are released
when the memory is freed or the work completes, fails, times out or is
cancelled. Not charged as queued bytes: kernel listen backlogs, outbound client
connection pools, each data connection's fixed 32 KiB read buffer, and
temporary buffers used while encoding responses or decoding a PD frame; those
are bounded by the connection and request counts instead.

The budget covers the data TCP and optional Unix-socket listeners, every data
shard, the PD Raft listener and its inbox, the metrics listener, and the node
and PD admin sockets. Metrics serves one scrape at a time with the same initial
and write deadlines. Each admin connection reserves its 64 KiB command cap
before reading and holds that and a request credit until it finishes; its
initial deadline is absolute, so trickling bytes cannot extend it. PD Raft
frames are not limited by the 64 MiB data-frame cap; a frame is accepted only if
its declared size fits in the byte budget, so a full-state snapshot can be as
large as that budget. PD writes to peers keep a tighter 250 ms deadline.

When a limit is hit the wire protocol does not change: a data request refused
at the request limit gets the usual retryable `Busy`, and a connection refused
before service, or one that cannot reserve bytes, is closed. The Rust client
resends an idempotent read once, on a freshly dialed connection, when the reused
idle connection it went out on fails before answering (counted in
`idle_conn_retries`); writes are never resent.

`scan_max_inflight` (default 8, must be positive) limits concurrent network
scans per shard thread. Scans over the limit get a retryable `Busy` without
starting, counted in `vals_scan_admission_busy`.

Rejections and timeouts are counted in `vals_transport_connection_rejections`,
`vals_transport_request_rejections`, `vals_transport_queued_byte_rejections`,
`vals_transport_initial_timeouts`, `vals_transport_frame_timeouts`,
`vals_transport_idle_timeouts` and `vals_transport_write_timeouts`; under normal
load they stay at zero. In hardware tests the default limits changed write and
read throughput by less than 2%. These are resource controls only: they provide
no authentication, encryption or authorization and do not replace the cluster
secret or network access controls.

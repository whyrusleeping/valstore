# distnode box bench — runbook (task 0012)

> **⚠️ 2026-07-15: `oldbox` is no longer accessible** (replacement
> machines are being provisioned). The "Box run" section's hostname, checkout
> paths, and PCI addresses are old-box-specific and must be re-derived on the
> new hardware.

A repeatable `vals-bench` mode ("distnode") that drives **live `Node`s** — real
anti-entropy sweep loops, background compaction, the wipe-before-rejoin liveness
lease, and a `PdDaemon` pushing membership — instead of the passive
`ShardedServer`s the other `dist*` benches use. This hardware-exercises the full
healing/rebalancing path (over-replication cleanup, eager rebalancing, per-zone
AE, the liveness lease) and reports the healing counters alongside throughput.

Off-hardware smoke (must be green before any box run):

```
cargo run -p vals-bench --release -- mem distnode 20000 64
# expect: FLOWS: OK  (all five phases print; 0 mismatches)
```

## Topology

Fixed to mirror the 4-drive SPDK box:

- **K = 3 initial nodes** (ids 1..3), **RF = 3** so every initial node replicates
  every key — this is what lets P3 read a healed node *alone*.
- **1 joiner** (id 4) held back, spawned mid-run in P4 (grow + rebalance).
- A **`PdDaemon::spawn_single`** (mem-backed raft state) pushes committed
  membership + the zone catalog to the nodes on every change; the coordinators
  adopt the PD-pushed ring via `adopt_from_any_member`.
- One named **unsorted zone** `znode` (rf=3), for the per-zone AE / eager-
  rebalancing check.

Each node runs its own `SpdkZoneStore` over one NVMe namespace (spdk) or one
`MemZoneStore` (mem), served by `Node::serve_zoned_with_liveness` — the exact
wiring `valsd node` uses (see `crates/vals-server/src/run_node.rs`).

## Phases (each prints throughput + per-phase observe-counter deltas)

- **P1 load** — `n` keys at `Quorum(2)` → ops/s.
- **P2 verify** — read all `n` at R=2 → ops/s + mismatch count.
- **P3 outage-heal via SWEEP** (not hint replay) — node 1 is made unreachable
  from the coordinator while `m = max(n/4, 2000)` NEW keys are written to the
  other replicas (hints recorded, **not** replayed); node 1 comes back and its
  own anti-entropy sweep pulls the missing keys from peers. Proven by reading
  every new key from **node 1 alone** (R=1, `ReadFanout::All`, peers marked
  down). Reports time-to-heal, heal keys/s, and `sweep_passes / ae_digests /
  ae_value_reads / ae_entries_applied` deltas.
- **P4 grow + rebalance + cleanup** — spawn the joiner (pristine, boot ring =
  self), advertise it via `pd.add_node_addressed`; the ring moves and the
  joiner's sweep eagerly materializes + heals the ranges (incl. the named zone)
  it now owns. Verify those serve from the **joiner alone**. Reports time-to-
  rebalance, `peers_added`, AE deltas, `live_keys` peak vs settled, and
  `cleanup_errors` / `cleanup_quiesce_timeouts` (expect 0).
- **P5 steady churn** — `VALS_DISTNODE_CHURN_SECS` of mixed overwrite `Quorum(2)`
  + read R=2 against the grown 4-node cluster while sweeps run → ops/s.
- **Final** — a full healing-counter delta table (sweep/ae/cleanup/compaction/
  fence/unfence/wipe/adoption/pd/hints/server_ops) and a `FLOWS: OK/CHECK` verdict:
  0 mismatches everywhere, heal + rebalance within timeout, `unfences ≥ 4`,
  `wipes == 0`, `cleanup_errors == 0`. Timeouts print CHECK, never panic.

## Env knobs

| var | default | meaning |
|-----|---------|---------|
| `VALS_DISTNODE_SWEEP_MS`   | 2000  | anti-entropy sweep period (per node) |
| `VALS_DISTNODE_GRACE_MS`   | 60000 | liveness lease `gc_grace` (fence window) |
| `VALS_DISTNODE_CHURN_SECS` | 20    | P5 churn duration |
| `VALS_DISTNODE_HEAL_SWEEPS` | 15   | P3 heal verify window, in sweep periods |
| `VALS_DISTNODE_REB_SWEEPS` | 20    | P4 rebalance verify window, in sweep periods |
| `VALS_DISTNODE_REGION_MIB` | 2048  | per-zone on-device region (spdk only) |
| `VALS_DISTNODE_REGION_MIB_MEM` | 128 | per-zone RAM region (mem only; preallocated) |

## Sizing (heal/rebalance windows vs. the AE budget)

Anti-entropy reconcile is **budgeted per pass** (by design — it resumes forward
next pass so a sweep never monopolizes the serving core; see
`crates/vals-cluster/src/antientropy.rs`):

- `AE_RECONCILE_MAX_DIGESTS_PER_PASS = 50_000` — `(key, version)` digests scanned
  per pass (phase 1, no device reads).
- `AE_RECONCILE_MAX_VALUE_READS_PER_PASS = 8_192` — entries actually read back off
  the device and applied per pass (phase 2).

So re-syncing `m` divergent keys over a keyspace of `keys` needs roughly

```
passes ≳ m / 8192   (value applies)  +  keys / 50000   (digest scan)
```

sweep passes — i.e. `≈ m/8192 + keys/50000` sweeps of wall time. A fixed 15/20-
sweep window therefore **cannot** pass at millions of keys; size the windows to
the data with `VALS_DISTNODE_HEAL_SWEEPS` / `VALS_DISTNODE_REB_SWEEPS`.

Two empirical corrections from the box (see `bench/distnode-box-notes.md`):

- **P3 (outage heal)** tracks the math well: 50k-key backlog healed at ~447
  keys/s on NVMe (n=200000 run) — but the *verify read pass itself* is serial
  (~450 reads/s), so budget wall time for verification too.
- **P4 (joiner fill) is much bigger than `m/8192`**: the joiner's backlog is
  ~RF/4 of the **whole keyspace** (~¾ of all keys at RF=3, 4 nodes), and the
  over-replication cleanup's confirmation reads on the old owners compete for
  the same per-pass budgets — observed in-window fill was ~170 applies/s at
  n=200000. Size `REB_SWEEPS` against `0.75 × total_keys`, not `n/4`.

**Recommended box recipe — verified green (FLOWS: OK, 2026-07-06)**:

```
sudo env LD_LIBRARY_PATH=$PWD/third_party/spdk/dpdk/build/lib \
  VALS_DISTNODE_HEAL_SWEEPS=60 VALS_DISTNODE_REB_SWEEPS=120 \
  ./target/release/vals-bench spdk distnode 20000 128 \
  0000:05:00.0,0000:07:00.0,0000:c1:00.0,0000:c3:00.0
```

(~3 min wall: P3 heals 5000 keys in ~11s, the joiner serves all its owned keys
in ~11s, clean graceful teardown.)

**Heal-at-scale variant** (P3 PASS with a 50k-key backlog, ~10 min wall; expect
P4 to print CHECK unless `REB_SWEEPS` is raised into the hundreds):

```
... VALS_DISTNODE_HEAL_SWEEPS=60 VALS_DISTNODE_REB_SWEEPS=40 \
  ./target/release/vals-bench spdk distnode 200000 128 ...
```

Multi-million-key runs (`n=2000000`) are useful for **throughput under sweep
pressure** (P1/P2/P5) but the sweep-heal windows would need *hours* per the
budget math — in production the fast path for a rejoining node's backlog is
**hint replay**, not the sweep; the budgeted sweep is the eventual backstop.
Don't size a bench to wait out the backstop at millions of keys. (Also see
**Known issues at large n** below: the graceful teardown wedge, task 0073.)

## Box run (oldbox)

### Prereqs

- `ssh oldbox` (repo checkout at `~/vals`, passwordless sudo).
- Four vfio-bound NVMe drives, free:
  `0000:05:00.0 0000:07:00.0 0000:c1:00.0 0000:c3:00.0`
  (the OS RAID nvme4/nvme5 are never touched).
- Verify no stale cluster / residue before starting:
  ```
  pgrep -a valsd || echo "no valsd"
  pgrep -a vals-bench || echo "no vals-bench"
  ls /var/run/dpdk/spdk* /dev/hugepages/spdk* 2>/dev/null || echo "no spdk residue"
  ```
  If a previous `valsd` cluster is up, stop it gracefully first (SIGTERM the pd +
  node PIDs) so the vfio drives are free to claim.

### Sync + build

```
rsync -a --exclude target --exclude .git ~/code/vals/ oldbox:~/vals/
ssh oldbox
cd ~/vals
VALS_SPDK_LINK=1 cargo build --release -p vals-bench --features spdk
```

### Run

Use the **verified green recipe** from **Sizing** above (`n=20000`,
`VALS_DISTNODE_HEAL_SWEEPS=60 VALS_DISTNODE_REB_SWEEPS=120`) for a run where
every flow completes (FLOWS: OK). A raw throughput probe (no heal/rebalance
expectation):

```
sudo env LD_LIBRARY_PATH=$PWD/third_party/spdk/dpdk/build/lib \
  ./target/release/vals-bench spdk distnode 2000000 128 \
  0000:05:00.0,0000:07:00.0,0000:c1:00.0,0000:c3:00.0
```

- `distnode` uses the first **4** PCIe addrs (nodes 1..3 + joiner 4); extra
  drives are ignored (a note is printed).
- The four drives are formatted (`format=true`) — the live cluster must be down.
- Tune `VALS_DISTNODE_*` for shorter/longer runs (e.g. `VALS_DISTNODE_CHURN_SECS=60`).

### What PASS looks like

```
--- distnode (spdk): 3 live Nodes (id 1..3) + a joiner (id 4), RF=3, R=2 ---
  boot lease  : OK — unfences +3 ... ring_adoptions +3 ...
  named zone  : id=1 rf=3 — serving (... keys loaded)
  P1 load     : <ops/s>
  P2 verify   : <ops/s> (0 mismatches of N)
  P3 heal     : node1 re-synced M keys via sweep in <t>s (... , 0 mismatches)
  P4 rebalance: joiner 4 owns ... keys, all served from it alone in <t>s
              ... cleanup_errors 0 cleanup_quiesce_timeouts 0
  P5 churn    : <ops/s> over Ns (0 read misses)
  --- healing-counter deltas (whole run) ---
    ... unfences: 4 ... wipes: 0 ... cleanup_errors: 0 ...
  FLOWS       : OK
```

`FLOWS: OK` requires: 0 mismatches everywhere, heal + rebalance within their
(env-sized) windows, `unfences ≥ 4` (3 initial + joiner), `wipes == 0`,
`cleanup_errors == 0`.

## Known issues at large n

First box run (`n=2000000 vsize=128`, defaults) completed **all phases** but with
CHECKs, as expected from the AE budget math:

- P1 load ~23,149 ops/s; P2 verify 0 mismatches but only ~433 ops/s (serial reads
  competing with back-to-back 2s sweeps over ~2.7M resident keys).
- P3 heal **CHECK** — 0 AE applies inside 15 sweeps (needs ≳ `m/8192 + keys/50k`
  ≈ hundreds of sweeps at this scale).
- P4 rebalance **CHECK** — `joined=true`, `ae_applied +16384` (= 2 zones × 8192,
  i.e. exactly one value pass per zone) but not complete in 20 sweeps.
- `FLOWS: CHECK`, and **graceful teardown wedged >1h** on
  `node 1: graceful shutdown requested; draining` (the shutdown drains behind the
  still-sweep-churning shard) and had to be SIGKILLed. Filed as **task 0073**; it
  is not a distnode-bench bug. At small/medium n the same teardown completes in
  milliseconds per node.

Fix for a *passing* run: size the windows (see **Sizing**) and keep `n` modest
(`20000` for the full-flow green run; `200000` for heal-at-scale). Reserve
millions-of-keys runs for throughput-only measurement. Full run records:
`bench/distnode-box-notes.md`.

### Leave the box clean

```
# a clean run tears its own nodes + PD down (clean SPDK detach) and exits 0.
pgrep -a vals-bench || echo "no vals-bench"
ls /var/run/dpdk/spdk* /dev/hugepages/spdk* 2>/dev/null || echo "no spdk residue"
# confirm the four drives are still vfio-bound (untouched binding)

# RECOVERY after a SIGKILLed run (e.g. the large-n teardown hang above): the
# killed process leaves DPDK/hugepage residue — clear it before the next run.
sudo pkill -9 vals-bench 2>/dev/null || true
sudo rm -rf /var/run/dpdk/spdk* /dev/hugepages/spdk*
```

Record the run output + numbers in `bench/distnode-box-notes.md`.

# Bootes soak runbook — Jetstream replay+live into the live lab SPDK cluster

How to stand up a **fresh** 3-node real-NIC SPDK cluster on the lab fleet and
drive a continuous bootes (atproto/Bluesky Jetstream v2: full-network archive
replay + live tail) soak into it through the valsgo `--vals` client. This is
the recurring "run another fresh bootes soak" job.

> **2026-08-21: bootes was rewritten on the Jetstream v2 Go SDK.** The indigo
> firehose+backfill machinery (repo pump, 300-job CAR downloads, `backfill.db`,
> the `indigo-stream-fix` overlay and its build script) is gone. One stream now
> replays the sealed archive from the persisted cursor (0 = the whole network)
> and cuts over to the live tail. Archive replay is byte-metered and needs a
> Jetstream API key (`JETSTREAM_API_KEY`); the live tail does not.

Companion docs: [`box-provisioning-runbook.md`](box-provisioning-runbook.md)
(fleet inventory, PCI addresses, vfio), `docs/zones.md` (sorted zones / splits),
`docs/SPDK.md`. The first live run and the `0106` client-parity bug are written
up in `bench/bootes-soak-lab/notes.md`.

---

## Topology at a glance

| Piece | Where | Notes |
|---|---|---|
| PD | lab-1 (10.0.0.114), user `why` | admin socket `~/vals/run/pd.sock`, metrics `:9465` |
| node 1/2/3 | .114 / .18 / .200, **root** (vfio) | 4 shards each (ports 7001-7004), SPDK, metrics `:9464` |
| bootes | dev workstation `devbox` (10.0.0.138) | `~/code/bootes`, metrics `:9091` |
| RF | 2 | zones `records`, `backlinks`, both **sorted** |

- **valsd checkout on the boxes:** `~/vals` (an rsync'd tree, *not* a git repo)
  with a prebuilt `~/vals/target/release/valsd`. The node/PD configs already live
  at `~/vals/run/{node.conf,pd.conf}` (per-box `pci`/`node_id` baked in).
- Every valsd/admin invocation needs **`LD_LIBRARY_PATH`** pointing at the SPDK
  DPDK libs. Define once:

  ```sh
  LD=/home/user/vals/third_party/spdk/dpdk/build/lib:/home/user/vals/third_party/spdk/dpdk/build/lib/dpdk/pmds-24.1
  ```

- Client node list (used by bootes, valsq, interop). Only shard-0 addrs are
  needed for bootstrap; the client learns the rest from membership:

  ```sh
  NODES='1@10.0.0.114:7001,2@10.0.0.18:7001,3@10.0.0.200:7001'
  ```

> ⚠️ **Launch valsd with `sudo systemd-run`, one command per line.** Multi-line
> ssh commands that mix `sudo`/`pkill`/backgrounding intermittently return ssh
> exit 255 with lost output on this fleet. The single-line `systemd-run` form
> below is reliable and gives you `systemctl` control + graceful SIGTERM on stop.

---

## 0. Preconditions (once per session)

```sh
# From devbox. Boxes may have been reprovisioned → refresh host keys.
for h in 10.0.0.114 10.0.0.18; do
  ssh-keygen -R $h >/dev/null 2>&1; ssh-keyscan -T5 $h >>~/.ssh/known_hosts 2>/dev/null
done
for h in 10.0.0.114 10.0.0.18 10.0.0.200; do ssh user@$h hostname; done
```

Sanity per box (all should already be true — the provisioner sets them up):

```sh
for h in 10.0.0.114 10.0.0.18 10.0.0.200; do ssh user@$h '
  sudo -n true && echo passwordless-sudo-ok
  grep HugePages_Total /proc/meminfo            # expect 4096 (8 GiB)
  sysctl -n kernel.panic_on_oops                # expect 1 (task 0131)
  PCI=$(grep -oP "pci\s*=\s*\K[0-9a-fA-F:.]+" ~/vals/run/node.conf)
  echo "node.conf pci=$PCI drv=$(basename $(readlink -f /sys/bus/pci/devices/$PCI/driver))"
'; done
```

If `HugePages_Total` is 0 and the drive is on the `nvme` driver, **the box has
rebooted** — SPDK prep is runtime-only. Restore it (resolving the scratch drive by
MODEL, never by device node or PCI address, both of which move):

```sh
pci=$(basename "$(readlink -f "/sys/block/$(lsblk -dno NAME,MODEL | awk '/9100 PRO/{print $1}')/device/device")")
cd ~/vals && sudo env PCI_ALLOWED="$pci" HUGEMEM=8192 third_party/spdk/scripts/setup.sh
```

`kernel.panic_on_oops` must be **1** on every box: a kernel Oops that dies holding
an mm lock otherwise wedges the whole host silently and indefinitely (task 0131 —
lab-2 lost ~10 h that way, taking the soak's producer state with it). With it
set, the box panics and reboots in 30 s, which replication/hints/AE absorb.

The configured `pci` **must** be the **9100 PRO** bound to **vfio-pci** (never
the OS 990 PRO — identify the scratch drive by MODEL, per the provisioning
runbook). Node configs must not contain `format = true`; daemon starts never
format. For a *fresh* soak, stop every node and run the offline workflow below
once per node before starting any daemon. Obtain `NVME_SERIAL` from the physical
label or trusted inventory, not from the PCI address alone.

```sh
PCI=$(awk -F= '/^[[:space:]]*pci[[:space:]]*=/{gsub(/[[:space:]]/,"",$2); print $2}' ~/vals/run/node.conf)
NODE_ID=$(awk -F= '/^[[:space:]]*node_id[[:space:]]*=/{gsub(/[[:space:]]/,"",$2); print $2}' ~/vals/run/node.conf)
sudo env LD_LIBRARY_PATH="$HOME/vals/third_party/spdk/dpdk/build/lib:$HOME/vals/third_party/spdk/dpdk/build/lib/dpdk/pmds-24.1" ~/vals/target/release/valsd provision -c ~/vals/run/node.conf --device "$PCI=$NVME_SERIAL" --confirm-destroy "node-$NODE_ID"
```

The provision command attaches and serial-checks all configured devices before
writing. Never combine it with the recurring `valsd node` systemd unit.

---

## 0.5 Ensure the box code is up to date — **DO NOT SKIP**

> ⚠️ **The `valsd` on the boxes must be built from current `valstore` HEAD.**
> `~/vals` is an rsync'd snapshot, *not* a git checkout — it does **not** update
> itself. A stale binary silently re-runs already-fixed bugs. Concretely: the
> 2026-08-01 soak OOM-killed two nodes by faithfully reproducing the *fixed*
> task **0107** (backlinks region fills → flush wedges → memtable grows → OOM)
> because the box binary predated the 0107 **and** 0108 (auto-split) fixes.
> Always sync + rebuild before a soak unless you just did and nothing changed.

Sync current source from devbox (excludes the prebuilt SPDK tree, the build
cache, and box-local runtime state) and rebuild the SPDK `valsd` on each box.
The boxes pin Rust **1.96.0** (matches `rust-toolchain.toml`) and already have
`~/vals/third_party/spdk` built, so only the workspace source changes.

```sh
# from devbox, in the valstore workspace root:
for h in 10.0.0.114 10.0.0.18 10.0.0.200; do
  rsync -a --delete \
    --exclude 'target/' --exclude 'third_party/' --exclude '.git/' \
    --exclude 'run/' --exclude 'bench/' --exclude '*.db*' \
    ./crates ./Cargo.toml ./Cargo.lock ./rust-toolchain.toml user@$h:/home/user/vals/
done

# build (SPDK feature — bench builds can't serve backend=spdk). Runs in parallel;
# a few min/box. `setsid … &` detaches so the ssh returns:
for h in 10.0.0.114 10.0.0.18 10.0.0.200; do
  ssh user@$h 'cd ~/vals && export PATH=$HOME/.cargo/bin:$PATH && nohup setsid sh -c "VALS_SPDK_LINK=1 cargo build --release -p vals-server --features spdk > ~/vals/build.log 2>&1; echo DONE_RC=\$? >> ~/vals/build.log" </dev/null >/dev/null 2>&1 &'
done

# wait for all three to finish, then confirm each ended DONE_RC=0:
for h in 10.0.0.114 10.0.0.18 10.0.0.200; do
  echo -n "$h: "; ssh user@$h 'tail -1 ~/vals/build.log'
done
```

Record the exact source revision **and dirty diff/source manifest**, successful build
log, toolchain/build flags, and SHA-256 of the resulting binary for each host. At
launch, record the unit's PID and hash `/proc/<pid>/exe` and compare it with that
binary hash. Keep these receipts with the run. Source-comment greps and executable
mtimes do not establish compiled-source identity or which binary is running.
Do not start a measured arm if source, built binary, and running executable cannot
be tied together.

---

## 1. Start the PD (lab-1) — fresh raft

> This is the single-PD performance-test mode: one non-HA PD, not the reference
> deployment. For three voting PDs (one per box) use `bench/0471-queue-hw/hw.py`
> (`provision --pd-mode three --reset-pd`, `check-configs`) or render configs with
> `scripts/pd_topology.py`; see
> [Three-PD reference deployment](../docs/deploy.md#three-pd-reference-deployment).

> If `~/vals/run/pd.conf` is missing (it vanished once, 2026-08-22), restore it
> from the checked-in copy: `scp bench/lab-rehome-20260811-0176/pd.conf
> user@10.0.0.114:/home/user/vals/run/pd.conf`.

For balanced canonical sorted placement (0226/0227) add
`--setenv=VALS_PD_OPAQUE_RANGE_MAPS=1` to the PD launch below.

```sh
ssh user@10.0.0.114 'cd ~/vals
  systemctl is-active vals-pd 2>/dev/null && sudo systemctl stop vals-pd
  systemctl reset-failed vals-pd 2>/dev/null
  mv -f run/pd.log run/pd.log.old 2>/dev/null
  rm -rf run/pd; mkdir -p run/pd'          # wipe raft state → fresh cluster

# launch (single line):
ssh user@10.0.0.114 'sudo systemd-run --unit=vals-pd --collect --property=User=why --property=WorkingDirectory=/home/user/vals --property=StandardOutput=append:/home/user/vals/run/pd.log --property=StandardError=append:/home/user/vals/run/pd.log --setenv=LD_LIBRARY_PATH=/home/user/vals/third_party/spdk/dpdk/build/lib:/home/user/vals/third_party/spdk/dpdk/build/lib/dpdk/pmds-24.1 /home/user/vals/target/release/valsd pd -c /home/user/vals/run/pd.conf'
```

Verify: `ssh user@10.0.0.114 'env LD_LIBRARY_PATH=$LD ~/vals/target/release/valsd admin -s ~/vals/run/pd.sock members'`
→ 3 members, weight 4 each. (`pd.log` will spew "connection refused" until the
nodes come up — expected.)

---

## 2. Start the three nodes (root, vfio)

For each host `H` in `10.0.0.114 10.0.0.18 10.0.0.200`:

```sh
# rotate log first (separate call):
ssh user@$H 'sudo rm -f /home/user/vals/run/node.log; systemctl reset-failed vals-node 2>/dev/null; true'

# launch (single line; 40 GiB process RSS budget on the 60 GiB boxes):
ssh user@$H 'sudo systemd-run --unit=vals-node --collect --property=LimitCORE=infinity --property=WorkingDirectory=/home/user/vals --property=StandardOutput=append:/home/user/vals/run/node.log --property=StandardError=append:/home/user/vals/run/node.log --setenv=VALS_RAM_BUDGET_BYTES=42949672960 --setenv=LD_LIBRARY_PATH=/home/user/vals/third_party/spdk/dpdk/build/lib:/home/user/vals/third_party/spdk/dpdk/build/lib/dpdk/pmds-24.1 /home/user/vals/target/release/valsd node -c /home/user/vals/run/node.conf'
```

Size a soak corpus at no more than `RAM budget / 350 bytes` keys per node,
then leave additional margin when values or maintenance working sets are large.
A 40 GiB budget is about 122 million keys/node by that index-only rule of thumb.
Set `VALS_RAM_BUDGET_BYTES` explicitly for lab runs: Linux `MemTotal` includes
the SPDK hugepage reservation, but those hugepages are outside valsd's statm RSS,
so the automatic 70% of `MemTotal` default overstates the process-available
memory on a hugepage-backed box.

SPDK init + format takes ~10-15 s. Verify each: `systemctl is-active vals-node`
→ `active`, and `sudo tail ~/vals/run/node.log` shows
`node N: serving 4 SPDK partitions` then `adopting ring config epoch … 3 members`.
PD's log flips its refused pushes to `recovered (confirmed at epoch …)`.

---

## 3. Create + pre-split the zones

Run on lab-1 (PD admin socket). `V` wraps valsd admin with the LD path:

```sh
# Backlink keys are {ref}\0{source_uri}\0{path} where ref is an at:// URI or a
# DID — every key starts with "at://did:plc:", "did:plc:", or (rarely)
# "did:web:". A numeric presplit=K cuts the FULL byte-space uniformly, so all
# backlinks land in ONE range → one replica pair → its zone slab fills with
# ~19.5 GiB of live data in ~60 min (the 0161-diagnosed capacity wall; task
# 0164). Presplit with REAL-KEY boundaries aligned to the distribution instead:
# 31 boundaries inside at://did:plc: (plc ids are base32(sha256) → uniform over
# the base32-sortable alphabet), 15 inside did:plc:, plus did:web: → 48 ranges,
# ~2% of backlink bytes each, spread over all 12 shard-engines from the first
# write (empty-zone presplit installs canonical tokens).
B='at://did:plc:3,at://did:plc:4,at://did:plc:5,at://did:plc:6,at://did:plc:7,at://did:plc:a,at://did:plc:b,at://did:plc:c,at://did:plc:d,at://did:plc:e,at://did:plc:f,at://did:plc:g,at://did:plc:h,at://did:plc:i,at://did:plc:j,at://did:plc:k,at://did:plc:l,at://did:plc:m,at://did:plc:n,at://did:plc:o,at://did:plc:p,at://did:plc:q,at://did:plc:r,at://did:plc:s,at://did:plc:t,at://did:plc:u,at://did:plc:v,at://did:plc:w,at://did:plc:x,at://did:plc:y,at://did:plc:z,did:plc:4,did:plc:6,did:plc:a,did:plc:c,did:plc:e,did:plc:g,did:plc:i,did:plc:k,did:plc:m,did:plc:o,did:plc:q,did:plc:s,did:plc:u,did:plc:w,did:plc:y,did:web:'

ssh user@10.0.0.114 "LD=/home/user/vals/third_party/spdk/dpdk/build/lib:/home/user/vals/third_party/spdk/dpdk/build/lib/dpdk/pmds-24.1
  V=\"env LD_LIBRARY_PATH=\$LD /home/user/vals/target/release/valsd admin -s /home/user/vals/run/pd.sock\"
  \$V create-zone records   2 sorted
  \$V create-zone backlinks 2 sorted presplit=$B
  # WAIT for the create to COMMIT before splitting — create-zone returns
  # \"ok queued\" (a raft proposal); splitting too soon fails \"no committed zone\".
  until \$V zones | grep -q ' records '; do sleep 1; done
  # pre-split the still-empty records zone canonically at hot-collection boundaries
  # (spreads load from the first write; auto-split covers structure but inherits placement):
  for at in app.bsky.feed.like/ app.bsky.feed.post/ app.bsky.feed.repost/ app.bsky.graph.follow/ app.bsky.graph0; do
    \$V split-zone records \"\$at\" canonical
  done
  sleep 2; \$V zones"
```

Expect `vals_pd_sorted_ranges` = 54 (48 backlinks + 6 records) on PD `:9465`.

**Balanced placement gotcha (0230):** with `VALS_PD_OPAQUE_RANGE_MAPS=1`,
balanced tokens engage only after every node certifies V4 capability for the
exact current epoch tuple. Creating zones seconds after node start silently
falls back to legacy placement for every range
(`vals_pd_canonical_placements_fallback` +N, `…_balanced` +0). Wait until
`vals_pd_balance_noop_capability` stops incrementing (~30 s sample), then
create zones, and put **all** boundaries in `presplit=` at create time — that
is the 0227-validated shape (records too:
`create-zone records 2 sorted presplit=app.bsky.feed.like/,…`). Mid-life
`split-zone … canonical` calls have been observed to fall back even with
pauses. If a create landed legacy, drop + recreate while empty (0228 makes
this safe: verify `vals_zone_drops_deferred` returns to 0) and check
`…_balanced` grew by exactly the range count.

**Capacity math (0164):** at full 300/64 rate (the pre-Jetstream ingester's
validated fanout; treat as the reference full-rate ingest figure) the cluster
accrues ~40–45 GiB/h
of raw backlinks (RF 2 included). Concentrated on one pair that fills a 32 GiB
zone slab in ~60–70 min; spread over the 12 shard-engines it is
~3.5 GiB/engine/h → ~7–8 h to the floor on 32 GiB slabs, ~19 h on 80 GiB. The
lab node.confs now set `zone_region_mib = 81920` (80 GiB/zone slab): slabs
are up-front per-zone allocations within each 256 GiB partition region, and
three zones exist (default + records + backlinks), so 3 × 80 = 240 GiB is the
safe maximum — 96 GiB slabs would NOT fit (288 > 256) and the third zone's
alloc would fail. This equal-slab geometry is a major fleet-sizing tax for a
24B-record backfill: the nearly empty default zone still reserves one third of
each partition, while records and backlinks cannot borrow from one another.
Size the proposed partition/drive count from the worst target zone plus
maintenance and placement-skew headroom; the existing one-drive lab geometry
is only a soak fixture, not a full-network capacity plan. RAM is then the next
ceiling (~120 M keys/node at the 40 GiB budget, ~350 B/key planning bound) and
sheds gracefully via `ram_busy_rejections`.

On a fresh sharded/JBOD format, task 0202's variable geometry can replace the
three equal reservations. Keep `zone_region_mib` as a safe fallback and add the
stable numeric IDs printed by the committed catalog, for example:

```ini
zone_region_mib = 81920
zone_region_mib_override = 0:1024
zone_region_mib_override = <records-id>:<records-MiB>
zone_region_mib_override = <backlinks-id>:<backlinks-MiB>
```

Do not copy the placeholders. Capture the actual IDs before formatting and size
each target from the real-slice **worst shard**, observed physical amplification
and placement skew, then retain 20–30% free/maintenance margin. The sum of zones
that will materialize must fit the partition region; each individual setting is
validated, while aggregate exhaustion remains a loud allocation-time error.
Existing regions retain their persisted lengths on ordinary daemon restarts,
even if the configuration changes. Applying a new size to an existing zone
therefore requires stop + offline `valsd provision` + catalog recreation +
replay. Rollback removes the overrides and performs the same destructive offline
provision/replay; do not point an
older equal-region binary at variable extents.

Task 0202's fresh lab gate used 1/139/100 GiB default/records/backlinks versus
three 80 GiB slabs. Over the common 3,291-second real-corpus replay window,
physical write rates were 39,729/s equal and 40,101/s variable (+0.94%), with
zero replay-window errors and 31–40 merges/node. `format = false` preserved exact
persisted lengths after configured overrides changed, and an 80 GiB fourth zone
failed cleanly in the remaining 16 GiB without changing target-zone scan output.
Raw evidence and production worst-shard math are in
`bench/task-0202-zone-geometry/FINDINGS.md`.

Admin mutations are queued raft proposals (`ok queued`); `zones` should list
both zones. (There is no range-map viewer command — trust `ok queued` + the
node logs `adopting range maps epoch …`.) With current `valsd` (0107+0108) a
region that still fills returns retryable `Busy`/backpressure and auto-splits
under load instead of OOMing — but pre-splitting avoids the early hot spot.

---

## 4. Preflight parity check (recommended)

Confirms the client writes and reads the **same** placement on a *split* range —
this is the exact thing the `0106` valsgo shard-routing fix repaired.

```sh
cd ~/code/vals-examples/valsgo/cmd/interop && go build -o /tmp/interop .
/tmp/interop -nodes "$NODES" batch-many records 'app.bsky.feed.post/pf' 100   # write
/tmp/interop -nodes "$NODES" check-many records 'app.bsky.feed.post/pf' 100   # read back → "verified 100 keys"
# and the deep backlinks presplit (both ref families route through real-key boundaries):
/tmp/interop -nodes "$NODES" batch-many backlinks 'at://did:plc:qzpreflight/' 100
/tmp/interop -nodes "$NODES" check-many backlinks 'at://did:plc:qzpreflight/' 100
/tmp/interop -nodes "$NODES" batch-many backlinks 'did:plc:hpreflight' 100
/tmp/interop -nodes "$NODES" check-many backlinks 'did:plc:hpreflight' 100
```

If this fails or reads 0, **stop** — the valsgo client and cluster disagree on
placement; do not start the soak. Ensure `~/code/vals-examples/valsgo` is at or
past commit `6c7aa2a` (task 0144 token-carrying range-map parity), then rebuild
`/tmp/interop` before running the preflight. Older valsgo binaries cannot decode
map-v2/v3 replies or route inherited tokens correctly.

### 4.1 SPDK qpair-affinity recovery gate

Run this gate after changing qpair ownership code and before a soak. First assess
fleet availability and use only a scheduled gate cluster; never interrupt an
ongoing diagnostic run or reformat its retained corpus. Materialize at least two
named zones, write at least 100 checked keys to each, and retain their expected
values. Then stop the writer, gracefully stop one node, and wait until its
unit is gone and its node PID is absent from `pgrep -x valsd` (a colocated PD
must remain running). Ensure that node's config has no `format = true`, preserve
the PD state and every drive, and restart it with the normal §2 command plus
`-E VALS_SPDK_AFFINITY_CHECK=1`.

The gate passes only when all of the following hold:

- the node remains active and all four shard listeners serve after boot recovery;
- its log reports every persisted named zone reopened and contains no
  `SPDK qpair used outside its bound owner thread`, ownership-steal panic, or
  `VALS SPDK FATAL`;
- all retained keys read back with their expected values through the restarted
  node, and the normal §4 parity checks still pass through the cluster;
- the strict rejection test passes in the SPDK-linked box checkout:
  `VALS_SPDK_LINK=1 cargo test -p vals-spdk --features spdk
  sealed_owner_binding_rejects_cross_thread_use_and_rebind`.

Repeat the ordinary non-destructive restart one node at a time if the candidate
is being certified fleet-wide. On any failure, stop that node and retain its
config, complete log, core, binary hash, and catalog/key evidence; do not retry
with the affinity check disabled and call that a pass. Restore the pre-gate
service state; destroy the gate corpus only with a separately planned offline
provision command.

---

## 5. Launch bootes (on devbox)

bootes builds against the local valsgo via a `replace` directive in `go.mod`
(`replace github.com/whyrusleeping/valsgo => ../vals-examples/valsgo`). No
go.work overlay or indigo build script anymore — a plain build works.

```sh
cd ~/code/bootes
go test ./... && go build -o bootes .
mkdir -p soak
TS=$(date -u +%Y%m%dT%H%M%SZ)
setsid nohup env GOMEMLIMIT=16GiB JETSTREAM_API_KEY=<key> ./bootes ingest \
  --vals "$NODES" \
  --metrics-addr 127.0.0.1:9091 \
  --log-file soak/bootes.$TS.log \
  </dev/null >soak/bootes.$TS.out 2>&1 &
echo $! > soak/current-pid; echo $TS > soak/current-ts
```

Notes:
- **`ingest` is a required subcommand** (`./bootes ingest --vals …`), not just flags.
- A fresh cluster has no `cursor/jetstream` key, so bootes replays the sealed
  archive **from seq 0** (whole-network history) and then cuts over to the live
  tail through the same loop — there is no separate backfiller or repo state to
  retire. On restart it resumes from the cursor persisted at the last clean
  shutdown (vals only saves it after a successful drain).
- `--jetstream` defaults to `https://jetstream.us-west.bsky.network`.
  `--live-only` skips replay for a tail-only run (no API key needed).
- Archive replay is **byte-metered against the API key**: part downloads that
  hit `429 byte limit exceeded` fail the segment after 3 attempts and surface
  as `jetstream stream error (continuing)`; the SDK then restarts the stream
  from its in-memory cursor, so nothing is lost, but pacing settles to the
  quota refill rate. **On a from-zero replay the progress stats are
  misleading** (0230): `planned_through`/`plan_pages` stay 0 and
  `residual_gap` stays pinned at `sealed_tip` for the whole run, because no
  plan page ever *completes*. Judge progress by `cursor` advancing (and
  `ingest_jetstream_events_total`). With a nonzero resume cursor the stats
  report real values and `residual_gap` shrinks as documented.
- **Pass `--shutdown-timeout 300s`** for soaks. The 30 s default cannot
  quiesce producers when a rehome stall is in flight (60–100 s, task 0231),
  and an expired deadline withholds the cursor even with zero data loss.
- bootes drops a batch only after 12 retry attempts (~75 s backoff + in-flight
  timeouts); `attempt=` values near 11 in `batch flush failed` warnings mean
  cluster stalls are brushing the budget. With 0231's bounded seeding the
  2026-08-23 gate run saw most retries resolve at attempt ≤ 4 and only one
  exhausted budget in 5.5 h; frequent deep-retry bursts indicate a regression.
- Oversized archive records (>1 MiB, which the vlog segment cannot store) are
  filtered client-side with `vals: skipping oversized record` + an
  `oversized_records` count in the shutdown summary. Before that filter, one
  such record poisoned its whole 1000-entry chunk.
- The vals sink uses one ordered `PutBatch` lane per zone and overlaps records
  with backlinks. Keep that shape initially: it preserves same-key LWW order and
  already exceeds the historical application ingest rate. A mature hardware
  probe found a clean knee at eight concurrent 1,000-entry calls and overload at
  twelve, so any future multi-lane producer must key-partition for ordering,
  retry Busy, and pass a fresh 1/2/4/8 lane sweep rather than merely adding
  goroutines. See `bench/task-0200-atproto-backfill/FINDINGS.md`.
- If the SDK's default parallel segment-download fanout under- or over-drives
  the sink, tune `--download-concurrency` (0 = SDK default).
- For repeated, source-stable performance runs, use the local cache workflow in
  §5.1. It removes archive metering, WAN variance, and the live tail from the
  workload, leaving bootes decode plus vals ingestion as the load generator.
- Memory: the CAR-walk spill machinery is gone; the producer is one decoded
  stream plus sink buffers. `GOMEMLIMIT=16GiB` is generous headroom, not a
  requirement.
- Metrics on **9091** — port 9090 is taken by another local service on devbox.

### 5.1 Cache one archive snapshot, then replay it locally

For repeated performance runs, capture the sealed archive once while running a
fresh soak. Use a fast filesystem with enough free space for the full-network
archive; the cache is rolling compressed Jetstream `.jss` files of roughly
256 MiB each, not a small metadata cache.

```sh
TS=$(date -u +%Y%m%dT%H%M%SZ)
CACHE=/data/jetstream-cache-$(date -u +%Y%m%d)
setsid nohup env GOMEMLIMIT=16GiB JETSTREAM_API_KEY=<key> ./bootes ingest \
  --vals "$NODES" \
  --jetstream-cache-record "$CACHE" \
  --shutdown-timeout 300s \
  --metrics-addr 127.0.0.1:9091 \
  --log-file soak/bootes-cache-record.$TS.log \
  </dev/null >soak/bootes-cache-record.$TS.out 2>&1 &
```

Add `--jetstream-cache-max-events N` to seal the cache **complete** once it
holds N commit events instead of recording through the whole sealed archive
(which is several billion events): the recorder finishes the in-flight batch,
marks the manifest complete, and bootes exits through the normal drain path.
The 2026-08-24 1B-event capture measured ~175 bytes/event on disk
(~175 GB/billion).

In record mode **any** archive download failure is fatal by design (snapshot
contiguity), and the SDK's default per-part budget (3 attempts, waits clamped
to 30 s) cannot ride out a metered-quota exhaustion — a drained byte quota
kills the run in minutes. Run recording under
`bootes/soak/record-supervisor.sh`, which loops resumable attempts until the
manifest is complete (touch `soak/STOP` to halt).

**Prefer `--record-only` for pure captures.** It discards all sink writes and
keeps the cursor in `<cache-dir>/cursor.local`, so no vals/ClickHouse backend
is needed and cluster health cannot throttle the capture: the coupled vals
sink capped recording at ~11-18M events/h (and ~0.3M/h against a degraded
cluster), while record-only sustained **~500M events/h** on the same WAN and
API key (2026-08-28). Couple the sink only when the incidental ingest soak is
itself wanted. When switching an existing cache to record-only, seed
`cursor.local` with the manifest's `last_seq` first — a zero local cursor
with a non-empty cache re-downloads from seq 0.

**Do not pass `--download-attempts` (SDK bug):** `WithMaxDownloadAttempts`
builds a sparse `xrpc.RetryPolicy{MaxAttempts: Some(n)}` (engine.go:74) whose
unset fields panic (`option has no value set, but Val() was called`) on the
block-fetch resume path (`JetstreamGetBlock`). Fine on fresh sweeps, fatal on
mid-segment resume. Until fixed upstream, rely on the supervisor loop for
quota exhaustion.

Recording is resumable, but a **new** cache directory requires a zero sink
cursor so it cannot accidentally label a suffix as the full corpus. The mode
pins a finite sealed-archive snapshot, records commit mutations (the complete
vals record/backlink workload), and exits instead of cutting over to the live
WebSocket. Any archive download error leaves `manifest.json` incomplete; rerun
the same command/directory to resume. The cache only becomes replayable after a
clean archive EOF atomically marks the manifest complete.

For each later perf run, format/start a **fresh vals cluster** as usual and
replay the fixed cache with no API key or Jetstream network dependency:

```sh
setsid nohup env GOMEMLIMIT=16GiB ./bootes ingest \
  --vals "$NODES" \
  --jetstream-cache-replay "$CACHE" \
  --shutdown-timeout 300s \
  --metrics-addr 127.0.0.1:9091 \
  --log-file soak/bootes-cache-replay.$TS.log \
  </dev/null >soak/bootes-cache-replay.$TS.out 2>&1 &
```

Replay honors `cursor/jetstream`, so restarting an interrupted run on the same
cluster resumes within the cache. Replaying into a cluster that already holds
the cache's final cursor intentionally emits nothing. At cache EOF bootes uses
the normal sink drain, loss accounting, and cursor-persistence path.

---

## 6. Monitor

```sh
# cluster-side: totals climb, errors stay 0
for h in 10.0.0.114 10.0.0.18 10.0.0.200; do echo -n "$h: ";
  curl -s $h:9464/metrics | grep -E '^vals_(records_written|server_errors|busy_rejections|ram_busy_rejections|ram_headroom_bytes|memtable_bytes_total|flush_build_bytes)' | tr '\n' ' '; echo; done

# node RAM — the real health signal. It must stay BOUNDED (single-digit GiB).
# A node whose MemoryCurrent climbs toward box RAM is heading for an OOM kill
# (the 2026-08-01 stale-binary failure): a region filled and flush wedged, so
# the memtable grew unbounded. With current valsd it stays flat.
for h in 10.0.0.114 10.0.0.18 10.0.0.200; do echo -n "$h RAM: ";
  ssh user@$h "systemctl show vals-node -p MemoryCurrent --value | awk '{printf \"%.1f GiB\n\", \$1/1073741824}'"; done

# bootes-side: real trouble looks like 'vals: … write quorum failed' /
# 'dropping batch'; jetstream stream errors log as
# 'jetstream stream error (continuing)' and are retried by the SDK:
grep -cE 'write quorum failed|dropping batch' ~/code/bootes/soak/bootes.$TS.log   # want 0

# replay progress (30 s cadence): residual_gap shrinking toward 0 = archive
# sweep advancing; residual_gap=0 = cut over to live. Also exported as
# ingest_jetstream_* on 127.0.0.1:9091/metrics.
grep 'jetstream progress' ~/code/bootes/soak/bootes.$TS.log | tail -3

# producer RAM (one decoded stream + sink buffers; should be low single-digit GiB):
ps -o pid,etime,rss,%cpu,cmd -p "$(cat ~/code/bootes/soak/current-pid)"
```

Healthy baseline (current valsd, 2026-08-01 rerun): ~7.5M `records_written`
cluster-wide in ~30 s, **0 server_errors, 0 busy_rejections, 0 vals write
errors**, and node RAM **bounded at 0.4-1.0 GiB**. RAM (not device) is the
ceiling — ~60 GiB/box, lab-3 only ~30 GiB — so watch it. If a region fills,
current `valsd` returns retryable `Busy` (visible as rising `busy_rejections`)
and auto-splits under load. If RSS reaches the configured process budget,
`ram_busy_rejections` rises and `ram_headroom_bytes` reaches zero while client
writes are shed with the same retryable `Busy`; reads and maintenance continue.
Either Busy counter climbing a little under a write storm is fine; RSS must not
continue marching toward the kernel OOM threshold.

**Gotcha (auto-split + debt-rehome churn — the current soak wall, 0230/0231):**
once ranges start auto-splitting (~1M entries each), PD debt-rehomes the
populated split children back to balanced placements, and each rehome stalls
the donor shard's applies for 60–100 s (`seed_step` monopolizes the executor —
task 0231). Signature: `batch flush failed … timed out` bursts on the client,
`rehome abort … mirror failures reported` loops in pd.log, cluster write rate
sawtoothing to a few k/s. Also watch for valsd process death (task 0232 hit a
`double free or corruption` SIGABRT under this churn on 2026-08-22; the
2026-08-23 5h31m gate run on the 0245-tripwired binary survived cleanly):
`systemctl is-active vals-node` on all three boxes. All boxes now set
`core_pattern=/home/user/vals/run/core.%e.%p` (a sysctl — re-set after reboot)
and node launches should pass `--property=LimitCORE=infinity` so any
recurrence or `VALS SPDK FATAL` tripwire abort yields a core. A node down under RF 2 means every range homed on its two
pairs fails W=2: bootes drops those batches after the retry budget and the
loss is only healed by the next from-cursor (or from-zero) replay.

**Gotcha:** a `valsq count` over a whole multi-million-row collection can blow
its 5 s RPC deadline during the write storm and report `0` — a measurement
artifact, not missing data. Verify with a bounded `scan -n` or a narrow
sub-range count.

**Gotcha (backlinks capacity wall — ELIMINATED by 0164, but keep the levers):**
before 0164, a full-rate soak's backlinks pair hit its zone-slab reserve floor
at ~60–70 min: numeric `presplit=8` concentrated ~all backlinks (99.8 % live,
write-once `ref\0source\0path` keys — the 0161 re-diagnosis) onto one range
pair, and the slab was 32 GiB. With the §3 distribution-aligned 48-range
presplit **plus** `zone_region_mib = 81920`, the 2026-08-10
`run-…-0130-ceiling` arm measured the full lifetime: **6 h 25 min at full
rate** with zero busy/ram_busy/errors/plan-failures, then the hottest node's
**records** engine (not backlinks!) hit the maintenance floor. Residual
placement skew is the ceiling: random token→pair assignment gave n1 1.79× n2's
applies, and records **auto-split children inherit their parent's placement**,
so one node's records engine ate its 80 GiB slab in ~6.4 h while another still
had 74 % free (levers — balance-aware placement 0120, load-based splits 0118,
gated re-home 0155). RAM is NOT the next wall at this corpus (~106–111 B/key
measured ⇒ ram_busy needs ~360 M keys/node ≈ 11–12 h). Watch
`vals_min_zone_free_segments` (starts at 81,920/slab): decline should
DECELERATE after the first hour; a pair declining linearly toward the floor
means placement has re-concentrated; late-run decline rates give ~50+ min of
warning. At the floor the engine stays live but the **whole producer stalls
within ~15 min**: bootes' per-zone batch pipelines head-of-line block on the
floored ranges (`N/1000 entries below W=2 acks (N busy)`, 30 s backoff loops),
busy_rejections plateau, and a bounded 1/min WARN loop (`compaction plan failed
(retrying)`, `compaction_plan_failures` +60/min) runs. Since task 0163 a
mostly-live floor engine turns capacity-terminal even with a non-empty
memtable: after ~3 consecutive infeasible pressure plans (with visible dead
low and the shadow estimate under one segment) writes get
`Response::Err("zone region exhausted…")` + `region_exhausted_rejections`
instead of endless Busy. Treat reaching the floor as the end of the soak's useful life. Reclaim cannot help a mostly-live
engine: the 0161 shadow estimator reads ~0.3–1.5 GB
(`vals_shadowed_value_bytes_est`) and its pre-floor fold correctly never
fires (`vals_shadow_folds` 0). A genuinely too-small region that empties its
memtable surfaces `Response::Err("zone region exhausted…")` +
`region_exhausted_rejections` — correct signaled behavior, not a wedge. It is no
longer a pre-OOM warning: 0153 bounded merge and cooperative-seed working
sets (swap-aware budget), 0154 bounded the AE digest responder to O(page),
and the 0152 run-3 validation replayed the full 2026-08-05 corpus through the
former kill windows with flat RSS, zero OOM kills, and bounded
`ae_digest_serve_bytes`
(`bench/replay-lab/run-20260808T010817Z-rambudget40g-0154/FINDINGS.md`).
If RSS ever marches anyway, the burst signature to look for is: growth with
`merge_in_flight`=0 and `ae_seed_staged_bytes`=0, that node's per-shard
counters frozen, and one `vals-shard` thread pinned at 100% CPU — capture
threads + perf before it dies.

---

## 7. Stop (bounded and loss-aware)

`bootes ingest` defaults to one **30 s** drain deadline (plus at most 1 s for
final diagnostic logging/process guard). Override it with
`--shutdown-timeout DURATION` or `ATTIE_SHUTDOWN_TIMEOUT` when launching. The
first SIGTERM immediately stops batch admission, waits for the in-flight batch
to quiesce, drains vals buffers, and only then persists the final jetstream
cursor. Every phase shares that one deadline; it is not 30 s per phase. If
producer quiescence times out, sinks are left open and the cursor is withheld
to avoid racing late writes. A second SIGINT/SIGTERM requests immediate force
exit.

```sh
# bootes first; wait up to the configured 30 s plus a small scheduling margin:
pkill -TERM -f 'bootes ingest'
timeout 35s sh -c 'while pgrep -f "[b]ootes ingest" >/dev/null; do sleep 1; done'

# Do not stop vals until bootes exits (or the bounded wait reports failure).
# Then stop the cluster (systemctl stop sends SIGTERM → drain, sync, persist):
for h in 10.0.0.114 10.0.0.18 10.0.0.200; do ssh user@$h 'sudo systemctl stop vals-node'; done
ssh user@10.0.0.114 'sudo systemctl stop vals-pd'
```

A clean log ends with `vals shutdown summary ... deadline_expired=false`, then
`cursor saved on shutdown`, then `ingester shutdown complete`. **The cursor is
persisted only when the run lost nothing**: any dropped batch during the run
(`lost=N` in the drain report) withholds it, and the next start replays from
the previous cursor (seq 0 on a fresh cluster) — idempotent, but it re-pays
the archive byte quota for the replayed window. Inspect the vals
summary's admitted/persisted/buffered/inflight/dropped/rejected record and
backlink counts. On timeout or a hard drain failure, bootes emits
`ingester shutdown incomplete`/`shutdown deadline expired` with quantitative
remaining and lost counts, and explicitly logs that final cursor persistence
was withheld. That is an unclean stop even though the process exits within the
bound; retain the log and expect replay from the older cursor. Do not SIGKILL
before the deadline unless immediate data loss is preferable.

The transient units are `--collect`, so they vanish after stop. A subsequent
fresh soak repeats from step 1: stop all nodes, wipe PD raft state, and run the
offline serial-checked provision command for each node before daemon startup. To
*resume* the same data instead, do not provision and skip the `rm -rf run/pd`
wipe — but the recurring job is a fresh soak.

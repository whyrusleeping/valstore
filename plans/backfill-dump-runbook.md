# bootes backfill → 1B → `valsd dump` — runbook (task 0095)

> **⚠️ 2026-07-15: `oldbox` is no longer accessible** (replacement
> machines are being provisioned). The recipe itself stays valid; substitute
> the new hostname and re-verify the box-specific caveats (vfio/hugepages,
> sudo env, pd.sock perms) on the new hardware.

Reusable recipe for the bootes-on-vals live test on `oldbox`: ingest
the Bluesky network to a target record count, profile along the way, and
capture a replayable `.vdump` corpus for write-path benchmarking. First
execution 2026-07-09: results + hot-spot analysis in
`bench/bootes-live-1b/notes.md`; harness lives in `vals-examples/backfill/`.

## Topology (fixed by the harness)

4 × `valsd` SPDK nodes (one 14 TB NVMe + one pinned poll core each, RF=1,
AE off, sorted zones `records`/`backlinks` pre-split across drives) + PD;
bootes ×NSHARD processes (`--shard n/N`, shard 0 also runs the firehose),
`--parallel-backfills PAR` each. Collectors/watchdog/profiler per
`backfill/README.md`.

## 1. Sync + build

```sh
# local (keep the sibling layout vals / vals-examples / bootes):
rsync -a --exclude target --exclude .git ~/code/vals/          oldbox:~/vals/
rsync -a --exclude target --exclude .git ~/code/vals-examples/ oldbox:~/vals-examples/
rsync -a --exclude .git                  ~/code/bootes/        oldbox:~/bootes/
# on the box:
ssh oldbox
chmod +x ~/vals-examples/backfill/*.sh     # rsync drops on-box exec bits
cd ~/vals && VALS_SPDK_LINK=1 cargo build --features spdk --release -p vals-server
cd ~/vals-examples && cargo build --release -p valsq -p valsdump
cd ~/bootes && go build -o ~/bootes/bootes .   # lib.sh's BOOTES_BIN path
```

Archive any previous run first: `mv ~/run/backfill ~/archive/backfill-<date>`.

## 2. Cluster up (TRIM matters for the dump)

```sh
cd ~/vals-examples/backfill
sudo ./cluster.sh setup     # vfio reset → blkdiscard ALL 4 scratch drives → rebind
sudo ./cluster.sh start     # MUST be root (vfio DMA remap fails unprivileged)
sudo chmod 666 ~/run/backfill/cluster/pd.sock   # unprivileged harness needs it
```

- **Verify the TRIM lines printed** — a non-TRIMmed drive leaks stale
  pre-format records into the eventual dump (task 0044).
- Before using this archived harness with a current binary, remove its `FORMAT`
  config knob and run the offline serial-checked `valsd provision` workflow from
  `docs/deploy.md` after `setup` and before `start`. Ordinary mid-run node restarts
  then only reopen; they cannot format the drive.
- Failed start attempt? Clean `/dev/hugepages/spdk*` + `/var/run/dpdk/*`
  before retrying (leaked hugepages halve the next attempt's budget).

## 3. Run to the cap

```sh
NSHARD=4 PAR=600 ./run.sh          # collectors + watchdog + periodic profiler + bootes ×4
nohup setsid ./milestone.sh </dev/null >logs/milestone.out 2>&1 &
# milestone.sh env: MILESTONES="100000000 500000000 900000000" CAP=1000000000 POLL=15
```

`milestone.sh` fires `profile.sh` (bootes pprof + perf of the busiest valsd +
status snapshot) at each milestone and at CAP takes a final capture then
**gracefully stops bootes only** (SIGTERM → buffers drain, cursor persists),
leaving cluster + collectors up.

Monitor: `./status.sh 24`, `tail obs/alerts.log`, or the truth directly:
`curl -s 127.0.0.1:946{1..4}/metrics | grep vals_records_written` (sum = cap
counter). Expect ~5 h to 1B on the 2026-07 build (June-26 build did it in
~1h43m; decline over time is the known 0069/0094/0096 regime, not a fault).

## 4. Capture the dump (cluster down, offline, parallel)

```sh
./stop.sh                                  # collectors (bootes already stopped)
./cluster.sh stop                          # NOTE: 30 s grace then SIGKILL — see caveat
sudo rm -rf /var/run/dpdk/* /dev/hugepages/spdk*
cd ~/run/backfill && mkdir -p dump
for i in 1 2 3 4; do ( for z in 1 2; do    # zone-id 1=records, 2=backlinks
  sudo env LD_LIBRARY_PATH=/home/user/why/vals/third_party/spdk/dpdk/build/lib \
    /home/user/why/vals/target/release/valsd dump -c cluster/node$i.conf \
    --zone-id $z --out dump/node$i.zone$z.vdump --parallel 16 \
    >> dump/node$i.dump.log 2>&1; done ) & done; wait
sudo chown why:why dump/*
grep -h "valsd dump:" dump/*.dump.log      # per-file record/tombstone/byte totals
```

- `sudo -E` does NOT carry `LD_LIBRARY_PATH` — use `sudo env ...` (above).
- SPDK offline dump needs `--zone-id` (names aren't on-device) and the cluster
  DOWN; all 4 nodes dump concurrently (independent drives). 2026-07-09: 1B-run
  corpus (1.85 TB) dumped in ~19 min at 0.78–1.97M rec/s per file.
- Disk: budget ≈ `record_bytes_written × inflation` (see below) on the OS
  RAID; check `df -h /` first.

## 5. Know what the corpus IS

- **Raw, not 1:1**: the vlog scan includes freed-but-unreused segments, so the
  dump carries each record once per co-compaction rewrite (2026-07-09: ×3.25
  the 1.003B written). **Last-wins replay converges to the exact final state**
  (epoch order = temporal order), but a replay bench drives the inflated
  volume. For an ingest-only corpus, fence freed segments (0044 follow-up) or
  dedup at replay.
- `records` zone carries tombstones (~298k at 1B) — replay with the
  tombstone-aware `valsdump replay` (vals-examples `7849f79`+). **The full
  replay-benchmark procedure is `plans/backfill-replay-runbook.md`** (incl.
  the `zone-1`/`zone-2` header-name trap for SPDK-path dumps).

## 6. Teardown / leave clean

`pgrep -x valsd; pgrep -x bootes` → nothing; hugepages freed
(`grep HugePages_Free /proc/meminfo` ≈ total); drives stay vfio-bound with the
cluster data intact (an ordinary restart revives it via the slow O(dataset)
boot, task 0005). Record results in a `bench/` notes file.

**Caveats bank**: `cluster.sh stop`'s 30 s grace SIGKILLs nodes still draining
big datasets (harmless for the dump — raw vlog — but kills the fast-boot
snapshot); never `pkill -f <script>.sh` from an inline ssh command (matches
the remote shell itself); bootes counters render in scientific notation
(`awk '{printf "%.0f",$2}'`); vals gauges read 0 with AE off — trust counters.

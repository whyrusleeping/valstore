# Replaying a `.vdump` corpus — lab-era benchmark runbook

How to replay a captured `valsd dump` corpus into a **fresh 3-node lab SPDK
cluster** as a repeatable write-path benchmark. First executed for task 0132
(two arms: `--tier one` + `--tier nosync`); earlier hardware-validation replays
of the same corpus live in `bench/replay-lab/run-2026080{7,8}*` (0152–0154).

**Current corpus:** `/home/user/code/bootes/soak/dump-20260805T1515Z` on
devbox — 16 raw `.vdump` files (node{1,3}.{records,backlinks}.shard{0..3}),
**155,158,024 records** (154.72M puts + 437,802 tombstones), 82 GiB raw,
~116M distinct keys, captured from the 2026-08-05 bootes soak at RF=2.
Headers carry the **real zone names** (`records`/`backlinks`) — the
`zone-1`/`zone-2` header trap only afflicted the old oldbox captures
(see Caveats).

> The pre-2026-08 procedure for the retired 4-drive `oldbox` rig
> (1B corpus, zones `zone-1`/`zone-2`) was replaced by this document; see git
> history if that rig ever returns.

## How `valsdump replay` works (what shapes this plan)

- `valsdump replay --nodes <id@addr,...> --in <prefix> [--batch N]
  [--tier nosync|one|majority|all] [--r N] [--rf N]` — finds every
  `<prefix>*.vdump` in the prefix's directory, spawns **one coordinator thread
  per file**, streams puts through `write_batch_in` (batch default 1000) and
  tombstones as coordinator deletes. Errors are counted, not fatal — the client
  does **not retry**, so any Busy burst shows up as write errors.
- Each thread reads the zone name from the file header and resolves it in the
  *target* cluster's catalog; the target must have sorted zones with those
  names or replay dies with "zone not in the cluster catalog".
- Any prefix subset works and every subset spans the whole target cluster
  (keys re-place by range hash): `--in <dump>/node` = all 16 files
  (16 threads), `--in <dump>/node1.` = one source node (8 files, ~68.8M
  records). Numbers are not comparable across subsets — record which one ran.
- The corpus replays **bigger than the ingest** (rewrite history; last-wins
  replay converges to the final state). Compare replay runs to each other, and
  to live ingest only per-record.

## Capacity + RAM math (why the full corpus fits — task 0132)

- **Device:** full corpus at RF=2 ≈ **310M physical applies**. The 0130
  ceiling soak sustained 484M applies before the first zone-slab floor with
  the 0164 levers (48-range real-key backlinks presplit + 80 GiB
  `zone_region_mib = 81920` slabs — both baked into the box node.confs / §3
  of the soak runbook). Without them (32 GiB slabs, numeric `presplit=8`) the
  reserve floor arrives mid-corpus and the unretried client eats tens of
  millions of write errors (runs 1–3, 2026-08-07/08). Measured 0132 troughs:
  hot-node `min_zone_free_segments` dipped to ~23.7k of 81,920 (~71% of the
  hottest slab transiently used) before merges reclaimed — comfortable, but
  not room to double the corpus.
- **RAM:** ~106–111 B/key resident (measured thrice); ~116M distinct keys ×
  RF 2 with ~2× placement skew ⇒ ~17 GiB peak RSS on the hottest node
  (0132 arms measured 16.6 GiB max) — far under the 40 GiB budget.
  Post-0153/0154 there is no runaway working set; the 08-06 OOMs cannot
  recur (validated in
  `bench/replay-lab/run-20260808T010817Z-rambudget40g-0154/`).

## 1. Target cluster (fresh)

Exactly §§0–3 of [`bootes-soak-runbook.md`](bootes-soak-runbook.md): refresh
host keys, verify hugepages/vfio (re-run SPDK setup if a box rebooted),
**rsync + rebuild valsd from current HEAD (§0.5 — do not skip)**, fresh PD
raft wipe, offline serial-checked `valsd provision` on every stopped node, then
an ordinary node start with `VALS_RAM_BUDGET_BYTES=42949672960`. Create zones per §3: `records 2 sorted` + 5 canonical hot-collection
splits, `backlinks 2 sorted presplit=<48 real-key boundaries>`. Verify
`vals_pd_sorted_ranges` = 54 on PD `:9465`, then run the §4 interop preflight
(records + both backlinks ref families).

## 2. Observability

```sh
cd ~/code/valstore
OUT=bench/replay-lab/run-$(date -u +%Y%m%dT%H%M%SZ)-tier-<one|nosync>
mkdir -p $OUT; date -u +%s > $OUT/start.epoch
nohup setsid sh bench/replay-lab/collect-replay.sh $OUT 600 30 \
  </dev/null >$OUT/collector.log 2>&1 &
```

`collect-replay.sh` samples every 30 s into `$OUT/timeseries.csv` (per-node
records_written, busy/ram_busy/region_exhausted/errors, plan_failures,
free/min_zone_free/used segments, batch_apply latency sum/count + >1s bucket,
batch_apply_gate_waits, batch_apply_run_reads, live_keys, RSS, run_merges,
resident_runs, PD splits/ranges) and drops full metric pages under
`$OUT/snapshots/` every 5 min.

## 3. Replay (from devbox)

```sh
nohup setsid ~/code/vals-examples/target/release/valsdump replay \
  --nodes '1@10.0.0.114:7001,2@10.0.0.18:7001,3@10.0.0.200:7001' \
  --in /home/user/code/bootes/soak/dump-20260805T1515Z/node \
  --batch 1000 --tier one --r 1 --rf 2 \
  </dev/null >$OUT/replay.log 2>&1 &
echo $! > $OUT/REPLAY_PID
```

- **`--tier one`** = group commit before ack — the ingest-faithful arm.
  **`--tier nosync`** = no durability barrier — the write-path ceiling arm.
  Run each against its own freshly formatted cluster (repeat §1 between arms).
- Full corpus takes **~1.5–2 h per arm** (0132 arms: tier one 102.8 min,
  peak 223k physical applies/s decaying through run-count regimes to a
  single-thread tail on the largest files; tier nosync peaks ~360k/s).
  Budget ~2 h wall per arm including cluster setup.

## 4. What to record (per arm)

- Replay log's final summary line (aggregate only): records, tombstones,
  **errs (must be 0)**, wall time, rec/s; plus the cluster
  `vals_records_written` delta
  (≈ 2× logical records replayed at RF=2, minus same-batch dedup),
  `server_errors` = 0, busy counters ≈ 0.
- Throughput-vs-time curve and batch_apply latency/gate-wait/run-read series
  from `timeseries.csv`; min_zone_free decline (should decelerate, never
  approach the floor); RSS bounded; split activity (`pd_splits`,
  `pd_sorted_ranges`).
- Spot validation: preflight keys still read back; a few narrow
  `valsq --zone records count` sub-ranges (stable across arms of the same
  corpus); `valsq get` of known finally-tombstoned keys → absent
  (`$OUT/tombstone-sample-keys.txt`; extractor: scan the `.vdump` for keys
  whose last occurrence is the `0xFFFF_FFFF` tombstone sentinel).

## 5. Stop / teardown

Soak-runbook §7 minus bootes: `sudo systemctl stop vals-node` on all three
boxes, then `stop vals-pd` on lab-1; kill the collector. `--collect` units
vanish after stop; the next arm repeats §1 (PD wipe + offline provisioning before
node startup = clean cluster). Keep the dump — it is the reusable input.

## Caveats bank

- **Zone-name header trap (historical):** old SPDK `valsd dump` captures wrote
  `zone-<id>` headers; check `head -c 16 <file> | od -c` before building the
  target catalog. The 2026-08-05 corpus carries real names.
- One replay thread per file, client ids 3000+t — don't run two replays
  concurrently against one cluster without disjoint id ranges.
- The unretried client turns any sustained Busy into write errors; if
  `min_zone_free_segments` declines linearly toward the floor mid-run, the
  arm is invalid as a clean benchmark — stop, fix capacity (levers: presplit
  depth, `zone_region_mib`, subset), and re-run.
- A `valsq count` over a whole collection can blow its 5 s RPC deadline during
  the write storm and report 0 — measurement artifact; use narrow sub-ranges.
- All the box footguns from `bootes-soak-runbook.md` apply (one-line
  `sudo systemd-run`, `LD_LIBRARY_PATH` for every valsd/admin call, pd.sock
  perms, stale-binary trap).

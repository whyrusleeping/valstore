# A/B and hardware benchmark harnesses (recipes)

Recipes, timings and caveats for the A/B harnesses that later tasks reuse. They were
moved here from memory.md on 2026-10-21. These are observations from past runs, so check
them against the harness source before relying on them. Box facts are in
[box-provisioning-runbook.md](box-provisioning-runbook.md), the local 3-node recipes are in
[local-multinode-valsd.md](local-multinode-valsd.md), and supported entry points are listed in
[bench/HARNESSES.md](../bench/HARNESSES.md).

## Local (devbox)

devbox has no perf (`perf_event_paranoid=4`). Its rates shift with other sessions' load, so
run repeat or ABBA pairs. Avoid heavy cargo work there during L1 arms, because bootes runs there.

- **Write path at R3's range count (3 nodes, RF 2):** `bench/0420-rangemap-borrow/ab.py`. It
  uses the file backend, 17k inherit-split ranges and bootes on the 1B-cache slice, and takes
  about 16 min per arm. At low range counts, shard µs/write tracks client rate on the shared box
  (±12%). Since 0449, its hard-coded `CACHE` (`/tmp/0408/it/cache`) symlinks the first two
  `jetstream-cache-5b-20261001` segments with 0435's hand-written slice manifest. The old
  20260824 manifest is kept as `manifest.json.20260824`.
- **Reads (1 node, RF 1):** `bench/0339-read-identity-check/ab.py`. It runs point gets via
  `vals-bench netread` and exact scans via real-client15 scanbench over 8M+8M keys, about
  7 min per arm. Same-semantics rebuilds shift scan shard µs/entry by about 1–2% (layout), so
  use at least 3 ABBA pairs plus a decode microbench to attribute small deltas.
- **AE availability under a queue workload:** `vals-bench <mem|file> rtqueue 0 100 <dir>` runs
  3 in-process nodes plus a PD with a sorted RF 3 zone and `VALS_RTQ_*` knobs. The matrix is
  `bench/0465-rt-ae-busy/run.sh` (about 9 min). In-process, any zone with ≥1k writes/s loses
  most AE reconcile slots to tree-scope-token exhaustion (task 0467). Read fold Busy from
  `rt_fold_not_ready_busy` and local skips, not from the total.
- **Sorted-zone RSS:** use `VALS_RSS_ORDERING=sorted VALS_RSS_KEY_SHAPE=replay` with
  `vals-store` `engine/tests/rss_bench.rs`. The default unsorted 8-byte HashIndex arm does not
  represent the soak zones.
- **Scan concurrency on the file backend:** file-backed scans never interleave on a shard (its
  reads are synchronous), so the scan gate never fills locally. To model SPDK scan concurrency,
  apply `bench/0328-scan-pacing/l1-file-read-yield.patch` and set `VALS_L1_FILE_READ_YIELD=1`.
- **Go client interop:** `~/code/vals-examples/run-interop.sh` (3 local nodes plus a PD on ports
  17101–17103) takes about 10 s with warm builds. Since 0468 (2aa077c), legs 7–10 cover range
  deletes, including a leg with node 3 lagging through a restart. To mutation-test valsgo with
  it, use a copy of the script without `go test` and remove the copy afterwards. Only valsq and
  valsdump build in that workspace (kvchurn, eventlog and valsmix: task 0470).

## lab

- **3-node replay arms** (`bench/0363-compact-values/run_full1.py`): about 40 min per 1B ACK,
  including discard and checks. lab-2 crashes dominate whole-arm throughput (0398). In soak
  triage, `soak5b-samples.jsonl` is a filtered subset, so check the full per-run
  `observer.jsonl` before calling a metric missing (0401).
- **L1 one-shard arms:** `bench/0414-shard-growth/arm.sh` adds a dtlb `walk_active` (`tlb`)
  capture. An arm to 333M writes takes about 34 min. The file and mem backends are single-shard
  only, so effects that depend on shard count (for example 8 shards × mallinfo2) need SPDK
  hardware. THP is `madvise` on lab-1/2/3. lab-3 has cargo, for running test suites off
  devbox during arms.
- **Write path by range count with perf (lab-1):** `bench/0435-range-residual/ab.py` runs 3
  file-backed nodes and the PD pinned to P-cores 0–7 and bootes on 8–23, with a hand-made
  2-segment slice of jetstream-cache-5b in `~/vals0435/cache`. A 17k-range arm takes about
  16 min and repeats within ±0.02 µs/write. `~/vals0442` (and the older `~/vals0449`) are
  ready bases: set `VALS0435=~/vals0442`. bootes and the cache are symlinked to `~/vals0435`.
  `build.sh` builds base and fix from a git-archive tar plus `fix.patch`, and `chain.sh` runs
  ABBA arms under `nohup setsid`, because an ssh timeout kills the loop between arms. Write
  cost at 17k ranges was about 2.87 µs/write after 0442 and about 3.09 with 0449-era builds.
  All three A/B harnesses save each node's final `/metrics`.
- **Queue cluster (0471):** `bench/0471-queue-hw/hw.py` runs 3 SPDK valsd × 8 shards from
  `~/vals0471` (build, provision, start, fresh-zone, arm, redis-*). Create zones at least 60 s
  after any drop, or the presplit falls back to unbalanced placement (fresh-zone checks this).
  `hw.py provision` does not reset PD state, so drop old zones before an arm. R15 kept ring14's
  204 empty ranges in `vals_pd_sorted_ranges`. As of 0485 (2026-10-19) the boxes ran tree
  0d8827f6 with no zones, and 0492 later re-provisioned node 1. Check `hw.py status` first.
- **Debt re-home analysis:** `bench/0485-rehome-decisions/analyze.py <arm-dir> [zone]`
  reconstructs the PD's debt re-home inputs from the `decision-inputs.tsv.gz` that
  `bench/0477-covered-merges/run.py` writes. `control.py` runs an unsplit append-only balancing
  control.
- **lab-2 reboots:** a reboot (RAM) drops hugepages and rebinds the drive to nvme, so the
  0471 node can't restart until `setup.sh` reruns. `bench/0477-covered-merges/restart2.py` is a
  watchdog that does this. Kill it before `hw.py stop` or `provision`. After a box reboot (not a
  process crash), 8 of 32 netqueue producers hang in `ep_poll` for the rest of the arm and the
  drain never finishes (no `NQ_VERIFY`), so such arms must be rerun.
- **File vs SPDK engine on one drive:** `bench/0492-file-vs-spdk/run.sh` runs vals-bench in a
  `systemd-run` scope pinned to CPU 0, with an optional `MemoryMax` cap to bound the page cache.
  To return the drive to vfio after an XFS arm, verify the serial and `wipefs -a` it first.
  `setup.sh` refuses to bind a device that has a filesystem signature ("Active devices:
  data@…").

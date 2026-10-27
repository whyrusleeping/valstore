# SPDK backend bring-up

The production block backend is userspace poll-mode NVMe via SPDK
(`vals_spdk::spdk::SpdkBlockDevice`, behind the `spdk` cargo feature). SPDK is
vendored under `third_party/spdk` (gitignored). The mem/file backends cover all
engine development; SPDK is only needed for real raw-NVMe IOPS.

## Status

The SPDK backend is **implemented, optimized, and validated on real NVMe**. The
single-drive and JBOD scaling numbers below were measured on a 256-core EPYC box
with 8× Samsung PM9A3 (3.84 TB) datacenter NVMe. Through 2026-07 the test box
was `oldbox` (96 cores, Samsung datacenter NVMe), where the SPDK
single-node and 3-node-RF=2 cluster paths (incl. the 1B-key stress) were
re-exercised in 2026-06. *(That box was retired 2026-07-15;
box validation now runs on the lab-1/2/3 fleet — see
`plans/box-provisioning-runbook.md`.)*

- ✅ **Backend** (`crates/vals-spdk/src/spdk.rs` over `csrc/spdk_shim.c`): attach
  controller + namespace, one I/O qpair per core, sync `read/write`, batched
  `read_batch`, async `read_at_async`/`write_at_async` (non-blocking submit +
  completion→waker bridge), `poll`. `build.rs` compiles the shim and links SPDK
  when `VALS_SPDK_LINK=1`. Correctness verified (round-trip, crash recovery).
- ✅ **Single-drive performance (one core), at the device's raw ceilings:**
  - Reads: random-read scales with queue depth to **~1.0M IOPS / 4 GiB/s** (QD128+).
    QD1 is ~56 µs (NAND latency). *Note: benchmark reads must hit populated LBAs —
    reading unmapped/TRIMmed blocks returns zeros without a NAND access and
    inflates IOPS.*
  - Writes: **~3.7 GiB/s** (the device write-bandwidth ceiling) for ≥8 KiB values;
    4 KiB values **858k ops/s / 3.2 GiB/s**; small (100 B) values **>4M ops/s**.
- ✅ **Write-path optimizations** that took writes from 16.7k → 858k ops/s @4 KiB:
  present a **4 KiB logical block** over 512 B namespaces (a value was 8 QD1
  writes); **multi-block coalescing**; **pipelined async writes** (QD>1, drained
  at `sync`); a **DMA buffer pool**; and **hardware CRC-32C** (SSE4.2, scalar
  Castagnoli fallback) — the CRC was the final write-path wall.
- ✅ **Thread-per-core sharding**: `SpdkController::attach` once (vfio attaches a
  controller once) + `open_shard` hands each core its own qpair over a disjoint
  LBA region. Engine reads scale 360k(1 core) → **~1.0M IOPS (16 cores) = the
  device ceiling**. (Must reset thread affinity after `spdk_env_init` — DPDK pins
  the caller to one lcore, which would confine every shard to one core.)
- ✅ **Multi-drive JBOD** (`bench mdwrite`/`mdread`): attach K controllers, spread
  partitions across them. Near-linear: **8 drives → ~8.0M read IOPS, ~29 GiB/s
  write** (~8× the single-drive ceilings).
- ✅ **Distributed path on SPDK** validated (`bench distspdk`): 3 nodes × 2 drives
  (one engine per drive = JBOD shard), RF=3, over loopback TCP. Quorum
  writes/reads, replication, node-down availability, and hinted handoff all
  correct on real drives (0 mismatches).
- ✅ **Clustered throughput scaling** (`bench distmdmc`, 8× PM9A3 as 4 nodes ×
  2 drives × P partitions, **64 server cores**, RF=3): spreading the drives over
  many server cores lifts the cluster off the old coordination bottleneck — writes
  **~4.0M**, strong reads **~2.0M** (**~2.85M** with `ReadFanout::Quorum`), get-any
  **~3.8M** ops/s, 0-error. Large-value writes hit **~21 GiB/s = 70 %** of the
  29.9 GiB/s raw JBOD ceiling and get-any reads **~83 %** of the 6.6M-IOPS raw read
  ceiling — the drives are essentially **saturated**. The staircase that got here
  (coordinator write pipelining → connection multiplexing → unified server read loop
  → server-core count → read fan-out) is in [`ARCHITECTURE.md`](../ARCHITECTURE.md)
  §Status. *(These strong-read figures predate `ReadFanout::Digest`, now the default:
  one value read + version-only digests (a hedged R-quorum) ≈ 1 device read per strong
  read, vs R/RF. Hardware re-measured on this box — digest beats All and Quorum at every
  config, e.g. 64-core RF=3: 100 B reads 1.56M @16 coord / 3.85M @48 coord vs Quorum
  1.48M / 3.08M, the gap widening as the drives saturate. See ARCHITECTURE.md §Status.)*
- ✅ **Current real-NIC strong-read path** (2026-08-14, lab-1/2/3, one 9100 PRO
  and four shards/node, RF=2/R=2, latency profile): batched winner-directory
  prefetch and token reuse sustain a repeated **4.96M 100-byte strong reads/s** at
  width 256, versus 4.07M/s matched baseline (**1.22×**), with zero
  errors/misses/mismatches, one checked value read/key, zero run probes, and improved
  p50/p99. See
  [`bench/lab-read-breakthrough-20260814-0204/FINDINGS.md`](../bench/lab-read-breakthrough-20260814-0204/FINDINGS.md).
  A fresh post-optimization profile reproduced 4.95–5.00M/s and found all twelve
  single-threaded serving shards saturated; mixed-batch result construction/copying
  is the first actionable CPU/memory cost. A corrected populated-LBA sweep over the
  actual four 256 GiB production regions reaches 1.493–1.500M 4 KiB reads/s at
  QD128–256, establishing the depth knee but not a command-for-command live ceiling;
  do not name NVMe as the next wall until after the CPU A/B. Client fanout and the
  10 GbE link were excluded by controls. See
  [`bench/lab-read-ceiling-20260816-0205/FINDINGS.md`](../bench/lab-read-ceiling-20260816-0205/FINDINGS.md).
- **Read-path experiment conclusions (2026-08):** per-value allocation/DMA-retention
  and winner-directory scratch/SIMD spikes were neutral and retired (0206/0210/0211).
  Batched SPDK value reads (0207) did not clear the measured benefit gate; compact
  mixed-read encoding (0209) reduced network traffic but regressed throughput and
  latency. Neither candidate shipped. See the
  [0207 findings](../bench/lab-read-batch-20260819-0207/FINDINGS.md) and
  [0209 findings](../bench/lab-compact-wire-20260818-0209/FINDINGS.md).
  Current work belongs in the [backlog](../backlog.md), not this historical summary.

## Serving and maintenance qpair ownership

A node has one maintenance pool for all of its serving shards. The pool has exactly
one flush worker and `N = VALS_MAINT_COMPACT_LANES` compaction workers (default 2,
capped at 4). Jobs carry a stable shard-owner number; the flush worker and the
compaction workers each take jobs from one queue that schedules queued owners
round-robin, and every completion plus wakeup goes only to that owner's shard. An
engine never has more than one merge in flight, so compaction lanes run merges of
different engines in parallel. A failed or stopped owner drops only its own
completion. Node shutdown first stops and joins all serving shards, which closes every
submission route, and then drains and joins every maintenance worker.

Serving qpairs remain dedicated to one shard/core. On each maintenance worker,
`BlockDevice::maintenance_sibling` caches one SPDK qpair per controller in
thread-local state. Zone and shard handles are range-restricted views over that qpair,
and the cluster worker cache is keyed by `(owner, zone)`, so sharing the controller
qpair does not merge zone bounds or engine state. The qpair's `Arc` controls lifetime;
it does not grant concurrent access. The SPDK wrapper records the creating worker
thread and aborts any I/O attempted from a different thread. There is no cross-thread
qpair handoff.

The steady one-controller budget is therefore `S + 1 + N`: `S` dedicated serving
qpairs plus one flush qpair and one qpair per compaction lane. Eight serving shards
with the default two compaction lanes use 11 live qpairs on the Samsung 9100 PRO,
below its observed limit of 15 (the lane cap of 4 keeps eight shards at 13). Sibling allocation failure is
returned only to the affected job and is retried by a later plan; it does not poison
another owner's completion route. Zone range checks, normal engine recovery, RAM
admission, and serving-device error handling are unchanged.

Allocation and release logs include `role=serving|maintenance`, controller id, live
count, and (for maintenance) owner thread. Successful off-core flush and run-merge
commit events include the stable maintenance owner and zone id. `SpdkController::qpair_counts()` exposes
live/serving/maintenance counts to in-process gates. For an eight-shard gate require
8 serving, no more than `1 + N` maintenance, no qpair-allocation errors, and observed flush
and merge/compaction progress on every shard.

The 2026-08-17 mature RF=2 Samsung 9100 PRO gate met those requirements on every
node: exactly 8 serving + 2 maintenance qpairs, every owner flushed and merged, clean
device/maintenance/client correctness, and a 17.6% median read-throughput gain over
four shards. The companion run-free gate also made every owner incrementally compact
at 20.0–20.4% reclaimable space under continuous reads, freeing
14,848–15,872 segments/node with zero flushes, merges, runs, or gated errors; because
that legacy path is serving-thread-owned it used 8+0 qpairs. Eight shards/node is
therefore the maximum-read-throughput guidance for
that controller with `profile=latency`; use four when CPU/RAM efficiency matters
more (eight used about twice both). Treat `S + 1 + N <= controller limit` as a deployment
precondition on other controllers rather than assuming the Samsung limit. Re-run the
destructive gate after controller, firmware, SPDK, profile, or maintenance-lane
changes, and monitor qpair allocations, every-owner progress, RAM headroom, and error
counters. Evidence and accounting limitations:
[`bench/lab-maint-qpair-gate-20260817-0208/FINDINGS.md`](../bench/lab-maint-qpair-gate-20260817-0208/FINDINGS.md).

## Building SPDK (no NVMe required to build)

**Pinned known-good tag: `v24.09`.** The shim + engine are validated against SPDK
**v24.09** (verified on the `oldbox` box, 2026-06-18). Newer SPDK may work but
is unpinned; v24.09 is what the shim's ABI assumptions are tested against — in
particular the `opts.opts_size` fix in `csrc/spdk_shim.c` (harmless on older SPDK)
that v24.09's ABI-versioned `spdk_env_opts` requires. Prefer this tag until a newer
one is re-validated on hardware.

**Build prerequisites `scripts/pkgdep.sh` does NOT install** (install these first):

- **meson** and **ninja** — the vendored DPDK (`third_party/spdk/dpdk`) builds with
  Meson/Ninja; `pkgdep.sh` does not pull them on all distros.
- **python3 pyelftools** — required by the DPDK build.
- **python3 jinja2** and **tabulate** — SPDK's `genrpc.py` codegen.
- **nasm** (2.14+) is *optional*: without it, `configure` auto-disables ISA-L/crypto.

```sh
# Distro packages (Debian/Ubuntu example) — plus SPDK's own scripts/pkgdep.sh:
sudo apt-get install -y meson ninja-build nasm
# Python deps not covered by pkgdep.sh:
python3 -m pip install --user --break-system-packages pyelftools jinja2 tabulate

# Fetch the pinned tag from scratch (into the gitignored third_party/spdk):
git clone --branch v24.09 --depth 1 https://github.com/spdk/spdk third_party/spdk
cd third_party/spdk
git submodule update --init          # dpdk (+ others) — required to build

# Configure the minimal build the shim needs, then build:
./configure --without-crypto --without-vhost --without-virtio \
            --disable-tests --disable-unit-tests --disable-examples --disable-apps
make -j"$(nproc)"
```

(If you already have a clone, `git -C third_party/spdk fetch --tags && git -C
third_party/spdk checkout v24.09 && git -C third_party/spdk submodule update --init`
gets you to the pinned tag.)

## Running on a box with a dedicated scratch NVMe

> ⚠️ Never bind the boot disk. Use a spare NVMe with no mounted partitions.

The NVMe path is implemented in `crates/vals-spdk/src/spdk.rs` over a hand-written
C shim (`crates/vals-spdk/csrc/spdk_shim.c`) that wraps the SPDK NVMe driver
(probe → namespace → per-core qpair → `spdk_nvme_ns_cmd_read/write` + poll, plus
the async `vals_spdk_aio_read` / `vals_spdk_poll`). `build.rs` compiles the shim
and links SPDK when `VALS_SPDK_LINK=1`. To run on a fresh box:

1. **TRIM the scratch device first.** A wiped-but-untrimmed SSD GC-thrashes (QD1
   randread can drop to ~480 IOPS). Always `blkdiscard /dev/nvmeXn1` before
   benchmarking.
2. Reserve hugepages and bind the scratch device to the userspace driver (per
   boot):
   ```sh
   sudo PCI_ALLOWED=<pci-addr> HUGEMEM=4096 third_party/spdk/scripts/setup.sh
   # lists NVMe devices + PCIe addresses; note the scratch one
   ```
   On an IOMMU box, **run the SPDK process as root** — the simplest path (non-root
   vfio fails "Cannot use IOVA as PA"). If you do want non-root: a one-time
   `/etc/security/limits.d/spdk.conf` (memlock unlimited for the user) and
   `chown <user> /dev/vfio/<iommu-group>`.
3. Build/run with linking enabled, pointing at the DPDK libs:
   ```sh
   VALS_SPDK_LINK=1 cargo build --features spdk --release
   LD_LIBRARY_PATH=third_party/spdk/dpdk/build/lib cargo run --features spdk ...
   ```
4. Validate: correctness (round-trip, crash recovery) and IOPS/latency vs an
   `fio` raw-device baseline on the same NVMe.

For **multi-drive** (JBOD) or a **multi-node cluster on one box**, bind several
drives at once and reserve more hugepages, e.g.
`PCI_ALLOWED="<a> <b> ...> HUGEMEM=32768 third_party/spdk/scripts/setup.sh`. Run
`bench mdwrite`/`mdread` (comma-separated PCIe list) for JBOD, or `bench distspdk`
/`distspdkmc` for the distributed path (≥6 PCIe addrs = 3 nodes × 2 drives).

## Notes

- `build.rs` emits SPDK link flags only when `VALS_SPDK_LINK` is set. Without
  SPDK libraries, use `cargo check --workspace --features spdk` to type-check
  every feature path or `cargo build -p vals-spdk --features spdk` to build the
  Rust library; binaries containing live NVMe calls require `VALS_SPDK_LINK=1`.
- The synchronous `BlockDevice` trait is QD1 per call; the `read_batch` /
  `read_at_async` / `write_at_async` paths and the value log's pipelined writes
  saturate the device at high queue depth (see Status).
- A SIGKILL'd SPDK process can leave a controller claimed and stale
  `/var/run/dpdk/*` runtime dirs, so the next run fails to attach. Let processes
  exit cleanly; if attach fails, `pkill` any stragglers and `rm -rf
  /var/run/dpdk/*`.
- Hugepage files: the shim always sets `unlink_hugepage` (DPDK
  `--huge-unlink`), so each `/dev/hugepages/spdk<shm_id>map_*` file is unlinked
  right after creation and the kernel frees the pages when the process exits on
  any path (clean exit, SIGTERM, `_exit`, kill -9). Before 0424 a nonnegative
  `spdk_shm_id` (valsd's default is the node id) kept every map file after exit,
  even a clean one; `rte_eal_cleanup` does not unlink them. Binaries built before
  0424 still need the guarded `rm -f /dev/hugepages/spdk*map_*` after the process
  is gone. Secondary DPDK processes cannot attach to valsd's memory; none do.
- Crash/teardown caveat: the distributed bench's detached server threads can
  segfault during process exit (busy-poll threads racing SPDK teardown) — benign
  (results print first), tracked in the ycc backlog (`../backlog.md`).

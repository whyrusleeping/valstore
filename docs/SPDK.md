# The SPDK backend

The production block backend is userspace poll-mode NVMe via SPDK
(`vals_spdk::spdk::SpdkBlockDevice`, behind the `spdk` cargo feature). SPDK is
built from source under `third_party/spdk`, which is gitignored. The in-memory
and file backends cover all engine development and testing; SPDK is only needed
for real raw-NVMe throughput.

This guide covers building SPDK, running on a machine with dedicated drives,
how the backend uses NVMe queue pairs, and what has been measured.

## What the backend does

`crates/vals-spdk/src/spdk.rs` wraps a small C shim
(`crates/vals-spdk/csrc/spdk_shim.c`) around the SPDK NVMe driver: attach a
controller and namespace, one I/O qpair per core, synchronous `read`/`write`,
batched `read_batch`, asynchronous `read_at_async`/`write_at_async`
(non-blocking submit plus a completion-to-waker bridge), and `poll`. `build.rs`
compiles the shim and links SPDK only when `VALS_SPDK_LINK=1`.

The things that matter for performance:

- The backend presents a **4 KiB logical block** over 512-byte namespaces,
  coalesces multi-block writes, pipelines asynchronous writes (drained at
  `sync`), uses a DMA buffer pool, and computes CRC-32C in hardware (SSE4.2,
  with a scalar fallback).
- **One controller attach, one qpair per shard.** `SpdkController::attach`
  runs once per process (vfio attaches a controller once) and `open_shard`
  gives each serving core its own qpair over a disjoint LBA region. Thread
  affinity is reset after `spdk_env_init`, because DPDK pins the calling
  thread to one core, which would otherwise confine every shard to it.
- **Several drives per node (JBOD):** attach K controllers and spread
  partitions across them; one engine per drive.

## Serving and maintenance qpairs

A node has one maintenance pool shared by all of its serving shards. The pool
has exactly one flush worker and `N = maint_compact_lanes` compaction workers
(default 2, at most 4; see [`configuration.md`](configuration.md)). Jobs carry
a stable shard-owner number. The flush worker and the compaction workers each
take jobs from one queue that schedules queued owners round-robin, and every
completion and wakeup goes only to that owner's shard. An engine never has more
than one merge in flight, so the compaction lanes run merges of different
engines in parallel. A failed or stopped owner drops only its own completion.
Node shutdown first stops and joins all serving shards, which closes every
submission route, and then drains and joins every maintenance worker.

Serving qpairs are dedicated to one shard and core. On each maintenance worker,
`BlockDevice::maintenance_sibling` caches one SPDK qpair per controller in
thread-local state. Zone and shard handles are range-restricted views over that
qpair, and the worker cache is keyed by `(owner, zone)`, so sharing the
controller qpair does not merge zone bounds or engine state. The qpair's `Arc`
controls lifetime; it does not grant concurrent access. The wrapper records the
creating thread and aborts any I/O attempted from a different thread. There is
no cross-thread qpair handoff.

The steady per-controller budget is therefore `S + 1 + N`: `S` serving qpairs
plus one flush qpair and one per compaction lane. Eight serving shards with the
default two compaction lanes use 11 qpairs on a Samsung 9100 PRO, below its
observed limit of 15 (with the maximum four lanes, eight shards use 13). A
sibling allocation failure is returned only to the affected job and retried by
a later plan; it does not poison another owner's completion route.

Allocation and release logs include `role=serving|maintenance`, the controller
id, the live count and, for maintenance, the owner thread. Off-core flush and
run-merge commit events name the maintenance owner and zone id.

Guidance from the eight-shard runs on that controller: eight shards per node
gives the highest read throughput with `profile = latency` (about 18% more
than four shards at the median), and uses about twice the CPU and RAM of four;
use four when efficiency matters more. On any other controller, treat
`S + 1 + N <= controller qpair limit` as a deployment precondition rather than
assuming the Samsung limit, and re-check after controller, firmware, SPDK,
profile or lane changes while watching qpair allocations, per-owner flush and
merge progress, RAM headroom and error counters.

## Building SPDK (no NVMe required to build)

**Pinned known-good tag: `v24.09`.** The shim and engine are validated against
SPDK v24.09. Newer SPDK may work but is unpinned; v24.09 is what the shim's ABI
assumptions are tested against, in particular the `opts.opts_size` field in
`csrc/spdk_shim.c` (harmless on older SPDK) that v24.09's ABI-versioned
`spdk_env_opts` requires. Prefer this tag until a newer one is validated on
hardware.

Build prerequisites that SPDK's `scripts/pkgdep.sh` does **not** install
(install these first):

- **meson** and **ninja**: the vendored DPDK (`third_party/spdk/dpdk`) builds
  with Meson/Ninja, and `pkgdep.sh` does not pull them on all distros.
- **python3 pyelftools**: required by the DPDK build.
- **python3 jinja2** and **tabulate**: SPDK's `genrpc.py` code generation.
- **nasm** (2.14+) is optional: without it, `configure` disables ISA-L/crypto.

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

If you already have a clone, `git -C third_party/spdk fetch --tags && git -C
third_party/spdk checkout v24.09 && git -C third_party/spdk submodule update --init`
gets you to the pinned tag. `scripts/build-spdk.sh` automates these steps and
the vals build.

## Running on a machine with a dedicated scratch NVMe

> ⚠️ Never bind the boot disk. Use a spare NVMe with no mounted partitions.

1. **TRIM the scratch device first.** A wiped but untrimmed SSD thrashes its
   garbage collector (QD1 random reads can drop to ~480 IOPS). Run
   `blkdiscard /dev/nvmeXn1` before measuring anything.
2. Reserve hugepages and bind the scratch device to the userspace driver (once
   per boot):
   ```sh
   sudo PCI_ALLOWED=<pci-addr> HUGEMEM=4096 third_party/spdk/scripts/setup.sh
   # lists NVMe devices + PCIe addresses; note the scratch one
   ```
   On a machine with an IOMMU, **run the SPDK process as root**; that is the
   simplest path (non-root vfio fails with "Cannot use IOVA as PA"). For
   non-root, add a one-time `/etc/security/limits.d/spdk.conf` (memlock
   unlimited for the user) and `chown <user> /dev/vfio/<iommu-group>`.
3. Build and run with linking enabled, pointing at the DPDK libraries:
   ```sh
   VALS_SPDK_LINK=1 cargo build --features spdk --release
   LD_LIBRARY_PATH=third_party/spdk/dpdk/build/lib cargo run --features spdk ...
   ```
4. Validate: correctness (round-trip, crash recovery) and IOPS/latency against
   an `fio` raw-device baseline on the same NVMe. Benchmark reads must hit
   populated LBAs: reading unmapped or TRIMmed blocks returns zeros without a
   NAND access and inflates IOPS.

For **several drives** (JBOD) or a **multi-node cluster on one machine**, bind
several drives at once and reserve more hugepages, for example
`PCI_ALLOWED="<a> <b> ..." HUGEMEM=32768 third_party/spdk/scripts/setup.sh`.
`vals-bench mdwrite`/`mdread` (comma-separated PCIe list) drive a JBOD, and
`vals-bench distspdk`/`distspdkmc` drive the distributed path (at least six
PCIe addresses: 3 nodes × 2 drives).

Provisioning a node's drives for `valsd` (`valsd provision`) and the SPDK keys
of the node config are in [`deploy.md`](deploy.md#offline-spdk-provisioning).

## Measured throughput

These are results from specific benchmark runs, not guarantees. This section
gives the setup behind each headline number in the [README](../README.md#performance);
the history of how they were reached is in the Status section of
[`ARCHITECTURE.md`](../ARCHITECTURE.md#status). For the default file backend
on ordinary machines, see [`benchmarks.md`](benchmarks.md). Unless stated
otherwise, the machine is one 256-core EPYC server with 8× Samsung PM9A3
(3.84 TB) datacenter NVMe, and the driver is `vals-bench` with its default
100-byte values.

**Single-node engine on the drives directly** (no network, no replication):

- **One drive, one core, at the device's raw ceilings.** Random reads scale
  with queue depth to about **1.0M IOPS / 4 GiB/s** (QD128 and above); QD1 is
  about 56 µs, which is NAND latency. Writes reach about **3.7 GiB/s** (the
  device's write-bandwidth ceiling) for values of 8 KiB and larger; 4 KiB
  values reach **858k ops/s / 3.2 GiB/s**, and 100-byte values **over 4M
  ops/s**. (Before the 4 KiB logical block, write coalescing, pipelined
  asynchronous writes and hardware CRC described above, 4 KiB writes ran at
  16.7k ops/s.)
- **One drive, many cores.** Engine reads scale from 360k IOPS on one core to
  about 1.0M IOPS on 16 cores, the device ceiling.
- **Eight drives as a JBOD** (`vals-bench mdwrite`/`mdread`, one engine per
  drive): near-linear, about **8.0M read IOPS and 29 GiB/s of writes**, roughly
  8× the single-drive ceilings. These are the README's "~8M read IOPS,
  ~29 GiB/s" figures.

**Replicated cluster on one machine** (`vals-bench distmdmc`): the 8 drives as
**4 node processes × 2 drives × 16 partitions = 64 serving cores**, one engine
per partition, **RF 3**, writes with `W = 2` (quorum), strong reads with
`R = 2` (digest fan-out, the default), get-any reads with `R = 1`, over
**loopback TCP** with the coordinators in the same process. The servers are the
passive in-process cluster, so the **anti-entropy sweep is off** and no
deployment profile applies. Every run is zero-error and every read value is
verified.

- Latest run (**n = 30M** keys per phase, 100-byte values): **3.51M replicated
  writes/s**, **4.45M strong reads/s**, **6.46M get-any reads/s**; a shorter
  get-any run peaked at 6.58M/s, which is the raw 8-drive read-IOPS ceiling.
  These are the README's "~3.5–4M writes, ~4.4M strong reads, ~6.5M `R = 1`
  reads" figures.
- Earlier runs of the same setup, before the last transport and read-path
  changes: about 4.0M writes/s (large-value writes at ~21 GiB/s, 70% of the
  29.9 GiB/s raw JBOD ceiling), strong 100-byte reads of 3.85M/s with 48
  coordinator cores (1.56M with 16) under digest fan-out against 3.08M / 1.48M
  under the older quorum fan-out, and get-any reads of about 3.8M/s.
- `vals-bench distspdk` (3 nodes × 2 drives, RF 3, loopback) is the
  correctness run on real drives: quorum writes and reads, replication,
  availability with a node down and hinted handoff, zero mismatches.

**Three machines on a real network** (`valsd` nodes driven by `vals-bench
netread` from external client hosts): three 24-core servers, **one Samsung
9100 PRO per node** split into **four shards per node** (12 serving shards in
all), all links **10 GbE**, **RF 2, R 2**, and the node config set to
**`profile = latency`** (not the default `capacity`). Repeated strong reads of
100-byte values over a mature, previously written data set at batch width 256
with 12 coordinators × 2 batches in flight sustained **4.96M strong reads/s**
(median; 4.07M/s before batched winner probing), with zero errors, misses or
mismatches, one checked value read per key and no run probes. This is the
README's "~5M strong 100-byte reads/s". A profile of that run found all twelve
serving shards CPU-saturated, with mixed-batch result construction the largest
remaining cost; the client fan-out and the network link were ruled out as the
limit. Several read-path variants (per-value allocation changes, batched SPDK
value reads, a more compact mixed-read encoding) were measured against this
baseline and did not help, so they were not kept.

## Notes

- `build.rs` emits SPDK link flags only when `VALS_SPDK_LINK` is set. Without
  SPDK libraries, `cargo check --workspace --features spdk` type-checks every
  feature path and `cargo build -p vals-spdk --features spdk` builds the Rust
  library; binaries that make live NVMe calls need `VALS_SPDK_LINK=1`.
- The synchronous `BlockDevice` calls are QD1 per call; the `read_batch`,
  `read_at_async` and `write_at_async` paths and the value log's pipelined
  writes are what saturate the device at high queue depth.
- **After a crash, OOM kill or `kill -9`, a restart needs no cleanup step.**
  Everything that ties a drive to a process is released by the kernel when the
  process dies, however it dies: the vfio device file descriptors (SPDK resets
  the controller on the next attach), `valsd`'s own per-drive
  `flock` on `/run/lock/valsd-spdk-<hex pci>.lock`, SPDK's `fcntl` lock on
  `/var/tmp/spdk_pci_lock_<pci>`, and DPDK's lock on
  `/var/run/dpdk/spdk<shm_id>/config`. The shim also sets `unlink_hugepage`
  (DPDK `--huge-unlink`), so each `/dev/hugepages/spdk<shm_id>map_*` file is
  unlinked as soon as it is created and the kernel frees the pages on any exit
  path (clean exit, SIGTERM, `_exit`, `kill -9`); this means no secondary DPDK
  process can attach to valsd's memory, and none needs to. The files left
  under `/var/run/dpdk/spdk<shm_id>/` and `/var/tmp/spdk_pci_lock_*` are
  inert; the next process re-locks and rewrites them. Hardware checks: eight
  systemd restarts of `valsd` after SIGSEGV, SIGABRT and exit 75 attached
  without cleanup and left every hugepage free, and `kill -9` of the shim with
  the SPDK environment initialised left no map files; a `kill -9` of `valsd`
  with an attached controller has not been run as a separate test. If an
  attach does fail with "exclusive ownership of SPDK device ... is
  unavailable" or SPDK's "Cannot create lock on device", the previous process
  is still alive: find it with `pgrep -x valsd` (or `vals-bench`) and stop it.
  Deleting `/var/run/dpdk/*` is never required.
- Known issue: the distributed benchmark's detached server threads can
  segfault during process exit (busy-poll threads racing SPDK teardown). The
  results are printed before that happens. It is not fixed yet.

# File-backed benchmarks on ordinary hardware

The [headline numbers](../README.md#performance) come from the SPDK
backend on datacenter or dedicated NVMe drives. This page measures what most
evaluations start with: `valsd` with the default file backend and default
node settings, on ordinary machines. Its results are the
[measured numbers](#results) and a
[recipe for repeating them on your own machines](#run-it-yourself). They come
from specific runs and are not guarantees. Repeated runs on the same hardware
varied by up to about 20%. The page compares the setups it measured with each
other and with no other system.

## What was measured

**Cluster.** Three file-backed data nodes and one PD, laid out as in the
[README quick start](../README.md#run-it-a-local-three-node-cluster). Each
node config adds `region_mib = 65536` (a sparse 64 GiB zone file) and a
`metrics` address. Everything else is the default:
`profile = capacity`, `sweep = 5s` (anti-entropy on), `gc_grace = 0s`, and
the default flush, compaction and admission settings. Every key is in the
default zone, RF 3. The file backend is unsharded, so each node serves its
requests from a single thread.

**Client.** `vals-bench vals netprogress` with 8 coordinator threads × 32
requests in flight (256 in all). It uses majority writes (`W = 2`) and
majority reads (`R = 2`), the default failure detector, and the default
client config. A request refused with `Busy` or `Overloaded` is retried
after 1 ms, doubling up to 32 ms. Its latency includes the retries, and the
tables count them. The client checks every value it reads against what it
wrote. After each phase it reads back every key that the phase wrote.

**Dataset.** 4,000,000 keys with 1 KiB values, about 4 GiB per replica. The
same client loads it in key order (the `load` row), and the write phases
then age it. By the read phases it had taken 11.3–11.6M acknowledged
writes, the load included, and the data
directories had grown from about 4 GiB per replica to about 13 GiB, because
overwrites leave garbage until compaction reclaims it. The read phases ran
9 to 13 minutes after the load.

**Phases.** Each phase runs for 60 s, first saturated and then moderate:

- **saturated**: a closed loop with all 256 requests in flight;
- **moderate**: an open loop at half the saturated rate, with request start
  times staggered across the slots.

Before each phase the script waits until every `valsd` process uses less than
0.05 of a core. This keeps a phase from measuring the flushes, merges and
anti-entropy left over from the phase before. The waits took 5–144 s.

| Phase | Requests | Persistence |
|---|---|---|
| `write-buffered` | 100% point writes, overwriting random existing keys | buffered (the default) |
| `write-device` | 100% point writes | device-durable: acknowledged after the replica's device sync |
| `read-warm` | 100% point reads | — |
| `read-cold` | 100% point reads, starting just after the page cache was dropped (`echo 3 > /proc/sys/vm/drop_caches`) on every node host | — |
| `mixed-device` | 25% writes, 65% point reads, 10% 8-key batch reads | device-durable writes: the [recommended policy](getting-started.md#reading-and-writing) of device-durable majority writes with majority reads |
| `mixed-buffered` | the same mix | buffered writes |

**Revision.** `valsd` was built from commit `2f21514`. The commit that adds
this page does not change the daemon. The client is `vals-bench` from the
commit that adds this page.

## Hardware

| | Single host | Three hosts |
|---|---|---|
| Data nodes | all three nodes and the PD on one host | one node per host, the PD on the first |
| Client | on the same host, over loopback | a separate machine on the same 10 GbE switch |
| Node host | Intel Core Ultra 9 285HX (24 cores), 61 GiB RAM, Samsung 990 PRO 4 TB, ext4 on LVM, Linux 7.0 | three such hosts; the third has a 990 PRO 2 TB |
| Network | loopback | 10 GbE, about 0.17 ms ping |

The 990 PRO is a consumer drive without power-loss protection. Each lab host
also ran an unrelated, idle vals deployment, which used under 0.1 core and
kept 8 GiB of hugepages reserved. The three-host client was a large shared
development machine (AMD EPYC 9684X). It used 0.5–7 cores and under 45 MB
of RSS.

## Results

Every run in both tables finished with zero errors, zero bad values, and a
clean read-back. The `valsd CPU` column is the total for all node processes
in the setup. The three-host table also gives a per-host range for CPU and
disk traffic.

### Single host (client on the same machine)

| Phase | Load | ops/s | p50 / p95 / p99 (ms) | Busy retries | valsd CPU (cores) | Disk read / write (MB/s) |
|---|---|---:|---|---:|---:|---|
| load (new keys) | saturated | 414,079 | 0.42 / 1.18 / 2.21 | 0 | 3.0 | 0 / 1340 |
| write-buffered | saturated | 77,929 | 3.01 / 4.62 / 8.06 | 0 | 3.1 | 0 / 281 |
| write-buffered | moderate | 38,899 | 0.43 / 1.90 / 6.23 | 0 | 2.1 | 0 / 130 |
| write-device | saturated | 2,876 | 60.0 / 249 / 413 | 224,975 | 0.2 | 0 / 16 |
| write-device | moderate | 1,433 | 38.6 / 54.3 / 61.8 | 105 | 0.1 | 0 / 11 |
| write-device, 64 in flight | saturated | 1,437 | 42.1 / 55.9 / 66.3 | 0 | 0.1 | 0 / 12 |
| read-warm | saturated | 103,922 | 2.45 / 4.28 / 5.17 | 0 | 2.7 | 0 / 0 |
| read-warm | moderate | 51,926 | 0.32 / 1.28 / 1.77 | 0 | 1.9 | 0 / 0 |
| read-cold | saturated | 48,143 | 5.01 / 9.51 / 12.2 | 0 | 1.9 | 156 / 0 |
| read-cold | moderate | 23,875 | 2.32 / 7.23 / 10.4 | 0 | 1.2 | 103 / 0 |
| mixed-device | saturated | 8,698 | write 45.9 / 59.2 / 67.9<br>read 22.8 / 35.1 / 41.0 | 1,332 | 0.7 | 21 / 12 |
| mixed-device | moderate | 4,338 | write 41.8 / 56.1 / 65.0<br>read 29.0 / 48.3 / 52.3 | 1 | 0.4 | 0 / 10 |
| mixed-buffered | saturated | 37,867 | write 3.23 / 6.92 / 8.50<br>read 7.78 / 13.9 / 16.6 | 0 | 2.3 | 76 / 35 |
| mixed-buffered | moderate | 18,447 | write 0.74 / 2.18 / 3.54<br>read 1.08 / 3.38 / 8.49 | 0 | 1.4 | 0 / 18 |

The three node processes held about 2.2 GiB of RSS in total. The client held
under 40 MB of RSS and used up to 2.2 cores. The host's 24 cores were never
more than 22% busy. The median batch read (8 keys) took 1.1–1.3× as long as
the median point read in the same phase.

### Three hosts (client on a separate machine)

| Phase | Load | ops/s | p50 / p95 / p99 (ms) | Busy retries | valsd CPU per host (cores) | Disk read / write per host (MB/s) |
|---|---|---:|---|---:|---:|---|
| load (new keys) | saturated | 386,847 | 0.52 / 1.17 / 1.81 | 0 | 0.9–1.0 | 0 / 371–414 |
| write-buffered | saturated | 79,007 | 3.06 / 4.71 / 6.99 | 0 | 1.0 | 0 / 93–98 |
| write-buffered | moderate | 39,422 | 0.23 / 0.40 / 1.67 | 0 | 0.6–0.7 | 0 / 43–47 |
| write-device | saturated | 5,073 | 33.3 / 136 / 244 | 340,676 | 0.1 | 0 / 11–13 |
| write-device | moderate | 2,526 | 20.9 / 44.5 / 56.0 | 29,828 | 0.1 | 0 / 7–8 |
| write-device, 64 in flight | saturated | 2,738 | 23.0 / 29.8 / 33.4 | 0 | 0.1 | 0 / 8–9 |
| read-warm | saturated | 124,673 | 2.03 / 3.57 / 4.41 | 0 | 0.7–1.0 | 0 / 0–1 |
| read-warm | moderate | 62,176 | 0.25 / 0.39 / 0.47 | 0 | 0.6–0.7 | 0 / 0–1 |
| read-cold | saturated | 46,441 | 5.19 / 10.1 / 12.8 | 0 | 0.5–0.6 | 48–53 / 0 |
| read-cold | moderate | 23,009 | 0.39 / 0.94 / 2.50 | 0 | 0.3–0.4 | 31–35 / 0–1 |
| mixed-device | saturated | 13,562 | write 29.0 / 36.1 / 41.2<br>read 14.9 / 20.6 / 23.3 | 1,960 | 0.3 | 10–12 / 7 |
| mixed-device | moderate | 6,768 | write 21.4 / 28.4 / 31.4<br>read 16.6 / 25.3 / 29.9 | 0 | 0.1–0.2 | 0 / 6–7 |
| mixed-buffered | saturated | 42,219 | write 3.03 / 5.78 / 7.28<br>read 6.38 / 13.5 / 17.9 | 0 | 0.7–0.8 | 24–28 / 11–12 |
| mixed-buffered | moderate | 21,079 | write 0.34 / 0.70 / 0.88<br>read 0.39 / 0.82 / 1.83 | 0 | 0.3–0.5 | 0 / 4–6 |

Each node process held 0.6–0.7 GiB of RSS, and each node host's CPUs were
under 6% busy.

### Device-durable phases after the off-thread sync (single host)

The tables above were measured before task 0523. Since then, the file backend
runs a device sync (`fdatasync`) on a helper thread, and the node keeps
serving while it runs (see [below](#what-the-numbers-show)). These runs used
the same host and recipe on 2026-10-29. Each arm loaded a fresh 4M-key dataset
(1M keys for 64 in flight) and ran only `write-device` and then
`mixed-device`. The "before" rows are the previous `valsd`, measured in the
same session.

| Phase | In flight | Load | Before: ops/s | Before: p50 / p95 / p99 (ms) | After: ops/s | After: p50 / p95 / p99 (ms) |
|---|---:|---|---:|---|---:|---|
| write-device | 256 | saturated | 2,604 | 65.9 / 283 / 435 | 11,085 | 24.4 / 27.3 / 28.0 |
| write-device | 256 | moderate | 1,296 | 35.1 / 52.7 / 58.2 | 5,534 | 18.5 / 26.0 / 27.3 |
| mixed-device | 256 | saturated | 10,681 | write 38.3 / 50.7 / 63.5<br>read 19.1 / 27.4 / 28.5 | 40,351 | write 21.9 / 27.2 / 28.6<br>read 1.11 / 2.62 / 3.78 |
| mixed-device | 256 | moderate | 5,328 | write 40.2 / 52.6 / 55.9<br>read 25.9 / 40.3 / 49.6 | 18,899 | write 20.3 / 27.1 / 28.3<br>read 0.81 / 2.27 / 3.02 |
| write-device | 64 | saturated | 1,497 | 40.9 / 57.7 / 67.8 | 2,684 | 25.3 / 28.3 / 29.0 |
| mixed-device | 64 | saturated | 1,986 | write 40.6 / 55.6 / 57.9<br>read 28.8 / 43.4 / 51.6 | 10,208 | write 23.5 / 28.4 / 29.5<br>read 0.66 / 1.99 / 2.57 |

Before the change, saturated runs at 256 in flight took 202k (`write-device`)
and 3.7k (`mixed-device`) `Busy` retries. After it, no run took any. Every
run finished with zero errors and a clean read-back. The moderate
`mixed-device` run after the change reached 18,899 of its 20,175 ops/s target.
At saturation the three nodes used 0.5 cores in all for `write-device` and
1.8 for `mixed-device`.

## What the numbers show

- **One serving thread per node sets the ceiling.** At saturation each
  file-backed node used about one core, which is its single serving thread,
  while the rest of the machine sat idle. Two things raise the ceiling: more
  nodes (more threads), or the sharded SPDK backend.
- **Overwrites cost far more than inserts.** Loading 4M new keys ran at about
  400k writes/s. Overwriting random existing keys ran at about 78k/s. To keep
  last-writer-wins order, an overwrite of a key that is already on disk may
  first have to read the stored version from the run that holds it.
- **Device-durable writes are bound by device syncs.** On the file backend a
  device sync is a sync of the zone file. With three nodes sharing one drive,
  each sync took 2.5–25 ms, about 11 ms on average, and each node completed
  about 80–100 syncs per second. Group commit lets concurrent writes share one
  sync, so throughput depends on how many writes each sync carries.
  - In the main tables, the sync was an `fsync` on the node's single serving
    thread, so the node served nothing else while it ran. Reads on a node
    waited behind its syncs, which is why reads in `mixed-device` had medians
    of 15–29 ms. Throughput tracked in-flight writes divided by latency
    (Little's law): 64 writes in flight gave 1.4k/s on one host and 2.7k/s on
    three. With 256 in flight, the nodes refused the excess with `Busy`,
    because device-durable writes held a `batch_apply_gate` permit (64 per
    shard by default) until acknowledged. The client's retries roughly
    doubled throughput, and p99 latency rose to 0.2–0.4 s.
  - Since task 0523, the sync is an `fdatasync` on a helper thread, and the
    serving thread keeps serving reads and other writes while it runs. A
    write waiting for the sync gives its gate permit back, so all the writes
    that arrive during one sync join the next. On one host each sync carried
    about 94 writes per node instead of 24, device-durable writes ran 1.8–4.3×
    faster, and reads in `mixed-device` had medians of 0.7–1.1 ms
    ([table](#device-durable-phases-after-the-off-thread-sync-single-host)).
    Moving the sync off the thread without releasing the permit did not
    help: `write-device` at 256 in flight then fell to 2.0k/s, because the
    permits held by writes waiting for one sync left fewer for the next, and
    the client took 520k `Busy` retries.
  - For more device-durable writes per second, use drives with power-loss
    protection, which sync faster.
- **Independent disks help device durability most.** Moving from one host to
  three left buffered writes and warm reads within 25% of each other. With
  three hosts each node syncs its own drive, so device-durable writes and the
  mixed device workload ran 1.6–2× faster.
- **Warm and cold reads.** Warm reads were served from the page cache, with
  no device reads. Right after the cache was dropped, saturated reads
  averaged about 0.5 device reads per request and ran at 37–46% of the warm
  rate. The cache refills as a phase runs, so a cold phase is a
  restart-like worst case, not a steady state. In the three-host moderate
  run, the cache recovered quickly enough that its median was 0.39 ms.
- **Background work after write bursts.** After a sustained burst of
  overwrites, the nodes need up to a few minutes of flushing, merging and
  anti-entropy catch-up. During the catch-up, foreground reads stalled for
  about 1.2 s at every 5 s sweep. One read phase that started straight after
  two minutes of overwrites saw these stalls for about 15 s. A phase that
  started straight after the device-durable write phase saw them for the
  whole phase, at 12.3k reads/s with a p99 of 1.1 s. The tables above were
  measured after the nodes went idle. Plan for this if your load arrives in
  bursts.
- **Same host vs network.** On one host the client, the three nodes and their
  disk traffic all share one machine and one drive. The loopback network adds
  little, but the shared drive matters for every phase that syncs or reads
  from the device. The three-host numbers include a real 10 GbE hop.

## Run it yourself

The recipe is [`scripts/file-bench.py`](../scripts/file-bench.py). It writes
the configs, starts `valsd`, loads the dataset, runs the phases, and writes
one JSON result per phase. You need Python 3 and a release build:

```sh
cargo build --release -p vals-server -p vals-bench
```

**One machine.** This starts three nodes and a PD on 127.0.0.1:7001–7003, the
ports the README quick start also uses (use `--port` to move them), in a
directory you choose:

```sh
scripts/file-bench.py start --dir /var/tmp/vals-bench
scripts/file-bench.py run   --dir /var/tmp/vals-bench --out results/
scripts/file-bench.py report --out results/          # markdown table
scripts/file-bench.py stop  --dir /var/tmp/vals-bench --wipe
```

**Three machines.** Give ssh names and the private addresses the nodes should
listen on. The PD runs on the first host. The script copies `valsd` into
`--dir` on each host. The client runs where you run the script, so run it from
a fourth machine:

```sh
H="--hosts node-a,node-b,node-c --addrs 10.0.0.1,10.0.0.2,10.0.0.3 --dir /var/tmp/vals-bench"
scripts/file-bench.py start $H
scripts/file-bench.py run   $H --out results/
scripts/file-bench.py stop  $H --wipe
```

What to know before you run it:

- A full run takes 45–60 minutes. `--secs`, `--phases` and `--keys` shorten it.
  To run a phase again, or at another concurrency, pass the same `--out`.
  Later runs reuse the loaded dataset as long as `--threads` and `--slots` do
  not exceed the values it was loaded with, for example
  `--slots 8 --phases write-device`.
- `read-cold` drops each node host's page cache, which needs passwordless
  `sudo` there. Without it the script skips the phase. The cache drop affects
  everything else on those hosts.
- Size `--region-mib` and the disk together. Each node's zone file is
  sparse, but these runs grew to about 14–15 GiB per replica. Size it before
  `start`: a zone file keeps the size it was created with.
- Run on otherwise quiet machines. The script records the hardware, kernel,
  binary hashes and settings of each run in `environment-<run>.json`. Each
  phase's per-second samples go in `<run>-<phase>.log`, and every
  `results.jsonl` row names its run.
- Use the same client host for every run against one dataset. The client
  numbers its writes from its own clock, and the read checks rely on that.

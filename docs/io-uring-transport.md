# io_uring transport — plan, rationale, and progress

**Status: REJECTED at Phase 0 (2026-06-19).** Measurement on the real box shows the
serving-core "60% syscalls" is overwhelmingly **in-kernel TCP protocol body**, not
syscall-transition overhead — io_uring's realistic upside here is **~5–10%**, not
worth the rewrite. See **Phase 0 results** and **Decision** below. The plan/design
sections are retained for reference (and in case a future cross-box measurement
changes the calculus). **Branch:** `perf/spdk-multizone`.

This doc is the single reference for the io_uring networking effort. It is kept
updated as work progresses — see the **Progress log** at the bottom. If you are a
future session picking this up, read top-to-bottom, then jump to the latest
progress entry.

---

## Goal

Reduce the syscall overhead of network request handling on the serving hot path.
A prior valsmix profiling session measured **syscalls ≈ 60% of serving-core CPU**.
The hypothesis: moving the transport from an epoll *readiness* model to an
io_uring *completion* model removes most of that overhead and lifts clustered
read/write throughput.

## Why io_uring, not DPDK

- **DPDK** is kernel-bypass for the NIC: a poll-mode driver owns the card and
  DMAs raw Ethernet frames to userspace; you must bring your own userspace TCP
  stack (F-Stack/Seastar/etc.). It dedicates busy-poll cores that would contend
  directly with our SPDK poll cores (we already see SHARDS>4 thrash from SPDK
  poll contention). It also does nothing for loopback. Huge surface, wrong fit
  now.
- **io_uring** keeps the kernel TCP stack, works on any socket, and directly
  attacks the syscall-transition cost. It slots under our existing per-core
  `LocalExecutor` as a swap of the `poll_io` hook — a contained change, not an
  architecture rewrite.

**Critical caveat (the whole Phase 0 reason):** io_uring removes syscall
*transition* cost (enter/exit, the epoll readiness→re-syscall round trip, per-op
fd/buffer overhead). It removes **none** of the *in-kernel TCP protocol body*
(`tcp_sendmsg`/`tcp_recvmsg`, skb alloc, socket locking, the loopback scheduler
hand-off). `perf` attributes in-kernel body time to the syscall, so "60%
syscalls" is two different pools. Only the transition pool is io_uring-
addressable. Phase 0 measures the split before we commit.

## Current architecture (what we're changing)

- **Per-core thread-per-core model.** One `vals_rt::LocalExecutor` per shard
  thread (`crates/vals-rt/src/lib.rs`). The executor's `run(poll_io)` calls a
  single `poll_io` closure whenever the ready queue drains but tasks remain —
  this is the one park seam (`vals-rt/src/lib.rs:163`).
- **epoll readiness reactor** (`crates/vals-cluster/src/reactor.rs`):
  edge-triggered, persistent registration (one `epoll_ctl ADD` per fd lifetime,
  later parks just swap the waker — no syscall). `poll_io()` = `epoll_wait` +
  wake ready tasks. A `FLUSH_HOOK` runs pre-park to batch outbound writes (one
  `write()` per connection per turn).
- **Framed I/O** (`crates/vals-cluster/src/net/mod.rs`): `Sock` enum (Tcp|Unix)
  with non-blocking `read`/`write`. `FrameRead::read_block` (mod.rs:284) parses
  length-prefixed `[len][reqid][payload]` frames, calling `Sock::read` (recv)
  only when the buffer is short (`READ_CHUNK` = 32 KiB), so a batch of frames
  drains in one syscall. Send paths: server `ConnDriver::flush_out`, client
  `flush_dirty` — one `write()` per connection per turn via the flush hook.
- **The model is readiness**: `read_block` calls `Sock::read`, gets `WouldBlock`,
  `reactor::register(fd, waker)`, returns `Pending`; epoll later says "readable",
  the task re-runs and *syscalls again* to read. This re-syscall round trip and
  the per-op enter/exit are what io_uring collapses.
- **SPDK coexistence:** the server's unified `poll_io` (`net/server.rs:1013`,
  `server_poll`) busy-polls every materialized NVMe qpair when device reads are
  in flight (`read_gate.inflight() > 0`, a device wait/flush, or any thread-local
  `vals_spdk::async_io_pending() > 0`) and drains sockets *without parking*
  (`poll_io_nonblocking`, epoll timeout 0); when idle it parks (`poll_io`, 1 ms
  cap). Any io_uring backend MUST preserve this: never block the CQ wait while
  device reads are in flight, or SPDK polling starves.

## The change: readiness → completion

Flip the transport to submit the actual `RECV`/`SEND` as io_uring SQEs tagged
with a token; the reactor hands back byte counts + filled buffers on CQE. A
readiness-style `IORING_OP_POLL_ADD` drop-in would keep every read/write syscall
and buy ~nothing — explicitly NOT that.

- **Crate:** `io-uring` (low-level ring only), **not** `tokio-uring`/`monoio`/
  `glommio` (whole runtimes that would replace our `LocalExecutor`). One ring per
  shard thread, thread-local, mirroring today's per-core epoll instance.
- **Op-table reactor** alongside `reactor.rs`, behind a `features = ["io_uring"]`
  build + runtime flag so we A/B with one toggle (matches how every prior perf
  change was validated box A/B). Keeps a slab `user_data → OpSlot { waker, state:
  Pending | Done(result) | Orphaned }`. `poll_io` = flush queued SQEs +
  `io_uring_enter`, reap CQEs into slots, wake their wakers.
- **Recv hot path** (`FrameRead`): multishot `IORING_OP_RECV` + a registered
  provided-buffer ring (`IORING_REGISTER_PBUF_RING`). One submission yields many
  completions as frames arrive; the kernel picks a pool buffer and returns its
  id. Eliminates per-recv resubmission AND sidesteps the Rust buffer-lifetime
  hazard (kernel owns pool buffers; a cancelled read can't write freed memory).
  The frame parser is unchanged — it consumes pool buffers instead of `scratch`.
- **Send hot path** (`flush_out`/`flush_dirty`): `IORING_OP_SEND`; the op table
  owns the buffer until the CQE (resubmit the tail on a short send). The existing
  `FLUSH_HOOK` batching collapses into the submission ring (many sends, one
  enter).
- **SPDK coexistence:** mirror the `inflight() > 0` branch — when device reads
  pending, reap the CQ non-blocking (`min_complete=0`) and return; when idle,
  block but bound the wait with a 1 ms `IORING_OP_TIMEOUT` SQE (preserve the
  missed-wakeup safety of today's `POLL_IO_TIMEOUT_MS`). Per-RPC deadline timerfd
  folds in as a `POLL_ADD` or `LINK_TIMEOUT`.
- **SQPOLL: opt-in, default OFF.** Zero submission syscalls, but dedicates a
  kernel poll core that contends with SPDK busy-poll cores. Ship completion-mode
  first; try SQPOLL as a separate flagged experiment only if `io_uring_enter`
  still shows hot.

## Landmines

- **Buffer lifetime on future cancellation** — the op table must outlive dropped
  futures: orphan-and-reclaim on CQE, never free-on-drop. (Provided buffers make
  recv safe; sends need the op table to own the buffer.)
- **Backpressure changes shape** — today we stop *reading*; with completion we
  stop *submitting recvs*. The caps (`MAX_CONN_INFLIGHT_READS`,
  `MAX_CONN_SEND_BUF`) move but must stay enforced.
- **Kernel floor** — multishot recv + pbuf rings need ≥5.19 (box is 6.8.0 ✓).
  Runtime feature-probe with epoll fallback.
- **Partial sends** must resubmit the tail.
- **SPDK starvation** if the CQ wait ever blocks while device reads are in
  flight.

## Phases

| Phase | Work | Gate |
|---|---|---|
| **0** | Split the 60% (transition vs TCP body) + epoll-vs-uring echo bench → empirical ceiling, on the real box | **Kill if transition pool small / ceiling < ~1.3×** |
| 1 | `io-uring` dep, op-table reactor behind feature flag, epoll stays default | Compiles, A/B toggle works, tests green |
| 2 | Multishot recv + provided buffers; send op-table; preserve backpressure caps | A/B on box, 0 consistency errors, throughput win |
| 3 | Registered files/buffers; optional SQPOLL experiment | Measured separately |

**Expected payoff (if transition pool is fat):** ~1.6–2.2× serving-core
throughput, *provided the core (not loopback/NIC) is the wall*.

## Test box

> **2026-07-15: this box was retired** (box validation moved to the lab-1/2/3
> fleet); details kept for provenance of the numbers above.

`ssh user@oldbox` — 48-core AMD EPYC 9454P (96 threads), single socket /
single NUMA, **kernel 6.8.0-124-generic** (io_uring multishot + pbuf rings OK),
passwordless sudo, `perf` present (paranoid=4, use sudo), **no `strace`** (use
`perf trace -s`). The load generator and cluster scripts came from a private
examples repository: `spdk-cluster.sh` + `run-valsmix-spdk.sh` + `perf-measure.sh` +
`RUNBOOK-valsmix.md`.

**Loopback caveat:** we only have one box, so Phase 0 measures over loopback TCP.
Real-NIC cost structure differs (NIC softirq/driver replaces the loopback
scheduler hand-off); the transition pool is present in both and likely *larger*
on a real NIC. Re-measure cross-box when a second box is available.

---

## Progress log

### 2026-06-19 — Phase 0 kickoff
- Wrote this doc. Confirmed box kernel 6.8.0 clears the io_uring feature floor.
- Tasks created; building the standalone epoll-vs-io_uring echo ceiling bench and
  the perf-trace syscall split next.

### 2026-06-19 — Phase 0 results + DECISION: do not implement io_uring

**Bench artifact:** a standalone `uring-ceiling` crate (its own `[workspace]`,
kept with the maintainer's local benchmark trees, not in this repository).
Modes: `epoll` (ET readiness baseline), `uring` (single-shot Recv re-arm + Send),
`uring-ms` (multishot Recv + provided buffers). Server pinned to 1 core, client on
a disjoint core set. Box: `oldbox`, 48-core EPYC 9454P, kernel 6.8.

**(1) Echo ceiling bench — io_uring ≈ epoll when batched.**

| pipeline | epoll | uring | uring-ms |
|---|---|---|---|
| 64 (core-saturated) | **3.10M/s** | 2.94M/s | 2.99M/s |
| 4  (latency-bound)  | 0.343M/s | 0.344M/s | **0.362M/s** |
| 1  (latency-bound)  | 0.089M/s | 0.089M/s | **0.095M/s** |

At depth, epoll is marginally *ahead* — batching already drives recv/send to
~0.05 syscalls/op, leaving io_uring no headroom (its SQE/CQE bookkeeping costs as
much as it saves). io_uring's only edge (~5–7%, multishot only) is in the
latency-bound low-pipeline regime, which is *not* where a saturated serving core
lives.

**(2) Real valsmix serving-core profile (node1, sustained reads, 12s).**

`perf trace -s` (the `vals-shard` serving thread, time-in-syscall):
| syscall | calls | errors | total ms | note |
|---|---|---|---|---|
| `sendto`     | 257,252 | 0       | 2854.8 | dominant; batched reply bytes |
| `recvfrom`   | 461,184 | 359,129 | 1414.3 | **78% EAGAIN** (speculative ET drains) |
| `epoll_wait` | 32,944  | 0       | 145.2  | only 1.2% — no busy-poll storm |
| rest         | —       | —       | <1     | negligible |

`perf record` self-time / call graph:
- **~47% kernel, ~52% userspace** (DSO split).
- Of the kernel time, **35% of *all* samples are inside `tcp_sendmsg`** →
  `tcp_sendmsg_locked → tcp_push → tcp_write_xmit → __ip_queue_xmit → ip_output →
  __dev_queue_xmit → __local_bh_enable_ip` (the loopback transmit path), plus
  `clear_page_erms`/`__alloc_pages` under `skb_page_frag_refill`. This is the TCP
  **protocol body** — io_uring does not remove it.
- **Pure syscall-transition overhead (`entry_SYSCALL_64`/`do_syscall_64` self) ≈
  2–3%.**
- Userspace self-time leaders: `ConnDriver::dispatch_frame`→`HashIndex::get`
  5.81%, `VersionIndex::get` 4.85%, `malloc` 3.94%, `cfree`/`vals_aio_cb` ~1.1%,
  `execute_read_plan`.

**Interpretation.** The "60% syscalls" is real but it is ~95% *in-kernel TCP
processing* (the serving core pushing 256B reply values — batched by `read_many`
into larger sends — through the loopback stack) and only ~2–3% *transition*. Two
predictions going in were both wrong-direction: (a) I expected the SPDK busy-poll
`epoll_wait(0)` to be a syscall storm — it's 1.2%; (b) I expected the send path to
be the io_uring win — it's the recv path (EAGAIN elimination).

**Realistic io_uring upside here ≈ 5–10%**, almost entirely from multishot recv
removing the 359K/461K speculative EAGAIN `recvfrom` drains, plus the ~2–3%
transition sliver. The dominant `tcp_sendmsg` body is untouchable by the
completion model. (Note: most of the EAGAIN-elimination upside could also be had
with a small surgical change to the *existing* epoll reactor — split read/write
readiness so writable edges don't trigger speculative reads — without the rewrite.)

**DECISION: do not implement io_uring.** ~5–10% best case for the full op-table
reactor rewrite (buffer-lifetime-on-cancel + SPDK-coexistence landmines) is a poor
trade against cheaper/larger wins (the serving-core network-cost levers):
per-read allocation pooling (~8% userspace, trivial by comparison), skb page-frag
churn reduction, `SHARDS` multi-core, and — the only lever that attacks the TCP
body itself — kernel-bypass / userspace TCP stack (a separate large bet, validate
cross-box first).

**Loopback caveat stands.** All of this is single-box loopback. A real cross-box
NIC shifts the kernel mix (NIC driver/NAPI replaces the loopback softirq path) and
could change the transition fraction. The *conclusion* (TCP body dominates,
io_uring removes only transition) is expected to hold, but re-measure on two boxes
before fully closing the door. That re-measurement is the one thing that would
reopen this.

### 2026-08-13 — shared client deadline ticker

Task 0198's cross-box scalar-write profile found a different client-side fixed cost:
every coordinator opened twelve shard connections and each connection with an RPC
deadline owned a timerfd. At 16 coordinators this was 192 TCP sockets plus 192
timerfds (403 total FDs), and all sixteen coordinator threads were CPU-saturated.
The request path was already multiplexed by request ID; adding semantic batching or
a second connection pool was therefore the wrong lever.

NetTransport now registers deadline-bearing connections weakly in an executor-local
sweep driven by the reactor's existing 1 ms poll/flush cadence. Per-call absolute
deadlines and independent waiter results are unchanged. The candidate smoke profile
had 192 sockets and no transport deadline timerfds (the 16 timerfds present were
Coordinator hedge clocks), cutting total FDs from 403 to 227. Reply draining is also owned by
the connection's elected driver; newly queued siblings no longer repeat a
speculative nonblocking read before parking. `NetTransport::connection_stats`
exposes per-connection current/peak pending calls and queued bytes plus write and
read-drain counts; `vals-bench netload` prints their aggregate when
`VALS_NET_TRANSPORT_STATS` is set.

**How to reproduce:** bench runner `/tmp/run-ceiling.sh` (modes × pipeline sweep);
syscall split `/tmp/perf-split.sh` (brings up the SPDK cluster via
`spdk-cluster.sh start` as root with `SPDK_MEM_MB=2048`, loads 5M keys, backgrounds
reads, runs `perf trace -s` + `perf record` on node1). The bench source is the
standalone `uring-ceiling` crate named above, which is not in this repository.
Cluster start needs **root** on this box (DPDK can't use
IOVA=PA as non-root) and the `.spdk-cluster/` dir must not be left root-owned from
a prior run.
</content>
</invoke>

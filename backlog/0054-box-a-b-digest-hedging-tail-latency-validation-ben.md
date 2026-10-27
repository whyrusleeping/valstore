---
id: "0054"
title: 'Box A/B: digest hedging tail-latency validation (bench wiring + induced slow replica + peak regression check)'
status: done
priority: 4
created: "2026-07-05"
updated: "2026-07-06"
depends_on:
    - "0019"
spec_refs: []
---

## Description
Follow-on from task 0019, which landed the time-staggered digest hedge off-hardware: `Coordinator::with_digest_hedge(delay)` parks digest strong reads on a shared logical `HedgeClock` (no per-read timerfd), advanced by a single ticker task auto-spawned on the `CoordinatorService` executor; `vals_rt::quorum_collect_reserve_hedged` activates one held-back reserve digest per hedge firing (staggered), with failure-driven back-fill unchanged. Observability: `digest_hedges_fired` counter. Default (no hedge) path is byte-for-byte unchanged. The *measured* acceptance from 0019 still needs a box run.

Scope:
1. **Bench wiring**: expose a hedge-delay knob in the read benches and host the bench coordinators on a `CoordinatorService` (or spawn the hedge ticker onto the bench executor directly) so the shared clock actually ticks under load.
2. **Induced slow replica**: add a controlled per-replica slowdown (slow-but-not-failed — must not trip the failure detector) and measure read tail latency (p99/p99.9) with the hedge enabled vs the failure-only baseline; confirm the hedge cuts the tail and `digest_hedges_fired` accounts for the escalations.
3. **No peak regression**: on a healthy cluster, confirm peak read throughput is unchanged with the hedge enabled (the reserve must stay unsent and the ticker overhead negligible on the fast path).

Run on box A and box B (SPDK cluster).

**Status note (2026-07-05):** scope item 1 (bench wiring: `VALS_HEDGE_US` knob + hedge ticker on the bench executor in `disthedge`/`distmdmc`, `VALS_SLOW_NODE` induced-slow-replica wrapper, read tail p50/p90/p99/p99.9 + `digest_hedges_fired` reporting) is landed and verified off-hardware (mem backend: slow-replica p99 5.83ms → 1.61ms with the hedge on, hedges ≈ 1/3 of reads, exit 0). **Remaining: the on-hardware measurements only** — the tail-latency A/B under an induced slow replica and the peak-throughput no-regression check on the SPDK boxes.

**UNBLOCKED (2026-07-05)**: owner approved tearing down the live 4-node valsd cluster on oldbox; the four vfio-bound NVMe drives are free to claim. Box access verified: `ssh oldbox` (repo checkout at `~/vals`, passwordless sudo). Gracefully stop the old cluster (valsd pd + node1–4, root PIDs up since Jul 03) before claiming the drives. **Single-box note (owner, 2026-07-05): there is no second box** — run both measurements on oldbox alone: `distmdmc` with `VALS_HEDGE_US` on/off (peak A/B; hedges should stay ≈0 healthy) and a disthedge-pattern slow-replica run for the tail A/B (the "box A and box B" phrasing above is obsolete).

## Acceptance criteria
- Measured p99/p99.9 read tail-latency improvement under an induced slow replica with the hedge enabled vs disabled, on real hardware.
- No peak read-throughput regression with the hedge enabled on a healthy cluster (A/B on the same boxes).
- Hedge-delay knob wired into the benches; `digest_hedges_fired` observed consistent with the induced slowdowns. Any deviation filed as a bug task.

## Acceptance criteria

## Plan

Remaining scope is the on-hardware measurements only (bench wiring landed in 46b2540 and reviewed). Owner note: single box — run both measurements on oldbox. Box state verified this session: old valsd cluster already torn down (no valsd/vals-bench processes), 4 NVMe drives (05:00.0, 07:00.0, c1:00.0, c3:00.0) vfio-bound, 24576 free 2MB hugepages, repo checkout at ~/vals (at 24b87a3, an ancestor of local HEAD).

1. Bench-only gap: `distmdmc` honors VALS_HEDGE_US but not VALS_SLOW_NODE, so the tail A/B currently has no SPDK-backed harness (disthedge is mem-backed). The 0054 plan explicitly allowed wiring VALS_SLOW_NODE into distmdmc "if it drops in cleanly" — it does: SlowNode<T: Transport> wraps any Transport, and distmdmc's coordinators only use the Transport trait. Implementer wires it: when slow_node_env() is Some, wrap each coordinator's NetTransport in SlowNode (writes unaffected by design); when unset, the transport must remain unwrapped (keep the default path unchanged — e.g. factor the per-thread phases into a generic fn over T: Transport, or duplicate the small construction branch). Print the slow-node config in the distmdmc header line. Update the module doc-comment env-knob note. Verify: cargo fmt --check, clippy --workspace --all-targets clean, cargo test --workspace green, cargo build --features spdk (stub link) compiles, and a quick `vals-bench mem disthedge` smoke still passes.
2. Sync the box: stash the box repo's uncommitted backlog/doc edits (recoverable), push local HEAD to a box branch over ssh, checkout, apply the implementer's staged diff as a patch (the commit lands at end of task), build with VALS_SPDK_LINK=1 cargo build --release -p vals-bench --features spdk.
3. Peak no-regression A/B (healthy cluster): `vals-bench spdk distmdmc 2000000 64 <4 pci> 4 4`, interleaved runs with VALS_HEDGE_US unset vs =1000 (≥2 runs per side). Accept: strong-read ops/s unchanged within run-to-run noise (~±2-3%), digest_hedges_fired ≈ 0 (tiny p99-straggler count acceptable), 0 errors.
4. Tail A/B (induced slow replica, SPDK-backed): `VALS_SLOW_NODE=2:5000:digest vals-bench spdk distmdmc 500000 64 <4 pci> 1 4` with hedge off vs VALS_HEDGE_US=1000 (≥2 runs per side). Accept: p99/p99.9 materially lower with the hedge on; digest_hedges_fired consistent with the fraction of reads whose active digest lands on the slow node; 0 read errors (slowdown 5 ms ≪ op timeout, failure detector must not trip — verify no failure-detector log lines).
5. If back-to-back runs hit stale-superblock issues, reset drives per bench/b1-box-ab-notes.md (setup.sh reset → dd-zero superblocks → setup.sh); distmdmc formats with create=true so this is likely unnecessary.
6. Results: send numbers to the implementer to write bench/hedge-box-notes.md (methodology + tables + conclusion, following the existing bench/*-box-notes.md pattern). Any deviation (regression or no tail win) filed as a bug task per acceptance criteria.
7. Review, task → done, commit (bench wiring + notes).

## Work log
- 2026-07-05 plan: Context: the SPDK box (oldbox) currently runs a live 4-node valsd cluster on all 4 vfio-bound NVMe drives (root PIDs up since Jul 03) — the on-hardware A/B measurement cannot run without d
…[truncated]
- 2026-07-05 context hints: 10 recorded with plan
- 2026-07-05 context hints: crates/vals-bench/src/main.rs:1880-2050 — distmdmc read/write phases (READ_CONC pipelined reads; VALS_READ_FANOUT env pattern to mirror); crates/vals-bench/src/main.rs:904-1030 — workload_distspdk
…[truncated]
- 2026-07-05 implementer report: Delivered the off-hardware half of task 0054 (bench wiring + induced slow replica + locally-runnable tail-latency harness). All changes are confined to `crates/vals-bench/src/main.rs` — zero product
…[truncated]
- 2026-07-05 review tier: single-opus — reviewers: Claude
- 2026-07-05 review (Claude): accept — The change delivers the intended off-hardware half of task 0054, confined entirely to crates/vals-bench (zero production-code change). It wires the VALS_HEDGE_US hedge-delay knob into both a new self-
…[truncated]
- 2026-07-05 decision: accept — commit: bench: digest-hedge tail-latency wiring — VALS_HEDGE_US knob, disthedge harness, induced slow replica (task 0054)  Off-hardware half of task 0054, all in vals-bench (zero production change): - VALS_
…[truncated]
- 2026-07-05 usage: 67,437 tok (in 218, out 67,219, cache_r 7,561,749, cache_w 241,522) · cost n/a (unpriced)
  implementer: 50,698 tok (in 158, out 50,540, cache_r 6,190,243, cache_w 123,973) · cost n/a (unpriced)
  coordinator: 10,005 tok (in 30, out 9,975, cache_r 1,098,083, cache_w 86,343) · cost n/a (unpriced)
  reviewer:Claude: 6,734 tok (in 30, out 6,704, cache_r 273,423, cache_w 31,206) · cost n/a (unpriced)
- 2026-07-06 plan: Remaining scope is the on-hardware measurements only (bench wiring landed in 46b2540 and reviewed). Owner note: single box — run both measurements on oldbox. Box state verified this sessio
…[truncated]
- 2026-07-06 implementer report: Wired the existing `VALS_SLOW_NODE` induced-slow-replica knob into `workload_distmdmc` so the on-hardware digest-hedge tail A/B can run against the real SPDK-backed cluster. All changes confined to `c
…[truncated]
- 2026-07-06 review tier: simple (coordinator self-review)
- 2026-07-06 revision: Addressed the review: wrote the box-results file and added an optional doc pointer. No code logic changed.  Changes: 1. **`bench/hedge-box-notes.md`** (new) — written following the existing `bench/*
…[truncated]
- 2026-07-06 decision: accept — commit: bench: on-hardware digest-hedge A/B — slow-replica tail p99 −35%/p99.9 −27%, no peak regression at saturation; VALS_SLOW_NODE wired into distmdmc (task 0054)  Box runs on oldbox (4× P
…[truncated]
- 2026-07-06 usage: 82,980 tok (in 218, out 82,762, cache_r 6,505,651, cache_w 506,433) · cost n/a (unpriced)
  coordinator: 45,820 tok (in 112, out 45,708, cache_r 3,876,781, cache_w 351,315) · cost n/a (unpriced)
  implementer: 37,160 tok (in 106, out 37,054, cache_r 2,628,870, cache_w 155,118) · cost n/a (unpriced)

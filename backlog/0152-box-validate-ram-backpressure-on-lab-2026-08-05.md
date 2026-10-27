---
id: "0152"
title: 'Box-validate RAM backpressure on lab: 2026-08-05 corpus replay at RF=2 degrades to Busy instead of OOM'
status: done
priority: 1
created: "2026-08-07"
updated: "2026-08-08"
depends_on:
    - "0134"
    - "0153"
    - "0154"
spec_refs: []
---

## Description
Task 0134 landed the per-process RAM budget (`VALS_RAM_BUDGET_BYTES`, default 70% of MemTotal; retryable `Busy` on all wire Apply/BatchApply intake; deferred local AE-heal/re-home-copy intake; gauges `ram_budget_bytes`/`ram_headroom_bytes`/`memtable_bytes_total`/`flush_build_bytes`/`zone_engines`, counter `ram_busy_rejections`; `VALS_FLUSH_MAX_BYTES` now a per-node aggregate divided across live engines). This task is the hardware reproduction criterion split from 0134.

Procedure (see plans/bootes-soak-runbook.md — REBUILD ~/vals first; launch line now includes `VALS_RAM_BUDGET_BYTES=42949672960`):
1. Fresh lab cluster (RF=2, 3 nodes), replay the 2026-08-05 soak corpus (`/home/user/code/bootes/soak/dump-20260805T1515Z`, ~155M records; replay command in `bench/bootes-soak-lab/run-20260804T220428Z-batch1000/dump-20260805T1515Z/README.md`). Prior baseline: nodes 2 and 3 OOM-killed at 47.1 GiB peak ~50 min in (`bench/replay-lab/run-20260806T001507Z-tier-one-flushcap512m/FINDINGS.md`).
2. With an explicit 40 GiB budget, confirm: RSS plateaus at the budget, `ram_busy_rejections` rises, replay throughput degrades but no node is OOM-killed; reads keep serving; maintenance (flush/merge) keeps progressing while over budget.
3. Confirm recovery semantics: after stopping ingest, node stays alive and serving; headroom gauge reads 0; restart-free.
4. Record whether the 70% MemTotal default would have been adequate on these hugepage-reserving boxes (it overstates available memory; runbook says set the budget explicitly) and tune the default fraction if warranted.

Acceptance criteria:
- Replay run degrades into `Busy`/reduced throughput instead of OOM-killing any node; artifacts + FINDINGS.md under bench/ (e.g. bench/replay-lab/).
- Metrics captured showing budget engagement (ram_busy_rejections, ram_headroom_bytes→0, RSS plateau) and continued maintenance progress.
- Decision recorded on the default budget fraction and any runbook updates.

## Acceptance criteria

## Plan

Run 3 (post-0154, HEAD 82ac8eb) on the lab fleet. Ops task driven directly via ssh by the coordinator (same as runs 1–2); no code changes expected unless the run exposes a defect.

1. Preflight (runbook §0): boxes reachable (confirmed idle), hugepages=4096, panic_on_oops=1, scratch 9100 PRO on vfio-pci; restore SPDK setup if a box rebooted.
2. Rebuild ~/vals on all three boxes from HEAD (rsync + parallel `VALS_SPDK_LINK=1 cargo build --release -p vals-server --features spdk`); verify DONE_RC=0, fresh binary mtime, and the synced source carries 0154 (streaming sidecar cursors / `ae_digest_serve_bytes`).
3. Fresh cluster: wipe PD raft, start PD, start 3 nodes with `VALS_RAM_BUDGET_BYTES=42949672960` (40 GiB; merge input cap at 2 GiB default). Zones per runbook: `records` RF=2 sorted + canonical collection presplits, `backlinks` RF=2 sorted presplit=8. valsq placement-parity preflight on a split range (valsgo still blocked by 0144). Confirm `ae_digest_serve_bytes` present in /metrics.
4. New bench dir `bench/replay-lab/run-<TS>-rambudget40g-0154/`; monitor.sh extended from run 2 to also sample `ae_digest_serve_bytes`. 30 s cadence, full /metrics page every 5 min.
5. Launch the full-corpus replay from devbox (155.16M records, `valsdump replay --batch 1000 --tier one --r 1 --rf 2`, same unretried client as runs 1–2 for comparability). Budget engagement expected ~40–70 min in.
6. Validate while over budget (the run-2 kill window is the critical test): RSS+swap plateaus at/near 40 GiB — no march to the ~47 GiB kill line; `ram_busy_rejections` rising with headroom→0; maintenance counters advancing; `ae_digest_serve_bytes` bounded (KB–MB, not GiB) during AE serving; read probes (valsq gets of parity keys + bounded scans) served throughout. Success = zero OOM kills on all three boxes (journalctl -k check). If a burst signature reappears (RSS marching, shard counters frozen, gauges 0), capture threads+perf on the bursting node before it dies.
7. Recovery: after replay exits (or is deliberately stopped once the outcome is decided — sustained over-budget soak with no OOM suffices per acceptance), all nodes alive and serving, restart-free; RSS recedes below budget.
8. Teardown; FINDINGS.md + artifacts; record default-fraction decision (likely keep 70% + explicit-budget runbook guidance); update the runbook's 0154 pre-OOM gotcha and docs/backpressure.md known-limitation if the fix holds; file follow-on tasks for any defect. On success mark 0152 done and commit; on another OOM, capture full state and file/reopen attribution follow-up.

## Work log
- 2026-08-07 run 1 (this session): validation executed per plan and **FAILED** — nodes 1 and 3 OOM-killed (~47 GiB, full 8 GiB swap consumed) despite the 40 GiB budget engaging correctly and shedding all intake. Established: growth is non-intake (no merge committed during node 1's 15→48 GiB burst — `run_merges` frozen; node 3 crept +6.6 GiB/5 min with AE seeding the only major activity); the RSS-only check is swap-blind; the failed-plan retry loop alone does not leak (survivor node 2). Attribution to a specific allocator is OPEN (`pressure_merges`=0, `ae_seed_placement_coalesces`=0 throughout) — see task 0153. Full analysis + artifacts: `bench/replay-lab/run-20260807T195353Z-rambudget40g/FINDINGS.md`. This task now depends on 0153 and is the re-validation run once it lands; the re-run must capture: zero OOM kills, RSS plateau at/near budget, `ram_busy_rejections` engagement with headroom→0, maintenance counters advancing while over budget, read probes served throughout, and restart-free recovery after ingest stops. Default-fraction decision recorded in FINDINGS (keep 70% + explicit-budget runbook guidance; fraction tuning is moot until 0153). Rust-client parity preflight used instead of valsgo interop (0144 gap). Cluster stopped and boxes left idle.
- 2026-08-07 plan: Box-validation run on the lab fleet (I drive it directly via ssh — this is an ops task, no code changes expected unless the run exposes a defect).  1. Preflight (runbook §0): boxes reachable/idl
…[truncated]
- 2026-08-07 review tier: single-opus — reviewers: sol
- 2026-08-07 review (sol): revise — The run and artifacts are useful and honestly demonstrate that the existing RAM intake backpressure is insufficient, but they do not satisfy task 0152: two nodes were OOM-killed, RSS did not plateau, 
…[truncated]
- 2026-08-07 review (sol): revise — The revision correctly qualifies the allocator diagnosis, fixes the cgroup swap-accounting guidance, and clearly records the failed run as useful evidence with 0153 as a prerequisite. Nevertheless, ta
…[truncated]
- 2026-08-07 review (sol): revise — The revised write-up now accurately distinguishes observed counters from allocator hypotheses and correctly specifies cgroup-v2 swap accounting. The failed-run documentation and follow-up task are goo
…[truncated]
- 2026-08-07 decision: accept — commit: 0152 run 1: RAM-backpressure box validation FAILED — budget engaged and shed all intake, but non-intake maintenance/AE growth OOM-killed nodes 1+3 at ~47 GiB (swap-blind RSS check consumed 8 GiB mor
…[truncated]
- 2026-08-07 usage: 1,685,396 tok (in 509,418, out 72,874, cache_r 9,719,883, cache_w 377,288) · cost n/a (unpriced)
  reviewer:sol: 1,621,976 tok (in 509,244, out 9,628, cache_r 1,103,104, cache_w 0) · cost n/a (unpriced)
  coordinator: 63,420 tok (in 174, out 63,246, cache_r 8,616,779, cache_w 377,288) · cost n/a (unpriced)
- 2026-08-07 plan: Re-validation run (run 2) on the lab fleet after 0153 landed (HEAD 9c365d6). Ops task driven directly via ssh; no code changes expected unless the run exposes a defect.  1. Preflight (runbook §0):
…[truncated]
- 2026-08-07 run 2 (post-0153, this session): re-validation executed and **FAILED again** — node 3 OOM-killed at 22:46:16 UTC (anon-rss 48.97 GiB + full 8 GiB swap vs the engaged 40 GiB budget); node 1 ran the identical burst and survived only because its AE counterpart died. **Attribution now CLOSED**: 0153's growth modes did not recur (`merge_in_flight`=0 throughout, `ae_seed_ram_sheds` fired and drained the staged seed bytes, swap counted into the budget), and a live perf capture on node 1 mid-burst pinned the allocator: the responder side of AE bucket-digest serving (`serve_bucket_digests` → `merge_digest_sources`) materializes O(keys×runs) per page on the shard thread, is read-path (admitted over budget), and re-materializes everything each page for a far-behind peer. Filed as task 0154 (p1); this task now also depends on it and remains the re-validation run. Survivor recovery semantics verified (reads served, restart-free, maintenance progressing). Replay stopped deliberately at 84 min once the outcome was decided. Full analysis + artifacts: `bench/replay-lab/run-20260807T215938Z-rambudget40g-0153/FINDINGS.md`; runbook gotcha and docs/backpressure.md known-limitation updated. Cluster stopped, boxes idle; raw dwarf perf left at lab-1:/tmp/burst2.perf.
- 2026-08-07 review tier: single-opus — reviewers: sol
- 2026-08-07 review (sol): accept — This is a sound incremental result for the still-open validation task: the run is honestly marked failed, the captured metrics and OOM journal substantiate that node 3 exceeded the engaged 40 GiB budg
…[truncated]
- 2026-08-07 review (sol): accept — The revision addresses the prior findings: the smaller raw perf capture is now present and accurately distinguished from the 235 MB remote dwarf capture, while the timeline and 0154 diagnosis now qual
…[truncated]
- 2026-08-07 decision: accept — commit: 0152 run 2: post-0153 RAM-backpressure validation FAILED but attribution CLOSED — 0153's bounds held (merge_in_flight 0 throughout, seed shed fired, swap counted), yet node 3 OOM'd at 48.97 GiB vs t
…[truncated]
- 2026-08-07 usage: 1,149,600 tok (in 408,248, out 55,528, cache_r 7,855,394, cache_w 706,162) · cost n/a (unpriced)
  reviewer:sol: 1,100,099 tok (in 408,060, out 6,215, cache_r 685,824, cache_w 0) · cost n/a (unpriced)
  coordinator: 49,501 tok (in 188, out 49,313, cache_r 7,169,570, cache_w 706,162) · cost n/a (unpriced)
- 2026-08-08 plan: Run 3 (post-0154, HEAD 82ac8eb) on the lab fleet. Ops task driven directly via ssh by the coordinator (same as runs 1–2); no code changes expected unless the run exposes a defect.  1. Preflight (
…[truncated]
- 2026-08-08 run 3 (post-0154, this session): re-validation executed and **PASSED**. Arm 1 (faithful reproduction, 40 GiB budget): the full 155.16M-record corpus replayed to completion (48 min, 53.5k rec/s) with zero OOM kills, zero swap, peak RSS 14.7 GiB — the fatal growth mode is gone outright (`ae_digest_serve_bytes` bounded at 12–90 MB through replay AND the post-replay far-behind-peer AE heal; run 2 had nodes at 22–49 GiB in the same windows). Degradation mode was retryable Busy at the device reserve floor (the corpus' RF=2 physical volume exceeds the scratch drives' maintenance reserve; 55.1M unretried-client write errors — capacity property, not a regression). Arm 2 (engagement demonstration, deliberately tight 8 GiB budget below the natural working set): ~35 min sustained RSS plateau/oscillation at the budget ceiling (logged excursions to +10%), headroom→0, `ram_busy_rejections`→479, `ae_seed_ram_sheds`→697, maintenance advancing while over budget, reads served, then restart-free survival after ingest stop (alive, serving, headroom returning in samples while AE re-staging keeps oscillating around the tight budget). Decision: keep the 70% MemTotal default + explicit-budget runbook guidance for hugepage boxes; no fraction tuning warranted. Runbook pre-OOM gotcha rewritten (reserve-floor spam is a device-capacity signal again) and docs/backpressure.md known-limitation replaced with the validated 0154 bound. Artifacts: `bench/replay-lab/run-20260808T010817Z-rambudget40g-0154/FINDINGS.md`. Cluster stopped, boxes left idle.
- 2026-08-08 review tier: single-opus — reviewers: sol
- 2026-08-08 review (sol): accept — The two-arm hardware validation is sufficient to close 0152. The faithful 40 GiB run attempted the complete 155.16M-record corpus, passed the prior kill windows with all three nodes alive, no swap/OOM
…[truncated]
- 2026-08-08 review (sol): accept — The revision addresses the prior finding: it now distinguishes the 30-second sampled maximum from log-observed excursions, records the roughly 10% overshoot, and accurately characterizes post-ingest b
…[truncated]
- 2026-08-08 decision: accept — commit: 0152 run 3: RAM-backpressure box validation PASSED post-0154 — full 2026-08-05 corpus replay at RF=2 completes with zero OOM kills and flat RSS (peak 14.7 GiB vs 40 GiB budget; ae_digest_serve_bytes
…[truncated]
- 2026-08-08 usage: 1,621,043 tok (in 569,120, out 51,219, cache_r 5,880,823, cache_w 677,942) · cost n/a (unpriced)
  reviewer:sol: 1,578,310 tok (in 568,980, out 8,626, cache_r 1,000,704, cache_w 0) · cost n/a (unpriced)
  coordinator: 42,733 tok (in 140, out 42,593, cache_r 4,880,119, cache_w 677,942) · cost n/a (unpriced)

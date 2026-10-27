---
id: "0022"
title: 'Serving-core/oversaturation levers: cross-coordinator admission, SHARDS, send-path tuning'
status: done
priority: 4
created: "2026-07-02"
updated: "2026-08-11"
depends_on:
    - "0014"
spec_refs: []
---

## Description
BACKLOG.md §Hardware/benchmarking + §io_uring "better levers": past the write knee, throughput declines mildly with coordinator count — per-coordinator credit pools can't bound *aggregate* load. Levers: cross-coordinator admission (open design), leaning on `SHARDS` multi-core serving (known sweet spot ≈4, parallelizes the ~47% kernel TCP), skb page-frag churn reduction (`TCP_NOTSENT_LOWAT`, bigger contiguous sends, MSG_ZEROCOPY only if replies ≥16–32 KB), optional background straggler completion. Kernel-bypass TCP was evaluated and deferred pending a real-NIC profile (task 0014).

Open design — needs a decision on which lever to pursue first; likely wait for the real-NIC run before investing.

## Acceptance criteria
- Decision recorded post-0014; if pursued, box A/B showing a flatter past-knee curve without under-saturation regression.

## Plan

Treat this as the post-0014 decision gate rather than starting speculative transport work. Verify the completed real-NIC evidence and architecture summary, then record the decision to close 0022 without a new A/B: SHARDS is already the validated serving-core lever (420k reads/s at one shard/node, 820k at four, and at least 2.02M with four client hosts while nodes remained about 15% CPU), so the observed wall is client syscall/load-generation capacity rather than aggregate server oversaturation. Cross-coordinator admission cannot be evaluated past the true server knee yet, and send-path or zero-copy tuning is premature without a saturated-node profile and suitably large replies. Capture the now-distinct batched multi-get load-generator/client idea as a separate proposed follow-up, so it does not silently expand this accepted task. Confirm that task 0014's evidence and ARCHITECTURE.md already satisfy the documentation side; no production code should change unless verification reveals a concrete gap.

### Starting points
- backlog/0014-multi-machine-real-nic-benchmark-run.md — completed dependency and cross-box results
- plans/box-provisioning-runbook.md — first real multi-machine SPDK run
- bench/lab-read-prof/notes.md — shard scaling and client-load ceiling evidence
- ARCHITECTURE.md §Status steps 7 and 10 — shipped summary

## Work log

- 2026-08-07 dep resolved, still needs a decision: task 0014 (real-NIC multi-machine run) is **done**. What it found on lab: reads scale 420k/core → 820k at 4 shards/node → ≥2.02M with 4 client hosts at ~15% node CPU, i.e. the wall moved to *serving-core count + client syscall cost*, and the true node ceiling is still unmeasured for lack of load-generation capacity. That reorders the levers in this task: `SHARDS` multi-core serving is already validated as the working one, and the named next lever is a **batched multi-get client** (to saturate a node from fewer client hosts) rather than cross-coordinator admission or send-path micro-tuning. Decide whether to pursue that, or close this task and file the multi-get client separately.
- 2026-08-11 decision: **close without another box A/B or production change.** The post-0014 real-NIC evidence already validates `SHARDS` as the serving-core lever: one shard/node sustained about 420k strong reads/s, four shards reached about 820k, and distributed load reached at least 2.02M with the node boxes only about 15% busy. That run did not reach an aggregate server oversaturation knee; it exhausted client load-generation capacity instead. Cross-coordinator admission therefore has no measurable server-knee target yet, and send-path/`MSG_ZEROCOPY` tuning is premature without a saturated-node profile and large enough replies. Reconsider either only after a client can saturate the sharded nodes. Batched multi-get is a distinct client/load-generator follow-up, not an expansion of this task. Task 0014, `ARCHITECTURE.md` §Status steps 7 and 10, the box runbook, and the profiling notes already carry the benchmark evidence.
- 2026-08-11 review tier: simple (coordinator self-review)
- 2026-08-11 decision: accept — commit: 0022: close post-real-NIC serving lever decision
- 2026-08-11 usage: 1,076,847 tok (in 434,446, out 7,265, cache_r 635,136, cache_w 0) · cost n/a (unpriced)
  coordinator: 904,307 tok (in 334,214, out 4,845, cache_r 565,248, cache_w 0) · cost n/a (unpriced)
  implementer: 172,540 tok (in 100,232, out 2,420, cache_r 69,888, cache_w 0) · cost n/a (unpriced)

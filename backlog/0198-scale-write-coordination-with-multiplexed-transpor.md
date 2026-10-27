---
id: "0198"
title: Scale write coordination with multiplexed transport and fewer connections
status: done
priority: 2
created: "2026-08-12"
updated: "2026-08-13"
depends_on: []
spec_refs:
    - docs/io-uring-transport.md
    - bench/lab-breakthrough-20260812-0195/FINDINGS.md
---

## Description
Reduce the client connection/timer/syscall cost needed to drive the four-shard cluster. The throughput knee currently requires 32 coordinators x 512 in flight and 48 coordinators overloads; redesign NetTransport/CoordinatorService to multiplex more independent in-flight writes per shard connection without transparent semantic batching.

## Acceptance criteria
- Profile client CPU, syscalls, timerfds, FDs, NIC packets, and per-connection queueing at 16/24/32/40/48 coordinator shapes.
- Preserve per-call acknowledgement, retry, durability, timeout, and ordering semantics.
- Demonstrate a noticeable, repeatable performance improvement with controlled A/B testing against the then-current baseline. The result must materially reduce the coordinator threads/connections needed to reach matched throughput or materially raise the clean zero-error throughput ceiling, without a p99 regression at matched offered load.
- Treat the previously measured 2.68M writes/s only as historical context, not as a fixed target or success threshold; use a freshly measured baseline that includes intervening improvements.

## Plan

1. Establish a fresh current-tree baseline on the lab fleet with the existing scalar width-1 netload at 16/24/32/40/48 coordinators × 512 in flight, retaining throughput, errors/correctness, sampled acknowledgement latency, and load/maintenance state. At representative clean points, profile the devbox client for CPU hot spots, per-thread utilization, syscall mix, timerfd/FD counts, TCP packet rates, and per-connection queue/in-flight behavior.
2. Use those measurements to identify the actual coordinator-side scaling wall. Implement the smallest semantics-preserving multiplexing change that lets each coordinator/thread drive more independent scalar calls over the existing request-ID-multiplexed per-shard connections (or removes a measured per-connection/reactor/timer cost). Do not introduce transparent BatchApply or shared acknowledgements. Add narrowly targeted observability needed to report connection queue depth and transport behavior.
3. Add kill-the-fix tests covering independent out-of-order per-call replies, cancellation, timeout, reconnect/failure, send completion, and ordering/durability behavior affected by the change. Update user-facing architecture/performance documentation where the selected design or tuning changes operational guidance.
4. Run focused tests plus full debug vals-client/vals-cluster suites. Rebuild and deploy before each hardware arm.
5. Run controlled interleaved baseline/candidate A/Bs on fresh comparable current-tree configurations, repeating the 16/24/32/40/48 sweep and clean confirmation arms. Require zero errors and correctness mismatches, no p99 regression at matched offered load, and a repeatable material reduction in coordinators/connections for matched throughput or a materially higher clean ceiling. Retain raw evidence and write a findings report that explicitly treats 2.68M/s as historical context only. If profiling disproves the premise or no candidate clears the gate, revert the non-observability candidate and document the measured negative result rather than shipping an unproven optimization.

### Starting points
- crates/vals-client/src/net/client.rs: MuxConn/ConnState/MuxRpc already multiplex request IDs over one connection per node shard; timer is currently per connection
- crates/vals-client/src/coord/service.rs: persistent LocalExecutor service API
- crates/vals-bench/src/main.rs:6471 netload_thread and scalar width-1 workload driver
- docs/io-uring-transport.md documents prior transport profiling and rejected io_uring rewrite
- bench/lab-breakthrough-20260812-0195/FINDINGS.md and raw shape sweeps provide historical methodology only
- plans/bootes-soak-runbook.md contains current lab/devbox operational conventions

## Work log
- 2026-08-13 plan: 1. Establish a fresh current-tree baseline on the lab fleet with the existing scalar width-1 netload at 16/24/32/40/48 coordinators × 512 in flight, retaining throughput, errors/correctness, sampl
…[truncated]
- 2026-08-13 context hints: 6 recorded with plan
- 2026-08-13 context hints: MuxConn is already request-ID multiplexed. Measure rather than assuming transport redesign.; Use plans/bootes-soak-runbook.md for lab/devbox access conventions; rebuild before every run and bew
…[truncated]
- 2026-08-13 preload: 7 file(s), ~64 KiB seeded into implementer context
- 2026-08-13 implementation/review revision: Replaced per-connection deadline timerfds with an executor-local deadline sweep, retained driver-only reads while fixing the expired-driver/EPOLLET sibling-read race, removed the obsolete timer-error metric, and added socket/reactor reverse-reply and timeout/readability-race tests. Repeated interleaved correct-topology 16/24/32/40/48 sweeps captured `/proc`, FD/timer/socket, per-thread, ss queue, NIC, node-maintenance, transport queue, syscall, latency, error, and correctness evidence. At 16×512 the candidate repeated 2.481–2.541M/s versus baseline 1.978–2.034M/s with non-regressed p99; at 48 it opened all 576 sockets and ran zero-error while baseline hit the 1024-FD limit and produced 10.35–11.38M errors. Findings: bench/task-0198-multiplexed-transport/FINDINGS.md.
- 2026-08-13 implementer report: Implemented the measured client transport candidate for task 0198.  Changes: - Replaced one RPC deadline timerfd per shard connection with a thread-local weak connection registry swept on the reactor�
…[truncated]
- 2026-08-13 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-13 review (sol): revise — The transport implementation is plausible and the retained evidence shows promising gains, fewer timerfds, and passing reported suites. However, the task is explicitly still incomplete: required profi
…[truncated]
- 2026-08-13 review (sol#2/sol): revise — The candidate is promising and the production change is fairly small, but it is not yet correct or complete enough to satisfy task 0198. In particular, the driver-only read optimization can lose an ed
…[truncated]
- 2026-08-13 revision: Addressed all review findings.  Code/test fixes: - Fixed the EPOLLET race: an elected `MuxRpc` driver now always drains inbound readiness before returning its own resolved/timeout result, so readable
…[truncated]
- 2026-08-13 review (sol): accept — The revision addresses the prior review findings. It adds repeated correct-topology 16/24/32/40/48 A/B evidence with CPU/FD/timer/socket/queue/NIC/maintenance captures and separate syscall profiling,
…[truncated]
- 2026-08-13 review (sol#2/sol): accept — The revision addresses the correctness and cleanliness concerns. The elected driver now always drains the ET socket before completing, and production-path socket tests cover reverse request-ID replies
…[truncated]
- 2026-08-13 revision: Completed final hygiene: - Stripped trailing spaces/tabs from every changed task-0198 text artifact, including all captured `process.txt`/`ss` output, without changing substantive values. - Removed tr
…[truncated]
- 2026-08-13 decision: accept — commit: client: scale multiplexed write transport
- 2026-08-13 usage: 28,308,295 tok (in 4,761,038, out 114,297, cache_r 23,432,960, cache_w 0) · cost n/a (unpriced)
  implementer: 20,713,166 tok (in 2,122,068, out 47,994, cache_r 18,543,104, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 3,510,664 tok (in 1,062,044, out 34,540, cache_r 2,414,080, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 2,322,629 tok (in 956,560, out 25,397, cache_r 1,340,672, cache_w 0) · cost n/a (unpriced)
  coordinator: 1,761,836 tok (in 620,366, out 6,366, cache_r 1,135,104, cache_w 0) · cost n/a (unpriced)

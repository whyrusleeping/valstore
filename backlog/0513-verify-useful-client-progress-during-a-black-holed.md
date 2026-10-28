---
id: "0513"
title: Verify useful client progress during a black-holed replica outage
status: done
priority: 2
created: "2026-10-28"
updated: "2026-10-28"
depends_on: []
spec_refs:
    - docs/deploy.md#Three-PD reference deployment
    - docs/getting-started.md#Operating it
---

## Description

Accepted follow-up to adopter review 0507, finding 6. Address the documented RF3/W2 collapse from ~2k to ~13 applies/s under one black-holed host, rather than equating quorum arithmetic with practical availability.

Acceptance criteria:
- Reproduce the failure cheaply with the actual production-bounded Rust client, then identify time spent in sending, admission, quorum completion, deadlines and failure-detector routing.
- Establish a documented recommended client configuration (including failure detector) and fix any remaining head-of-line blocking that prevents useful healthy-quorum progress.
- Cover unreachable/black-holed and promptly refused/crashed peers; include reads and writes, deadlines, error/value checks, recovery and bounded memory behavior.
- Validate a regression locally and confirm with the shortest representative host-blackhole hardware drill; report throughput and latency before/during/after failure, not just election time.
- Keep automatic membership eviction out of scope unless separately accepted; no weakening of strict quorum or requested device durability.

## Outcome

0280 write collapse reproduced locally with the pre-0496 client and gone at HEAD. Remaining HOL (reads, recovery) found with new `vals-bench vals netprogress`. vals-client: FD-ranked reads with ride-along probes; hedge-overtaken replicas count as FD failures; batched reads stop on Timeout/Unavailable (23 s → 1 s); hints replay only to acking nodes, 64-wide; refresh prefers healthy members; connections silent for a deadline are redialed. Recommended setup (FdConfig::default(), optional service + 50 ms hedge, majority concerns) in vals-client docs "Replica outages"; tokio_bridge enables FD. Hardware (host black-holed 30 s): no FD ~17/s; FD normal after one 5 s deadline; FD+hedge no dip (p99 62 ms onset); recovery ~2 s; values verified. Follow-up for 0505: deploy.md 0280 lesson stale. Evidence: bench/0513-blackhole/FINDINGS.md.

Commit: 0513: keep client progress through a black-holed replica (FD-ranked reads, hedge-taught detector, batched-read failover, ack-gated hint replay, silent-connection reset, netprogress client)

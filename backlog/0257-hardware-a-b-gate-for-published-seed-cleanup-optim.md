---
id: "0257"
title: Hardware A/B gate for published seed cleanup optimization (0253) under rehome ingest
status: done
priority: 2
created: "2026-09-05"
updated: "2026-09-07"
depends_on:
    - "0253"
spec_refs: []
---

## Description

Split hardware acceptance from task 0253 so its bounded cleanup software change can be reviewed/committed without disturbing the ongoing 0248 heap-corruption fleet arms.

Acceptance criteria:
- Run short matched SPDK before/after profiles under rehome ingest, using the 0253 parent and optimized code with identical allocator, workload, safety fences, and AE enabled.
- Verify driver event-rate exposure and PD latency/delivery/rehome progress, not just process liveness; report restart/recovery or crashed-node confounds explicitly.
- Attribute published seed cleanup samples and check final map deallocation/cleanup-turn tail latency at representative seed sizes; no claim based solely on whole-boot comparisons.
- Archive commands, revisions/configuration, raw profiles/metrics and conclusions under bench/. Coordinate any fleet restart/deployment with the 0248 investigation; do not wipe drives or change allocator-control arms silently.

Original evidence: bench/0248-takeover-20260905/stock-seed-annotate.txt and perf-node1-stock-recovery.txt.

## Outcome

Passed scoped 0253 published-cleanup hardware gate; independent standard review accepted after pinned-source rebuilds byte-matched both retained binaries. Matched SPDK arms processed ~12M events and committed 9/9 populated rehomes each. Non-final mean improved 904.5→89.2 µs and p99 bucket 2.5ms→250µs; all final turns ≤25ms, but candidate final p99 was higher (no final-tail improvement claim). Profiles confirm removed map/gauge work. Coordinator reran cleanup, production seed-budget and observe tests plus raw evidence analysis and diff checks. Evidence/instrumentation in b983da07 and bench/0257-hardware-20260907. Known mirror timeouts, AER storms and post-evidence shutdown SIGKILLs explicitly excluded from broader claims; fleet stopped format=false with cache/forensics preserved, retained data needs crash recovery.

Commit: bench: verify cleanup hardware gate provenance and review

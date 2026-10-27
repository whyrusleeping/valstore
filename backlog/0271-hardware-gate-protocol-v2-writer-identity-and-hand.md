---
id: "0271"
title: Hardware-gate protocol-v2 writer identity and handshake changes
status: done
priority: 2
created: "2026-09-07"
updated: "2026-09-08"
depends_on:
    - "0236"
    - "0270"
spec_refs:
    - spec.md#Constraints / conventions
    - docs/formats.md
---

## Description

Split required hot-path hardware validation from 0236's locally verified correctness fix. Versions grow from 12 to 16 wire bytes, HLC uses shared atomics, and first requests wait for accepted peer hello. Before deployment adoption, run short source-pinned SPDK matched baseline/candidate arms using compatible clients for each protocol; never run an old binary over candidate-written data.

Acceptance: record throughput and latency distributions for steady-state batch writes and reconnect-heavy writes, CPU profiles, full-value correctness, restart/recovery and AE convergence with full-width writer IDs. Quantify handshake RTT and payload-width effects. Preserve cores/cache/retained evidence and respect current fleet maintenance constraints; no implicit format=true or unapproved wipe. Record evidence and exact binaries/configurations, and leave fleet in a documented stopped or explicitly authorized state.

## Outcome

Accepted by both independent reviewers: source-isolated RF=2 SPDK baseline/candidate gate on lab-2/3. Median steady throughput -1.14%; reconnect p50 +1.657ms with workload-only throughput -39.8%; noisy extreme tails disclosed. Sampled full-value and full-width Go checks, forced-stop crash recovery, and 2048-entry AE repair passed. Exact source patch/hash, configs, raw distributions and CPU profiles retained; analysis reproduced independently. Scope excludes unavailable lab-1 and post-f1529cbd fixes. Reachable fleet stopped, retained v2 corpus needs crash recovery; no v1 reuse, cores/cache preserved. Evidence: bench/0271-hardware-20260908/FINDINGS.md.

Commit: Record reviewed protocol-v2 SPDK hardware gate

---
id: "0210"
title: Winner-directory batch scratch reuse, probe telemetry, and SIMD follow-ups
status: done
priority: 3
created: "2026-08-14"
updated: "2026-08-16"
depends_on:
    - "0205"
spec_refs:
    - ARCHITECTURE.md#Status
    - docs/out-of-core-index.md
    - bench/lab-read-breakthrough-20260814-0204/FINDINGS.md
---

## Description

Pursue bounded incremental improvements to task 0204's batched winner probing only where task 0205 demonstrates remaining directory cost.

Acceptance criteria:
- Add low-overhead counters or sampled telemetry for home-slot hits, empty misses, fingerprint mismatches, deleted buckets, collision fallbacks, and probe lengths.
- Remove avoidable per-request allocation for key references, hashes, candidates, tokens, and staging slots through bounded reusable per-connection or per-shard scratch without retaining request data across calls.
- Prototype and A/B a tuned prefetch distance and SIMD control-byte group scan when measured fallback/probe distributions justify them; preserve a portable scalar implementation.
- If a client-provided hash hint is tested, treat it only as a hint and recompute/fallback on mismatch so malformed hints cannot create false absence.
- Preserve full-key collision authority, token invalidation across update/remove/rebuild/compaction, absent-key semantics, paging, and capacity-profile fallback; add adversarial collision/deletion tests.
- Hardware gate CPU/stalls/LLC misses/key, throughput, p50/p99/max, allocations, RSS, and correctness. Revert individually neutral or negative sub-experiments rather than accumulating optional paths.

## Outcome

Added shard-scoped one-in-1024 winner-directory probe telemetry with adversarial collision/deletion accounting and Prometheus export. Hardware-gated reusable mixed-read scratch on the recreated mature RF2 corpus; reverted it as throughput/CPU neutral, and declined SIMD/prefetch/hash-hint follow-ups because fresh profiles left directory work at ~3% with only 2.08% of probes over eight buckets. Retained complete A/B evidence and left lab inactive with format=false.

Commit: perf: sample winner-directory probe telemetry

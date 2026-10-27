---
id: "0200"
title: Large-scale write-path performance pass for 24B-record ATProto backfill
status: done
priority: 1
created: "2026-08-13"
updated: "2026-09-08"
depends_on: []
spec_refs:
    - Current focus areas
    - docs/backpressure.md
    - docs/optional-feature-survey.md
---

## Description
Review and profile the current write path for an upcoming full-network ATProto backfill (~12B primary records plus ~12B backlink records). Establish capacity/runtime estimates and bottlenecks, run targeted benchmarks and profiling where feasible, identify high-leverage improvements, implement and validate safe wins, and document hardware-validation follow-ups.

Acceptance criteria:
- Current bulk-write path and supported deployment profile are reviewed against the ~24B-record workload.
- Representative write benchmarks are run with enough workload detail to interpret throughput, amplification, RAM, and capacity implications.
- CPU/I/O/space bottlenecks are measured before proposing substantial design changes.
- Safe high-confidence improvements found during the pass are implemented with regression tests and workspace checks.
- Remaining hardware experiments or design work are captured as accepted or proposed backlog items, as appropriate.
- Durable performance/capacity conclusions and operational guidance are documented.

## Acceptance criteria

## Work log

- 2026-09-08 fresh review/closure: acceptance met through retained successor work. 0201 completed the owed A/B and initially rejected/reverted the candidate; 0229 later reproduced the mature hotspot repeatedly and hardware-validated the retained linear `victim_lives_ascending` (not `smallest_victim`). 0202 shipped/validated variable extents and supersedes the historical equal-slab procurement guidance. Corrected findings and architecture to state this history and distinguish the contextual original probe summary from the later retained raw evidence. Reviewer independently reran the 0201 analyzer (24 clean arms, byte-matching CSVs), 0229 summarizer, and existing victim-reference test. No further 0200 code change or hardware run is owed. The production-scale real-slice go/no-go remains proposed task 0203, with 0248/0250 blockers; closing this measurement pass does not accept those separate experiments or claim production readiness.

- 2026-08-13: Reviewed the Rust Apply/BatchApply path, the Go valsgo coordinator, bootes' ordered two-zone sink, deployment profiles, retained real-corpus replay/space evidence, and sorted-range placement/capacity behavior. Derived a 40–43 TiB live RF=2 allocation estimate plus headroom for the 12B+12B target; documented the equal per-zone slab tax and why latency profile is not viable at this cardinality.
- 2026-08-13: Probed the mature three-node/four-shard SPDK cluster with durable 1,000-entry, 400-byte batches. Clean throughput rose 78.9k → 173.8k → 259.7k → 314.9k logical entries/s at 1/2/4/8 callers; 12+ callers overloaded the bounded BatchApply gate and are invalid capacity points. Two longer 8-caller arms sustained 270.8k and 281.4k/s with zero client errors.
- 2026-08-13: Profiled the hottest serving shard during the clean mature batch arm. `Engine::victim_lives_ascending` consumed 22.99% of core-cycle self samples (38.55% atom). Replaced its O(victims × segment references) rescans with one victim-map aggregation pass; `smallest_victim` reuses it. Added shared-segment/zero-live/tie-order and malformed-metadata tests. Full debug vals-store and vals-cluster suites pass; optimized vals-store build passes.
- 2026-08-13: Wrote `bench/task-0200-atproto-backfill/FINDINGS.md` and updated deployment/soak/architecture guidance. Hardware throughput A/B of the linear victim aggregation remains owed on a fresh or safely restartable corpus; the current latency-profile scratch corpus is near its RAM budget and has O(dataset) restart rebuild, so it was not destructively restarted.

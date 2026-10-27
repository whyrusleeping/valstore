---
id: "0344"
title: 'Finite-client residuals from 0287: combined hedge-loser credit-release test and width-32 batch-read credit-path cost'
status: done
priority: 4
created: "2026-09-29"
updated: "2026-10-12"
depends_on: []
spec_refs: []
---

## Description
Residual from the 0287 no-go (bench/0287-hardware-20260909/FINDINGS.md). (1) No test proves that a fired digest hedge loser releases its finite transport/logical credits. vals-cluster coord tests cover hedge activation, and vals-client transport tests cover cancellation and credit release, but only separately. A local controlled-TCP test combining the two would close the unmet criterion without hardware. (2) Finite defaults cost 4.59%/6.78% median width-32 Digest/Quorum read throughput on SPDK RF2. Profiles show try_transport_credit, release and reserve on the read path. Optimize only if a concrete current Rust-client read-throughput need exists (production ingest uses the Go client).

Acceptance (if accepted): (1) a local test in which a hedge fires and the loser is cancelled, with terminal credits/waiters back to baseline; (2) optional, measure first: a matched A/B showing reduced finite batch-read overhead, with no removal of bounds.

## Acceptance criteria

## Work log
- 2026-10-12: declined. Production ingest uses the Go client, so there is no current Rust-client read-throughput need, and the hedge/credit pieces are already tested separately.

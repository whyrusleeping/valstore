---
id: "0269"
title: Define the supported public API and ship a runnable client demo
status: done
priority: 2
created: "2026-09-07"
updated: "2026-10-28"
depends_on:
    - "0242"
    - "0266"
spec_refs:
    - docs/production-readiness-review-2026-08.md#R15 — Define and shrink the supported product/API surface (P1/P2)
    - docs/production-readiness-review-2026-08.md#R17 — Repair the first-user path and status documentation (P1/P2)
---

## Description

Accepted conditional API/demo work (2026-09-29), blocked on selecting an external-user scope and the contract/publication decisions in 0242/0266. The redundant umbrella 0244 was removed. Decide supported daemon/client/embedding artifacts and seal or explicitly mark internals; document cancellation, retry, persistence, and thread-safety contracts. Ship a versioned runnable TCP put/get/delete example and a repeatable local demo without sibling-directory assumptions. Repair status/compatibility/config references without claiming unresolved blockers are fixed. Coordinate API decisions with 0242/0264 and publication intent with 0266; no public API freeze or release is implied by this proposal.

## Acceptance criteria

- Identify the supported daemon, client and embedding artifacts and API surface; seal or explicitly mark internals without silently breaking existing users. Evaluate three distinct contracts: remote `vals-client`, local `vals-store` engine, and embedded `vals-cluster` node/PD. The daemon client is not an embedded deployment.
- Keep embedding first-class: cover persistent local engine create/reopen/sync/maintenance and distributed-node runtime ownership, security, recovery and shutdown separately from daemon configuration. Daemon-first onboarding does not deprioritize embedded API correctness or support. Make the raw Engine's version allocation and low-level delete/removal contract explicit; do not advertise it as interchangeable with the replica layer's logical tombstones.
- Document cancellation, retry/ambiguous-write outcomes, persistence, thread-safety and executor/reactor requirements at their public API entry points.
- Add useful method/type rustdoc for client connection/configuration, reads/writes, quorum and durability options, errors, zones and scans; document limits and consistency guarantees rather than merely repeating signatures.
- Keep the runnable TCP demo and local deployment instructions working without private repositories or sibling-directory assumptions.
- Repair relevant status/compatibility/config references without claiming unresolved gates have passed. No API freeze or production release is implied.
- Coordinate the Tokio integration example (0508), quorum/durability ergonomics (0512), and evaluation-release/support statement (0514) without duplicating their deliverables.

## Outcome

Supported API defined in crate rustdoc for vals-client, vals-store, vals-cluster and vals-rt, with internals marked hidden or named internal (non-breaking). Client entry points document threading/executor, cancellation, ambiguous writes and retries, persistence, consistency and limits; Engine and embedded Node/PD contracts are explicit. kv and tokio_bridge examples landed with a locked Tokio dev-dependency (1.50.0, single syn major). Verified with locked build/tests/doctests, -D warnings rustdoc, engine scratch checks and a local 3-node file cluster (kv, tokio_bridge, oversized write, range delete, reverse scan). Onboarding prose (README/getting-started/embedding.md) remains with the 0505 stack.

Commit: 0269: document the supported client, engine and embedding API; land kv and tokio_bridge examples

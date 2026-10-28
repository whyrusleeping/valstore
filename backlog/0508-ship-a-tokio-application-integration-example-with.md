---
id: "0508"
title: Ship a Tokio application integration example with a bounded vals worker
status: done
priority: 2
created: "2026-10-28"
updated: "2026-10-28"
depends_on: []
spec_refs:
    - docs/getting-started.md#Clients
---

## Description
Accepted follow-up to adopter review 0507 (finding 1, priority 1). Make the existing thread-local vals client practical to use from a normal multithreaded Tokio application without changing the production runtime or adding Tokio to its hot-path dependencies.

Acceptance criteria:
- Runnable in-tree example constructs Coordinator/NetTransport on a dedicated OS thread and exposes a cloneable Send handle using a bounded request queue and async replies.
- Demonstrate put/get/delete from Tokio tasks, startup seed discovery, device-durable majority writes, explicit shutdown/join and propagation of initialization/operation/worker failures.
- Document queue/concurrency limits, timeout and cancellation semantics (dropping a request cannot retract remote writes), and the fact this is an integration pattern rather than a new supported high-throughput client API.
- Add focused tests for the cross-runtime boundary, including dropped callers and clean shutdown, and run the example against the documented local file-backed cluster.
- Link the example from the getting-started client section; preserve unrelated dirty work. No axum dependency or runtime rewrite is required.

## Acceptance criteria

## Work log

### 2026-10-28 — implemented and verified
- Added `crates/vals-client/examples/tokio_bridge.rs`: cloneable Send request handle, count-bounded inbox (32 queued requests), per-request replies and a separate lifecycle owner. Coordinator, DNS and transport initialization stay on a dedicated OS thread; a private current-thread Tokio runtime parks the inbox while vals-rt/reactor drive each serial operation. No Tokio dependency enters the production client graph.
- Startup tries seed discovery and derives the default-zone read majority. Puts/deletes request device-durable majority acknowledgements. Four Tokio tasks demonstrate value/version round trips and deletion using independent per-run keys.
- Shutdown atomically closes admission, drains accepted non-cancelled requests even with live handle clones, and joins through spawn_blocking. Dropped owners request termination; only explicit shutdown observes worker completion/errors. Queued abandoned requests are skipped, while started writes are not cancelled.
- Added the getting-started Tokio section and README link, including count-versus-byte bounds, serial head-of-line waiting, operation/timeout/worker-error ambiguity, long-idle first-write failure, and the startup-only numeric read quorum. The example is a pattern to copy, not a new supported throughput API.
- Added seven focused tests and `[[example]] test = true`, so ordinary workspace/package tests execute them. Tests cover !Send worker state with Send async handles, cancellation before/after execution, queue backpressure, draining shutdown with live clones, initialization/operation/panic/channel failures, owner drop, seed parsing, and a real refused-seed startup path.
- Verification: `cargo test --locked -p vals-client` passed (173 library tests, 7 example tests, 2 doctests); example build passed; example Clippy passed with five existing library warnings and no new example warnings; `git diff --check` passed. `cargo tree --locked -p vals-client --edges normal` confirms no Tokio production dependency.
- Live file-backed smoke: README RF3 cluster passed all four concurrent-task round trips/deletes, both with default seeds and an unreachable first seed followed by one live discovery seed. Single-node RF1 passed with a dynamically selected loopback port. The initial RF1 fixture used an occupied port and failed daemon startup; no existing process was stopped, and the corrected fixture passed. All test-owned daemons shut down cleanly.
- No hardware qualification or performance claim is made. No production API/defaults or unrelated dirty work was changed.

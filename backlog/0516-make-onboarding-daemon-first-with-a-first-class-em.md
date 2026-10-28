---
id: "0516"
title: Make onboarding daemon-first with a first-class embedding guide
status: done
priority: 2
created: "2026-10-28"
updated: "2026-10-28"
depends_on: []
spec_refs:
    - README.md#Quick start
    - docs/getting-started.md
    - docs/README.md
---

## Description
Owner accepted daemon-first presentation with first-class embedding support. Implement the docs restructuring now; do not imply the existing growth, benchmark, recovery or API tasks are complete.

Acceptance criteria:
- README leads with the persistent local valsd deployment and CLI, followed by application/client and operations links; retain a prominent compact embedding entry point rather than a long embedded toy as the first onboarding step.
- Getting-started follows the daemon/client path and shared consistency/durability/zone contracts, without forcing readers through Rust node/PD construction.
- Dedicated embedding guide clearly distinguishes vals-client (remote daemon client), vals-store (local engine), and vals-cluster (embedded distributed node/control plane). Cover real persistent engine create/reopen/sync, runtime/thread ownership, shutdown/recovery, local-only versus replicated guarantees, and responsibilities inherited from valsd. Preserve useful existing embedded-node/PD guidance while correcting unsupported grow-single-PD implications.
- Share distributed semantics by linking to the common user guide; keep lifecycle mechanics separate. Preserve important incoming documentation links or update them.
- Verify changed runnable examples against current APIs, run cheap local docs/quick-start checks, and review the two journeys independently.
- Update related backlog scope/priorities to reflect daemon-first adoption and separate local-engine versus distributed embedding contracts. Preserve all unrelated work.

## Acceptance criteria

## Work log

### 2026-10-28 — implemented and verified
- README now leads with the persistent local valsd deployment, CLI, application clients and operator links. Replaced the long first-step embedded toy with a compact, prominent embedding entry point; explicitly distinguish a remote Rust client from embedding the database.
- Getting-started now follows daemon startup -> CLI/client/Tokio integration -> shared distributed model and durability contract -> zones/scans -> daemon operations. Moved Node/PD construction and the in-process toy to the new embedding guide. Zone creation and manual splitting use daemon admin commands, with queued-versus-committed receipts and authoritative confirmation explained.
- Added docs/embedding.md with separate remote-client, local-engine and distributed-node paths. Includes safe fresh file creation versus reopen, checked sync and recovery, version/deletion caveats, thread/reactor ownership, persistent Node/PD sketches, liveness and maintenance, shutdown and inherited configuration/security/monitoring responsibilities. The raw-engine demo intentionally demonstrates puts/reads rather than presenting low-level Engine::delete as a general logical tombstone API.
- Both guides link to the shared quorum/LWW/durability/zone contract. Added the guide to docs/README.md and README; recorded the accepted entry-point design in spec.md without replacing its index layout. Fixed the unsupported single-PD grow-in-place implication in the user entry points.
- Backlog: retained full 0269 criteria while distinguishing remote client, local engine and embedded Node/PD contracts; 0509 remains the next daemon-onboarding growth/migration deliverable; raised 0510 to P2 for representative valsd measurements. Recorded partial docs progress in 0512 without marking policy/API work complete. No migration, benchmark, recovery or API freeze is implied.
- Verification: extracted exact complete Rust snippets into temporary Cargo harnesses and compiled/ran the TCP client, client zone operations, local engine and LocalCluster demo. Fresh engine create/sync/drop/reopen and subsequent reopen passed; repeated creation refused with backing-file bytes unchanged; missing-store reopen refused without creating data. Node/PD partial sketches typechecked with explicit harness placeholders; they were not run as a deployment.
- Live file-backed smoke: README cluster setup, CLI put/get/delete/scan, exact device-durable TCP snippet, zone creation, Tokio bridge and graceful restart passed. Manual split returned queued intent; confirmed its expected canonical boundary through authoritative ranges before scanning successfully. All owned daemons shut down; no unrelated processes or data touched.
- Checked 137 local paths/anchors across README, getting-started, embedding, docs index and spec; git diff --check passed. Independent read-only journey review found a leftover embedded split call and ambiguous distributed-path wording; both were corrected and the manual-split replacement was exercised.
- Docs-only changes; no production code or defaults changed, no full workspace suite or hardware/power-failure qualification claimed. Existing unrelated dirty work preserved.

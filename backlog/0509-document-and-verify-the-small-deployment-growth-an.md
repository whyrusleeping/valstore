---
id: "0509"
title: Document and verify the small-deployment growth and migration path
status: blocked
priority: 2
created: "2026-10-28"
updated: "2026-10-28"
depends_on: []
spec_refs:
    - docs/deploy.md
    - docs/getting-started.md
---

## Description
Accepted follow-up to adopter review 0507, finding 4 and priority 2. Give a production-bound side project an honest supported path from one machine to a larger HA deployment.

Acceptance criteria:
- Verify and document a minimal file-backed single-node/RF1 plus PD setup and its failure/security/durability limitations.
- Explain per-zone region sizing, full-zone behavior, and supported growth of existing file-backed data; do not imply raising region_mib resizes existing files unless implemented and tested.
- Document migration to multiple hosts and three voting PDs, including what can be done in place and what requires a new cluster/backup/restore; remove the incorrect 'raft-replicate later' implication.
- Provide verifiable migration/validation commands and explicit capacity and downtime prerequisites. Use local smoke tests for the documented supported transitions; do not imply hardware qualification.
- Link the progression clearly from the quick-start/user guide. Preserve existing docs work.

## Acceptance criteria

## Work log

### 2026-10-28 — daemon adoption is the next onboarding priority
Owner accepted daemon-first presentation with first-class embedding. Prioritize this growth/migration path as the next daemon-onboarding deliverable; do not replace it with more embedded demos. Task 0516 removes the unsupported single-PD grow-in-place implication and adds prominent links to HA and backup/restore guidance. Minimal deployment, capacity growth/full-zone behavior and verified migration steps remain open under this task's original criteria.

### 2026-10-28 — growth guide written and smoke-tested
Added `docs/growing.md`: a transition table, a minimal single-node RF 1 + one PD file-backed setup with its limits, file-backend zone sizing and full-zone behaviour, online `add-node`, the backup/restore migration to three PDs / new addresses / larger regions (prerequisites, downtime, capacity, steps, rollback), and RF. Linked from deploy.md (five-minute cluster, unsupported voter-count changes, `region_mib` comment, `remove-node` RF 1 caveat), backup-restore.md (Restoring a backup), and in the working tree from README.md, docs/README.md and docs/getting-started.md.

Local smoke tests (release valsd 0b546f51 tree, file backend, gc_grace = 0s, one machine; scripts in local-only bench/0509-growth):
- Minimal setup: the documented node/PD configs pass `--check-config`; status/members/zones, kv put/get/scan, restart persistence, a deleted key stays deleted; `drop-zone 0` refused (`zone 0 does not exist`).
- `region_mib` 64 -> 128 + restart: existing zone files stayed 64 MiB (sparse), a new zone got 128 MiB.
- Full zone (16 MiB region, 120 KB values): Busy (`backpressure` client error) after 65 writes (~7.8 MB); writes and deletes both refused; reads fine; still Busy ~3 min later. `add-node` of a second node handed off 34/65 keys but node 1 stayed at 5 free segments with compaction plans failing for ~3 more minutes (proposed 0521). Backup of the full cluster worked; restore into 3 PD voters + a 64 MiB region: load/compare equal, 60/60 big writes succeeded, then add-node 2 and 3.
- RF 1 add-node (200 keys per zone, value-checked reads): 1/200 default-zone keys read as not found in the first pass, all 200 ~2 s later. RF 1 remove-node: 105/200 not found even with the removed node running; re-adding it restored all (proposed 0520).
- PD `rf` 1 -> 3 config edit + PD restarts (all members in data_node): nodes adopted `rf 3`, data spread; untested path, documented as not to do (proposed 0522).
- End-to-end rehearsal of the documented runbook (default + sorted zone, 100 keys each, a deleted key): backup/verify, stop source PD only, restore-pd x3 with one incarnation (same digest), start, status (voters=1,2,3), load, compare `ok target matches`, all keys value-checked and the deleted key absent, source nodes still served every key with their PD stopped, add-node 2 and 3 (all keys read), RF 3 zone created and used, and rollback (old cluster restarted, every key read).
- Docs: example ini blocks pass `--check-config`; all relative links/anchors in growing.md resolve against HEAD; `git diff --check` clean.

### 2026-10-28 — review: blocked on landing the onboarding stack
Review round 1 (sol, revise) found the guide's content sound (its independent `cargo test -p vals-server --test backup restore` passed) but the committable snapshot incoherent: (1) the guide's `cargo build --release -p vals-client --example kv` fails on the snapshot because `crates/vals-client/examples/` is untracked (0269's demo); (2) HEAD's README/getting-started/docs index do not link the guide, and HEAD's getting-started still says "Single-PD (durable): fine to start with; raft-replicate later." Both are already fixed in the working tree, but by uncommitted 0505 (in_review) / 0516 (done) / 0269 edits, whose closure also needs untracked docs/embedding.md, SECURITY.md, CONTRIBUTING.md, docs/format-history.md, the tokio_bridge example and the Cargo.toml/Cargo.lock dev-dependency change. The commit tool only takes files this session changed, so 0509 cannot land a coherent tree without bypassing 0505's review gate or making artificial edits to those files.

Unblock: commit the pending onboarding stack (0505's review and commit, carrying the 0516/0269 files), or have the owner approve landing it together with 0509. Then re-run 0509's review on a snapshot that includes the `kv` example and the README/getting-started/docs-index links (already in the working tree; see the note in 0505), and commit `docs/growing.md`, deploy.md and backup-restore.md. Nothing else in 0509 remains open.

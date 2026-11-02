---
id: "0509"
title: Document and verify the small-deployment growth and migration path
status: done
priority: 2
created: "2026-10-28"
updated: "2026-11-02"
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

## Outcome

docs/growing.md (minimal RF1+PD setup and limits, zone sizing/full-zone behaviour, online add-node, backup/restore migration to three PDs/new hosts/larger regions with prerequisites and rollback, RF) is landed and linked from README, docs index, getting-started, deploy and backup-restore; stale gc_grace wording fixed; local rehearsal passes on HEAD 72459e9; reviewer (sol) accepted after running the backup restore tests and kv example build.

Commit: 0509: growing.md gc_grace advice reflects 0517 (restore into tombstone-GC nodes tested locally, hardware drill used gc_grace = 0s); growth guide re-verified against HEAD

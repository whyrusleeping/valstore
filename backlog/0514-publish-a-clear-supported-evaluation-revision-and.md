---
id: "0514"
title: Publish a clear supported evaluation revision and production security checklist
status: done
priority: 2
created: "2026-10-28"
updated: "2026-10-28"
depends_on:
    - "0269"
spec_refs:
    - SECURITY.md
    - docs/deploy.md
    - docs/production-readiness-review-2026-08.md
---

## Description

Accepted follow-up to adopter review 0507, finding 7 and priority 5 (release/evidence portion; supported API remains in 0269). Let adopters determine current support and deployment obligations without archaeology through historical reviews/backlog.

Acceptance criteria:
- Identify a pinned revision or experimental release for evaluation with explicit software/hardware/recovery evidence, supported artifacts/platforms, compatibility/upgrade expectations and limitations; do not claim production qualification from unrelated ingest results.
- Reconcile the historical production-readiness review with a concise current disposition per finding, retaining original evidence/history rather than rewriting it as current fact.
- Provide a copyable security checklist for enforced data-port isolation, cleartext control token/overlay requirements, local admin socket access, metrics binding and secret permissions.
- Make example systemd units implement the documented restricted admin-socket permissions (UMask/private runtime directory and appropriate daemon user guidance) without breaking SPDK ownership requirements.
- Separate backup/restore qualification from hardware ingest evidence in status summaries. Coordinate with docs task 0505 and API task 0269; no new security transport implementation is implied.

## Outcome

docs/evaluation.md pins ba4694a: supported artifacts/platforms, evidence per source revision (clean-checkout software lane: build/python/clippy pass, 2 of 1,978 tests parallel flakes passing on rerun; file benchmarks; black-hole drill; backup/restore as a separate qualification; 5B replay; soak; PD drills), evidence gaps (non-ECC hosts, unexplained heap aborts), compatibility/upgrades, limitations. deploy.md: systemd units as user vals with UMask=0077, 0700 per-unit runtime dirs, TimeoutStopSec=300, SPDK root guidance; copyable security checklist; stale black-hole lesson fixed. Units verified with transient systemd units and systemd-analyze. Readiness review gains an R0–R18 disposition table, original kept; ARCHITECTURE separates backup/restore from ingest. README/docs/README links stay in the working tree for 0505.

Commit: 0514: publish the evaluation revision (docs/evaluation.md), a security checklist and restricted systemd units; current disposition of the production-readiness review; separate backup/restore from ingest evidence

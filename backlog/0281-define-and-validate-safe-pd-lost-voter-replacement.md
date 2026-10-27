---
id: "0281"
title: Define and validate safe PD lost-voter replacement and membership repair
status: done
priority: 3
created: "2026-09-08"
updated: "2026-10-14"
depends_on:
    - "0112"
    - "0282"
spec_refs:
    - docs/deploy.md#PD availability and recovery
---

## Description

Accepted 2026-09-29, but blocked on commit-aware administration (0282) and the shared recovery/obsolete-identity fencing contract (0268/0283). First implementation should cover empty-state replacement under a surviving quorum; defer general address/voter-set redesign. Backlog acceptance does not choose an incarnation scheme or authorize a fleet repair.

0112 documents that a surviving two-of-three quorum tolerates one lost PD but there is no supported operator workflow for replacing a lost directory, changing peer addressing, or changing the PD voter set. Design and ship the smallest safe supported workflow without bootstrapping a rival group or wiping survivors.

Acceptance: explicitly distinguish intact-state relocation, empty-state voter replacement and membership/address changes; preserve group identity and committed authority; require fencing of obsolete replica identities; reject unsafe loss-of-quorum repair. Validate replacement, catch-up, interruption/retry, restart and restored redundancy. Document unsupported cases and coordinate with restore proposal 0268. No fleet mutation authorized by this proposal.

## Outcome

Empty-state PD voter replacement via Raft single-voter changes in the same group (incarnation kept): new pd_id started with `join = true` (never votes; fail-stops if its id is already a member), `pd-add-learner` commits it with its address (PD key V), conf changes force snapshots, `pd-replace <old> <new>` removes then promotes the caught-up learner (rerunnable, retargetable while half done; refuses leader, uninitialized learners, concurrent replacement, id reuse), `pd-remove-learner` for unreserved learners; restore-pd drops V. deploy.md distinguishes intact relocation, replacement runbook and unsupported cases. Local validation: vals-raft, vals-cluster, vals-server suites pass incl. tests/pd_replace.rs (3 valsd PDs + 3 nodes). No three-host run (0280).

Commit: feat(0281): replace a lost PD voter under the surviving quorum via raft membership changes (join = true learner, pd-add-learner, pd-replace, pd-remove-learner); new ids never reused; 3-PD valsd drill

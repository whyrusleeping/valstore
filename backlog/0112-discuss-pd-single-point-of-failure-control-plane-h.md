---
id: "0112"
title: Discuss PD single-point-of-failure / control-plane HA story
status: done
priority: 3
created: "2026-08-01"
updated: "2026-09-08"
depends_on: []
spec_refs: []
---

## Description
## Context

Raised in discussion (2026-08-01): the PD (placement driver / control plane) currently runs as a **single-replica Raft group** on one box (lab-1 in the box-validation cluster; `pd_id=1`, no `pd_peer`). That makes the PD host a **control-plane single point of failure**.

Current design position (spec.md §Vision; docs/deploy.md §Operational notes):
- The data path is leaderless and keeps serving reads/writes on the **last PD-pushed config** even while the PD is down — so a PD outage is **not** a data-path outage.
- But while the PD is down the cluster **cannot reconfigure**: no add/remove-node, no create/split/merge-zone, no failure-detector eviction (task 0017), no auto-split (0108) commit. And the PD's durable Raft state (`data_dir`) must survive the box.
- The documented production answer is a **3-replica PD Raft group** (`pd_peer` + raft `listen` port), which we do not currently run.

## Questions to work through
- How bad is a PD outage in practice, given the data path survives? Enumerate exactly what stalls (auto-split/merge, eviction, rebalancing, admin) and the blast radius / time-to-impact of each.
- What is the recovery story for a **lost PD host** (not just a restart)? Is the Raft `data_dir` on durable/replicated storage? Can a fresh PD re-bootstrap safely from node-persisted zone state, or is PD raft state authoritative and unrecoverable if lost?
- Should the lab box-validation cluster (and any reference deployment) run a **3-replica PD** by default? Cost is low (3 lightweight control daemons) and it removes the SPOF — is there a reason not to?
- Operational ergonomics: co-locate PD replicas with data nodes vs dedicated; how the runbook (`plans/bootes-soak-runbook.md`) and `provision-node.sh` should stand up a 3-PD group; how `admin` targets a raft group (any replica? leader only?).
- Any split-brain / stale-config risks if a partitioned old PD keeps pushing an old epoch after a new group forms.

## Outcome
Recorded three distinct-failure-domain PD voters as the production/reference default, with explicit non-HA dev/test exceptions and no fleet changes. Documented outage blast radius, conditional data-path continuity, authoritative PD-state recovery limits, admin semantics, and external fencing requirements in spec.md/docs/deploy.md. Corrected the full-voter-list configuration example after independent review; final reviewer accepted. Proposed follow-ups 0280–0283 cover provisioning/failover validation, lost-voter repair, admin UX, and cross-group fencing; 0268 received restore requirements. Verified source-backed semantics and task-scoped git diff --check; documentation-only, no code or hardware tests run. Initial documentation commit: 226b493d. Completion uses a path-scoped git commit because the workflow commit tool stages the entire tree and unrelated unaccepted 0278/0279 changes must remain uncommitted.

## Acceptance
- [x] SPOF blast-radius enumerated (what stalls, for how long) and recovery-from-lost-host story documented
- [x] Decision recorded on default PD replication for reference/box deployments
- [x] Any concrete follow-up work (3-PD provisioning, admin-to-raft-group UX, split-brain guards) filed as its own task(s)

## Acceptance criteria

## Plan

Audit current PD persistence, leadership/config push, recovery, and deployment guidance (including completed 0237). Record a bounded documentation decision: three independent PD replicas for production/reference HA, explicit single-PD test exception without fleet changes; enumerate outage stalls and conditional data-path availability; distinguish intact-state restart, single-replica loss in a surviving quorum, quorum/state loss, and unsafe rebootstrap. Document current admin/fencing limitations without claiming unsupported recovery. File concrete deployment/admin/safety follow-ups separately as proposed work and feed decisions into 0268. Review source-backed claims and documentation consistency; commit only this task's changes once unrelated work is safely separated.

### Starting points
- docs/deploy.md PD config reference, Administering a running cluster, Operational notes
- backlog/0237-make-raft-ready-persistence-ordering-fail-safe.md (done); backlog/0268-ship-versioned-restore-tooling-and-demonstrate-a-d.md (proposed)
- spec.md; plans/bootes-soak-runbook.md; plans/box-provisioning-runbook.md
- Unrelated staged 0278/0279 client-budget work is present; do not modify/stage/revert it.

## Work log
- Coordinator review scope: implementation is already in commit `226b493d`; review `git show 226b493d -- spec.md docs/deploy.md backlog/0268*`, not the unrelated staged task 0278 transport changes. Follow-ups filed as proposed 0280 (three-PD provisioning/gate), 0281 (lost-voter repair), 0282 (admin UX), and 0283 (cross-group fencing). No implementation or fleet authorization is implied.
- Owner-approved triage promotes the bounded decision/documentation task only. No fleet topology change or HA implementation is authorized by this promotion. Resolve lost-state recovery and reference-deployment/failover posture; complete 0237 failure handling before treating multi-replica PD deployment as the safety answer. Feed recovery decisions into proposed restore task 0268.
- 2026-09-08 implementer report: Implemented and committed task 0112 as `226b493d` (`Document PD HA and lost-state recovery posture`).  Changed: - Recorded three voting PD replicas on distinct failure domains as the production/refere
…[truncated]
- 2026-09-08 revision: Revised the task 0112 documentation only: - `docs/deploy.md`: corrected the three-PD example so every replica uses an identical full three-member `pd_peer` voter list, including its own `pd_id`; the `
…[truncated]

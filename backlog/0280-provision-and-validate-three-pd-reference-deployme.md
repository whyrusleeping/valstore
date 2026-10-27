---
id: "0280"
title: Provision and validate three-PD reference deployment and failover
status: done
priority: 3
created: "2026-09-08"
updated: "2026-10-21"
depends_on:
    - "0112"
spec_refs:
    - docs/deploy.md#PD availability and recovery
---

## Description

Follow-up to 0112's HA posture. Update provision-node.sh and reference/box/bootes configuration/runbooks for three voting PDs, preferably one per data host, with distinct durable directories and consistent peers/boot intent. Preserve an explicitly named single-PD performance-test mode. No live topology change or destructive operation is authorized by this proposal.

Acceptance: validate election, one-PD/host loss, restart, quorum loss/recovery and authoritative config-push continuity using source-pinned binaries; distinguish control-plane from data-quorum effects and record observed failover timing rather than promising an unmeasured SLO. Validate that every generated replica config has the identical full three-member `pd_peer` voter list, including its own `pd_id` with a reachable peer-advertised address, and treats `listen` separately as the local bind address. Document upgrade/rollback safety, secret/port setup, fencing and operator checks. Fleet execution requires explicit authorization.

## Outcome

Three voting PDs (one per lab host) validated under 2k writes/s RF3 load on valsd 63b8a1c2 (tree 5e9d8db9): election, follower restart (term kept), leader kill (new leader 0.97–1.30 s; zone pushed in 0.35–0.38 s), rolling restarts, quorum loss/recovery (mutations refused, data path unaffected), group restart, host loss (election 1.3 s); NQ_VERIFY ok. Fixed PD defects found: per-iteration raft ticks (presplit create-zone deposed the leader) → 100 ms ticks; campaign=true on every restart → empty data_dir only; lost first message on peer-closed raft connections → peek+reconnect (tests for the latter two). Added scripts/pd_topology.py generator/checker + tests, hw.py three-PD mode, drill/analyze scripts, provision-node.sh checks, docs/deploy.md three-PD section and runbook notes. Gaps proposed as 0495–0498.

Commit: feat(pd): validate the three-PD reference deployment on lab; fix tick timing, restart campaigns and stale raft connections (0280)

---
id: "0283"
title: Design cross-group PD authority fencing for restart and disaster recovery
status: done
priority: 3
created: "2026-09-08"
updated: "2026-10-14"
depends_on:
    - "0112"
spec_refs:
    - docs/deploy.md#PD availability and recovery
---

## Description

0112 documents that authenticated epoch checks are not cluster-incarnation fencing: equal epochs can be accepted and independently bootstrapped groups can advance. External stop/network fencing remains mandatory today. Assess a persistent authority/incarnation token or equivalent guard; do not pick an irreversible restore/token scheme without an explicit contract.

Acceptance: define safe bootstrap, normal leader failover, node restart and versioned restore semantics with 0268; reject pushes from obsolete independently bootstrapped authority, including equal/higher epochs, under the supported recovery procedure; test restart/restore and partitioned old-authority cases and document remaining external fencing requirements. Any protocol/persistence migration must have coordinated upgrade rules. No fleet operations authorized.

## Outcome

PD leader mints a random u128 into Raft key `I` and pushes nothing until it is committed. Every Set* push carries it as an optional trailer, which pre-0283 decoders ignore. Each node shard binds to the first id it receives, persisting it first (superblock v4 only while bound, or a file on the file backend). It then refuses other or missing ids at any epoch, counted in `vals_control_incarnation_rejected`. Offline `valsd reenrol --incarnation|--clear` (`--clear` rewrites both slots as v3). PD status shows the id. Docs: deploy/formats/pd-admin. Tests: wire, node restart/reenrol, persistence, withholding, rival group refused at a higher epoch, restore drill, PD restart and failover. Full suites pass and two reviewers accepted. The SPDK path has not been run on hardware.

Commit: feat(0283): cluster incarnation id fences data nodes against obsolete or independently bootstrapped PD groups; persisted per shard (superblock v4 / file), offline `valsd reenrol`, PD mints once and withholds pushes until committed

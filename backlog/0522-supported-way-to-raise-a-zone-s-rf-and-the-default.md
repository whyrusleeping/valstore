---
id: "0522"
title: Refuse a changed PD rf on a PD with committed state (in-place RF change deferred)
status: done
priority: 3
created: "2026-10-28"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description

Found while documenting the growth path (0509). A deployment that starts on one machine must use RF 1, and today no supported path raises RF later: catalog zones' RF is fixed at create-zone (no admin command changes it), and backup/restore requires the same PD `rf`, `vnodes` and zone RFs. docs/growing.md therefore tells users to create new RF 3 zones and copy data in the application.

Observation (local smoke test, gc_grace = 0s): editing the PD `rf` from 1 to 3 in all three PD configs (with data_node lines for all three members) and restarting the PDs made every node adopt `rf 3` for the ring (epoch bump) and anti-entropy spread the default zone's data. Nothing tests this transition, and with gc_grace > 0 old puts would not reach the new replicas (0517). The PD also silently accepts an rf change on restart.

Scope to decide: an admin command to change a zone's RF (and the ring RF) with validation, availability guarantees during the fill (reads at the new majority can miss keys not yet copied), interaction with 0517, and tests; or a guard that refuses a changed PD `rf` on a PD with committed state until such support exists.

Scope (decided 2026-10-29): the guard only. A PD whose committed state records a ring RF refuses to start with a different configured `rf`, with an error naming both values and pointing at docs/growing.md. A supported in-place RF change is deferred: it depends on 0517, and growing.md already documents the create-new-zones path.

Acceptance: a test that restarts a PD with a changed `rf` and expects the refusal (and an unchanged `rf` still starts); docs/growing.md mentions the refusal.

## Outcome

The PD leader writes its config rf once into committed state (key `G`, conditional write, batched with the incarnation `I` when both are absent; existing clusters record it on their next leadership). set_global_rf, run at both single and multi PD startup, refuses a configured rf that differs from the recorded one with "committed state records ring rf X, but its config sets rf = Y ... (docs/growing.md#replication-factor)". Restore keeps `G`. Test pd::tests::durable_pd_refuses_to_restart_with_a_changed_rf (fails with the guard disabled) shows refusal and that the unchanged rf reopens; restored_state test asserts `G` survives. docs/growing.md §Replication factor describes the refusal (and drops the stale gc_grace claim); docs/formats.md §3.6 documents `G`. vals-cluster + vals-server tests and clippy -D warnings pass; sol accepted.

Commit: 0522: PD records the ring rf (raft key G, minted with/after the incarnation) and refuses to start with a different configured rf; regression test, growing.md and formats.md

---
id: "0522"
title: Supported way to raise a zone's RF (and the default zone's ring RF) in place
status: proposed
priority: 3
created: "2026-10-28"
updated: "2026-10-28"
depends_on: []
spec_refs: []
---

## Description
Found while documenting the growth path (0509). A deployment that starts on one machine must use RF 1, and today no supported path raises RF later: catalog zones' RF is fixed at create-zone (no admin command changes it), and backup/restore requires the same PD `rf`, `vnodes` and zone RFs. docs/growing.md therefore tells users to create new RF 3 zones and copy data in the application.

Observation (local smoke test, gc_grace = 0s): editing the PD `rf` from 1 to 3 in all three PD configs (with data_node lines for all three members) and restarting the PDs made every node adopt `rf 3` for the ring (epoch bump) and anti-entropy spread the default zone's data. Nothing tests this transition, and with gc_grace > 0 old puts would not reach the new replicas (0517). The PD also silently accepts an rf change on restart.

Scope to decide: an admin command to change a zone's RF (and the ring RF) with validation, availability guarantees during the fill (reads at the new majority can miss keys not yet copied), interaction with 0517, and tests; or a guard that refuses a changed PD `rf` on a PD with committed state until such support exists.

## Acceptance criteria

## Work log

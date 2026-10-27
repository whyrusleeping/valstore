---
id: "0369"
title: '0367/0368 follow-ups: pending-I/O counter assert, PD restart detection for held nodes, error mapping, test robustness, AE-window liveness in harness'
status: done
priority: 3
created: "2026-10-02"
updated: "2026-10-11"
depends_on:
    - "0367"
    - "0368"
spec_refs: []
---

## Description

Minor items from the agent_5 review of 0367/0368 (2026-10-02). There are no data-loss findings.

## Outcome

Shipped the L0 items on files no other session had in flight, each with a test. - 1: SPDK pending-I/O underflow counter plus debug_assert (no saturation). - 4: the PD beacon also compares each endpoint's zone-catalog epoch (not persisted, restarts at 0), so a restart is caught even when a current SetMembers arrives first, including at range-map epoch 0. Tests fail without the check. - 5: a one-time PD error when committed maps are V4 but opaque maps are off. - 6: StaleEpoch(0) write refusals now give StaleRing, not QuorumFailed. - 8: the epoch-0 ack rule is explicit. - 9 (part): a direct test of the pusher_loop epoch-0 gate. - The vals-cluster lib suite passes. Items 3, 5 (docs), 7, 9 (rest), 10, 11, 12 and 13 move to 0425. Item 2 stays in 0418.

Commit: fix(0369): beacon catches restarts behind a current members push via catalog-epoch regression; StaleEpoch(0) writes surface StaleRing; SPDK pending-I/O underflow counter; explicit epoch-0 ack; rest split to 0425

## Hardware confirmation (0418, 2026-10-12)

- Item 2 was checked in 0418 (bench/0363-compact-values/R3-SESSION-0418.md) during a 15 min idle window with 8 recovery seeds active per node.
  - **Confirmed:** `vals_sweep_pass_age_seconds` peaked at 37/69/61 s (84 s on node2 over the whole hold), and `vals_sweep_passes` advanced on every node.
  - Item 11: `vals_ae_sync_errors` rose 0→223/16/61. No reason is logged; that logging is 0426's item.
  - Item 12: the restart-to-`recovery_seeding` gap was ≤58 s, at 30 s sample resolution.

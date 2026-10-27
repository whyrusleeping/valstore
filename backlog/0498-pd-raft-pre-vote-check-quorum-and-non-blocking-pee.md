---
id: "0498"
title: 'PD Raft: pre-vote/check-quorum and non-blocking peer sends for rejoining and unreachable voters'
status: done
priority: 4
created: "2026-10-21"
updated: "2026-10-27"
depends_on: []
spec_refs: []
---

## Description

Found by the 0280 drills (bench/0280-three-pd/FINDINGS.md, 2026-10-21), after 0280 fixed tick timing, campaign-on-restart and peer-closed cached connections.

- After its host rebooted, PD 1 rejoined but heard nothing from the leader for ~1.2 s (the leader's and follower's cached TCP connections to the old host only fail when TCP retransmits hit the new kernel), campaigned at terms 51 and 52, and caused a leader change 3 → 2.
- A lone survivor without quorum raised its term 18 → 49 in 48 s; the returning voters adopt it.
- `send_raft` runs on the drive loop with a 250 ms connect timeout, so every message to an unreachable voter blocks the loop for up to 250 ms.

Candidate fixes: raft-rs `pre_vote` + `check_quorum` (existing tests that force leadership with `campaign()` would need leader transfer instead), TCP_USER_TIMEOUT or a per-peer sender thread so sends never block the drive loop. Acceptance: hardware host-loss drill where the rejoining voter does not depose a healthy leader, the drive loop never blocks on an unreachable peer, and failover stays ≈1–1.3 s.

## Outcome

lab A/B (bench/0498-pd-prevote/FINDINGS.md): - Leader-host reboot: before, the rejoining PD ran terms 57–62 and left no leader for 4.7 s. After, it followed the leader 0.27 s after start, with no election. - Raft-port blackhole: admin-status p50 went from 205 ms (leader deposed) to 6.5 ms (leader kept). - Failover median 1.25 s in both arms. - Quorum-loss survivor's term stayed flat. - New raft and send tests; vals-raft, vals-cluster and vals-server suites pass; deploy docs updated. - Cluster left on 41d0f973.

Commit: fix(0498): PD raft pre-vote + check-quorum, per-peer bounded sender threads and 1 s TCP_USER_TIMEOUT; rejoining/cut-off voters no longer depose the leader and unreachable peers no longer stall the drive loop

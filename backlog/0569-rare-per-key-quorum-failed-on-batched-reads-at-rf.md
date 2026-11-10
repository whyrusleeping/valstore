---
id: "0569"
title: Rare per-key quorum_failed on batched reads at RF 1 with no faults (netprogress multiread, cloud v1)
status: proposed
priority: 3
created: "2026-11-10"
updated: "2026-11-10"
depends_on: []
spec_refs: []
---

## Description
Evidence: bench/cloud/runs/v1-20261110 (task 0567): single node, RF 1, no fault injection, colocated netprogress client. multiread (read_many_in, batch 8) reported per-key `quorum_failed` 40 of 1.37M calls (closed loop) and 22 of 454k (open loop at 50%); point reads and writes had none, every value verified, and no server error/busy/reject/admission counter moved. Find which client path turns a per-key result into QuorumFailed without a server error (deadline? a partial batch reply? transport credit?), and whether the same happens at RF 3. Also fix vals-bench netqueue::parse_peers reading a lone sharded node `1@a,b` (no ';') as several nodes.

## Acceptance criteria

## Work log

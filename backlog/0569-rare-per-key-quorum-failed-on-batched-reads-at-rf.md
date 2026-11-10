---
id: "0569"
title: Rare per-key quorum_failed on batched reads at RF 1 with no faults (netprogress multiread, cloud v1)
status: done
priority: 3
created: "2026-11-10"
updated: "2026-11-10"
depends_on: []
spec_refs: []
---

## Description

Evidence: bench/cloud/runs/v1-20261110 (task 0567): single node, RF 1, no fault injection, colocated netprogress client. multiread (read_many_in, batch 8) reported per-key `quorum_failed` 40 of 1.37M calls (closed loop) and 22 of 454k (open loop at 50%); point reads and writes had none, every value verified, and no server error/busy/reject/admission counter moved. Find which client path turns a per-key result into QuorumFailed without a server error (deadline? a partial batch reply? transport credit?), and whether the same happens at RF 3. Also fix vals-bench netqueue::parse_peers reading a lone sharded node `1@a,b` (no ';') as several nodes.

## Outcome

Cause found in code: serve_mixed_batch_read answered per-key Busy (no counter) when a flush/merge/reclaim commit or newer write changed the key during its value device read, and the client mapped per-key Busy to QuorumFailed (no retry). Server now re-plans/re-reads up to 8 attempts (Busy only if a re-plan exceeds the key's reserved reply bytes); all read paths (batched digest/quorum/all, point value/digest incl. winner fetch) return CoordError::Busy when Busy replicas explain the shortfall; FD scores Busy reads alive; new vals_value_read_restarts/vals_value_read_busy counters; RF 3 analysis in work log; parse_peers fixed; docs/formats.md updated. Unit regressions fail without the fix; vals-client/vals-bench/vals-cluster lib + netsim/wire_fuzz pass, clippy clean. Not reproduced or verified on SPDK hardware (file backend reads are synchronous, so the race cannot occur locally).

Commit: 0569: mixed batch reads re-read values changed mid-read instead of per-key Busy; client reads report CoordError::Busy not QuorumFailed; read restart/busy counters; parse_peers lone sharded node

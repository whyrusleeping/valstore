---
id: "0326"
title: Node shutdown exceeds 300 s at large store size (waits on in-flight maintenance after shards drain) → SIGKILL
status: done
priority: 3
created: "2026-09-28"
updated: "2026-09-29"
depends_on: []
spec_refs: []
---

## Description

## Description
## Evidence

Nodes did not exit within `TimeoutStopSec=300` and were SIGKILLed:
- SOAK0321-F4: node1.
- F4 re-verify: node1 and node2 (node3 took 227 s).
- F2: 96–110 s.

In the node logs, every shard reports "graceful shutdown complete (engines synced)" within about 1 s of SIGTERM. The process then stays up for minutes, most likely waiting for an in-flight off-core maintenance job (a large merge; `vals_merge_in_flight` was 1–2) or seed work. At 1.4B keys per node, a merge can run for minutes.

A SIGKILL leaves SPDK hugepage maps behind and forces crash recovery on the next start. It also makes every harness stop slow.

## Candidate

Make in-flight merge and maintenance jobs cancellable at shutdown. Merges are restartable: abandon the output and keep the inputs. Otherwise, bound the wait.

## Acceptance

- Stop completes in under 30 s under load at about 1B keys per node, with a clean reopen and no data loss (verify).
- Unit tests for the cancellation paths.

## Acceptance criteria

## Outcome

Shutdown now cancels the maint pool after shards join. Queued jobs are dropped, and in-flight merges stop at chunk boundaries (EngineError::Cancelled); their output is uncommitted and reclaimed as orphans. Unit tests plus an ignored randomized cancel/reopen stress test cover this. Hardware (bench/0326-shutdown/FINDINGS.md), 3-node RF2 with the full 2.09B-KV cache (~1.39B KV/node), stopped ~3 s after the last write with 2 merges in flight per node: 10.7/18.2/12.3 s against F4's 123–300 s plus SIGKILL, later stops 1–6 s, clean reopen. Cursor, samples and exact records and backlinks census (1,090,186,118 / 207d92db) all matched. One node1 ValuePtr.len bit flip, not caused by 0326, is filed as 0336; SPDK map leftovers as 0337.

Commit: maint(0326): cancel queued/in-flight maintenance at node shutdown; hardware gate (stops 1–18 s vs 123–300 s SIGKILL at ~1.39B KV/node) + exact census

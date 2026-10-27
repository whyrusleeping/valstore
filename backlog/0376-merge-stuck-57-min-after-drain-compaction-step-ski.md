---
id: "0376"
title: 'Merge stuck ~57 min after drain: "compaction step skipped — 1 in-flight read(s) did not drain" (RECOV01-OFF-R2 node2)'
status: done
priority: 2
created: "2026-10-04"
updated: "2026-10-06"
depends_on: []
spec_refs: []
---

## Description

From RECOV01-OFF-R2 (`bench/0363-compact-values/RECOV01-FINDINGS.md`, 3 nodes, `ae_seed_isolation=0`, binary 9b36a510… = master 90a3db95 + 0374):
- During the post-drain settle, node2 held one merge in flight (13.6 GB input) with no progress for ~57 min.
- It logged `compaction step skipped — 1 in-flight read(s) did not drain`; at shutdown it reported "1 merges cancelled".
- Correctness passed on the unsettled cluster.

**Suspects:**
- an in-flight async read that never completes or is never reaped on a maintenance qpair (cf. 0368 for shard threads);
- a read-gate/drain accounting leak in the streaming merge.

**Investigate:**
- the code path that logs this;
- whether the maintenance thread polls its qpair while waiting for the drain;
- the raw logs/metrics in `/data/dataset/task0363-results/RECOV01-OFF-R2/`.

**Then:** fix, with a test using a device that completes reads only when polled.

## Outcome

The RECOV01-OFF-R2 node2 merge was not stuck: cleanup rewrote the whole engine (13.6 GB, ~7 min) to forget ≤85 keys per pass, dropped keys released meanwhile, and logged nothing. The read-drain warning was incidental. Fix: released keys accumulate per engine, tagged with the placement fingerprint and separate from the in-flight plan. One forget plan is issued at cycle wrap or ~32 MiB. Zones stay dirty while keys are pending, and the read gate is taken only when a batch is due. A placement change discards the batch, and no-run engines revalidate before forgetting. New: an info log and the cleanup_forget_compactions/keys/discards counters. Tests cover convergence, the cap, the placement fence, newer-write survival, in-flight batching and a poll-completed device. All 781 vals-cluster lib tests pass in debug; 2/2 comprehensive reviewers accepted. A declined plan now counts as a discard and keeps the zone dirty. Hardware confirmation is left to 0386.

Commit: fix(0376): batch cleanup forgets — one forget compaction per cycle wrap or ~32 MiB of released keys, not one whole-engine rewrite per ≤85-key pass; log + counters (RECOV01-OFF-R2 "stuck merge")

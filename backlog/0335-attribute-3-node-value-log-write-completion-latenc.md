---
id: "0335"
title: Attribute 3-node value-log write-completion latency (~300 µs/stall vs ~35 µs on 2 nodes) with the 0334 counters
status: done
priority: 2
created: "2026-09-28"
updated: "2026-10-06"
depends_on: []
spec_refs: []
---

## Description
Follow-up from 0334 (bench/0334-sync-spin/FINDINGS.md).

## What 0334 found
- The three-node shard µs/write deficit is time spent waiting on device write completion in the sync path:
  - about 0.9 ms of synchronous drain plus partial-tail write per sync;
  - append queue-full stalls of about 300 µs each, against about 35 µs on two nodes;
  - busy-polling while the domain FLUSH is in flight.
- A cooperative (non-spinning) sync-prepare lost 5–27% of throughput in a matched ABBA, so the limit is device latency, not shard CPU.
- Why write completions are about 10× slower per stall on three nodes is unexplained.

## Work
1. Run one two-node D16 reference with a master build that includes the 0334 counters, matched to LONG01-D13 / F1 if possible, to get per-sync drain/tail ms, domain FLUSH latency and busy-poll share.
2. Test whether value-log writes queue behind in-flight NVMe FLUSHes: for example, count writes submitted while a domain flush is in flight against their completion latency, or run a short single-node SPDK microbench of write latency with and without a concurrent FLUSH.
3. Propose, but do not implement, a remedy. Candidates: fewer and larger syncs per node, or grouping flushes across zones.

## Acceptance
- A measured comparison of write-completion and FLUSH latency between two and three nodes, with a supported explanation of the gap.


## Acceptance criteria

## Work log

- 2026-09-29 backlog triage: highest-priority performance investigation once hardware gates clear (P2). Keep to the matched reference and one discriminating write/FLUSH experiment; no remedy implementation or revival of the rejected cooperative-sync candidate without new evidence. Requires a trustworthy platform (0248/0250 disposition or qualified alternative) as well as explicit retention/reuse approval. Completion of 0336 alone does not release the drives.

- 2026-09-29 (coordinator, unattended): BLOCKED on an owner retention decision. This run needs a fresh destructive provision of the lab scratch 9100 PRO drives (vfio-bound: lab-1 81:00.0, lab-2/3 01:00.0). Those drives currently hold the retained SHUT0326-F2-R2 data, which is the only evidence for 0336 (node1 ValuePtr.len 0x800 flip). 0336 says to decide retention before the next provision. Any one of these unblocks it: (a) the owner authorizes erasing R2, optionally after capturing node1's evidence; (b) 0336's on-drive investigation is accepted and completed first; (c) other scratch drives are provided. Then set this task back to todo.
- 2026-09-29: the owner **released the retained SHUT0326-F2-R2 drives** after 0336's read-only node1 probe; receipts are archived on devbox. The retention blocker is cleared, so destructive reprovisioning of the lab scratch drives is allowed for this run. The platform-health gate from triage is still open: the 0248/0250 disposition, now informed by 0336 FINDINGS §9 (offline Memtest86+ per box, or validated ECC SO-DIMMs on the ECC-capable 285HX SKU).
- 2026-09-30 (coordinator, from 0348): the premise is probably wrong. The ~300 µs stall appears on **2-node** lab-1+3 too (about 0.75 stall-s/s and 11 µs/write on master and on the F1 server), and a whole-namespace blkdiscard cut per-stall waits 8.2–8.5× and raised ACK by 20–24% (bench/0348-scale/FINDINGS.md, 'Drive deallocation discriminator'). The drives' NUSE was 69–73%, because vals never deallocates. The 3N-vs-2N gap in 0327/0334 likely reflects the drive FTL state at the time of each run, not topology. Fix and A/B: 0350. Re-scope or close this task after 0350's hardware A/B.
- 2026-10-06: closed as **superseded** (backlog cleanup, owner-approved). The 3-node vs 2-node premise was wrong: drive FTL state explains the stall (0348/0350). Domain-flush attribution was done in 0373/DRIVE02 (per-command flush cost; post-step rate ≈ device GB/s ÷ bytes per ACK). 3-node runs since then reach 1.5–1.7M ACK/s before the drive step. Any remaining device-wait share is measured in the write-throughput decay task.

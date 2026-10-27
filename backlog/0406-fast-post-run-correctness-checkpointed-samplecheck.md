---
id: "0406"
title: 'Fast post-run correctness: checkpointed samplecheck derive for the sealed cache + run counts/aediag/sample Gets in parallel'
status: done
priority: 2
created: "2026-10-09"
updated: "2026-10-12"
depends_on: []
spec_refs: []
---

## Description
## Why
Correctness after a run takes 1–4 h. Most of that time is samplecheck's derive, which replays the sealed cache through the bootes encoder up to the saved cursor to rebuild the expected counts and samples:
- 1,988 s at 2.4B;
- 3,341 s at 3.88B;
- ~75–90 min for the full cache (SOAK5B-V5-README "R3 outcome").

Zone counts and owned aediag only start after samplecheck finishes, so the run is serial. The cache is sealed and the encoder is pinned, so derive is deterministic. Its result for a given (cache, encoder, `--every`) can be computed once and reused for every attempt.

## Scope
1. **Derive checkpoints** in `bench/0363-compact-values/samplecheck`:
   - A `derive-checkpoint` subcommand replays the cache once and writes snapshots every N source events (e.g. 250M). Each snapshot holds the full sink state: lastSeq, events, cumulative per-zone counts, and the sample map with values, presence, seq and superseded counts.
   - The `check` command takes a `--checkpoints <dir>` option. It loads the newest snapshot at or below the cutoff, then replays only the delta from that snapshot's seq. This needs the ingester's cache replay to start at a mid-cache cursor: the sink's `LoadCursor` returns the snapshot seq. First confirm that bootes' replay has no cross-event state that a mid-cache start would lose; deletes and bad-delete detection are the obvious candidates. If it does have such state, include that state in the snapshot.
   - Every snapshot records the cache manifest hash, encoder identity (bootes commit / samplecheck binary SHA), `every` and schema, and is refused on any mismatch.
2. **Equivalence check.** For at least two cutoffs (one mid-cache and the whole cache), the checkpointed derive must give byte-identical counts and sample sets to a from-zero derive. Add it as a Go test on a small synthetic cache, and run it once on the real 5B cache.
3. **Parallel checks in the harness** (run_full1.py post-drain phase). Exact counts need only the derive output, not the sample Gets. Order the steps as:
   1. derive, now fast;
   2. exact zone counts;
   3. sample Gets and owned aediag run concurrently;
   4. any remaining checks.

   Check that aediag and sample Gets running together do not distort each other's bounds, and keep the existing time bounds for each check.
4. **Optional:** export the derived per-zone counts at every checkpoint seq so that resume (0403) and from-corpus arms (0404) can check intermediate counts cheaply.

## Acceptance
- On the real 5B cache, derive for a near-EOF cutoff takes under 10 min with checkpoints, against ~75 min today, with an identical receipt.
- The runner uses the checkpoints by default for SOAK5B-family arms, records the checkpoint identity in its receipts, and falls back to a full derive when no checkpoint matches.
- Tier **L0/L1** for checkpoint identity, equivalence, runner wiring and real-cache derive timing. Keep the two-cutoff real-cache equivalence check: it directly validates this task's optimization, but requires no hardware ingest.
- End-to-end hardware timing is a shared receipt, not a new campaign. Reuse 0401's R3 confirmation if it runs the changed checkpointed derive and parallel sample Gets/aediag paths with sufficient timing/identity receipts; otherwise measure on the next already-planned eligible run. Record pending or failed timing honestly. No dedicated reopen/replay or duplicate R3 post-drain suite is required to close the software work.
- 0401's completed confirmation fell back to a full derive because its checkpoint selection used a legacy cache path. Its concurrent Gets/aediag receipt can inform parallel-check timing, but cannot establish checkpointed derive speed. Use the selection receipt and cache/checkpoint identities, not just the samplecheck binary version, to identify eligible evidence.
- The Go tests and runner unit tests are green.

## Acceptance criteria

## Work log

- 2026-10-10: the end-to-end hardware timing receipts come from 0418: parallel Gets+aediag in step 3 and checkpointed derive at EOF in step 6. Software closure is not gated on 0418.

## Outcome

Checkpointed samplecheck derive with refusal/fallback paths and equivalence tests. On the real 5B cache: R3 cutoff 4,302 s → 145 s, R2 cutoff 1,865 s → 275 s, whole cache 4,413 s → 8.8 s, with identical receipts and byte-identical exports (SAMPLECHECK-CKPT-README.md). The runner selects checkpoints by default for SOAK5B-family arms, records their identity and falls back to a full derive. End-to-end hardware timing receipts are pending in 0418 steps 3 and 6; software closure is not gated on them.

Commit: feat(0403,0406): resumable SOAK5B-V5 harness (resume/resume-check, refusal policy, ledger carry, runbook) + checkpointed samplecheck derive (R3 cutoff 4302 s -> 145 s, identical receipts); hardware receipts pending in 0418

## Hardware confirmation (0418, 2026-10-12)

- Hardware receipts from 0418 (bench/0363-compact-values/R3-SESSION-0418.md).
  - Pre-ingest at the R3 cursor: the checkpointed derive took 114 s; the parallel Gets took 1,153 s and passed 10,172,125/10,172,125, with aediag running beside them.
  - At EOF: the derive took 80 s, overlapping the 601 s AE window. Gets ran from 601 to 1,343 s (740 s) and passed 10,407,283/10,407,283. The owned aediag ran to its 3,600 s bound, so the post-drain checks took 4,203 s end to end.
  - **Confirmed**, with no checkpoint fallback.

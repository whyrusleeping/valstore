---
id: "0123"
title: Make bootes vals shutdown bounded and loss-aware
status: done
priority: 3
created: "2026-08-02"
updated: "2026-08-04"
depends_on:
    - "0121"
spec_refs:
    - bench/bootes-soak-lab/run-20260802T184023Z/FINDINGS.md
    - plans/bootes-soak-runbook.md#7. Stop (graceful)
---

## Description
Both the prior offline-cluster process and the fresh 2026-08-02 soak's live-cluster process failed to exit within 30s/120s after SIGTERM and required SIGKILL. During the fresh run (`bench/bootes-soak-lab/run-20260802T184023Z/`), shutdown continued draining long-running backfills and retrying vals batches for over two minutes, then was killed before a clean completion marker. This makes soak automation unreliable and can lose buffered records/cursor state.

Add a bounded shutdown protocol for vals ingest: stop admitting new backfills, cancel or bound upstream work, flush/persist buffers and cursor with an explicit deadline, report any remaining/lost work, then exit. Hard failures should not make graceful shutdown hang indefinitely.

## Acceptance criteria
- SIGTERM makes `bootes ingest --vals` stop accepting new work immediately and exit within a documented bounded deadline under both healthy and unavailable cluster conditions.
- Buffered vals batches and cursor state are flushed when possible; deadline expiry emits quantitative remaining/loss diagnostics.
- Tests exercise shutdown with in-flight backfills and retrying/unavailable vals writes.

## Plan

Implement a single-deadline, loss-aware shutdown path for `bootes ingest`, focused on the vals backend. Add a documented/configurable shutdown timeout (with a safe bounded default) and have first SIGTERM immediately cancel firehose/repo-pump admission, start shutdown under that one deadline, and retain second-signal force-exit behavior. Refactor ingester shutdown so backfiller stop, sink drain, and final cursor persistence cannot each extend the total bound; report stop/timeout errors instead of silently continuing. Extend `ValsStore` with context-aware close/drain behavior: reject and count writes arriving after close begins, make Busy retry/backoff interruptible by the shutdown deadline, account quantitatively for buffered/in-flight/persisted/dropped/rejected records and backlinks, and emit a final structured summary. Preserve cursor safety by persisting the final firehose cursor only after the vals data drain succeeds; on deadline/failure, explicitly report that cursor persistence was withheld or failed. Add deterministic tests using fakes/hooks for (1) shutdown while backfill work is in flight, (2) unavailable/Busy vals writes reaching the deadline without hanging, and (3) healthy drain plus cursor ordering/accounting. Update the valstore bootes soak runbook with the deadline, expected shutdown marker/diagnostics, and operational command/wait guidance. Run the bootes Go test suite (including race-relevant focused tests where practical), build the CLI, and validate the valstore documentation diff. Commit code in the bootes repository and task/runbook state in valstore without including unrelated dirty files.

### Starting points
- Bootes code: /home/user/code/bootes/main.go signal flow and ingest/ingest.go Close currently has a 5-minute backfiller wait.
- Vals sink: /home/user/code/bootes/ingest/valsstore.go has unbounded Busy retries, contextless Close, and all sink roles alias one store.
- Interfaces: /home/user/code/bootes/ingest/store.go; existing tests: ingest/valsstore_test.go.
- Docs: plans/bootes-soak-runbook.md §7; evidence: bench/bootes-soak-lab/run-20260802T184023Z/FINDINGS.md.
- Both trees have unrelated dirty/untracked files; do not modify, stage, clean, or commit them.

## Work log
- 2026-08-04 plan: Implement a single-deadline, loss-aware shutdown path for `bootes ingest`, focused on the vals backend. Add a documented/configurable shutdown timeout (with a safe bounded default) and have first SIGT
…[truncated]
- 2026-08-04 context hints: 5 recorded with plan
- 2026-08-04 context hints: Primary implementation repo is sibling /home/user/code/bootes (git repo); docs/task repo is /home/user/code/valstore. You are authorized to edit both for task 0123.; Bootes code: /home/user/code/bootes/m
…[truncated]
- 2026-08-04 implementer report: Implemented task 0123 across bootes and valstore without committing.  Bootes changes: - Added configurable `--shutdown-timeout` / `ATTIE_SHUTDOWN_TIMEOUT` (30s default), immediate admission cancellati
…[truncated]
- 2026-08-04 review tier: high-powered — reviewers: sol
- 2026-08-04 review (sol): revise — The implementation adds the shared configurable deadline, vals accounting, structured shutdown logs, cursor-after-drain ordering, documentation, and focused tests; the Go suite, race-focused ingest te
…[truncated]
- 2026-08-04 revision: Addressed all shutdown review findings without committing.  Changes made: - Main now creates the single shutdown context at first signal, stops callback admission, cancels Run, and waits for `Run`/the
…[truncated]
- 2026-08-04 review (sol): accept — The revision addresses the prior blocking issues. Shutdown now closes producer admission first, waits for the firehose scheduler and tracked callbacks before draining sinks, bounds backfiller stop and
…[truncated]
- 2026-08-04 usage: 10,012,061 tok (in 940,249, out 72,644, cache_r 8,999,168, cache_w 0) · cost n/a (unpriced)
  implementer: 6,763,035 tok (in 385,191, out 42,612, cache_r 6,335,232, cache_w 0) · cost n/a (unpriced)
  coordinator: 2,444,890 tok (in 354,592, out 10,554, cache_r 2,079,744, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 804,136 tok (in 200,466, out 19,478, cache_r 584,192, cache_w 0) · cost n/a (unpriced)

---
id: "0128"
title: Bound bootes/indigo streaming-backfill memory amplification
status: done
priority: 2
created: "2026-08-04"
updated: "2026-08-04"
depends_on: []
spec_refs: []
---

## Description
The 2026-08-04 post-qpair bootes soak found the producer, not valstore, is now limiting. At `--parallel-backfills 300`, bootes reached 92.3 GB RSS in 5m41s. Heap profiles attributed 30.4 GB / 83% to indigo `repo/stream.go`'s 1 KiB `smallBlockPool` buffers retained through `readStreamBlockstore.otherBlocks` while many repository MST walks remain active. A fresh A/B at 50 parallel backfills sustained essentially the same backfill-record rate (~22.6k/s vs ~22.0k/s) with 69% lower RSS and 28% lower CPU. Full evidence: `bench/bootes-soak-lab/run-20260804-post-qpair/FINDINGS.md`.

Implement a bounded-memory producer configuration/fix in bootes/indigo. Near-term guardrail: reduce or memory-bound repository concurrency. Underlying fix should promptly recycle retained CAR block buffers and/or avoid one dedicated pooled 1 KiB backing allocation per unmatched retained block.

## Acceptance criteria
- 300-vs-bounded-concurrency A/B is reproducible from a fresh cluster/corpus.
- Bootes RSS reaches a stable bound under a sustained soak rather than growing toward host memory.
- Throughput does not materially regress from the ~22k backfill records/s observed at 50-way concurrency.
- Heap profile no longer attributes the dominant retained heap to `repo.init.func2` / `smallBlockPool` through `readStreamBlockstore.otherBlocks`.
- Runbook default and operational memory guidance are updated.

## Work log
- 2026-08-04: Fixed the ineffective pool return (`cap`, not sliced `len`), added deferred cleanup, and moved unmatched CAR blocks from heap to per-stream temporary spill files. Added `ParallelDownloads` so 300 active jobs can be retained while only 64 downloads/CAR walks carry parser state.
- 2026-08-04: Fresh 300/64 hardware arm stayed at 4.2 GiB RSS at 3m12s and 6.3 GiB at 8m07s, versus 92.3 GiB at 5m41s before the fix. Goroutines fell from ~12.9k to ~3.1k; the final heap no longer contained `repo.init.func2`/`smallBlockPool`; all three vals nodes had zero server errors and zero Busy.
- 2026-08-04: Findings and artifacts are in `bench/bootes-soak-lab/run-20260804-stream-memory-fix/`; runbook default is 300 jobs / 64 downloads with `GOMEMLIMIT=64GiB` and temporary-disk guidance. Indigo patch is shipped in bootes' `indigo-stream-fix.bundle` because the available GitHub identity cannot push upstream; `scripts/build-with-indigo-fix.sh` reproducibly checks it out, tests, and builds.

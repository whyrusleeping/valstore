---
id: "0162"
title: 'Backport check: audit for on-hardware fallout of the 0160 orphan-sweep memtable-dangling corruption (fixed in 0161 session)'
status: done
priority: 3
created: "2026-08-09"
updated: "2026-08-10"
depends_on: []
spec_refs: []
---

## Description
While building the 0161 repro, a real corruption bug in the 0160 orphan sweep was found and fixed: `Engine::referenced_segments()` did not include memtable-referenced value segments, only runs + manifest + open seg + vlog tail. Because vlog epochs are per-SEGMENT and flush covers `next_epoch-1` (the still-open segment's epoch), post-flush puts land in a segment already pruned from the tail; once the adopting run's reference is dropped (e.g. single-run aged-vlog GC rewrites it away, or an LWW fold supersedes it), the memtable entry is the sole referencer and the plan-time orphan sweep frees the segment — dangling `ValuePtr` → `Log(Corrupt("bad magic"))`/CRC errors on read/rewrite, or silent wrong data after segment reuse. Repro hit it reliably within seconds under an overwrite-heavy fill.

Exposure window: HEAD 9b2fbee (2026-08-09, 0160 landing) until the 0161 fix commit; the 2026-08-09 soak arms ran this code on lab.

Acceptance criteria:
- Grep preserved soak/replay logs (bench/bootes-soak-lab/run-20260809T*, on-box journals if still present) for "bad magic" / "crc mismatch" / corrupt-read signatures and record whether any hardware run was affected.
- Decide whether any preserved corpus/dump from those arms needs invalidation.
- Note the finding in ARCHITECTURE.md/docs if operator guidance changes (likely not — fix ships before any release).

## Acceptance criteria

## Plan

Audit for on-hardware fallout of the 0160 orphan-sweep memtable-dangling corruption (exposure window: commit 9b2fbee up to fix commit c9fd679).

1. Enumerate exposed runs: the 2026-08-09 arms run-20260809T092233Z-post0159-0160 (built at 9b2fbee), run-20260809T110159Z-vlog-gc, run-20260809T122519Z-floor-gc, run-20260809T134528Z-fold-gc (working-tree builds containing the 0160 sweep, pre-fix). Confirm from each run's FINDINGS/notes which build it used. run-20260809T063307Z-post0157 and earlier predate 0160 (no orphan sweep → not exposed); run-20260809T163755Z-0161-shadow onward carry the fix.
2. Grep all preserved artifacts under bench/ (esp. bench/bootes-soak-lab/run-20260809T*) for corruption signatures: "bad magic", "crc mismatch", "Corrupt(", "CRC", "checksum", "corrupt" (case-insensitive), plus server error counters in FINDINGS/metrics snapshots. Also check whether on-box journals are still reachable (timeout-wrapped ssh per runbook); if not, record that the box logs are unavailable and rely on preserved artifacts.
3. Assess corpus impact: determine whether any preserved corpus/dump was produced by an exposed build (e.g. anything the 0132 replay corpus depends on — verify the 08-05 corpus predates 0160) and record whether invalidation is needed.
4. Write the finding into the task work log (via finish/commit) and, only if operator guidance changes, touch ARCHITECTURE.md/docs — expected outcome is "no release shipped the bug; no doc change".
5. Commit (backlog state only, unless docs change).

### Starting points
- bench/bootes-soak-lab/run-20260809T092233Z-post0159-0160/
- bench/bootes-soak-lab/notes.md
- exposure: 9b2fbee..c9fd679; fix = Index::for_each_value_segment in referenced_segments + compact_commit keep set
- signatures: Log(Corrupt("bad magic")), crc mismatch, silent wrong data after segment reuse

## Work log
- 2026-08-10 plan: Audit for on-hardware fallout of the 0160 orphan-sweep memtable-dangling corruption (exposure window: commit 9b2fbee up to fix commit c9fd679).  1. Enumerate exposed runs: the 2026-08-09 arms run-2026
…[truncated]
- 2026-08-10 context hints: 4 recorded with plan
- 2026-08-10 review tier: simple (coordinator self-review)
- 2026-08-10 decision: accept — commit: 0162: audit CLEAN — no on-hardware fallout from the 0160 orphan-sweep memtable-dangling corruption (fixed in c9fd679). Exposure window 9b2fbee..c9fd679 = the four 2026-08-09 soak arms (post0159-0160
…[truncated]
- 2026-08-10 usage: 15,313 tok (in 52, out 15,261, cache_r 1,570,503, cache_w 60,977) · cost n/a (unpriced)

---
id: "0197"
title: Move hash-memtable maintenance snapshots off the serving core
status: done
priority: 2
created: "2026-08-12"
updated: "2026-08-13"
depends_on: []
spec_refs:
    - docs/out-of-core-index.md
    - bench/lab-breakthrough-20260812-0195/FINDINGS.md
---

## Description
Investigate the 8–12% serving CPU plus maintenance-overlap throughput/tail loss attributed to HashIndex::maintenance_page during flush snapshot preparation. Prototype a bounded, generation-safe immutable snapshot path, measure it on lab, and ship it only if it improves the end-to-end result without correctness or resource regressions. A measured rejection with the production tree restored is an acceptable outcome; capture a follow-on design when the experiment identifies a structural blocker.

Acceptance criteria:
- Capture phase-aligned throughput and p99 against flush/merge metrics to quantify the collapse before design.
- Prototype a bounded, generation-safe path with kill-the-fix interleave tests.
- Measure throughput/p99 during flush and merge windows together with RAM, write amplification, recovery, and correctness on lab.
- Ship only a passing candidate; otherwise revert it, retain reproducible evidence, explain the blocker, and capture a concrete follow-on.

## Acceptance criteria

## Plan

1. Establish the pre-change collapse with a short, repeatable lab control run that samples client throughput/latency together with flush/merge activity and plan-phase histograms; retain raw evidence and summarize phase-aligned windows before choosing the implementation.
2. Replace unsorted/hash-memtable serving-core page traversal with a bounded immutable handoff: atomically rotate the active hash memtable into a frozen generation at flush start, keep that frozen generation visible to reads and protected from reclamation while the active generation accepts writes, and let the maintenance worker perform ordered traversal, key cloning, and flush-run preparation. Keep the existing cooperative path where needed for sorted indexes, and avoid adding steady-state per-write cloning or unbounded duplicate key storage.
3. Preserve durability and no-resurrection semantics: establish covered seq/epoch only after the generation handoff, flush the corresponding value-log tail before publishing work, prefer the active generation over frozen data during reads, and retire only the exact frozen generation after durable manifest commit. Make cancellation/submission/error paths release or restore state safely.
4. Add kill-the-fix interleave tests covering overwrite/new-key/delete behavior between handoff and commit, reads while the off-core snapshot is in flight, exact generation retirement, value-segment pinning, worker failure/cancellation, and recovery. Add a liveness witness proving ordered traversal/key materialization occurs on the maintenance worker rather than the serving executor.
5. Run targeted vals-store/vals-cluster tests plus broader affected suites without cargo fmt (the tree is not rustfmt-clean). Then rebuild and run matched lab candidate arms, capturing the same phase-aligned throughput/p99 and maintenance metrics, plus RSS, run-write amplification, recovery/read validation, and profiles demonstrating that HashIndex::maintenance_page is absent from serving-core CPU. Document the implementation, evidence, and operational outcome.

### Starting points
- crates/vals-store/src/index.rs: HashIndex and Index::maintenance_page
- crates/vals-store/src/engine.rs: flush_plan_step, flush_commit_start/step, protected_segments
- crates/vals-cluster/src/maint.rs: MaintJob::Flush worker and off-core interleave tests
- crates/vals-cluster/src/node.rs: maintenance scheduling loop
- bench/lab-breakthrough-20260812-0195/FINDINGS.md and plans/mixed-profile-soak-runbook.md

## Work log
- 2026-08-13 scope decision: the task owner explicitly approved closing 0197 as a measured-negative investigation with production reverted and requested a new backlog item for a better-performing attempt. Acceptance criteria were updated to record that decision; follow-on task 0199 captures the cadence-preserving design direction.
- 2026-08-13 corrected measured decision: the first insert-baseline versus overwrite-candidate comparison was invalid for throughput and is retained only as diagnostic history. A corrected same-key overwrite-only candidate-3 → baseline-2 → candidate-4 sequence kept live-key counts identical and unchanged on all nodes. Candidate means were 2.635M/2.689M writes/s; baseline all-pass mean was 2.974M (passes 2–3 mean 3.070M; pass 1 retained as a 2.782M startup/maintenance outlier). Candidate p99 was 38.74–44.59 ms versus baseline 17.42–20.28 ms, and candidate cadence remained 88–92 snapshots/node versus baseline 50–64. All passes had zero errors and 1,000/1,000 correctness. Physical starting run/RSS state drift is documented; the bracketing B/A/B still decisively rejects immutable rotation. Production remains fully reverted; ready to close as a measured-negative design.
- 2026-08-13 measured decision: rejected immutable hash-generation rotation after two prototype candidate arms, later superseded for throughput comparison by the corrected overwrite-only B/A/B above. Rotation doubled snapshot cadence, changed generation sizing/run layout, and kept the flush worker continuously busy. All production changes were reverted; raw evidence, harness, and rejection analysis remain under `bench/lab-hash-freeze-20260812-0197/`.
- 2026-08-12 implementation: removed HashIndex's steady-state ordered side structure and added an O(1) frozen-generation handoff for unsorted flushes. The flush worker now materializes keys off-core while active writes and reads continue; exact frozen retirement, value-segment protection, error cleanup, and sorted-index fallback are preserved. Local suites pass (vals-store 305/7 ignored; vals-cluster 544/3 ignored). Control evidence and the outstanding fleet candidate protocol are recorded in `bench/lab-hash-freeze-20260812-0197/FINDINGS.md`; lab candidate measurement remains required before closure.
- 2026-08-12 plan: 1. Establish the pre-change collapse with a short, repeatable lab control run that samples client throughput/latency together with flush/merge activity and plan-phase histograms; retain raw evidenc
…[truncated]
- 2026-08-12 context hints: 5 recorded with plan
- 2026-08-12 context hints: The user explicitly accepted this formerly proposed task.; Do not run cargo fmt; the tree is not rustfmt-clean.; Preserve unrelated modified backlog/0196 file; do not edit or stage it.; Likely relevan
…[truncated]
- 2026-08-12 preload: 6 file(s), ~42 KiB seeded into implementer context
- 2026-08-12 implementer report: Implemented task 0197’s frozen hash-memtable flush path.  Changes: - Removed HashIndex’s per-write `BTreeSet`; hash writes now maintain only the hash map. - Added O(1) unsorted-index rotation into
…[truncated]
- 2026-08-12 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-12 review (sol): revise — The immutable hash-generation handoff is a promising implementation and the point-read/commit/cancellation paths cover the basic interleave, but the task is not complete and there are significant inte
…[truncated]
- 2026-08-12 review (sol#2/sol): revise — The generation handoff is a promising direction, but the implementation still performs full hash traversal on the serving core, leaves important frozen-generation correctness holes during concurrent m
…[truncated]
- 2026-08-12 revision: Addressed both review rounds comprehensively while leaving task 0197 `in_progress` because lab hardware evidence remains outstanding.  Key fixes: - Removed the O(N) serving-core value-segment walk
…[truncated]
- 2026-08-12 review (sol): revise — The revision fixes several integration gaps: frozen winners are now included in point/digest/maintenance views, pressure pacing and phase metrics are propagated, and the added pinning/winner-directory
…[truncated]
- 2026-08-12 review (sol#2/sol): revise — The revision fixes several earlier issues: freeze no longer eagerly enumerates value segments, point/maintenance views mostly merge the frozen generation, pacing/phase metrics are propagated, and comp
…[truncated]
- 2026-08-12 revision: Completed the second revision and kept task 0197 `in_progress` pending lab hardware validation.  Changes: - Added generation-safe forget handling: `forget_plan` now marks an incompatible forget com
…[truncated]
- 2026-08-12 review (sol): revise — The latest revision resolves the specific forget interleave, makes frozen materialization a single worker-side traversal plus sort, and now performs an actual shared-device reopen test. The core imple
…[truncated]
- 2026-08-12 review (sol#2/sol): revise — The code-level revisions address the previously identified correctness issues: synchronous digests now include frozen winners, unbounded hash maintenance paging is safe, forget/flush ordering is fence
…[truncated]
- 2026-08-12 revision: Completed the final code revision before hardware validation; task 0197 remains `in_progress`.  Changes: - Restored the baseline `HashIndex` ordered `BTreeSet<Arc<[u8]>>`, synchronized on insert/remov
…[truncated]
- 2026-08-12 review (sol): revise — The latest code revision addresses the prior algorithmic concern by restoring the baseline ordered maintenance cursor, and compact frozen segment metadata removes the entry-count scan from protection
…[truncated]
- 2026-08-12 review (sol#2/sol): revise — The final code revision is locally sound and cleaner: it preserves the baseline ordered maintenance cursor, moves frozen materialization off-core, tracks value-segment pins in bounded metadata, and th
…[truncated]
- 2026-08-13 revision: Fixed the hardware-gate retirement regression and documented the failed first candidate honestly.  Changes: - Successful frozen flush commit now detaches both the mature frozen `HashIndex` and its lar
…[truncated]
- 2026-08-13 revision: Reverted the immutable frozen-generation design completely and retained only the measured experiment record.  What changed: - Restored baseline production behavior exactly in:   - `crates/vals-store/s
…[truncated]
- 2026-08-13 review (sol): revise — Reverting the measured-negative implementation is the right production decision, and the retained logs are useful. However, the claimed controlled hardware A/B is not actually same-corpus/same-operati
…[truncated]
- 2026-08-13 review (sol#2/sol): revise — The evidence is well retained and supports the decision to reject this particular immutable-rotation design; production code has been cleanly restored to baseline, so I found no new code-correctness i
…[truncated]
- 2026-08-13 revision: Updated the evidence and task record for the corrected overwrite-only B/A/B; production remains fully reverted.  Changes: - Reworked `FINDINGS.md` to lead with the valid candidate-3 → baseline-2 →
…[truncated]
- 2026-08-13 review (sol): accept — The corrected overwrite-only candidate→baseline→candidate hardware sequence addresses the prior corpus mismatch. Raw logs support the reported throughput, p99, correctness, live-key stability, and
…[truncated]
- 2026-08-13 review (sol#2/sol): revise — The corrected overwrite-only B/A/B is much stronger evidence: it fixes the workload mismatch, retains the outlier, discloses physical-state drift, and robustly shows immutable rotation is a regression
…[truncated]
- 2026-08-13 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-13 review (sol): revise — The retained B/A/B evidence is candid and supports rejecting the immutable-generation prototype, and reverting a measured regression was the correct production choice. However, this closes the accepte
…[truncated]
- 2026-08-13 review (sol#2/sol): revise — The experiment record is clean and candid, and reverting a measured regression is the correct production decision. However, the change closes task 0197 even though its required outcome was not achieve
…[truncated]
- 2026-08-13 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-13 review (sol): accept — The task is satisfactorily closed as the explicitly permitted measured-negative investigation. The corrected overwrite-only candidate→baseline→candidate sequence is auditable from the retained log
…[truncated]
- 2026-08-13 review (sol#2/sol): revise — Production code has been cleanly restored, the retained B/A/B measurements support rejecting the cadence-changing prototype, and task 0199 captures a sensible follow-on. However, the rejected candidat
…[truncated]
- 2026-08-13 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-13 review (sol): accept — Task 0197 is satisfactorily completed as the explicitly allowed measured-negative investigation. The retained overwrite-only candidate→baseline→candidate lab sequence supports rejecting the imm
…[truncated]
- 2026-08-13 review (sol#2/sol): accept — The change cleanly closes 0197 as the explicitly approved measured-negative investigation. Production Rust code is fully restored to baseline, while the rejected candidate is retained as a patch that
…[truncated]
- 2026-08-13 decision: accept — commit: bench: reject hash snapshot generation rotation
- 2026-08-13 usage: 108,252,060 tok (in 10,634,667, out 253,169, cache_r 97,364,224, cache_w 0) · cost n/a (unpriced)
  implementer: 59,092,884 tok (in 2,338,639, out 73,797, cache_r 56,680,448, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 17,288,259 tok (in 2,635,343, out 74,996, cache_r 14,577,920, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 15,999,300 tok (in 3,294,306, out 75,490, cache_r 12,629,504, cache_w 0) · cost n/a (unpriced)
  coordinator: 15,871,617 tok (in 2,366,379, out 28,886, cache_r 13,476,352, cache_w 0) · cost n/a (unpriced)

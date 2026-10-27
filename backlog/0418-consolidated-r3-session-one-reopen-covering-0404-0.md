---
id: "0418"
title: 'Consolidated R3 session: one reopen covering 0404/0369.2/0412/(0328)/0403/0409/0417.1/0406/0337 hardware signals, then resume to EOF'
status: done
priority: 2
created: "2026-10-10"
updated: "2026-10-12"
depends_on:
    - "0404"
    - "0409"
spec_refs: []
---

## Description

## Description
## Why
The owner approved this on 2026-10-10. Several tasks each wanted their own reopen of the retained SOAK5B-V5-R3 corpus, and every reopen costs up to 75–90 min of recovery before anything is measured. This task does **one** reopen, runs the users in the `docs/validation.md` R3 order (application-read-only first, writes last), and records each task's preregistered signal separately.

R3: 10.17B ACK, drained, saved cursor 4,951,424,335 of 5,000,000,002 source events. That leaves ≈48.6M events (≈100M KV, a few minutes of ingest), which fits inside 0403's ≤50M-event confirmation bound. The write continuation therefore runs R3 to EOF.

## Steps (in order)
1. **Reopen** through 0404 `--from-corpus R3` (corpus `R3` in `bench/CORPORA.md`, registered as `reserved`). Use `resume --from-corpus R3 --amendment REF --hold-s N`: the pre-ingest hold keeps steps 2–4 in the same reopen, and `touch <segment>/INGEST` starts step 5 (`plans/corpus-registry-runbook.md`). This is 0404's first hardware use: one reopen plus a short cursor/value probe. Record the binary and every intervening binary.
2. **Idle observation, ≥15 min, no client load.** This is **0369 item 2**: `vals_sweep_pass_age_seconds` stays bounded and `vals_sweep_passes` advances on every node. Also record:
   - 0369 item 11: `vals_ae_sync_errors` during boot seeding/cover-scan, with the reason if the build logs it;
   - 0369 item 12: the restart gap before `recovery_seeding`.
3. **0412 arm:** the four never-compared owned-aediag comparisons (n1n2 and n2n3, z0/z2, shard 0), using the fix 0412 chooses (seed-completion gate or per-shard retry for seed-not-ready Busy). Run sampled value-byte Gets against the sealed cache in parallel, using the 0406 checkpoints. The parallel Gets+aediag timing is a 0406 receipt. 0412's bounds apply: aediag ≤3,600 s.
4. **Optional 0328 slot.** Run only if, before launch, 0328 has its L0 baseline from retained census logs and an L1-tested client-pacing candidate. Then run one census pass per arm (A = current client, B = candidate), ≤1 h/arm, and report keys/s, Busy retries and the scan latency distribution. Otherwise skip it; 0328 stays todo.
5. **Write continuation:** resume bootes from the saved cursor to EOF under 0403's resume path and 0409's DiskStop rule. Signals:
   - **0403:** the saved/replay cursor, the carried ledger and cumulative ACK base, rising ACK, and sampled value bytes;
   - **0409:** the decider's live window and receipts, including a window rebuilt from saved zone-stats;
   - **0417 item 1:** cpu_core leaf captures, plus fp/lbr call graphs if the build has frame pointers, at continuation start and near EOF. Record on-box `date +%s%N` around `perf record` and the scrape counter endpoints that bracket each capture.
6. **EOF correctness:** loss-free drain, cursor match, samplecheck through the 0406 checkpointed derive (record the selection identity and any fallback), exact counts and owned aediag. This is 0406's end-to-end timing receipt. The result is **mixed-binary confirmation evidence, not a 0386 release pass** (validation.md).
7. **Clean stop:** count `/dev/hugepages/spdk*map_*` and HugePages_Free on each node after `graceful shutdown complete` and before the guarded cleanup. If the 0337 fix is in the binary, this verifies its SIGTERM leg. Otherwise it is the before-fix baseline. The kill -9 / exit-75 leg does **not** run here; it runs on 0405's scratch provision, so as not to deliberately crash a retained corpus.

## Budget and exit
- One session. Preregister an elapsed bound before launch from the 0401/0411 receipts. Expected about 4–6 h with step 4, about 3–4 h without. Recovery ≤90 min.
- Each task's signal stands alone. A missing or inconclusive signal is reported for that task and does not trigger another reopen without owner approval.
- Integrity events are classified under the RAMAB/0386 policy (lab-2 record-and-continue).
- Stop the continuation on a correctness failure or confirmed persisted corruption.
- After this session R3 counts as **consumed**: update the registry. Discard-requiring arms (0405, then 0386) come after.
- Do not run concurrently with 0416's lab-1 L1 runs, because that contaminates both CPU measurements.

## Gates before launch
- 0403 review done (software closure); 0404 and 0409 implemented.
- 0412's tool/gate fix chosen and landed.
- Optional: 0337 fix landed; 0328 prep for step 4.

## Acceptance
- A findings file under `bench/0363-compact-values/` with one section per task signal (0404, 0369.2/11/12, 0412, 0328 if run, 0403, 0409, 0417.1, 0406, 0337 SIGTERM), each marked confirmed, failed or inconclusive, with receipts.
- The registry shows R3 consumed, and the owning tasks have work-log entries citing the findings.

## Acceptance criteria

## Outcome

One mixed-binary R3 reopen (HEAD 674fa37d, 3 h 33 min). Findings in bench/0363-compact-values/R3-SESSION-0418.md, cited by every owning task. Confirmed: 0404 reopen with hold; 0369 item 2 idle sweep; 0406 Gets+aediag timing; 0403 resume to cache EOF (5,065,790,169; 10.407B ACK; samplecheck 10,407,283/10,407,283; counts tile); 0409 DiskStop (no stop); 0417 item 1 lbr captures (RangeMap::clone 19–24% of shard CPU, for 0420); 0337 SIGTERM (0 maps, 4096 free). 0412 inconclusive as preregistered (60/72). A seed-gated rerun found the other 12 equal; the owner decides. 0328 skipped (no census fixture). EOF owned aediag 71/72 is inconclusive: n1n3 z1/s4's 292M "differing" is an out-of-order digest stream (sampled keys identical) → 0432. Post-drain aediag pin is now the 0423 tool. R3 consumed and reserved for 0432; release before 0405.

Commit: bench(0418): consolidated R3 session — resume to cache EOF on HEAD 674fa37d (samplecheck/counts PASS), per-task findings, aediag pin to 0423 tool; R3 consumed, reserved for 0432

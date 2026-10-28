# Validation tiers (local-first)

Hardware time on the lab fleet is the scarcest resource in the project. A full
5B-event replay takes 5–10 h of ingest plus 1–4 h of checks, and the lab machines' non-ECC
RAM aborts many attempts for reasons unrelated to the change under test. This
document sets the default: **establish each acceptance criterion at the cheapest
tier that can really establish it, and use hardware to confirm, in the smallest
step that answers the question.**

Owner direction, 2026-10-08: local-first validation; resumable runs; start
from retained corpora; scaled-down hardware runs where they make sense; faster
post-run checks.

## Tiers

| Tier | What | Typical cost | Valid for |
|---|---|---|---|
| **L0** | Unit, property and fault-injection tests (`MemBlockDevice`/`FileBlockDevice`), plus analysis of results already collected under `/data/dataset/task*-results` or `bench/` | minutes | logic, policy, accounting, error paths, test gaps, projections from existing data |
| **L1** | Local scale model: file-backed runs and benches at reduced scale, e.g. `crates/engine/tests/rss_bench.rs` (see the memory note on `VALS_RSS_ORDERING=sorted`), `plans/overwrite-cliff-local-repro.md`, `plans/capacity-local-baseline.sh` | minutes–1 h | per-key RAM, merge/space policy, CPU attribution with perf, local A/Bs of algorithmic changes |
| **L2a** | Hardware, starting from a **retained corpus** (0404): restart the fleet on data already on the drives, then run reads or a short continuation | 0.5–2 h | late-regime behaviour, post-drain or read-path issues, confirming a fix in the state where it was found |
| **L2b** | Hardware **scaled-down profile** (0405, `run_full1.py run --attempt SCALED-k<k>`): caps and geometry ÷k, so disk, merge-policy and RAM walls arrive at ~1/k of the data | ~0.5–1 h (SCALED-k8: 24 min) | walls that scale with bytes per shard, keys per node or runs per engine. Calibrated against R3 at k=8 (`bench/0405-scaled-profile/CALIBRATION-K8.md`): mean-shard fill, run counts, RSS per key and shadow-fold size transfer. Not valid for worst-shard records fill (a prefix replay keeps the early placement skew, so the disk wall comes early), merge counts, f > 0.97, throughput, late decay, the drive write step, SLC cache or fixed per-node overheads |
| **L2c** | Short targeted hardware A/B (preregistered, ≤ ~1–2 h per arm, whole-arm distributions) | 2–4 h | SPDK async behaviour, real-NIC and 3-node interactions, early-regime throughput |
| **L3** | Full replay of the sealed 5B cache end to end (0386-style) | 6–14 h | release gate only: end-to-end correctness at full scale, EOF disk/RAM, full-length decay |

Rules:

1. **Every task states its intended tier** in its plan or acceptance section. If a
   task asks for hardware, it says why a lower tier can't answer the question.
   Valid reasons include SPDK async races (file-backed repros can't hit them),
   drive physics, real-NIC or 3-node interactions, or behaviour that only
   appears at a state no lower tier can reach.
2. **Local first, hardware to confirm.** Land fixes on L0/L1 evidence where
   that evidence is valid. Hardware confirmation can be **batched**: one L2/L3
   run can confirm several landed fixes, each with its own preregistered
   signal. The tradeoff is that attribution is coarser when something regresses.
3. **L3 is a release gate, not a per-fix check.** Don't open a full replay just to
   measure one fix. Use L2a or L2b. Run L3 when a batch of fixes is ready, or when
   the question genuinely needs EOF.
4. **Never restart from zero what can safely be resumed** (0403). Clean-drained
   pauses and reclassified planned stops can resume. Crash-ended runs need a
   durable cursor (0408); 0403 alone cannot resume an unclean stop with no saved
   cursor. Resume is never allowed after a correctness failure or confirmed
   persisted corruption, or when the recorded crash budget is exhausted.
5. **Reuse corpora deliberately** (0404). Check the corpus registry before any
   discard. Read-only experiments keep a corpus pristine. A write experiment
   consumes it. Format-changing builds can't use old corpora.
6. **Post-run checks reuse a compatible checkpointed derive when available**
   (0406); record selection identity or full-derive fallback. Checks that don't
   depend on each other run in parallel.
7. **Keep fast feedback inside long runs.** Preregister early-read signals, for
   example rate per 100M against the reference arm or a RAM slope, so a doomed
   arm can be stopped early. Owner preference stands: judge on whole-arm
   distributions, and early stops are for clear failures, not for picking
   winners early.

## Task-sized validation contracts

Unless a large run is the explicit goal of the task, hardware confirmation is a
bounded experiment, not a validation campaign. Before launch, record:

- **Question and signal:** the acceptance criterion, required state/event, and
  pass/fail measurement.
- **Cheapest evidence:** retained receipts or local tests first; why hardware is
  still necessary. Tiers are alternatives, not a checklist to execute in full.
- **Budget:** number of arms, data/operation limit, and elapsed-time bounds,
  including preparation, recovery/warmup and post-run checks. Default to one
  targeted confirmation, or a bounded matched A/B for a performance decision.
- **Exit:** stop after the registered signal and required checks are collected.
  If the required state never occurs or hardware faults confound the result,
  report it as inconclusive. Expanding the budget, adding replicates or making
  extra diagnostic gates into closure requirements needs owner approval unless
  the larger campaign is already the task's explicit scope.

Every arm retains value verification and integrity monitoring appropriate to its
claim. Throughput replicates do not automatically require the entire SOAK5B
post-run suite: preregister targeted checks and a representative campaign-end
full check where applicable. Correctness signals must still be investigated;
never claim an unchecked arm passed the full gate.

Separate **fix validation**, **corpus qualification**, and **release qualification**.
Reuse receipts across tasks when they establish the same criteria. Harness
changes start with mocks/local integration and piggyback hardware confirmation
on a suitable planned run where possible. Optional measurements do not justify
extending or relaunching that run; missing signals stay explicitly unconfirmed.
Review verifies the changed behavior and agreed acceptance evidence, not a
second experiment; real correctness defects still block and unrelated
improvements become follow-ups.

## Triage of the open backlog (2026-10-08, task 0407)

This is the cheapest valid tier for each open task. The original "Amend" column
was approved on 2026-10-08 (0407). The owner subsequently approved the task-sized
contracts above and the tightened harness/retained-corpus rows below. Current
task acceptance sections take precedence over historical run instructions.

| Task | Cheapest valid tier | Smallest hardware step | Amend |
|---|---|---|---|
| 0328 scan under admission | L0 baseline from retained census logs → L1 client pacing → L2a | one census pass per arm on the R3 corpus (~1 h/arm) | done: the L2a census A/B ran on the K8 corpus instead of R3 (read-only reopen, four ~20 min `verify` arms, bench/0328-scan-pacing); shuffled order kept |
| 0332 skew-gated rehome confirm | **L0**: retained PD metrics from SOAK5B/V2/V3-R2/V4/V5-R3 | none | answer from retained metrics; drop the F4 throughput criterion |
| 0337 hugepage maps after exit | L0 (named `shm_id` disabled `--huge-unlink`; fixed in 0424) + L1 shim-only box check (bench/0337-hugepage-maps) + minimal valsd check | one box, scratch device, start/SIGTERM/kill-9, ~10 min | done: SIGTERM leg on 0418; crash / exit-75 restart legs taken from the 0386 R4 restarts instead of 0405 |
| 0357 runtime-dealloc long arm | L3-class (drive physics) | piggyback only | re-run only if runtime dealloc is re-enabled by default, as a piggyback on the next L3 |
| 0366 0364 follow-ups | L0; item 2 L1 (reopen a file-backed engine with a planted merge backlog) | none | item 2: local measurement; hardware restart optional |
| 0369 0367/0368 follow-ups | L0; item 2 L2a idle restart; item 10 L1 | idle restart on a retained corpus, ≤15 min, read-only | item 2: idle restart, no ingest run |
| 0371 0365 follow-ups | **L0** (old binary from a pinned worktree vs a v5 run, file-backed) | none | — |
| 0372 0370 follow-ups | L0; item 6 folds into 0337's box check | same as 0337 | item 6: covered by 0337 |
| 0377 rehome span-only reseed | L0 property test + L1 3-process file-backed cluster | piggyback on the next L3's rehome windows | replace the dedicated hardware datapoint with an L1 test plus a piggyback |
| 0380 0379 follow-ups | **L0** + counters from SOAK5B-V3/V4/V5-R3 | none | — |
| 0383 frozen-run garbage | L1 (small cap ⇒ ≥50 frozen runs locally), L0 resurrection property test; L2b for the chosen design | prefer 0405 piggyback; standalone one-node arm ≤2 h only with a calibrated applicable profile | local design does not wait for 0405 |
| 0386 full 5B replay | **L3** (the release gate) | — | — |
| 0387 0384 follow-ups | **L0** (+ CPU microbench) | none | — |
| 0390 pre-step shard CPU growth | L1 first: frame-pointer build, sorted/replay `rss_bench` ingest to ≥1B keys, `perf --call-graph fp` | one-node fresh ingest with an FP build, 2 captures, ~1.5 h, only if L1 does not reproduce | allow local FP profiles. R3 perf files are leaf-only, so new stacks are needed |
| 0391 late-regime decay | L0 from R3 (perf-node1 300M–8000M, watch, drive-step, merge logs) → L2a | short continuation per arm on the R3 corpus (~1 h) | done (0417): on-box-timed captures at continuation start and near EOF in 0418 step 5; the shadow-refresh A/B was not run, owner settled it with 0391's L0 ceiling (≤0.6% of shard CPU) |
| 0393 0382 run-count costs | **L0** from R3 artifacts (check which 0382/0389 gauges exist at pin 8736b2b1) | none | record from R3; refresh later only for missing gauges |
| 0401 node1 post-drain heap (done) | L0 regression + retained-corpus confirmation | reuse committed receipts; do not reopen to repeat them | owned-aediag/corpus verdict moved to 0411 |
| 0403 resume harness | L0/L1 mocks → one L2a piggyback | one resume, ≤50M additional source events or 15 min ingest, ≤2 h targeted confirmation | no dedicated 2–3B prefix/reboot campaign; full checks at shared endpoint |
| 0404 corpus registry/from-corpus | L0/L1 command/state integration | first use piggybacks on a planned reopen | no imaging study or repeated 0401 load |
| 0405 scaled profile | L0/L1 calculations → L2b calibration | one factor/arm, ≤2 h total | use calibration trace for 0402 offline; no second real-use arm |
| 0406 checkpointed correctness | L0/L1 synthetic + real-cache equivalence | reuse eligible 0401 timing, otherwise next planned run | no separate timing replay; failed/pending signals remain explicit |
| 0408 durable crash cursor | L0/L1 failure injection + local kill/restart | non-blocking piggyback | no dedicated crash/reboot prefix |
| 0411 digest progress / owned aediag | L0/L1 request-duration/abandonment → L2a | one diagnostic-only reopen, ≤3 h total, aediag ≤3600 s | reuse valid 0401 checks; no automatic extra arms |
| 0402 shadow-fold disk stop | **L0**: replay the candidate rule offline over the R3 time series | none (preregister for 0386-R4); a code fix is validated via 0383's L1 | — |
| 0339 corrupt-read identity check (proposed) | L0 + decode-path microbench | optional read-only Get A/B on R3 | — |
| 0340 ValuePtr fsck (proposed) | **L0** | optional read-only demo on R3 | — |
| 0341 replica self-repair (proposed) | L0 (3-process file-backed, injected bad pointer) | none | — |
| 0344 finite-client residuals (proposed) | L0 | none now | — |
| 0394 GET-hit extra block reads (proposed) | **L0** (0382 microbench + R3 Get/blocks-read counters) | none | confirm from R3 counters, not a new 0393 run |

Open caveat: no one has yet checked whether R3's prom files carry every 0382/0389
gauge. Check that before relying on L0 for 0391/0393.

### The R3 corpus is the shared L2a asset

The SOAK5B-V5-R3 corpus (10.17B ACK, drained, saved cursor 4,951,424,335) is
used for 0401's completed confirmation and wanted by 0411, 0403's hardware check,
0328, 0369 item 2, 0391 and the optional
0339/0340 demos. Run them in this order, cheapest-to-destroy last:

1. **Application-read-only users:** reuse completed 0401 checks; 0411
   diagnostic confirmation; 0369 item 2 idle restart; 0328 census arms;
   optional 0340 fsck. No new ingest or destructive preparation. Background
   maintenance may still write, so record binary/history changes rather than
   claiming sector-level pristine state.
2. **0403's hardware check:** piggyback one bounded resume on a suitable planned
   continuation under 0402's preregistered disk rule. Reuse 0401's post-drain
   timing receipts for 0406 where the changed paths were exercised; do not repeat
   them just for timing. Full correctness remains required at the campaign
   endpoint. A corpus reopened or maintained under a changed binary has mixed
   history, even without application writes; it is confirmation evidence, not
   automatically L3-equivalent. Record all intervening binaries before deciding
   whether any resumed result qualifies for 0386.
3. **Write continuations** (consume it): 0391 shadow-refresh A/B arms (not run: settled by
   0391's L0 ceiling, owner decision 2026-10-10).

Steps 1–3 run as **one** reopen, task 0418 (owner-approved 2026-10-10): idle
check (0369), 0412 aediag with parallel value Gets (0406), optional 0328
census, then resume to EOF (0403/0409/0417 item 1) and a clean stop that records
the SPDK hugepage map files (0337). Each task keeps its own signal; a missed one
is reported, not reopened for. 0337's kill-9 leg runs on 0405's fresh provision.

Discard-requiring arms (0405 calibration, a fresh 0383 L2b arm or fresh 0386)
come after these users, or after an explicit corpus release. They do not override
R3's reservation simply because their task is ready.

R3 is registered as corpus `R3` (`bench/CORPORA.md`). Task 0418 consumed it on 2026-10-12, by
reopening it, running the read-only checks and resuming to EOF
(`bench/0363-compact-values/R3-SESSION-0418.md`). It stays `reserved` for 0432's digest-order
diagnosis, and `dealloc.py` refuses to discard it meanwhile. Release it before 0405's calibration
arm. Procedure: `plans/corpus-registry-runbook.md`.

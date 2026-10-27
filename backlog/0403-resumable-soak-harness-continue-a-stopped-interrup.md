---
id: "0403"
title: 'Resumable soak harness: continue a stopped/interrupted replay on its existing corpus instead of relaunching from zero'
status: done
priority: 2
created: "2026-10-09"
updated: "2026-10-12"
depends_on: []
spec_refs: []
---

## Description
## Why
0386 has made six attempts and none reached EOF. Several of them died late from causes unrelated to the code under test:
- SOAK5B-V5 died at 9.93B ACK, about 95% of EOF, when a bit flip in lab-3's kernel text caused a panic and reboot. `final.json` recorded FAIL / hard_safety / host_unreachable.
- R3 stopped at 97.3% on a transient disk planned stop (0402).

Each relaunch starts again from a BEFORE discard, so a full arm (5–10 h) is lost every time. The memory note confirms that "the harness can't resume a run".

The product already supports what a resume needs:
- valsd recovers its corpus on restart; crash recovery is a tested feature.
- bootes persists `cursor/jetstream` in the cluster and resumes from it.

What is missing is harness support.

## Scope
Add a `--resume <attempt>` mode (name to be settled) to `bench/0363-compact-values/run_full1.py`, and to the underlying 0321/0327/0348 runner layers as needed. Given a stopped or interrupted attempt directory, it:
1. **Restores box state on any host that rebooted.** Check that boot_id changed. Then restore core_pattern, `panic_on_oops`/`panic`, hugepages and vfio-pci binding, and check for a kernel change (see the memory note on lab-3 reboots). Write receipts. It must never discard, format or provision.
2. **Checks that the corpus matches the attempt.** The same drive serials/topology must be present (the 0392 superblock topology descriptor), and the server binary SHA must be identical on every node and the PD. A resume with a different binary must be refused unless `--allow-binary-change` is given. That flag is recorded in the receipts and marks the attempt as a mixed-binary run.
3. **Restarts the fleet without reformatting, waits for serve, and restarts bootes.** bootes loads its persisted cursor. The harness records the cursor and ACK totals at the moment of resume.
4. **Carries the attempt's state forward:**
   - the crash ledger and crash budget windows;
   - the integrity-event classifier state;
   - the per-100M report boundaries;
   - the watch rules and stop policy;
   - the drive-step detector.

   Resume is an explicit segment boundary in every report (`resume_segments`), so per-1B rates exclude the downtime and attribute recovery time.
5. **Runs end-of-run correctness the same way as a non-resumed run:** loss-free drain, cursor match, samplecheck, exact counts, owned aediag. Correctness is not weakened. The expected state is defined by the sealed cache prefix at the final cursor, so a resume does not change what correctness means.

Policy, to be preregistered in the 0386 README before first use:
- **Resume is allowed** after the following, which do not themselves mean failure:
  - host_unreachable or host reboot;
  - a harness/controller crash (e.g. the coordinator box dies);
  - a planned stop that a later preregistered amendment reclassifies (e.g. 0402).
- **Resume is not allowed** after a correctness failure, after confirmed persisted or published corruption, or after the crash budget is exceeded.
- A host reboot counts as one entry in the crash budget, so resume cannot hide flaky hardware.

Current implementation scope is clean-drained segments with a saved cursor. A host/controller crash is resumable here only if a clean drain was subsequently obtained; an unclean crash with no durable cursor requires 0408. Automatic resume is not a closure requirement.

## Acceptance
- Unit tests for the runner wiring:
  - the resume refuses discard/format;
  - binary-SHA mismatch is refused;
  - the ledger, crash budget and report segments are carried across a resume;
  - the policy refuses to resume after a correctness failure or confirmed corruption.
  - Existing tests stay green: test_soak5b, test_full1, test_soak5bv5_runner, test_soak5bv5_projection.
- Local or mock end-to-end exercise of the resume path. The existing profile-soak self-test/mock scripts under plans/ may serve as a starting point.
- Tier **L0/L1**, with one **L2a** confirmation piggybacked on the next suitable retained-corpus continuation. Exercise a clean pause and one resume; confirm the saved/replay cursor, carried ledger and cumulative ACK base, rising ACK, and sampled value bytes. No dedicated fresh 2–3B-event prefix, reboot or controller-crash leg is required; refusal, reboot preparation and interrupted-controller paths are covered locally. Mid-ingest bootes crash-resume belongs to 0408, not this clean-drain task.
- Hardware budget: one resume, an observation window of at most 50M additional source events or 15 min of resumed ingest (whichever comes first), with at most 2 h total preparation/recovery/observation/targeted checks and at most 75 min recovery. If recovery overruns or leaves insufficient time for required checks, report inconclusive. This bounds the confirmation window, not a to-EOF parent campaign; do not stop that campaign solely to end the observation. A shorter suitable prefix is preferred. Full correctness runs once at the shared campaign endpoint under that campaign's registered bounds, not at every validation pause. If the necessary state cannot be reached within budget, record the result as inconclusive and seek approval before another arm. Coordinate corpus use with 0401; mixed-binary history is diagnostic/confirmation evidence, not automatically a 0386 release pass.
- Local/mock acceptance closes the software work; hardware confirmation may remain explicitly pending until that suitable planned run. Do not block 0404/0408 software work or create a prerequisite campaign solely to obtain the piggyback.
- A runbook section under plans/ and an update to the SOAK5B-V5 README preregistration.

## Acceptance criteria

## Work log

- 2026-10-10: the L2a hardware confirmation is step 5 of 0418, the single R3 session. Its ≈48.6M remaining source events fit this task's ≤50M bound, so the continuation runs R3 to EOF. Software closure is not gated on 0418.

## Outcome

Resume harness: run_full1.py resume/resume-check and soak5b/resume-v5.sh, with refusal of discard/format, binary-SHA mismatch and post-failure resumes; ledger, crash budget and report segments carried across a resume; runbook plans/soak-resume-runbook.md and the SOAK5B-V5 README resume policy (owner-approved 2026-10-08). Bench tests and samplecheck Go tests green; two review passes (all findings fixed with kill-the-fix tests). The L2a hardware confirmation remains pending as 0418 step 5; software closure is not gated on it. Crash-resume is 0408.

Commit: feat(0403,0406): resumable SOAK5B-V5 harness (resume/resume-check, refusal policy, ledger carry, runbook) + checkpointed samplecheck derive (R3 cutoff 4302 s -> 145 s, identical receipts); hardware receipts pending in 0418

## Hardware confirmation (0418, 2026-10-12)

- L2a confirmation in 0418 (bench/0363-compact-values/R3-SESSION-0418.md), segment RESUME-2. Replay started at the saved cursor 4,951,424,335 and ran to the cache EOF, 5,065,790,169. The carried ledger (16 rows) and ACK base were preserved, and ACK reached 10,407,116,633. Per-segment exact counts tiled the prefix, and whole-cache samplecheck passed 10,407,283/10,407,283. **Confirmed.** It ran about 114M events, beyond the 50M observation bound, because this was the R3-to-EOF campaign that the bound exempts.

---
id: "0168"
title: Hardware churn re-validation of re-home robustness (0155) and default-on decision for VALS_PD_REHOME_ENABLED
status: done
priority: 3
created: "2026-08-10"
updated: "2026-08-10"
depends_on:
    - "0155"
spec_refs: []
---

## Description
Task 0155 landed the churn-robustness stack in-process: progress-aware report deadline (abort only on silent owners or stalled copies, attributed via pd_rehomes_aborted_{report_gap,mirror_failure,copy_stall}), mark-validated epoch-stable copy resume (rehome_copy_{pages,resumes,restarts}), copy holds against target-side cleanup, PD prepare/commit/abort logging, and pace levers (VALS_REHOME_COPY_PAGE_ENTRIES / _PAGES_PER_TICK / _HOLD_MS). docs/zones.md notes a fresh hardware churn run is still owed before default-on.

Re-run the 0151 protocol (bench/lab-rehome-20260808/FINDINGS.md: no-presplit 20M-key sorted zone, valsmix load + churn oracle, SIGKILL + PD-restart interruptions) on lab with the new build and compare:

- Abort rate under sustained churn (0151 baseline: 32 of 77 prepares aborted; expect near zero outside genuine outages, with causes attributed in the new counters/logs).
- Re-scan work on retried moves (rehome_copy_resumes vs rehome_copy_restarts; 0151 baseline: every retry re-scanned from the range start).
- Zero consistency errors through kills/aborts/cutovers; exact key counts (as 0151).
- Watch the residual windows noted in review: AE-heal mark invalidation is post-hoc (bounded by one pull step), and copy-hold expiry vs long abort→retry gaps.

Then decide and record whether VALS_PD_REHOME_ENABLED flips default-on (docs/zones.md + ARCHITECTURE.md if shipped), or document what still blocks it.

## Acceptance criteria
- FINDINGS.md under bench/ with the comparison table vs 0151.
- Default-on decision recorded in docs/zones.md (either flipped with rationale, or the remaining blocker documented).

## Plan

Re-run the 0151 worst-case hardware protocol on the idle lab fleet with a freshly synced/rebuilt current valstore checkout, preserving raw evidence and making the default decision from measured results.

1. Create a new dated `bench/lab-rehome-*/` artifact directory. Record source revision/build timestamps, exact node/PD/client configuration, topology, commands, and a sampler that captures all re-home totals and cause counters, copy pages/resumes/restarts, unrehomed/sorted ranges, key/error/cleanup counters, plus relevant PD/node logs.
2. Verify all three hosts are idle and SPDK residue is absent; sync the current source to `~/vals` on all nodes and rebuild `valsd` with SPDK before every run. Rebuild the sibling `vals-examples` tools used by the oracle. Start a fresh RF=2, 4-shard/node cluster with `VALS_PD_REHOME_ENABLED=1`, 40 GiB RAM budget, default deadline/copy pace/hold settings, and clean-formatted data/PD state. Use one-line `sudo systemd-run` launches and preserve the actual config/env in the artifacts.
3. Reproduce the 0151 shape: one no-presplit sorted zone, 20M ordered 256-byte keys, concurrent load/read churn with periodic catalog refresh, and automatic splits/re-homes. Keep a continuous churn oracle and metric sampler. During active moves, SIGKILL one data node, leave it down long enough to exercise genuine-outage attribution, restart it with `format=false`, then restart PD mid-rehome without wiping raft state.
4. Continue until re-home debt converges (or record a bounded, evidenced failure). Capture prepare/completion/abort books and abort causes across process restarts, copy resume/restart/page deltas, exact full-zone count, representative scan windows, final consistency/transport-error verification, and evidence around AE mark invalidation/copy-hold expiry and cleanup behavior. Leave the fleet clean.
5. Write `FINDINGS.md` with a direct comparison table against the 0151 baseline (32/77 aborted; every retry rescanned), interruption timeline, consistency/count results, residual-window observations, build/config provenance, and artifact index.
6. Decide from the run whether re-home is ready to default on. If it is, change the code default and update `docs/zones.md` plus `ARCHITECTURE.md`; otherwise keep it off and update `docs/zones.md` with the specific measured blocker and evidence. Run focused local checks for any code/default change and validate all documentation claims against raw logs before review.

### Starting points
- bench/lab-rehome-20260808/FINDINGS.md and sample.sh — 0151 baseline/protocol/artifact style
- docs/zones.md:117-221 — protocol, current default-off statement, and outstanding hardware gate
- plans/bootes-soak-runbook.md:19-221 — current lab topology, sync/build, one-line systemd-run launch, configs, cleanup
- memory.md: lab `~/vals` is rsynced (not git); rebuild each run; `node.conf format=true` reformats on restart

## Work log
- 2026-08-10 plan: Re-run the 0151 worst-case hardware protocol on the idle lab fleet with a freshly synced/rebuilt current valstore checkout, preserving raw evidence and making the default decision from measured res
…[truncated]
- 2026-08-10 context hints: 4 recorded with plan
- 2026-08-10 context hints: bench/lab-rehome-20260808/FINDINGS.md and sample.sh — 0151 baseline/protocol/artifact style; docs/zones.md:117-221 — protocol, current default-off statement, outstanding hardware gate; plans/bo
…[truncated]
- 2026-08-10 preload: 4 file(s), ~32 KiB seeded into implementer context
- 2026-08-10 implementer report: Implemented task 0168 and completed the full lab hardware run.  - Freshly rsynced/rebuilt valstore on all 3 SPDK hosts and rebuilt valsmix/valsq; ran the no-presplit 20M-key RF=2 churn protocol wit
…[truncated]
- 2026-08-10 review tier: high-powered — reviewers: sol, claude
- 2026-08-10 review (sol): accept — The change satisfies task 0168. It records a substantive hardware churn rerun with a direct 0151 comparison, balanced prepare/commit/abort accounting across the PD restart, attributed abort counters, 
…[truncated]
- 2026-08-10 review (claude): accept — The change completes task 0168: a full re-run of the 0151 hardware churn protocol on lab with the 0155 build, with comprehensive raw artifacts under bench/lab-rehome-20260810/. Both acceptance c
…[truncated]
- 2026-08-10 revision: Addressed the provenance review without changing any run result, raw artifact, or the default-off decision.  - Expanded `bench/lab-rehome-20260810/COMMANDS.md` from placeholders/summaries into the 
…[truncated]
- 2026-08-10 review (sol): accept — The revision fully addresses the prior provenance finding. COMMANDS.md now preserves the reconstructed literal sync/build, service launch, workload, interruption/restart, convergence, verification, ar
…[truncated]
- 2026-08-10 review (claude): accept — The revision fully addresses the prior provenance findings without altering any run result or decision. COMMANDS.md is now the literal, complete command sequence (sync/build, one-line systemd-run laun
…[truncated]
- 2026-08-10 decision: accept — commit: 0168: revalidate re-home churn on hardware
- 2026-08-10 usage: 6,750,522 tok (in 1,923,231, out 52,123, cache_r 5,568,360, cache_w 72,684) · cost n/a (unpriced)
  implementer: 4,038,560 tok (in 710,907, out 27,301, cache_r 3,300,352, cache_w 0) · cost n/a (unpriced)
  coordinator: 1,483,681 tok (in 643,700, out 8,493, cache_r 831,488, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 1,217,716 tok (in 568,592, out 5,796, cache_r 643,328, cache_w 0) · cost n/a (unpriced)
  reviewer:claude: 10,565 tok (in 32, out 10,533, cache_r 793,192, cache_w 72,684) · cost n/a (unpriced)

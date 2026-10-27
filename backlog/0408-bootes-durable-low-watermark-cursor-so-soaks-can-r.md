---
id: "0408"
title: bootes durable low-watermark cursor so soaks can resume after a crash/reboot (not only after a clean drain)
status: done
priority: 2
created: "2026-10-09"
updated: "2026-10-12"
depends_on:
    - "0403"
spec_refs: []
---

## Description

## Description
## Why
0403's implementation found that a soak can only be resumed after a **clean drain**. bootes (`~/code/bootes`, at 3d5b2e9) deliberately never saves the cursor periodically for buffered stores. See ingest/ingest.go `startAuxLoops`: "Persisting their cursor periodically could put it ahead of undrained data, so they only save the cursor after the final successful drain in CloseContext."

The consequence: if a host reboots, the coordinator box dies, or bootes is killed mid-ingest, the cluster cursor stays at 0 or at its last clean value. That is exactly the SOAK5B-V5 case: lab-3 rebooted at 9.93B, the drain failed and the log says "final cursor persistence withheld". 0403's `resume-check` correctly refuses such attempts. As things stand, resume covers only pauses and planned stops, not the crashes that cost the most.

## Scope
1. **bootes: durable low-watermark cursor for ValsStore.** Track the highest source seq W such that every prepared KV with seq ≤ W has been acknowledged as persisted at the configured write quorum. Count KVs that ended oversized, rejected or dropped as resolved, matching the existing drain accounting. Then:
   - Save W periodically, e.g. every 30 s or every N events (configurable, off by default to keep current behaviour, and on for the soak arms).
   - Write it through the same `cursor/jetstream` key, or a separate `cursor/jetstream-watermark` key if the final-drain semantics must stay distinct; decide which and document it.
   - Never save a value ahead of unacknowledged data. Unit tests must cover out-of-order acks across lanes, retries, partial batch failures and shutdown.
   - Keep the existing final-drain save.
2. **Resume semantics in 0363's harness** (`run_full1.py resume`, from 0403):
   - Allow resuming from a watermark cursor when there was no clean drain. bootes replays from W, so events in (W, X] are re-written, where X is the unknown crash point.
   - Data correctness is unaffected. The replay runs in source order, rewrites with newer HLC versions, replays deletes in order, and `indexed_at` falls inside the full multi-segment bootes window that samplecheck already uses.
   - **Exact counts are affected.** Segment 1's bootes persisted counters include the overlap (W, X]. Define the rule and preregister it. Either:
     - (a) the last segment is exact (`derive(end) − derive(W_last)`) and earlier crash-ended segments are checked as `≥ derive(W) − derive(start)` and `≤ derive(end_of_admission_estimate)`; or
     - (b) bootes also persists W together with per-zone resolved counts at W, which makes every segment exact. (b) is preferred if it is cheap.

     State clearly in the receipts if (a) weakens anything.
   - Treat a crash-resume (no clean drain) as a distinct resume class in the policy. It is allowed only after host reboot, host unreachable or harness/bootes death, and it still counts toward the crash budget.
3. **Update 0403's runbook, policy draft and tests** for the crash-resume class.

## Acceptance
- bootes unit tests show W never passes unacknowledged data, including concurrency and failure injection, plus an integration test against a local vals cluster: kill bootes mid-ingest, check the saved W ≤ the true durable prefix, restart, and finish the replay. A small local samplecheck shows exact data and the chosen counts rule passes.
- 0363 harness tests cover the crash-resume class and the counts rule.
- Tier **L0/L1**: the local kill-bootes/restart integration and failure-injection tests above establish the durable-prefix and counts contracts. No dedicated hardware prefix, destructive provision or reboot leg is required for task closure. Confirm the hardware path as a non-blocking piggyback on the next suitable planned replay interruption or real crash, preserving its integrity and crash-budget policy. Record that signal as unconfirmed until observed; do not deliberately crash a shared retained corpus merely to obtain it.
- The new bootes commit is pinned in the soak arms. Validate samplecheck encoder/checkpoint identities after the pin change; regenerate incompatible derive checkpoints locally, not during hardware ingest, and record any full-derive fallback honestly. Notes in the SOAK5B-V5 README preregistration.

## Acceptance criteria

## Outcome

L0/L1 done; hardware unconfirmed (piggyback). bootes e27dde7 (~/code/bootes): --vals-cursor-watermark saves durable W to cursor/jetstream after a fsynced intent line with exact counts; a dropped chunk freezes W (deliberate deviation: V5 had 23 deadline drops); saver awaited before the final cursor. Go tests + mutation checks. SOAK5B-V5 pinned to e27dde7 (BOOTES-e27dde7) at 30s; encoder files unchanged, so samplecheck pins/checkpoints stay valid. run_full1 crash-resume class: watermark candidates resolved by next start or cluster cursor (rebase), exact per-segment counts, reboot-dependent gate, one budget entry, rate ledger capped at W with voided/synthesized rows. 208 harness tests pass. L1 (3-node local, SIGKILL and 2-node-kill+SIGTERM): prefix and whole-prefix samplecheck --every 1 exact, harness counts match (/data/dataset/task0408-results/IT-L1-e27dde7/).

Commit: feat(0408): crash-resume from a bootes durable low-watermark cursor (bootes e27dde7 pinned for SOAK5B-V5, run_full1 crash-resume class with exact per-segment counts, runbook + V5 README preregistration)

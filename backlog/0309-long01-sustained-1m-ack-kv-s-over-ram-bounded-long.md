---
id: "0309"
title: 'LONG01: sustained 1M+ ACK KV/s over RAM-bounded long ingestion runs (production bootes client, 2-node RF2)'
status: done
priority: 1
created: "2026-09-25"
updated: "2026-09-27"
depends_on:
    - "0297"
spec_refs: []
---

## Description
## Context

Child of 0297. Master (MASTER0306 code = current master crates: gate-32 + node flush domain) with the production bootes client (0304, real-client12 FULL) reaches 1.27–1.36M ACK KV/s whole-write on the dual-link 2-node RF2/W2 device-sync fixture — but only over the frozen 4,000,699-event / 8,391,720-KV interval (≈6 s of writes, no auto-splits). The owner asked (2026-09-26) to make that rate sustain across much longer and bigger runs.

## Physical envelope (measured/derived, 2026-09-26)

- Nodes: ~48 GB MemAvailable (8 GiB hugepages reserved); harness sets `VALS_RAM_BUDGET_BYTES` = 36 GiB (RAM Busy admission above it). Server RSS ≈ 200 B/key/replica (P0304-FULL4: 1.65–2.0 GB at 8.39 M keys; docs/out-of-core-index.md ~200 B/replica). RF2 on 2 nodes ⇒ each node holds every key ⇒ ceiling ≈ 170–190 M KVs per run (≈ 2.2–2.5 min at 1.3 M/s) on this fleet. Longer than that needs more nodes or per-key RAM reduction (0288), not harness work.
- Devices: 4 TB 9100 PRO per node; ~443 B/KV written (P0304). Current zone regions (records/backlinks 80 GiB each) are too small for ~150 M KVs → enlarge for LONG runs.
- Source: sealed cache /data/dataset/jetstream-cache-20260824 has 1,000,000,009 events (seq 1..1,008,858,566) — ample.
- Auto-split default 1 M entries/range ⇒ a 150 M-KV run crosses O(100+) splits from the 104 presplit ranges: this is the growth/maintenance pressure the short runs never reached.

## Plan

1. Harness (no fleet): real-client13 = real-client12 with a parameterised, pre-audited fixture interval (manifest with exact counts/canonical/raw identity produced once by the same binary; runtime verifies source identity + exact counts/cursor/samples), bounded client memory, per-second progress; `bench/0297-ingest-1m/long01/run_long.py` scaling guards/phase limits, enlarged zone regions, server RSS/key + split/flush/seed/staged/Busy timelines; analysis of per-second/per-10 s/per-progress-quartile rates.
2. Preregister in `bench/0297-ingest-1m/long01/README.md` before any fleet run: nested prefixes from after_seq 800,000,000 (A≈20 M events, B≈45 M, C≈75 M conditional on B's RSS/key projection with ≥15 % headroom).
3. Run the ladder; if the rate decays with growth, attribute (profiles + metrics at matched progress) and fix one evidence-backed mechanism at a time via child tasks with matched A/Bs.

## Acceptance criteria

- [ ] Preregistered sustain gates (written before runs): whole-write ≥ 1.0 M ACK KV/s; every ACK-progress quartile mean ≥ 0.9 M; no 10 s window < 0.5 M; maintenance debt converges after drain (staged bytes/flush backlog/split+seed queues empty, zero RAM Busy / region-exhausted / drops / rejects); run reaches representative growth (≥ 50 auto-splits in the largest arm).
- [ ] Every run passes exact ACK/per-zone persisted counts, full-value samples with versions, cursor readback, clean owned stop, no new cores.
- [ ] Result is either a reproducible sustained ≥ 1 M run at the largest RAM-feasible size, or a measured decay curve with attributed limiter and exactly one next fix proposal. Below-target labelled target unmet.
- [ ] Evidence compacted under bench/0297-ingest-1m/long01/ with receipts; fleet stopped after every attempt.

## Work log

### 2026-09-26: harness, A/B ladder and diagnostics. Target unmet; the dominant limiter was attributed and fixed on the 0310 branch

- **Harness.** `bench/0297-ingest-1m/long01/`: real-client13 fixture client, run_long.py and analyze.py. Fixture: nested prefixes after 800M. A = 42.1M KV, B = 94.9M, C = 158.1M. All are creates.
- **A1.** 985k ACK KV/s, quartiles 1.29M → 0.84M.
- **B on master.** 417–469k, quartiles 1.1M → 0.24M, with about 50 auto-splits. Node1 took more than 300 s to stop and was SIGKILLed, leaving hugepage map files behind; those were cleaned up.
- **Diagnostics:**
  - With no splits: 794k.
  - Adding maintenance duty 100: 837k.
  - Adding the latency profile: 930k.
- **Attribution: seeding.** Every auto-split does a full O(store) AE/logical reseed on every shard store of the zone: 1.1 B folds per node against 95M keys.
- **Attribution: residual decay.** Three sources:
  - run-probe growth (runs 0 → 170 per node; compaction lane paced at 33%);
  - a device sync-latency step. Both drives were blkdiscarded with owner approval; lab-1 is fixed, lab-2 still steps, and it is not thermal;
  - the client's 800k in-flight window being pinned, so ACK tracks batch latency.
- **RAM.** Measured about 40 B/key on the capacity profile, so RAM is not the limit.
- **Fix.** Task 0310 branch `2660f6b1`, B8 against B9 on the same trimmed drives: **793k vs 463k (+71%)**. Seed folds went from 1.16 B to 0, with 0 mismatches and clean stops.
- **With the latency profile.** 0310 plus latency profile reached 902k.

Details are in FINDINGS.md and SUMMARY.json. Fleet is stopped, hugepages are 4087 free on both nodes, and the drives are on vfio.

### 2026-09-27 (later): D7–D12 — rehome refold fixed (0317), recount prefetch (0318); first run passing every sustain gate

- **D7 (1 s split poll diagnostic)** revealed two things:
  - The default 5 s poll capped splitting at about 0.4/s, so debt could never converge.
  - The same cap silently starved rehome.
- **Once rehome ran, each commit refolded every zone store** (121M folds, a 50–300k ACK trough). The cause was an order-sensitive `placement_equivalent`. The fix is **0317, merged**.
- **D9 (0317)** was the first D run whose debt converged, at 166 s. It ran at 826k because the synchronous split recount cost about 2.5 µs/key.
- **0318** (async prefetch + sync merge, **merged**) brought this down to 0.17 µs/key.
  - The ABBA gave 1.169×.
  - **D10: 1,010,109 ACK KV/s**, quartiles 1082/1008/994/965k, sliding-10 s minimum 860k, settle 171 s. It passes every preregistered sustain gate.
  - D11: 911k, missing on Q3/Q4.
- **Status: target unmet on a strict reading.**
  - Two runs passing are required.
  - The 1 s poll is still an env knob.
  - The R pair spread 10 %.
- **Next:** 0319 (default split cadence), then two default-config D runs.

### 2026-09-27 (evening): 0319 merged. The preregistered sustain claim is met twice on production defaults

- **Runs.** LONG01-D13/D14 on master code with default settings: 1,146,904 and 1,152,680 ACK KV/s over 400,661,663 KV.
  - Quartiles: 1292/1171/1109/1044k and 1323/1164/1096/1061k.
  - Sliding-10 s minimum: 960k on both.
  - Splits: 493 and 496. Settle converged in 21 s and 91 s.
- **Gates.** Every correctness gate and every sustain gate passes on both runs.
- **analyze.py.** The debt gate now uses settle.json when present.
- **Status.** "1M sustained on the largest arm, twice" holds for the D fixture, 400.7M KV. D is the largest fixture built so far. RSS is about 6.8 GB per node, so larger RAM-feasible runs (an E prefix, or longer) remain possible if the owner wants them.

### 2026-09-27: closed by the owner

The owner accepted D13/D14, which pass every sustain gate twice on production defaults. The longer bootes soak the owner asked for is tracked as a separate follow-up task.

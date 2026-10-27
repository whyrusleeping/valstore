---
id: "0303"
title: Review async group-commit flush + node-wide flush domain (latency/CPU headroom, throughput-neutral at client-link bound)
status: done
priority: 2
created: "2026-09-22"
updated: "2026-09-24"
depends_on: []
spec_refs: []
---

## Description
## Why

Task 0297 SCALE01: the group-commit leader spin-polled the shard qpair through each NVMe flush (33–38% of shard cycles in completion polling at 8 shards), and 8 shards x 2 zones queued up to 16 independent flushes per device (flush wall 1 -> 4–7 ms). Branches: `0297-async-group-commit` (`7cbbc1e9`: `vals_spdk_aio_flush`, cooperative wait, per-round waiter snapshot, leadership hand-off) and `0297-node-flush` (`72026f9f`, on top of gate 32: shards sharing a namespace share one flush domain; claim counter read after prepare decides coverage; ≤1 flush in flight per device; 2 s stall fallback). Debug suites pass on each (+3 tests).

Measured: ASYNC alone +3.3% (inconclusive band). NODEFLUSH at 930–941k ACK/s: device flushes 1.5–2x fewer, flush wall 5.3–5.7 -> 2.8–3.2 ms, response-ready 13–15 -> 8.5–10 ms, shard CPU ~63 -> ~55%, throughput equal (client egress bound). Evidence `bench/0297-ingest-1m/scale01/FINDINGS.md` §4, §7.

## Decide

Worth adopting only if a regime where the server is the bound (after client-egress reduction, or more client NICs) shows throughput or tail-latency gain in a matched A/B; otherwise per cleanup doctrine keep off master. Needs independent review of the NVMe cross-queue flush-coverage argument and of cancellation/shutdown paths (stall fallback, published completions after submitter cancellation).

## Acceptance criteria

## Work log

### 2026-09-24 — AB01 matched A/B on the dual-link client: ACCEPT (throughput); review says adoptable with fixes

Entry point `bench/0303-flush-domain-ab/FINDINGS.md` (prereg `README.md`, `SUMMARY.json`, `evidence/`). Six attempts G,N,N,G,G,N, all PASS. G = GATEONLY `8f2b4649` ≡ master; N = NODEFLUSH `72026f9f`. N averaged **1,352,952 vs 1,185,058 ACK KV/s (+14.2 %)**; per-second p50 +12.4 %; min N > max G; gate sheds 0; domain stalls 0; RSS 0.91×. That meets the preregistered accept band. Mechanism: about 2.7× fewer device flushes, response-ready 25–27 → 19–22 ms, PutBatch 88–91 → 74–77 ms, with the in-flight window fixed. Neither arm is link-bound.

Independent read-only review found no durability bug: cross-queue coverage, waiter hand-off and stall fallback are all sound. Fixes required before merge: (1) a cancelled leader must not fail its batch and successors (re-lead instead); (2) drain async/domain flushes before device close (the shim aborts on in-flight operations); (3) recover a stuck domain `inflight` flag when the owning shard drops or stalls. Tests required: an async-completion coverage oracle, stall + cancel across shards, and a shutdown drain. Next: implement the fixes and tests on `0297-node-flush`, rerun the debug suites, then merge.

### 2026-09-24 — fixes implemented, re-reviewed, hardware-confirmed; MERGED `b74f5e30`

Branch `0303-flush-domain` (72026f9f + master) with `69c81342` (leader hand-off on cancel, abandoned-claim recovery, bounded shutdown flush drain), `6f3f1939` (coverage-oracle mock + 7 tests, mutation-checked), `4d75cde2` (read-only inflight check), `1d613371` (docs/batch-writes.md + ARCHITECTURE.md). Re-review verdict: merge, no durability issue; oracle test fails under a claim-before-prepare mutation. Low-severity follow-ups filed as proposed 0305.

Hardware: FIX01 (2× N2) passed every gate, and the clean SPDK stops exercised the drain, but it missed the preregistered throughput bounds (1.17 M / 1.27 M), which blocked the merge. FIX02 interleaved old/fixed builds: N mean 1,290,075 vs N2 1,250,784 (0.970×, best run N2), all PASS. That meets the "no regression" band. The old build dipped the same way, so the FIX01 shortfall is run-day drift; a ~3 % effect can be neither confirmed nor excluded at n=3. Merged into master with `--no-ff` as `b74f5e30`. The main-tree overlay was reapplied 3-way with identical +/- lines and identical status. Debug suites pass with the overlay: vals-cluster lib 647 (+6 ignored) plus integration, vals-store 312, vals-client 150, vals-spdk 9. Evidence `bench/0303-flush-domain-ab/`. Fleet stopped clean, no new cores, lab-3 untouched.

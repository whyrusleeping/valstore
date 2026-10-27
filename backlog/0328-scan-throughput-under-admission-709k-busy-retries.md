---
id: "0328"
title: 'Scan throughput under admission: 709k Busy retries, 148k keys/s census at 1.4B keys/node'
status: done
priority: 4
created: "2026-09-28"
updated: "2026-10-12"
depends_on: []
spec_refs: []
---

## Description

Follow-on from 0325 (SOAK0321-F4/reverify0325b). The per-shard scan admission gate (VALS_SCAN_MAX_INFLIGHT=8) fixed the census completion: the exact census PASSED. But throughput was worse. The census took 14,131 s at 148k keys/s. It had 709,451 Busy retries against 1,157 page retries, and `vals_scan_admission_busy` reached 426k/197k/329k on nodes 1/2/3. The earlier failed run averaged about 196k keys/s. Some minutes still showed very high per-minute scan latency (node1 57.9 s/scan).

Measure before designing. Two directions:
- **Client pacing.** The census client uses a fixed 500 ms Busy retry with 48 workers. Consider a per-node concurrency budget, or jittered or adaptive backoff.
- **Server.** Look at the gate size, the page budget, and the per-scan cost at about 185 resident runs.

Acceptance: a short, matched retained-data census A/B that reports keys/s, Busy retries and the scan latency distribution, with a keep/remove decision.

## Outcome

Matched retained-data census A/B done on corpus K8 (read-only reopen on its own binary c57e90ed, corpus stays pristine), using a new census fixture for exactly K8's block-aligned prefix (594,685,041 events). Four full verify arms in ABBA order, all PASS on exact count and digest. A (48 workers, key order): 1.42M keys/s, 34.2k client Busy. G (24 workers, shuffled): 1.90M keys/s (1.34x), 14 Busy (about 2,500x fewer), 16% fewer server scans. Decision: keep `--census-order shuffled --census-workers 24` for censuses. verify defaults are unchanged, and no server gate or page-budget change is justified. Results are in bench/0328-scan-pacing/README.md (L2) and results/l2-k8.

Commit: bench(0328): L2 census A/B on corpus K8 — keep shuffled order with 24 workers (1.34x keys/s, ~2,500x fewer Busy, exact census in all ABBA arms)

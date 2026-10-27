---
id: "0400"
title: Bound and observe worst-engine key-filter chain FPR so EOF absent-Apply ≤ 0.05 reads is provable (split from 0398 criterion A)
status: done
priority: 2
created: "2026-10-07"
updated: "2026-10-08"
depends_on:
    - "0398"
spec_refs: []
---

## Description

Split from 0398 criterion A (see bench/0398-ram-hardware/REPORT.md, "per-swap chains and EOF permit queue"; receipt /data/dataset/task0398-results/analysis-3-v1/filter-chains-v2.json).

## Outcome

Rebuild permits now go to the highest-FPR engine first, with a bounded urgent extra at FPR ≥25000 ppm. Seven new worst-engine gauges are documented and sampled. RAMAB-KF2 (5B ACK, correctness PASS): - Worst measured FPR 2.38%, which is 0.031 extra reads. - Projected at EOF (8 saturated subfilters): 0.043 reads, PASS. The +2-subfilter worst case gives 0.053, FAIL. - EOF MemAvailable 9.43 GiB, vs 8.92 for 0398's trim-off run; the burst case is 7.19 GiB, still short of 8. Throughput, pooled with KF2-R2: 1.055/1.027 vs CUT, PASS. The stricter NOTRIM pool fails at 0.893/0.965, from lab-2 RAM fail-fast outages; rebuild and scan work matched NOTRIM. The subfilter-count trigger (KF arm) cost throughput, so it was dropped. Report: bench/0400-kf-chain/REPORT.md. 0386 is handed off and stays on HOLD for the RAM burst risk (0399).

Commit: feat(0400): rebuild permits to the highest-FPR engine first, plus a bounded FPR-triggered urgent extra; worst-engine key-filter gauges; 3 hardware arms (KF, KF2 to 5B, KF2-R2); read gate primary PASS, handoff to 0386

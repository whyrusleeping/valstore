---
id: "0398"
title: Hardware-confirm per-key RAM cuts (0395–0397) at ≥1B ACK and re-project the 0386 EOF RAM gate (MemAvailable ≥ 8 GiB, absent-Apply ≤ 0.05 reads, no ACK regression)
status: done
priority: 2
created: "2026-10-07"
updated: "2026-10-07"
depends_on:
    - "0395"
    - "0396"
    - "0397"
spec_refs: []
---

## Description

This task carries the end-to-end acceptance that was originally on 0395, now split into 0395, 0396 and 0397.

## Outcome

11 RAMAB hardware arms (PRE 7da018b1, NOTRIM and CUT 26053c4e; two to 5B ACK). The registered trim-on throughput test FAILED: CUT/PRE whole-run mean 0.944, median 0.961. Trim-off passes the same thresholds, compared after the fact: 1.072 / 1.012. Trim-on arms also had more lab-2 crashes, and trim calls took 290–420 ms, so periodic malloc_trim is now off by default (VALS_MALLOC_TRIM_SECS to opt in). The trim-off EOF projection passes narrowly at 8.92 GiB MemAvailable, but a burst sensitivity gives 7.08 GiB, so 0399 is the next RAM mitigation. The worst-engine FPR gate is split to 0400, and 0386 now depends on it. Handoff is in bench/0398-ram-hardware/REPORT.md and SOAK5B-V5-README.md.

Commit: bench(0398): hardware RAM A/B of 0395–0397 cuts (11 RAMAB arms on lab-1/2/3), measured EOF reprojection, periodic malloc_trim off by default; split worst-engine FPR gate to 0400

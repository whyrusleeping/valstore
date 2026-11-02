---
id: "0546"
title: Two-node (lab-1+3) replay profile and a single-box commit-path microbenchmark while lab-2 awaits ECC RAM
status: done
priority: 2
created: "2026-10-31"
updated: "2026-11-02"
depends_on: []
spec_refs: []
---

## Description

Owner 2026-10-31: lab-2 is out until its RAM is replaced with ECC, so benchmark on lab-1 and lab-3 only.

## Outcome

N2 baseline (86983dc) PASS, 4,290 s to 4.5B. commitwrite reproduces the post-step regime and ranked FT (FUA + async commit tail, no flush) +75..86%, FUA +31%, async tail +10..16%, two flushes -7%. Top candidate FUA0546 (branch wip-0546-fua d4daa3e4) rejected on N2-R2: 1.566/1.439/1.399/0.967M per 0.5B vs 1.732/1.626/1.558/1.445M; correctness PASS at 2.31B STOP, 0 crashes. Isolated bench over-predicted FUA (server FUA drain 10.5 ms under merge traffic). commitwrite gains VALS_BENCH_FLUSH_DEPTH / VALS_BENCH_FLUSH=0. Follow-up 0562 proposed.

Commit: 0546: commitwrite flush-depth/no-flush knobs; commit-path candidates ranked on lab-1; top candidate FUA0546 (FUA value-log writes + async tail, no flush) rejected on N2; proposed 0562

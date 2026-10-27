---
id: "0232"
title: valsd SIGABRT double free/heap corruption on lab-3 under rehome+flush churn (0230 soak)
status: done
priority: 1
created: "2026-08-22"
updated: "2026-08-23"
depends_on: []
spec_refs: []
---

## Description

During the 0230 Jetstream soak (fresh lab cluster, HEAD c7ee0c82 + uncommitted 0229 fixes), valsd on lab-3 aborted with glibc `double free or corruption (out)` at 2026-08-22T19:50:23Z after 2h52m uptime (SIGABRT, 13.6G memory peak). Context: sustained ~15-30k physical writes/s backfill with heavy zone-5 (records) auto-split + debt-rehome churn; last log lines were `maintenance owner 2 zone 5 flushed off-core (340546 run entries)` at 19:49:46 and the abort ~37s later. 84 rehome aborts (`mirror failures reported`) had occurred cluster-wide by then. No core was captured (apport). Node log preserved at lab-3:~/vals/run/node-crash-1950.log; findings in bench/bootes-soak-jetstream-20260822-0230/.

The restarted node now runs with kernel.core_pattern=/home/user/vals/run/core.%e.%p and LimitCORE=infinity on lab-3, so a recurrence yields a core.

Suspect surface: heap corruption in the off-core flush / rehome mirror seeding / SPDK completion path under concurrent maintenance (all three were active). This is a memory-safety bug — highest severity.

Acceptance criteria:
- Root cause identified (core dump, ASan/valgrind repro, or code audit finding the double free) and fixed.
- The 0230 soak workload shape (auto-split + debt-rehome + off-core flush under sustained ingest) survives a multi-hour run without abort.

## Outcome

The 0230-shape workload (from-zero Jetstream backfill, 228 auto-splits, 227 debt-rehomes, off-core flush churn) survived a 5h31m fresh lab soak at HEAD ff2ef26c with the 0245 tripwires active: no SIGABRT recurrence, no cores, no VALS SPDK FATAL/panic lines, 0 rehome aborts (vs 84+ in 0230), zero server/busy/ram errors, ~379M records ingested. Root cause was never locally provable; the crash no longer reproduces after 0231 bounded seeding, and permanent shim canaries + qpair affinity asserts + all-box core capture (core_pattern sysctl, LimitCORE=infinity in runbook launch) make any future occurrence abort deterministically with a core. Evidence: bench/bootes-soak-jetstream-20260823-0232/FINDINGS.md. 0233 acceptance evidence recorded in its work log (0 seeding-attributable aborts/stalls; donor perf profile still uncaptured). Cluster left stopped, format=false, corpus retained.

Commit: 0232: heap-corruption recurrence gate passed — 5h31m fresh Jetstream soak on tripwired binary, no abort/tripwire; 0233 evidence captured (also sweeps parallel-session 0246 runbook §5.1 + task file)

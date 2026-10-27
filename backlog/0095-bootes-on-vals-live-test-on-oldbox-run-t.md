---
id: "0095"
title: 'Bootes-on-vals live test on oldbox: run to 1B records, profile + record hot spots, capture replayable dump'
status: done
priority: 2
created: "2026-07-08"
updated: "2026-07-09"
depends_on: []
spec_refs: []
---

## Description
User-requested box run (2026-07-09):

- Bring up the vals-examples/backfill harness (4-node SPDK, RF=1, AE off) on oldbox with current HEADs of vals/vals-examples/bootes.
- Run bootes full-network backfill + firehose until cluster-total vals_records_written >= 1e9 (~2h at prior-run rates).
- Grab profiles along the way (harness periodic profiler + manual milestone grabs ~100M/500M/900M); analyze perf + pprof, record hot spots with detail in a bench/ notes file in the vals repo.
- At 1B: stop bootes, graceful cluster stop, TRIM-clean offline `valsd dump --parallel` per node to the box OS RAID (~/run/), sanity-check counts vs records_written, keep the dump on-box for a later replay benchmark.

Acceptance:
- [ ] >=1B records ingested, run timeline + throughput recorded
- [ ] hot-spot analysis written up (bench notes committed)
- [ ] per-node dumps captured on box, record counts reconciled vs counters
- [ ] box left clean (no valsd/bootes running, drives accounted for)

## Acceptance criteria

## Work log

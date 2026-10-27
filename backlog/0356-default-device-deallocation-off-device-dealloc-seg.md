---
id: "0356"
title: Default device deallocation OFF (device_dealloc_segments_per_tick=0) until 0355 clears runtime DSM
status: done
priority: 1
created: "2026-09-30"
updated: "2026-09-30"
depends_on:
    - "0350"
spec_refs: []
---

## Description

Safety mitigation from 0353 (bench/0353-dealloc/FINDINGS.md). 0350 ships provision-time and runtime NVMe DSM deallocation ON by default (`device_dealloc_segments_per_tick` = 256). On hardware:
- The matched OFFa→ONa→ONb→OFFb A/B showed **no throughput benefit** (ON−OFF −0.62% / −8.10% in 30–350M / 150–400M).
- The only runtime-dealloc long arm **failed correctness**: an exact backlinks census found 3 extra keys; node3 logged two `corrupt snapshot: index crc mismatch` off-core compaction failures; there were DSM errors (2 / 1) and a node1 `nvme write: Cannot allocate memory` flush failure.

The cause is not yet isolated (0355). Until it is, the default must not risk data.

## Outcome

device_dealloc_segments_per_tick now defaults to 0: no provision-time or runtime DSM and no dealloc tracking unless explicitly set nonzero (opt-in behaviour unchanged). node_settings default test asserts 0; valsd --check-config daemon test uses a config omitting the key and asserts effective 0. docs/deploy.md, docs/configuration.md and ARCHITECTURE.md say off by default, experimental pending 0355, citing bench/0353-dealloc/FINDINGS.md. vals-server tests and vals-cluster settings/config tests pass in debug. Self-review.

Commit: fix(0356): default device_dealloc_segments_per_tick to 0 (provision + runtime NVMe DSM off) pending 0355; check-config test asserts omitted-key default; docs mark opt-in experimental

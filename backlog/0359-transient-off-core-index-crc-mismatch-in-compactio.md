---
id: "0359"
title: Transient off-core `index crc mismatch` in compaction/flush jobs (pre-0350; split from 0355)
status: done
priority: 1
created: "2026-09-30"
updated: "2026-09-30"
depends_on: []
spec_refs: []
---

## Description

Split from 0355 (coordinator, 2026-10-01). This is the index-CRC half of the 0353 LONG correctness investigation; 0355 keeps the census (+3 backlinks keys) half and the overall explanation.

## Outcome

No root-cause defect isolated locally. The audit excluded free/reuse of segments during off-core jobs, range offset math and SPDK buffer lifetime; an output overwrite or host/device read fault is still possible. Diagnostics landed as the acceptance alternative. The first 32 index CRC mismatches per process, at site=open (DeviceRun::open, merge inputs) or site=readback (read_run_parts, compaction/flush output), re-read the index twice and log one WARN line: segment ids/LBAs, geometry, CRCs, a transient/persistent/unstable verdict and the diff location. The original error is unchanged. Regression tests and the vals-store/vals-spdk suites pass. Not run on hardware; the next occurrence will show the phase and whether the fault is transient or on the device.

Commit: diag(0359): attribute run index CRC mismatches (site open/readback, segment LBAs, CRCs, fresh re-read transient/persistent/unstable verdict and diff location); record audit of excluded mechanisms

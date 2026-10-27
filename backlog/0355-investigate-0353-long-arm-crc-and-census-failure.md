---
id: "0355"
title: Investigate 0353 LONG CRC, DSM errors and backlinks census mismatch; preserve both replicas
status: done
priority: 1
created: "2026-09-30"
updated: "2026-10-01"
depends_on:
    - "0350"
    - "0360"
spec_refs: []
---

## Description

0353's full 2.09B ACK replay on lab-1/3, server 57234ef6 / SHA 9a99d002…, completed a zero-loss drain but FAILED the hardware correctness gate. The exact records census matched; backlinks returned 1,090,186,121 keys instead of 1,090,186,118, with a different digest. All 4,489 normalized value/version samples matched, which does not override the census failure. During the same arm node3 logged two `corrupt snapshot: index crc mismatch` compaction failures, node1 logged a retried flush commit `nvme write: Cannot allocate memory`, and DSM error counters ended at 2 / 1. There were no crashes/restarts/new cores. No cause has been isolated; do not attribute this to DSM, the verifier, or hardware without evidence.

Source of truth: `bench/0353-dealloc/FINDINGS.md`. Raw receipts: `/data/dataset/task0353-results/DEALLOC0353-LONG/` (`verify.json`, `correctness-health.json`, observer/threads/commands, full node logs), `DEALLOC0353-LONG-analysis.json`, and `STATE-DEALLOC0353-LONG-readback/`. Both on-box runtimes and matching binaries are under `/home/user/vals0353-dealloc/DEALLOC0353-LONG/`; PD state is on lab-1. The drives are stopped, vfio-bound, with 4096/4096 hugepages free. The retained long corpus was not reprovisioned after the failure.

## Outcome

Read-only keyscan KS0355-B on lab-1/3 (pin 49020b62; lab-2 never contacted; DUW/NSZE/NCAP unchanged; NUSE only fell, recorded as 0353 DSM settlement) explains the 0353 census failure. Node1's backlinks and both nodes' records match the expected sets exactly. Node3's partition 7 run 10 has 3 index keys with byte 73 changed 0x3a→0x2a (at:// → at*//) in place of 3 expected keys. Each points to a CRC-valid record holding the correct key. The union reproduces the verifier's 1,090,186,121 keys and its delta. The altered keys are adjacent in sorted order, pointing to one index-path corruption before run 10 was built. Its origin (memory fault vs software) and the CRC events are split to 0361 (p1); 0357 depends on it. 0353 stays failed and the corpus stays held. Evidence: bench/0355-keyscan/FINDINGS.md.

Commit: bench(0355): read-only keyscan of held 0353 corpus identifies the +3 backlinks census keys (node3 single-bit run-key alteration, records intact); runbook loader/gate amendments; split origin + CRC attribution to 0361 (0357 depends on it)

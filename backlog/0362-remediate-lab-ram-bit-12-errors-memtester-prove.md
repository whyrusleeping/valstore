---
id: "0362"
title: Remediate lab RAM bit-12 errors (memtester-proven) and re-qualify boxes before correctness-gated runs
status: done
priority: 1
created: "2026-10-01"
updated: "2026-10-12"
depends_on:
    - "0361"
spec_refs: []
---

## Description
Split from 0361. On 2026-10-01 a 6 h memtester run (8×4400M per host) found uncorrected single-bit RAM errors on both tested boxes:
- **lab-3:** 22 failures, all on bit 12 of the 64-bit word, i.e. XOR 0x10 on the byte at address ≡1 mod 8.
- **lab-1:** 3 failures on bit 12 and 1 on bit 9.

Both boxes have the same hardware: Core Ultra 9 285HX and 2×32 GB non-ECC DDR5 rated 5600 and configured 4400, with no EDAC driver. This matches node3's 0355 key alteration (XOR 0x10). Evidence: `bench/0361-attribution/FINDINGS.md`, receipts in `/data/dataset/task0361-memtest/`. Correctness-gated runs on these boxes are untrustworthy until this is fixed.

## Needs the owner (physical/BIOS access)
Try remediation in this order, one change at a time, re-testing after each:
1. Check the BIOS/firmware memory settings: XMP/EXPO profile, voltage, training, and fixing the speed lower.
2. Update the BIOS.
3. Reseat or swap DIMMs between boxes to see whether the fault follows the DIMM or the board.
4. Replace the DIMMs.

Lab-2 should also be tested once it is released from 0248 evidence hold. Its drive and data must be preserved.

## Acceptance
- Each box that will host correctness-gated runs (at least lab-1 and lab-3) passes at least 6 h of memtester (same `run.sh`, 8 instances, or more coverage) with zero failures after remediation.
- The remediation is recorded in the findings.
- Scratch drives stay vfio-bound and untouched during the tests, and the held 0353 corpus is preserved unless the owner releases it.
- 0357 must not run before this is done.

## Acceptance criteria

## Work log
- 2026-10-12 (owner): closed as won't-fix. The RAM is not being remediated, and faults on this hardware are not to be chased (see 0248).
- 2026-10-06 (owner): no DIMM purchase for now (RAM is expensive), and no further memory testing of lab-2: it is known bad (V4, V4-R2 and V4-R3 all stopped on lab-2 faults while lab-1/3 ran clean). Stays blocked on budget. Soaks proceed on the current RAM, relying on the 0370/0375/0384 in-RAM and encode checks plus RF2 replicas.

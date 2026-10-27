---
id: "0135"
title: Decide how to quiet the fleet-wide PCIe AER storm (Thunderbolt/USB4 root port)
status: done
priority: 4
created: "2026-08-06"
updated: "2026-08-11"
depends_on: []
spec_refs:
    - plans/box-provisioning-runbook.md
---

## Description
Split out of task 0131 (lab-2 kernel-Oops autopsy), which is otherwise closed.

All three lab boxes emit a chronic PCIe AER storm (~24 msg/s; on the wedged boot 2.38M `device recovery failed` + xhci/thunderbolt `can't recover` ≈ 4.9M of 8.41M kernel lines). It is unrelated to the MGLRU Oops but it floods the journal, burns CPU, and buries real errors during soaks.

The targeted fix — mask `ACSViol` on `0000:80:1b.4` (the Thunderbolt/USB4 root port; both NVMe drives verified to sit on other root ports) — is blocked: SecureBoot is on ⇒ kernel lockdown `integrity` ⇒ `setpci` denied.

Remaining options, all with a cost, so this needs an owner decision rather than effort:
- `pci=noaer` fleet-wide (loses PCIe error visibility on the NVMe drives)
- `pcie_ports=compat`
- disable SecureBoot, then mask `ACSViol` from a boot-time unit (keeps NVMe AER visibility; weakens boot integrity)
- BIOS-disable Thunderbolt (needs physical access; cleanest)

Acceptance criteria:
- Decision recorded (which option, or explicitly live with the noise).
- If applied: persisted across reboots on all three boxes, journal rate verified back to normal, NVMe error reporting behaviour documented in plans/box-provisioning-runbook.md.

Evidence: `bench/bootes-soak-lab/run-20260804T220428Z-batch1000/node2-wedge-autopsy/`.

## Acceptance criteria

## Plan

1. Reconfirm the recorded AER evidence, PCI topology, Secure Boot/lockdown constraint, and operational trade-offs; use live read-only host checks only if they are readily available and safe.
2. Make the least-destructive explicit decision: retain Secure Boot and NVMe AER visibility, reject fleet-wide `pci=noaer` and `pcie_ports=compat`, and disable the unused Thunderbolt/USB4 controller in BIOS during the next attended maintenance. Until physical access, explicitly accept the noise rather than weaken storage diagnostics or boot integrity. If the BIOS exposes no suitable disable control, document targeted `ACSViol` masking after disabling Secure Boot as a fallback requiring a separate explicit decision, not an automatic action.
3. Update `plans/box-provisioning-runbook.md` with the decision, rationale, current pending/not-applied state, per-host attended application procedure, reboot checks, journal-rate verification, and confirmation that NVMe AER remains enabled. Cross-reference the existing autopsy evidence rather than duplicating it.
4. Verify the documentation is internally consistent, commands are safe/read-only unless clearly labeled as BIOS actions, markdown formatting is sound, and the task acceptance criteria are explicitly met without claiming fleet application that did not occur.

### Starting points
- plans/box-provisioning-runbook.md: fleet inventory, boot-time provisioning, and open-items sections
- bench/bootes-soak-lab/run-20260804T220428Z-batch1000/node2-wedge-autopsy/FINDINGS.md:77-143 documents rate, topology, lockdown failure, and alternatives

## Work log
- 2026-08-11 plan: 1. Reconfirm the recorded AER evidence, PCI topology, Secure Boot/lockdown constraint, and operational trade-offs; use live read-only host checks only if they are readily available and safe. 2. Make t
…[truncated]
- 2026-08-11 context hints: 2 recorded with plan
- 2026-08-11 context hints: The accepted outcome may be a documented decision without immediate application because the acceptance criteria say “If applied”; do not claim the storm is fixed unless verified on every host.; Ke
…[truncated]
- 2026-08-11 preload: 2 file(s), ~21 KiB seeded into implementer context
- 2026-08-11 implementer report: Updated `plans/box-provisioning-runbook.md` to record task 0135’s explicit decision: retain Secure Boot and NVMe AER visibility, reject `pci=noaer`/`pcie_ports=compat`, and BIOS-disable the unused T
…[truncated]
- 2026-08-11 review tier: single-opus — reviewers: sol
- 2026-08-11 review (sol): revise — The decision itself is clear, appropriately conservative, and accurately marked as pending rather than applied. The runbook also covers the intended one-host-at-a-time workflow and reboot/journal chec
…[truncated]
- 2026-08-11 revision: Revised the task 0135 runbook checks for stock Ubuntu hosts: removed every `rg` dependency, replaced both non-portable `awk IGNORECASE` filters with POSIX `tolower($0)` matching, and changed the suppr
…[truncated]
- 2026-08-11 review (sol): accept — The revision addresses the prior operational defects: all checks now use stock utilities, topology matching is portable across Ubuntu awk implementations, and failure to inspect the kernel command lin
…[truncated]
- 2026-08-11 decision: accept — commit: docs: decide PCIe AER storm handling
- 2026-08-11 usage: 1,196,703 tok (in 615,098, out 23,269, cache_r 558,336, cache_w 0) · cost n/a (unpriced)
  implementer: 584,120 tok (in 289,738, out 13,294, cache_r 281,088, cache_w 0) · cost n/a (unpriced)
  coordinator: 421,674 tok (in 180,805, out 4,325, cache_r 236,544, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 190,909 tok (in 144,555, out 5,650, cache_r 40,704, cache_w 0) · cost n/a (unpriced)

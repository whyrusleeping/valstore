---
id: "0250"
title: Recurring PCIe AER storm (thunderbolt/xhci @ 80:1b.4) wedges/reboots lab boxes under load
status: done
priority: 2
created: "2026-08-26"
updated: "2026-10-12"
depends_on: []
spec_refs: []
---

## Description
Two identical hardware failures during the task-0247 record run, on two different lab boxes:
- lab-2: 2026-08-25 04:03Z — AER storm `pcieport 0000:80:1b.4: device recovery failed` + `thunderbolt 0000:84:00.0` / `xhci_hcd 0000:97:00.0` "can't recover", box wedged → rebooted 04:04:23. Node was down ~40h (nobody restarted; SPDK prep gone post-reboot).
- lab-1: 2026-08-26 ~19:53Z — same signature, same root port/devices; PD+node died silently mid-write (logs cut, no cores), box rebooted 19:55:36, **storm continued after reboot** (still logging 19:58).

Same PCI IDs on both boxes (identical platforms) — thunderbolt/xhci behind root port 80:1b.4, unrelated to the SPDK NVMe or NIC data path. Both occurred under heavy ingest, ~40h apart. Impact: silent node loss under RF2 → W=2 write failures, batch drops, rehome abort storms.

Investigate/mitigate:
- Check BIOS/firmware for the TB/USB controller; consider disabling thunderbolt/USB4 in BIOS on all three boxes (nothing on the data path uses them).
- Alternatively kernel quieting: `pci=noaer` is too broad; per-device AER masking or `pcie_ports=compat` worth evaluating.
- Ops mitigation until then: monitoring should alert on node metrics endpoint disappearing; recovery = SPDK re-setup + re-set core_pattern/panic_on_oops sysctls + relaunch (runbook §0 preconditions), affinity check off for format=false boots (0249).

## Acceptance criteria

## Work log
- 2026-10-12: closed as won't-fix, per the owner's decision not to chase faults on the lab hardware (see 0248). The controller-specific BIOS disable in `plans/box-provisioning-runbook.md` remains the fix if someone is ever at the console.

- 2026-09-29: retained at P2, still blocked on attended maintenance. The prior approved controller-specific BIOS approach in the runbook supersedes the speculative masking alternatives above; do not disable diagnostics globally or infer reboot/firmware authorization from backlog acceptance.

- 2026-09-05 unattended triage: blocked pending an attended maintenance window/local-console operator to apply and verify the controller-specific Thunderbolt/USB4 BIOS disable. `plans/box-provisioning-runbook.md` §PCIe AER storm decision already records the approved approach, rejects global AER suppression and disabling Secure Boot, and requires two post-change boots per host; all fleet ledger rows remain pending. Cannot responsibly reboot/change firmware during the ongoing 0248 investigation unattended. No host configuration changed in this triage.

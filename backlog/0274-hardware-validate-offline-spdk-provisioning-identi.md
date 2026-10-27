---
id: "0274"
title: Hardware-validate offline SPDK provisioning identity and exclusivity safeguards
status: done
priority: 2
created: "2026-09-08"
updated: "2026-10-06"
depends_on:
    - "0240"
spec_refs: []
---

## Description

Task 0240 removes restart-time formatting and adds `valsd provision`; local tests and SPDK feature type-checks pass, but the changed C serial shim and real device ownership behavior were not exercised in an SPDK-linked build. On an explicitly approved disposable device, build with VALS_SPDK_LINK=1, verify serial mismatch and competing daemon/provision ownership reject without layout writes, provision with exact identities/confirmation, and prove persisted data survives repeated ordinary daemon restarts. Check both supported layout topologies where hardware permits. Preserve existing fleet data and forensics unless a separate wipe authorization applies; do not infer permission from this proposed task.

Acceptance: record linked-build provenance and bounded hardware evidence for identity rejection, exclusive ownership, successful provisioning, and data-preserving restarts.

## Outcome

PASS on lab-3 9100 PRO 0000:01:00.0 (serial independently read via sysfs/id-ctrl) with an SPDK-linked valsd built on-box from 3ff851c0 (sha b7249630…, vals_spdk_ctrlr_serial linked). Identity rejections (wrong/prefix/lowercase serial, wrong confirm, missing/extra/wrong-pci device) on both topologies; bracketed by SMART host_write_commands Δ0 and unchanged head-256MiB hash (positive control: provision Δ+16,408). Exclusivity: provision and second daemon rejected by flock while a daemon serves; with flock bypassed (private /run/lock) vfio/SPDK attach still refuses. Both 8-partition and single-partition topologies provision and keep all 400k keys across 3 graceful restarts each plus a SIGKILL restart. Defect found: a config with a different listen count/topology is not refused — 8-listen over single-partition layout opened stale partition superblocks, wrote, and lost 2,304/400k acknowledged keys; filed P1 task 0392 and added a deploy.md caveat. Evidence: bench/0274-provision-hardware/.

Commit: bench(0274): hardware-validate offline SPDK provisioning (identity, exclusivity, restarts, both topologies); file wrong-topology data-loss defect 0392

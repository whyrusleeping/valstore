---
id: "0337"
title: SPDK hugepage map files (100–290/node) remain after clean valsd exit and pin hugepages until manual cleanup
status: done
priority: 3
created: "2026-09-29"
updated: "2026-10-12"
depends_on: []
spec_refs: []
---

## Description

Observed during 0326 (bench/0326-shutdown/FINDINGS.md). The node logs a clean `graceful shutdown complete` and the systemd unit result is success, yet /dev/hugepages/spdk*map_* files remain after the process exits: counts were 92–291 per node, and HugePages_Free was 3994–4076 of 4096. Every restart therefore needs the runbook's guarded cleanup, which deletes the maps only once `pgrep -x valsd` is empty.

Possible cause: the SPDK env is never finalized (spdk_env_fini / unlink of hugepage files) at exit, or it runs with a no-unlink memory mode.

Acceptance:
- A clean valsd exit leaves no map files and all hugepages free, verified on one lab box.
- Alternatively, record why this is inherent and fold the cleanup into the unit's ExecStopPost.

## Outcome

Fix was 0424 (shim sets unlink_hugepage). Verified on valsd hardware: SIGTERM leg in 0418 (0 maps, 4096/4096 free on all 3 hosts); crash/exit-75 + systemd Restart=on-failure leg taken from 0386 R4's 8 restarts (SIGSEGV/SIGABRT/exit 75), every wrapper found 0 maps and stops left 4096/4096 free. Watchdog _exit(75) path covered by the 0424 shim box check only. docs/deploy.md, box-provisioning runbook and validation.md updated; 0405 no longer carries the 0337/0372-item-6 leg.

Commit: docs(0337): close hugepage-map task — valsd legs confirmed (0418 SIGTERM; 0386 R4 crash/exit-75 systemd restarts all found 0 maps, 4096/4096 free); deploy.md restart bullet, runbook clean-stop note, validation row

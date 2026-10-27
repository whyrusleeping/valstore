---
id: "0424"
title: '0337 software fix: valsd SPDK env unlinks hugepage files at creation so no spdk<id>map_* outlives any exit path'
status: done
priority: 3
created: "2026-10-11"
updated: "2026-10-11"
depends_on: []
spec_refs: []
---

## Description

Split from 0337 (whose acceptance needs the valsd hardware legs in 0418 step 7 / 0405 pre-ingest, both of which occupy the R3 drives). The fix must land before 0418 so its clean-stop count verifies it rather than recording a baseline.

Cause: valsd passes `shm_id = node_id`, and SPDK only auto-adds DPDK `--huge-unlink` when `shm_id < 0`. In multi-process mode DPDK keeps the `spdk<id>map_*` hugetlbfs files; `rte_eal_cleanup` (already run by SPDK's destructor on `exit`) does not unlink them, and `_exit`/kill -9 skip it anyway. The tools that use `shm_id = -1` (dump/keyscan/provision) already leave no maps.

Fix: set `opts.unlink_hugepage = true` in `vals_spdk_env_init` (DPDK `--huge-unlink=always`: unlink after ftruncate, before mmap). The kernel then frees the pages when the process exits on any path. Nothing attaches to valsd as a DPDK secondary process.

Acceptance:
- Shim sets unlink_hugepage; the box shim builds.
- One-box (lab) mechanism check with the real shim code, no NVMe attach and no retained-drive access: before vs after map-file count and HugePages_Free after clean exit, SIGTERM, `_exit(75)` and kill -9.
- Docs updated (SPDK.md / validation.md); the 0337 work log records what remains (the valsd legs in 0418/0405, and the docs/deploy.md:1063 paragraph, which holds another task's uncommitted hunks).

## Outcome

vals_spdk_env_init sets opts.unlink_hugepage (DPDK --huge-unlink=always). Root cause: named shm_id (default node id) suppressed SPDK's implicit --huge-unlink; rte_eal_cleanup already runs at exit and does not unlink. lab-1 shim-only check (bench/0337-hugepage-maps): old shim 400–401 maps / 3695–3696 free after return, SIGTERM, _exit(75), kill -9; new shim 0 maps / 4096 free on all four. SPDK.md and validation.md updated; 0337 stays todo for the valsd SIGTERM leg (0418 step 7), kill-9/exit-75 leg (0405) and the deferred docs/deploy.md paragraph.

Commit: fix(0424): SPDK shim sets unlink_hugepage so valsd leaves no spdk<id>map_* files on any exit path; lab-1 shim A/B (old 400 maps, new 0); 0337 keeps valsd legs + deploy.md

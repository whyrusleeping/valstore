---
id: "0131"
title: Autopsy lab-2 host-wide wedge from long bootes soak
status: done
priority: 1
created: "2026-08-05"
updated: "2026-08-06"
depends_on: []
spec_refs:
    - plans/bootes-soak-runbook.md
---

## Description
During task 0130, lab-2 entered a host-wide partial wedge around 2026-08-05 04:58–05:02 UTC: vals :7001–7004 silently drop SYNs, SSH completes TCP handshake but never emits a banner, and a pre-existing `systemctl show` session hung indefinitely. The vals metrics thread on :9464 remains responsive but all counters/gauges are frozen (`records_written=72,078,111`); `graceful_shutdowns=0`, `accept_transient_errors=0`, and pre-failure vals RSS was 18–21 GiB on a 60 GiB host. Stopping all peer vals nodes and PD did not recover it. Preserve the wedged state until console/BMC access if practical. Inspect kernel/systemd journal, OOM records, blocked tasks/stacks, root 990 PRO health/timeouts, scratch 9100/SPDK errors, pressure stalls, cgroup state, and sshd before reboot. Determine whether this is hardware/OS/root-device, SPDK/valsd, or service-manager failure; add monitoring/recovery guardrails and fix any vals cause found.

Acceptance criteria:
- Capture pre-reboot diagnostics through console/BMC or explain why unavailable.
- Establish a supported root cause with kernel/service/hardware evidence, not just the network symptom.
- Record timeline and findings under the task-0130 run artifacts.
- Implement or file concrete prevention/detection/recovery follow-ups.

## Acceptance criteria

## Work log

- 2026-08-05 ROOT CAUSED (kernel bug, not vals). The persistent journal survived the reboot, so `journalctl -b -1` gave the full pre-reboot evidence without console/BMC. At 04:58:13 UTC kernel `7.0.0-28-generic` took `BUG: unable to handle page fault` → `Oops: 0000 [#1]` at `RIP: lru_gen_del_folio.constprop.0+0x19a/0x380`, on `Comm: vals-shard`, via `__x64_sys_munmap` → `unmap_region` → `zap_pte_range` → `tlb_flush_mmu` → `free_pages_and_swap_cache` → `folios_put_refs` → `__page_cache_release` → MGLRU. The Oops killed the thread while it held the **lruvec spinlock**, so every later task needing that lock spun forever in `native_queued_spin_lock_slowpath`: ext4 writeback `kworker/u96:6`, `(udev-worker)` shmem eviction, RCU preempt stalls with NMI backtraces, and `sshd:2204` soft-locked from 23 s (14:55:23) to 13974 s at reboot. That precisely explains every observed symptom (SYN drops, SSH banner hang, frozen-but-serving metrics, `graceful_shutdowns=0`, `accept_transient_errors=0`, no OOM, immunity to stopping peers). Scope: all three boxes run the same kernel with MGLRU `0x0007`, but only lab-2 oopsed (lab-1 current boot and lab-3 previous boot: 0 matching lines) — a rare race, recurrence possible anywhere. Aggravating config: `kernel.panic_on_oops=0` / `kernel.panic=0` fleet-wide turned a momentary kernel fault into a ~10 h silent wedge. Also found a chronic, unrelated PCIe AER storm (~24 msg/s; 2.38M `device recovery failed` + xhci/thunderbolt `can't recover` = ~4.9M of the boot's 8.41M kernel lines) still active on all three boxes. Findings + trimmed logs + the raw Oops: `bench/bootes-soak-lab/run-20260804T220428Z-batch1000/node2-wedge-autopsy/`. Remaining work is prevention only: set `panic_on_oops=1`/`panic=30`, upgrade `7.0.0-28.28` → available `7.0.0-29.29` (no fallback kernel installed today), consider `lru_gen.enabled=0` if it recurs, quieten AER, and fix the collector's untimed `ssh` probe + add a stalled-`records_written` alert.

- 2026-08-05 PREVENTION APPLIED, one box at a time (lab-2 → lab-3 → lab-1, each fully verified before the next; lab-1 last as PD host / longest uptime). (1) `kernel.panic_on_oops=1` + `kernel.panic=30` persisted via `/etc/sysctl.d/99-vals-panic.conf` on all three. (2) Kernel `7.0.0-28` → `7.0.0-29` on all three, with `-28` retained as a bootable fallback and `GRUB_TIMEOUT=5`/`GRUB_TIMEOUT_STYLE=menu` set so a physically-present operator can select it (no BMC on this fleet). All three returned in 20–40 s on `7.0.0-29-generic`, 61 GiB, both NVMe drives present, 0 Oops/lockups, 0 EDAC errors. (3) SPDK prep restored on all three (4096 hugepages + 9100 PRO on vfio-pci, resolved BY MODEL); fleet now uniform with lab-1's pre-existing state. (4) Collector hardened: `ssh` wrapped in `timeout -k 5 20` (ConnectTimeout alone cannot bound a banner hang) plus a stall watchdog that alerts to stderr and `$RUN/ALERTS` when cluster `records_written` stops advancing for 3 samples while the producer is still running; both smoke-tested. (5) Docs: soak-runbook preflight now checks `panic_on_oops` and documents the post-reboot SPDK restore; fixed a real unit bug in the provisioning runbook (`HUGEMEM` is MiB, so `4096` yielded 2048 hugepages — half what the soak preflight checks for; now `8192`).
- STILL OPEN (needs a decision, not effort): the PCIe AER storm. SecureBoot is on ⇒ kernel lockdown `integrity` ⇒ `setpci` denied, so the *targeted* fix (mask `ACSViol` on `0000:80:1b.4`, the Thunderbolt/USB4 root port — both NVMe drives verified to sit on other root ports, so it would have been safe) is unavailable. Remaining options are blunt and cost PCIe error visibility on the NVMe drives: `pci=noaer` fleet-wide, `pcie_ports=compat`, disable SecureBoot then mask via a boot-time unit, or BIOS-disable Thunderbolt (physical access). Also deliberately deferred: `lru_gen.enabled=0` (MGLRU off) — hold unless the Oops recurs on `-29`.
- 2026-08-06 CLOSED. Root cause established (kernel 7.0.0-28 MGLRU Oops holding the lruvec spinlock) and all prevention landed: panic_on_oops/panic sysctls, kernel 7.0.0-29 fleet-wide with -28 as a selectable fallback, SPDK prep restored, collector ssh timeout + records_written stall watchdog, runbook fixes. The one remaining item (PCIe AER storm) is an owner decision with no vals content and is now tracked separately as task 0135; `lru_gen.enabled=0` stays deliberately deferred unless the Oops recurs on -29.

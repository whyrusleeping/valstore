---
id: "0349"
title: ram_broker overdraw assertion aborted node1 during SCALE0348-M1 drain (ram_broker.rs:245 grow(.., true) returned false)
status: done
priority: 2
created: "2026-09-30"
updated: "2026-09-30"
depends_on: []
spec_refs: []
---

## Description

Found by 0348 M1 (bench/0348-scale/FINDINGS.md, raw /data/dataset/task0348-results/SCALE0348-M1/).

At 07:49:26 UTC on 2026-09-30, lab-1 valsd (master 2005fe17, SHA-256 e824f155…) aborted on a `vals-shard` thread: `panicked at crates/vals-cluster/src/ram_broker.rs:245:5: assertion failed: grow(&source, waiter, bytes, true)`. That is `reserve_overdraw`: an overdraw grow is expected never to fail, but it did.
- **Context.** RF2/W2 was on 2 nodes, and node2 was down (bit-42 segfault followed by recovery panics). bootes was retrying writes during its 3600 s drain. At 1.1B ACK, node1 had about 2.6 GB of RAM reservation debt, about 12.6 GB of merge input, and about 2.9 GB of memtables. The default RAM budget was 45.8 GB.
- **Evidence.** The core `core.vals-shard.1983992` (about 18 GB) is in /home/user/vals/run on lab-1, with the matching binary at /home/user/vals0348-scale/SCALE0348-M1/valsd. `gdb-node1.txt` is in the raw dir.
- **Why it matters.** This is a software crash, independent of the lab-2 hardware signatures. It blocks crash-free sustained ingest (0348).

## Outcome

Root condition: reserve_overdraw's grow(.., true) could only fail on reserved.checked_add(bytes) overflow, i.e. an absurd (~u64::MAX) estimate while another reservation (core: 917 MB merge) was outstanding; ledger itself was not wrapped (core + observer). Overdraw now uses a saturating CAS, records only applied bytes in class counters and the Reservation, so shrink/drop are exact and the shard cannot abort. #[track_caller] + rate-limited warn (class, requested/applied/reserved bytes, caller file:line) when an overdraw exceeds the budget or saturates, so a recurrence names the caller; the originating estimate remains unproven from the unwound core. Deterministic tests reproduce the old failing sequence. No throughput effect: crash hit the already-failed drain ~34 min after ACK plateau. vals-cluster lib tests 708 pass.

Commit: fix(0349): make RAM-broker overdraw infallible (saturating, exact release) instead of asserting on u64 overflow; rate-limited caller-attributed warn for oversized overdraws; regression tests

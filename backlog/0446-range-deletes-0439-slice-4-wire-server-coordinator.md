---
id: "0446"
title: 'Range deletes (0439) slice 4: wire + server + coordinator — RangeDelete op, Scan trailer and replica skip-scan, delete_range_in, fragment-aware scan merge'
status: done
priority: 3
created: "2026-10-14"
updated: "2026-10-15"
depends_on:
    - "0445"
    - "0459"
    - "0460"
    - "0461"
spec_refs:
    - docs/range-deletes.md
---

## Description

Implements docs/range-deletes.md "Semantics", §2 and the wire half of §8 (vals-client wire/proto/coord, vals-cluster server).

## Outcome

Delivered by 0459 (wire: hello bit 3, RangeDelete=21, Scan trailer codec, server handler with ownership fence/sync/re-home mirror/accounting kick, NetTransport::range_delete), 0460 (cover-aware staged replica scan, covered-work budget + resume keys, trailer for bit-3 peers, Err to old clients over F, vals_scan_rt_* counters; vals_rt_* engine statics exported in vals-server metrics) and 0461 (Coordinator::delete_range_in(_with_persistence), fragment-aware scan merge with resume coverage). Acceptance re-checked 2026-10-15: e2e older-hidden/newer-wins + straggler tests (LocalCluster and NetTransport), R=2/3 lagging-replica multi-page scans, head-of-queue counter tests (N=10k vs 200k, fold/compaction/reopen; fail with block skip or max tree disabled), wire round-trip + wire_fuzz RangeDelete/trailer cases; `cargo test -p vals-client -p vals-cluster` green on 3d804ebd.

Commit: chore(0446): close range-delete wire/server/coordinator slice after acceptance re-check (sub-slices 0459-0461)

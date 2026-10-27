---
id: "0447"
title: 'Range deletes (0439) slice 5: anti-entropy RT exchange + range lifecycle (re-home copy/mirror, cleanup handoff, split/merge)'
status: done
priority: 3
created: "2026-10-14"
updated: "2026-10-15"
depends_on:
    - "0446"
    - "0462"
    - "0463"
    - "0464"
    - "0465"
spec_refs:
    - docs/range-deletes.md
---

## Description

Implements docs/range-deletes.md §4 "RT set reconciliation" and §5.

## Outcome

Delivered by 0462 (SyncRangeTombstones=22/RangeTombstones=20, scoped digest + paging, per-peer-shard RT exchange before the Merkle compare, refill gate, vals_ae_rt_exchanges_total), 0463 (re-home copy applies Scan page fragments before entries and follows resume keys; mirror failure not acked), 0464 (cleanup hands released F ∩ S to owners before keys, forget rewrite drops covered entries and clips RT records) and 0465 (AE Busy under continuous range deletes measured: 39% → 0% paced via inline fold turn and (S_A, S_T] record reads; residual write-churn Busy filed as proposed 0467). Acceptance re-checked 2026-10-15: AE convergence with mixed compaction + missed RT (kill-the-fix), inherited split inside a fragment + auto-merge across RT edges, re-home copy/mirror and mirror-failure poison, released-span cleanup, liveness-wipe refill tests all present; `cargo test -p vals-cluster -p vals-server` green on 2995f12d.

Commit: chore(0447): close range-delete AE/lifecycle slice after acceptance re-check (sub-slices 0462-0465)

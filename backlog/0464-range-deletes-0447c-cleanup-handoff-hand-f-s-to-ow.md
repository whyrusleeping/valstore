---
id: "0464"
title: 'Range deletes 0447c: cleanup handoff — hand F ∩ S to owners before uncovered winners, forget covered entries after confirmation, clip released RT records (RAM + rewritten runs); released-span cleanup test'
status: done
priority: 3
created: "2026-10-15"
updated: "2026-10-15"
depends_on:
    - "0463"
spec_refs:
    - docs/range-deletes.md
---

## Description

Part of 0447 (docs/range-deletes.md §5 "Cleanup handoff"). vals-cluster antientropy cleanup + vals-store forget plan.

## Outcome

Cleanup hands each page window's F in released ranges to the owners as RangeDelete before the keys (at most half the RPC cap, window cut where it runs out). A piece is released on key-rule confirmation plus A covering it at its version. The forget rewrite releases only pieces that F covers at exactly their version, drops entries under them, clips its RT block and removes covered memtable entries. The commit clips tail records (version-bounded, not during a flush) and refreshes F, A and T. The forget predicate now drops versions <= the confirmed one. Tests for the released span, reopen, RPC cap and a newer RT are all checked kill-the-fix. cargo test -p vals-store -p vals-cluster is green, and review accepted in round 2. A pre-existing resurrection of forgotten run keys on reopen is filed as proposed 0466.

Commit: feat(0464): range-delete cleanup handoff — hand released fragments to owners before keys, forget rewrite drops entries under them and clips RT records; released-span, reopen, RPC-cap and newer-RT tests

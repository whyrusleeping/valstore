---
id: "0253"
title: Remove measured-dead per-key hash/map bookkeeping from published seed cleanup
status: done
priority: 2
created: "2026-09-05"
updated: "2026-09-05"
depends_on: []
spec_refs: []
---

## Description

Measured on lab1 stock-glibc recovery arm c4c28276, Sep5~13:59, four shard threads, perf99Hz dwarf16384: ~79.36% self cpu_core cycles in seed_step. Disassembly attributes nearly all of this to **published cleanup**, lifecycle.rs:71–104, not source fold: 36.05% of seed-local samples at0x6368c8 (HashMap ctrl load),42.01% at0x6368f8 (comparison branch/load skid),7.15% at0x636750 (per-key staged-gauge atomic). Each dead winner is rehashed and removed from winner_slots even though publication set seeding=false and the only subsequent consumer of this map is cleanup itself. This is a measured perf target, not an implemented fix.

Candidate: keep bounded4096-key frees per turn but stop maintaining the dead hash map after publication; drop map storage once cleanup completes (HashMap<u64,usize> elements have no destructors). Batch estimate/gauge adjustment once per turn rather than per key. Preserve publication/invalidation/journal semantics, test epoch change during drain, continued writes/retire, forced hash collisions, gauge returning to baseline, and bounded per-turn releases. Validate final HashMap deallocation cost: do not trade random-probe work for an unbounded destructor/munmap stall. Original debug_assert map-empty will need a replacement state invariant.

Acceptance: deterministic functional/kill-the-fix mechanism tests, matched seeded cleanup benchmark (not whole boot), and short SPDK before/after profile plus driver/PD latency under rehome; no safety fence changes, no disabling AE. Evidence bench/0248-takeover-20260905/stock-seed-annotate.txt and perf-node1-stock-recovery.txt. Whole-arm recovery comparison is confounded by node3 rescue and cannot itself establish speedup.

## Acceptance criteria

- Software scope: bounded published cleanup without dead per-key map work; batched gauge accounting; deterministic functional/mechanism tests and matched cleanup-only benchmark including scalar-map deallocation review.
- Live SPDK before/after profiles and driver/PD latency under rehome are split into dependent task 0257; software completion is not a hardware-pass claim.

## Outcome

Accepted software change in faa89674: published seed cleanup preserves bounded releases and lifecycle semantics, removes dead winner-map probes/mutations, and batches staged-byte accounting. Standard independent review accepted; coordinator reran replica tests (98 passed, 3 ignored), production seed_budget test, and diff whitespace check. Matched local cleanup benchmark reports 2.74–3.25x improvement; scalar-map drop has no element walk, with pre-existing allocator release cost unchanged. Live SPDK/rehome acceptance split to dependent 0257; no fleet touched. Task0250 blocked on existing attended BIOS maintenance requirement. Final bookkeeping commit also captures the pre-existing ongoing 0248 driver metrics snapshot without changing its collector.

Commit: chore: record reviewed seed cleanup outcome and hardware follow-up

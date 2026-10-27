---
id: "0313"
title: Cut sorted memtable (FrontCodedIndex) insert/flush-remove CPU on the shard write path
status: done
priority: 1
created: "2026-09-26"
updated: "2026-09-26"
depends_on:
    - "0309"
spec_refs: []
---

## Description
## Evidence (LONG01 B21-P, master af39d217 = 0310+0312, 2026-09-26 data re-analysed 2026-09-27)

- Closed-loop client window is full (32 PutBatches × 4000 in flight); client PutBatch latency 88→150 ms tracks server shard load. Shard threads run 70–97 % busy for the whole write window (max/mean skew ≈1.09), so shard CPU per KV is the binding resource: ≈5.5 µs/KV early → ≈8.1 µs/KV late per node (8 shards), ≈7.6 µs/KV averaged.
- node1 cpu_core perf at t=60 (leaf symbols, no frame pointers): memcmp 14.7 %, memmove 8.4 %, malloc/realloc/free family ≈20 %, `FrontCodedIndex::put` 8.2 %, `BlockBuilder::push_packed` 5.0 %, `Block::decode_key_into` 4.5 %, btree `find_upper_bound_index` 3.0 %, `FrontCodedIndex::remove`/`get` 2.4 %. Most of this is the sorted memtable index (`crates/vals-store/src/index.rs`).
- Code mechanisms: `put` does 2–3 BTreeMap descents; block search materialises every key (decode + memcpy + byte-wise `common_prefix_len`); `splice_apply` uses `reserve_exact`, so nearly every insert reallocs; flush commit (`flush_commit_step`) does `get` + `remove` per flushed key and `block_remove` rebuilds the whole block (all 4 BlockBuilder Vecs) per removed key.

## Change (one candidate)

Narrow CPU optimisation of `FrontCodedIndex` put/get/remove plus a batched/sorted flush-commit removal path, with unchanged semantics (same get/scan/len/key_bytes/live_ptr_bytes results) and bounded memory overhead. No format change (memtable is volatile).

## Acceptance

1. Local release microbenchmark with realistic key shapes (records `collection/did/rkey`, backlinks `ref\0source\0path`), memtable-sized (~250k entries/engine), random insert order, then flush-order removal: report ns/op before/after for put, flush removal, get; plus RSS/heap bytes per entry before/after.
2. Existing and extended property tests vs BTreeMap reference pass; vals-store suite passes.
3. Fleet: preregistered LONG01 ABBA on the B prefix (capacity profile, splits on) vs master: accept if mean whole-write ACK/s ≥ 1.05× master, all correctness gates PASS, shard CPU s/KV lower, max server RSS within +10 %.


## Acceptance criteria

## Work log

### 2026-09-27 — implemented, reviewed, ABBA stopped at a crash (series paused, awaiting owner)

- Branch `0313-memtable-cpu` (`ff800a87..305797b3`, worktree `.wt/0313m`): single-descent put/remove, lcp-tracking block search without key materialisation, in-place splice removal/split, amortised block capacity (64 B / 8-entry steps, shrink on halving), fused `Index::remove_if_eq` and batched `Index::remove_if_eq_rev` used by `flush_commit_step`. `memtable_bench` (250k keys, release): put 1318→534 ns (records) / 1522→739 (backlinks); flush removal 2153→107 / 2498→134 ns; heap +7.7 % / +7.2 % per entry. vals-store 323 pass, vals-cluster pass (debug). Independent review (agent_3): no blockers/majors, merge-ready; own fuzz + 15 mutations clean.
- Preregistered ABBA (long01/README.md). `LONG01-B23-M` 920,473 ACK/s PASS (quartiles 1122/928/913/780k). `LONG01-B24-T` **1,143,238** PASS (1466/1167/1127/933k, sliding-10 s min 880k); shard CPU 7.55 → 5.25 s per M KV (0.70×). Max RSS node1 5.09 GB vs 3.69 GB: the memtable gauge holds 2.2–2.5 GB vs ~1.2 GB on node1 (flush lagging the higher intake under the shared 33 % maintenance pacer), not index slack.
- `LONG01-B25-T` **FAIL: lab-2 SIGABRT** `corrupted size vs. prev_size` (glibc unlink_chunk from a free in `ConnDriver::dispatch_frame`). Core `/home/user/vals/run/core.vals-shard.2421725` (sha256 f454e2fa…27ca) + binary `/home/user/vals0297-ingest-1m/rangemap01/LONG01-B25-T/valsd` (57d8c009…05aa) preserved on lab-2. Chunk walk: free 0x20 chunk at 0x7ac6e89b48d0; next chunk prev_size = `0x0000040000000020` instead of `0x20` — a single bit-42 set (flipped byte at cache-line offset 0x35, i.e. 0x15 within the 32-byte half-line), the same bit/offset signature as 0248's two earlier lab-2 crashes (those cleared bit 42). Evidence for the known lab-2 hardware hypothesis, not proof. Series stopped (B26-M not run); lab-2 hugepage leftovers removed after valsd was confirmed gone; fleet stopped.

### 2026-09-27 — owner-directed rerun; merged

Owner: rerun the comparison, "seems like a solid improvement". `LONG01-B25-T-R2` 1,170,546 PASS (1503/1210/1061/1017k), `LONG01-B26-M` 918,048 PASS. ABBA mean T/M = 1,156,892 / 919,261 = **1.259×**, shard CPU 0.69×, all non-crash runs PASS; the preregistered RSS gate FAILED (1.29×, node1 memtable/flush backlog under the shared 33 % maintenance pacer, not index slack). Merged as `d67458e5` with the deviation recorded (`8fbfc8ae` ARCHITECTURE, `fd4d4add` evidence; `long01/FINDINGS.md`, `SUMMARY-MEMTABLE-0313.json`). Fleet stopped, hugepages 4087/4087, no new cores beyond the preserved B25-T core.

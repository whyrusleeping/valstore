---
id: "0364"
title: Engine-wide folded "key may exist" filter so capacity-profile writes stop probing every run (flatten write cost vs run count)
status: done
priority: 2
created: "2026-10-02"
updated: "2026-10-02"
depends_on: []
spec_refs:
    - docs/out-of-core-index.md
    - docs/blind-append-lww.md
---

## Description
## Why
In FULL1 (the whole 1B cache, 2 nodes, `bench/0363-compact-values/FULL1-FINDINGS.md`), write throughput fell 25% over 0–1B.
- **Mechanism.** Every capacity-profile unconditional write calls `probe_version` (`crates/vals-store/src/engine/read.rs` ~L372). It prefetches and probes every resident run's filter: ~0.40 µs per extra run, with shard µs/write 4.37→7.19 as runs/engine went 2.9→10.1 (r≈−0.99).
- **Merge policy makes runs O(data).** With `VALS_MERGE_INPUT_CAP_BYTES` 2 GiB, two runs whose sizes sum to more than 2 GiB never merge. The model predicts ~11 and ~21 runs/engine at 1B and 2.09B; observed 10 and 21.
- **More lanes did not help** (FULL2-LANES4).
- **Raising the cap adds device writes**, and device bandwidth is the binding resource after the ~1 TB drive state change.
- Read-only analysis (agent_3, 2026-10-02) has the code references.

## Why the prior version is needed (cannot simply go blind)
- **AE Merkle leaf:** `store` → `apply_leaf(OldWinner::Exact(old))` (`version_index.rs:269-303`). An Unknown old version dirties the bucket.
- **Per-range logical counts used by auto-split:** `note_logical_winner_apply` (`lifecycle.rs:1315-1328`). Unknown discards the range counts.
- **Won/Lost reporting** is ack semantics only.

Blind append without the winner directory exists in code but is gated, and it would break both of the first two.

## Change
An engine-wide superset filter folded from the per-run blocked bloom filters.
- **Folding.** The block is chosen by multiply-high of `z` (`run.rs:371-376`), and in-block bits are independent of `num_blocks`. So m·G blocks fold to G by OR-ing consecutive groups.
- **Lifecycle.** Build at open by folding the loaded run filters (CPU only, no I/O, nothing persisted). OR each new run in on publish; refold after merges and forget.
- **Small runs.** New runs' filter block counts become multiples of G (power-of-two sizes). Runs smaller than G (the newest few) are probed individually.
- **Write path.** One global probe; only on a hit fall back to per-run probes.
- **Invariant.** The global filter ⊇ every key of every visible run at each manifest generation. A false negative would silently corrupt AE XOR, counts and Lost decisions.
- **RAM.** About +1.5 B/key (~3.1 GB/node at 2.09B keys).
- **Size.** Estimated 400–700 lines in vals-store plus property tests.

## Optional constant-factor wins (separate commits, only if measured)
- u64 prefix compare for run min/max;
- remove per-write allocations (`group_of` Vec in `lifecycle.rs:919`, `Box::from(group)` in `version_index.rs:294`, `encode` Vec in `codec.rs:46`);
- reuse the stage probe at commit instead of re-probing (`write.rs:279`);
- in-place compares in `block_lookup`.

## Acceptance (preregister; full-cache A/B vs FULL1 setup, merge config unchanged)
1. **Mechanism:** a new counter shows global-filter negatives ≥ 99% of writes and per-run filter probes ≤ 4/write at 1.0B and 2.0B.
2. **Per-write cost:** shard µs/write ≤ 5.0 at 900M–1.0B (FULL1 7.19). The rise from 100M→1.0B ≤ 0.8 µs.
3. **Throughput:** mean ACK over 0–1.0B ≥ 1.15M (FULL1 1.066M), and every 100M bucket ≥ 1.0M until the drive step.
4. **No cost shift:**
   - resident runs within ±10% of FULL1;
   - write amp at 1.0B ≤ 1.85;
   - RSS at 2.0B ≤ 26 GB;
   - the drive step at the same written TB ±3%.
5. **After the step (descriptive):** shard µs/write ≤ FULL1 − 2 µs.
6. **Correctness:**
   - AE roots equal on both nodes after the drain, with zero repairs;
   - per-zone counts match bootes;
   - in-run cursor and samplecheck clean;
   - unit/property tests show the filter covers every run after flush, merge, open and forget, plus a knockout test that fails without the fix.

Expectation: this flattens the pre-drive-step phase only. Sustained ≥1M across 2.09B also needs the drive-state fix (TRIM, FULL2-TRIM) and/or lazier merges, which become affordable once run count no longer costs CPU.

## Acceptance criteria

## Work log

### 2026-10-02: direct-insertion design built (folding rejected); review clean with fixes pending; FULL3-KF measured

- **Folding rejected.** A folded filter's density is Σn/G, so with 10–21 similar-size runs it reaches 84–99.6% FPR. Coordinator decision: a direct-insertion scalable filter.
  - Sub-filters grow 1.5× with +1 bit/key; flush and merge builds insert keys.
  - On restart a cover-scan maintenance job covers recovered runs.
  - A generation reset on churn.
  - `VALS_KEY_FILTER=0` disables it. No format change.
- **Uncommitted implementation:**
  - `crates/vals-store/src/keyfilter.rs`, `engine/keyfilter.rs`, `engine/tests/keyfilter.rs`, plus edits across vals-store and vals-cluster;
  - docs in `docs/out-of-core-index.md` and ARCHITECTURE.md.
  - vals-store 357, vals-cluster 743 and vals-server 31 tests pass in debug.
  - Release microbench at 20 runs: 938 → 237 ns per absent probe.
- **Independent review (agent_5): no false-negative path found; safe after fixes.**
  - MAJOR: the property test never keeps flush/merge plans in flight across other events (reset, growth, interleaving), and the knockouts cover only the flush sink. A persistently failing cover-scan loops on the largest run and starves the engine.
  - MINOR:
    - document that reads also use the filter;
    - count cover-scan chunks in `maint_jobs`;
    - guard that a scan's key count equals the run's entry_count;
    - cap the sidecar record length;
    - account reserved capacity / sizing;
    - orphaned sub-filter RAM;
    - cover-scan only plans when no flush is due;
    - disable the filter under the winner directory.
- **FULL3-KF** (`bench/0363-compact-values/FULL3-FINDINGS.md`; pin `2d6a5f3c` = 88c25c2c + this diff, valsd `f4e2bbdd…`).
  - **Stopped at ~2.04B ACK by the persistent-integrity rule.** lab-3 zone 2 front-coded memtable mismatch; the key byte was 0x7f where 'o' = XOR 0x10 is expected, matching lab-3's memtester signature. No crash; the drain was clean (2,038,719,036 persisted).
  - **Throughput:**

    | | FULL1 | FULL3-KF |
    |---|---:|---:|
    | 0–1.0B | 1.066M | 1.165M |
    | 1.0–2.0B | 605k | 831k (+37%) |

  - **Mechanism:**
    - filter negatives 99.5% at 1.0B and 99.3% at 2.0B; per-run probes 0.05 and 0.17 per write;
    - index-probe cycles flat at ~2.0k/write (FULL1 3.65k→6.0k);
    - drive step at the same written TB.
  - **Misses:**
    - shard µs/write 5.81 at 900M–1.0B against ≤ 5.0 (FULL1 7.19); the remaining rise is malloc/memcmp;
    - runs +17% at 2.0B;
    - RSS 26.5 against a ≤ 26 GB criterion. That criterion was a preregistration error: it omitted the filter's own 5.17 GB/node (2.54 B/key). RSS is well within the 45.8 GB budget.
  - **Not yet run:** correctness (cursor, samplecheck, AE) and the restart cover-scan timing. Next: a restart check on the preserved corpus.

### 2026-10-02: FULL3 restart check passed; review fixes landed; second review says safe to merge; committed

- **FULL3-KF restart check** (`FULL3-FINDINGS.md` "Restart check"):
  - both nodes reopened cleanly; node3's replayed memtable flushed clean, so the 0x6f→0x7f flip was RAM-only;
  - cursor and samplecheck pass (2,038,901 keys, 0 mismatches), per-zone counts exact, AE clean (0 digests/reads/applies).
  - Cover-scans starved 44–50 min behind the post-ingest merge backlog, then took ~6 min per node.
- **Fix round, all 10 review findings:**
  - property test with in-flight flush/merge/cover-scan queues (16 seeds × 600 ops) and 4 cfg(test) knockouts, each caught;
  - cover-scan failure backoff;
  - three-class compaction queue (pressure > cover-scan > merges), with lane 0 lingering 100 ms between scan chunks;
  - scans plannable while a flush is due;
  - `maint_jobs` accounting, key-count check, key-length cap, sink reservations, orphan RAM charging, filter disabled under the winner directory;
  - docs.
  - Suites pass in debug; the microbench is unchanged.
- **Second review (agent_5): safe to merge.** Remaining minors, filed as a follow-up:
  - with `maint_compact_lanes=1`, merges wait behind all cover-scans;
  - with 2 lanes, merge concurrency halves during coverage (measure in FULL4);
  - winner-directory flapping can repeat full re-scans, and the degrade path is untested;
  - a merge sink can overfill a newer sub-filter;
  - a per-tick allocation in `cover_scan_merge_soon_start`.
- **Confirmation run FULL4-KF2 in progress** on this exact tree. Its harness restarts a node on a persistent memtable integrity event (bench policy only).

### 2026-10-03: closed
FULL3-KF met the mechanism and throughput criteria; FULL5-V5 (with 0365) confirms them on the whole cache:
FULL5-V5 (`bench/0363-compact-values/FULL5-FINDINGS.md`; pin 8c22af67 = master 448ef977 + 0365, content-identical to 0267a341; valsd 7eee30e0…; lab-1+3; whole 1B cache to EOF; 0 incidents).
- **Throughput:** whole run **1.074M ACK KV/s** (FULL3-KF 962k, FULL1 752k). 0–1.0B 1.198M; 1.0–2.0B 998k; post-step 862k.
- **Drive step** at 1.645B ACK (1.03 TB written), previously ~1.08–1.09B.
- **Bytes:** 399–401 B/KV stored (FULL1 532); device writes 632–636 B/ack (FULL3-KF ~920, FULL1 1005); merge output 220–224 B/ack.
- **Correctness:** cursor, samplecheck 2,090,402 keys, exact counts; AE 600 s window with sweeps advancing (960 passes/node, max age 5 s), 0 repairs, aediag 24/24 trees equal; all again after restart.
- **Restart coverage:** after reopen, uncovered key-filter runs reached 0 at +467/+397 s. But the merge backlog had already drained in the settle (0 merges in flight), so 0364's "cover-scans progress under merges" is still unproven on hardware (0366 item 2).
- **Post-step** gain is smaller than the byte model predicted (+7% vs FULL3-KF): device write rate fell to 0.53–0.60 GB/s with vlog ~180–204 µs/stall, i.e. latency-bound. The v5 verify readback (~0.17–0.20 GB/s) has not been separated out.
- **Remaining:** the cover-scan-under-merges hardware test is tracked in 0366 item 2.

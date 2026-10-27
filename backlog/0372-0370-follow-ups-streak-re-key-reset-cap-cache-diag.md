---
id: "0372"
title: '0370 follow-ups: streak re-key reset, cap/cache diagnosis cost, watchdog _exit before logging, end-to-end shutdown and exit-code tests'
status: done
priority: 3
created: "2026-10-02"
updated: "2026-10-12"
depends_on:
    - "0370"
spec_refs: []
---

## Description
From the agent_5 review of 0370 (2026-10-03). The docs fixes (systemd template, local-only persistence claim, hugepage maps, reset-failed note, cost wording) are applied with the 0370 commit. The code items remaining:

1. **The streak can reset forever** (`crates/vals-cluster/src/integrity.rs` ~80 with `index.rs` ~1287). If the corrupt block is the zone's first block, any insert below its first key re-keys the block. The new `first_key` resets both the attempt count and `since`, so with descending or low-key arrivals the node may never trip. Key the streak on (zone, engine), or preserve `since`/attempts across a re-key of the same block.
2. **The diagnosis cost is unbounded.** On a well-formed block the single-bit search hashes roughly 8·n·L² bytes (L = key length); the 4 KiB cap applies only to malformed blocks. That is ~50 ms for 1 KiB keys and seconds for ~4 KiB shared-prefix keys, on the shard thread, on every attempt, and repeated every tick when the policy is disabled. Add a byte/time budget (report `unexplained (budget)`) and cache the diagnosis on the block so retries don't recompute it.
3. **The watchdog can block on logging** (`vals-server/src/main.rs` `arm_fail_fast_watchdog`). It logs before `_exit(75)` and the logger takes `stderr().lock()`. If the hang is a thread blocked writing to a full stderr pipe, the watchdog blocks too. Call `_exit` first, or write a fixed message with raw `write(2)`.
4. **Nits:**
   - `block_first_key` in the evidence line is unbounded in length; truncate it.
   - With candidates > 1, `expected`/XOR come from the first candidate (the count is logged); consider listing up to k.
   - A SIGTERM coinciding with a trip exits 75, so the unit shows failed after a manual stop (cosmetic).
   - The `REQUESTED` flag is never reset (test hygiene).
5. **Tests:**
   - an end-to-end test that runs `finalize_shutdown`/`NodeHandle::shutdown` after a trip and asserts nothing derived from the corrupt memtable was persisted, then reopens;
   - a valsd-level test of the exit code 75 path and the watchdog (subprocess test).
6. **Hardware:** verify that a systemd restart after the watchdog `_exit` path works with stale `spdk<id>map_*` files (0337); the bench unit wrappers already clear them.

## Validation amendment (0407, owner-approved 2026-10-08)
Tier L0 for items 1–5: subprocess tests for exit-75 and the watchdog, and a microbench for the diagnosis cost. See docs/validation.md. Item 6 is covered by 0337's one-box start/stop check. That check must include a kill -9 / exit-75 case followed by `systemctl restart`.

## Acceptance criteria

## Work log

- 2026-10-10: consolidated (owner-approved). **0387 items 2 and 6 are merged into this task**: implement them together with items 1 and 2 here, as one change with one review and one diagnosis-cost microbench. Item 1 here and 0387 item 2 both re-key the 0370 fail-fast streak: pick one keying that solves both, e.g. (zone, engine) or the source group, with `since`/attempts kept across a re-key. Item 2 here and 0387 item 6 are the same diagnosis-cost bound. Item 6 (hardware restart with stale maps) runs as the kill -9 / exit-75 leg of 0337 on 0405's fresh provision.

### 2026-10-10: implemented, reviewed, and integrated into the main tree (uncommitted); scope includes 0387 items 1, 2 and 6
- **Streak (items 1, 0387.2, 0387.1):**
  - The streak is keyed on (zone, source engine), with attempts/`since` preserved across re-keys and changing block boundaries.
  - Non-integrity plan/build/write/commit errors clear it, which is safe because they only surface after full verification.
  - Zone/engine lifecycle hooks (materialize, drop, replace, wipe) clear it.
- **Diagnosis (items 2, 0387.6):**
  - Charged at actual decoded key bytes + 44 per key. Budget: 128 MiB aggregate, 4 MiB per key, a 500 ms deadline sampled every 256 ops (5 s under cfg(test)).
  - On exhaustion, candidates are kept as `partial`, and the trip headline prints `xor=… (partial search)`.
  - Entry images are searched before key bits.
  - A single cold cache slot on `FrontCodedIndex`, fingerprinted by (first key, integrity, computed sum, packed len, entries). `Block` stays 64 B.
  - Release: realistic 36/66/153 B 64-entry blocks give complete findings in 2–8 ms; 4 KiB shared-prefix keys are bounded at ~25 ms (previously 21.8 s).
- **Item 3:** the watchdog and spawn-failure paths do a non-blocking `poll` + ≤PIPE_BUF `write(2)`, then `_exit(75)`. No logger lock and no O_NONBLOCK left on a shared fd.
- **Items 4 and 5:** evidence/detail bounded at 64 B keys and a 768-char detail. Subprocess tests for exit 75, a held stderr lock and a full pipe. The trip → finalize → reopen test persists nothing corrupt.
- **Perf:** interleaved release A/B of `front_coded_integrity_bench` (ABABAB 3×5, implementer and reviewer independently) is neutral at ±1%. Distributions are in bench/0372-streak-diag/perf-ab.json; ledger in REVIEW.md.
- **Review:** opus (agent_8), two rounds. Blocker B1 (budget mis-sized, findings discarded) and majors M1–M3 fixed; approved. The coordinator fixed minors 1, 2, 4 and 5; the second-flip cache fingerprint has a test whose knockout fails.
- **Remaining:** item 6 (hardware restart with stale maps) runs as the 0337 kill -9 / exit-75 leg on 0405's provision.

## Outcome

Streak re-key, reset and cap plus bounded single-bit diagnosis, including 0387 items 1, 2 and 6. The worst diagnosis case drops from 21.8 s to ~25 ms on the shard thread; normal-sized blocks still give a complete single-bit finding. The write-path benchmark is neutral (±1%) in two interleaved A/Bs (bench/0372-streak-diag). The reviewer caught a mis-sized budget that discarded single-bit evidence; fixed and re-reviewed. Integrated in s_8666773a (2026-10-11); committed 2026-10-12.

Commit: fix(0372): fail-fast streak re-key/reset/cap and bounded diagnosis (incl. 0387 items 1/2/6) — worst diagnosis 21.8 s -> ~25 ms on the shard thread, write path neutral (±1%)

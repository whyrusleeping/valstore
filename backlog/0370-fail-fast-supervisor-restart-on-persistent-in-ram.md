---
id: "0370"
title: Fail fast (supervisor restart) on persistent in-RAM integrity failure instead of retrying a corrupted memtable flush forever; log mismatch bytes
status: done
priority: 2
created: "2026-10-02"
updated: "2026-10-03"
depends_on: []
spec_refs: []
---

## Description
## Why
The lab RAM is non-ECC and flips bits (memtester, 0361/0362). The BIOS mitigation is not before ~2026-10-09.

Across the 0363/0364 campaign the integrity guards caught every flip:
- NEWb, front-coded memtable mismatch;
- FULL3-KF, lab-3 XOR 0x10 key byte;
- FULL4-KF2, transient merge-read CRC.

But a persistent memtable integrity mismatch makes the flush retry the same corrupted in-RAM block forever. The node stalls and RF2/W2 ingest stalls with it.

The FULL3-KF restart check proved the remedy: the flip existed only in RAM, and after a restart the memtable replayed from the value log flushed cleanly, with both replicas identical (`bench/0363-compact-values/FULL3-FINDINGS.md`, "Restart check").

**Owner approval (2026-10-02):** "You can restart nodes whenever you like."

## Change
When an in-RAM integrity check fails persistently, the node fails fast so the supervisor (systemd `Restart=`) restarts it and it recovers from disk. Persistent means the same front-coded memtable block mismatching on N consecutive flush attempts or for more than T seconds; defaults about 3 attempts / 30 s, configurable.
- **Exit path:** log the evidence (block first key, the differing bytes and XOR if the guard can compute them), increment a counter, then exit with a distinctive code. Prefer a clean process exit over a panic, so no multi-GB core is written.
- **Scope:** only for signals that are provably RAM-side, i.e. the source memtable is in memory and the vlog on disk is intact. Do NOT restart-loop on on-disk corruption: persistent run index CRC, compaction CRC on re-read and recovery failures keep their current behaviour (stop or alert).
- **Restart storms:** guard against them by persisting a small restart ledger, or relying on systemd StartLimit. Document the unit settings in docs/deploy.md.

Also extend the integrity guard's log to include the mismatching offset/bytes, so flips (single-bit XOR) can be told from software bugs (the 0363 AB01 gap).

## Acceptance
- **Unit/integration test:** inject a persistent memtable integrity mismatch and show the node exits with the documented code. After a restart (test harness) the data is intact and the flush succeeds.
- **No restart for on-disk corruption:** a transient single mismatch does not exit (retry succeeds); on-disk corruption does not trigger restarts.
- **Docs:** deploy.md (systemd unit Restart/StartLimit), configuration.md (thresholds), ARCHITECTURE.md (integrity guard behaviour).
- **Hardware:** exercised opportunistically in the next full-cache run (the bench harness's own integrity-restart rule becomes a fallback).

## Acceptance criteria

## Work log

### 2026-10-03: implemented (agent_4), reviewed (agent_5), committed
- **Guard.** Each memtable block keeps an additive FxHash over (key, entry) from the caller's key. Before a flush the block is decoded with bounds checks, its first key checked, and the hash recomputed. On a mismatch `EngineError::MemtableIntegrity` carries a single-bit-flip diagnosis: region, key index, offset, actual/expected byte, XOR, candidates, a window, or `unexplained`.
- **Policy.** The streak is per (zone engine, block first key). It trips after `integrity_fail_fast_attempts` (default 3; 0 or ≥2) or `integrity_fail_fast_secs` (default 30, at least 2 attempts); both 0 disables it.
  - On a trip: ERROR with evidence, `vals_integrity_fail_fast_total`, and a process flag.
  - valsd then runs the normal graceful shutdown (no memtable flush; value-log sync and watermark only) and exits 75. A 30 s watchdog calls `_exit(75)`.
- **Tests.** The diagnosis covers every region and the split and unexplained cases; the engine test recovers the exact flip `offset=19 0x7f→0x6f xor=0x10`. The real flush loop shows trip, cleared retry, the disabled knockout (bounded) and on-disk CRC never tripping.
- **Review verdict:** the safety claim holds; nothing from the corrupt memtable persists locally.
  - **MAJOR (fixed in this commit, docs):** the systemd template now uses `RestartPreventExitStatus=1 2`, so startup or recovery failures (on-disk corruption) do not restart.
  - **Docs corrected:** persistence is local-only (peers can receive a corrupt key through scans/AE/rehome during the window); stale spdk map files persist on every exit path (0337); `systemctl reset-failed` after rolling restarts; the diagnosis cost is not capped.
  - **Code minors:** filed as 0372.
- **Hardware.** In FULL6 (pin 1ee9151a = ccde1d27 + this change) the exit-75 path was armed but never fired, so the auto-restart on hardware is still untested.

### 2026-10-04: hardware-confirmed (FULL11-3N, lab-2)
A zone-1 memtable block failed the integrity guard 3 times; the diagnosis was a single bit (`xor=0x04`). The node exited 75 and systemd restarted it. Recovery replayed the value log and the flush passed. (A separate heap-corruption SIGSEGV followed 20 s into the restart; see 0248.) The cluster recovered at t80 79 s and R600 0.788. Correctness passed. `bench/0363-compact-values/FULL11-FINDINGS.md`.


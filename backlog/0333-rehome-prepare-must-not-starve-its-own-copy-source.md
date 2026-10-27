---
id: "0333"
title: 'Rehome prepare must not starve its own copy source: source-store AE reseed pushes copy scans past the 5 s peer deadline (copy_stall)'
status: done
priority: 3
created: "2026-09-28"
updated: "2026-09-29"
depends_on: []
spec_refs: []
---

## Description

## Description
## Evidence (0324 offline diagnosis of SOAK0321-F4 settle; see bench/0321-long-soak/FINDINGS.md "0324 diagnosis")
- **Prepare triggers a reseed of the copy source.** On nodes > RF, every rehome prepare bumped `ae_seed_placement_invalidations` and `ae_seed_starts` on the source owner nodes, and a streaming reseed of the source store followed.
- **Scan latency climbs past the copier's deadline.** While the reseed ran, source scan latency rose from 0.04 s to over 5 s, and up to 12–80 s. The copier (`node/rehome.rs`) re-issues the same page every 100 ms tick once `NODE_PEER_RPC_DEADLINE` (5 s) expires. Abandoned scans keep running on the source, so it made zero progress.
- **The PD aborts, which restarts the cycle.** After 60 s the PD aborts with `copy_stall`. The re-prepare invalidates the seed again.
- **Recovery once the seed finishes.** Scans dropped to 0.01–0.05 s at once, and the copy ran at about 100 pages per 10 s.
- **Scale.** 5 of 15 settle rehomes aborted this way; one range aborted 5 times.
- **0324 reduces the frequency.** Debt rehome now runs only under measured skew, so this happens less often, but the livelock remains for any rehome or balance move.

## Candidates (pick one after measurement)
- Rehome copy scans take priority over, or pause, the seed stream on the same store.
- Prepare does not invalidate the source store's seed; only commit does. Or do a range-local reprojection (flagged out of scope in 0317 and 0322).
- The copier backs off and does not re-issue a page while its earlier scan may still be running on the source. It could also use a longer deadline or smaller pages under source latency.

## Acceptance
- A local repro with forced seed contention, or a hardware settle, shows rehome copies progressing and no `copy_stall` aborts that the source's own reseed causes.
- Seed correctness is unchanged.

## Acceptance criteria

## Measurement (done by coordinator, SOAK0321-F4 observer.jsonl, per-node 10 s deltas)
- Source scan latency tracks seed activity exactly. While a source node's streaming seed runs (about 6k–12k `ae_seed_steps` per 10 s), `scan_latency` climbs from 0.5 s to 1, 2, 3, 5 and 10 s, and on node2 to 30–80 s. Within 10 s of that node's seed finishing, it falls back to 0.01–0.05 s and `rehome_copy_pages` jumps to about 100 per 10 s.
- There are no `scan_sync_fallbacks` or `ae_seed_sync_fallbacks`, and `scan_staged_restarts` are rare. So the seed is not blocking the shard synchronously, and scans are not being restarted.
- Mechanism: `vals_rt::LocalExecutor::drain_ready` polls each ready task once per pass, then calls `poll_io`. The seed driver in `node/sweep.rs` `logical_accounting_loop` runs one `seed_step_cooperative` step (up to 4096 folds, or a synchronous 256 KiB sidecar read), then `yield_now`, so it is in every pass. A staged scan awaits hundreds of device completions per 4096-entry page, one per pass, so every await costs a full seed step. Once scans pass the copier's 5 s `NODE_PEER_RPC_DEADLINE`, they are abandoned and re-issued, which compounds the load.

## Fix: scans take priority over the seed on the same shard
- In `logical_accounting_loop`, before each `seed_step_cooperative`: if this shard has scans in flight (`shared.scan_gate.inflight() > 0`; add a small accessor), and less than `SEED_SCAN_YIELD_MAX` (10 ms) has passed since the last seed step, `yield_now` without stepping. This keeps the seed out of the drain passes while scans run. The 10 ms bound guarantees the seed at least one step per 10 ms, so it cannot starve under continuous scans.
- Add a counter `ae_seed_scan_yields` (in vals-client `observe.rs`, next to the other `ae_seed_*` counters) for passes that were skipped. Add a kill switch `VALS_SEED_SCAN_PRIORITY=0` for hardware A/B, following the existing `env_parse`/OnceLock pattern.
- The seed algorithm, invalidation and PD logic are unchanged. The copier is unchanged: scan admission already bounds abandoned scans to 8 per shard.

## Verification
- New `node/tests.rs` test next to the `logical_accounting_loop_*` tests. Build a run-backed store large enough that the seed needs many steps. Invalidate the seed, start `logical_accounting_loop`, and while the seed is running, serve a multi-page scan through the real server scan path (holding a scan permit). Assert the scan completes with far fewer seed steps interleaved than scan awaits. With the kill switch off (A arm), assert the interleaving is much larger (A/B via counters, not timing). Also assert the seed still reaches seeded while a scan permit is held continuously (starvation bound).
- Run the existing `logical_accounting_loop_*`, seed and scan tests in debug.

### Starting points
- crates/vals-cluster/src/node/sweep.rs:52 logical_accounting_loop (seed step + yield_now at :74-78)
- crates/vals-cluster/src/net/server/shared.rs:60 scan_gate, :172 try_scan; ReadGate::inflight in net/server/read_gate.rs
- crates/vals-rt/src/lib.rs:184 drain_ready one pass per poll_io
- crates/vals-cluster/src/node/tests.rs:2484 logical_accounting_loop test harness
- crates/vals-client/src/observe.rs:179 ae_seed_steps counter list

## Outcome

F4 observer: while a source's streaming seed ran, its scan latency climbed from 0.5 s to 10–80 s, then fell to 0.01–0.05 s within 10 s of the seed finishing, and copies resumed. Cause: drain_ready polls each ready task once per pass, and the seed stepped and yielded every pass, so each scan await cost one seed step. Fix: logical_accounting_loop skips seed turns while this shard has scans in flight, but still steps at least every 10 ms. Adds the ae_seed_scan_yields counter and the VALS_SEED_SCAN_PRIORITY=0 kill switch. Seed, PD and copier logic are unchanged. Local A/B test: 6 seed steps during scans with priority vs 59 with the kill switch, and the seed still completes exactly. Not yet run on hardware; confirm on the 0332 settle (copy_stall should be about 0, scan yields > 0).

Commit: sweep(0333): shard scans take priority over AE seed turns (10 ms max deferral) — F4 source-reseed copy_stall mechanism; ae_seed_scan_yields counter, VALS_SEED_SCAN_PRIORITY kill switch, local A/B test

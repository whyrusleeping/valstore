---
id: "0325"
title: Range scans fail quorum under post-restart reseed/rehome at 1.4B keys/node (census could not complete)
status: done
priority: 2
created: "2026-09-28"
updated: "2026-09-28"
depends_on: []
spec_refs: []
---

## Description
## Evidence (SOAK0321-F4 re-verify on retained data, 3-node RF2, about 1.4B keys/node)

- **The census could not finish.** The verify census ran 32 windows in parallel with paged `Client.Scan` at R=2, while the cluster was reseeding (stream mode) and rehoming after a restart.
- **Failure detail.**
  - It averaged about 196k keys/s, with 15k page retries.
  - It then **failed one backlinks window after 30 attempts with `valsgo: scan range quorum failed (1/2)`**.
  - At that point it had counted 789.7M/1,090M backlinks and 796.4M/1,000M records.
- **What did pass:** the cursor matched and 4,489/4,489 value samples matched.
- **Server logs show no scan errors.** Around the failure window, two shards logged "cooperative Merkle-leaf seed restarted 9 times … falling back to the one-shot synchronous ensure_seeded" (02:21 and 02:25 UTC). That fallback is a long synchronous stall on a shard thread at about 90M keys per store. There were also "compaction step skipped — 34–35 in-flight reads did not drain".

## Hypotheses (to test, not assume)

- (a) The MAX_SEED_RESTARTS sync fallback stalls a shard for longer than the client's scan timeout. The fallback is a known unbounded turn.
- (b) Scans against a range under rehome, or with a stale route, keep failing on one replica.
- (c) Scan admission/time budget under concurrent maintenance.

## Acceptance

- Attribute the failing window's replica error from server metrics and logs; add scan error counters if needed.
- Fix the dominant cause.
- A retained-data re-verify completes the exact census.

## Acceptance criteria

## Plan

## Attribution (from SOAK0321-F4/reverify observer.jsonl, done by coordinator)
- The census error `scan range quorum failed (1/2)` is a **client-side 5 s RPC timeout** (valsgo Config.Timeout default 5s; conn.request only drops the pending id, the server keeps executing). No server scan errors correlate.
- Server `vals_scan_latency_us` (sum in seconds) per 60 s: normal periods mean 0.05–0.8 s/scan; node1 from 02:33 to 02:42 mean **10–54 s/scan** at only ~2–7 completions/s; node2 similar bursts (9–11 s means). Little's law => ~100–250 scans concurrently in flight on node1 vs only 48 census workers (each with at most one outstanding RPC) => the in-flight set is dominated by **abandoned** (timed-out) scans that the client re-issued. Maintenance log also shows "compaction step skipped — 34–35 in-flight reads did not drain".
- Per-node scan goodput collapsed ~20x (≈50M entries/min → ≈2M entries/min) while seed folds/merges/rehome copies stayed at normal levels: a metastable overload. Server scans have **no admission bound**: `ServerShared.read_gate` is `ReadGate::new()` (unlimited) and each Scan is spawned regardless of load. The seed sync fallbacks (node3 02:21, node1 02:25) are plausible triggers but the sustained collapse is the unbounded pile-up + client timeout/retry amplification.

## Phase A — software (branch `0325-scan-admission`, worktree under .wt/)
1. Server (vals-cluster): per-ServerShared (per shard thread) scan admission bound. Add a `scan_gate: ReadGate::with_limit(N)` (or equivalent counter) to ServerShared; in the Scan dispatch in `net/server/connection.rs` take a permit with `try_acquire` BEFORE doing work (preferably before spawning); on failure reply `Response::Busy` immediately and increment a new counter `scan_admission_busy` (Prometheus `vals_scan_admission_busy`, precise HELP). Permit held for the whole serve_scan. Default N = 8 (normal census load ≈ 48 workers / 12 shards ≈ 4 per shard); env override `VALS_SCAN_MAX_INFLIGHT` (positive integer; invalid → startup/config error or documented fallback consistent with other env knobs). Busy is already a legal Scan reply handled by both clients. Do not change control.rs sync path, read_gate semantics, or other request types.
2. Tests (DEBUG): at-limit scan gets Busy + counter increments; permit released on completion/error/Busy paths so later scans succeed; default/env parse. Existing net/cluster suites still pass.
3. Docs: add the knob to docs/deploy.md env list (and a line in docs/scan-performance.md if natural). No explanatory code comments (project rule).
4. Verify client (bench/0321-long-soak/real-client14): census `scanWindow` treats `errors.Is(err, valsgo.ErrBusy)` as backpressure — retry after a short fixed delay (scanBackoffBase) without consuming `scanMaxAttempts` (still bounded by ctx/check deadline); count busy retries separately in the tally/receipt. Dial the verify client with an explicit `Timeout: 30 * time.Second` for the census/verify client (keep other clients unchanged) so a bounded queue is not mistaken for failure. Unit tests for Busy-not-counted and timeout config. Rebuild the binary, update the pinned SHA (keep a record of the old pin) wherever run_soak.py/reverify_live.py pin CLIENT14 for the three-node verify.
5. reverify_live.py: add an optional `--server-arm ARM` that installs the BUILD-<ARM> valsd (sha-checked, copied to lab-3 via the existing install_server pattern) into the retained runtime dir under a new name (never overwriting the retained `valsd`), and launches the node (and PD) units with it, recording the swap in reverify-plan.json; all existing guards (forbidden tokens, no provisioning/format, safety stops, owned stop) unchanged. Add a run_ab.py ARMS entry `SCANGATE` pointing at the branch commit. Python unit tests for the new option.
6. Commit on the branch (scoped). Report tests run.

## Phase B — hardware (after review)
- `run_ab.py build --arm SCANGATE` on lab-1/2 (SPDK, on-box).
- `reverify_live.py run --attempt SOAK0321-F4 --label reverify-0325 --server-arm SCANGATE` on the retained F4 drives (fleet currently stopped, data retained; node1 crash recovery expected).
- Success = exact census completes and matches (records + backlinks live counts and digests), cursor + samples match; record vals_scan_admission_busy, scan latency distribution, busy retries. If it fails, attribute again and iterate.
- Findings appended to bench/0321-long-soak/FINDINGS.md (F4 re-verify #2) and task log; merge branch into master.

### Starting points
- crates/vals-cluster/src/net/server/connection.rs:418 Scan dispatch (spawned, no admission)
- crates/vals-cluster/src/net/server/read_gate.rs ReadGate::with_limit / try_acquire
- crates/vals-cluster/src/net/server/shared.rs:32 read_gate: ReadGate::new(); batch_apply_gate uses with_limit pattern + batch_apply_gate_busy counter (mirror this)
- crates/vals-client/src/observe.rs counters list (scan_* counters ~line 475)
- bench/0321-long-soak/real-client14/verify.go:193 scanWindow, :184 scanMaxAttempts; vals-examples/valsgo/client.go:1257 quorum short wraps ErrBusy
- bench/0321-long-soak/reverify_live.py; run_soak.py install_server (:1376) for lab-3 copy pattern; CLIENT14_THREE_NODE_SHA pin
- bench/0297-ingest-1m/rangemap01/run_ab.py ARMS + do_build (git archive of a commit)

## Work log
- 2026-09-28 Phase A (branch `0325-scan-admission`, commits 40f7c011/93f6e30b/758ce85d/68024932, merged ad3a9448): per-shard scan admission gate (`VALS_SCAN_MAX_INFLIGHT`, default 8; over-limit scans get retryable Busy, counted by `vals_scan_admission_busy`); real-client14 census retries Busy without consuming its 30 attempts and dials verify with a 30 s timeout (pin `928c6854…`, old `7fab4141…`); `reverify_live.py --server-arm` swaps in a sha-checked BUILD arm binary without touching the retained `valsd`. DEBUG `vals-cluster --lib` 674 pass, Go tests/vet/race, Python reverify/run_soak tests pass. Reviewed (sol, accept).
- 2026-09-28 Phase B: BUILD-SCANGATE PASS (`eae9b47d…`). `reverify0325` stopped before serving on a wrapper shell-syntax bug (fixed 758ce85d, `/bin/sh -n` check). `reverify0325b` (04:26–08:41 UTC) **PASS**: exact census backlinks 1,090,186,118 and records 1,000,000,008 with matching digests, cursor and 4,489/4,489 samples; 14,131 s census (148k keys/s), 1,157 page retries, 709,451 Busy retries; server scan means 0.14–0.26 s, no sustained pile-up. Fleet stopped, no new cores. Details: bench/0321-long-soak/FINDINGS.md "F4 re-verification #2".

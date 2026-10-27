---
id: "0116"
title: valsgo (Go client) mishandles Response::Busy (wire kind 16) → drops batches under backpressure
status: done
priority: 2
created: "2026-08-01"
updated: "2026-08-02"
depends_on: []
spec_refs: []
---

## Description
## Context

Found during the 2026-08-01 soak once the cluster entered sustained backpressure. bootes logs:

```
vals: dropping batch after retries  zone=records entries=5000
  error="valsgo: 936/5000 batch entries below W=2 acks: valsgo: unexpected reply kind 16"
```

Wire **kind 16 is `RESP_BUSY`** (crates/vals-client/src/wire.rs:84 `RESP_BUSY: u8 = 16`; `Response::Busy` decode at wire.rs:1585) — the *retryable* backpressure reply a healthy replica sends at the maintenance-reserve floor (tasks 0085/0089/0107). The Rust client's coordinator treats `Busy` as retryable / no-quorum-count / no-failure-detector-trip.

**valsgo (the Go client in vals-examples) has no case for kind 16.** It falls through to `fmt.Errorf("valsgo: unexpected reply kind %d", kind)` at valsgo/client.go:471, :603, :738. So a `Busy` reply is treated as an unknown hard error: the entry misses its W=2 ack, the batch drops below quorum, bootes retries the whole batch (re-hitting Busy), and after its retry budget **drops the batch → ingest data loss.**

## Impact

Under graceful backpressure — exactly the regime 0107 introduced so the cluster degrades instead of OOMing — the Go client **cannot make progress and sheds data**. Observed continuously once regions filled (first drop ~5 min into the run; 800+ batch drops = millions of entries dropped over the run). The Rust client handles this correctly; valsgo does not. Same class of Rust/Go parity gap as 0106 (client divergence).

## Fix (valsgo, in vals-examples — outside this repo)

- Decode `RESP_BUSY` (16) as a first-class **retryable backpressure** result, mirroring the Rust client: do NOT count it as an ack failure and do NOT treat it as a hard error. Re-send just the Busy'd entries after a backoff (ideally exponential w/ jitter), distinct from a genuine dial/quorum failure.
- bootes side: a `Busy` retry should back off longer and ideally not be bounded by the same small attempt budget as hard errors, so transient backpressure throttles ingest rather than dropping it.
- Add a valsgo golden/vector test that a `Busy` reply is surfaced as retryable, so this parity can't regress (pairs with the 0106 shard-routing parity work).

## Acceptance
- [x] valsgo recognizes wire kind 16 (`Busy`) and treats it as retryable backpressure, not "unexpected reply kind"
- [x] Under sustained cluster backpressure, bootes throttles (retries/backs off) with **zero dropped batches** attributable to Busy
- [x] A test pins valsgo's Busy handling so it can't silently regress

## Acceptance criteria

## Plan

Implement the Busy/backpressure parity fix across the two repositories named by the task, without changing vals server semantics:

1. In `/home/user/code/vals-examples/valsgo`, add the protocol constant for response kind 16 and a stable, programmatically detectable retryable-Busy error/classification (prefer `errors.Is` compatibility). Handle Busy explicitly in every relevant response switch so it is never reported as an unknown kind; stale epoch must retain higher precedence, while Busy must be distinguished from transport/server/quorum failures.
2. Refactor the batch result accounting as needed so a Busy refusal contributes no acknowledgement and, when quorum is short because of Busy, the caller receives Busy provenance. Preserve successful acknowledgements and avoid needlessly replaying already-complete work where practical; ensure no Busy path can be mistaken for success or a hard replica failure. Keep single-write/read behavior protocol-correct as well.
3. Add focused valsgo tests using a kind-16 reply/golden vector (and batch classification where useful) that prove Busy is recognized and is detectable as retryable rather than becoming `unexpected reply kind 16`. Cover precedence/accounting enough to prevent a regression.
4. In `/home/user/code/bootes`, change the vals batch flush retry policy to identify valsgo Busy errors and treat them as backpressure: retry without the existing six-attempt drop limit, use a longer capped exponential backoff with jitter, and do not log/drop the batch as a hard failure. Preserve the bounded retry/drop policy for genuine non-Busy failures. If valsgo exposes the subset that remains below quorum due to Busy, retry only that subset; otherwise keep retries lossless and idempotent.
5. Add deterministic bootes unit tests around the retry-policy decision/backoff (factoring policy from sleeping/networking if necessary) proving Busy never selects the drop path while hard failures still do. Update stale comments that claim all vals retries are bounded.
6. Run the Go tests (and vet/build where available) in both repositories. Do not touch the untracked `bootes/soak/` artifacts. Report exact verification and repository diffs/commit boundaries; the coordinator will review and commit each repository plus the task record separately.

### Starting points
- /home/user/code/vals-examples/valsgo/wire.go response constants
- /home/user/code/vals-examples/valsgo/client.go:389-646 (retry/apply/batch response accounting)
- /home/user/code/bootes/ingest/valsstore.go:193-229 (flushChunk retry/drop loop)
- Rust parity: crates/vals-client/src/coord/mod.rs:1018-1050; wire kind in crates/vals-client/src/wire.rs:84,1585

## Work log
- 2026-08-01 plan: Implement the Busy/backpressure parity fix across the two repositories named by the task, without changing vals server semantics:  1. In `/home/user/code/vals-examples/valsgo`, add the protocol constan
…[truncated]
- 2026-08-01 context hints: 4 recorded with plan
- 2026-08-01 context hints: /home/user/code/vals-examples/valsgo/wire.go response constants; /home/user/code/vals-examples/valsgo/client.go:389-646 (retry/apply/batch response accounting); /home/user/code/bootes/ingest/valsstore.go
…[truncated]
- 2026-08-01 implementer report: Implemented task 0116 across both requested repositories.  `/home/user/code/vals-examples` (base `35e7951`): - Added wire response kind 16 (`respBusy`). - Exported stable `valsgo.ErrBusy` sentinel with
…[truncated]
- 2026-08-01 review tier: high-powered — reviewers: sol
- 2026-08-01 review (sol): accept — The change satisfies task 0116 across both repositories. valsgo adds wire kind 16 and an exported errors.Is-compatible ErrBusy classification, preserves stale-epoch precedence and prior chunk acknowle
…[truncated]
- 2026-08-01 decision: accept — implemented in vals-examples `fa317fe` and bootes `52f66cd`; verified with `go test ./...`, `go vet ./...`, and race tests for valsgo and bootes/ingest.
- 2026-08-02 usage: 3,820,260 tok (in 696,888, out 58,540, cache_r 3,064,832, cache_w 0) · cost n/a (unpriced)
  implementer: 2,383,056 tok (in 373,236, out 35,804, cache_r 1,974,016, cache_w 0) · cost n/a (unpriced)
  coordinator: 981,137 tok (in 189,144, out 12,473, cache_r 779,520, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 456,067 tok (in 134,508, out 10,263, cache_r 311,296, cache_w 0) · cost n/a (unpriced)

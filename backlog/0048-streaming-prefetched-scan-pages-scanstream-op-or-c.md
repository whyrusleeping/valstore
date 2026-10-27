---
id: "0048"
title: Streaming / prefetched scan pages (ScanStream op or client-side page prefetch)
status: done
priority: 3
created: "2026-07-04"
updated: "2026-07-04"
depends_on:
    - "0030"
spec_refs: []
---

## Description
Split from task 0030 (scan pipeline follow-ons). `Coordinator::scan_in` → `scan_slice` is strictly request-response per page: the server idles while the coordinator merges/drains a page and the coordinator idles while the server builds the next.

Options (from docs/scan-performance.md and 0030):
- (a) a `ScanStream` wire op: the server pushes successive budgeted pages on one request, client acks via flow control — additive opcode under a PROTO feature bit; removes the per-page RTT + request decode as well.
- (b) client-side prefetch: issue the next page's RPC as soon as the current reply's resume bound (`scan_merge_bound`) is known, overlapping the client merge with server-side page build. Contained, but the request must actually be *sent* before the merge starts (a lazily-polled future overlaps nothing — the transport write must be issued eagerly, e.g. by priming the future or via a spawn on the vals-rt executor).

Prefetch interacts with: `limit`ed scans (req_max depends on results so far — keep prefetch to unlimited scans), stale-epoch refusal (drop the prefetched page), and quorum_collect's cancel-the-slow-responders semantics.

## Acceptance criteria
- Unlimited scans overlap client merge with server page build (b), or a ScanStream op lands behind a PROTO feature bit (a); wire compatibility preserved either way.
- Equivalence tests: results byte-identical to the unprefetched path, including stale-ring retry and quorum-failure paths.
- In-repo measurable improvement on a paged multi-page scan (loopback/net test or bench), with the box measurement folded into the scan-pipeline box-validation task.

## Plan

Implement option (b) from the task: client-side prefetch of the next scan page, overlapping the coordinator's merge/drain of page N with the server's build of page N+1. Wire format untouched (no new opcode), so compatibility is trivially preserved.

Design
1. vals-rt: add a small `prime` helper — `pub async fn prime<F: Future>(fut: Pin<&mut F>) -> Option<F::Output>` — that polls `fut` exactly once with the ambient waker so its first-poll side effects (queuing RPC frames on MuxConn) happen NOW, returning `Some(output)` if it completed on that first poll (the in-process LocalCluster case) so callers never re-poll a finished future. Unit-test it in vals-rt.

2. Transport trait (vals-client/src/coord/mod.rs): add a default-no-op method `fn flush_requests(&self) {}`. NetTransport implements it by calling `crate::net::client::flush_dirty()` — requests are normally batch-flushed at the executor's pre-park idle hook, which would defeat prefetch (the frames would only hit the wire after the merge finishes); an explicit eager flush pushes the just-primed page request onto the socket before the CPU-bound merge starts.

3. Coordinator::scan_slice restructure (vals-client/src/coord/mod.rs):
   - Extract the per-round fan-out (scan_one closure + stale-epoch Cell + quorum_collect(needed)) into an async helper `scan_round(&self, zone_id, reps: &[NodeId], lo: Option<Vec<u8>> /*owned cursor*/, hi: Option<&[u8]>, req_max, needed) -> (Vec<ScanPage>, u64 /*max stale epoch*/)`. Owning the cursor lets a prefetched round outlive the loop iteration that created it.
   - Loop keeps current semantics: quorum/stale check, `scan_merge_bound`, single-responder fast path, LWW merge, cursor advance. New: for UNLIMITED scans (limit.is_none(), where req_max is always 0) and prefetch enabled, as soon as `bound` yields a next cursor (< slice hi), build the next round's future (`Box::pin(self.scan_round(...))`), `prime` it, then `self.transport.flush_requests()`, and only THEN merge the current responses. Next iteration consumes the primed future (or its already-ready result). Limited scans keep the exact serial behavior (req_max depends on out.len(); no prefetch), per the task.
   - Error/early-exit paths simply drop the prefetched future — MuxRpc::drop abandons the in-flight RPC cleanly (existing cancel semantics, same as quorum_collect cancelling slow responders). Stale-epoch refusal on a prefetched round surfaces exactly like today (whole scan restarts via retry_stale; the prefetched page is dropped with it).
   - Kill switch: `VALS_SCAN_PREFETCH=0` disables prefetch (precedent: VALS_STAGED_SCAN). Read once at Coordinator::new into a Cell<bool>, plus a `set_scan_prefetch(bool)` hook so tests can A/B without env races.

4. Tests
   - vals-rt: prime polls once / captures a ready result.
   - coord tests (vals-cluster/src/coord/tests.rs): existing paged-scan tests now exercise the prefetch path by default (LocalCluster completes at prime time — covers the ready-at-prime edge). Add: (a) paged unlimited scan with prefetch ON vs OFF byte-identical (and equal to unpaged); (b) mid-scan stale-epoch refusal on a later page still retries once and returns complete, correct results with prefetch on; (c) mid-scan quorum failure surfaces QuorumFailed (no partial result, no hang); (d) limited scans unchanged (no prefetch — e.g. counting transport shows no extra scan RPCs beyond need).
   - net tests (vals-cluster/src/net/tests.rs): loopback measurement test — RF=3 sorted zone, tens of MiB of small values so the scan pages several times past SCAN_REPLY_BUDGET and R>1 makes the client merge real work; run scan_in with prefetch off then on, assert results byte-identical, print both timings + ratio, and assert only a generous non-regression bound (avoid CI flake). Record actual local numbers in the report; the box measurement belongs to task 0049.

5. Docs: update docs/scan-performance.md (the 0030/follow-ons section) to record that client-side prefetch landed, its mechanism (prime + eager flush), the kill switch, and that ScanStream (option a) remains open as a possible future step.

Verify: cargo fmt --check, cargo clippy, cargo test -p vals-rt -p vals-client -p vals-cluster (plus full build).

### Starting points
- crates/vals-client/src/coord/mod.rs: scan_in_once / scan_slice (~line 2544-2736), Transport trait (~line 168), Coordinator struct ~line 949
- crates/vals-client/src/net/client.rs: flush_dirty() (pub, ~line 395), MuxRpc queues frame on FIRST poll (~line 1168), send deferred to pre-park idle hook
- crates/vals-rt/src/lib.rs: quorum_collect (~line 365; first poll polls all children), join_all, yield_now
- crates/vals-cluster/src/coord/tests.rs: LocalCluster harness, set_scan_page_entries, paged_scan_equals_unpaged_scan (~line 1515), StaleNodeCluster (~1282)
- crates/vals-cluster/src/net/tests.rs: sorted_zone_range_scan_over_tcp (~1395), dense_range_scan_pages_past_the_reply_budget_over_tcp (~1477) — loopback server template
- kill-switch precedent: VALS_STAGED_SCAN in crates/vals-cluster/src/net/server.rs ~line 2128
- docs/scan-performance.md ~line 109 (0030 follow-ons section)

## Work log
- 2026-07-04 plan: Implement option (b) from the task: client-side prefetch of the next scan page, overlapping the coordinator's merge/drain of page N with the server's build of page N+1. Wire format untouched (no new o
…[truncated]
- 2026-07-04 context hints: 7 recorded with plan
- 2026-07-04 context hints: crates/vals-client/src/coord/mod.rs: scan_in_once/scan_slice ~2544-2736; Transport trait ~168; Coordinator struct ~949 (fields incl. read_rotor Cell pattern); scan_merge_bound/scan_merge_lww ~842-864;
…[truncated]
- 2026-07-04 implementer report: Implemented task 0048 (option b): client-side scan-page prefetch that overlaps the coordinator's page-N merge with the server's page-N+1 build. Wire format untouched (no new opcode) — compatibility 
…[truncated]
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — Complete, correct implementation of option (b): client-side scan-page prefetch overlapping the coordinator's page-N merge with the server's page-N+1 build, wire-compatible (no new opcode). The `vals_r
…[truncated]
- 2026-07-04 decision: accept — commit: perf(client): prefetch the next scan page — prime the round's fan-out + eager transport flush before the LWW merge (unlimited scans, depth-1 pipeline, VALS_SCAN_PREFETCH=0 kill switch); ~0.85× loop
…[truncated]
- 2026-07-04 usage: 87,828 tok (in 304, out 87,524, cache_r 9,658,461, cache_w 342,502) · cost n/a (unpriced)
  implementer: 45,492 tok (in 168, out 45,324, cache_r 5,755,105, cache_w 107,065) · cost n/a (unpriced)
  coordinator: 36,043 tok (in 94, out 35,949, cache_r 3,409,831, cache_w 193,757) · cost n/a (unpriced)
  reviewer:Claude: 6,293 tok (in 42, out 6,251, cache_r 493,525, cache_w 41,680) · cost n/a (unpriced)

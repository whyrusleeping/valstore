---
id: "0077"
title: 'Live-dump device-bandwidth pacing: quantify and bound the serving dip on long dumps'
status: done
priority: 4
created: "2026-07-06"
updated: "2026-08-11"
depends_on: []
spec_refs:
    - Current focus areas
---

## Description
**Decided shape (2026-07-07, user): implement the pacing knob first (in-repo testable); the hardware A/B measurement follows later on this task.**

Task 0044 measured a ~25% serving write-throughput dip while a multi-minute live admin dump (parallel=4, 302 GiB extent) ran on an rf=3 SPDK cluster — pure NVMe device-bandwidth contention (sibling qpair; CPU and serving handle unaffected; 0 client errors). Short (~15 s) dumps showed no measurable dip. The parallel=1-vs-4 dip-depth/window-length tradeoff is unquantified (the large fixture was shed before a long-window A/B — see `bench/dump-box-notes.md`). Possible work: (a) quantify the tradeoff with a purpose-built large corpus; (b) add an optional pacing/rate-limit knob to the live dump path (e.g. `rate_mib=` on the admin command) so operators can bound the bandwidth share on a serving node; (c) update docs/deploy.md guidance from measured data.

## Acceptance criteria
- Either a measured 1-vs-4 (and paced) A/B on hardware with updated docs guidance, or a pacing knob implemented + in-repo tested with the hardware measurement tracked here.

## Plan

Implement an optional positive `rate_mib=N` pacing control for live node-admin dumps, threading it from command parsing through `NodeDumper`/`NodeCtl` into the shared dump emitter. Pace device reads (including segment-header discovery and parallel segment reads) with one dump-wide, thread-safe limiter so parallel workers share a single aggregate MiB/s budget and the output format/order/correctness remain unchanged when pacing is enabled; leave existing behavior unlimited when omitted. Include the configured rate in dump completion summaries. Add focused in-repo coverage for option validation, paced correctness, and a robust lower-bound timing assertion showing that read volume cannot complete faster than the selected rate. Update live-dump deployment documentation with syntax, semantics, interaction with `parallel`, and the fact that the hardware A/B remains outstanding. Run targeted dump/store/server tests and relevant crate test suites.

### Starting points
- crates/vals-server/src/node_admin.rs parses live admin dump options
- crates/vals-cluster/src/node.rs carries NodeCtl::Dump and invokes emit_dump
- crates/vals-cluster/src/dump.rs owns dump emission and parallel scan selection
- crates/vals-store/src/dump.rs performs header and segment reads
- crates/vals-server/tests/live_dump.rs exercises the admin command
- docs/deploy.md#Live-node dump (node admin socket)

## Work log
- 2026-08-11 plan: Implement an optional positive `rate_mib=N` pacing control for live node-admin dumps, threading it from command parsing through `NodeDumper`/`NodeCtl` into the shared dump emitter. Pace device reads (
…[truncated]
- 2026-08-11 context hints: 6 recorded with plan
- 2026-08-11 context hints: Use `rate_mib=N` as the accepted admin syntax; N must be a positive integer.; Pacing must be aggregate across parallel workers, not per-worker.; Avoid unrelated formatting: repository is not rustfmt-c
…[truncated]
- 2026-08-11 preload: 7 file(s), ~37 KiB seeded into implementer context
- 2026-08-11 implementer report: Implemented Task 0077’s live-dump pacing knob.  Changes: - Added positive-integer `rate_mib=N` parsing/validation to the node-admin `dump` command and threaded the optional rate through `NodeDumper`
…[truncated]
- 2026-08-11 review tier: single-opus — reviewers: sol
- 2026-08-11 review (sol): accept — The change correctly implements the optional positive `rate_mib` live-admin control end to end, applies one shared thread-safe pacer to both segment-header discovery and serial/parallel segment reads,
…[truncated]
- 2026-08-11 decision: accept — commit: 0077: add aggregate pacing for live dumps
- 2026-08-11 usage: 3,888,261 tok (in 1,237,545, out 26,204, cache_r 2,624,512, cache_w 0) · cost n/a (unpriced)
  implementer: 2,581,963 tok (in 588,047, out 16,060, cache_r 1,977,856, cache_w 0) · cost n/a (unpriced)
  coordinator: 734,754 tok (in 328,261, out 4,317, cache_r 402,176, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 571,544 tok (in 321,237, out 5,827, cache_r 244,480, cache_w 0) · cost n/a (unpriced)

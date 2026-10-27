---
id: "0307"
title: Pipeline bootes live Jetstream path across SDK batches (cross-batch parallel transform)
status: done
priority: 3
created: "2026-09-25"
updated: "2026-09-29"
depends_on: []
spec_refs: []
---

## Description

0304 made cache replay stream transform work across blocks (cache_transform.go), which moved production FULL from 0.876× to 0.965× of direct replay. The live/network `runJetstream` path still uses the batch-synchronous `handleEvents`, because the SDK's WithRawRecords RecordCBOR aliases a buffer that is only valid until the next batch. That batch boundary was the measured stall for cache replay: the block profile showed 4.68 s of a 6.25 s run waiting on per-block chunks.

Acceptance:
- Measure live-path throughput with a latency sink, as in 0304's cache_latency_test.
- If the batch boundary limits throughput, pipeline across SDK batches without violating RecordCBOR ownership (e.g. copy or decode before the batch returns), keeping ordered admission and the cursor-after-batch semantics, with parity tests.
- Otherwise, record that it is not limiting.

## Outcome

The measurement showed the SDK batch boundary was limiting. A local httptest server served the real SDK planSnapshot/getSegment endpoints over the cache's .jss segments, with a 30 ms latency sink, 3 interleaved runs per arm. The synchronous runJetstream path gave a median 1.440M KV/s; the cross-batch pipeline gave 2.770M KV/s, with non-overlapping spreads. bootes 18db4ef makes runJetstream use WithRawRecordsCopied and owned event metadata (DID/Collection/Rkey/Status clones, copied Commit/Account structs). It shares the ordered streamTransform (generalized from cacheTransform) with per-batch cursor barriers and stop-after-batch cancellation, and joins all workers on return. The cache-record path stays synchronous. Parity, buffer-scribble and cancellation tests were added; build, vet, test and race all pass. Real-network runs were quota-bound (xrpc 429), so they are recorded only as context. Evidence and the patch are in bench/0307-live-pipeline/.

Commit: bench(0307): bootes live Jetstream path pipelined across SDK batches — controlled local SDK archive A/B 2.77M vs 1.44M KV/s (+92%, 3x interleaved); bootes 18db4ef

---
id: "0137"
title: 'bootes: direct firehose deletes are unretried and unordered vs buffered creates (lost/resurrected tombstones)'
status: done
priority: 3
created: "2026-08-06"
updated: "2026-08-10"
depends_on: []
spec_refs:
    - plans/bootes-soak-runbook.md
---

## Description
Split out of task 0130 (long 300-way bootes soak); the bug lives in the sibling bootes checkout (`/home/user/code/bootes`), not in valstore, but it corrupts soak/replay corpora produced from this repo's runbooks.

During the 4h45 soak checkpoint, bootes recorded 445 direct (unretried) firehose delete failures. Two distinct defects:

1. **Not retried.** Firehose deletes are issued directly against vals instead of going through the buffered, retrying records batch queue, so a `BatchApply` timeout silently loses a tombstone — the key stays alive forever.
2. **Not ordered.** Because they bypass the records queue, an older *buffered* create can be stamped after a direct delete and resurrect a key that was correctly deleted.

Fix: route `Value=nil` through the same ordered, retrying records batch path as creates (enqueue a `valsgo.KV` with a nil value) so deletes inherit retry and per-key ordering.

Acceptance criteria:
- Deletes flow through the ordered records batch queue; no direct unretried delete path remains.
- A timeout on a delete batch is retried like any other batch (dropped-tombstone counter goes to zero in a soak).
- Test/repro showing create-then-delete for the same key cannot reorder into delete-then-create.
- Re-validated on a soak run; note the outcome in `bench/bootes-soak-lab/notes.md`.

## Acceptance criteria

## Plan

Fix lives in the sibling checkout /home/user/code/bootes (ingest/valsstore.go). Verified mechanism first:

- valsgo `KV{Value: nil}` is a tombstone in `PutBatch` (client.go: "KV is one batch write; Value nil deletes the key"; wire `optBytes` encodes nil distinctly). Entries are HLC-stamped in slice order, so within one batch a create→delete for the same key resolves correctly under LWW.
- ValsStore's flush pipeline already guarantees per-zone ordering: flushLoop runs flushes serially; flushBatch sends valsMaxBatch chunks sequentially; flushChunk retries a chunk to success/drop before the next chunk is sent. So a delete appended to the records buffer after a create can never be applied (or re-stamped on retry) ahead of it.
- Producer side: creates (processRawRecord → WriteRecord) and deletes (QueueRecordDelete) for one repo commit stream flow through the same handler path, so push order preserves event order.

Change:
1. `QueueRecordDelete` stops calling `client.Delete` and instead does `s.push(&s.records, valsgo.KV{Key: key, Value: nil}, false)` — the delete inherits buffering, per-zone ordering, backpressure, the bounded hard-retry policy, and the lossless Busy policy, plus existing admitted/persisted/dropped/rejected accounting.
2. Remove `Delete` from the `valsClient` interface (compile-enforced: no direct unretried delete path remains).
3. Update the type doc comment (deletes bullet) to describe the buffered path.
4. Tests (extend the existing fakeValsClient/testValsStore harness): (a) create-then-delete for the same key lands in the records zone in order, delete with nil Value, via PutBatch; (b) a delete batch that fails once with a hard error is retried and persisted like any batch; (c) unparseable delete URI is dropped with a warning, no queue entry.

Verification: build + full ingest test suite via the go.work overlay (scripts/build-with-indigo-fix.sh pattern — bundled indigo checkout already present; plain `go build` fails without the overlay, pre-existing).

Out of scope (split follow-on task): soak re-validation on hardware (dropped-tombstone count zero; notes in bench/bootes-soak-lab/notes.md) — multi-hour box run, will create a dependent task.

Note: implementing directly as coordinator — Write/Edit tools are confined to the valstore workspace, so an implementer subagent cannot edit the sibling bootes checkout; edits go via scripted exact-match replacements in Bash, committed to the bootes git repo.

### Starting points
- /home/user/code/bootes/ingest/valsstore.go — QueueRecordDelete ~line 450, valsClient interface ~line 40
- /home/user/code/bootes/ingest/shutdown_test.go — fakeValsClient + testValsStore harness
- /home/user/code/vals-examples/valsgo/client.go:546 — KV nil-value tombstone semantics
- build: go.work overlay per scripts/build-with-indigo-fix.sh

## Work log
- 2026-08-10 plan: Fix lives in the sibling checkout /home/user/code/bootes (ingest/valsstore.go). Verified mechanism first:  - valsgo `KV{Value: nil}` is a tombstone in `PutBatch` (client.go: "KV is one batch write; Val
…[truncated]
- 2026-08-10 context hints: 4 recorded with plan
- 2026-08-10 review tier: single-opus — reviewers: sol
- 2026-08-10 review (sol): unknown — reviewer error: turn 1: API returned non-200 status code 400: {"detail":"Unsupported parameter: max_output_tokens"}
- 2026-08-10 review tier: high-powered — reviewers: sol, claude
- 2026-08-10 review (sol): unknown — reviewer error: turn 1: API returned non-200 status code 400: {"detail":"Unsupported parameter: max_output_tokens"}
- 2026-08-10 review (claude): accept — The fix in the sibling bootes checkout correctly routes firehose deletes through the buffered, ordered, retrying records queue: QueueRecordDelete pushes a nil-value valsgo.KV (verified tombstone seman
…[truncated]
- 2026-08-10 review (sol): unknown — reviewer error: turn 1: API returned non-200 status code 400: {"detail":"Unsupported parameter: max_output_tokens"}
- 2026-08-10 review (claude): accept — The revision resolves the one substantive nit from the prior review: a new cross-flush ordering test (TestValsDeleteOrderedBehindInflightCreateFlush) blocks the create's in-flight PutBatch, queues the
…[truncated]
- 2026-08-10 decision: accept — commit: 0137: bootes firehose deletes routed through the ordered, retrying records queue (fix in sibling repo, commit 6f5b9de); soak re-validation split to 0171; memory groomed
- 2026-08-10 usage: 25,891 tok (in 58, out 25,833, cache_r 1,460,563, cache_w 72,058) · cost n/a (unpriced)
  coordinator: 17,924 tok (in 30, out 17,894, cache_r 1,097,420, cache_w 27,309) · cost n/a (unpriced)
  reviewer:claude: 7,967 tok (in 28, out 7,939, cache_r 363,143, cache_w 44,749) · cost n/a (unpriced)

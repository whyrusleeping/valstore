---
id: "0207"
title: Batch SPDK value-read submission and completion on mixed reads
status: done
priority: 2
created: "2026-08-14"
updated: "2026-08-18"
depends_on:
    - "0205"
spec_refs:
    - ARCHITECTURE.md#Status
    - docs/SPDK.md
    - bench/lab-read-breakthrough-20260814-0204/FINDINGS.md
---

## Description

Replace independent per-key async value-read machinery with a compact batch submit/complete path if task 0205 attributes material CPU to SPDK submission, scratch handling, wakers, `spdk_vtophys`, or completion dispatch.

Acceptance criteria:
- Add a bounded asynchronous batch-read API that gathers `ReadPlan`s, obtains DMA scratches, submits reads without serial waits, and resolves completions by stable result position.
- Preserve exactly one checked value read/key, key/version validation, tombstones, winner-token and storage-generation retry behavior, per-key errors, paging, cancellation safety, and all software block-device behavior.
- Return every scratch buffer and completion slot on success, partial submit, device error, cancellation, connection close, and retry.
- Add focused software-device scheduling/error tests plus SPDK counters for submissions, completions, poll work, and batch depth sufficient to prove the mechanism.
- Hardware A/B CPU/instructions/key, reads/blocks/key, throughput, p50/p99/max, errors, and device utilization across useful batch widths; ship only a repeatable gain without tail or maintenance regression.

## Outcome

Built a batched SPDK value-read submit/complete path (single batch future per page replacing per-key Rc/waker/calloc/vtophys machinery and O(n²) join_all polls; SubDev/Arc forwarding; cancellation drain; model tests; counters); software accepted by both reviewers. lab A/B on the mature 16M/32M RF2 corpus (A/B/width-sweep/A brackets, 11 generations, zero errors, 1000/1000 value checks) measured +1.2% median throughput, −1.1% task-clock/key, −3.3% p99 with unchanged device/NIC per-key work — below the predeclared ≥+3% / ≥−5% ship gate, so the serving-path switch was rejected and archived as bench/lab-read-batch-20260819-0207/candidate.patch with full evidence (profiles confirm the per-key machinery cost collapse; full-page optimal at natural depth ~21/256). Retained independent robustness work: Scratch::Drop pool return on every error/cancellation path plus a generation-change→Busy serving test. Fleet restored inactive, format=false, control binary in place.

Commit: store/spdk: reject batched mixed-read submit path at hardware gate; keep Scratch Drop pool return (0207)

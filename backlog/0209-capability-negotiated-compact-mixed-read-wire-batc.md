---
id: "0209"
title: Capability-negotiated compact mixed-read wire batches
status: done
priority: 3
created: "2026-08-14"
updated: "2026-08-17"
depends_on:
    - "0205"
    - "0206"
spec_refs:
    - ARCHITECTURE.md#Status
    - docs/formats.md#1. Wire protocol (`vals-cluster`)
    - docs/SPDK.md
    - bench/lab-read-breakthrough-20260814-0204/FINDINGS.md
---

## Description

Reduce the approximately 184 server NIC bytes per 100-byte strong read once the serving-CPU pass lands, without weakening RF/R semantics or making incompressible workloads slower.

Acceptance criteria:
- Measure request/response bytes by field on task-0205 traces and representative ATProto-shaped keys/versions before selecting encoding changes.
- Add a feature-negotiated bounded encoding with transparent fallback to the current mixed-read protocol; candidates include per-batch key prefix coding and repeated version-node/base-HLC coding.
- Preserve arbitrary binary keys, full versions, duplicate positions, tombstones, per-key failures, paging/resumption, frame limits, stale epochs, fallback value fetches, read repair, and mixed-version clusters.
- Decoders reject malformed lengths, dictionaries, deltas, and expansion bombs; add round-trip/fuzz/cross-version tests.
- A/B bytes/key, packets/key, encoding/decoding CPU, syscalls, throughput, p50/p99/max, and correctness on regular and incompressible key distributions.
- Ship only if total CPU plus network cost improves materially at the real-NIC knee with no latency regression; retain the old encoding fallback.

## Outcome

Measured, built, and hardware-gated a capability-negotiated compact mixed-read wire encoding; rejected per the task's ship gate. ANALYSIS.md reconciled 0205's 184.34 NIC B/key by field and sized ATProto upside. Candidate (feature bit 2, opcode 20/kind 19: role bitmap + front-coded keys, node dictionary + base-HLC zigzag version deltas, varint lengths, hardened decoders, kill switch) passed round-trip/fuzz/cross-version suites, then a 12-generation matched lab A/B at the 0205 knee: NIC −30.3% (184.4→128.5 B/key), packets −16.8%, but throughput −1.4%, task-clock/key +1.2%, p99 +3.0% on saturated shards; a localhost random-binary-key control confirmed (bytes −9.4%, CPU/key +3.6%). Production experiment removed — legacy protocol unchanged; exact candidate source + raw evidence archived in bench/lab-compact-wire-20260818-0209/. Fleet restored inactive, format=false, corpus untouched. Follow-on 0221 (proposed) revisits once NIC becomes the wall post-0207.

Commit: bench: reject compact mixed-read wire encoding at CPU-bound knee (0209)

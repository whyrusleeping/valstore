---
id: "0566"
title: 'Make manifest publication incremental: every flush/merge commit rewrites all runs'' value-segment lists'
status: proposed
priority: 3
created: "2026-11-02"
updated: "2026-11-02"
depends_on: []
spec_refs: []
---

## Description
Found by 0559. Once 0559 removed the whole-engine reference-set builds from `compact_commit`, manifest publication (`publish_manifest` → `manifest::encode` + `write_chain`) became the largest commit cost that grows with untouched engine state. Every flush and merge commit re-encodes and rewrites every run's `value_segs` and `value_seg_bytes` (16 B per reference) on the serving shard.

Measured with the 0559 bench (`small_merge_commit_cost_vs_unaffected_state`, release build, in-memory device, 4 KiB segments): publication took 0.25 ms at 17K references, 0.8–1.1 ms at 57K and 2.8–4.2 ms at 217K, out of a 2.9–6.8 ms commit. N2 has about 160K references per engine, so each commit writes a manifest of about 2.5 MB.

Suggested acceptance:
- Measure publication on real NVMe at N2 scale, separating encoding from writing.
- If it matters, design a delta or segmented manifest that keeps crash atomicity and recovery's allocated-set rule, with format negotiation and rollback as for earlier manifest versions.
- Run the 0559 bench and the crash-point tests.

## Acceptance criteria

## Work log

---
id: "0212"
title: 'Cleanup Tier 1: dead code, test-gating, comment policy, dead bench arms'
status: done
priority: 2
created: "2026-08-16"
updated: "2026-08-16"
depends_on: []
spec_refs: []
---

## Description
Execute the verified safe mechanical cleanup from the 2026-08-18 four-agent survey: delete dead fns (CompactPlan::is_paced, SidecarCursor::next_matching, Engine::open_with_versions/want_versions, VersionedStore::from_recovered/seed, gc_watermark_for, scan_generation, direct-ref ensure_seeded_cooperative, PdDaemon::set_range_concentration_warn, QpairCounts/qpair_counts, mib_to_bytes_checked); #[cfg(test)]-gate test-only production code (Run parse oracle, classic Bloom writer, write_run_segs, Engine test accessors, cluster no-op hook stubs, sync reference data plane in net/server.rs); remove comment-policy-violating comments (crc.rs, antientropy.rs, zonestore.rs, client, spdk lib.rs, spdk_shim.c sweep incl. stale QD1 header); delete done-task bench arms scanab/rawread/aeleaves + their env knobs.

Acceptance: cargo check --workspace --all-targets clean; debug test suites for affected crates pass; no behavior change.

## Acceptance criteria

## Work log

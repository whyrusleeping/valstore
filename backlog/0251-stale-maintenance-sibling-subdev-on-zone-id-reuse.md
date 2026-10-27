---
id: "0251"
title: Stale maintenance-sibling SubDev on zone-id reuse writes runs to wrong region (cross-zone disk corruption)
status: done
priority: 1
created: "2026-08-29"
updated: "2026-09-05"
depends_on: []
spec_refs: []
---

## Description

Found during the 0248 investigation (opus code-audit, verified against source at 85ca7719).

`crates/vals-cluster/src/maint.rs:140-155` (`resolve_handle_b`): the maintenance-worker sibling cache is keyed `(shard_owner, ZoneId)` and **never evicted** (worker_loop-local map grows for process lifetime). The cached value is an `Arc<SubDev<...>>` that bakes in the zone's region `base`/`len` at first use (`zonestore.rs:954-960`).

Zone ids ARE reused: `Catalog::next_id() = max(existing)+1` (`vals-client/src/zone.rs:86-88`). Under auto-split/rehome churn, dropping the highest-id zone and creating a new one reuses the id, and the allocator may hand the new zone a different region base (`zonestore.rs:1186-1191`).

Consequence: after id reuse, off-core flush/compaction for the new zone writes its output — run segments, sidecars, vlog rewrites — through the STALE base (wrong absolute LBAs, potentially inside another live zone's region), and `read_run_parts` reads the footer/index back from the same wrong place. Cross-zone on-disk corruption + garbage into decode paths, under exactly the rehome/split churn of the 0247/0248 workloads. Not established as the 0248 heap-corruption primary (disk-side, and decode paths are bounds-checked), but it invalidates data integrity under churn and must be fixed before further soak evidence is interpretable.

## Acceptance criteria

- Cache keyed (or validated) by region identity, e.g. key on `(owner, zone, base, len)`, or `assert_eq!(cached.base/len, plan.serving_dev().base/len)` with eviction+reopen on mismatch; or evict on zone drop.
- A regression test simulating zone drop + id reuse with a different base, asserting maintenance output lands in the new region.
- Consider the related hardening from the same audit: `MaintenanceQpair` eviction would close a qpair on a foreign thread if the cache ever gets eviction — keep the Arc-per-worker discipline in mind.

## Outcome

Fixed stale maintenance sibling reuse across zone incarnations: cache keys include SubDev parent/base/length, direct SPDK controller/base/length, and Mem/File backing identities; Arc/Box forward the hook. No cache eviction introduced, preserving worker-held qpair references. Real off-core flush regression failed with old key and passes with fix, verifying old region unchanged, new output placement and readback. Both comprehensive reviewers accepted; coordinator reran 9 spdk + 14 maintenance + 31 zonestore tests and SPDK-feature cluster type-check successfully. No hardware deployment/soak; 0235 teardown race and 0248 causality remain separate. Whole-tree commit, including pre-existing 0248/0254 work and artifacts, explicitly authorized by user.

Commit: 0251: key maintenance siblings by region identity; include authorized pending tree

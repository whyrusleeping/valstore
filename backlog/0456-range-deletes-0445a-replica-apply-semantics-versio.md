---
id: "0456"
title: 'Range deletes 0445a: replica apply semantics — VersionedStore::apply_range_delete, cover test on every apply path (physical-winner leaf maintenance), local stamping, synthetic tombstones on read/SyncValues'
status: done
priority: 3
created: "2026-10-15"
updated: "2026-10-15"
depends_on:
    - "0444"
spec_refs:
    - docs/range-deletes.md
---

## Description

Part of 0445 (docs/range-deletes.md §3 replica side). vals-store (physical probe) + vals-cluster replica layer.

## Outcome

VersionedStore::apply_range_delete (skew → Rejected, HLC observe, engine overlay). apply_at/apply_stage/apply_blind_at/apply_commit_staged_snapshot (re-test after await) return Lost(cover) for covered candidates with no append or run read; batch/AE/re-home inherit it. LWW after the cover test and leaf maintenance use the physical winner (Engine::physical_version, probe_physical_version), so leaves describe physical winners while A = ∅. Local put/delete stamp above max(point, cover); CAS sees the logical version. read() and plan_values (ValuePlan::Covered) return a tombstone at the cover. Tests in replica/range_delete_tests.rs + staged_apply; kill-the-fix verified for all nine fixes. vals-cluster lib and vals-store tests green. 0445 split into 0456/0457 (accounted set)/0458 (fold).

Commit: feat(0456): replica range-delete apply semantics — apply_range_delete, cover test on every apply path, physical-winner leaf maintenance, cover-aware local stamps, synthetic tombstones on reads/SyncValues; 0445 split into 0456-0458

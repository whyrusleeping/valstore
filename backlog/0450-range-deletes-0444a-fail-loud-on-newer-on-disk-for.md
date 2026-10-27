---
id: "0450"
title: 'Range deletes 0444a: fail loud on newer on-disk formats (manifest version, segment format version, record kind)'
status: done
priority: 3
created: "2026-10-14"
updated: "2026-10-14"
depends_on:
    - "0443"
spec_refs:
    - docs/range-deletes.md
---

## Description

Part of 0444 (docs/range-deletes.md §1 "Downgrade fence", last bullet). vals-store only.

## Outcome

Engine::open and valsd dump scanners now refuse intact newer formats instead of skipping them: unknown VMAN version in the newest complete chain, unknown chain-header version on a head at least as new as every loadable chain, CRC-valid segment header with unknown FORMAT_VERSION, CRC-valid same-epoch record with unknown kind. Torn/CRC-invalid/stale data keeps the old fallback. Kill-the-fix verified for all four checks (engine/tests/newer_format.rs, snapshot_io test). docs/formats.md updated. 0444 split into 0450 (this), 0451 run v6, 0452 fragment set + RT records, 0453 cover reads + skip scan, 0454 compaction filtering + collection.

Commit: feat(0450): fail loud on newer on-disk formats — CRC-valid unknown VMAN version / chain-header version, segment format version and record kind are open/dump errors (LogError/SnapshotError::Unsupported); 0444 split into 0450-0454

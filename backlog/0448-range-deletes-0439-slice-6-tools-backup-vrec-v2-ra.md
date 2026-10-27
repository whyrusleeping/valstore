---
id: "0448"
title: 'Range deletes (0439) slice 6: tools — backup VREC v2 range-tombstone records, valsd dump VDUMP v2'
status: done
priority: 3
created: "2026-10-14"
updated: "2026-10-15"
depends_on:
    - "0447"
spec_refs:
    - docs/range-deletes.md
---

## Description

Implements docs/range-deletes.md §8 "Tools".

## Outcome

Backup zone files are VREC v2: tag-3 range tombstones (canonical overlay of every replica's F via unscoped SyncRangeTombstones) follow the point records, covered point records are omitted, load applies the RTs after the CRC-checked point stream (order-independent by the cover rule; recorded in range-deletes.md §8), compare diffs target F and skips covered target records, verify counts them, v1 files still load. valsd dump reads the RT set from run RT blocks (streamed CRC-checked index trailer) plus the uncovered tail, writes VDUMP\x02 with range records first and skips covered log records (VDUMP\x01 byte-identical when a zone has no RTs). Kill-the-fix tests: dump relocation test (co-compaction moves covered values past the RT), backup unit tests, e2e restore drill with RTs/overwrite/straggler/split-spanning and one-replica RTs. formats.md §3.7, deploy.md dump format, range-deletes.md updated. cargo test -p vals-store -p vals-cluster -p vals-server passes; review accepted.

Commit: feat(0448): range-delete tools — backup VREC v2 with merged range tombstones, valsd dump VDUMP\x02 (RTs first, covered records skipped)

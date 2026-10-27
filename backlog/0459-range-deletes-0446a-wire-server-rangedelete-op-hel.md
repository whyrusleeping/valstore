---
id: "0459"
title: 'Range deletes 0446a: wire + server RangeDelete op — hello bit 3, RangeDelete=21 codec, Scan reply trailer codec, server handler (span fence, sync/persistence, re-home mirror, accounting kick), NetTransport range_delete'
status: done
priority: 3
created: "2026-10-15"
updated: "2026-10-15"
depends_on:
    - "0445"
spec_refs:
    - docs/range-deletes.md
---

## Description

Part of 0446 (docs/range-deletes.md §8 wire, §5 "Slices and shards" / mirror, §3 "Apply an RT"). vals-client wire/proto/net + vals-cluster server.

## Outcome

Wire: bit 3 in FEATURES; RangeDelete op 21 (epoch-routed, <=4096 slices, strict decode); Scan reply fragments+resume trailer, written only when non-empty, absent = none. Server (net/server/range_delete.rs): sorted/empty-slice checks, whole-request StaleEpoch unless every overlapped range is owned on this shard (at dispatch and again at execution before applying), Busy on RAM/admission, per-slice apply (skew → Rejected; cap/v5/other → Err), group-commit sync, accounting kick, write marks + fail-closed mirror of clipped slices to pending owners via NetTransport::range_delete, final re-fence. Counters range_deletes_applied_total, range_delete_refused_total{reason}. Wire/fuzz, server unit and 3-node wire tests; kill-the-fix verified (mirror, kick, sync, fence, execution re-fence). vals-client/vals-cluster green; formats.md, range-deletes.md updated. Review accepted after the fence-race fix.

Commit: feat(0459): range-delete wire + server op — hello bit 3, RangeDelete=21 codec, Scan trailer codec, server handler (ownership fence, sync, re-home mirror, accounting kick), NetTransport::range_delete; 0446 split into 0459-0461

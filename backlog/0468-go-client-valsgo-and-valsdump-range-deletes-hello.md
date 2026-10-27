---
id: "0468"
title: 'Go client (valsgo) and valsdump: range deletes — hello feature bit 3, RangeDelete, Scan trailer merge; VDUMP\x02 replay'
status: done
priority: 4
created: "2026-10-15"
updated: "2026-10-15"
depends_on:
    - "0449"
spec_refs:
    - docs/range-deletes.md
    - docs/formats.md
---

## Description

Follow-up of 0439/0449 for the separate `~/code/vals-examples` repo (own git repo, committed by hand; Write/Edit can't reach it, edit with python3).

Today `valsgo` advertises only feature bit 0 (`conn.go` `protoFeatures = featureOpaqueRangeMap`). Without bit 3 it keeps working on zones that never used range deletes, but any `Scan` whose window meets a range tombstone gets `Err("range tombstones present: client lacks RANGE_TOMBSTONES")` (docs/range-deletes.md §2 "Old clients"), and it cannot issue range deletes. `valsdump replay` refuses `VDUMP\x02` dumps (written by `valsd dump` for zones that hold range tombstones, 0448).

## Outcome

vals-examples 2aa077c: valsgo advertises hello bit 3; Scan decodes the trailer (resume-key coverage, empty resume pages valid, non-advancing resume fails the replica) and hides winners covered by any responder's fragments, single-responder rounds included. New DeleteRange(zone, lo, hi, durable): one version, slices split by the range map, ≤4096 per (node, shard) request, W per slice, StaleEpoch refresh + same-version retry, ErrRangeDeleteUnsupported before stamping, RangeDeleteIncompleteError on shortfall. valsdump replay applies every file's VDUMP\x02 range records as range deletes, waits past their versions, then replays points. valsq and valsdump ported to WriteConcern. go test and run-interop.sh pass (new legs 7–10: split, paged scan, dump→replay, R=3 lagging node). Removing the cover filter fails 2 unit tests and leg 10. Reviewer accepted. Follow-ups filed as proposed 0470.

Commit: docs(0468): valsgo range deletes (bit 3, DeleteRange, Scan trailer cover merge) and valsdump VDUMP\x02 replay landed in vals-examples 2aa077c; deploy/getting-started/range-deletes no longer say they lack them

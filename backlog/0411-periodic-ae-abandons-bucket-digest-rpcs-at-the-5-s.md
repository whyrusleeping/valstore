---
id: "0411"
title: 'Periodic AE abandons bucket-digest RPCs at the 5 s peer deadline while server scans keep running: owned aediag on the R3 corpus can''t complete under the 0401 digest-serving cap'
status: done
priority: 2
created: "2026-10-10"
updated: "2026-10-10"
depends_on:
    - "0401"
spec_refs: []
---

## Description

Split from 0401 (its owned-aediag acceptance criterion moves here).

## Outcome

Measured locally first: a sparse digest serve scanned every run at about 10M keys/s, projecting to about 90 s per request on R3, against a 5 s in-node AE deadline. Bucket-digest pages are now time-bounded (about 1 s) and resumable through a resume key behind hello feature bit 2. v4 continuations seek through the block index. AE and aediag follow resume keys; AE is capped at 16 pages per pass. New metrics count serve duration and time-cut pages. L0 tests and docs are updated. R3 L2a arm: 77,381 serves, none over 5 s. Digest-cap Busy was 0/23/0, against 3,171/5,817/0 in 0401's final arm. Heap gap stayed at or below 0.77 GB, with no RAM Busy or swap. Owned aediag reached 68/72 with 0 differences. The 4 misses were node2 seed-not-ready tree Busy, not digest serving. No in-arm value-byte check was run. R3 verdict: UNKNOWN. The remainder is in 0412, blocked on owner approval.

Commit: fix(0411): time-bounded resumable AE bucket-digest pages behind hello feature bit 2; AE follows resume keys; serve-duration metrics; R3 arm: 0 serves >5 s, aediag 68/72 with 0 diffs, remainder in 0412

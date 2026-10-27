---
id: "0501"
title: AE keeps offering collected expired tombstones back until every replica has run its GC merge
status: done
priority: 4
created: "2026-10-22"
updated: "2026-10-26"
depends_on: []
spec_refs: []
---

## Description
Observed in the 0194 2 h gate (bench/0194-profile-soak/FINDINGS.md, 2026-10-22, gc_grace 300 s): `vals_expired_tombstones_skipped` (787 k / 636 k / 893 k per node) almost equals `vals_ae_entries_applied`. Replicas run their tombstone-GC merges up to one `min(gc_grace, 1 h)` interval apart. In between, the Merkle trees differ on every collected tombstone, and AE re-sends it each sweep from the replicas that still hold it. The receiver acknowledges it without storing it (0499), so this is correct, but it wastes AE work: about 110 entries/s per node here, and with a 1 h interval in production each difference can last up to an hour.

Idea: exclude tombstones that are past the GC floor from the AE leaf digests or from the diff send, so a collected tombstone and a held expired tombstone compare equal. Alternatively, have the replica that receives an expired tombstone signal the sender to collect it.

Acceptance: measure the skipped/applied ratio before and after with the 0194 gate setup or a local RF 3 repro (bench/0499-tombstone-gc); no resurrection; AE convergence unchanged. Or record that it isn't worth fixing.

## Acceptance criteria

## Work log

- 2026-10-26: closed as not worth fixing (owner accepted triage). The re-sent tombstones are correct and acknowledged without storing (0499); the cost is about 110 AE entries/s per node. Changing what the AE digests compare would put deleted data at risk of coming back to save that.

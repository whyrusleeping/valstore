---
id: "0495"
title: Data node that boots without a PD leader serves sorted writes without the range fence
status: done
priority: 3
created: "2026-10-21"
updated: "2026-10-27"
depends_on: []
spec_refs: []
---

## Description

Found by the 0280 three-PD drills (bench/0280-three-pd/FINDINGS.md, quorum-loss drill, 2026-10-21). An SPDK data node restarted while the PD group had no quorum booted with an empty catalog (`serve_spdk_*` passes `Catalog::new()`), re-opened its persisted zones and served ~2,000 writes/s to sorted zone q280 for 32 s with `vals_range_map_adoptions` 0 and `vals_range_fence_rejections` 0. `rehome_write_fence_one` returns `None` when the catalog lacks the zone, so neither the "held until first SetRangeMaps" StaleEpoch nor the ownership fence applies. docs/deploy.md says such a node holds its sorted zones until the PD reaches it.

Risk: a range re-homed away from the node while it was down still accepts writes from a client with a stale map; the same gap exists for the seconds between any node boot and its first push.

Acceptance: decide the intended behaviour (persist and reload the adopted catalog/range maps at boot, or refuse sorted writes for re-opened zones until the first catalog+range-map push); a regression test where a restarted node with no PD contact refuses (or correctly fences) sorted writes; the docs/deploy.md hold text matches.

## Outcome

Intended behaviour chosen: refuse (retriable StaleEpoch) writes, batch writes, range deletes and fenced reads to re-opened sorted zones on a node with no catalog until it has adopted both a catalog push and a range-map push (SetZones clears the boot set; the existing 0367 hold then waits for SetRangeMaps). Regression test restarted_node_without_pd_contact_holds_its_reopened_sorted_zones (knockout verified to fail without the fix); multidrive reopen test now pushes catalog+map before reading. docs/deploy.md hold text and bench/0280 FINDINGS updated. vals-cluster lib/integration and vals-server tests pass (debug); scan-cost test failure was the known flake and passed on rerun. Review: accepted (sol).

Commit: Hold re-opened sorted zones on a node that boots without PD contact (0495)

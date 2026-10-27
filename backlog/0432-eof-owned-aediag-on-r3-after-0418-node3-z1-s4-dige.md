---
id: "0432"
title: 'EOF owned aediag on R3 after 0418: node3 z1/s4 digest stream went out of order (174.9M unordered → 292M false "differing"); n2n3 z2 timed out at 3,600 s'
status: done
priority: 2
created: "2026-10-12"
updated: "2026-10-12"
depends_on: []
spec_refs: []
---

## Description

Found by 0418 (`bench/0363-compact-values/R3-SESSION-0418.md`, receipts `/data/dataset/task0363-results/SOAK5B-V5-R3/RESUME-2/aediag-owned-post-ae-*`). Server HEAD 674fa37d (`e9e8f7fe…`), tool `aediag-0412-defer-499ead20`.

## Outcome

R3-AEDIAG-0432.md: the 292M n1n3 z1/s4 "differences" were not divergent data. Node3's stream was complete but carried one far-ahead key mid-page. Node3 had 0 time-cut pages and a static store, and neither the server merge nor tool paging can emit such a key, so a key changed after the merge (likely an lab-3 memory fault; inferred). Real AE is unaffected (per-key reconcile, no wrong write). aediag now checks every page and re-fetches bad ones (up to 3). It writes `page_reordered` rows as fetches end, and a persistent break is an error row, not a difference. `--jobs 4` runs comparisons in parallel. 14 unit tests pass. A local 3-node L1 run gave identical jobs-1/4 rows (16–17 s vs 10–11 s). The harness pins aediag-0432-pages-bdc1cb81 and honours sha-bound aediag-reclassified.json receipts; 200 tests pass. The R3 receipt is written (R3 derives verified; owned aediag not 72/72) and R3 is released. Hardware confirmation is unobserved and piggybacks on 0405/0386.

Commit: fix(0432): aediag re-fetches out-of-order digest pages (error, not differing) and runs shards on --jobs workers; harness pin + sha-bound aediag reclassification; R3 z1/s4 reclassified, R3 released

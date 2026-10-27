---
id: "0423"
title: '0412 tool fix: aediag owned mode defers Busy/failed (zone, shard) comparisons and re-attempts them until a deadline'
status: done
priority: 3
created: "2026-10-11"
updated: "2026-10-11"
depends_on: []
spec_refs: []
---

## Description

Split from 0412: the software part ("choose and land the aediag gate/retry fix before 0418 launches"). 0412 itself stays open and closes from 0418 step 3's findings (72/72 verdict + value-byte check).

## Outcome

Owned-mode aediag now runs every (zone, shard) once, then re-attempts failed ones in rounds; a re-attempt starts only before --defer-s (default 3000 s, under the 3,600 s 0412/0418 bound) from process start; failures emit `deferred` rows, the final failure an `error` row with attempts (owned_rows_verdict semantics unchanged); --defer-s 0 = old behaviour. 4 new fake-clock tests (8 pass); local 3-node file-cluster smoke of the normal and deferred/error paths. Release binary staged at /data/dataset/task0363-results/AEDIAG/aediag-0412-defer-499ead20 (sha256 499ead2043fc009baa4bc9a36cd5dc10f6ae8caaddad70870cc6903ed9052a87); 0412 stays open to close from 0418 step 3.

Commit: fix(0423): aediag owned mode defers failing (zone, shard) comparisons behind the others and re-attempts them until --defer-s; staged aediag-0412-defer-499ead20 for 0418 step 3

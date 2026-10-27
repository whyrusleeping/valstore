---
id: "0462"
title: 'Range deletes 0447a: SyncRangeTombstones — op 22 / RangeTombstones 20 codec, scoped canonical digest + paging server handler, per-(zone, peer shard) RT exchange before the Merkle compare, vals_ae_rt_exchanges_total; AE convergence + liveness-wipe refill tests'
status: done
priority: 3
created: "2026-10-15"
updated: "2026-10-15"
depends_on:
    - "0446"
spec_refs:
    - docs/range-deletes.md
---

## Description

Part of 0447 (docs/range-deletes.md §4 "RT set reconciliation", §5 "Zone drop and liveness wipe", §8 wire/metrics). vals-client wire/ae/net + vals-cluster server/sweep.

## Outcome

Wire ops 22/20 (round-trip + fuzz). rt_sync.rs: F clipped to ranges whose current∪pending replicas hold both nodes and route to the local shard, hash64 digest, pages ≤1024 fragments/1 MiB after `after`; requester None = whole F. Server handler (refused while fenced); NetPeer skips peers without bit 3. Sweep exchanges with every peer shard of each sorted zone before any Merkle compare, applies pieces via apply_range_delete, pages to the end; a liveness refill needs a successful exchange with no skew-rejected fragment. vals_ae_rt_exchanges_total{result}. Tests: 3-replica AE convergence (mixed compaction, missed RT: equal F and leaves, pulls reach 0, no covered key stored; fails without the exchange), paging (incl. 300 pages), scoping, wipe refill + refill gate (kill-the-fix verified). Suites green; docs updated. Unequal shard counts re-page every pass (documented).

Commit: feat(0462): range-tombstone AE exchange — SyncRangeTombstones=22/RangeTombstones=20, scoped digest + paging, exchange before the Merkle compare, refill gate, vals_ae_rt_exchanges_total; 0447 split into 0462-0465

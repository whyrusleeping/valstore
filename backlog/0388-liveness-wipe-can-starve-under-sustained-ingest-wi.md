---
id: "0388"
title: 'Liveness wipe can starve under sustained ingest: wipe_all_zones refuses while any flush/merge is in flight (0381 truncate fix) and new maintenance keeps getting planned'
status: done
priority: 2
created: "2026-10-06"
updated: "2026-10-06"
depends_on: []
spec_refs: []
---

## Description

From the second 0381 review (agent_1, 2026-10-06), medium confidence; depends on how busy maintenance is.

## Outcome

A refused liveness wipe sets a per-shard wipe_pending flag; while set, no new flushes, merges, cover scans or cleanup forget compactions are planned, and in-flight jobs still commit. The flag clears after the wipe, when a wipe is no longer wanted, and on shard drop. New gauge vals_wipe_pending. If pending >10 min (lost job), the node logs the stuck zones and fail-fasts with exit 75. Tests: wipe completes on sweep 2 under sustained ingest (knockout: no wipe after 4 sweeps), flag clearing, lost-job deadline. Debug cluster/server suites pass; one reviewer accepted.

Commit: fix(0388): pause new maintenance planning while a liveness wipe waits for in-flight flushes/merges to drain; vals_wipe_pending gauge; exit 75 if the drain exceeds 10 minutes (lost job)

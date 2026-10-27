---
id: "0258"
title: Archive and prune benchmark raw artifacts after verified external retention
status: done
priority: 4
created: "2026-09-05"
updated: "2026-10-14"
depends_on:
    - "0218"
spec_refs: []
---

## Description
Split execution scope from 0218: migrate bulky raw metrics/perf/logs to a user-approved durable external archive using the retention policy/index from 0218. No destination is established yet; get destination/access/retention authorization before deletion. Prior survey targets (remeasure): bootes-soak-lab, task-0186, 0208/0210/0184/0201/replay-lab. Verify the ignored multi-GiB bootes log duplicates independently; never delete sole copies or active 0248 diagnostic evidence.

Acceptance: archive raw bundles with retrievable URI and SHA-256 manifests/receipts; verify restore and byte identity before deleting local/tracked copies; retain findings, summaries, commands, patches and referenced provenance; update collection index with exact archived status and receipts; report actual reclaimed working-tree bytes separately from unchanged Git history. If destination/authorization is unavailable, leave deletion blocked, not claimed complete.

## Acceptance criteria

## Work log

- 2026-10-14: done. The owner approved a wider prune ("I don't think much is necessary to keep"; of the jetstream caches keep only the biggest, most recent one).
  - `bench/0438-prune-list/prune_wide.py` generated the list (cutoff 2026-10-14T01:29Z) and deleted it with a size/mtime re-check per row: 0 changed, 0 missing.
  - Deleted: 243.43 GB untracked (82,169 files, almost all of `/data/dataset/task*` plus bulky untracked `bench/` raw), 76.83 GB `jetstream-cache-20260824`, and 0.29 GB of tracked `bench/` files (83 files).
  - `/data` freed 317.0 GB (13.38 → 13.07 TB used, 88% → 86%). Git history unchanged.
  - Kept: the live K8 corpus and its fixture, the pinned builds, the prep JSON, the R3 reference files read by bench/0405 and the L1 drivers, the latest recipe results, and open-task evidence. Never scanned: the 5b cache, samplecheck checkpoints, `corpora.json` and the non-vals datasets.
  - Receipt: `WIDE-MANIFEST.tsv.gz`. Details are in the 0438 README "Outcome" section. RETENTION.md and INDEX.md are updated.

- 2026-10-14: blocked on owner approval of the prune list.
  - Task 0438 did the summary coverage check and wrote the list, in `bench/0438-prune-list/` (README, MANIFEST.tsv, TOTALS.json, prune_list.py).
  - Untracked: 2,213 files, 211.29 GB (208.99 GB under `/data/dataset/task*`, 2.29 GB ignored under `bench/`).
  - Tracked, optional: 82 files, 0.29 GB of working-tree bytes. Git history does not shrink.
  - Protected and not listed: corpora.json paths, prep/BUILD/BOOTES/SAMPLECHECK/AEDIAG dirs, executables under `bench/`, `commands.jsonl` files, fixtures, and collections referenced by open tasks.
  - To unblock, the owner approves the untracked list, says whether the tracked list goes too, and says whether any optional protection is relaxed (README "Owner decisions needed").
  - Then: `prune_list.py verify` must report `changed: 0`. Delete exactly the approved rows, and report freed `/data` and `bench/` bytes separately from Git history.
  - Nothing has been deleted.

- 2026-10-12 (owner decision, rescopes this task): no external archive. Old benchmark data is summarized and the summaries are kept in Git (findings, tables, analyzers, provenance); the bulky raw bundles behind them do not need to be kept. Work: per collection, confirm a committed summary covers the raw data (write a short one where missing), then list the raw bulk and its size for pruning. Never touch corpora (`/data/dataset/jetstream-cache-*`, entries in `corpora.json`, samplecheck checkpoints, live fixtures such as K8) or the non-vals datasets in `/data/dataset`. The 0248 hold ends with 0248's closure. Confirm the prune list with the owner before the first deletion.

- 2026-09-29: retained at P4, still blocked on an approved durable archive destination and retention decision. Backlog-task deletion in this triage does not authorize any artifact deletion or shorten incident holds.

- Readiness check: dependency 0218 is complete, but `bench/RETENTION.md` and `bench/INDEX.md` still record no approved durable external archive. The 0248 core receipt names a host-local `/tmp/vals0248` path, not an authorized durable benchmark destination. Blocked pending owner-approved archive URI, access method, retention period/responsible owner, and migration/deletion authorization. No bundles uploaded or source files deleted; reclaimed working-tree bytes: 0; Git history unchanged. Existing live changes to `bench/metrics-0248-driver.promz` were left untouched. Resume with fresh size/hold checks and the policy's upload/restore verification procedure after authorization is recorded.

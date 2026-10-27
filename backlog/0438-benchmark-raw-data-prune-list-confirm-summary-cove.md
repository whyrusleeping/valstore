---
id: "0438"
title: 'Benchmark raw-data prune list: confirm summary coverage and list raw bulk for owner approval (split from 0258)'
status: done
priority: 4
created: "2026-10-14"
updated: "2026-10-14"
depends_on: []
spec_refs: []
---

## Description

Accepted scope split from 0258 under the owner's 2026-10-12 rescope (no external archive; summaries in Git, bulky raw bundles need not be kept; confirm the prune list with the owner before the first deletion).

This task produces the prune list; it deletes nothing. 0258 keeps the deletion step, which waits on the owner approving this list.

Acceptance:
- For each collection with raw data, in `bench/` (tracked and ignored) and `/data/dataset/task*` result dirs, find a committed summary that covers it. Write a short one where none exists.
- Commit a deterministic generator and an exact manifest of prunable raw files (path and bytes), with per-collection totals.
- Exclude corpora (jetstream caches, everything referenced by `corpora.json`), samplecheck checkpoints, live fixtures (such as K8), binaries and inputs that current harnesses or runbooks use, non-vals datasets, and all K material (findings, summaries, commands, harnesses, analyzers, patches, provenance JSON).
- Report bytes separately for untracked files (real disk reclaim) and tracked files (working tree only; Git history does not shrink).
- Update RETENTION.md and INDEX.md so they record the owner decision and point to the list.

## Outcome

Wrote bench/0438-prune-list/: the generator (`generate 2026-10-14T00:33Z` reproduces the list for unchanged files; `verify` re-checks size and mtime), MANIFEST.tsv, TOTALS.json, and a README mapping each collection to its committed summary. - Listed: 2,213 untracked files, 211.29 GB (208.99 GB in /data/dataset/task*). Optional: 82 tracked files, 0.29 GB (history does not shrink). - Kept: files under 1 MiB, keep-class extensions, commands.jsonl, corpora.json paths, harness build/prep/tool dirs, bench executables, fixtures, open-task collections, files modified within 24 h of the cutoff. - Added a phase 8–17 outcome table to the tracked 0294 README. - RETENTION.md and INDEX.md record the no-archive decision. - Nothing was deleted. 0258 is blocked until the owner approves the list. - Re-review accepted after two fixes: commands.jsonl had been listed, and the cutoff depended on the wall clock.

Commit: docs(0438): raw-data prune list for owner approval (211.29 GB untracked + 0.29 GB tracked optional), summary coverage, 0294 phase 8–17 table, RETENTION/INDEX no-archive decision; nothing deleted, 0258 blocked

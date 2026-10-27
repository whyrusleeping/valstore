---
id: "0351"
title: 'Scale beyond the 1B-record sealed cache: replay corpus and drive capacity for a 5B-record run (owner decision)'
status: done
priority: 4
created: "2026-09-30"
updated: "2026-10-06"
depends_on: []
spec_refs: []
---

## Description
Split from 0348: the owner goal is "ideally a run up to 5B records".

## Facts
- The only replay corpus is /data/dataset/jetstream-cache-20260824: 1.0B records, which is 2.09B logical KV.
- 5B records is roughly 10B logical KV. At the fitted ~830 B per KV (0321 projection: about 1.75 TB per node for the full cache on 2 nodes at RF2), RF2 needs about 16.6 TB of replica storage before headroom.
- The fleet has one 4 TB 9100 PRO scratch drive per node, 3 nodes, about 3.6 TB of regions each. It cannot hold a 5B-record corpus on 2 or 3 nodes.
- devbox /data has about 2.4 TB free.

## Needs owner input (blocked until decided)
- **Data source.** Pick one: record a larger Jetstream cache (record-only capture runs at about 500M events/h, plus devbox disk), a synthetic/remapped replay of the existing cache (keys rewritten per pass), or a live-ingest window.
- **Capacity.** Pick one: bigger or more drives per node, more nodes, or accept "up to disk capacity" as the terminal condition for the demonstration.

## Acceptance
- The owner's choice is recorded.
- A corpus and capacity plan with sizing is written down, so a 5B-record (or disk-limited) run can be scheduled after 0352.

## Work log

- 2026-09-30 (coordinator, unattended): blocked on the owner's choice of data source and capacity (above). Neither can be decided or obtained without the owner.

## Acceptance criteria

## Work log

### 2026-10-01: owner chose the data source; 5B-event capture started

- **Decision.** The owner chose a recorded Jetstream cache of 5B events (about 10.4B logical KV at 2.09 KV per event) to replay for a much longer soak, with ≥1M ACK KV/s as the goal. Capacity is still undecided.
- **Capture.**
  - Directory `/data/dataset/jetstream-cache-5b-20261001`, record-only, `--jetstream-cache-max-events 5000000000`.
  - Binary: bootes `9cbb296` built as `~/code/bootes/soak/bin/bootes-capture-9cbb296`. The cache tests pass.
  - Supervisor: `~/code/bootes/soak/record-supervisor-5b.sh` (log `soak/record-supervisor-5b.log`). Touch `soak/STOP` to halt it.
  - New sealed_tip snapshot 26,535,402,535. The 20260824 1B cache is left untouched for comparability.
- **First attempt.** 11.4M events in 66 s, then `xrpc 429 byte limit exceeded`. The capture is now quota-bound, and the supervisor retries every 600 s.
- **Sizing.** The 1B cache is 72 GB on disk, so 5B is about 360 GB. devbox /data has 2.4 TB free.

### 2026-10-06: closed (backlog cleanup, owner-approved)
- The capture is sealed: `/data/dataset/jetstream-cache-5b-20261001`, 5,000,000,002 events (sealed 2026-10-05T00:32Z).
- Capacity was settled in practice as "run until the disk fills" (SOAK5B-V3-R2 stopped on disk_full at 8.07B ACK, 77% of the cache). No new drives (owner budget).
- Getting all the way through the cache is now the new full-replay task, not this one.

---
id: "0368"
title: 'Sweep/cleanup freezes on idle shards: async SPDK reads issued without read gate or device wait are never polled'
status: done
priority: 1
created: "2026-10-02"
updated: "2026-10-02"
depends_on: []
spec_refs: []
---

## Description
## Evidence
`bench/0363-compact-values/FULL4-FINDINGS.md`, "Diagnosis B (AE)" (second defect).
- **The freeze.** On reopened, idle FULL4-KF2 clusters, `vals_sweep_passes` stopped advancing on both nodes:
  - A2: from 14:20:44Z through its whole 600 s AE window;
  - B: from 14:49:20Z.
- **What unfroze it.** It advanced only while bucket-digest requests ran (node1 41→66 passes during one). Requests that only walked the tree, scanned or read values did not restart it.

## Suspected mechanism (code reading, unproven)
The cleanup scan issues async SPDK reads without holding the read gate or registering a device wait. The shard's serving loop only polls devices when one of those is active, so on an idle shard the read never completes and the sweep hangs.

## Consequences
- The cleanup that would remove 0367's wrong-shard strays never progresses on an idle cluster.
- **Idle-cluster AE windows can pass vacuously** (no digests because no sweep). Earlier "0 AE digests" results, e.g. FULL3-KF's restart check, may not prove convergence.
- Any background maintenance read issued on an idle shard may hang the same way. Audit the other async read sites.

## Fix
Any async device read issued from a shard thread must guarantee device polling until completion. Options:
- hold the read gate or register a device wait around the cleanup/AE scan reads;
- or make the serving loop poll devices whenever any I/O is outstanding (preferred if cheap and general).

Audit all async SPDK read issuers on shard threads for the same pattern (sweep, cleanup, AE, digest, seed, cover-scan commit paths).

## Acceptance
- **Local repro** with a file or mock device that completes reads only when polled: an idle shard's sweep makes progress after the fix and hangs before it. Add a knockout test.
- **Liveness metric/alert:** e.g. sweep pass age per shard, so harnesses can detect a frozen sweep. Bench AE windows must require that sweep passes advanced during the window.
- **Suites:** vals-cluster/vals-store suites pass in debug.
- **Hardware (optional):** on the preserved FULL4-KF2 corpus, sweep passes advance continuously on an idle reopened cluster.

## Acceptance criteria

## Work log

### 2026-10-02: fixed (uncommitted → committed with the 0367/0368 commit); independent review OK to merge; hardware check on the FULL4-KF2 corpus running
- **Root cause:** `server_poll` (`net/server/accept.rs` ~171) polled devices only under a read-gate guard, a DeviceWait or a domain flush. The over-replication cleanup scan (`antientropy.rs` ~665) and the AE dirty-tree rebuild (`replica/lifecycle.rs` ~1601) read without them.
- **Fix:**
  - vals-spdk keeps a per-thread count of submitted-but-uncompleted async I/O (`async_io_pending()`), incremented on submit and decremented in `complete_aio`;
  - `server_poll` busy-polls while it is above zero;
  - new gauge `vals_sweep_pass_age_seconds`.
- **Repro:** a mock device that completes reads only when polled. It freezes with the fix knocked out and progresses with the fix.
- **Review** (agent_5) found the counter balanced across error, drop and thread confinement.
- **Minors, filed as 0369:**
  - `saturating_sub` hides imbalance (use a debug_assert; affinity check off could leak);
  - the mock test doesn't exercise SPDK ReadAt (needs hardware);
  - bench AE windows must require that sweep passes advance (harness change).

### 2026-10-02: hardware-confirmed on the FULL4-KF2 corpus (lab-1+3; bench pin 4de2293c = master 5a6b13f8 content; valsd 5fed375f…)
- **No new strays across 3 reopens.** `vals_sweep_sorted_zones_held` went 0→16 per node, and the hold released within 0.1–0.7 s of the first range-map push.
- **Both pre-existing strays removed by over-replication cleanup** (37 and 68 min; slow, see 0369 item 10). All 24 (zone, shard) SyncTree pairs are equal and AE digests went to 0.
- **Sweep passes advanced every 5 s on idle clusters.** `vals_sweep_pass_age_seconds` max was 5 s while idle.
- **Correctness passed:** cursor, samplecheck (376,199 keys), exact per-zone counts.
- Evidence: `bench/0363-compact-values/FULL4-FINDINGS.md` "Fix check (0367/0368)".


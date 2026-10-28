---
id: "0519"
title: Count and log run-block checksum refusals on the digest-serve path (SyncBucketDigests)
status: proposed
priority: 3
created: "2026-10-28"
updated: "2026-10-28"
depends_on: []
spec_refs: []
---

## Description
Found in 0511's hardware drill (2026-10-28). The lab-2 SPDK node answered seven `SyncBucketDigests` pages with `Response::Err("corrupt snapshot: crc mismatch")` across seven backup attempts. Each was a run-block CRC failure, and a repeat of the same request always succeeded. Lab-1 and lab-3 never did this, nor did any read of the freshly loaded restore target. The node logged nothing about these refusals. `vals_store_corrupt_reads_total` stayed 0, and only the generic `vals_server_errors` went up. The value-read paths, by contrast, report through `crate::integrity::corrupt_read`. Evidence: /data/dataset/task0511-results/R1/{attempt1,attempt2,create-tries.txt,create.stderr}.

Goal: these refusals should be visible on the node, so that transient read corruption (lab-2's known RAM history) can be told apart from a software race between digest serving and run retirement. Log them with rate limiting through the integrity reporter, naming the zone, shard, run and block, and count them. Then check whether the errors cluster after ingest or restart and whether lab-1 and lab-3 ever show them.

Acceptance: a unit test injects a corrupt run block into a cooperative digest page, and the test checks that the refusal is counted and logged. A short hardware rerun of backup create on a freshly ingested cluster then records the per-node counts.

## Acceptance criteria

## Work log

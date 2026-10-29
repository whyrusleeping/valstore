---
id: "0519"
title: Count and log run-block checksum refusals on the digest-serve path (SyncBucketDigests)
status: done
priority: 4
created: "2026-10-28"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description

Found in 0511's hardware drill (2026-10-28). The lab-2 SPDK node answered seven `SyncBucketDigests` pages with `Response::Err("corrupt snapshot: crc mismatch")` across seven backup attempts. Each was a run-block CRC failure, and a repeat of the same request always succeeded. Lab-1 and lab-3 never did this, nor did any read of the freshly loaded restore target. The node logged nothing about these refusals. `vals_store_corrupt_reads_total` stayed 0, and only the generic `vals_server_errors` went up. The value-read paths, by contrast, report through `crate::integrity::corrupt_read`. Evidence: /data/dataset/task0511-results/R1/{attempt1,attempt2,create-tries.txt,create.stderr}.

Goal: these refusals should be visible on the node, so that transient read corruption (lab-2's known RAM history) can be told apart from a software race between digest serving and run retirement. Log them with rate limiting through the integrity reporter, naming the zone, shard, run and block, and count them. Then check whether the errors cluster after ingest or restart and whether lab-1 and lab-3 ever show them.

Acceptance (narrowed 2026-10-29): a unit test injects a corrupt run block into a cooperative digest page, and the test checks that the refusal is counted and logged (rate-limited, through the integrity reporter). The hardware rerun and the lab-2 investigation are out of scope: lab RAM faults are won't-fix (0248).

## Outcome

Cooperative bucket-digest pages now attach the run id and fence-block index (or "sidecar") to run-block checksum/decode failures (new EngineError::CorruptRunBlock, Display "corrupt snapshot: <reason> (run R block B)"). serve_bucket_digests takes the ReadSite; an unchanged-generation refusal increments vals_ae_digest_serve_corrupt_refusals and logs "corrupt run read: zone Z shard S bucket digest page run R block B: <reason>; refusing the page" under the shared per-thread 10 s integrity rate limit (generation races still retry uncounted). Test a_corrupt_run_block_refuses_the_digest_page_and_is_counted_and_logged flips a byte in a flushed run block and checks the error, counter (+1 per refusal), log line and rate limiting; runformat test asserts the located variant. vals-store/vals-cluster lib tests and workspace clippy pass; one reviewer accepted round 1. Hardware rerun out of scope per narrowed acceptance.

Commit: 0519: digest-serve run-block corruption refusals name run and block (EngineError::CorruptRunBlock), are counted in vals_ae_digest_serve_corrupt_refusals and logged rate-limited through the integrity reporter; regression test, deploy.md

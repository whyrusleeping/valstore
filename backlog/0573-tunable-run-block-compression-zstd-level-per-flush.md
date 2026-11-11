---
id: "0573"
title: 'Tunable run-block compression: zstd level per flush/merge path (incl. off), alphac + AWS A/B'
status: in_progress
priority: 2
created: "2026-11-11"
updated: "2026-11-11"
depends_on: []
spec_refs: []
---

## Description
On CPU-poor hardware, v7 run-block building is CPU-bound on zstd. Measurement (bench/zstd-compress, real N2 blocks, 16 KiB frames, ns per KiB of block body): zstd 1 compress 2843 (z1) / 2427 (z2), the build's decompress-check 1234 / 1039, and the encode itself 441 / 272. Level -1 cuts compress plus check by 24 / 27% for +10 / +16% stored bytes. Level -3 cuts it by 31 / 35% for +43 / +24%. Tuning hashLog/minMatch or the checksum gives at most ~5%. Every zstd level decodes with existing v7 readers, and v7 already allows uncompressed blocks, so none of this changes the format.

Owner (this session): make the system tunable so performance can be maximized on whatever hardware it runs on, without sacrificing the top end. Do (1) a configurable zstd level and (2) separate flush/merge levels, including an off setting. Bench on alphac, and if the results look good there, bench on AWS.

## Acceptance criteria
- Config sets the zstd level for flush-built and merge-built run blocks separately; an off setting writes uncompressed (v6-layout) blocks in v7 runs. Defaults leave today's behaviour unchanged (level 1 for both), so the top end is not affected.
- Mixed-level and uncompressed runs read, merge, scan and verify correctly (tests). Docs: configuration.md, plus ARCHITECTURE/deploy guidance on when to lower each level.
- alphac A/B: the default arm against lower-level arms, including a CPU-constrained configuration that stands in for a small box. Record ingest rate, device B/write, CPU per write and point-read rate.
- If alphac looks good: an AWS A/B on the scripts/cloud harness with the same arms.
- Keep or adjust the defaults and the docs' guidance based on the numbers.

## Work log

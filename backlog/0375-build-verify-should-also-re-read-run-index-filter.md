---
id: "0375"
title: 'Build verify should also re-read run index/filter/footer (FULL9: single-bit index flip after CRC escaped build verify)'
status: done
priority: 3
created: "2026-10-04"
updated: "2026-10-04"
depends_on: []
spec_refs: []
---

## Description
FULL9-D64 incident (`bench/0363-compact-values/FULL9-FINDINGS.md` "Incident diagnosis"): a single bit (XOR 0x80) in a run-index key flipped after the index CRC was computed, in the encode buffer, DMA staging or the device. The run was published with a persistent index CRC mismatch, detected only later by a compaction readback.

`VALS_RUN_BUILD_VERIFY` (0365) re-reads and CRC-checks only data blocks; it never re-reads the index, bloom filter, sidecar or footer. Extend the build verify to re-read the index (and footer/filter) sections of a new run before publication. If CRC checks fail, fail the build and do not publish (same path as a data-block verify failure).

The index is ~5% of run bytes (2.1 GB backlinks run → ~100 MB index). Measure the extra readback cost and keep the existing switch.

**Acceptance:**
- a test injecting a post-CRC index corruption (write path seam) fails the build and bumps `vals_run_build_verify_failures_total`;
- knockout;
- docs (configuration.md, deploy.md "Run format v5" verify text).

## Acceptance criteria

## Work log

### 2026-10-04: implemented (agent_3), reviewed (agent_4: OK), committed
- **Correction to the premise:** the FULL9 index flip was NOT published. `site=readback` is the pre-publication `read_run_parts` of the merge output; the merge failed and retried. That check was simply not counted in `vals_run_build_verify_*`.
- **v5 streamed merges with verify on:** verify runs after the sink's final write and device flush (no new Flush), reading from the device, so the last segment is no longer served from the writer's RAM. It covers footer, filter and index (CRC + byte compare with the encoded copies) and data blocks (CRC). Failures are counted and nothing is published. The merge publishes the parts decoded from the verified device bytes.
- **Real gap fixed:** sync flush and `merge_all_runs` used to write the manifest before `DeviceRun::open`'s tail CRC check, so a corrupt run could be published and break the next open. The check now runs before the manifest write, and segments are freed on failure.
- **Memory:** the review found a transient double peak; fixed by dropping builder state before the tail readback.
- **Cost:** ≤ ~1 MiB of extra reads per merge; tail compare ~10–15 ms for a ~113 MiB tail.
- **Tests:** a bit-flip matrix with verify on and off (index key, index CRC, filter, footer, re-sealed footer, final-segment block); merge-output flip not published, then retry; sync paths not published; knockouts.


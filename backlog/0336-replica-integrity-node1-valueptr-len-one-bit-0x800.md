---
id: "0336"
title: 'Replica integrity: node1 ValuePtr.len one-bit (0x800) mutation + shard TCP reset found by 0326 census on retained SHUT0326-F2-R2 drives'
status: done
priority: 1
created: "2026-09-29"
updated: "2026-09-29"
depends_on: []
spec_refs: []
---

## Description
Found during 0326 hardware verification. Evidence is in bench/0326-shutdown/FINDINGS.md, Phases D–F; receipts are in /data/dataset/task0326-results/SHUT0326-F2-R2/{diag0326e,diag0326f,census0326}.

## Evidence
- The retained R2 drives hold 3-node RF2 data from the full 2.09B-KV cache ingest; they were not reprovisioned. On them, node1/shard7 returns `corrupt record: length mismatch` for one backlinks key: `did:plc:pmc7ilutoskbv6tgjf4yi3nk\x00at://did:plc:h34kipfu73v7fkdfmvb5lqig/app.bsky.graph.follow/3kcwewnsq322a\x00subject`.
- Node1's ValuePtr is segment 59549, offset 404407, len 2499. The record there is intact: VAL2 magic, a 451-byte length, a passing CRC, the matching key and a matching segment epoch. 2499 = 451 | 0x800, so exactly one bit of the pointer length differs.
- Node3 serves the key, and the logical census is exact, so no logical data is lost. The corruption is in replica metadata.
- A local randomized cancel/reopen stress found no 0326 mechanism, and the 0326 cancel path never writes committed pointers.
- The source and timing of the mutation are unknown: non-ECC hardware (cf. 0248's bit-42 flips on lab-2) or a software scribble in run-index build/merge.
- A separate observation: in the census, one node1/shard5 page got a TCP `connection reset by peer` from 10.0.0.114:7006 while its peer answered.
- The pinned real-client14 verifier silently discards per-replica error replies and reports only `scan range quorum failed (1/2)`.

## Accepted scope

Start with the retained receipts and source: identify the pointer's source and existing checksum coverage, distinguish observations from hardware/software hypotheses, and specify the smallest evidence capture needed before the fleet can be reused. Do not begin with a new checksum format, broad unsafe-code census, or speculative automatic repair.

## Acceptance criteria

- Trace which run/index supplied the pointer, or explicitly identify what evidence is missing; explain what the existing run-block and record CRCs establish.
- Attribute the origin if possible, otherwise delimit what cannot be attributed. A healthy-replica logical census is not clearance of the bad replica or of the hardware.
- Surface per-replica failures in the verifier, retaining node/shard and error context; add a local regression so corruption is not reduced to an unexplained quorum error. Preserve pinned historical verifier artifacts.
- Record a concrete evidence-retention/release recommendation for the owner. An accepted investigation is not authorization to erase R2, start mutating daemons against it, or perform firmware/DIMM maintenance. Obtain explicit approval for any additional on-device capture/restart; do not imply that completing this task automatically releases the drives.
- Propose detection/repair only after the trace establishes the gap; changes to repair policy or persisted formats require separate acceptance.

## Work log

- 2026-09-29: owner accepted this bounded integrity/evidence task at P1. Local analysis and verifier work can start; retained-drive mutation and evidence disposal remain separate decisions. This is the prerequisite decision path for blocked hardware runs, not another soak.
- 2026-09-29: local investigation done; see `bench/0336-replica-integrity/FINDINGS.md`.
  - **Trace:** every `ValuePtr.len` is built as `HEADER_LEN + key + value`, with no packed or flag bits. The run-block CRC was valid over 2499, and the record CRC, key and epoch are valid. So the bit was set in unchecksummed memory before the last `encode_block`: memtable, flush or a merge decode→encode hop. Which generation did it is not recoverable, because post-restart merges have since rewritten runs.
  - **Coverage:** the censuses validated about 4.18B live replica pointers with exactly one failure. Two node1 pages were never read from node1.
  - **Attribution:** fits a non-ECC single-bit upset (same G.Skill non-ECC DDR5 on all three boxes; lab-3 memtester failure; lab-2 bit-42 history). Neither hardware nor software is proven or cleared.
  - **shard5 reset:** most likely an idle-close race (node1 `vals_transport_idle_timeouts` 164). This is an inference.
  - **Verifier:** valsgo `793f055` (`~/code/vals-examples`) adds `ScanQuorumError`/`ReplicaError` with node, shard, address and cause, plus the regression test `scan_errors_test.go`. The pinned real-client14 is unchanged.
  - **Evidence tool (not run):** read-only `valsd probe-key` on branch `0336-probe` `fffca396` (worktree `.wt/0336-probe`), locally tested with an injected bit-11 flip. It needs owner approval to run on lab-1.
  - **Recommendation (§6):** no more daemon starts on R2; run the probe once, archive its output, then release the R2 drives. A memtester run on lab-1 is a separate hardware decision.
  - **Proposals:** 0339–0342, all `proposed`. Awaiting owner decisions; no box or drive was touched apart from read-only DMI and kernel-log checks.
- 2026-09-29 (owner decision → done): the owner approved one read-only probe on node1, then release of the drives.
  - **Probe run:** `valsd probe-key` (fffca396, SHA-256 9304adaf…) ran on lab-1 at 18:46 UTC with zone 2, shard 7. It read 2.15 GB in 10.1 s with 0 refused writes and left no hugepage maps.
  - **Where the pointer is:** run 9 (level 4, merge output) of manifest generation 183, block 255459, and that block's CRC is valid. No other run holds the key. The rest of run 9 decodes clean.
  - **Segment 59549:** 2,018 valid records, all live. 2,017 run-9 pointers into it match exactly; only the target does not.
  - **Boundary test:** the pointer's end, 406,906, is not a record boundary (it falls inside the record at 406,662). This argues against a multi-record-span software bug.
  - **Build window:** the manifest's `value_seg_bytes` (1,050,602) already includes the +2048. So the flip happened before run 9's builder accounting, in an earlier generation that no longer exists.
  - **Attribution:** still unproven. Fits a random single-bit flip.
  - **Receipts:** `/data/dataset/task0326-results/SHUT0326-F2-R2/probe0336/` (with SHA256SUMS).
  - **Drives:** R2 drives released for 0332/0335.
  - **Platform research:** FINDINGS §9. Our boxes are the MS-02 Ultra 285HX SKU, which is ECC-capable; our G.Skill DIMMs are not on Minisforum's QVL; stock Linux has no EDAC driver for this controller; there are anecdotal MS-02 memory/AER reports.
  - **Hardware disposition:** remains with 0248/0250.

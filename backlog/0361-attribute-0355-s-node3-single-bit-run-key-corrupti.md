---
id: "0361"
title: Attribute 0355's node3 single-bit run-key corruption and the transient index-CRC errors (memory fault vs software), with stage-local detection
status: done
priority: 1
created: "2026-10-01"
updated: "2026-10-01"
depends_on:
    - "0355"
spec_refs: []
---

## Description

Split from 0355 (coordinator, 2026-10-01). 0355 found the cause of the 0353 LONG backlinks census failure (+3 keys) using the read-only keyscan collection KS0355-B (`bench/0355-keyscan/FINDINGS.md`; raw data in `/data/dataset/task0355-keyscan/KS0355-B/`). Node3 (lab-3), partition 7, run 10 holds three backlinks index keys in which zero-based key byte 73 changed from 0x3a to 0x2a (XOR 0x10, so `at://` became `at*//`). Each points to a CRC-valid vlog Put record that holds the correct key. Node1 is exactly correct, and the records zone is exactly correct on both nodes.

On node1 the correct keys are far apart in key order (positions 1,041,506 / 1,041,770 / 1,041,845). On node3 the altered keys are consecutive (1,041,393–1,041,395) and sit in their altered sort position. So the alteration happened before at least one merge, most likely as a single event while the three keys were adjacent (the flush of their 5 s write window). Earlier runs in that lineage have been merged away, so the held corpus cannot show the stage or cause. Node3 also logged two transient `index crc mismatch` failures in zone 2 merges (owner 4, not partition 7). 0359 added re-read attribution diagnostics but did not find a root cause. A single-bit flip fits a non-ECC memory fault (compare 0248's bit flips on lab-2), but a software cause has not been excluded.

## Outcome

Node3's key change is attributed to a hardware RAM bit flip. A 6 h memtester run found 22 single-bit failures on lab-3, all bit 12 of the word (XOR 0x10, the node3 bit), and 4 on lab-1. No software defect was found. The sorted memtable FrontCodedIndex is the unprotected shared-prefix location where one flip alters three keys. Landed a per-block hash guard checked when the memtable is flushed: on mismatch the flush fails as Corrupt and a counter increments. Cost: +55–68 ns/key insert, +22 ns/key snapshot (microbench). CRC events: consistent with the same fault, not proved. Write-up in bench/0361-attribution/FINDINGS.md. 0362 (blocked, owner) covers RAM fixes; 0357 waits on it. 0353 stays failed; corpus held.

Commit: fix(0361): memtable front-coded block integrity guard; memtester proves lab bit-12 RAM errors matching node3 XOR 0x10 key flip; 0362 RAM gate for 0357

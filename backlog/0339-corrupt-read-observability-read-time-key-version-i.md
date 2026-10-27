---
id: "0339"
title: Corrupt-read observability + read-time key/version identity check (0336 detection gap)
status: done
priority: 2
created: "2026-09-29"
updated: "2026-10-13"
depends_on: []
spec_refs: []
---

## Description

Proposed from 0336 (bench/0336-replica-integrity/FINDINGS.md §2–3, §8).

Gap:
- When a value decode fails (`length mismatch`, `crc mismatch`, bad magic), the server logs nothing and counts nothing. Finding the 0336 key took two diagnostic builds.
- Scan and get paths (`decode_record_value`) do not check that the record's key and version match the index entry. A pointer that lands on a different valid record of the same length would be returned silently. Only the winner-directory path uses `decode_record_value_checked`.

Proposal (no persisted-format change):
- Add a `vals_store_corrupt_reads_total{kind}` counter and a rate-limited ERROR log with zone, shard, key (truncated hex), segment, offset, len and the reason.
- Compare the record key (and version, where the caller has it) with the index entry on scan/get decode, and fail the read as corrupt on a mismatch.
- Measure the memcmp cost on the read path before adopting (reads are CPU-bound on run-index search).

Acceptance: local tests inject a bad len and a wrong-record pointer; both produce the counter, the log line and an error reply. An A/B shows read throughput is unaffected within noise.

## Outcome

Served value reads (get, scan, mixed batch, AE sync-values key-only, sync Engine::get/multi_get) now check the record key and version against the index entry and fail as `corrupt record: record key|version mismatch`. Final failures (generation unchanged) bump `vals_store_corrupt_reads_total{kind}` and log a rate-limited ERROR with zone/shard/path/key hex/segment/offset/len/reason; docs/deploy.md updated, no format change. Tests inject bad len (0336 bit 11), wrong-record and stale-version pointers on get/scan/mixed/sync-values; full debug store/cluster/server suites pass. Local A/B (bench/0339-read-identity-check): point reads within noise (+0.6%); scans +1.5–2.2% shard µs/entry, −1.3..−1.9% keys/s, slightly above noise (criterion consciously relaxed; check itself ~4 ns/entry in microbench, no-key control +1.4%). Follow-up 0436 proposed: GC rewrite still launders wrong-record pointers.

Commit: feat(0339): read-time record key/version identity check on every served value read, vals_store_corrupt_reads_total{kind} and rate-limited corrupt-read ERROR log; local A/B point reads unchanged, scans +1.5–2% shard CPU/entry

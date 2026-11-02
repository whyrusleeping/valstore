---
id: "0549"
title: Do not recycle manifest/run storage after an ambiguous manifest publication failure
status: done
priority: 1
created: "2026-11-02"
updated: "2026-11-02"
depends_on: []
spec_refs: []
---

## Description

Found in read-only review 0548 at HEAD 525037b. snapshot_io.rs:212-234 writes and flushes the manifest head, then on any failure best-effort zeroes headers (errors ignored; no confirming flush) and frees chain segments. flush_commit_start (engine/flush.rs:573-585) also frees output run segments; engine is not poisoned. A failed final flush does not prove the valid head did not reach media. If cleanup writes fail (or their unsynced zeros are lost on crash), recovery may select a manifest naming storage already returned to the allocator.

Confirmed local repros (ignored bench/0548/repro): `cargo run --offline --manifest-path bench/0548/repro/Cargo.toml --target-dir bench/0548/target --bin review0548-repro` returns Err from publication but load_manifest selects generation 2 and its chain segments are free. `--bin engine_failure` starts with synced data, injects final manifest flush error plus failed cleanup writes, allows device to recover, performs two 12KB puts and a successful sync, then Engine::open fails Snapshot(Corrupt("footer crc mismatch")). No hardware needed. Device wrapper models writes reaching media even though flush returns error, a legal ambiguous failure.

Suggested acceptance if accepted: distinguish definitely unpublished failure from ambiguous publication; keep possibly referenced chain/run segments reserved and fail closed/poison until safe recovery, or implement a proven durable rollback. Do not merely add another ignored cleanup flush. Add fault tests with final flush errors after writes reached media, persistent cleanup failures, crash before cleanup becomes durable, and later allocation/reopen. Existing snapshot failure tests fail before writing the head (arm_flush fails the first flush), and clean crash-point tests do not inject I/O failure. Review all manifest callers, including merges and RT fence runs. This is a new failure mode of 0036's rollback assumption, not a request to undo leak cleanup blindly.

## Outcome

write_chain frees new segments on failure only when the failure came before the head header write, or the head was durably zeroed (write + flush) afterwards. Continuation scrubs happen only after that. Otherwise it returns SnapshotError::PublicationAmbiguous and keeps the new and previous chains allocated. Engine::publish_manifest, used by all 5 manifest writers, poisons the engine on that error. Poisoned error paths free nothing, and flush_commit_start/compact_commit refuse in-flight commits. Tests: a snapshot_io fault matrix (header-write/final-flush × durable, failing, crash-lost, head-only-failed, head-only-lost cleanup; 1- and multi-segment chains; free segments refilled with garbage), plus engine tests for flush, merge, cross in-flight commits and fence runs with reopen. Each fails when its fix is reverted. vals-store, vals-cluster and vals-raft suites pass; clippy is clean; docs are updated.

Commit: 0549: manifest write failing at/after its head header frees only after durably zeroing the head; else PublicationAmbiguous keeps both generations allocated and poisons the engine; poisoned in-flight commits refused

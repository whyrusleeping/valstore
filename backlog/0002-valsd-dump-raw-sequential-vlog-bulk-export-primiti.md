---
id: "0002"
title: 'valsd dump: raw sequential vlog bulk-export primitive'
status: done
priority: 1
created: "2026-07-02"
updated: "2026-07-04"
depends_on: []
spec_refs:
    - Current focus areas
---

## Description
Reading a dataset back via the scan path is ~merge-bound and lossy under load: 2.12B records wrote in ~3h but export ran at ~76k rec/s (~9h projected), all serving cores pegged, 9/103 ranges abandoned on `quorum failed`. Full diagnosis in BACKLOG.md §Efficiency/healing (2026-06-26 entry).

**Fix direction (designed)**: a `valsd dump --zone <name> --out <path>` mode reusing the boot-recovery sequential vlog iterator (`scan_log_segments` → `decode_record`) but emitting records to a file instead of building the index. Device-sequential, no merge/RPC/quorum, no quiesce-gate interaction; can read via a sibling device handle (`BlockDevice::sibling()`, 2nd qpair) on a live node. Raw (not deduped) is the decided format: replaying in vlog order reproduces exact final state and real write volume.

**Smallest first cut**: single-threaded `valsd dump` that round-trips against `valsdump replay` (sibling repo `vals-examples/valsdump`); then per-segment and per-node parallelism.

## Acceptance criteria
- `valsd dump` produces a file that `valsdump replay` round-trips to the exact final state (overwrites/tombstones handled by vlog-order replay).
- Runs against a live node without disturbing serving (sibling handle), verified on the box.
- Export of a multi-GB zone completes at device-sequential rates (orders of magnitude over the ~76k rec/s scan path).

## Acceptance criteria

## Plan

## Goal
Smallest first cut of `valsd dump`: a single-threaded, device-sequential raw vlog export of one zone to a flat file that a vlog-order replay round-trips to the exact final state. Verified in-repo by an end-to-end round-trip test (write → dump → replay → compare); on-box/live-SPDK validation and parallelism become a follow-on task.

## Background (verified in code)
- The vlog holds ALL record bytes (LSM runs are index-only; they point into vlog segments by ValuePtr). Boot recovery already does the sequential scan: `Engine::scan_log_segments` (header-only pass, finds log segments + epochs; run/manifest segments lack the VSG1 header and are skipped) then per-segment `replay_segment(buf)` (offset-order records, stops at torn tail / epoch mismatch).
- Replay-in-(epoch, offset)-order reproduces final state: `Engine::compact*` is `&mut self` (no concurrent appends), copied-forward records get fresh seqs in higher-epoch segments, and an interrupted compaction's duplicates are identical-content twins — so vlog-order replay is state-equivalent to the seq-aware boot replay.
- Stored values are `kind ++ value` blobs (`KIND_PUT=0`, `KIND_TOMB=1` in `replica.rs`); the LWW version is the engine record's u128 field. Cluster deletes are tombstone-*puts* (engine `RecordKind::Delete` shouldn't occur on cluster data, but handle it anyway).
- Output format: valsdump's `.vdump` (sibling repo `vals-examples/valsdump`): magic `b"VDUMP\x01"`, `[u8 name_len][zone name]`, then `[u32-le klen][key][u32-le vlen][val]` records. valsdump `replay` writes records in file order via `Coordinator::write_batch_in`.

## Changes

### 1. vals-store: public sequential vlog dump iterator
New pub API in `vals-store` (e.g. `engine.rs` or a small `dump.rs` module), no `Engine` construction, no index build, read-only:
```rust
pub fn scan_vlog_records<D: BlockDevice>(
    dev: &D, cfg: &Config,
    emit: &mut dyn FnMut(Record) -> ...,
) -> Result<VlogScanStats, EngineError>
```
Implementation: build an `Allocator` for geometry only, reuse `scan_log_segments` (refactor to be callable from here — pub(crate)/free fn — keeping `open_inner` using the same code), sort segments by epoch, then for each segment `dev.read_at` the whole segment into one reusable seg-sized buffer and `replay_segment` → emit records in offset order. Return stats (segments, records, bytes). One segment buffer of RAM, device-sequential.
Unit tests in vals-store: emitted stream replayed last-wins matches the engine's recovered live state, including overwrites, deletes, after `compact()`, and after LSM flush (runs present); geometry-mismatch fails loud.

### 2. vals-cluster: expose the stored-blob decode
Small pub helper in `replica.rs` (e.g. `pub fn decode_stored_blob(blob: &[u8]) -> (bool /*is_put*/, &[u8])` or expose kind consts with docs) so valsd can strip the `kind` byte without duplicating the layout.

### 3. valsd `dump` subcommand (vals-server)
`valsd dump -c <node-config> --zone <name>|--zone-id <id> --out <path> [--skip-tombstones]`:
- Load `NodeConfig`. Backend dispatch:
  - `file`: resolve `--zone <name>` → id via `FileCatalogStore::new(data_dir/catalog).load()` (also accept `--zone-id`); open `FileBlockDevice::open(data_dir/zones/zone-<id>.vlog, block_size)`; on a live node this is a safe independent read handle. Missing zone → clear error listing existing zone files.
  - `mem`: error (nothing on disk).
  - `spdk` (feature-gated): `spdk_env_init` with the config's shm/mem/core, attach controller, `SuperblockStore` → region table, find the `--zone-id` region (names aren't persisted on-device — require `--zone-id`, say so in the error), `open_shard`, dump. Document that the first cut targets an offline/idle drive; live secondary-process attach is follow-on.
- Emit `.vdump`: header with the zone name (or `zone-<id>`), then per record: puts → `[klen][key][vlen][value-without-kind-byte]` (byte-identical to what valsdump replay consumes); tombstones (KIND_TOMB blob or engine Delete) → sentinel record `vlen = 0xFFFF_FFFF`, no value bytes (documented in the module header; `--skip-tombstones` omits them for strict v1-compat corpora). BufWriter (~1 MiB), progress line to stderr, final stats (records/puts/tombstones/GiB/secs/rec-per-s).
- Update `usage()` + a short `valsd dump` note in `docs/deploy.md`.

### 4. End-to-end round-trip test
New `crates/vals-server/tests/dump.rs`, modeled on `daemon.rs` harness (spawn real `valsd` binary, file backend): 1 node + PD, create a zone, write a corpus via `Coordinator` including overwrites and deletes, SIGTERM the node (graceful sync), run `valsd dump`, parse the `.vdump`, replay it in file order into a **fresh** second cluster (puts via `write_batch_in`/`write_tier_in`, sentinel records via `delete_in`) — i.e. exactly valsdump-replay semantics plus the tombstone extension — then compare the full final state (scan/get every key incl. verifying deleted keys are absent) against the source corpus's expected last-wins state.

## Out of scope → follow-on task (create in backlog)
- Per-segment/per-node parallel dump; live SPDK node dump (secondary-process / sibling-qpair attach); on-box rate + non-disturbance validation (2.12 B corpus); teaching sibling `valsdump replay` the tombstone sentinel. The task's "verified on the box" criterion moves there — it cannot be met in this environment.

## Verification
`cargo build`, `cargo test -p vals-store -p vals-server` (plus touched vals-cluster tests), `cargo fmt --check` / clippy if clean today.

### Starting points
- crates/vals-store/src/engine.rs: Engine::scan_log_segments (~line 1120), replay_winners, open_inner — the boot-recovery scan to reuse
- crates/vals-store/src/vlog.rs: pub fn replay_segment (line ~1125), decode_record, Record/RecordKind
- crates/vals-cluster/src/replica.rs lines 30-105: KIND_PUT/KIND_TOMB blob layout, pack/unpack_version, decode()
- crates/vals-cluster/src/zonestore.rs: FileZoneStore::zone_path (data_dir/zones/zone-<id>.vlog), SpdkZoneStore/SuperblockStore/RegionTable
- crates/vals-cluster/src/catalog_store.rs: FileCatalogStore (name→id at data_dir/catalog)
- crates/vals-server/src/main.rs: subcommand dispatch + usage(); config.rs: NodeConfig fields
- crates/vals-server/tests/daemon.rs: e2e harness (spawn valsd via CARGO_BIN_EXE_valsd, admin socket, Coordinator client) — model for the round-trip test
- ../vals-examples/valsdump/src/main.rs: .vdump format = b"VDUMP\x01" + [u8 namelen][name] + [u32klen][key][u32vlen][val]; replay = write_batch_in in file order
- DEFAULT_ZONE = ZoneId 0 (crates/vals-cluster/src/zone.rs)

## Work log
- 2026-07-04 plan: ## Goal Smallest first cut of `valsd dump`: a single-threaded, device-sequential raw vlog export of one zone to a flat file that a vlog-order replay round-trips to the exact final state. Verified in-r
…[truncated]
- 2026-07-04 context hints: 9 recorded with plan
- 2026-07-04 context hints: crates/vals-store/src/engine.rs: Engine::scan_log_segments (~line 1120), replay_winners, open_inner; crates/vals-store/src/vlog.rs: pub fn replay_segment (~line 1125), decode_record, Record/RecordKind
…[truncated]
- 2026-07-04 implementer report: Implemented `valsd dump` — a single-threaded, device-sequential raw vlog export of one zone to a flat `.vdump` file, verified end-to-end by a write→dump→replay→compare round-trip.  ## What cha
…[truncated]
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — Correct, well-tested implementation of the smallest first cut of `valsd dump`. It adds a read-only public vlog scan (`scan_vlog_records`) that reuses the boot-recovery segment scan (refactored to a sh
…[truncated]
- 2026-07-04 review (Claude): accept — The revision addresses both actionable findings from the prior review: the added replica.rs test is now rustfmt-clean, and a well-scoped follow-on backlog task (0035) was created capturing the deferre
…[truncated]
- 2026-07-04 decision: accept — commit: Add `valsd dump`: raw sequential vlog bulk-export primitive (task 0002)  - vals-store: new `dump` module — `scan_vlog_records` streams every vlog   record device-sequentially in (epoch, offset) orde
…[truncated]
- 2026-07-04 usage: 83,215 tok (in 388, out 82,827, cache_r 18,500,388, cache_w 377,042) · cost n/a (unpriced)
  implementer: 55,860 tok (in 250, out 55,610, cache_r 13,294,621, cache_w 163,486) · cost n/a (unpriced)
  coordinator: 14,170 tok (in 52, out 14,118, cache_r 3,156,289, cache_w 144,937) · cost n/a (unpriced)
  reviewer:Claude: 13,185 tok (in 86, out 13,099, cache_r 2,049,478, cache_w 68,619) · cost n/a (unpriced)

---
id: "0075"
title: Format-generation fence for vlog segment scan — settle the format=false reopen resurrection question
status: done
priority: 3
created: "2026-07-06"
updated: "2026-07-06"
depends_on: []
spec_refs:
    - Current focus areas
---

## Description
Task 0044's box validation found that `vals_store::scan_log_segments` (shared by `valsd dump` AND boot recovery via `Engine::open_inner`) includes **every** segment carrying a valid `VSG1` header in the region, with no fence against segments written before the last `format = true`. On a re-used, non-zeroed SPDK drive the box dump surfaced ~2.03B stale pre-format records on top of the 501.5M live corpus (see `bench/dump-box-notes.md`). The dump-side impact is bounded (inflation, non-persistent across reformat cycles — severity downgraded), but the recovery-side question is **OPEN and potentially a data-correctness bug**: a `format=false` reopen of a reused drive may resurrect stale pre-format records into the served index (and if stale epochs exceed live ones, stale records could even win last-wins replay). The on-box attempt to settle this was blocked (PD zone catalog had been wiped, so the reopened zone never activated).

Scope:
1. **Settle the question in a controlled in-repo harness** (file or mem backend; no hardware needed): simulate a reused device — write corpus A, capture the device bytes, format + write corpus B *without zeroing*, ensure stale corpus-A segments with valid headers remain outside the new log head, then `format=false` reopen and check whether corpus-A keys resurrect into the served/recovered state (and whether epoch ordering can make them win).
2. **If resurrection is real (or even just for dump hygiene): implement a fence** — e.g. a format-generation stamped in the superblock and each segment header, with `scan_log_segments` ignoring segments from older generations. Mind on-device format compatibility (formats.md) and dump/recovery both.
3. Update `docs/deploy.md`'s re-used-drive caveat (currently flags the concern as OPEN and advises against `format=false` reopens of reused drives in prod) to the settled answer.

## Acceptance criteria
- A test pins the `format=false`-reopen behavior on a device with valid stale pre-format segments (resurrection either demonstrated-and-fixed, or demonstrated-impossible with the mechanism explained).
- If a fence is added: dump and recovery both ignore pre-format segments; existing round-trip/recovery tests stay green; format documented in docs/formats.md.
- `docs/deploy.md` + BACKLOG.md OPEN notes resolved to the settled verdict.

## Plan

## Settled analysis (coordinator investigation)

**Resurrection is REAL at the engine level.** `Engine::create` (vals-store/src/engine.rs ~1144) starts fresh counters but never touches the device; stale CRC-valid segments (valid `VSG1` headers) beyond the new log head survive a "format + smaller reload" cycle, and a later `Engine::open` (`open_inner` → `scan_log_segments`) replays them. Replay winners are argmax(seq), and stale corpus-A segments carry *higher* seqs and epochs than a smaller corpus B — so stale records not only resurrect, they can WIN over live ones. Stale manifests (higher generation) are a second vector, same class.

**Production exposure map:**
- Zoned SPDK single/multi-drive (`SpdkZoneStore`/`RegionZoneStore::open`, zonestore.rs ~1255/~1414): protected — `wipe_region` durably zeroes every segment-header block before the superblock names the region.
- File zones (zonestore.rs ~283): protected — `FileBlockDevice::create` truncates.
- **Multi-drive JBOD partition path: UNPROTECTED.** `SpdkDriveSet::open_region(create=true)` (drive.rs ~1018) deliberately does not wipe, and `open_partition` (drive.rs ~425) then calls `Engine::create` — so `format=true` over a reused drive leaves stale segments in every partition and a later `format=false` reopen resurrects them. Same hole for `build_stores_mixed` replaced-drive formats.
- Drives formatted by pre-fence/old builds (the box case): contaminated on-device state is *indistinguishable* from live data (same format version, shared epoch/seq space) — no in-repo change can filter it after the fact. Remedy is one `format=true` boot with a current (wiping) build.

**Design decision: the fence is a durable segment-header wipe at `Engine::create`, NOT a format-generation stamp.** A generation stamped in headers+superblock (a) can't help for anything written before the feature exists (the actual box contamination), (b) requires a new engine-level superblock (engine has none; `reserved_blocks` defaults to 0) and an on-device format bump, and (c) adds nothing over the wipe for any drive formatted by a current build — the wipe already guarantees "valid header ⇒ written since this format". Record this rationale in docs.

## Implementation

1. **vals-store/src/engine.rs**: make `Engine::create(dev, cfg) -> Result<Self, EngineError>` a real *format*: durably zero every segment-start block (one block_size zero write at `alloc.segment_range(s).0` for every segment), then `dev.flush()`, then construct as today. Deep-queue the writes via the async seam (`dev.submit_write` with a bounded in-flight window ~256, reap via `poll_writes`/`WriteHandle::is_done`, check every `into_result`) — a synchronous per-segment loop would reintroduce the O(region) boot spin task 0067 fixed. Add an info-level log for large wipes (mirror `wipe_region`'s). Doc the semantics: without the wipe, pre-create CRC-valid segments/manifests replay at the next open and can beat live data on seq.
2. **Add `Engine::create_prewiped(dev, cfg) -> Self`** (the old infallible constructor) with a documented contract: the device must hold no CRC-valid segment headers at this geometry (fresh `MemBlockDevice`, truncated file, or a region just passed through `wipe_region`/equivalent). Use it at the production sites that already wipe, to avoid a redundant second O(segments) pass: zonestore.rs ~1257 and ~1416 (right after `wipe_region`), and zonestore.rs file/mem zone-create paths (~212, ~284, truncated/fresh devices). Every production `create_prewiped` use must carry a one-line justification comment.
3. **Fix all other `Engine::create` call sites** (~118 across the workspace; `rg -n "Engine::create"`): mostly tests/bench → `.unwrap()` / `?`. `open_partition` in drive.rs MUST use the new wiping `create` (that's the hole being closed) — propagate the error via `LayoutError`. run_pd.rs, vals-raft test, lib.rs doctest: unwrap. CAREFUL with fault-injection wrapper devices in tests (e.g. "fail the k-th write" devices in engine.rs/zonestore.rs, ReadFailDevice in replica.rs): the create-wipe now issues writes that would shift operation counts — construct those engines with `create_prewiped` (device is fresh anyway) to preserve each test's intent rather than blindly re-tuning counts.
4. **Tests** (new `crates/vals-store/tests/format_fence.rs` or engine.rs mod tests):
   - `create_prewiped_over_dirty_device_resurrects_stale_records` — pins the MECHANISM (the pre-fence behavior): corpus A (several segments, include a flush so a manifest exists, plus keys `stale-*` and overlapping keys `k*`), sync, drop; `create_prewiped` over the same (unwiped) device; write smaller corpus B overwriting some `k*`; sync, drop; `Engine::open` → assert `stale-*` keys ARE served (resurrection) and an overlapping `k*` serves corpus A's value (stale wins by seq).
   - `create_fences_stale_pre_format_segments` — same shape but the second engine uses the new wiping `Engine::create` → reopen serves exactly corpus B: `stale-*` absent, overlapping keys = B's values, no runs/manifest from A.
   - Dump-side: `scan_vlog_records` over the fenced device emits only corpus-B-keyspace records (dump hygiene).
5. **Docs**:
   - `docs/formats.md`: add the segment-liveness / format discipline to the segment-header section: a segment is presumed live iff its header block decodes at the open geometry; *every* format path (Engine::create, zone-region alloc `wipe_region`, `Engine::truncate`) durably zeroes every segment-start block first; record-epoch binding fences intra-segment reuse. Note explicitly that a format-generation stamp was considered and rejected (rationale above); FORMAT_VERSION unchanged — no on-device format change.
   - `docs/deploy.md` (~236–250, re-used-drive caveat): replace the OPEN question with the settled verdict: resurrection was real and is demonstrated by an in-repo test; current builds fence it (all format paths durably wipe segment headers, including the multi-drive partition path as of this task); a `format=false` reopen of a region formatted by a current build cannot replay pre-format segments, and dumps of such regions are clean 1:1. Drives last formatted by older builds must get one `format=true` boot under a current build before `format=false` reopens are trusted; until then dumps inflate (bounded; last-wins dedups on replay).
   - `BACKLOG.md` (~905–921): resolve the "Left OPEN" note to the settled verdict + test pointer.
   - `bench/dump-box-notes.md`: short addendum under the graded finding: question settled in-repo (task 0075), verdict + pointer.
6. **Verify**: `cargo test --workspace`, `cargo clippy --workspace --all-targets`, `cargo fmt --check`. All existing recovery/round-trip suites must stay green.

Out of scope (follow-on task): `Engine::truncate` still issues O(segments) synchronous `write_at`s — same spin class 0067 fixed elsewhere; file separately.

### Starting points
- crates/vals-store/src/engine.rs:1144 Engine::create (no wipe today), :1196 open_inner, :4016 truncate (the wipe pattern, sync writes)
- crates/vals-store/src/dump.rs:56 scan_log_segments (header-valid ⇒ included)
- crates/vals-cluster/src/zonestore.rs:674 wipe_region (deep-queued submit_write pattern to copy), :1255/:1414 wipe_region→persist_table→Engine::create call sites
- crates/vals-cluster/src/drive.rs:425 open_partition + :1018 SpdkDriveSet::open_region(_create ignored) — the unprotected JBOD path
- crates/vals-store/src/vlog.rs:1421 replay_segment (record-epoch fence), encode_seg_header/decode_seg_header
- ~118 call sites: rg -n 'Engine::create' — tests/bench mostly; fault-injection device tests must use create_prewiped to keep op counts
- docs/deploy.md:236-250 (caveat to rewrite), docs/formats.md segment-header section ~265-290, BACKLOG.md:905-921 (OPEN note)

## Work log
- 2026-07-06 plan: ## Settled analysis (coordinator investigation)  **Resurrection is REAL at the engine level.** `Engine::create` (vals-store/src/engine.rs ~1144) starts fresh counters but never touches the device; sta
…[truncated]
- 2026-07-06 context hints: 7 recorded with plan
- 2026-07-06 context hints: crates/vals-store/src/engine.rs:1144 Engine::create (no wipe today), :1196 open_inner, :4016 truncate (wipe pattern, sync); crates/vals-store/src/dump.rs:56 scan_log_segments (header-valid ⇒ include
…[truncated]
- 2026-07-06 implementer report: ## Task 0075 complete — format-generation fence for the vlog segment scan (via durable segment-header wipe)  ### Verdict (settled in-repo) Resurrection is **REAL**: a `format=false` reopen of a re-u
…[truncated]
- 2026-07-06 revision: ## Revisions complete  ### 1. vals-bench: raw/reused physical-device engines now use the wiping `Engine::create` Converted every bench engine constructed over a device that could be a **reused physica
…[truncated]
- 2026-07-06 review tier: single-opus — reviewers: Claude
- 2026-07-06 review (Claude): accept — The change fully satisfies task 0075. It settles the format=false-reopen resurrection question with a controlled in-repo harness (crates/vals-store/tests/format_fence.rs): resurrection is demonstrated
…[truncated]
- 2026-07-06 decision: accept — commit: store: settle the format=false reopen resurrection question — fence pre-format segments with a durable header wipe in Engine::create (task 0075)  Resurrection was REAL: recovery/dump presume a segme
…[truncated]
- 2026-07-06 usage: 106,920 tok (in 396, out 106,524, cache_r 16,258,633, cache_w 534,403) · cost n/a (unpriced)
  implementer: 55,214 tok (in 240, out 54,974, cache_r 8,942,847, cache_w 127,508) · cost n/a (unpriced)
  coordinator: 43,403 tok (in 106, out 43,297, cache_r 6,417,468, cache_w 348,536) · cost n/a (unpriced)
  reviewer:Claude: 8,303 tok (in 50, out 8,253, cache_r 898,318, cache_w 58,359) · cost n/a (unpriced)

---
id: "0035"
title: 'valsd dump follow-ons: on-box validation, live SPDK attach, parallel export, valsdump tombstone replay'
status: done
priority: 2
created: "2026-07-04"
updated: "2026-07-04"
depends_on:
    - "0002"
spec_refs:
    - Current focus areas
---

## Description
Task 0002 landed the smallest first cut of `valsd dump`: single-threaded, device-sequential raw vlog export of one zone to a `.vdump` file, round-trip-verified in-repo (write → dump → replay → compare, overwrites/tombstones included). The remaining scope from 0002's design — everything that needs hardware or parallelism — moves here:

- **On-box validation** (the 0002 acceptance criterion that could not be met off-box): dump a multi-GB zone from the SPDK cluster and confirm device-sequential rates (orders of magnitude over the ~76k rec/s scan path), and that a dump against a live node does not disturb serving.
- **Live SPDK node dump**: the current spdk path targets an offline/idle drive (it attaches the controller itself). Add a live-safe path — SPDK secondary-process attach (same shm_id) or an in-daemon dump triggered via admin, using the sibling-qpair trick (`BlockDevice::sibling()`). The file backend is already live-safe (independent read handle).
- **Parallelism**: per-segment parallel dump within a node (each segment is independently decodable; keep vlog output ordering semantics documented — parallel output needs either per-segment files replayed epoch-ordered, or a merge), and per-node fan-out for a whole-cluster capture.
- **Sibling tooling**: teach `vals-examples/valsdump replay` the tombstone sentinel (`val_len == 0xFFFF_FFFF` → coordinator delete); until then use `--skip-tombstones` for put-only corpora. (Sibling repo change — track here, implement there.)

**SPLIT (2026-07-04)**: only the parallelism sub-scope is implementable and verifiable off-box, so this task was rescoped to it (landed as `valsd dump --parallel <N>` + `vals_store::scan_vlog_segments_parallel`). The box-/sibling-repo-bound scope moved to follow-on tasks:
- task 0043 — live SPDK node dump (secondary-process attach or in-daemon admin dump).
- task 0044 (blocked: needs the box + sibling repo) — on-box rate validation, live-node non-disturbance, whole-cluster capture, and `valsdump replay` tombstone support. The original box-bound acceptance criteria (single-digit-minutes export of a 2.12B-record corpus; live-node dump without serving disturbance; valsdump tombstone handling) now live there verbatim.

## Acceptance criteria
- Parallel dump output still round-trips to the exact final state via replay, and is byte-identical to the serial dump (pinned by vals-store unit tests and the vals-server e2e round-trip test).
- (Moved to 0044: on-box rate + live-node criteria; valsdump tombstone replay. Moved to 0043: live SPDK attach.)

## Plan

## Goal (this session — rescoped)
Task 0035 bundles four sub-scopes; only one is implementable AND verifiable in this environment: **per-segment parallel dump within a node**. Implement that (`valsd dump --parallel <N>`), verified in-repo by round-trip + byte-identity tests. Split the box-bound scope (on-box validation, live SPDK secondary/in-daemon attach, sibling-repo valsdump tombstone replay) into follow-on backlog tasks and rewrite 0035's acceptance criteria accordingly (transparently noted in the task file).

## Why this shape
- On-box validation needs hardware we don't have — same class as blocked tasks 0041/0012/0014.
- Live SPDK attach needs new multi-process plumbing in vals-spdk (`SpdkEnv` has no proc_type today) or in-daemon admin-triggered dump — a distinct, sizeable feature whose point (live-node safety on SPDK) is only meaningfully verifiable on the box.
- `valsdump replay` tombstone support lives in the sibling repo `vals-examples`, outside this workspace.
- Parallelism is the piece that makes the on-box rate goal achievable later and is fully testable here.

## Design: parallel per-segment scan with in-order output
Segments are independently decodable; output must preserve vlog `(epoch, offset)` order so last-wins replay semantics (and byte-identity with the serial dump) hold.

### 1. vals-store `dump.rs`: `scan_vlog_segments_parallel`
New pub API alongside `scan_vlog_records`:
```rust
pub fn scan_vlog_segments_parallel<D, T, E>(
    dev: &D, cfg: &Config, workers: usize,
    map: impl Fn(&[(u32 /*off*/, Record)]) -> Result<T, E> + Sync,   // worker thread, per segment
    emit: impl FnMut(T) -> Result<(), E>,                            // caller thread, strict epoch order
) -> Result<VlogScanStats, ScanError<E>>
```
(match `replay_segment`'s actual item type for the offset). Semantics:
- Header scan (`scan_log_segments`) stays serial/batched; sort by epoch exactly as the serial path (stable sort ⇒ identical order).
- `std::thread::scope` with N workers. Each worker calls `dev.sibling()` **on its own thread** (SPDK's one-thread-per-qpair rule; File/Mem siblings are cheap), owns one segment-sized buffer, and pulls segment indices from a shared `AtomicUsize` counter, so in-flight indices form a moving window.
- Bounded memory: a token/permit channel with `window = 2 × workers` permits; a worker acquires a permit before taking an index, sends `(idx, Result<T, …>)` to the coordinator over an mpsc channel; the coordinator keeps a small reorder buffer (e.g. `BTreeMap<usize, T>`), emits strictly in index order, and returns a permit per emitted item. Since indices are taken in increasing order and the oldest in-flight index always holds a permit, this cannot deadlock.
- Error handling: a worker read/decode/map error is sent to the coordinator, which stops emitting, drops the permit channel (unblocking waiting workers, which then exit), joins the scope, and returns the error. Emit errors likewise abort.
- `workers <= 1` should just delegate to the serial `scan_vlog_records`-equivalent logic (or be handled by the caller).
- Unit tests in vals-store: parallel (workers=4) result equals serial result on corpora with overwrites, deletes, compaction, and flushed runs; degenerate cases (0 segments, workers > segments); error propagation from map/emit doesn't hang.

### 2. vals-server `dump.rs`: `--parallel <N>` flag
- New flag `--parallel <N>` (default 1 = existing serial path; validate N ≥ 1, cap at something sane like 64).
- For N > 1: `map` encodes one segment's records into a `Vec<u8>` vdump fragment (same put/tombstone encoding, honoring `--skip-tombstones`) plus per-fragment counters (puts, tombstones, value bytes); `emit` writes the fragment to the BufWriter and accumulates totals + progress line. Header written up front as today. Final stats line unchanged in shape (mention workers).
- Keep the module doc's ordering-semantics story updated: parallel output is byte-identical to serial because emission is strictly epoch-ordered.
- Update `USAGE`, top-level `usage()` in main.rs if it embeds dump usage, and the `valsd dump` note in docs/deploy.md (including a sentence on whole-cluster capture = one dump per node concurrently, since nodes are independent).

### 3. Integration test (vals-server/tests/dump.rs)
Extend the existing round-trip test (or add a sibling test) to:
- run `valsd dump` serial and `--parallel 4` against the same stopped node;
- assert the two `.vdump` files are **byte-identical**;
- (round-trip to exact final state then follows from the existing serial verification — keep the replay-compare on the parallel file if cheap).

### 4. Backlog surgery
- create_task: "valsd dump: live SPDK node dump (secondary-process attach or in-daemon admin dump)" — p3, todo, depends_on 0035; describes both design options (SpdkEnv proc_type=secondary w/ same shm_id vs admin-triggered in-daemon dump over `BlockDevice::sibling()`), acceptance = live SPDK node dumps while serving (box-verified).
- create_task: "valsd dump on-box validation: rates + live-node non-disturbance (+ valsdump tombstone replay)" — p3, depends_on 0035 + the live-attach task; then update_task → blocked (needs the box + sibling repo), carrying the 2.12B-corpus / single-digit-minutes / serving-undisturbed criteria and the sibling-repo `valsdump replay` tombstone item.
- Edit backlog/0035-…md: note the split; acceptance criteria for 0035 reduce to the parallel round-trip criterion (met in-repo).

## Verification
`cargo build`, `cargo test -p vals-store -p vals-server` (dump tests), `cargo fmt --check`, clippy for touched crates.

### Starting points
- crates/vals-store/src/dump.rs — scan_vlog_records + scan_log_segments (serial path to mirror; stable epoch sort at line ~140)
- crates/vals-server/src/dump.rs — emit_dump/write_record/write_tombstone/parse_args/USAGE; fragment-encode should reuse these encoders
- crates/vals-spdk/src/lib.rs:255 + spdk.rs:717 — BlockDevice::sibling(); SPDK sibling MUST be created on the thread that polls it
- crates/vals-store/src/vlog.rs — replay_segment(&buf) -> (hdr, Vec<(off, Record)>)
- crates/vals-server/tests/dump.rs — e2e harness (spawn valsd, corpus with overwrites+deletes, SIGTERM, dump, replay-compare)
- docs/deploy.md — existing `valsd dump` note to extend

## Work log
- 2026-07-04 plan: ## Goal (this session — rescoped) Task 0035 bundles four sub-scopes; only one is implementable AND verifiable in this environment: **per-segment parallel dump within a node**. Implement that (`valsd
…[truncated]
- 2026-07-04 context hints: 6 recorded with plan
- 2026-07-04 context hints: crates/vals-store/src/dump.rs — scan_vlog_records + scan_log_segments (serial path to mirror; stable epoch sort ~line 140); tests at bottom show corpus patterns; crates/vals-server/src/dump.rs — e
…[truncated]
- 2026-07-04 implementer report: Implemented the one in-repo-verifiable sub-scope of task 0035: **per-segment parallel `valsd dump`** (`--parallel <N>`), with strict epoch-ordered output that is byte-identical to the serial dump.  ##
…[truncated]
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — The in-repo-verifiable sub-scope — per-segment parallel `valsd dump` (`--parallel <N>`) — is implemented correctly and completely. The new `scan_vlog_segments_parallel` uses a sound, deadlock-free
…[truncated]
- 2026-07-04 decision: accept — commit: feat(server,store): per-segment parallel `valsd dump --parallel <N>` (task 0035)  - vals-store: new `scan_vlog_segments_parallel` — N workers each read+decode   log segments on their own `BlockDevic
…[truncated]
- 2026-07-04 usage: 66,391 tok (in 176, out 66,215, cache_r 4,954,097, cache_w 221,566) · cost n/a (unpriced)
  implementer: 44,076 tok (in 106, out 43,970, cache_r 3,343,029, cache_w 88,657) · cost n/a (unpriced)
  reviewer:Claude: 11,713 tok (in 38, out 11,675, cache_r 634,296, cache_w 50,751) · cost n/a (unpriced)
  coordinator: 10,602 tok (in 32, out 10,570, cache_r 976,772, cache_w 82,158) · cost n/a (unpriced)

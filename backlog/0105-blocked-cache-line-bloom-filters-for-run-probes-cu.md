---
id: "0105"
title: Blocked (cache-line) bloom filters for run probes — cut k random DRAM misses/run to 1
status: done
priority: 3
created: "2026-07-10"
updated: "2026-07-10"
depends_on: []
spec_refs: []
---

## Description
Same t0097 profile evidence as the early-exit task: every put (fresh keys included — they bloom-reject in all runs) pays R_runs × k≈7 independent random-word bloom probes on the serving core; `locate_block` (bloom inlined) = 32% of cycles at ~15–20 runs/node. Classic fix: **register-blocked / cache-line-blocked bloom** — first hash picks a 64-byte block, remaining k probes hit words within that block → 1 DRAM miss per run per lookup instead of k. Slightly higher FP rate at equal bits/key (compensate with +1–2 bits/key; blooms are sized from entry-count upper bounds already).

Notes:
- Run format: blooms serialize in the run footer — new encoding needs a version bump or a new filter tag with read-compat for v1–v3 blooms (dispatch like the sidecar versioning done in 0100).
- Keep `Bloom::build`'s sizing API; benchmark contains() with perf on the box or a microbench.
- Combines multiplicatively with the early-exit task (fewer probes × cheaper probe).

## Acceptance criteria

## Plan

## Goal
Replace the per-run classic bloom (k≈7 independent random cache-line probes per lookup, 32% of serving-core cycles at 15–20 runs) with a **cache-line-blocked bloom**: first hash picks one 64-byte block, the remaining k probes hit words inside that block → 1 DRAM miss per run per lookup. Combines multiplicatively with 0104's early exit.

## Design
1. **New `BlockedBloom` type** (run.rs, next to `Bloom`):
   - Layout: bit array organized as 512-bit (64-byte = 8×u64) blocks; `num_bits` a multiple of 512.
   - Hashing: reuse the existing deterministic `bloom_hash` (FNV-1a — MUST stay deterministic across processes) as h1. Block selection: multiply-shift reduce `((h1 as u128 * num_blocks as u128) >> 64)`. In-block positions: derive h2 via the same splitmix64 mix as `Bloom::positions`, force odd, then Kirsch–Mitzenmacher within the block: `pos_i = (h_base + i*h2) % 512` (512 is a power of two, odd stride visits distinct residues). Keep the whole 64-byte block in one cache line by storing bits as a flat `Vec<u64>` and indexing `block*8 + (pos/64)`.
   - API mirrors `Bloom`: `with_capacity(n_upper, bits_per_key)` (sizing from an upper bound stays safe — oversizing only lowers FP), `build(keys, n, bpk)`, `insert`, `contains`, `encode`, `decode`. `k = bits_per_key·ln2` clamped like the classic.
   - Encoding: prepend a distinct u32 magic (e.g. `0x31464242` "BBF1") before `num_bits(u64) | k(u32) | words | crc32`, and check it in `decode` — so a version-mismatched decode fails loud ("bad blocked-bloom magic") instead of silently mis-probing. Classic `Bloom::decode` stays as-is for v1–v3 sections.
2. **Run-format version bump** (mirrors the 0100 sidecar precedent): `RUN_VERSION = 4`. `decode_footer` accepts 1..=4. Writers (`build_run_into` and `RunStreamBuilder`) always emit v4 with a blocked bloom. Filter decode dispatches on the footer version: v1–v3 → `Bloom::decode`, v4 → `BlockedBloom::decode`. Introduce `enum RunFilter { Classic(Bloom), Blocked(BlockedBloom) }` with an inlined `contains(&self, key)`, stored in `Run`, `DeviceRun`, and `RunParts` (decode sites: `Run::parse`, `DeviceRun::open`, `read_run_parts`). Update the module docs' version table (v4 = blocked bloom; data blocks/index/sidecar identical to v3) and the `RUN_VERSION`/`format_version` doc comments (1–4). NOTE (as with 0100): pre-0105 binaries cannot read v4 runs — no downgrade; document that in the module docs.
3. **FP compensation**: bump `RunBuildOpts::default().bloom_bits_per_key` 10 → 12 (task's +1–2 bits/key; blocked blooms pay ~1.5–2× FP at equal bits/key). Check for any explicit `bloom_bits_per_key:` constructions in vals-cluster and leave them consistent. `run_len_upper_bound_agg`'s bloom term uses `opts.bloom_bits_per_key` so the reservation bound tracks automatically — but verify the bound still covers the blocked layout: num_bits rounds up to a 512 multiple (≤ +64 bytes) plus the ~24-byte header, both inside the existing `+1024` slack; extend the bound comment to say so.
4. **Sizing edge cases**: `with_capacity` must produce ≥1 block (n=0/small n), matching the classic's `.max(64)` guard.

## Tests
1. Existing back-compat fixtures now must emit genuinely CLASSIC blooms: `v1_run_footer_decodes_as_all_live` patches the footer version onto a current-writer run — that run now carries a blocked bloom, so patching to v1 would mis-decode. Add a small test-only v3-format writer (classic bloom; reuse `build_run_v2`'s pattern or a `build_run_with_filter` helper) and base the v1/v2/v3 fixtures on it. Ensure `v2_and_v3_runs_serve_identical_digests` still constructs a true v3 run.
2. New: blocked bloom unit tests — (a) no false negatives over a few thousand inserted keys; (b) FP rate sanity: at bpk=12 over ≥100k absent probes, FP ≤ ~2.5% (use a deterministic key generator, no new dev-deps); (c) encode/decode round-trip equality + magic/crc corruption rejected.
3. New: v3 classic-bloom run (via the fixture writer) still opens through `Run::parse` AND `DeviceRun::open` and serves correct lookups (present + absent keys) — the read-compat acceptance.
4. New: v4 round-trip — `build_run` → `DeviceRun::open` → lookups/locate_block correct; and a **streamed** (`RunStreamBuilder`) run parses identically (bloom pre-sized from n_upper > actual n stays correct).
5. Optional `#[ignore]`d microbench test: classic vs blocked `contains()` ns/op over a large filter (manual run; the real measurement is on-box in task 0097).
6. Full `cargo test -p vals-store -p vals-cluster` green (flush/merge/recovery paths all round-trip v4 runs).

## Out of scope
Box perf measurement (task 0097 depends on 0104+0105 and is the designated A/B). No change to 0104's probe pipeline.

### Starting points
- crates/vals-store/src/run.rs: Bloom ~287 (bloom_hash FNV ~276, positions/insert/contains, encode/decode ~348), RUN_VERSION=3 ~406, RunBuildOpts default bloom_bits_per_key=10 ~416, build_run_into bloom emit ~516, encode_footer ~608 (always RUN_VERSION), run_len_upper_bound_agg ~653 (bloom bound n*bpk/8+1024), Run::parse ~703, decode_footer ~887 (accepts 1..=RUN_VERSION), RunStreamBuilder ~1291 (with_capacity n_upper, finish ~1348), RunParts ~1761 / read_run_parts ~1780, DeviceRun::open ~1844 (Bloom::decode ~1866), lookup ~2090/locate_block ~1993 (bloom.contains)
- test fixtures: v1_run_footer_decodes_as_all_live ~2627 (patches footer version — will need a classic-bloom fixture), build_run_v2 ~3233/3329 (v2 fixture writer), v2_and_v3_runs_serve_identical_digests ~3400
- cargo test -p vals-store -p vals-cluster

## Work log
- 2026-07-10 plan: ## Goal Replace the per-run classic bloom (k≈7 independent random cache-line probes per lookup, 32% of serving-core cycles at 15–20 runs) with a **cache-line-blocked bloom**: first hash picks one 
…[truncated]
- 2026-07-10 context hints: 3 recorded with plan
- 2026-07-10 context hints: crates/vals-store/src/run.rs: Bloom ~287 (bloom_hash FNV ~276, positions/insert/contains, encode/decode ~348), RUN_VERSION=3 ~406, RunBuildOpts default bloom_bits_per_key=10 ~416, build_run_into bloom
…[truncated]
- 2026-07-10 implementer report: Task 0105 complete — cache-line-blocked bloom filters for run probes. All changes in crates/vals-store/src/run.rs.  **What changed** 1. **`BlockedBloom`** (new pub type next to `Bloom`): 512-bit (64
…[truncated]
- 2026-07-10 review tier: single-opus — reviewers: Claude
- 2026-07-10 review (Claude): accept — Task 0105 is correctly and completely implemented in crates/vals-store/src/run.rs. The new `BlockedBloom` type organizes bits into 512-bit cache-line blocks: first hash (splitmix64 of FNV bloom_hash, 
…[truncated]
- 2026-07-10 decision: accept — commit: 0105: cache-line-blocked bloom filters for run probes (run format v4)  BlockedBloom: one 64-byte block per lookup (splitmix64-remixed FNV hash, multiply-shift block pick, KM probes in-block) — 1 DRA
…[truncated]
- 2026-07-10 usage: 62,980 tok (in 188, out 62,792, cache_r 7,565,352, cache_w 277,494) · $13.7819
  implementer: 53,765 tok (in 154, out 53,611, cache_r 6,697,639, cache_w 125,380) · $10.9470
  coordinator: 5,232 tok (in 14, out 5,218, cache_r 671,555, cache_w 120,694) · $2.4413
  reviewer:Claude: 3,983 tok (in 20, out 3,963, cache_r 196,158, cache_w 31,420) · $0.3936

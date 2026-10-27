---
id: "0365"
title: 'Drop the redundant per-run sidecar (key+version copy) and derive AE/digest streams from run blocks: cut device writes ~36%/ACK'
status: done
priority: 1
created: "2026-10-02"
updated: "2026-10-02"
depends_on: []
spec_refs: []
---

## Description
## Why
Long ingests are bound by device bytes written per ACK.
- **The drive step.** After ~1.04 TB written since a full discard, the 9100 PRO steps down. vals then sustains ~0.62 GB/s per node (FULL1, FULL2-TRIM in `bench/0363-compact-values/`). FULL1 wrote **1005 B/ack** (node1 `vals_spdk_api_write_bytes`), so the post-step rate is ~590k ACK/s.
- **Target.** Sustaining 1M needs ≤ ~620 B/ack.

## Measured/derived breakdown (agent_3 read-only analysis, 2026-10-02; FULL1 node1 counters)

| Component | B/ack |
|---|---:|
| Merge output (index-only; co-compaction off) | 464 [V] (1.95 × run bytes) |
| Vlog appends | ~294 |
| Flush run build | ~238 |
| Sync tail rewrites | 3.4 |

- **Runs account for ~70% of writes**, because merges rewrite them about twice.
- **Stored bytes model**, both matching measurement:
  - backlink ≈ 523–543 B (measured 527–530);
  - record ≈ 540–560 B (measured 539).
- **Garbage ≈ 0:** creates only.

### The redundancy
Every run writes a **sidecar** record `u32 key_len + FULL key + u128 version` per entry (`run.rs:610-615`).
- It is written in block (key) order and built by reading the just-written blocks back (`run.rs:1288-1313`, `run.rs:1301`).
- It contains nothing the blocks don't: they already hold the key (front-coded) and the version in the same order.
- The key is therefore stored three times per entry (vlog, block suffix, sidecar) and the version three times.
- **Sidecar ≈ 125–129 B/KV of the ~238 B/KV run bytes.**

## Change
Stop writing the sidecar in new runs. Derive the (key, version) stream from the run blocks wherever the sidecar is consumed:
- AE seed (`lifecycle.rs:615-640`);
- digest pages (`digest.rs:130`);
- `bucket_digests` (`run.rs:740-779`);
- the cooperative seed loader (0231/0323 seed permits; "one-shot sidecar load/CRC");
- anything else found.

Constraints:
- **Format:** new run format version; old runs with sidecars stay readable.
- **No downgrade** once new runs exist; document it in the upgrade notes.
- **Bit-identical outputs:** AE digests and seeds must be identical to the sidecar-derived ones.
- **Cost:** block streaming reads about the same bytes as the sidecar (48 + suffix vs 20 + key per entry), plus front-coded key decoding CPU; check that seeds stay cooperative and bounded.
- **Also removes** the post-build readback reads.

## Expected [E]
- Stored ≈ −125 B/KV (531.6 → ~405–415).
- Device writes ≈ −355 to −380 B/ack (1005 → ~627).
- The drive step moves from ~1.07B to ~1.55–1.65B ACK.
- Post-step ACK rises proportionally.
- Lazier merges (`VALS_MERGE_TIER_K=8 VALS_MERGE_TIER_MIN_K=8`, affordable once 0364 makes run count cheap) are a separate follow-up worth about −95 B/ack more.

## Acceptance (preregister; full-cache run, FULL1 setup; baseline = a 0364 build if 0364 lands first, same settings)
1. **Format proof:**
   - new runs have no sidecar;
   - a unit test opens a corpus written by the old format and checks reads and AE digests;
   - a property test shows the block-derived (key, version) stream equals the old sidecar stream;
   - a knockout test.
2. **Bytes per KV:** used segments after drain ÷ KV ≤ 430 (FULL1 531.6).
3. **Device writes:**
   - device bytes written ÷ ACK ≤ 700 whole-run (FULL1 1005) and ≤ 680 over 0–1.0B;
   - merge output per ACK ≤ 0.6 × FULL1.
4. **Drive step:** at ≥ 1.45B ACK (FULL1 1.068–1.079B), at ~1.04 TB written.
5. **Throughput:**
   - post-step mean ACK ≥ 800k (FULL1 ~590k);
   - pre-step mean ≥ 0.97 × baseline;
   - shard µs/write ≤ baseline +5%.
6. **RAM:** RSS ≤ baseline +1 GB.
7. **Correctness:**
   - cursor and samplecheck PASS, per-zone counts exact;
   - AE roots equal on both nodes after settle, zero repairs;
   - AE seed count = per-zone counts;
   - 0 integrity errors.

## Sequencing
0364 (in progress) touches `run.rs`/`probe_version`. Implement this after 0364 lands to avoid conflicts, and A/B it against the 0364 build.

## Acceptance criteria

## Work log

### 2026-10-02: implementation plan (agent_3, read-only, on master d4be3026; re-verify line numbers on current master)
**Producers (`run.rs`).**
- `build_run_into` writes the sidecar at 583-613 (flush `flush.rs:111,220`, sync merge `merge.rs:951`).
- `RunStreamBuilder::finish` 1283-1333 builds the sidecar by reading the just-written blocks back via `read_logical` 1136-1161, `SIDECAR_READBACK_WINDOW` 1 MiB (1418) (streaming merge `merge.rs:287`).
- `encode_sidecar_record` 615-620.
- Footer sidecar fields 643-644, written 659-660, decoded 967-968.
- `run_len_upper_bound_agg` (682) counts `n*68 + 2*key_bytes`, callers `space.rs:318`, `flush.rs:368`.
- Test builders `run.rs:3666-3685`; `legacy_fixture.rs:33-60` (v2).

**Consumers.**
- `SidecarStream` (`run.rs:1551-1646`): sync, reserve-aware, used by the AE seed (`DeviceRun::sidecar_stream` 2620 → `merge.rs:861` → `lifecycle.rs:612-623`; `SeedCursor::Stream` `replica/mod.rs:165-192`).
- `SidecarVersions` (`run.rs:798-872`, `sidecar_versions` 2667): seed fallback for v<3 (`merge.rs:871`, `lifecycle.rs:625-643`).
- `SidecarCursor` (`run.rs:1654-1791`): async, checkpointed, used by digest pages (`digest.rs:122-131` `locate_sidecar`, 183-211, `DigestPagePlan` 283-311, `DigestSourceCursor` 314-350).
- `want_bucket_digests`/`_page` (`run.rs:2583-2618` → `parse_want_bucket_digests*` 1793-1895): sync `version_digest_page` `digest.rs:38` from `replica/read.rs:105,134`.
- `for_each_bucket_digest` (`run.rs:2675-2703`): used by `recovered_versions` `winner.rs:661` → `replica/read.rs:48,61`.
- `bucket_digests` (`run.rs:2705+`, `745-779`): tests only.
- 0364 `RunKeySource` (`run.rs:2139-2260`, selector `key_source` 2628-2641, `version >= 3 && sidecar_len >= 4`) for cover-scan; the key-count check vs `entry_count`.

**Metadata.** `DeviceRun.sidecar_off/len` (2270-2271, 2342), `RunParts` (2133-2134), `sidecar_len()` 2368 → `space.rs:38` `sidecar_disk_bytes` (`engine/mod.rs:120`, `vals-bench/capacity.rs:168-174`). Counter help in `observe.rs` 134, 138, 198-199, 216-218, 250, 908.

**Format.**
- `RUN_VERSION = 4` (`run.rs:495`); `decode_footer` accepts 1..=RUN_VERSION (957). The footer is a fixed 76 B.
- v1/v2 have bucket-grouped sidecars; v3/v4 have a flat, key-ordered sidecar. The filter format branches at `version >= 4` (463).
- New `RUN_VERSION = 5`: same footer, `sidecar_off` = end of index, `sidecar_len = 0`. Keep `num_buckets` (checked by `runs_bucket_granularity`, `digest.rs:33,125`).
- Add `DeviceRun::has_sidecar()` = version ≤ 4. Branches: legacy grouped (≤2), flat sidecar (3-4), blocks (≥5).
- Old runs stay readable; merges converge nodes to v5.
- Downgrade: an old binary fails closed ("unsupported run version"), so the zone won't open. **Coordinator decision:** add `VALS_RUN_WRITE_VERSION=4`, read once, to keep writing sidecars during a rollback-capable window; document it in deploy.md.

**Equivalence.**
- The v3/v4 sidecar stream is every entry, **including tombstones**, in block (= strictly increasing key) order, as exactly `(full key, version u128)`. The `num_buckets` header is only the modulus for `bucket_of(k) % n`.
- The block equivalent: iterate the index in order, `decode_block` each block, and emit `(key, version)` for every entry including tombstones; take the modulus from the footer.
- Preserve the "keys strictly increasing" check (now also across blocks), and a Corrupt error on a block CRC failure, so that `ae_seed_corrupt_sidecars` still fires.
- No consumer needs random access by offset.

**Bounds.**
- Sync seed: ≤256 KiB windows (`SIDECAR_CURSOR_WINDOW`), a Checkpoint after each read, a reserve of 2× each read and 2× each key length, one entry per step, with `SEED_MEMORY_OPS_PER_STEP` 4096 above.
- Async digest: windowed with `yield_now` + Checkpoint.
- Block streaming: whole contiguous blocks ≥256 KiB (≥1 block), decode one block at a time, Checkpoint after each read, reserve window + decoded block. Report `resident_bound_bytes`; use 2×window + max block for v5 in `DigestPagePlan::resident_bound_bytes`.

**Readback tripwire.** Today's sidecar build re-reads and CRC-checks every new block before publication. **Coordinator decision:** keep a CRC-only verify readback for v5, **default on**, behind a switch (e.g. `VALS_RUN_BUILD_VERIFY`, default 1), with a counter of verified bytes and failures. A later A/B can measure its device-read cost.

**Change list.**
1. Gate the sidecar write in `build_run_into` / `RunStreamBuilder::finish` on the write version. v5 uses a verify-only readback.
2. `run_len_upper_bound_agg` v5: `n*48 + key_bytes`; callers pass the write version.
3. `BlockVersionStream` (sync, `next_step_reserving`, async `next_matching_step` with want/after/modulus).
4. A `RunVersionSource { Sidecar(..), Blocks{seg_lbas, seg_bytes, blocks, num_buckets} }` (or enums over the existing types).
5. `sidecar_stream` → `Some(Blocks)` for v5; branch `want_bucket_digests*`, `for_each_bucket_digest`, `bucket_digests`.
6. `key_source` uses `has_sidecar()`; v5 → Blocks.
7. `locate_sidecar` → `version_source()`; `digest.rs` plans and cursors carry `RunVersionSource`.
8. `merge.rs:861-878`, `replica/mod.rs:165-192` type renames; `lifecycle.rs:612-660` keeps the corrupt counter.
9. `space.rs:38` v5 → 0.
10. Docs: `out-of-core-index.md` 446-448, 535-555, 901-960; `backpressure.md` 281, 303, 557-571; `scan-performance.md` 57-61; `algorithms.md` 352, 689; `ARCHITECTURE.md` 172, 536; `blind-append-lww.md`; `deploy.md` upgrade/downgrade.

**Tests.**
- **Property test:** v4 vs v5 built from the same random entries (tombstones, shared prefixes, a 1-entry run, keys spanning block windows, an empty run). Every consumer must be identical: seed stream, async digest pages for random want/after/max_bytes, `for_each_bucket_digest`, `bucket_digests` for each bucket, the RunKeySource multiset with count == `entry_count`, and `recovered_versions`.
- **Mixed-format engine:** v2 + v4 + v5 runs. Reads, AE seed, digest pages and cover-scan all work; a merge converts to v5 with identical digests; reopen.
- **Bounds:** port the flush tests 1664-1777 to v5; `seed_apply_ab` on both formats.
- **Corruption:** a block corruption in v5 makes the seed, digest and cover-scan error and the counter increment. Retarget keyfilter test 872-897 to a v4 fixture.
- **Knockouts:** drop tombstones from the block stream; v5 with the sidecar selector; skip the build verify.
- **Version gate:** a reader capped at 4 rejects a v5 footer; `VALS_RUN_WRITE_VERSION=4` writes sidecars.

**Risks.** An AE digest mismatch (tombstone, modulus or order rules); downgrade lockout; loss of the readback tripwire (mitigated by the verify pass); more decode CPU on maintenance paths (optional: a borrowed decoder without per-entry allocation).

### 2026-10-02: implemented (agent_4); review clean (agent_5); committed; FULL5-V5 hardware run in progress
- **Format.** RUN_VERSION 5: no sidecar; the footer is unchanged (sidecar_len 0, num_buckets kept as the modulus).
- **Block streams.** A shared block walker (CRC before use, in-place front-coded decode, tombstones included, strictly increasing across blocks, non-advancing on reserve failure) backs:
  - the AE seed (`RunVersionStream`);
  - async digest pages (`BlockVersionCursor` in `VersionSource`, starting at the resume block);
  - sync digests, `for_each_bucket_digest` / `recovered_versions`, `bucket_digests`;
  - 0364 cover-scan (`key_source` keyed on `has_sidecar()`).
- **Knobs.** `VALS_RUN_WRITE_VERSION` (4 keeps sidecars; a merge with any v5 input writes v5, so the knob must be set before the first v5 run). `VALS_RUN_BUILD_VERIFY` (default on; CRC re-read of streamed merge output; counters `vals_run_build_verify_{bytes,failures}_total`, gauge `vals_run_write_version`).
- **Tests.**
  - Property test v4 vs v5 for every consumer; mixed v2/v4/v5 engine; rollback rule; corruption; bounds; version gate (including the real env).
  - Knockouts: tombstone drop, selector, verify skip.
  - Review mutation tests (after-semantics ×2, start block, reserve advance) were all caught.
  - Suites pass in debug.
- **Release micro (synthetic backlinks, 148 B keys):**

  | | v4 | v5 |
  |---|---:|---:|
  | Run bytes/entry | 290.7 | 122.7 |
  | Build ns/entry | 568 | 400 |
  | Seed ns/entry | ~70 | ~43 |
  | `for_each_bucket_digest` ns/entry | ~130 | 29 |

- **Review minors** filed as 0371.
- **Hardware acceptance:** FULL5-V5 run in progress (pin = master 448ef977 + this exact diff).

### 2026-10-03: hardware acceptance met (criteria 1–7)
FULL5-V5 (`bench/0363-compact-values/FULL5-FINDINGS.md`; pin 8c22af67 = master 448ef977 + 0365, content-identical to 0267a341; valsd 7eee30e0…; lab-1+3; whole 1B cache to EOF; 0 incidents).
- **Throughput:** whole run **1.074M ACK KV/s** (FULL3-KF 962k, FULL1 752k). 0–1.0B 1.198M; 1.0–2.0B 998k; post-step 862k.
- **Drive step** at 1.645B ACK (1.03 TB written), previously ~1.08–1.09B.
- **Bytes:** 399–401 B/KV stored (FULL1 532); device writes 632–636 B/ack (FULL3-KF ~920, FULL1 1005); merge output 220–224 B/ack.
- **Correctness:** cursor, samplecheck 2,090,402 keys, exact counts; AE 600 s window with sweeps advancing (960 passes/node, max age 5 s), 0 repairs, aediag 24/24 trees equal; all again after restart.
- **Restart coverage:** after reopen, uncovered key-filter runs reached 0 at +467/+397 s. But the merge backlog had already drained in the settle (0 merges in flight), so 0364's "cover-scans progress under merges" is still unproven on hardware (0366 item 2).
- **Post-step** gain is smaller than the byte model predicted (+7% vs FULL3-KF): device write rate fell to 0.53–0.60 GB/s with vlog ~180–204 µs/stall, i.e. latency-bound. The v5 verify readback (~0.17–0.20 GB/s) has not been separated out.
- **Closing as done.** Follow-ups: 0371; a verify-off and lazier-merge A/B is the next experiment.

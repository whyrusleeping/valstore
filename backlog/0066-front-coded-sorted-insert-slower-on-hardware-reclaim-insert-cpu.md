---
id: "0066"
title: Front-coded sorted-zone insert ~1.55× slower on hardware (box A/B residual) — reclaim insert CPU
status: done
priority: 4
created: "2026-07-06"
updated: "2026-07-07"
depends_on:
    - "0027"
spec_refs: []
---

## Description
Filed from task 0027's box A/B (oldbox, 2026-07-06). The isolating A/B
(A = f39104e, the true B1 parent = `5077227^`, vs B1 = 5077227) confirmed the named
Stage-B1 risk **on hardware**: the front-coded sorted-zone in-RAM index (`FrontCodedIndex`,
`BTreeMap<Box<[u8]>, Block>` of ~32-entry front-coded blocks) costs **~1.55× sorted-zone
insert throughput** vs the pre-B1 `BTreeMap` index, because every insert stream-re-encodes
its whole block (O(block≈32)).

Measured with `distsorted` (in-process 4-node RF=3 SPDK cluster, sorted zone), identical
bench source both sides (store/index differs only by B1), n=500 k, vsize=64, 4 vfio drives:

| index | sorted insert ops/s (×3 runs) | full sorted scan keys/s |
|-------|-------------------------------|--------------------------|
| A = `BTreeMap` (pre-B1) | 402435 / 410716 / 419603(n=2M) | ~56–62 k |
| B1 = front-coded | 260685 / 265056 / 260763 | ~56–59 k |

Within-side spread ±2%; between-side ~35% drop → clearly outside noise. This is **milder
than the pure-CPU `file`-backend microbench (3–6×)** — SPDK device latency partially
amortizes the re-encode — but it is **not hidden**: the insert CPU is still on the critical
write path. Full-scan (read) is flat, so the read side is fine; the Unsorted/HashIndex + AE
digest-serving paths showed **no** regression (task 0027, kvchurn value-verified 0 errors).
Full numbers + methodology in `bench/b1-box-ab-notes.md`; doc entry in
`docs/out-of-core-index.md` ("B1 — BOX A/B on the SPDK cluster").

This is the deliberate RAM-for-CPU trade (3.44× resident index RAM win banked at B1). The
regression is **acceptable/by-design** for the RAM-bound target; this task tracks the
option to reclaim some of the insert CPU if the sorted write path becomes a bottleneck.

## Acceptance criteria
- A decision recorded: accept the ~1.55× sorted-insert cost as the RAM trade, OR land a
  mitigation that measurably narrows it without giving back the RAM win.
- If mitigating: candidate directions to evaluate — smaller `BLOCK_TARGET` (less re-encode
  per insert, more top-level `BTreeMap` entries = more RAM, quantify the curve); a deferred
  / append-then-batch-re-encode block layout (amortize re-encode across inserts); or a
  cheaper in-block overwrite/insert encoding. Any change re-runs the box distsorted A/B
  (sorted insert + scan) and the `file memprof` RAM microbench to show the new point on the
  RAM↔insert-CPU curve.
- No regression to sorted-zone read/scan or to the Unsorted/HashIndex paths.

## Notes
- Reproduce (box): build vbench at the two commits with the identical bench-only patches
  (store.put 3→2 arg; 0056 raw-scan hello); `distsorted <n> 64 <4 pci> 4 1`; reset the
  drives (setup.sh reset → dd-zero superblocks → setup.sh) between runs. See
  `bench/b1-box-ab-notes.md` for the exact recipe and the separate valsd-cluster
  box-recipe fixes discovered in 0027.

## Plan

Goal: land a mitigation that measurably narrows the ~1.55× sorted-insert regression without giving back the B1 RAM win, then re-run the required benchmarks (local `file memprof`, box `distsorted` A/B) and record the new point on the RAM↔insert-CPU curve.

## Mitigation: in-place block splice for the common insert case

Key observation: front-coding is local — entry i's packed encoding depends only on key i-1. Inserting key K at position `ins` in a block only changes (a) K's own encoding (against key ins-1) and (b) the old entry at `ins`'s encoding (now coded against K instead of key ins-1). All later entries' bytes are unchanged. So for the common case we can splice bytes instead of stream-re-encoding the whole block:

FAST PATH — when `ins > 0` (block first key unchanged → no BTreeMap re-key) and `n + 1 <= BLOCK_MAX` (no split):
1. During the in-block search, track: the byte offset where entry `ins`'s encoding starts, and the LCP of the previous decoded key with K (computable incrementally from the compare loop — no need to retain the predecessor key itself; when the scan stops, `buf` holds the successor S = old key at `ins`).
2. Build the replacement bytes in a small reusable scratch: K encoded as (shared(P,K), suffix) + (if ins < n) S re-encoded as (shared(K,S), suffix) — both K and S are in hand.
3. Splice `packed` (replace S's old encoding bytes, or append when ins == n) and `entries.insert(ins, packed)`. Use `reserve_exact` before growing so packed/entries capacity stays ~= len (no hidden RAM giveback from Vec doubling slack; splits still rebuild exact).
4. Use `locate_block_mut` (valid because ins > 0 ⇒ K >= block first key) — no Box clone, no BTreeMap remove+insert.

SLOW PATH (unchanged, existing `block_insert`): ins == 0 (re-key needed, ~1/33 of random inserts) and splits (~1/32). Overwrite path unchanged. Remove path unchanged (or left as-is — not on the measured hot path).

Expected win: removes the full-block decode + re-encode + block alloc + BTreeMap remove/insert/Box-clone from ~30/32 of inserts, leaving the O(avg block/2) search decode. Pure-CPU microbench regression (3–6×) should narrow substantially; hardware 1.55× should narrow measurably.

## Correctness
- Existing randomized property test `front_coded_matches_btreemap_reference` is the main guard; extend/verify it exercises long random insert sequences (splice fast path dominates there) and keep the invariant checks (map key == block's true first key).
- Add directed tests: splice append-at-end; splice mid-block with successor re-encode where shared(K,S) > and == shared(P,S); insert at position 1; insert just below split threshold vs at it (slow path); interleaved overwrite/remove after splices; scan/scan_page over spliced blocks.
- `cargo test -p vals-store` (all), plus workspace build.

## Verification / benchmarks (acceptance criteria)
1. Local pure-CPU A/B: `cargo run -p vals-bench --release -- file memprof <n> 100` (bsky-shaped keys, sorted zone) at HEAD vs patched — report load keys/s (insert CPU) and bytes/key RSS delta (RAM must be ~flat). Also `bench/prefix-index/measure.rs` is deterministic-bytes and unaffected (encoding format unchanged) — note that.
2. Box A/B (oldbox, reachable via ssh): build vbench at A = current HEAD and B = patched, `--features spdk` + VALS_SPDK_LINK=1; run the distsorted recipe from bench/ds_ab_box.sh (n=500000, vsize=64, 4 vfio drives, drive reset between runs, 3 runs/side): expect WRITE ops/s gap vs the pre-B1 402k baseline to narrow from ~262k, SCAN keys/s flat. I (coordinator) run this after review acceptance, before commit.
3. Record results in bench/b1-box-ab-notes.md (new section) and docs/out-of-core-index.md (B1 section update), and record the decision in this task.

## Fallback
If the box A/B shows no measurable narrowing (unlikely given the CPU profile), keep the change only if local CPU win is real and RAM is flat, and record the decision accordingly; if anything regresses, revert and record "accept the 1.55× trade" instead.

## Work log
- 2026-07-07 plan: Goal: land a mitigation that measurably narrows the ~1.55× sorted-insert regression without giving back the B1 RAM win, then re-run the required benchmarks (local `file memprof`, box `distsorted` A/B
…[truncated]
- 2026-07-07 context hints: crates/vals-store/src/index.rs — FrontCodedIndex (~line 384-830): put/block_insert/search_into/decode_key_into/BlockBuilder; search_into early-exits on Greater with buf holding the successor key; tr
…[truncated]
- 2026-07-07 implementer report: ## Summary  Landed the in-place block-splice fast path for `FrontCodedIndex` inserts in `crates/vals-store/src/index.rs`, narrowing the B1 sorted-insert CPU cost without giving back the RAM win. All c
…[truncated]
- 2026-07-07 review tier: single-opus — reviewers: Claude
- 2026-07-07 review (Claude): accept — The change lands the planned in-place block-splice fast path for FrontCodedIndex inserts in crates/vals-store/src/index.rs. The core logic is correct: front-coding locality means only the inserted key
…[truncated]
- 2026-07-07 revision: ## Both review nits addressed  **1. Hot-path allocation removed.** Added a second `splice_buf: RefCell<Vec<u8>>` field on `FrontCodedIndex` (separate from `scratch`) with a doc comment explaining the 
…[truncated]
- 2026-07-07 review (Claude): accept — The revision cleanly addresses both prior nits. The fast-path replacement bytes now reuse a persistent `splice_buf: RefCell<Vec<u8>>` (taken out via mem::take before the &mut self splice, then restore
…[truncated]
- 2026-07-07 revision: Recorded the accepted box distsorted A/B results in the two docs; no code changes (diff touches only the two markdown files).  1. **`bench/b1-box-ab-notes.md`** — appended a new section "## Splice f
…[truncated]
- 2026-07-07 decision: accept — commit: store: in-place block-splice insert fast path for the front-coded sorted index — front-coding is local, so an interior no-split insert re-encodes only the new key + its displaced successor (spliced 
…[truncated]
- 2026-07-07 usage: 98,388 tok (in 300, out 98,088, cache_r 9,333,080, cache_w 487,050) · cost n/a (unpriced)
  implementer: 53,761 tok (in 152, out 53,609, cache_r 4,809,181, cache_w 195,885) · cost n/a (unpriced)
  coordinator: 34,275 tok (in 100, out 34,175, cache_r 3,879,475, cache_w 244,370) · cost n/a (unpriced)
  reviewer:Claude: 10,352 tok (in 48, out 10,304, cache_r 644,424, cache_w 46,795) · cost n/a (unpriced)

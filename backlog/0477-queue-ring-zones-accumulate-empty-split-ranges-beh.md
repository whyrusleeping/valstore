---
id: "0477"
title: 'Queue/ring zones accumulate empty split ranges behind the deleted head: range count, range-delete fan-out and RSS grow without bound'
status: done
priority: 2
created: "2026-10-15"
updated: "2026-10-19"
depends_on:
    - "0478"
spec_refs: []
---

## Description

## Description
## Found by
0471 arm V-Q2-RING-500000. Ring buffer on lab: 64 partitions × 1M entries live, 500k msgs/s, and a trimmer issuing cumulative `[p{part}/, head − 1M)` range deletes every 100 ms per partition (640 deletes/s). Run length 2 h.

## Observed
- **Range count.** Zone `ring` grew from 64 presplit ranges to 4,246 after about 1 h and 8,550 after 2 h. A partition's live window (1M) sits at the default `range_split_threshold` (1,000,000), so size splits fire at the moving head. The ranges behind the head are then emptied by the deletes and never merge back: `range_merge_threshold` is 0 (off) by default, and even when on, auto-merge does at most one merge per zone per poll while splits do up to 16.
- **Fan-out.** Each cumulative delete is sliced across every empty range of its partition behind the head, so the work per trim grows over time. `vals_range_deletes_applied_total` per node went from 7.4k/s at 1 h to 16k/s at 2 h, for a constant 640 client deletes/s. Trim p50 went 1.6 → 2.9 ms and p99 3.7 → 15 ms.
- **RSS.** Every node's RSS grew linearly from 1.6 to 18.4 GB over 2 h, about 140 MB/min. Allocator in-use was 7.9 GB against RAM-charged 1.8 GB at 1 h. This is most likely the per-slice RT records: every slice of every trim is a new record, all dominated (see proposed 0475).
- **Otherwise healthy.** Throughput held at 500k, used segments leveled off at ~200k per node after ~1 h, run entries leveled off at ~300M per node (vs 64M live), and the final verify was ok. The head-probe violations are tracked separately in 0476.

## Options (to decide)
- (a) Merge empty / fully-covered ranges eagerly and cheaply. Without data movement, this needs only F handoff.
- (b) Don't size-split a range whose low side is covered by a range tombstone, or split at the live midpoint and immediately merge the empty remainder.
- (c) A per-zone queue profile: no size splits, with partitions supplied by the client (0440 #5 hash/partition bucketing).
- (d) Dominated-record collection (0475) to bound RAM regardless.

## Acceptance (owner revised, 2026-10-18)
Land covered merges separately from remaining default-config re-home growth (accepted 0485). Require bounded ranges and constant trim fan-out (owner accepted R14's ~2.3 applied slices/trim/replica after correcting the historical double-RF normalization); flat RSS with re-homes disabled is demonstrated by R12, not a claim about default settings.

## Acceptance criteria
- Test the combined current tree, including newer wire changes: vals-client, vals-cluster, vals-server and wire_fuzz; rerun known flakes alone.
- Before landing, complete a 2 h default-config ring arm at 500k msgs/s on that build, with bounded ranges, constant range-delete fan-out (~2.3 applied slices/trim/replica accepted), zero probe violations and exact final verification. Interrupted arms without verification do not satisfy this gate.
- Record live-key/RSS/disk slopes and re-home decision inputs as the baseline for 0485; remaining default-config RSS growth is explicitly deferred, not a passing flat-memory result.
- Correct findings/build provenance and review the final scoped diff before committing.

## Agreed approach (owner, 2026-10-16)
PD eagerly merges adjacent ranges that both have zero live (cover-filtered) entries, several per poll, independent of `range_merge_threshold`. This depends on 0478, because merges move data through the cleanup handoff. While implementing:
- confirm that the PD's per-range entry counts exclude covered entries;
- re-check whether re-homing mostly-covered head ranges is wasted work.
Keep the per-zone "queue, no size splits" option in reserve.

## Acceptance criteria

- Test the combined current tree, including newer wire changes: vals-client, vals-cluster, vals-server and wire_fuzz; rerun known flakes alone.
- Before landing, complete a 2 h default-config ring arm at 500k msgs/s on that build, with bounded ranges, constant range-delete fan-out (~2.3 applied slices/trim/replica accepted), zero probe violations and exact final verification. Interrupted arms without verification do not satisfy this gate.
- Record live-key/RSS/disk slopes and re-home decision inputs as the baseline for 0485; remaining default-config RSS growth is explicitly deferred, not a passing flat-memory result.
- Correct findings/build provenance and review the final scoped diff before committing.

## Outcome

Default-on all-owner covered-empty merges are batched per zone; covered-empty ranges are excluded from splits and debt re-homes, without a merge-size ceiling. Full current-tree client/cluster/server suites and wire_fuzz pass; kill-the-fix regression fails without split exclusion. Final default-config R14 ran 2 h at 500k msgs/s: ranges plateaued at 169–239, exact 64,083,712 live entries, zero violations/errors in 28,324 probes. Corrected historical double-RF normalization; owner accepted constant 2.31 applied slices/trim/replica. RSS still grows 1.7–2.7 MB/min; live-key/RSS/disk slopes and re-home inputs retained for accepted 0485, not claimed flat. Two independent reviewers accepted the full scoped diff. Non-blocking review note: client transport response_owned_bytes omits the new covered_empty vector. Evidence/provenance: bench/0477-covered-merges/FINDINGS.md and R14-receipt.json. Unrelated user work preserved.

Commit: fix(pd): reclaim covered queue ranges without re-splitting empty prefixes

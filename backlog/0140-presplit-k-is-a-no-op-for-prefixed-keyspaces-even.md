---
id: "0140"
title: 'presplit=K is a no-op for prefixed keyspaces: even_boundaries() splits on single bytes, so all keys land in one range'
status: done
priority: 2
created: "2026-08-06"
updated: "2026-08-07"
depends_on: []
spec_refs:
    - docs/zones.md#Growth = range splits
---

## Description
## What happened

On the lab bench (task 0138) a sorted zone was created with `create-zone smix 2 sorted presplit=64` and loaded with 20M keys of the form `valsmix/<seed>/<idx:012>`. PD then reported:

```
vals_pd_sorted_ranges       64
vals_pd_max_range_entries   22100088     # the whole zone
vals_pd_range_skew_pct      6397
```

and node 3 served **0** sorted rows (`vals_scan_entries_returned 0`, `live_keys` = its unsorted share only). All 20M keys sat in **one** range, on one replica pair, on one intra-node shard.

## Cause

`even_boundaries(k)` (crates/vals-client/src/partition.rs:172) emits **single-byte** boundaries spread over `0x00..0xFF`:

```rust
let b = (i * 256 / k) as u8;
let boundary = vec![b];
```

Every key in a realistic keyspace shares a first byte — `valsmix/…` (0x76), `app.bsky.feed.post/…` (0x61), `did:plc:…` (0x64). So all keys fall into whichever single-byte bucket contains that first byte, and the other K-1 ranges stay empty forever. `presplit=K` is effectively a **no-op for any prefixed keyspace**, which is the normal case (it only helps when keys are uniformly distributed over the first byte, e.g. hashes).

This also silently defeats the guardrails: `pd_single_range_sorted_zones` counts zones whose range map has one range, and this zone has 64 — so the "single-range footgun" alert never fires even though the zone behaves exactly like a single range. The operator sees `presplit=64` in the create command and reasonably believes the zone is spread.

Knock-on effects measured on the bench:
- Sorted-zone throughput collapses to one shard's capacity: 22.8k reads/s vs 542k/s on the unsorted zone, with transport (quorum/deadline) errors under mixed load.
- With auto-split default-on, that one oversized range is split over and over under write load — which is the trigger for the data-stranding bug in task 0139.

## Directions

- Make `presplit=K` distribution-aware, or at least prefix-aware: sample existing keys (or accept an explicit boundary list / key-pattern) instead of assuming a uniform first byte. `valsd admin split-zone <zone> <key>` already covers explicit boundaries — presplit should be able to take them too (e.g. `presplit=key1,key2,...` or `presplit=K@<prefix>`).
- Fix the guardrail so it measures **fill concentration**, not range count: alert when one range holds most of a zone's entries (`pd_max_range_entries / total`), which catches both the true single-range case and this degenerate one.
- Document the limitation in `docs/zones.md` (the presplit paragraph currently promises "spreads immediately").

## Acceptance criteria
- `presplit` on a common-prefix keyspace produces ranges that actually divide the loaded keys (verified by `pd_max_range_entries ≈ total/K` on a box load).
- A concentration-based guardrail fires for this shape.
- `docs/zones.md` states what `presplit=K` does and does not do.

## Artifacts
`bench/lab-bench-20260806/` — `B2-valsmix-nosplit.log`, `metrics.after-armB2.*`.

## Plan

Scope per the 2026-08-07 work-log decision: (a) stop the operator-facing lie — let `presplit` take explicit real-key boundaries (the box-validated mechanism from task 0148) instead of only single-byte `even_boundaries`; (b) add a concentration-based guardrail that fires when one range holds most of a zone's entries (the shape `pd_single_range_sorted_zones` misses); (c) document the limitation in docs.

1. Admin: explicit presplit boundaries (crates/vals-server/src/admin.rs, `["create-zone", name, rf, ordering, presplit]` arm)
   - Keep `presplit=K` (u32, 2..=256) → `even_boundaries(k)` as today.
   - If the value after `presplit=` does NOT parse as u32, treat it as a comma-separated list of explicit boundary keys: `presplit=key1,key2,...`. Validate: every key non-empty, list strictly increasing (byte order), 1..=255 boundaries (≤256 ranges). Keys become bytes exactly as typed (same as `split-zone <zone> <key>`). Pass them to the existing `pd.create_zone_presplit(cfg, boundaries)` — no PD-side change needed; the creation path already installs each boundary as a canonical split (lower-bound token) while the zone is empty, which task 0148 box-validated to spread correctly (0 unrehomed pre-write).
   - Clear error messages for: non-sorted zone, empty key, out-of-order/duplicate keys, too many. Note in the error/help text that a purely numeric single boundary key can't use this shorthand (it parses as K) — use `split-zone ... canonical` for that edge case.
   - Update the unknown-command help string only if it mentions presplit syntax.

2. PD: concentration guardrail (crates/vals-cluster/src/pd.rs, `publish_range_metrics` ~line 685)
   - New knob `VALS_PD_RANGE_CONCENTRATION_WARN_PCT` (default 80, `0` disables), parsed alongside `VALS_PD_SINGLE_RANGE_WARN_ENTRIES` (~line 837), stored in `PushState`, plus a test setter like the existing `set_single_range_warn` (~line 1396).
   - In the multi-range branch (`num > 1`, `sum_entries > 0`): if `zone_max > single_range_warn_entries` (reuse the existing entries floor so small zones stay quiet) AND `zone_max * 100 / sum_entries >= concentration_pct` → count the zone in a new gauge `pd_concentrated_range_zones`, and emit a throttled `log::warn!` (reuse the `last_warn` map + `SINGLE_RANGE_WARN_EVERY` cadence) naming the zone, its range count, zone_max/sum, and the remedy (real-key `split-zone ... canonical` on an empty range / re-home via 0142 / explicit `presplit=key,...`). Bump a new counter `pd_range_concentration_warnings`.
   - Fix the `last_warn.retain` clause (currently keeps only single-range sorted zones) so multi-range sorted zones' throttle timestamps survive too.
   - Register the new gauge + counter in observe.rs with Prometheus HELP text mirroring the existing pd_* metrics.

3. Docs
   - docs/zones.md presplit bullet: state plainly that numeric `presplit=K` only spreads keyspaces uniform in their first byte and is a no-op for prefixed keyspaces; document `presplit=key1,key2,...` as the recommended form for real keyspaces; document the new guardrail (env knob, gauge, counter) in the guardrails bullet.
   - docs/deploy.md admin-command table: update the create-zone row to `[presplit=K | presplit=key,key,...]`.
   - docs/deploy.md (or wherever PD env knobs are tabulated — check for VALS_PD_SINGLE_RANGE_WARN_ENTRIES) add the new env var.

4. Tests
   - admin.rs: parse tests — explicit key list yields the right boundaries/range count (mirror `create_zone_presplit_commits_k_ranges`); rejection tests for out-of-order, duplicate, empty keys, and unsorted zone.
   - pd.rs or node.rs: guardrail test mirroring the existing single-range-warn integration test (node.rs ~5430–5515): build a presplit zone whose entries all land in one of several ranges, set warn thresholds low via setters, assert `pd_concentrated_range_zones >= 1` and the warnings counter increments, and that the map is not resharded by the warn. A cheaper pure-unit test of the counting logic is fine if the integration shape is too heavy — but prefer mirroring the existing test since the harness exists.
   - `cargo build` + targeted `cargo test -p vals-cluster -p vals-server` (respect existing test naming; do NOT run cargo fmt).

Acceptance-criteria mapping: criterion 1 (real division of loaded keys) is satisfied by explicit-key presplit — mechanism box-validated under task 0148 (`bench/lab-canonical-split-20260807/`, 0 unrehomed pre-write, 288k keys/s arm); unit/integration tests here pin equivalence. Criterion 2 = new concentration guardrail with test. Criterion 3 = docs updates.

### Starting points
- crates/vals-server/src/admin.rs:125-147 — create-zone presplit arm; tests at :226-289
- crates/vals-client/src/partition.rs:286 — even_boundaries (unchanged)
- crates/vals-cluster/src/pd.rs:685-776 — publish_range_metrics (guardrail lives here); env parse ~:837; set_single_range_warn ~:1396
- crates/vals-cluster/src/node.rs:5430-5515 — existing single-range guardrail integration test to mirror
- docs/zones.md:120-165 — presplit + guardrails bullets; docs/deploy.md:198 — admin command table
- User pref: no explanatory comments in code; only SAFETY notes and Prometheus HELP docs. Do not run cargo fmt (version skew).

## Work log

- 2026-08-07 (PM, owner decision): NOT the next step — the owner chose to go straight to task 0142 (range re-homing). Rationale: auto-split picks boundaries from **reported midpoints of real data**, so once re-homing can move a range, a grown zone spreads correctly regardless of key prefix, and `presplit=K` stops being the only spreading mechanism. This task therefore shrinks in importance to (a) not lying to the operator (`presplit=K` on a prefixed keyspace silently does nothing) and (b) the concentration-based guardrail, which is still worth having because it detects the degenerate shape that `pd_single_range_sorted_zones` misses. Keep at p2, sequence after 0142.
- 2026-08-07 plan: Scope per the 2026-08-07 work-log decision: (a) stop the operator-facing lie — let `presplit` take explicit real-key boundaries (the box-validated mechanism from task 0148) instead of only single-by
…[truncated]
- 2026-08-07 context hints: 6 recorded with plan
- 2026-08-07 context hints: crates/vals-server/src/admin.rs:125-147 — create-zone presplit arm; tests at :226-289; crates/vals-client/src/partition.rs:286 — even_boundaries (leave unchanged); crates/vals-cluster/src/pd.rs:68
…[truncated]
- 2026-08-07 implementer report: Implemented task 0140.  Changes: - Added explicit real-key presplit syntax to `valsd admin create-zone`: `presplit=key1,key2,...`.   - Preserves numeric `presplit=K` behavior for K=2..=256.   - Valida
…[truncated]
- 2026-08-07 review tier: single-opus — reviewers: sol
- 2026-08-07 review (sol): accept — The change satisfies task 0140’s core acceptance criteria: explicit real-key presplit boundaries are parsed, validated, and installed canonically; the PD now exposes and tests a concentration-based 
…[truncated]
- 2026-08-07 decision: accept — commit: 0140: real-key presplit boundaries (presplit=key,...) + PD range-concentration guardrail; document numeric presplit=K's first-byte limitation
- 2026-08-07 usage: 4,489,465 tok (in 917,728, out 36,377, cache_r 4,323,429, cache_w 110,279) · cost n/a (unpriced)
  reviewer:sol: 2,885,162 tok (in 573,628, out 18,542, cache_r 2,292,992, cache_w 0) · cost n/a (unpriced)
  implementer: 1,597,461 tok (in 344,066, out 11,027, cache_r 1,242,368, cache_w 0) · cost n/a (unpriced)
  coordinator: 6,842 tok (in 34, out 6,808, cache_r 788,069, cache_w 110,279) · cost n/a (unpriced)

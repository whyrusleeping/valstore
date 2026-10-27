---
id: "0478"
title: Re-home cleanup never forgets former-owner copies while any zone keeps splitting (progress reset by the shared range-map epoch)
status: done
priority: 2
created: "2026-10-16"
updated: "2026-10-17"
depends_on: []
spec_refs: []
---

## Description

## Description
## Found by
0476 investigation on the 0471 lab cluster, zone `ring`, which went through 52 re-homes while auto-split was churning.

## Observed
- On every node `vals_cleanup_forget_keys` = 0 and forget compactions = 0, while forget discards are about 535–633.
- The former owner shards still hold their whole pre-cutover copies of the re-homed ranges. For example, partition 48 range #6473 data still sits on shard 5 of all three nodes after moving to shard 2.

## Suspected cause
The cleanup progress check includes the range-map epoch, and that epoch is shared by all zones. Auto-splits in any zone advance it every ~5 s, so the cleanup pass keeps discarding its progress and restarting, and never reaches the forget.

## Impact
- Space leak: leftover copies are never reclaimed.
- Before 0476's read fence, stale-map readers could read these frozen copies indefinitely.
- Any heavy split activity, e.g. queue/ring zones (0477) or bulk loads, stalls cleanup cluster-wide.

## Acceptance
Cleanup progress no longer resets on unrelated range-map epoch bumps; it is scoped to the zone, or to the affected ranges. A test with continuous splits in another zone shows the forget completing. Confirm on hardware that `vals_cleanup_forget_keys` advances during a ring arm.

## Acceptance criteria

## Outcome

Remaining 0-forget cause was handoff pace (≤256 apply RPCs = 85 keys per page per 5 s sweep; no lap wrap, no 32 MiB batch). cleanup_with now probes owners with batched MixedBatchRead digests (max(entry, RT cover)) in windows (1→1024 keys, grow while fast, cut at the 250 ms page budget or stop, stop probing after an all-unconfirmed window); keys every owner holds at ≥ listed version are released without value read/apply, the rest use the unchanged push path. New counter vals_cleanup_probe_released_keys; unit tests incl. kill-the-fix, slow-scalar, grown-window cutoff and stop. Hardware (lab 0471 ring, 500k msgs/s, with a verification-only PD patch for 0479): R4 1 h 1.5–2.4M forget keys/node, R6 (committed code) 0.36/1.18/2.54M, 0 discards, verify exact; strays drain after load. Reclaim still lags re-home inflow (proposed 0480); dropped-zone range-map bug filed as todo 0479.

Commit: fix(0478): cleanup confirms not-owned keys with a windowed batched owner digest probe before the per-key push, so former owners forget re-homed copies during ring load (lab R4-R6); follow-ups 0479, 0480

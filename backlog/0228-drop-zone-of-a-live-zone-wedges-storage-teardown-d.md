---
id: "0228"
title: drop-zone of a live zone wedges storage teardown (deferred drop leak) and zone-id reuse breaks the recreated zone
status: done
priority: 2
created: "2026-08-20"
updated: "2026-08-20"
depends_on: []
spec_refs: []
---

## Description

Found during 0227 hardware acceptance on lab (HEAD 1b75e75c). Sequence: nodes booted re-opening persisted sorted zone 1 (smix); `admin drop-zone smix`; `create-zone smix 2 sorted presplit=...` reused zone id 1. On 2–3 of 4 shards per node the pending drop never completed (`vals_zone_drops_deferred` stuck at 2/2/3 for 20+ min, `complete_pending_drops` requires `Rc::strong_count(&p.store) == 1` — some long-lived component, likely maintenance owner loop / AE / staged cache, retains the store Rc). Those shards then answer every op on reused zone id 1 with `zone 1 teardown pending; retry` (surfaced only as vals_server_errors + client quorum failures: ~16M read errors, half of all replica writes silently lost during a 20M load — physical writes were 1× logical instead of 2×). No log line, no dedicated metric beyond the deferred gauge.

Also note: after a node restart with deferred drops, the un-freed old region would be re-opened as the (recreated) zone's data — mixing old and new corpora silently.

## Acceptance criteria

- Identify and release the retained store Rc(s) so a dropped zone's storage teardown completes promptly (or make teardown forcible/cancel-safe for live zones).
- Zone id reuse cannot attach a recreated zone to a wedged/stale region: either defer id reuse until teardown completed on all shards, or generation-tag regions so a recreated zone never re-opens a prior incarnation's region (boot path included).
- The wedged state is observable: deferred-drop age surfaced (log or metric), and ops rejected due to teardown-pending are distinguishable from generic server errors.
- Regression test covering drop→recreate of a materialized zone under active maintenance/AE.

## Outcome

Fixed the 0227 drop-zone wedge: (1) compact_loop, snapshot_loop's paced flush-plan loop, maint_loop's flush-commit loop, and sweep cleanup now re-check zone_is_current at every bounded step, releasing a dropped zone's store Rc within one step so pending drops free promptly. (2) PD zone ids are now monotonic: a persisted next-zone-id high-water key ('N') proposed before each zone config, and driver open() drives raft before campaign so replay precedes next_zone_id() — drop→create never reuses an id, so a recreated zone can't attach to a wedged/stale region (boot path included; stale regions drop at next catalog adoption). (3) Observability: vals_zone_drop_pending_age_seconds gauge, rate-limited WARN with zone/age/refcount, and vals_zone_teardown_rejections counter. Regression tests cover drop→recreate under active compaction, highest-id non-reuse, durable PD restart, and rejection accounting. Full vals-cluster (563) + vals-client suites pass in debug; reviewer accepted.

Commit: 0228: prompt zone teardown, monotonic PD zone ids, teardown observability

---
id: "0545"
title: A failed async value-log write reaped by an append's stall path is not reported to the sync that covers it (synchronous drain paths)
status: proposed
priority: 3
created: "2026-10-30"
updated: "2026-10-30"
depends_on: []
spec_refs: []
---

## Description
Found in 0544. `ValueLog::reap` hands a failed async write's error to whichever caller reaps it. That can be `submit_block`'s inflight-cap stall during an unrelated append, which fails only that put. A later `drain_inflight`/`drain_and_flush_tail`/`sync` finds no failed handle and reports success, so records that earlier puts appended into the failed block can be acknowledged by a subsequent commit (or published by a flush) without being on the device.

0544's yielding group-commit path (`sync_submit`/`sync_reached`) closes this for group commits: `reap` records the earliest unreported failed submit sequence, and the covering sync fails. The synchronous paths (engine flush plan `drain_and_flush_tail`, `ValueLog::sync`, domain-flush stall fallback) still have the gap.

Acceptance: every synchronous drain fails when a write it covers failed, whoever reaped it; a regression test with an injected async write failure reaped by an append. Decide whether a write failure should poison the engine instead.

## Acceptance criteria

## Work log

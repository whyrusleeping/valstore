---
id: "0545"
title: A failed async value-log write reaped by an append's stall path is not reported to the sync that covers it (synchronous drain paths)
status: done
priority: 3
created: "2026-10-30"
updated: "2026-11-02"
depends_on: []
spec_refs: []
---

## Description

Found in 0544. `ValueLog::reap` hands a failed async write's error to whichever caller reaps it. That can be `submit_block`'s inflight-cap stall during an unrelated append, which fails only that put. A later `drain_inflight`/`drain_and_flush_tail`/`sync` finds no failed handle and reports success, so records that earlier puts appended into the failed block can be acknowledged by a subsequent commit (or published by a flush) without being on the device.

Correction from review 0548 at HEAD 525037b: the yielding group-commit path (`sync_submit`/`sync_reached`) described in the original note is not present on master (it was in a rejected 0544 experiment). Current group commit calls `sync_store` → `sync_prepare` → `drain_and_flush_tail` (`net/server/apply.rs:460`, `engine/flush.rs:231-233`), so it also has the gap. Reads call `drain_inflight` too and can consume the only report of a failed write. The engine flush plan, `ValueLog::sync`, and domain-flush stall fallback are affected as originally noted. Consider a sticky write-path failure/fail-closed policy rather than only passing the error to whichever drain happens next.

Acceptance: every synchronous drain fails when a write it covers failed, whoever reaped it; a regression test with an injected async write failure reaped by an append. Decide whether a write failure should poison the engine instead.

## Outcome

ValueLog records a reaped async write failure in a sticky write_lost flag; wait_inflight (sync, drain_and_flush_tail, flush plan, group-commit sync_prepare, domain-flush fallback) and append fail with LogError::WriteLost until the engine is reopened; reads drain without failing (a lost record fails its key/CRC check). Decision: sticky value-log failure rather than engine poison (engine poison has a distinct meaning; reads stay served). Regression test a_failed_async_write_fails_every_later_sync_whoever_reaped_it covers append-stall and read reapers and fails without the fix; vals-store tests and workspace clippy -D warnings pass; docs/algorithms.md and Engine::sync doc updated.

Commit: vlog: a failed async value-log write is sticky (LogError::WriteLost): every later sync/drain and append fails whoever reaped it; reads of other records keep working (0545)

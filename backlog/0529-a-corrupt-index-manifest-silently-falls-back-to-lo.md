---
id: "0529"
title: A corrupt index manifest silently falls back to log replay, dropping every run and freeing their segments
status: todo
priority: 2
created: "2026-10-29"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description
Found in the 2026-10-29 project review. `load_latest_chain` maps payload checksum failures to `Ok(None)` (vals-store engine snapshot/manifest loading), and `open_inner` (crates/vals-store/src/engine/recovery.rs ~195–200) maps every error other than Io/Unsupported to "no manifest" (`Err(_) => None`). The test `corrupt_manifest_still_falls_back_to_log_replay` makes the fallback deliberate. Review repro: a 1-bit flip in the manifest payload, reopen → `runs = 0`, used segments 16→14 (run segments freed for reuse), no error. Values that vlog rewrites moved into non-tail segments are reachable only through runs, so they are lost too (not reproduced; check).

Acceptance:
- When a chain head's header is valid but no payload generation decodes, open fails closed with a corruption error instead of silently rebuilding; an older intact generation may still be used if that is provably safe.
- Rebuilding from the log after manifest loss requires an explicit operator action (flag/config), and is logged and counted when used.
- The previous-manifest scrub (zeroing old header blocks with `let _ = write_at`, no flush) is reviewed so the fail-closed rule cannot pick a stale generation.
- `corrupt_manifest_still_falls_back_to_log_replay` is replaced by tests for the new behaviour that fail with the fix reverted; docs updated.

## Acceptance criteria

## Work log

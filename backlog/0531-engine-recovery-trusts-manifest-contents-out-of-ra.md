---
id: "0531"
title: 'Engine recovery trusts manifest contents: out-of-range or shared segments panic or are accepted, and a v1 manifest plus log segments panics'
status: todo
priority: 2
created: "2026-10-29"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description
Found in the 2026-10-29 project review (scratch repros, not committed):
- A checksum-valid manifest with an out-of-range `value_segs` entry panics in recovery: `recover: segment marked twice: OutOfRange{seg: 1099511627776, ..}` (crates/vals-store/src/engine/recovery.rs ~243).
- Two runs that claim the same segments are accepted (`Ok(2)`) because a `HashSet` (~214) hides the duplicate; freeing one run later frees the other's data.
- A legacy manifest without `value_segs` (not `manifest_driven`) plus any log segment panics with `segment marked twice: AlreadyUsed(0)`: every log segment is marked at ~236–243 and `replay_winners(mark = true)` marks them again (~363–365). docs/formats.md says v1 manifests stay decodable.

Acceptance:
- Recovery validates that every segment a manifest references is in range and that runs, value segments and the manifest chain are pairwise disjoint; violations return a `Corrupt` error, never a panic.
- The v1-manifest-plus-log case opens correctly (no double mark).
- One regression test per case, each failing with its fix reverted.

## Acceptance criteria

## Work log

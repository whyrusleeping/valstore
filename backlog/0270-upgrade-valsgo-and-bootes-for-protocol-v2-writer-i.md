---
id: "0270"
title: Upgrade valsgo and bootes for protocol-v2 writer identities and coordinated rollout
status: done
priority: 1
created: "2026-09-07"
updated: "2026-09-08"
depends_on:
    - "0236"
spec_refs:
    - docs/formats.md#1.1 Connection hello
    - docs/production-readiness-review-2026-08.md#R1 — Colliding coordinator IDs can permanently diverge replicas (P0 correctness)
---

## Description

Split deployment integration from 0236. Rust protocol v2 carries [u64 HLC][u64 writer], refuses v1 before requests, and requires a coordinated upgrade; sibling valsgo/bootes still need auditing/updating before deployment. Preserve the full writer identity in every read/write/repair codec and implement collision-resistant writer-incarnation generation with shared monotonic clock semantics matching the Rust contract wherever Go generates versions. Do not merely truncate the writer or advertise v2 while retaining v1 codecs.

Acceptance: Rust/Go cross-language fixtures and end-to-end put/read/tombstone/batch tests preserve full-width versions; incompatible v1 peers are rejected without requests reaching them; same-clock/same-configured-ID writers converge; document/rehearse coordinated all-peer upgrade including PD/admin consumers. Existing Rust-packed data reads correctly after upgrade; explicitly forbid old binaries opening newly written data. No fleet wipe or rollout without checking current maintenance/forensic constraints.

## Outcome

Accepted by both comprehensive reviewers. valsgo upgrade committed separately as vals-examples 3160b76de0b9d3eea5a2092647061aeb5bdfb708: v2-only handshake before requests, full-width codecs/batch sizing, secure writer incarnations, monotonic HLC carry, tombstone versions and interop tooling. Shared Rust/Go fixtures, Go race tests, Bootes tests/build, Rust client 127 unit + 2 doc tests and targeted packed legacy/wide reopen/repair tests passed. Local file-backed node+PD coordinated restart rehearsal passed. Runbook and evidence committed here; Bootes needed no source change, prior sibling work preserved. No fleet access/wipe/rollout; hardware gate remains 0271.

Commit: Verify Go protocol-v2 interoperability and document coordinated upgrade

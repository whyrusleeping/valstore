---
id: "0340"
title: Read-only ValuePtr scrub/fsck (all run entries incl. shadowed) from 0336 probe-key
status: done
priority: 3
created: "2026-09-29"
updated: "2026-10-12"
depends_on: []
spec_refs: []
---

## Description
Proposed from 0336 (bench/0336-replica-integrity/FINDINGS.md §3, §8).

Today the only pointer integrity check is a client scan, which reads live winners only. Merges and AE never validate pointers, so a damaged pointer stays latent until a read touches it.

Proposal:
- Generalize the diag-only `valsd probe-key` (branch 0336-probe) into an offline, read-only `valsd fsck`. For every run entry, winners and shadowed alike, check len, CRC, key and version against the pointed-to record.
- Report counts and every failure with its bit-XOR pattern.
- Later, optionally, a low-rate online background scrub.

Value: gives per-node integrity evidence without a cluster census, and turns a single bit-flip observation into a rate across the fleet.

Acceptance: file-backend tests with injected pointer faults; the scrub is bounded and paced, and read-only by construction (ReadOnlyDev).

## Acceptance criteria

## Work log
- 2026-10-12: declined. Its purpose is to measure bit-flip rates across the fleet, and the owner isn't chasing faults on the lab hardware. The cheap read-time detection lives in 0339.

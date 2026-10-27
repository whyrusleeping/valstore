---
id: "0341"
title: Replica self-repair for a locally corrupt value pointer (policy decision needed)
status: done
priority: 3
created: "2026-09-29"
updated: "2026-10-12"
depends_on: []
spec_refs: []
---

## Description
Proposed from 0336 (bench/0336-replica-integrity/FINDINGS.md §2, §8). This changes repair policy and needs separate acceptance.

Gap:
- AE digests cover (key, version) only, so a replica whose pointer is corrupt still looks identical to the healthy one and is never healed.
- Re-writing the key with the same version presumably loses LWW against the equal version.

Proposal to scope:
- On a detected corrupt local read (see the corrupt-read observability proposal), queue the key for a fetch from a healthy replica.
- Install the fetched value as a fresh local record at the same version, overriding only the local pointer. Define the equal-version override rules and how they interact with tombstones and GC.
- Alternatively, an operator-only admin repair command.

No persisted-format change is intended.

## Acceptance criteria

## Work log
- 2026-10-12: declined. It needs a new equal-version repair policy, and the only observed cause is bad RAM on the test fleet, which the owner isn't chasing. Revisit if corrupt-read counters (0339) show the problem on good hardware.

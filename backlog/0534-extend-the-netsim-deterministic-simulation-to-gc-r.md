---
id: "0534"
title: Extend the netsim deterministic simulation to GC, range tombstones, sorted zones, restarts from disk and placement changes
status: todo
priority: 3
created: "2026-10-29"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description
Found in the 2026-10-29 project review. crates/vals-cluster/tests/netsim.rs is a real deterministic simulation with an acknowledged-version/convergence checker (~384–404), but it uses unsorted memory stores without tombstone GC (~204–214), runs AE through `LocalPeer` (~229–245), and models a crash as replacing a store with an empty one (~490–501). The hardest interacting state machines — GC admission floors, range tombstones, sorted zones, production re-home, PD-driven placement changes, persisted state across restart — are not explored together; recent bugs (0515, 0517, 0518, 0530) live exactly there.

Scope (extend, don't replace):
- Sorted zones with range deletes and `gc_grace > 0` (virtual clock).
- Crash = reopen from a persisted (file or crash-simulating) device rather than an empty store.
- At least one placement transition (add-node/re-home or membership change) in the schedule.
- A read-history checker against the documented LWW + tombstone semantics (no acknowledged write lost below its overwrite, no resurrection after a collected delete), not linearizability.
- Default lane stays a few seconds with a fixed seed set; a long-run seed sweep behind an env var.

Acceptance: the extended sim runs in the default lane; an injected known bug (e.g. reverting 0515's or 0530's fix) is caught by some seed.

## Acceptance criteria

## Work log

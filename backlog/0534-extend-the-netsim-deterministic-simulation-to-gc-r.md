---
id: "0534"
title: Extend the netsim deterministic simulation to GC, range tombstones, sorted zones, restarts from disk and placement changes
status: done
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

## Outcome

New crate-internal test module crates/vals-cluster/src/netsim_gc_tests.rs; tests/netsim.rs is unchanged. It covers a sorted zone with point and range deletes and gc_grace on a virtual clock, plus a node join (epoch 2). It drives production code: Coordinator, AE with range-tombstone exchange, cleanup handoffs, compact_step and compact_runs. Crash reopens the store from its device, losing unsynced data, with the persisted watermark. Faults: drops, duplicates, late delivery past the grace, down/cut windows. Checks: phantoms, lost acknowledged writes, resurrection, admission below an old delete, GC above the persisted cutoff, owner convergence. The default lane runs 2 seeds (about 3.5 s) plus a replay test. VALS_SIM_SEEDS runs a sweep. With 0530's bug reinjected, default seed 2 fails, and so do 8 of 24 sweep seeds. A 200-seed sweep passes with the fixed code. Filed proposed 0543.

Commit: 0534: deterministic sorted-zone GC simulation with range deletes, crash-reopen, a node join, late delivery and an LWW/tombstone history checker; VALS_SIM_SEEDS sweep

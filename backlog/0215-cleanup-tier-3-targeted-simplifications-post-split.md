---
id: "0215"
title: 'Cleanup Tier 3: targeted simplifications (post-split)'
status: done
priority: 3
created: "2026-08-16"
updated: "2026-08-17"
depends_on: []
spec_refs: []
---

## Description

Behavior-preserving refactors identified in the survey, best done after the Tier 2 splits: vlog checked/unchecked decode dedup; scan materialization/planning dedup; env parsing consolidation (incl. caching per-request VALS_STAGED_SCAN read in net/server.rs:3106); compact_plan_inner breakup; shared CompactPlan/Engine/ServerShared/VersionedStore constructors; staged-apply retry-loop dedup (node.rs rehome vs server Apply); ConnDriver inflight RAII; client fd_* wrapper and shard-grouping dedup; wire.rs list-size validation unification; vals-server config/MiB helpers; bench net-mode unification + usage text generated from dispatch table. Each item lands separately with tests.

## Outcome

Landed the vals-store chunk of the Tier-3 cleanup as pure behavior-preserving refactors: (1) unified checked/unchecked vlog value decoding via decode_record_value_inner + shared record_bytes tail-overlay slice prep across batch/single reads; (2) split the ~300-line compact_plan_inner into named stages (eligibility, pressure preamble, start selection, headroom fitting, pressure rewrite selection, plan assembly) with identical control flow and error paths; (3) shared CompactPlan::new used by both planner and aged-vlog-GC literal sites; (4) shared Engine::from_parts for create and open/recovery. Verified line-by-line against originals; cargo test -p vals-store all green (307+ tests, debug), workspace builds; reviewer accepted. Remaining 0215 scope split into 0219 (cluster chunk) and 0220 (client/bench/server chunk); backlog.md index regenerated from frontmatter.

Commit: store: dedup vlog decode paths, break up compact_plan_inner, share Engine/CompactPlan construction

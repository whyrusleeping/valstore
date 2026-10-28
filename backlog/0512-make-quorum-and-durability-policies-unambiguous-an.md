---
id: "0512"
title: Make quorum and durability policies unambiguous and easy to select
status: done
priority: 2
created: "2026-10-28"
updated: "2026-10-28"
depends_on: []
spec_refs:
    - docs/getting-started.md#Reading and writing
    - ARCHITECTURE.md#Consistency model
---

## Description

Accepted follow-up to adopter review 0507, finding 3. Reduce correctness-policy traps without silently weakening existing guarantees or changing defaults merely for ergonomics.

Acceptance criteria:
- Put greatest-LWW-Version versus latest-completed-request semantics beside introductory strong/latest claims; explain non-linearizable CAS with unsafe application patterns.
- Provide an explicit recommended production policy and runnable device-durable majority example, contrasting the existing buffered/All write() defaults and single-node-failure availability.
- Review read quorum behavior across zones with differing RF: introduce a majority-derived read policy or equally ergonomic safe helper, document numeric/quorum clamping semantics, and cover mixed-RF behavior with tests that would catch weakening overlap guarantees.
- Decide and document whether write() defaults stay unchanged; any behavior/API change needs explicit compatibility documentation and tests, not an incidental default flip.
- Include CLI selection of persistence if extending kv, so a reader can exercise the recommended policy without rewriting the example. Coordinate public API boundaries with task 0269.

## Outcome

New public ReadConcern {One, Majority, All, Quorum(n)}, clamped to 1..=RF; a usize converts to Quorum(n). Coordinator::new and the read/scan methods take impl Into<ReadConcern>, with R computed per key or range; a count of 0 now reads one replica (documented). Defaults are unchanged (write() stays buffered All). The crate docs gain "Choosing a policy" (device-durable majority writes plus majority reads); the crate docs and ARCHITECTURE explain greatest-version semantics, unsafe CAS patterns and the mixed-RF trap. kv gains --persistence buffered|device; kv and tokio_bridge use ReadConcern::Majority. Mixed-RF LocalCluster tests (RF 1/3/5, sorted and unsorted) catch a weakened majority. Verified with the workspace build, 1000 vals-cluster lib tests, vals-client tests and doctests, a clean rustdoc, and a local 3-node kv run. Follow-up for the 0505 stack: getting-started and embedding docs could switch to ReadConcern::Majority.

Commit: 0512: add ReadConcern (majority reads per key's RF), document the recommended device-durable majority policy, kv --persistence

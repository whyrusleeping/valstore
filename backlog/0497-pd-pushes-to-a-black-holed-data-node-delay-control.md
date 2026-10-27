---
id: "0497"
title: PD pushes to a black-holed data node delay control pushes to the healthy nodes
status: done
priority: 4
created: "2026-10-21"
updated: "2026-10-27"
depends_on: []
spec_refs: []
---

## Description

Found by the 0280 host-loss drill (bench/0280-three-pd/FINDINGS.md, 2026-10-21). With lab-1 unreachable, the PD leader logged `opaque range-map delivery to 10.0.0.114:700N blocked by capability preflight; connection timed out` once per second for each of the 8 shard addresses, and a zone created during the outage reached the two healthy nodes' shards after 23.3 s instead of the 0.35–0.38 s measured after ordinary leader changes.

Acceptance: identify the serialization (capability preflight or push loop waiting on per-address connect timeouts); a test with one black-holed push target shows the other targets receive a new zone within a bound close to the healthy-cluster time.

## Outcome

Cause: one PD pusher thread ran config pushes, the restart beacon and the range-stats poll, and each request to a dead address waited out its timeout in turn (the stats poll alone tried 3 versions at 1 s each per address, ~24 s for 8 addresses, matching the 23.3 s drill result). Fix: PushLanes gives each address one in-flight push thread (members→zones→range maps order kept, existing retry/preflight logic applied to the results), and beacon probes and the stats fetch run in parallel on side threads. New test a_black_holed_node_does_not_delay_zone_pushes_to_the_healthy_nodes: with 8 black-holed addresses a new zone reaches the healthy nodes in 11–47 ms (13–17 ms with none, 40–48 s before). vals-cluster suites pass apart from a known flaky test that passes alone. deploy.md and 0280 FINDINGS updated; the hardware drill was not re-run.

Commit: fix(0497): PD pushes each data-node address on its own thread and runs the restart beacon and range-stats fetch off the push loop, so a black-holed node no longer delays control pushes to healthy nodes

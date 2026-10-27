---
id: "0470"
title: 'vals-examples: port kvchurn/eventlog/valsmix to WriteConcern (0242) and retire valsgo ForwardReplicas'' opcode 21 (now RangeDelete)'
status: done
priority: 4
created: "2026-10-15"
updated: "2026-10-22"
depends_on: []
spec_refs: []
---

## Description

Found during 0468 (vals-examples 2aa077c).

- `kvchurn`, `eventlog` and `valsmix` in `~/code/vals-examples` still import `vals_cluster::Durability` / call `write_tier_in`, which 0242 removed, so `cargo build` of the whole examples workspace fails (0468 ported only `valsq` and `valsdump`, which the interop test needs). Port them with the 0242 migration table (docs/getting-started.md §Write options): `NoSync/OneNode/Majority/Replicated` → `WriteConcern::{Unacknowledged, One, Majority, All}`, `write_tier_in` → `write_in`.
- valsgo `Config.ForwardReplicas` sends `ForwardBatchApply` as opcode 21 (server side only on the unmerged 0302 branch). Current servers assign 21 to `RangeDelete`, so a ForwardReplicas client against them sends frames that decode as malformed range deletes (refused in practice: the slice count lands far above 4096). Either remove the opt-in path (0302/0306 were rejected) or renumber it, and make bootes' `--forward` flag fail clearly.

Acceptance: `cargo build --release` of the examples workspace succeeds; ForwardReplicas can no longer emit opcode 21.

## Outcome

vals-examples 312d61c: kvchurn, eventlog and valsmix are ported to WriteConcern/write_in, so `cargo build --release` of the examples workspace succeeds. valsgo's rejected ForwardReplicas path (0302/0306) is removed, including the config, dispatch, encoder, decoder and tests; the shared fake-node helpers moved to fakenode_test.go. Opcode 21 is now sent only by RangeDelete. bootes a0edb9e: the hidden `--vals-forward-replicas` flag fails before setup with a clear "was removed" error. Go vet and tests pass in both repos, and one reviewer accepted with independent rebuilds.

Commit: backlog(0470): examples build against WriteConcern; valsgo ForwardReplicas removed, bootes --vals-forward-replicas fails clearly (vals-examples 312d61c, bootes a0edb9e)

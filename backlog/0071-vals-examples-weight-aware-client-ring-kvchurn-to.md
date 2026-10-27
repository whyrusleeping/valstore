---
id: "0071"
title: 'vals-examples: weight-aware client ring (kvchurn) to demonstrate placement skew on hardware'
status: done
priority: 5
created: "2026-07-06"
updated: "2026-08-11"
depends_on: []
spec_refs: []
---

## Description
Follow-on from task 0041 scenario C: the PD commits per-node ring weights (`data_node = <id>*<weight>`) and `admin members` shows them. Audit the stock `kvchurn` (sibling repo `vals-examples`) and make its adoption of the PD-committed weighted ring and per-node shard addresses explicit and regression-protected.

The audit corrected the original premise: `kvchurn`'s hardcoded weight-1, single-address ring is only its bootstrap ring. Its pre-I/O `Coordinator::refresh_ring` already installs the PD's weighted members and all advertised shard addresses. At RF=2 replicas are distinct, so a 2:1:1 ring uses weighted sampling without replacement: expected replica-record shares are near 5/6:7/12:7/12, or about 1.43× for the weight-2 node versus the weight-1 peer mean. Approximately 2× is the RF=1/primary-placement expectation. The old hardware counts near 1.36× are evidence of weighted adoption, not a client failure.

Note: lives in the sibling repo `vals-examples` (`~/code/vals-examples` locally, `~/vals-examples` on the box) — outside this workspace root; may need a shell-level change there or a user hand-off.

## Acceptance criteria
- Before issuing I/O, `kvchurn` adopts the PD-committed member weights and every advertised per-node shard address, with regression coverage and visible startup topology.
- For a 2:1:1 ring at RF=2, deterministic replica ownership is consistent with distinct-node weighted sampling without replacement (about 1.43× for the weight-2 node versus the peer mean); RF=1 primary ownership is the case expected near 2×.
- Scenario C records the corrected interpretation of the preserved hardware metrics and the exact deterministic 500k-key replay. A new run is not required on the retired original rig, and unrelated idle hardware is not repurposed.

## Plan

1. Audit current `kvchurn` startup against the current `vals-client` membership protocol to determine whether pre-I/O `refresh_ring` already installs PD-committed weights and all per-node shard addresses; preserve this path or make the smallest explicit client fix needed.
2. Add focused coverage/documentation in `vals-examples` so weighted membership and sharded-address adoption cannot silently regress and operators know the bootstrap syntax/behavior.
3. Replay a deterministic 500k-key corpus through the production ring implementation and confirm RF=2 distinct-replica ownership is near the expected 1.43× weight-2/peer-mean ratio. Reuse the preserved hardware metrics because the original four-drive rig has been retired; do not repurpose unrelated idle hosts.
4. Record the exact command, cluster membership, counts/ratios, corrected result, and hardware limitation in the existing scenario-C benchmark notes; run relevant local checks for all code/docs changes.

### Starting points
- /home/user/code/vals-examples/kvchurn/src/main.rs:181-203 (`make_coordinator`, `adopt_config`)
- crates/vals-client/src/coord/mod.rs:956-985 (`refresh_ring` installs weighted members and addrs)
- bench/spdk-liveness-box-notes.md#Scenario C

## Work log
- 2026-08-11 plan: 1. Audit current `kvchurn` startup against the current `vals-client` membership protocol to determine whether pre-I/O `refresh_ring` already installs PD-committed weights and all per-node shard addres
…[truncated]
- 2026-08-11 context hints: 3 recorded with plan
- 2026-08-11 context hints: `/home/user/code/vals-examples/kvchurn/src/main.rs:181-203`: bootstrap ring then pre-I/O `refresh_ring`; `crates/vals-client/src/coord/mod.rs:956-985`: current refresh adopts `view.members` weights and
…[truncated]
- 2026-08-11 preload: 4 file(s), ~24 KiB seeded into implementer context
- 2026-08-11 implementer report: BLOCKED — Task 0071’s acceptance criterion conflicts with the current distinct-replica ring semantics. The audit shows kvchurn already calls `refresh_ring` before any I/O, and `Coordinator::refresh_ring` atom
…[truncated]
- 2026-08-11 revision: Implemented Task 0071 with the corrected distinct-replica semantics.  Changes: - In `vals-client`, factored membership installation into `Coordinator::adopt_ring_view` and added `ring_view_adopts_weig
…[truncated]
- 2026-08-11 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-11 review (sol): accept — The change satisfies Task 0071: kvchurn validates and visibly reports the PD-adopted weighted/sharded topology before workers cross the first-I/O barrier, the coordinator adoption path now has focused
…[truncated]
- 2026-08-11 review (sol#2/sol): accept — The change correctly makes kvchurn’s pre-I/O PD-ring adoption explicit, validates installed weighted membership and shard-route counts at startup, and adds focused tests for RingView adoption and de
…[truncated]
- 2026-08-11 revision: Applied the requested documentation-only corrections in `bench/spdk-liveness-box-notes.md`: - B.2 now retains the observed zero per-partition watermark while explicitly withdrawing the unsupported sha
…[truncated]
- 2026-08-11 review (sol): accept — The revision resolves the prior documentation contradiction by withdrawing the unsupported shard-0-only explanation while retaining the historical watermark observation. It also corrects the recorded 
…[truncated]
- 2026-08-11 review (sol#2/sol): accept — The revision resolves the prior documentation issues: the hardware ratios are now accurate, the typo is fixed, and the unsupported shard-0 attribution for the zero watermark has been appropriately wit
…[truncated]
- 2026-08-11 decision: accept — commit: client: expose weighted sharded ring adoption
- 2026-08-11 usage: 7,641,171 tok (in 1,816,082, out 38,977, cache_r 5,786,112, cache_w 0) · cost n/a (unpriced)
  implementer: 4,399,476 tok (in 839,319, out 18,397, cache_r 3,541,760, cache_w 0) · cost n/a (unpriced)
  coordinator: 1,387,541 tok (in 446,429, out 7,736, cache_r 933,376, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 1,274,619 tok (in 323,176, out 6,803, cache_r 944,640, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 579,535 tok (in 207,158, out 6,041, cache_r 366,336, cache_w 0) · cost n/a (unpriced)

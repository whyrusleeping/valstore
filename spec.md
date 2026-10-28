# vals — project spec (hub)

> This file is the planning hub, not the design document. The canonical design +
> status docs are:
>
> - [`ARCHITECTURE.md`](ARCHITECTURE.md) — the target architecture + as-built status map.
> - [`docs/`](docs/README.md) — reference (wire/on-disk formats, agreement algorithms)
>   and subsystem deep-dives (zones, multi-drive, SPDK, out-of-core index, …).
> - [`README.md`](README.md) — user-facing overview, build/test, doc map.
> - Open work is tracked in the **backlog** (`backlog.md` index / `backlog/`
>   task files); the record of shipped work lives in git history and the
>   `ARCHITECTURE.md` status map.
>
> This spec grows incrementally as new work is planned — spec the work, not the repo.

## Vision (one breath)

A distributed key/value store on raw NVMe — userspace/SPDK, thread-per-core,
shared-nothing — with a **leaderless** Dynamo-style data path (consistent-hash
placement, tunable W/R quorums, HLC last-writer-wins, read-repair + hinted handoff +
Merkle anti-entropy) and Raft **only in the control plane** (a PD owning membership,
zones, and the placement ring). Zones give per-namespace RF, placement scope, and key
ordering; sorted zones support globally-ordered range scans. Production and reference
deployments use three voting PD replicas on distinct failure domains; a single PD is
an explicitly non-HA dev/test exception. The data path can continue on its last
consistent pushed config during a PD outage, but reconfiguration and restart healing
require a PD quorum (see [`docs/deploy.md`](docs/deploy.md#pd-availability-and-recovery)).

## Where the project stands

Feature-complete end-to-end and validated on real hardware. All 2026-06 deep-review
production blockers are resolved; contracts were frozen pre-users (versioned wire
hello `PROTO_VERSION 2`, on-disk `[magic][u16 version]` envelopes, sealed public API).
The single-node engine reaches the raw drive ceilings, and a clustered SPDK run
sustains millions of replicated writes/s and strong reads/s (see the `README.md` and
`docs/SPDK.md` headline numbers).

The single-node RAM-ceiling and scan/recovery workstreams that dominated mid-2026 have
shipped: the out-of-core LSM (byte-cap memtable flush → on-disk runs, tiered index-only
run-merge, off-core flush/compaction), the durable index snapshot (recovery is
manifest + runs + tail replay, boot-before-serve), the front-coded in-RAM index
(Stage B1), the ordered-scan performance pass, key-ordered vlog co-compaction, and
the `valsd dump` bulk-export primitive. Sorted zones now auto-split at a live-entry
threshold (**default-on** as of task 0108) with a PD-wired range merge.

## Current work

The [backlog](backlog.md) is the sole work-status index. Check each task's current
status, prerequisites, and evidence before resuming an old experiment. In particular,
the read-path candidates 0207 and 0209 were measured and rejected, not left awaiting
implementation. Historical performance results do not imply that outstanding
release or integrity gates have passed.

## User entry points

The [README](README.md) and [getting-started guide](docs/getting-started.md)
lead with running `valsd`, sending CLI traffic and connecting an application.
The [embedding guide](docs/embedding.md) is a first-class alternative for a
local `vals-store` engine or a distributed `vals-cluster` node. A remote
`vals-client` application is not an embedded database. Distributed semantics
stay in the common user guide; daemon lifecycle mechanics live in
[deploy.md](docs/deploy.md), and application-owned lifecycle mechanics live in
the embedding guide. This presentation is not an API freeze or a new recovery
qualification.

## Cleanup survey

The owner-reviewed inventory and final dispositions are in
[`docs/optional-feature-survey.md`](docs/optional-feature-survey.md).

## Constraints / conventions

- **Dependency-light by philosophy**: hand-rolled wire codec, config parser, HTTP
  metrics endpoint; no serde/gRPC/tokio on the hot path. New dependencies need a
  strong case.
- **Everything except the SPDK backend runs off-hardware** (`MemBlockDevice` /
  `FileBlockDevice`); `cargo test --workspace` must stay green without hardware.
  Hot-path changes need **box validation** (an SPDK cluster on datacenter-NVMe
  class hardware) before adoption — A/B, perf-profiled, value-verified (`kvchurn`).
  This is task-sized confirmation under [`docs/validation.md`](docs/validation.md),
  not a fresh full replay per change. Plans bound hardware arms, data, time and
  exit conditions; large campaigns belong to tasks that explicitly ask for them.
  *(The long-time test box `oldbox` was retired 2026-07-15; box
  validation now runs on the **lab-1/2/3** fleet — see
  `plans/box-provisioning-runbook.md`. Older bench notes / runbooks still
  reference the old hostname/paths and must be re-pointed on the new hardware.)*
  The recurring end-to-end load test — a fresh bootes (atproto firehose +
  backfill) soak into the live SPDK cluster — has a step-by-step runbook at
  `plans/bootes-soak-runbook.md`.
- **Local-first validation**: every task picks the cheapest validation tier that
  can really establish its acceptance criteria. The tiers run from L0 local
  tests up to the L3 full replay, which is a release gate only. Hardware is used
  to confirm, in the smallest step that answers the question. See
  [`docs/validation.md`](docs/validation.md).
- **Scale with what changed** (owner, 2026-10-20): vals must handle massive
  datasets. A per-shard or background cost that grows with total cluster data or
  range count, rather than with the shard's own share or with what changed, is a
  scaling bug worth fixing (examples: tasks 0469, 0502, 0503).
- **Kill-the-fix testing**: regressions are pinned by tests that fail with the fix
  reverted.
- Verification workflow: `scripts/ci.sh` (locked build, tests, clippy, Python tests;
  see `docs/ci.md`). The tree is not rustfmt-clean, so never run `cargo fmt`. SPDK
  build via `VALS_SPDK_LINK=1 cargo build --release -p vals-bench --features spdk`.
  Contributor-facing conventions are in `CONTRIBUTING.md`.

## Open questions (need owner input)

- **Transport security**: **out of scope** (owner, 2026-10-12). vals does not
  encrypt or authenticate its data transport; deployments run on a private network
  or an overlay such as Tailscale/WireGuard (tasks 0010, 0241).
- **Licensing/publication**: **open source under MIT** (owner, 2026-10-27; `LICENSE`,
  workspace `license = "MIT"`); crates are still `publish = false` (not on crates.io).
  The `vals-examples` sibling repo (valsgo, example clients) stays private.
- **Erasure coding**: EC-ready claim **downgraded** — placement is whole-value, not
  per-shard; out of scope unless placement is redesigned (task 0023, done docs-only).
- **Multi-machine / real-NIC run**: now unblocked on the lab fleet (a live
  3-node real-NIC SPDK cluster runs there — task 0014).
- **Linearizable zones**: kept parked, tracked as P5 design task 0026.
- **Auto-eviction** (failure detector → PD `remove_node`, task 0017): owner wants a
  primer before deciding; stays deferred (P4) meanwhile.

## Backlog conventions

Open work lives in the backlog (`backlog.md` index, canonical task files under
`backlog/`). Priorities: P1 = current focus, P2 = next up / correctness-adjacent,
P3 = valuable but unscheduled, P4–5 = deferred/parked.

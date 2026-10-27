---
id: "0481"
title: 'vals-bench graph: a LinkBench-style graph workload to track vals as a graph store'
status: done
priority: 3
created: "2026-10-17"
updated: "2026-10-17"
depends_on: []
spec_refs: []
---

## Description

## Description
## Why
Owner discussion 2026-10-16: how well could vals serve as a graph database? Short answer: it is a strong **edge store** for huge, write-heavy, mostly one-hop graphs, the TAO / JanusGraph-on-Cassandra pattern. bootes already uses it this way for the ATProto social graph, with `records` holding follow/like edges and the `backlinks` sorted zone `{ref}\0{source_uri}\0{path}` holding reverse edges. It is weak at short adjacency scans on aged engines, multi-hop traversals and atomic edge pairs. The owner wants a standing benchmark to measure that gap and to A/B the follow-on work (the batched multi-prefix scan task, the cheaper-short-scans task, the descending-scan task).

## Data model (fixed, documented in the bench README)
- `gv`: vertices, `v/{id}` → properties (~100–300 B).
- `ge`: out-edges, sorted zone, `{src}\0{type}\0{dst}` → small edge properties. A time-ordered variant is `{src}\0{type}\0{ts_be}\0{dst}`. Until descending scans exist, "latest N" uses an inverted-timestamp variant; record which one each query uses.
- `gr`: in-edges, sorted zone, `{dst}\0{type}\0{src}`, written alongside every `ge` edge (non-atomic pair).
- Vertex ids are fixed-width, so adjacency prefixes don't collide.

## Graph generator
- Deterministic and seeded: N vertices, power-law (Zipf or preferential-attachment) out-degree with a configurable mean.
- A few "celebrity" vertices large enough (≥ 1M in-edges at full scale) to drive range auto-split and re-home.
- Scale knob from a ~15 min local run up to an lab run.
- Optional, later: real follow edges from jetstream-cache-5b (`app.bsky.graph.follow`).

## Phases
1. **Load.** Edge ingest through `write_batch`, both halves of each edge, plus vertices. Report ACK edges/s.
2. **Age.** Short scans pay one block read per overlapping run, so a fresh engine flatters them. Reach a realistic run count per engine: either ingest enough data, or shrink `VALS_MERGE_INPUT_CAP_BYTES` / `VALS_MERGE_FREEZE_BYTES` locally so an L1 run holds ~50–100 frozen runs/engine. Report runs/engine at the start of the query phase.
3. **Mixed query phase.** A LinkBench-like op mix:
   - get_vertex: point read.
   - edge exists: point read of an edge key.
   - out-neighbours with limit N, and in-neighbours with limit N: short scans.
   - latest-N edges.
   - degree count: full prefix scan today; keys-only once that exists.
   - 2-hop friends-of-friends: per-hop fan-out cap, frontier properties via `read_many`.
   - Churn: add/delete edge (both halves), and delete vertex (range delete of its `ge` prefix plus per-key deletes of its `gr` back-references).
4. **Pair check.** Count `ge` edges without a `gr` twin and the reverse. This is a correctness signal for the non-atomic pair writes, and it should be 0 without faults.

## Measurements
- Per op type: throughput, p50/p99/p999, RPCs per query, bytes returned per query.
- Server/engine counters per scan:
  - run cursors opened;
  - run blocks read (and bytes);
  - value-log reads (and spans);
  - entries returned.

  Add these counters if missing (today only `point_value_device_blocks` and scan latency exist). Task "cheaper short scans" needs them for attribution.
- Output: a JSON summary per arm.

## Shape
- In-process mode, `vals-bench <mem|file> graph ...`: 3 nodes + PD, following the `rtqueue` pattern, for L0/L1.
- Networked mode against a live `valsd` cluster, following `netqueue`/`netload`, for lab SPDK confirmation.
- Harness and baseline under `bench/<id>-graph/` (`run.sh`, README, FINDINGS).

## Acceptance
- A default local file-backend run completes in ≲ 15 min and reports every op type plus the per-scan counters.
- Repeat runs are reproducible enough to A/B (report run-to-run spread over ≥ 2 repeats).
- The baseline FINDINGS is committed: local first, then one lab arm.
- The pair check reports 0 mismatches on a fault-free run.

## Acceptance criteria

## Outcome

Implemented deterministic graph/netgraph workloads, paired adjacency/latest indexes, ten operation types, churn and exhaustive pair checks; added scan IO and per-transport RPC attribution. Two default local file arms completed in 143 s, subsequent lab SPDK arm in 172 s, all with identical final 319804 edge pairs and zero mismatches. Retained JSON summaries, spread, README and FINDINGS. Focused suites and knockout tests passed; independent reviewer rebuilt and repeated two default arms successfully and accepted. Restored original lab binaries/settings and preserved unrelated 0477 work.

Commit: feat(0481): add aged graph workload and scan IO attribution with local/SPDK baselines

# vals docs

Design and reference docs for `vals` (a distributed KV store on raw NVMe).

**Using vals?** Start with [`getting-started.md`](getting-started.md) — the
end-user walkthrough (embedding the library, standing up a cluster, the
coordinator API, zones, operating it) — and [`deploy.md`](deploy.md) for
running real servers with `valsd` (config reference, admin socket, systemd).
API reference (signatures; the design prose lives here in `docs/`):
`cargo doc --workspace --no-deps --open`.

**Working on vals?** Start with [`../ARCHITECTURE.md`](../ARCHITECTURE.md) — the
target architecture and status map, and the hub that links everything here. Open
work is in the ycc backlog ([`../backlog.md`](../backlog.md)). How to validate
work, from local tests up to the hardware release gate, is in
[`validation.md`](validation.md).

## Reference (what a reimplementer needs)

- [`formats.md`](formats.md) — **byte-level** wire and on-disk layouts: the RPC
  protocol (opcode/kind tags + per-message fields), the vlog record/segment headers,
  the packed record version and one-byte cluster value kind, zone superblock,
  drive-layout map, and the hint/liveness files, with a magic-number table.
- [`algorithms.md`](algorithms.md) — the **agreement algorithms** that must be
  bit-identical across nodes: the placement hash (`hash64`), ring + range-token
  placement, shard routing, HLC/last-writer-wins order, the Merkle anti-entropy
  digest, the CRC-32C variant, and a parameters/defaults table.

## Subsystem deep-dives

- [`zones.md`](zones.md) — zones (per-zone RF/scope/ordering), the sorted-zone range
  partitioner, range maps, distributed scans, and splits.
- [`range-deletes.md`](range-deletes.md) — design (not built yet) for versioned
  range tombstones in sorted zones: cover semantics, engine persistence, skip-scans,
  anti-entropy over covered keys, GC and formats (task 0439).
- [`pd-admin.md`](pd-admin.md) — leader-aware PD administration and safe handling
  of ambiguous mutation outcomes.
- [`backup-restore.md`](backup-restore.md) — the logical backup contract
  (`valsd backup create`/`verify`): consistency point, replica merging, versions,
  tombstones and limits; and the restore runbook (`restore-pd`/`load`/`compare`)
  into a new, separately fenced cluster.
- [`multi-drive.md`](multi-drive.md) — JBOD multi-drive layout, ring-weighting by
  drive count, intra-node rebalance/relocate, and the layout-fingerprint guard.
- [`batch-writes.md`](batch-writes.md) — the batch write API (per-node group commit,
  per-write outcomes).
- [`backpressure.md`](backpressure.md) — the credit-pool flow control and the
  per-conn send-buffer backstop.
- [`SPDK.md`](SPDK.md) — the userspace-NVMe backend: build/run, device ceilings, and
  clustered throughput.
- [`scan-performance.md`](scan-performance.md) — the ordered-scan (sorted-zone
  range scan) performance pass: span-coalesced value reads, lazy memtable
  paging, parallel range scans, measured numbers, and the key-ordered
  co-compaction roadmap.
- [`future-linearizable-zones.md`](future-linearizable-zones.md) — the parked design
  for an opt-in per-key-consensus zone mode (linearizable CAS / in-partition
  multi-key atomicity).

## Conventions

All integers are little-endian; checksums are CRC-32C (Castagnoli). The wire
protocol is versioned and negotiation-gated by the `PROTO_VERSION` hello; on-disk
containers carry `[magic][u16 version]` envelopes and unknown versions fail loud.
[`formats.md`](formats.md) is the current-contract snapshot. These version fences
make upgrades explicit; they are not an unlimited backward-compatibility promise.

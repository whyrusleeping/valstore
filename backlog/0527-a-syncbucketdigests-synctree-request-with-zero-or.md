---
id: "0527"
title: A SyncBucketDigests/SyncTree request with zero or oversized tree geometry panics the data node
status: todo
priority: 3
created: "2026-10-29"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description
Found in the 2026-10-29 project review. `OP_SYNC_BUCKET_DIGESTS` decodes `num_buckets` without validation (crates/vals-client/src/wire/decode.rs ~328) and the server passes it through `serve_bucket_digests` (net/server/scan.rs:80) → `plan_bucket_digest_page` (replica/read.rs:250) → `antientropy::bucket_of`, which computes `hash64(key) % num_buckets`: `num_buckets = 0` divides by zero on any zone with a resident key, and a server-thread panic aborts the process. `SyncTree` geometry is guarded only by debug assertions (replica/version_index.rs ~665–688): `scope_g = 0` reaches a modulo by zero and a huge `level` requests a huge allocation in release.

Owner treats input as trusted for now, so this is a robustness fix (a buggy peer or client must not take a node down), not a security gate.

Acceptance:
- The server validates SyncBucketDigests/SyncTree geometry (non-zero bucket/scope counts, bounded level, scope ids in range) before dispatch and replies `Response::Err` instead of panicking.
- Release-mode-reachable regression tests (not debug_assert) that send the bad geometries and fail with the validation removed.

## Acceptance criteria

## Work log

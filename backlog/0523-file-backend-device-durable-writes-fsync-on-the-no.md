---
id: "0523"
title: 'File backend: device-durable writes fsync on the node''s only serving thread, stalling its reads'
status: done
priority: 2
created: "2026-10-28"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description

Found by 0510's file-backed benchmarks (local evidence in bench/0510-file-bench/results-single*, gitignored).

`FileBlockDevice` has no `submit_flush`, so `sync_store` (vals-cluster net/server/apply.rs) falls back to a synchronous `store.sync()` (`File::sync_all`) on the shard thread. A file-backed node is unsharded, so each group commit blocks every request on that node for the whole fsync.

Measured on one lab host (Core Ultra 9 285HX, Samsung 990 PRO, ext4 on LVM; three file-backed nodes, RF 3, majority writes and reads, 1 KiB values, 4M keys, `scripts/file-bench.py`):
- `vals_sync_flush_us`: every commit took 2.5–25 ms (about 10.5 ms mean); the shard thread sat in `D` state.
- Device-durable majority writes: about 2.8k/s at 256 in flight, with roughly 218k `batch_apply_gate` Busy retries; 1.4k/s at 64 in flight, p50 about 43 ms.
- Mixed workload (25% device-durable writes, 65% reads, 10% 8-key batch reads): 9.0k ops/s, read p50 about 22 ms. With buffered writes it was 42.9k ops/s, read p50 6.6 ms, and 1.3 ms at half load.

Acceptance:
- A device-durable write on the file backend no longer blocks reads and buffered writes on that node for the length of an fsync. One way is an off-thread fdatasync completion behind `submit_flush`, keeping group commit and its durability ordering.
- Durability semantics are unchanged: an ack still means the device sync covering that write has completed. Add a test for that ordering.
- Re-measure `scripts/file-bench.py --phases write-device,mixed-device` against the current numbers on the same host.

## Outcome

File-backed nodes no longer block on fsync: submit_flush hands fdatasync to a per-file helper thread; the group-commit leader awaits a waker (eventfd interrupts epoll); acks still follow the covering flush (new staged_apply ordering test: no ack before completion, reads/buffered writes served mid-flush, failure propagates; vals-spdk helper-thread test). Writes waiting on an off-thread flush release their batch_apply_gate permit (flush_completes_off_thread; SPDK unchanged) — without it 256 in flight fell to 2.0k/s with 520k Busy. Lab-host A/B: write-device 2,604→11,085 ops/s at 256 in flight (Busy 202k→0), mixed-device 10,681→40,351 with read p50 19.1→1.1 ms; at 64 in flight 1,497→2,684 and 1,986→10,208. Docs: benchmarks, configuration, embedding; ARCHITECTURE/batch-writes/backpressure left alone (held by 0538's uncommitted pass). ci.sh passed on a clean snapshot.

Commit: 0523: file backend fdatasyncs on a helper thread that wakes the shard; writes waiting on it release their batch_apply_gate permit; FileBlockDevice::flush uses fdatasync

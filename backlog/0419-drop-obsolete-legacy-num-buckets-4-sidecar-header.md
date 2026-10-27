---
id: "0419"
title: Drop obsolete legacy `num_buckets*4` sidecar-header slack from run_len_upper_bound_agg for v4/v5 runs
status: done
priority: 4
created: "2026-10-11"
updated: "2026-10-13"
depends_on: []
spec_refs: []
---

## Description

From the 0371 review (2026-10-10). `run_len_upper_bound_agg` (`crates/vals-store/src/run.rs` ~1081) still adds `4 + num_buckets*4 + 4`, the bucket-grouped sidecar header from v1/v2. Neither v4 nor v5 writes that header: both store `num_buckets` as a fixed-width field. The bound is therefore correct, since it only over-estimates, but every run reservation is inflated by 16 KiB at the default 4096 buckets, or up to 256 KiB at 65535. That can reject work near capacity.

Acceptance: remove or tighten the term for v4/v5, keeping the bound dominant. The randomized bound test (`run_len_upper_bound_dominates_build_run_over_randomized_shapes`, now v4 + v5 with uniform-key shapes) must stay green. Add reservation-boundary tests. Tier L0.

## Outcome

run_len_upper_bound_agg drops the v1/v2 4+num_buckets*4+4 term: v4 adds its exact sidecar (8 + n*20 + key_bytes), v5 nothing, so reservations no longer grow with num_buckets (was +16 KiB at 4096, +256 KiB at 65535). Tests: bound equal across num_buckets 1/4096/65535 and v4−v5 bound == actual v4 sidecar_len; a 65535-bucket run streams (v4, v5) into a device sized exactly to its reservation. The tighter reserve exposed a planner bug: plan_aged_vlog_gc allocated 4 vlog segs for all-dead victims (live=0) though the budget check counts 0, starving manifest headroom at the floor (DeviceFull in file_first_floor_flush_preserves_one_run_victim_reclaim); it now allocates none and the test asserts manifest headroom remains. vals-store tests green; v4-env engine failures match HEAD baseline. Review accepted.

Commit: fix(0419): drop legacy num_buckets*4 sidecar slack from run_len_upper_bound_agg; aged vlog GC reserves no vlog segs for zero-live victims

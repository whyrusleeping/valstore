---
id: "0390"
title: 'Pre-step shard CPU growth (4.9→7.3 µs/write by 2.5B): attribute allocator/memcmp/memcpy callers with call stacks, then preregistered A/B of the top caller'
status: done
priority: 2
created: "2026-10-06"
updated: "2026-10-10"
depends_on:
    - "0385"
spec_refs: []
---

## Description

Split from 0385 (DECAY-FINDINGS.md fix #2). Before the drive step, shard CPU per synced write grows +48% (V3-R2 0.4–0.5B → 2.0–2.5B) and ACK/s falls ~30%. V4 has the same growth with flat merge bytes, and V2 shows it without recovery seeds. In the leaf perf reports, malloc/free, memcmp, memcpy and device poll make up ~56% of the sampled growth. The callers are unknown because the builds have no frame pointers. The 0382 shadow gate is not reached before 3.6B, so it is not the cause.

Steps:
1. Capture call-stack profiles (a frame-pointer or DWARF build, or a matched-cost alternative) at ~0.3B and ~2B of ingest. A retained large corpus is acceptable if it reproduces the cost. Attribute the allocator/copy/comparison growth to callers: request decode, version index, run publication/manifest, AE seed, maintenance planning, key-filter.
2. Preregister and run a short A/B against the top growing caller. Compare whole-arm distributions (~13% run-to-run variance).

Acceptance: a findings doc naming the callers responsible for most of the pre-step shard µs/write growth; one A/B with a verdict; any fix merged with evidence, or filed. Remove measured-neutral or negative paths.

## Outcome

Profiled with FP/LBR call stacks on a 1-shard file-backed valsd model at R3's per-engine state; it reproduced +26% shard µs/write growth. Top growing caller: refresh_data_reserve → one_run_victim_needed_segs → select_victims, a per-run segment hash map rebuilt on every vlog segment roll (31–44% of growth). Next: probe_version_for and KeyFilter::excludes. Fix: per-run cached victim list, charged to run metadata, with an equivalence test. Preregistered A/B over 100–175M writes: 3.156/3.145 → 2.951 µs/write (−6.3%); refresh_data_reserve 1008 → 2 cycles/write. The 1B-key rss_bench profile never runs this path; fix neutral there. No hardware arm. Follow-ups proposed: 0414, 0415. Details: bench/0390-shard-cpu/FINDINGS.md.

Commit: perf(0390): cache per-run vlog victim lists in refresh_data_reserve; shard CPU growth attribution + A/B (-6.3% us/write at 100-175M)

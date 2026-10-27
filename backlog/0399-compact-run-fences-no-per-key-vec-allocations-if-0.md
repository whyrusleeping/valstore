---
id: "0399"
title: Compact run fences (no per-key Vec allocations) if 0398 still shows EOF RAM short
status: done
priority: 3
created: "2026-10-07"
updated: "2026-10-08"
depends_on:
    - "0397"
    - "0398"
spec_refs: []
---

## Description

Split from 0397 (its "compact fences if still short" step). Do this only if 0398's hardware re-projection still leaves the 0386 EOF gate short of MemAvailable ≥ 8 GiB, or if allocator trimming is rejected on ACK tails.

## Outcome

Each run's fences are now one exact-capacity key buffer plus 16 B block records (run/fence.rs). No per-key Vecs; wire format, 32 KiB blocks and search semantics unchanged. Local 100M replay A/B: - fences .748→.637 B/key, a 0.111 GB per 1B keys cut; - native in-use −20 MB, post-trim RSS −33 MB; - untrimmed RSS not improved; - rates neutral-to-positive. Lookup and scan improved. A matched-work, interleaved merge A/B shows no regression (medians −1.1% / −0.8%, CIs include 0). Equivalence and malformed-index tests were added; vals-store tests pass. Report: bench/0399-compact-fences/REPORT.md. Hardware EOF MemAvailable has not been re-measured; that is 0386's prelaunch RAM step.

Commit: perf(0399): compact run fences — one exact-capacity key buffer + 16 B block records per run instead of a Vec per fence key; equivalence/malformed-index tests; local 100M A/B fences .748→.637 B/key, matched-work merge A/B neutral

---
id: "0214"
title: 'Cleanup Tier 4: docs staleness pass'
status: done
priority: 2
created: "2026-08-16"
updated: "2026-08-16"
depends_on: []
spec_refs: []
---

## Description
Fix confirmed-stale docs from the survey: P0 docs/formats.md vlog record layout (real: 50-byte VAL2 header with version at 30–46, CRC 46–50; cluster one-byte kind wrapper, not 13-byte VHDR); spec.md current-focus refresh; ARCHITECTURE.md + docs/algorithms.md + docs/getting-started.md in-memory key→version map descriptions → VersionIndex/dirty-bucket/winner-directory reality; hot-swap scoped wording (non-zoned library path shipped, zoned daemon offline); docs/README.md + getting-started "formats not frozen" → versioned/negotiated wording; docs/backpressure.md legacy-merge paragraph; docs/scan-performance.md completed roadmap items (0029/0005/0015); README.md stale gap list; docs/SPDK.md stale loopback "Remaining"; docs/zones.md re-home default wording; docs/blind-append-lww.md roadmap tail; optional-feature-survey.md final disposition table; docs/out-of-core-index.md opt-in wording + aged-vlog GC. Also groom memory.md entries advising removed flags, and fix scripts/provision-node.sh:111 wrong runbook path; delete scripts/strip-comments.py (one-shot, unreferenced).

## Acceptance criteria

## Work log

---
id: "0194"
title: Complete the mixed-profile SPDK soak gate for shipped deployment profiles
status: done
priority: 3
created: "2026-08-12"
updated: "2026-10-22"
depends_on:
    - "0280"
    - "0499"
spec_refs:
    - docs/optional-feature-survey.md#Blind append — keep, complete rollout decision
    - docs/optional-feature-survey.md#Compact winner directory — keep as an explicit memory/performance tier
    - docs/blind-append-lww.md
    - docs/out-of-core-index.md#Optional winner-directory performance tier
---

## Description

Current scope (2026-09-29): profile configuration, default-capacity behavior and the software harness have shipped. Only the mandatory 24-hour mixed-profile SPDK validation and its evidence disposition remain. Do not reimplement productization or repeatedly revise the harness without a demonstrated defect. The original requirements/history below remain evidence, not an instruction to redo completed steps.

Blocked on the three-voter PD topology (0280), a trustworthy fleet/health disposition (0248/0250), and explicit retention/reuse approval for the 0336 corpus or approved alternative devices. The later capacity-profile full-cache runs are not substitutes for this mixed-profile gate. Backlog acceptance does not waive these prerequisites.

### Original productization scope
Replace the ad hoc default-off positioning of the proven blind-append and compact winner-directory tiers with explicit supported deployment profiles. The capacity profile retains the out-of-core path and its lower RAM/startup cost; the latency profile enables blind unconditional writes plus the compact winner directory with explicit RAM sizing and startup semantics. Complete the blind mixed-version soak before changing any default.

Acceptance criteria:
- Specify exact profile semantics, configuration surface, compatibility/rollback constraints, RAM sizing, startup/recovery behavior, and observability.
- Run the owed long mixed-version blind-append soak with correctness, AE dirty-backlog, space, restart, and client-semantics checks.
- Expose profile selection through normal daemon configuration rather than requiring operators to combine undocumented environment variables.
- Preserve the capacity-oriented mode and prevent unsafe downgrade after unresolved blind candidates exist.
- Update getting-started/deploy/architecture docs and add config/parser/integration coverage.
- Make an explicit default-profile decision from retained evidence; do not imply the winner directory is free (62.9 RSS B/physical key and O(dataset) open rebuild).

## Outcome

The 2 h mixed-profile SPDK gate passed on lab on 2026-10-22, with valsd 41d0f973 (= 0851f65e, the 0499 fix), gc_grace 300 s and zone 17. The run had 11 restarts, including node 3 going capacity→latency→capacity, then a final rolling restart. Final exact scans matched on all 3 replicas: 1,065,626 live keys and 34,058 collected tombstones each. The in-run GC probe found all 3,648 overdue tombstone reads collected. Five omitted cold-seed winners healed, AE converged, and there were no panics, ERROR lines or cores. The harness now runs live checks: a driver GC probe, per-sample counter bounds and a space trend check, plus perf and log diagnostics, and it stops early on failure. Selftest 35/35. Evidence is in bench/0194-profile-soak (FINDINGS.md, evidence/gate-20261022T194044Z). Follow-up 0501 (proposed): AE keeps re-offering collected expired tombstones.

Commit: test(soak): mixed-profile SPDK gate passes in 2 h with in-run GC probe, live checks and diagnostics (0194)

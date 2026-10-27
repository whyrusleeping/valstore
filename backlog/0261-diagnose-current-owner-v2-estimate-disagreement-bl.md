---
id: "0261"
title: Diagnose current-owner V2 estimate disagreement blocking balance execution
status: done
priority: 3
created: "2026-09-07"
updated: "2026-09-08"
depends_on:
    - "0182"
spec_refs:
    - docs/zones.md
---

## Description

Task 0182's fresh lab RF=2 A/B found persistent small current-owner V2 range-estimate disagreement after a successful 2.4M-key load (e.g. 400018 vs 400000), causing the fail-closed planner to block all 12 ranges and admit no move. Evidence: bench/lab-balance-20260907-0182/FINDINGS.md and execute/state-blocked.txt. Investigate the estimator accounting/coherence mechanism and propose or implement a narrowly justified correction; do not simply weaken the planner safety fence.

Acceptance: reproduce and explain the discrepancy; distinguish logical data consistency from estimate accounting; add a targeted regression check for a demonstrated fix; preserve complete-poll/owner/topology validation and default-off execution. Record any required design decision explicitly before changing safety semantics. This proposed diagnostic/fix is separate from rerunning the deferred movement hardware gate.

## Outcome

Reproduced identical logical data with layout-dependent V2 estimates; added metadata-only disjoint-run extrema filtering and focused regressions. Store suite passed: 310 unit + 8 integration tests, 7 ignored; standard review accepted. Planner equality/complete-poll/owner/topology validation and default-off execution unchanged. Broader coherence remains unresolved: proposed 0284 records exact-accounting design/implementation and gates 0262. Preserved prior unaccepted 0278 code/docs in stash b68db4bc2d5b6cc268a0d75f71260604e7f445b4 with restoration instructions; retained its backlog review blockers and 0279 dependency metadata without shipping its code.

Commit: fix(store): exclude disjoint runs from range estimates; diagnose owner disagreement

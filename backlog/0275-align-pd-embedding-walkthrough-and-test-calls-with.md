---
id: "0275"
title: Align PD embedding walkthrough and test calls with validated mutation API
status: done
priority: 3
created: "2026-09-08"
updated: "2026-09-08"
depends_on:
    - "0243"
spec_refs: []
---

## Description

Small compatibility follow-through split from 0243 after its validation implementation was accepted. docs/getting-started.md still shows pre-0243 queued/void PD mutation semantics and adding a first member to an empty RF=3 daemon; this must instead show atomic bootstrap_members and Result-returning leader-only administrative calls. Existing vals-cluster PD/node test call sites discard newly returned Results, producing unused_must_use warnings.

Acceptance criteria:
- Update the existing PD walkthrough to show valid atomic membership bootstrap and accurate proposal-vs-commit, leader-only, and error handling semantics.
- Handle Results at affected existing PD/node test call sites (assert success where appropriate; explicitly ignore only where intentional).
- Relevant tests pass and no new unused_must_use warnings remain from the 0243 signature changes.
- Do not expand into a new demo or public-API redesign.

## Outcome

Updated PD embedding walkthrough for atomic RF=3 bootstrap, leader-only Result mutations, and separate commit/node-delivery observation. Asserted success at 40 affected PD/node test call sites and corrected shutdown fixture RF. Standard review accepted after fixing zone example delivery race. Verification: warning-free cargo test -p vals-cluster --no-run; 76 PD and 58 node tests passed, one unrelated ignored; git diff --check clean.

Commit: docs: align PD walkthrough and tests with validated mutation API

---
id: "0538"
title: 'Docs and repo hygiene: one quick start, external tracker, inline evidence, contributor validation text, monitoring guide, valsd --version/--help'
status: done
priority: 2
created: "2026-10-29"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description
From the 2026-10-29 project review; owner agreed to fix docs and repo hygiene.

- **One quick start.** README (~77–132) and docs/deploy.md (~54–105) carry near-identical quick starts; keep README's (fold in deploy's `ping` step if useful) and have deploy.md link to it.
- **External tracker.** CONTRIBUTING.md says open an issue while spec.md calls backlog/ the sole work index; state plainly that GitHub Issues is how outsiders report and propose work, and that spec.md/plans/backlog are internal planning notes (README doc map should label them that way).
- **Unreachable evidence citations.** About 30 references to `bench/NNNN-…/FINDINGS.md` in reference docs (algorithms.md, range-deletes.md, out-of-core-index.md, …) point at gitignored local files. Replace each with the key number/date inline (keep task ids where they help).
- **CONTRIBUTING → validation.md.** CONTRIBUTING.md and docs/ci.md send contributors to docs/validation.md, which is written for the maintainer's lab (dataset paths, memory notes, owner directions). Give contributors a short self-contained statement in CONTRIBUTING.md and keep validation.md as the maintainer's reference (move it under plans/ or label it, fixing links).
- **Monitoring.** deploy.md has no "what to watch" guidance; add a short Monitoring section listing the handful of metrics to graph/alert on (restarts/exit 75, integrity failures, quorum failures/Busy, AE backlog, disk/segment headroom, PD leader/term) with example alert expressions, using metric names verified against the code.
- **ARCHITECTURE.md.** The "Planned subsystems" section (~1011) is mostly built; retitle it and move what is not built into Known limitations.
- **valsd identification and help.** `valsd --version` and `--help` print usage and exit 2; startup logs no version; `valsd admin … help` is "unknown command". Add `--version` (crate version plus the git commit when available at build time, e.g. `option_env!` set by a build script without new dependencies), `--help`/`help` exiting 0, log the version at node/PD startup, add admin `help`; update docs/evaluation.md's "no --version flag" text.

Out of scope (decided 2026-10-29): renaming the counters that lack `_total` (owner's bench analysis scripts and retained metric snapshots use the current names); moving done backlog tasks out of backlog/ (bookkeeping tooling reads them).

Acceptance: links resolve, no doc cites a gitignored path, the new valsd flags have a test, scripts/ci.sh stays green.

## Acceptance criteria

## Work log

- 2026-10-29 (uncommitted, in the main worktree): README quick start is canonical (gained deploy's `ping` step); deploy.md "A local test cluster" points to it. CONTRIBUTING/README name GitHub Issues as the external tracker and label spec.md/plans/backlog as internal planning notes. Every `bench/…` evidence citation in README/ARCHITECTURE/docs replaced by the inline fact/date (docs/validation.md too, which also lost its dataset-path and memory-note references and is labelled the maintainer's hardware reference); CONTRIBUTING no longer links it. New deploy.md "Monitoring" section (signal table and 12 example Prometheus rules; every metric name checked against a running 3-node + PD cluster's /metrics; no leader/term metric exists, so it points at `valsd admin status`). ARCHITECTURE "Planned subsystems" → "Subsystems beyond the core data path"; EC and auto-eviction moved to Known limitations. valsd: `--version`/`version` (crate version, commit from build.rs, features) exit 0; `--help`/`-h`/`help` and `<subcommand> --help` exit 0; no args and unknown flags still exit 2; version logged at node/PD startup; admin `help` (one shared command list, also used by the unknown-command error); tests in crates/vals-server/tests/cli.rs; docs/evaluation.md updated. A markdown link/anchor check over README, CONTRIBUTING, ARCHITECTURE, SECURITY and docs/ found no problems.
- 2026-10-29 review (opus, 2 rounds, approve): links (412) and anchors resolve; no gitignored or private references; sampled inline evidence matches its sources. Fixed from round 1: the hint metrics are client-side (always 0 on valsd), so the Monitoring table points at `vals_client::observe::snapshot()` and the hint alert is replaced by `ValsRegionExhausted` (`vals_region_exhausted_rejections`); `ValsPdStateNearCapacity` is guarded so it doesn't fire on data nodes; `vals_snapshot_errors` added to the durable-write alert; docs/ci.md, optional-feature-survey (private repo name), evaluation, deploy CLI line, the R17 review row and stale validation.md R3 text updated. build.rs no longer watches missing paths (a missing packed-refs relinked valsd on every build). `scripts/ci.sh` green.

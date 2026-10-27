---
id: "0024"
title: 'docs/SPDK.md: pin a known-good SPDK tag + build prerequisites'
status: done
priority: 4
created: "2026-07-02"
updated: "2026-07-04"
depends_on: []
spec_refs: []
---

## Description
BACKLOG.md §Productization (SPDK follow-up): the shim build vs SPDK v24.09 was fixed (opts_size), but `docs/SPDK.md` still doesn't pin a known-good SPDK tag; the box needed a v24.09 clone + `meson`/`ninja`/`pyelftools` that `scripts/pkgdep.sh` did not pull. Docs-only task.

## Acceptance criteria
- `docs/SPDK.md` names a pinned known-good tag (v24.09) and the extra build prerequisites, with the from-scratch build steps verified against the doc.

## Acceptance criteria

## Plan

Docs-only task: update docs/SPDK.md to pin SPDK v24.09 as the known-good tag and document the extra build prerequisites the box needed, then verify the documented from-scratch steps as far as this environment allows.

1. Edit docs/SPDK.md §"Building SPDK":
   - State that v24.09 is the pinned known-good tag (verified on the oldbox box, 2026-06-18, incl. the opts_size shim fix which is harmless on older SPDK).
   - Give explicit from-scratch steps: `git clone --branch v24.09 https://github.com/spdk/spdk` (or `git -C third_party/spdk checkout v24.09`), `git submodule update --init` (dpdk at minimum), then configure/make as already documented.
   - Document the prerequisites that `scripts/pkgdep.sh` does NOT pull: meson, ninja, python3 pyelftools (needed by the vendored DPDK build), plus the already-noted jinja2 + tabulate (genrpc.py) and optional nasm.
   - Keep the existing configure flags (`--without-crypto --without-vhost --without-virtio --disable-tests --disable-unit-tests --disable-examples --disable-apps`).
2. Verify: attempt the documented from-scratch build in a scratch dir (clone v24.09 using the local vendored repo as reference to save bandwidth, submodules included, configure + make with nohup/background polling since each shell call caps at 2 min; 24 cores available). If the build fails purely because of the modern local toolchain (gcc 15 vs a Sep-2024 release, likely -Werror in DPDK), record how far it got (configure success, prereq checks exercised) and rely on the recorded on-box verification (BACKLOG.md, 2026-06-18, oldbox v24.09); optionally note a workaround in the doc if it is simple and known-good.
3. No code changes; confirm the repo still builds untouched (no-op) and the doc renders sensibly.

### Starting points
- docs/SPDK.md §'Building SPDK (no NVMe required to build)' — the section to extend
- BACKLOG.md lines ~634-641: the opts_size fix entry + 'Still TODO: pin a known-good SPDK tag… meson/ninja/pyelftools that scripts/pkgdep.sh did not pull'
- third_party/spdk is a git checkout at v26.09-pre with tag v24.09 present locally; use `git clone --reference third_party/spdk` for a cheap scratch clone
- local toolchain: gcc 15.2, meson 1.10.2, ninja 1.13.2, python 3.14 with pyelftools/jinja2/tabulate installed; 24 cores
- Bash calls time out at 2 min — run long builds with nohup … & and poll the log

## Work log
- 2026-07-04 plan: Docs-only task: update docs/SPDK.md to pin SPDK v24.09 as the known-good tag and document the extra build prerequisites the box needed, then verify the documented from-scratch steps as far as this env
…[truncated]
- 2026-07-04 context hints: 5 recorded with plan
- 2026-07-04 context hints: docs/SPDK.md — §'Building SPDK (no NVMe required to build)' is the section to extend (lines ~67-77); BACKLOG.md lines ~634-641: opts_size fix entry + 'Still TODO: pin a known-good SPDK tag … meso
…[truncated]
- 2026-07-04 implementer report: Docs-only task complete: pinned SPDK v24.09 as the known-good tag in docs/SPDK.md and documented the build prerequisites scripts/pkgdep.sh misses.  Changes: - docs/SPDK.md §"Building SPDK (no NVMe re
…[truncated]
- 2026-07-04 review tier: simple (coordinator self-review)
- 2026-07-04 decision: accept — commit: docs(spdk): pin known-good SPDK tag v24.09 + build prerequisites pkgdep.sh misses (meson/ninja/pyelftools); from-scratch build steps re-verified
- 2026-07-04 usage: 10,311 tok (in 42, out 10,269, cache_r 341,660, cache_w 50,974) · cost n/a (unpriced)
  coordinator: 5,340 tok (in 14, out 5,326, cache_r 162,644, cache_w 35,426) · cost n/a (unpriced)
  implementer: 4,971 tok (in 28, out 4,943, cache_r 179,016, cache_w 15,548) · cost n/a (unpriced)

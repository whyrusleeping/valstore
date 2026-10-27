---
id: "0037"
title: Clean up remaining warn-level clippy lints under pinned 1.96 toolchain
status: done
priority: 5
created: "2026-07-04"
updated: "2026-07-04"
depends_on: []
spec_refs: []
---

## Description
Task 0031 pinned rust-toolchain.toml to 1.96.0 and made the workspace clippy-clean at deny level (exit 0), but ~33 pre-existing warn-level lints remain (e.g. clippy::type_complexity, useless_vec, redundant_closure, len_zero at vals-cluster replica.rs:2032, clone_on_copy at replica.rs:2831, useless_vec at drive.rs:1064). Sweep them: fix where trivially safe, or add targeted, justified `#[allow]`s where the lint is wrong for the code.

## Acceptance criteria
- `cargo clippy --workspace --all-targets` emits zero warnings under the pinned toolchain.
- No behavior changes; full workspace test suite stays green.

## Acceptance criteria

## Plan

Sweep the ~30 remaining warn-level clippy lint sites so `cargo clippy --workspace --all-targets` is warning-free under the pinned 1.96.0 toolchain, with zero behavior change.

Approach per lint class:
1. Trivially-safe mechanical fixes (apply the clippy suggestion directly):
   - `useless_vec` (8 sites, mostly tests): `&vec![...]` → slice literal.
   - `redundant_closure` (2): pass the function/constructor directly.
   - `len_zero` (replica.rs:2831): `len() > 0` → `!is_empty()`.
   - `clone_on_copy` (replica.rs:3878): drop `.clone()` on `Copy` Config.
   - `needless_borrow` / `explicit_auto_deref` / `unnecessary_to_vec`-style: apply suggestion.
   - `question_mark` (match replaceable with `?`): apply only if it doesn't change error mapping/readability; otherwise targeted allow with a comment.
   - `needless_range_loop` (loop var `s` only indexes `nodes`): rewrite with iterator (`.iter().enumerate()` or `iter_mut`) if it stays clear; else targeted allow.
2. Judgement lints — prefer targeted, justified `#[allow]` where refactoring adds noise:
   - `type_complexity` (12 sites): introduce a small local `type` alias where a signature repeats or the alias genuinely clarifies (e.g. the repeated Result tuple in engine.rs); otherwise `#[allow(clippy::type_complexity)]` on the item with a brief justification comment (test-local Arc<Mutex<Option<...>>> capture cells are a classic case).
   - `result_large_err` (2 sites): do NOT box the error type (behavior/API change risk); add `#[allow(clippy::result_large_err)]` with a comment explaining the variant size is accepted.
3. Verification:
   - `cargo clippy --workspace --all-targets` → zero warnings.
   - `cargo fmt --check` clean.
   - `cargo build --workspace` and full `cargo test --workspace` green (behavior-neutral change).
No `#![allow]` at crate level; all allows must be item-scoped with a one-line justification.

### Starting points
- Warning sites (cargo clippy --workspace --all-targets): crates/vals-cluster/src/{pd.rs:1993,2201; replica.rs:1715,2831,3878; drive.rs:1423,1555; maint.rs:231,274; net/server.rs:1732}
- crates/vals-store/src/engine.rs: 1381,1774,1818,1955 (type_complexity fn sigs), 4640,5085,7615-7628,7885,7899,8781,8906; crates/vals-store/src/{dump.rs:709, index.rs:266}
- crates/vals-client/src/wire.rs:731,1094
- Lint classes: 12x type_complexity, 8x useless_vec, 2x result_large_err, 2x redundant_closure, 1 each: clone_on_copy, unnecessary to_vec, question_mark, needless_borrow, needless_range_loop, len_zero, explicit auto-deref
- rust-toolchain.toml pins 1.96.0 — run clippy/fmt/test with the pinned toolchain

## Work log
- 2026-07-04 plan: Sweep the ~30 remaining warn-level clippy lint sites so `cargo clippy --workspace --all-targets` is warning-free under the pinned 1.96.0 toolchain, with zero behavior change.  Approach per lint class:
…[truncated]
- 2026-07-04 context hints: 5 recorded with plan
- 2026-07-04 context hints: Warning sites (cargo clippy --workspace --all-targets): crates/vals-cluster/src/{pd.rs:1993,2201; replica.rs:1715,2831,3878; drive.rs:1423,1555; maint.rs:231,274; net/server.rs:1732}; crates/vals-stor
…[truncated]
- 2026-07-04 implementer report: Swept all ~33 remaining warn-level clippy lints so `cargo clippy --workspace --all-targets` now emits zero warnings under the pinned 1.96.0 toolchain, with no behavior changes.  Changes by lint class:
…[truncated]
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change cleanly sweeps the remaining warn-level clippy lints: I confirmed `cargo clippy --workspace --all-targets` (including a from-clean rebuild of vals-store) emits zero warnings under the pinne
…[truncated]
- 2026-07-04 decision: accept — commit: chore(lint): sweep remaining warn-level clippy lints to zero under pinned 1.96 toolchain  Type aliases for repeated complex signatures (VersionDigestPage, VersionedScanPage, WinnersPage, ResolvedItems
…[truncated]
- 2026-07-04 usage: 30,326 tok (in 186, out 30,140, cache_r 2,091,532, cache_w 102,519) · cost n/a (unpriced)
  implementer: 16,708 tok (in 112, out 16,596, cache_r 1,363,918, cache_w 36,659) · cost n/a (unpriced)
  reviewer:Claude: 7,394 tok (in 42, out 7,352, cache_r 308,826, cache_w 25,884) · cost n/a (unpriced)
  coordinator: 6,224 tok (in 32, out 6,192, cache_r 418,788, cache_w 39,976) · cost n/a (unpriced)

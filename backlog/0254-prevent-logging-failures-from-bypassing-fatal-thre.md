---
id: "0254"
title: Prevent logging failures from bypassing fatal-thread abort and leaving a shardless live node
status: done
priority: 1
created: "2026-09-05"
updated: "2026-09-05"
depends_on: []
spec_refs: []
---

## Description
0248 rig Sep5: lab3 crash core filled filesystem holding stderr/node.log. Restarted pid1630480 had ZERO vals-shard threads but metrics/admin/maint/DPDK/main alive; all four listen queues129/128; log ends midline at13:04 until parent stop13:56. Restart after freeing disk restored all4 shards. A second-order availability failure, independent of original abort attribution.

Source defects: StderrLogger::log (vals-client/src/observe.rs) uses eprintln!, which panics on stderr write failure. spawn_abort_on_panic catches a worker panic then calls log::error! BEFORE process::abort(); if logger panics again, abort is skipped and worker dies silently. Main never supervises shard joins; executor Rc/spawner cycles can keep listeners bound after unwind. ENOSPC mechanism strongly supported, not dynamically reproduced yet.

Minimal fix scope: best-effort non-panicking stderr writes; fatal-thread catch path must abort even with a panicking logger or failed stderr. Add isolated subprocess tests using /dev/full and a deliberately panicking logger: ordinary logging must not kill shards, while an actual fatal-worker panic must terminate the process rather than leave it alive. Preserve original panic diagnostics where possible; avoid abort handler depending on log locks. Explicitly distinguish normal log failure (continue) from worker panic (abort).

Follow-up, not automatically folded into this minimal fix: supervise unexpected shard exit/live-shard metric; executor cycle cleanup; reactor fatal-error semantics. Do not use blanket reactor/executor refactors as a prerequisite. Evidence task0248 and bench/0248-takeover-20260905.

## Acceptance criteria

- Built-in stderr logging ignores write errors without unwinding the worker; successful output retains its format.
- Caught worker panics abort without invoking the logger or dropping the panic payload; failed stderr and a deliberately panicking logger cannot bypass abort.
- Isolated Linux subprocess tests verify continued execution for ordinary `/dev/full` logging and SIGABRT for worker panics. Healthy stderr retains the original panic message and thread name.
- Regression tests fail when their corresponding fixes are reverted; no shard supervision or executor/reactor refactor is required.

## Work log

- Replaced `eprintln!` with best-effort `writeln!` to locked stderr. Removed the secondary fatal `log::error!` entirely: the existing panic hook emits original diagnostics, and the catch branch immediately aborts without logging locks.
- Held the caught payload until abort. The previous `.is_err()` also dropped the payload before abort; a custom panicking destructor could unwind out of the wrapper. Added a regression for this related bypass.
- Expanded `abort_on_panic` to eight parent scenarios plus one isolated child entry point: healthy logging/format, ordinary ENOSPC logging, normal worker panic/diagnostics, worker panic with ENOSPC, panicking logger installed, panic originating in logger, logging disabled, and panicking payload destructor. Children disable core dumps and have a 10-second parent-enforced timeout.
- Targeted suite initially passed (9/9). Kill-the-fix: reverting only stderr handling makes `ordinary_stderr_logging_survives_enospc` fail by SIGABRT. Restoring stderr handling and reverting only the fatal wrapper makes both panicking-logger tests and the payload test fail (exit 101 instead of SIGABRT). Both fixes restored.
- Read-only independent review found no correctness blocker. Documented the remaining pre-catch limitation: a blocked/non-returning panic hook can prevent unwind from reaching the abort handler; no new log-error metric or thread supervision was added. `--nocapture` in child invocations is essential so libtest does not divert panic diagnostics away from `/dev/full`.
- Validation: `cargo build --workspace` passed; `cargo test --workspace -- --test-threads=1` passed in full. Parallel workspace attempts hit `cold_overwrites_count_probe_runs_and_blocks` (process-wide counter delta mismatch) and `three_node_cluster_with_pd_serves_zones_and_survives_restart` (membership reply contained only `ok`); both passed on exact isolated reruns and in the full serial run. `cargo clippy --workspace` passed with existing warnings outside changed code. `git diff --check` and targeted test-file `rustfmt --check` passed; no repository-wide formatting changes made.
- Updated `ARCHITECTURE.md` operability contract. Local code/tests complete; no fleet deployment or reproduction of the full lab3 incident was attempted.

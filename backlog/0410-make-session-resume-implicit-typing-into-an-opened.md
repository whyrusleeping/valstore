---
id: "0410"
title: 'Make session resume implicit: typing into an opened session auto-resumes it'
status: done
priority: 3
created: "2026-10-09"
updated: "2026-10-14"
depends_on: []
spec_refs: []
---

## Description
"Resuming" a session is currently a separate, user-facing step. It shouldn't be. Resuming is a backend detail. When the user opens an existing session and starts typing, it should resume without any extra step.

Scope: the session tooling/UI (ycc sessions, `.ycc/sessions/`, `session-view.sqlite`). This is not about valstore storage or soak-harness resume (0403/0408).

## Acceptance criteria
- Opening an existing session and sending a message resumes it automatically. There is no separate "resume" action, button, command or prompt.
- No user-facing text, buttons or commands mention "resume" or "resuming" as a step the user has to take. Status messages may still mention it if that helps.
- When the backend can't resume a session (missing state, process gone, etc.), it handles that without the user's involvement: it reattaches or restarts using the saved context and shows a clear message only if that fails.
- Any existing explicit resume entry point is removed, or kept only as an internal/back-compat alias.
- Tests cover: open an idle or stopped session → type → the message is delivered and the session continues, with no manual resume.

## Work log

- 2026-10-14: moved to the ycc backlog at the owner's request, as ycc task 0464 (`~/code/ycc` commit 279b018). Closed here; no valstore work.

- 2026-10-11: blocked, because this task is in the wrong repository. All of its scope is ycc session tooling, which lives in `~/code/ycc`: a separate git repo with its own backlog (~458 tasks) and dirty files from live sessions. valstore sessions can't edit or commit there. The task landed in this backlog through commit 28b31f69, which swept it in. **To unblock:** file it in the ycc backlog and close it here. Or, if the owner wants it done from valstore sessions, say so and grant write and commit access to `~/code/ycc`.

# Committing in a shared dirty worktree (agent tool workarounds)

Several sessions leave uncommitted work in the main worktree at once. These workarounds for
the agent `commit`, review and spawn tools were moved here from memory.md on 2026-10-29. They
are observations from past sessions, so check `git show --stat HEAD` after every commit.

## What the tools take

- **Scope.** Review and commit take only files whose content this session changed (Edit or
  Write) since its start baseline. Add-then-revert and shell-only (python3) edits don't
  count: `commit` once dropped a clean tracked file edited only with python3. Edit every file
  you mean to commit with the Edit tool at least once.
- **Whole files.** `commit` sweeps in whole dirty or untracked files, including every dirty
  `backlog/*.md` (another session's modified and new task files). Run
  `git status --short -- <file>` first. Dirty crate files may be another session's A/B (check
  `bench/*/FINDINGS.md`). Never stash.
- **Limits.** The commit tool rejects an outcome over 1000 characters or a subject over 240.
- **Workers** never commit, merge or switch branch in the main worktree (box builds use a
  separate worktree). About two big review rounds exhaust a worker's context.
- **spawn_implementer** needs memory.md as it was at session start: remove remember notes newer
  than `session_started`, spawn, and restore them after `commit`.

## Recipes

- **Other sessions' backlog files.** Before commit, copy them to /tmp, `git checkout --` the
  modified ones and move the untracked ones aside; afterwards copy them back.
- **Another task's hunks in your file.** Remove those hunks with Edit before
  spawn_reviewers/commit, then reapply them with python3 after commit. The commit holds only
  your change and the worktree keeps the other task's hunks.
- **Inherited uncommitted task work** is invisible to reviewers. For one or two files make a
  small real Edit. For many files mixed with other tasks' hunks: build the task-only tree with
  a temp `GIT_INDEX_FILE` (read-tree HEAD, add the task files, write-tree), check it with
  `scripts/ci.sh` in a separate `git worktree`, commit it with `git commit-tree` +
  `git update-ref HEAD` + `git reset -q` (the worktree keeps the other hunks), then re-review
  with a handoff pointing at that commit and close with the commit tool.

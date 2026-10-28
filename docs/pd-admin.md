# PD administration

Each PD replica has a **host-local** admin Unix socket (`admin_socket` in its PD
config). `valsd admin -s <socket> <command>` connects to only that replica;
there is no redirect or automatic multi-replica retry. The command list is in
[`deploy.md`](deploy.md#administering-a-running-cluster); this page explains
how to read the replies and what to do when a mutation's outcome is unclear.

Mutations are accepted only by the current, caught-up leader. Query `status`
on the available hosts and locate the current `role=leader`, noting its `pd`
id and `term`. A follower's `leader` is its last known leader, not a lease: if a
mutation says `not leader` or `not ready`, check status again and direct the
command to the current leader's socket. Do not assume a disconnected socket
means the replica or its proposals are gone.

Mutation replies start with one of:

- `ok committed index=N term=T`: the direct membership/zone mutation was
  observed committed **and applied** on the proposing PD. Exit code 0.
- `ok queued, NOT committed`: a split/merge was accepted into leader-local
  memory, not committed. Confirm its result using `ranges <zone>` on an
  authoritative leader. Exit code 0 does **not** mean the map changed.
- `ok committed ...; initial range map queued, NOT committed`: for
  `create-zone ... presplit=...`, only the zone creation is confirmed; the
  range-map installation remains queued only after the original creation is
  observed committed. If leadership changed before confirmation, the reply may
  instead say `initial range map NOT queued`; inspect `ranges <zone>` before
  using the intended range layout. An `unknown:` creation may later commit, but
  its installation is not authorized until that exact creation is observed.
- `err: ...`: no proposal was admitted (or a **different committed entry**
  occupied this proposal's slot). Validation, follower and not-ready errors
  are safe to reconsider; `timed out in PD queue before proposal` also means
  the drive loop did not propose this command. A locally overwritten but
  uncommitted log slot is **not** enough to prove supersession. Exit code 1.
- `unknown: commit not observed index=N term=T`: the proposal was admitted,
  but commitment could not be confirmed before the bounded wait. Exit code 3.
  **Never blindly replay it.** A quorum may still commit that entry after the
  timeout, or the log slot may be replaced. Check `status` on each replica,
  find the current leader, then use `members`, `zones`, or `ranges <zone>` as
  appropriate; retry inspection until its first line says `ok authoritative`.
  Reconcile the observed state against the intended mutation before deciding
  whether a *new* proposal is warranted. If the socket fails after sending a
  request without a reply (including `unknown: no reply from PD drive loop`),
  treat the outcome as ambiguous too.

`members` lists the committed membership and `zones` the committed catalog.
`ranges <zone>` lists committed boundary keys as `hex=<bytes>` (an empty body
means one range). The first line of each is either `ok authoritative pd=... term=...
index=...` (leader ReadIndex quorum confirmation, applied through that index)
or `ok non-authoritative ... (may lag; not confirmation)`. A follower is
always non-authoritative, even if it appears caught up. A partitioned former
leader cannot supply an authoritative inspection; wait for a quorum-backed
leader. `status` is an immediate **local** view with `pd`, `role`, `term`,
`leader`, `commit`, `applied`, `caught_up`, `incarnation` (the group's
32-hex-digit cluster incarnation id, `-` until its first leader has committed
one), and `voters` and `learners` (this replica's applied PD raft membership,
comma-separated ids or `-`), not a confirmation of leadership.
A timeout or unavailable socket is not evidence of non-application.

`export-state <absolute-path>` writes the complete PD Raft state machine to a
new file at that path on the PD host. The PD process writes the file, and it
refuses an existing path. Like an authoritative inspection, it requires a
leader confirmed by a ReadIndex quorum. The file is the `pd-state` layout of
[`formats.md`](formats.md) §3.7, captured at the leader's applied index. On
success the reply is `ok authoritative pd=... term=... index=... entries=...
bytes=... incarnation=...`. A follower or an unconfirmed leader replies with an
`err:` and writes nothing. `valsd backup create`, `load` and `compare` use this
command (see [`backup-restore.md`](backup-restore.md)). It is read-only: it proposes nothing.

`pd-add-learner <id> <addr>`, `pd-replace <old> <new>` and
`pd-remove-learner <id>` change the PD raft group itself (the runbook is
[`deploy.md`](deploy.md#replacing-a-lost-pd-voter)). Like other mutations they
need the caught-up leader and reply `ok committed`, `err:` or `unknown:`. A
committed change is checked against the leader's applied membership, and the
reply ends with the resulting `voters=... learners=...`. `ok unchanged: already
in effect` means the same learner (id and address) already exists or was
already removed. `pd-replace` commits up to two changes (remove the old voter,
then promote the learner) and reports each one. If it stops with `err:` or
`unknown:` after the first, the reply names the committed step and says to rerun
the same command, which continues from the committed membership. While a
replacement is half done, `pd-replace <old> <other learner>` promotes another
caught-up learner instead, and `pd-remove-learner` refuses the learner the
replacement chose until then. A finished
replacement replies `ok pd <old> replaced by pd <new> (already done)`.

This workflow concerns **PD Raft state only**. Data nodes are fenced from a
stale PD group by the cluster incarnation id; see
[`deploy.md`](deploy.md#pd-availability-and-recovery) for what it fences and
what still needs external fencing.

---
id: "0282"
title: Add leader-aware and commit-aware PD administration
status: done
priority: 3
created: "2026-09-08"
updated: "2026-09-29"
depends_on:
    - "0112"
spec_refs:
    - docs/deploy.md#Administering a running cluster
---

## Description

0112 identifies current host-local admin sockets, follower retry errors, no leader discovery/redirect, and `ok proposed` rather than commit confirmation. Provide a bounded multi-replica operator workflow that locates the leader and handles ambiguous outcomes safely.

Acceptance: document and test leader changes, follower targeting, quorum loss and ambiguous retries; distinguish proposal admission from observed commitment; prevent stale follower inspection from being represented as authoritative confirmation; avoid unsafe automatic replay of non-idempotent mutations. Do not imply this provides cross-group fencing.

## Outcome

PD admin locates the leader (`status`; not-leader errors name it) and separates admission from commitment: direct mutations reply `ok committed` only when their exact (index, term) entry applies, `err: not committed` only when a different entry commits in that slot, `unknown:` on timeout (never re-proposed). Split/merge reply `ok queued, NOT committed`; presplit map installs are bound to their create entry, CAS-guarded in Raft, and stale ones discarded without blocking the queue. members/zones/ranges first line is `ok authoritative` (leader ReadIndex) or `ok non-authoritative`; bodies unchanged. CLI exits 0/1/3. Workflow in docs/pd-admin.md (not cross-group fencing). 3-PD harness tests cover follower targeting, leader change, quorum loss, supersession, compaction, stale reads. Build, vals-raft/cluster/server debug suites, clippy pass; standard review accepted after 3 rounds. deploy.md/spec.md update deferred to 0345 (owner's 0338 edits).

Commit: pd(0282): leader-aware, commit-aware PD admin (status/ranges, committed|queued|err|unknown replies, exit 0/1/3, ReadIndex inspection, presplit installs bound to create entry); docs/pd-admin.md

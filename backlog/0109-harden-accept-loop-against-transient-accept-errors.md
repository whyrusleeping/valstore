---
id: "0109"
title: Harden accept_loop against transient accept() errors (don't let a poller error silently kill a shard listener)
status: done
priority: 2
created: "2026-07-31"
updated: "2026-08-01"
depends_on:
    - "0107"
spec_refs:
    - docs/backpressure.md#Backpressure
---

## Description
## Motivation
Autopsy of the 2026-07-31 bootes soak (task 0107): a node's terminal state was a ~5 MB process "husk" with **all four shard listeners (7001-7004) and the metrics endpoint (9464) refusing connections**, the log frozen mid-spam, no panic line, no OOM (dmesg clean). The `accept_loop` in `crates/vals-cluster/src/net/server.rs` treats **any** `accept()` error as permanent:

```
Err(_) => { reactor::deregister(fd); return; }
```

A transient/resource error (`EMFILE`/`ENOMEM` under a hit `RLIMIT_NOFILE`/cgroup memory cap, or `ECONNABORTED`) therefore **permanently** kills that shard's listener while the executor thread keeps polling (process lingers as a husk). Under a resource pinch every shard + the metrics acceptor hit it near-simultaneously → the observed all-listeners-refused husk. 0107 removes the first domino (space exhaustion no longer returns a hard error that trips the failure-detector cascade), but this latent accept-loop fragility remains an independent single-node availability bug.

## Acceptance
- `accept_loop` distinguishes **transient** accept errors (`EMFILE`, `ENFILE`, `ENOMEM`, `ENOBUFS`, `ECONNABORTED`, `EINTR`/`Interrupted`, `EWOULDBLOCK`) from **fatal** ones (e.g. `EBADF` / listener genuinely gone) and does **not** permanently exit the listener on a transient error — it logs (rate-limited) and retries, with a small backoff so it doesn't hot-spin while fds are exhausted.
- A shard whose `accept()` transiently fails **recovers** and resumes serving once the pressure clears, rather than staying a silent refuser for the process lifetime.
- Metrics acceptor gets the same treatment (or a shared helper).
- Test: inject/simulate a transient accept error and assert the loop keeps running and later accepts a real connection (kill-the-fix: reverting to `return` fails it).
- Graceful-shutdown behavior (the `is_shutdown()` fast path and the loopback self-connect poke) is preserved unchanged.

## Notes
Relates to task 0107 (space-exhaustion fault isolation) — this is the "husk amplifier" half. See 0107's plan/work log for the full root-cause chain.

## Acceptance criteria

## Work log

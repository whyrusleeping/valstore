---
id: "0553"
title: Own and close each thread-local reactor's epoll/kqueue descriptor
status: done
priority: 3
created: "2026-11-02"
updated: "2026-11-02"
depends_on: []
spec_refs: []
---

## Description

0548: vals-client/src/reactor.rs:17-34 stores poller as RawFd with no Drop implementation. sys::new_poller allocates epoll/kqueue. Thread exit drops waker map but leaks poller FD. Independent local probe spawning/joining 32 threads that call reactor::poll_io_nonblocking increased /proc/self/fd count from 4 to 36 (one leaked FD/thread). Suggested acceptance: own with OwnedFd (or equivalent correct Drop), handle creation failure rather than storing invalid descriptor, regression checking bounded descriptor count across repeated thread lifecycles on supported platforms. This affects thread churn/embedded clients; fixed long-lived shard threads don't grow it per request.

## Outcome

Reactor.poller is an OwnedFd (closed with the thread-local on thread exit); new_poller returns io::Result on Linux and macOS and with_reactor panics with the error rather than keeping an invalid fd; poll_io no longer creates a reactor when nothing is registered. Regression tests/reactor_fds.rs (fds 5->37 without fix, bounded with it); vals-client and vals-cluster tests and clippy pass; macOS branch inspected only.

Commit: vals-client reactor: own the per-thread epoll/kqueue poller as OwnedFd so thread exit closes it, panic on creation failure instead of storing -1, and don't create a poller on idle poll_io (0553); 32 thread lifecycles no longer leak 32 fds

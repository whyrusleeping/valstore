---
id: "0076"
title: Node admin socket serializes connections — ping queues behind an in-flight dump
status: done
priority: 4
created: "2026-07-06"
updated: "2026-07-06"
depends_on: []
spec_refs:
    - Current focus areas
---

## Description
Observed on the box during task 0044: with a live node-admin `dump` in flight (minutes-long on a large zone), a `ping` on the same admin socket only replies after the dump completes. Task 0043's design intent was the opposite ("the dump is dispatched on its own thread ... so a long dump never blocks ping/signal handling"). The serving TCP data path is unaffected — this is admin-socket-only. Likely cause: the admin accept loop (`crates/vals-server/src/admin.rs` `spawn_listener`) or the node_main command loop serves one connection/command fully before the next, so the blocking dump reply holds the queue. `docs/deploy.md` currently documents the serialization as a caveat.

## Acceptance criteria
- A `ping` (or any admin command) issued while a dump is in flight answers promptly (in-repo test on the file backend: start a live admin dump of a non-trivial zone, concurrently ping, assert reply before dump completion).
- The dump reply still arrives correctly on completion; concurrent dumps either work or are cleanly rejected (documented either way).
- Remove/adjust the `docs/deploy.md` serialization caveat.

## Plan

Root cause (verified by reading the code): `crates/vals-server/src/admin.rs` `spawn_listener` accepts one connection at a time and BLOCKS on `reply_rx.recv()` before accepting the next. Node-admin `dump` correctly runs on its own thread (node_admin.rs dispatches, shard ctl_loop spawns a per-dump `vals-dump` thread), but its `reply_tx` only fires when the dump completes — so the accept loop is held for the whole dump and a concurrent `ping` queues in the listener backlog. Fix the listener, not the dump path.

Implementation:
1. admin.rs `spawn_listener`: keep the accept loop accept-only; serve each accepted connection on its own named thread (`std::thread::Builder::new().name("vals-admin-conn")`), which does: read the command line (move the `read_line` into the connection thread too, so a slow client can't hold the accept loop either), clone-send `(line, reply_tx)` into the main-loop channel, block on `reply_rx.recv()` in the connection thread, write the reply, close. If `tx.send` fails (main loop gone / shutting down) just close the connection unanswered. Reword the fallback reply "err: pd shut down" to daemon-neutral wording (e.g. "err: daemon shut down") since node_main reuses this listener (no other code/tests reference that string). Update the module/function doc comments: one-command-per-connection protocol unchanged; connections are now served concurrently; commands still execute serially in the daemon's main loop (each command's execution is quick — long work like dump is dispatched to its own thread by the node dispatcher).
2. Update the stale doc claims: node_admin.rs / main.rs comments are already consistent ("dump never blocks ping") — verify and leave or touch up as needed.
3. docs/deploy.md (~line 308-312): replace the "admin socket serializes" caveat with the new behavior: a `ping` (or any admin command) issued mid-dump answers promptly; concurrent dumps to different output paths run concurrently (each dump gets its own thread); the serving data path was and remains unaffected.
4. Test in `crates/vals-server/tests/live_dump.rs` (reuse its harness: bring_up, admin(), parse_vdump, replay, eventually): new #[test] `ping_answers_while_dump_in_flight` (own temp dir/ports/sockets so it can run in parallel with the existing test):
   - Boot the single-node file-backed cluster, write a non-trivial corpus (reuse the write loop, a few hundred records is enough).
   - First do a plain dump to a regular file with `eventually` until the replayed state matches expected — this both proves the corpus is flushed and gives a baseline.
   - Create a FIFO with `libc::mkfifo` (libc is already a test dep) as the dump output path. `emit_dump` uses `File::create(out)`, which on a FIFO blocks until a reader opens — a deterministic, sleep-free "dump in flight".
   - Open a UnixStream, send `dump <zone> <fifo-path>`, do NOT read the reply yet (dump thread now blocked opening the FIFO; before the fix, this connection would hold the accept loop forever → the old code would deadlock this test, proving the test bites).
   - While the dump is in flight: (a) `admin(ping)` must reply "ok" — with a read timeout (e.g. set_read_timeout ~10s) so a regression fails fast instead of hanging; (b) issue a second `dump <zone> <regular-file2>` and assert it completes "ok " — pins that concurrent dumps work.
   - Then open the FIFO for read, drain to EOF (writer closes at dump end), then read the first connection's reply → must start with "ok ". Parse the drained bytes: header zone name matches, last-wins replay matches expected (zone quiesced since the baseline dump, so deterministic).
   - Structural ordering (ping answered before the FIFO was drained) makes the assertion race-free without sleeps.
5. Verify: `cargo build` + run the live_dump test suite (`cargo test -p vals-server --test live_dump`), plus `cargo test -p vals-server` for the daemon/dump suites touching the admin socket. Also confirm existing PD-admin users (run_pd.rs main loop) are unaffected — they only consume the channel.

Acceptance mapping: prompt ping mid-dump → test (a); dump reply still correct on completion → FIFO drain + reply check; concurrent dumps work (documented) → test (b) + deploy.md wording; deploy.md caveat removed → step 3.

### Starting points
- crates/vals-server/src/admin.rs:42-72 spawn_listener — the blocking reply_rx.recv() in the accept loop is the bug
- crates/vals-server/src/node_admin.rs dispatch — already thread-per-dump, don't change
- crates/vals-cluster/src/node.rs:1475 start_dump — spawns one vals-dump thread per request (concurrent dumps already OK)
- crates/vals-cluster/src/dump.rs:119 emit_dump uses File::create(out) — blocks on a FIFO until a reader opens (test lever)
- crates/vals-server/tests/live_dump.rs — harness to reuse: bring_up, admin(), parse_vdump, replay, eventually
- docs/deploy.md ~line 308-312 — the 'admin socket serializes' caveat to replace

## Work log
- 2026-07-06 plan: Root cause (verified by reading the code): `crates/vals-server/src/admin.rs` `spawn_listener` accepts one connection at a time and BLOCKS on `reply_rx.recv()` before accepting the next. Node-admin `du
…[truncated]
- 2026-07-06 context hints: 6 recorded with plan
- 2026-07-06 context hints: crates/vals-server/src/admin.rs:42-72 spawn_listener — the blocking reply_rx.recv() in the accept loop is the bug; crates/vals-server/src/node_admin.rs dispatch — already thread-per-dump; don't ch
…[truncated]
- 2026-07-06 implementer report: Fixed task 0076: the node/PD admin socket no longer serializes connections behind an in-flight dump.  ## Root cause (confirmed) `crates/vals-server/src/admin.rs` `spawn_listener`'s accept loop served 
…[truncated]
- 2026-07-06 review tier: single-opus — reviewers: Claude
- 2026-07-06 review (Claude): accept — The change correctly fixes the admin-socket serialization bug per task 0076. The accept loop in `spawn_listener` is now accept-only, delegating each connection to a short-lived `vals-admin-conn` threa
…[truncated]
- 2026-07-06 decision: accept — commit: server: serve admin-socket connections concurrently — ping no longer queues behind an in-flight dump (task 0076)  spawn_listener's accept loop blocked on reply_rx.recv() per connection, so a minutes
…[truncated]
- 2026-07-06 usage: 20,610 tok (in 92, out 20,518, cache_r 1,299,101, cache_w 71,799) · cost n/a (unpriced)
  implementer: 11,249 tok (in 42, out 11,207, cache_r 566,487, cache_w 35,329) · cost n/a (unpriced)
  coordinator: 6,561 tok (in 22, out 6,539, cache_r 573,117, cache_w 17,128) · cost n/a (unpriced)
  reviewer:Claude: 2,800 tok (in 28, out 2,772, cache_r 159,497, cache_w 19,342) · cost n/a (unpriced)

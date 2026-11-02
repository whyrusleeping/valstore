---
id: "0558"
title: Stop reserved rewrite epochs from advancing the flush recovery watermark past an open value-log segment
status: done
priority: 1
created: "2026-11-02"
updated: "2026-11-02"
depends_on: []
spec_refs: []
---

## Description

Critical correctness finding from 0548 at HEAD525037b, reproduced locally without environment opt-ins. Both flush paths set covered_epoch=log.next_epoch()-1 (engine/flush.rs:246,411-412). Value-log rewrite planning reserves epochs on the live log (merge.rs:2009,2354; vlog.rs:1081-1085), even when its current segment remains open or the plan is later aborted. A later flush therefore records a watermark strictly above the still-open segment's epoch. Puts appended to that segment AFTER the flush and successfully synced are skipped by recovery.rs:405-407; open succeeds and silently returns None. Recovery tail seeding at343-348 has a similar filter.

Preserved public API regression reproducer: bench/0548/repro/tests/epoch_reservation.rs; command `cargo test --offline --manifest-path bench/0548/repro/Cargo.toml --target-dir bench/0548/target --test epoch_reservation -- --test-threads=1`. Coordinator verified: committed default-on agedGC case FAILS (late-b=None after sync/reopen); aborted agedGC plan case FAILS identically; no-merge control passes. Test uses real plan/build/commit APIs, 256-byte MemBlockDevice geometry, no private log mutation and no VALS_VLOG_CO_COMPACT. An opt-in co-compaction variant independently reproduced same mechanism. This is not a simulated I/O failure or corrupt input.

Suggested acceptance if accepted: flush watermarks reflect covered foreground log data, not allocated future rewrite epochs; preserve late synced puts/tombstones/RTs under committed and aborted rewrite plans and both flush APIs. Fix Engine::tail_epochs accounting too, avoiding reserved epochs forcing constant flush due state. Decide safe recovery handling for stores already holding an over-advanced watermark (replay eligible foreground segment(s) using covered_seq filtering; preserve truncate/rewrite epoch fences). Validate crash-point suite, real GC/co-compaction interleavings and cluster maintenance flush cadence. A scratch diagnosis using the actual open epoch fixed the repro, but that alone is not a complete validated recovery/cadence design. No production fix applied.

## Outcome

Both flush APIs record ValueLog::flush_watermark() (open segment's epoch) as covered_epoch, so synced writes after a flush survive reopen under committed/aborted rewrite plans. tail_epochs counts tail segments above last_flush_epoch (tail_epoch_gap removed), so reservations don't force flushes. Legacy over-advanced manifests: recovery replays/allocates from the newest ordinary segment below covered_epoch when it holds seq > covered_seq; epoch resume still folds the manifest epoch. New crash_points tests each fail with their fix reverted; vals-store + vals-cluster suites and clippy pass; opus+sol accepted. 0548 repro passes as bench/0548/repro/tests/epoch_reservation_post0558.rs. No hardware soak run.

Commit: 0558: flush watermark is the open value-log segment's epoch, not a rewrite-reserved one; tail_epochs counts tail segments; recovery replays below an over-advanced legacy watermark

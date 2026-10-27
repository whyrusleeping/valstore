---
id: "0437"
title: 'Rust client: a reply followed immediately by connection close is reported as Unavailable (flakes delayed_hello_parks_on_read_then_releases_queued_request)'
status: done
priority: 4
created: "2026-10-13"
updated: "2026-10-15"
depends_on: []
spec_refs: []
---

## Description

Found during 0342. `ConnState::drain_inbound` can read a reply frame (setting `waiter.result = Some(Ok(..))`) and then EOF in the same loop; `reset_failed_as` then drains every pending waiter into `failed` with `Unavailable`, discarding the completed reply. `net::client::tests::delayed_hello_parks_on_read_then_releases_queued_request` (server writes the reply and exits) fails this way in ~5/16 parallel `cargo test -p vals-client --lib` runs on clean d40c34e1.

Proposal: keep already-completed results across a reset (e.g. move completed waiters' results into a separate completed map that MuxRpc::poll/Drop consult) so a reply that arrived before the close is delivered; make the test deterministic.

Acceptance: a test where the server writes a reply then closes delivers Ok; the delayed-hello test passes 30 consecutive parallel runs.

## Outcome

ConnState.failed became `finished: IdMap<Result<AccountedBytes, TransportError>>`; reset_failed_as moves completed waiter results (with their response credits) there and fails only unanswered waiters with the reset error; MuxRpc poll/Drop consume it. New deterministic test reply_then_close_in_one_drain_delivers_the_reply (fails with the fix removed). Full vals-client lib suite passed 30/30 consecutive parallel runs (also independently by reviewer); `cargo test -p vals-client` green.

Commit: fix(0437): vals-client connection reset keeps already-completed replies (reply+EOF in one drain no longer reports Unavailable)

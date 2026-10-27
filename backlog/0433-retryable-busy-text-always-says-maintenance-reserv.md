---
id: "0433"
title: Retryable Busy text always says "maintenance-reserve floor", but seed-not-ready and digest-cap refusals return Busy too
status: done
priority: 4
created: "2026-10-12"
updated: "2026-10-13"
depends_on: []
spec_refs: []
---

## Description

## Description
## Source
Memory note m-dm2otba78295 (0418 R3 reopen) said that owned aediag hit seed-not-ready `tree` Busy, and that "its message misleadingly says 'maintenance-reserve floor'". Groomed 2026-10-12.

## Evidence (checked against the tree on 2026-10-12)
- `crates/vals-client/src/coord/mod.rs`: `TransportError::Busy` always displays as `busy: replica at maintenance-reserve floor, retry with backoff`.
- `Response::Busy` is also returned in other cases:
  - seed-not-ready refusals. `crates/vals-cluster/src/replica/lifecycle.rs` `seed_not_ready_for_caller` increments `ae_seed_not_ready_busy`.
  - the digest-serve cap (0411).
- `docs/formats.md` (wire kind 16 row) describes Busy only as the maintenance-reserve floor.
- Effect: in 0411/0412/0418, operators reading aediag/tool errors were pointed at disk reserve when the actual cause was a recovery seed still running. The useful gate is `vals_ae_seed_active == 0` on every node.

## Possible fix (choose the smallest)
- Make the Display/docs text cause-neutral: "replica busy (reserve floor, seed not ready, or serve cap); retry with backoff". Point operators to the `vals_ae_seed_not_ready_busy` counter and the reserve counters.
- Or carry a cause byte in the Busy reply. That is a wire change, so it needs protocol-version handling and is probably not worth it.

## Acceptance
- The client error text and `docs/formats.md` no longer attribute every Busy to the reserve floor.
- Existing Busy retry behaviour is unchanged.


## Acceptance criteria

## Outcome

TransportError::Busy and CoordError::Busy Display no longer blame every Busy on the maintenance-reserve floor; they list the actual cause families (reserve floor, RAM budget, admission gates, AE seed not ready). docs/formats.md wire kind 16 row now says the reply carries no cause and names each Busy source with its counter (vals_busy_rejections, vals_admit_shed_rejections, vals_ram_busy_rejections, vals_transport_request_rejections, vals_batch_apply_gate_busy, vals_scan_admission_busy, vals_ae_seed_not_ready_busy, vals_ae_regroup_not_ready_busy, vals_ae_digest_serve_rejections). Display-only change; retry/quorum handling matches on variants and is unchanged. vals-client lib tests 148/148 pass; vals-cluster tests compile. Self-reviewed.

Commit: fix(0433): cause-neutral Busy text in client errors and docs/formats.md

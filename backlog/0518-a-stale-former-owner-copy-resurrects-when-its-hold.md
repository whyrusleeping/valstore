---
id: "0518"
title: A stale former-owner copy resurrects when a membership change makes its holder an owner again
status: todo
priority: 2
created: "2026-10-28"
updated: "2026-10-29"
depends_on:
    - "0517"
spec_refs: []
---

## Description
Found by 0515's review. A node keeps a put for a key it no longer owns when every owner refuses the cleanup handoff, for example when the owners have already collected a newer tombstone (the 0038 admission floor). That copy stays on the node indefinitely.

0515 fences the re-home path. When `gc_grace > 0`, the copier on the target checks each page window, keys only. If the target holds a live value that the source lacks and that is older than half the grace, the copy is held, so cutover cannot happen and the PD aborts the move (`vals_rehome_copy_target_only_blocks`). Backup create treats a copy held only by a pending target as a non-owner copy.

What is still open:

- **A membership change moves ownership back to the holder.** For example, add-node or remove-node shifts the ring so a former owner owns the key again. The node then serves the stale copy, and the deleted value comes back.
- **A new owner has admission floor 0.** An owner that has seen no data progress yet (an empty new node or a pending target) accepts such a copy from a cleanup handoff until its first write.
- **A held copy may not clear on its own.** With RF > 1, the hold clears once anti-entropy repairs a source that missed an acknowledged write. A copy that no current owner holds keeps the move held until an operator deletes the key on the target.

Possible directions:

- At ring adoption, mark keys the node regains, then run the same source-lacks-it-and-old check against the co-owners before serving them.
- Raise a store's floor to the maximum of peer watermarks minus `gc_grace`, using liveness gossip.

Decide how this interacts with 0517: stranded valid data has the same signature.

Acceptance: a deterministic regression in which a former owner holding a stale put becomes an owner again through a membership change, with the newer delete already collected. The deleted key must stay deleted, and no valid record may be lost.

## Acceptance criteria

## Work log

- 2026-10-29: owner accepted (backlog triage), after 0517: 0517 decides which copies to refuse and this task decides which to trust, so the two share one design.
- 2026-10-29 (from 0517): the floor is now waived (`LiveFloor::Waive`) for AE pulls from the peer shard that owns the key under the committed placement, for re-home copies from the current owner and for `backup load` (`BatchApply` `fill` flag); pulls from a pending re-home target or a sibling shard, client writes, read repair, hint replay and cleanup handoffs keep it. Consequences for this task: (1) a stale copy on a node that becomes an owner again (or one admitted by an owner whose floor is still 0) now also spreads to its co-owners by AE, not only through that node's reads; (2) a cleanup handoff of a disjoint move (RF 1, or two quick membership changes) still refuses values older than the grace, so they stay on the former owner and the PD gate refuses the next change — the trust rule for handoffs from a recent former owner belongs here too.

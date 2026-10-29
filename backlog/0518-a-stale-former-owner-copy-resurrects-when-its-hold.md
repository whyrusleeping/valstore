---
id: "0518"
title: A stale former-owner copy resurrects when a membership change makes its holder an owner again
status: done
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

## Outcome

Handoffs from the key's committed previous owner skip the GC floor (BatchApply fill) for gc_grace/2 after a move, so RF 1 moves carry aged values. A put every owner refuses, no owner holds (digest check) and older than gc_grace/2 is recorded in memory per zone/shard: never re-offered (empty floor-0 owners cannot admit it), and deleted locally (tombstone hlc+1) when a placement adoption makes the node an owner again, before the reply; a failed delete fences reads/AE until it succeeds. Policy: an all-owners refusal counts as a delete, so a valid value whose handoff missed the window is lost only if --force returns it to its holder (documented; follow-up 0541). Regressions fail without each mechanism. vals-cluster lib 1015 pass, clippy clean. Reviews: claude accept; sol objects to the late-handoff policy (work log). No hardware run.

Commit: 0518: trusted handoffs from the previous owner; copies every owner refuses are not re-offered and are deleted locally when their holder owns the key again (fail closed)

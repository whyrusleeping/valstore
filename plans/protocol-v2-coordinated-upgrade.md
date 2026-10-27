# Protocol-v2 coordinated upgrade runbook

Protocol v2 changes every wire `version` from `[u64 hlc][u32 node]` to
`[u64 hlc][u64 writer]`. It is a flag-day protocol: v2 peers reject v1 before
processing requests, while a v1 decoder cannot safely consume v2 frames.

## Scope

Treat these as one release unit:

- every `valsd node` data-plane listener;
- every PD daemon/peer, including control pushes and restart beacons;
- the `valsd admin` binary used with those PDs/nodes;
- every Rust coordinator and operational tool;
- valsgo and every linked consumer, including Bootes and interop/soak tools.

Bootes resolves `github.com/whyrusleeping/valsgo` through its `go.mod` replace,
so build it only after the pinned valsgo source has been updated and verified.
Do not infer compatibility merely from a successful hello: a consumer must use
the 16-byte version codec on every apply, read, tombstone, batch, scan, digest,
repair, and anti-entropy path it implements.

## Preflight and exclusions

1. Record immutable source revisions for valstore, vals-examples, and Bootes and
   produce all binaries from those revisions. Keep the manifest with the
   deployment evidence.
2. Recheck the current backlog, maintenance holds, retained-data state, and
   forensic preservation requirements immediately before scheduling. Task 0270
   authorizes no fleet access, drive provisioning, formatting, wipe, or rollout.
   In particular, do not disturb retained cores, matching binaries, sealed
   caches, or a corpus awaiting crash recovery unless the current owner has
   explicitly released it.
3. Back up PD state and the data configuration/catalog. Verify a supported
   restore before the outage; this protocol change is not permission to format
   storage.
4. Build and test all peer and client artifacts. Confirm protocol-v1 refusal
   happens before a request and run the Rust/Go full-width wire fixtures.
5. Stop external writers, Bootes, repair/soak tools, and other clients. Confirm
   no process capable of issuing a v1 request remains.

## Upgrade

A rolling mixed-version deployment is unsupported.

1. During the coordinated outage, stop all PD peers and all data nodes.
2. Install the pinned v2 data-node binaries on every node, the matching v2 PD
   binaries on every PD peer, the matching admin binary, and rebuilt v2 client
   applications (including Bootes). Do not start traffic yet.
3. Start all data nodes and all PD peers as one coordinated generation. Wait for
   the PD group to become healthy and for all membership/catalog/range-map
   pushes to be confirmed.
4. With the matching admin binary, verify PD ping, members, zones, and range
   maps. Verify every advertised data listener accepts a v2 hello and rejects a
   v1 hello without processing a request.
5. Run a bounded canary that puts and reads a value, reads a tombstone, executes
   a batch containing a value and tombstone, and confirms every returned writer
   is wider than `u32`. Restart the canary node/PD generation cleanly and verify
   the same packed versions and values remain readable.
6. Start one rebuilt application generation, verify its error rate and readback,
   then start the remaining rebuilt clients. Keep old binaries quarantined.

## Rollback boundary

Rollback to v1 is allowed only before any v2 process has written data, and must
be an all-peer rollback while every process is stopped. Existing records whose
writer is a legacy `u32` are valid packed `u128` records and v2 zero-extends and
reads them correctly.

After the first v2 write, never open the data with an old binary. A v1 decoder
truncates the high 32 bits of the writer identity and may select a different LWW
winner. At that point rollback means restoring a verified pre-upgrade snapshot
with every process stopped, or fixing forward with v2; it does not mean running
old code over v2-written data.

## Local rehearsal recorded for 0270

The 2026-09-08 rehearsal used file-backed RF=1 data and single-peer PD daemons,
stopped both cleanly, restarted both from retained state, checked PD admin
`ping`/`members`/`zones`, and re-read the v2 put, tombstone, batch value, and
batch tombstone with full-width writers. No fleet host was contacted. See
`bench/0270-local-interop-20260908/README.md`.

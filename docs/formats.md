# vals byte-level formats

The byte-level reference for everything `vals` puts **on the wire** or **on disk**.
[`ARCHITECTURE.md`](../ARCHITECTURE.md) describes the *behavior*; this file pins the
*layouts*, so a second implementation, a packet/disk-dump debugger, or a
forward/back-compat reviewer doesn't have to read the encoders. Every layout here
is extracted from the code cited per section — keep them in lock-step. Its
companion, [`algorithms.md`](algorithms.md), pins the *agreement algorithms* that
compute the values these bytes carry (the placement hash, HLC/LWW order, the Merkle
digest, the CRC variant).

> **Stability:** the encodings are hand-rolled (no serde/protobuf) and **not yet
> frozen** — treat this as a snapshot of the current contract, not a compatibility
> guarantee. There is, however, a **negotiated protocol version**: every data-path
> TCP connection now opens with a version/feature handshake (§1.1.1), so a future
> wire change can be gated on the negotiated version rather than requiring a flag
> day. Individual message layouts still carry no per-message version field — they
> are governed by the connection's negotiated `PROTO_VERSION` (currently `1`). The
> PD raft peer channel carries the same version in its own (auth) hello. On-disk
> formats are versioned separately (a clean-baseline `[magic][u16 version]` envelope;
> a node fails loud on an unrecognized version — see §2/§3).

## Conventions

- **Endianness:** every integer is **little-endian**. (The magic-number *constants*
  below are written as the `u32` value; on disk they appear as their LE bytes.)
- **`bytes`** (length-prefixed blob): `[u32 len][len × u8]`.
- **`opt<bytes>`** (optional blob / tombstone-vs-value): `[u8 tag]` — `0` = none,
  `1` = some followed by `bytes`. A tombstone (`None`) and an empty value
  (`Some(&[])`) are distinct: `00` vs `01 00000000`.
- **`version`** (16 bytes): `[u64 hlc][u64 writer]`. `hlc = (physical_ms << 16) |
  counter16` — 48 bits of ms-since-epoch, 16-bit intra-ms counter. Total order is
  by `hlc` then writer identity.
- **CRC:** **CRC-32C** (Castagnoli), hardware SSE4.2 with a scalar fallback
  (`vals-store/src/crc.rs`), stored as a trailing `u32`.

### Magic numbers

| Constant | `u32` | LE bytes on disk | Where |
|---|---|---|---|
| `REC_MAGIC` | `0x324C4156` | `56 41 4C 32` = `VAL2` | vlog record (`vlog.rs`) |
| `SEG_MAGIC` | `0x31475356` | `56 53 47 31` = `VSG1` | vlog segment header (`vlog.rs`) |
| `SUPER_MAGIC` | `0x564C5A53` | `53 5A 4C 56` (mnemonic `VLZS`) | zone superblock (`zonestore.rs:296`) |
| `LAYOUT_MAGIC` | `0x564C444D` | `4D 44 4C 56` (mnemonic `VLDM`) | drive layout map (`drive.rs:634`) |
| `HINT_MAGIC` | `0x56484E54` | `54 4E 48 56` (mnemonic `VHNT`) | hint-backlog file (`hints.rs`) |
| `WM_MAGIC` | `0x56574D4B` | `4B 4D 57 56` (mnemonic `VWMK`) | liveness watermark file (`liveness.rs`) |
| `INCARNATION_MAGIC` | `0x56494E43` | `43 4E 49 56` (mnemonic `VINC`) | file-backend cluster incarnation file (`zonestore.rs`) |
| `PD_STATE_MAGIC` | `0x56504453` | `53 44 50 56` (mnemonic `VPDS`) | backup PD-state file (`backup/mod.rs`) |
| `RECORDS_MAGIC` | `0x56524543` | `43 45 52 56` (mnemonic `VREC`) | backup zone record file (`backup/mod.rs`) |

The vlog magics were chosen so the **on-disk LE bytes** spell the mnemonic; the
superblock/layout magics spell their mnemonic as the **hex literal** (so the LE
on-disk bytes are reversed). Match on the `u32`, not the byte order of the name.

### Format versioning (on disk)

Every persisted format carries an explicit version, under a **clean-baseline**
policy: the layouts below are **version 1**, and a node that meets an
**unrecognized** version **fails loud** — there is no read-old codepath, a
pre-versioning or future device must be wiped and re-synced (or the build
restored). Two patterns:

- **Container-level** (the engine): the **segment header** carries a `u16`
  format version; the records in a segment inherit it (so the hot-path record
  header pays no per-read version cost). An unknown version fails replay.
- **Standalone-file envelope** `[magic][u16 version][payload][crc32c]`: the zone
  superblock (§3.1), drive layout (§3.2), hint backlog (§3.4), and liveness
  watermark (§3.5). The version sits at a fixed offset right after the magic,
  checked before any layout-dependent parsing. Fail-loud strength varies by how
  load-bearing the file is: the **superblock** and **watermark** reject an
  unknown version hard (a misread would re-init a live namespace / trigger an
  unnecessary wipe); the **hint backlog** decodes a bad envelope to an empty
  backlog (best-effort; anti-entropy is the backstop); the **drive layout** is
  recomputable, so a bad snapshot is ignored and the reopen *fingerprint* guard
  (which now includes the version) catches a skew as a `LayoutMismatch`.

Values that ride **inside an already-checked container** — the PD membership
value and the range-map splits, both in the CRC-checked raft log and the
version-negotiated wire frame — are versioned by that container, not by a
per-value magic+crc envelope. The membership value does carry a leading `u8`
layout version (§3.6), replacing an older length-sniffing heuristic.

---

## 1. Wire protocol (`vals-cluster`)

Custom binary RPC over TCP — no gRPC/serde on the hot path. Defined in
[`wire/`](../crates/vals-client/src/wire) (pure encode/decode, re-exported by
`vals-cluster`) and framed by the mux transport in
[`net/`](../crates/vals-cluster/src/net/).

### 1.1 Frame envelope

The live multiplexed transport wraps every payload with a request id
(`net.rs:351`, `write_framed`):

```
[u32 len][u64 reqid][payload]      len = 8 + payload.len()
```

Replies carry the same `reqid` so the demux matches them to their caller; a
cancelled request just drops its slot and its late reply is discarded by id. A
frame whose `len` exceeds **`MAX_FRAME` = 64 MiB** (`net.rs:857`) fails the
connection before buffering (remote-OOM guard). *(The `wire::encode_request` /
`encode_response` helpers emit a bare `[u32 len][payload]` with no reqid — used only
by unit tests; the socket path always uses the reqid envelope above.)*

### 1.1.1 Connection handshake (`proto.rs`)

Before any request/reply frame, both ends exchange a **hello** as the very first
frame on the connection — the client writes its hello first on connect, the server
writes its hello first on accept. The hello rides the same `[u32 len][block]`
framing (it carries **no** reqid); its `block` is:

```
[u32 len = 14][u8×4 magic "VWP1"][u16 version][u64 features]
```

Each side runs `negotiate`: the effective protocol version is `min(ours, theirs)`
and the effective features are `ours & theirs`. The connection is **dropped** (no
error reply — the socket simply closes) when the magic is wrong, the block is
malformed, or the negotiated version is below `MIN_PROTO_VERSION`. Today
`PROTO_VERSION = MIN_PROTO_VERSION = 2`; v1 peers are rejected before requests are
processed because their 12-byte versions lack the full writer identity. Feature bit 0
(`OPAQUE_RANGE_MAP`) advertises RangeMap V4 decode/routing support. Feature bit 1
(`MIXED_BATCH_READ`) advertises the bounded mixed value/digest batch read. A client
uses one scalar probe to establish capability on a connection whose hello has not
resolved yet, and falls back to scalar `Read`/`Digest` probes when bit 1 is absent.
Feature bit 2 (`RESUMABLE_DIGESTS`, 0411) lets a server time-bound a `BucketDigests`
page and return a trailing resume key (§1.5); without it the server serves each page
to its byte budget with no time bound. Feature bit 3 (`RANGE_TOMBSTONES`, 0459)
advertises range deletes (docs/range-deletes.md): the `RangeDelete` and
`SyncRangeTombstones` requests and the `Scan` reply trailer. Feature bit 4 (`MULTI_SCAN`, 0482) enables batched scans and their keys-only mode. Feature bit 5 (`REVERSE_SCAN`, 0491) enables the descending direction flag on MultiScan; it requires bit 4 as well. Scalar reverse scans use a one-query MultiScan, not a new encoding of opcode 11. Unsupported peers return an explicit error; reverse scans are never emulated by ascending scans. Current Rust peers set all six bits. Since protocol v2 changed the version width, the
client sends only its hello until it has accepted the server hello; queued requests
remain unsent when an old or otherwise incompatible peer is rejected. The PD's
blocking control pushes and restart beacon perform the same client handshake before
their request; a second implementation must send a valid hello first or it is
dropped.

Protocol v2 is a flag-day deployment: mixed v1/v2 clusters are unsupported, and
out-of-tree clients and tools must be upgraded before the cluster.
Do not use a v1 binary to read data written by v2; old decoders truncate the high
32 bits of the writer identity and can select a different LWW winner.

### 1.2 Shared sub-encodings

| Name | Layout |
|---|---|
| `version` | `[u64 hlc][u64 writer]` (16 B; the Rust field remains legacy-named `node`) |
| `opt<bytes>` | `[u8 0]` \| `[u8 1][u32 len][bytes]` |
| `opt<u32>` | `[u8 0]` \| `[u8 1][u32]` (e.g. the anti-entropy `SyncTree` `requester`) |
| `opt<u32[]>` (zone scope) | `[u8 0]` \| `[u8 1][u32 count][u32 × count]` |
| `zone-entry` | `[u32 id][u32 rf][u8 ordering][opt<u32[]> scope][bytes name]` |
| `rangemap-entry` | `[u32 zone_id]` followed by the versioned RangeMap body in §3.3 (wire V2/V3/V4; never the persistence-only canonical shorthand) |
| `entry-list` | `[u32 count]` then `[bytes key][version][opt<bytes> value] × count` |
| `digest-list` | `[u32 count]` then `[bytes key][version] × count` (value-free) |
| `rt-entry` | `[opt<bytes> lo][opt<bytes> hi][version]` (one range-tombstone fragment) |
| `sockaddr` | `[u8 4][4-byte v4 ip][u16 port]` \| `[u8 6][16-byte v6 ip][u16 port]` |
| `addr-list` | `[u32 node_count]` then `[u32 id][u32 addr_count][sockaddr × addr_count] × node_count` (per-member shard addresses) |
| `ordering` byte | `0` = Unsorted (hash), `1` = Sorted (`zone.rs:34`) |

The `entry-list` is shared by the anti-entropy `Values` reply and the `Scan` reply;
the `digest-list` is the anti-entropy `BucketDigests` reply body.

### 1.3 Requests

Payload = `[u8 opcode]` then the fields below (`wire.rs:815`). Opcodes (`wire.rs:26`):

| Op | # | Payload after opcode |
|---|---|---|
| `Apply` | 1 | `[u64 epoch][u32 zone][version][bytes key][opt<bytes> value]` |
| `Read` | 2 | `[u64 epoch][u32 zone][bytes key]` |
| `SyncTree` | 3 | `[u32 zone][u32 level][u32 scope_g][u32 scope_count][u32 × scope_count][opt<u32> requester]` (one Merkle descent level; `requester` = the reconcile initiator's node id, scoping the fold to the shared keyspace — task 0015) |
| `SyncBucketDigests` | 4 | `[u32 zone][u32 num_buckets][u32 id_count][u32 × id_count][opt<bytes> after]` |
| `SetMembers` | 5 | `[bytes auth][u64 epoch][u32 vnodes][u32 rf][u32 count]` then `[u32 id][u32 weight] × count`, then `addr-list`, then the optional incarnation trailer |
| `Members` | 6 | *(none)* |
| `BatchApply` | 7 | `[u64 epoch][u32 zone][u8 sync][u32 count]` then `[version][bytes key][opt<bytes> value] × count` |
| `SetZones` | 8 | `[bytes auth][u64 epoch][u32 count]` then `zone-entry × count`, then the optional incarnation trailer |
| `Zones` | 9 | *(none)* |
| `GetWatermark` | 10 | *(none)* |
| `Scan` | 11 | `[u64 epoch][u32 zone][opt<bytes> lo][opt<bytes> hi][u32 max]` |
| `SetRangeMaps` | 12 | `[bytes auth][u64 epoch][u32 count]` then `rangemap-entry × count`, then the optional incarnation trailer |
| `RangeMaps` | 13 | *(none)* |
| `Digest` | 14 | `[u64 epoch][u32 zone][bytes key]` (version-only read; answered from the in-memory index, no value/device read) |
| `SyncValues` | 15 | `[u32 zone][u32 key_count][bytes key × key_count]` (anti-entropy phase 2: fetch full entries for the divergent keys, in request order) |
| `RangeStats` | 16 | *(none)* (legacy per-range entry/re-home statistics) |
| `RangeWriteMark` | 17 | `[u64 range-map epoch][u32 zone][bytes lower]` |
| `RangeStatsV2` | 18 | *(none)* (per-range statistics including recent ops/s) |
| `MultiScan` | 23 | `[u64 epoch][u32 zone][u8 flags][u32 start][u32 count]`, then `[opt<bytes> lo][opt<bytes> hi][u32 max] × count`; bit 0 = keys-only, bit 1 = reverse (requires hello bit 5), other flag bits rejected; `count <= 256`, `start <= count`, trailing bytes rejected; requires hello bit 4 |
| `MixedBatchRead` | 19 | `[u64 epoch][u32 zone][u32 start][u32 count]` then `[u8 role][bytes key] × count`, where role `0` requests the full stored value and role `1` requests only its version; `count <= 4096` and `start <= count` |
| `RangeStatsV3` | 20 | *(none)* (V2 statistics plus exact logical-winner accounting readiness) |
| `RangeDelete` | 21 | `[u64 epoch][u32 zone][u8 sync][version][u32 count]` then `[opt<bytes> lo][opt<bytes> hi] × count`; `count <= 4096`, no trailing bytes (sorted zones; `lo` inclusive, `hi` exclusive, absent = unbounded) |
| `SyncRangeTombstones` | 22 | `[u32 zone][opt<u32> requester][u64 digest][opt<bytes> after]`, no trailing bytes (anti-entropy range-tombstone exchange, 0462) |
| `GcFloor` | 24 | `[u32 zone]`, no trailing bytes (the shard's tombstone-GC admission floor for the zone; read by `valsd backup create` to judge a put held only by a non-owner, docs/backup-restore.md) |
| `CleanupStatus` | 25 | *(none)*, no trailing bytes (over-replication cleanup state of the shard; read by the PD before `add-node`/`remove-node`, docs/backup-restore.md). Starts a fresh cleanup check unless one started within `min(gc_grace / 2, 10 min)` |

`MixedBatchRead` preserves request positions, including duplicate keys. The route
stamp applies to the whole request. `start` resumes an earlier request without
changing or reordering its key/role vector.

`epoch` is the coordinator's ring-config version; a replica refuses an `epoch`
older than its own ring with `Response::StaleEpoch` (see §1.4). A `0` stamp
(a client that never adopted a config) is **included**: against a PD-managed
replica (adopted epoch > 0) it is refused — a client must fetch `Members` once
and stamp the adopted epoch before its first data op. Replicas that never
adopted a config (epoch 0, pre-PD setups) accept any stamp. For sorted zones,
`Read`, `Digest`, `MixedBatchRead`, `Scan` and every `MultiScan` sub-span are also refused with `StaleEpoch`
when a requested key or any range overlapping the scan span is not owned by this
node and shard (committed or pending re-home target), as writes are; the stamp
`u64::MAX` (`INSPECT_EPOCH`) exempts a read from both fences and is meant for
diagnostics that list a shard's physical contents. A `StaleEpoch`
refusal also applies to a whole `BatchApply` (the reply replaces `Batch`;
nothing was applied). `BatchApply.sync=0` requests buffered acknowledgement;
`sync=1` delays acknowledgement until the coalesced shard/zone device-sync
barrier. Protocol-v1 `Apply` is always buffered, so an explicitly
device-persistent single write is sent as a one-entry `BatchApply(sync=1)`.

`RangeDelete` applies one range tombstone at `version` per slice and replies
`Batch` with one `applied` per slice (`Won` when the slice raised the cover or was
already implied, `Rejected` beyond the clock-skew bound). The whole request is
refused with `StaleEpoch` (nothing applied) when any range of the replica's map
that overlaps any slice is not owned by this node and shard, committed or as a
pending re-home target, with `Busy` under RAM or space pressure, and with `Err`
for an unsorted zone, an empty slice (`lo >= hi`), the fragment cap or a node
that writes pre-v6 runs (slices before the failing one stay applied; a retry with
the same version is idempotent). `sync=1` acknowledges after the device-sync
barrier, as for `BatchApply`. A current owner mirrors each applied slice that
overlaps a pending range to the pending owners, clipped to that range, under the
write-mirror rules (timeouts retried twice; a failure or unresolved timeout answers
`Err` and counts against the move; docs/range-deletes.md §5).

`SyncRangeTombstones` compares range-tombstone sets (docs/range-deletes.md §4 "RT
set reconciliation"). With a `requester`, the responder clips its fragment set to
the ranges of its map whose current or pending replica sets contain both nodes and
that route to its shard (current or pending token), then re-coalesces adjacent
equal-version pieces. `digest` is the requester's `hash64` fold over its own
fragment set clipped the same way: `h = hash64(h_le ‖ u32 len(lo) ‖ lo ‖ [u8 0] |
[u8 1][u32 len(hi)][hi] ‖ u128 version_le)` from `h = 0` over the fragments in key
order (an unbounded `lo` is empty). Equal digests answer `RangeTombstones` with
`same = 1`; otherwise the reply is a page of the clipped fragments with
`lo > after` (absent `after` = from the start; an unbounded `lo` sorts as the
empty key), at most 1,024 fragments or 1 MiB, with `more = 1` when fragments
remain; the requester continues with `after` = the last fragment's `lo`. Without a
`requester` the responder pages its whole fragment set (backup). A fenced node
answers `Err` as for the other anti-entropy reads.

`auth` (the mutating `Set*` ops only) is the cluster control secret's token
(`auth.rs`), verified constant-time; a mismatch is refused with `Denied` (§1.4).
With no secret configured the gate is open (tests/benches).

The **incarnation trailer** of the three `Set*` ops is either absent (a push from
a PD without a committed cluster incarnation, or a pre-0283 PD) or exactly
`[u8 1][u128 incarnation]`, ending the payload; any other tag, a short id or
further bytes are malformed. It is a trailer rather than a new field so that
pre-0283 decoders, which ignore trailing bytes on these three ops, still apply
pushes from an upgraded PD. A node bound to an incarnation answers a push whose
trailer is absent or carries another id with `Err("cluster incarnation mismatch:
…")` and applies nothing (deploy.md §Cluster incarnation id).

### 1.4 Responses

Payload = `[u8 kind]` then fields (`wire.rs:979`). Kinds (`wire.rs:54`):

| Resp | # | Payload after kind |
|---|---|---|
| `Apply` | 0 | `applied` (see below) |
| `Read` | 1 | `[u8 0]` (absent) \| `[u8 1][version][opt<bytes> value]` |
| `Err` | 2 | `[bytes message]` (UTF-8) |
| `Tree` | 3 | `[u32 count]` then `[u32 bucket_id][u64 digest] × count` (one Merkle descent level; reply to `SyncTree`) |
| `BucketDigests` | 4 | `[u8 more]` then `digest-list`, then an optional trailing `[opt<bytes> resume]` (reply to `SyncBucketDigests`; written only when a resume key is present, which requires feature bit 2; decoders treat a missing trailer as no resume key) |
| `Members` | 5 | `[u64 epoch][u32 vnodes][u32 rf][u32 count]` then `[u32 id][u32 weight] × count`, then `addr-list` |
| `Batch` | 6 | `[u32 count]` then `applied × count` |
| `Zones` | 7 | `[u64 epoch][u32 count]` then `zone-entry × count` |
| `StaleEpoch` | 8 | `[u64 epoch]` (the replica's current ring epoch) |
| `Watermark` | 9 | `[u64 ms]` (data-progress watermark) |
| `Scan` | 10 | `[u8 more]` then `entry-list`, then an optional trailer `[u32 count]` `rt-entry × count` `[opt<bytes> resume]` (written only when it carries a fragment or a resume key, which requires feature bit 3; decoders treat a missing trailer as no fragments and no resume key; nothing may follow it). The fragments are the replica's range tombstones over the page window `[lo, coverage)`, clipped to it; a resume key means a covered-work or fragment byte budget cut the page, which may then carry no entries (docs/range-deletes.md §2). A peer without bit 3 whose window meets a range tombstone gets `Err("range tombstones present: client lacks RANGE_TOMBSTONES")` instead |
| `RangeMaps` | 11 | `[u64 epoch][u32 count]` then `rangemap-entry × count` |
| `Digest` | 12 | `[u8 0]` (no record) \| `[u8 1][version]` (version only, no value) |
| `Denied` | 13 | *(none)* (a `Set*` op's auth token did not match the cluster secret; op not applied) |
| `Values` | 14 | `[u8 more]` then `entry-list` (reply to `SyncValues`) |
| `RangeStats` | 15 | `[u64 range-map epoch][u32 count]` then legacy `range-stat × count` |
| `Busy` | 16 | *(none)* (retryable: a healthy replica refused the request; back off and retry, op not applied. The reply carries no cause. Causes include the maintenance-reserve floor and its admission shaping (`vals_busy_rejections`, `vals_admit_shed_rejections`, task 0085/0089), the RAM budget (`vals_ram_busy_rejections`), full transport, staged-apply or scan admission gates (`vals_transport_request_rejections`, `vals_batch_apply_gate_busy`, `vals_scan_admission_busy`), an unfinished anti-entropy seed or span regroup (`vals_ae_seed_not_ready_busy`, `vals_ae_regroup_not_ready_busy`) and the digest-serve budget (`vals_ae_digest_serve_rejections`)) |
| `RangeStatsV2` | 17 | `[u64 range-map epoch][u32 count]` then `range-stat-v2 × count` |
| `MultiScan` | 21 | `[u32 start][u8 more][u8 flags][u32 count]` then sub-scan pages. Each page: `[u8 more]`, entry-list, `[u32 fragment_count] rt-entry × fragment_count`, `[opt<bytes> resume]`. Flags echo request bit 0. In keys-only mode entry-list is `[u32 count]` then `[bytes key][version][u8 live]` (0 tombstone, 1 live; no value bytes or value length). Boolean fields reject other tags; trailing bytes rejected |
| `MixedBatchRead` | 18 | `[u32 start][u8 more][u32 count]` then mixed results in request order. Result tags: `0` value-absent; `1` value-present + `[version][opt<bytes> value]`; `2` digest-absent; `3` digest-present + `[version]`; `4` per-key Busy; `5` per-key Err + `[bytes message]` |
| `RangeStatsV3` | 19 | `[u64 range-map epoch][u64 accounting epoch][u8 accounting ready][u32 count]` then `range-stat-v2 × count`, then an optional `[u8 recovery seeding]` (0374; 1 while an engine of a configured zone on the shard, reopened with runs at process start, has not published its first leaf seed; reseeds caused by placement changes report 0; decoders that predate it ignore the trailing byte) |
| `RangeTombstones` | 20 | `[u8 same][u8 more][u32 count]` then `rt-entry × count`, no trailing bytes (reply to `SyncRangeTombstones`; `same = 1` carries no fragments) |
| `GcFloor` | 22 | `[u64 ms]`: a put whose physical time is below it is refused by this shard (`0` = tombstone GC off). The floor only rises. Reply to `GcFloor`; a peer that predates it answers the request with an error |
| `CleanupStatus` | 23 | `[u64 ring epoch][u64 gc_grace ms][u64 records][u32 count]` then per unsettled zone `[u32 zone][u8 flags][u64 kept][u32 n][bytes key × n]` (`n <= 4` sample keys; flags bit 0 `lapped`, bit 1 `pending re-home`, other bits rejected). `records` counts the entries the shard's engines track (0 only when the shard holds no data). A zone is settled once a cleanup lap that started after the latest check, under the zone's current placement and with no re-home copy hold active, released every copy of a key the shard does not own, and the zone has no pending re-home or active copy hold; `lapped` is set when such a lap finished but kept `kept` copies. Reply to `CleanupStatus` |

A `MixedBatchRead` reply count cannot exceed 4096 or the unserved request tail.
`StaleEpoch` and connection-level `Busy` replace the whole mixed reply; ordinary
read failures use the per-key result tags so successful keys still contribute to
their quorums.

A legacy `range-stat` is `[u32 zone][bytes lower][u64 entries][opt<bytes> midpoint]`
then `[u32 shard_index][u32 shard_count]` and five `u64` re-home fields (mirror
epoch/ok/failures, copy-complete epoch, copy progress). `range-stat-v2` inserts
`[u64 ops_per_sec]` after `midpoint`. The separate request/response tags preserve
the legacy list shape. V3 keeps that range-stat shape and prefixes an accounting
epoch/readiness receipt. A new PD falls back through V2 to `RangeStats` for
monitoring when a node lacks V3, but only a ready V3 receipt can authorize balance
execution.

**`applied`** (`wire.rs:961`): `[u8 tag]` — `0` = Won, `1` = Lost + `[version]` (the
beating version), `2` = Rejected (clock-skew refusal).

### 1.5 Reply paging (`Scan` / `MultiScan` / `BucketDigests` / `Values` / `MixedBatchRead`)

`MultiScan` (feature bit 4) has two independent paging levels. Its outer reply is
a contiguous prefix starting at the request's `start`; `more` means re-issue the
same vector with `start += returned_count`. Each returned page has its own
`more`, optional resume key and clipped range-tombstone fragments, with exactly
the `Scan` coverage rules below. The normal outer reply budget is 8 MiB including
entry, fragment and cursor encoding; a first sub-scan page is always returned to
make progress. Each sub-scan is individually bounded like `Scan`. Clients reject
wrong positions, mode mismatches, empty nonterminal replies and inconsistent
outer `more`. For ascending queries, missing bit 4 falls back to scalar scans,
never sends opcode 23. Reverse queries require both bits 4 and 5 and fail explicitly
without them; no ascending fallback is attempted. Pre-reverse MultiScan decoders
reject flags 2/3, so even a reconnect to an older peer cannot silently reverse the
direction. Replies retain the same layout and echo only the keys-only flag;
direction is known from the request, not repeated in the response.

For reverse queries, entries are descending in the same half-open `[lo, hi)`
window. A completed page covers the whole window; a truncated page covers
`[resume, hi)` if it carries an explicit resume key, otherwise `[last_key, hi)`.
Fragments remain ascending and are clipped to that suffix. Resume with **`hi =
resume`**, or **`hi = last_key`** if absent, keeping `lo` fixed. The upper bound is
exclusive: do not append a zero byte. A covered-work or fragment-budget page may
contain no entries and still advance via its resume key. The quorum coordinator
uses the maximum lower coverage bound across replica pages (algorithms.md §6).


A dense range, bucket set, or key list can hold more data than one frame may carry,
so these replies are **paged** rather than ever exceeding `MAX_FRAME`:

- The replica fills a reply up to its **reply byte budget** (`SCAN_REPLY_BUDGET`
  = 8 MiB of estimated entry bytes, far under `MAX_FRAME`) — and, for `Scan`,
  also at most `max` entries when `max > 0`. `Scan` / `BucketDigests` entries are
  always in **ascending key order**, tombstones included; `Values` entries are in
  the puller's **request order**. The first entry is always included even if it
  alone exceeds the budget, so a paging client always makes progress. A single
  `MixedBatchRead` value may exceed the normal 8 MiB page budget and is returned
  when its conservatively bounded framed reply still fits `MAX_FRAME`; only a value
  whose framed response cannot fit `MAX_FRAME` advances with a bounded per-key `Err`.
- `more = 1` flags a truncated reply. The client resumes:
  - **`Scan`**: re-issue with `lo = last_key ++ 0x00` (the smallest key strictly
    greater than the last entry's key — the bound is inclusive), same `hi`.
  - **`SyncBucketDigests`**: re-issue with `after = resume` when the reply carries a
    resume key, else `after = last_key` (the bound is exclusive), same
    `num_buckets`/`ids`.
  - **`SyncValues`**: re-request the unsent tail of `keys` (the entries the reply
    did not reach), same order.
  - **`MixedBatchRead`**: re-issue the identical key/role vector with `start` set to
    the prior reply's `start + count`. A non-final page is a non-empty contiguous
    prefix; results are never split or reordered.
- With feature bit 2 negotiated, a `BucketDigests` page is also cut by a serve time
  budget (`AE_DIGEST_SERVE_TIME_BUDGET`, 1 s, checked at run-window reads). A sparse
  bucket set can need a scan of every run in an engine, so a time-cut page may carry
  **no** entries; it then sets `more = 1` and a `resume` key strictly greater than
  `after`. The replica has examined every run through `resume`, and on the next
  request it seeks v4/v5 runs past `after` through their block index. The budget is
  cooperative: a page ends at the first window read after it lapses, so a page
  normally overruns it by at most one window read per run. Pre-v4 legacy sidecar runs
  cannot seek and still rescan from offset 0 on every continuation page.
- A truncated `Scan` / `BucketDigests` reply covers exactly `[lo, last_key]` (resp.
  `(after, resume]` with a resume key, else `(after, last_key]`): every entry the
  replica holds in that window is present. A `Scan` reply with a resume key (bit 3
  only) covers `[lo, resume)` instead, and its client resumes with `lo = resume`;
  covered entries in the window are omitted and its fragments describe them. A
  multi-replica merger must only combine the window **all** queried replicas covered
  — see `algorithms.md` §"Distributed range scan" for the coordinator's
  min-coverage merge.

`max` (a `Scan` request's entry cap, 0 = uncapped) bounds the *reply page*, not
the live results: tombstones count toward it.

---

## 2. Storage-engine on-disk (`vals-store`)

Each engine is an append-only **value log** of fixed-size blocks over a
`BlockDevice`. The segment allocator is **not persisted** — it is a free bitmap
(1 bit/segment) reconstructed at `open` by replaying segment headers
(`alloc.rs`), so there is no on-disk allocator format to spec.

### 2.1 Value-log record

`[50-byte header][key][value]` (`vlog.rs`):

| Offset | Field | Type |
|---|---|---|
| `0..4` | magic `REC_MAGIC` (`VAL2`, `0x324C4156`) | u32 |
| `4` | kind | u8 — `0` Put, `1` Delete, `2` RangeDelete (0452) |
| `5` | flags | u8 — bit `0x01` is tombstone |
| `6..10` | key_len | u32 |
| `10..14` | val_len | u32 |
| `14..22` | seq (monotonic record sequence) | u64 |
| `22..30` | epoch (the segment incarnation the record was written into) | u64 |
| `30..46` | version (opaque to the engine; cluster LWW uses packed u128) | u128 |
| `46..50` | crc32c | u32 |
| `50..` | key, then value | bytes |

A **RangeDelete** record (sorted zones, docs/range-deletes.md) has an empty key, the
range tombstone's packed version in `version`, and the value
`[u8 flags: bit 0 lo present, bit 1 hi present][u32 len][lo][u32 len][hi]` (a non-empty
span). Tail replay overlays records with `seq > covered_seq` into the engine's
fragment set; a flush moves them into its run's RT block (§2.2a), after which the
log record is dead. Releases before 0452 fail open on kind `2` (0450 hardening) or,
before 0450, stop replay at it silently, which is why an engine commits a v6 run
before its first range delete (range-deletes.md §1 "Downgrade fence").

The **CRC covers `header[0..46]` + key + value**: every byte except the CRC field
itself. On recovery, the winner for a key is the greatest `version` across all
replayed segments, with `seq` only breaking an equal-version tie; replay order is
not authoritative. A winner with `seq <= covered_seq` does not enter the memtable
even when its segment is replayed (`epoch >= covered_epoch`): it is in a run or
was dropped on purpose, so inserting it would bring back a key that a merge later
forgot (0466). Three rules keep every record with `seq > covered_seq` either live
in the memtable or beaten by a later tail record (a newer put or a Delete), so
replay never needs a run entry's old log record to stay dead:

- A flush captures only memtable entries with `seq <= covered_seq`. A paged
  flush leaves writes made after its watermark in the memtable for the next flush.
- A put that loses to the memtable winner's version is not logged.
- A forget that drops a run entry with `seq > covered_seq` (written by a paged
  flush before 0466) or any run entry while `covered_seq` is 0 appends a Delete
  record for it, like a forget of a memtable entry.

A `covered_seq` of 0 filters nothing, because a fence manifest written before any
flush also records 0. The cluster packs
`(hlc, writer)` so unsigned u128 order is the
same as its LWW `Version` order. Legacy records used only the low 32 bits for a
caller-selected node ID and left bits 32..63 zero; they decode unchanged, while new
CSPRNG writer-incarnation identities reject that u32 range and occupy the full low
64-bit field.

The `epoch` field is what binds a record to its segment *incarnation* (v2 format
change): freeing a segment wipes only its header block, so a reused segment still
holds CRC-valid records of the prior generation past the new data's end. Replay
accepts a record only when its epoch equals the segment header's, so that stale
chain can never be walked into — even when the new data ends exactly at a block
boundary and a prior-generation record starts right there.

### 2.2 Segment header

Each segment begins with a 22-byte header, then packed records, then zero padding
(`vlog.rs`):

| Offset | Field | Type |
|---|---|---|
| `0..4` | magic `SEG_MAGIC` (`VSG1`) | u32 |
| `4..6` | format version (`FORMAT_VERSION = 1`) plus `0x8000` `not_tail` flag | u16 |
| `6..14` | epoch (segment write generation) | u64 |
| `14..18` | block_size | u32 |
| `18..22` | crc32c (over `[0..18]`) | u32 |

The decoder removes the `not_tail` flag before checking `FORMAT_VERSION`. A
CRC-valid header with an unrecognized version fails `Engine::open` and `valsd dump`
(`LogError::Unsupported`) instead of being treated as free space, so a downgraded
binary refuses a log written by a newer release (0450). The format version governs the **record**
codec for the whole segment, so §2.1 has no separate record-format field (its
`version` field is the data's LWW version). Co-compaction output marks `not_tail`
and recovery does not replay those segments into the mutable index; their values
remain reachable through run pointers.

Replay walks records until the first non-`VAL2` magic, a torn/short record, or a
record whose epoch differs from the segment header's (a prior-incarnation
leftover) — whichever comes first marks the live tail. A CRC-valid record of the
segment's epoch whose `kind` this release does not know is not a tail: replay
fails with `LogError::Unsupported("record kind")` (0450).

#### Segment liveness & format discipline (the pre-format resurrection fence)

Both boot recovery (`Engine::open`) and `valsd dump` share one scan
(`dump::scan_log_segments`): **a segment is presumed live iff its header block
decodes at the open geometry.** That rule is only sound if a valid header can
*never* predate the last format. On a re-used, non-zeroed device it otherwise
would: a previous tenant's CRC-valid `VSG1` headers (and durable manifest chains)
survive a "format + write a smaller corpus" cycle **beyond the new log head**, so
the next open replays them. Because per-key replay winners are argmax(`version`)
(with `seq` only an equal-version tie-break), stale records carrying higher LWW
versions don't merely resurrect — they can *win* over live ones.

Every format path therefore **durably zeroes every segment-start (header) block
first**, then flushes, before any new data is written or the region is named:

- `Engine::create` — formats the raw device (deep-queued header wipe at queue
  depth, then flush). `Engine::create_prewiped` is the wipe-skipping constructor,
  used only where the device is already known blank (a fresh `MemBlockDevice`, a
  freshly truncated file, or a region just passed through `wipe_region`).
- zone-region allocation — `wipe_region` (`zonestore.rs`) zeroes every segment
  header **before** the superblock names the region, so a reused region never
  replays a dropped zone's segments.
- `Engine::truncate` — a two-phase full-sweep header wipe: phase A deep-queues
  every non-used header, drains every completion, and flushes; only after that
  flush succeeds may phase B deep-queue the headers that were used at truncate
  entry, drain every completion, and flush. Thus phase A is durable before any
  phase-B header is submitted, and a logical wipe leaves recovery nothing to
  replay.

A valid header thus always postdates the last format, and the intra-segment
record-epoch binding (§2.1) fences reuse *within* a segment. Together they make
"valid header ⇒ written since this format" hold on any device, reused or not.

A **format-generation stamp** (a monotone counter in each header + a superblock,
with the scan ignoring older generations) was considered and rejected: (a) it
cannot help anything written before the feature exists — the real hazard is
drives already contaminated by pre-fence/old builds; (b) the engine has no
superblock (`reserved_blocks` defaults to `0`), so it would need a new on-device
structure plus a `FORMAT_VERSION` bump; and (c) it adds nothing over the wipe for
any drive formatted by a current build, since the wipe already guarantees the
liveness invariant. **`FORMAT_VERSION` is unchanged — no on-device format
change.** See `crates/vals-store/tests/format_fence.rs` and `docs/deploy.md`.

### 2.2a Run file — `vals-store`

A run is one immutable sorted file written across whole segments (`run.rs`). In
order:

```
data blocks | filter | [v6: RT block] | index | [v1–v4: sidecar] | footer (80 B)
```

- **Footer**: `magic("VRUN") | version(2) | reserved(2) | entry_count(8) |
  num_blocks(8) | index_off(8) | index_len(8) | filter_off(8) | filter_len(8) |
  sidecar_off(8) | sidecar_len(8) | num_buckets(4) | crc32c(4)`. Readers reject a
  version above their own (`unsupported run version`), and they read the footer
  before log replay, so a run version bump is a downgrade fence.
- **Index**: `[u64 n] n × ([u32 len][first key][u64 off][u32 len][v6: u64 max_hlc])
  [v6: u64 rt_seq_floor][v6: u64 rt_off][v6: u64 rt_len] [crc32c]`. `max_hlc` is
  the largest `version >> 64` (the HLC of the packed `Version`) among the block's
  entries, so `max_hlc < V.hlc` proves every entry of the block is older than V.
  It stays resident with the fence index (8 B per block).
- **RT block** (v6, only when the run holds range-tombstone records, `rt_len > 0`):
  `[u32 count] count × ([u8 flags: bit 0 lo present, bit 1 hi present][u32 len][lo]
  [u32 len][hi][u128 version][u64 seq]) [crc32c]`, a non-empty span per record. The
  builder decodes it back and compares it with its source records before writing,
  and build verify byte-compares it from the device. `rt_seq_floor` records the
  highest RT record seq the build filtered with (docs/range-deletes.md §7).
- **Versions**: v4 keeps the per-run sidecar, v5 drops it (0365), v6 (0451) adds
  `max_hlc`, the index trailer and the RT block. A run with no point entries
  (`entry_count = 0`, no data blocks) is valid in v6 and holds only RT records.
  `VALS_RUN_WRITE_VERSION` selects 4, 5 or 6 (default) for new runs; merges write
  at least the newest non-sidecar input version, so a v6 input stays v6.

### 2.3 Cluster value encoding — `vals-cluster`

The cluster prepends exactly **one byte** to the user value before handing the blob
to the engine (`replica/codec.rs`):

```
[u8 kind][value bytes]      kind: 0 = Put, 1 = Tombstone
```

The blob contains no HLC or writer identity. `pack_version` stores the LWW version
in the engine record header's u128 `version` field as
`(hlc as u128) << 64 | writer`, so it survives reopen and compares in the same
order as `{ hlc, writer }`. A cluster
tombstone is an engine-level `Put` with the tombstone flag set, `kind = 1`, and no
user-value bytes; its record version participates in LWW like any put.

### 2.4 Run manifest (`VMAN`) — `vals-store`

The manifest is the durable list of a zone's live runs plus the value-log watermark
they cover (`manifest.rs`). Standalone-file envelope with a `u16` version:

```
manifest: magic("VMAN") | version(2) | generation(8) | covered_seq(8)
          | covered_epoch(8) | run_count(4) | run* | crc32c(4)
run:      level(4) | entry_count(8) | min_key(len4+bytes) | max_key(len4+bytes)
          | run_len(8) | seg_count(4) | segment(8)*
          | [v2:] value_bytes(flag+8) value_segs(flag+len4+seg8*)
          | [v3:] tombstone_min_version(flag+16)
          | [v4:] value_seg_bytes(flag+len4+u64*)
```

Version history (all decodable; older runs decode the newer fields as `None`):
`v1` predates per-run value liveness; `v2` appends `value_bytes` + `value_segs` (the
distinct value-log segments a run's entries point into — recovery's manifest-driven
liveness rule: a segment named by no run's `value_segs` nor the replayed tail is
free); `v3` appends `tombstone_min_version` (drives the tombstone-GC merge
trigger); **`v4` appends `value_seg_bytes`** — `Σ ptr.len` per referenced value
segment, **parallel to `value_segs`** — which drives dead-fraction **victim
selection** for the decoupled vlog reclaim (task 0069; a run predating `v4` decodes
`value_seg_bytes = None`, so the pressure path falls back to clustering-all reclaim
for it until the run is next rewritten). The engine writes `v4` on every fresh
manifest.

Recovery loads the newest complete chain whose payload decodes. A CRC-valid payload
with an unknown `VMAN` version, or a CRC-valid chain head (`seg_index = 0`) with an
unknown chain-header version and a generation at least that of every loadable
chain, fails `Engine::open` (`SnapshotError::Unsupported`) rather than falling back
to an older generation or to "no manifest" (0450). CRC-invalid or incomplete chains
still fall back.

---

## 3. Placement & operational on-disk (`vals-cluster`)

### 3.1 Zone superblock + region table

Lets many zone engines share one NVMe namespace; **double-buffered** (written to
slot `generation % 2`; `load` picks the higher valid generation) (`zonestore.rs:296,307`):

V1:

```
[u32 magic VLZS][u16 version=1][u64 generation][u32 count]
  then count × [u32 zone_id][u64 base][u64 len][u8 ordering]   (21 B each)
  then [u32 crc32c]                                            (over all preceding)
```

V2 adds the durable liveness watermark before `count`:

```
[u32 magic VLZS][u16 version=2][u64 generation]
[u8 watermark_present][u64 watermark][u32 count]
  then count × [u32 zone_id][u64 base][u64 len][u8 ordering]
  then [u32 crc32c]
```

V3 adds an optional topology descriptor after `count`, before the region entries:

```
[u32 magic VLZS][u16 version=3][u64 generation]
[u8 watermark_present][u64 watermark][u32 count][u8 topology_present]
  if present: [u64 layout_id][u32 node_id][u8 kind]
              [u32 partition_count][u32 partition_index][u32 drive]
              [u64 base][u64 len][u64 geometry]
  then count × [u32 zone_id][u64 base][u64 len][u8 ordering]
  then [u32 crc32c]
```

V4 adds the shard's cluster incarnation (task 0283) after the topology
descriptor:

```
[u32 magic VLZS][u16 version=4][u64 generation]
[u8 watermark_present][u64 watermark][u32 count][u8 topology_present]
  if present: (topology descriptor as in V3)
[u8 incarnation_present][u128 incarnation if present]
  then count × [u32 zone_id][u64 base][u64 len][u8 ordering]
  then [u32 crc32c]
```

A writer emits V4 only while the shard is bound to an incarnation and V3
otherwise, so pre-0283 binaries still open unbound shards. Every table and
watermark persist carries the incarnation forward; provisioning (`create`) starts
unbound.

Topology `kind` is 0 for a single whole-namespace store, 1 for partitioned JBOD.
Descriptor `base`/`len` are absolute blocks on the drive. `geometry` is `hash64`
(FNV-1a with the ring hash's SplitMix64 finalizer) of the little-endian u64 block
size, superblock extent, segment size, and reserved blocks. One fresh `layout_id`
is shared by all partitions
at provisioning. Checked opens require every descriptor field except `layout_id`
to match the configuration and all partition identities to agree, before opening
any engine. Every table/watermark persist retains the descriptor. This makes stale
headers from the other topology unreachable through checked opens, even without
NVMe deallocation.

`ordering` is the §1.2 byte. V1/V2 remain decodable (V1 supplies no watermark),
but checked opens reject their missing descriptor and require offline
`valsd provision`; no automatic legacy migration is performed. A
slot with bad magic/crc, truncation, or an unknown ordering reads as "no
superblock"; a magic-matching slot with an **unknown version** is a hard error
(fail-loud, so a future device is never re-initialized as fresh).

`base` and `len` are authoritative per-zone contiguous extents. They need not be
equal between IDs. Recovery requires unique IDs, non-overlapping in-range
extents, checked ends, at least one usable segment, and segment-aligned lengths
and bases relative to the end of the zone superblock. Ordinary daemon restarts
never rewrite these fields. Zone-region and override size changes only affect
`len` for future allocations; existing zones retain their persisted extents.
Equal-region-only readers reject a variable layout rather than guessing a size.

### 3.2 Drive layout map + fingerprint

The JBOD partition→drive map. Recovery normally **recomputes** it deterministically
via `drive::plan`; this codec is the snapshot form and the reopen-guard fingerprint
(`drive.rs:634,640`):

```
[u32 magic VLDM][u16 version][u64 region_blocks][u32 num_cores][u32 count]
  then count × [u32 partition][u32 drive][u64 base][u64 len][u32 core]   (28 B each)
  then [u32 crc32c]
```

The **layout fingerprint is this exact byte encoding** (`drive.rs:146`): persisted
on first format and re-checked on reopen — `verify_layout` refuses with
`LayoutMismatch` if a capacity/geometry change recomputes a different map. The
version is part of that fingerprint, so a version skew also surfaces as a
`LayoutMismatch` (and `decode` ignores an unknown-version snapshot — the layout is
recomputable). *(Inert on the SPDK backend today — the seam is a no-op there.)*

### 3.3 RangeMap codec

A sorted zone persists split keys and explicit per-range placement tokens in the
PD's raft state. Canonical maps (token = range lower bound) retain the v1 bytes:

```
[u32 split_count] then [u32 len][len bytes] × split_count
```

Non-canonical maps use the versioned form. V2 carries current tokens and V3
adds pending re-home tokens:

```
[u32 0xffffffff][u32 version=2]
[u32 split_count] then [bytes split] × split_count
[u32 token_count] then [bytes token] × token_count

[u32 0xffffffff][u32 version=3]
[u32 split_count] then [bytes split] × split_count
[u32 token_count] then [bytes token] × token_count
[u32 pending_count] then ([u8 0] | [u8 1][bytes token]) × pending_count
```

For V1/V2/V3, `token_count = split_count + 1`; every current or pending token is
`b""` or an existing boundary no greater than its range's lower bound, and a
pending token differs from its current token. Legacy decode derives each range's
placement debt as `token != lower_bound`, including after commit, and canonical
short-form eligibility remains token-shape-derived. Thus a legacy commit to a
valid earlier boundary remains V2 and preserves that token byte-for-byte. Maps
that have not opted into V4 retain these validation and encoding choices.

V4 is the fail-closed opaque-placement capability. It always carries current,
pending, and explicit debt vectors:

```
[u32 0xffffffff][u32 version=4]
[u32 split_count] then [bytes split] × split_count
[u32 token_count] then [bytes token] × token_count
[u32 pending_count] then ([u8 0] | [u8 1][bytes token]) × pending_count
[u32 debt_count] then [u8 debt] × debt_count        (debt is exactly 0 or 1)
```

All three counts after `split_count` must equal `split_count + 1`. A V4 token may
use the legacy boundary form or this one fixed-size opaque domain:

```
[u8×4 magic "VOPT"][u8 opaque_version=1][u8×16 seed]   (21 bytes total)
```

A non-boundary token with the `VOPT` identity but an unknown version, wrong
length, or malformed form is rejected. V1/V2/V3 accept only their existing
boundary forms and therefore reject opaque seeds. V4 placement debt is independent
of token shape: an intentional V4 commit clears the selected bit, abort preserves
it, inherited split preserves the left
bit and sets the new right bit, and canonical presplit creates a clear right bit.
A V4 merge conservatively ORs the merged ranges and marks any later token rewritten
away from a removed boundary; legacy merges continue deriving debt from token
shape so their bytes remain unchanged. Routing hashes only the committed token
and never reads debt.

The wire `rangemap-entry` (§1.2) always uses an explicit V2/V3/V4 envelope and
carries the fields for that version; version, shape, tag, and token mismatches fail
decoding loudly. Persistence may use the canonical shorthand above only for a
non-V4 map with no placement debt.

### 3.4 FileHintStore backlog

The hinted-handoff backlog (`hints.rs`) is best-effort; anti-entropy is its
durability backstop. V2 and V3 are append logs with a durable snapshot rewrite on
open/compaction (temp → fsync → rename → dir-fsync), followed by individually
CRC-protected delta records:

```
[u32 magic VHNT][u16 version]
  repeated: [u8 op][u32 payload_len][payload][u32 crc32c]
  snapshot payload:
    [u32 node_count]
      per node: [u32 node_id][u32 hint_count]
        per hint: [u64 hlc][writer][bytes key][u8 0 | u8 1 + bytes value]
```

V1 is the original whole-file snapshot with a final CRC and a `u32` writer/node.
V2 keeps that 12-byte `[u64 hlc][u32 node]` version inside the append-log envelope.
V3 changes `writer` to u64. V1 and V2 decode their node IDs by zero-extension and
are immediately rewritten as V3; a recognized `VHNT` file with an unknown version
is rejected without mutation rather than being reinterpreted.

### 3.5 FileLivenessLog watermark

The data-progress watermark behind the liveness lease (`liveness.rs`), atomically
rewritten (temp → fsync → rename → dir-fsync). An 18-byte
`[magic][u16 version][u64 ms][crc32c]` envelope — **fail-loud**: a *present* file
that isn't a valid v1 envelope (unknown version, bad magic/crc) refuses to load
rather than silently reseeding to "no progress" (which could trigger an
unnecessary wipe); an absent file is a fresh node (`None`):

```
[u32 magic VWMK][u16 version][u64 watermark_ms][u32 crc32c]
```

### 3.5.1 File-backend cluster incarnation

`<data_dir>/zones/incarnation` holds a file-backend node's cluster incarnation
(the SPDK backends keep it in the superblock, §3.1). Absent means unbound. It is
rewritten temp → fsync → rename → dir-fsync, and removed by `valsd reenrol
--clear`. A present file that fails magic, version or CRC refuses the node's
start rather than silently unbinding it:

```
[u32 magic VINC][u16 version=1][u128 incarnation][u32 crc32c]
```

### 3.6 PD raft membership value

A membership entry's raft value (key = the 4-byte node id; `pd.rs
encode_member_value`). The shard-address list is raft state on purpose — it must
be agreed and durable with the membership it belongs to, and it is what every
`SetMembers` push carries so the cluster can *reach* a grown-in node:

```
[u8 version][u32 weight][u32 addr_count][sockaddr × addr_count]   (sockaddr per §1.2)
```

A leading `u8` layout version (currently `1`) replaces the old length-sniffing
heuristic; a value without the current version byte (absent/empty, or an
incompatible build) reads as the safe default `(weight 1, no addresses)`. No
magic/crc of its own — it is always inside the CRC-checked raft log.

The cluster incarnation is the PD raft key `I` (one byte), value
`[u8 version=1][u128 incarnation]`, written once by a conditional write that
requires the key to be absent. A missing key, wrong version or a zero id reads
as "no incarnation yet".

Each PD replica added or removed by a PD membership change (task 0281) has a
record under PD raft key `V` + u64 LE PD id (9 bytes), written atomically with
the raft membership change (it is carried in that raft entry's context):
`[u8 version=1][u8 kind=1][sockaddr list of 1]` for a replica added as a learner
(its peer address), or `[u8 version=1][u8 kind=2][u64 replaced_by]` for a
removed replica (`replaced_by` is the learner chosen to take its place,
rewritten when another learner is promoted instead; 0 for a dropped learner). Founding voters have no record; their addresses come from
`pd_peer`. A removed id is never readmitted. Every applied membership change
makes the replica snapshot its state, so its latest snapshot carries the newest
membership it has applied. A PD `data_dir/raft` that was started empty with `join =
true` holds an empty marker file `joining` (checksummed like the other raft
files) until its first snapshot is installed.

A PD restored from a backup (`valsd backup restore-pd`, §3.7) also holds the
restore lineage under PD raft key `L` (one byte, never a 4-byte member key):
`[u8 version=1][u128 predecessor incarnation][u128 backup_id][u64 source pd_applied]`.
A restore of a restored cluster replaces it. Nothing reads it but `valsd backup
load|compare`, which refuse a target whose lineage does not name the backup.

A restored PD also holds the restore seed. Key `W` is
`[u8 version=1][u32 n][n × u64 PD id]`, the voter set restore-pd seeded
(sorted). Key `A` + u64 LE PD id (9 bytes) is `[u8 version=1][u32 digest]`.
Each voter writes its own `A` key through Raft while it runs with a
`restore-seed` file (§3.7). `valsd backup load|compare` need an `A` key from
every voter in `W` (unless `W` lists only one voter), and all of them must
carry the same digest. A restore drops the `W` and `A` keys of its source.

### 3.7 Backup directory (`valsd backup`, `vals-cluster/src/backup`)

A backup is a directory (contract: [`backup-restore.md`](backup-restore.md)).
Its files are written once and never modified; `MANIFEST` is written last, so a
directory without one is incomplete.

`MANIFEST` is UTF-8 text, one space-separated record per line, keys in this
order, every scalar key exactly once, ending in `end` with nothing after it. A
reader rejects any other first line, unknown keys, repeats and a missing `end`:

```
vals-backup 1
backup_id <32 hex>                 random, names this backup
created_unix_ms <u64>
tool <free text to end of line>    e.g. `valsd 0.0.1`
source_incarnation <32 hex>        cluster incarnation of the source PD state
pd_applied <u64>                   PD applied index of pd-state
pd_applied_end <u64>               PD applied index of the end-of-backup check
ring <vnodes> <rf>                 the source data nodes' ring parameters
pd_state <file> <bytes> <crc32c 8 hex>
zone <id> <name> <sorted|unsorted> <rf> <file> <bytes> <crc32c 8 hex> <records> <puts> <tombstones>
end
```

There is one `zone` line per zone in ascending id order: the implicit default
zone `0` (name `default`, unsorted, RF = ring RF) and then every zone in the
PD catalog. The file for zone `<id>` is `zone-<id>.vrec`. `<crc32c>` covers the
whole file.

`pd-state` is the PD Raft state machine (every key/value, §3.6 and the PD
keys) at `pd_applied`:

```
[u32 magic VPDS][u16 version=1][u64 applied_index][u64 term][u64 captured_unix_ms]
[u32 entry_count] entry_count × ([bytes key][bytes value])   keys strictly increasing
[u32 crc32c of every preceding byte]
```

`zone-<id>.vrec` holds the zone's merged logical records in strictly increasing
key order, then the zone's range tombstones:

```
[u32 magic VREC][u16 version=2][u32 zone_id]
record*:  [u8 1][bytes key][version][bytes value]     put
          [u8 2][bytes key][version]                  tombstone
range*:   [u8 3][opt<bytes> lo][opt<bytes> hi][version]   range tombstone (sorted zones only)
trailer:  [u8 0][u64 record_count][u32 crc32c of every preceding byte]
```

`version` is the 16-byte `[u64 hlc][u64 writer]` of §Conventions. Nothing may
follow the trailer. `record_count` counts every record, range tombstones
included; the MANIFEST's `<records> <puts> <tombstones>` count point records
only.

Range tombstones ([`range-deletes.md`](range-deletes.md) §8) follow every point
record, ordered by `lo` and non-overlapping (`lo` of each is at or above the
previous `hi`, so only the last may have an unbounded `hi`), each with a
non-empty span. They are the canonical overlay of every source replica's
fragment set, fetched with an unscoped `SyncRangeTombstones`, and no point
record they cover (`version <= cover(key)`) is written. `backup load` applies
them only after the whole file has been read and its trailer checked, so a
damaged file never deletes data; the order does not change the result, since the
cover rule compares versions. `backup compare` overlays the target replicas'
sets the same way, skips target records they cover, and counts every fragment
held on one side only as a difference. Version 1 files (no tag 3) still load.

`valsd backup restore-pd` turns `pd-state` into the first snapshot of a new PD:
the raft storage files under `<data_dir>/raft` hold one snapshot at index
`pd_applied` and the captured term, with the target voter set as conf state
and an empty log. Its data is the `pd-state` key/values, except that member
values carry the target `data_node` addresses (ids and weights unchanged), the
balance lock `B` and every PD replica record `V…` are dropped, every pending
re-home target is cleared, `I` holds a new incarnation, `L` the lineage and
`W` the seeded voter set (§3.6). The printed state digest is the CRC-32C of
the snapshot data encoded as a `pd-state` file, without its trailing CRC. It
is also written to `<data_dir>/restore-seed`, a UTF-8 text file:
`vals-restore-seed 1`, then the lines `digest <8 hex>`, `backup_id <32 hex>`,
`incarnation <32 hex>` and `index <u64>`. A PD that finds this file at start
confirms the digest to its group (key `A`, §3.6).

---

## 4. In-memory structures (no on-disk/wire format)

These have no serialized form — listed so a reader knows there is nothing to spec
and how the algorithm works.

- **Index** (`index.rs`) — rebuilt by log replay at `open`, never persisted. Two
  impls behind the `Index` trait: `HashIndex` (a `HashMap` keyed with `FxHasher`,
  seed `0x517cc1b727220a95`; no scan) for unsorted zones, and `FrontCodedIndex` (a
  prefix-compressed sorted block list — Stage B1, see `out-of-core-index.md`) for
  sorted zones. An ordered **range scan** seeks to the block straddling `lo` and
  decodes forward over `[lo, hi)`, both ends optional — the same semantics the
  earlier `BTreeMap` gave, `((Included(lo)|Unbounded, Excluded(hi)|Unbounded))`. The entry value
  is a `ValuePtr` `(segment, offset, len)` into the value log.
- **Segment allocator** (`alloc.rs`) — a free bitmap (1 bit/segment, `1` = free),
  reconstructed from segment headers at recovery. Not persisted.
- **HLC `Version`** — its in-memory form is `{ hlc: u64, node: u64 }`, where
  `node` is the legacy field name for the writer identity; on the wire it is
  §1.2's 16-byte `version`, while on disk it is packed into §2.1's u128
  engine-record `version` field. The §2.3 value blob carries only its kind byte.

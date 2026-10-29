# Out-of-core index — staged design

Status (2026-06-26): **Stage A landed** (durable index snapshot — `snapshot.rs`/`manifest.rs`,
`vals_snapshots`). **Stage B1 IMPLEMENTED (2026-07-02)** — the sorted-zone memtable is now a
front-coded (prefix-compressed) block list (`FrontCodedIndex`, `crates/vals-store/src/index.rs`),
measured **3.4× less resident index RAM live** on the realistic firehose keyspace (218.9→63.6
B/key marginal RSS, `vals-bench memprof`, 64 records/DID) — corroborating the 3.2–4.1×
deterministic accounting. The `Arc`-key-sharing integration blocker the B1 design flagged was
**already retired** by Stage C (C.5a-3/4 deleted the cluster's per-key version index), so
front-coding dropped in as an `Index` impl (the "option 1" API surface — dropping the `Arc`
key-handle API — came for free). The live OOM (1.41 B-record soak) confirmed the in-RAM index
(~117 B/key, non-draining) is the cap this attacks. Related landed RAM work: the memtable byte-cap
flush trigger + off-core flush streaming (bound the flush-build transient — a
*separate* OOM vector from this baseline). **Stage C status, clarified 2026-06-27:** the
LSM itself (C.5/C.6 + non-blocking C.5b flush + off-core maintenance) is **implemented,
committed, and on by default** (epoch-lag + `VALS_FLUSH_MAX_BYTES` byte-cap flush triggers,
run-merge at `VALS_RUN_COMPACT_THRESHOLD`) — see §C.5/C.6 status below. The earlier
1.41 B soak's ~117 B/key resident, non-draining result motivated B1; B1 has since landed
and measured 3.4× lower marginal resident index RAM on the realistic firehose keyspace.
Any *further* out-of-core push remains contingent on the B2 gate.
Structural follow-up to the 2026-06-20 startup-perf
work (the restart/recovery + out-of-core-keys threads). Stage 1 made recovery
cheaper (single-pass, boot-before-serve). This doc covers what comes after — but
**reframed**: an earlier draft bundled a cheap, high-confidence recovery win and a
contingent RAM-relief win into one large LSM rewrite. Review (three independent passes,
2026-06-21) showed the two are separable and the rewrite was being oversold as "free."
This version stages them so the cheap win ships without the risky one, and the risky one
is gated on a cost-benefit it must actually pass.

## Context

A vals node holds **every key it stores in RAM, twice**:

1. The engine index (`crates/vals-store/src/index.rs`): `key → {ValuePtr, seq}`, a
   `HashIndex`/`FrontCodedIndex` over the value log. `get()` is pure RAM; only reading the
   *value* touches the device (via the `ValuePtr`).
2. The cluster version index (`VersionedStore.versions`,
   `crates/vals-cluster/src/replica.rs:472`): a per-bucket `BTreeMap<Arc<[u8]>, Version>`
   (4096 buckets) used by store-if-newer LWW and anti-entropy. ~52–62 B/key (the key
   bytes are shared with the engine index via `Arc`, so not double-counted).

Two distinct pains fall out of this, and they are **not the same problem**:

- **Recovery stall.** On boot the index is rebuilt by replaying the whole value log —
  box-measured ~56 s before the node serves (`vals-startup-perf.md`). This is a
  *time* cost, paid every restart, independent of how RAM-bound the node is.
- **RAM cap.** Measured: 1B keys × RF=2 in ~400 GB across 3 nodes (~200 B/replica), so a
  755 GB box caps at **~1.8 B keys @ RF=2**. That cap — not the 24 TB drive behind it —
  sets how many nodes a dataset needs, hence cluster cost.

The earlier draft solved both at once with a full LSM (bounded memtable + on-disk sorted
runs + blooms + block cache + compaction). It can, but that is a multi-week build over
this project's most scar-tissued regime (churn), and it makes the recovery win a
*byproduct* of the read-path rewrite. The two wins have very different risk/reward, so
they get their own stages.

## The two separable wins

| | Recovery (O(tail) boot) | RAM-cap relief (out-of-core read path) |
|---|---|---|
| Driver | ~56 s boot stall, every restart | ~1.8 B-key/box ceiling → cluster cost |
| Needs | a durable index snapshot + tail replay | sorted runs, blooms, block cache, compaction, AE-on-disk |
| Index stays in RAM? | **yes** — fully | no — that's the point |
| New read-path regressions | **none** | cold-churn read-before-write, bloom CPU, 3-way RAM split |
| Consistency-contract surface | **none** (index is still authoritative in RAM) | large (version read becomes a device op) |
| Risk | low | high |
| Stage | **A (ship now)** | **C (contingent)** |

Recovery does *not* require giving up the in-RAM index. That realization is the spine of
this restructure.

---

## Stage A — durable index snapshot (ships the recovery win, no read-path risk)

**Goal:** boot in O(recent tail), index stays 100 % in RAM, zero change to the read path
or the consistency contract.

**Mechanism.** Periodically serialize the current in-RAM index — `{key, ValuePtr, seq,
version}`, sorted — to freshly-allocated segments, and record in the manifest/superblock
the vlog position the snapshot covers. On boot: load the snapshot, then replay only the
value-log tail past that position into the live in-RAM index. No O(dataset) replay; the
snapshot *is* the checkpoint.

**Why this is the right first increment.**

- It captures the **entire** recovery win — the literal "stop reading the whole vlog on
  startup" ask — and nothing else changes. The index stays fully resident, so every
  property the system relies on today is untouched: store-if-newer is a RAM check, AE
  digest serving is free RAM reads, `current_version` stays infallible.
- It is **smaller than even the first LSM step**: no blooms, no block cache, no
  compaction-vs-foreground p99 problem, no cold-churn read-before-write, no three-way RAM
  split, no new merge-precedence rules.
- It **reuses the exact Stage-1 seams** already built: `Engine::open_with_aux` →
  `VersionedStore::from_recovered` ("seed from recovered state without a re-read pass" —
  the snapshot feeds it instead of the vlog scan), the boot-before-serve sequencing
  (`ServerShared::zoned` opens before the listener binds), the persisted-catalog /
  `ZoneStore::zone_ordering` seam.

**Why the Stage-1 deferral note was too pessimistic.** Stage 1 deferred the durable
checkpoint by reasoning "a checkpoint ≈ the in-RAM index size per partition… segment-stored
is right but overlaps Stage 2." That quietly upgraded "checkpoint" into "full SSTable
tier." It doesn't follow: a flat sorted segment-stored snapshot is far smaller than the
LSM and overlaps Stage 2 only in that both write the index to segments.

**Cost to weigh.** A full snapshot is O(index) bytes written periodically (index write-amp,
*not* value write-amp — values never move). At hundreds of millions of keys/node that's a
non-trivial periodic write, but it is **schedulable and off the critical path** exactly
like the LSM's flush would be, and far cheaper than replaying the whole vlog every boot.
If the periodic cost bites, an incremental snapshot (delta since last) is the refinement —
but ship the simple full snapshot first.

**Crash safety (the one new invariant).** The snapshot's covered-vlog-position must become
durable **atomically with** the snapshot segments being named live in the manifest, and
recovery must treat that position as a **lower bound** — always replay forward from it,
never assume it's exact. This is a *new* durable-ordering obligation: the existing
`compact_filtered` crash-safety discipline is about *segments* ("new durable before old
freed"), not about *a manifest naming a live set + a vlog watermark*. Tail-vs-snapshot
collisions on a key resolve by **cluster `Version`** (the LWW key), which the snapshot
carries — see *Correctness obligations*, this rule is shared with Stage C.

**Box measurement.** Boot time vs. dataset size (should go flat past the snapshot), and
snapshot-write impact on foreground (should be invisible if scheduled on slack).

---

## Stage B — prefix compression + the break-even gate

Stage A leaves the RAM cap untouched. Before committing to the Stage-C rewrite, two things
should happen, because either may move the cap far enough that Stage C never pays off.

**B1. Prefix compression.** The firehose keyspace is sorted keys
with long shared prefixes (`app.bsky.feed.like/<did>/<rkey>`). Key interning already banked
−30 % (~88 B/key). A radix/ART or front-coded leaf collapses the shared prefix instead of
storing it per key — a 3–5× constant-factor RAM win **specifically on the sorted zones that
dominate the real workload**, with zero new device-read semantics, zero compaction
scheduler, and zero consistency-contract surface. The earlier draft dismissed
constant-factor wins as "only pushing the ceiling out by a constant" — but a 3–5× constant
on *this* keyspace could push ~1.8 B keys to ~5–9 B, which may be past the horizon where
you'd add nodes anyway.

**B1 — VALIDATED (2026-06-26).** Two empirical inputs now back this stage:

- *Live confirmation the RAM cap is the real ceiling, not the drive.* A 4-node SPDK soak to
  1.41 B records OOM-killed a node; forensics (`smaps_rollup`) showed the surviving nodes'
  ~40–95 GB RSS is ~100 % anonymous heap = the in-RAM index, **~117 B/key** resident and
  non-draining (it does not shrink after flush — values are on device, keys are not). That is
  exactly the cap this stage attacks. (The OOM's acute trigger was a separate flush-build
  transient, since fixed by the memtable byte-cap; the *baseline* that sets the cap is this
  index.)
- *Deterministic byte accounting of front-coding on the real keyspace* (sorted keys, block of
  16/32, first-key-full + per-key `(shared_prefix_len, suffix)` varints + the 40 B
  `IndexEntry`):

  | zone | mean key | baseline B/key | front-coded (block=32) | ratio |
  |---|---|---|---|---|
  | `records` (`app.bsky.feed.like/did:plc:…/rkey`) | 69 B | 149 | 47 | **3.2×** |
  | `backlinks` (`at://did:plc:…/…\0…`) | 130 B | 210 | 51 | **4.1×** |

  Front-coding strips the shared `did:plc:<author>/…` prefix: stored key bytes drop 69→4 and
  130→7. The 3–5× claim holds on this keyspace.

  **Consequence — the term that dominates flips.** Once the key bytes collapse to single digits,
  the **40 B `IndexEntry` ({ValuePtr 16, seq 8, version 16}) becomes the dominant per-key cost**
  (~85 % of the 47–51 B). So Stage B1's ceiling is ~40–50 B/key; squeezing further means
  shrinking `IndexEntry` (e.g. pack `seq`/`version`, narrow `ValuePtr`), a separable follow-on.

**B1 — the integration blocker (RESOLVED — was already retired by Stage C).** The B1 design
flagged prefix compression as *not* a drop-in `Index` impl because of the **`Arc`-key sharing**
between the two indices: `Index::put` returned an `Arc<[u8]>` key handle that the cluster version
index (`VersionedStore.versions`) stored as its own key, so each key's bytes lived once, referenced
by both — front-coding the engine index alone would break the sharing and total RAM would not drop.
**But Stage C (C.5a-3/4) deleted the cluster's per-key in-RAM version index entirely** (the version
is now sourced from the memtable-or-runs; only the tiny per-bucket XOR leaf accumulator survives).
So on HEAD there was no second index sharing the key bytes: the `Arc<[u8]>` handle `put` returned
was discarded by its only caller, and `key_handle`/`Engine::key_handle` had no callers. That made
front-coding a **drop-in `Index` impl** — the "option 1" surface (drop the `Arc`-handle API) came
for free. `Index::put` now returns `()`, `key_handle` is deleted, keys are owned `Box<[u8]>`
(unsorted `HashIndex` also dropped its `Arc` refcount word), and `Index::versions()` /
`RecoveredVersions` return owned `Box<[u8]>` keys.

**B1 — IMPLEMENTED (2026-07-02).** `new_index(Ordering::Sorted)` now builds a `FrontCodedIndex`:
a **`BTreeMap<Box<[u8]>, Block>` keyed by each block's first key**, where each block is a single
packed key buffer (first key full, then `(shared_prefix_len, suffix_len, suffix)` varints) plus a
parallel `Vec<IndexEntry>` (target 32 entries/block, split at 64). Point lookup finds the straddling
block via `range(..=key).next_back()` (O(log #blocks)) then scans that ≤64-entry block,
reconstructing keys into a reused scratch buffer (no per-`get` allocation); overwrite replaces only
the parallel entry (no re-encode, no re-key); insert/remove stream-re-encode just the one block
(allocating only the new block buffer — the "put allocates at most the re-encoded block" guard) and
a block whose first key changed is re-keyed by a `BTreeMap` remove + re-insert; `scan`/`scan_page`
seek to the straddling block and decode forward, honoring the exact bound/budget/`more` semantics of
the old `BTreeMap` path. **Keying the top level by block (not by key) makes a split or block-drop an
O(log #blocks) map edit rather than an O(#blocks) `Vec` memmove** — at the default multi-GiB
memtable cap there are ~1 M blocks and random-order (firehose-DID) inserts split roughly every
`BLOCK_TARGET` writes, so a `Vec` top level would memmove tens of MB per split on the serving core.
`key_bytes()` keeps its *logical* Σ`key.len()` semantics (the flush byte-cap depends on it). A
property test pins `FrontCodedIndex` against a `BTreeMap` reference over 20 k randomized ops
(insert/overwrite/remove/get/scan/scan_page with random bounds and budgets) on both random-binary
and shared-prefix keys, plus a targeted re-key test (split, insert-before-first-key, first-key
removal); the engine/cluster flush/scan/digest/recovery suites are the integration net (all green).

**B1 — measured win (2026-07-02).** Live marginal index RSS (`vals-bench memprof`, `file` backend
so values sit in page cache and the number is index-only, 1 M bsky-shaped 61 B sorted keys):

  | keyspace | baseline (`BTreeMap`) | front-coded | ratio |
  |---|---|---|---|
  | realistic fanout (64 records/DID — the firehose shape) | 218.9 B/key | 63.6 B/key | **3.44×** |
  | worst case (unique DID/key, no author-level prefix sharing) | 195.8 B/key | 86.4 B/key | **2.27×** |

  The 3.44× on the realistic keyspace corroborates the 3.2× deterministic accounting for `records`
  (a standalone prefix-index measurement program over synthetic firehose-shaped keys, re-run and
  unchanged: records 149→47 = 3.19×, backlinks
  210→51 = 4.08× at block=32). The `BTreeMap`-of-blocks top level costs only ~2–3 B/key amortized
  (1/32 of a block-index entry). As the design predicted, the floor is now **`IndexEntry`-dominated**
  (40 B {ValuePtr 16, seq 8, version 16} of the ~64 B/key), so squeezing further means shrinking
  `IndexEntry` — the separable follow-on (backlog task 0021).

  **Task 0021 done (2026-07-04) — packed resident entry.** The entry is now stored in RAM packed to
  **36 B** (from the 48 B the padded public `IndexEntry` occupies — the trailing `tombstone` bool
  costs 8 B of tail padding behind the u128 `version`). Layout: u32 `segment` (narrowed from the u64
  `SegmentId`; device geometry is far below 2^32 segments, bounds-checked at pack time and guarded in
  `Allocator::new`) + u32 `offset` + u32 `len` + a 64-bit word holding `seq` in its low 63 bits with
  the `tombstone` flag stolen into the top bit + u128 `version` verbatim. The public `IndexEntry`/
  `ValuePtr` types and the on-disk format are unchanged; conversion is a couple of `to/from_le_bytes`
  at the index boundary. Measured on `file memprof 1000000 100` (unique-DID shape, avg 61 B keys):
  **86.5 → 74.6 bytes/key (−11.9 B/key)**, the predicted ~−12 B.

**B1 — known cost / the box A/B follow-on.** Front-coding trades RAM for CPU on the sorted-zone
hot path: every insert re-encodes its block (O(block ≈32)), so on the CPU-bound `file`-backend
microbench the memtable *insert* throughput dropped (unique-DID random inserts: ~0.75 M→0.27 M
keys/s at 2 M keys; realistic fanout, where the `BTreeMap` baseline is unusually fast from insert
locality: ~1.9 M→0.29 M keys/s at 1 M keys — the memtable insert is CPU-only here, no device latency
to amortize under). Crucially, the `BTreeMap`-of-blocks top level keeps this cost **flat in the
block count**: front-coded insert throughput fell only ~10 % (274 k→245 k keys/s) as the keyspace
grew 4× (2 M→8 M keys, ~60 k→250 k blocks) — i.e. no O(#blocks) memmove cliff (a `Vec` top level
would have degraded ~linearly with block count). Point `get` is an O(block) in-block scan preceded
by an O(log #blocks) map descent vs a plain `BTreeMap` O(log n) chase — comparable comparison count,
better in-block cache locality. **The read/write-CPU regression under real device latency is the
named risk; it was validated by a box A/B on the SPDK cluster — see "B1 — BOX A/B" directly below:**
the Unsorted/HashIndex + AE path shows no regression (kvchurn value-verified, 0 errors), but the
front-coded **sorted-insert** throughput does regress ~1.55× on hardware (the re-encode cost is only
partially amortized by device latency).

**B1 — BOX A/B on the SPDK cluster (2026-07-06, oldbox, task 0027).** Isolating A/B:
**A = f39104e (the true B1 parent — `5077227^`; note the plan's "48b4c98" was wrong, that is Stage C
C.1, 24 commits earlier)** vs **B1 = 5077227**, plus HEAD = 24b87a3 (informational). Three release
`--features spdk` valsd builds; 3-node RF=2 SPDK cluster (`rsweep.sh`) + kvchurn for the
Unsorted/HashIndex + AE path, and the in-process 4-node RF=3 `distsorted` bench for the
front-coded **sorted** path (the rsweep cluster only serves Unsorted zones, so it does *not*
exercise the FrontCodedIndex — only B1's HashIndex `Box<[u8]>`/owned-key-digest changes).

- **Unsorted / HashIndex + AE digest serving — no regression.** Clean control (sweep off,
  count 2 M, ×2/side): write (`load`) A ≈968 k, B1 ≈1015 k, HEAD ≈897 k ops/s (B1 ≥ its parent);
  read (`verify`/`final-verify`) flat ≈266–271 k / ≈436–445 k across all three (within the ~2%
  within-side spread). **kvchurn value-verified with 0 errors on every run, including both HEAD
  runs.** Aggressive AE-heavy control (sweep 5 s, count 3 M, ×2/side): steady serving CPU (pd+3
  nodes, /proc jiffies) 290–303% for **all** of A/B1/HEAD — B1's owned-key digest-page copies
  did not move steady CPU; 0 read errors (the write "quorum failed" tail is the documented
  sweep-vs-serving availability tail, same order across binaries, not divergence).
- **Front-coded / sorted path — the named INSERT regression is real on hardware.** `distsorted`
  n=500 k, front-coded (B1) vs `BTreeMap` (A), identical bench source both sides: sorted **insert**
  A ≈406 k → B1 ≈262 k ops/s = **1.55× slower** (~35% drop; within-side spread ±2%, so clearly
  outside noise). This is the O(block≈32) re-encode-per-insert cost — **milder than the pure-CPU
  `file` microbench (3–6×) because SPDK device latency partially amortizes it, but not hidden**:
  the insert CPU is still on the critical write path. Full **sorted scan (read) is flat** (A ≈56–62 k,
  B1 ≈56–59 k keys/s), so front-coded in-block decode ≈ `BTreeMap` chase on the read side; 0 errs,
  replica spread OK on both. **Verdict:** B1 is box-validated as *no regression on the
  Unsorted/HashIndex/AE path*, but the front-coded **sorted-insert throughput regresses ~1.55× on
  hardware** — the deliberate RAM-for-CPU trade (3.44× resident index RAM for ~35% sorted-insert
  throughput). This residual write-CPU cost warrants a tracked follow-up (the RAM win is banked;
  the sorted-insert re-encode is the price). The box A/B ran on 2026-07-06 (task 0027); running
  the valsd cluster back-to-back needed two box-recipe fixes (SIGKILL-teardown DPDK residue, and
  the single-drive `serve_spdk_zoned` path ignoring `format=true` / spinning on the 1 TiB region).

**B1 — insert-CPU residual MITIGATED (2026-07-07, task 0066).** The O(block≈32)
re-encode-per-insert cost that drove the ~1.55× sorted-insert regression above was reclaimed with an
**in-place block-splice insert fast path**. Front-coding is local: on an interior, no-split insert
only the new key's own encoding and its displaced successor's encoding change (every later entry's
packed bytes are untouched), so the common case splices those bytes in place instead of
stream-re-encoding the whole block. Inserts at position 0 (block re-key) and inserts that overflow a
full block (split) keep the full re-encode. Re-run box A/B (`distsorted 500000 64 <4 pci> 4 1`,
oldbox, interleaved ×3): sorted **insert ~264 k → ~338 k ops/s = +28% on hardware**, so the
residual gap vs the pre-B1 `BTreeMap` baseline (~406 k) narrows **1.55× → ~1.20×**; point reads and
full scans stay flat. The **RAM win is intact** — capacity is kept tight via `reserve_exact` on the
splice plus a builder `shrink_to_fit`, so `bytes/key` is flat at ~75 (local `file memprof`:
317 k → 452 k keys/s, ~1.43× insert CPU, bytes/key 75.0 → 75.1–75.2; the packed encoding format is
unchanged, so the prefix-index accounting above still holds).

Historical note — the two integration options the design weighed (now moot):
  1. **Compress both indices together** — the front-coded store owns keys *and* versions. This is
     effectively what HEAD already is (Stage C retired the separate version index), so B1 landed at
     this endpoint without the feared store-if-newer / AE-digest surface change.
  2. **Front-code only the engine index** (half win) — unnecessary, since there is no longer a
     second full key copy to leave uncompressed.

**B2. The break-even arithmetic (the gate Stage C must pass).** The whole economic case for
out-of-core is "the box is RAM-bound and wasting its 24 TB drive." Whether that waste
exceeds the cost (engineering + operational risk over the churn regime) of the LSM **depends
entirely on value size**, and the doc must do this math for a concrete target workload:

- **Large values** (4 KiB+, the firehose `records` zone): 1.8 B keys × 4 KiB ≈ 7 TB of
  values — you approach a disk bound not far past the RAM bound. Out-of-core's
  disk-utilization win is modest. **Case weak.**
- **Tiny values** (likes/follows, ~tens of B): 1.8 B keys × 50 B ≈ 90 GB on a 24 TB drive —
  RAM-capped at <0.5 % disk use. Out-of-core could pack ~50–200× more per box. **Case
  strong — but** this is *also* exactly where B1 (prefix compression) gives the biggest
  relief and where Stage C's worst regression (cold-churn read-before-write over a huge
  keyspace) hurts most.

**Kill criterion.** Stage C proceeds only if, for a concrete target workload, the
wasted-disk-vs-nodes-saved math holds **after** B1's constant-factor relief is banked. If it
doesn't, the answer is "prefix-compress + add nodes," and Stage C is shelved. State the
threshold before building, not after.

**B2 — gate now runs against the banked B1 win (2026-07-02).** With B1 implemented, the sorted-zone
per-key resident cost is ~64 B/key on the firehose keyspace (3.4× below the ~218 B `BTreeMap`
baseline; the ~117 B/key the 1.41 B soak measured was a mix of sorted + already-flushed state).
The break-even arithmetic above must be recomputed with this banked floor: a 755 GB box's sorted-zone
key ceiling moves out ~3.4×, and the remaining per-key cost is `IndexEntry`-dominated (task 0021
would push it further before *any* out-of-core read-path rewrite is justified). Stage C stays
contingent on this recomputed gate.

---

## Stage C — out-of-core LSM (contingent endpoint)

Everything below is the read-path rewrite. It is the right design *if* Stage B's gate
passes. It carries real correctness obligations (next section) and real regressions
(*Performance implications*) that Stages A/B do not.

### Decision: LSM, not an on-device B+-tree

The value log is already an append-only, epoch-ordered log — i.e. an LSM with an unbounded
in-RAM memtable (the index) and no SSTable tier. Adding the SSTable tier is the design:

- **Bounded memtable** — the current in-RAM index, size-capped.
- **On-disk sorted runs** over the existing `Allocator` (segments). A run is `key →
  {ValuePtr, seq, version}` entries, sorted, block-indexed. (Note: a run *block* read is a
  new read shape — `ReadPlan` today is a single-record *point* read, `vlog.rs:299`; runs
  need a multi-entry block read on the same async substrate, closer to `read_many_async`.)
- **Per-run bloom (or partition) filter** in RAM. A negative lookup is a RAM filter check.
- **Block cache** for hot run blocks + upper index levels.
- **Manifest** naming the live runs + the snapshot/WAL position (the Stage-A manifest grows
  into this).

A B+-tree was rejected: in-place random-write update semantics fight a substrate built for
sequential append + whole-segment reclaim. The LSM reuses the allocator, the async read
path, and the compaction crash-safety discipline.

**Honest framing:** "finish the LSM we already have" undersells it. There is *no* existing
sorted-run / SSTable / manifest / bloom / memtable infrastructure — adding the SSTable tier
**is** net-new infrastructure. The reuse (allocator, async reads, crash-safety discipline)
is real but is plumbing, not the hard parts.

### Architecture

```
        writes ──> memtable (RAM, bounded)  ──flush──>  L0 runs ──compact──> L1.. runs
                      │  (+ tail on the value log = WAL)        (on device, sorted)
 point read:  memtable → bloom-filtered runs newest→oldest → highest Version wins
 negative:    memtable miss → every run's bloom says "absent" → done, no device read
```

- **Write.** Append the record to the value log (still the durability substrate), then
  insert `{ValuePtr, seq, version}` into the memtable. The vlog *is* the WAL.
- **Memtable flush.** At cap, write the memtable as a new sorted run (L0), build its bloom,
  add it to the manifest, drop those entries from the memtable. This also fixes the
  index-resize stall (the memtable never grows unboundedly).
- **Compaction.** Merge runs dropping superseded entries/tombstones — a natural place to
  co-compact the value log (copy live values forward as runs merge).
- **Lookup.** memtable, then runs newest→oldest, short-circuiting on bloom misses. **The
  winner is the highest `Version`, not the newest level** — see *Correctness obligations*;
  "newest level wins" is unsafe once read-repair applies older-timestamped writes.

### Store-if-newer

`apply_at`/`cas_at` need the *current version* of the key:

1. Check the memtable + every sealed memtable (RAM).
2. Check each run's bloom (RAM). If all say absent → key is new → write wins. **This fast
   path is correct only under the atomic-snapshot and tombstone-watermark obligations
   below** — it is *not* unconditionally correct, contrary to the earlier draft.
3. Only if a bloom says "maybe" do we read that run's block for the current version, then
   LWW. The read is **fallible** (device error / deadline cancel) and a failure must be a
   non-ack, **never** a silent "assume new" (*Correctness obligations*).

### Engine-wide key filter (0364)

**Why.** Step 2 used to probe every resident run's filter. FULL1 (task 0363's
whole-cache replay of 2026-10-02) measured about 0.4 µs per extra run, and
shard µs/write rose 4.37 → 7.19 as runs/engine rose 2.9 → 10.1. The merge input cap keeps
run count O(data). Since 0381 runs too large for a capped merge are frozen, so run count
grows ~linearly with data ÷ cap: about 100 runs/engine at 160 GB of run index. That is
why write probes must not scale with run count.

**Folding was rejected.** The first design folded per-run blocked blooms into one filter by
OR-ing groups of consecutive blocks; multiply-high block selection makes that exact. But a run
can only fold into a filter no larger than its own, so the folded density is Σkeys / (one run's
blocks). With the 1.5 size-ratio tiers and the 2 GiB cap, runs are similar in size. Ten
similar runs folded together give about 2 bits/key, an 84% false-positive rate (FPR); 20 runs
give 99.6%. Growing such a filter needs the keys again.

**Design (direct insertion, scalable).** Each engine owns a `KeyFilter`
(`crates/vals-store/src/keyfilter.rs`):
- **Sub-filters.** It is a list of blocked-bloom sub-filters (512-bit blocks, own seeds,
  atomic words). In-block positions are independent 9-bit slices of two mixed hashes, not
  double hashing; this matches the textbook FPR and is about 40% lower at the same size.
  Inserts go to the newest. In default rebuild mode (0396), a new sub-filter is sized at
  max(expected sink keys, 20% of the generation's total capacity), rather than doubling
  the previous sub-filter. Each adds one bit/key up to `VALS_KEY_FILTER_MAX_BITS_PER_KEY`.
  The defaults (12 and 12) give a constant 12 bits/key at k=7 (k = round(0.6 × bits)).
  A probe tests every **active** sub-filter; a negative needs all of them to miss.
- **First sub-filter.** It is sized at max(`VALS_KEY_FILTER_MIN_KEYS`, 16M by default;
  1.25 × the engine's keys; expected sink keys).
- **Scale.** One full sub-filter has a measured FPR of 0.93% at 10 bits, 0.63% at 11 and
  0.40% at 12 (the model gives 0.95/0.61/0.41%). Compound FPR is
  1 − product(1 − sub-filter FPR). Small steps reduce growth slack, but would increase FPR
  indefinitely without rebuilding. At four active sub-filters (or the stale-key trigger),
  once every run is active-covered and no cover scan is in flight, the engine attempts to
  acquire a process-wide rebuild permit (one concurrent engine by default). It allocates a
  fresh shadow generation sized at max(floor, 1.1 × estimate), where estimate is
  min(active inserted count, sum of run entry counts) + memtable entries. The shadow grows
  with the same small-step rule. A newly flushed snapshot may still be in the memtable at
  this point, so the estimate can include it twice; large bulk reservations and small
  corpora can exceed the nominal steady-state target.
  The target is approximately 1.5–1.8 B/live key after coverage catches up, not a hard
  byte ceiling under arbitrarily slow scans, duplicate run entries, stale keys, concurrent
  sinks or the floor. If a permit is unavailable the chain keeps growing and retries later.
  Legacy mode (`VALS_KEY_FILTER_REBUILD=0`) retains the former geometric growth schedule
  (`VALS_KEY_FILTER_GROWTH_PCT=200`): the scaled 400k-key/5k-floor test measures seven
  sub-filters, 2.48% FPR and 2.38 B/key. In task 0396's local 100M-key file-backed
  measurement (the ignored `keyfilter_file_ingest_measurement` harness in
  `crates/vals-store/src/engine/tests/keyfilter_bench.rs`, run with
  `VALS_KEY_FILTER_REBUILD=1` and `=0`), the shadow rebuild settled at 1.62 B per
  live key against 2.38 for legacy growth (−32%), with a maximum checkpoint FPR of
  0.78% against 1.42%; hardware confirmation remains task 0398.
- **Sizing under concurrency.** Each sink reserves its expected key count on its sub-filter
  until the keys are counted, so concurrent flush/merge/cover-scan sinks cannot overfill one
  sub-filter. Each requested merge lane reserves the sum of **all** input entry counts,
  an upper bound on the output keys inserted into that lane, even if some inputs are
  already covered by its generation. Reserving only uncovered entries could saturate a
  small tail during a partially covered merge (a latent bug in legacy mode too, corrected
  in 0396 review). Zero-entry scan and requested merge lanes reserve at least one key so
  empty runs can acquire coverage and cannot stall a rebuild.
- **RAM accounting.** The touched-page estimate counts in `filter_resident_bytes` and run
  metadata, so the RAM broker charges it. Sub-filters orphaned by a generation reset stay
  charged while an in-flight sink still references them. Shadow-generation allocated and
  touched bytes are included too. Old-plus-new overlap lasts until every current run is
  shadow-covered; the permit limits simultaneous engines, not bytes. Retired sub-filters
  without a sink reference are pruned on maintenance and sink creation.
- **Huge pages (0414).** Every apply probes the filter, and almost every probe is a new key
  that must miss in every active sub-filter, so the probe is one cold cache line per
  sub-filter. On 4 KiB pages a filter of hundreds of MB also misses the TLB on nearly every
  probe, and the page walk itself misses cache. A sub-filter of ≥ 2 MiB therefore lives in its
  own 2 MiB-aligned anonymous mapping, advised `MADV_HUGEPAGE` over its whole 2 MiB pages
  (the tail stays on 4 KiB pages, so RSS never exceeds the filter's bytes) and unmapped on
  drop. Pages are still faulted lazily on insert, which happens on flush, merge and
  cover-scan threads, never on the shard. The touched-page estimate counts the advised part
  at 2 MiB granularity. With THP disabled (`never`) the kernel ignores the advice and the
  pages stay 4 KiB; the estimate then overstates only a sparsely filled sub-filter, briefly.
  In the L1 model this cut page-walk cycles inside `excludes` by 92% and its cycles by 25–30%
  (task 0414, 2026-10-13).
- **Latency profile.** With the winner directory enabled the filter is off: no inserts, no
  cover-scans, no RAM. Its writes and reads use the directory instead.

**Coverage invariant.** Every run carries a `covered` flag. If a run is covered, all of its
keys were inserted into a sub-filter of the engine's current filter generation before the
flag was set. Sub-filters are only removed by dropping the whole generation, which clears
every flag in the same `&mut Engine` step. The flag is set in only four places, each on the
shard thread in the same step that publishes the run:
- **Flush.** The build (off-core on the maintenance thread, or synchronous) inserts every
  entry's key through a `KeyFilterSink`. The sink is bound to one sub-filter and generation,
  and uses atomic OR. The commit marks the run covered only if the sink's generation is still
  current. The `MaintDone` channel orders the maintenance thread's inserts before the commit.
- **Merge.** A merge's output keys are a subset of its inputs' keys. The output is covered
  if every input is still covered at commit. If some input was uncovered when planned, the
  merge build inserts its output keys, and the output is covered if that sink's generation is
  current. `compact_runs`/`forget_keys` do the same synchronously.
- **Open.** The filter starts empty and every recovered run is uncovered: today's per-run
  probing, correct but slower.
- **Cover-scan.** A `MaintJob::CoverScan` picks an uncovered run: runs outside the suffix the
  merge policy would merge next ("frozen") first, then the largest. It streams the run's keys
  in chunks of at most 16 MiB on a compaction lane, inserts them, and is resubmitted until
  done. `cover_scan_commit` marks the run covered only if the plan's token, filter generation
  and unique run id (not the reusable `RunHandle`) are all still current, and the streamed key
  count equals the run's `entry_count`; otherwise the scan is discarded.
  - **Key stream.** `RunKeySource` reads the sidecar of v3/v4 runs and the data blocks of
    v1/v2 and v5 runs (v5 has no sidecar, task 0365). A sidecar record whose key length exceeds
    the run's largest data block fails at once.
  - **Failures.** A read/CRC error or a key-count mismatch puts that run into retry backoff
    (60 s, doubling to 1 h) and the next run is scanned; the run stays uncovered (probed
    individually) meanwhile.
  - **Priority.** FULL3-KF's restart check measured the old FIFO queueing: 0 cover-scans
    committed in the first 44–50 min while both lanes merged the post-ingest backlog, then
    ~270 GB/node covered in ~6 min once merges ran out. Now the compaction queue has four
    classes: pressure merges, active cover-scan chunks (the run is active-uncovered, so every
    apply probes it individually, as after a reopen), shadow-rebuild scan chunks, other merges.
    Lane 0 takes scan chunks before ordinary merges and, for 100 ms after a chunk finishes,
    waits for the resubmitted next chunk instead of starting a merge. With a single compaction
    lane, shadow-rebuild chunks give one ordinary merge a turn after every four chunks (0366);
    active chunks do not. The other lanes take merges first and help scan when no merge is
    queued. Pressure merges go first on every lane. Scans are planned on any tick without
    region pressure, including ticks with a flush due (flushes have their own lane). Each
    in-flight scan counts as a zone maintenance job, so deferred zone drops wait for it.
    Shadow-rebuild scans are paced like other maintenance; active cover scans are not (their
    work is still counted in `maint_pace_charged_ms`). The shard finishes and resubmits a
    chunk between flush-commit steps too, so a long flush commit does not hold it. In the L1
    restart model (one shard, one lane, 62 GB of runs) this covers every reopened run in
    ~10 min instead of ~40 and cuts first-hour shard CPU per write by 12%
    (task 0421, 2026-10-13).

**Shadow coverage and swap (0396).** Each run also has a RAM-only `rebuild_covered` flag:
all of its keys must be in the current shadow generation before this is set. During a
rebuild, flush sinks insert into both active and shadow generations. Merge sinks request
only the lanes whose inputs are not all covered in that generation; output coverage is
proved independently for each lane by a current sink or by all inputs still covered at
commit. Cover scans first cover active-uncovered runs using the existing priority, then
scan shadow-uncovered runs into the shadow lane alone, with the same token, unique run id,
key-count check and backoff rules. Probes **never** consult the shadow generation.

When every current run is shadow-covered, one `&mut Engine` step makes the shadow
sub-filters and generation active, sets every run active-covered, clears shadow flags and
releases the permit. Old sub-filters retire and remain charged only while held by sinks.
A plan from before rebuild start that commits after swap has no lane in the new active
generation: it conservatively publishes an uncovered run (unless a merge's inputs prove
coverage) and is subsequently scanned. A dual-lane sink can continue to prove active
coverage after its shadow lane has become active. Reset/disable/winner-directory activation
abandons the shadow as well as the active generation, clears both sets of flags and
releases the permit. A crash or restart persists neither filters nor coverage: all reopened
runs are uncovered and use normal per-run probing until scanned again.

**Probe.** `probe_version` (and `lookup_runs`) hash the key once. A covered run is skipped
only when every sub-filter misses. Uncovered runs and the memtable are always checked, so a
negative behaves exactly like today's "no run may contain the key" result.

**Who depends on it.** Reads too, not only writes, so a false negative would also return
wrong data:
- `get`, `version`, `multi_get`/`multi_get_async` and `plan_get` resolve through
  `resolve_slow` → `lookup_runs` (capacity profile).
- Anti-entropy digest serving uses `probe_version`.
- `delete` uses `lookup_runs` even with a winner directory; there the filter is off, so every
  run is probed.

Only `probe_get` ignores the filter.

**Tests.** A randomized property test (`engine/tests/keyfilter.rs`) keeps flush, merge and
cover-scan plans in flight across generation resets (explicit, and the real stale reset with a
tiny floor), sub-filter growth and reopen. After every step it checks that every covered
run's keys hit the filter and that filtered probes equal unfiltered ones. The rebuild-mode
variant also checks every shadow-covered run against the shadow generation through start,
swap, abandon and reopen. Knocking out flush, merge, cover-scan, shadow-lane or shadow-scan
insertion, or forcing the all-inputs-covered rule, makes the harness fail. Separate tests
check permit release, restart during a rebuild, and 50M-key allocation/RSS and FPR bounds.

**Microbenchmark.** `keyfilter_microbench_probe_version_vs_run_count` is ignored by default;
run it with `--release`. It uses 4M keys per run, interleaved key ranges, and all-absent keys:

| Runs | No filter (ns/probe) | All runs covered (ns/probe) | Absent writes needing a device read: no filter → covered |
|---:|---:|---:|---:|
| 3 | 143 | 134 | 1.7% → 0.002% |
| 10 | 413 | 205 | 5.6% → 0.01% |
| 20 | 938 | 237 | 10.8% → 0.03% |

With half the runs uncovered, it costs 193/380/615 ns.

**Stale keys.** Forget, deletes and range moves leave stale bits; that only raises FPR. When
the keys inserted into a generation exceed both the floor and 2× the live run entries, the
engine requests a shadow rebuild in default mode, retaining active coverage until swap.
Legacy mode resets the generation immediately: all runs become uncovered and cover-scans
re-cover them. The stale trigger still uses physical run entries rather than an exact
logical-key census; duplicate versions and tombstones are not removed from that estimate.

**Knobs and metrics.**
- **Knobs.** `VALS_KEY_FILTER=0` disables the filter (kill switch). The others are
  `VALS_KEY_FILTER_MIN_KEYS`, `VALS_KEY_FILTER_BITS_PER_KEY` (12),
  `VALS_KEY_FILTER_MAX_BITS_PER_KEY` (12), `VALS_KEY_FILTER_REBUILD` (on),
  `VALS_KEY_FILTER_STEP_PCT` (20), `VALS_KEY_FILTER_REBUILD_SUBS` (4),
  `VALS_KEY_FILTER_REBUILD_CONCURRENCY` (1), and legacy `VALS_KEY_FILTER_GROWTH_PCT` (200).
  Parsing, ranges and semantics are listed in `configuration.md`.
- **Gauges (engine-sourced).** `vals_key_filter_{probes, negatives, run_probes, sub_filters,
  bytes, resident_bytes, capacity_keys, inserted_keys, retired_bytes, retired_resident_bytes,
  uncovered_runs, uncovered_run_bytes, est_fpr_ppm_sum, resets, cover_scan_failures,
  cover_scan_mismatches, cover_scan_backoff_runs, rebuild_bytes, rebuild_resident_bytes,
  rebuilding, rebuild_covered_runs, rebuilds_started, rebuilds_completed, rebuilds_abandoned,
  rebuild_peak_bytes}`. Exact accounting and high-water scope are documented in `deploy.md`.
- **Counters.** `vals_key_filter_cover_scan_{chunks, bytes, us, errors}` and
  `vals_key_filter_cover_scans_{committed, discarded}`.

**Upgrade.** There is no on-disk format change and no new persisted state. Old runs open
unchanged and are covered by cover-scans after boot.

### Anti-entropy — adapts, but **not** "for free"

The earlier draft claimed the Merkle path was unchanged. It is not — this was the most
severe review finding. Today the per-bucket XOR leaf accumulator (`VersionIndex.leaves`,
`replica.rs:205`) and the per-key version are mutated in one atomic, non-`await` step, so
`leaves[b] == XOR over bucket[b]` always holds. Out-of-core, "a key's current version
changed" decouples from "an insert/remove happened," and the accumulator can silently drift:

- A corrupted leaf that happens to match a peer → `differing.is_empty()` →
  **permanently skipped repair** (`antientropy.rs:403`). Worst-case failure for AE.

The accumulator can still be kept in RAM with zero device reads for the clean case — but
**only under an explicit invariant**: it is updated *exclusively* on a genuine current-version
change (the apply path), and **flush and compaction are provable no-ops for it** (they move
entries between structures without changing any key's merged current version). The one
operation that *does* change a merged current version with no write event is **GC dropping
an aged tombstone** (tombstone → absent); that drop happens in the compactor and must
explicitly signal the serving core with the *exact* version it removed, racing-safe against
a concurrent revive. Add a budget-gated periodic **device-backed audit** that recomputes a
bucket's leaf from the merged on-disk+memtable view and compares (the
`incremental_leaves_match_recompute` invariant, now spanning device reads), and treat a
not-yet-loaded run's contribution as **unknown → force the bucket to compare as differing**
(safe degradation, never silently trust a stale accumulator).

**Phase-1 digest serving needs a per-run bucket-digest sidecar — a key-sorted run alone is
NOT enough** (corrected 2026-06-23; the earlier "on-disk ordered range scan of the requested
buckets" was wrong). AE buckets are **hash-based** — `bucket_of(key) = hash64(key) % N`
(`antientropy.rs:131`) — so a Merkle bucket's keys are *scattered* across a key-sorted run,
not a contiguous range. Serving a bucket from a key-sorted run would scan the whole run
filtered by bucket: O(run), reintroducing exactly the digest-scan starvation the per-bucket
`VersionIndex` BTreeMaps were built to fix (commits caa2590+8cf5c14). The same orthogonal
partitioning (key-order for scans vs hash-bucket for AE) is why the two in-RAM indexes can't
be merged today. The resolution on-device: **each run carries a compact bucket-partitioned
digest sidecar** — `bucket → sorted [(key, version)] digests` — built at flush/compaction
time alongside the key-sorted block index and bloom. AE phase-1 reads only the *divergent*
buckets' sidecar lists from each run + the memtable's resident keys (the bounded memtable is
small enough to bucket-iterate by scanning), k-way-merges them in key order, and compares —
O(divergent-bucket-content), never O(run). The leaf clean-skip still skips clean buckets with
zero device reads. Sidecar reads share the maintenance I/O budget + qpair with compaction, so
the A/B must run **under churn together with compaction**. Phase-2 value reads (divergent keys
only) are unchanged.

This is also why the *sequencing* changed (see below): "fold `VersionIndex` into the engine
index" as a pure in-RAM step is **blocked** for sorted zones by this same partitioning, so
Stage C carries the version in the on-disk run entry + the bucket-digest sidecar instead of
unifying the two in-RAM maps.

**Update (task 0365): run format v5 has no sidecar.** v3/v4 sidecars were already flat and
key-ordered (bucket = `bucket_of(key) % num_buckets`, applied while streaming), so they held
exactly the blocks' `(key, version)` sequence a second time (~20 B + the full key per entry).
v5 runs derive that stream from the data blocks instead: every entry, tombstones included, in
strictly increasing key order, modulus from the footer's `num_buckets`, block CRC failures
reported as `Corrupt`. v1–v4 runs keep their sidecar paths; merges converge to v5. Format and
rollback rules: [deploy.md](deploy.md#on-disk-format-and-compatibility) and
[format-history.md](format-history.md#run-format-v5-no-per-run-sidecar).

### Maintenance scheduling — keeping compaction off the hot path

Flush/compaction starving the live workload is the failure mode that turns "fast" KV stores
slow under load, and this codebase already hit its cousin (synchronous AE reconcile reads
starving the SPDK serving core, `vals-hotspare-1b-and-cluster`). Two vals properties make it
avoidable *structurally*:

- **The vlog is already the WAL, so a flush is never on the write critical path.** A
  memtable is purely an in-RAM index format; flushing reclaims RAM and shortens the recovery
  tail — it acknowledges nothing. A flush backlog degrades to "use more RAM," never "stall
  writes" (categorically better than the RocksDB write-stall).
- **Runs hold `{ValuePtr, seq, version}`, not values, so compaction urgency is decoupled
  from any read-latency SLA.** Compaction is driven by RAM pressure / run count / garbage
  ratio, not a read deadline — it can be lazy.

The goal is **not** "make compaction keep up with foreground" but "let maintenance consume
only foreground's slack, and absorb bursts in RAM."

**The execution-model reality (corrected from the earlier draft).** Off-core maintenance is
**net-new infrastructure, not a reuse of the current model.** Today: tasks are `!Send`
(`vals-rt/src/lib.rs:29`), there is no cross-core channel anywhere, and compaction runs *on
the serving core* behind a stop-the-world quiesce gate (`node.rs:575`, `engine.rs:628` fully
synchronous, no `.await`). The shared device handle `Arc<D>` is the only cross-core seam;
index/log/alloc/manifest are single-core-owned. So the staging within Stage C is:

1. **First cut — on-core, cooperatively chunked.** Keep flush/compaction on the serving core
   but break it into bounded work units that `yield_now` between, using the bounded
   `drain_ready` generation (`vals-rt/src/lib.rs:130`) so `poll_io` interleaves serving —
   exactly the proven AE-sweep pattern (`CLEANUP_YIELD_EVERY`, `antientropy.rs:92`). This
   gets "doesn't starve foreground" with **no** second core, `Send` snapshot, or RCU. The
   per-unit latency the serving core can be blocked is then **one work unit**, so cap unit
   size (max N blocks per maintenance I/O).
2. **Later — true off-core**, only if step 1's chunking proves insufficient. This is the
   largest single lift in the whole design and needs, from scratch: a maintenance
   thread/core per shard, a `Send` snapshot of the immutable inputs, a lock-free manifest
   publish, and an epoch/RCU reclaim handshake (extending "new durable before old freed"
   with an epoch grace that bounds on the existing `inflight` device-await counter — and the
   grace must cover the **AE digest/value serve readers too**, not just the compactor). The
   separate maintenance qpair only pays off when a *separate core* polls it (a qpair's
   completions only fire when its owning thread polls — `spdk.rs:712`), and it addresses the
   *same* blocks the serving shard owns, so it needs an explicitly-overlapping shard-open
   (today's `open_shard` slices are disjoint, `spdk.rs:538`) plus care that two qpairs never
   write the same LBA concurrently (today's single-writer-per-LBA invariant).

**Closed-loop budget.** Drive the per-tick maintenance budget from a **live serving-core
slack signal**, not an open-loop byte/sec limit (the fragile magic-number knob). The signal
is ~90 % already plumbed: `server_poll` takes the `epoll_wait` idle branch exactly when
`inflight()==0`, and `poll_device` already *returns* a completion count that's currently
discarded (`server.rs:971`). What's reused from the AE precedent (`antientropy.rs:108`) is
the **rotating cursor + bounded-pass shape**; what's *new* is making the per-pass bound a
function of slack rather than a constant (`AE_*_PER_PASS` are fixed open-loop counts — the
precedent gives resumability, not the closed loop). Keep a fixed **floor and ceiling** on
the budget so a wrong estimate can't wedge maintenance at zero forever — the space-amp
ceiling below *is* the floor.

**Lazy trigger + space-amp override.** Trigger on sealed-memtable RAM, run count, and
garbage ratio (`reclaimable_fraction` already computes vlog dead bytes from the index,
`engine.rs:564`) — never a read deadline. But lazy index compaction **pins the dead value
each superseded entry points at** in the vlog, so a space-amp ceiling must be able to
*override* the slack throttle. That override **requires the bounded/incremental compactor to
exist first** — it cannot ride the current full-rewrite path, whose `InsufficientHeadroom`
precondition (`engine.rs:651`) rejects exactly the high-space-amp region the override would
target.

**Region-pressure trigger (task 0084, LANDED).** The lazy triggers above are necessary but
not sufficient: they were all denominated in absolute bytes/cadence (flush at 256 tail
epochs ≈ 256 MiB, run-merge at 8 runs ≈ 2 GiB of appends per reclaim cycle, checks on the
sweep-derived tick), with **no coupling to region pressure** — and for a *flushed* zone the
legacy vlog compactor is skipped entirely (its `reclaimable_fraction` mis-reads ~1.0, see
the note on `Engine::reclaimable_fraction`), leaving run-merge co-compaction as the only
reclaim path. Sustained in-place overwrite churn generates garbage at the full write
bandwidth, fills the region before those triggers engage, and a **full region is a
permanent write wedge**: `flush_plan`/`compact_plan` can no longer reserve output segments
to run the very reclamation that would free space (box: valsmix write bursts sank
865k → 31k ops/s then quorum-failed, `vals_compactions = 0`; local repro wedged a 256 MiB
region in ~9 s). The fix (`node.rs zone_maint_due`): when a zone's free-segment fraction
drops to ≤ 50 % **and** the LSM-aware dead estimate (`Engine::lsm_dead_fraction` — memtable
`live_bytes` + Σ runs' index + `value_bytes`, so it stays honest for flushed zones) says
≥ 10 % of the region is reclaimable garbage, the scheduler stops waiting: it flushes any
non-empty tail and run-merges from **2** runs up (planning the merge *before* the flush, so
the merge's larger output reservation gets first claim on the remaining headroom); the
flush-check cadence is clamped to
≤ 1 s (`SNAPSHOT_CHECK_MAX_INTERVAL`) so a slow sweep can't lose the race by cadence alone.
A zone nearly full of *live* data (dead < 10 %) is deliberately left to device-full
backpressure. Observability: run-merges now count as `vals_run_merges` (no longer conflated
into `vals_snapshots`) and their freed segments feed `vals_segments_freed`;
`vals_pressure_flushes`/`vals_pressure_merges` count the trigger engaging. Regression:
`vals-cluster/tests/churn_reclaim.rs` (kill-the-fix: reverting the pressure terms wedges
it).

**Guaranteed maintenance headroom (task 0085).** The pressure trigger engages reclamation
*before* the region fills — but only when the scheduler can observe the fill. A writer
strictly faster than reclaim can fill the last free segments within a single trigger→
merge-commit window (tick ≤ 1 s + plan + off-core merge duration, which grows with data),
and at **0 free segments** `flush_plan`/`compact_plan` can no longer reserve output segments
— a permanent deadlock (`churn_reclaim.rs` sidesteps this by *pacing* its writer; the
unpaced corner is what wedged the box). The fix makes the wedge structurally impossible via
a **dynamic maintenance-headroom reserve** in the allocator (`Engine::refresh_data_reserve`
→ `Allocator::set_data_reserve`):

- The data-path value-log append allocates through `Allocator::allocate_data`, which refuses
  while `free_segments ≤ reserve`; the maintenance reservations (flush/merge) use the
  unchecked `Allocator::allocate` and **may** draw the reserve down. So a pressure flush +
  reclaim merge can *always* reserve its output even from an otherwise-full region —
  reclamation always has room to make room, and the zone self-heals once the in-flight merge
  commits. Data-path writes see a bounded, retryable device-full/backpressure error window
  (the in-flight merge's duration), never a permanent cliff.
- The reserve is an **honest upper bound** on what a pressure flush + reclaim merge will
  request, recomputed only on structural events (recovery, flush/merge commit, segment-roll
  boundary — never per record; the per-append cost is one integer compare) from O(1) live
  state: (a) the memtable→run *flush* output (`run::run_len_upper_bound_agg` over the index's
  `len`/`key_bytes`/`max_key_len` — the same bound `flush_plan` reserves), plus (b) the
  *reclaim-merge* output — run index (`Σ run byte lengths` + the flush's not-yet-built run)
  and the value-log co-compaction (`2·⌈live value bytes / usable⌉ + 2`, mirroring
  `compact_plan_inner`), summed because the flush's run is still live when the merge reserves.
  The reserve is also at least the cheapest one-run value-log GC plan. Each run caches its victim
  list (mostly-dead segments ascending by live bytes; run metadata is immutable) on first use, so a
  segment roll filters out the open segment and scans each run's list up to its first gainful
  prefix, instead of rebuilding a hash map of every referenced segment per run (0390: that rebuild
  was the largest growing shard cost before the drive step).
  It tracks *live* data, so it is a small fraction of the region at low fill (a mostly-live
  zone with ample free space is never rejected) and only binds as the region nears full.
- Bootstrap: a single churned run pins a value log full of superseded records, so the
  pressure merge co-compacts from **one** run up (`Engine::compact_plan_pressure` /
  `node.rs zone_maint_due`, run count ≥ 1 under pressure) — no waiting for a second run to
  merge with. A mostly-*live* full zone (dead < 10 %) remains legitimate device-full.

Regression: `vals-cluster/tests/churn_reclaim_wedge.rs` drives **unpaced** overwrite churn
(writer strictly faster than reclaim) into a small region and asserts the zone always
recovers once a merge commits (kill-the-fix: reverting `allocate_data`→`allocate`, i.e.
reserve 0, wedges it — reclamation never engages). A bare `Engine` driven with no maintenance
scheduler (hand-run `compact()` in unit tests) disables the reserve via
`Engine::set_maint_reserve_enabled(false)` — with no reclaimer the reserve would only
backpressure the data path with nothing to free it.


**Per-shard, staggered, fused.** Each shard (`SHARDS=N`) compacts independently and out of
phase (no synchronized storm). Fuse index-run compaction with the value-log compactor — one
budget and one quiesce gate, not two compactors each claiming the device's slack.

**Proof:** box A/B on valsmix/kvchurn — drive background churn (forces flush+compaction)
while measuring foreground read p99; p99 must stay flat as compaction kicks in. The
transient to watch: the slack signal shedding maintenance *fast enough* under a sudden
foreground spike (bounded to one work unit by the chunking above).

### Correctness obligations (pin before code)

Findings from the correctness review. Root cause of most: **the version is no longer in a
single atomically-mutated map, so "current version" becomes a merge over structures whose
membership changes without a write event.** None are blockers; all must be designed up front.
The accepted blind-append follow-on makes `argmax(Version)` authoritative on every read surface
and replaces old-version Merkle deltas with fail-closed dirty-bucket rebuilds; see
[`blind-append-lww.md`](blind-append-lww.md). That document is a target design, not a claim that
0167 is implemented.

1. **Merge precedence is `argmax(Version)`, not newest-level/seq.** Read-repair and AE apply
   *older-timestamped* writes that legitimately lose LWW; "newest level wins" would report a
   lower `Version` than `apply_at` would have accepted (`replica.rs:1103`). Either define the
   cross-level merge as `argmax(Version)`, or guarantee a losing version is never inserted
   into any level (so newest-level *is* `argmax` by construction). `seq` and `Version` are
   **not** interchangeable — co-compaction assigns a record a new higher `seq` while keeping
   its old `version` (`engine.rs:784`), so recovery/merge must key on `Version`, the LWW key,
   not `seq`.
2. **Atomic snapshot of {memtables}∪{runs} per operation.** The manifest swap must make a new
   run **visible before** the flushed memtable becomes invisible, and each op takes one
   consistent RCU/epoch snapshot of the lookup set — otherwise a flush race can drop a key
   from the consulted set, the bloom-negative fast path fires, and a *stale* write wins LWW
   with no compare → lost newer write / resurrection.
3. **Tombstone-GC vs. the bloom fast path.** Once GC empties every run/bloom of a deleted
   key, the fast path answers "absent → new → write wins," resurrecting it for any straggling
   pre-delete write — a *new* resurrection vector beyond the rejoining-replica-wipe the
   `gc_grace` contract assumes. Retain a per-bucket **min un-GC'd tombstone version
   watermark** in RAM: a write older than the watermark cannot be admitted as "new" without a
   check. **DISCHARGED (task 0038):** rather than a per-bucket watermark, the admission floor
   is a single monotone bound in `Replica::apply_at` — when `gc_grace_ms > 0` and the key has
   no current version, a brand-new **live put** with `physical_ms < min(wall, high_water) −
   grace` is `Applied::Rejected`. That floor is ≥ every past GC cutoff (wall and the
   data-progress watermark only advance), so it needs no persisted per-bucket state and
   survives restarts; tombstone applies are never rejected (they cannot resurrect data), but
   one below the floor for a key with no current version is acknowledged without being
   stored, so repair cannot re-copy a tombstone another replica already collected (0499). See
   the kill-the-fix test `admission_floor_rejects_stale_new_put_below_gc_horizon`.
4. **Fallible version read = non-ack.** Out-of-core `current_version` can fail or be
   deadline-cancelled. A failure must propagate as a non-ack (like `Applied::Rejected`,
   `replica.rs:141`), never as "absent/assume new" — the cold-churn pathological case is
   exactly when this temptation appears.
5. **CAS keeps *local* atomicity.** `cas_at` is locally atomic today because the precondition
   read is synchronous with no `await` before the store (`replica.rs:1176`). A device read
   inserts a yield point. Re-validate the current version immediately before the store with
   **no `await` between**, retrying on change (the back-to-back borrow pattern
   `cleanup_with` uses, `antientropy.rs:652`), or document that local CAS atomicity is
   demoted.
6. **Recovery merges tail-vs-run/snapshot by `Version`** (shared with Stage A): the manifest
   watermark is a lower bound, always replay forward, resolve collisions by `argmax(Version)`.

### Index-light alternative (consider before the full LSM)

The review noted a cheaper midpoint worth pricing against the full LSM: keep **all** keys'
blooms/version-digests in RAM and spill only the value-pointers to disk. The RAM floor that
*still scales with keys* even in the full LSM is the bloom (~1.25 B/key) — so "all keys in
RAM" was never eliminated; the full `{key, ValuePtr, seq, version}` entry was. Index-light
captures most of that gap with **no leveled compaction, no sorted runs, no manifest of
overlapping runs**: a compact in-RAM `key → (fingerprint + locator)`, value-pointer on disk.
It still needs the Stage-A snapshot/recovery machinery and still pays the cold-positive
device read; its real saving is dropping compaction (and with it the space-amp coupling and
much of the maintenance-scheduling surface). It beats the full LSM when churn is low enough
that compaction's value-GC fusion isn't the main justification.

### Optional winner-directory performance tier (task 0183 spike)

The capacity-oriented default intentionally pays cold run-block reads to recover a key's maximum
`Version` and `ValuePtr`. On a mature RF=2 digest read, the value replica can therefore read a run
block plus the value while the digest replica reads another run block. The original one-device-read
shape is not preserved once the point index is out of core.

The supported daemon `profile = latency` enables this performance tier together with blind
unconditional writes; `profile = capacity` (the default) keeps it disabled. At create/open latency
maintains a collision-safe in-RAM `key → {maximum Version, winning ValuePtr}` directory in every
engine. Keys live in one dense byte arena. Each winner uses a 48-byte packed slot (key offset/length
plus the packed 36-byte index entry), and an open-addressed `u32` table locates slots. A parallel
control-byte table stores a seven-bit hash fingerprint per bucket, so probes reject nonmatching
buckets before dereferencing a winner slot or key-arena location. Hashes and fingerprints choose
candidates only: every fingerprint match compares the complete key from the arena, so a collision
can never select a value. Deletes reclaim winner slots immediately and compact dead arena bytes and
table capacity opportunistically.

The representation is volatile and makes no on-disk format commitment. Open synchronously rebuilds
it by scanning every recovered run and then the replayed memtable, retaining the maximum version.
Rebuild grows from observed unique winners rather than summed physical run entries, then shrinks the
key arena, winner slots, and table to the final unique-key count before publishing; overlapping
rewrite history therefore increases rebuild work but not retained directory capacity. A disabled
process performs none of this work and continues to use the capacity-oriented run path.

With the tier enabled:

- point reads plan the winner's value directly; synchronous and asynchronous read paths validate the
  decoded record's complete key and exact version before returning it;
- point digests and legacy unconditional LWW resolution return the maximum version without a run
  read;
- blind unconditional writes retain optimistic `Won` acknowledgement, but a known loser/exact
  retry need not append, and a new winner updates the Merkle leaf from an exact old winner rather
  than dirtying the bucket;
- flush and index-only merge preserve directory pointers, while value-pointer rewrites, tombstone
  GC, and forget rebuild before serving resumes. An open-time rebuild error fails `Engine::open`, so
  the engine never serves a partial directory. A post-maintenance rebuild error disables directory
  authority, returns the maintenance error, and leaves subsequent requests on the out-of-core path;
- `winner_directory_keys` and `winner_directory_bytes` expose coverage and actual allocated arena,
  packed-slot, lookup-table, and control-byte capacities. A 1,000-key synthetic check with 16-byte
  keys allocates 74.6 bytes/key, including 2.048 bytes/key for the power-of-two control table. On the
  mature hardware corpus, whose key shape and vector slack differ, the pre-control metric
  was 105.4 B/physical key and the final matched process-RSS increment was 62.9 B/key, down 47% from
  the crude `HashMap<Box<[u8]>, IndexEntry>` arm's 118.9 RSS B/key. Process RSS remains governed by
  the existing RAM budget.

The repeated mature flushed-corpus A/B passed the gate: 2.320M strong reads/s at the tuned 16×512
shape, 2.51× matched control, zero run-probe blocks, p99 7.22 ms versus control's 66.64 ms, and 1.93×
legacy overwrite throughput. Fresh inserts retained an 8.8% cost. The latency profile remains opt-in because its 62.9 RSS B/key and O(dataset)
synchronous open scan are an explicit performance-tier tradeoff; capacity is the
default. Persisted or lazy recovery is separate future work.

### Performance implications (Stage C)

Trades a flat RAM tax for conditional device/CPU costs:

- **RAM footprint (the win).** ~200 B/key → ~1–4 B/key floor (bloom ~1.25 B/key @ 10 bits,
  ~0.8 % FP, + block index). Moves the 755 GB box from ~1.8 B keys toward tens of billions.
- **Read path.** Hot keys unchanged (RAM/cache). Cold *positive* adds ≥1 device read for the
  version/ptr on top of the value read — and across overlapping L0 runs may block-read
  several runs before the hit (same multiplier as the negative FP cascade). Cold *negative*
  (unique ingest) stays at **zero** device reads — the property the design protects.
- **Write path.** Unique ingest barely changes. **Cold churn over a large keyspace is the
  pathological case** — every overwrite becomes bloom-positive → read-before-write → append.
  This is the single highest-risk regression and lands in the regime with the most scar
  tissue (`vals-hotspare-1b-and-cluster`). Measure it first.
- **Serving-core CPU.** The bloom-negative "RAM" fast path is **not free**: `k` hashes ×
  *run count* per write, on the serving core. At k=7 and 8 runs that's ~56 hashes/write —
  **re-introducing exactly the per-op hashing the multicore sprint just removed**
  (per-request SipHash deleted; `FxHasher` chosen because hashing dominated per-op CPU,
  `index.rs:18`). Quantify against the measured single-core ceiling (827k/615k); it may
  motivate a blocked/split bloom or fewer bits/key + partition filter to protect the hot
  write path. Strong argument for leveled (O(levels)) over size-tiered (O(runs)) upper levels.
- **Sealed-memtable fan-out.** A lookup now probes active + every sealed-but-unflushed
  memtable + runs. The backpressure relief ("absorb the burst in RAM") therefore *increases*
  per-op serving CPU as the sealed count K grows — bound K as a first-class knob.
- **Write amp.** Index-run compaction atop value-log GC; index entries (~30–40 B) are tiny
  vs. values, so byte write-amp is small **iff** the two are co-compacted (shared read-amp).
- **Device space amp.** The space-amp coupling above; needs the override + bounded compactor.
- **Recovery (already won in Stage A).** Caveat at extreme scale: loading all blooms at boot
  is ~1.25 B/key of reads (~60 GB at 50 B keys) and would use the *synchronous* recovery
  scanner (`engine.rs:296`), far slower than the async serving path. Lazy/paged bloom load
  (unloaded bloom = "maybe present", safe) is **required at scale**, not optional, and needs
  the async read path wired into boot.
- **Block-cache RAM is a three-way split** (memtable / blooms / cache), per shard, dividing
  again as `SHARDS` grows — and the measured throughput sweet spot is `SHARDS=4` (more
  thrashes the SPDK busy-poll), so out-of-core's per-shard RAM pressure is in direct tension
  with the multicore-serving win. Reconcile explicitly.

### Risks / open questions (Stage C)

- **Cold-churn read-before-write** — highest-risk regression; measure write throughput under
  churn over a keyspace larger than the block cache *before* committing.
- **AE accumulator correctness** (the "not for free" obligation) — the audit + no-op-on-flush
  invariant must be designed and tested, not assumed.
- **AE digest-scan under churn** — now device-bound; validate the clean-skip + budgets +
  separate-qpair pacing keep it off the critical path, measured *with* compaction.
- **Maintenance slack-signal lag** — confirm it sheds compaction within one work unit under a
  foreground spike.
- **Bloom bits/key** — tune against FP device-read rate *and* boot bloom-load cost (opposed).
- **Leveled vs. size-tiered** — pick against serving-core bloom-probe CPU and AE scan width,
  not just write-amp.

### Record/version model — decision (C.4b, 2026-06-23)

The version is promoted to a **first-class, opaque field of the value-log record
and the index entry** — *not* bytes smuggled in the value prefix. The earlier
design (and Stage A's `aux_len`/`RecoveredAux`) had the cluster prepend a 13-byte
`hlc|node|kind` header to every value and the engine scrape the leading bytes back
out on recovery. Every awkward edge (the "12 vs 13 vs 16 bytes" ambiguity, the
engine guessing a byte count, the value-prefix coupling) is a symptom of that
smuggling. Effort of touching many call sites is **not** a reason to keep it — the
compiler finds every site and the fix is mechanical with zero correctness risk.

Decision:
- The vlog **record** gains an explicit `version` field (opaque `u128` to the
  engine), written on `put`, read directly on recovery — no value-prefix decode.
- `IndexEntry { ptr, seq, version }`; `Engine::put(key, value, version)` takes it.
- Runs/snapshot/manifest already carry it (`SnapshotEntry`'s opaque bytes); the
  engine sets those from the field. `aux_len`/`RecoveredAux`/`open_with_aux` retire
  (recovery returns `(key, version)` directly).
- The engine **never interprets** the version: it keeps merging by `seq`, which is
  correct because per key `seq`-order == `version`-order (the cluster only `put`s a
  *winning*, newer version — losers never reach the engine — and compaction copies
  only the winner forward). So opaque bytes suffice; no generic `Index<V>` needed.
- The cluster stops prepending the version; `from_recovered` seeds the version
  index from the recovered field. (Promoting `kind` likewise and making tombstones
  version-carrying `Delete`s is the natural completion — deferred as a follow-on so
  this step stays focused on the version.)

Migration: a value-log **format bump** (existing devices re-sync, fine pre-prod).
Landed in two compiling steps — store-layer threading first (cluster keeps the
prefix transitionally), then the cluster cleanup that drops the prefix.

### Suggested sequencing within Stage C

Revised 2026-06-23. The old step 1 ("fold `VersionIndex` into the engine index" as a pure
in-RAM step) is **dropped — it is architecturally blocked** for sorted zones: the engine
index is key-ordered (for range scans) while AE needs hash-bucket partitioning, two orthogonal
partitionings of one keyspace that cannot collapse into a single map (and `Arc` interning
already dedups the key bytes, so the remaining in-RAM "duplication" can't be removed without
regressing AE — see *Anti-entropy*). Out-of-core sidesteps this by carrying the version in the
**on-disk run entry** + a **bucket-digest sidecar** (v5 runs, task 0365: derived from the run
blocks instead), not by merging the in-RAM maps.

1. **On-disk run (SSTable) format + codec** (no integration yet): key-sorted
   `{key → (ValuePtr, seq, version)}` blocks + a block index + a per-run bloom + the
   **bucket-digest sidecar** (`bucket → sorted [(key, version)]` for AE). Pure encode/decode
   with round-trip + corruption tests — the Stage A snapshot codec is the template. Foundational
   to everything below; de-risks the on-disk data model with zero behavior change.
2. **Run writer/reader over the device** (allocator segments + batched reads, reusing the Stage A
   `snapshot_io` chain/commit discipline): write a run, read it back, point-lookup with the
   bloom + block index.
3. **Bounded memtable + L0 flush + manifest** (runs read-only, no compaction): the in-RAM index
   becomes the size-capped memtable (carrying the version); at cap it seals and flushes to an L0
   run via the seal-and-handoff rotating memtable. Recovery loads the manifest + runs (the Stage
   A snapshot generalizes into the manifest). Lookup = memtable → bloom-filtered runs by
   `argmax(Version)` (see *Correctness obligations*).
4. **Run compaction** (on-core cooperatively-chunked first), co-compacted with the value log,
   with the space-amp override. Where compaction-vs-foreground p99 is measured — do not ship on
   the slack signal alone without that A/B.
5. **AE phase-1 over the bucket-digest sidecars** + the leaf-accumulator audit; box A/B under
   churn *with* (4).
6. **Block cache + bloom bits/key tuning**; true off-core maintenance *iff* (4)'s chunking
   proved insufficient.

Every step keeps the consistency contract invariant (the obligations above) and is measurable
against the valsmix/kvchurn SPDK harness.

### C.5 implementation plan — full out-of-core (decided 2026-06-23)

C.1–C.4 landed the on-disk run format/codec, the device run writer/reader, the manifest codec,
the version-as-record-field, and (C.5a-1) the generic durable chain + manifest persistence. C.5
is the *bounded memtable* (doc step 3). **Decision: go fully out-of-core, the correct end state —
no throwaway full-in-RAM version index.** The engine index becomes the size-capped memtable;
flushed keys leave RAM; the **version is sourced from the memtable-or-runs** (the cluster's
separate full `VersionIndex` is *not* kept as a step-3 placeholder that step 5 would rip out).
That pulls the store-if-newer device-read (obligations #2–#5) into C.5, so it is built in tested,
compiling stages:

- **C.5a-2 — engine out-of-core core.** `Engine` holds `runs: Vec<OpenRun>` (each = `RunMeta` +
  an open `DeviceRun`: bloom + block-index resident, blocks/sidecar on device). `flush(opts,
  bucket_of)` builds a key-sorted L0 run from the memtable (`build_run`/`write_run`), commits the
  manifest (`write_manifest`, new-before-old free), opens the run, and **drops the flushed entries
  from the in-RAM index**. Reads merge memtable → bloom-filtered runs by **`argmax(version)`**
  (obligation #1): `get`/`multi_get`/`scan_bounded` and a new fallible `version(key)` /
  `current_version`. Recovery loads the manifest + opens runs + replays only the vlog tail
  (`epoch ≥ covered_epoch`) into the memtable; the **snapshot is retired** (recovery via
  manifest+runs). All vlog segments stay allocated (runs point into old segments). Engine-level
  tests on `MemBlockDevice`. **(landed.)**
- **C.5a-2b — ordered scan over runs (landed).** A lazy memtable+runs k-way merge by
  `argmax(version)` over `[lo, hi)`: `DeviceRun::block_scan`/`RunScan` reads run blocks on demand
  (binary-search to the straddling start block; O(page) device work, not O(run)), shared by
  `scan_bounded` (batched value reads) and the borrow-free `scan_plan` (`ReadPlan` per winner).
  `EngineError::ScanOverRuns` retired. Run-vs-run version-tie precedence matches `lookup_runs`
  (newest run wins) so a scan and a point read never disagree on the winner.
- **C.5a-3 / C.5a-4 — retire the per-key in-RAM version index (LANDED).** The cluster's
  `VersionIndex` per-key `BTreeMap`s are **deleted** (the RAM-relief payoff — flushed keys now hold
  *no* in-RAM per-key footprint); only the tiny per-bucket XOR **leaf accumulator** stays.
  `current_version` → `engine.version` (memtable RAM hit, else a **synchronous** run device read,
  fallible → non-ack, #4; kept sync so store-if-newer/CAS have no `await` between check and store,
  #2/#5). AE digest serve → `engine.version_digest_page`; `bucket_entries`/`entries`/`keys`/
  recovery's leaf-rebuild → `engine.recovered_versions` (memtable + run sidecars — also fixes a
  C.5a-2 latent bug where these saw memtable-only). The leaf accumulator is maintained without the
  map: a winning write XORs out the `old` the LWW read already fetched and XORs in the new; recovery
  rebuilds it from `recovered_versions`; review verified it stays exact across put/overwrite/delete/
  flush/GC/clear/recovery. `pack_version` repacked hlc-high so the engine's `argmax` = LWW winner.
  Subagent-impl + adversarial review (verified: leaf exact, error paths fail-closed/non-ack,
  store-if-newer/CAS stayed sync/atomic) + self-review. **Known limitations (tracked):**
  (F1) point `Read` + the `Digest`/`SyncBucketDigests` serve still do a *synchronous* run read for
  flushed keys on the connection driver — the **async overlapped two-stage serve** (value + digest)
  is the next increment, to **measure on the box** (the cold-churn read-before-write the design
  flags as the #1 risk); (F2) `forget`/`compact` can't shed/GC *flushed* keys while runs exist
  (`compact_filtered` no-ops with runs) — leaf-safe (drops nothing, retires nothing), so over-
  replication cleanup + tombstone GC stall post-flush until **C.6** run-aware co-compaction.
  **F2 CLOSED (task 0038):** kind-promotion + tombstone GC over runs + `Engine::forget_keys` make
  `forget`/cleanup and tombstone GC effective over flushed keys — see the C.6 entry below.
  Obligation #3 (tombstone-GC watermark) **DISCHARGED (task 0038)** via the `apply_at` admission
  floor — see obligation #3 above.
- **C.5a-5 — async overlapped serve over runs (F1 fix, LANDED).** `Read`/`Digest`/
  `SyncBucketDigests` now read flushed keys via the async device path **off the store borrow**:
  stage 1 (RAM, brief borrow) gathers borrow-free `RangeLocator`s (pre-resolved segment LBAs) via
  bloom + block-index; the borrow is dropped; stage 2 reads run blocks/sidecars `async`; stage 3
  re-borrows briefly to build the value `ReadPlan`. `Digest`/`SyncBucketDigests` moved to spawned
  overlapped tasks (like `SyncValues`). Sync and async share `merge_digest_sources` /
  `parse_want_bucket_digests` so results can't drift; review verified no borrow is held across an
  `.await`, async==sync equivalence (argmax/paging/tombstone/error), no TOCTOU (flush only adds
  runs; compact no-ops with runs, so captured LBAs/ptrs stay valid), errors stay non-ack, and the
  `reads_inflight` gauge is balanced. **Deferred to box-tuning:** `RangeLocator::read` uses a plain
  buffer, so on real SPDK it takes the DMA bounce-buffer path — thread a pooled `Scratch` if the
  box A/B shows serving-core copy overhead matters.
- **C.6 — run-merge compaction (LANDED; tombstone GC + F2 landed in task 0038; vlog co-compaction LANDED in task 0028).**
  `Engine::compact_runs(opts, bucket_of, gc_collect_before)` k-way-merges all of a zone's runs into
  one by `argmax(version)` per key (drops superseded older-version copies), commits the new manifest
  (head-last, watermark preserved via a new `last_flush_seq`/`last_flush_epoch` carry), then frees
  the old runs' data segments (new-before-old; crash-safe — verified with a torn-manifest-commit
  test that falls back to the prior generation). Bounds run count / read-amp / per-run RAM
  (bloom+block-index). Triggered from the node maintenance loop whenever the engine's tiered
  selector (`Engine::recurring_merge_selectable`, task 0145) has an admissible suffix, with
  `RUN_COMPACT_THRESHOLD` (8) runs kept as a forcing backstop.
  **Read-transparent + leaf no-op with GC off:** only non-current entries drop, so no key's current
  version changes — `get`/`scan`/digests/`version_leaf_digests` are byte-identical across a merge
  (tested).
  Subagent-impl + adversarial review (verified watermark preserved across compaction AND recovery,
  merge argmax/all-entries/key-ordered, crash-safety/new-before-old/no live-segment free, leaf
  untouched) + self-review.
  **Tombstone GC over runs + F2 (task 0038, LANDED):** run entries now carry the caller-tombstone
  flag (**kind-promotion** — packed into the top bit of the block entry's `suffix_len`, footer
  bumped to `RUN_VERSION=2`; the vlog record header's reserved `flags` byte carries `FLAG_TOMBSTONE`
  so recovery reconstructs it). `compact_runs`/`compact_plan` take a `gc_collect_before` version
  cutoff: an aged tombstone winner (`version < cutoff`, cutoff = `min(wall, high_water) − grace`) is
  omitted from the merged run and reported (only when it was the key's *current* version — filtered
  against the memtable + survivor runs) so the cluster retires the Merkle leaf. Safe because the
  merge is bottommost (all runs) and store-if-newer keeps any memtable/survivor copy strictly newer;
  the obligation-#3 admission floor (above) closes the straggler-resurrection vector. **F2** —
  `Engine::forget_keys` merges the runs omitting the handed-off keys and tombstones any memtable
  copies (deferred while a flush/compaction is in flight); `Replica::forget` uses it once runs exist
  (the old `compact_filtered` path stays for the no-runs case), so over-replication cleanup now sheds
  *flushed* keys.
  **Value-log co-compaction (task 0028, LANDED):** the off-core run-merge now co-compacts the value
  log **in key order**. `compact_plan` reserves fresh value-log output segments (bound
  `2·⌈Σ value_bytes / usable⌉ + 2` — the old + new copies resident transiently, like `compact()`)
  and assigns them fresh epochs; `compact_and_write` walks the merged winners in key order, reads each
  live value **span-coalesced** from the old segments over handle-B, and appends it to the new
  segments through a `vlog::VlogWriter`, repointing each run entry's `ptr`. After one pass the bulk of
  a zone's values are physically key-ordered, so `plan_read_spans` turns any ordered scan over
  compacted data into large sequential reads (an in-repo A/B: a 600-key shuffled corpus dropped from
  184 to 10 device spans over 64-key scan pages, ~18×). GC-dropped/superseded winners are never copied
  — that **is** the space reclaim (an overwrite-heavy corpus reclaims ≥½ its segments, verified across
  a reboot). Crash-safety: output segments carry a **not-tail** header marker
  (`vlog::SEG_FLAG_NOT_TAIL`, the high bit of the segment-header version) so recovery never replays
  them into the memtable; original record `seq`/`version`/tombstone are preserved (a fresh seq could
  resurrect a key on tail replay), only `epoch` is refreshed. Liveness is now **manifest-driven**:
  `RunMeta` carries `value_segs` (the distinct segments a run's entries point into) + `value_bytes`
  (manifest `v2`, back-compatible — a `v1`/legacy run decodes them as `None`), and recovery marks
  allocated = manifest chain ∪ run segs ∪ every run's `value_segs` ∪ the replayed ordinary tail
  (epoch ≥ covered_epoch), leaving everything else FREE — so a co-compaction's dead old segments (and
  a *crashed* pass's partial output) are reclaimed on reboot without header-wiping. The rule is active
  only when **every** run carries `value_segs`; a legacy manifest falls back to the conservative
  "every header-valid log segment is allocated" rule (never mixed).
  **Hot-key orphan-segment leak (task 0086 and follow-ups, FIXED):** superseded-before-flush
  value segments can be named by no run. `protected_segments` now includes every run/manifest,
  the open and retained tail, an in-flight flush's value segments, and every mutable-index value
  pointer; `reclaim_orphan_segments` can therefore sweep any allocated segment outside that complete
  keep set without waiting for reboot or co-compaction. Current recurring merges are index-only.
  Full key-ordered value clustering is
  **opt-in only**: `VALS_VLOG_CO_COMPACT=1` enables it, while an unset variable (or any value other
  than `1`) leaves it off. Space reclamation no longer waits for a multi-run clustering pass: the
  pressure path first runs the unconditional orphan-segment sweep, then
  `aged_vlog_gc_candidate` / `plan_aged_vlog_gc` can rewrite victim segments from an aged run,
  including a lone run, when dead bytes and net-gain/headroom checks qualify it. Cooperative
  chunking remains contingent on a measured p99 need. Box/SPDK validation of the aged-zone scan
  speedup is split to task 0042.
- **C.5b — auto-flush trigger + bounded sealed memtables** (seal-and-handoff, bound K). (Flush is
  already auto-triggered by the node maintenance loop on the tail-epochs threshold; this bounds the
  in-RAM sealed-memtable backlog so a flush never stalls writes. Lower priority — an availability/
  write-latency refinement, not a correctness or RAM-relief gap.)

Merge precedence is **`argmax(version)`** everywhere, never newest-level/`seq` (obligation #1, #6).
The opaque record version (`pack_version`) is packed **hlc-high / node-low** so its `u128` order
equals the cluster's LWW `Version` order — the engine's `argmax(version)` over runs is then the
LWW winner without the engine decoding it (a review of the digest source caught a node-dominant
packing that made multi-node run-merges resolve to the stale entry; fixed + regression-tested).

### C.5/C.6 status — complete, reviewed, box-validated (2026-06-24)

The out-of-core LSM is **functionally complete and committed** (C.4b(2/2) → C.5a-1 → C.5a-2 →
C.5a-2b → C.5a-4-foundation → C.5a-3/4 → C.5a-5 → C.6). A final **holistic review** across the
increment seams returned *safe to ship* (recovery correct across flush+compact+tail+crash; the
Merkle leaf accumulator is a no-op across flush/compact and exact vs `recovered_versions`; argmax
precedence identical across all six merge sites; no mis-wired memtable-only/snapshot path; the
deferrals open no resurrection vector). **Box validation** (multi-node, RF=3, file backend): **0
consistency errors** under churn *and* a post-recovery matched read-verify; flush + **run
compaction** both exercised; a restarted node recovered via manifest+runs+tail and served
consistently. The validation **empirically confirmed** the serving-core stall from *synchronous*
flush/compaction under frequent maintenance.

**C.5b (non-blocking flush) — LANDED.** The flush is now three phases so the multi-second QD1 run
write no longer monopolizes the serving core: `Engine::flush_plan` (brief borrow — `log.sync`,
snapshot the memtable into a sorted `Vec`, `build_run`, reserve+resolve the run's segment LBAs)
→ free `write_flush` (**off the store borrow** — `write_run_segs` writes the run chunked,
`vals_rt::yield_now().await` every `FLUSH_WRITE_BATCH_SEGS=4` segments so serving interleaves) →
`Engine::flush_commit` (brief borrow — open the run, write the manifest = the atomic commit point,
install it, **version-recheck-drop** the flushed keys). The memtable stays authoritative for
serving throughout; the in-progress run is invisible until the manifest names it; no `RefCell`
borrow is held across the `.await`. **Version-recheck drop:** a snapshotted key leaves the memtable
iff its live `seq` still equals the snapshot's (unchanged) — a key overwritten during the write has
a newer `seq` and is *kept* (newer than the run copy, flushes next round), keeping reads
`argmax(version)`-correct. A cluster delete is a tombstone-*put* (higher-`seq` entry) so it is kept
too; a direct `Engine::delete` (index *removal*) must not interleave with a flush (it would not be
shadowed by the in-flight run → resurrection) — guarded by `flush_in_flight` + a debug-assert, and
the cluster never engine-deletes. Crash-safe: `flush_plan` mutates no committed/durable state (only
`log.sync` + segment reservation + clone); a crash before `flush_commit` leaves the prior manifest
live and the reserved-but-uncommitted segments reclaimable (no manifest names them; `free_flush_segs`
returns them now on a write error). The Merkle leaf accumulator is untouched. Tests:
`nonblocking_flush_then_tail_recovers_all`, `flush_keeps_keys_overwritten_during_the_flush`,
`flush_plan_reserves_segments_freeable_on_abandon`, cluster
`nonblocking_flush_is_read_transparent_and_leaves_leaf_unchanged`. Reviewed adversarially (the
delete-resurrection contract hole was found here and closed).

**Box validation of C.5b (2026-06-24) — correct, but the SPDK stall A/B is inconclusive (honest
finding).** Across file-backed and SPDK clusters, HEAD vs the synchronous-flush parent, read-dominated
churn with `VALS_FLUSH_TAIL_EPOCHS=4`: **0 consistency errors everywhere**, flushes fire and recover,
reads stay served (file backend: HEAD ≈ parent, no regression on the fast path where page-cache hides
write latency). **But the SPDK A/B could not isolate — let alone demonstrate elimination of — the
flush stall**, for two structural reasons: (1) **flush shares the maintenance-loop timer with
anti-entropy** (`snapshot_loop` runs on the `sweep` cadence; `SWEEP=3600s` ⇒ `snapshots=0`), so on
SPDK every run that fires flushes *also* fires 5 s AE sweeps, and AE's synchronous reconcile reads are
the dominant single-serving-core read-starver (see [[vals-hotspare-1b-and-cluster]]); both HEAD and
parent showed ~12–14 s read-collapse windows dominated by AE, not flush. (2) SPDK busy-poll gives high
run-to-run variance (baseline read rate swung ~3×), swamping the flush signal. **Conclusion:** the
non-blocking flush is a *correct, necessary prerequisite* — it removes the borrow-across-`.await`
hazard, keeps reads transparent, and moves the multi-second QD1 *device write* off the store borrow —
but it does **not by itself** eliminate the empirical serving-core stall, because (a) `flush_plan`
still runs `build_run` (O(memtable) CPU) + `log.sync` under the borrow, (b) `write_flush`'s chunked
SPDK writes still busy-poll the *serving* core between yields, and (c) the bigger starver (AE
reconcile) is also on that core. **Real stall elimination needs maintenance *off* the serving core**
(seal-and-handoff to a separate thread/reactor for flush; the same for AE reconcile and compaction) or
more serving cores (`SHARDS`, already measured 2–2.7× in [[vals-spdk-perf-multicore]]) — that is now
the highest-priority follow-up, ahead of the cosmetic items below.

**C.6 compaction is still
synchronous** — cooperative-chunked compaction is the next refinement (rare at the default 256/8
thresholds, real under sustained maintenance). Other tracked follow-ups (perf/space, not correctness):
vlog co-compaction (space reclaim — the last piece: tombstone-GC-over-runs + F2 forget/cleanup
**landed in task 0038** via kind-promotion + the `apply_at` admission floor), block cache + bloom-bits
tuning (step 6), and a pooled DMA `Scratch` for `RangeLocator::read`. The lazy boot leaf-rebuild
**landed in task 0005**: a restart no longer reads every run sidecar to rebuild the cluster Merkle
leaf accumulator — a store with runs boots *unseeded* and folds `recovered_versions` in once, on the
first anti-entropy leaf touch (fail-closed; the seed recomputes from the engine's crash-recovered
keyset, so it stays exact vs `recovered_versions`).

---

## Provenance

This design was reviewed 2026-06-21 by three independent passes (correctness/consistency,
performance/execution-model, and scope/alternatives). The restructure into Stages A/B/C, the
"AE is not free" correction, the "off-core is net-new infra" correction, the correctness
obligations, and the break-even gate all came out of that review. Earlier single-stage framing
is preserved in git history.

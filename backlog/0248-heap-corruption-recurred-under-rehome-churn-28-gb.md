---
id: "0248"
title: 'Heap corruption RECURRED under rehome churn — 28 GB core captured (glibc free(): invalid next size)'
status: done
priority: 1
created: "2026-08-24"
updated: "2026-10-12"
depends_on: []
spec_refs: []
---

## Description

Current triage (2026-09-29): remains P1 and blocked on hardware diagnosis/qualification or an explicitly selected known-good platform. Preserve the latest memory-failure and bit-flip evidence below. The next useful step is an attended hardware isolation/repair decision, not another open-ended software holder hunt or an unqualified long soak. Earlier hypotheses, live-arm descriptions and temporary paths in this chronological record are historical and may have been superseded; they are not current fleet state or standing run authorization. 0336 separately owns the retained R2 pointer-corruption evidence.

### Original incident report
2026-08-24 22:15Z, lab-1 (node 1), binary at HEAD 85ca7719 (0245-tripwired), ~6.5h into the task-0247 cache-record ingest under heavy auto-split/rehome churn (326 rehomes started, 309 sorted ranges, epochs adopting every few seconds): process died with glibc `free(): invalid next size (normal)`. **No 0245 tripwire fired first** — allocator detected adjacent-chunk header corruption at free() before any SPDK tripwire tripped. This invalidates the 0232 recurrence-gate conclusion (5.5h clean gate was too short / lighter churn).

**Evidence preserved on lab-1:**
- Core: `/home/user/vals/run/core.vals-shard.1370950` (28 GB, root-owned, thread `vals-shard` pid 1370950)
- Log: `/home/user/vals/run/node-0248-crash.log` (crash at tail; 2948 range-map adoptions over run)
- Binary: `/home/user/vals/target/release/valsd` built 2026-08-23 18:08 from 85ca7719 (keep this exact binary for symbolized core analysis)

Analysis start: `gdb ~/vals/target/release/valsd ~/vals/run/core.vals-shard.1370950` on-box (needs sudo for root-owned core), thread apply all bt, then inspect the corrupted chunk neighborhood.

Context: same family as 0232 (`double free or corruption` 2026-08-22, also under rehome churn). Crash coincides with off-core maintenance merges + continuous rehome epoch adoption.

## Acceptance criteria

## Work log

### 2026-10-12 — closed: owner will not chase faults on the lab hardware
Owner decision: don't investigate memory issues or crashes on the lab boxes (non-ECC RAM with memtester-proven bit errors, not being replaced). Soaks keep the record-and-continue rule; persisted or published corruption is still a stop. The evidence hold on cores and binaries ends; disposal goes through 0258. If heap-corruption aborts appear on qualified (ECC or memtester-clean) hardware, file a new task citing this record. Note: not every signature was a single-bit flip (the 2026-10-03 FULL6-LAZY ASCII free-list scribble), and R4 crash 6 aborted while dropping a `RangeMap`, the path 0420 is reworking.

### 2026-09-20 — lab3 RAM comparison failure confirmed; quarantined

User-authorized Sep19 background32GiB test ended FAIL_MEMORY at22:57:16Z after16m12s. Full32GiBmlock verified;10patterns ok, Block Sequential testing112 failed with `0x7070707070707070 != 0x7070707070706070 at offset 0xc189d838` (XOR0x1000, bit12). It entered Checkerboard before the5swatchdog stopped it, so Checkerboard is NOT the failure site. Zero complete passes. Allocation-relative offset is not a physical DIMM address. Tester−15 fromownedwatchdogSIGTERM, supervisor2/unitfailed; independently confirmedgone Sep20 06:33–06:36Z, no newcores/no retry.194samples minimumavailable13,890,408,448B, maxsampled80C/no critical thermal event. Five original logs/receipts2,093,586B preserved/read-only copied with hashes; protectedcore2308180stats and matchingbinarySHA unchanged. See `bench/0248-ram-check-20260919/RESULTS.md` and SOURCE-RECEIPTS.json. Definite unreliable memory behavior on3; DIMM/controller/platform attribution and relationshiptoearlierheap/compilerfaults notproved. Keep3outof0296 benchmark; nextmaintenancewouldbeofflineRAMtest/DIMM isolation underseparateauthorization. No reboot/BIOS/config/cleanup action taken. Task remains unresolved, blocked on hardware diagnosis.

### 2026-09-19 — user-requested background lab-3 RAM test started

User explicitly requested a background RAM test after the unrelated rustc GPF/load-test corruption discussion. Started `vals-ramcheck-20260919t2234.service` at22:41:03Z using the previously verified memtester4.7.1 binary (SHAf8c4465f…8ae8),32GiB,2complete passes,8h watchdog/8h05m systemd cap. Verified active PID2494840 and full34,359,738,368byte mlock; available~13GiB, no critical thermal indication, reserved hugepages4096/4091 unchanged. Five-second memory/swap/thermal/workload/lock/error guards; no restart. No reboot/config/hugepage/disk/corpus/cache/forensic operation or package install. This is RUNNING, not a pass or hardware clearance; previous45min/zero-pass test remains unchanged historical evidence. Raw status/logs on lab3 `/home/user/ram-check-20260919T2234Z/`; source/scope in `bench/0248-ram-check-20260919/README.md`. Keep lab3 out of vals/bootes/compiler workloads while test runs. The remote service survives SSH disconnection; a local notification watcher was detached to avoid holding the workspace writer lock, not to stop the tester.

### 2026-09-15 — bounded lab-3 online RAM check incomplete, no errors in partial coverage

With vals/PD/replay stopped, one authorized 32 GiB ordinary malloc+mlock memtester invocation ran on lab-3 under a five-second resource/process/thermal watchdog. All 32 GiB locked; 13 patterns completed `ok`, then the hard 45-minute guard terminated Bit Flip at setting32. Result is **INCOMPLETE (0 full passes), not a clean pass**; no memory error was observed in completed coverage. No second pass/retry followed. DMI confirms two non-ECC 32 GB G.Skill SODIMMs at4400 MT/s and no EDAC memory-controller telemetry. Exact interval kernel deltas had zero MCE/EDAC/RAS/ECC/memory-error/IOMMU/thermal/OOM matches; the known Thunderbolt root-port AER storm continued and is not memory telemetry. Core2308180 and matching daf4 binary stats remained exact, tester/diagnostic unit fully reaped, fleet stayed stopped, and no reboot/config/install/update/disk/core/corpus/source action occurred. Package provenance, partial pattern coverage, limits, watchdog, telemetry and limitations: `bench/0248-ram-check-20260915/RESULTS.md`; raw artifacts retained on lab-3 at `/home/user/ram-check-20260915T0534Z/`.

### 2026-09-14 — user-authorized phase17 core triage, initiating cause unresolved

Read-only subagent sol job99 examined lab-3 core2308180 from task0294's failed long candidate arm. Exact executable hash and core-memory executable/libc/loader/DPDK EAL Build IDs match. SIGABRT originates in munmap_chunk while published-seed cleanup frees a SeedWinner.key. Assembly plus a21-record neighborhood reconstruct stored pointer0x7b2eea71a530 versus expected0x7b2eea71b530 (XOR0x1000, bit12 clear); expected key and plausible0x50 chunk remain present. Wrong pointer causes libc to read key text as chunk metadata. This establishes the immediate detection mechanism, not the initiating writer/cause. Similar to prior48-byte-record key-pointer bit clears, but different bit/cache-line offset (12/0x11 versus historical42/0x15); no hardware/software/candidate attribution or revival of withdrawn key-suffix-writer theory. No whole-vector census yet. Fleet stayed stopped; no restart/recovery/reproduction/cleanup. Full evidence/provenance/limitations and next bounded core-only census proposal: `bench/0294-ingest-optimization/phase17/20260914T171300Z-long/CORE-TRIAGE.md`. Core and matching daf4 binary preserved. Validation remains paused; task unresolved.

### 2026-09-07 — fresh task-0247 replay reproduces on current source

A coordinated, source-pinned fresh replay for task 0247 reproduced the allocator-corruption failure on lab-3 after 7m12s and ~17M events, before any capacity rejection. Current binary `1183d60a…a2f5104` (valstore `2837f7c6`, production code at parent `3362328e`) ran stock glibc with strict SPDK affinity checks. PID 1702165 exited `status=11/SEGV`, 7.4 GiB peak. New 4,129,230,848-byte core `/home/user/vals/run/core.vals-shard.1702165` (SHA-256 `191e5f79…529676ac`) faults in `_int_free_merge_chunk` from `VersionedStore::seed_step` with impossible size 3,630,520,837,777,552,432; another shard was concurrently freeing from `seed_step`. Core and matching `~/vals/valsd-2837f7c6-0247` remain on lab-3. This is a fresh recurrence, not a capacity wall, and reinforces that 0248 must remain unresolved/blocked. Exact setup, metrics, stack receipt, and stopped fleet state: `bench/0247-cache-replay-20260907/FINDINGS.md`.

### 2026-09-06 — owner pauses validation and releases fleet

Owner explicitly requested stopping the 0248 validation to unblock the cluster, then authorized wiping the cluster's test data. The protected fleet/allocator-arm reservation is released for coordinated hardware gates; rebuilds, restarts, allocator normalization and fresh-format test setups are authorized. Preserve forensic cores, matching binaries, logs/metrics and sealed replay cache. Heap corruption remains unresolved; this is a pause, not a pass or closure. Do not automatically resume the 0248 fleet validation while other gates own the cluster. Operational stop receipts and final state: `bench/0248-pause-20260906/FINDINGS.md`. BIOS maintenance remains separately attended; this permission does not resolve 0250.

Stop completed 04:36 UTC: replay exhausted its 300-second drain deadline and withheld final cursor; node1/node3 exhausted their 300-second systemd stop deadlines and were SIGKILLed (not new corruption crashes); node2 was already down. PD and all scrapers stopped. Failure journals preserved, failed transient units reset; final checks confirm no valsd process and no loaded vals units on any host. No drives formatted or PD state erased. Status `blocked` means owner-paused validation; the fleet is released, not reserved by this task. Resume validation only by an explicit scheduling decision, not automatically when gates finish.


## Update 2026-08-29 — THIRD instance, now with a fast repro candidate

Phase-B cache replay (task 0247: fresh 3-node cluster, `--jetstream-cache-replay` of the fixed 1B-event corpus at full sink rate, ~68k physical writes/s cluster-wide): **lab-3 node died 6 minutes in** (00:22:13Z) — `status=11/SEGV`, `Result=core-dump`, 4.6G peak RSS.

- Core: lab-3 `/home/user/vals/run/core.vals-shard.1378585` (**2.4 GB — much smaller heap than the 28 GB Aug-24 core; best analysis target**)
- Log: lab-3 `/home/user/vals/run/node-0248-crash2.log`
- Same binary (HEAD 85ca7719). No tripwire, no abort message — raw segfault during normal-looking off-core flush/merge + epoch adoption activity.

Three manifestations so far, all under sorted-zone ingest churn: SIGABRT double-free (0232, 8-22), `free(): invalid next size` (8-24, 28 GB core), SIGSEGV (8-29, 2.4 GB core).

**Repro lever:** the replay workload is now fixed and local (72 GB cache, no network/quota). Fresh format + replay from seq 0 crashed a node in ~6 min on first try. If that rate holds, gdb-loop iteration and an instrumented binary (ASan/heap tripwires) become practical on real hardware.


## Core analysis 2026-08-29 (both cores, gdb)

**Aug-29 SEGV core (lab-3, 2.4 GB) — the victim view:**
- Crash in glibc `_int_malloc` smallbin→tcache stuffing: a 336-byte-class bin link was `0x000102010001ca80` — structured binary (u32 0x1ca80=117376 + u16 0x0201 + u16 0x0010; ValuePtr/version-entry shape), not text/pointer.
- 336B class = wire-decode buffers (`Reader::list`; two other shard threads were mallocing 320/322B in `decode_request` at crash). Chunk payloads in that class: decoded PutBatch backlink entries (at:// keys + `"RefType":"uri"` JSON).
- Thread 2 (same process) was in `seed_step → SidecarVersions::fold_page → SeedProgress::fold_key` at crash.
- SPDK hugepages mapped at 0x2000_0000_0000 (vfio/IOMMU) — device DMA into glibc heap effectively excluded. 0245 affinity check was ON and silent. 0245 aio tripwires (magic/underflow) silent.

**Aug-24 abort core (lab-1, 28 GB) — the culprit view (much stronger):**
- `free(): invalid next size (normal)` raised FROM `seed_step` itself: `_int_free_merge_chunk(p=0x7324ddf9c7f0, size=1200)` via `VersionedStore::seed_step → ensure_seeded_cooperative_cell → sweep_loop`.
- Memory at/around p: a **contiguous stream of concatenated backlink keys** (`at://did:plc:…/app.bsky.feed.like/<rkey>` + NUL + `subject.uri` + …) running linearly THROUGH the next chunk's header — classic linear overflow/spray, and the format matches the **sidecar v3 body layout** (u32 klen + key + u128 version). The Aug-29 garbage link value is consistent with a u128 version fragment from the same stream shape.
- p's OWN header was re-scribbled between free() reading the size (1200) and the core snapshot — **the writer was still actively appending while the process aborted** → a concurrent thread (or reentrant completion) sprays serialized key/version data through a stale/overrunning pointer.

**Code review so far:** `fold_page`/`fold_key` are safe Rust (bounds-checked cursor). Sidecar loads are synchronous (`vals_spdk_read` bounce+memcpy; wait loop is correct). Async `ReadAt` guards buffer lifetime via SubmittedIo::drop spin. Not yet reviewed: sidecar/wire ENCODE paths, maintenance-thread run-build writers, vals-rt waker/reentrancy (sync-read wait loops fire pending async completions reentrantly inside engine call stacks).

**Hypothesis:** a writer of sidecar-format data (key stream + u128 versions) appends through a stale or under-sized buffer pointer under seed/rehome churn; the allocator classes hit (336B, 1200B neighbor) are collateral.

**Next (armed 2026-08-29):** node 3 (lab-3) relaunched with `GLIBC_TUNABLES=glibc.malloc.tcache_count=0:glibc.malloc.perturb=254` under the phase-B replay (6-min first-crash repro). Perturb poisons freed memory with 0xFE: the next core distinguishes write-after-free (spray atop 0xFE) from live-buffer overflow (spray contiguous with a live allocation), aborts earlier, and lands closer to the culprit.


## Crash #4 + instrumentation status (2026-08-29 14:30Z)

- **Instrumented node 3** (tcache=0, perturb=254): **13.5 h clean** vs 6 min stock — supports a timing-sensitive race over a deterministic overflow.
- **Crash #4, node 2 (STOCK), 05:01:32Z** after 4h47m of replay: SIGSEGV **inside seed_step itself** (not in allocator): faulting insn is the inlined FNV-1a `hash64(key)` byte loop (rdx = 0xcbf29ce484222325 basis), fault addr 0x70aae0760230 = unmapped. Registers show r13=17,198,757 / r12=17,201,253 ≈ mem_pos/len paging the ~17.2M-entry `mem_pending` memtable snapshot (matches node's 36.6G peak RSS). All snapshot paths deep-copy (`Box::from`), so the read went through a **corrupted (Box<[u8]>,u128) entry or a freed+munmapped key box**. Core: lab-2 `/home/user/vals/run/core.vals-shard.91534` (20 GB); log `node-0248-crash3.log`. Node 2 relaunched instrumented + `Restart=on-failure`.
- Pattern across 4 crashes: seed_step always live; scribbles always `klen+key+u128` shaped; always during off-core maintenance merges + rehome churn.

## Cleared by review (safe code): fold_page/fold_key, sidecar load (sync read, correct wait), run-build sidecar serialization (safe extends), recovered_versions/index.versions (deep copies), ReadAt buffer lifetime (drop-spin).

## NEW SUSPECT — 0245 blind spot on the cancellation path
`SubmittedIo::drop` (vals-spdk/src/spdk.rs) spin-polls via `poll_submitted_spdk` → `ffi::vals_spdk_poll(dev)` **directly on the raw dev pointer, bypassing `assert_owner_thread`** (which only guards `raw_dev()`). If a future holding an in-flight I/O is ever dropped on a non-owner thread (cancellation during rehome/seed teardown), it spin-processes completions on a qpair concurrently with the owner thread — UB in SPDK: replayed/double completions, `vals_aio_cb` re-firing with recycled DMA slots → `memcpy(op->out, slot)` sprays **another read's content (sidecar bytes!) into a freed buffer**. Matches every signature: content shape, concurrency-with-abort, churn correlation, and why the affinity assert never fired.

**Proposed tripwires (next binary rev, do not disturb current instrumented run):**
1. Assert/count owner-thread in `poll_submitted_spdk` / `SubmittedIo::drop` (same policy as assert_owner_thread, incl. the VALS_SPDK_AFFINITY_CHECK=0 kill switch).
2. C shim: track per-qpair `polling` flag; abort on re-entrant/concurrent process_completions.
3. Keep node 1 stock as crash control; nodes 2/3 stay perturb-instrumented.


## HANDOFF: code-perspective investigation map (2026-08-29)

### Exact code coordinates (workspace @ 85ca7719; box binaries built from this exact commit)
- `crates/vals-spdk/src/spdk.rs`
  - `IoState` + `complete_aio` (~131-150): **`Rc<IoState>` — non-atomic refcount** — reconstituted via `Rc::from_raw` in the completion. Thread-confinement is an ASSUMPTION documented in the SAFETY comment ("the shim completes on the thread that polls the qpair"). Any cross-thread poll makes this UB independent of SPDK internals.
  - `SubmittedIo::drop` (~180-190) + `poll_submitted_spdk` (~192-198): spin-poll on raw `dev` pointer, **no `assert_owner_thread`** — the 0245 blind spot. Whoever drops a ReadAt/WriteAt with in-flight I/O polls the qpair from THEIR thread.
  - `ReadAt`/`WriteAt` futures (~200-330): buffer lifetime guarded only by that drop-spin.
  - `assert_owner_thread` (~409-429): first-use-binds owner; guards only `raw_dev()` (submit path); kill switch `VALS_SPDK_AFFINITY_CHECK=0`.
- `crates/vals-spdk/csrc/spdk_shim.c`
  - `vals_aio_cb` (~403): completion does `memcpy(op->out, op->dma, op->nbytes)` then `vals_dma_put` then `complete(ctx)`. Bounce slots recycled through a LIFO pool (`vals_dma_get/put`, ~45-60, VALS_AIO_SLOT-sized slots; larger reads use fresh `spdk_dma_zmalloc`).
  - `vals_spdk_aio_read` (~430): **`spdk_vtophys(out, NULL)` checks only the FIRST byte** to pick direct-DMA vs bounce. Verify assumptions about what memory is registered (iova mode, spdk_mem_register usage) — if any glibc-heap region ever becomes vtophys-translatable, the device DMAs straight into heap.
  - Sync path `vals_spdk_read`/`vals_poll` (~226-255): correct per-op wait, BUT `spdk_nvme_qpair_process_completions` inside the wait **fires pending async completions reentrantly inside arbitrary engine call stacks** (waker wake → executor enqueue happens under whatever the shard thread was doing).
- `crates/vals-cluster/src/replica/lifecycle.rs` (~110-215): `seed_step` turn structure — memtable snapshot (`index_versions()`), paged folds, `sp.current_run` sidecar retention across turns, `drop(sidecar)` on Corrupt, `runs_epoch` staleness guard.
- `crates/vals-cluster/src/replica/version_index.rs`: `SeedProgress` (78), `fold_key` (144, inlined FNV `crate::ring::hash64` = crash #4 fault site), `SEED_MEMORY_OPS_PER_STEP=4096` (0231).
- `crates/vals-store/src/run.rs`: `fold_page` (760), `sidecar_versions` (2109) → `range` (1884) → `read_run_range` (sync read into fresh Vec), sidecar serialization (~18-42, safe extends).
- `crates/vals-store/src/engine/winner.rs`: `recovered_versions` (644, deep copies); `crates/vals-store/src/index.rs`: `versions()` (275, deep copies).

### Facts any theory must explain (from 4 crashes / 3 cores)
1. Scribble content is always **sidecar/backlink-shaped**: `u32 klen + key-bytes + u128 version` streams; long contiguous runs (>1.2KB through chunk headers); writer STILL APPENDING during process abort (Aug-24 core: freed chunk's header re-scribbled between free()'s size read and core snapshot).
2. Victim allocations are collateral in hot small classes (336B wire-decode list bufs; 1200B chunk; a `(Box<[u8]>,u128)` entry in the ~17.2M-element `mem_pending` Vec → fault addr was UNMAPPED, so either a scribbled pointer field or a freed+munmapped key box).
3. `seed_step` live at/near every crash; off-core maintenance merges + rehome churn always running; crash #1 (0232, Aug 22) predates the 0231 paging rewrite — suspect must exist in pre-0231 code too (or be two bugs).
4. Allocator perturbation suppresses it: stock time-to-crash 6 min / 4h47m; perturb+no-tcache nodes clean >13.5 h ⇒ timing-sensitive race, not deterministic overflow.
5. Device DMA into heap ~excluded (hugepages at 0x2000..., vfio/IOMMU); 0245 aio tripwires (magic, inflight-underflow, pool-overflow) never fired; affinity assert never fired **but see caveat below**.

### Important environment caveat
**All three nodes currently run `VALS_SPDK_AFFINITY_CHECK=0`** (0249 forces it for every format=false recovery boot). Only crash #3 (node 3, fresh format=true boot, 6-min crash) ran with the assert armed. So "affinity assert silent" is strong evidence only for crash #3 — and that assert never covered the drop-spin path anyway.

### Hypotheses with discriminating predictions
- **H1 (prime): cancellation-path cross-thread qpair poll** (`SubmittedIo::drop` blind spot) → concurrent `process_completions` UB → replayed completions memcpy recycled bounce slots into freed buffers. Predicts: a code path exists where a task holding an in-flight ReadAt is dropped off-owner-thread (hunt: rehome abort/replica teardown/executor drop paths in vals-cluster + vals-rt task lifecycle); tripwire in drop-spin fires under churn; poisoned-core spray sits atop 0xFE.
- **H2: Rc<IoState> refcount race** (same trigger as H1, different corruption vector): predicts small-chunk (~48B) corruption too — not yet observed.
- **H3: stale sidecar/page pointer in seeding** (pre-0231 seed code path shared): predicts spray SOURCE is a sidecar the node itself recently loaded; check poisoned core for whether sprayed keys belong to a run currently being seeded.
- **H4: wire encode/response path UAF** (unreviewed): AE digest/seed-transfer encode also serializes key+version streams — review `vals_client::wire::encode` + AE responder buffers (0154 bounded these; check buffer reuse).
- Ruled unlikely: device DMA (IOMMU), fold/deserialize logic (safe+bounds-checked), run-build serializer (safe), glibc bug (2.43, three detection modes consistent with app-side writes).

### Repro + analysis rig (all boxes have gdb 17.1 + libc debuginfo resolving)
- Fixed local workload: `--jetstream-cache-replay /data/dataset/jetstream-cache-20260824` (sealed, 1B events, 72 GB) into a fresh cluster per plans/bootes-soak-runbook.md §§1-4 + §5.1; first stock crash came 6 min in at ~68k physical writes/s.
- Cores: lab-1 `core.vals-shard.1370950` (28 GB, abort/overflow view; NOTE `core.vals-shard.1390597` there is the 0249 affinity PANIC, not corruption); lab-3 `core.vals-shard.1378585` (2.4 GB SEGV, smallest/fastest to iterate); lab-2 `core.vals-shard.91534` (20 GB, seed_step fault, r13/r12 = mem_pos/len).
- Open with: `sudo gdb -q -batch -ex 'set pagination off' -ex 'bt 25' -ex 'info locals' ~/vals/target/release/valsd <core>`; malloc internals resolve (libc source lines + locals). Rust frames have symbols, no line info (release, no debug=1) — consider a `debug=1` rebuild for future arms.
- Current fleet arms: node 1 STOCK (crash control), nodes 2+3 `GLIBC_TUNABLES=glibc.malloc.tcache_count=0:glibc.malloc.perturb=254`; node 2 also `Restart=on-failure`. Replay running; every new stock crash = new core; a PERTURBED core is the jackpot (0xFE-poison discriminates write-after-free vs live-overflow).

### Suggested code-agent division
1. vals-rt task/future lifecycle: can a task with in-flight SPDK I/O be dropped on a non-owner thread? (executor teardown, cross-thread spawn/cancel, rehome abort paths in vals-cluster).
2. Full `unsafe` census: vals-spdk, vals-rt, vals-client wire (encode AND decode), FrontCodedIndex, any set_len/from_raw_parts/transmute in vals-store.
3. AE/seed-transfer encode buffers (H4) incl. 0154's bounded digest responder.
4. The C shim + SPDK API contract review (qpair thread rules, completion replay semantics, vtophys/registration assumptions).


## BREAKTHROUGH 2026-08-29 (PM session): mechanism PROVEN from the Aug-24 core — allocator carving bogus overlapping free chunks; "spray" reinterpreted

### Forensic method (all artifacts listed at bottom)
Full thread backtraces of all 3 cores; 64MB dump of the heap containing p (`0x7324dc000000..0x7324e0000000`, crash arena subheap); glibc arena/heap/tcache audit via gdb python; shim `vals_spdk_dev` census via pattern scan of whole cores.

### Key facts established
1. **At every crash instant, NO thread was in any SPDK path** (all maint workers idle in `worker_loop` recv; no `process_completions` anywhere). Multiple shard threads were mid-`seed_step` (fold/hash/ring-walk) — they are the top *allocators*, i.e. the detectors, not the writers.
2. **Shim qpair state healthy in both cores**: 6 qpairs/node (4 serving + 2 maint), zero duplicate bounce slots in every `aio_free` LIFO, `aio_inflight` 0 or 1 (plausible outstanding writes). No physical evidence of completion replay/double-put.
3. **Arena audit (Aug-24 core)**: heap lists coherent, no cross-arena overlap, no foreign tcache/fastbin entries in the P-heap; only thread 1 (the aborting vals-shard) allocates from the crash arena. NOTE this glibc build's `malloc_state` has NO fastbins: `top` at av+0x8, `bins[]` at av+0x18 ⇒ `unsorted_chunks(av) = av+0x8`.
4. **The "spray" around p decodes exactly as glibc `_int_malloc` split-remainder trails, not I/O data**:
   - After p: 372 slots at 0xb0 (176B) stride, each `{size = descending by 0xb0 each slot, |PREV_INUSE, NO non-main bit; fd=0x7329afdd3c80; bk=0x732ae0000038; fd_nextsize=bk_nextsize=0}` — byte-for-byte the `set_head(remainder, size|PREV_INUSE)` + `remainder->bk=unsorted_chunks(av)` + `fd=fwd` + nextsize-NULL stamps of the bin-split path. `bk=0x732ae0000038` **IS** `unsorted_chunks(crash arena)` (verified: av+0x8 in this layout).
   - Before p: same trail at 0x370 (880B) stride with descending remainder sizes ~0x594xx (an earlier carve pass serving ~864B mallocs).
   - The key text in the slots is **pre-existing live data**: thread 1's own 176B `Box<[u8]>` backlink-key allocations (`fold_key`'s `Box::from(key)`, keys ~153B). The trail is stamped THROUGH them.
5. **Crash-arena state (Aug-24)**: `mutex=1` (locked by aborting thread), `unsorted bins[0].fd = 0x7324ddf9c8a0` — a "free" chunk of size 0x495b0 (~300KB) **overlapping ~300KB of live key chunks** right after p; chained to another suspicious free chunk `0x7329afdd3c80` (size 0x67320, ~422KB). `system_mem=0x152e3f000`.
6. p's header history now consistent: p was stamped as a 1200-victim (0x4b5) in one carve pass; seed_step later freed a (legitimately allocated!) 1200-object whose chunk was carved from the bogus region; free read size=1200, `nextchunk` at p+1200 = key text ⇒ "invalid next size" ✓; concurrent/subsequent carve pass re-stamped p as a 176-victim (0xb5 at dump) — carver still running during the abort = the "writer still appending" observation ✓.

### PROVEN mechanism (secondary cascade)
**Bogus large free chunks (~300-422KB) entered the unsorted bin of the shard thread's arena while their address ranges were still occupied by live allocations. glibc then legitimately carved them into 176/880/1200-byte victims to serve seed_step's mallocs, stamping split-remainder headers and bin links across live data.** Every observed signature follows: double free (0232), invalid-next-size (this crash), SEGV on scribbled pointers/unmapped addrs (crashes #3/#4), key/version-"shaped" corruption content (the live key boxes themselves + version-bearing structs), always-under-seed/rehome-churn (seed_step is the heaviest small-alloc churner, so its arena detects first), perturb+tcache=0 suppression (changes reuse order and restores detection), all 0245 tripwires silent (no SPDK involvement in the cascade).

### What the cascade does NOT reveal: the PRIMARY event
Something either (a) double-frees/UAF-frees a large buffer, (b) overwrites a chunk size field so a later legit free inserts an oversized chunk, or (c) corrupts a bin/tcache link (cf. Aug-29 smallbin link `0x000102010001ca80` = structured data in a bin link). Safe Rust can't do (a); candidate scribblers for (b)/(c) are small rare writes (stale `WriteState` atomics, stale `Rc<IoState>` cells, shim `free(op)` family) or an as-yet-unknown bug. The cores post-date the primary event by too long to attribute it.

### Code review verdicts (3 subagents, this session)
- **H1 structurally EXCLUDED as worded**: `ReadAt`/`WriteAt`/`SubmittedIo` are `!Send` (Rc + raw ptrs, no unsafe impls anywhere but spdk.rs); executor tasks are `!Send`, dropped on their own thread; no pointer smuggling (full census). No production path where two threads do I/O on one qpair either (qpair-ownership census: boot→shard sequential handoff; maint siblings per-worker; dump siblings per-thread TLS). Cross-thread waking is eventfd-only (`Notify`); no `Waker` crosses threads.
- **Shim**: single-threaded-clean (ENOMEM retry loops, slot LIFO, batch packing, `s.remaining` all verified). Known weaknesses if a qpair WERE shared: magic tripwire can't catch concurrent both-read-then-free replay; slot LIFO has no in-flight/duplicate tracking; `vtophys(out)` checks first byte only. `lba_count` truncation unreachable (check_range).
- **Wire/AE/seed encode (H4): cleared** — fresh owned Vecs everywhere, no reuse across await, no unsafe in wire; per-connection out buffer is Rc-single-threaded with no retained slices. Wire Version is 12B (not u128) — wire data can't even produce the observed record shape.
- **Only notable unsafe outside vals-spdk**: `vlog::io_buf` uninitialized `Vec::set_len` idiom (UAF *destination* candidate only, all callers bail on Err) — cleanup-worthy, not the writer.
- **REAL BUG found (filed separately): stale maintenance-sibling SubDev on zone-id reuse** — `resolve_handle_b`'s `(owner, zone)`-keyed, never-evicted cache bakes in the region base; zone ids are reused (`Catalog::next_id = max+1`) after drop-zone, so post-reuse flush/compaction output (incl. sidecars) is written to and read back from the OLD region base ⇒ cross-zone on-disk corruption under exactly our churn. Not obviously the heap smasher (disk-side), but it can feed garbage into decode paths and MUST be fixed before further soaks are interpretable.

### Next steps (sharp, in priority order)
1. **Arm one node with `GLIBC_TUNABLES=glibc.malloc.check=3`** (malloc-check: heavy verification, aborts INSIDE the culprit free/malloc with its backtrace). With the 6-min..5-h repro this is the fastest path to the primary event. Keep perturb arms on the other nodes (a perturbed core remains the jackpot for distinguishing scribble-then-free from double-free).
2. Fix + assert the maintenance-sibling zone-reuse bug (independent task; also removes a confound).
3. If malloc.check finds a legit free of a corrupted header: dump the header neighborhood pre-abort — the scribbler's fingerprint (16B? 2B atomics? version-shaped?) discriminates the remaining candidates (stale WriteState/IoState writes vs other).
4. C-shim hardening for future arms (cheap, converts silent paths to aborts): per-dev owner-tid + polling-reentry asserts in vals_spdk_poll/read/write/aio_*/close (survives VALS_SPDK_AFFINITY_CHECK=0); poison whole `op` before `free`; slot-pool membership check on put.

### malloc-check arm live (2026-08-29 15:50Z)
Node 1 (lab-1) restarted under the phase-B replay with **`GLIBC_TUNABLES=glibc.malloc.check=3` + `LD_PRELOAD=/lib/x86_64-linux-gnu/libc_malloc_debug.so.0`** (required since glibc 2.34; verified mapped in the live process) + `VALS_SPDK_AFFINITY_CHECK=0` (first relaunch without it hit the known 0249 format=false-recovery panic — spdk.rs:416 — at 15:48Z; that abort is NOT a 0248 event). Log: `~/vals/run/node-0248-mcheck.log`. Unit env otherwise identical (RAM budget 40G, LD_LIBRARY_PATH), `format=false` recovery, binary unchanged 85ca7719. Prior stock run stopped gracefully at 15:47Z (watermark 1788018428826ms).
Fleet arms now: node 1 = malloc.check=3 (aborts inside the culprit malloc/free with backtrace — the primary-event catcher; stock crash-control sacrificed, user-approved), nodes 2+3 = tcache_count=0 + perturb=254 (node 2 with Restart=on-failure). On a node-1 abort: grab backtrace + core; the faulting free's argument identifies the object whose header/lifetime was violated.

### Metrics collection alongside the repro arms (2026-08-29 16:35Z)
Scrapers live for the whole run: each lab box runs unit `vals-scrape` (`~/vals/run/scrape-0248.sh`) gzip-appending full `/metrics` snapshots every ~55s to `~/vals/run/metrics-0248.promz` (lab-1 also captures PD :9465); devbox user-unit `vals-scrape-driver` captures the replay driver (:9091, `ingest_jetstream_*` incl. `replay_residual_gap` progress) to `bench/metrics-0248-driver.promz` (~20MB/day/node). **Caveat for perf use: ALL nodes have perturbed allocators this run** (node 1 malloc.check=3 ≫ overhead; nodes 2/3 tcache=0+perturb=254) — treat as the labeled "instrumented arm"; rerun the same sealed corpus on stock glibc later for a clean baseline pair (fixed workload makes the curves directly comparable).

### Artifacts
- lab-1: `/tmp/bt-crash1.txt`, `/tmp/p-window.bin`, `/tmp/p-big.bin`, `/tmp/pheap.bin` (64MB P-heap dump), `/tmp/scan_dev.py`, `/tmp/arena_audit.py`
- lab-2: `/tmp/bt-crash3.txt`; lab-3: `/tmp/bt-crash2.txt`, `/tmp/scan_dev.py`
- local workstation: `/tmp/0248/{p-window.bin,p-big.bin,pheap.bin}`
- Key addresses (Aug-24 core): crash arena `0x732ae0000030` (unsorted head pseudo-chunk `0x732ae0000038`), bogus chunks `0x7324ddf9c8a0` (0x495b0) and `0x7329afdd3c80` (0x67320), p `0x7324ddf9c7f0`.

## FORENSIC BREAKTHROUGH #2 (2026-09-04 session, 3 glm tracks + fleet rescue)

### Rig audit — the arms had been starved for ~3 days (exposure correction)
- **All three nodes were restarted Sep 1–2** (node 2/3 ~Sep 1 06:02–06:33, node 1 Sep 2 06:27; deliberate "Stopping vals-node.service" in journal, no cores, no crash) — so the "13.5h+ clean instrumented" numbers were stale: node 1's mcheck arm had only ~2.3 days, nearly all at crawl.
- **The replay has been starved since Sep 1**: driver ~50 seqs/s (vs ~68k writes/s at the 6-min first crash). Cause chain: node 3 fell into the **MAX_SEED_RESTARTS one-shot sync seed fallback** on Sep 1 06:34 (log: "run churn outpacing the cooperative seed; falling back to the one-shot synchronous ensure_seeded") and ground slowly on it for 3 days (perf: FrontCodedIndex::scan_page + Block::decode_pairs); node 2 drifted to the 40G RAM budget (0153-class non-intake growth; RSS oscillating 23–40G, busy_rejections 9k→22k, Busy throttling RF=2 writes cluster-wide); node 1 idle (13.8G RSS, 29G headroom).
- **Fleet rescued 2026-09-04**: node 2 restarted 14:35Z and node 3 ~15:25Z (identical env via systemd-run: perturb+tcache=0, AFFINITY_CHECK=0, budget 40G; both now `Restart=on-failure`; binary unchanged 85ca7719 — NO rebuild, must match cores). Node 1's mcheck arm verified INTACT post-restart (unit env has `glibc.malloc.check=3` + LD_PRELOAD; `libc_malloc_debug.so.0` confirmed mapped in live pid). lab-3 now has 60G RAM (DIMM replaced). Driver resumed (+480k events immediately, then gated again on node 3 pre-restart; expect full churn as node 3 recovers).
- **Lesson**: "N days clean" is meaningless without driver-rate verification — check `ingest_jetstream_events_total` slope, not just unit liveness.

### Track B — SPDK/executor reentrancy + async-lifetime review: EXCLUDED (with line refs)
- **Generator drop-order empirically verified** on this toolchain (rustc 1.96.0): an awaited `ReadAt` future drops BEFORE its destination buffer — combined with `SubmittedIo::drop`'s drain-to-completion, **no glibc destination can be freed before the `vals_aio_cb` memcpy** in any Rust-visible drop path. `SubmittedIo::drop` has NO non-completing exit (would-block loops forever; poll error = abort; submit-failure paths free the op before `SubmittedIo` exists).
- `Rc<IoState>` refcount ledger balanced (strong=2 after submit; `SubmittedIo::drop` can only run after completion consumed the ctx ref) — no resurrection interleaving.
- **Wakers only queue/set flags — nothing runs task code inline** (task waker pushes id to ready deque; flag_waker sets Cell; no RefCell borrow held across poll). So reentrant completions during drop-spins/sync-waits cannot run/drop futures on the same stack.
- Direct-DMA-into-glibc-heap impossible: `spdk_vtophys(out)` only resolves SPDK-registered memory (hugepages); heap Vecs always take the bounce path (confirmed by reading shim.c:438-449; also IOMMU would block). Agent 3's contrary row-9 note was wrong.
- Shim `free(op)` family: single-free per op across completion + both submit-error paths. `vals_spdk_close` aborts on in-flight ops.
- vals-rt RawWakers (0239): unsound in principle, unreachable in production topology (all waker stores thread-confined; cross-thread wake is eventfd Notify only).
- Residuals (hardening, not the bug): drop-spin polls qpair without owner assert (spdk.rs:184); `ReadAt.dev` is a raw ptr with a false SAFETY comment (spdk.rs:193-201) — latent under refactor; unbounded drop-spin = hang on AER/reset.
- Recommended tripwires: (1) thread-local in-drop-spin flag + counter exported as metric; (2) owner ThreadId stored in SubmittedIo, asserted in drop; (3) redirect async-read glibc destinations to SPDK scratch (would make the class vanish if it existed); (4) shim completion registry (op,out,nbytes) check in vals_aio_cb.

### Track C — large-buffer census + 8-byte stamp decode
- **8B stamp `80 ca 01 00 01 02 10 00` decode**: best = **wire frame header** `(len u32 = 117,376; reqid.low u32 = 1,049,089)` (write_framed/push_framed, net/mod.rs:130-139, accept.rs:331-387). The rival PackedEntry reading `{segment=117,376; offset=1,049,089}` is **excluded by cluster config**: node.conf `profile=capacity` does not set `seg_blocks` → default 256 → 1 MiB segments → offset 1,049,089 (513 past 1 MiB) invalid. (But see Track A: stamp may be key_offset-shaped instead — family-level ambiguity only.)
- **Bogus chunk sizes are data-dependent, not fixed I/O buffers**: 0x495b0 = 24B×12,519 exactly (Vec<Vec<u8>> decoded-list/key-snapshot element pitch); 0x67320 = 40B×10,567 exactly (`Vec<(Box<[u8]>,u128)>` = mem_pending/versions element pitch); neither is a 4096-multiple → excludes all fixed-size I/O buffers. Same structure family as the 336B victims.
- **No Rust-side post-free write exposure anywhere** in the census (19 rows: flush/compact snapshots, VlogWriter, merge windows, sidecar bodies, SidecarCursor, io_buf, scratch, mem_pending, recovered_versions, winner arrays, AE pages, conn send/read buffers, wire replies, sweep, snapshot_io, shim). No ManuallyDrop/forget/transmute outside io_buf (capacity-correct, callers bail on Err — verified).
- Mechanism noted: plan/snapshot buffers allocated on the SHARD thread, freed on the MAINT worker → chunks return to the shard arena (how large churn buffers populate the shard's bins legitimately).

### Track A — core forensics: the "spray" theory is DEAD; geometry = double-free/stale-free of large sorted-churn buffers
- **TEST 1 (size fields)**: NOT text/spray-stamped. p itself was the original bogus chunk (split-remainder trail arithmetic: 1706 stamps, first 0x7324ddf9c8a0 (0x495b1) → last 0x7324ddfe59e0 (0x471), span exactly first−last; trail end = p + **0x49660** → the freed chunk at p carried ~300,640B). bogus2 `0x7329afdd3c80`: size **0x67321 with NO NON_MAIN_ARENA bit** despite living in a sub-heap (heap_info ar_ptr=0x732ad4000030 ≠ crash arena) — a legit `_int_malloc` split stamp is impossible there; it was `_int_free`'s `set_head(size|PREV_INUSE)` propagating an already-corrupt even value.
- **TEST 2 (framing)**: no single klen+key+u128 sidecar stream exists — the "contiguous backlink keys" are **prefix-compressed index-Block streams** (`[uvarint shared][uvarint suflen][suffix]`, BlockBuilder::push_packed, index.rs:342-358; parses cleanly for 50+ consecutive records at 3 sites) + whole keys/JSON/PackedWinner 48B entries = **the app's own recycled live data**. Key text only ever flows through prev_size slots (usable payload of the PREVIOUS chunk) — no positive proof of any giant memcpy crossing live chunk headers.
- **TEST 3 (lab-3 smallbin)**: crash = `_int_malloc(320)` in `Reader::list` (wire decode, 336B class ✓) dereferencing bin21 head `bk = 0x000102010001ca80` — **payload bytes propagated into the arena bin head by a normal unlink** from a poisoned free-chunk bk. Victim chunk 0x7f4da793acc0 (size 0x151, properly linked) has prev_size = an HLC version and is surrounded by dense **48B PackedWinner records** `[key_offset u64][key_len u32][PackedEntry 36B]` (winner.rs:70-76) with node=0xfa0 (driver writer id — ordinary corpus data).
- **Unified geometry (proven)**: (1) churn frees large buffers (WinnerDirectory.keys compaction sizes ≈ the 0x496xx/0x673xx/0x5b1xx/0x807xx family; sidecar readback Vecs; packed index blocks) → unsorted bin; (2) malloc recycles them into small live allocations (index blocks, Arc<[u8]> keys, wire-decode buffers) whose content is exactly what we see "inside free chunks"; (3) a **stale second free of the buffer base** re-inserts the region while live: if re-carved, free() reads the small victim size and aborts ("invalid next size" = crash #2, p's 1200→0xb5 re-stamp = carver racing abort); if the big stamp survives, overlapping free chunks → split cascades + payload over bin links (crashes #1/#3/#4). ~250 unsorted + 2,018 large-bin members in the 422KB–16MB family in the crash arena = **many generations of the same buffer family freed repeatedly**.
- **Sharpest new clue**: the bogus "sizes" (0x495b0, 0x49660, 0x67320, 0x5b130, 0x807a0) are all even values that fit **key_offset u64 fields of PackedWinner records** into a ≥526KB keys Vec, and free() stamps `|PREV_INUSE` on top (0x495b1, 0x49661, 0x67321 ✓). Hypothesis: the primary scribbler is a **48B-record-array writer (PackedWinner family) writing through a stale base or with offset/index desync** — landing record fields on chunk size fields; OR the primary is a double-free whose second free's size read is itself a scribbled key_offset. Both need a holder audit (next).
- Artifacts (fresh, on boxes): lab-1 `/tmp/{pheap.bin,p-big.bin,p-window.bin,b2-near.bin,b2-wide.bin,arena.bin,census2.out,arena_census*.py,scan_dev.py,arena_audit.py}`; lab-3 `/tmp/{a3-victim.bin,a3-arena.bin,a3.out,a3_census.py,a3_dump.py,scan_dev.py}`; workstation `/tmp/vals0248/` (+ trail.py, stream.py, framing.py, parse_block.py, hd.py). Arena-chain walk bug noted: `next` at av+0x818 needs a 0x840-wide read (short read returns 0 spuriously) — fix before any all-arena census.

### Standing next steps (post-breakthrough)
1. **Holder hunt (round-2 agent, in flight)**: find the stale handle / double-free / offset-desync for the WinnerDirectory.keys / sidecar / index-block buffer family — specifically any Cell-held index or offset that can go stale across a reentrant single-threaded call while all pointers stay valid (safe-Rust-legal stale WRITE via index desync), any Arc<[u8]> refcount anomaly, any reentrancy that swaps `self.keys`/entries arrays while a fold holds offsets into the old array.
2. **malloc.check=3 node 1 under RESTORED churn** is now the primary-event catcher (arms properly exposed only since 2026-09-04 14:35Z).
3. On the next core: walk ALL arenas (fix the chain-walk) + per-thread tcache around the aborting stack; dump the faulting free's argument neighborhood.
4. Shim/spdk hardening tripwires (Track B list) for the next binary rev — do not disturb current arms.

## CRASH #5 — FIRST PERTURBED CORE (2026-09-04 17:23Z, the jackpot)
- **lab-2, pid 294655** (the perturb arm I relaunched 14:35Z), crashed **2h48m after churn restore** — validates the rig rescue: restored churn reproduces the corruption on an instrumented node in ~3h.
- `vals-shard` TID 294666 SEGV, `segfault at 705f23028790 ip 00005faf6a5c12d0 error 4` (user-mode read, unmapped). Core: lab-2 `/home/user/vals/run/core.vals-shard.294655` (**32.8 GB**). Node auto-restarted (Restart=on-failure, pid 298985) and is serving again.
- **Instrumentation on the crashed process: `tcache_count=0` + `perturb=254`** — freed memory is 0xFE-poisoned, so this core can discriminate write-after-free (data atop 0xFE) from live-overflow, and with tcache off, a small-chunk double-free goes through fastbins where a dup (same chunk twice in one fastbin list) is directly census-able.
- Static holder hunt (round-2, glm) found NO safe-Rust mechanism across all 5 candidate classes (stale-index writes all panic-not-corrupt; plan→worker→commit single-owner on every error path; into_raw/from_raw pairs balanced; no Arc over-decrement; no second-Vec-from-one-allocation). Surviving hypothesis: dynamic free()-with-wrong-pointer or small-chunk double-free → fastbin-dup → consolidation.
- Forensic track on the core launched (glm, 2026-09-04): bt of TID 294666, perturb discrimination, arena+fastbin census, 0xFE-boundary walk for the late-write fingerprint.
- Watcher re-armed with new baseline (node 2 newest core = 294655) + PD liveness check.

### Crash #5 forensic verdict (2026-09-04 PM session) — PRIMARY FINGERPRINT SHARPENED TO BYTES
- Fault site: `seed_step` published-branch drain (lifecycle.rs:72-85 shape), `sp.winners[slot].take()` then `hash64(&winner.key)` — inlined FNV-1a, SEGV reading key bytes. `SeedProgress.winners` is ≥1.65 GB (34M+ × 48B entries); 2926 entries left in the turn's ≤4096 drain. Second shard thread mid-hash of its own winners (same loop family).
- **Perturb verdict: NOT write-after-free, NOT double-free/fastbin-dup** (readable memory: 16,131 bin links verified consistent, 0 bad; 0xFE poison pockets are pristine never-reused large frees; twin winners array of 688k entries completely clean ⇒ damage is LOCAL/sparse).
- **The scribble: exactly ONE byte of a live entry's key-pointer field — byte 5, `0x74→0x70` ('t'→'p', lowercase ASCII)**. The Box itself live/intact (0x80 chunk, 112B key, valid bsky content). No glibc mechanism writes one lowercase byte (stamps are aligned 8-byte; perturb=0xFE; alloc-perturb=0x01).
- **Writer fingerprint: sparse, short, prefix-compressed index-block records (`[uvarint shared][uvarint suflen][suffix]`, BlockBuilder::push_packed format) written through a STALE BASE** — a 1-byte key-suffix landing on ptr+5 (byte 5 is the only pointer byte where lowercase yields a canonical-but-unmapped address — explains all 5 crashes clustering at seed hashing/malloc). Prior cores' bogus "sizes" (24×/40× multiples, key_offset-like values) = the same writer landing record fields on free-chunk geometry; crash-5's crash heap shows the resulting bogus chunks being carved (~0xf00-pitch split-remainder trails).
- **Regime link (the livelock)**: epoch churn every ~5-6 s ⇒ seed reset/reseed every few seconds ⇒ the huge seed structures (winners, mem_pending, sidecar bodies) freed+reallocated at high frequency ⇒ freed VA immediately reused by the next generation ⇒ a late writer with a stale base lands on the NEW live array. Explains churn-correlation, timing sensitivity, locality, and why seed arrays are always both victim and recycler.
- Caveats: core truncated at 32.8 GB — root fs on lab-2 filled (98G disk, 0 free) DURING the dump; thread stacks, crash-arena malloc_state, and the damaged entry's neighbors are permanently lost. Raw core archived (gzip) to workstation `/tmp/vals0248/core.vals-shard.294655.gz` before freeing the box; targeted dumps (a2_crashheap.bin 58MB, a2_boxheap.bin 128MB, a2_winners.bin 128KB) + full script set on workstation `/tmp/vals0248/`.
- Next static hunt (glm, in flight): the stale-base record writer — raw-pointer/index-capture census into winners/mem_pending/WinnerDirectory/FrontCodedIndex across seed resets; the 48B SeedWinner/PackedWinner pitch coincidence; cross-generation VA-reuse writers. **Standing suspicion class: a writer that legally holds a base into seed-generation N while reset drops it and generation N+1 reuses the VA.**

### Round-3 verdict (2026-09-04 late PM) — "the lifetimes are sound; someone is lying"
- **Static census complete (glm)**: NO stale-base writer is constructible in the censused safe code — no raw ptr/index capture into winners/mem_pending/WinnerDirectory/FrontCodedIndex/sidecar bodies survives a reset; every cursor is intra-generation or revalidated at use (WinnerReadToken via token_matches; AsyncRunCursor via storage_generation gates); seed_step fully synchronous across winners/mem_pending writes; shed-path re-checks provably sufficient (chains descend). The 48B SeedWinner/PackedWinner pitch coincidence is a red herring (no shared arena/helper/handoff).
- The census's proposed two-stage mechanism: stage-1 = unsafe root corruption in the shim aio path (unguarded pre-ff2ef26c) → allocator lies → stage-2 = ordinary safe-Rust record writers (BlockBuilder/splice/encode_block) scribbling through allocator-granted aliases.
- **My discriminating checks REJECT all three stage-1 candidates for crash #5** (binary 85ca7719, post-tripwire):
  1. Per-arm build IDs verified: all 3 boxes identical md5 43c1204a…, mtime 2026-08-23 18:08-18:10 — no pre-tripwire arm exists.
  2. Tripwires are STRONGER than assumed: a same-op double-completion hits the magic check (magic zeroed before free(op)); a misdelivered/duplicate completion hits the `aio_inflight` underflow abort arithmetically (+1 per submit, −1 per cb ⇒ any extra cb fires it). **Zero "VALS SPDK FATAL" in every node log across the whole 6-day run incl. crash #5** ⇒ the shim completion path is exonerated INSTRUMENTEDLY, not just by review.
  3. **Zero DMAR/IOMMU fault lines on all three kernels since Aug 29** ⇒ device mis-DMA excluded (IOMMU would log stale/unmapped IOVA; and glibc heap is never DMA-mapped). The 0250 AER storm is confined to the thunderbolt/xhci root port — unrelated to the NVMe path.
  4. Cross-thread qpair poll remains excluded structurally (`!Send`).
- **Theory inverted as a result**: the sparse record writer is the PRIMARY, not stage-2 — a stale-generation write into the seed arrays across reset/VA-reuse (livelock resets seed state every ~5-6 s; the 1.65 GB winners array and mem_pending are freed+regrown at that cadence, so VA reuse by the next generation is near-certain), and the bogus free chunks arise DOWNSTREAM when the same misdirected records land on free-chunk headers (matches the earlier cores: size fields containing record-field values; crash-5's crash heap shows the resulting bogus chunks being carved at 0xf00 pitch). The one-byte 't'→'p' at winners[slot].key+5 is a key-suffix byte from a `BlockBuilder::push_packed`-shaped record written at 48B pitch into the wrong generation's array.
- **How this survives the "no constructible path" census**: the census proved no cursor/pointer ESCAPES a reset in the current code shape — but the writer need not hold a pointer; it may hold a POSITION computed against generation N and apply it after re-borrow of generation N+1 (in-bounds, borrows-checked, silent), or the reset ordering itself has a gap the census's intra-call assumptions miss. The runtime discriminators below settle it without further argument.
- **Runtime discriminators for the next binary rev (with the livelock fix)**:
  1. `SeedProgress` generation counter (static AtomicU64 → field; debug_assert in fold_key/retire/shed against a stack copy) — catches any stale-&mut resurrection.
  2. **Seed poison-on-reset + VA-reuse canary** (the decisive one): on reset, memset first/last 4 KiB of winners/mem_pending/sidecar body with 0xA5 and register (base,len,ts); when the next generation's Vec lands at a registered base, memcmp the canaries before the first legit write — mismatch = the late writer, log VA + byte delta, abort. ~8 KiB memset per ~5 s reset.
  3. CRC-mismatch (`SnapshotError::Corrupt`) rate as a metric — PCIe/decode-health canary, currently uncounted.
  4. mcheck=3 node 1 stays armed (catches the first invalid free if any free-side corruption still precedes the scribble).
- Ops completed: crash-5 core archived to workstation `/tmp/vals0248/core.vals-shard.294655.gz` (3.59 GB, gzip -t OK) and deleted from lab-2; crash-4 core (91534) archiving+deletion in flight — lab-2 root fs freed for future full cores. Watcher v5 running: self-adapting core baselines (deletion-tolerant), disk-low guard (20 GB), PD liveness, driver-stall detection.

### 2026-09-05 takeover — forensic correction (supersedes the primary-writer claims above)

Independent core re-derivation found **the same single-bit clear in crashes #4 and #5 on lab-2**. This is evidence for a hardware/platform investigation, NOT proof of bad RAM and NOT proof that software is correct.

- Crash #5: damaged 48B record at `0x7459d2538cc0`, pointer `0x705f23028790` instead of `0x745f23028790`. Neighbors' key pointers form a +0x80 sequence; length is **0x72 = 114**, not 112. Other fields intact. XOR = `0x40000000000` (bit 42). The damaged entry and neighbors **ARE present** in the truncated core; the previous search used the wrong length (0x70 from a register), then wrongly attributed the miss to truncation.
- Crash #4: damaged record at `0x74a3060f2300`, pointer `0x70aae0760230` instead of `0x74aae0760230`. Neighbors form a +0x50 sequence, lengths 0x41, HLC increments by one. Same XOR bit 42; same flipped-byte cache-line offset 0x15 in both crashes.
- `'t' -> 'p'` is just `0x74 -> 0x70`, a single-bit clear in the pointer's address-space byte. **It does not identify a key-suffix/prefix-compressed writer.** Prior claims of a proven primary stale-base writer, size-multiple object attribution, and timing-race proof from perturb suppression are withdrawn. Allocator-cascade findings describe secondary damage, not the initiating mechanism. Static review/tripwires constrain software hypotheses but do not categorically exclude all software corruption.
- Hardware audit: Core Ultra 9 285HX, G.Skill F5-5600S4040A32G non-ECC DIMMs at 4400 MT/s on all nodes; no EDAC memory controller registered or active memory-error telemetry. Swap active. Clean MCE/IOMMU logs do not clear DRAM/cache/IMC or swap/kernel corruption. Controlled memory tests (ideally physical-memory/offline coverage), repeat signature checks in other cores, and an independent-hardware workload arm are next discriminators; none has yet established hardware causality.
- Re-derivation artifacts: `/tmp/{scan_core_0248,scan91534,census294655,flipcensus}.py`, matching outputs, `/tmp/win{294655,91534}.bin`. Existing compressed cores and matching 85ca7719 binary remain under `/tmp/vals0248/`.

### 2026-09-05 perf/space operations and new crash

- Evidence/run scripts: `bench/0248-takeover-20260905/`. At 05:10:57–05:12:57 UTC replay advanced **0 events**; physical writes ~189/s cluster-wide (includes retries). Node-1 served batch mean **1377.6 ms** vs node-2 **3.95 ms** and node-3 **2.95 ms**. Node-1 shard perf: ~43.6% inclusive cycles in `do_futex`, ~11.3% self `_int_malloc`, ~11.5% self `seed_step`; allocator-debug contention is a major confound. No new RAM-Busy rejection or sync-seed fallback in this window. Node-1 has 931 cumulative seed RAM sheds; not equivalent to 931 fully completed folds.
- User approved node-1 stock-glibc A/B. At **13:51:30** requested stop; old unit exceeded its 90s shutdown deadline and was killed (do not call this a graceful shutdown or a corruption crash). Relaunched **13:53:18**, pid346400, same c4c28276 binary/config, format=false, budget50GiB, affinity kill switch unchanged; **no malloc-check or LD_PRELOAD**. New log `node-0248-stock-20260905.log`, Restart=on-failure, TimeoutStopSec=300, LimitCORE=infinity. Nodes2/3 retain perturb+tcache=0. PD unchanged. Restart/recovery is an A/B confound; do not claim a clean same-state allocator speedup.
- Old lab-1 27,985,620,992-byte core1370950 archived to `/tmp/vals0248/core.vals-shard.1370950.gz`, decompressed SHA256 verified against remote (`fe1a537556b8149bc994b9048f76fe1363f3afe58ff8a6aad3ba1a475d7a4138`), then remote original removed. Three old ~12GB bootes log duplicates per node removed only after SHA256 matched retained workstation files; per-host receipts in bench folder. Free space node1 ~52GiB, node2 ~54GiB.
- **NEW abort on lab-3 at 13:03 UTC**, BEFORE the allocator A/B: pid1600983, new c4c28276 binary, perturb+tcache=0; journal reports status6/ABRT, 12h57m wall, 46.6GiB memory peak, 5GiB swap peak. Core `~/vals/run/core.vals-shard.1600983` ~44GiB, apparently filled disk. Attribution pending gdb; do not yet count it as confirmed heap corruption. Binary preserved locally `/tmp/vals0248/valsd-c4c28276`; core archival in progress.
- Auto-restarted lab-3 pid1630480 is a **zombie serving process**: live gdb at ~13:56 shows NO shard threads, only main waiting on channel, metrics, DPDK, maint workers, admin. All 7001–7004 listener queues full at129/128. Log stops midline after map6709 adoption. Cleanup left6.9GiB free, but shard threads did not return; unchanged-unit restart requested after capturing live stack. This independently blocks throughput and invalidates any simple before/after allocator attribution until rescued.

### 2026-09-05 final takeover results (~14:15 UTC)

- **Crash #6 confirmed allocator abort:** glibc `double free or corruption (out)`, not a Rust panic/tripwire. Core47,057,059,840B is truncated by~1.89GiB of requested dump content; missing high-address tail includes all thread stacks and main_arena. Cannot recover aborting free's argument/backtrace. Readable census:312 thread heaps (~19.4GiB,31.7M chunks), main heap2770 chunks,36 mmap arenas show no established header corruption. Compatible with free(non-chunk pointer), but **not enough to choose flipped pointer vs software wild/double free**. Hardware one-bit evidence remains exactly two lab2 crashes; no one-bit confirmation on node3. One thread WAS polling SPDK at crash6; prior across-all-crashes absence claim must not be extended.
- New core archived/verified at `/tmp/vals0248/core.vals-shard.1600983.gz`, SHA256 `dc8e9d94e4cbf04932a53ff227eee9b098252f9b62dbae29aa7ad3baf5d2ed48`, matching new binary preserved. Deleted remote original AFTER forensic reader finished. Free space now node1~52GiB,node2~54GiB,node3~50GiB. No unique evidence removed without a verified retained copy.
- Core-capture mitigation: all live node pids changed `/proc/PID/coredump_filter`0x33→**0x13** (retain private/shared anonymous memory + ELF headers; exclude private hugetlb pages). Persistent `/etc/systemd/system/vals-node.service.d/90-core-capture.conf` on every box sets same filter and TimeoutStopSec300; daemon-reloaded and verified, **no further restart**. LimitCORE remains infinity. This reduces irrelevant hugepage content, but disk headroom must still be watched; it does not guarantee arbitrarily large cores fit.
- Node3 rescued13:57:34 pid1632883; its old shardless process also hit90s stop deadline. Source audit found ENOSPC can panic `eprintln!` logger and then panic the fatal-thread handler's `log::error!` BEFORE `abort()`, allowing shard death without process exit. Strong explanation for midline/full-disk zombie, not dynamically reproduced yet; minimal proposed regression0254.
- After rescue, two120s windows: replay~991 then1015 events/s; cluster physical writes~4527 then4312/s (includes retries); node1 served batch mean4.90 then6.54ms vs1377.6ms baseline. **Not a clean allocator-only A/B**, since restart/recovery and node3 rescue also changed state. Rehomes still0 commits. Followup profile:~79% node1 self cycles in published-seed cleanup, mostly dead winner-map random probes, not the source fold. Proposed0253 targets this bookkeeping while retaining bounded drops;0252 covers PD delivery starvation. All remain proposed, no code fix deployed.
- Instrumentation correction: c4c28276's generation `debug_assert_eq!` is absent in release, and asserting a field against an immediate copy within synchronous exclusive access would not establish the absent stale-writer theory anyway. Do not present this as an armed effective lifetime tripwire. CRC counter is live.
- New forensic scripts in local `/tmp/{notes0248,heapwalk0248,heapwalk2,arenawalk,aw2,mm,hdiag}.py`; primary core/binary archives stay `/tmp/vals0248/`. Full operational/perf report: `bench/0248-takeover-20260905/FINDINGS.md`.

### 2026-09-27 — third lab-2 bit-42 signature (task 0313 LONG01-B25-T)

lab-2 valsd (branch 0313-memtable-cpu build 57d8c009…) aborted with glibc `corrupted size vs. prev_size` during a free in `ConnDriver::dispatch_frame`. The prev_size word of the chunk after a free 0x20 chunk at 0x7ac6e89b48d0 reads `0x0000040000000020` (expected `0x20`): one bit-42 SET, flipped byte at cache-line offset 0x35 (0x15 mod 32). Crashes #4/#5 cleared bit 42 at offset 0x15. Core `/home/user/vals/run/core.vals-shard.2421725` (sha256 f454e2fa7cf04d7886a30a686cd6293872e93c4e88a5fb9ab34b141ac07327ca) and binary preserved on lab-2. Still not proof of hardware causality.

### 2026-09-27: recurrence in the 0321 full-cache soak (SOAK0321-F1)

- **Crash.** At 19:28:44 lab-2 vals-shard crashed with `segfault at 40000000028` in libc. That is bit 42 plus 0x28, after 387M KV at about 1.0M ACK KV/s.
- **Checksum failures first.** In the 2 minutes before, node 2 had three `corrupt snapshot: crc mismatch` failures in off-core compaction. Node 1 had none. The only other occurrence in any LONG01 run is also on node 2 (D5-L).
- **Preserved.** The core `core.vals-shard.3574986` (6.59 GB) and the matching valsd are kept on lab-2. Details: `bench/0321-long-soak/FINDINGS.md`.
- **AER.** Continuous AER logging is present on both lab-1 and lab-2, so it does not discriminate.

### 2026-09-30 — SCALE0348-M1 persisted corruption on lab-2

Node2 segfaulted at 07:15:18 in BTree `find_upper_bound_index` (`core.vals-shard.2132008`): pointer `0x7f07203893d0` becomes readable heap data after clearing bit 42 (`0x7b07203893d0`). This resembles prior signatures but does not prove hardware causality. Recovery panics at 07:15:44 / 07:16:04 found persisted segment id `4398046527342 = 2^42 + 22238` out of range (`core.vals-shard.2174151` / `2175457`), evidence of persisted corruption on lab-2's scratch drive. Compaction CRC/index-CRC failures began at 07:05:19, following the planned SIGKILL/restart and preceding the terminal fault. The scratch-drive corpus, cores in `~/vals/run`, and matching binary in `~/vals0348-scale/SCALE0348-M1/valsd` are preserved as incident evidence; **lab-2 must not be reprovisioned without owner release**. Raw journals/GDB receipts: `/data/dataset/task0348-results/SCALE0348-M1/`; summary: `bench/0348-scale/FINDINGS.md`. Node1's separate RAM-broker assertion is task 0349.

### 2026-10-03: two more heap-corruption crashes in the 0363 campaign (lab-1/3, current master)
- **FULL2-LANES4, node1 (lab-1), 1.675B ACK.**
  - glibc `corrupted size vs. prev_size` (SIGABRT) in a shard thread's request decode (`unlink_chunk` → `_int_malloc` → `vals_client::wire::decode::Reader::list` → `decode_request`).
  - A free chunk's size is consistent only with bit 11 (0x800) cleared, the same bit-11 signature as 0336 on lab-1.
  - Core `core.vals-shard.255563` and matching binary on lab-1; gdb in `bench/0363-compact-values/crashes/FULL2-LANES4-node1/`.
- **FULL6-LAZY, node3 (lab-3), ~864M ACK.**
  - SIGSEGV in glibc `_int_malloc` from the request decoder.
  - The corrupted free-list pointer held 8 ASCII bytes ("DpsjVyPR"). That is a scribble/UAF-style signature like this task's earlier cores (not a single-bit flip), so possibly software.
  - Core (15 GB) and matching binary (valsd b7a4496d…, pin 1ee9151a = ccde1d27 + 0370) kept on lab-3; gdb in `/data/dataset/task0363-results/FULL6-LAZY/crashes/crash-001-node3/gdb-bt.txt`.
  - The node auto-restarted at +14 s; correctness of the run passed.
- **Both crashes are in the request decode allocation path.** A `GLIBC_TUNABLES=glibc.malloc.check=3` arm (see memory lesson) would catch the bad free closer to its origin.

### 2026-10-04: FULL10-3N node2 (lab-2) build-verify CRC then segfault; FULL9 lab-3 index single-bit flip
- **FULL10-3N, node2 (lab-2), ~1.6B ACK** (`bench/0363-compact-values/FULL10-FINDINGS.md`).
  - At 06:35:52Z a build-verify block CRC mismatch: a freshly written merge output block read back with a bad CRC, and the merge was retried.
  - At 06:36:02Z a shard SIGSEGV near NULL in `vals_store::index::layout`. Core `core.vals-shard.2616149` on lab-2; binary valsd 1dd59275… (b8f20e16).
  - The ordering (CRC, then a crash in index code 10 s later) fits a memory fault, but the cause is not established.
- **FULL9-D64, node3 (lab-3).** A persistent run-index CRC was localized by CRC-32C linearity to a single bit: XOR 0x80 in a key byte, 'y'→0xf9, flipped after the CRC was computed. That points to the encode buffer, DMA staging or the device. The run was caught by the pre-publication merge-output readback and never published (0375 correction).


---
id: "0114"
title: SPDK NVMe I/O-qpair exhaustion starves off-core compaction (demand scales shards×zones×2 workers)
status: done
priority: 2
created: "2026-08-01"
updated: "2026-08-01"
depends_on: []
spec_refs: []
---

## Description
## Context

Found during a perf pass on the 2026-08-01 bootes soak (3-node lab SPDK cluster, 4 shards/node, RF 2). Off-core maintenance I/O is failing continuously on **all three nodes**:

```
WARN node N: zone Z off-core compaction write failed (retrying next tick):
     block io error: failed to allocate NVMe qpair for shard
```

Counts after ~2.5 h: node1 31,838 failures / node2 16,618 / node3 23,256; successful `run_merges` only 28–40 and `snapshots`(flushes) 318–421 per node. The `snapshot_errors` counter is these retries (node.rs ~L2372). Node1 (the most-loaded / most-ranges node — see 0113) has the most failures, so compaction starvation compounds with the load skew.

## Root cause (high confidence; verify the controller queue count on-box)

- The NULL comes from `vals_spdk_open_qpair` → `spdk_nvme_ctrlr_alloc_io_qpair` returning NULL (crates/vals-spdk/csrc/spdk_shim.c:214-222; surfaced as the "failed to allocate NVMe qpair for shard" string at crates/vals-spdk/src/spdk.rs:552). **Not** hugepage/DMA exhaustion — checked live: `HugePages_Free` ~3872/4096 (~7.7 GiB free), and each qpair's DMA footprint (`VALS_CHUNK`128K + `VALS_DMA_POOL`8M + 1024×16K AIO ≈ 24 MiB) is well within budget. So it's genuine NVMe **I/O-qpair exhaustion**.
- Qpair demand scales badly. Per `maint.rs`: **two** maintenance worker threads per shard (flush + compact), and **each opens its own sibling qpair per zone, lazily on first use**. So demand ≈ `serving (1/shard) + 2 workers × zones-touched-per-shard`. With 4 shards and sorted zones (`records`,`backlinks`) sharded intra-node by range token so each shard hosts ranges of both, that's ~`4×(1 + 2×2) = 20` qpairs (+ default zone). This exceeds the controller's negotiated I/O-queue count and the losers fail forever.
- The C shim never sets `opts.num_io_queues` on the controller opts at attach — it takes the SPDK default, which is evidently below what `shards × zones × 2 workers` needs on this drive/config.

## Impact

Off-core compaction/run-merge is largely disabled for the shard×zone pairs that lost the qpair race → runs accumulate → read amplification grows (cold LWW-resolve run-reads, the t0097 cost). Currently **masked**: the cluster is CPU-idle under bootes (upstream-bound), `used_segments` is bounded, RAM is flat, latencies healthy — so it hasn't bitten. But it's a latent scaling limiter and unbounded-run-growth risk under real read/write load, and it silently degrades the exact maintenance path that 0107 depends on.

## Directions
- Raise `num_io_queues` in `spdk_nvme_ctrlr_opts` at attach so the controller advertises enough I/O queues for `serving + maintenance` across all shards/zones (and log the negotiated count so exhaustion is visible, not silent).
- And/or reduce demand: share one maintenance qpair per worker across zones (qpairs touch disjoint blocks already), or pool/limit maintenance qpairs, so demand scales with shards not shards×zones.
- Make the failure loud/observable: a dedicated counter + a startup check that maintenance qpairs actually opened, rather than an infinite per-tick retry that only shows in WARN spam.

## Acceptance
- [ ] Confirm the controller's negotiated I/O-qpair count on-box and that demand exceeds it
- [ ] Maintenance qpair allocation no longer fails under a normal multi-shard multi-zone soak (0 "failed to allocate NVMe qpair" over a sustained run)
- [ ] Off-core compaction keeps up (run_merges proceed; runs-per-range bounded); regression/observability so silent starvation can't recur

## Acceptance criteria

## Plan

## Root-cause analysis (corrected)

The task's "raise num_io_queues" direction is a **dead end**: SPDK's `spdk_nvme_connect(NULL opts)` already requests `DEFAULT_MAX_IO_QUEUES = 1024` I/O queues, so the controller already negotiates its hardware maximum. Requesting more cannot help. The real lever is **demand reduction**.

**Demand today** (per node): each of the 2 maintenance worker threads per shard opens its **own** sibling qpair **per zone** (via `SubDev::sibling()` in `resolve_handle_b`, cached per `ZoneId`). So demand ≈ `shards × (1 serving + 2 workers × zones-touched)`, which grows unbounded as auto-split (0108) adds ranges/zones. With SPDK, every `SubDev::sibling()` opens a fresh parent qpair.

**Key structural fact**: all zones of one partition share ONE parent device (`SubDev.parent: Arc<D>`), and one maintenance worker thread only ever serves that one partition's parent. So a worker needs exactly **one** maintenance qpair on the parent, re-used across all its zones — the per-zone `SubDev` handle-B only needs its own `base/len`, not its own qpair.

## The fix (demand scales with shards, not shards×zones)

### 1. `SubDev::sibling()` shares one parent maintenance qpair per worker thread — `crates/vals-cluster/src/zonestore.rs` (~L1101)
Add a thread-local cache of parent maintenance siblings keyed by the parent's `Arc` identity, guarded by a `Weak` so pointer identity is reliable:

```rust
thread_local! {
    // Per-thread maintenance parent-sibling cache (task 0114). A maint worker
    // serves one partition (one parent) and flushes/compacts many zones; sharing
    // ONE parent sibling qpair across those zones makes qpair demand scale with
    // shards, not shards×zones. The Weak keeps the parent's Arc allocation (hence
    // its as_ptr identity) reserved so keys can't alias a freed-then-reused parent.
    static MAINT_PARENT_SIBLINGS: RefCell<Vec<(usize, std::sync::Weak<D-erased>, Arc<D-erased>)>>
}
```
Because the type is generic, store the `(Weak<D>, Arc<D>)` payload type-erased as `Box<dyn Any>` (downcast on lookup); a worker thread only ever has one `D`, so downcast always succeeds. In `sibling()`:
- Compute `key = Arc::as_ptr(&self.parent) as *const () as usize`.
- Prune entries whose `Weak::upgrade()` is `None` (stale parent — drops the leaked qpair Arc + frees the reserved address; matters if a partition's parent is ever swapped under a live worker).
- If an entry matches `key` (⟹ same live parent, since a held `Weak` reserves the allocation), clone its shared `Arc<D>` parent-sibling.
- Else open a fresh `self.parent.as_ref().sibling()?` **outside** the `RefCell` borrow (re-entrancy safety), cache `(key, Weak::from parent, Arc::new(sib))`, use it.
- Return `SubDev { parent: shared, base: self.base, len: self.len }`.

Correctness: within a worker, jobs run one at a time (sequential `run_job`, each `block_on` runs to a `flush()` barrier), so multiple zones sharing one qpair are never used concurrently — safe for SPDK's one-thread-per-qpair rule. The per-zone `siblings` cache in `maint.rs` stays as-is (now cheap `SubDev` views over the shared parent qpair) — **no maint.rs change needed for the sharing**.

`Box<dyn Any>` needs `D: 'static`. `maint::spawn` already requires `D: BlockDevice + AsyncRead + 'static`, so this is not a new constraint at the call site. Add `'static` wherever the compiler needs it — prefer adding `+ 'static` to the `BlockDevice` supertrait bound in `crates/vals-spdk/src/lib.rs` (~L161) if the localized `impl BlockDevice for SubDev<D>` bound cascades awkwardly; every concrete device is already `'static` and `dyn BlockDevice`/`forward_block_device!(Box/Arc)` stay consistent. Whichever compiles cleanest.

### 2. Observability: make qpair demand & exhaustion visible (not silent) 
- **`crates/vals-spdk/src/spdk.rs`** `SpdkController`: add an `AtomicUsize` of open qpairs. In `open_shard`, on success increment and `log::info!` the running total (this is the live demand); on the NULL/alloc-failure branch `log::warn!` loudly including the count reached — the count at first failure **is** the controller's usable I/O-qpair ceiling, which directly answers acceptance #1 on-box without any C change. (Decrement on `SpdkBlockDevice::drop` so the gauge tracks live qpairs.) This is Rust-only, verifiable with `cargo check -p vals-spdk --features spdk`.
- **New counter `maint_qpair_errors`** in `crates/vals-client/src/observe.rs` (`counters!` macro block, near `snapshot_errors`) with a clear doc string ("Maintenance handle-B (sibling qpair) allocations that failed — off-core flush/compaction for that zone fell back / retried; sustained non-zero = NVMe I/O-qpair exhaustion starving compaction"). Increment it in `resolve_handle_b` (`crates/vals-cluster/src/maint.rs` ~L234) when `sibling()` returns `Err` (convert the `?` to an explicit match/inspect_err). This gives operators a distinct, non-conflated signal (today these are lumped into `snapshot_errors`).

## Verification
- `cargo build --workspace` and `cargo test -p vals-cluster -p vals-store -p vals-spdk` (Mem/File siblings: confirm the shared-qpair path is exercised and existing maint/flush/compaction tests still pass — Mem sibling shares the buffer so sharing one handle is equivalent).
- `cargo check -p vals-spdk --features spdk` (SPDK Rust path type-checks; the C shim is unchanged so no on-box C build risk).
- `cargo clippy --workspace` clean; hand-format new regions only (do NOT run crate-wide `cargo fmt` — rustfmt version skew, per memory).

## Out of scope (hardware-gated → follow-up)
Acceptance #2 (0 "failed to allocate NVMe qpair" over a sustained multi-shard/zone soak) and the on-box confirmation of the negotiated ceiling require the lab SPDK cluster + bootes soak — a hardware session. I'll capture that as a box-validation follow-up task. The code here makes demand ≈ `shards × 3` (well under any drive) and makes the ceiling + failures observable so the soak can confirm.

### Starting points
- crates/vals-cluster/src/zonestore.rs:1101 SubDev::sibling() — the change site
- crates/vals-cluster/src/maint.rs:225 resolve_handle_b (per-zone handle-B cache; add maint_qpair_errors increment on sibling() Err)
- crates/vals-cluster/src/maint.rs:351 maint::spawn already bounds D: ...+'static
- crates/vals-spdk/src/spdk.rs:538 open_shard / 550 alloc-failure branch / 743 SpdkBlockDevice Drop
- crates/vals-spdk/src/lib.rs:161 BlockDevice supertrait bound; :296 forward_block_device! for Box/Arc
- crates/vals-client/src/observe.rs:168 counters! macro; :254 snapshot_errors — add maint_qpair_errors nearby
- node.rs:2368 the qpair-alloc failure currently surfaces here as snapshot_errors WARN spam

## Work log
- 2026-08-01 plan: ## Root-cause analysis (corrected)  The task's "raise num_io_queues" direction is a **dead end**: SPDK's `spdk_nvme_connect(NULL opts)` already requests `DEFAULT_MAX_IO_QUEUES = 1024` I/O queues, so t
…[truncated]
- 2026-08-01 context hints: 7 recorded with plan
- 2026-08-01 implementer report: Implemented task 0114 (SPDK NVMe I/O-qpair exhaustion) as a demand-reduction fix plus observability. Did NOT touch the C shim / num_io_queues (confirmed dead end per plan).  ## Changes  **1. The fix �
…[truncated]
- 2026-08-01 review tier: single-opus — reviewers: claude
- 2026-08-01 review (claude): accept — The 0114 change implements the plan's demand-reduction approach: SubDev::sibling() now shares one maintenance parent qpair per worker thread across all zones of a partition (thread-local cache keyed b
…[truncated]
- 2026-08-01 decision: accept — commit: 0114: share one SPDK maintenance qpair per worker across zones (demand scales with shards, not shards×zones)  SubDev::sibling() now caches and reuses one parent maintenance sibling per parent Arc ide
…[truncated]
- 2026-08-01 usage: 61,715 tok (in 162, out 61,553, cache_r 5,102,385, cache_w 419,154) · $6.7105
  coordinator: 26,206 tok (in 42, out 26,164, cache_r 2,316,346, cache_w 264,041) · $3.4627
  reviewer:claude: 20,198 tok (in 50, out 20,148, cache_r 1,412,159, cache_w 102,419) · $1.8501
  implementer: 15,311 tok (in 70, out 15,241, cache_r 1,373,880, cache_w 52,694) · $1.3977

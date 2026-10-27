# Production-readiness review — 2026-08

Review target: `25914ed4` (`master`), with task 0232 and the review task modified/untracked in the working tree during review.

## Disposition

**Do not describe or ship this revision as production-ready.** The software-backed build and test suite are strong, and the project has unusually detailed correctness and hardware evidence, but the review found several first-release blockers: a known SPDK heap-corruption abort, an LWW identity collision that can make replicas permanently disagree, unsafe Raft persistence ordering, an unsound public executor waker, destructive formatting in persistent daemon configuration, and no authenticated/confidential data path.

A public **experimental/alpha** release is reasonable after the correctness and legal/package blockers are fixed and the README clearly limits the support claim. A production claim should additionally require transport security, safe resource defaults, a shipped restore path, and repeatable release/soak automation.

## What was verified

- `cargo build --workspace`: passed.
- `cargo test --workspace`: passed.
- `cargo clippy --workspace --all-targets --all-features`: passed as a type/lint check; this is not a linked-SPDK hardware validation.
- `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps`: passed.
- Workspace doc tests: passed.
- `cargo fmt --check`: failed with widespread formatting drift. The repository's project memory says not to run wholesale `cargo fmt`, so this needs a deliberate baseline cleanup rather than an incidental rewrite.
- `cargo package --workspace --allow-dirty --no-verify`: packaged `vals-rt`, then failed on `vals-client` because path dependency `vals-rt` has no version requirement.
- No CI workflow, dependency-audit policy, release workflow, `LICENSE`, `SECURITY.md`, or `CONTRIBUTING.md` is present.

## Release blockers

### R0 — Known production-backend heap corruption remains open (P0)

Task 0232 records a real `SIGABRT` with `double free or corruption (out)` during sustained SPDK ingest, range re-home, and off-core flush churn (`backlog/0232-valsd-sigabrt-double-free-heap-corruption-on-alpha.md:12-21`). Its root cause and multi-hour regression gate are still open.

This conflicts with the feature-complete/productized wording in `README.md:19-31` and `spec.md:25-32`.

The concurrent 0232 audit also opened task 0235: zone teardown can free and reuse a device region while an off-core flush/compaction plan still holds a device handle and writes to that region. That creates a concrete cross-zone corruption path under drop/re-home churn (`backlog/0235-zone-teardown-can-free-reuse-a-region-while-an-off.md`).

**Required:** keep release blocked until 0232 and 0235 are fixed and the same workload shape passes repeated hardware soaks. Until then, label the project pre-release and separate software-backend evaluation from SPDK production readiness.

### R1 — Colliding coordinator IDs can permanently diverge replicas (P0 correctness)

> **Resolved by task 0236:** Coordinators now generate a CSPRNG-backed 64-bit
> writer-incarnation identity independently of the caller's routing label. Cloned
> `Hlc` handles share atomic monotonic state, and observed versions advance it.
> A fresh incarnation prevents version reuse across restart/rollback, subject to
> the documented random-identity collision probability. The writer is carried in the 16-byte
> version through protocol v2, storage, hints, repair, and anti-entropy. Deterministic
> equal-clock/opposite-delivery tests cover local and quorum reads plus anti-entropy
> convergence.

`Coordinator::new(node: u32, ...)` directly uses the caller-supplied ID as the writer component of the HLC (`crates/vals-client/src/coord/routing.rs:3-12`). A fresh writer's first timestamp in a millisecond is deterministic (`crates/vals-client/src/hlc.rs:16-20,59-69`). Two coordinators configured with the same ID and clock millisecond can therefore issue different values with exactly the same `Version`.

Replicas only accept a strictly greater version (`crates/vals-cluster/src/replica/write.rs:165-183`), so replicas receiving the two values in different orders can retain different bytes. Anti-entropy compares `(key, version)` and explicitly excludes value bytes (`docs/algorithms.md:416-434`), so it sees those divergent replicas as equal and never repairs them. The getting-started example only says client ID 100 is distinct from data nodes (`docs/getting-started.md:67-68`); copied deployments can readily reuse it.

**Required before freezing wire/disk formats:** replace manual `u32` writer IDs with a collision-resistant, durable writer identity (preferably a persisted 128-bit ID) and persist enough HLC state to tolerate clock rollback/restart. Add a deterministic equal-clock/equal-ID test that delivers conflicting writes in opposite orders and proves convergence. Including a write nonce or value digest in the total-order tie-break is another viable design, but equal `Version` must unambiguously mean equal logical write.

### R2 — Raft messages escape before durable state, and persistence failure is non-fatal (P0 control-plane safety)

> **Post-review correction (owner-approved backlog triage):** The original ordering
> diagnosis below is not established. raft-rs 0.7 explicitly distinguishes messages
> permitted before persistence from `persisted_messages`; current `Partition::drive`
> uses that distinction. Previously committed entries may also be applied before
> persisting the current Ready. The confirmed outstanding issue is continuing after
> persistence/apply failure with an incompletely handled Ready. Task 0237 is now
> scoped to fail-stop behavior, Ready/LightReady failure tests, and restart recovery,
> not an unsupported unsafe-vote claim or blanket message-ordering rewrite.

`Partition::drive` queues outbound messages before snapshot/log/hard-state persistence (`crates/vals-raft/src/lib.rs:327-345`). It also applies committed state-machine entries before persisting the current Ready (`:335-343`). Any storage or state-engine error returns with the already-queued messages retained.

The PD drive loop records the error but continues, and then drains/sends the outbox. A failed hard-state fsync can therefore put a vote/append response on the network without durably recording the term/vote. After restart, the replica can vote again in the same term. State-machine `DeviceFull` also leaves a committed entry repeatedly failing rather than taking the control plane out of service loudly.

**Required:** persist snapshot/entries/hard state before exposing persistence-dependent messages; only apply committed entries in the order required by the `raft-rs` Ready contract; never retain/send messages from a failed Ready. Treat durable Raft storage/apply failure as fatal and restart-required, not a recoverable drive-loop warning. Add fault-injection tests for log, hard-state, snapshot, and state-machine failures asserting that no unsafe message is sent.

### R3 — The daemon PD state machine is a fixed 64 MiB append-only engine, while range maps are rewritten whole (P0 for the stated scale target; resolved by Task 0238)

`valsd pd` always creates a 64 MiB in-memory state engine (`4096 * 16,384` bytes) at `crates/vals-server/src/run_pd.rs:14-23`. Raft storage is durable, but this materialized engine is where committed catalog/range-map commands apply. It is not flushed or compacted by the PD paths.

Every sorted-range transition rewrites the complete encoded range map as one value (`crates/vals-cluster/src/pd/driver.rs:249-252` and the split/re-home callers around it). Repeated splits therefore consume state-engine vlog space quadratically in range count, and eventually one map exceeds the roughly one-segment maximum record size. At the documented default one-million-entry auto-split threshold, the stated 24-billion-record target implies roughly 24,000 ranges—well beyond a one-MiB whole-map record for realistic boundaries/tokens.

A restart can rebuild a compact logical state from the Raft snapshot while maps still fit in one record, but the daemon does not fail/restart automatically on apply failure; once a single map exceeds the record limit, restart no longer helps.

**Required:** store range state as O(1)-sized per-range records or another incrementally updated structure; make the PD state engine durable/configurable and maintain it; expose capacity/free-space metrics and fail proposals before exhaustion. Add a thousands-of-splits scale test based on the target key shape.

**Resolution:** Task 0238 replaced the daemon engine with a configurable bounded live-key materialization rebuilt from Raft snapshots/logs, added in-flight capacity admission and capacity/headroom gauges, and moved range maps to atomic versioned metadata plus per-range records. Its realistic 24-byte-boundary test materializes the 24,000 ranges implied by the 24-billion-record target and bounds one additional split's state growth. See `docs/deploy.md` for configuration and upgrade restrictions.

### R4 — `vals-rt` constructs `Send + Sync` `Waker`s over `Rc`/`Cell` (P0 safe-Rust unsoundness)

The executor's RawWaker implementations store `Rc<WakerData>` and `Rc<Cell<bool>>` (`crates/vals-rt/src/lib.rs:456-533`). The safety comments assume wakers never leave the executor thread, but `std::task::Waker` is `Send + Sync`; any safely implemented user future may clone its waker, send it to another thread, and wake there. That races the non-atomic `Rc` refcount and `RefCell`/`Cell`, causing undefined behavior through a safe public API.

**Required:** make the raw-waker payload and wake path genuinely cross-thread safe—for example, an `Arc` wake state feeding a thread-safe ready queue plus an eventfd/notification consumed by the local executor. Do not merely switch the refcount to `Arc` while still mutating thread-local `RefCell` from the waking thread. Add a test whose future wakes from another OS thread, and run the executor under Miri or an appropriate sanitizer where possible.

### R5 — The network trust model is not suitable for an external production claim (P0 security)

When no secret is configured, verification intentionally succeeds for every token (`crates/vals-cluster/src/auth.rs:15-19`); `valsd` only logs a warning and continues (`crates/vals-server/src/main.rs:78-88`). Even with a secret, only selected control operations and the PD peer port are gated. Data reads/writes/deletes are unauthenticated, and the control token is sent in cleartext (`docs/getting-started.md:348-355`; `docs/deploy.md:433-438`). Production examples bind the data service to `0.0.0.0` (`docs/deploy.md:76-90`).

The server also accepts unbounded connections. Each connection permits a 64 MiB frame, up to 256 MiB queued output, and 1,024 in-flight reads (`crates/vals-client/src/net/mod.rs:172-191`); the server read gate and accept loop have no process-wide connection/read cap (`crates/vals-cluster/src/net/server/accept.rs:206-229`).

**Required for production:** authenticate and encrypt every RPC, preferably with mTLS and explicit node/client identities and authorization. Fail closed on non-loopback binds unless an explicit, loudly named insecure-development setting is present. Add global connection, handshake, read, queued-byte, and request-rate limits plus idle/handshake deadlines.

### R6 — `format = true` is a persistent restart-time data-destruction switch (P0 operational safety)

Node config stores formatting as a boolean (`crates/vals-server/src/config.rs:289-313,405-408`) and passes it on every SPDK daemon startup (`crates/vals-server/src/run_node.rs:188-220,250-261`). The zoned store's create path reformats the superblock (`crates/vals-cluster/src/zonestore.rs:718-738`); opening zone 0 then allocates and wipes its region (`:1044-1064`). Leaving `format = true` in a systemd-managed config destroys data on an ordinary restart.

The docs warn that it is for deliberate first boot, but the durable config shape makes the unsafe action easy to repeat (`docs/deploy.md:98-101,125-127`).

**Required:** remove formatting from normal daemon startup configuration. Provide a separate offline `valsd format` command requiring an explicit device identity/serial and destructive confirmation (with a noninteractive confirmation token for automation). A serving daemon should only open an existing layout and fail closed on blank/incompatible media.

### R7 — The repository is not legally/package/release ready (P0 distribution)

The workspace declares Apache-2.0 (`Cargo.toml:14-18`) but contains no license text. The repository metadata is the invalid value `vals`, is not inherited by members, and intended packages lack descriptions, READMEs, `rust-version`, keywords/categories, and explicit publication intent. Internal path dependencies have no version, which makes `cargo package` fail at `vals-client`.

There is no CI/release workflow, security policy, dependency vulnerability/license audit, release tag, or changelog. At review time the local branch was 118 commits ahead of `origin/master` and the working tree was dirty, so it was not an immutable reproducible release candidate.

**Required:** add the canonical license and third-party notices/SBOM; choose which crates are public and mark all others `publish = false`; add real metadata and versioned path dependencies for publishable crates; add `cargo package`/publish dry-runs to CI. Release only a clean pushed commit with an annotated tag, checksums, toolchain/SPDK provenance, and release notes.

## Must-fix first-release design and operational issues

### R8 — “Durability” controls replica acknowledgements while default writes are not device-durable (P1)

`Durability::{NoSync,OneNode,Majority,Replicated,Quorum}` computes W, while the separate `Persistence` enum controls buffered versus device-stable acknowledgement (`crates/vals-client/src/coord/mod.rs:259-282`). `Coordinator::write` chooses `Replicated` but reaches `Persistence::Buffered` (`crates/vals-client/src/coord/write.rs:49-78`); ordinary deletes, sloppy writes, and batches do the same.

This is disclosed in `docs/getting-started.md:260-300`, but the names and safe-looking default are hazardous. A replicated delete can be acknowledged from every replica's memory and then disappear on a simultaneous process/power failure, allowing an older durable value to return.

**Recommendation:** rename the W policy to `ReplicationAck`/`WriteConcern`; replace combinatorial method names with validated `WriteOptions { acknowledgement, persistence }`; make the simple `write`/`delete` API device-durable or name the buffered method explicitly. Reject `Quorum(0)` and impossible W instead of silently clamping. Keep the high-throughput buffered path explicit for users who accept it.

**Current status (task 0242):** the owner chose (2026-10-14) to keep replica-memory acknowledgement as the default and make device durability an explicit per-write option. The Rust client now takes validated `WriteOptions { concern: WriteConcern, persistence: Persistence }` (`WriteConcern::{Unacknowledged, One, Majority, All, Quorum(n)}`); a bare `WriteConcern` and `write(k, v)` are `Persistence::Buffered`, `WriteOptions::device_durable(concern)` waits for device sync, and the `*_with_persistence` and `write_tier*` method families are gone. `Quorum(0)`, a W above the replica count and `Unacknowledged` with `Device` fail with `InvalidWriteOptions` before anything is sent. The contract and migration notes, including the unchanged Go client (`Put` buffered, `PutBatch` device-synced), are in `docs/getting-started.md` §Reading and writing.

### R9 — Client liveness and memory protection are opt-in (P1)

`NetTransport` defaults to no RPC deadline (`crates/vals-client/src/net/client/transport.rs:15-18,43-59`). `Coordinator` defaults to effectively unlimited global and per-node credits (`crates/vals-client/src/coord/routing.rs:3-20`; `credits.rs:45-46`). A black-holed peer can prevent a failed quorum from resolving forever, and an unbounded producer can exhaust memory.

**Recommendation:** introduce a required/validated `ClientConfig` with finite production defaults for connect/RPC deadlines, operations, bytes, per-node pressure, retries, and failure detection. Keep benchmark opt-outs explicit.

**Current status (task 0264):** the Rust `NetTransport` now installs validated finite connect/hello and RPC deadlines plus global/per-node coordinator write budgets by default; in-tree benchmarks use explicit unbounded constructors. Retry, cancellation, partial-write, and accounting scope are documented in `docs/getting-started.md` and `docs/backpressure.md`. This does not change independent Go clients or resolve R8's separate write-option naming/persistence contract.

### R10 — Configuration and control-plane invariants are not validated centrally (P1)

Examples:

- `sweep = 0s` is accepted (`crates/vals-server/src/config.rs:369-377`) but produces a successfully created, disarmed timer (`crates/vals-client/src/reactor.rs:121-146`), silently disabling periodic anti-entropy and maintenance.
- File/memory backends do not reject zero/overflowing block size, segment count, or region size before `run_node` divides/multiplies them (`crates/vals-server/src/run_node.rs:48-105`). Duration and geometry arithmetic also has unchecked multiplication.
- Boot zones accept RF 0; `ZoneConfig` exposes unrestricted `usize`; persistence truncates RF to `u32` (`crates/vals-server/src/config.rs:604-642`; `crates/vals-client/src/zone.rs:30-36`; `crates/vals-cluster/src/pd/driver.rs:476-506`).
- `PlacementDriver::create_zone` persists configs without rejecting duplicate names, empty/invalid scopes, or RF greater than eligible members (`crates/vals-cluster/src/pd/driver.rs:198-214`). Later membership removal can also leave zones below intended RF while `Durability::Replicated` counts only the replicas actually returned.

**Recommendation:** use wire-sized types and validated constructors; enforce invariants in the PD state machine, not only the CLI; reject under-replicated writes or make degraded mode explicit; guard membership removal against zone RF/scope violations. Add `valsd node|pd --check-config` that prints effective settings and destructive/restart-only actions.

### R11 — A supported backup is described, but no supported restore ships (P1)

`valsd dump` is extensive and useful, but it is raw, replica-local, may omit unsynced tails, and can race compaction (`docs/deploy.md:281-408`). Restore is delegated to a sibling `valsdump replay` tool that is absent from this repository and not linked by a stable URL/tag (`docs/deploy.md:398-401`). The README similarly points to a sibling examples repository via a relative path (`README.md:102-104`).

**Recommendation:** ship/version the restore tool and an end-to-end restore test. Define how catalogs/range maps, zones, versions, tombstones, duplicate replicas, consistency points, and validation are handled. Perform and document a disaster-recovery drill before a production claim.

### R12 — Use the no-user window to simplify formats and make the handshake extensible (P1/P2)

Legacy manifest/run decode branches are still reachable. In particular, a manifest with runs lacking `value_segs` takes the non-manifest-driven recovery path, marks every log segment allocated, then marks them again during replay and panics (`crates/vals-store/src/engine/recovery.rs:172-235,294-310`). Older classic Bloom decoding also lacks current-format validation. With no compatibility obligation, carrying these branches adds panic and fuzz surface.

The protocol hello requires an exact 14-byte body (`crates/vals-client/src/proto.rs:13,38-39,72-74`), so adding identity/authentication/features later requires a flag day even though version negotiation otherwise anticipates mixed versions.

**Recommendation:** accept only the current on-disk version (or fully test every retained legacy version), convert all disk-derived `expect` paths to corruption errors, and fuzz all decoders. Let v1 peers ignore bounded trailing hello extension bytes and reserve an extension/TLV area now. Reconcile the contradictory “frozen” claims in `spec.md:27-30` with `docs/formats.md:12-21`.

### R13 — A bad peer clock can trigger destructive liveness wipes (P1 when tombstone GC is enabled)

`rejoin_decision` wipes when any peer watermark exceeds local watermark plus GC grace (`crates/vals-cluster/src/liveness.rs:7-17`). Peer watermarks are ultimately based on versions accepted relative to each peer's own wall clock. A node whose clock jumps forward by more than the configured grace can therefore cause fenced peers to discard all local zones during rejoin.

**Recommendation:** clamp peer watermarks against local physical time plus an explicit skew allowance, require an independently stale local-contact condition, and suppress/alert rather than wipe on implausible skew. Add a kill-the-fix clock-jump test and a wipe-suppressed metric.

### R14 — Tighten remaining unsafe I/O boundaries (P1/P2)

- `WriteState` publishes completion/error with Relaxed atomics (`crates/vals-spdk/src/lib.rs:8-42`); use Release/Acquire so observing completion also observes the error/result state.
- `Scratch::heap_take` sets `Vec<u8>` length over uninitialized capacity and exposes slices (`crates/vals-spdk/src/lib.rs:329-346,378-388`). Replace this with initialized storage or a `MaybeUninit` API whose initialized transition occurs only after a successful full read.
- `BlockDevice::submit_write` has no lifetime tie to its input buffer (`crates/vals-spdk/src/lib.rs:173-197`). The current SPDK shim copies writes synchronously, but that requirement is not in the trait. Document/enforce ownership or return a lifetime-bound handle.

These are especially worth cleaning up while task 0232 is investigating heap corruption.

## Productization, documentation, and cleanup

### R15 — Define and shrink the supported product/API surface (P1/P2)

The workspace exposes roughly a thousand public declarations, including implementation modules (`vals-store` allocator/run/snapshot internals; `vals-client` wire/reactor/proto modules). There is no `missing_docs` policy, and central public contracts such as `Coordinator`, `Transport`, `Durability`, `Persistence`, and `CoordError` lack complete API-level semantics.

Choose the supported artifacts explicitly: likely `valsd`, `vals-client`, and a deliberately bounded embedding facade. Mark benchmark/internal crates non-publishable, seal implementation modules where possible, use `#[non_exhaustive]` where evolution is intended, and document cancellation, retry, thread-safety, persistence, and compatibility contracts.

### R16 — Move correctness-affecting environment switches into typed startup config (P1/P2)

Dozens of `VALS_*` variables affect merge, compaction, placement, scanning, and re-home behavior; many are read dynamically and silently fall back on malformed input. Important cluster-wide behavior such as opaque range-map/balance capability depends on these settings, but they are absent from the daemon's config schema.

Promote supported/correctness-affecting settings into typed node/PD configuration, parse once at startup, validate cluster-wide compatibility, log/export the effective settings, and reserve environment variables for clearly experimental options. Warn or fail on malformed supported variables rather than silently using defaults.

### R17 — Repair the first-user path and status documentation (P1/P2)

- The five-minute daemon walkthrough performs only admin metadata commands, not a put/get (`docs/deploy.md:20-63`). Ship a small `valsctl` or runnable TCP example and a one-command local demo.
- Restore and examples rely on relative sibling repositories.
- README/spec/SPDK status text names completed/rejected work as the active frontier and claims production blockers resolved while 0232 is open.
- `docs/algorithms.md:604-618` says the default Merkle bucket count is 256, while the implementation and the same document earlier use 4096 (`crates/vals-cluster/src/antientropy.rs:14`; `docs/algorithms.md:426-430`).
- Source links still target removed flat modules such as `wire.rs` and `replica.rs` after the module split.
- The CLI has no standard `--help`, `--version`, or config-check mode, and silently ignores trailing arguments (`crates/vals-server/src/main.rs:39-75`).

Add Markdown link/config-reference checks to CI. Generate version/config constants where practical instead of duplicating them in prose.

### R18 — Establish release and evidence hygiene (P2)

- Add CI lanes for locked build/test, clippy with warnings denied, the chosen formatting baseline, docs, package dry-runs, audit/license policy, Markdown links, and a real linked-SPDK build.
- Pin SPDK by immutable commit and submodule SHAs. `scripts/build-spdk.sh:12-31` currently uses a mutable tag, ignores dependency-install failures, and installs unpinned Python packages.
- Run the ignored million-entry re-home release harness (`crates/vals-cluster/src/node/tests.rs:1489-1498`) in a named scheduled/release lane.
- Isolate tests that mutate process-global `VALS_*` environment variables and replace fixed sleeps with bounded readiness polling where possible.
- Curate `bench/`: it accounts for 16,419 of 16,853 tracked files. Keep findings/provenance in-tree, move raw captures to release assets/object storage, and use `export-ignore` for internal plans/backlog/raw evidence if source archives are distributed. Scrub machine-specific paths/IPs before making the repository public.
- Replace the sample systemd `TimeoutStopSec=30` (`docs/deploy.md:410-427`) with a measured safe policy; large syncs and live dumps can exceed it. Add user/group, runtime directory, umask, limits, capabilities, and hardening guidance.

## Recommended implementation order

1. **Change the public status now:** pre-release warning; no production claim while 0232 and the P0 correctness issues are open.
2. **Correctness/safety sprint:** R1 writer identity, R2 Raft Ready persistence, R3 incremental PD state, R4 cross-thread-safe wakers, R6 offline formatting, R13 wipe skew, R14 unsafe I/O cleanup.
3. **Security/resource sprint:** R5 mTLS/authz plus global connection/request budgets; R9 finite client defaults.
4. **Pre-user contract cleanup:** R8 write options/naming, R10 typed validation, R12 one current format + extensible hello, R15 reduced supported API.
5. **Operability/release:** R7 licensing/packaging/CI, R11 restore drill, R16 effective config, R17 runnable user path/docs, R18 reproducible release evidence.
6. **Release gates:** all software CI green from a clean tag; package dry-runs; task 0232 regression; Raft persistence fault tests; duplicate-writer convergence test; thousands-of-ranges PD scale test; auth/resource abuse tests; backup/restore drill; repeated multi-hour SPDK cluster soak.

## Positive assessment

The project already has several production-grade foundations: versioned and CRC-protected formats, substantial kill-the-fix and failure-injection coverage, bounded wire decoding, durable Raft files, explicit liveness fencing, backpressure machinery, crash-aware engine manifests, detailed metrics, and extensive real-hardware performance/correctness evidence. The major issue is not general code quality or unfinished feature breadth. It is that a small number of deep safety/contract problems and unsafe defaults sit underneath an overly confident release posture. With no external users, this is the right moment to break the wire/API/config shapes and remove compatibility code rather than document around them.

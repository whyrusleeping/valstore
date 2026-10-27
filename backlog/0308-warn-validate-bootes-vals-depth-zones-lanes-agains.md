---
id: "0308"
title: Warn/validate bootes vals depth × zones × lanes against the server staged-apply gate
status: done
priority: 4
created: "2026-09-25"
updated: "2026-09-29"
depends_on: []
spec_refs: []
---

## Description

From the 0304 review. The default `--vals-depth 16` × 2 zones fits the server's per-shard staged-apply gate of 32 on vals master. `--vals-lanes 2` doubles that to 64 chunks in flight. Against gate-8 servers any depth above 4 sheds Busy (PRESSURE01-K8 collapsed to 39.6k ACK/s). Busy backpressure is lossless but silent.

Acceptance: bootes logs a startup warning, or rejects the config, when depth × 2 × lanes exceeds a documented gate value (e.g. a `--vals-server-gate` flag defaulting to 32). Unit-tested.

## Outcome

bootes 9cbb296 adds a --vals-server-gate flag (default 32; 0 disables the check). In vals mode, bootes logs a startup slog.Warn when effective depth × 2 zones × lanes is above the gate; it warns but does not reject. A negative gate is rejected. Unit tests cover the gate arithmetic (default 32 fits; lanes 2 → 64 exceeds; gate 8 with depth 4 fits, with depth 5 exceeds; gate 0 never exceeds) and the CLI parsing. go build/vet/test pass.

Commit: backlog(0308): bootes --vals-server-gate startup warning (bootes 9cbb296); block 0332/0335 on R2 drive-retention decision (0336 evidence)

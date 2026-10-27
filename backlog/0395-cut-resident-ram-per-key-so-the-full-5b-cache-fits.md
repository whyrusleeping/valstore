---
id: "0395"
title: Cut resident RAM per key so the full 5B cache fits 3×64 GiB nodes at RF2 with a useful key filter and 32 KiB blocks (0386 RAM wall)
status: done
priority: 2
created: "2026-10-07"
updated: "2026-10-07"
depends_on: []
spec_refs: []
---

## Description

Split from 0386 (2026-10-07). The SOAK5B-V5 prelaunch RAM gate fails with every config-only setting, so the full 5B replay cannot launch until resident RAM per key is cut in code.

## Outcome

Narrowed 0395 landed. Bloom (blocked + classic) and fence decoders now validate the declared lengths against the payload and then allocate exactly. The old code grew blooms over 8 MiB geometrically: about 0.43–0.56 B/entry of slack, 20.8 MB per 37M-entry run copy. Encoded blooms are released before index reads in DeviceRun::open and in build readback. The format, hashing and FPR are unchanged. New gauges: vals_run_filter_payload_bytes, vals_run_fence_payload_bytes, vals_key_filter_retired_bytes and vals_key_filter_retired_resident_bytes, documented in deploy.md. The Phase A report finds an ordinary-case EOF deficit of 12.65–13.99 GB/node. The remaining scope is split: 0396 (key-filter bound), 0397 (the unexplained remainder and fences) and 0398 (hardware confirmation and the 0386 re-projection); 0386 now depends on 0398. Tests: vals-store/server/cluster pass in debug, apart from the spdk_lock test, which the reviewer's sandbox cannot run (/run/lock).

Commit: feat(0395): exact-capacity bloom/fence decode, payload and key-filter retired gauges; Phase A RSS attribution; split 0396/0397/0398 from the 0386 RAM wall

---
id: "0246"
title: Add bootes Jetstream cache-and-replay mode
status: done
priority: 2
created: "2026-08-23"
updated: "2026-08-23"
depends_on: []
spec_refs:
    - plans/bootes-soak-runbook.md
---

## Description
Add a durable local cache for a complete Jetstream sealed-archive snapshot and an offline replay source in the sibling bootes tool, so repeated vals soaks are not limited by archive download/rate-limit variability.

Acceptance criteria:
- A CLI mode can record a zero-cursor sealed-archive snapshot into a resumable, compressed on-disk cache.
- A CLI mode can replay a completed cache without Jetstream network/API-key access, honoring the vals/ClickHouse cursor for interrupted replay.
- Cache completion is explicit; incomplete/corrupt caches are not silently treated as complete replay inputs.
- Recording/replay paths have focused tests and bootes build/tests pass.
- bootes README and the vals bootes soak runbook document capture and replay usage.

## Acceptance criteria

## Work log

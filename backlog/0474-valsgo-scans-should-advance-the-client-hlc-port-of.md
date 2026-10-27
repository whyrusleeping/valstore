---
id: "0474"
title: 'valsgo: scans should advance the client HLC (port of 0472)'
status: done
priority: 3
created: "2026-10-15"
updated: "2026-10-17"
depends_on: []
spec_refs: []
---

## Description

0472 fixed the Rust coordinator: `scan_in` now observes the highest version it read into the HLC. valsgo (~/code/vals-examples/valsgo) observes on read winners (client.go ~1225) and on lost writes (~585), but its scan path appears not to. A Go consumer that scans and then deletes, point or range, can therefore stamp the delete at or below the scanned write's version, and the delete silently loses. Port the fix: observe the maximum entry, tombstone and fragment version per scan page, and add a Go test with a key planted ahead of the client clock. Verify with ./run-interop.sh.

## Outcome

Implemented in vals-examples commit 24b974a5ef7f06cadafb4e5b154325755309635d: Scan observes each validated page's maximum entry/tombstone/fragment HLC before filtering and limits. Clock-ahead scan-then-point/range-delete and hidden-version paging regressions pass; all eight cases fail with observation removed. Full Go tests, race tests, 20 regression repetitions and all ten run-interop.sh legs pass. Independent reviewer accepted and independently rebuilt/reran Go and live interop tests. Unrelated 0477 changes preserved.

Commit: fix(0474): complete valsgo scan HLC observation port

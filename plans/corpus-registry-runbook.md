# Starting hardware experiments from a retained corpus (0404)

A retained corpus is the data left on the three lab scratch drives by a
drained attempt. Starting an experiment from one replaces a 3–8 h replay from
zero. The registry records which corpus is on the drives, what produced it,
who opened it with which binary, and whether it may still be discarded.

- Registry: `/data/dataset/corpora.json` (`VALS_CORPORA_REGISTRY` overrides it).
  Changed only under an flock and published atomically.
- Mirror: `bench/CORPORA.md`, rewritten on every change. Commit it with the
  task that changed the registry. Do not edit it by hand.
- Code: `bench/0363-compact-values/corpora.py` (registry),
  `run_full1.py corpus-register / corpus-open / resume --from-corpus`, and the
  discard guard in `dealloc.py`. Tests: `test_corpora.py`.

## Entries and states

Each entry records the source attempt and result dir; the drives (BDF/serial)
and their BEFORE identity receipts; the binary that built it and every
segment's binary; the formats (`run_write_version` from the final scrapes,
`superblock_version`); the layout sha256 of the topology/geometry lines of the
node and PD configs; the saved bootes cursor and cache EOF; the cumulative ACK
and persisted records/backlinks; per node the live keys and device bytes
written over the attempt; whether the last segment drained cleanly; and the
correctness status (`verified`, `unverified`, `known-bad`) with its evidence.

States: `pristine` → `consumed-by:<arm>` → `discarded` (terminal). `reserved`
is a flag with a reason: it refuses a discard, nothing else. The `history`
list records every event (registered, imported-open, opened, consumed, closed,
reserved, released, discarded). `opens` lists every arm with its mode and
server SHA. `mixed_history` is true once two different binaries built or
opened the corpus.

Current corpora: see `bench/CORPORA.md`. `R3` is SOAK5B-V5-R3. Task 0418 consumed it on
2026-10-12 by running it to EOF. It is still reserved for 0432.

## Register a corpus

After an attempt drains and its corpus is to be kept:

```sh
python3 bench/0363-compact-values/run_full1.py corpus-register --attempt SOAK5B-V5-R3 --corpus R3 \
    --reserve "0418 consolidated R3 session" --opens-file bench/0363-compact-values/corpora-r3-opens.json \
    --evidence "0401 confirmation 2: cursor/samplecheck/counts PASS, owned aediag 66/72"
```

The entry is derived from the attempt's own receipts. Registration refuses when:

- the final scrapes do not show one run write version on every node;
- the formats or the layout differ from what the harness generates now;
- a drive identity receipt is missing;
- the drives already hold a live corpus.

`--correctness` may only lower the derived status. `--opens-file` imports
earlier reopens done outside the harness. For R3 these are the 0401 and 0411
arms (`corpora-r3-opens.json`).

Reserve or release: `corpora.py reserve R3 --reason "..."`, `corpora.py release R3`.

## Discard

`dealloc.py discard` (every launcher's BEFORE step, and DRIVE01/02's prep):

- refuses before it touches a drive when a live corpus on it is reserved, or
  when the registry does not exist;
- just before the blkdiscard, marks every live corpus on that drive
  `discarded` and appends a row to `discards`.

Release a reservation explicitly before a discard-requiring arm.

## Start from a corpus

All three modes gate locally before any fleet contact:

- the corpus exists and is not discarded;
- the attempt, formats (run write version, superblock), layout sha256 and drives
  match;
- while the corpus is pristine, the saved cursor matches too.

They then reopen it the 0403 way. Host identity and reboot detection run;
re-prep is non-destructive (sysctls, reset-failed, stale SPDK maps, the drive
identity check via dealloc's **measure** path, vfio-pci rebind). The runtime
configs and wrappers must be byte-identical to the attempt's. There is never
any discard, format, provision or create-zone.

A different server binary is allowed and expected. Install it at the attempt's
runtime `valsd` path on every host and the PD, the same SHA everywhere. Keep
the old binary beside it, as `swap_binaries` does. The harness records the SHA
in the open and in the receipts. A binary that can not open the format refuses
on its own without writing (0371). After serving, the harness requires every
node's `vals_run_write_version` to equal the corpus's (`corpus-format.json`).

**(a) Read-only** — diagnostics, read probes, scans, aediag:

```sh
python3 bench/0363-compact-values/run_full1.py corpus-open --from-corpus R3 --mode read-only --hold-s 10800
```

Receipts go to `<attempt>/CORPUS-READ-ONLY-<utc>/`. The arm checks the cursor
at the saved cursor and the format, then holds the fleet with the watcher and
observer running. Read-only tools (samplecheck with 0406 checkpoints, aediag,
census) run against it from outside. `touch <dir>/STOP` or the bound ends the
hold, followed by the owned stop. The corpus state does not change. This is
best-effort: background maintenance and AE still write, so it is not a
sector-level pristine claim.

**(b) Continued ingest** — from the saved cursor to EOF or `--stop-at-ack`:

```sh
python3 bench/0363-compact-values/run_full1.py resume --from-corpus R3 --amendment REF [--hold-s 7200]
```

This is the 0403 resume (`plans/soak-resume-runbook.md`) with the corpus gate
added; an explicit `--from-corpus` implies `--allow-binary-change`. A resume
of a registered attempt without `--from-corpus` still uses its entry.

With `--hold-s`, the reopened fleet holds after the cursor check and before
bootes. During the hold, read-only signals can run in the same reopen (0418):

- `touch <segment>/INGEST` starts the continuation;
- `touch <segment>/STOP`, or reaching the bound, ends the segment without ingest
  (outcome `operator_stop` / `hold_expired`, corpus not consumed).

The corpus becomes `consumed-by:<attempt>/RESUME-n` when bootes starts.
`corpus-start.json` (in the prep dir and the segment) records the corpus state
at start.

**(c) Synthetic load** — a fixed-duration vals-bench overwrite/read load on top:

```sh
python3 bench/0363-compact-values/run_full1.py corpus-open --from-corpus R3 --mode synthetic --hold-s 3600 \
    --load-argv '["/path/vals-bench", "..."]'
```

The corpus is consumed when the load starts (after the cursor and format checks). The load runs for at most the hold
and is stopped at the bound or on STOP. Its output is in `load.stdout/stderr`.

`resume-check` shows the attempt's corpus gate under `corpus`.

## Single-use rule

- Read-only arms leave the state as it was and record the open.
- A write arm (ingest or synthetic) consumes the corpus. A later arm on a
  consumed corpus starts from a different state, and must say so. A consumed
  corpus keeps its id and its registered (pre-consumption) identity; there is
  no supersede operation yet that would register the continued state under a
  new id. The open rows' outcomes record where each continuation ended.
- A/B arms need either:
  - two corpora at the same point (equal registry identity: format, layout,
    cursor, ACK); or
  - an order-tolerant design, where A then B run on the same corpus and the
    next corpus runs in the opposite order (crossover). Only read-only arms can
    share one corpus without crossover.
- Cite the corpus id, and its state at start, in every task or README that
  starts from one. 0386's SOAK5B-V5 README and 0418 cite `R3`.

## Caveats

- Format-changing builds can not use old corpora. The gate refuses a run write
  version or superblock change. Use a full replay or a scaled-down arm (0405).
  The pre-contact gate compares the registry with the harness's constants
  (`V5_WRITE_VERSION`, `SUPERBLOCK_VERSION` in `run_full1.py`); bump them with
  any format change. The candidate binary itself is checked only after it
  serves (`corpus-format.json`), and a binary that can not read the format
  refuses without writing (0371).
- Throughput measured on a reused corpus is not comparable with fresh-discard
  arms, because the drive state differs (the 9100 PRO post-discard slowdown,
  bytes written since the discard). Compare only arms started from equivalent
  corpus states.
- A corpus built by binary X and continued by binary Y has a mixed history.
  Use it to diagnose and confirm, not as release-gate evidence. The full 0386
  replay remains the release gate.
- Crash rows of a `corpus-open` arm stay in its own dir. They do not enter the
  attempt's resume crash ledger, as with the 0401/0411 reopens.

## Imaging

Nothing here copies or images a corpus. A corpus is consumable: one write arm
uses it up, and reproducing its state means replaying again.

No imaging option is known or qualified for these drives. A raw copy of three
4 TB namespaces would need about 12 TB of spare storage and hours per copy, and
nobody has measured whether a restored image behaves like the original. The
9100 PRO's post-discard state and its FTL state are not part of a block image.

If a concrete write A/B needs two equal corpora, propose that as a separate
task. Do not run an imaging feasibility study under this one.

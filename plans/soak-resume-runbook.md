# Resuming a SOAK5B-V5 / RAMAB soak (0403)

This runbook resumes a stopped or interrupted replay on its retained corpus. It
does not relaunch from zero. The policy is in
`bench/0363-compact-values/SOAK5B-V5-README.md` ("Resume policy (0403)"),
which the owner approved on 2026-10-08, plus the 0408 crash-resume class
(bootes durable watermark cursor, below). The
harness code is `run_full1.py resume` / `resume-check`; the launcher is
`soak5b/resume-v5.sh`. Validation tier: L2a in `docs/validation.md`.

## What can be resumed

A resume needs the interrupted segment to have ended either in a **clean bootes
drain with a saved cursor**, or (0408) with a **bootes durable watermark**.

bootes 3d5b2e9 writes `cursor/jetstream` for vals only after a successful final
drain. Several kinds of stop therefore end the run with the cursor withheld:

- a host died mid-run, so the drain could not reach it;
- the coordinator box died, which kills bootes too;
- bootes was killed.

With 3d5b2e9 the cluster then still holds the previous clean cursor, which is 0
for a first segment, and `resume-check` refuses. This is why SOAK5B-V5
(lab-3 kernel panic at 9.93B), whose bootes was 3d5b2e9, can never be resumed.

**Crash-resume (0408).** The SOAK5B-V5 family now runs bootes `e27dde7` with
`--vals-cursor-watermark 30s`. Every 30 s bootes saves W, the latest admitted
batch cursor whose KVs were all acknowledged or skipped as oversized, to the same
`cursor/jetstream` key. A dropped chunk freezes W. Before each save it logs an intent line
`vals cursor watermark saving seq=W records= backlinks= oversized_records= oversized_backlinks=`
with the exact per-process counts at W and fsyncs `bootes.log`. A failed fsync
skips that save. After the Put it logs `... saved`.

- A segment without a clean drain whose `bootes.log` has intent lines is a
  crash-resume segment (`resume-check`: `policy.resume_class = crash_resume`).
  Its end cursor is one of its **watermark candidates**: the last `saved`
  watermark, or any intent after it whose Put may have landed before the crash.
  Its persisted/oversized counts are that candidate's.
- `resume-check` shows the latest intent as the provisional end
  (`policy.crash_resume`). Decisions that depend on a host reboot are listed
  under `policy.pending_at_resume`: the outcome gate and the crash-budget entry.
  The resume decides them once host identities are read. At resume time the reopened cluster's cursor decides.
  The probe receipt is `watermark-resume-probe.json`. If the cluster serves
  another candidate, the segment is rebased to it before bootes starts:
  cursor, counts and ACK base (`resume-start.json` `crash_resume_rebased`).
  A cursor that is not a candidate ends as `resume_refused`.
- Allowed only after a host reboot, an unreachable host, or harness or bootes
  death (outcome none / `host_unreachable` / `harness_error` / `crash`). Other
  outcomes are refused, and an amendment cannot change that. Each crash-resume
  costs one crash-budget entry: the host-reboot entry, or else a
  `crash_resume` row on node `client`.
- Rates: the carried ledger stops the crash-ended segment at W. Its per-100M rows
  beyond W are voided (`carried.report_100m_rows.voided_by_crash_resume`) and
  re-emitted by the resumed segment; drop them when concatenating. A bucket that
  only W completes, after the crashed reporter's last scrape, is published once,
  first in the resumed segment's `report-100m.jsonl` (`synthesized_at_watermark`).
- Events between W and the crash point are written again by the resumed
  segment. The data is unchanged: rewrites happen in source order with newer
  versions, and `indexed_at` falls inside the multi-segment bootes window. Exact
  counts stay exact per segment: `(start, W]` for the crash-ended segment, from
  its intent line.

To **pause** a soak on purpose so it can be resumed later, use either:

- `touch <result>/STOP` (operator stop);
- `--stop-at-ack N`.

Both drain bootes and save the cursor, and a pause stop needs no amendment.

| Last segment ended in | Resume |
|---|---|
| host_unreachable / host reboot, harness crash (with a clean drain) | allowed |
| host reboot, host_unreachable, harness or bootes death with no clean drain, but a bootes watermark (0408 crash-resume) | allowed; one crash-budget entry |
| `stop_at_ack`, operator `STOP` | allowed |
| RAM/disk planned stop, integrity incident, guard/stall/capacity stop, changed stop policy | only with `--amendment REF` (preregistered reclassification) |
| correctness failure (any segment), `corruption-confirmed.json`, crash budget exhausted, no saved cursor and no watermark, EOF reached | refused |
| no clean drain, watermark present, but another outcome (planned stop, integrity, guard, stall) | refused |
| a malformed or truncated receipt, carried state that does not rebuild exactly, missing drive identity receipt | refused |
| a history that is empty or ends before the segment's `soak5b-state.json` / `watch-state.json` | refused |
| a segment that started the fleet or bootes but has no `server-install.json` | refused at resume time |

Each rebooted host counts once against the crash budget. A crash-budget window
(3 crashes of one node within 1800 s, on real crash times) that was ever reached
stays exhausted: downtime before a resume does not expire it.

A recorded correctness failure is read from every segment's own receipts, not
only from `final.json`/`soak-outcome.json`: `samplecheck.json` (or its stdout
copy), `samplecheck-controller.json` (exit 1), `samplecheck-derive.json`,
`zone-counts.json`, `cursor-probe.json` and other cursor receipts (a served
cursor that differs), `aediag-*-summary.json` (differing replicas) and
`verify.json`. A receipt that exists but is empty or not one JSON object refuses
too. `samplecheck.stdout` is read on its own as well, whatever state
`samplecheck.json` is in: a FAIL or malformed stdout refuses even without a
`samplecheck.json`. The one exception is samplecheck's empty `samplecheck.json`
placeholder of a check that was killed (controller return code not 0/1, empty
or absent stdout): that is an unfinished check, not a failure (R3).

## 1. Local check (read-only, no fleet contact)

```sh
python3 bench/0363-compact-values/run_full1.py resume-check --attempt SOAK5B-V5-R3 [--amendment REF] > /tmp/resume-check.json; echo rc=$?
```

The check rebuilds the state that a resume carries and compares it against what
the attempt recorded. Every gap or mismatch is listed in `carried.errors` and
refuses the resume (`policy.refusals`, "carried state: ..."):

- a segment that ran bootes without `watch.jsonl`, `client-progress.jsonl`
  (or without any admission scrape), `soak5b-samples.jsonl` or a readable
  `soak-params.json`, or with an empty `watch.jsonl` / `soak5b-samples.jsonl`;
- a history that does not reach the segment's independently persisted terminal
  state. The reporter rewrites `soak5b-state.json` atomically after every sample,
  so it is the anchor. `soak5b-samples.jsonl` must hold the row of its last tick
  (same unix and ACK). The drive steps replayed up to that row must equal its
  `drive_steps`. Unless the ACK is still the segment's base, `client-progress.jsonl`
  must hold an admission scrape at its cumulative ACK. The watcher rewrites
  `watch-state.json` (tick count, last tick) atomically after every tick, and
  `watch.jsonl` must hold exactly that tick at that count, plus at most one
  later tick. A segment without a readable `soak5b-state.json` is refused.
  Segment 1 of an attempt started before `watch-state.json` existed (V5, R2,
  R3) is checked on time instead: its last watch tick must be no more than
  300 s before `soak5b-state.json`'s last tick. A `RESUME-n` segment always
  needs `watch-state.json`;
- a torn line in any carried JSONL file;
- `classifier.mismatched_ticks != 0`, `report_100m_rows` or `drive_steps`
  replayed != recorded;
- a resumed segment whose `carried-ledger.json` is missing, incomplete or does
  not preserve the history before it;
- no `DRIVE-DEALLOCATION/<host>-BEFORE-<attempt>-settled.json` (or `-before.json`)
  identity receipt for a host.

Read the crash-budget counts as well. Reboots are added at resume time.

`corpus` shows the attempt's registered corpus (0404, `plans/corpus-registry-runbook.md`)
and its gate errors. A format, layout, drive or (pristine) cursor mismatch refuses.

`carried.disk_stop` shows the 0402 disk-stop decider (0409) rebuilt from the
saved zone-stats rows in every segment's `watch.jsonl`. It is information, not
a refusal. The floor-only Busy baseline is not carried, and the downtime gap
restarts every low-free window. The runner records the rebuild in the
resumed segment's `disk-stop.json`. Attempts recorded before 0409 also need
their `--amendment` to cover the new disk rule, because their `soak5b_policy`
differs from it.

Results on the 2026-10-08 data:

| Attempt | Verdict | Replay check |
|---|---|---|
| V5 | refused (no saved cursor) | 99/99 rows, 3/3 steps, 0 mismatches |
| R2 | needs an amendment (integrity_log); cursor 2,259,385,663 | 46/46, 3/3, 0 |
| R3 | needs an amendment (disk_full); cursor 4,951,424,335 | 101/101, 3/3, 0 |

## 2. Bring the fleet to a stopped state

- **The harness died but bootes is still running on devbox.** Run
  `pkill -TERM -x bootes`. Wait for it to exit (drain bound 1800 s), then confirm
  `grep 'cursor saved on shutdown' <result>/bootes.log`.
- **Node units are still active** (`systemctl list-units 'vals*'` on each host).
  Stop them with `sudo -n systemctl stop vals0297-scale01-<attempt>-node{N}.service`
  on each host and `...-pd.service` on lab-1. Wait until `pgrep -x valsd` is
  empty everywhere.
- **Do not** discard, format or re-provision. Leave `spdk*map_*` and rebooted
  hosts alone: the resume handles both.

## 3. Resume (detached)

```sh
setsid nohup sh bench/0363-compact-values/soak5b/resume-v5.sh SOAK5B-V5-R3 --amendment SOAK5B-V5-README#R3-disk-reclass > /dev/null 2>&1 &
```

`REF` must not contain spaces. Optional flags:

- `--stop-at-ack N` (cumulative ACK over all segments);
- `--allow-binary-change` (records a mixed-binary run).

The launcher logs to `SOAK5B-V5-prep/launch/launch.log` and writes to
`.../launch/<attempt>-resume-<utc>/`.

The resume runs these steps in order:

1. **Local policy and carried state.** Uses the same local preconditions as `run`: BUILD receipt, pinned client15/samplecheck, metrics port free, no local bootes.
2. **`RESUME-n-prep-<utc>/`** (one per try; a refused try leaves its receipts there):
   - `host-identity-before.json`: boot_id, boot time, kernel, sysctls, hugepages, driver, valsd, pstore.
   - Reboots are detected against the segment's `host-identity-initial.json`, or the boot time when no such file exists. Each reboot is budgeted.
   - The final policy goes to `resume-policy.json`.
3. **Runtime identity (`resume-runtime.json`):**
   - The same `valsd` SHA on every node and the PD, equal to the SHA the latest segment ran (every segment's `server-install.json`). Once two segments ran different SHAs the attempt stays mixed-binary (A→B→A included).
   - A resumed segment may lack `server-install.json` only if it stopped before install. That means a readable `final.json` and no sign that the fleet or bootes started: no post-install receipt (`resume-skipped.jsonl`, `carried-ledger.json`, `crashes.jsonl`, `resume-start.json`, `cursor-resume.json`, `client-controller.json`, `bootes.log`, watch/sample/progress histories, `soak5b-state.json`) and no `systemd-run` of the attempt's node/PD units in its `commands.jsonl`. Otherwise the resume is refused, because the binary that segment ran is unknown.
   - Node/PD configs and wrappers byte-identical to the attempt's copies and to what the harness generates now (drives, listen count, geometry, `format = false`).
   - The drive table equals `dealloc.py`'s.
4. **Re-prep (`resume-hosts.json`, never destructive):**
   - Restores `core_pattern`, `panic_on_oops=1` and `panic=30`.
   - Runs `reset-failed` on the attempt's own units.
   - Removes stale `spdk*map_*` files (only with no valsd running).
   - On **every** host, runs dealloc.py's **measure** path (no discard) as `DRIVE-DEALLOCATION/<host>-RESUME-<attempt>-<n>-*`. This takes a kernel-nvme identity receipt, rebinds to vfio-pci (HUGEMEM=8192) and requires 4096/4096 free hugepages. The drive's serial, model, subnqn, namespace size, EUI-64 and NGUID must equal the attempt's `BEFORE-<attempt>` receipt, else the resume stops before the reopen.
   - A host that did not reboot must already be on vfio-pci with its hugepages.
   - `host-identity-after.json` must then show vfio-pci, 4096 pages and the sysctls.
5. **Normal lifecycle into `RESUME-n/`:**
   - Install, provision and create-zones are replaced by the reopen (`resume-skipped.jsonl`, `server-install.json` with `resume: true`, `attempt-*` config copies, read-only `--check-config`).
   - Stale maps are cleaned only with no valsd running.
   - The fleet starts with a 3600 s serving bound. The watcher (carried classifier) runs during recovery.
   - bootes starts only once the cluster serves the saved cursor (`resume-start.json`; a mismatch ends as `resume_refused`). A crash-resume accepts any of its watermark candidates and rebases to the one served.
   - The crash ledger starts with every carried row (`carried: true`). The whole carried ledger is first published atomically as `carried-ledger.json` (rows, count, sha256); an interrupted copy into `crashes.jsonl` can therefore not shorten the history a later resume carries.
6. **End-of-run correctness, as in a normal run:**
   - Drain and cursor probe at the final cursor.
   - Samplecheck over the whole prefix with the bootes window from segment 1's start, plus `--count-at` at every segment boundary.
   - Per-segment exact counts in `zone-counts.json`.
   - Owned aediag.
   - `final.json` carries `resume`, `resume_segments`, `mixed_binary` and `cumulative_ack`. The attempt-level `resume-segments.json` is updated, and fleet receipts and the crash classification go to `RESUME-n/`.

## 4. Reading the reports

- `report-100m.jsonl` / `report-30min.jsonl` in each segment continue one ACK and active-time axis. Concatenate the segments' files for per-1B rates. After a crash-resume, first drop the crash-ended segment's rows listed under `carried.report_100m_rows.voided_by_crash_resume`.
- Rows after a resume carry `segment`, `downtime_excluded_s` and `spans_resume`. The bucket that spans the resume excludes the gap from the interrupted bootes exit to one scrape before the first resumed ACK.
- Recovery timings (harness start, fleet serving, cursor ready) are under `resume_segments[*].recovery`.
- Outage-tracker ACK fields (`outages.json`) are segment-local.

## 5. Task-sized confirmation for 0403

Use local/mock integration for refusal paths, reboot preparation and interrupted
controller state. Mid-ingest bootes crash-resume is 0408's local integration
criterion, not an extra hardware leg here.

Piggyback one clean pause/resume on a suitable already-planned continuation; an
existing clean-drained attempt may supply the pause. Do not provision a fresh
2–3B-event prefix, deliberately reboot a box or destroy shared retained data
solely for this check. Register the corpus, binaries and stop policy first.

Budget one resume and an observation window of at most 50M additional source
events or 15 min resumed ingest, within 2 h total preparation/recovery/observation/
targeted checks. Recovery has a 75 min sub-bound; overruns or insufficient time
for required checks produce an inconclusive confirmation, not an extension.
Source-event limits and logical ACK are not interchangeable. This is an
observation limit, not an instruction to stop a to-EOF parent campaign; its
already-registered endpoint still applies. If no suitable planned continuation
exists, keep hardware confirmation pending rather than open a new campaign.
Local/mock acceptance can close 0403's software work while that signal is pending.

Confirm:

- `resume-start.json` PASS and bootes' replay cursor equal the saved cursor;
- the carried ledger and cumulative ACK base are preserved and ACK rises;
- sampled values match expected bytes; no discard/format/provision occurred;
- full samplecheck/counts/owned-aediag pass once at the shared campaign endpoint,
  under its registered check bounds, with per-segment counts matching.

The current runner's normal post-drain path still performs its full checks; this
scope amendment is not a new skip-check option. Choose a suitable planned
endpoint instead of chaining pauses that each repeat those checks. Reuse 0401
receipts for 0406 timing where they exercise the changed paths. Report mixed
binary history and missing signals honestly; neither is automatically a 0386
release pass. No extra arm beyond this budget without owner approval.

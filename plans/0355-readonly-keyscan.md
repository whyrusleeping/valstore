# 0355: held 0353 corpus, offline read-only key census

This is a collection procedure, **not permission to run it**. Execution belongs
to 0355 after review of the pinned tooling commit. Task 0360 makes no hardware
contact. Keep both replicas stopped throughout both passes and between them.
Never run `valsd node`, PD, AE, maintenance, provision, fio, discard, format,
repair, or replay against this evidence. `valsd dump` scans raw history and is
not a substitute for the live winner census.

## Identity and evidence boundary

Only these two hosts/devices are in scope; **never contact lab-2**, including
through a fleet helper, inventory sweep, rsync target list, or benchmark harness.

| Host | Scratch PCI BDF | Required scratch serial | Held config |
| --- | --- | --- | --- |
| lab-1 | `0000:81:00.0` | `SERIAL-REDACTED` | `/home/user/vals0353-dealloc/DEALLOC0353-LONG/node1.conf` |
| lab-3 | `0000:01:00.0` | `SERIAL-REDACTED` | `/home/user/vals0353-dealloc/DEALLOC0353-LONG/node3.conf` |

Original receipts are on devbox at
`/data/dataset/task0353-results/DEALLOC0353-LONG/`. Preserve those, the original
on-box binaries/configs, PD state, and cores. Use new output directories, not
anything inside the held run directory. Neither success nor failure releases
this corpus for reuse.

The retained configs have eight `listen` entries (eight partitions),
`region_mib=460800`, `zone_region_mib=2048`, and overrides
`0:2048`, `1:262144`, `2:190464`. Zone **1 is records**, zone **2 is backlinks**.
Use the original configs unchanged, including the original `seg_blocks` default.
The reader recomputes the multidrive layout exactly as the node does and calls
its normal layout verification (the current SPDK DriveSet does not persist a
fingerprint; region superblock validation still applies). It does not use the
single-listener `SpdkZoneStore` geometry. It opens only the requested
zone, and errors if that zone is missing from a partition's region table.

## 1. Pin and build (no storage process started)

Record a reviewed full 40-hex commit containing 0360 as `PIN`; do not substitute
an uncommitted tree or `HEAD` at collection time. On each allowed host use a
**separate** detached worktree and target directory, leaving `/home/user/vals`
and the held binaries untouched. Transfer only committed source if the host
repo does not yet contain the pin. Do not copy local benchmark data trees.

```sh
PIN=<reviewed-full-commit>
BUILD=/home/user/vals-keyscan-0360-$PIN
SPDK=/home/user/vals/third_party/spdk
# In the existing on-box source repository, after verifying PIN is present:
git worktree add --detach "$BUILD" "$PIN"
git -C "$BUILD" rev-parse HEAD
VALS_SPDK_LINK=1 VALS_SPDK_DIR="$SPDK" CARGO_TARGET_DIR="$BUILD/target" \
  cargo build --manifest-path "$BUILD/Cargo.toml" -p vals-server --release --features spdk
sha256sum "$BUILD/target/release/valsd"
```

Save commit, build output, binary hash, original config hash, toolchain version,
UTC times, host name, and exact commands in the new collection directory.
A non-SPDK build or an unlinked `--features spdk` build is not a collection
binary. Do not install this binary as a service or replace the held binary.

## 2. Preconditions, health receipts, read-only hardware gate

Execute host-local commands separately on lab-1 and lab-3; if accessing
remotely, use an explicit host and an outer `timeout` around SSH. Do not reuse
`drive_state.py prepare`, `complete-tail`, or an ingest lifecycle helper.

Before any attach/reset/map removal prove:

```sh
test -z "$(systemctl list-units --all --no-legend --plain 'vals*')"
test -z "$(pgrep -x valsd || true)"
test -z "$(pgrep -x fio || true)"
```

Also check there is no other SPDK/benchmark process and no supervisor, timer or
cron job that can start one; do not rely on a process-name check alone. Abort on
an unexpected unit/process rather than killing something not owned by 0355.
Record `lsblk -o NAME,MODEL,SERIAL,MOUNTPOINTS` and PCI driver bindings. Verify
the *other* NVMe remains the boot drive (lab-1: `0000:01:00.0`, serial
`S7KGNU0L301998W`; lab-3: `0000:81:00.0`, serial `S7L9NS0L420486E`).

Only after these proofs, remove stale mappings and record the result:

```sh
sudo -n rm -f /dev/hugepages/spdk*map_*
grep Huge /proc/meminfo
```

For each pass collect full SMART, Identify Namespace and Identify Controller
receipts **before and after**. The established 0353 health-receipt method uses
a temporary kernel binding of the exact scratch BDF, with no daemon running,
then returns that BDF to vfio-pci. Disable any automatic mounting first. This
binding cycle is solely for admin reads, never provisioning or data I/O.

Set `PCI` and `SERIAL` from the table for the current host, and use a new
`OUT=/home/user/keyscan-0355/<collection-id>/<zone>/pass<N>` on its boot disk.
Require it not to exist, then `mkdir -p "$OUT"`. Reserve at least 40 GB free on
each boot disk for both zones' pass-1 outputs plus headroom; never store output
on the scratch namespace.

```sh
SETUP=/home/user/vals/third_party/spdk/scripts/setup.sh
sudo -n env PCI_ALLOWED="$PCI" DEV_TYPE=NVME "$SETUP" reset
```

For the receipt, discover the controller **from**
`/sys/bus/pci/devices/$PCI/nvme/`, not a remembered `/dev/nvmeN` name. Assert:

- exactly one controller and namespace, serial equals `SERIAL`;
- sysfs namespace ancestry contains `PCI`, driver is `nvme`;
- size is exactly `4,000,787,030,016` bytes;
- no mounts, partitions or holders (`lsblk`, namespace `holders/`);
- boot BDF/serial unchanged.

Set the kernel namespace software-read-only (`sudo blockdev --setro "$NS"`)
and verify `blockdev --getro` is 1. Read only these admin commands, saving each
output with exclusive file creation (`set -C` in the collection shell):

```sh
sudo -n nvme smart-log "$NS" -o json > "$OUT/smart.before.json"
sudo -n nvme id-ns "$NS" -o json > "$OUT/ns.before.json"
sudo -n nvme id-ctrl "$NS" -o json > "$OUT/ctrl.before.json"
sudo -n env PCI_ALLOWED="$PCI" DEV_TYPE=NVME HUGEMEM=8192 "$SETUP"
```

Prove driver is now `vfio-pci`, no vals units/valsd/fio, no stale mappings,
`HugePages_Total=4096` and `HugePages_Free=4096` (2 MiB pages). Save those checks
before keyscan. Do not change hugepage settings while another process owns
pages. `keyscan` itself takes the normal per-device lock; lock failure is a
stop condition, not an invitation to delete lock files.

After keyscan exits, repeat the stopped/map checks, then the same identity-
checked kernel receipt cycle, writing `*.after.json`, and return the device to
vfio-pci with all hugepages free. Collect these receipts even if a partition
failed. Never reset/rebind a device while keyscan is running.

**Read-only gate (coordinator-approved amendment, 2026-10-01):** compare numeric
`smart.data_units_written` (host Data Units Written), `ns.nuse`, `ns.nsze`, and
`ns.ncap` before/after. Require exact DUW, NSZE and NCAP equality and **no NUSE
increase** on each device; any violation is a hard stop. Record NUSE decreases
as settlement of prior 0353 DSM, not a failed gate. Save full SMART, including
errors, temperature and thermal counters; Data Units Read may increase. Example
after receipt collection:

```sh
python3 - "$OUT" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
b, a = [json.loads((p / ('smart.' + x + '.json')).read_text()) for x in ('before','after')]
nb, na = [json.loads((p / ('ns.' + x + '.json')).read_text()) for x in ('before','after')]
def number(v): return int(str(v).replace(',', ''))
assert number(b['data_units_written']) == number(a['data_units_written']), (b, a)
for k in ('nsze', 'ncap'): assert number(nb[k]) == number(na[k]), (k, nb, na)
assert number(na['nuse']) <= number(nb['nuse']), ('nuse', nb, na)
print('PASS: DUW/NSZE/NCAP unchanged; NUSE before/after:', nb['nuse'], na['nuse'])
PY
```

These hardware counters corroborate the write-trapping regression; DUW is a
coarse controller counter, not a byte-for-byte hardware proof by itself. The
coordinator approved NUSE decreases as asynchronous settlement of DSM issued
during 0353: NUSE already fell while stopped/vfio before KS0355-A, and the 0353
provision-only probe continued settling after completed DSM with no further
commands (see `bench/0353-dealloc/FINDINGS.md`, "Provision success versus delayed
namespace utilization"). Keyscan has no deallocate caller, never runs engine
maintenance, and the pin defaults `device_dealloc_segments_per_tick` to 0;
0360's regression traps deallocate/write/flush. Settlement concerns LBAs already
deallocated in 0353, not live data. Record this caveat when certifying a collection
under the amended gate; preserve original failed equality-gate receipts. A DUW
change, NUSE increase, identity/lock failure or CRC/open/read error is a stop,
not a reason to retry, start a daemon or repair the device.

## 3. Pass 1: index winner ids

Under the preconditions above, for each zone and host:

```sh
# CFG is the original node1.conf or node3.conf; ZONE is 2 first, then 1.
# OUT contains only the health/provenance receipts, not earlier keyscan output.
set -C
sudo -n env LD_LIBRARY_PATH=/home/user/vals/third_party/spdk/dpdk/build/lib:/home/user/vals/third_party/spdk/dpdk/build/lib/dpdk/pmds-24.1 \
  "$BUILD/target/release/valsd" keyscan -c "$CFG" \
  --zone-id "$ZONE" --out-dir "$OUT" > "$OUT/stdout.txt" 2> "$OUT/stderr.txt"
```

Set `LD_LIBRARY_PATH` explicitly through `sudo -n env` so the SPDK binary finds its DPDK libraries; `sudo -E` does not preserve it.

Record the exit status explicitly even on failure, then take the after receipts.
The command processes partitions sequentially, drops each engine, and starts
no listener, PD or maintenance thread. Engine recovery necessarily reads the
log tail to reconstruct the memtable; the subsequent winner scan is index-only
and reads no values. Each output id is the first 12 bytes of SHA-256(key), in
big-endian binary order. Files are in **key order**, not id order:

- `p0.ids` through `p7.ids`: live winners;
- `p0.tomb.ids` through `p7.tomb.ids`: tombstone winners;
- `summary.json`: per-partition counts, run count, manifest generation, elapsed
  time and error (null on success).

No existing output file is overwritten. Errors in an individual partition are
recorded and scanning continues; final exit is nonzero. Counts/files from a
failed partition can be partial. Require all eight partitions, null errors,
exit 0, and file lengths exactly 12 times their respective summary counts
before interpreting a full census. `--partition N` permits a targeted diagnostic
retry to a **new** directory; it is not a full-replica census.

Transfer only new output to devbox, explicitly from lab-1 and lab-3,
into `/data/dataset/task0355-keyscan/<collection-id>/lab-<N>/<zone>/pass1/`.
Use rsync with no deletion, save SHA-256 hashes of every transferred file on
both ends and compare. Retain failed-partition files and stderr, not just the
successful results. Never glob hosts or contact lab-2.

## 4. Expected ids and local diff (devbox, no network client)

Build the additive Go tooling from the same pin with the normal sibling bootes
module setup. Preserve the original real-client14 binary. Expected-id derivation
reads the sealed source cache and does not connect to Vals:

```sh
go -C bench/0321-long-soak/real-client14 build -o /data/dataset/task0355-keyscan/real-client-0360 .
ROOT=/data/dataset/task0355-keyscan/<collection-id>
FIXTURE=/data/dataset/task0353-results/DEALLOC0353-LONG/fixture.json
TOOL=/data/dataset/task0355-keyscan/real-client-0360
"$TOOL" census-ids --cache /data/dataset/jetstream-cache-20260824 \
  --fixture "$FIXTURE" \
  --fixture-sha256 4c3ef689fb7e87527607462a0bef67449dc616a7b10a99a311fe6fe41c9d021a \
  --prefix-events 1000000009 --source-host https://jetstream.us-west.bsky.network \
  --transform-workers 16 --out-dir "$ROOT/expected"
```

The new output directory must not exist. This reuses the fixture admission and
last-write reducer, checks source/canonical identity, and checks sorted unique
output count/digest against the pinned fixture census. `receipt.json` signals
successful completion; files without it are not certified expected sets.
Transient bucket files are removed on success and retained on failure. Budget
RAM and boot-disk scratch space for the large derivation; devbox, not the
64 GiB replicas, is the intended host. Expected final files occupy about 25 GB;
allow at least twice that for bucket concatenation plus observed outputs.

Diff each replica separately first, then the union (duplicates are allowed).
Explicit `p[0-7].ids` does **not** include tombstone files:

```sh
"$TOOL" census-diff --expected "$ROOT/expected/backlinks.ids" \
  --other-zone "$ROOT/expected/records.ids" --out "$ROOT/backlinks-union.json" \
  "$ROOT"/lab-1/backlinks/pass1/p[0-7].ids \
  "$ROOT"/lab-3/backlinks/pass1/p[0-7].ids
"$TOOL" census-diff --expected "$ROOT/expected/records.ids" \
  --other-zone "$ROOT/expected/backlinks.ids" --out "$ROOT/records-union.json" \
  "$ROOT"/lab-1/records/pass1/p[0-7].ids \
  "$ROOT"/lab-3/records/pass1/p[0-7].ids
```

Use distinct `--out` names for per-replica diffs. The JSON reports observed
union count/digest, extras with other-zone membership, missing count and the
smallest 100 missing ids, extras digest, and observed-minus-expected digest
(modulo 2^128). Compare observed counts and digests to the original verifier's
receipts, but do not mistake a union for agreement of both replicas. A union
can hide replica-specific missing keys.

## 5. Pass 2: identify keys, versions, pointers and records

Locally collect `extras[].id` and `missing_sample[]` from the backlinks union
**and both per-replica** diffs into a new text file, one unique 24-hex id per line.
If a missing count exceeds the 100-ID sample, report that tooling limit before
claiming all missing IDs were inspected. Absence on a replica is evidence too.
Transfer this small `want.txt` only to lab-1 and lab-3. Repeat the complete
precondition and before/after health gate on each host. Use new pass-2 output
directories; leave pass 1 untouched:

```sh
sudo -n env LD_LIBRARY_PATH=/home/user/vals/third_party/spdk/dpdk/build/lib:/home/user/vals/third_party/spdk/dpdk/build/lib/dpdk/pmds-24.1 \
  "$BUILD/target/release/valsd" keyscan -c "$CFG" --zone-id 2 \
  --want-ids "$WANT" --out-dir "$OUT2" > "$OUT2/stdout.txt" 2> "$OUT2/stderr.txt"
```

Pass 2 still scans all index winners (including tombstones), but writes only
`p<N>.jsonl` plus `summary.json`, not another full id set. Each matching key
reports hex and lossy text, its winning entry, the memtable entry if present,
and every version present in each run without bloom-filter shortcuts. Run
numbers are positions in the recovered manifest; handles include segment ids
and byte length. Versions are 32-hex u128 strings; pointers include segment,
offset and length. Each pointer's record is read and CRC/decode checked; an
error is recorded as `record.status`, not concealed by a successful lookup.
Check `key_matches`, record/index version and tombstone agreement, record kind,
value length, and all versions on both replicas. A memtable contains its current
entry, not every historical log write; this is deliberately not a raw vlog
history enumeration. Preserve all JSONL and before/after receipts and transfer
with hashes as for pass 1. If an extra belongs to the records expected set,
repeat pass 2 for zone 1 with the same wanted ids and a new output directory.

## 6. Confirmation and stopping point

If the three persistent backlinks extras explain the original failure, require:

- expected backlinks live count `1,090,186,118`, observed union `1,090,186,121`;
- exactly three unique extra ids, missing count zero; inspect each replica's
  diff to establish where each key persists;
- extras sum, and observed-minus-expected digest, both equal
  **`00000001ccdf183e889d1ebdd0d89ccb`** (i.e. `0x1ccdf183e889d1ebdd0d89ccb`);
- recomputed SHA-256(key_hex decoded)[:12] equals the reported id for each key.

For example, on devbox, after collection:

```python
import hashlib, json, pathlib
root = pathlib.Path('/data/dataset/task0355-keyscan/<collection-id>')
d = json.loads((root / 'backlinks-union.json').read_text())
ids = {e['id'] for e in d['extras']}
assert len(ids) == 3 and d['missing_count'] == 0
assert d['observed_union_count'] == 1090186121
assert d['expected_count'] == 1090186118
delta = f'{sum(int(i, 16) for i in ids) % (1 << 128):032x}'
assert delta == d['extras_digest'] == d['observed_minus_expected_digest'] == '00000001ccdf183e889d1ebdd0d89ccb'
wanted = set((root / 'want.txt').read_text().splitlines())
assert ids <= wanted
found_extras = set()
for p in root.glob('lab-*/backlinks/pass2/p*.jsonl'):
    for line in p.read_text().splitlines():
        row = json.loads(line)
        assert hashlib.sha256(bytes.fromhex(row['key_hex'])).hexdigest()[:24] == row['id']
        assert row['id'] in wanted
        if row['id'] in ids:
            found_extras.add(row['id'])
assert found_extras == ids
```

If there are fewer/different extras, missing ids, replica disagreement, corrupt
runs or pointer records, report exactly that. Do not force the desired delta,
ignore failed partitions, or count duplicates as separate extras. The tooling
uses 96-bit identities; any different keys sharing an id must be reported, not
silently treated as proven identical. Finish with both devices vfio-bound,
units absent, no valsd, hugepages free, retained evidence hashes and a 0355
investigation note. Identification is not a repair or a correctness release.

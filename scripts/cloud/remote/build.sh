#!/usr/bin/env bash
#
# build.sh — build valsd + vals-bench with the SPDK backend on a fresh Ubuntu
# 24.04 EC2 builder and pack a deployable artifact (scripts/cloud/fleet.py build).
#
#   bash build.sh <build-dir> <git-rev>
#
# <build-dir>/src.tar.gz is `git archive --prefix=valstore/ <rev>`. The artifact
# lands in <build-dir>/artifact/vals-<rev12>.tar.gz with:
#   bin/valsd bin/vals-bench    (release, line tables for perf; SPDK statically linked)
#   lib/                         DPDK shared libraries (run with LD_LIBRARY_PATH=lib)
#   spdk/scripts/ spdk/include/spdk/pci_ids.h   (setup.sh and what it reads)
#   artifact.json                rev, toolchain, SPDK tag, builder CPU model + flags
#
# DPDK/SPDK build with -march=native (SPDK's default), so build on the same
# CPU generation as the fleet: fleet.py refuses to deploy on a host whose CPU
# lacks any flag the builder had.
set -euo pipefail

BUILD="${1:?build dir}"
REV="${2:?git rev}"
SPDK_TAG="v24.09"
RUST_TOOLCHAIN="1.96.0"
SHORT="${REV:0:12}"
cd "$BUILD"
log() { printf '[build %s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }

log "apt build dependencies"
export DEBIAN_FRONTEND=noninteractive NEEDRESTART_MODE=a
APT="apt-get -o DPkg::Lock::Timeout=900"
sudo -E $APT update -q
sudo -E $APT install -y -q build-essential pkg-config libssl-dev clang git curl \
  autoconf automake libtool libtool-bin libnuma-dev libaio-dev uuid-dev libncurses-dev \
  meson ninja-build nasm python3-pip python3-pyelftools python3-jinja2 python3-tabulate \
  libcunit1-dev libjson-c-dev libcmocka-dev

if ! command -v "$HOME/.cargo/bin/rustup" >/dev/null 2>&1; then
  log "rustup"
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path --default-toolchain none
fi
export PATH="$HOME/.cargo/bin:$PATH"
rustup toolchain install "$RUST_TOOLCHAIN" --profile minimal

log "source $REV"
rm -rf valstore && tar xzf src.tar.gz
cd valstore
if [ ! -d "$BUILD/spdk/.git" ]; then
  log "clone SPDK $SPDK_TAG"
  git clone --branch "$SPDK_TAG" --depth 1 https://github.com/spdk/spdk "$BUILD/spdk"
  git -C "$BUILD/spdk" submodule update --init --depth 1
fi
mkdir -p third_party && ln -sfn "$BUILD/spdk" third_party/spdk
sudo "$BUILD/spdk/scripts/pkgdep.sh" >"$BUILD/pkgdep.log" 2>&1 || log "pkgdep.sh exited nonzero (continuing; see pkgdep.log)"
if [ ! -e "$BUILD/spdk/build/lib/libspdk_nvme.a" ]; then
  log "configure + make SPDK"
  ( cd "$BUILD/spdk" && ./configure --without-crypto --without-vhost --without-virtio \
      --disable-tests --disable-unit-tests --disable-examples --disable-apps )
  make -C "$BUILD/spdk" -j"$(nproc)" >"$BUILD/spdk-make.log" 2>&1 || { tail -50 "$BUILD/spdk-make.log"; exit 1; }
fi

log "cargo build (spdk)"
CARGO_PROFILE_RELEASE_DEBUG=line-tables-only VALS_SPDK_LINK=1 \
  cargo +"$RUST_TOOLCHAIN" build --locked --release -p vals-server -p vals-bench --features spdk

log "pack"
OUT="$BUILD/artifact/vals-$SHORT"
rm -rf "$OUT" && mkdir -p "$OUT/bin" "$OUT/lib" "$OUT/spdk/include/spdk"
cp target/release/valsd target/release/vals-bench "$OUT/bin/"
# DPDK: every shared object under dpdk/build/lib, dereferenced and flattened into lib/ (first copy of a
# name wins; dangling links are skipped). EAL also dlopen()s every driver in its compiled-in plugin
# directory (RTE_EAL_PMD_PATH, an absolute path on this builder; it holds e.g. librte_mempool_ring,
# which the linker drops from NEEDED). Ship that directory as pmds/ with links into ../lib (same
# inodes as the NEEDED copies, so nothing loads twice); deploy recreates the path as a link to pmds/.
DPDK_LIB="$BUILD/spdk/dpdk/build/lib"
echo "--- dpdk shared objects (path type link-target)"; find "$DPDK_LIB" -name '*.so*' -printf '%P %y %l\n' | sort
find "$DPDK_LIB" -name '*.so*' | sort | while read -r f; do
  n=$(basename "$f")
  [ -e "$OUT/lib/$n" ] || cp -L "$f" "$OUT/lib/$n" 2>/dev/null || true
done
PMD_PATH=$(sed -n 's/^#define RTE_EAL_PMD_PATH "\(.*\)"$/\1/p' "$BUILD/spdk/dpdk/build/include/rte_build_config.h" 2>/dev/null || true)
[ -n "$PMD_PATH" ] || PMD_PATH=$(find "$DPDK_LIB/dpdk" -maxdepth 1 -type d -name 'pmds-*' | head -1)
echo "--- RTE_EAL_PMD_PATH=$PMD_PATH"
mkdir -p "$OUT/pmds"
if [ -d "$PMD_PATH" ]; then
  for f in "$PMD_PATH"/*.so*; do
    [ -e "$f" ] || continue
    n=$(basename "$f")
    [ -e "$OUT/lib/$n" ] || cp -L "$f" "$OUT/lib/$n"
    ln -sfn "../lib/$n" "$OUT/pmds/$n"
  done
fi
# Resolve anything still missing from the whole DPDK tree (e.g. a soname link only under build-tmp).
for b in valsd vals-bench; do
  LD_LIBRARY_PATH="$OUT/lib" ldd "$OUT/bin/$b" | awk '/not found/{print $1}' | while read -r n; do
    f=$(find "$BUILD/spdk/dpdk" -name "$n" | head -1)
    [ -n "$f" ] && cp -L "$f" "$OUT/lib/$n" && echo "resolved $n from $f"
  done
done
cp -a "$BUILD/spdk/scripts" "$OUT/spdk/"
cp "$BUILD/spdk/include/spdk/pci_ids.h" "$OUT/spdk/include/spdk/"
for b in valsd vals-bench; do
  if LD_LIBRARY_PATH="$OUT/lib" ldd "$OUT/bin/$b" | grep -q 'not found'; then
    LD_LIBRARY_PATH="$OUT/lib" ldd "$OUT/bin/$b"
    echo "$b has unresolved libraries" >&2
    exit 1
  fi
done
LD_LIBRARY_PATH="$OUT/lib" ldd "$OUT/bin/valsd" "$OUT/bin/vals-bench" | awk '/=> \//{print $1}' | sort -u > "$OUT/needed-libs.txt"
log "smoke test: packed binaries with the build tree moved away"
SMOKE="$BUILD/smoke.log"
mv "$BUILD/spdk" "$BUILD/spdk.away"
mkdir -p "$(dirname "$PMD_PATH")" && ln -sfn "$OUT/pmds" "$PMD_PATH"
sudo sysctl -q vm.nr_hugepages=1024
set +e
sudo env LD_LIBRARY_PATH="$OUT/lib" LD_DEBUG=libs "$OUT/bin/vals-bench" spdk asyncread 16 4096 0000:ff:1f.7 1 >"$SMOKE" 2>&1
set -e
rm -rf "$BUILD/spdk" && mv "$BUILD/spdk.away" "$BUILD/spdk"
grep -E 'spdk_env_init|attach|EAL|panicked' "$SMOKE" | grep -v 'LD_DEBUG\|^ *[0-9]*:' | head -20 || true
grep -q 'failed to attach NVMe' "$SMOKE" || { echo "smoke: did not get past spdk_env_init"; tail -40 "$SMOKE"; exit 1; }
grep -q 'calling init: .*/pmds/librte_mempool_ring\|calling init: .*librte_mempool_ring' "$SMOKE" \
  || { echo "smoke: librte_mempool_ring was not loaded"; grep -i 'mempool\|pmds' "$SMOKE" | head; exit 1; }
log "smoke ok: EAL initialised from the artifact, mempool_ring loaded, bogus PCI refused"

python3 - "$OUT/artifact.json" "$REV" "$SPDK_TAG" "$PMD_PATH" <<'PY'
import json, platform, subprocess, sys, datetime
out, rev, tag, pmd_path = sys.argv[1:]
flags, model = [], ""
for line in open("/proc/cpuinfo"):
    if line.startswith("flags") and not flags:
        flags = line.split(":", 1)[1].split()
    if line.startswith("model name") and not model:
        model = line.split(":", 1)[1].strip()
def cmd(*a):
    return subprocess.run(a, capture_output=True, text=True).stdout.strip()
imds_type = cmd("sh", "-c", "t=$(curl -s -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token) && curl -s -H \"X-aws-ec2-metadata-token: $t\" http://169.254.169.254/latest/meta-data/instance-type")
json.dump({"rev": rev, "spdk_tag": tag, "rustc": cmd("rustc", "+1.96.0", "-V"),
           "built": datetime.datetime.now(datetime.timezone.utc).isoformat(),
           "builder_instance_type": imds_type, "cpu_model": model, "cpu_flags": sorted(flags),
           "kernel": platform.release(), "glibc": platform.libc_ver()[1],
           "profile": "release + debug=line-tables-only", "features": ["spdk"],
           "pmd_path": pmd_path},
          open(out, "w"), indent=2)
PY
tar -C "$BUILD/artifact" -czf "$BUILD/artifact/vals-$SHORT.tar.gz" "vals-$SHORT"
ls -l "$BUILD/artifact/vals-$SHORT.tar.gz"
log "done"

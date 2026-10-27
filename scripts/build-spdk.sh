#!/usr/bin/env bash
#
# build-spdk.sh — clone + build the pinned SPDK, then build vals with the SPDK
# backend, on an lab box-validation node. Repeatable/idempotent; run from the
# vals checkout root (default ~/vals). See docs/SPDK.md §Building SPDK.
#
#   ./scripts/build-spdk.sh            # full: spdk deps + spdk build + vals build
#   ./scripts/build-spdk.sh --spdk     # just (re)build third_party/spdk
#   ./scripts/build-spdk.sh --vals     # just (re)build vals (spdk already built)
set -euo pipefail

SPDK_TAG="v24.09"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"
export PATH="$HOME/.cargo/bin:$PATH"

log() { printf '\033[1;32m[build-spdk]\033[0m %s\n' "$*"; }

build_spdk() {
  if [ ! -d third_party/spdk/.git ]; then
    log "cloning SPDK $SPDK_TAG"
    git clone --branch "$SPDK_TAG" --depth 1 https://github.com/spdk/spdk third_party/spdk
  fi
  log "fetching SPDK submodules (dpdk, …)"
  git -C third_party/spdk submodule update --init
  # SPDK's own distro deps (libnuma-dev, libaio-dev, …) — needs sudo.
  log "installing SPDK distro deps (pkgdep.sh)"
  sudo third_party/spdk/scripts/pkgdep.sh || log "pkgdep.sh returned nonzero (continuing)"
  # Python deps pkgdep.sh misses (docs/SPDK.md); provisioning installed these,
  # re-ensure for a standalone run.
  python3 -m pip install --user --break-system-packages -q pyelftools jinja2 tabulate || true
  log "configure (minimal: no crypto/vhost/virtio/tests/apps)"
  ( cd third_party/spdk && ./configure \
      --without-crypto --without-vhost --without-virtio \
      --disable-tests --disable-unit-tests --disable-examples --disable-apps )
  log "make -j$(nproc) (this is the long part)"
  make -C third_party/spdk -j"$(nproc)"
  log "SPDK built"
}

build_vals() {
  log "building vals with --features spdk (VALS_SPDK_LINK=1)"
  VALS_SPDK_LINK=1 cargo build --release -p vals-server -p vals-bench --features spdk
  log "vals built: target/release/{valsd,vals-bench}"
  ls -l target/release/valsd target/release/vals-bench 2>/dev/null || true
}

case "${1:---all}" in
  --spdk) build_spdk;;
  --vals) build_vals;;
  --all)  build_spdk; build_vals;;
  *) echo "usage: $0 [--all|--spdk|--vals]"; exit 1;;
esac
log "done"

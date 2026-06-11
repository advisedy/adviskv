#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"

ADVISKV_DEPS_ROOT="${ADVISKV_DEPS_ROOT:-$REPO_ROOT/.adviskv_deps}"
VCPKG_ROOT="${VCPKG_ROOT:-$REPO_ROOT/third_party/vcpkg}"
VCPKG_EXE="${VCPKG_EXE:-$VCPKG_ROOT/vcpkg}"
VCPKG_STATE_ROOT="${VCPKG_STATE_ROOT:-$ADVISKV_DEPS_ROOT/vcpkg}"
VCPKG_INSTALL_ROOT="$VCPKG_STATE_ROOT/installed"
VCPKG_DOWNLOADS="$VCPKG_STATE_ROOT/downloads"
VCPKG_BINARY_CACHE="$VCPKG_STATE_ROOT/binary-cache"

cd "$REPO_ROOT"

if [[ ! -f "$VCPKG_ROOT/bootstrap-vcpkg.sh" ]]; then
  echo "missing vcpkg submodule under $VCPKG_ROOT" >&2
  echo "try: git submodule update --init --recursive" >&2
  exit 1
fi

if [[ ! -x "$VCPKG_EXE" ]]; then
  "$VCPKG_ROOT/bootstrap-vcpkg.sh"
fi

mkdir -p "$VCPKG_INSTALL_ROOT" "$VCPKG_DOWNLOADS" "$VCPKG_BINARY_CACHE"

"$VCPKG_EXE" install \
  --downloads-root="$VCPKG_DOWNLOADS" \
  --x-install-root="$VCPKG_INSTALL_ROOT" \
  --binarysource=clear \
  --binarysource="files,$VCPKG_BINARY_CACHE,readwrite" \
  "$@"

echo "vcpkg dependencies are ready under $VCPKG_INSTALL_ROOT"

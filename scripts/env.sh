#!/usr/bin/env bash
# Shared defaults for AdvisKV scripts. Source from other scripts.
# Day-to-day paths:
#   .adviskv_deps/   deps (setup.sh)
#   build/           binaries + runtime (build.sh)
#   third_party/vcpkg/  vcpkg tool only

if [[ -n "${ADVISKV_ENV_LOADED:-}" ]]; then
  return 0 2>/dev/null || exit 0
fi

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
BUILD_TYPE="${BUILD_TYPE:-Debug}"
BUILD_DIR="${BUILD_DIR:-build}"

case "$BUILD_DIR" in
  /*) ;;
  *) BUILD_DIR="$ROOT_DIR/$BUILD_DIR" ;;
esac

export ROOT_DIR BUILD_TYPE BUILD_DIR
ADVISKV_ENV_LOADED=1
export ADVISKV_ENV_LOADED

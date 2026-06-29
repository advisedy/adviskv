#!/usr/bin/env bash
set -euo pipefail

BUILD_TYPE="${BUILD_TYPE:-Release}"

if [[ -z "${BUILD_DIR:-}" ]]; then
  case "$BUILD_TYPE" in
    Debug)
      BUILD_DIR="build/debug"
      ;;
    Release)
      BUILD_DIR="build/release"
      ;;
    *)
      BUILD_DIR="build/$BUILD_TYPE"
      ;;
  esac
fi

"$(dirname "$0")/build.sh" "$@"

ctest --test-dir "$BUILD_DIR" --output-on-failure

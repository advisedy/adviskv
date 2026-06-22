#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$REPO_ROOT"

BUILD_TYPE="${BUILD_TYPE:-Release}"
GENERATOR="${GENERATOR:-Ninja}"

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

cmake \
  -S . \
  -B "$BUILD_DIR" \
  -G "$GENERATOR" \
  -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
  "$@"

build_args=(--build "$BUILD_DIR" --parallel)

if [[ -n "${BUILD_TARGETS:-}" ]]; then
  read -r -a targets <<< "$BUILD_TARGETS"
  build_args+=(--target "${targets[@]}")
fi

cmake "${build_args[@]}"

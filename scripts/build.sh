#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"
cd "$ROOT_DIR"

GENERATOR="${GENERATOR:-Ninja}"

cmake -S . -B "$BUILD_DIR" -G "$GENERATOR" -DCMAKE_BUILD_TYPE="$BUILD_TYPE" "$@"

build_args=(--build "$BUILD_DIR" --parallel)
if [[ -n "${BUILD_TARGETS:-}" ]]; then
  read -r -a targets <<< "$BUILD_TARGETS"
  build_args+=(--target "${targets[@]}")
fi
cmake "${build_args[@]}"

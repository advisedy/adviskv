#!/usr/bin/env bash
set -euo pipefail

BUILD_DIR="${BUILD_DIR:-build}"

"$(dirname "$0")/build.sh" "$@"

ctest --test-dir "$BUILD_DIR" --output-on-failure

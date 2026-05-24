#!/usr/bin/env bash
set -euo pipefail

BUILD_DIR="${BUILD_DIR:-build-coverage}"
GCOVR="${GCOVR:-gcovr}"

if ! command -v "$GCOVR" >/dev/null 2>&1; then
  echo "gcovr is required. Install it with 'brew install gcovr', 'pipx install gcovr', or your system package manager." >&2
  exit 1
fi

BUILD_DIR="$BUILD_DIR" BUILD_TYPE=Debug ./scripts/build.sh \
  -DADVISKV_ENABLE_COVERAGE=ON \
  "$@"

ctest --test-dir "$BUILD_DIR" --output-on-failure

"$GCOVR" -r . "$BUILD_DIR" \
  --exclude 'third_party/.*' \
  --exclude 'test/.*' \
  --exclude 'build.*/.*' \
  --html --html-details -o "$BUILD_DIR/coverage.html" \
  --xml -o "$BUILD_DIR/coverage.xml" \
  --print-summary

echo "coverage html: $BUILD_DIR/coverage.html"
echo "coverage xml:  $BUILD_DIR/coverage.xml"

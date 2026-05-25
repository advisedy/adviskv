#!/usr/bin/env bash
set -euo pipefail

BUILD_DIR="${BUILD_DIR:-build-coverage}"
GCOVR="${GCOVR:-gcovr}"
GCOV="${GCOV:-}"
REPORT_DIR="${REPORT_DIR:-$BUILD_DIR/coverage-report}"

if ! command -v "$GCOVR" >/dev/null 2>&1; then
  echo "gcovr is required. Install it with 'brew install gcovr', 'pipx install gcovr', or your system package manager." >&2
  exit 1
fi

if [ -z "$GCOV" ] && command -v xcrun >/dev/null 2>&1; then
  LLVM_COV="$(xcrun --find llvm-cov 2>/dev/null || true)"
  if [ -n "$LLVM_COV" ]; then
    GCOV="$LLVM_COV gcov"
  fi
fi

if [ -d "$BUILD_DIR" ]; then
  find "$BUILD_DIR" -name '*.gcda' -delete
fi

GCOV_ARGS=()
if [ -n "$GCOV" ]; then
  GCOV_ARGS=(--gcov-executable "$GCOV")
fi

BUILD_DIR="$BUILD_DIR" BUILD_TYPE=Debug ./scripts/build.sh \
  -DADVISKV_ENABLE_COVERAGE=ON \
  "$@"

find "$BUILD_DIR" -name '*.gcda' -delete

ctest --test-dir "$BUILD_DIR" --output-on-failure --parallel 1

rm -rf "$REPORT_DIR"
mkdir -p "$REPORT_DIR"
find "$BUILD_DIR" -maxdepth 1 \
  \( -name 'coverage*.html' -o -name 'coverage*.css' -o -name 'coverage*.js' -o -name 'coverage*.xml' \) \
  -delete

"$GCOVR" -r . "$BUILD_DIR" \
  "${GCOV_ARGS[@]}" \
  --exclude 'third_party/.*' \
  --exclude 'test/.*' \
  --exclude 'build.*/.*' \
  --html-details "$REPORT_DIR/index.html" \
  --xml "$REPORT_DIR/coverage.xml" \
  --print-summary

printf '%s\n' \
  '<!doctype html>' \
  '<html><head><meta charset="utf-8">' \
  '<meta http-equiv="refresh" content="0; url=coverage-report/index.html">' \
  '<title>Coverage Report</title></head>' \
  '<body><a href="coverage-report/index.html">Open coverage report</a></body></html>' \
  > "$BUILD_DIR/coverage.html"

echo "coverage html: $BUILD_DIR/coverage.html"
echo "coverage xml:  $REPORT_DIR/coverage.xml"

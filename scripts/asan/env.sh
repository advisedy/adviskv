#!/usr/bin/env bash

ASAN_SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
export ROOT_DIR="$(cd -- "$ASAN_SCRIPT_DIR/../.." && pwd -P)"

export SANITIZER="address"
export BUILD_TYPE="${BUILD_TYPE:-Debug}"
export BUILD_DIR="${BUILD_DIR:-build/asan}"

# The address sanitizer build also enables UBSan in CMake.
default_asan_options="halt_on_error=1:abort_on_error=1:strict_string_checks=1:check_initialization_order=1"
if [[ "$(uname -s)" == "Darwin" ]]; then
  default_asan_options="allow_user_poisoning=0:${default_asan_options}"
else
  default_asan_options="detect_leaks=1:${default_asan_options}"
fi

export ASAN_OPTIONS="${ASAN_OPTIONS:-$default_asan_options}"
export UBSAN_OPTIONS="${UBSAN_OPTIONS:-halt_on_error=1:abort_on_error=1:print_stacktrace=1}"
export LSAN_OPTIONS="${LSAN_OPTIONS:-print_suppressions=0}"

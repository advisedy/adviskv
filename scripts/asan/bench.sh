#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/env.sh"

cd "$ROOT_DIR"
BUILD_TARGETS="sdm meta storage bench_client" \
  ./scripts/build.sh -DADVISKV_SANITIZER=address

SKIP_BUILD=1 exec ./scripts/bench.sh "$@"

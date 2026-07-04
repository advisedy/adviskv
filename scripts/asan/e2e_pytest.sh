#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/env.sh"

cd "$ROOT_DIR"
BUILD_TARGETS="sdm sdm_test meta storage e2e_client" \
  ./scripts/build.sh -DADVISKV_SANITIZER=address

exec ./scripts/e2e_pytest.sh "$@"

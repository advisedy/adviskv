#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/env.sh"

cd "$ROOT_DIR"
exec ./scripts/run_test.sh -DADVISKV_SANITIZER=address "$@"

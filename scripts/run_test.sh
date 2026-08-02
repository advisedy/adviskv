#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"

"$SCRIPT_DIR/build.sh" "$@"
ctest --test-dir "$BUILD_DIR" --output-on-failure

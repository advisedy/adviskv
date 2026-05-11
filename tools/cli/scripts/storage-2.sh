#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
exec "$ROOT_DIR/build/bin/storage_cli" \
  --conf "$ROOT_DIR/tools/cli/conf/storage-2.yaml"

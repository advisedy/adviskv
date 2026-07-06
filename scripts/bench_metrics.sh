#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Same user-facing parameters as scripts/bench.sh. It enables service metrics
# sampling and prints the generated text report path when the run ends.
export BENCH_METRICS_REPORT=1
exec "$ROOT_DIR/scripts/bench.sh" "$@"

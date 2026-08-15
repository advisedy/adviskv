#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"

"$SCRIPT_DIR/build.sh" "$@"
ctest --test-dir "$BUILD_DIR" --output-on-failure

MAELSTROM_BIN="${MAELSTROM_BIN:-$ROOT_DIR/third_party/maelstrom/maelstrom}"
MAELSTROM_NODE_BIN="${NODE_BIN:-$BUILD_DIR/bin/maelstrom_raft_node}"
if [[ -x "$MAELSTROM_BIN" && -x "$MAELSTROM_NODE_BIN" ]]; then
  echo "[test] Maelstrom detected; running normal Raft test"
  NODE_COUNT="${MAELSTROM_NORMAL_NODE_COUNT:-3}" \
  CONCURRENCY="${MAELSTROM_NORMAL_CONCURRENCY:-2n}" \
  RATE="${MAELSTROM_NORMAL_RATE:-50}" \
  TIME_LIMIT="${MAELSTROM_NORMAL_TIME_LIMIT:-20}" \
  MAELSTROM_BIN="$MAELSTROM_BIN" \
  NODE_BIN="$MAELSTROM_NODE_BIN" \
  "$SCRIPT_DIR/internal/run_maelstrom_raft.sh"

  echo "[test] Running fault-injection stress test"
  NODE_COUNT="${MAELSTROM_TEST_NODE_COUNT:-5}" \
  CONCURRENCY="${MAELSTROM_TEST_CONCURRENCY:-10}" \
  RATE="${MAELSTROM_TEST_RATE:-100}" \
  TIME_LIMIT="${MAELSTROM_TEST_TIME_LIMIT:-60}" \
  MAELSTROM_BIN="$MAELSTROM_BIN" \
  NODE_BIN="$MAELSTROM_NODE_BIN" \
  "$SCRIPT_DIR/internal/run_maelstrom_raft.sh" --nemesis partition
elif [[ ! -x "$MAELSTROM_BIN" ]]; then
  echo "[test] Maelstrom not found; skipping Maelstrom test"
else
  echo "[test] Maelstrom adapter not found; skipping Maelstrom test"
fi

#!/usr/bin/env bash
# Run Maelstrom lin-kv against AdvisKV's maelstrom_raft_node.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MAELSTROM_BIN="${MAELSTROM_BIN:-${ROOT}/third_party/maelstrom/maelstrom}"
NODE_BIN="${NODE_BIN:-${ROOT}/build/bin/maelstrom_raft_node}"
export ADVISKV_MAELSTROM_DATA="${ADVISKV_MAELSTROM_DATA:-/tmp/adviskv_maelstrom}"

# Jepsen/Maelstrom always write results to ./store under the process CWD.
# Default: tools/maelstrom_raft/store (override with MAELSTROM_STORE_DIR = parent of store/).
MAELSTROM_STORE_DIR="${MAELSTROM_STORE_DIR:-${ROOT}/tools/maelstrom_raft}"

if [[ ! -e "${MAELSTROM_BIN}" ]]; then
  echo "Maelstrom not found at ${MAELSTROM_BIN}" >&2
  echo "Install with: ./scripts/setup.sh --with-maelstrom-test" >&2
  echo "(Maelstrom is a JVM tool under third_party/, not a vcpkg package.)" >&2
  exit 1
fi

if [[ ! -x "${NODE_BIN}" ]]; then
  echo "Node binary not found: ${NODE_BIN}" >&2
  echo "Build with: ./scripts/build.sh   (target: maelstrom_raft_node)" >&2
  exit 1
fi

mkdir -p "${ADVISKV_MAELSTROM_DATA}"
mkdir -p "${MAELSTROM_STORE_DIR}"

# Fresh node data dir for each run unless KEEP_MAELSTROM_DATA=1
if [[ "${KEEP_MAELSTROM_DATA:-0}" != "1" ]]; then
  rm -rf "${ADVISKV_MAELSTROM_DATA}/n"* "${ADVISKV_MAELSTROM_DATA}/logs" 2>/dev/null || true
  mkdir -p "${ADVISKV_MAELSTROM_DATA}/logs"
fi

NODE_COUNT="${NODE_COUNT:-3}"
CONCURRENCY="${CONCURRENCY:-2n}"
RATE="${RATE:-50}"
TIME_LIMIT="${TIME_LIMIT:-20}"

EXTRA_ARGS=("$@")

echo "Using Maelstrom: ${MAELSTROM_BIN}"
echo "Using node:      ${NODE_BIN}"
echo "Data dir:        ${ADVISKV_MAELSTROM_DATA}"
echo "Results store:   ${MAELSTROM_STORE_DIR}/store  (latest -> store/latest)"

# Note: without gnuplot, Maelstrom may exit non-zero with :valid? :unknown even when
# the lin-kv linearizability checker passes. Install gnuplot for full reports.
cd "${MAELSTROM_STORE_DIR}"
exec "${MAELSTROM_BIN}" test -w lin-kv \
  --bin "${NODE_BIN}" \
  --node-count "${NODE_COUNT}" \
  --concurrency "${CONCURRENCY}" \
  --rate "${RATE}" \
  --time-limit "${TIME_LIMIT}" \
  --log-stderr \
  "${EXTRA_ARGS[@]}"

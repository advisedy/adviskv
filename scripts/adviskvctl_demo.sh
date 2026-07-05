#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${PYTHON:-python3}"
BUILD_TYPE="${BUILD_TYPE:-Release}"
if [[ -z "${BUILD_DIR:-}" ]]; then
  case "$BUILD_TYPE" in
    Debug)
      BUILD_DIR="$ROOT_DIR/build/debug"
      ;;
    Release)
      BUILD_DIR="$ROOT_DIR/build/release"
      ;;
    *)
      BUILD_DIR="$ROOT_DIR/build/$BUILD_TYPE"
      ;;
  esac
fi
case "$BUILD_DIR" in
  /*) ;;
  *) BUILD_DIR="$ROOT_DIR/$BUILD_DIR" ;;
esac
ADVISKVCTL="$BUILD_DIR/bin/adviskvctl"
CLIENT_CONF="${CLIENT_CONF:-conf/client.yaml}"

cleanup() {
  echo
  echo "[demo] stopping local cluster"
  "$PYTHON" "$ROOT_DIR/scripts/internal/local_cluster.py" stop || true
}

require_binary() {
  local binary="$1"
  if [[ -x "$binary" ]]; then
    return
  fi
  echo "[demo] missing binary: $binary" >&2
  echo "[demo] run ./scripts/build.sh first, or set BUILD_DIR to the build output" >&2
  exit 1
}

echo "[demo] stopping stale local processes"
"$ROOT_DIR/scripts/stop_cluster.sh"

echo "[demo] clearing local demo data"
rm -rf "$BUILD_DIR/runtime"

require_binary "$BUILD_DIR/bin/sdm"
require_binary "$BUILD_DIR/bin/meta"
require_binary "$BUILD_DIR/bin/storage"
require_binary "$ADVISKVCTL"

trap cleanup EXIT INT TERM

echo "[demo] starting local cluster"
"$PYTHON" "$ROOT_DIR/scripts/internal/local_cluster.py" start

echo "[demo] opening AdvisKV shell"
"$ADVISKVCTL" --conf="$CLIENT_CONF"

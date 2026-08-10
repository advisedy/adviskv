#!/usr/bin/env bash
# Download Maelstrom into third_party/maelstrom (NOT vcpkg).
# Maelstrom is a JVM test harness; AdvisKV only provides --bin node process.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEST="${ROOT}/third_party/maelstrom"
VERSION="${MAELSTROM_VERSION:-0.2.3}"
URL="${MAELSTROM_URL:-https://github.com/jepsen-io/maelstrom/releases/download/v${VERSION}/maelstrom.tar.bz2}"

if [[ -x "${DEST}/maelstrom" ]]; then
  echo "Maelstrom already present: ${DEST}/maelstrom"
  "${DEST}/maelstrom" test --help >/dev/null 2>&1 || true
  exit 0
fi

if ! command -v java >/dev/null 2>&1; then
  echo "ERROR: Java is required to run Maelstrom (e.g. OpenJDK 17+)." >&2
  exit 1
fi

mkdir -p "${ROOT}/third_party"
TMP="$(mktemp -d)"
cleanup() { rm -rf "${TMP}"; }
trap cleanup EXIT

echo "Downloading Maelstrom ${VERSION} from ${URL}"
if command -v curl >/dev/null 2>&1; then
  curl -fL --retry 3 -o "${TMP}/maelstrom.tar.bz2" "${URL}"
elif command -v wget >/dev/null 2>&1; then
  wget -O "${TMP}/maelstrom.tar.bz2" "${URL}"
else
  echo "ERROR: need curl or wget to download Maelstrom." >&2
  exit 1
fi

tar -xjf "${TMP}/maelstrom.tar.bz2" -C "${TMP}"
# Release tarball typically contains a top-level "maelstrom/" directory.
if [[ -d "${TMP}/maelstrom" ]]; then
  rm -rf "${DEST}"
  mv "${TMP}/maelstrom" "${DEST}"
else
  rm -rf "${DEST}"
  mkdir -p "${DEST}"
  mv "${TMP}"/* "${DEST}/"
fi

if [[ ! -x "${DEST}/maelstrom" ]]; then
  # Some layouts use a shell wrapper; make sure it is executable if present.
  if [[ -f "${DEST}/maelstrom" ]]; then
    chmod +x "${DEST}/maelstrom"
  fi
fi

if [[ ! -e "${DEST}/maelstrom" ]]; then
  echo "ERROR: expected ${DEST}/maelstrom after extract. Contents:" >&2
  ls -la "${DEST}" >&2
  exit 1
fi

echo "Installed Maelstrom to ${DEST}"
echo "Run: ./scripts/internal/run_maelstrom_raft.sh"

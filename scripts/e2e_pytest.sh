#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${VENV_DIR:-$ROOT_DIR/.venv}"
PYTHON="${PYTHON:-$VENV_DIR/bin/python}"
REQUIREMENTS="$ROOT_DIR/test/e2e/requirements.txt"

if [[ ! -x "$PYTHON" ]]; then
    if [[ -n "${PYTHON:-}" && "$PYTHON" != "$VENV_DIR/bin/python" ]]; then
        echo "configured PYTHON is not executable: $PYTHON" >&2
        exit 1
    fi
    echo "[e2e] creating Python virtual environment: $VENV_DIR"
    python3 -m venv "$VENV_DIR"
    PYTHON="$VENV_DIR/bin/python"
fi

if ! "$PYTHON" -m pytest --version >/dev/null 2>&1; then
    echo "[e2e] installing Python E2E dependencies from: $REQUIREMENTS"
    "$PYTHON" -m pip install -r "$REQUIREMENTS"
fi

"$ROOT_DIR/scripts/stop_cluster.sh"
"$PYTHON" -m pytest -c "$ROOT_DIR/test/e2e/pytest.ini" "$ROOT_DIR/test/e2e" "$@"

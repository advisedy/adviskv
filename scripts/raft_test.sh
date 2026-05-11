#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="$ROOT_DIR/build/bin"
LOG_DIR="$ROOT_DIR/build/logs/raft-test"

mkdir -p "$LOG_DIR"

declare -a PIDS=()

require_bin() {
    local bin_path="$1"
    if [[ ! -x "$bin_path" ]]; then
        echo "missing binary: $bin_path"
        echo "run ./scripts/build.sh first"
        exit 1
    fi
}

cleanup() {
    local exit_code=$?
    trap - EXIT INT TERM

    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    wait || true
    exit "$exit_code"
}

start_process() {
    local name="$1"
    shift

    local log_file="$LOG_DIR/${name}.log"
    echo "starting $name ..."
    (
        cd "$ROOT_DIR"
        "$@"
    ) >"$log_file" 2>&1 &

    local pid=$!
    PIDS+=("$pid")

    sleep 1
    if ! kill -0 "$pid" 2>/dev/null; then
        echo "$name exited early, check log: $log_file"
        exit 1
    fi
    echo "$name started, pid=$pid, log=$log_file"
}

trap cleanup EXIT INT TERM

require_bin "$BIN_DIR/sdm"
require_bin "$BIN_DIR/meta"
require_bin "$BIN_DIR/storage"

start_process "sdm" "$BIN_DIR/sdm"
start_process "meta" "$BIN_DIR/meta"
start_process "storage-1" "$BIN_DIR/storage" "conf/storage-1.yaml"
start_process "storage-2" "$BIN_DIR/storage" "conf/storage-2.yaml"
start_process "storage-3" "$BIN_DIR/storage" "conf/storage-3.yaml"

echo "raft test cluster started"
echo "logs are under: $LOG_DIR"
echo "press Ctrl-C to stop all processes"

wait

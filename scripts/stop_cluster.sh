#!/usr/bin/env bash
set -euo pipefail

find_pids_by_binary_name() {
    local binary_name="$1"
    ps -Ao pid=,command= | awk -v target="$binary_name" '
        {
            pid = $1;
            $1 = "";
            sub(/^ +/, "", $0);
            cmd = $1;
            n = split(cmd, parts, "/");
            base = parts[n];
            if (base == target) {
                print pid;
            }
        }
    '
}

kill_group() {
    local label="$1"
    local binary_name="$2"
    local pid_output
    local -a pids=()
    local -a remaining=()
    local pid

    pid_output="$(find_pids_by_binary_name "$binary_name")"
    while IFS= read -r pid; do
        if [[ -n "$pid" ]]; then
            pids+=("$pid")
        fi
    done <<< "$pid_output"

    if [[ ${#pids[@]} -eq 0 ]]; then
        echo "no $label process found"
        return
    fi

    echo "stopping $label: ${pids[*]}"
    kill "${pids[@]}" 2>/dev/null || true

    sleep 1

    pid_output="$(find_pids_by_binary_name "$binary_name")"
    while IFS= read -r pid; do
        if [[ -n "$pid" ]]; then
            remaining+=("$pid")
        fi
    done <<< "$pid_output"
    if [[ ${#remaining[@]} -gt 0 ]]; then
        echo "force killing $label: ${remaining[*]}"
        kill -9 "${remaining[@]}" 2>/dev/null || true
    fi
}

kill_group "sdm" "sdm"
kill_group "meta" "meta"
kill_group "storage" "storage"

echo "cluster processes stopped"
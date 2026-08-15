#!/usr/bin/env python3
"""Run a sustained SDK workload while killing its current Raft leader.

The script is intentionally an acceptance tool rather than a correctness test:
it records the business-visible result of a leader failover under a controlled
local-cluster setup.  The request trace contains one record per logical SDK
request, so retry attempts are not counted as separate business requests.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Optional


ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))

from scripts.internal.bench import (  # noqa: E402
    STORAGE_CONFS,
    materialize_bench_confs,
    reset_run_dirs,
    write_conf_from_template,
)
from scripts.internal.local_cluster import (  # noqa: E402
    BIN_DIR,
    BUILD_DIR,
    LocalCluster,
    build_targets,
    cluster_from_confs,
)


BENCH_BIN = BIN_DIR / "bench_client"
E2E_BIN = BIN_DIR / "e2e_client"
LEADER_RE = re.compile(r"\[ ROUTE_LEADER \] ([^:\s]+):(\d+)")

DEFAULT_CATALOG_HOST = "127.0.0.1"
DEFAULT_TOPO_HOST = "127.0.0.1"
DEFAULT_ZONE = "dc1"
DEFAULT_RESOURCE_POOL = "default"
DEFAULT_SEED_TIMEOUT_MS = 60_000
DEFAULT_READY_TIMEOUT_S = 30.0
DEFAULT_WORKLOAD_TIMEOUT_S = 180.0
DEFAULT_POLL_INTERVAL_S = 0.05


def env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return int(value)


def env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return float(value)


def env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value.lower() in ("1", "true", "yes", "on")


def absolute_repo_path(path: str) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return ROOT_DIR / candidate


def shell_command(command: list[str]) -> str:
    return " ".join(subprocess.list2cmdline([arg]) for arg in command)


def write_e2e_client_conf(path: Path, log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                'logger_name: "failover_e2e_client_log"',
                f'log_dir: "{log_dir}"',
                'log_filename: "e2e_client.log"',
                'log_level: "warning"',
                "log_to_console: false",
                "log_to_file: true",
                "",
            ]
        ),
        encoding="utf-8",
    )


def remove_previous_outputs(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    reset_run_dirs(
        output_dir / "conf",
        output_dir / "data",
        output_dir / "logs",
        output_dir / "process_logs",
    )
    for name in (
        "bench_result.json",
        "failover_result.json",
        "request_trace.jsonl",
        "workload.ready",
        "seed.log",
        "bench_client.log",
    ):
        (output_dir / name).unlink(missing_ok=True)


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def override_service_endpoints(
    *,
    topo_conf: Path,
    catalog_conf: Path,
    storage_confs: list[Path],
    catalog_host: str,
    catalog_port: int,
    topo_host: str,
    topo_port: int,
) -> None:
    write_conf_from_template(
        topo_conf,
        topo_conf,
        {"listen_host": topo_host, "port": topo_port},
    )
    write_conf_from_template(
        catalog_conf,
        catalog_conf,
        {
            "listen_host": catalog_host,
            "port": catalog_port,
            "topo_host": topo_host,
            "topo_port": topo_port,
        },
    )
    for storage_conf in storage_confs:
        write_conf_from_template(
            storage_conf,
            storage_conf,
            {"topo_host": topo_host, "topo_port": topo_port},
        )


def run_seed(
    *,
    e2e_conf: Path,
    seed_log: Path,
    catalog_host: str,
    catalog_port: int,
    topo_host: str,
    topo_port: int,
    db: str,
    table: str,
    zone: str,
    resource_pool: str,
    shard_count: int,
    replica_count: int,
    key_count: int,
    timeout_ms: int,
    poll_interval_ms: int,
) -> tuple[str, int, str]:
    command = [
        str(E2E_BIN),
        f"--conf={e2e_conf}",
        "--case=leader_failover_seed",
        f"--catalog_host={catalog_host}",
        f"--catalog_port={catalog_port}",
        f"--topo_host={topo_host}",
        f"--topo_port={topo_port}",
        f"--db={db}",
        f"--table={table}",
        f"--zone={zone}",
        f"--resource_pool={resource_pool}",
        f"--shard_count={shard_count}",
        f"--replica_count={replica_count}",
        f"--key_count={key_count}",
        f"--timeout_ms={timeout_ms}",
        f"--poll_interval_ms={poll_interval_ms}",
    ]
    print(f"[failover] seeding table: {shell_command(command)}", flush=True)
    with seed_log.open("w", encoding="utf-8") as log:
        completed = subprocess.run(
            command,
            cwd=ROOT_DIR,
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
    output = read_text(seed_log)
    print(output, end="" if output.endswith("\n") else "\n", flush=True)
    if completed.returncode != 0:
        raise RuntimeError(
            f"leader seed failed with exit code {completed.returncode}; "
            f"see {seed_log}"
        )

    matches = list(LEADER_RE.finditer(output))
    if not matches:
        raise RuntimeError(
            f"leader seed did not print [ ROUTE_LEADER ]; see {seed_log}"
        )
    leader_match = matches[-1]
    return leader_match.group(1), int(leader_match.group(2)), output


def wait_for_ready(
    process: subprocess.Popen,
    ready_file: Path,
    log_file: Path,
    timeout_s: float,
    poll_interval_s: float,
) -> int:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if ready_file.exists():
            contents = read_text(ready_file).strip()
            if contents:
                try:
                    return int(contents.splitlines()[0])
                except ValueError as exc:
                    raise RuntimeError(
                        f"invalid workload ready marker in {ready_file}"
                    ) from exc

        return_code = process.poll()
        if return_code is not None:
            raise RuntimeError(
                f"bench_client exited before workload ready, code={return_code}; "
                f"see {log_file}"
            )
        time.sleep(poll_interval_s)

    raise TimeoutError(
        f"timed out waiting for workload ready marker: {ready_file}; "
        f"see {log_file}"
    )


def wait_for_process(
    process: subprocess.Popen,
    timeout_s: float,
    poll_interval_s: float,
) -> int:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        return_code = process.poll()
        if return_code is not None:
            return return_code
        time.sleep(poll_interval_s)

    process.kill()
    return process.wait(timeout=5)


def stop_process(process: Optional[subprocess.Popen]) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as file:
        value = json.load(file)
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object in {path}")
    return value


def load_trace(path: Path) -> tuple[list[dict[str, Any]], int]:
    records: list[dict[str, Any]] = []
    parse_errors = 0
    if not path.exists():
        return records, 1

    with path.open(encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                parse_errors += 1
                continue
            if not isinstance(value, dict):
                parse_errors += 1
                continue
            required = ("request_id", "start_unix_ns", "end_unix_ns", "ok")
            if any(key not in value for key in required):
                parse_errors += 1
                continue
            records.append(value)
    return records, parse_errors


def request_is_valid(record: dict[str, Any]) -> bool:
    return (
        isinstance(record.get("start_unix_ns"), int)
        and isinstance(record.get("end_unix_ns"), int)
        and isinstance(record.get("ok"), bool)
    )


def summarize_trace(
    records: list[dict[str, Any]],
    parse_errors: int,
    kill_start_unix_ns: int,
) -> dict[str, Any]:
    valid_records = [record for record in records if request_is_valid(record)]
    post_kill = [
        record
        for record in valid_records
        if record["start_unix_ns"] >= kill_start_unix_ns
    ]
    overlapping_kill = [
        record
        for record in valid_records
        if record["start_unix_ns"] < kill_start_unix_ns
        and record["end_unix_ns"] >= kill_start_unix_ns
    ]

    post_kill_success = [record for record in post_kill if record["ok"]]
    post_kill_failure = [record for record in post_kill if not record["ok"]]
    overlapping_failure = [
        record for record in overlapping_kill if not record["ok"]
    ]

    first_success = min(
        post_kill_success,
        key=lambda record: record["end_unix_ns"],
        default=None,
    )
    first_success_end = (
        first_success["end_unix_ns"] if first_success is not None else None
    )
    recovery_window_ms = (
        (first_success_end - kill_start_unix_ns) / 1_000_000.0
        if first_success_end is not None
        else None
    )

    def rate(count: int, total: int) -> float:
        return count / total if total else 0.0

    post_kill_status_codes: dict[str, int] = {}
    for record in post_kill_failure:
        code = str(record.get("status_code", "unknown"))
        post_kill_status_codes[code] = post_kill_status_codes.get(code, 0) + 1

    unknown_count = sum(
        1
        for record in post_kill_failure
        if record.get("status_code") == 4000
    )

    return {
        "trace_records": len(records),
        "trace_valid_records": len(valid_records),
        "trace_parse_errors": parse_errors
        + (len(records) - len(valid_records)),
        "post_kill_requests": len(post_kill),
        "post_kill_success": len(post_kill_success),
        "post_kill_failure": len(post_kill_failure),
        "post_kill_failure_rate": rate(
            len(post_kill_failure), len(post_kill)
        ),
        "post_kill_unknown": unknown_count,
        "post_kill_unknown_rate": rate(unknown_count, len(post_kill)),
        "post_kill_status_codes": post_kill_status_codes,
        "overlapping_kill_requests": len(overlapping_kill),
        "overlapping_kill_failure": len(overlapping_failure),
        "overlapping_kill_failure_rate": rate(
            len(overlapping_failure), len(overlapping_kill)
        ),
        "first_success_after_kill_unix_ns": first_success_end,
        "recovery_window_ms": recovery_window_ms,
    }


def add_bench_arg(command: list[str], name: str, value: Any) -> None:
    command.append(f"--{name}={value}")


def build_bench_command(
    args: argparse.Namespace, paths: dict[str, Path]
) -> list[str]:
    command = [
        str(BENCH_BIN),
        f"--catalog_host={args.catalog_host}",
        f"--catalog_port={args.catalog_port}",
        f"--topo_host={args.topo_host}",
        f"--topo_port={args.topo_port}",
        f"--db={args.db}",
        f"--table={args.table}",
        f"--zone={args.zone}",
        f"--resource_pool={args.resource_pool}",
        f"--shard_count={args.shard_count}",
        f"--replica_count={args.replica_count}",
        f"--workload={args.workload}",
        f"--read_ratio={args.read_ratio}",
        f"--threads={args.threads}",
        f"--requests={args.requests}",
        f"--key_count={args.key_count}",
        f"--value_size={args.value_size}",
        f"--warmup_requests={args.warmup_requests}",
        f"--timeout_ms={args.timeout_ms}",
        f"--sdk_rpc_timeout_ms={args.sdk_rpc_timeout_ms}",
        f"--sdk_write_timeout_ms={args.sdk_write_timeout_ms}",
        f"--sdk_read_timeout_ms={args.sdk_read_timeout_ms}",
        f"--sdk_max_retries={args.max_retries}",
        f"--sdk_route_max_retries={args.route_max_retries}",
        f"--sdk_retry_delay_ms={args.retry_delay_ms}",
        f"--sdk_route_shard_refresh_interval_ms={args.route_refresh_interval_ms}",
        f"--output_json={paths['bench_result']}",
        f"--workload_ready_file={paths['ready']}",
        f"--request_trace_jsonl={paths['trace']}",
    ]
    if args.metrics_http_port > 0:
        add_bench_arg(command, "metrics_http_host", args.metrics_http_host)
        add_bench_arg(command, "metrics_http_port", args.metrics_http_port)
        add_bench_arg(command, "metrics_hold_seconds", args.metrics_hold_seconds)
    return command


def print_result(result: dict[str, Any]) -> None:
    print(
        "[failover] result: "
        f"post_kill={result['post_kill_requests']}, "
        f"success={result['post_kill_success']}, "
        f"failure={result['post_kill_failure']}, "
        f"failure_rate={result['post_kill_failure_rate']:.4f}, "
        f"unknown={result['post_kill_unknown']}, "
        f"recovery_window_ms={result['recovery_window_ms']}",
        flush=True,
    )
    print(
        f"[failover] full result: {result['failover_result']}",
        flush=True,
    )


def build_parser() -> argparse.ArgumentParser:
    run_id = os.environ.get("RUN_ID", time.strftime("%Y%m%d_%H%M%S"))
    parser = argparse.ArgumentParser(
        description="kill the current leader during a sustained SDK workload"
    )
    parser.add_argument("--run-id", default=run_id)
    parser.add_argument("--output-dir", default=os.environ.get("OUTPUT_DIR"))
    parser.add_argument(
        "--skip-build", action="store_true", default=env_bool("SKIP_BUILD")
    )

    parser.add_argument("--catalog-host", default=DEFAULT_CATALOG_HOST)
    parser.add_argument("--catalog-port", type=int, default=50048)
    parser.add_argument("--topo-host", default=DEFAULT_TOPO_HOST)
    parser.add_argument("--topo-port", type=int, default=50049)
    parser.add_argument("--db", default="failover_bench_db")
    parser.add_argument("--table", default="failover_bench_table")
    parser.add_argument("--zone", default=DEFAULT_ZONE)
    parser.add_argument("--resource-pool", default=DEFAULT_RESOURCE_POOL)
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument("--replica-count", type=int, default=3)

    parser.add_argument(
        "--workload", choices=("put", "get", "mixed"), default="put"
    )
    parser.add_argument("--read-ratio", type=float, default=0.8)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--requests", type=int, default=10_000)
    parser.add_argument("--warmup-requests", type=int, default=200)
    parser.add_argument("--key-count", type=int, default=1_000)
    parser.add_argument("--value-size", type=int, default=128)

    parser.add_argument("--timeout-ms", type=int, default=60_000)
    parser.add_argument("--sdk-rpc-timeout-ms", type=int, default=3_000)
    parser.add_argument("--sdk-write-timeout-ms", type=int, default=3_000)
    parser.add_argument("--sdk-read-timeout-ms", type=int, default=3_000)
    parser.add_argument(
        "--max-retries",
        type=int,
        default=env_int("MAX_RETRIES", 3),
        help="SDK storage retry budget per logical request",
    )
    parser.add_argument(
        "--route-max-retries",
        type=int,
        default=env_int("ROUTE_MAX_RETRIES", 1),
        help="SDK route refresh retry budget per logical request",
    )
    parser.add_argument("--retry-delay-ms", type=int, default=20)
    parser.add_argument(
        "--route-refresh-interval-ms",
        type=int,
        default=60_000,
        help="background route refresh interval; large value keeps the "
        "measurement driven by request failures",
    )

    parser.add_argument(
        "--seed-key-count", type=int, default=8,
        help="number of seed keys written before killing the leader",
    )
    parser.add_argument(
        "--seed-timeout-ms", type=int, default=DEFAULT_SEED_TIMEOUT_MS
    )
    parser.add_argument("--poll-interval-ms", type=int, default=500)
    parser.add_argument(
        "--ready-timeout-s",
        type=float,
        default=env_float("READY_TIMEOUT_S", DEFAULT_READY_TIMEOUT_S),
    )
    parser.add_argument(
        "--workload-timeout-s",
        type=float,
        default=env_float(
            "WORKLOAD_TIMEOUT_S", DEFAULT_WORKLOAD_TIMEOUT_S
        ),
    )
    parser.add_argument("--metrics-http-host", default="127.0.0.1")
    parser.add_argument("--metrics-http-port", type=int, default=0)
    parser.add_argument("--metrics-hold-seconds", type=int, default=0)
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.replica_count <= 0 or args.replica_count > len(STORAGE_CONFS):
        raise ValueError(
            f"--replica-count must be in [1, {len(STORAGE_CONFS)}]"
        )
    if args.shard_count <= 0 or args.threads <= 0 or args.requests <= 0:
        raise ValueError(
            "--shard-count, --threads, and --requests must be positive"
        )
    if args.warmup_requests < 0 or args.key_count <= 0 or args.value_size < 0:
        raise ValueError(
            "--warmup-requests must be non-negative; --key-count must be "
            "positive; --value-size must be non-negative"
        )
    if not 0.0 <= args.read_ratio <= 1.0:
        raise ValueError("--read-ratio must be in [0, 1]")
    for name in (
        "max_retries",
        "route_max_retries",
        "retry_delay_ms",
        "route_refresh_interval_ms",
    ):
        if getattr(args, name) < 0:
            raise ValueError(f"--{name.replace('_', '-')} must be non-negative")
    if args.metrics_http_port < 0 or args.metrics_hold_seconds < 0:
        raise ValueError("metrics arguments must be non-negative")


def run(args: argparse.Namespace) -> int:
    validate_args(args)
    output_arg = args.output_dir or str(
        BUILD_DIR / "bench" / "failover" / args.run_id
    )
    output_dir = absolute_repo_path(output_arg)
    remove_previous_outputs(output_dir)

    conf_dir = output_dir / "conf"
    data_dir = output_dir / "data"
    service_log_dir = output_dir / "logs"
    process_log_dir = output_dir / "process_logs"
    paths = {
        "bench_result": output_dir / "bench_result.json",
        "failover_result": output_dir / "failover_result.json",
        "trace": output_dir / "request_trace.jsonl",
        "ready": output_dir / "workload.ready",
        "seed_log": output_dir / "seed.log",
        "bench_log": output_dir / "bench_client.log",
    }

    if not args.skip_build:
        print(
            "[failover] building topo catalog storage bench_client e2e_client",
            flush=True,
        )
        build_targets("topo catalog storage bench_client e2e_client")
    if not BENCH_BIN.is_file() or not os.access(BENCH_BIN, os.X_OK):
        raise RuntimeError(f"bench_client is not executable: {BENCH_BIN}")
    if not E2E_BIN.is_file() or not os.access(E2E_BIN, os.X_OK):
        raise RuntimeError(f"e2e_client is not executable: {E2E_BIN}")

    subprocess.run(
        [str(ROOT_DIR / "scripts" / "stop_cluster.sh")],
        cwd=ROOT_DIR,
        check=True,
    )

    topo_conf, catalog_conf, storage_confs = materialize_bench_confs(
        conf_dir=conf_dir,
        data_dir=data_dir,
        service_log_dir=service_log_dir,
    )
    override_service_endpoints(
        topo_conf=topo_conf,
        catalog_conf=catalog_conf,
        storage_confs=storage_confs,
        catalog_host=args.catalog_host,
        catalog_port=args.catalog_port,
        topo_host=args.topo_host,
        topo_port=args.topo_port,
    )
    e2e_conf = conf_dir / "e2e_client.yaml"
    write_e2e_client_conf(e2e_conf, service_log_dir / "e2e_client")

    cluster: Optional[LocalCluster] = None
    bench_process: Optional[subprocess.Popen] = None
    try:
        cluster = cluster_from_confs(
            log_dir=process_log_dir,
            topo_conf=topo_conf,
            catalog_conf=catalog_conf,
            storage_confs=storage_confs,
            capture_output=True,
        )
        cluster.start()
        print(f"[failover] generated configs: {conf_dir}", flush=True)
        print(f"[failover] service logs: {service_log_dir}", flush=True)
        print(f"[failover] process logs: {process_log_dir}", flush=True)

        leader_host, leader_port, _ = run_seed(
            e2e_conf=e2e_conf,
            seed_log=paths["seed_log"],
            catalog_host=args.catalog_host,
            catalog_port=args.catalog_port,
            topo_host=args.topo_host,
            topo_port=args.topo_port,
            db=args.db,
            table=args.table,
            zone=args.zone,
            resource_pool=args.resource_pool,
            shard_count=args.shard_count,
            replica_count=args.replica_count,
            key_count=args.seed_key_count,
            timeout_ms=args.seed_timeout_ms,
            poll_interval_ms=args.poll_interval_ms,
        )
        killed_service = cluster.service_name_for_port(leader_port)
        print(
            f"[failover] seed leader={leader_host}:{leader_port} "
            f"service={killed_service}",
            flush=True,
        )

        bench_command = build_bench_command(args, paths)
        print(
            f"[failover] starting workload: {shell_command(bench_command)}",
            flush=True,
        )
        with paths["bench_log"].open("w", encoding="utf-8") as bench_log:
            bench_process = subprocess.Popen(
                bench_command,
                cwd=ROOT_DIR,
                stdout=bench_log,
                stderr=subprocess.STDOUT,
                text=True,
            )

        ready_unix_ns = wait_for_ready(
            bench_process,
            paths["ready"],
            paths["bench_log"],
            args.ready_timeout_s,
            DEFAULT_POLL_INTERVAL_S,
        )
        kill_start_unix_ns = time.time_ns()
        print(
            f"[failover] workload ready={ready_unix_ns}; "
            f"killing leader={killed_service} at {kill_start_unix_ns}",
            flush=True,
        )
        cluster.kill_service(killed_service)

        bench_exit_code = wait_for_process(
            bench_process,
            args.workload_timeout_s,
            DEFAULT_POLL_INTERVAL_S,
        )
        bench_process = None
        print(
            f"[failover] bench_client exit_code={bench_exit_code}; "
            f"log={paths['bench_log']}",
            flush=True,
        )

        bench_result: dict[str, Any] = {}
        if paths["bench_result"].exists():
            bench_result = load_json(paths["bench_result"])
        trace_records, trace_parse_errors = load_trace(paths["trace"])
        summary = summarize_trace(
            trace_records, trace_parse_errors, kill_start_unix_ns
        )
        result: dict[str, Any] = {
            "run_id": args.run_id,
            "kill_target": killed_service,
            "leader_host": leader_host,
            "leader_port": leader_port,
            "kill_start_unix_ns": kill_start_unix_ns,
            "workload_ready_unix_ns": ready_unix_ns,
            "bench_exit_code": bench_exit_code,
            "bench_result": bench_result,
            "config": {
                "workload": args.workload,
                "threads": args.threads,
                "requests": args.requests,
                "warmup_requests": args.warmup_requests,
                "key_count": args.key_count,
                "replica_count": args.replica_count,
                "sdk_max_retries": args.max_retries,
                "sdk_route_max_retries": args.route_max_retries,
                "sdk_retry_delay_ms": args.retry_delay_ms,
                "sdk_route_shard_refresh_interval_ms": (
                    args.route_refresh_interval_ms
                ),
            },
            "trace_file": str(paths["trace"]),
            "bench_result_file": str(paths["bench_result"]),
            "bench_log": str(paths["bench_log"]),
            **summary,
        }
        paths["failover_result"].write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print_result({**result, "failover_result": paths["failover_result"]})

        if bench_exit_code != 0:
            raise RuntimeError(
                f"bench_client failed with exit code {bench_exit_code}; "
                f"see {paths['bench_log']}"
            )
        return 0
    finally:
        stop_process(bench_process)
        if cluster is not None:
            cluster.stop()


def main(argv: list[str]) -> int:
    try:
        return run(build_parser().parse_args(argv))
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"[failover] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

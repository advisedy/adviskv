#!/usr/bin/env python3
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional, Union


ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))

from scripts.internal.local_cluster import (  # noqa: E402
    BIN_DIR,
    BUILD_DIR,
    build_targets,
    cluster_from_confs,
    connect_host,
    load_simple_kv_conf,
)
from scripts.internal.metrics_report import (  # noqa: E402
    MetricsTarget,
    MetricsSampler,
    build_report,
    write_text_report,
)


BENCH_BIN = BIN_DIR / "bench_client"
SDM_CONF = ROOT_DIR / "conf" / "sdm.yaml"
META_CONF = ROOT_DIR / "conf" / "meta.yaml"
STORAGE_CONFS = [
    ROOT_DIR / "conf" / "storage-1.yaml",
    ROOT_DIR / "conf" / "storage-2.yaml",
    ROOT_DIR / "conf" / "storage-3.yaml",
    ROOT_DIR / "conf" / "storage-4.yaml",
    ROOT_DIR / "conf" / "storage-5.yaml",
]
DEFAULT_BENCH_METRICS_PORT = 52000
DEFAULT_BENCH_METRICS_HOLD_SECONDS = 2


YamlValue = Union[bool, int, str, Path]


def env(name: str, default: str) -> str:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value


def repo_path(path: str) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return ROOT_DIR / candidate


def bench_args(argv: list[str]) -> list[str]:
    if argv and argv[0] == "--":
        return argv[1:]
    return argv


def bench_arg_value(args: list[str], name: str) -> Optional[str]:
    prefix = f"--{name}="
    for arg in reversed(args):
        if arg.startswith(prefix):
            return arg[len(prefix):]
    return None


def append_arg_if_missing(args: list[str], name: str, value: str) -> None:
    if bench_arg_value(args, name) is None:
        args.append(f"--{name}={value}")


def normalize_metrics_path(path: str) -> str:
    if not path.startswith("/"):
        return f"/{path}"
    return path


def can_bind_tcp(host: str, port: int) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((host, port))
            return True
    except OSError:
        return False


def pick_bench_metrics_port(host: str) -> int:
    preferred = int(env("BENCH_CLIENT_METRICS_PORT",
                        str(DEFAULT_BENCH_METRICS_PORT)))
    candidates = [preferred]
    if preferred == DEFAULT_BENCH_METRICS_PORT:
        candidates.extend(range(DEFAULT_BENCH_METRICS_PORT + 1,
                                DEFAULT_BENCH_METRICS_PORT + 51))
    for port in candidates:
        if port > 0 and can_bind_tcp(host, port):
            return port

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def prepare_bench_client_metrics_args(args: list[str]) -> list[str]:
    prepared = list(args)
    if bench_arg_value(prepared, "metrics_http_port") is None:
        host = connect_host(bench_arg_value(prepared, "metrics_http_host") or
                            "127.0.0.1")
        prepared.append(
            f"--metrics_http_port={pick_bench_metrics_port(host)}")
    append_arg_if_missing(
        prepared,
        "metrics_hold_seconds",
        env("BENCH_CLIENT_METRICS_HOLD_SECONDS",
            str(DEFAULT_BENCH_METRICS_HOLD_SECONDS)),
    )
    return prepared


def bench_client_metrics_target(args: list[str]) -> Optional[MetricsTarget]:
    port = bench_arg_value(args, "metrics_http_port")
    if port is None or int(port) <= 0:
        return None
    host = connect_host(bench_arg_value(args, "metrics_http_host") or
                        "127.0.0.1")
    path = normalize_metrics_path(
        bench_arg_value(args, "metrics_http_path") or "/metrics")
    return MetricsTarget("bench-client", f"http://{host}:{int(port)}{path}",
                         cumulative_from_zero=True)


def yaml_value(value: YamlValue) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    return f'"{value}"'


def write_conf_from_template(
    source: Path,
    dest: Path,
    overrides: dict[str, YamlValue],
) -> None:
    remaining = dict(overrides)
    lines: list[str] = []
    for raw_line in source.read_text(encoding="utf-8").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#") or ":" not in stripped:
            lines.append(raw_line)
            continue
        key = stripped.split(":", 1)[0].strip()
        if key in remaining:
            indent = raw_line[:len(raw_line) - len(raw_line.lstrip())]
            lines.append(f"{indent}{key}: {yaml_value(remaining.pop(key))}")
        else:
            lines.append(raw_line)
    if remaining:
        lines.append("")
        for key, value in remaining.items():
            lines.append(f"{key}: {yaml_value(value)}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text("\n".join(lines) + "\n", encoding="utf-8")


def reset_run_dirs(*dirs: Path) -> None:
    for path in dirs:
        shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True, exist_ok=True)


def materialize_bench_confs(
    *,
    conf_dir: Path,
    data_dir: Path,
    service_log_dir: Path,
) -> tuple[Path, Path, list[Path]]:
    services = [
        ("sdm", SDM_CONF),
        ("meta", META_CONF),
        *[(f"storage-{index}", conf) for index, conf in enumerate(STORAGE_CONFS, 1)],
    ]
    generated: dict[str, Path] = {}
    for name, source in services:
        generated[name] = conf_dir / source.name
        write_conf_from_template(
            source,
            generated[name],
            {
                "data_dir": data_dir / name,
                "logger_name": f"bench_{name}_log",
                "log_dir": service_log_dir / name,
                "log_filename": f"{name}.log",
                "log_level": env("BENCH_LOG_LEVEL", "info"),
                "log_to_console": False,
                "log_to_file": True,
            },
        )
    return (
        generated["sdm"],
        generated["meta"],
        [generated[f"storage-{index}"] for index in range(1, len(STORAGE_CONFS) + 1)],
    )


def metrics_target_from_conf(name: str, conf: Path) -> Optional[MetricsTarget]:
    values = load_simple_kv_conf(conf)
    enabled = values.get("metrics_http_enable", "false").lower()
    if enabled not in ("true", "1", "yes"):
        return None
    host = connect_host(values.get("metrics_http_host", "127.0.0.1"))
    port = values.get("metrics_http_port")
    if not port:
        return None
    path = normalize_metrics_path(values.get("metrics_http_path", "/metrics"))
    return MetricsTarget(name, f"http://{host}:{int(port)}{path}")


def storage_metrics_targets(storage_confs: list[Path]) -> list[MetricsTarget]:
    targets: list[MetricsTarget] = []
    for index, conf in enumerate(storage_confs, 1):
        target = metrics_target_from_conf(f"storage-{index}", conf)
        if target is not None:
            targets.append(target)
    return targets


def bench_metrics_targets(
    sdm_conf: Path,
    storage_confs: list[Path],
    bench_target: Optional[MetricsTarget],
) -> list[MetricsTarget]:
    targets: list[MetricsTarget] = []
    if bench_target is not None:
        targets.append(bench_target)
    sdm_target = metrics_target_from_conf("sdm", sdm_conf)
    if sdm_target is not None:
        targets.append(sdm_target)
    targets.extend(storage_metrics_targets(storage_confs))
    return targets


def run_bench(argv: list[str]) -> None:
    metrics_report_enabled = env("BENCH_METRICS_REPORT", "0") == "1"
    run_id = env("RUN_ID", time.strftime("%Y%m%d_%H%M%S"))
    output_dir = repo_path(
        env("OUTPUT_DIR", str(BUILD_DIR / "bench_results" / run_id))
    )
    conf_dir = output_dir / "conf"
    data_dir = output_dir / "data"
    service_log_dir = output_dir / "logs"
    process_log_dir = output_dir / "process_logs"
    output_dir.mkdir(parents=True, exist_ok=True)

    if env("SKIP_BUILD", "0") != "1":
        print("[bench] building local cluster and bench_client", flush=True)
        build_targets("sdm meta storage bench_client")
    if not BENCH_BIN.exists() or not os.access(BENCH_BIN, os.X_OK):
        raise RuntimeError(f"bench_client not executable: {BENCH_BIN}")

    subprocess.run([str(ROOT_DIR / "scripts" / "stop_cluster.sh")],
                   cwd=ROOT_DIR,
                   check=True)

    reset_run_dirs(conf_dir, data_dir, service_log_dir, process_log_dir)
    sdm_conf, meta_conf, storage_confs = materialize_bench_confs(
        conf_dir=conf_dir,
        data_dir=data_dir,
        service_log_dir=service_log_dir,
    )
    cluster = cluster_from_confs(
        log_dir=process_log_dir,
        sdm_conf=sdm_conf,
        meta_conf=meta_conf,
        storage_confs=storage_confs,
        capture_output=True,
    )
    metrics_sampler = None
    try:
        cluster.start()
        print(f"[bench] generated confs: {conf_dir}", flush=True)
        print(f"[bench] service data: {data_dir}", flush=True)
        print(f"[bench] service logs: {service_log_dir}", flush=True)
        print(f"[bench] process logs: {process_log_dir}", flush=True)
        command_args = bench_args(argv)
        bench_target = None
        if metrics_report_enabled:
            command_args = prepare_bench_client_metrics_args(command_args)
            bench_target = bench_client_metrics_target(command_args)

        command = [str(BENCH_BIN), *command_args]
        print(f"[bench] running bench_client: {' '.join(command)}", flush=True)

        if metrics_report_enabled:
            targets = bench_metrics_targets(sdm_conf, storage_confs,
                                            bench_target)
            metrics_sampler = MetricsSampler(targets)
            metrics_sampler.start()

        try:
            subprocess.run(command, cwd=ROOT_DIR, check=True)
        finally:
            if metrics_sampler is not None:
                report = build_report(metrics_sampler.stop())
                metrics_report_path = output_dir / "metrics_report.txt"
                write_text_report(report, metrics_report_path)
                print(f"[bench] metrics report: {metrics_report_path}",
                      flush=True)
        print("[bench] done", flush=True)
    finally:
        cluster.stop()


def main() -> int:
    try:
        run_bench(sys.argv[1:])
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"[bench] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

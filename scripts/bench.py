#!/usr/bin/env python3
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Union


ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from scripts.local_cluster import (  # noqa: E402
    BIN_DIR,
    BUILD_DIR,
    build_targets,
    cluster_from_confs,
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


def run_bench(argv: list[str]) -> None:
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
    try:
        cluster.start()
        print(f"[bench] generated confs: {conf_dir}", flush=True)
        print(f"[bench] service data: {data_dir}", flush=True)
        print(f"[bench] service logs: {service_log_dir}", flush=True)
        print(f"[bench] process logs: {process_log_dir}", flush=True)
        command = [str(BENCH_BIN), *bench_args(argv)]
        print(f"[bench] running bench_client: {' '.join(command)}", flush=True)
        subprocess.run(command, cwd=ROOT_DIR, check=True)
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

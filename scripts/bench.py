#!/usr/bin/env python3
import os
import subprocess
import sys
import time
from pathlib import Path


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
]


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


def run_bench(argv: list[str]) -> None:
    run_id = env("RUN_ID", time.strftime("%Y%m%d_%H%M%S"))
    output_dir = repo_path(
        env("OUTPUT_DIR", str(BUILD_DIR / "bench_results" / run_id))
    )
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

    cluster = cluster_from_confs(
        log_dir=process_log_dir,
        sdm_conf=SDM_CONF,
        meta_conf=META_CONF,
        storage_confs=STORAGE_CONFS,
        capture_output=True,
    )
    try:
        cluster.start()
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
